//! `exit_actor()`：让 actor 自己结束。
//!
//! Ray 的退出机制是一条必须一直往上走的异常：`AbstractRayRuntime::ExitActor` 抛
//! `RayIntentionalSystemExitException`（`cpp/src/ray/runtime/abstract_ray_runtime.cc:275-281`），
//! `GetExecuteResult` 用 `catch (RayIntentionalSystemExitException &)` 把它变成
//! `Status::IntentionalSystemExit`（`cpp/src/ray/runtime/task/task_executor.cc:97-100`），
//! `ExecuteTask` 见到该状态直接返回（同文件 209-211），core worker 据此关掉 actor 且
//! 不按 `max_restarts` 重启。
//!
//! Rust 帧里抛不了这个异常，C 的 trampoline 捕获它又会掐掉机制。所以
//! [`exit_actor`] 只置一个线程局部标志并返回；宏生成的三个回调在用户代码返回之后
//! 读这个标志，置位时返回 `rayrust_sys` 的哨兵长度，C 侧在 Rust 帧退出之后再抛对应
//! 的异常。详见 `crates/rayrust-sys/wrapper/ray_c.h` 里两个哨兵的说明。
//!
//! ## 本地模式
//!
//! 本地模式（[`RayConfig::local`](crate::RayConfig)）没有 core worker，actor 方法在
//! 驱动进程里直接执行，退出语义没有承接方：`TaskExecutor::Invoke` 的
//! `catch (std::exception &)`（`cpp/src/ray/runtime/task/task_executor.cc:336-341`）会把
//! 哨兵抛出的异常当成普通失败打包成错误载荷（`abstract_ray_runtime.cc:31-36`）。所以
//! [`exit_actor`] 在本地模式**不置标志、直接返回**，是一个明确的空操作——让哨兵抛出
//! 去只会得到与退出无关的结果（返回 `()` 的方法看到 `Ok(())`，返回值的方法看到
//! `Serialization` 解码错误），比空操作更难理解。

use std::cell::Cell;

thread_local! {
    /// 本次回调里调用过 [`exit_actor`]。线程局部：Ray 在一次方法调用里只用一个线程
    /// 执行用户代码，所以标志不会跨调用串台。
    static EXIT_REQUESTED: Cell<bool> = const { Cell::new(false) };
}

/// 结束当前 actor。
///
/// 集群模式下在 `#[rayrust::actor]` 的方法体里有意义：方法返回之后 actor 被关闭，
/// 关闭不会被 `max_restarts` 重启（上游把这次退出记成 `Status::IntentionalSystemExit`，
/// 与失败区分开）。在 `#[rayrust::remote]` 任务或 actor 的 `new()` 里调用会让该次调用
/// 失败：C 侧抛 `RayException("exit_actor() can only be called from inside an actor
/// method")`。任务失败从对象读取回报；构造函数失败的 actor 不会启动，`actor_create`
/// 照常返回 handle，第一次方法调用报 `RayError::Actor`。
///
/// 本地模式（[`RayConfig::local`](crate::RayConfig)）里这个调用是空操作：本地模式没有
/// core worker，上面的退出语义由 core worker 承担，本地模式没有对应的路径。三类位置
/// 都是「什么也不发生」：
///
/// - actor 方法里调用：方法照常返回它自己的值，actor 继续活着，状态不变；
/// - `#[rayrust::remote]` 任务里调用：任务照常返回它自己的值；
/// - `new()` 里调用：actor 正常创建。
///
/// 空操作是刻意选的。本地模式里让哨兵照旧抛出去，异常会被 `TaskExecutor::Invoke` 的
/// `catch (std::exception &)`（`cpp/src/ray/runtime/task/task_executor.cc:336-341`）当成
/// 普通失败打包成错误载荷；返回 `()` 的方法看到 `Ok(())`（看起来像退出成功），返回值
/// 的方法看到一条与退出无关的 `Serialization("invalid type: unit value, ...")` 解码错误
/// （#22：`get` 只取载荷里的第一个值）。两种都比空操作更难理解。
///
/// 返回 `()`：集群模式下调用之后这段代码仍然会跑完，结果被丢弃（退出路径不序列化
/// 返回值）。
///
/// # Example
/// ```ignore
/// #[rayrust::actor]
/// impl Worker {
///     fn new() -> Self { Worker }
///     fn stop(&self) { rayrust::exit_actor(); }
/// }
/// ```
pub fn exit_actor() {
    if crate::runtime::is_local_mode() {
        // 本地模式没有 actor 关闭路径，见模块文档的「本地模式」一节。
        return;
    }
    EXIT_REQUESTED.with(|flag| flag.set(true));
}

/// 清掉上一次回调留下的标志。宏生成的回调在调用用户代码之前调用它。
///
/// 标志只在回调结束时被读走，正常路径上本来就是 false；这里清一次是为了让「用户
/// 代码 panic 导致标志没被读走」这种情形不会把退出语义带进同一个线程上的下一次
/// 调用。
pub fn clear_exit_request() {
    EXIT_REQUESTED.with(|flag| flag.set(false));
}

/// 读走标志：返回 true 表示本次回调里调用过 [`exit_actor`]，同时把它清掉。
/// 宏生成的回调在用户代码返回之后调用它。
pub fn take_exit_request() -> bool {
    EXIT_REQUESTED.with(|flag| flag.replace(false))
}

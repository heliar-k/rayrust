//! Example: `TaskOptions` 的五个字段（name、resources、placement group、
//! runtime_env、默认资源）。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/task_options_test cluster
//! ./target/release/examples/task_options_test local
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! # 环境前提
//!
//! - 集群必须是 **1 个 CPU + 1 个 `Custom` 资源**：启动命令是
//!   `--num-cpus=1 --resources='{"Custom":1}'`。第 2、5 项靠单 bundle 的组占住
//!   唯一 CPU 才有区分力，第 4 项靠 `CPU 2.0` 的请求在这台机器上永远无法满足才有
//!   区分力，第 5 项的两个对照靠节点上确实有 1 个 `Custom` 才有区分力。
//! - 同一集群上别的 job 正在占着 CPU 或 `Custom` 时会干扰第 1、2、4、5 项（四处都
//!   依赖「空集群只剩待测任务能跑」：第 1 项的 `env_check` 是默认任务，按默认值
//!   要 1 个 CPU）。非 detached 的 placement group 随创建它的 job 结束而销毁，
//!   所以本示例自己上一轮留下的组不需要手动清理。
//! - 示例启动时清掉自己的日志目录 `task_options_test_logs/`，结束时再清一次，不在
//!   工作树里留痕迹。
//!
//! # 检查项
//!
//! 1. runtime_env：同步与异步的 `_with_options` 调用器各一次正向断言（读到
//!    `hello`），各一次不带 runtime_env 的对照（读到 `None`）。C++ 工人进程确实
//!    拿到这个环境变量。
//! 2. name + placement group + 异步 options（一段流程）：建单 bundle `{"CPU":1}`
//!    的组 → 用异步 `_with_options` 提交「绑 bundle 0 + `CPU 1`」的 `sleep_for`
//!    并断言期限内完成（它只能在 bundle 里跑，完成就证明组已经生效）→ 再用异步
//!    `_with_options` 提交「带名字 + `CPU 1` + 不绑组」的 `sleep_for`，`get_timeout`
//!    断言 `Timeout`（组占着 CPU）→ 句柄存活时 `cancel` 断言 `Ok` → 引用变成失败 →
//!    从本轮驱动日志里取到 `task_name=<本轮名字>` → 移除组 → 把同一个「不绑组 +
//!    `CPU 1`」的任务再提交一次并断言它现在能完成（证明上一步的不就绪确实来自组）。
//! 3. 未知 placement group：随机 18 字节 id → 提交返回 `Err(RayError::Ffi(_))`，
//!    进程存活。C 侧 `parse_call_options` 对查不到的 group 抛异常，绝不静默退回
//!    默认调度。同一项还断言孤立的 `bundle_index`（没有 group）经 `Ray::task_call`
//!    在提交前就被拒，不会因为 `to_json` 不输出它而静默丢掉字段。
//! 4. resources：`resource("CPU", 2.0)` 的任务在 1 CPU 集群上 `wait` 期限内不就绪
//!    （随后 cancel），`resource("CPU", 1.0)` 的任务正常完成。
//! 5. 默认资源：建单 bundle `{"CPU":1}` 的组占住唯一 CPU 后，`TaskOptions::new()`
//!    的任务 `wait` 期限内不就绪（默认要 1 个 CPU）；显式 `resource("CPU", 0.0)`
//!    的任务立刻完成；只写 `Custom 1.0` 的任务同样不就绪（CPU 1.0 被合并进来），
//!    补上 `CPU 0.0` 的对照完成；`task_call_python` 的任务同样不就绪；移除组之后
//!    前面两个不就绪的任务都完成。`to_json()` 为空串由单元测试覆盖。
//!
//! 本地模式（`local`）跑能跑的子集并说明其余跳过的原因：
//! `LocalModeTaskSubmitter::SubmitTask` 把 `CallOptions` 整个丢弃
//! （`Submit(invocation, {})`），所以 runtime_env（第 1 项的对照读到 `None`）、
//! resources（第 4 项）与默认资源（第 5 项）都没有效果；placement group 在本地
//! 模式直接报错（第 3 项可以观察到）；name（第 2 项）没有 core worker 日志可查。
//!
//! # 名字的观察点
//!
//! 任务名进任务规格之后，能自动断言的地方是驱动进程自己的 core worker 日志：
//! `task_manager.cc` 在任务失败时按 INFO 级打印 `Task failed: ... task_name=<名字>`
//! （被 cancel 的排队任务也走这条）。示例用 `RayConfig::log_dir` 把驱动日志收到
//! `task_options_test_logs/`，只读文件名里含**本进程 pid** 的
//! `cpp-core-driver-*.log`，任务名也带本进程 pid，所以上一轮留下的日志不会让断言
//! 恒真。

use std::time::{Duration, Instant};

use rayrust::prelude::*;

/// 与 worker .so 的 env_check 同名同签名：集群模式下由工人执行，本地模式下
/// 由这个进程执行。
#[rayrust::remote]
fn env_check(name: String) -> Option<String> {
    std::env::var(&name).ok()
}

/// 与 worker .so 的 sleep_for 同名同签名，第 2、4、5 项用它占资源。
#[rayrust::remote]
fn sleep_for(seconds: f64) -> i64 {
    std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
    0
}

/// 读一个对象的期限（毫秒）。
const GET_TIMEOUT_MS: i32 = 5000;

/// `wait` 的期限（毫秒）：证明排队任务在期限内不就绪。
const WAIT_TIMEOUT_MS: i32 = 4000;

/// 绑组任务的读数期限：必须在此之前完成。
const PG_BOUND_TIMEOUT_MS: i32 = 8000;

/// 任务时长：短于期限，长于调度余量。
const TASK_SLEEP_S: f64 = 1.0;

/// 第 5 项的等待期限（毫秒）：默认要 1 个 CPU 的任务在期限内不该就绪。
const DEFAULT_CPU_WAIT_MS: i32 = 1000;

/// 第 5 项的短任务时长：远短于期限，有条件时立刻就能跑。
const SHORT_TASK_SLEEP_S: f64 = 0.2;

/// 第 5 项调的 Python 任务模块。
const PYTHON_MODULE: &str = "rayrust_test";

/// 本示例自己的驱动日志目录（集群模式第 2 项扫描这里）。
const DRIVER_LOG_DIR: &str = "task_options_test_logs";

/// 第 1 项的任务从环境变量读它。
const ENV_VAR: &str = "RAYRUST_TASK_ENV";

/// 清掉示例自己产生的日志目录。
fn clean_own_output() {
    let _ = std::fs::remove_dir_all(DRIVER_LOG_DIR);
}

/// 本轮的任务名：带本进程 pid，上一轮的名字不会再命中。
fn named_task_name() -> String {
    format!("rayrust_task_options_named_{}", std::process::id())
}

/// 在本轮驱动日志里找一行包含 `needle` 的记录（只扫一遍）。
///
/// 只读文件名以 `_<本进程 pid>.log` 结尾的 `cpp-core-driver-*.log`：日志目录里
/// 可能有上一轮留下的文件，它们的 pid 不同。读不了的条目跳过，不影响其它条目。
fn scan_driver_log(needle: &str) -> Option<String> {
    let suffix = format!("_{}.log", std::process::id());
    let entries = std::fs::read_dir(DRIVER_LOG_DIR).ok()?;
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(_) => continue,
        };
        let name = match path.file_name() {
            Some(name) => name.to_string_lossy().to_string(),
            None => continue,
        };
        if !name.starts_with("cpp-core-driver-") || !name.ends_with(&suffix) {
            continue;
        }
        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(_) => continue,
        };
        for line in content.lines() {
            if line.contains(needle) {
                return Some(line.trim().to_string());
            }
        }
    }
    None
}

/// 等日志写完：每 50ms 扫一次，最多 2 秒。
///
/// cancel 之后 core worker 写日志与进程读文件之间有延迟，读一次就断言会偶然失败。
async fn wait_driver_log_line(needle: &str) -> Option<String> {
    const RETRY_TIMES: usize = 40;
    const RETRY_INTERVAL_MS: u64 = 50;
    for attempt in 0..RETRY_TIMES {
        if let Some(line) = scan_driver_log(needle) {
            return Some(line);
        }
        if attempt + 1 < RETRY_TIMES {
            tokio::time::sleep(Duration::from_millis(RETRY_INTERVAL_MS)).await;
        }
    }
    None
}

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
        .log_dir(DRIVER_LOG_DIR)
}

/// 1. runtime_env：env_vars 到达工人进程；同步与异步调用器各一次正向断言，
/// 不带 runtime_env 的对照读到 None。
async fn runtime_env_check(ray: &Ray, cluster: bool) {
    println!("\n--- 1. runtime_env 的 env_vars ---");
    let opts = TaskOptions::new().runtime_env(r#"{"env_vars":{"RAYRUST_TASK_ENV":"hello"}}"#);

    let sync = env_check_remote_with_options(ray, &opts, ENV_VAR.to_string())
        .expect("submit the sync env_check with env_vars")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read the sync env_check with env_vars");
    let asynchronous = env_check_remote_async_with_options(ray, &opts, ENV_VAR.to_string())
        .await
        .expect("submit the async env_check with env_vars")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read the async env_check with env_vars");
    if cluster {
        // 一次断言同时钉住两个调用器：任何一个漏传 options 都会让这一条失败。
        assert_eq!(
            (sync.as_deref(), asynchronous.as_deref()),
            (Some("hello"), Some("hello")),
            "带 env_vars 的同步/异步调用器读到 (sync {:?}, async {:?})",
            sync,
            asynchronous
        );
        println!("  同步 _with_options + env_vars → {:?}", sync);
        println!("  异步 _with_options + env_vars → {:?}", asynchronous);
    } else {
        // 本地模式丢弃 CallOptions，这里只要求提交与读取不失败。
        println!(
            "  同步 _with_options + env_vars → {:?}（本地模式丢弃 CallOptions）",
            sync
        );
        println!(
            "  异步 _with_options + env_vars → {:?}（本地模式丢弃 CallOptions）",
            asynchronous
        );
    }

    let plain_sync = env_check_remote(ray, ENV_VAR.to_string())
        .expect("submit the sync env_check without env_vars")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read the sync env_check without env_vars");
    let plain_async = env_check_remote_async(ray, ENV_VAR.to_string())
        .await
        .expect("submit the async env_check without env_vars")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read the async env_check without env_vars");
    assert_eq!(
        (plain_sync, plain_async),
        (None, None),
        "不带 runtime_env 的任务读到了不该存在的值"
    );
    println!("  对照（不带 runtime_env）→ 同步 None，异步 None");
}

/// 2. name + placement group + 异步 options：一段流程三个证据。
async fn name_and_placement_group_check(ray: &Ray, cluster: bool) {
    println!("\n--- 2. name + placement group + 异步 options ---");
    if !cluster {
        println!("  本地模式跳过：LocalModeTaskSubmitter 丢弃 CallOptions，PG 绑定");
        println!("  无从谈起；AbstractRayRuntime 的 GetPlacementGroupById 在本地模式");
        println!("  解引用空的 GCS accessor 会终止进程，C 侧直接报错（第 3 项）。");
        return;
    }

    // 单 bundle 的组占走集群唯一的 CPU。
    let pg_id = ray
        .placement_group_create("rayrust_task_options_pg", r#"[{"CPU":1}]"#, 0)
        .expect("create the placement group");
    println!("  placement_group_create → {} 字节 id", pg_id.len());

    // a) 先证明组已经生效：绑 bundle 0 + CPU 1。这个任务只能在 bundle 里跑，它完成
    // 就说明 raylet 已经把 bundle 建好、CPU 也从节点可用资源里扣掉了。不先做这一步
    // 就提交不绑组的对照，会在「组刚创建、资源还没扣」的窗口里被直接调度（实测第一
    // 跑会踩到）。
    let opts = TaskOptions::new()
        .placement_group(pg_id.clone(), 0)
        .resource("CPU", 1.0);
    let bound = sleep_for_remote_async_with_options(ray, &opts, TASK_SLEEP_S)
        .await
        .expect("submit the bound sleep_for");
    let started = Instant::now();
    let value = bound
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the bound task must finish within its deadline");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!(
        "  绑 bundle 0 + CPU 1（异步 _with_options）→ 完成，耗时 {:?}（组已生效）",
        started.elapsed()
    );

    // b) 带名字 + CPU 1 + 不绑组，用异步 _with_options 提交：组占着 CPU，它排队。
    let name = named_task_name();
    let queued_opts = TaskOptions::new().name(name.clone()).resource("CPU", 1.0);
    let queued: ObjectRef<i64> =
        sleep_for_remote_async_with_options(ray, &queued_opts, TASK_SLEEP_S)
            .await
            .expect("submit the named queued sleep_for")
            .cast();
    println!("  已提交排队任务，名字 {}", name);

    // c) 期限内拿不到结果：返回的是 Timeout。
    let started = Instant::now();
    match queued.get_timeout(WAIT_TIMEOUT_MS) {
        Err(RayError::Timeout(msg)) => println!(
            "  排队任务 get_timeout({}ms) → Timeout（{:?}）：{}",
            WAIT_TIMEOUT_MS,
            started.elapsed(),
            msg
        ),
        Err(other) => panic!("排队任务返回了别的错误变体: {}", other),
        Ok(v) => panic!(
            "组已经生效，不绑组的任务却完成了（{}）——绑定的对照不成立",
            v
        ),
    }

    // d) 句柄存活时取消：返回 Ok。
    ray.cancel(queued.id(), false, true)
        .expect("cancel the queued task with its handle alive");
    println!("  句柄存活时 cancel → Ok");

    // e) 引用变成失败，不再继续等资源。
    match queued.get_timeout(PG_BOUND_TIMEOUT_MS) {
        Err(RayError::Timeout(_)) => panic!("cancel 之后任务还在等资源，没有变成失败"),
        Err(other) => println!("  cancel 之后读取 → {}（引用已失败）", other),
        Ok(v) => panic!("被 cancel 的任务返回了 Ok({})", v),
    }

    // f) 名字进入任务规格：本轮驱动日志里有 task_name=<本轮名字>。
    let needle = format!("task_name={}", name);
    let line = wait_driver_log_line(&needle).await.unwrap_or_else(|| {
        panic!(
            "驱动日志 {} 里没有本轮（pid {}）的 {}",
            DRIVER_LOG_DIR,
            std::process::id(),
            needle
        )
    });
    println!("  日志证据: {}", line);

    // g) 移除组，再把同一个不绑组的任务提交一次：这次它应该能跑完。这一步把
    //「b/c 的不就绪」归因到组本身，而不是任务或调用器有问题。
    ray.placement_group_remove(&pg_id)
        .expect("remove the placement group after the cancel check");
    println!("  placement group 已 remove，资源归还");

    let unbound_after = sleep_for_remote_async_with_options(ray, &queued_opts, TASK_SLEEP_S)
        .await
        .expect("submit the unbound sleep_for after the group is gone");
    let started = Instant::now();
    let value = unbound_after
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the same task must finish once the group is gone");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!(
        "  移除组后同一个不绑组任务 → 完成，耗时 {:?}",
        started.elapsed()
    );
}

/// 3. 未知 placement group 与孤立的 bundle_index：提交时立即报错，不静默调度。
async fn unknown_pg_check(ray: &Ray) {
    println!("\n--- 3. 未知 placement group ---");
    // PlacementGroupID 的长度是 18（14 唯一字节 + 4 字节 JobID）。集群模式：查不到
    // 的 group 报错；本地模式：直接不支持。两者都是提交侧的 Ffi。
    let unknown: Vec<u8> = (0..18u8).collect();
    let opts = TaskOptions::new().placement_group(unknown, 0);
    let name = serialize(&ENV_VAR.to_string()).expect("serialize");
    match ray.task_call("env_check", &[&name], &[], &opts) {
        Err(RayError::Ffi(msg)) => println!("  随机 18 字节 group id → Ffi({})", msg),
        Err(other) => panic!("未知 group 返回了别的错误变体: {}", other),
        Ok(_) => panic!("未知 group 的任务被提交了（静默退回默认调度）"),
    }
    println!("  进程存活：错误发生在提交侧，没有 abort");

    // 孤立的 bundle_index：`to_json` 不输出它，提交前必须被拒，不能静默丢掉字段。
    let standalone = TaskOptions {
        bundle_index: 0,
        ..Default::default()
    };
    match ray.task_call("env_check", &[&name], &[], &standalone) {
        Err(RayError::Ffi(msg)) => println!("  孤立 bundle_index=0（无 group）→ Ffi({})", msg),
        Err(other) => panic!("孤立 bundle_index 返回了别的错误变体: {}", other),
        Ok(_) => panic!("孤立 bundle_index 被静默丢掉了，任务照常提交"),
    }
    println!("  进程存活：孤立 bundle_index 在提交前就被拒");
}

/// 4. resources：`CPU 2.0` 在这台 1 CPU 的集群上永远不就绪，`CPU 1.0` 正常完成。
async fn resources_check(ray: &Ray, cluster: bool) {
    println!("\n--- 4. resources ---");
    if !cluster {
        println!("  本地模式跳过：LocalModeTaskSubmitter 丢弃资源需求，CPU 2.0 与");
        println!("  CPU 1.0 没有区别，构不成对照。");
        return;
    }

    let opts = TaskOptions::new().resource("CPU", 2.0);
    let oversize: ObjectRef<i64> = sleep_for_remote_with_options(ray, &opts, TASK_SLEEP_S)
        .expect("submit the CPU 2.0 sleep_for")
        .cast();
    let started = Instant::now();
    let (ready, unready) = ray
        .wait(&[oversize.clone()], 1, WAIT_TIMEOUT_MS)
        .expect("wait on the CPU 2.0 task");
    assert_eq!(ready.len(), 0, "CPU 2.0 的任务在 1 CPU 集群上就绪了");
    assert_eq!(unready.len(), 1);
    println!(
        "  resource(\"CPU\", 2.0) → wait({}ms) 后仍不就绪（{:?}）",
        WAIT_TIMEOUT_MS,
        started.elapsed()
    );
    ray.cancel(oversize.id(), false, true)
        .expect("cancel the CPU 2.0 task with its handle alive");
    println!("  句柄存活时 cancel → Ok");

    let opts = TaskOptions::new().resource("CPU", 1.0);
    let fits = sleep_for_remote_with_options(ray, &opts, TASK_SLEEP_S)
        .expect("submit the CPU 1.0 sleep_for");
    let started = Instant::now();
    let value = fits
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the CPU 1.0 task must finish");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!(
        "  resource(\"CPU\", 1.0) → 完成，耗时 {:?}",
        started.elapsed()
    );
}

/// 5. 默认资源：不给 resources 的任务要 1 个 CPU。
async fn default_cpu_check(ray: &Ray, cluster: bool) {
    println!("\n--- 5. 默认资源 = 1 个 CPU ---");
    if !cluster {
        println!("  本地模式跳过：LocalModeTaskSubmitter 丢弃 CallOptions，资源需求");
        println!("  不生效，「占住 CPU 后默认任务排队」无从观察。");
        println!("  to_json() 为空串由单元测试覆盖（cargo test -p rayrust --lib）");
        return;
    }

    // 单 bundle 的组占走集群唯一的 CPU，先确认 raylet 已经把它扣掉。
    let pg_id = ray
        .placement_group_create("rayrust_task_options_default_cpu", r#"[{"CPU":1}]"#, 0)
        .expect("create the placement group");
    let bound_opts = TaskOptions::new()
        .placement_group(pg_id.clone(), 0)
        .resource("CPU", 1.0);
    let value = sleep_for_remote_with_options(ray, &bound_opts, SHORT_TASK_SLEEP_S)
        .expect("submit the bound sleep_for")
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the bound task must finish within its deadline");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!("  绑组任务完成（组已占住唯一 CPU）");

    // a) 空资源表 = Ray 的默认值 1 个 CPU：此刻没有空闲 CPU。
    let default_task: ObjectRef<i64> =
        sleep_for_remote_with_options(ray, &TaskOptions::new(), SHORT_TASK_SLEEP_S)
            .expect("submit the default-options sleep_for");
    let (ready, unready) = ray
        .wait(&[default_task.clone()], 1, DEFAULT_CPU_WAIT_MS)
        .expect("wait on the default-options task");
    assert_eq!(ready.len(), 0, "默认任务在 CPU 被占住时就绪了");
    assert_eq!(unready.len(), 1);
    println!(
        "  TaskOptions::new() → wait({}ms) 后仍不就绪（默认要 1 个 CPU）",
        DEFAULT_CPU_WAIT_MS
    );

    // b) 显式 CPU 0.0 的任务现在就能跑，所以 a) 的不就绪来自 CPU。
    let zero_cpu_opts = TaskOptions::new().resource("CPU", 0.0);
    let value = sleep_for_remote_with_options(ray, &zero_cpu_opts, SHORT_TASK_SLEEP_S)
        .expect("submit the CPU 0.0 sleep_for")
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the CPU 0.0 task must finish while the CPU is taken");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!("  resource(\"CPU\", 0.0) → 完成（零 CPU 的任务不受占住的 CPU 影响）");

    // c) 只给别的资源、不给 CPU 时，Ray 会合并 1.0 个 CPU。
    let custom_only: ObjectRef<i64> = sleep_for_remote_with_options(
        ray,
        &TaskOptions::new().resource("Custom", 1.0),
        SHORT_TASK_SLEEP_S,
    )
    .expect("submit the Custom-only sleep_for");
    let (ready, unready) = ray
        .wait(&[custom_only.clone()], 1, DEFAULT_CPU_WAIT_MS)
        .expect("wait on the Custom-only task");
    assert_eq!(ready.len(), 0, "只写 Custom 的任务在 CPU 被占住时就绪了");
    assert_eq!(unready.len(), 1);
    println!(
        "  resource(\"Custom\", 1.0)（没有 CPU 键）→ wait({}ms) 后仍不就绪（合并了 CPU）",
        DEFAULT_CPU_WAIT_MS
    );
    ray.cancel(custom_only.id(), false, true)
        .expect("cancel the Custom-only task with its handle alive");
    println!("  句柄存活时 cancel → Ok");

    // 对照：同样要 Custom，显式写 CPU 0.0 就能跑，所以节点上确实有 1 个 Custom，
    // c) 的不就绪来自合并进来的 CPU。
    let custom_zero_cpu_opts = TaskOptions::new()
        .resource("Custom", 1.0)
        .resource("CPU", 0.0);
    let value = sleep_for_remote_with_options(ray, &custom_zero_cpu_opts, SHORT_TASK_SLEEP_S)
        .expect("submit the Custom + CPU 0.0 sleep_for")
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the Custom + CPU 0.0 task must finish");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!("  resource(\"Custom\", 1.0) + resource(\"CPU\", 0.0) → 完成");

    // d) Python 任务入口没有 options，所以它总是要 1 个 CPU。
    let arg = serialize(&SHORT_TASK_SLEEP_S).expect("serialize the Python sleep_for argument");
    let python_task = ray
        .task_call_python(PYTHON_MODULE, "sleep_for", &[&arg], &[])
        .expect("submit the Python sleep_for");
    let (ready, unready) = ray
        .wait(&[python_task.clone()], 1, DEFAULT_CPU_WAIT_MS)
        .expect("wait on the Python sleep_for");
    assert_eq!(ready.len(), 0, "Python 任务在 CPU 被占住时就绪了");
    assert_eq!(unready.len(), 1);
    println!(
        "  task_call_python(sleep_for) → wait({}ms) 后仍不就绪（总是要 1 个 CPU）",
        DEFAULT_CPU_WAIT_MS
    );

    // e) 移除组：a) 与 d) 里不就绪的任务都该跑完，不就绪确实来自被占住的 CPU。
    ray.placement_group_remove(&pg_id)
        .expect("remove the placement group after the Custom resource check");
    println!("  placement group 已 remove，资源归还");

    let value = python_task
        .cast::<i64>()
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the Python task must finish once the group is gone");
    assert_eq!(value, 0, "Python sleep_for 返回了 {}", value);
    println!("  移除组后同一个 Python 任务 → 完成");

    let value = default_task
        .get_timeout(PG_BOUND_TIMEOUT_MS)
        .expect("the default-options task must finish once the group is gone");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!("  移除组后 TaskOptions::new() 的任务 → 完成");
    println!("  to_json() 为空串由单元测试覆盖（cargo test -p rayrust --lib）");
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== TaskOptions 五个字段 ({}) ===", mode);
    clean_own_output();
    let cluster = mode == "cluster";
    let config = if cluster {
        cluster_config()
    } else {
        RayConfig::local()
    };
    let ray = Ray::connect(&config).expect("Ray::connect");

    runtime_env_check(&ray, cluster).await;
    name_and_placement_group_check(&ray, cluster).await;
    unknown_pg_check(&ray).await;
    resources_check(&ray, cluster).await;
    default_cpu_check(&ray, cluster).await;

    // 让 tokio 的 blocking pool 里的 FFI 调用有机会收尾，避免退出竞态。
    tokio::time::sleep(Duration::from_millis(200)).await;

    clean_own_output();
    println!("\n✓ TaskOptions 检查全部通过");
}

//! Example: `Ray::actor_create` 的空函数名检查，以及非空但没人注册的名字的行为。
//!
//! 空函数名在本地模式里跑不起来。本地模式只构造驱动
//! （cpp/src/ray/runtime/local_mode_ray_runtime.cc:30-34），创建任务在调用线程里跑完了
//! `CreateActor` 才返回（cpp/src/ray/runtime/task/local_mode_task_submitter.cc:115-121、
//! :143-151）：`TaskExecutionHandler` 在任何查找之前抛 `Task function name is empty`
//! （cpp/src/ray/runtime/task/task_executor.cc:32-34），`Invoke` 的 catch 把这段文本
//! 放进创建任务的返回值、跳过 actor context 登记（同文件 :336-339，:327-331 那条分支
//! 没有走到），所以进程里没有这个 id 的 actor context，第一次调用在
//! `actor_contexts_.at` 上抛裸的 `_Map_base::at`
//! （cpp/src/ray/runtime/task/local_mode_task_submitter.cc:110-112）。C 入口
//! `ray_actor_create_with_options` 在取运行时之前用 `RayError::Ffi` 拒掉空函数名，
//! 消息以 `the function name is empty` 开头；被拒的调用不提交任务、不登记名字，
//! 所以随后按名字查还是 `Ok(None)`。
//!
//! 一次运行跑完全部形态，本地模式，不需要集群：
//! ```text
//! cargo run --release --example actor_create_name_test
//! ```
//!
//! 检查项，每项都打印实际结果：
//! 1. `actor_create("")`：被拒成 `RayError::Ffi`，进程存活
//! 2. 空函数名 + `name("t25_refused")`：同样被拒，随后 `get_actor("t25_refused")` 是
//!    `Ok(None)`（被拒的创建没有登记名字）
//! 3. `actor_create(" ")`：只有空白字符的名字不 trim，创建返回 `Ok`，之后对它的一次调用
//!    失败
//! 4. `actor_create("no_such_function")`：没人注册的名字保留 Ray 的异步语义，创建返回
//!    `Ok`，失败在第一次调用或读值时暴露
//! 5. 对照：`#[rayrust::actor]` 定义的 `Counter` 用 `__rayrust_actor_factory_counter`
//!    建出来，调用 `__rayrust_actor_factory_counter::get` 读到 7

use rayrust::prelude::*;

/// `#[rayrust::actor]` 给 `Counter` 注册的构造名。
const COUNTER_FACTORY: &str = "__rayrust_actor_factory_counter";

/// `Counter` 的 `get` 注册的方法名。
const COUNTER_GET: &str = "__rayrust_actor_factory_counter::get";

/// 第 2 项用的名字。
const REFUSED_NAME: &str = "t25_refused";

/// 读一个调用结果时的期限（毫秒）。
const READ_TIMEOUT_MS: i32 = 15_000;

struct Counter {
    value: i64,
}

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }
}

/// 建一个 actor 并读它的一次调用，成功时给出值，失败时给出调用与读值两段里先出错的那一段。
fn call_and_read(ray: &Ray, actor_id: &[u8], method: &str) -> Result<i64, RayError> {
    let reference = ray.actor_call(actor_id, method, &[], &[])?;
    reference.cast::<i64>().get_timeout(READ_TIMEOUT_MS)
}

/// 一次应当被拒绝的创建：必须是 `RayError::Ffi`，消息以空函数名那一句开头。
fn expect_refused(label: &str, result: Result<ActorHandle, RayError>) {
    match result {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("the function name is empty"),
                "{}: 错误文本不是空函数名那一句: {}",
                label,
                message
            );
            println!("  {} → Ffi({})", label, message);
        }
        Err(other) => panic!("{}: 返回了别的错误变体: {}", label, other),
        Ok(handle) => panic!("{}: 被接受了，交回 {} 字节的 id", label, handle.id().len()),
    }
}

/// 一次保留异步语义的创建：返回 `Ok`，之后那次调用读不出值。
fn expect_created_then_call_fails(ray: &Ray, label: &str, func_name: &str, method: &str) {
    let handle = match ray.actor_create(func_name, &[], &ActorOptions::new()) {
        Ok(handle) => {
            println!("  {} 创建 → Ok({} 字节 id)", label, handle.id().len());
            handle
        }
        Err(error) => panic!("{}: 创建没有返回 Ok: {:?}", label, error),
    };
    match call_and_read(ray, handle.id(), method) {
        Ok(value) => panic!(
            "{}: 调用读到了值 {}，这个 actor 里没有那个函数",
            label, value
        ),
        Err(error) => println!("  {} 调用 → Err({:?})", label, error),
    }
}

fn main() {
    println!("=== actor_create 的函数名检查 (local) ===");
    let ray = Ray::connect(&RayConfig::local()).expect("Ray::connect");
    assert!(ray.is_local_mode(), "这个例子只跑本地模式");

    println!("\n--- 1. 空函数名 ---");
    expect_refused(
        "actor_create(\"\")",
        ray.actor_create("", &[], &ActorOptions::new()),
    );
    let value = ray
        .put(&42i32)
        .expect("拒绝之后 put")
        .get()
        .expect("拒绝之后 get");
    assert_eq!(value, 42, "拒绝之后 put + get 读到 {}", value);
    println!("  进程存活，put + get 照常读到 {}", value);

    println!("\n--- 2. 空函数名 + name(\"{}\") ---", REFUSED_NAME);
    expect_refused(
        "actor_create(\"\", name)",
        ray.actor_create("", &[], &ActorOptions::new().name(REFUSED_NAME)),
    );
    match ray.get_actor(REFUSED_NAME, "") {
        Ok(None) => println!("  get_actor(\"{}\") → Ok(None)", REFUSED_NAME),
        Ok(Some(handle)) => panic!(
            "被拒的创建登记了名字，查回来 {} 字节的 id",
            handle.id().len()
        ),
        Err(error) => panic!("get_actor(\"{}\") 返回了错误: {:?}", REFUSED_NAME, error),
    }

    println!("\n--- 3. 只有空白字符的名字 ---");
    expect_created_then_call_fails(&ray, "actor_create(\" \")", " ", COUNTER_GET);
    println!("  进程存活");

    println!("\n--- 4. 非空但没人注册的名字 ---");
    expect_created_then_call_fails(
        &ray,
        "actor_create(\"no_such_function\")",
        "no_such_function",
        "no_such_function::get",
    );
    println!("  进程存活");

    println!("\n--- 5. 对照：真实 actor ---");
    let start = serialize(&7i64).expect("serialize");
    let counter = match ray.actor_create(COUNTER_FACTORY, &[&start[..]], &ActorOptions::new()) {
        Ok(handle) => {
            println!(
                "  {} 创建 → Ok({} 字节 id)",
                COUNTER_FACTORY,
                handle.id().len()
            );
            handle
        }
        Err(error) => panic!("对照的创建失败了: {:?}", error),
    };
    match call_and_read(&ray, counter.id(), COUNTER_GET) {
        Ok(value) => {
            assert_eq!(value, 7, "{} 读到 {}", COUNTER_GET, value);
            println!("  {} → Ok({})", COUNTER_GET, value);
        }
        Err(error) => panic!("对照的调用失败了: {:?}", error),
    }

    drop(ray);
    println!("\n✓ 函数名检查在 5 项上成立");
}

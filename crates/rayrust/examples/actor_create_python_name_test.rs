//! Example: `Ray::actor_create_python` 的空 module 名与空 class 名检查。
//!
//! Python actor 由 module 与 class 两个名字确定，worker 在自己的 Python 侧解析它们，
//! 所以任何一个为空都交出一个跑不起来的 actor：驱动只提交创建任务，调用方拿到 `Ok`
//! 与 16 字节 id，第一次读值得到 `RayError::Actor` 变体（消息为从错误载荷提取出的错误
//! 文本，无法提取时按 UTF-8 lossy 呈现原始字节）。空 module 名在
//! `python/ray/_private/function_manager.py:531`（`load_actor_class`）→ `:589`
//! （`_load_actor_class_from_local`）→ `:138`（`load_function_or_class_from_local`）→
//! `importlib.import_module(module_name)` 抛 `ValueError: Empty module name`；空 class
//! 名在 `python/ray/_raylet.pyx:2253` 抛
//! `TypeError: module.__new__(X): X is not a type object (module)`（实测 traceback 只有
//! `_raylet.pyx:2432` 与 `:2253` 两帧，没有进 `load_actor_class`）。两条都由 C 入口
//! `ray_actor_create_python_with_options` 在取运行时之前拒掉，消息分别以
//! `the module name is empty`、`the class name is empty` 开头；被拒的调用不提交任务、
//! 不登记名字。
//!
//! 只跑集群模式：本地模式整体拒绝这个入口，建不出任何 Python actor。
//! ```text
//! PYTHONPATH=tests/python ray start --head --port=6452 --num-cpus=2 --include-dashboard=false
//! RAY_ADDRESS=172.18.92.125:6452 RAY_NODE_IP=172.18.92.125 \
//!   ./target/release/examples/actor_create_python_name_test
//! ```
//! 起集群时必须带 `PYTHONPATH=tests/python`：Python actor 的 worker 要 `import
//! rayrust_test`。
//!
//! 检查项，每项都打印实际结果：
//! 1. `actor_create_python("", "", ...)`：被拒成 `RayError::Ffi`，消息含
//!    `the module name is empty`；随后 `put`/`get` 照常（进程存活）
//! 2. `actor_create_python("rayrust_test", "", ...)`：被拒成 `RayError::Ffi`，消息含
//!    `the class name is empty`
//! 3. `actor_create_python("", "Counter", ...)`：被拒成 `RayError::Ffi`，消息含
//!    `the module name is empty`
//! 4. 空名字 + `name("t26_refused")`：同样被拒，随后 `get_actor("t26_refused", "")` 是
//!    `Ok(None)`（被拒的创建没有登记名字）
//! 5. 对照：`actor_create_python("rayrust_test", "Counter", ...)`（构造参数 7）建出来，
//!    `actor_call_python(id, "get", ...)` 读回 7

use rayrust::prelude::*;

/// 第 4 项用的名字。
const REFUSED_NAME: &str = "t26_refused";

/// 读一个调用结果时的期限（毫秒）：包含 actor 进程启动的时间。
const READ_TIMEOUT_MS: i32 = 20_000;

/// 集群模式的配置：地址和节点 IP 从环境变量取。
fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    RayConfig::new(&address).node_ip(&node_ip)
}

/// 一次应当被拒绝的创建：必须是 `RayError::Ffi`，消息里含 `needle`。
fn expect_refused<T>(label: &str, result: Result<T, RayError>, needle: &str) {
    match result {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains(needle),
                "{}: 错误文本里没有 {:?}: {}",
                label,
                needle,
                message
            );
            println!("  {} → Ffi({})", label, message);
        }
        Err(other) => panic!("{}: 返回了别的错误变体: {}", label, other),
        Ok(_) => panic!("{}: 被接受了", label),
    }
}

fn main() {
    println!("=== actor_create_python 的空名字检查 (cluster) ===");
    let ray = Ray::connect(&cluster_config()).expect("Ray::connect");
    assert!(
        !ray.is_local_mode(),
        "这个例子只跑集群模式：本地模式整体拒绝 actor_create_python"
    );

    println!("\n--- 1. 空 module 名与空 class 名 ---");
    expect_refused(
        "actor_create_python(\"\", \"\")",
        ray.actor_create_python("", "", &[], &ActorOptions::new()),
        "the module name is empty",
    );
    let value = ray
        .put(&42i32)
        .expect("拒绝之后 put")
        .get()
        .expect("拒绝之后 get");
    assert_eq!(value, 42, "拒绝之后 put + get 读到 {}", value);
    println!("  进程存活，put + get 照常读到 {}", value);

    println!("\n--- 2. 空 class 名 ---");
    expect_refused(
        "actor_create_python(\"rayrust_test\", \"\")",
        ray.actor_create_python("rayrust_test", "", &[], &ActorOptions::new()),
        "the class name is empty",
    );

    println!("\n--- 3. 空 module 名 ---");
    expect_refused(
        "actor_create_python(\"\", \"Counter\")",
        ray.actor_create_python("", "Counter", &[], &ActorOptions::new()),
        "the module name is empty",
    );

    println!("\n--- 4. 空名字 + name(\"{}\") ---", REFUSED_NAME);
    expect_refused(
        "actor_create_python(\"\", \"\", name)",
        ray.actor_create_python("", "", &[], &ActorOptions::new().name(REFUSED_NAME)),
        "the module name is empty",
    );
    match ray.get_actor(REFUSED_NAME, "") {
        Ok(None) => println!("  get_actor(\"{}\") → Ok(None)", REFUSED_NAME),
        Ok(Some(handle)) => panic!(
            "被拒的创建登记了名字，查回来 {} 字节的 id",
            handle.id().len()
        ),
        Err(error) => panic!("get_actor(\"{}\") 返回了错误: {:?}", REFUSED_NAME, error),
    }

    println!("\n--- 5. 对照：真实 Python actor ---");
    let start = serialize(&7i64).expect("serialize the constructor argument");
    let counter = match ray.actor_create_python(
        "rayrust_test",
        "Counter",
        &[&start[..]],
        &ActorOptions::new(),
    ) {
        Ok(handle) => {
            println!("  actor_create_python → Ok({} 字节 id)", handle.id().len());
            handle
        }
        Err(error) => panic!("对照的创建失败了: {:?}", error),
    };
    let call = ray
        .actor_call_python(counter.id(), "get", &[], &[])
        .expect("submit get");
    let value: i64 = call.cast().get_timeout(READ_TIMEOUT_MS).expect("read get");
    assert_eq!(value, 7, "Python Counter 的 get() 读到 {}", value);
    println!("  actor_call_python(id, \"get\") → Ok({})", value);

    drop(ray);
    println!("\n✓ 空名字检查在 5 项上成立");
}

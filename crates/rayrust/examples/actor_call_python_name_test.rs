//! Example: `Ray::actor_call_python` 的空方法名检查。
//!
//! Python actor 的方法按实例属性名调用，而空名字不总是跑不通：Python 实例可以用
//! `setattr(Class, "", method)` 带一个空名字的属性，跨语言调用这个名字也能达到它
//! （实测 Ray 2.51.1 读到 42，actor 随后照常回答）。驱动判不出这个 actor 有没有这样
//! 的属性，而没有的时候这次调用会让 actor 的 worker 以 `SYSTEM_ERROR`
//! （`Worker exits unexpectedly`）退出，traceback 为
//! `python/ray/_raylet.pyx:2432`（`task_execution_handler`）→ `:2291`（它调用
//! `execute_task`）→ `:1873`，那一行是
//! `function_name = execution_info.function_name`，抛
//! `AttributeError: 'dict' object has no attribute 'function_name'`；驱动第一次读值
//! 得到 `RayError::Actor` 变体（消息为从错误载荷提取出的错误文本，无法提取时按 UTF-8
//! lossy 呈现原始字节），这个 actor 之后不能再调用。C 入口
//! `ray_actor_call_python` 在 id 宽度检查之后、取运行时之前拒掉空方法名（两种情形都
//! 拒，包括方法存在的那种），消息以 `the method name is empty` 开头；被拒的调用不提交
//! 任务，actor 照常活着。带内部 NUL 的方法名在进入 C 之前由 Rust FFI 边界拒绝，返回
//! 点名 `method_name` 且含 `interior NUL` 的 `RayError::Ffi`，同样不提交任务，actor
//! 照常存活。
//!
//! 只跑集群模式：本地模式整体拒绝这个入口。
//! ```text
//! PYTHONPATH=tests/python ray start --head --port=6482 --num-cpus=2 --include-dashboard=false
//! RAY_ADDRESS=172.18.92.125:6482 RAY_NODE_IP=172.18.92.125 \
//!   RAY_WORKER_SO=$PWD/target/release/librayrust_worker.so \
//!   ./target/release/examples/actor_call_python_name_test
//! ```
//! 起集群时必须带 `PYTHONPATH=tests/python`：Python actor 的 worker 要 `import
//! rayrust_test`。`RAY_WORKER_SO` 是第 4 项那个 C++ actor 的工人进程要加载的共享库。
//!
//! 检查项，每项都打印实际结果：
//! 1. 对照：`actor_create_python("rayrust_test", "Counter", start=7)` 建出来，
//!    `actor_call_python(id, "increment", [1])` 读到 8
//! 2. `actor_call_python(id, "")`：被拒成 `RayError::Ffi`，消息含
//!    `the method name is empty`；随后同一个 actor 的 `get` 仍读到 8
//! 3. `actor_call_python(id, "in\0crement")`：在 Rust FFI 边界被拒成 `RayError::Ffi`，
//!    消息含 `method_name` 与 `interior NUL`；随后 `get` 仍读到 8
//! 4. 对照：C++ actor（`__rayrust_actor_factory_counter`，构造参数 41）的
//!    `actor_call(id, "")`：被拒成 `RayError::Ffi`，消息含 `Function not found`；随后
//!    它的 `get` 读到 41（C++ 入口在驱动里构造 `RemoteFunctionHolder` 时就抛
//!    `Function not found`，`cpp/include/ray/api/ray_runtime.h:41-45`，任务根本没提交）

use rayrust::prelude::*;

/// `#[rayrust::actor]` 给 `Counter` 注册的构造名。
const COUNTER_FACTORY: &str = "__rayrust_actor_factory_counter";

/// `Counter` 的 `get` 注册的方法名。
const COUNTER_GET: &str = "__rayrust_actor_factory_counter::get";

/// 读一个调用结果时的期限（毫秒）：包含 actor 进程启动的时间。
const READ_TIMEOUT_MS: i32 = 20_000;

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

/// 集群模式的配置：地址、节点 IP 与 C++ 工人进程要加载的共享库都从环境变量取。
fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
}

/// 一次应当被拒绝的调用：必须是 `RayError::Ffi`，消息里含全部 `needles`。
fn expect_refused<T>(label: &str, result: Result<T, RayError>, needles: &[&str]) {
    match result {
        Err(RayError::Ffi(message)) => {
            for needle in needles {
                assert!(
                    message.contains(needle),
                    "{}: error message does not contain {:?}: {}",
                    label,
                    needle,
                    message
                );
            }
            println!("  {} -> Ffi({})", label, message);
        }
        Err(other) => panic!("{}: returned unexpected error variant: {}", label, other),
        Ok(_) => panic!("{}: unexpectedly succeeded", label),
    }
}

/// 读一次方法调用的值，期限同上。
fn read_i64(ray: &Ray, actor_id: &[u8], method: &str) -> i64 {
    ray.actor_call_python(actor_id, method, &[], &[])
        .unwrap_or_else(|error| panic!("{}: submit failed: {:?}", method, error))
        .cast::<i64>()
        .get_timeout(READ_TIMEOUT_MS)
        .unwrap_or_else(|error| panic!("{}: get failed: {:?}", method, error))
}

fn main() {
    println!("=== actor_call_python empty method name check (cluster) ===");
    let ray = Ray::connect(&cluster_config()).expect("Ray::connect");
    assert!(
        !ray.is_local_mode(),
        "this example only runs in cluster mode: local mode rejects actor_call_python"
    );

    println!("\n--- 1. Control: real Python actor ---");
    let start = serialize(&7i64).expect("serialize the constructor argument");
    let counter = match ray.actor_create_python(
        "rayrust_test",
        "Counter",
        &[&start[..]],
        &ActorOptions::new(),
    ) {
        Ok(handle) => {
            println!("  actor_create_python -> Ok({}-byte id)", handle.id().len());
            handle
        }
        Err(error) => panic!("control creation failed: {:?}", error),
    };
    let step = serialize(&1i64).expect("serialize the increment argument");
    let value: i64 = ray
        .actor_call_python(counter.id(), "increment", &[&step[..]], &[])
        .expect("submit increment")
        .cast()
        .get_timeout(READ_TIMEOUT_MS)
        .expect("read increment");
    assert_eq!(value, 8, "Python Counter increment(1) returned {}", value);
    println!(
        "  actor_call_python(id, \"increment\", [1]) -> Ok({})",
        value
    );

    println!("\n--- 2. Empty method name ---");
    expect_refused(
        "actor_call_python(id, \"\")",
        ray.actor_call_python(counter.id(), "", &[], &[]),
        &["the method name is empty"],
    );
    let value = read_i64(&ray, counter.id(), "get");
    assert_eq!(value, 8, "get after empty method name returned {}", value);
    println!("  get on the same actor still returned {}", value);

    println!("\n--- 3. Method name with interior NUL ---");
    expect_refused(
        "actor_call_python(id, \"in\\0crement\")",
        ray.actor_call_python(counter.id(), "in\0crement", &[], &[]),
        &["method_name", "interior NUL"],
    );
    let value = read_i64(&ray, counter.id(), "get");
    assert_eq!(value, 8, "get after interior NUL returned {}", value);
    println!("  get on the same actor still returned {}", value);

    println!("\n--- 4. Control: C++ actor empty method name ---");
    let start_cpp = serialize(&41i64).expect("serialize the C++ constructor argument");
    let cpp_counter =
        match ray.actor_create(COUNTER_FACTORY, &[&start_cpp[..]], &ActorOptions::new()) {
            Ok(handle) => {
                println!(
                    "  {} created -> Ok({}-byte id)",
                    COUNTER_FACTORY,
                    handle.id().len()
                );
                handle
            }
            Err(error) => panic!("control creation failed: {:?}", error),
        };
    expect_refused(
        "actor_call(cpp_id, \"\")",
        ray.actor_call(cpp_counter.id(), "", &[], &[]),
        &["Function not found"],
    );
    let value: i64 = ray
        .actor_call(cpp_counter.id(), COUNTER_GET, &[], &[])
        .unwrap_or_else(|error| panic!("{}: submit failed: {:?}", COUNTER_GET, error))
        .cast()
        .get_timeout(READ_TIMEOUT_MS)
        .unwrap_or_else(|error| panic!("{}: get failed: {:?}", COUNTER_GET, error));
    assert_eq!(value, 41, "C++ Counter get returned {}", value);
    println!("  {} -> Ok({})", COUNTER_GET, value);

    drop(ray);
    println!("\nPASS: empty method name checks passed on all 4 cases");
}

//! Example: `Ray::task_call_python` 的空 module 名检查与空 function 名的语义。
//!
//! 空 module 名永远跑不通：worker 的加载器走 `importlib.import_module("")`
//! （`python/ray/_private/function_manager.py:138`），它在任何查找之前就抛
//! `ValueError: Empty module name`；异常逃出 `task_execution_handler`
//! （`python/ray/_raylet.pyx:2432` → `:2275`），worker 以 `SYSTEM_ERROR` 退出，
//! 驱动第一次读值得到 `RayError::Worker` 变体（消息为从错误载荷提取出的错误文本，无法
//! 提取时按 UTF-8 lossy 呈现原始字节），worker 池补一个新 worker。C 入口 `ray_task_call_python` 在取
//! 运行时之前就拒掉这个输入，错误里带 `the module name is empty`。
//!
//! 空 function 名不拒，因为它能跑通：加载器把名字按点切开、丢掉空片段
//! （`function_manager.py:139`），空名字查到的是 module 对象本身，加载器把它登记
//! 成任务的函数（`:391-395`）。普通 module 不可调用，任务就以
//! `TypeError: 'module' object is not callable` 失败——普通任务失败，worker 活着，
//! 驱动第一次读值得到 `RayError::Task` 变体，消息为从错误载荷提取出的格式化异常文本
//! （Python traceback；无法提取时按 UTF-8 lossy 呈现原始字节；第 3 项的断言只匹配 `RayError::Task` 变体）；而 module 对象可以可调用
//! （`types.ModuleType` 子类带 `__call__`，或者往 `sys.modules` 放可调用对象），那时
//! 任务真的执行（实测返回 3）。第 3 项断言的就是这条异步语义。
//!
//! 带内部 NUL 的 module 名与 function 名在进入 C 之前由 Rust FFI 边界拒绝，返回点名
//! `module` 或 `function` 且含 `interior NUL` 的 `RayError::Ffi`，任务不会提交。
//!
//! 只跑集群模式：本地模式对 Python 任务没有 Python 执行器。
//! ```text
//! PYTHONPATH=tests/python ray start --head --port=6492 --num-cpus=2 --include-dashboard=false
//! RAY_ADDRESS=172.18.92.125:6492 RAY_NODE_IP=172.18.92.125 \
//!   ./target/release/examples/task_call_python_name_test
//! ```
//! 起集群时必须带 `PYTHONPATH=tests/python`：Python 任务的 worker 要 `import
//! rayrust_test`。
//!
//! 检查项，每项都打印实际结果：
//! 1. 对照：`task_call_python("rayrust_test", "add", [2, 1])` 读到 3
//! 2. `task_call_python("", "add", ...)`：被拒成 `RayError::Ffi`，消息含
//!    `the module name is empty`；随后对照仍读到 3
//! 3. `task_call_python("rayrust_test", "", ...)`：提交成功，读值失败（普通任务失败，
//!    空 function 名没有被拒），随后对照仍读到 3
//! 4. `task_call_python("rayrust\0test", "add", ...)`：在 Rust FFI 边界被拒成
//!    `RayError::Ffi`，消息含 `module` 与 `interior NUL`；随后对照仍读到 3
//! 5. `task_call_python("rayrust_test", "ad\0d", ...)`：在 Rust FFI 边界被拒成
//!    `RayError::Ffi`，消息含 `function` 与 `interior NUL`；随后对照仍读到 3
//!
//! 非空但没人注册的名字同样保留 Ray 的异步语义：驱动没有函数表，无从判断，实测读值
//! 会一直等到超时；这个入口不拒这类名字，示例里不做断言。

use rayrust::prelude::*;

/// Python 测试模块；worker 必须能 import 到它。
const PYTHON_MODULE: &str = "rayrust_test";

/// 读一个调用结果时的期限（毫秒）。
const READ_TIMEOUT_MS: i32 = 20_000;

/// 集群模式的配置：地址与节点 IP 从环境变量取。
fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    RayConfig::new(&address).node_ip(&node_ip)
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

/// 对照：`rayrust_test.add(2, 1)` 读到 3。
fn check_control(ray: &Ray, label: &str) {
    let a = serialize(&2i32).expect("serialize the first argument");
    let b = serialize(&1i32).expect("serialize the second argument");
    let value: i32 = ray
        .task_call_python(PYTHON_MODULE, "add", &[&a[..], &b[..]], &[])
        .unwrap_or_else(|error| panic!("{}: submit failed: {:?}", label, error))
        .cast()
        .get_timeout(READ_TIMEOUT_MS)
        .unwrap_or_else(|error| panic!("{}: get failed: {:?}", label, error));
    assert_eq!(value, 3, "{} returned {}", label, value);
    println!(
        "  task_call_python(\"{}\", \"add\", [2, 1]) -> Ok({})",
        PYTHON_MODULE, value
    );
}

fn main() {
    println!("=== task_call_python empty name check (cluster) ===");
    let ray = Ray::connect(&cluster_config()).expect("Ray::connect");
    assert!(
        !ray.is_local_mode(),
        "this example only runs in cluster mode: local mode has no Python executor"
    );

    println!("\n--- 1. Control: real Python task ---");
    check_control(&ray, "control");

    println!("\n--- 2. Empty module name ---");
    let a = serialize(&2i32).expect("serialize the first argument");
    let b = serialize(&1i32).expect("serialize the second argument");
    expect_refused(
        "task_call_python(\"\", \"add\", [2, 1])",
        ray.task_call_python("", "add", &[&a[..], &b[..]], &[]),
        &["the module name is empty"],
    );
    check_control(&ray, "control after empty module name");

    println!("\n--- 3. Empty function name (submit succeeds, get fails) ---");
    match ray.task_call_python(PYTHON_MODULE, "", &[&a[..], &b[..]], &[]) {
        Ok(reference) => {
            println!(
                "  task_call_python(\"{}\", \"\", [2, 1]) -> Ok(submitted)",
                PYTHON_MODULE
            );
            match reference.cast::<i64>().get_timeout(READ_TIMEOUT_MS) {
                Ok(value) => panic!("empty function name unexpectedly returned {}", value),
                Err(error @ RayError::Task(_)) => println!("  get -> {:?}", error),
                Err(other) => panic!(
                    "empty function name get returned unexpected error variant: {:?}",
                    other
                ),
            }
        }
        Err(error) => panic!(
            "empty function name was unexpectedly refused: {:?}",
            error
        ),
    }
    check_control(&ray, "control after empty function name");

    println!("\n--- 4. Module name with interior NUL ---");
    expect_refused(
        "task_call_python(\"rayrust\\0test\", \"add\", [2, 1])",
        ray.task_call_python("rayrust\0test", "add", &[&a[..], &b[..]], &[]),
        &["module", "interior NUL"],
    );
    check_control(&ray, "control after interior NUL module name");

    println!("\n--- 5. Function name with interior NUL ---");
    expect_refused(
        "task_call_python(\"rayrust_test\", \"ad\\0d\", [2, 1])",
        ray.task_call_python(PYTHON_MODULE, "ad\0d", &[&a[..], &b[..]], &[]),
        &["function", "interior NUL"],
    );
    check_control(&ray, "control after interior NUL function name");

    drop(ray);
    println!("\nPASS: empty module name check, interior NUL check, and empty function name semantics passed on all 5 cases");
}

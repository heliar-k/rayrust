//! Example: `ActorOptions::runtime_env` 收用户形态的 runtime env 文档。
//!
//! 这个字段与 Python 的 `runtime_env=`、`TaskOptions::runtime_env` 同形，例如
//! `{"env_vars":{"FOO":"bar"}}`。C 侧把它转成 core worker 要的
//! `RuntimeEnvInfo`（`ray::RuntimeEnv::Deserialize(...).SerializeToRuntimeEnvInfo()`），
//! 所以交给 core worker 的永远是合法 proto-JSON；不是 JSON 对象的文档在提交之前就被
//! 拒成 `RayError::Ffi`，驱动进程不会因为 core worker 的
//! `RAY_CHECK(JsonStringToMessage(...).ok())` 退出。
//!
//! 一次运行跑完七项，**必须集群模式**（RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO）：
//! ```text
//! ./target/release/examples/actor_runtime_env_test
//! ```
//! 本地模式跑不了这些检查：`ray_actor_create_python_with_options` 在本地模式拒绝每一次
//! 创建（#68）；Rust actor 在本地模式虽然能建，文档也照样被校验（非法文档返回
//! `RayError::Ffi`），但 `LocalModeTaskSubmitter::Submit` 调用 `SetCommonTaskSpec` 时没有
//! 传可选的 `runtime_env_info` 参数（`task_util.h:153`、`:184-186`），任务规格里没有运行时
//! 环境，所以第 6 项在那个工人里读不到 runtime env 设的变量。本地模式仍会读
//! `CreateActor` 里的 `name`（`local_mode_task_submitter.cc:146-149`）与 `Submit` 里的
//! `max_restarts`、`max_concurrency`（`:81`、`:84`）。
//!
//! # 检查项
//!
//! 1. 对照：Python actor（`rayrust_test.Counter`，`start=7`）不带 runtime env → `get`
//!    读到 7。
//! 2. Python actor 带 `{"env_vars":{"RAYRUST_ACTOR_PY":"1"}}` → 创建成功、`get` 读到 7。
//!    证明用户文档被接受、被转成 core worker 要的 proto-JSON 之后照常建出 actor。
//! 3. Python actor 带 `"not valid"` → `Err(RayError::Ffi(_))`，消息含 `parse error`；
//!    随后的对照仍读到 7，证明驱动进程没有退出。
//! 4. Python actor 带 `"[1,2]"` → `Err(RayError::Ffi(_))`，消息含
//!    `runtime_env must be a JSON object`；随后的对照仍读到 7。
//! 5. 对照：Rust actor `EnvReader`（`crates/rayrust-example-worker`）不带 runtime env →
//!    `read("RAYRUST_ACTOR_ENV")` 读到 `None`。这一项先建立基线：集群默认环境里没有
//!    这个变量；集群模式下每个 actor 在自己的工人进程里执行，所以第 6 项的 `Some("rust")`
//!    只能来自它自己的 runtime env。
//! 6. Rust actor `EnvReader` 带 `{"env_vars":{"RAYRUST_ACTOR_ENV":"rust"}}` →
//!    `read("RAYRUST_ACTOR_ENV")` 读到 `Some("rust")`。证明文档真的到了 actor 的工人
//!    进程：读的是那个进程自己的环境，不是驱动进程的。
//! 7. Rust actor 带 `"{"`（非法 JSON）→ `Err(RayError::Ffi(_))`，消息含 `parse error`；
//!    随后的对照（不带 runtime env 的 `EnvReader`）仍能建出来并读到 `None`。
//!
//! 环境变量名：Python actor 用 `RAYRUST_ACTOR_PY`，Rust actor 用 `RAYRUST_ACTOR_ENV`。
//! 两个变量都不在驱动进程的环境里设置，所以第 6 项读到 `Some("rust")` 只能来自
//! runtime env。

use rayrust::prelude::*;

/// 第 2 项交给 Python actor 的环境变量。
const PY_ENV_VAR: &str = "RAYRUST_ACTOR_PY";

/// 第 5、6、7 项交给 Rust actor 的环境变量。
const RUST_ENV_VAR: &str = "RAYRUST_ACTOR_ENV";

/// 读方法结果的期限（毫秒）：包含 actor 进程启动的时间。
const READ_TIMEOUT_MS: i32 = 20_000;

/// 与 worker .so 的 `EnvReader` 同名同实现：集群模式下由工人执行，这里的客户侧
/// 只提供创建器与调用器。
struct EnvReader;

#[rayrust::actor]
impl EnvReader {
    fn new() -> Self {
        EnvReader
    }

    fn read(&self, name: String) -> Option<String> {
        std::env::var(&name).ok()
    }
}

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    assert!(
        !address.is_empty() && address != "local",
        "this example only runs in cluster mode (RAY_ADDRESS={}): local mode does not apply actor runtime_env",
        address
    );
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
}

/// 断言调用返回 `RayError::Ffi`，且消息里含 `needles` 中的任意一条。
fn expect_ffi_containing<T>(what: &str, result: Result<T, RayError>, needles: &[&str]) {
    match result {
        Err(RayError::Ffi(msg)) => {
            assert!(
                needles.iter().any(|needle| msg.contains(needle)),
                "{}: error message does not contain {:?}: {}",
                what,
                needles,
                msg
            );
            println!("  {} -> Ffi({})", what, msg);
        }
        Err(other) => panic!("{}: returned unexpected error variant: {}", what, other),
        Ok(_) => panic!("{}: returned Ok, expected refusal", what),
    }
}

/// 建一个 Python `Counter`（`start=7`）并读 `get`。
fn python_counter_get(ray: &Ray, opts: &ActorOptions) -> Result<i64, RayError> {
    let start = serialize(&7i64).expect("serialize the constructor argument");
    let handle = ray.actor_create_python("rayrust_test", "Counter", &[&start[..]], opts)?;
    ray.actor_call_python(handle.id(), "get", &[], &[])
        .and_then(|reference| reference.cast::<i64>().get_timeout(READ_TIMEOUT_MS))
}

/// 第 1、3、4 项的对照：不带 runtime env 的 Python actor 读到 7。
fn expect_python_control(ray: &Ray, label: &str) {
    let value = match python_counter_get(ray, &ActorOptions::new()) {
        Ok(value) => value,
        Err(error) => panic!("{}: control actor read failed: {}", label, error),
    };
    assert_eq!(value, 7, "{}: control actor returned {}", label, value);
    println!(
        "  {}: Python actor without runtime_env -> get() = {}",
        label, value
    );
}

/// 建一个 Rust `EnvReader` 并读 `RAYRUST_ACTOR_ENV`。
fn rust_env_reader_read(ray: &Ray, opts: &ActorOptions) -> Result<Option<String>, RayError> {
    let handle = envreader_actor_create(ray, opts)?;
    envreader_read_sync(ray, &handle, RUST_ENV_VAR.to_string()).and_then(|reference| {
        reference
            .cast::<Option<String>>()
            .get_timeout(READ_TIMEOUT_MS)
    })
}

/// 1 到 4：Python actor 侧，文档被接受，坏文档在提交前被拒。
fn python_actor_checks(ray: &Ray) {
    println!("\n--- 1. Control: Python actor without runtime_env ---");
    expect_python_control(ray, "case 1");

    println!("\n--- 2. Python actor with user runtime_env ---");
    let document = format!(r#"{{"env_vars":{{"{}":"1"}}}}"#, PY_ENV_VAR);
    let opts = ActorOptions::new().runtime_env(document.clone());
    match python_counter_get(ray, &opts) {
        Ok(value) => {
            assert_eq!(
                value, 7,
                "Python actor with user runtime_env returned {}",
                value
            );
            println!(
                "  runtime_env = {} -> created Ok, get() = {}",
                document, value
            );
        }
        Err(error) => panic!(
            "Python actor creation with runtime_env = {} failed: {}",
            document, error
        ),
    }

    println!("\n--- 3. Python actor with invalid JSON ---");
    expect_ffi_containing(
        r#"runtime_env = "not valid""#,
        python_counter_get(ray, &ActorOptions::new().runtime_env("not valid")),
        &["parse error", "runtime_env must be"],
    );
    expect_python_control(ray, "after refusal");

    println!("\n--- 4. Python actor with JSON array ---");
    expect_ffi_containing(
        r#"runtime_env = "[1,2]""#,
        python_counter_get(ray, &ActorOptions::new().runtime_env("[1,2]")),
        &["runtime_env must be a JSON object"],
    );
    expect_python_control(ray, "after refusal");
}

/// 5 到 7：Rust actor 侧，`env_vars` 真的到了工人进程。
fn rust_actor_checks(ray: &Ray) {
    println!("\n--- 5. Control: Rust actor without runtime_env ---");
    match rust_env_reader_read(ray, &ActorOptions::new()) {
        Ok(value) => {
            assert_eq!(
                value, None,
                "EnvReader without runtime_env returned {:?}, expected None in worker env",
                value
            );
            println!("  without runtime_env -> read({}) = None", RUST_ENV_VAR);
        }
        Err(error) => panic!("control EnvReader creation or call failed: {}", error),
    }

    println!("\n--- 6. Rust actor with user runtime_env ---");
    let document = format!(r#"{{"env_vars":{{"{}":"rust"}}}}"#, RUST_ENV_VAR);
    let opts = ActorOptions::new().runtime_env(document.clone());
    match rust_env_reader_read(ray, &opts) {
        Ok(value) => {
            assert_eq!(
                value,
                Some("rust".to_string()),
                "EnvReader with runtime_env = {} returned {:?}",
                document,
                value
            );
            println!(
                "  runtime_env = {} -> read({}) = {:?} (applied in worker process)",
                document, RUST_ENV_VAR, value
            );
        }
        Err(error) => panic!(
            "EnvReader with runtime_env = {} failed: {}",
            document, error
        ),
    }

    println!("\n--- 7. Rust actor with invalid JSON ---");
    expect_ffi_containing(
        r#"runtime_env = "{""#,
        rust_env_reader_read(ray, &ActorOptions::new().runtime_env("{")),
        &["parse error", "runtime_env must be"],
    );
    match rust_env_reader_read(ray, &ActorOptions::new()) {
        Ok(value) => {
            assert_eq!(
                value, None,
                "control EnvReader after refusal returned {:?}",
                value
            );
            println!(
                "  after refusal: EnvReader without runtime_env -> read({}) = None",
                RUST_ENV_VAR
            );
        }
        Err(error) => panic!("control EnvReader after refusal failed: {}", error),
    }
}

fn main() {
    println!("=== ActorOptions::runtime_env (user-shaped runtime_env document) ===");
    let ray = Ray::connect(&cluster_config()).expect("Ray::connect");

    python_actor_checks(&ray);
    rust_actor_checks(&ray);

    // 让 FFI 调用有机会收尾，避免退出竞态。
    std::thread::sleep(std::time::Duration::from_millis(200));

    println!("\nPASS: all ActorOptions::runtime_env checks passed");
}

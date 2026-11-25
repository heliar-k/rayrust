//! Example: Ray's error classes survive the C ABI as distinct RayError variants.
//!
//! Nine failures are triggered on the driver and their variants compared, plus
//! one control that a normal `bytes` value still decodes:
//!   - a get_timeout on a ref that never resolves → RayError::Timeout
//!   - a get_timeout on that ref after cancel → RayError::Unclassified (zero-length object)
//!   - a get_many_timeout on a cancelled ref → RayError::Unclassified carrying the index
//!   - a task whose runtime_env fails to set up → RayError::Unclassified carrying the decoded text
//!   - the same frame read as `Vec<u8>` → RayError::Unclassified
//!   - the same frame read as `rmpv::Value` → RayError::Unclassified
//!   - the same frame on a Rust task read as `i64` → RayError::Unclassified
//!   - a task whose function name no worker registers → RayError::Task
//!   - a Python task that raises ValueError → RayError::Task carrying the text
//!
//! Cluster setup:
//!   PYTHONPATH=tests/python ray start --head --port=6399 --num-cpus=2 --include-dashboard=false
//!
//! Environment: RAY_ADDRESS, RAY_NODE_IP, RAY_WORKER_SO.

use std::os::raw::c_char;
use std::time::Duration;

use rayrust::prelude::*;

/// A Python task that sleeps stays running for the whole example, so its ref
/// never resolves while the checks run.
const LONG_TASK_SLEEP_S: f64 = 120.0;

/// Much smaller than the sleep, so the get has to expire.
const PROBE_TIMEOUT_MS: i32 = 200;

/// No worker registers a function by this name.
const MISSING_FUNCTION: &str = "error_kinds_test_unregistered_function";

/// Python module and failing remote function in `tests/python/rayrust_test.py`.
const PYTHON_MODULE: &str = "rayrust_test";
const PYTHON_FAIL_FUNCTION: &str = "fail";
const PYTHON_FAIL_MESSAGE: &str = "python-task-boom-t37";

/// A `working_dir` the runtime env agent rejects: it must be a URI, so any bare
/// path fails setup and the task's object carries `RUNTIME_ENV_SETUP_FAILED`.
const NOT_A_URI_WORKING_DIR: &str = "nonexistent-t39-dir";

fn hex_prefix(bytes: &[u8], limit: usize) -> String {
    bytes
        .iter()
        .take(limit)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Cross-language call to `time.sleep`, which needs no result from any worker.
fn submit_long_task(ray: &Ray) -> ObjectRef<()> {
    let arg = serialize(&LONG_TASK_SLEEP_S).expect("serialize the sleep argument");
    ray.task_call_python("time", "sleep", &[&arg], &[])
        .expect("submit the sleeping task")
        .cast()
}

fn main() {
    let address =
        std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| {
        eprintln!("ERROR: RAY_WORKER_SO not set. Build the worker first:");
        eprintln!("  cargo build --release -p rayrust-example-worker");
        std::process::exit(1);
    });

    println!("=== Ray Error Kinds ===");
    println!("Address: {}", address);
    println!("Node IP: {}", node_ip);
    println!("Worker .so: {}", worker_so);

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);

    println!("\nConnecting to Ray cluster...");
    let ray = match Ray::connect(&config) {
        Ok(ray) => {
            println!("✓ Ray initialized (cluster mode)");
            ray
        }
        Err(e) => {
            eprintln!("✗ Failed to init Ray: {}", e);
            std::process::exit(1);
        }
    };

    // ── A get that expires ─────────────────────────────────
    println!("\n--- Timeout: get on an object that is not ready ---");
    let long_task = submit_long_task(&ray);
    println!(
        "  submitted time.sleep({}) → {}",
        LONG_TASK_SLEEP_S,
        long_task.id_hex()
    );
    std::thread::sleep(Duration::from_millis(300));

    let timeout_err = match long_task.get_timeout(PROBE_TIMEOUT_MS) {
        Ok(()) => panic!(
            "the sleeping task delivered a value within {} ms",
            PROBE_TIMEOUT_MS
        ),
        Err(e) => e,
    };
    match &timeout_err {
        RayError::Timeout(msg) => {
            println!("  get_timeout({} ms) → RayError::Timeout", PROBE_TIMEOUT_MS);
            println!("    message: {}", msg);
            assert!(
                !msg.trim().is_empty(),
                "the timeout error carries no message: {:?}",
                msg
            );
        }
        other => panic!(
            "get_timeout on an unresolved ref returned {:?}, expected RayError::Timeout",
            other
        ),
    }
    ray.cancel(long_task.id(), true, true)
        .expect("cancel the sleeping task after the timeout check");

    // ── A cancelled task read through get_timeout ──────────
    println!("\n--- Unclassified (single object): cancelled task carries no data ---");
    let single_cancel_err = match long_task.get_timeout(10_000) {
        Ok(()) => panic!("the cancelled task still delivered a value"),
        Err(e) => e,
    };
    match &single_cancel_err {
        RayError::Unclassified(msg) => {
            println!("  get_timeout(10000 ms) → RayError::Unclassified");
            println!("    message: {}", msg);
            assert!(
                msg.contains("ray_get:"),
                "the single-object unclassified error does not carry the ray_get prefix: {:?}",
                msg
            );
            assert!(
                msg.contains("TASK_CANCELLED"),
                "the single-object unclassified error does not name TASK_CANCELLED: {:?}",
                msg
            );
            assert!(
                msg.contains("does not translate"),
                "the single-object unclassified error does not state that the SDK does not translate the error type: {:?}",
                msg
            );
            assert!(
                !msg.contains('\u{FFFD}'),
                "the single-object unclassified error contains replacement characters: {:?}",
                msg
            );
        }
        other => panic!(
            "get_timeout on a cancelled ref returned {:?}, expected RayError::Unclassified",
            other
        ),
    }

    // ── A cancelled task read through get_many_timeout ─────
    println!("\n--- Unclassified (batch): cancelled task in get_many_timeout ---");
    let batch_cancelled = submit_long_task(&ray);
    println!(
        "  submitted time.sleep({}) → {}",
        LONG_TASK_SLEEP_S,
        batch_cancelled.id_hex()
    );
    ray.cancel(batch_cancelled.id(), true, true)
        .expect("cancel the sleeping task for the batch check");
    let batch_cancel_err =
        match ray.get_many_timeout(std::slice::from_ref(&batch_cancelled), 10_000) {
            Ok(_) => panic!("get_many_timeout on a cancelled task returned Ok"),
            Err(e) => e,
        };
    match &batch_cancel_err {
        RayError::Unclassified(msg) => {
            println!("  get_many_timeout([cancelled], 10000 ms) → RayError::Unclassified");
            println!("    message: {}", msg);
            assert!(
                msg.contains("ray_get_many (0)"),
                "the batch unclassified error does not carry 'ray_get_many (0)': {:?}",
                msg
            );
            assert!(
                msg.contains("TASK_CANCELLED"),
                "the batch unclassified error does not name TASK_CANCELLED: {:?}",
                msg
            );
            assert!(
                msg.contains("does not translate"),
                "the batch unclassified error does not state that the SDK does not translate the error type: {:?}",
                msg
            );
            assert!(
                !msg.contains('\u{FFFD}'),
                "the batch unclassified error contains replacement characters: {:?}",
                msg
            );
        }
        other => panic!(
            "get_many_timeout on a cancelled ref returned {:?}, expected RayError::Unclassified",
            other
        ),
    }

    // ── A runtime_env that fails to set up: non-empty error frame ──
    println!("\n--- Unclassified (payload): runtime_env setup failure ---");
    let arg1 = serialize(&1i64).expect("serialize 1");
    let bad_env_json = format!(r#"{{"working_dir":"{}"}}"#, NOT_A_URI_WORKING_DIR);
    let bad_env_opts = TaskOptions::new().runtime_env(bad_env_json);
    let bad_env: ObjectRef<i64> = ray
        .task_call_python_with_options(PYTHON_MODULE, "add", &[&arg1, &arg1], &[], &bad_env_opts)
        .expect("submit a task with a bad runtime_env")
        .cast();
    println!(
        "  submitted '{}.add(1, 1)' with bad runtime_env → {}",
        PYTHON_MODULE,
        bad_env.id_hex()
    );
    let bad_env_err = match bad_env.get_timeout(120_000) {
        Ok(val) => panic!(
            "a task with a bad runtime_env returned {} instead of failing",
            val
        ),
        Err(e) => e,
    };
    match &bad_env_err {
        RayError::Unclassified(msg) => {
            println!("  get_timeout(120000 ms) → RayError::Unclassified");
            println!("    message:\n{}", msg);
            assert!(
                msg.contains("does not translate"),
                "the runtime_env unclassified error does not state that the SDK does not translate the error type: {:?}",
                msg
            );
            assert!(
                msg.contains(&format!(
                    "Expected URI but received path {}",
                    NOT_A_URI_WORKING_DIR
                )),
                "the runtime_env unclassified error does not contain the ValueError path message: {:?}",
                msg
            );
            assert!(
                msg.contains("Traceback"),
                "the runtime_env unclassified error does not contain 'Traceback': {:?}",
                msg
            );
            assert!(
                !msg.contains('\u{FFFD}'),
                "the runtime_env unclassified error contains replacement characters: {:?}",
                msg
            );
        }
        other => panic!(
            "get_timeout on a failed runtime_env task returned {}, expected RayError::Unclassified",
            other
        ),
    }

    // ── The same frame is never decoded into a value ───────
    println!("\n--- Unclassified (payload): the frame is checked before decoding ---");
    let bad_env_as_bytes = bad_env.clone().cast::<Vec<u8>>();
    match bad_env_as_bytes.get_timeout(10_000) {
        Ok(bytes) => panic!(
            "the runtime_env frame read as Vec<u8> returned {} bytes instead of failing",
            bytes.len()
        ),
        Err(RayError::Unclassified(msg)) => {
            println!("  get_timeout::<Vec<u8>> → RayError::Unclassified");
            println!(
                "    message: {} bytes, first line: {}",
                msg.len(),
                msg.lines().next().unwrap_or("")
            );
            assert!(
                msg.contains("does not translate"),
                "the Vec<u8> unclassified error does not state that the SDK does not translate the error type: {:?}",
                msg
            );
            assert!(
                msg.contains("Traceback"),
                "the Vec<u8> unclassified error does not contain 'Traceback': {:?}",
                msg
            );
        }
        other => panic!(
            "the runtime_env frame read as Vec<u8> returned {:?}, expected RayError::Unclassified",
            other
        ),
    }

    match bad_env.get_value() {
        Ok(value) => panic!(
            "the runtime_env frame read as rmpv::Value returned {:?} instead of failing",
            value
        ),
        Err(RayError::Unclassified(msg)) => {
            println!("  get_value() → RayError::Unclassified");
            assert!(
                msg.contains("does not translate"),
                "the rmpv::Value unclassified error does not state that the SDK does not translate the error type: {:?}",
                msg
            );
        }
        other => panic!(
            "the runtime_env frame read as rmpv::Value returned {:?}, expected RayError::Unclassified",
            other
        ),
    }

    // 非跨语言任务（`is_xlang = false`）：帧首值是正文长度、后跟头内填充与 `bin` 正文，
    // 前置判定让它返回携带错误文本的 `RayError::Unclassified`，避免进入解码器被报告为
    // 尾随字节的 `RayError::Serialization`。
    let bad_env_rust: ObjectRef<i64> = ray
        .task_call("add", &[&arg1, &arg1], &[], &bad_env_opts)
        .expect("submit a Rust task with a bad runtime_env")
        .cast();
    println!(
        "  submitted Rust add(1, 1) with bad runtime_env → {}",
        bad_env_rust.id_hex()
    );
    match bad_env_rust.get_timeout(120_000) {
        Ok(val) => panic!(
            "the non-xlang runtime_env frame read as i64 returned {} instead of failing",
            val
        ),
        Err(RayError::Unclassified(msg)) => {
            println!("  get_timeout::<i64>() on the Rust task → RayError::Unclassified");
            assert!(
                msg.contains("does not translate"),
                "the non-xlang unclassified error does not state that the SDK does not translate the error type: {:?}",
                msg
            );
            assert!(
                msg.contains("Traceback"),
                "the non-xlang unclassified error does not contain 'Traceback': {:?}",
                msg
            );
        }
        other => panic!(
            "the non-xlang runtime_env frame read as i64 returned {:?}, expected RayError::Unclassified",
            other
        ),
    }

    // ── Control: a normal bytes value still decodes ────────
    println!("\n--- Control: normal bytes values decode, they are not classified ---");
    for (label, text) in [("non-empty", "t39-bytes"), ("empty", "")] {
        let arg = serialize(&text).expect("serialize the os.fsencode argument");
        let bytes_ref: ObjectRef<Vec<u8>> = ray
            .task_call_python("os", "fsencode", &[&arg], &[])
            .expect("submit os.fsencode")
            .cast();
        let got = match bytes_ref.get_timeout(10_000) {
            Ok(got) => got,
            Err(e) => panic!("os.fsencode({:?}) failed: {}", text, e),
        };
        println!(
            "  os.fsencode({:?}) ({}) → Ok(Vec<u8> len={})",
            text,
            label,
            got.len()
        );
        assert_eq!(
            got,
            text.as_bytes(),
            "os.fsencode({:?}) did not round-trip",
            text
        );
    }

    // ── A task whose function does not exist ───────────────
    println!("\n--- Task failure: unregistered function name ---");
    let missing = ray
        .task_call(MISSING_FUNCTION, &[], &[], &TaskOptions::new())
        .expect("submitting a task does not require the function to be registered");
    println!("  submitted '{}' → {}", MISSING_FUNCTION, missing.id_hex());

    let task_err = match missing.get() {
        Ok(()) => panic!(
            "a task with the unregistered function '{}' returned a value",
            MISSING_FUNCTION
        ),
        Err(e) => e,
    };
    match &task_err {
        RayError::Task(msg) => {
            println!("  get() → RayError::Task");
            println!("    message: {}", msg);
            assert!(
                msg.contains(MISSING_FUNCTION),
                "the task error does not name the missing function: {}",
                msg
            );
        }
        other => panic!(
            "a task with an unregistered function returned {:?}, expected RayError::Task",
            other
        ),
    }

    // ── A Python task that raises ValueError ───────────────
    println!("\n--- Python task failure: formatted traceback extracted from payload ---");
    let fail_arg = serialize(&PYTHON_FAIL_MESSAGE).expect("serialize the failure message");
    let py_failed = ray
        .task_call_python(PYTHON_MODULE, PYTHON_FAIL_FUNCTION, &[&fail_arg], &[])
        .expect("submit rayrust_test.fail");
    println!(
        "  submitted '{}.{}({:?})' → {}",
        PYTHON_MODULE,
        PYTHON_FAIL_FUNCTION,
        PYTHON_FAIL_MESSAGE,
        py_failed.id_hex()
    );

    let py_err = match py_failed.get() {
        Ok(()) => panic!(
            "'{}.{}' returned a value instead of raising ValueError",
            PYTHON_MODULE, PYTHON_FAIL_FUNCTION
        ),
        Err(e) => e,
    };
    let mut raw_ptr: *const c_char = std::ptr::null();
    let mut raw_len: usize = 0;
    unsafe { rayrust::sys::ray_last_error_bytes(&mut raw_ptr, &mut raw_len) };
    let raw_prefix = if raw_ptr.is_null() {
        String::new()
    } else {
        let raw_slice = unsafe { std::slice::from_raw_parts(raw_ptr as *const u8, raw_len) };
        hex_prefix(raw_slice, 32)
    };
    match &py_err {
        RayError::Task(msg) => {
            println!("  get() → RayError::Task");
            println!(
                "    raw C payload: len={} bytes, first 32 bytes hex: {}",
                raw_len, raw_prefix
            );
            println!(
                "    RayError::Task message: len={} bytes, first 32 bytes hex: {}",
                msg.len(),
                hex_prefix(msg.as_bytes(), 32)
            );
            println!("    decoded message:\n{}", msg);
            // `RayError.to_bytes`（`ray/exceptions.py:29-37`）对 `RayTaskError` 调用
            // `format_exception` 生成字段 3，而 `RayTaskError.__str__`
            // （`ray/exceptions.py:246-309`，`:261-275`）把原始 `"Traceback "` 行改写成
            // `proctitle() (pid=..., ip=...)`，因此字段 3 以
            // `"ray.exceptions.RayTaskError: "` 开头，后接 `File ".../rayrust_test.py"` 调用帧与
            // `"ValueError: ..."`。
            let expected_value_error = format!("ValueError: {}", PYTHON_FAIL_MESSAGE);
            assert!(
                msg.starts_with("ray_get: ray.exceptions.RayTaskError:"),
                "the Python task error does not start with 'ray_get: ray.exceptions.RayTaskError:': {:?}",
                msg
            );
            assert!(
                msg.contains("rayrust_test.py") && msg.contains("in fail"),
                "the Python task error does not contain the Python stack frame: {:?}",
                msg
            );
            assert!(
                msg.contains(&expected_value_error),
                "the Python task error does not contain {:?}: {:?}",
                expected_value_error,
                msg
            );
            assert!(
                !msg.contains('\u{FFFD}'),
                "the Python task error still contains replacement characters: {:?}",
                msg
            );
        }
        other => panic!(
            "a failing Python task returned {:?}, expected RayError::Task",
            other
        ),
    }

    // ── The failures stay distinguishable ──────────────────
    println!("\n--- Distinct classes ---");
    println!(
        "  RayError::Timeout, RayError::Task, and RayError::Unclassified are distinct variants"
    );
    println!("  RayError::Unclassified covers zero-length objects and untranslated error frames");
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ All error-kind checks passed");
}

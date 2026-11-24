//! Example: decode cross-language Python results, including ones that carry a
//! pickle5 section.
//!
//! Ray's Python serializer writes a 9-byte header whose integer field records
//! only the length of the msgpack part. When a return value cannot be
//! represented in msgpack, msgpack holds a placeholder index and the real
//! object goes into a pickle5 section appended after the msgpack part, so the
//! declared length is smaller than the remaining bytes. Rust cannot decode
//! that section; such a value must come back as `RayError::Serialization`.
//!
//! The cluster mode (argv[1] = "cluster") runs seven checks:
//!   1. return_list → a msgpack array with the expected elements
//!   2. return_custom_object(7) → Err(RayError::Serialization)
//!   3. return_list_with_custom_object(3) → Err(RayError::Serialization)
//!   4. the raw bytes of that ref split into msgpack part + a tail > 0
//!   5. Rust put/get round-trips in both modes; in cluster mode put_xlang
//!      round-trips via get/get_value and passes by reference to Python
//!      `sum_list` and `echo_dict`, while in local mode put_xlang returns
//!      `Err(RayError::Ffi)` naming `local mode`
//!   6. a normal Python call still works after the rejections
//!   7. options reach a Python task: with the single CPU held by one Python task,
//!      a call through `task_call_python` waits (Err(RayError::Timeout)) while the
//!      same call with `.resource("CPU", 0.0)` finishes, and the waiting call
//!      finishes once the CPU is released
//!
//! The local mode (argv[1] = "local") runs the Rust round-trip of check 5 plus
//! one check of its own: a C++/Rust driver's local mode runs tasks in-process
//! through a C++ function descriptor (`local_mode_task_submitter.cc`), so
//! Python functions cannot run there and `task_call_python` returns the
//! in-process `nil + [3, "unknown function: ..."]` error instead of a
//! cross-language payload. Checks 1-4 and 6 need the cluster mode, and check 7
//! runs there only: it needs a cluster with one CPU (`--num-cpus=1`) whose single
//! CPU is taken, which local mode has no equivalent of.
//!
//! Environment for the cluster mode: RAY_ADDRESS, RAY_NODE_IP, RAY_WORKER_SO, and
//! a cluster started with `--num-cpus=1`.

use std::collections::HashMap;
use std::time::Duration;

use rayrust::prelude::*;

/// Python module holding the test tasks; the worker must be able to import it.
const PYTHON_MODULE: &str = "rayrust_test";

/// Length of the header Ray puts in front of a cross-language payload.
const XLANG_HEADER_LEN: usize = 9;

/// Directory of PYTHON_MODULE, passed to the worker through runtime_env.
fn python_module_dir() -> String {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/python");
    let path = std::fs::canonicalize(dir).expect("canonicalize the Python test module directory");
    path.to_str()
        .expect("the Python module path is valid UTF-8")
        .to_string()
}

/// Read the msgpack integer the 9-byte header starts with.
///
/// Written out here so the check reads the payload the same way Python's
/// `split_buffer` does, independently of the library's own header parsing.
fn declared_msgpack_len(data: &[u8]) -> usize {
    assert!(
        data.len() >= XLANG_HEADER_LEN,
        "the payload is {} bytes, shorter than the {} byte header",
        data.len(),
        XLANG_HEADER_LEN
    );
    match data[0] {
        // 正 fixint
        v if v <= 0x7f => v as usize,
        // uint8
        0xcc => data[1] as usize,
        // uint16
        0xcd => u16::from_be_bytes([data[1], data[2]]) as usize,
        // uint32
        0xce => u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize,
        other => panic!(
            "the payload starts with msgpack marker {:#04x}, not a length",
            other
        ),
    }
}

/// Connect in the mode named on the command line.
fn connect(mode: &str) -> Ray {
    match mode {
        "local" => {
            println!("Mode: local");
            match Ray::local() {
                Ok(ray) => {
                    println!("✓ Ray initialized (local mode)");
                    ray
                }
                Err(e) => {
                    eprintln!("✗ Failed to init Ray (local mode): {}", e);
                    std::process::exit(1);
                }
            }
        }
        "cluster" => {
            let address =
                std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
            let node_ip =
                std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
            let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| {
                eprintln!("ERROR: RAY_WORKER_SO not set. Build the worker first:");
                eprintln!("  cargo build --release -p rayrust-example-worker");
                std::process::exit(1);
            });
            let python_dir = python_module_dir();
            println!("Mode: cluster");
            println!("Address: {}", address);
            println!("Node IP: {}", node_ip);
            println!("Worker .so: {}", worker_so);
            println!("Python module dir: {}", python_dir);

            let runtime_env = format!(r#"{{"env_vars": {{"PYTHONPATH": "{}"}}}}"#, python_dir);
            let config = RayConfig::new(&address)
                .node_ip(&node_ip)
                .code_search_path(vec![worker_so])
                .runtime_env(runtime_env);

            match Ray::connect(&config) {
                Ok(ray) => {
                    println!("✓ Ray initialized (cluster mode)");
                    ray
                }
                Err(e) => {
                    eprintln!("✗ Failed to init Ray: {}", e);
                    std::process::exit(1);
                }
            }
        }
        other => {
            eprintln!(
                "ERROR: unknown mode {:?}; pass \"local\" or \"cluster\"",
                other
            );
            std::process::exit(2);
        }
    }
}

/// Check 1: a value msgpack can represent comes back as an array.
fn check_plain_list(ray: &Ray) {
    println!("\n--- 1. return_list → msgpack array ---");
    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "return_list", &[], &[])
        .expect("submit return_list()");
    println!(
        "  ray.{}.return_list() → {}",
        PYTHON_MODULE,
        obj_ref.id_hex()
    );

    let value = match obj_ref.get_value() {
        Ok(value) => value,
        Err(e) => panic!("return_list() did not decode: {}", e),
    };
    let items = value
        .as_array()
        .unwrap_or_else(|| panic!("return_list() decoded to {:?}, not an array", value));
    let numbers: Vec<i64> = items
        .iter()
        .map(|v| v.as_i64().expect("an integer element"))
        .collect();
    assert_eq!(
        numbers,
        vec![1, 2, 3, 4, 5],
        "return_list() returned {:?}",
        numbers
    );
    println!("  value → {:?} ✓", numbers);

    check_raw_bytes(&obj_ref, 0);
}

/// Check 4: split the raw payload on the declared length.
///
/// `expected_tail` is 0 for a plain msgpack payload and greater than 0 for one
/// that carries a pickle5 section.
fn check_raw_bytes(obj_ref: &ObjectRef<()>, expected_tail: u64) {
    let raw = obj_ref.get_raw_bytes().expect("get_raw_bytes()");
    let declared = declared_msgpack_len(&raw);
    let after_header = raw.len() - XLANG_HEADER_LEN;
    let tail = after_header.checked_sub(declared).unwrap_or_else(|| {
        panic!(
            "the header declares {} msgpack bytes but only {} follow it",
            declared, after_header
        )
    });
    println!(
        "  raw bytes: {} total, {} header, {} msgpack, {} tail; header first bytes {:02x?}",
        raw.len(),
        XLANG_HEADER_LEN,
        declared,
        tail,
        &raw[..XLANG_HEADER_LEN]
    );
    if expected_tail > 0 {
        assert!(
            tail > 0,
            "the payload carries no pickle5 section, so it cannot show the reported shape"
        );
    } else {
        assert_eq!(
            tail, 0,
            "the payload carries an unexpected {} byte tail",
            tail
        );
    }
}

/// Check 2: a value that only exists in the pickle5 section is rejected.
fn check_custom_object(ray: &Ray) {
    println!("\n--- 2. return_custom_object(7) → rejected ---");
    let arg = serialize(&7i64).expect("serialize the argument");
    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "return_custom_object", &[&arg], &[])
        .expect("submit return_custom_object(7)");
    println!(
        "  ray.{}.return_custom_object(7) → {}",
        PYTHON_MODULE,
        obj_ref.id_hex()
    );

    match obj_ref.get_value() {
        Ok(value) => panic!(
            "return_custom_object(7) decoded to {:?}; the value lives in the pickle5 section and has no msgpack form",
            value
        ),
        Err(RayError::Serialization(msg)) => {
            // Deliberate exception to the "match the error variant only" rule: a
            // msgpack type mismatch also returns Serialization (casting an
            // integer payload to Vec<i64> does), so the variant cannot tell the
            // pickle5 rejection from a decode failure. The message text is that
            // difference, and the unit tests assert the same text.
            assert!(
                msg.contains("pickle5"),
                "get_value() failed with a msgpack decode error instead of the pickle5 rejection: {}",
                msg
            );
            println!("  get_value() → Err(RayError::Serialization)");
            println!("  message: {}", msg);
        }
        Err(other) => panic!(
            "return_custom_object(7) returned {:?}, expected RayError::Serialization",
            other
        ),
    }

    check_raw_bytes(&obj_ref, 1);
}

/// Check 3: a container holding such a value is rejected too, and does not
/// come back as a list of integers.
fn check_list_with_custom_object(ray: &Ray) {
    println!("\n--- 3. return_list_with_custom_object(3) → rejected ---");
    let arg = serialize(&3i64).expect("serialize the argument");
    let obj_ref = ray
        .task_call_python(
            PYTHON_MODULE,
            "return_list_with_custom_object",
            &[&arg],
            &[],
        )
        .expect("submit return_list_with_custom_object(3)");
    println!(
        "  ray.{}.return_list_with_custom_object(3) → {}",
        PYTHON_MODULE,
        obj_ref.id_hex()
    );

    match obj_ref.get_value() {
        Ok(value) => panic!(
            "return_list_with_custom_object(3) decoded to {:?}; the element lives in the pickle5 section",
            value
        ),
        Err(RayError::Serialization(msg)) => {
            // Same exception to the "match the error variant only" rule as in
            // check 2: only the message tells the pickle5 rejection from a
            // msgpack type mismatch.
            assert!(
                msg.contains("pickle5"),
                "get_value() failed with a msgpack decode error instead of the pickle5 rejection: {}",
                msg
            );
            println!("  get_value() → Err(RayError::Serialization)");
            println!("  message: {}", msg);
        }
        Err(other) => panic!(
            "return_list_with_custom_object(3) returned {:?}, expected RayError::Serialization",
            other
        ),
    }
}

/// Local mode: a Python task never reaches a Python worker, so its payload is
/// the in-process error, not a cross-language value.
///
/// The local-mode task submitter always builds a C++ function descriptor
/// (`local_mode_task_submitter.cc`) and the in-process executor answers an
/// unknown function with `nil + [3, "unknown function: ..."]`
/// (`task_executor.cc:48`). Neither shape carries a 9-byte header, so this run
/// proves that a local run can produce no pickle5 payload at all.
fn check_local_mode_python_task(ray: &Ray) {
    println!("\n--- local mode: task_call_python returns the in-process error ---");
    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "return_list", &[], &[])
        .expect("submit return_list() in local mode");

    let raw = obj_ref.get_raw_bytes().expect("get_raw_bytes()");
    println!(
        "  raw bytes ({} total): {:02x?} / {:?}",
        raw.len(),
        raw,
        String::from_utf8_lossy(&raw)
    );
    assert!(
        raw.len() >= 3 && raw[..3] == [0xc0, 0x92, 0x03],
        "expected the local-mode error nil + [3, \"unknown function: ...\"], got {} bytes starting with {:02x?}",
        raw.len(),
        &raw[..raw.len().min(3)]
    );

    let value = obj_ref
        .get_value()
        .expect("get_value() of the local-mode error");
    assert!(
        value.is_nil(),
        "the local-mode error decoded to {:?}, not Nil; raw bytes {:02x?}",
        value,
        raw
    );
    println!("  get_value() → Nil ✓ (no cross-language payload exists in local mode)");
}

/// Check 5: values Rust wrote itself round-trip; `put_xlang` refuses local mode
/// and attaches `"XLANG"` metadata in cluster mode so Python tasks can consume
/// it by reference.
fn check_rust_roundtrip(ray: &Ray, is_local: bool) {
    println!("\n--- 5. Rust put/get and put_xlang ---");
    let obj = ray.put(&42i32).expect("put(42i32)");
    assert!(!obj.is_xlang(), "put() returned is_xlang() == true");
    let value: i32 = obj.get().expect("get() of put(42i32)");
    assert_eq!(value, 42, "put/get returned {}", value);
    println!("  put(42i32) → get() → {} ✓", value);

    let obj = ray.put(&vec![1i64, 2, 3]).expect("put(vec![1, 2, 3])");
    let value = obj.get_value().expect("get_value() of put(vec![1, 2, 3])");
    let numbers: Vec<i64> = value
        .as_array()
        .expect("an array")
        .iter()
        .map(|v| v.as_i64().expect("an integer element"))
        .collect();
    assert_eq!(numbers, vec![1, 2, 3], "put/get returned {:?}", numbers);
    println!("  put(vec![1, 2, 3]) → get_value() → {:?} ✓", numbers);

    if is_local {
        match ray.put_xlang(&vec![7i64, 8, 9]) {
            Err(RayError::Ffi(msg)) => {
                assert!(
                    msg.contains("local mode"),
                    "put_xlang in local mode did not mention local mode: {}",
                    msg
                );
                println!("  put_xlang in local mode → Err(RayError::Ffi({})) ✓", msg);
            }
            Ok(_) => panic!("put_xlang succeeded in local mode"),
            Err(other) => panic!(
                "put_xlang in local mode returned {:?}, expected RayError::Ffi",
                other
            ),
        }
        return;
    }

    let obj = ray
        .put_xlang(&vec![7i64, 8, 9])
        .expect("put_xlang(vec![7, 8, 9])");
    assert!(obj.is_xlang(), "put_xlang() returned is_xlang() == false");
    let typed: Vec<i64> = obj.get().expect("get() of put_xlang(vec![7, 8, 9])");
    assert_eq!(typed, vec![7, 8, 9], "put_xlang/get returned {:?}", typed);
    let value = obj
        .get_value()
        .expect("get_value() of put_xlang(vec![7, 8, 9])");
    let numbers: Vec<i64> = value
        .as_array()
        .expect("an array")
        .iter()
        .map(|v| v.as_i64().expect("an integer element"))
        .collect();
    assert_eq!(
        numbers,
        vec![7, 8, 9],
        "put_xlang/get_value returned {:?}",
        numbers
    );
    println!(
        "  put_xlang(vec![7, 8, 9]) → get() & get_value() → {:?} ✓",
        numbers
    );

    let sum_ref = ray
        .task_call_python(PYTHON_MODULE, "sum_list", &[obj.id()], &[true])
        .expect("submit sum_list(&put_xlang)");
    let sum_val: i64 = sum_ref
        .cast::<i64>()
        .get()
        .expect("get() of sum_list(&put_xlang)");
    assert_eq!(sum_val, 24, "sum_list(&put_xlang) returned {}", sum_val);
    println!(
        "  ray.{}.sum_list(&put_xlang([7, 8, 9])) → {} ✓",
        PYTHON_MODULE, sum_val
    );

    let mut dict = HashMap::new();
    dict.insert("alpha".to_string(), 10i64);
    dict.insert("beta".to_string(), 32i64);
    let dict_obj = ray.put_xlang(&dict).expect("put_xlang(dict)");
    let echoed_ref = ray
        .task_call_python(PYTHON_MODULE, "echo_dict", &[dict_obj.id()], &[true])
        .expect("submit echo_dict(&put_xlang)");
    let echoed: HashMap<String, i64> = echoed_ref
        .cast::<HashMap<String, i64>>()
        .get()
        .expect("get() of echo_dict(&put_xlang)");
    assert_eq!(echoed, dict, "echo_dict(&put_xlang) returned {:?}", echoed);
    println!(
        "  ray.{}.echo_dict(&put_xlang(dict)) → {:?} ✓",
        PYTHON_MODULE, echoed
    );
}

/// Check 6: a rejection does not leave the process unable to talk to Ray.
fn check_after_rejections(ray: &Ray) {
    println!("\n--- 6. a normal call still works after the rejections ---");
    let arg_a = serialize(&2i64).expect("serialize the first argument");
    let arg_b = serialize(&40i64).expect("serialize the second argument");
    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "add", &[&arg_a, &arg_b], &[])
        .expect("submit add(2, 40)");
    let value: i64 = obj_ref.cast().get().expect("get() of add(2, 40)");
    assert_eq!(value, 42, "add(2, 40) returned {}", value);
    println!("  ray.{}.add(2, 40) → {} ✓", PYTHON_MODULE, value);

    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "return_dict", &[], &[])
        .expect("submit return_dict()");
    let value = obj_ref.get_value().expect("get_value() of return_dict()");
    let entries = value
        .as_map()
        .unwrap_or_else(|| panic!("return_dict() decoded to {:?}, not a map", value));
    println!(
        "  ray.{}.return_dict() → {} map entries ✓",
        PYTHON_MODULE,
        entries.len()
    );
    let a = entries
        .iter()
        .find(|(k, _)| k.as_str() == Some("a"))
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("return_dict() has no key \"a\""));
    assert_eq!(a.as_i64(), Some(1), "return_dict()[\"a\"] is {:?}", a);
}

/// Check 7: options reach a Python task.
///
/// The cluster must have one CPU (`--num-cpus=1`). One Python task holds that CPU
/// for 60 s. A second call through `task_call_python` keeps the default one CPU
/// and cannot start, so it waits; the same call through
/// `task_call_python_with_options` with `("CPU", 0.0)` runs anyway. Cancelling the
/// holding task frees the CPU and the waiting call finishes with its value.
///
/// The 60 s are what makes the zero-CPU call able to fail: 4 s of waiting for the
/// holder to take the CPU, the 15 s window of the default call and the 20 s window
/// of the zero-CPU call add up to 39 s, so an implementation that ignores the
/// options still finds the CPU taken when the zero-CPU call gives up.
fn check_python_task_options(ray: &Ray) {
    println!("\n--- 7. options reach a Python task ---");
    let seconds = serialize(&60.0f64).expect("serialize 60.0");
    let holder = ray
        .task_call_python(PYTHON_MODULE, "sleep_for", &[&seconds], &[])
        .expect("submit the CPU-holding Python task");
    println!(
        "  ray.{}.sleep_for(60.0) → {}",
        PYTHON_MODULE,
        holder.id_hex()
    );
    // 等它真的拿到那个唯一的 CPU。
    std::thread::sleep(Duration::from_secs(4));

    let arg_a = serialize(&2i64).expect("serialize the first argument");
    let arg_b = serialize(&40i64).expect("serialize the second argument");

    // 对照项：默认 options 要 1 个 CPU，CPU 被占住时它排不上队。
    let queued = ray
        .task_call_python(PYTHON_MODULE, "add", &[&arg_a, &arg_b], &[])
        .expect("submit add(2, 40) with the default options");
    match queued.get_timeout(15000) {
        Err(RayError::Timeout(_)) => {
            println!("  add(2, 40) with no options → Err(RayError::Timeout) ✓")
        }
        Ok(()) => panic!(
            "add(2, 40) with no options finished while the only CPU is held by sleep_for(60.0)"
        ),
        Err(other) => panic!(
            "add(2, 40) with no options returned {:?}, expected RayError::Timeout",
            other
        ),
    }

    // 判别项：显式的 CPU 0 让同一个调用在 CPU 被占住时也跑得起来。
    let zero_cpu = TaskOptions::new().resource("CPU", 0.0);
    let no_cpu = ray
        .task_call_python_with_options(PYTHON_MODULE, "add", &[&arg_a, &arg_b], &[], &zero_cpu)
        .expect("submit add(2, 40) with CPU 0");
    let value: i64 = no_cpu
        .cast()
        .get_timeout(20000)
        .expect("get_timeout of the zero-CPU add(2, 40)");
    assert_eq!(value, 42, "the zero-CPU add(2, 40) returned {}", value);
    println!("  add(2, 40) with .resource(\"CPU\", 0.0) → {} ✓", value);

    // 这里 cancel 的是 Python 任务，执行它的 Python worker 走 `on_cancel_callback` 的
    // force_kill 分支（src/ray/core_worker/core_worker.cc:3902-3912），以
    // INTENDED_USER_EXIT 立即退出（"…has received a force ray.cancel request."，经
    // ray::QuickExit 到 _Exit(1)，src/ray/util/process.cc:845-848），CPU 随之释放；
    // force_kill=false 对正在运行的任务只调 kill_main（同文件 :3946-3948），
    // sleep_for 不会提前结束，会一直占着 CPU。
    ray.cancel(holder.id(), true, true)
        .expect("cancel the CPU-holding task");
    std::thread::sleep(Duration::from_secs(3));
    let value: i64 = queued
        .cast()
        .get_timeout(15000)
        .expect("get_timeout of the queued add(2, 40)");
    assert_eq!(value, 42, "the queued add(2, 40) returned {}", value);
    println!(
        "  the queued add(2, 40) finished after the CPU was released → {} ✓",
        value
    );
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "local".to_string());
    println!("=== Ray Cross-Language Decode ===");

    let ray = connect(&mode);

    if mode == "local" {
        // C++/Rust driver 的本地模式固定用 C++ 函数描述符提交任务，Python 函数
        // 在那里跑不了，所以本地模式只能验证「产生不了跨语言载荷」这一点。
        check_local_mode_python_task(&ray);
        check_rust_roundtrip(&ray, true);
    } else {
        check_plain_list(&ray);
        check_custom_object(&ray);
        check_list_with_custom_object(&ray);
        check_rust_roundtrip(&ray, false);
        check_after_rejections(&ray);
        check_python_task_options(&ray);
    }

    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ All cross-language decode checks passed");
}

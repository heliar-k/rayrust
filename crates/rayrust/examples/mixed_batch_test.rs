//! Example: `get_many` / `get_many_timeout` on mixed batches (#41).
//!
//! Upstream's batch `Get` leaves null `RayObject` entries when any element in the
//! batch is an exception object — both for unready elements and for plasma
//! elements — and `NativeObjectStore::GetRaw` dereferences them with SIGSEGV.
//! `ray_get_many` instead runs one deduplicated `Wait` to bound the deadline and
//! then one single-object `Get` per ready element, reporting the first failing
//! element's class and index.
//!
//! Four checks run by default:
//!   1. unready + failed: `[time.sleep(120), rayrust_test.fail]` → `RayError::Timeout` with `ray_get_many (0)`
//!   2. failed + unready: `[rayrust_test.fail, time.sleep(120)]` → `RayError::Task` with `ray_get_many (0)` and traceback
//!   3. plasma + failed:  `[ray.put(&111i64), cancelled time.sleep]` → `RayError::Unclassified` with `ray_get_many (1)`
//!   4. control: three normal values (`put` + two Rust tasks) in order, plus duplicate refs in one batch
//!
//! Cluster setup:
//!   PYTHONPATH=tests/python ray start --head --port=6616 --num-cpus=2 --include-dashboard=false
//!
//! Environment: RAY_ADDRESS, RAY_NODE_IP, RAY_WORKER_SO.

use std::time::Instant;

use rayrust::prelude::*;

const LONG_TASK_SLEEP_S: f64 = 120.0;
const PYTHON_MODULE: &str = "rayrust_test";
const PYTHON_FAIL_FUNCTION: &str = "fail";
const PYTHON_FAIL_MESSAGE: &str = "python-task-boom-t40";
const BENCH_PUT_COUNT: usize = 200;

fn submit_long_task(ray: &Ray) -> ObjectRef<i64> {
    let arg = serialize(&LONG_TASK_SLEEP_S).expect("serialize sleep duration");
    ray.task_call_python("time", "sleep", &[&arg], &[])
        .expect("submit time.sleep")
        .cast()
}

fn submit_fail_task(ray: &Ray) -> ObjectRef<i64> {
    let arg = serialize(&PYTHON_FAIL_MESSAGE).expect("serialize fail message");
    ray.task_call_python(PYTHON_MODULE, PYTHON_FAIL_FUNCTION, &[&arg], &[])
        .expect("submit rayrust_test.fail")
        .cast()
}

fn submit_multiply(ray: &Ray, a: i64, b: i64) -> ObjectRef<i64> {
    let arg_a = serialize(&a).expect("serialize multiply a");
    let arg_b = serialize(&b).expect("serialize multiply b");
    ray.task_call("multiply", &[&arg_a, &arg_b], &[], &TaskOptions::new())
        .expect("submit multiply")
        .cast()
}

fn submit_compute(ray: &Ray, n: i64) -> ObjectRef<i64> {
    let arg = serialize(&n).expect("serialize compute n");
    ray.task_call("compute", &[&arg], &[], &TaskOptions::new())
        .expect("submit compute")
        .cast()
}

fn check_unready_then_failed(ray: &Ray) {
    println!("\n--- 1. Unready + failed: [time.sleep(120), rayrust_test.fail] ---");
    let sleep_ref = submit_long_task(ray);
    let fail_ref = submit_fail_task(ray);
    println!(
        "  submitted sleep={} fail={}",
        sleep_ref.id_hex(),
        fail_ref.id_hex()
    );

    let (ready, _) = ray
        .wait(std::slice::from_ref(&fail_ref), 1, 30_000)
        .expect("wait for failing task to settle");
    assert_eq!(ready.len(), 1, "failing task did not settle within 30s");

    let result = ray.get_many_timeout(&[sleep_ref.clone(), fail_ref], 5_000);
    ray.cancel(sleep_ref.id(), true, true)
        .expect("cancel sleeping task after case 1");

    match result {
        Ok(vals) => panic!("case 1: expected RayError::Timeout, got Ok({:?})", vals),
        Err(RayError::Timeout(msg)) => {
            println!("  get_many_timeout([sleep, fail], 5000) -> RayError::Timeout");
            println!("    message: {}", msg);
            assert!(
                msg.contains("ray_get_many (0)"),
                "case 1: message does not carry 'ray_get_many (0)': {:?}",
                msg
            );
            assert!(
                msg.contains("Get timed out"),
                "case 1: message does not mention timeout: {:?}",
                msg
            );
        }
        Err(other) => panic!("case 1: expected RayError::Timeout, got {:?}", other),
    }
}

fn check_failed_then_unready(ray: &Ray) {
    println!("\n--- 2. Failed + unready: [rayrust_test.fail, time.sleep(120)] ---");
    let sleep_ref = submit_long_task(ray);
    let fail_ref = submit_fail_task(ray);
    println!(
        "  submitted fail={} sleep={}",
        fail_ref.id_hex(),
        sleep_ref.id_hex()
    );

    let (ready, _) = ray
        .wait(std::slice::from_ref(&fail_ref), 1, 30_000)
        .expect("wait for failing task to settle");
    assert_eq!(ready.len(), 1, "failing task did not settle within 30s");

    let result = ray.get_many_timeout(&[fail_ref, sleep_ref.clone()], 5_000);
    ray.cancel(sleep_ref.id(), true, true)
        .expect("cancel sleeping task after case 2");

    match result {
        Ok(vals) => panic!("case 2: expected RayError::Task, got Ok({:?})", vals),
        Err(RayError::Task(msg)) => {
            println!("  get_many_timeout([fail, sleep], 5000) -> RayError::Task");
            println!("    message:\n{}", msg);
            let expected_value_error = format!("ValueError: {}", PYTHON_FAIL_MESSAGE);
            assert!(
                msg.contains("ray_get_many (0)"),
                "case 2: message does not carry 'ray_get_many (0)': {:?}",
                msg
            );
            assert!(
                msg.contains("ray.exceptions.RayTaskError:"),
                "case 2: message does not carry 'ray.exceptions.RayTaskError:': {:?}",
                msg
            );
            assert!(
                msg.contains(&expected_value_error),
                "case 2: message does not contain {:?}: {:?}",
                expected_value_error,
                msg
            );
            assert!(
                !msg.contains('\u{FFFD}'),
                "case 2: message contains replacement characters: {:?}",
                msg
            );
        }
        Err(other) => panic!("case 2: expected RayError::Task, got {:?}", other),
    }
}

fn check_plasma_and_cancelled(ray: &Ray) {
    println!("\n--- 3. Plasma + failed: [ray.put(&111i64), cancelled time.sleep] ---");
    let put_ref = ray.put(&111i64).expect("put 111i64");
    let sleep_ref = submit_long_task(ray);
    println!("  put={} sleep={}", put_ref.id_hex(), sleep_ref.id_hex());
    ray.cancel(sleep_ref.id(), true, true)
        .expect("cancel sleeping task for case 3");

    let result = ray.get_many_timeout(&[put_ref, sleep_ref], 10_000);
    match result {
        Ok(vals) => panic!(
            "case 3: expected RayError::Unclassified, got Ok({:?})",
            vals
        ),
        Err(RayError::Unclassified(msg)) => {
            println!("  get_many_timeout([put, cancelled], 10000) -> RayError::Unclassified");
            println!("    message: {}", msg);
            assert!(
                msg.contains("ray_get_many (1)"),
                "case 3: message does not carry 'ray_get_many (1)': {:?}",
                msg
            );
            assert!(
                msg.contains("TASK_CANCELLED"),
                "case 3: message does not name TASK_CANCELLED: {:?}",
                msg
            );
            assert!(
                msg.contains("does not translate"),
                "case 3: message does not explain untranslated error type: {:?}",
                msg
            );
            assert!(
                !msg.contains('\u{FFFD}'),
                "case 3: message contains replacement characters: {:?}",
                msg
            );
        }
        Err(other) => panic!("case 3: expected RayError::Unclassified, got {:?}", other),
    }
}

fn check_normal_and_duplicates(ray: &Ray) {
    println!("\n--- 4. Control: normal batch in order and duplicate refs ---");
    let r_put = ray.put(&111i64).expect("put 111i64");
    let r_mul = submit_multiply(ray, 6, 7);
    let r_comp = submit_compute(ray, 10);

    let values = ray
        .get_many(&[r_put.clone(), r_mul.clone(), r_comp.clone()])
        .expect("get_many on three normal refs");
    println!(
        "  get_many([put(111), multiply(6,7), compute(10)]) -> {:?}",
        values
    );
    assert_eq!(
        values,
        vec![111, 42, 45],
        "case 4: normal batch values or order mismatch"
    );

    let dup_values = ray
        .get_many(&[r_mul.clone(), r_put.clone(), r_mul, r_comp, r_put])
        .expect("get_many with duplicate refs");
    println!("  get_many([mul, put, mul, comp, put]) -> {:?}", dup_values);
    assert_eq!(
        dup_values,
        vec![42, 111, 42, 45, 111],
        "case 4: duplicate ref batch values or order mismatch"
    );
}

fn run_put_batch_bench(ray: &Ray) {
    println!(
        "\n--- Benchmark: {} ray.put objects via get_many ---",
        BENCH_PUT_COUNT
    );
    let refs: Vec<ObjectRef<i64>> = (0..BENCH_PUT_COUNT as i64)
        .map(|i| ray.put(&i).expect("put benchmark item"))
        .collect();
    let start = Instant::now();
    let values = ray.get_many(&refs).expect("get_many benchmark batch");
    let elapsed = start.elapsed();
    for (idx, val) in values.iter().enumerate() {
        assert_eq!(*val, idx as i64, "benchmark value mismatch at {}", idx);
    }
    println!(
        "  get_many({} puts): total={:.3} ms ({:.1} us/item)",
        BENCH_PUT_COUNT,
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 1_000_000.0 / BENCH_PUT_COUNT as f64
    );
}

fn main() {
    let address = std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| {
        eprintln!("ERROR: RAY_WORKER_SO not set. Build the worker first:");
        eprintln!("  cargo build --release --all-targets");
        std::process::exit(1);
    });
    let selector = std::env::args().nth(1);

    println!("=== Mixed Batch Get Test (#41) ===");
    println!("Address: {}", address);
    println!("Node IP: {}", node_ip);
    println!("Worker .so: {}", worker_so);

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so]);

    let ray = match Ray::connect(&config) {
        Ok(ray) => {
            println!("[OK] Ray initialized (cluster mode)");
            ray
        }
        Err(e) => {
            eprintln!("Failed to init Ray: {}", e);
            std::process::exit(1);
        }
    };

    match selector.as_deref() {
        None => {
            check_unready_then_failed(&ray);
            check_failed_then_unready(&ray);
            check_plasma_and_cancelled(&ray);
            check_normal_and_duplicates(&ray);
        }
        Some("case1") => check_unready_then_failed(&ray),
        Some("case3") => check_plasma_and_cancelled(&ray),
        Some("bench") => run_put_batch_bench(&ray),
        Some(other) => {
            eprintln!(
                "Unknown selector {:?}; expected case1, case3, or bench",
                other
            );
            std::process::exit(2);
        }
    }

    println!("\n--- Shutdown ---");
    drop(ray);
    println!("[OK] Mixed batch checks passed");
}

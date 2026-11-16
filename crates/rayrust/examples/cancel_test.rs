//! Example: cancel tasks by object id.
//!
//! The long task is a Python `time.sleep` call that holds no CPU but never
//! finishes by itself; the task queued behind it waits on its result.
//!   ray start --head --port=6399 --num-cpus=1 --include-dashboard=false
//!
//! Environment: RAY_ADDRESS, RAY_NODE_IP, RAY_WORKER_SO.

use std::time::{Duration, Instant};

use rayrust::prelude::*;
use serde::de::DeserializeOwned;

/// Ray 2.51.1 object ids are 28 bytes; every other length must be rejected.
const OBJECT_ID_LEN: usize = 28;

/// A Python task that sleeps stays running for the whole example, and its
/// result never arrives while the test runs.
const LONG_TASK_SLEEP_S: f64 = 120.0;

/// Rust worker task: the control run, and the task queued behind the long one.
const COMPUTE_N: i64 = 1_000_000;

/// Timeout handed to get_timeout; a cancelled ref must fail long before it.
const GET_TIMEOUT_MS: i32 = 10_000;
const FAST_FAIL_MS: u128 = 2_000;

/// Cross-language call to `time.sleep`, which stays running for LONG_TASK_SLEEP_S
/// seconds and never returns a result while the example runs.
fn submit_long_task(ray: &Ray) -> ObjectRef<()> {
    let arg = serialize(&LONG_TASK_SLEEP_S).expect("serialize the sleep argument");
    ray.task_call_python("time", "sleep", &[&arg], &[])
        .expect("submit the sleeping task")
        .cast()
}

/// Call the Rust worker's `compute(n)`.
fn submit_compute(ray: &Ray, n: i64) -> ObjectRef<i64> {
    let arg = serialize(&n).expect("serialize the compute argument");
    ray.task_call("compute", &[&arg], &[], &TaskOptions::new())
        .expect("submit the compute task")
        .cast()
}

/// Submit compute with the long task's result as a by-reference argument. The
/// task cannot start before that result exists, so it stays queued.
fn submit_queued_task(ray: &Ray, dependency: &[u8]) -> ObjectRef<i64> {
    ray.task_call("compute", &[&dependency], &[true], &TaskOptions::new())
        .expect("submit the task queued behind the long task")
        .cast()
}

/// cancel() must reject an id of the wrong length and name the length it wants.
fn expect_rejected(ray: &Ray, id: &[u8], label: &str) {
    match ray.cancel(id, false, false) {
        Ok(()) => panic!("{}: cancel returned Ok for a {}-byte id", label, id.len()),
        Err(e) => {
            let msg = e.to_string();
            println!("  {} ({} bytes) rejected: {}", label, id.len(), msg);
            assert!(
                msg.contains(&OBJECT_ID_LEN.to_string()),
                "{}: message does not name the expected id length: {}",
                label,
                msg
            );
        }
    }
}

/// An id of the right length that no task owns must be refused by the core
/// worker, which returns a non-OK status for it.
fn expect_refused(ray: &Ray) {
    let unknown_id = [0xABu8; OBJECT_ID_LEN];
    match ray.cancel(&unknown_id, false, false) {
        Ok(()) => panic!("cancel returned Ok for an object id no task owns"),
        Err(e) => {
            let msg = e.to_string();
            println!("  unknown id ({} bytes) refused: {}", unknown_id.len(), msg);
            assert!(
                msg.contains("refused the cancellation"),
                "unknown id: the core worker status was not reported: {}",
                msg
            );
        }
    }
}

/// A cancelled ref must resolve to a failure on its own and quickly. An expired
/// get_timeout is the failure an unresolved object gives, so that variant is
/// what this rejects. Elapsed time is checked separately against the timeout.
fn expect_fast_failure<T: DeserializeOwned>(obj_ref: &ObjectRef<T>, label: &str) {
    let start = Instant::now();
    let result = obj_ref.get_timeout(GET_TIMEOUT_MS);
    let elapsed = start.elapsed().as_millis();
    match result {
        Ok(_) => panic!("{}: the cancelled task still delivered a value", label),
        Err(RayError::Timeout(detail)) => panic!(
            "{}: get_timeout expired ({}) instead of the ref failing",
            label, detail
        ),
        Err(e) => {
            println!("  {} failed after {} ms: {}", label, elapsed, e);
            assert!(
                elapsed < FAST_FAIL_MS,
                "{}: failure took {} ms, not well under the {} ms timeout",
                label,
                elapsed,
                GET_TIMEOUT_MS
            );
        }
    }
}

/// The object must still be unresolved, i.e. its task has not finished. An
/// expired get_timeout reports RayError::Timeout, which is the only accepted
/// evidence; any other error means the task already failed and is reported.
fn expect_unresolved<T: DeserializeOwned>(obj_ref: &ObjectRef<T>, label: &str, probe_ms: i32) {
    match obj_ref.get_timeout(probe_ms) {
        Ok(_) => panic!(
            "{}: task already finished within {} ms, the state this check needs did not hold",
            label, probe_ms
        ),
        Err(RayError::Timeout(_)) => {
            println!("  {} still unresolved after {} ms", label, probe_ms)
        }
        Err(e) => panic!("{}: the ref failed before it was cancelled: {}", label, e),
    }
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

    println!("=== Ray Task Cancellation ===");
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

    // ── Malformed and unknown ids ──────────────────────────
    println!("\n--- Malformed object ids ---");
    let short_id = vec![0u8; OBJECT_ID_LEN - 1];
    let long_id = vec![0u8; OBJECT_ID_LEN + 1];
    expect_rejected(&ray, &short_id, "truncated id");
    expect_rejected(&ray, &long_id, "over-long id");
    expect_refused(&ray);

    // ── Control: the same call shape, never cancelled ──────
    println!("\n--- Control: compute on a free CPU ---");
    let control = submit_compute(&ray, COMPUTE_N);
    let control_value: i64 = match control.get() {
        Ok(val) => val,
        Err(e) => panic!("the control compute failed: {}", e),
    };
    println!("  compute({}) → {}", COMPUTE_N, control_value);
    assert_eq!(
        control_value, 499_999_500_000,
        "control compute returned {} instead of 499_999_500_000",
        control_value
    );

    // ── Pending task ───────────────────────────────────────
    println!("\n--- Cancel a pending task ---");
    let long_task = submit_long_task(&ray);
    println!(
        "  submitted time.sleep({}) → {} (id_len={})",
        LONG_TASK_SLEEP_S,
        long_task.id_hex(),
        long_task.id().len()
    );
    std::thread::sleep(Duration::from_millis(300));
    expect_unresolved(&long_task, "long task", 200);

    let pending_task = submit_queued_task(&ray, long_task.id());
    println!(
        "  queued compute behind it → {} (waits on the long task's result)",
        pending_task.id_hex()
    );
    expect_unresolved(&pending_task, "pending task", 200);
    match ray.cancel(pending_task.id(), false, false) {
        Ok(()) => println!("  cancel(pending, force_kill=false, recursive=false) → Ok"),
        Err(e) => panic!("cancel of the pending task failed: {}", e),
    }
    expect_fast_failure(&pending_task, "pending task");

    // ── Running task ───────────────────────────────────────
    println!("\n--- Cancel a running task with force_kill ---");
    expect_unresolved(&long_task, "long task", 200);
    match ray.cancel(long_task.id(), true, true) {
        Ok(()) => println!("  cancel(running, force_kill=true, recursive=true) → Ok"),
        Err(e) => panic!("cancel of the running task failed: {}", e),
    }
    expect_fast_failure(&long_task, "running task");

    // ── Shutdown (automatic on drop) ──────────────────────
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ All cancel checks passed");
}

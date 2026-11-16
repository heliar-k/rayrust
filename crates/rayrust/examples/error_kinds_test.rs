//! Example: Ray's error classes survive the C ABI as distinct RayError variants.
//!
//! Two failures are triggered on the driver and their variants compared:
//!   - a get_timeout on a ref that never resolves → RayError::Timeout
//!   - a task whose function name no worker registers → RayError::Task
//!
//! The cluster needs the same setup as cancel_test:
//!   ray start --head --port=6399 --num-cpus=1 --include-dashboard=false
//!
//! Environment: RAY_ADDRESS, RAY_NODE_IP, RAY_WORKER_SO.

use std::time::Duration;

use rayrust::prelude::*;

/// A Python task that sleeps stays running for the whole example, so its ref
/// never resolves while the checks run.
const LONG_TASK_SLEEP_S: f64 = 120.0;

/// Much smaller than the sleep, so the get has to expire.
const PROBE_TIMEOUT_MS: i32 = 200;

/// No worker registers a function by this name.
const MISSING_FUNCTION: &str = "error_kinds_test_unregistered_function";

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

    // ── The two failures stay distinguishable ──────────────
    println!("\n--- Distinct classes ---");
    println!("  RayError::Timeout and RayError::Task are distinct variants");

    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ All error-kind checks passed");
}

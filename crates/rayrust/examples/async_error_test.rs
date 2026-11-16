//! Example: async get fails fast, honours its deadline, survives pressure.
//!
//! Four checks, each printing the elapsed time:
//!   1. a Python task that raises → Err within seconds, not after 300 s
//!   2. a get whose deadline expires → RayError::Timeout at the deadline
//!   3. 200 gets with a 1 ms timeout on one unready ref → all Err, no hang
//!   4. a normal task → the same value as get()
//!
//! The cluster needs the same setup as cancel_test:
//!   ray start --head --port=6399 --num-cpus=1 --include-dashboard=false
//!
//! Environment: RAY_ADDRESS, RAY_NODE_IP, RAY_WORKER_SO.

use std::time::{Duration, Instant};

use rayrust::prelude::*;

/// Python module holding the test tasks; the worker must be able to import it.
const PYTHON_MODULE: &str = "rayrust_test";

/// The sleep keeps its ref unresolved for far longer than any check here.
const LONG_TASK_SLEEP_S: f64 = 120.0;

/// Check 1: the failed task must fail long before this deadline.
const FAILED_TASK_TIMEOUT_S: u64 = 60;
const FAILED_TASK_MAX_MS: u128 = 10_000;

/// Check 2: the deadline handed to a task that never finishes in time.
const PENDING_GET_TIMEOUT: Duration = Duration::from_secs(2);
/// Room on top of the deadline for scheduling and the C ABI.
const PENDING_GET_SLACK_MS: u128 = 3_000;

/// Check 3: pressure on an unready ref.
const STRESS_ROUNDS: usize = 200;
const STRESS_TIMEOUT: Duration = Duration::from_millis(1);

/// Check 4: the deadline for a task that finishes normally.
const NORMAL_TASK_TIMEOUT: Duration = Duration::from_secs(30);

/// Worker-side directory of PYTHON_MODULE, passed through runtime_env.
fn python_module_dir() -> String {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/python");
    let path = std::fs::canonicalize(dir).expect("canonicalize the Python test module directory");
    path.to_str()
        .expect("the Python module path is valid UTF-8")
        .to_string()
}

/// Submit `time.sleep(LONG_TASK_SLEEP_S)`, whose ref stays unresolved.
fn submit_long_task(ray: &Ray) -> ObjectRef<()> {
    let arg = serialize(&LONG_TASK_SLEEP_S).expect("serialize the sleep argument");
    ray.task_call_python("time", "sleep", &[&arg], &[])
        .expect("submit the sleeping task")
}

/// Cancel a task that is already running; force_kill is the only option Ray
/// offers for those.
fn cancel_running(ray: &Ray, obj_ref: &ObjectRef<()>, label: &str) {
    match ray.cancel(obj_ref.id(), true, true) {
        Ok(()) => println!("  cancel({}, force_kill=true, recursive=true) → Ok", label),
        Err(e) => panic!("cancel of {} failed: {}", label, e),
    }
}

/// A task that raises must report the failure itself, not run into the deadline.
///
/// The message text is not asserted: a Python failure crosses the C ABI as the
/// binary msgpack payload of a RayTaskError, which the C string error channel
/// cannot carry, so the detail reads "unknown FFI error". Readable task text
/// only exists for workers that report plain strings.
async fn check_failed_task(ray: &Ray) {
    println!("\n--- 1. A failed task returns at once ---");
    // add() takes two arguments; one string makes the worker raise TypeError.
    let arg = serialize(&"only-one-argument".to_string()).expect("serialize the argument");
    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "add", &[&arg], &[])
        .expect("submit add() with one argument");
    println!("  ray.{}.add(<one string arg>) → {}", PYTHON_MODULE, obj_ref.id_hex());

    let start = Instant::now();
    let result = obj_ref
        .get_timeout_async(Duration::from_secs(FAILED_TASK_TIMEOUT_S))
        .await;
    let elapsed = start.elapsed().as_millis();

    match result {
        Ok(()) => panic!("a task that raises TypeError delivered a value"),
        Err(RayError::Task(msg)) => {
            println!("  failed after {} ms with RayError::Task", elapsed);
            println!("  message: {}", msg);
            assert!(
                elapsed < FAILED_TASK_MAX_MS,
                "the failure took {} ms, not the seconds the check needs",
                elapsed
            );
        }
        Err(other) => panic!(
            "the failed task returned {:?}, expected RayError::Task",
            other
        ),
    }
}

/// An expired deadline must fail the get and leave the process alive.
async fn check_timeout(ray: &Ray) {
    println!("\n--- 2. The deadline bounds the whole get ---");
    let long_task = submit_long_task(ray);
    println!(
        "  submitted time.sleep({}) → {}",
        LONG_TASK_SLEEP_S,
        long_task.id_hex()
    );

    let start = Instant::now();
    let result = long_task.get_timeout_async(PENDING_GET_TIMEOUT).await;
    let elapsed = start.elapsed().as_millis();

    match result {
        Ok(()) => panic!(
            "the sleeping task delivered a value within {:?}",
            PENDING_GET_TIMEOUT
        ),
        Err(RayError::Timeout(msg)) => {
            println!(
                "  get_timeout_async({:?}) → RayError::Timeout after {} ms",
                PENDING_GET_TIMEOUT, elapsed
            );
            println!("  message: {}", msg);
            assert!(
                elapsed >= PENDING_GET_TIMEOUT.as_millis(),
                "the get gave up after {} ms, before the {:?} deadline",
                elapsed,
                PENDING_GET_TIMEOUT
            );
            assert!(
                elapsed < PENDING_GET_TIMEOUT.as_millis() + PENDING_GET_SLACK_MS,
                "the get returned {} ms after the {:?} deadline",
                elapsed - PENDING_GET_TIMEOUT.as_millis(),
                PENDING_GET_TIMEOUT
            );
        }
        Err(other) => panic!(
            "an unready ref returned {:?}, expected RayError::Timeout",
            other
        ),
    }

    // The sleeping task holds the only CPU, so it must be cancelled before the
    // next tasks can run.
    cancel_running(ray, &long_task, "sleeping task");
}

/// Repeated short-deadline gets on one unready ref must all fail, without
/// hanging and without the process dying.
async fn check_pressure(ray: &Ray) {
    println!(
        "\n--- 3. {} gets with a {} ms timeout on one unready ref ---",
        STRESS_ROUNDS,
        STRESS_TIMEOUT.as_millis()
    );
    let long_task = submit_long_task(ray);
    println!(
        "  submitted time.sleep({}) → {}",
        LONG_TASK_SLEEP_S,
        long_task.id_hex()
    );

    let start = Instant::now();
    let mut succeeded = 0usize;
    for round in 0..STRESS_ROUNDS {
        match long_task.get_timeout_async(STRESS_TIMEOUT).await {
            Ok(()) => succeeded += 1,
            Err(RayError::Timeout(_)) => {}
            Err(other) => panic!(
                "round {} returned {:?}, expected RayError::Timeout",
                round, other
            ),
        }
    }
    let elapsed = start.elapsed();
    println!("  successful gets: {}", succeeded);
    println!(
        "  total time: {} ms over {} rounds ({} µs per round)",
        elapsed.as_millis(),
        STRESS_ROUNDS,
        elapsed.as_micros() / STRESS_ROUNDS as u128
    );
    assert_eq!(
        succeeded, 0,
        "{} of {} gets delivered a value for a task that is still sleeping",
        succeeded, STRESS_ROUNDS
    );

    cancel_running(ray, &long_task, "sleeping task");
}

/// A task that finishes must deliver the same value through both paths.
async fn check_normal_path(ray: &Ray) {
    println!("\n--- 4. A normal task delivers its value ---");
    let arg_a = serialize(&7i64).expect("serialize the first argument");
    let arg_b = serialize(&35i64).expect("serialize the second argument");
    let obj_ref = ray
        .task_call_python(PYTHON_MODULE, "add", &[&arg_a, &arg_b], &[])
        .expect("submit add(7, 35)");
    println!("  ray.{}.add(7, 35) → {}", PYTHON_MODULE, obj_ref.id_hex());

    let sync_ref: ObjectRef<i64> = obj_ref.clone().cast();
    let async_ref: ObjectRef<i64> = obj_ref.cast();

    let sync_value: i64 = sync_ref.get().expect("get() of add(7, 35)");

    let start = Instant::now();
    let async_value: i64 = async_ref
        .get_timeout_async(NORMAL_TASK_TIMEOUT)
        .await
        .expect("get_timeout_async() of add(7, 35)");
    let elapsed = start.elapsed().as_millis();

    println!(
        "  get() → {}, get_timeout_async({:?}) → {} after {} ms",
        sync_value, NORMAL_TASK_TIMEOUT, async_value, elapsed
    );
    assert_eq!(
        async_value, sync_value,
        "get_timeout_async returned {} but get() returned {}",
        async_value, sync_value
    );
    assert_eq!(async_value, 42, "add(7, 35) returned {}", async_value);
}

#[tokio::main]
async fn main() {
    let address =
        std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| {
        eprintln!("ERROR: RAY_WORKER_SO not set. Build the worker first:");
        eprintln!("  cargo build --release -p rayrust-example-worker");
        std::process::exit(1);
    });

    println!("=== Ray Async Get Errors ===");
    println!("Address: {}", address);
    println!("Node IP: {}", node_ip);
    println!("Worker .so: {}", worker_so);

    let python_dir = python_module_dir();
    println!("Python module dir: {}", python_dir);
    let runtime_env = format!(
        "{{\"env_vars\": {{\"PYTHONPATH\": \"{}\"}}}}",
        python_dir
    );

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()])
        .runtime_env(runtime_env);

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

    check_failed_task(&ray).await;
    check_timeout(&ray).await;
    check_pressure(&ray).await;
    check_normal_path(&ray).await;

    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ All async get checks passed");
}

//! Example: local-mode `Ray` shutdown drains submitted tasks before teardown.
//!
//! Run:
//! ```text
//! ./target/release/examples/local_task_lifecycle_test
//! ```
//! Optional child-process mode used internally by check 2:
//! ```text
//! ./target/release/examples/local_task_lifecycle_test exit-while-task
//! ```
//!
//! In local mode, dropping the last `Ray` handle waits inside `ray_shutdown` for
//! all tasks submitted through the current runtime to become ready in the local
//! object store before closing the reference-count gate and shutting Ray down.
//! Once shutdown begins, new local-mode task submissions are rejected. Cluster
//! mode does not wait for submitted tasks on drop.
//!
//! Checks, in order:
//! 1. Dropping the last handle waits for an in-flight 300 ms task (drop >= 250 ms),
//!    and a subsequent `Ray::local()` reconnects and runs another task cleanly.
//! 2. A child process submitting a 400 ms task and returning from `main` prints
//!    its completion marker and exits with code 0.
//! 3. 20 rounds of `Ray::local()` -> 20 ms task -> `drop` finish cleanly and
//!    keep the thread count bounded (`threads_end <= threads_start + 4`).
//! 4. Waiting on a task whose result was already read via `get()` does not stall
//!    shutdown (`drop < 100 ms`).
//! 5. Submitting 1200 fast tasks (crossing the 1024-entry prune threshold) and
//!    dropping the handle finishes cleanly (`drop < 1000 ms`) without thread growth.
//! 6. The prune keeps ids that are not ready: a 1 s task is left in flight while
//!    1023 fast tasks fill the bookkeeping to the 1024-entry threshold, the 1025th
//!    submission runs the prune and must not block (`< 200 ms`), and the following
//!    `drop` still waits for the slow task (`>= 400 ms`). An inverted prune that
//!    throws away the unready id skips that wait and crashes the process later.
//! 7. A task body that asks for a handle while the driver drops its only handle is
//!    refused with `RayError::Runtime` instead of waiting for the runtime lock: the
//!    child process prints `TASK_CONNECT_REFUSED` (never `TASK_CONNECT_OK` and never
//!    `TASK_CONNECT_WRONG_ERROR`), its `drop` still waits for the 1 s task
//!    (`>= 400 ms`), and it exits with code 0. The parent supervises it with a 15 s
//!    wall-clock deadline and kills it if the drain never returns.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rayrust::prelude::*;

const CHILD_MARKER: &str = "LOCAL_TASK_EXIT_MARKER";
const TASK_CONNECT_REFUSED: &str = "TASK_CONNECT_REFUSED";
const TASK_CONNECT_OK: &str = "TASK_CONNECT_OK";
const TASK_CONNECT_WRONG_ERROR: &str = "TASK_CONNECT_WRONG_ERROR";
const TASK_CHILD_DROP: &str = "CHILD_DROP_MS=";

/// 第 7 项的子进程允许的最长墙上时钟（秒），到期就 kill 并判失败。
const TASK_CONNECT_CHILD_DEADLINE_S: u64 = 15;

#[rayrust::remote]
fn sleep_for(seconds: f64) -> i64 {
    std::thread::sleep(Duration::from_secs_f64(seconds));
    42
}

#[rayrust::remote]
fn identity_i64(value: i64) -> i64 {
    value
}

/// 第 7 项：任务体在驱动丢弃最后一个句柄期间尝试取句柄，必须被拒并且错误是
/// `RayError::Runtime`（关闭期间的请求），而不是绕道 C 层的 `RayError::Ffi`。
/// 任务总时长 1 秒：先睡 100 ms 去取句柄，再睡 900 ms 返回。
#[rayrust::remote]
fn task_connect_during_drop() -> i64 {
    std::thread::sleep(Duration::from_millis(100));
    match Ray::local() {
        Ok(_ray) => println!("{}", TASK_CONNECT_OK),
        Err(RayError::Runtime(_)) => println!("{}", TASK_CONNECT_REFUSED),
        Err(other) => println!("{}: {}", TASK_CONNECT_WRONG_ERROR, other),
    }
    std::thread::sleep(Duration::from_millis(900));
    42
}

fn read_thread_count() -> usize {
    let status =
        std::fs::read_to_string("/proc/self/status").expect("failed to read /proc/self/status");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("Threads:") {
            return rest
                .trim()
                .parse::<usize>()
                .expect("failed to parse Threads value from /proc/self/status");
        }
    }
    panic!("Threads field missing from /proc/self/status");
}

fn run_exit_while_task_child() {
    let ray = Ray::local().expect("child: Ray::local failed");
    let _pending = sleep_for_remote(&ray, 0.4).expect("child: submit 400 ms task failed");
    println!("{}", CHILD_MARKER);
}

fn run_task_connect_during_drop_child() {
    let ray = Ray::local().expect("child: Ray::local failed");
    let _pending = task_connect_during_drop_remote(&ray)
        .expect("child: submit the task that takes a handle failed");
    let t0 = Instant::now();
    drop(ray);
    let drop_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!("{}{:.2}", TASK_CHILD_DROP, drop_ms);
    println!("CHILD_DONE");
}

fn main() {
    if let Some(arg) = std::env::args().nth(1) {
        if arg == "exit-while-task" {
            run_exit_while_task_child();
            return;
        }
        if arg == "task-connect-during-drop" {
            run_task_connect_during_drop_child();
            return;
        }
        panic!(
            "unknown argument {:?}, expected none, \"exit-while-task\" or \"task-connect-during-drop\"",
            arg
        );
    }

    println!("=== Local-mode task lifecycle ===");

    // ── 1. Drop waits for in-flight task, then reconnect succeeds ──
    println!("\n--- 1. drop waits for in-flight task and reconnect succeeds ---");
    {
        let ray = Ray::local().expect("Ray::local failed");
        let _pending = sleep_for_remote(&ray, 0.3).expect("submit 300 ms task failed");
        let t0 = Instant::now();
        drop(ray);
        let drop_ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("  drop(ray) after 300 ms task: {:.2} ms", drop_ms);
        assert!(
            drop_ms >= 250.0,
            "expected drop(ray) to wait at least 250 ms for the 300 ms task, got {:.2} ms",
            drop_ms
        );

        let ray2 = Ray::local().expect("second Ray::local failed");
        let obj = identity_i64_remote(&ray2, 99).expect("submit after reconnect failed");
        let val = obj.get().expect("get after reconnect failed");
        println!("  reconnect task result: {}", val);
        assert_eq!(val, 99, "unexpected value after reconnect: {}", val);
        drop(ray2);
    }

    // ── 2. Returning from main while a 400 ms task is running ──────
    println!("\n--- 2. child process exits cleanly while 400 ms task runs ---");
    {
        let exe = std::env::current_exe().expect("failed to locate current executable");
        let t0 = Instant::now();
        let output = Command::new(&exe)
            .arg("exit-while-task")
            .output()
            .expect("failed to spawn exit-while-task child");
        let wall_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let code = output.status.code();
        println!(
            "  child status={:?} wall={:.2} ms stdout={:?}",
            output.status,
            wall_ms,
            stdout.trim()
        );
        assert!(
            output.status.success() && code == Some(0),
            "expected child exit code 0, got {:?}; stdout={:?}; stderr={:?}",
            output.status,
            stdout,
            stderr
        );
        assert!(
            stdout.lines().any(|line| line.trim() == CHILD_MARKER),
            "child stdout missing marker {:?}; stdout={:?}; stderr={:?}",
            CHILD_MARKER,
            stdout,
            stderr
        );
    }

    // ── 3. 20 rounds of connect -> 20 ms task -> drop ──────────────
    println!("\n--- 3. 20 rounds of connect -> 20 ms task -> drop ---");
    {
        const ROUNDS: usize = 20;
        let threads_start = read_thread_count();
        let total_start = Instant::now();
        let mut min_drop_ms = f64::INFINITY;
        let mut max_drop_ms = 0.0_f64;
        let mut sum_drop_ms = 0.0_f64;

        for round in 1..=ROUNDS {
            let ray = Ray::local()
                .unwrap_or_else(|e| panic!("round {}: Ray::local failed: {}", round, e));
            let _pending = sleep_for_remote(&ray, 0.02)
                .unwrap_or_else(|e| panic!("round {}: submit 20 ms task failed: {}", round, e));
            let t0 = Instant::now();
            drop(ray);
            let drop_ms = t0.elapsed().as_secs_f64() * 1000.0;
            min_drop_ms = min_drop_ms.min(drop_ms);
            max_drop_ms = max_drop_ms.max(drop_ms);
            sum_drop_ms += drop_ms;
        }

        let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
        let threads_end = read_thread_count();
        println!(
            "  rounds={} total={:.2} ms drop_ms(min={:.2}, avg={:.2}, max={:.2}) threads({} -> {})",
            ROUNDS,
            total_ms,
            min_drop_ms,
            sum_drop_ms / ROUNDS as f64,
            max_drop_ms,
            threads_start,
            threads_end
        );
        assert!(
            threads_end <= threads_start + 4,
            "thread count grew unexpectedly across {} rounds: {} -> {}",
            ROUNDS,
            threads_start,
            threads_end
        );
    }

    // ── 4. Already-read result does not stall shutdown ─────────────
    println!("\n--- 4. already-read result does not stall shutdown ---");
    {
        let ray = Ray::local().expect("Ray::local failed");
        let obj = sleep_for_remote(&ray, 0.05).expect("submit 50 ms task failed");
        let val = obj.get().expect("get on 50 ms task failed");
        assert_eq!(val, 42, "unexpected value from sleep_for: {}", val);
        let t0 = Instant::now();
        drop(ray);
        let drop_ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("  got={}, drop(ray) after get: {:.3} ms", val, drop_ms);
        assert!(
            drop_ms < 100.0,
            "expected drop(ray) after get to finish in < 100 ms, got {:.3} ms",
            drop_ms
        );
    }

    // ── 5. Prune path: 1200 fast tasks crossing 1024 threshold ─────
    println!("\n--- 5. 1200 fast tasks crossing prune threshold ---");
    {
        const TASK_COUNT: i64 = 1200;
        let threads_before = read_thread_count();
        let ray = Ray::local().expect("Ray::local failed");
        let submit_start = Instant::now();
        let mut last_ref = None;
        for i in 0..TASK_COUNT {
            let obj = identity_i64_remote(&ray, i)
                .unwrap_or_else(|e| panic!("submit task {} failed: {}", i, e));
            if i == TASK_COUNT - 1 {
                last_ref = Some(obj);
            }
        }
        let submit_ms = submit_start.elapsed().as_secs_f64() * 1000.0;
        let last_val = last_ref
            .expect("last task ref missing")
            .get()
            .expect("get on last fast task failed");
        assert_eq!(last_val, TASK_COUNT - 1);

        let t0 = Instant::now();
        drop(ray);
        let drop_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let threads_after = read_thread_count();
        println!(
            "  submitted={} submit={:.2} ms last_val={} drop={:.2} ms threads({} -> {})",
            TASK_COUNT, submit_ms, last_val, drop_ms, threads_before, threads_after
        );
        assert!(
            drop_ms < 1000.0,
            "expected drop(ray) after {} fast tasks to finish in < 1000 ms, got {:.2} ms",
            TASK_COUNT,
            drop_ms
        );
        assert!(
            threads_after <= threads_before + 4,
            "thread count grew unexpectedly during prune test: {} -> {}",
            threads_before,
            threads_after
        );
    }

    // ── 6. Prune keeps ids that are not ready ─────────────────────
    println!("\n--- 6. prune keeps unready ids across the threshold ---");
    {
        const FAST_TASKS: i64 = 1023;
        let ray = Ray::local().expect("Ray::local failed");
        // 先让一个 1 秒的任务进线程池，它在裁剪发生时仍未就绪。
        let _slow = sleep_for_remote(&ray, 1.0).expect("submit 1 s slow task failed");
        for i in 0..FAST_TASKS {
            let _ = identity_i64_remote(&ray, i)
                .unwrap_or_else(|e| panic!("submit fast task {} failed: {}", i, e));
        }
        // 提交到这里 id 列表已满 1024 条，下面这次提交会先跑裁剪。
        let t0 = Instant::now();
        let trigger = identity_i64_remote(&ray, FAST_TASKS)
            .expect("submit the task that triggers the prune failed");
        let trigger_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let trigger_val = trigger.get().expect("get on the triggering task failed");
        assert_eq!(trigger_val, FAST_TASKS);
        assert!(
            trigger_ms < 200.0,
            "expected the submission that runs the prune to finish in < 200 ms, got {:.2} ms",
            trigger_ms
        );

        let t0 = Instant::now();
        drop(ray);
        let drop_ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "  slow=1 s fast={} trigger_submit={:.2} ms drop={:.2} ms",
            FAST_TASKS, trigger_ms, drop_ms
        );
        assert!(
            drop_ms >= 400.0,
            "expected drop(ray) to keep waiting for the unready 1 s task (>= 400 ms), got {:.2} ms",
            drop_ms
        );
    }

    // ── 7. A task body asking for a handle during shutdown is refused ──
    println!("\n--- 7. a task taking a handle during drop is refused ---");
    {
        let exe = std::env::current_exe().expect("failed to locate current executable");
        let mut child = Command::new(&exe)
            .arg("task-connect-during-drop")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn the task-connect-during-drop child");
        let mut child_stdout = child.stdout.take().expect("child stdout missing");
        let mut child_stderr = child.stderr.take().expect("child stderr missing");
        let t0 = Instant::now();
        let deadline = t0 + Duration::from_secs(TASK_CONNECT_CHILD_DEADLINE_S);
        let status = loop {
            match child.try_wait().expect("try_wait on the child failed") {
                Some(status) => break status,
                None => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!(
                            "the child did not return within {} s: the drop never finished, the \
                             task body is probably waiting on the runtime lock",
                            TASK_CONNECT_CHILD_DEADLINE_S
                        );
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };
        let wall_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let mut stdout = String::new();
        let mut stderr = String::new();
        child_stdout
            .read_to_string(&mut stdout)
            .expect("failed to read child stdout");
        child_stderr
            .read_to_string(&mut stderr)
            .expect("failed to read child stderr");
        let child_drop_ms = stdout
            .lines()
            .find_map(|line| line.strip_prefix(TASK_CHILD_DROP))
            .and_then(|value| value.trim().parse::<f64>().ok())
            .unwrap_or_else(|| {
                panic!(
                    "child stdout missing {}: stdout={:?} stderr={:?}",
                    TASK_CHILD_DROP, stdout, stderr
                )
            });
        println!(
            "  child status={:?} wall={:.2} ms child_drop={:.2} ms stdout={:?}",
            status,
            wall_ms,
            child_drop_ms,
            stdout.trim()
        );
        assert!(
            status.success(),
            "expected the child to exit with code 0, got {:?}; stdout={:?} stderr={:?}",
            status,
            stdout,
            stderr
        );
        assert!(
            stdout.contains(TASK_CONNECT_REFUSED),
            "the task body did not see the shutdown refusal; stdout={:?} stderr={:?}",
            stdout,
            stderr
        );
        assert!(
            !stdout.contains(TASK_CONNECT_OK),
            "the task body obtained a handle while the last handle was being dropped; stdout={:?}",
            stdout
        );
        assert!(
            !stdout.contains(TASK_CONNECT_WRONG_ERROR),
            "the task body was refused with something other than RayError::Runtime; stdout={:?}",
            stdout
        );
        assert!(
            child_drop_ms >= 400.0,
            "expected the child drop to keep waiting for the 1 s task (>= 400 ms), got {:.2} ms",
            child_drop_ms
        );
    }

    println!("\nAll local task lifecycle checks passed.");
}

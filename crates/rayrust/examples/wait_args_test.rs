//! Example: `Ray::wait` / `Ray::wait_async` 的参数规则。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/wait_args_test local
//! ./target/release/examples/wait_args_test cluster
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! 规则：`num_objects` 必须在 `1..=object_refs.len()`，id 必须两两不同。违反时
//! `Ray::wait` 与 `Ray::wait_async` 都返回 `RayError::Ffi`，两种模式的句子一致：
//! * `Number of objects to wait for must be between 1 and the number of ids.`
//! * `Duplicate object IDs not supported in wait.`
//!
//! 这两条由 C 入口 `ray_wait` 在碰 runtime 之前判掉，与集群模式一致：Ray 的
//! `CoreWorker::Wait` 不看对象是否就绪，两类参数一律拒绝（`core_worker.cc:1475-1483`）。
//! 本地模式没有 core worker，它的 `Wait` 把 id 收进集合去重却把原数量传下去
//! （`local_mode_object_store.cc:82-97`）：数量大于去重后的集合大小、且还有 id 没进本地
//! 存储时，`memory_store.cc:89` 的 RAY_CHECK 结束整个进程；全部 id 都已经在本地存储里时
//! `GetImpl` 在构造 `GetRequest` 之前就返回，同一个输入得到的是 `Ok`。下面第 1、2 项
//! 用的就是已就绪的 `put` 引用：拦下它们的是入口守卫，本地存储那条路径在全部 id
//! 已就绪时本来会返回 `Ok`。
//!
//! 检查项，每一项都打印实际结果：
//! 1. `wait([r], 2, 20)`：数量大于引用数
//! 2. `wait([r.clone(), r], 2, 20)`：同一个引用两次
//! 3. `wait([r], 0, 20)`：数量为 0
//! 4. `wait([], 0, 20)`：空引用表
//! 5. `wait([r], usize::MAX, 20)`：数量超过 `i32::MAX`
//! 6. `wait_async([r], 2, 20ms)`：异步入口同样拦下
//! 7. 正向对照：两个 `put` 出来的引用，`num_objects` 为 2 时两个都就绪；为 1 时
//!    Ray 只报一个就绪（两种模式都把 ready 限制在 `num_objects` 个），两组合起来
//!    仍是全部引用
//!
//! 每一次拒绝之后都跑一次合法 `wait([a], 1, 1000)` 并断言 `Ok`：进程与运行时都还活着。

use std::time::Duration;

use rayrust::prelude::*;

/// 被拒绝的那几次等待用的期限（毫秒）。
const REJECT_TIMEOUT_MS: i32 = 20;

/// 拒绝之后那次合法等待的期限（毫秒）。
const LEGAL_TIMEOUT_MS: i32 = 1000;

/// 正向对照的期限（毫秒）：两个对象都已经在本地存储里。
const READY_TIMEOUT_MS: i32 = 5000;

/// 数量与重复 id 的两句话，逐字取自 Ray 的 `CoreWorker::Wait`。
const COUNT_SENTENCE: &str =
    "Number of objects to wait for must be between 1 and the number of ids.";
const DUPLICATE_SENTENCE: &str = "Duplicate object IDs not supported in wait.";

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
}

/// 打印失败原因并以退出码 1 结束（不用 panic 制造失败路径）。
fn fail(reason: String) -> ! {
    println!("  FAIL: {}", reason);
    std::process::exit(1);
}

/// 一次应当被拒绝的等待：必须是 `RayError::Ffi`，且带上指定的那句话。
fn expect_rejected<T>(
    label: &str,
    result: Result<(Vec<ObjectRef<T>>, Vec<ObjectRef<T>>), RayError>,
    sentence: &str,
) {
    match result {
        Err(RayError::Ffi(message)) => {
            if !message.contains(sentence) {
                fail(format!("{}: error message missing expected phrase: {}", label, message));
            }
            if !message.contains("ray_wait") {
                fail(format!("{}: error message missing entry name: {}", label, message));
            }
            println!("  {} -> Ffi({})", label, message);
        }
        Err(other) => fail(format!("{}: returned unexpected error variant: {}", label, other)),
        Ok((ready, unready)) => fail(format!(
            "{}: unexpectedly succeeded, ready={}, unready={}",
            label,
            ready.len(),
            unready.len()
        )),
    }
}

/// 拒绝之后运行时仍然可用：一次合法等待照常返回 `Ok`。
fn assert_runtime_alive(ray: &Ray, after: &str) {
    let value = ray.put(&99i64).expect("put(99)");
    match ray.wait(&[value], 1, LEGAL_TIMEOUT_MS) {
        Ok((ready, unready)) => {
            if ready.len() != 1 || !unready.is_empty() {
                fail(format!(
                    "valid wait after {}: ready={}, unready={}",
                    after,
                    ready.len(),
                    unready.len()
                ));
            }
            println!(
                "  valid wait([a], 1, {}) after {} -> Ok, ready=1",
                LEGAL_TIMEOUT_MS, after
            );
        }
        Err(e) => fail(format!("valid wait after {} returned error: {}", after, e)),
    }
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== Ray::wait count and duplicate id ({}) ===", mode);
    let config = match mode.as_str() {
        "local" => RayConfig::local(),
        "cluster" => cluster_config(),
        other => fail(format!(
            "unknown mode {:?}, expected local or cluster",
            other
        )),
    };
    let ray = Ray::connect(&config).expect("Ray::connect");

    // 被拒绝的那几次等待都用同一个引用。
    let r = ray.put(&7i64).expect("put(7)");

    println!("\n--- 1. num_objects greater than refs len ---");
    expect_rejected(
        "wait([r], 2, 20)",
        ray.wait(&[r.clone()], 2, REJECT_TIMEOUT_MS),
        COUNT_SENTENCE,
    );
    assert_runtime_alive(&ray, "count > refs len");

    println!("\n--- 2. duplicate ref passed twice ---");
    expect_rejected(
        "wait([r.clone(), r], 2, 20)",
        ray.wait(&[r.clone(), r.clone()], 2, REJECT_TIMEOUT_MS),
        DUPLICATE_SENTENCE,
    );
    assert_runtime_alive(&ray, "duplicate id");

    println!("\n--- 3. num_objects is 0 ---");
    expect_rejected(
        "wait([r], 0, 20)",
        ray.wait(&[r.clone()], 0, REJECT_TIMEOUT_MS),
        COUNT_SENTENCE,
    );
    assert_runtime_alive(&ray, "count == 0");

    println!("\n--- 4. empty ref slice ---");
    let empty: [ObjectRef<i64>; 0] = [];
    expect_rejected(
        "wait([], 0, 20)",
        ray.wait(&empty, 0, REJECT_TIMEOUT_MS),
        COUNT_SENTENCE,
    );
    assert_runtime_alive(&ray, "empty refs");

    println!("\n--- 5. num_objects exceeds i32::MAX ---");
    expect_rejected(
        "wait([r], usize::MAX, 20)",
        ray.wait(&[r.clone()], usize::MAX, REJECT_TIMEOUT_MS),
        COUNT_SENTENCE,
    );
    assert_runtime_alive(&ray, "count > i32::MAX");

    println!("\n--- 6. async entry point ---");
    let async_result = ray
        .wait_async(
            &[r.clone()],
            2,
            Duration::from_millis(REJECT_TIMEOUT_MS as u64),
        )
        .await;
    expect_rejected("wait_async([r], 2, 20ms)", async_result, COUNT_SENTENCE);
    assert_runtime_alive(&ray, "wait_async");

    println!("\n--- 7. Control: both refs ready ---");
    let a = ray.put(&1i64).expect("put(1)");
    let b = ray.put(&2i64).expect("put(2)");
    match ray.wait(&[a.clone(), b.clone()], 2, READY_TIMEOUT_MS) {
        Ok((ready, unready)) => {
            if ready.len() != 2 || !unready.is_empty() {
                fail(format!(
                    "wait([a, b], 2, {}) returned ready={}, unready={}",
                    READY_TIMEOUT_MS,
                    ready.len(),
                    unready.len()
                ));
            }
            println!("  wait([a, b], 2, {}) -> both ready", READY_TIMEOUT_MS);
        }
        Err(e) => fail(format!("wait([a, b], 2, ...) returned error: {}", e)),
    }
    match ray.wait(&[a.clone(), b.clone()], 1, READY_TIMEOUT_MS) {
        Ok((ready, unready)) => {
            if ready.len() != 1 || unready.len() != 1 {
                fail(format!(
                    "wait([a, b], 1, {}) returned ready={}, unready={}",
                    READY_TIMEOUT_MS,
                    ready.len(),
                    unready.len()
                ));
            }
            let ready_id = ready[0].id_hex();
            if ready_id != a.id_hex() && ready_id != b.id_hex() {
                fail(format!(
                    "wait([a, b], 1, ...) reported unknown ready id: {}",
                    ready_id
                ));
            }
            println!(
                "  wait([a, b], 1, {}) -> ready=1 (capped at num_objects), unready=1, total=2",
                READY_TIMEOUT_MS
            );
        }
        Err(e) => fail(format!("wait([a, b], 1, ...) returned error: {}", e)),
    }

    println!("\nPASS: wait argument rules passed in {} mode", mode);
}

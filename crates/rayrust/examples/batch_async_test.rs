//! 示例：批量取、异步批量取、异步 wait。
//!
//! 六项检查，每项都打印实际结果：
//!   1. 500 个任务：一次批量取 / 逐个引用的顺序取 / 500 个并发单引用取
//!   2. 批次里有一个永远不就绪的元素：get_many_timeout_async 与
//!      get_many_timeout 都在期限附近返回 RayError::Timeout
//!   3. wait_async 处理混合批次：已完成的那一个就绪，睡眠的那一个不就绪
//!   4. 2000 次批量取，第 2 个元素解码失败：常驻内存不增长
//!   5. 空批次返回 Ok(vec![])
//!   6. 小于 -1 的期限：get_many_timeout / get_timeout / wait 都返回
//!      RayError::Ffi，进程继续运行
//!
//! 集群配置与 async_error_test 相同：
//!   ray start --head --port=6399 --num-cpus=1 --include-dashboard=false
//!
//! 环境变量：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。

use std::time::{Duration, Instant};

use rayrust::prelude::*;

/// worker 侧的任务，与 crates/rayrust-example-worker 的 multiply 对应。
#[rayrust::remote]
fn multiply(a: i64, b: i64) -> i64 {
    a * b
}

/// 检查 1 的任务数。
const BATCH_TASKS: usize = 500;
/// 检查 1 里每一次取的期限。
const BATCH_TIMEOUT: Duration = Duration::from_secs(60);

/// 睡眠任务让引用在整段检查里都不就绪。
const LONG_TASK_SLEEP_S: f64 = 120.0;
/// 检查 2 的异步批量取期限。
const ASYNC_BATCH_TIMEOUT: Duration = Duration::from_secs(2);
/// 检查 2 的同步批量取期限。
const SYNC_BATCH_TIMEOUT_MS: i32 = 1000;
/// 期限之外的调度与 C ABI 余量。
const TIMEOUT_SLACK_MS: u128 = 3_000;

/// 检查 3 的 wait 期限。
const WAIT_TIMEOUT: Duration = Duration::from_millis(500);

/// 检查 4 的批次大小与轮数；批次越大，每轮泄漏的数组与 buffer 越多。
const LEAK_BATCH: usize = 2048;
const LEAK_ROUNDS: usize = 2000;
/// 检查 4 允许的常驻内存最大增长。
const LEAK_MAX_GROWTH_BYTES: u64 = 20 * 1024 * 1024;

/// 检查 6 的非法期限。Ray 只接受 -1 与非负值，这个值会让内部的 RAY_CHECK
/// 终止整个进程，入口必须先返回错误。
const INVALID_TIMEOUT_MS: i32 = -2;

/// worker 侧 Python 模块目录，通过 runtime_env 传给 worker。
fn python_module_dir() -> String {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/python");
    let path = std::fs::canonicalize(dir).expect("canonicalize the Python test module directory");
    path.to_str()
        .expect("the Python module path is valid UTF-8")
        .to_string()
}

/// 读 /proc/self/statm 的第二项（常驻页数）换算成字节。
fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read /proc/self/statm");
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .expect("the resident field of /proc/self/statm")
        .parse()
        .expect("parse the resident page count");
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    resident_pages * page_size
}

/// 提交 `time.sleep(LONG_TASK_SLEEP_S)`，其引用在整段检查里都不就绪。
fn submit_long_task(ray: &Ray) -> ObjectRef<()> {
    let arg = serialize(&LONG_TASK_SLEEP_S).expect("serialize the sleep argument");
    ray.task_call_python("time", "sleep", &[&arg], &[])
        .expect("submit the sleeping task")
}

/// 取消一个正在运行的任务；force_kill 是 Ray 对这类任务唯一的手段。
fn cancel_running(ray: &Ray, obj_ref: &ObjectRef<()>, label: &str) {
    match ray.cancel(obj_ref.id(), true, true) {
        Ok(()) => println!("  cancel({}, force_kill=true, recursive=true) → Ok", label),
        Err(e) => panic!("cancel of {} failed: {}", label, e),
    }
}

/// 只用来确认 `ObjectRef::clone` 不要求 `T: Clone`。
struct NoClone(#[allow(dead_code)] i64);

/// 编译期检查：`T` 不实现 `Clone` 时 `ObjectRef<T>` 仍然可以克隆。
fn check_clone_without_t_clone() {
    println!("\n--- 编译检查：ObjectRef<NoClone>::clone() 不要求 T: Clone ---");
    let id = vec![7u8; 28];
    let obj_ref: ObjectRef<NoClone> = ObjectRef::from(id.clone());
    let cloned = obj_ref.clone();
    assert_eq!(cloned.id(), id.as_slice(), "clone 之后 id 变了");
    println!("  ObjectRef<NoClone>::clone() → id {} 字节", cloned.id().len());
}

/// 一次批量取应当比 500 个并发单引用快出一倍。
async fn check_batch_vs_single(ray: &Ray) {
    println!("\n--- 1. 批量取 vs 逐个引用取（{} 个任务）---", BATCH_TASKS);
    let refs: Vec<ObjectRef<i64>> = (0..BATCH_TASKS).map(|i| multiply_remote(ray, i as i64, 1)).collect();
    println!("  已提交 {} 个 multiply(i, 1) 任务", refs.len());

    let start = Instant::now();
    let mut sequential = Vec::with_capacity(refs.len());
    for obj_ref in &refs {
        sequential.push(
            obj_ref
                .get_timeout_async(BATCH_TIMEOUT)
                .await
                .expect("get_timeout_async 逐个取"),
        );
    }
    let sequential_elapsed = start.elapsed();

    let start = Instant::now();
    let batch = ray
        .get_many_timeout_async(&refs, BATCH_TIMEOUT)
        .await
        .expect("get_many_timeout_async 批量取");
    let batch_elapsed = start.elapsed();

    let start = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for (i, obj_ref) in refs.clone().into_iter().enumerate() {
        set.spawn(async move { (i, obj_ref.get_timeout_async(BATCH_TIMEOUT).await) });
    }
    let mut concurrent = vec![None; refs.len()];
    while let Some(joined) = set.join_next().await {
        let (i, value) = joined.expect("JoinSet 的任务失败");
        concurrent[i] = Some(value.expect("并发 get_timeout_async"));
    }
    let concurrent: Vec<i64> = concurrent
        .into_iter()
        .map(|value| value.expect("并发档缺了某个引用"))
        .collect();
    let concurrent_elapsed = start.elapsed();

    let rate = |elapsed: Duration| refs.len() as f64 / elapsed.as_secs_f64();
    println!(
        "  顺序单引用（参考）：{} ms，{:.0} 个/秒",
        sequential_elapsed.as_millis(),
        rate(sequential_elapsed)
    );
    println!(
        "  批量一次：  {} ms，{:.0} 个/秒",
        batch_elapsed.as_millis(),
        rate(batch_elapsed)
    );
    println!(
        "  并发单引用：{} ms，{:.0} 个/秒",
        concurrent_elapsed.as_millis(),
        rate(concurrent_elapsed)
    );
    println!(
        "  顺序取是批量取的 {:.2} 倍",
        sequential_elapsed.as_secs_f64() / batch_elapsed.as_secs_f64()
    );

    assert_eq!(batch.len(), refs.len(), "批量取返回 {} 个值", batch.len());
    for (i, value) in batch.iter().enumerate() {
        assert_eq!(
            *value, i as i64,
            "批量取的第 {} 个值是 {}，应为 {}",
            i, value, i
        );
    }
    assert_eq!(sequential.len(), refs.len(), "顺序取返回 {} 个值", sequential.len());
    for (i, value) in sequential.iter().enumerate() {
        assert_eq!(*value, i as i64, "顺序取的第 {} 个值是 {}", i, value);
    }
    assert_eq!(concurrent.len(), refs.len(), "并发取返回 {} 个值", concurrent.len());
    for (i, value) in concurrent.iter().enumerate() {
        assert_eq!(*value, i as i64, "并发取的第 {} 个值是 {}", i, value);
    }
    // 判据取并发档：500 个并发单引用正是批量实现退化成逐个取时的耗时上界；
    // 顺序档先把 500 个任务的执行时间付掉了，退化后的批量取仍旧远快于它。
    assert!(
        batch_elapsed * 2 < concurrent_elapsed,
        "批量取用了 {} ms，并发单引用用了 {} ms，没有快出一倍",
        batch_elapsed.as_millis(),
        concurrent_elapsed.as_millis()
    );
}

/// 批量取的期限与单引用一样，由 Ray 在 `Get` 内部执行。
async fn check_batch_timeout(ray: &Ray) {
    println!("\n--- 2. 批量取的期限 ---");
    let long_task = submit_long_task(ray);
    println!("  提交 time.sleep({}) → {}", LONG_TASK_SLEEP_S, long_task.id_hex());
    let refs: Vec<ObjectRef<()>> = vec![long_task.clone()];

    let start = Instant::now();
    match ray.get_many_timeout_async(&refs, ASYNC_BATCH_TIMEOUT).await {
        Ok(_) => panic!("睡眠任务在 {:?} 内交付了值", ASYNC_BATCH_TIMEOUT),
        Err(RayError::Timeout(msg)) => {
            let elapsed = start.elapsed().as_millis();
            println!(
                "  get_many_timeout_async({:?}) → RayError::Timeout，{} ms",
                ASYNC_BATCH_TIMEOUT, elapsed
            );
            println!("  message: {}", msg);
            assert!(
                elapsed >= ASYNC_BATCH_TIMEOUT.as_millis(),
                "批量取在 {} ms 就放弃，早于 {:?} 的期限",
                elapsed,
                ASYNC_BATCH_TIMEOUT
            );
            assert!(
                elapsed < ASYNC_BATCH_TIMEOUT.as_millis() + TIMEOUT_SLACK_MS,
                "批量取在期限之后 {} ms 才返回",
                elapsed - ASYNC_BATCH_TIMEOUT.as_millis()
            );
        }
        Err(other) => panic!("未就绪的批量取返回 {:?}，应为 RayError::Timeout", other),
    }

    let start = Instant::now();
    match ray.get_many_timeout(&refs, SYNC_BATCH_TIMEOUT_MS) {
        Ok(_) => panic!("睡眠任务在 {} ms 内交付了值", SYNC_BATCH_TIMEOUT_MS),
        Err(RayError::Timeout(msg)) => {
            let elapsed = start.elapsed().as_millis();
            println!(
                "  get_many_timeout({} ms) → RayError::Timeout，{} ms",
                SYNC_BATCH_TIMEOUT_MS, elapsed
            );
            println!("  message: {}", msg);
            assert!(
                elapsed >= SYNC_BATCH_TIMEOUT_MS as u128,
                "同步批量取在 {} ms 就放弃，早于 {} ms 的期限",
                elapsed,
                SYNC_BATCH_TIMEOUT_MS
            );
            assert!(
                elapsed < SYNC_BATCH_TIMEOUT_MS as u128 + TIMEOUT_SLACK_MS,
                "同步批量取在期限之后 {} ms 才返回",
                elapsed - SYNC_BATCH_TIMEOUT_MS as u128
            );
        }
        Err(other) => panic!("未就绪的同步批量取返回 {:?}，应为 RayError::Timeout", other),
    }

    // 睡眠任务占着唯一的 CPU，必须取消掉下一个任务才能跑。
    cancel_running(ray, &long_task, "睡眠任务");
}

/// `wait_async` 必须按引用本身区分就绪与未就绪，而不是按顺序。
async fn check_wait_async(ray: &Ray) {
    println!("\n--- 3. wait_async 处理混合批次 ---");
    let done = multiply_remote(ray, 6, 7);
    let done_value: i64 = done
        .get_timeout_async(Duration::from_secs(30))
        .await
        .expect("取已完成任务的值");
    assert_eq!(done_value, 42, "multiply(6, 7) 返回了 {}", done_value);
    let long_task = submit_long_task(ray);

    let done_ref: ObjectRef<i64> = done.clone();
    let sleep_ref: ObjectRef<i64> = long_task.clone().cast();
    let refs = vec![done_ref, sleep_ref];
    println!("  已完成：{}", refs[0].id_hex());
    println!("  睡眠：  {}", refs[1].id_hex());

    let start = Instant::now();
    let (ready, unready) = ray
        .wait_async(&refs, 1, WAIT_TIMEOUT)
        .await
        .expect("wait_async");
    let elapsed = start.elapsed();

    println!(
        "  wait_async(2 个引用, num_objects=1, {} ms) → ready {} 个, unready {} 个, 用时 {} ms",
        WAIT_TIMEOUT.as_millis(),
        ready.len(),
        unready.len(),
        elapsed.as_millis()
    );
    assert_eq!(ready.len(), 1, "ready 有 {} 个引用", ready.len());
    assert_eq!(unready.len(), 1, "unready 有 {} 个引用", unready.len());
    assert_eq!(
        ready[0].id(),
        refs[0].id(),
        "ready 里是 {}，应为已完成的 {}",
        ready[0].id_hex(),
        refs[0].id_hex()
    );
    assert_eq!(
        unready[0].id(),
        refs[1].id(),
        "unready 里是 {}，应为睡眠中的 {}",
        unready[0].id_hex(),
        refs[1].id_hex()
    );

    let values = ray
        .get_many_timeout_async(&[done], Duration::from_secs(30))
        .await
        .expect("取已完成的引用");
    println!("  get_many_timeout_async([已完成]) → {:?}", values);
    assert_eq!(values, vec![42], "已完成的任务返回了 {:?}", values);

    cancel_running(ray, &long_task, "睡眠任务");
}

/// 解码中途失败时，整组 buffer 与数组都必须释放。
async fn check_leak(ray: &Ray) {
    println!(
        "\n--- 4. {} 次解码失败的批量取（每批 {} 个引用）---",
        LEAK_ROUNDS, LEAK_BATCH
    );
    let good: ObjectRef<Vec<i64>> = ray.put(&vec![42i64]).expect("put the good element");
    let bad: ObjectRef<Vec<i64>> = multiply_remote(ray, 21, 2).cast();
    let mut refs: Vec<ObjectRef<Vec<i64>>> = (0..LEAK_BATCH).map(|_| good.clone()).collect();
    refs[1] = bad;
    println!("  第 1 个元素可解码，第 2 个元素是 i64 结果被 .cast() 成 Vec<i64>");

    let before = rss_bytes();
    println!("  循环前常驻内存：{} 字节", before);

    let start = Instant::now();
    for round in 0..LEAK_ROUNDS {
        match ray.get_many(&refs) {
            Err(RayError::Serialization(_)) => {}
            Err(other) => panic!("第 {} 轮返回 {:?}，应为 RayError::Serialization", round, other),
            Ok(values) => panic!("第 {} 轮把 i64 载荷解码成了 {} 个 Vec<i64>", round, values.len()),
        }
    }
    let elapsed = start.elapsed();

    let after = rss_bytes();
    let growth = after.saturating_sub(before);
    println!("  循环后常驻内存：{} 字节", after);
    println!("  增长：{} 字节", growth);
    println!(
        "  {} 轮共 {} ms（每轮 {} µs）",
        LEAK_ROUNDS,
        elapsed.as_millis(),
        elapsed.as_micros() / LEAK_ROUNDS as u128
    );
    assert!(
        growth < LEAK_MAX_GROWTH_BYTES,
        "常驻内存增长了 {} 字节，超过 {} 字节的上限",
        growth,
        LEAK_MAX_GROWTH_BYTES
    );
}

/// 空批次直接返回空结果。
async fn check_empty_batch(ray: &Ray) {
    println!("\n--- 5. 空批次 ---");
    let values = ray
        .get_many_async::<i64>(&[])
        .await
        .expect("空批次的 get_many_async");
    println!("  get_many_async::<i64>(&[]) → {} 个值", values.len());
    assert!(values.is_empty(), "空批次返回了 {} 个值", values.len());
}

/// 打印各入口对非法期限的返回值，并断言它被拒绝成 `RayError::Ffi`。
fn expect_rejected<T>(label: &str, timeout_ms: i32, result: Result<T, RayError>) {
    match result {
        Ok(_) => panic!("{} 接受了小于 -1 的期限", label),
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains(&format!("got {}", timeout_ms)),
                "{} 的消息没有带上入参 {}：{}",
                label,
                timeout_ms,
                msg
            );
            println!("  {} → RayError::Ffi: {}", label, msg);
        }
        Err(other) => panic!("{} 返回 {:?}，应为 RayError::Ffi", label, other),
    }
}

/// 小于 -1 的期限会让 Ray 内部的 `RAY_CHECK` 终止整个进程，三个入口都必须先拒绝。
fn check_negative_timeout(ray: &Ray) {
    println!("\n--- 6. 小于 -1 的期限被拒绝 ---");
    // 被测对象未就绪：没有校验时 Ray 的 RAY_CHECK 会在“有缺失元素”这条
    // 路上终止整个进程，这项检查因此同时覆盖“被拒绝”与“进程还活着”。
    let pending = submit_long_task(ray);
    let refs: Vec<ObjectRef<()>> = vec![pending.clone()];
    let value: ObjectRef<i64> = ray.put(&5i64).expect("put the checked element");

    expect_rejected(
        &format!("get_many_timeout(&refs, {})", INVALID_TIMEOUT_MS),
        INVALID_TIMEOUT_MS,
        ray.get_many_timeout(&refs, INVALID_TIMEOUT_MS),
    );
    expect_rejected(
        &format!("refs[0].get_timeout({})", INVALID_TIMEOUT_MS),
        INVALID_TIMEOUT_MS,
        refs[0].get_timeout(INVALID_TIMEOUT_MS),
    );
    expect_rejected(
        &format!("wait(&refs, 1, {})", INVALID_TIMEOUT_MS),
        INVALID_TIMEOUT_MS,
        ray.wait(&refs, 1, INVALID_TIMEOUT_MS),
    );

    let recovered: i64 = value
        .get_timeout(30_000)
        .expect("被拒绝之后的正常取");
    assert_eq!(recovered, 5, "被拒绝之后的正常取返回了 {}", recovered);
    println!("  被拒绝之后 value.get_timeout(30000) → {}", recovered);

    cancel_running(ray, &pending, "睡眠任务");
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

    println!("=== Ray Batch Async Get / Wait ===");
    println!("Address: {}", address);
    println!("Node IP: {}", node_ip);
    println!("Worker .so: {}", worker_so);

    let python_dir = python_module_dir();
    println!("Python module dir: {}", python_dir);
    let runtime_env = format!("{{\"env_vars\": {{\"PYTHONPATH\": \"{}\"}}}}", python_dir);

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

    check_clone_without_t_clone();
    check_batch_vs_single(&ray).await;
    check_batch_timeout(&ray).await;
    check_wait_async(&ray).await;
    check_leak(&ray).await;
    check_empty_batch(&ray).await;
    check_negative_timeout(&ray);

    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ All batch async checks passed");
}

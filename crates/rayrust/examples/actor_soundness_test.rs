//! Example: 回调异常/panic 回传通道与 Actor 状态互斥健全性端到端测试。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/actor_soundness_test
//! ./target/release/examples/actor_soundness_test local
//! ```
//! 也可以通过 `RAY_LOCAL=1` 选择本地模式。
//! 集群模式环境变量：`RAY_ADDRESS`、`RAY_NODE_IP`、`RAY_WORKER_SO`。

use std::time::Duration;

use rayrust::prelude::*;

const GET_TIMEOUT_MS: i32 = 15_000;

const TASK_ECHO: &str = "soundness_echo_i32";
const TASK_PANIC_SYNC: &str = "soundness_panic_sync";
const TASK_PANIC_UNIT: &str = "soundness_panic_unit";
const TASK_PANIC_ASYNC: &str = "soundness_panic_async";

const ACTOR_FACTORY: &str = "__rayrust_actor_factory_soundnessactor";
const ACTOR_SLEEP_ADD: &str = "__rayrust_actor_factory_soundnessactor::sleep_and_add";
const ACTOR_READ_STATE: &str = "__rayrust_actor_factory_soundnessactor::read_state";
const ACTOR_ADD_PID: &str = "__rayrust_actor_factory_soundnessactor::add_and_pid";
const ACTOR_GET_VALUE: &str = "__rayrust_actor_factory_soundnessactor::get_value";
const ACTOR_PANIC_METHOD: &str = "__rayrust_actor_factory_soundnessactor::panic_method";
const ACTOR_PANIC_UNIT_METHOD: &str = "__rayrust_actor_factory_soundnessactor::panic_unit_method";
const ACTOR_WORKER_PID: &str = "__rayrust_actor_factory_soundnessactor::worker_pid";

const PANIC_CTOR_FACTORY: &str = "__rayrust_actor_factory_panickingctoractor";
const PANIC_CTOR_PING: &str = "__rayrust_actor_factory_panickingctoractor::ping";

fn resolve_worker_so() -> String {
    let raw = std::env::var("RAY_WORKER_SO")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../target/release/librayrust_worker.so"
            )
            .to_string()
        });
    assert!(
        std::path::Path::new(&raw).exists(),
        "worker library not found at {raw}; run `cargo build --release --all-targets` first"
    );
    std::fs::canonicalize(&raw)
        .expect("canonicalize worker library path")
        .to_str()
        .expect("worker library path is valid UTF-8")
        .to_string()
}

fn dlopen_worker(worker_so: &str) {
    let c_path = std::ffi::CString::new(worker_so).expect("worker_so path has interior NUL");
    let handle = unsafe { libc::dlopen(c_path.as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL) };
    assert!(!handle.is_null(), "dlopen failed for {worker_so}");
}

/// 本地模式下 `TaskExecutor::Invoke` 捕获 `std::exception` 后调用
/// `PackError(e.what())`：msgpack `nil` (`0xc0`) + 2 元数组 (`0x92`) +
/// `TASK_EXECUTION_EXCEPTION` (`0x03`) + 报错文本。
fn assert_local_pack_error(label: &str, bytes: &[u8], expected_substrings: &[&str]) {
    assert!(
        bytes.starts_with(&[0xc0, 0x92, 0x03]),
        "{label}: expected PackError prefix [0xc0, 0x92, 0x03], got {:?}",
        &bytes[..bytes.len().min(16)]
    );
    let text = String::from_utf8_lossy(&bytes[3..]);
    for needle in expected_substrings {
        assert!(
            text.contains(needle),
            "{label}: error payload does not contain {needle:?}; payload={text:?}"
        );
    }
    println!("  {label} → PackError({text})");
}

async fn run_cluster(worker_so: String) {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let ray = Ray::connect(
        &RayConfig::new(&address)
            .node_ip(&node_ip)
            .code_search_path(vec![worker_so]),
    )
    .expect("Ray::connect cluster");
    println!("✓ Connected to Ray cluster ({address})");

    // 1. 同步 #[remote] 任务 panic! → Err(RayError::Task) 且工人继续服务
    println!("\n--- 1. Sync #[remote] task panic ---");
    let sync_msg = serialize(&"sync-task-panic-t43".to_string()).unwrap();
    let sync_ref = ray
        .task_call(TASK_PANIC_SYNC, &[&sync_msg], &[], &TaskOptions::new())
        .expect("submit sync panic task")
        .cast::<i64>();
    match sync_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("sync-task-panic-t43"), "got: {msg}");
            println!("  soundness_panic_sync → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }
    let unit_msg = serialize(&"unit-task-panic-t46".to_string()).unwrap();
    let unit_ref: ObjectRef<()> = ray
        .task_call(TASK_PANIC_UNIT, &[&unit_msg], &[], &TaskOptions::new())
        .expect("submit unit panic task");
    match unit_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("unit-task-panic-t46"), "got: {msg}");
            println!("  soundness_panic_unit → Task({msg})");
        }
        other => panic!("expected RayError::Task for unit panic task, got {other:?}"),
    }
    let echo_arg = serialize(&123i32).unwrap();
    let echo_val: i32 = ray
        .task_call(TASK_ECHO, &[&echo_arg], &[], &TaskOptions::new())
        .expect("submit echo after sync panic")
        .cast::<i32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("echo after sync panic");
    assert_eq!(echo_val, 123);
    println!("  subsequent echo(123) → {echo_val}");

    // 2. 异步 #[remote] async fn 在 .await 之后 panic!
    println!("\n--- 2. Async #[remote] task panic after .await ---");
    let async_msg = serialize(&"async-task-panic-t43".to_string()).unwrap();
    let async_ref = ray
        .task_call(TASK_PANIC_ASYNC, &[&async_msg], &[], &TaskOptions::new())
        .expect("submit async panic task")
        .cast::<i64>();
    match async_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("async-task-panic-t43"), "got: {msg}");
            println!("  soundness_panic_async → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }

    // 3. #[remote] 任务参数类型错误与参数个数错误
    println!("\n--- 3. #[remote] task bad arg type & count ---");
    let bad_str = serialize(&"not-an-i32".to_string()).unwrap();
    let bad_type_ref = ray
        .task_call(TASK_ECHO, &[&bad_str], &[], &TaskOptions::new())
        .expect("submit echo with wrong type")
        .cast::<i32>();
    match bad_type_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(
                msg.contains("argument 0 of soundness_echo_i32"),
                "got: {msg}"
            );
            println!("  wrong arg type → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }

    let bad_count_ref = ray
        .task_call(TASK_ECHO, &[], &[], &TaskOptions::new())
        .expect("submit echo with wrong arg count")
        .cast::<i32>();
    match bad_count_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("expected 1 argument(s), got 0"), "got: {msg}");
            println!("  wrong arg count → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }

    let mut trailing_i32 = serialize(&123i32).unwrap();
    trailing_i32.push(0x00);
    let trailing_task_ref = ray
        .task_call(TASK_ECHO, &[&trailing_i32], &[], &TaskOptions::new())
        .expect("submit echo with trailing arg byte")
        .cast::<i32>();
    match trailing_task_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(
                msg.contains("argument 0 of soundness_echo_i32") && msg.contains("trailing bytes"),
                "got: {msg}"
            );
            println!("  trailing arg byte → Task({msg})");
        }
        other => panic!("expected RayError::Task with trailing bytes, got {other:?}"),
    }

    let echo_after: i32 = ray
        .task_call(TASK_ECHO, &[&echo_arg], &[], &TaskOptions::new())
        .expect("submit echo after arg errors")
        .cast::<i32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("echo after arg errors");
    assert_eq!(echo_after, 123);
    println!("  worker alive after arg errors: echo(123) → {echo_after}");

    // 4. Actor 方法参数类型/个数错误：Worker 存活、PID 不变、Mutex 未中毒
    println!("\n--- 4. Actor method bad arg type & count ---");
    let start_arg = serialize(&10i64).unwrap();
    let actor_a = ray
        .actor_create(ACTOR_FACTORY, &[&start_arg], &ActorOptions::new())
        .expect("create SoundnessActor A");
    let five = serialize(&5i64).unwrap();
    let (v_before, pid_before): (i64, u32) = ray
        .actor_call(actor_a.id(), ACTOR_ADD_PID, &[&five], &[])
        .expect("add_and_pid before")
        .cast::<(i64, u32)>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get add_and_pid before");
    assert_eq!(v_before, 15);
    println!("  before bad calls: value={v_before}, pid={pid_before}");

    let bad_type = serialize(&"not-an-i64".to_string()).unwrap();
    match ray
        .actor_call(actor_a.id(), ACTOR_ADD_PID, &[&bad_type], &[])
        .expect("submit actor bad arg type")
        .cast::<(i64, u32)>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("argument 0"), "got: {msg}");
            println!("  actor wrong arg type → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }

    match ray
        .actor_call(actor_a.id(), ACTOR_ADD_PID, &[], &[])
        .expect("submit actor bad arg count")
        .cast::<(i64, u32)>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("expected 1 argument(s), got 0"), "got: {msg}");
            println!("  actor wrong arg count → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }

    let mut trailing_i64 = serialize(&5i64).unwrap();
    trailing_i64.push(0x00);
    match ray
        .actor_call(actor_a.id(), ACTOR_ADD_PID, &[&trailing_i64], &[])
        .expect("submit actor trailing arg byte")
        .cast::<(i64, u32)>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Task(msg)) => {
            assert!(
                msg.contains("argument 0") && msg.contains("trailing bytes"),
                "got: {msg}"
            );
            println!("  actor trailing arg byte → Task({msg})");
        }
        other => panic!("expected RayError::Task with trailing bytes, got {other:?}"),
    }

    let (v_after, pid_after): (i64, u32) = ray
        .actor_call(actor_a.id(), ACTOR_ADD_PID, &[&five], &[])
        .expect("add_and_pid after")
        .cast::<(i64, u32)>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get add_and_pid after");
    assert_eq!(v_after, 20);
    assert_eq!(pid_after, pid_before, "actor worker PID changed");
    let static_pid: u32 = ray
        .actor_call(actor_a.id(), ACTOR_WORKER_PID, &[], &[])
        .expect("worker_pid")
        .cast::<u32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get worker_pid");
    assert_eq!(static_pid, pid_before);
    println!("  after bad calls: value={v_after}, pid={pid_after} (same worker, mutex clean)");

    // 5. max_concurrency(4) 下并发调用 &mut self 与 &self，观测 peak_in_flight == 1
    println!("\n--- 5. max_concurrency(4) mutual exclusion ---");
    let conc_opts = ActorOptions::new().max_concurrency(4);
    let actor_b = ray
        .actor_create(ACTOR_FACTORY, &[&start_arg], &conc_opts)
        .expect("create SoundnessActor B with max_concurrency(4)");
    let one = serialize(&1i64).unwrap();
    let sleep_ms = serialize(&30u64).unwrap();
    let mut mut_refs = Vec::with_capacity(8);
    let mut ref_refs = Vec::with_capacity(4);
    for i in 0..8 {
        mut_refs.push(
            ray.actor_call(actor_b.id(), ACTOR_SLEEP_ADD, &[&one, &sleep_ms], &[])
                .expect("submit sleep_and_add")
                .cast::<(i64, i64)>(),
        );
        if i % 2 == 0 {
            ref_refs.push(
                ray.actor_call(actor_b.id(), ACTOR_READ_STATE, &[], &[])
                    .expect("submit read_state")
                    .cast::<(i64, i64)>(),
            );
        }
    }
    let mut max_peak = 0i64;
    for r in mut_refs {
        let (_val, peak) = r.get_timeout(GET_TIMEOUT_MS).expect("get sleep_and_add");
        max_peak = max_peak.max(peak);
    }
    for r in ref_refs {
        let (_val, peak) = r.get_timeout(GET_TIMEOUT_MS).expect("get read_state");
        max_peak = max_peak.max(peak);
    }
    let (final_val, final_peak): (i64, i64) = ray
        .actor_call(actor_b.id(), ACTOR_READ_STATE, &[], &[])
        .expect("final read_state")
        .cast::<(i64, i64)>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get final read_state");
    max_peak = max_peak.max(final_peak);
    assert_eq!(final_val, 18, "10 + 8 * 1 = 18");
    assert_eq!(max_peak, 1, "peak_in_flight exceeded 1: {max_peak}");
    let assoc_pid: u32 = ray
        .actor_call(actor_b.id(), ACTOR_WORKER_PID, &[], &[])
        .expect("associated worker_pid")
        .cast::<u32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get associated worker_pid");
    assert!(assoc_pid > 0);
    println!("  final_val={final_val}, peak_in_flight={max_peak}, worker_pid={assoc_pid}");

    // 6. Actor 实例方法 panic! → Err(RayError::Task)、后续 &self 报 poisoned、无 self 仍成功
    println!("\n--- 6. Actor method panic -> poisoned mutex -> associated fn survives ---");
    let panic_msg = serialize(&"actor-method-panic-t43".to_string()).unwrap();
    match ray
        .actor_call(actor_a.id(), ACTOR_PANIC_METHOD, &[&panic_msg], &[])
        .expect("submit panic_method")
        .cast::<i64>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("actor-method-panic-t43"), "got: {msg}");
            println!("  panic_method → Task({msg})");
        }
        other => panic!("expected RayError::Task, got {other:?}"),
    }

    match ray
        .actor_call(actor_a.id(), ACTOR_GET_VALUE, &[], &[])
        .expect("submit get_value on poisoned actor")
        .cast::<i64>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("poisoned"), "got: {msg}");
            println!("  get_value after panic → Task({msg})");
        }
        other => panic!("expected RayError::Task with poisoned, got {other:?}"),
    }

    let pid_after_panic: u32 = ray
        .actor_call(actor_a.id(), ACTOR_WORKER_PID, &[], &[])
        .expect("submit worker_pid after panic")
        .cast::<u32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get worker_pid after panic");
    assert_eq!(pid_after_panic, pid_before);
    println!("  worker_pid after panic → {pid_after_panic} (same worker alive)");

    let actor_unit = ray
        .actor_create(ACTOR_FACTORY, &[&start_arg], &ActorOptions::new())
        .expect("create SoundnessActor for unit panic method");
    let unit_actor_msg = serialize(&"actor-unit-method-panic-t46".to_string()).unwrap();
    let unit_actor_ref: ObjectRef<()> = ray
        .actor_call(
            actor_unit.id(),
            ACTOR_PANIC_UNIT_METHOD,
            &[&unit_actor_msg],
            &[],
        )
        .expect("submit panic_unit_method");
    match unit_actor_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Task(msg)) => {
            assert!(msg.contains("actor-unit-method-panic-t46"), "got: {msg}");
            println!("  panic_unit_method → Task({msg})");
        }
        other => panic!("expected RayError::Task for panic_unit_method, got {other:?}"),
    }

    // 7. Actor 构造函数 new() panic 与构造函数参数类型错误
    println!("\n--- 7. Actor constructor panic & bad constructor arg ---");
    let ctor_panic_arg = serialize(&"ctor-panic-t43".to_string()).unwrap();
    let ctor_panic_actor = ray
        .actor_create(PANIC_CTOR_FACTORY, &[&ctor_panic_arg], &ActorOptions::new())
        .expect("actor_create returns Ok(handle) even if ctor panics");
    match ray
        .actor_call(ctor_panic_actor.id(), PANIC_CTOR_PING, &[], &[])
        .expect("submit ping on ctor-panic actor")
        .cast::<i64>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Actor(msg)) => {
            assert!(
                msg.contains("USER_ERROR") && msg.contains("initialization method"),
                "expected USER_ERROR initialization failure, got: {msg}"
            );
            println!("  ctor panic first call → Actor({msg})");
        }
        other => panic!("expected RayError::Actor, got {other:?}"),
    }

    let bad_ctor_actor = ray
        .actor_create(ACTOR_FACTORY, &[&bad_type], &ActorOptions::new())
        .expect("actor_create returns Ok(handle) even if ctor args fail to decode");
    match ray
        .actor_call(bad_ctor_actor.id(), ACTOR_GET_VALUE, &[], &[])
        .expect("submit get_value on bad-ctor-arg actor")
        .cast::<i64>()
        .get_timeout(GET_TIMEOUT_MS)
    {
        Err(RayError::Actor(msg)) => {
            assert!(
                msg.contains("USER_ERROR") && msg.contains("initialization method"),
                "expected USER_ERROR initialization failure, got: {msg}"
            );
            println!("  ctor bad arg first call → Actor({msg})");
        }
        other => panic!("expected RayError::Actor, got {other:?}"),
    }

    // 8. 清理存活的测试 actor
    println!("\n--- 8. Cleanup ---");
    ray.kill_actor(&actor_a, true).expect("kill actor_a");
    ray.kill_actor(&actor_b, true).expect("kill actor_b");
    ray.kill_actor(&actor_unit, true).expect("kill actor_unit");
    println!("  killed surviving test actors");
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(ray);
}

fn run_local(worker_so: String) {
    dlopen_worker(&worker_so);
    let ray = Ray::connect(&RayConfig::local().code_search_path(vec![worker_so]))
        .expect("Ray::connect local");
    println!("✓ Connected to Ray (local mode)");

    // 1. 同步任务 panic、异步任务 panic、任务参数类型/个数错误
    println!("\n--- 1. Local mode tasks: panic & bad args ---");
    let sync_msg = serialize(&"local-sync-panic-t43".to_string()).unwrap();
    let sync_ref = ray
        .task_call(TASK_PANIC_SYNC, &[&sync_msg], &[], &TaskOptions::new())
        .expect("submit local sync panic")
        .cast::<i64>();
    let sync_raw = sync_ref.get_raw_bytes().expect("raw bytes for sync panic");
    assert_local_pack_error("local sync panic", &sync_raw, &["local-sync-panic-t43"]);

    let async_msg = serialize(&"local-async-panic-t43".to_string()).unwrap();
    let async_ref = ray
        .task_call(TASK_PANIC_ASYNC, &[&async_msg], &[], &TaskOptions::new())
        .expect("submit local async panic")
        .cast::<i64>();
    let async_raw = async_ref
        .get_raw_bytes()
        .expect("raw bytes for async panic");
    assert_local_pack_error("local async panic", &async_raw, &["local-async-panic-t43"]);

    let bad_str = serialize(&"not-an-i32".to_string()).unwrap();
    let bad_type_raw = ray
        .task_call(TASK_ECHO, &[&bad_str], &[], &TaskOptions::new())
        .expect("submit local task bad arg type")
        .get_raw_bytes()
        .expect("raw bytes for task bad arg type");
    assert_local_pack_error(
        "local task bad arg type",
        &bad_type_raw,
        &["argument 0 of soundness_echo_i32"],
    );

    let bad_count_raw = ray
        .task_call(TASK_ECHO, &[], &[], &TaskOptions::new())
        .expect("submit local task bad arg count")
        .get_raw_bytes()
        .expect("raw bytes for task bad arg count");
    assert_local_pack_error(
        "local task bad arg count",
        &bad_count_raw,
        &["expected 1 argument(s), got 0"],
    );

    let mut local_trailing_i32 = serialize(&123i32).unwrap();
    local_trailing_i32.push(0x00);
    let bad_trailing_raw = ray
        .task_call(TASK_ECHO, &[&local_trailing_i32], &[], &TaskOptions::new())
        .expect("submit local task trailing arg byte")
        .get_raw_bytes()
        .expect("raw bytes for local task trailing arg byte");
    assert_local_pack_error(
        "local task trailing arg byte",
        &bad_trailing_raw,
        &["argument 0 of soundness_echo_i32", "trailing bytes"],
    );

    let ok_arg = serialize(&456i32).unwrap();
    let ok_val: i32 = ray
        .task_call(TASK_ECHO, &[&ok_arg], &[], &TaskOptions::new())
        .expect("submit local echo")
        .cast::<i32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("local echo");
    assert_eq!(ok_val, 456);
    println!("  local subsequent echo(456) → {ok_val}");

    // 2. 8 个 OS 线程并发调用同一个本地 Actor 的睡眠 &mut self 方法
    println!("\n--- 2. Local mode 8-thread concurrent actor access ---");
    let start_arg = serialize(&100i64).unwrap();
    let local_actor = ray
        .actor_create(ACTOR_FACTORY, &[&start_arg], &ActorOptions::new())
        .expect("create local SoundnessActor");
    let one = serialize(&1i64).unwrap();
    let sleep_ms = serialize(&20u64).unwrap();
    let peak_observed = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(8);
        for _ in 0..8 {
            let r = &ray;
            let h = &local_actor;
            let one_ref = &one;
            let sleep_ref = &sleep_ms;
            handles.push(s.spawn(move || {
                let obj = r
                    .actor_call(h.id(), ACTOR_SLEEP_ADD, &[one_ref, sleep_ref], &[])
                    .expect("local sleep_and_add")
                    .cast::<(i64, i64)>();
                obj.get_timeout(GET_TIMEOUT_MS)
                    .expect("get local sleep_and_add")
            }));
        }
        let mut max_peak = 0i64;
        for h in handles {
            let (_val, peak) = h.join().expect("thread join");
            max_peak = max_peak.max(peak);
        }
        max_peak
    });
    let final_local_val: i64 = ray
        .actor_call(local_actor.id(), ACTOR_GET_VALUE, &[], &[])
        .expect("local get_value")
        .cast::<i64>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get local get_value");
    assert_eq!(final_local_val, 108);
    assert_eq!(peak_observed, 1, "local peak_in_flight={peak_observed}");
    println!("  local final_val={final_local_val}, peak_in_flight={peak_observed}");

    // 3. 本地 Actor 方法参数类型/个数错误：未污染 Mutex，随后正常方法成功累加
    println!("\n--- 3. Local mode actor bad arg type & count ---");
    let bad_type = serialize(&"not-an-i64".to_string()).unwrap();
    let actor_bad_type_raw = ray
        .actor_call(local_actor.id(), ACTOR_ADD_PID, &[&bad_type], &[])
        .expect("submit local actor bad arg type")
        .get_raw_bytes()
        .expect("raw bytes for local actor bad arg type");
    assert_local_pack_error(
        "local actor bad arg type",
        &actor_bad_type_raw,
        &["argument 0"],
    );

    let actor_bad_count_raw = ray
        .actor_call(local_actor.id(), ACTOR_ADD_PID, &[], &[])
        .expect("submit local actor bad arg count")
        .get_raw_bytes()
        .expect("raw bytes for local actor bad arg count");
    assert_local_pack_error(
        "local actor bad arg count",
        &actor_bad_count_raw,
        &["expected 1 argument(s), got 0"],
    );

    let mut local_trailing_i64 = serialize(&5i64).unwrap();
    local_trailing_i64.push(0x00);
    let actor_trailing_raw = ray
        .actor_call(local_actor.id(), ACTOR_ADD_PID, &[&local_trailing_i64], &[])
        .expect("submit local actor trailing arg byte")
        .get_raw_bytes()
        .expect("raw bytes for local actor trailing arg byte");
    assert_local_pack_error(
        "local actor trailing arg byte",
        &actor_trailing_raw,
        &["argument 0", "trailing bytes"],
    );

    let five = serialize(&5i64).unwrap();
    let (after_bad_val, _pid): (i64, u32) = ray
        .actor_call(local_actor.id(), ACTOR_ADD_PID, &[&five], &[])
        .expect("local add_and_pid after arg errors")
        .cast::<(i64, u32)>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get local add_and_pid");
    assert_eq!(after_bad_val, 113);
    println!("  local actor after arg errors: value={after_bad_val}");

    // 4. 本地 Actor 方法体 panic：进程存活、后续 &self 报 poisoned、无 self 仍正常返回
    println!("\n--- 4. Local mode actor method panic & poison ---");
    let actor_panic_msg = serialize(&"local-actor-panic-t43".to_string()).unwrap();
    let actor_panic_raw = ray
        .actor_call(
            local_actor.id(),
            ACTOR_PANIC_METHOD,
            &[&actor_panic_msg],
            &[],
        )
        .expect("submit local actor panic_method")
        .get_raw_bytes()
        .expect("raw bytes for local actor panic_method");
    assert_local_pack_error(
        "local actor panic_method",
        &actor_panic_raw,
        &["local-actor-panic-t43"],
    );

    let poisoned_raw = ray
        .actor_call(local_actor.id(), ACTOR_GET_VALUE, &[], &[])
        .expect("submit local get_value after panic")
        .get_raw_bytes()
        .expect("raw bytes for local get_value after panic");
    assert_local_pack_error(
        "local actor get_value after panic",
        &poisoned_raw,
        &["poisoned"],
    );

    let local_assoc_pid: u32 = ray
        .actor_call(local_actor.id(), ACTOR_WORKER_PID, &[], &[])
        .expect("submit local worker_pid after panic")
        .cast::<u32>()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("get local worker_pid");
    assert_eq!(local_assoc_pid, std::process::id());
    println!("  local associated worker_pid after panic → {local_assoc_pid}");

    // 5. 本地 Actor 构造函数 panic：actor_create 返回 Ok(handle)，后续 actor_call 返回 Err(RayError::Ffi)
    println!("\n--- 5. Local mode actor constructor panic ---");
    let ctor_panic_arg = serialize(&"local-ctor-panic-t43".to_string()).unwrap();
    let bad_local_actor = ray
        .actor_create(PANIC_CTOR_FACTORY, &[&ctor_panic_arg], &ActorOptions::new())
        .expect("local actor_create returns Ok(handle) even when ctor panics");
    match ray.actor_call(bad_local_actor.id(), PANIC_CTOR_PING, &[], &[]) {
        Err(RayError::Ffi(msg)) => {
            println!("  local actor_call after ctor panic → Ffi({msg})");
        }
        other => panic!("expected RayError::Ffi, got {other:?}"),
    }

    // 6. 本地模式下返回 () 的失败任务、未注册函数与失败 Actor 方法均拒绝解码为 Ok(()) 或 Ok(Nil)
    println!("\n--- 6. Local mode unit-returning task & actor failures rejected on decode ---");
    let unit_panic_msg = serialize(&"local-unit-task-panic-t46".to_string()).unwrap();
    let unit_panic_ref: ObjectRef<()> = ray
        .task_call(
            TASK_PANIC_UNIT,
            &[&unit_panic_msg],
            &[],
            &TaskOptions::new(),
        )
        .expect("submit local soundness_panic_unit");
    let unit_panic_raw = unit_panic_ref
        .get_raw_bytes()
        .expect("raw bytes for local soundness_panic_unit");
    assert_local_pack_error(
        "local soundness_panic_unit",
        &unit_panic_raw,
        &["local-unit-task-panic-t46"],
    );
    match unit_panic_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Serialization(msg)) => {
            assert!(msg.contains("trailing bytes"), "got: {msg}");
            println!("  soundness_panic_unit get_timeout::<()>() → Serialization({msg})");
        }
        other => panic!("expected RayError::Serialization with trailing bytes, got {other:?}"),
    }
    match unit_panic_ref.get_value() {
        Err(RayError::Serialization(msg)) => {
            assert!(msg.contains("trailing bytes"), "got: {msg}");
            println!("  soundness_panic_unit get_value() → Serialization({msg})");
        }
        other => panic!("expected RayError::Serialization with trailing bytes, got {other:?}"),
    }

    let unreg_ref: ObjectRef<()> = ray
        .task_call("unregistered_local_unit_fn", &[], &[], &TaskOptions::new())
        .expect("submit unregistered local unit task");
    let unreg_raw = unreg_ref
        .get_raw_bytes()
        .expect("raw bytes for unregistered local unit task");
    assert_local_pack_error(
        "local unregistered fn",
        &unreg_raw,
        &["unknown function: unregistered_local_unit_fn"],
    );
    match unreg_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Serialization(msg)) => {
            assert!(msg.contains("trailing bytes"), "got: {msg}");
            println!("  unregistered_local_unit_fn get_timeout::<()>() → Serialization({msg})");
        }
        other => panic!("expected RayError::Serialization with trailing bytes, got {other:?}"),
    }
    match unreg_ref.get_value() {
        Err(RayError::Serialization(msg)) => {
            assert!(msg.contains("trailing bytes"), "got: {msg}");
            println!("  unregistered_local_unit_fn get_value() → Serialization({msg})");
        }
        other => panic!("expected RayError::Serialization with trailing bytes, got {other:?}"),
    }

    let unit_local_actor = ray
        .actor_create(ACTOR_FACTORY, &[&start_arg], &ActorOptions::new())
        .expect("create separate local SoundnessActor for panic_unit_method");
    let unit_actor_msg = serialize(&"local-unit-actor-panic-t46".to_string()).unwrap();
    let unit_actor_ref: ObjectRef<()> = ray
        .actor_call(
            unit_local_actor.id(),
            ACTOR_PANIC_UNIT_METHOD,
            &[&unit_actor_msg],
            &[],
        )
        .expect("submit local panic_unit_method");
    let unit_actor_raw = unit_actor_ref
        .get_raw_bytes()
        .expect("raw bytes for local panic_unit_method");
    assert_local_pack_error(
        "local actor panic_unit_method",
        &unit_actor_raw,
        &["local-unit-actor-panic-t46"],
    );
    match unit_actor_ref.get_timeout(GET_TIMEOUT_MS) {
        Err(RayError::Serialization(msg)) => {
            assert!(msg.contains("trailing bytes"), "got: {msg}");
            println!("  panic_unit_method get_timeout::<()>() → Serialization({msg})");
        }
        other => panic!("expected RayError::Serialization with trailing bytes, got {other:?}"),
    }
    match unit_actor_ref.get_value() {
        Err(RayError::Serialization(msg)) => {
            assert!(msg.contains("trailing bytes"), "got: {msg}");
            println!("  panic_unit_method get_value() → Serialization({msg})");
        }
        other => panic!("expected RayError::Serialization with trailing bytes, got {other:?}"),
    }

    drop(ray);
}

#[tokio::main]
async fn main() {
    let arg_local = std::env::args().nth(1).as_deref() == Some("local");
    let env_local = std::env::var("RAY_LOCAL").ok().as_deref() == Some("1");
    let is_local = arg_local || env_local;
    let worker_so = resolve_worker_so();

    println!(
        "=== Actor Soundness & Callback Error Channel Test ({}) ===",
        if is_local { "local" } else { "cluster" }
    );
    if is_local {
        run_local(worker_so);
    } else {
        run_cluster(worker_so).await;
    }
    println!("\n✓ All actor soundness checks passed");
}

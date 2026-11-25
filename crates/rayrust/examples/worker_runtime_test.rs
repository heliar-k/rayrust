//! 集群模式示例：验证工人进程（`ray/cpp/default_worker`）内 `#[remote]` 函数体调用本绑定的
//! 四类 Ray 入口（读取驱动对象、工人侧写入、嵌套任务提交、actor 方法调用）以及驱动进程
//! 自身的 `put`/`get` 均正常工作。

use rayrust::prelude::*;

const TIMEOUT_MS: i32 = 15_000;
const OBJECT_ID_LEN: usize = 28;
const COUNTER_FACTORY: &str = "__rayrust_actor_factory_counter";
const COUNTER_GET: &str = "__rayrust_actor_factory_counter::get";

fn main() {
    let address =
        std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| {
        eprintln!("ERROR: RAY_WORKER_SO not set. Build the worker first:");
        eprintln!("  cargo build --release -p rayrust-example-worker");
        eprintln!("Then set RAY_WORKER_SO to the path of librayrust_worker.so");
        std::process::exit(1);
    });

    println!("=== Worker Runtime Test ===");
    println!("Address: {}", address);
    println!("Node IP: {}", node_ip);
    println!("Worker .so: {}", worker_so);

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so]);
    let ray = Ray::connect(&config).expect("Ray::connect");

    // 前置对照：驱动自身的 put/get
    println!("\n--- 0. Driver baseline put/get ---");
    let driver_obj = ray.put(&42i64).expect("driver put(42)");
    let driver_val = driver_obj.get_timeout(TIMEOUT_MS).expect("driver get(42)");
    println!("  driver put/get -> {}", driver_val);
    assert_eq!(driver_val, 42, "driver get returned unexpected value");

    // 创建 Rust Counter actor（起始值 10）
    let start_arg = serialize(&10i64).expect("serialize(10)");
    let counter = ray
        .actor_create(COUNTER_FACTORY, &[&start_arg], &ActorOptions::new())
        .expect("create Counter actor");
    let initial_count: i64 = ray
        .actor_call(counter.id(), COUNTER_GET, &[], &[])
        .expect("submit Counter::get")
        .cast::<i64>()
        .get_timeout(TIMEOUT_MS)
        .expect("read initial Counter::get");
    println!("  Counter initial value -> {}", initial_count);
    assert_eq!(initial_count, 10, "Counter initial value mismatch");
    let serialized_counter = counter.serialize().expect("serialize Counter handle");

    // 1. 工人侧读取驱动写入的对象
    println!("\n--- 1. read_driver_object ---");
    let id_arg = serialize(&driver_obj.id().to_vec()).expect("serialize driver object id");
    let read_res: Result<i64, String> = ray
        .task_call(
            "read_driver_object",
            &[driver_obj.id(), &id_arg],
            &[true, false],
            &TaskOptions::new(),
        )
        .expect("submit read_driver_object")
        .cast::<Result<i64, String>>()
        .get_timeout(TIMEOUT_MS)
        .expect("get read_driver_object");
    println!("  read_driver_object -> {:?}", read_res);

    // 2. 工人侧调用 ray_put 与 ray_put_xlang 写入对象并读回
    println!("\n--- 2. put_in_worker & put_xlang_in_worker ---");
    let put_arg = serialize(&99i64).expect("serialize(99)");
    let put_res: Result<(Vec<u8>, i64), String> = ray
        .task_call("put_in_worker", &[&put_arg], &[], &TaskOptions::new())
        .expect("submit put_in_worker")
        .cast::<Result<(Vec<u8>, i64), String>>()
        .get_timeout(TIMEOUT_MS)
        .expect("get put_in_worker");
    println!("  put_in_worker -> {:?}", put_res);

    let put_xlang_arg = serialize(&123i64).expect("serialize(123)");
    let put_xlang_res: Result<(Vec<u8>, i64), String> = ray
        .task_call(
            "put_xlang_in_worker",
            &[&put_xlang_arg],
            &[],
            &TaskOptions::new(),
        )
        .expect("submit put_xlang_in_worker")
        .cast::<Result<(Vec<u8>, i64), String>>()
        .get_timeout(TIMEOUT_MS)
        .expect("get put_xlang_in_worker");
    println!("  put_xlang_in_worker -> {:?}", put_xlang_res);

    // 3. 工人侧嵌套提交 multiply(6, 7) 并读回
    println!("\n--- 3. submit_nested ---");
    let nested_arg = serialize(&6i64).expect("serialize(6)");
    let nested_res: Result<(Vec<u8>, i64), String> = ray
        .task_call("submit_nested", &[&nested_arg], &[], &TaskOptions::new())
        .expect("submit submit_nested")
        .cast::<Result<(Vec<u8>, i64), String>>()
        .get_timeout(TIMEOUT_MS)
        .expect("get submit_nested");
    println!("  submit_nested -> {:?}", nested_res);

    // 4. 工人侧调用 Counter::increment(5) 并读回
    println!("\n--- 4. call_actor_in_worker ---");
    let ser_arg = serialize(&serialized_counter).expect("serialize Counter handle bytes");
    let inc_arg = serialize(&5i64).expect("serialize(5)");
    let actor_res: Result<(Vec<u8>, i64), String> = ray
        .task_call(
            "call_actor_in_worker",
            &[&ser_arg, &inc_arg],
            &[],
            &TaskOptions::new(),
        )
        .expect("submit call_actor_in_worker")
        .cast::<Result<(Vec<u8>, i64), String>>()
        .get_timeout(TIMEOUT_MS)
        .expect("get call_actor_in_worker");
    println!("  call_actor_in_worker -> {:?}", actor_res);

    // 逐项断言
    assert!(
        read_res == Ok(42),
        "read_driver_object failed: {:?}",
        read_res
    );

    let (put_id, put_val) = match put_res {
        Ok(pair) => pair,
        Err(err) => panic!("put_in_worker failed: {}", err),
    };
    assert_eq!(put_id.len(), OBJECT_ID_LEN, "put_in_worker id length");
    assert_eq!(put_val, 99, "put_in_worker value mismatch");

    let (put_xlang_id, put_xlang_val) = match put_xlang_res {
        Ok(pair) => pair,
        Err(err) => panic!("put_xlang_in_worker failed: {}", err),
    };
    assert_eq!(
        put_xlang_id.len(),
        OBJECT_ID_LEN,
        "put_xlang_in_worker id length"
    );
    assert_eq!(put_xlang_val, 123, "put_xlang_in_worker value mismatch");

    let (nested_id, nested_val) = match nested_res {
        Ok(pair) => pair,
        Err(err) => panic!("submit_nested failed: {}", err),
    };
    assert_eq!(nested_id.len(), OBJECT_ID_LEN, "submit_nested id length");
    assert_eq!(nested_val, 42, "submit_nested value mismatch");

    let (actor_call_id, actor_val) = match actor_res {
        Ok(pair) => pair,
        Err(err) => panic!("call_actor_in_worker failed: {}", err),
    };
    assert_eq!(
        actor_call_id.len(),
        OBJECT_ID_LEN,
        "call_actor_in_worker id length"
    );
    assert_eq!(actor_val, 15, "call_actor_in_worker value mismatch");

    // 驱动侧核对 Counter 状态已变为 15，并校验驱动自身的 put/get 仍正常
    println!("\n--- 5. Driver post-check ---");
    let final_count: i64 = ray
        .actor_call(counter.id(), COUNTER_GET, &[], &[])
        .expect("submit final Counter::get")
        .cast::<i64>()
        .get_timeout(TIMEOUT_MS)
        .expect("read final Counter::get");
    println!("  Counter final value -> {}", final_count);
    assert_eq!(final_count, 15, "Counter final value mismatch");
    ray.kill_actor(&counter, true).expect("kill Counter actor");

    let after_obj = ray.put(&77i64).expect("driver post put(77)");
    let after_val = after_obj
        .get_timeout(TIMEOUT_MS)
        .expect("driver post get(77)");
    println!("  driver post put/get -> {}", after_val);
    assert_eq!(after_val, 77, "driver post get returned unexpected value");

    drop(ray);
    println!("\nPASS: worker_runtime_test all checks passed");
}

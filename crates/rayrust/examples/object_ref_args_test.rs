//! Example: `ObjectRef` 作为任务与 actor 方法的参数。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/object_ref_args_test cluster
//! ./target/release/examples/object_ref_args_test local
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! 检查项，每一项都打印实际结果：
//! 1. 值参数与引用参数混在一起：加法对照，以及非交换的 `sub(10, &ref(3))` 与
//!    `sub(&ref(10), 3)` 都是 7（参数被挪位时结果会错）
//! 2. 两步任务图：`multiply(6, 7)` 的结果直接当 `add` 的参数，驱动中途不 `get`，
//!    最终 43
//! 3. 扇出：两个任务消费同一个引用，两个结果都对
//! 4. 异步调用带引用参数；再补一条「future 先建、引用先 drop、等一会儿再轮询」
//! 5. actor 方法带引用参数，同步与异步各一次；异步同样补「先建 future、先 drop」
//! 6. （集群）Python 任务与 Python actor 的引用参数：驱动 `put_xlang` 写入的对象携带
//!    `XLANG` metadata，与 Python 自己产生的对象均可按引用传给 Python 任务与 actor；
//!    驱动 `put` 写入的对象无 metadata，按引用传给 Python 时返回 `RayError::Task`；
//!    Python actor 的 handle 也要能 `serialize` → `from_serialized` → 调用
//! 7. 提交之后立刻 drop 参数引用，任务仍读到值
//! 8. （集群）任务返回的 id 不可读：驱动自己持有引用时交回的 id 能读；actor 里
//!    put 并保活的对象，驱动按同一个 id 读也读不到（对象活着也一样）
//! 9. 非法或类型不匹配的引用参数：保存字节重建的、自己拼的 28 字节、27 字节都返回
//!    `Err`，进程继续运行且不阻塞；裸通道的 `is_ref` 长度对不上时拒绝；集群模式下
//!    把 `put_xlang` 句柄传给 Rust 宏调用器在提交前返回 `Err(RayError::Serialization)`
//! 10. 异步入口的错误路径：27 字节 id 的 future 返回 `Err(RayError::Ffi)`
//! 11. id 宽度：3 字节与 0 字节的 id 在进 Ray 之前被拒，进程不终止。单个读取、
//!    `get_many_timeout`、`wait`、任务的引用参数（含 0 字节那个修复前永久挂起的
//!    情形）、Python 任务的引用参数、`actor_call` 的短 actor id 各一条；本地模式
//!    再加一条 `actor_call_python` 的短 id，断言报告的是宽度错误，顺序与 `cancel`
//!    的守卫一致。最后 `put` + `get` 照常读到值，证明这一串拒绝没有打坏运行时。
//!
//! 本地模式补充说明（不是一项检查）：第 1、2 项在本地模式下由
//! `build_task_args_cpp` 在提交前用 `Runtime()->Get(id)` 取成值。

use std::time::{Duration, Instant};

use rayrust::prelude::*;

/// 读一个对象时的期限（毫秒）。
const PROBE_TIMEOUT_MS: i32 = 2000;

/// 「future 先建、引用先 drop」之间的间隔（毫秒），让窗口足够宽。
const KEEP_ALIVE_GAP_MS: u64 = 800;

/// Python 测试模块；worker 必须能 import 到它。
const PYTHON_MODULE: &str = "rayrust_test";

/// 工人侧的任务，与 `crates/rayrust-example-worker` 的 `add` 对应。
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// 非交换的任务：参数位置被挪动时结果就不对。
#[rayrust::remote]
fn sub(a: i32, b: i32) -> i32 {
    a - b
}

/// 工人侧的任务，与 `crates/rayrust-example-worker` 的 `multiply` 对应。
#[rayrust::remote]
fn multiply(a: i64, b: i64) -> i64 {
    a * b
}

/// 与工人侧的 `Counter` 同名同方法签名，本地模式下由这个进程执行。
struct Counter {
    value: i64,
}

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn get(&self) -> i64 {
        self.value
    }
}

/// PYTHON_MODULE 所在目录，通过 runtime_env 交给 worker。
fn python_module_dir() -> String {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/python");
    let path = std::fs::canonicalize(dir).expect("canonicalize the Python test module directory");
    path.to_str()
        .expect("the Python module path is valid UTF-8")
        .to_string()
}

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    let runtime_env = format!(
        r#"{{"env_vars": {{"PYTHONPATH": "{}"}}}}"#,
        python_module_dir()
    );
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
        .runtime_env(runtime_env)
}

/// 1. 值参数与引用参数混合，两个方向各一次；非交换任务钉住参数位置。
async fn mixed_value_and_ref(ray: &Ray) {
    println!("\n--- 1. 值参数与引用参数混合 ---");
    let ten = ray.put(&10i32).expect("put(10)");

    let sum = add_remote(ray, 32, &ten)
        .expect("submit add(32, &ref(10))")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read add(32, &ref(10))");
    assert_eq!(sum, 42, "add(32, &ref(10)) 返回了 {}", sum);
    println!("  add(32, &ref(10)) → {}", sum);

    let sum = add_remote(ray, &ten, 32)
        .expect("submit add(&ref(10), 32)")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read add(&ref(10), 32)");
    assert_eq!(sum, 42, "add(&ref(10), 32) 返回了 {}", sum);
    println!("  add(&ref(10), 32) → {}（加法对照，交换律）", sum);

    // 引用放在第一个参数位置的两种写法：位置被挪动时 sub 的结果会变成 -7。
    let three = ray.put(&3i32).expect("put(3)");
    let value = sub_remote(ray, 10, &three)
        .expect("submit sub(10, &ref(3))")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read sub(10, &ref(3))");
    assert_eq!(value, 7, "sub(10, &ref(3)) 返回了 {}", value);
    println!("  sub(10, &ref(3)) → {}", value);

    let value = sub_remote(ray, &ten, 3)
        .expect("submit sub(&ref(10), 3)")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read sub(&ref(10), 3)");
    assert_eq!(value, 7, "sub(&ref(10), 3) 返回了 {}", value);
    println!("  sub(&ref(10), 3) → {}（引用在前，参数位置不交换）", value);
}

/// 2. 两步任务图：中间结果只以引用的形式出现在第二个任务的参数里。
async fn two_step_graph(ray: &Ray) {
    println!("\n--- 2. 两步任务图（驱动中途不 get）---");
    let product = multiply_remote(ray, 6, 7).expect("submit multiply(6, 7)");
    // add 的参数是 i32，multiply 的结果是 i64：只换幻影类型，引用仍是同一个 id。
    let product: ObjectRef<i32> = product.cast();
    let sum = add_remote(ray, &product, 1)
        .expect("submit add(&multiply(6, 7), 1)")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the second task");
    assert_eq!(sum, 43, "两步任务图返回了 {}", sum);
    println!("  multiply(6, 7) → add(&乘积, 1) = {}", sum);
}

/// 3. 扇出：两个任务消费同一个引用。
async fn fan_out(ray: &Ray) {
    println!("\n--- 3. 扇出：两个任务消费同一个引用 ---");
    let seven = ray.put(&7i32).expect("put(7)");
    let first = add_remote(ray, &seven, 100)
        .expect("submit add(&ref(7), 100)")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read add(&ref(7), 100)");
    let second = add_remote(ray, &seven, 200)
        .expect("submit add(&ref(7), 200)")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read add(&ref(7), 200)");
    assert_eq!(first, 107, "add(&ref(7), 100) 返回了 {}", first);
    assert_eq!(second, 207, "add(&ref(7), 200) 返回了 {}", second);
    println!(
        "  add(&ref(7), 100) = {}，add(&ref(7), 200) = {}",
        first, second
    );
}

/// 4. 异步调用带引用参数；包括「future 先建、引用先 drop、再轮询」。
async fn async_call_with_ref(ray: &Ray, cluster: bool) {
    println!("\n--- 4. 异步调用带引用参数 ---");
    let seven = ray.put(&7i32).expect("put(7)");
    let sum = add_remote_async(ray, &seven, 35)
        .await
        .expect("submit the async call")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the async call result");
    assert_eq!(sum, 42, "异步调用返回了 {}", sum);
    println!("  add_remote_async(&ref(7), 35).await → {}", sum);

    // future 是 'static 的，提交发生在第一次轮询；调用器自己保活到提交返回，
    // 所以「先 drop 调用方的句柄、再轮询」是合法写法。
    let five = ray.put(&5i32).expect("put(5)");
    let fut = add_remote_async(ray, &five, 34);
    drop(five);
    tokio::time::sleep(Duration::from_millis(KEEP_ALIVE_GAP_MS)).await;
    let sum = fut
        .await
        .expect("submit after dropping the caller's handle")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read after dropping the caller's handle");
    assert_eq!(sum, 39, "future 先建、引用先 drop 之后读到 {}", sum);
    println!(
        "  future 先建、引用先 drop、等 {}ms 再轮询 → {}{}",
        KEEP_ALIVE_GAP_MS,
        sum,
        if cluster {
            ""
        } else {
            "（本地模式不参与引用计数，这一条在集群模式下才有区分力）"
        }
    );
}

/// 5. actor 方法带引用参数，同步与异步各一次。
///
/// 本地模式不 kill actor：`KillActor` 直接找 core worker
/// （`abstract_ray_runtime.cc:265-272`），而本地模式不初始化 core worker，进程会在
/// 那里终止。本地模式下的 actor 随进程退出而消失。
async fn actor_with_ref(ray: &Ray, cluster: bool) {
    println!("\n--- 5. actor 方法带引用参数 ---");
    let handle =
        counter_actor_create(ray, &ActorOptions::new(), 100i64).expect("create the Counter actor");

    let five = ray.put(&5i64).expect("put(5)");
    let value = counter_increment_sync(ray, &handle, &five)
        .expect("sync increment(&ref(5))")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the sync increment result");
    assert_eq!(value, 105, "increment_sync(&ref(5)) 返回了 {}", value);
    println!("  increment_sync(&ref(5)) → {}", value);

    let ten = ray.put(&10i64).expect("put(10)");
    let fut = counter_increment(ray, &handle, &ten);
    drop(ten);
    tokio::time::sleep(Duration::from_millis(KEEP_ALIVE_GAP_MS)).await;
    let value = fut
        .await
        .expect("submit after dropping the caller's handle")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the async increment result");
    assert_eq!(value, 115, "increment(&ref(10)) 返回了 {}", value);
    println!("  increment: future 先建、引用先 drop、再轮询 → {}", value);

    let value = counter_get_sync(ray, &handle)
        .expect("sync get()")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the sync get result");
    assert_eq!(value, 115, "get_sync() 返回了 {}", value);
    println!("  get_sync() → {}", value);

    if cluster {
        let _ = ray.kill_actor(&handle, true);
        println!("  actor 已 kill");
    } else {
        println!(
            "  本地模式不 kill actor（KillActor 要 core worker，本地模式没有），随进程退出消失"
        );
    }
}

/// 6. Python 任务与 Python actor 的引用参数。
///
/// 驱动 `put_xlang` 经 `CoreWorker::Put` 写入 9 字节 XLANG 头与 `"XLANG"` metadata，
/// Python 任务与 Python actor 可直接按引用读取；驱动 `put` 经 `NativeObjectStore::PutRaw`
/// （`cpp/src/ray/runtime/object/native_object_store.cc:31-44`）写入空 metadata，Python
/// 在 `SerializationContext._deserialize_object`（`ray/_private/serialization.py:393-544`，
/// `:537-538`）拒收有数据而无 metadata 的参数并报 `RayError::Task`。
async fn python_with_ref(ray: &Ray) {
    println!("\n--- 6. Python 任务与 Python actor 的引用参数 ---");
    let two = serialize(&2i32).expect("serialize(2)");

    // 6a. 驱动 put_xlang 写入的对象当引用参数：携带 XLANG metadata，Python 任务正常解码。
    let driver_put = ray.put_xlang(&40i32).expect("put_xlang(40)");
    assert!(driver_put.is_xlang(), "put_xlang 应返回 is_xlang() == true");
    let sum_6a: i32 = ray
        .task_call_python(
            PYTHON_MODULE,
            "add",
            &[driver_put.id(), &two],
            &[true, false],
        )
        .expect("submit Python add(&put_xlang(40), 2)")
        .cast::<i32>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read Python add(&put_xlang(40), 2)");
    assert_eq!(
        sum_6a, 42,
        "Python add(&put_xlang(40), 2) 返回了 {}",
        sum_6a
    );
    println!(
        "  驱动 put_xlang 的对象当引用参数：add(&put_xlang(40), 2) → {}",
        sum_6a
    );

    let nums = ray.put_xlang(&vec![10i64, 20, 12]).expect("put_xlang(vec)");
    let list_sum: i64 = ray
        .task_call_python(PYTHON_MODULE, "sum_list", &[nums.id()], &[true])
        .expect("submit Python sum_list(&put_xlang([10, 20, 12]))")
        .cast::<i64>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read Python sum_list(&put_xlang([10, 20, 12]))");
    assert_eq!(
        list_sum, 42,
        "Python sum_list(&put_xlang([10, 20, 12])) 返回了 {}",
        list_sum
    );
    println!(
        "  驱动 put_xlang 的列表当引用参数：sum_list(&put_xlang([10, 20, 12])) → {}",
        list_sum
    );

    // 6a'. API 区分对照：驱动 put（无 metadata）按引用传给 Python add，Python 侧拒收。
    let rust_only = ray.put(&40i32).expect("put(40)");
    assert!(!rust_only.is_xlang(), "put 应返回 is_xlang() == false");
    match ray.task_call_python(
        PYTHON_MODULE,
        "add",
        &[rust_only.id(), &two],
        &[true, false],
    ) {
        Ok(obj) => match obj.cast::<i32>().get_timeout(PROBE_TIMEOUT_MS) {
            Err(RayError::Task(msg)) => {
                println!(
                    "  驱动 put（无 metadata）当引用参数 → Python 任务失败 Task({})",
                    msg
                )
            }
            Ok(value) => panic!(
                "驱动 put（无 metadata）当引用参数，Python 任务却成功了：{}",
                value
            ),
            Err(other) => panic!("返回了别的错误变体: {}", other),
        },
        Err(other) => panic!("Python 任务提交就失败了: {}", other),
    }

    // 6b. Python 任务产生的对象（带 PYTHON metadata）当引用参数：正常读到值。
    let zero = serialize(&0i32).expect("serialize(0)");
    let forty = serialize(&40i32).expect("serialize(40)");
    let forty: ObjectRef<i32> = ray
        .task_call_python(PYTHON_MODULE, "add", &[&zero, &forty], &[false, false])
        .expect("submit the Python add that produces 40")
        .cast::<i32>();
    let sum: i32 = ray
        .task_call_python(PYTHON_MODULE, "add", &[forty.id(), &two], &[true, false])
        .expect("submit the Python add with a ref arg")
        .cast::<i32>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the Python add result");
    assert_eq!(sum, 42, "Python add(&ref(40), 2) 返回了 {}", sum);
    println!("  Python 产生的对象当引用参数：add(&ref(40), 2) → {}", sum);

    let start = serialize(&0i64).expect("serialize(0)");
    let handle = ray
        .actor_create_python(PYTHON_MODULE, "Counter", &[&start], &ActorOptions::new())
        .expect("create the Python Counter actor");

    // 6c. actor 方法拿驱动 put_xlang 的引用：成功返回 5。
    let driver_put_five = ray.put_xlang(&5i64).expect("put_xlang(5)");
    let val_6c: i64 = ray
        .actor_call_python(handle.id(), "increment", &[driver_put_five.id()], &[true])
        .expect("call Python Counter.increment(&put_xlang(5))")
        .cast::<i64>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read Python Counter.increment(&put_xlang(5))");
    assert_eq!(
        val_6c, 5,
        "Python Counter.increment(&put_xlang(5)) 返回了 {}",
        val_6c
    );
    println!(
        "  驱动 put_xlang 的对象当 actor 参数：Counter.increment(&put_xlang(5)) → {}",
        val_6c
    );

    // 6c'. API 区分对照：驱动 put（无 metadata）按引用传给同一个 Python actor，Python 侧拒收。
    let rust_only_five = ray.put(&5i64).expect("put(5)");
    match ray.actor_call_python(handle.id(), "increment", &[rust_only_five.id()], &[true]) {
        Ok(obj) => match obj.cast::<i64>().get_timeout(PROBE_TIMEOUT_MS) {
            Err(RayError::Task(msg)) => println!(
                "  驱动 put（无 metadata）当 actor 参数 → Python actor 失败 Task({})",
                msg
            ),
            Ok(value) => panic!(
                "驱动 put（无 metadata）当 actor 参数，Python actor 却成功了：{}",
                value
            ),
            Err(other) => panic!("返回了别的错误变体: {}", other),
        },
        Err(other) => panic!("Python actor 调用提交就失败了: {}", other),
    }

    // 6d. actor 方法拿 Python 产生的引用：在 6c 的 5 基础上累加得到 10。
    let two = serialize(&2i64).expect("serialize(2)");
    let three = serialize(&3i64).expect("serialize(3)");
    let five: ObjectRef<i64> = ray
        .task_call_python(PYTHON_MODULE, "add", &[&two, &three], &[false, false])
        .expect("submit the Python add that produces 5")
        .cast::<i64>();
    let value: i64 = ray
        .actor_call_python(handle.id(), "increment", &[five.id()], &[true])
        .expect("call the Python actor with a ref")
        .cast::<i64>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the Python actor result");
    assert_eq!(
        value, 10,
        "Python Counter.increment(&ref(5)) 返回了 {}",
        value
    );
    println!(
        "  Python 产生的对象当 actor 参数：Counter.increment(&ref(5)) → {}",
        value
    );

    // 6e. Python actor 的 handle 往返。它的
    // `actor_creation_task_function_descriptor` 里是 `python_function_descriptor`，
    // 字段 6 的嵌套比 C++ actor 深一层，载荷校验必须照旧收下；复原出来的 handle
    // 登记在本进程，接着调用要能读到当前值。
    let serialized = handle
        .serialize()
        .expect("serialize the Python actor handle");
    let restored =
        ActorHandle::from_serialized(&serialized).expect("from_serialized the Python handle");
    assert_eq!(
        restored.id(),
        handle.id(),
        "复原的 Python actor id 与原 handle 不同"
    );
    let current: i64 = ray
        .actor_call_python(restored.id(), "get", &[], &[])
        .expect("用复原的 Python handle 提交 get")
        .cast::<i64>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("读复原 Python handle 的 get");
    assert_eq!(current, value, "复原的 Python handle 读到的值不对");
    println!(
        "  Python handle 往返：serialize {} 字节 → from_serialized → get() → {}",
        serialized.len(),
        current
    );

    let _ = ray.kill_actor(&handle, true);
    println!("  Python actor 已 kill");
}

/// 7. 提交之后立刻 drop 参数引用。
async fn drop_after_submit(ray: &Ray) {
    println!("\n--- 7. 提交之后立刻 drop 参数引用 ---");
    let five = ray.put(&5i32).expect("put(5)");
    let out = add_remote(ray, &five, 34).expect("submit add(&ref(5), 34)");
    drop(five);
    let sum = out
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the result after dropping the argument ref");
    assert_eq!(sum, 39, "drop 参数引用之后任务返回了 {}", sum);
    println!("  参数引用已 drop，任务仍读到 {}", sum);
}

/// 8. 任务返回的 id 不能当引用用。
///
/// 8a 是对照：对象由驱动自己持有，Rust 任务只把 id 字节交回来，驱动照样能读。
/// 8b 是真实情形：Python actor 在自己进程里 put 并保活，只把 id 字节交回；驱动按
/// 这个 id 读不到，因为驱动侧的 core worker 只在它的引用表里有这个 id 的 owner
/// 时才能取对象（`core_worker.cc` 的 `GetObjects` 先查 `HasOwner`），与对象是否
/// 还活着无关。8b 里 actor 自己把那对象读回来，证明它活着。
async fn returned_ref_is_unusable(ray: &Ray) {
    println!("\n--- 8. 任务返回的 id 不可读 ---");

    // 8a. 驱动自己 put、Rust 任务把 id 字节原样交回：读得到。
    let owner = ray.put(&5i32).expect("put(5)");
    let owner_id = serialize(&owner.id().to_vec()).expect("serialize the id bytes");
    let echoed: Vec<u8> = ray
        .task_call("return_id_bytes", &[&owner_id], &[], &TaskOptions::new())
        .expect("submit return_id_bytes")
        .cast::<Vec<u8>>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the id bytes the task returned");
    assert_eq!(echoed, owner.id(), "任务交回的 id 与提交的 id 不同");
    let rebuilt: ObjectRef<i32> = ObjectRef::from(echoed);
    let value = rebuilt
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("驱动自己持有引用时，回来的 id 应当能读");
    assert_eq!(value, 5, "回来的 id 读到了 {}", value);
    println!(
        "  control: 驱动自己持有引用时，任务交回的 id 读到 {}",
        value
    );
    drop(owner);

    // 8b. actor 里 put 并保活，只交回 id 字节：对象活着，驱动仍读不到。
    let keeper = ray
        .actor_create_python(PYTHON_MODULE, "Keeper", &[], &ActorOptions::new())
        .expect("create the Python Keeper actor");
    let five = serialize(&5i32).expect("serialize(5)");
    let worker_id: Vec<u8> = ray
        .actor_call_python(keeper.id(), "put", &[&five], &[false])
        .expect("ask the Keeper to put 5")
        .cast::<Vec<u8>>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the id bytes the Keeper returned");
    assert_eq!(
        worker_id.len(),
        28,
        "Keeper 交回的 id 是 {} 字节",
        worker_id.len()
    );
    let alive: i32 = ray
        .actor_call_python(keeper.id(), "kept_value", &[], &[])
        .expect("read the kept object back inside the actor")
        .cast::<i32>()
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("read the kept value");
    assert_eq!(alive, 5, "actor 保活的对象读回来是 {}", alive);
    println!(
        "  actor 里保活的对象读到 {}（对象活着），id_hex={}",
        alive,
        ObjectRef::<i32>::from(worker_id.clone()).id_hex()
    );

    let rebuilt: ObjectRef<i32> = ObjectRef::from(worker_id.clone());
    let started = Instant::now();
    match rebuilt.get_timeout(PROBE_TIMEOUT_MS) {
        Err(RayError::Ffi(msg)) => {
            let elapsed = started.elapsed();
            assert!(
                elapsed < Duration::from_secs(1),
                "快速失败变成了等满期限: {:?}",
                elapsed
            );
            println!("  驱动按同一个 id 读 → Ffi after {:?}", elapsed);
            println!("  ({})", msg);
        }
        Ok(value) => panic!("驱动读到了工人 put 的对象：{}", value),
        Err(other) => panic!("驱动读这个 id 返回了别的错误变体: {}", other),
    }
    let _ = ray.kill_actor(&keeper, true);
    println!("  工人/别的进程 put 出来的 id 对驱动是来路不明的 id，对象活着也读不到");

    // 这样的 id 也不能当引用参数提交（否则 Ray 的 GetOwnerAddressOrDie 会终止进程）。
    let as_arg: ObjectRef<i32> = ObjectRef::from(worker_id);
    match add_remote(ray, &as_arg, 1) {
        Err(RayError::Runtime(_)) => println!("  同一个 id 当引用参数 → Runtime（被守卫拦住）"),
        Ok(_) => panic!("工人交回的 id 被当成引用参数提交了"),
        Err(other) => panic!("工人交回的 id 当引用参数返回了别的错误变体: {}", other),
    }
}

/// 9. 来路不明的 28 字节 id、27 字节 id 与跨语言 `ObjectRef`：都在 Rust 侧被拒，进程继续运行。
async fn unknown_ids_are_rejected(ray: &Ray, cluster: bool) {
    println!("\n--- 9. 来路不明的 id 当引用参数 ---");

    // 9a. `put` 之后保存 id 字节、drop 句柄、再按字节重建：本进程不再持有这个引用。
    let obj = ray.put(&5i32).expect("put(5)");
    let saved = obj.id().to_vec();
    drop(obj);
    let rebuilt: ObjectRef<i32> = ObjectRef::from(saved);
    match add_remote(ray, &rebuilt, 1) {
        Err(RayError::Runtime(_)) => {
            println!("  add(&ref(保存字节后重建的 id), 1) → Runtime")
        }
        Ok(_) => panic!("保存字节后重建的 id 被当成引用参数提交了"),
        Err(other) => panic!("重建的 id 返回了别的错误变体: {}", other),
    }

    // 9b. 自己拼的 28 字节 id。
    let rebuilt: ObjectRef<i32> = ObjectRef::from(vec![7u8; 28]);
    let started = Instant::now();
    match add_remote(ray, &rebuilt, 1) {
        Err(RayError::Runtime(msg)) => {
            let elapsed = started.elapsed();
            assert!(
                elapsed < Duration::from_secs(1),
                "拒绝变成了阻塞: {:?}",
                elapsed
            );
            println!(
                "  add(&ref(来路不明的 28 字节 id), 1) → Runtime after {:?}",
                elapsed
            );
            println!("  ({})", msg);
        }
        Ok(_) => panic!("来路不明的 28 字节 id 被当成引用参数提交了"),
        Err(other) => panic!("来路不明的 28 字节 id 返回了别的错误变体: {}", other),
    }

    // 9c. 同一个自己拼的 id 按值传：按值的形状也过同一道守卫。
    let owned: ObjectRef<i32> = ObjectRef::from(vec![7u8; 28]);
    match add_remote(ray, owned, 1) {
        Err(RayError::Runtime(_)) => {
            println!("  add(来路不明的 28 字节 id, 1)（按值）→ Runtime")
        }
        Ok(_) => panic!("按值传的来路不明 id 被提交了"),
        Err(other) => panic!("按值传的来路不明 id 返回了别的错误变体: {}", other),
    }

    // 9d. 27 字节：长度不对，错误变体与消息与 C 侧一致。
    let short: ObjectRef<i32> = ObjectRef::from(vec![0u8; 27]);
    match add_remote(ray, &short, 1) {
        Err(RayError::Ffi(msg)) => println!("  add(&27 字节 id, 1) → Ffi({})", msg),
        Ok(_) => panic!("27 字节的 id 被当成引用参数提交了"),
        Err(other) => panic!("27 字节的 id 返回了别的错误变体: {}", other),
    }

    // 9e. 裸通道：`is_ref` 与参数个数对不上时拒绝，不进入 C 侧的越界读。
    let one = serialize(&1i32).expect("serialize(1)");
    let two = serialize(&2i32).expect("serialize(2)");
    let args: [&[u8]; 2] = [&one, &two];
    match ray.task_call("add", &args, &[true], &TaskOptions::new()) {
        Err(RayError::Ffi(msg)) => {
            println!("  task_call(2 个参数, 1 个 is_ref) → Ffi({})", msg)
        }
        Ok(_) => panic!("is_ref 长度对不上却提交了"),
        Err(other) => panic!("is_ref 长度对不上返回了别的错误变体: {}", other),
    }
    // 9f. 集群模式下 put_xlang 句柄交给 Rust 宏调用器：提交前立即返回 RayError::Serialization。
    if cluster {
        let driver_put = ray.put_xlang(&40i32).expect("put_xlang(40)");
        match add_remote(ray, &driver_put, 2) {
            Err(RayError::Serialization(msg)) => {
                println!("  add(&put_xlang(40), 2) → Serialization({})", msg);
            }
            Ok(obj) => panic!(
                "put_xlang 句柄被当成 Rust 任务引用参数提交了: {:?}",
                obj.get_timeout(PROBE_TIMEOUT_MS)
            ),
            Err(other) => panic!("put_xlang 借用句柄返回了别的错误变体: {}", other),
        }
        match add_remote(ray, driver_put, 2) {
            Err(RayError::Serialization(_)) => {
                println!("  add(put_xlang(40), 2)（按值）→ Serialization");
            }
            Ok(obj) => panic!(
                "按值传的 put_xlang 句柄被当成 Rust 任务引用参数提交了: {:?}",
                obj.get_timeout(PROBE_TIMEOUT_MS)
            ),
            Err(other) => panic!("put_xlang 按值句柄返回了别的错误变体: {}", other),
        }
    }
    println!("  进程继续运行：没有 GetOwnerAddressOrDie 的终止，也没有本地模式的永久等待");
}

/// 10. 异步入口的错误路径：27 字节 id 的 future 返回错误，不会 panic。
async fn async_error_path(ray: &Ray) {
    println!("\n--- 10. 异步入口的错误路径 ---");
    let short: ObjectRef<i32> = ObjectRef::from(vec![0u8; 27]);
    match add_remote_async(ray, &short, 1).await {
        Err(RayError::Ffi(msg)) => {
            println!("  add_remote_async(&27 字节 id, 1).await → Ffi({})", msg)
        }
        Ok(_) => panic!("27 字节 id 的异步调用成功了"),
        Err(other) => panic!("异步入口返回了别的错误变体: {}", other),
    }
}

/// 11. id 宽度：读侧与提交侧都在进 Ray 之前拒掉，进程不终止。
///
/// 3 字节与 27 字节的 id 进入 `BaseID<ObjectID>::FromBinary` 会命中
/// `RAY_CHECK(binary.size() == T::Size())`（`src/ray/common/id.h:430-441`）终止进程；
/// 0 字节的 id 变成 nil：`get` 报 TimedOut、`wait` 把它当成永不就绪的对象。
/// 引用参数的 0 字节更糟：本地模式在 `build_task_args_cpp` 里用无期限的
/// `runtime->Get(id)` 读值，永久等待。所以每条入口都在进 Ray 之前拒绝。
async fn id_width_rejections(ray: &Ray, cluster: bool) {
    println!("\n--- 11. id 宽度不对的 id 在进 Ray 之前被拒 ---");

    // 11a. 单个读取：3 字节与 0 字节都报出实际长度。
    let short: ObjectRef<i64> = ObjectRef::from(vec![1u8, 2, 3]);
    match short.get_timeout(1000) {
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains("28 bytes"),
                "3 字节 id 的错误里没有长度要求: {}",
                msg
            );
            assert!(
                msg.contains("3 bytes"),
                "3 字节 id 的错误里没有实际长度: {}",
                msg
            );
            println!("  get_timeout(3 字节 id) → Ffi({})", msg);
        }
        Err(other) => panic!("3 字节 id 返回了别的错误变体: {}", other),
        Ok(value) => panic!("3 字节 id 读到了 {}", value),
    }
    let empty: ObjectRef<i64> = ObjectRef::from(Vec::new());
    match empty.get_timeout(1000) {
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains("28 bytes"),
                "0 字节 id 的错误里没有长度要求: {}",
                msg
            );
            assert!(
                msg.contains("0 bytes"),
                "0 字节 id 的错误里没有实际长度: {}",
                msg
            );
            println!("  get_timeout(0 字节 id) → Ffi({})", msg);
        }
        Err(other) => panic!("0 字节 id 返回了别的错误变体: {}", other),
        Ok(value) => panic!("0 字节 id 读到了 {}", value),
    }

    // 11b. 批量读取：正常 id 与 3 字节 id 混在一批。
    let good = ray.put(&11i64).expect("put(11)");
    let short: ObjectRef<i64> = ObjectRef::from(vec![1u8, 2, 3]);
    match ray.get_many_timeout(&[good.clone(), short], 1000) {
        Err(RayError::Ffi(msg)) => {
            println!("  get_many_timeout([好 id, 3 字节 id]) → Ffi({})", msg)
        }
        Err(other) => panic!("批量读取返回了别的错误变体: {}", other),
        Ok(values) => panic!("批量读取读到了 {:?}", values),
    }

    // 11c. wait：3 字节 id。
    let short: ObjectRef<i64> = ObjectRef::from(vec![1u8, 2, 3]);
    match ray.wait(&[short], 1, 1000) {
        Err(RayError::Ffi(msg)) => println!("  wait([3 字节 id], 1, 1000) → Ffi({})", msg),
        Err(other) => panic!("wait 返回了别的错误变体: {}", other),
        Ok((ready, unready)) => panic!(
            "wait 接受了 3 字节 id: ready={} unready={}",
            ready.len(),
            unready.len()
        ),
    }

    // 11d. 引用参数：3 字节与 0 字节。错误文本带参数下标。
    let three = [1u8, 2, 3];
    let args: [&[u8]; 1] = [&three];
    match ray.task_call("add", &args, &[true], &TaskOptions::new()) {
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains("argument 0"),
                "引用参数的错误里没有参数下标: {}",
                msg
            );
            assert!(
                msg.contains("28 bytes"),
                "引用参数的错误里没有长度要求: {}",
                msg
            );
            println!("  task_call(引用参数 3 字节) → Ffi({})", msg);
        }
        Err(other) => panic!("3 字节引用参数返回了别的错误变体: {}", other),
        Ok(_) => panic!("3 字节的引用参数被提交了"),
    }
    let empty: [u8; 0] = [];
    let args: [&[u8]; 1] = [&empty];
    match ray.task_call("add", &args, &[true], &TaskOptions::new()) {
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains("0 bytes"),
                "0 字节引用参数的错误里没有实际长度: {}",
                msg
            );
            println!("  task_call(引用参数 0 字节) → Ffi({})", msg);
        }
        Err(other) => panic!("0 字节引用参数返回了别的错误变体: {}", other),
        Ok(_) => panic!("0 字节的引用参数被提交了（修复前这里在本地模式永久挂起）"),
    }

    // 11e. Python 任务的引用参数：同一个宽度检查。
    let three = [1u8, 2, 3];
    let args: [&[u8]; 1] = [&three];
    match ray.task_call_python(PYTHON_MODULE, "add", &args, &[true]) {
        Err(RayError::Ffi(msg)) => println!("  task_call_python(引用参数 3 字节) → Ffi({})", msg),
        Err(other) => panic!("Python 任务的 3 字节引用参数返回了别的错误变体: {}", other),
        Ok(_) => panic!("Python 任务的 3 字节引用参数被提交了"),
    }

    // 11f. actor id 宽度：3 字节与 0 字节。
    for (label, actor_id) in [("3 字节", vec![1u8, 2, 3]), ("0 字节", Vec::new())] {
        match ray.actor_call(&actor_id, "Counter::increment", &[], &[]) {
            Err(RayError::Ffi(msg)) => {
                assert!(
                    msg.contains("actor id must be 16 bytes"),
                    "{} actor id 的错误里没有长度要求: {}",
                    label,
                    msg
                );
                println!("  actor_call({} actor id) → Ffi({})", label, msg);
            }
            Err(other) => panic!("{} actor id 返回了别的错误变体: {}", label, other),
            Ok(_) => panic!("{}的 actor id 被接受了", label),
        }
    }

    // 11g. 本地模式：`actor_call_python` 的宽度检查排在本地模式拒绝之前，与
    // `ray_cancel` 的次序一致。集群模式没有这条本地模式分支，跳过。
    if cluster {
        println!("  actor_call_python 的守卫次序在集群模式没有本地模式分支可区分：跳过");
    } else {
        match ray.actor_call_python(&[1u8, 2, 3], "increment", &[], &[]) {
            Err(RayError::Ffi(msg)) => {
                assert!(
                    msg.contains("actor id must be 16 bytes"),
                    "本地模式 actor_call_python 的短 id 没有报宽度: {}",
                    msg
                );
                assert!(
                    !msg.contains("local mode"),
                    "宽度检查排在了本地模式拒绝之后: {}",
                    msg
                );
                println!("  actor_call_python(3 字节 actor id) → Ffi({})", msg);
            }
            Err(other) => panic!("本地模式 actor_call_python 返回了别的错误变体: {}", other),
            Ok(_) => panic!("本地模式 actor_call_python 接受了 3 字节 actor id"),
        }
    }

    // 11h. 以上拒绝之后运行时状态没有被打坏。
    let value = ray
        .put(&42i64)
        .expect("拒绝之后 put")
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("拒绝之后 get");
    assert_eq!(value, 42, "拒绝之后 put + get 读到 {}", value);
    println!("  以上全部被拒之后 put + get 照常读到 {}", value);
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== ObjectRef 作为任务/actor 方法参数 ({}) ===", mode);
    let config = match mode.as_str() {
        "local" => RayConfig::local(),
        "cluster" => cluster_config(),
        other => panic!("unknown mode {:?}, expected local or cluster", other),
    };
    let cluster = mode == "cluster";
    let ray = Ray::connect(&config).expect("Ray::connect");

    mixed_value_and_ref(&ray).await;
    two_step_graph(&ray).await;
    fan_out(&ray).await;
    async_call_with_ref(&ray, cluster).await;
    actor_with_ref(&ray, cluster).await;

    if cluster {
        python_with_ref(&ray).await;
    } else {
        println!("\n--- 6. Python 任务与 Python actor 的引用参数：跳过 ---");
        println!("  本地模式没有 Python worker（ref_counting_test 记录过这条）");
    }

    drop_after_submit(&ray).await;

    if cluster {
        returned_ref_is_unusable(&ray).await;
    } else {
        println!("\n--- 8. 任务返回的 id 不可读：跳过 ---");
        println!("  这一项要 worker .so 里的 return_id_bytes 与 Python actor，本地模式两样都没有");
    }

    unknown_ids_are_rejected(&ray, cluster).await;
    async_error_path(&ray).await;
    id_width_rejections(&ray, cluster).await;

    if !cluster {
        println!(
            "\n说明：第 1、2 项在本地模式下由 build_task_args_cpp 在提交前用 Runtime()->Get(id)"
        );
        println!(
            "取成值（与 SDK 在 ray/api/arguments.h:43-52 的做法一致），集群模式下同一个引用以 id"
        );
        println!("交给 core worker，由它把对象的值投递给工人");
    }

    println!("\n✓ 引用参数检查全部通过");
}

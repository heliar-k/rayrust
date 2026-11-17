//! Example: `ObjectRef` 持有 Ray 的一个本地引用。
//!
//! 每次运行只跑一种模式：
//! ```text
//! ./target/release/examples/ref_counting_test cluster
//! ./target/release/examples/ref_counting_test local
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! 集群模式的检查，按顺序：
//! 1. `put(42)`：读到值 → drop 引用 → 用 `ObjectRef::from(id_bytes)` 重建 →
//!    `get_timeout` 与 `wait` 都立即报 `ObjectUnknownOwner`；紧接着一个新的
//!    `put` 照常读到，排除「运行时自己坏了」这个解释
//! 2. `put(7)` 之后 `Clone`：drop 原引用，clone 仍读到值；drop clone 之后
//!    重建的引用读不到，证明 clone 加了引用、drop 减了引用
//! 3. Rust 任务（`task_call`）与 Python 任务（`task_call_python`）的结果各一条
//!    同样的「drop 之后重建读不到」
//! 4. `ObjectRef` 转成 `Vec<u8>` 之后引用保留：重建的引用仍然读到值
//! 5. Python 函数返回一个装着 `ObjectRef` 的 dict：嵌套引用在 Rust 侧不可见，
//!    `get_value` 返回 `RayError::Serialization`
//! 6. 最后一个 `Ray` 句柄 drop 之后，再 drop 一个仍然活着的引用：代次守卫让
//!    这次 drop 跳过引用计数，进程退出码 0
//!
//! 本地模式的检查：本地模式（`RunMode::SINGLE_PROCESS`）不初始化 core worker
//! （`abstract_ray_runtime.cc:48-56`），`AddLocalReference`/`RemoveLocalReference`
//! 的 `CoreWorkerProcess::IsInitialized()` 守卫直接跳过
//! （`abstract_ray_runtime.cc:241-253`），对象留在 `LocalModeObjectStore` 的
//! 内存表里，所以 put 与 Rust 任务的结果在 drop 之后重建的引用照样读到值。
//! 示例断言的就是这个公开行为。本地模式跑不了 Python 函数
//! （`xlang_decode_test` 记录过这条），跨语言那条检查只存在于集群模式。

use rayrust::prelude::*;

/// 等待一个「取不到」的引用时的期限（毫秒）。
const PROBE_TIMEOUT_MS: i32 = 2000;

/// 等 Python 任务的结果就绪的期限（毫秒）：就绪之后下面那条没有期限的
/// `get_raw_bytes` 才能保证返回。
const NESTED_WAIT_MS: i32 = 60000;

/// 跨语言载荷的 9 字节头长度。
const XLANG_HEADER_LEN: usize = 9;

/// Python 测试模块；worker 必须能 import 到它。
const PYTHON_MODULE: &str = "rayrust_test";

/// 本地模式的 Rust 任务。`#[rayrust::remote]` 在驱动进程里也注册一份，
/// 本地模式因此能真的跑通这个任务。
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
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

/// drop 之前先确认这个引用真的能读到值，后面的「读不到」才有意义。
fn read_before_drop<T>(obj: &ObjectRef<T>, expected: T, label: &str)
where
    T: serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    match obj.get() {
        Ok(value) => {
            assert_eq!(value, expected, "{}: drop 之前读到的值不对", label);
            println!("  {}: before drop → Ok({:?})", label, value);
        }
        Err(e) => panic!("{}: drop 之前就读不到: {}", label, e),
    }
}

/// 引用已经 drop，重建的引用不持有 Ray 的本地引用：读取与等待都必须快速失败。
///
/// 引用计数生效时对象已经被 Ray 释放，本地 core worker 不再认识这个 id，
/// `get_timeout` 与 `wait` 都立即报 `ObjectUnknownOwner`；计数没生效时对象还活着，
/// 读到值、`wait` 报就绪。快不快速也一起断言：等满期限的 `Timeout` 说明还有别的
/// 引用在保着这个 id。
fn assert_released<T>(ray: &Ray, label: &str, id: Vec<u8>)
where
    T: serde::de::DeserializeOwned,
{
    let rebuilt: ObjectRef<T> = ObjectRef::from(id);
    let started = std::time::Instant::now();
    match rebuilt.get_timeout(PROBE_TIMEOUT_MS) {
        Ok(_) => panic!("{}: 引用已经 drop，重建的引用却读到了值", label),
        Err(RayError::Ffi(_)) => {
            let elapsed = started.elapsed();
            assert!(
                elapsed < std::time::Duration::from_secs(1),
                "{}: 快速失败变成了等满期限: {:?}",
                label,
                elapsed
            );
            println!(
                "  {}: rebuilt get_timeout → Ffi after {:?}",
                label, elapsed
            )
        }
        Err(e) => panic!("{}: 重建的引用返回了别的错误变体: {}", label, e),
    }
    match ray.wait(std::slice::from_ref(&rebuilt), 1, PROBE_TIMEOUT_MS) {
        Err(RayError::Ffi(_)) => println!("  {}: rebuilt wait → Ffi", label),
        Ok((ready, unready)) => panic!(
            "{}: 对象已经释放，wait 却返回 ready={} unready={}",
            label,
            ready.len(),
            unready.len()
        ),
        Err(e) => panic!("{}: wait 返回了别的错误变体: {}", label, e),
    }
}

/// 本地模式：运行时没有初始化 core worker，引用计数调用被守卫跳过，drop 之后
/// 对象仍然可读。
fn assert_survives_drop<T>(label: &str, id: Vec<u8>, expected: T)
where
    T: serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let rebuilt: ObjectRef<T> = ObjectRef::from(id);
    let value = rebuilt
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("本地模式 drop 之后对象应当仍然可读");
    assert_eq!(value, expected, "{}: 本地模式读到的值不对", label);
    println!(
        "  {}: drop 之后重建的引用仍读到 {:?}（本地模式不释放）",
        label, value
    );
}

/// 9 字节跨语言头里声明的 msgpack 段长度。第一个值不是 msgpack 整数时返回
/// `None`（没有跨语言头），与库里的 `split_xlang` 判法相同。
fn declared_msgpack_len(raw: &[u8]) -> Option<usize> {
    if raw.len() < XLANG_HEADER_LEN {
        return None;
    }
    let mut header = &raw[..XLANG_HEADER_LEN];
    let value = rmpv::decode::read_value(&mut header).ok()?;
    Some(usize::try_from(value.as_u64()?).ok()?)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn run_cluster(ray: &Ray) {
    // ── 1. put 的结果 ──────────────────────────────────────
    println!("\n--- 1. put(42) ---");
    let obj = ray.put(&42i32).expect("put(42)");
    let id = obj.id().to_vec();
    println!("  id_hex={}", obj.id_hex());
    read_before_drop(&obj, 42, "put");
    drop(obj);
    assert_released::<i32>(ray, "put", id);

    // 对照：释放掉一个对象之后运行时本身照常工作。
    let fresh = ray.put(&99i32).expect("put(99) after the released object");
    let value = fresh.get().expect("新 put 的对象读不到");
    assert_eq!(value, 99, "对照场景读到的值不对");
    println!("  control: 新 put(99) 仍读到 {}", value);

    // ── 2. clone 的引用 ────────────────────────────────────
    println!("\n--- 2. put(7) 加 Clone ---");
    let original = ray.put(&7i32).expect("put(7)");
    let clone = original.clone();
    let id = original.id().to_vec();
    drop(original);
    println!("  原引用已 drop");
    let value = clone.get().expect("clone 读不到值");
    assert_eq!(value, 7, "clone 读到的值不对");
    println!("  clone: 原引用 drop 之后仍读到 {}", value);
    drop(clone);
    assert_released::<i32>(ray, "clone", id);

    // ── 3. Rust 任务与 Python 任务的结果 ───────────────────
    println!("\n--- 3. Rust 任务 add(10, 32) ---");
    let arg1 = rayrust::serialize(&10i32).expect("serialize 10");
    let arg2 = rayrust::serialize(&32i32).expect("serialize 32");
    let args: Vec<&[u8]> = vec![&arg1, &arg2];
    let task_ref: ObjectRef<i32> = ray
        .task_call("add", &args, &[], &TaskOptions::new())
        .expect("task_call add")
        .cast();
    let id = task_ref.id().to_vec();
    read_before_drop(&task_ref, 42, "rust_task");
    drop(task_ref);
    assert_released::<i32>(ray, "rust_task", id);

    println!("\n--- 3. Python 任务 add(10, 32) ---");
    let py_ref = ray
        .task_call_python(PYTHON_MODULE, "add", &args, &[])
        .expect("task_call_python add");
    let id = py_ref.id().to_vec();
    match py_ref.get_value() {
        Ok(value) => println!("  python_task: before drop → Ok({})", value),
        Err(e) => panic!("python_task: drop 之前就读不到: {}", e),
    }
    drop(py_ref);
    assert_released::<i32>(ray, "python_task", id);

    // ── 4. 转成字节之后引用保留 ────────────────────────────
    println!("\n--- 4. ObjectRef 转成 Vec<u8> ---");
    let obj = ray.put(&11i32).expect("put(11)");
    let id: Vec<u8> = obj.into();
    println!("  转成 {} 字节的 id", id.len());
    let rebuilt: ObjectRef<i32> = ObjectRef::from(id);
    let value = rebuilt
        .get_timeout(PROBE_TIMEOUT_MS)
        .expect("转成字节之后对象应当仍然活着");
    assert_eq!(value, 11, "转成字节之后读到的值不对");
    println!("  转成字节之后重建的引用读到 {}（引用随字节保留）", value);

    // ── 5. 嵌套引用不可见 ────────────────────────────────────
    println!("\n--- 5. Python 返回装着 ObjectRef 的 dict ---");
    let nested = ray
        .task_call_python(PYTHON_MODULE, "return_dict_with_ref", &[], &[])
        .expect("task_call_python return_dict_with_ref");
    // 先等到就绪，下面那条没有期限的 get_raw_bytes 才一定返回。
    let (ready, unready) = ray
        .wait(std::slice::from_ref(&nested), 1, NESTED_WAIT_MS)
        .expect("wait return_dict_with_ref");
    assert!(
        ready.len() == 1 && unready.is_empty(),
        "return_dict_with_ref 没有就绪: ready={} unready={}",
        ready.len(),
        unready.len()
    );
    // Rust 拿不到 GetNestedRefs()：wheel 只带 ray/api（RayObject 在
    // src/ray/common/ray_object.h），RayRuntime::Get 只返回 msgbuffer。所以嵌套
    // 引用在 Rust 侧不可见：dict 里那个 ObjectRef 进了 pickle5 段，Rust 解不开，
    // 也不会因为它保持存活。这个检查钉住的就是这个限制。
    let raw = nested.get_raw_bytes().expect("return_dict_with_ref 的原始字节");
    let declared = declared_msgpack_len(&raw).expect("9 字节头里的 msgpack 段长度");
    assert!(
        raw.len() > XLANG_HEADER_LEN + declared,
        "载荷里没有 pickle5 段: len={} 头={} 声明的 msgpack 段={}",
        raw.len(),
        XLANG_HEADER_LEN,
        declared
    );
    let pickle = raw.len() - XLANG_HEADER_LEN - declared;
    println!(
        "  raw len={} 前 9 字节={} 声明的 msgpack 段={} pickle5 段={}",
        raw.len(),
        hex(&raw[..XLANG_HEADER_LEN]),
        declared,
        pickle
    );
    match nested.get_value() {
        Err(RayError::Serialization(msg)) => {
            println!("  get_value → Serialization({})", msg)
        }
        Ok(value) => panic!("嵌套引用应当读不出来，却读到了 {}", value),
        Err(e) => panic!("get_value 返回了别的错误变体: {}", e),
    }
}

fn run_local(ray: &Ray) {
    println!("\n--- 1. put(42) ---");
    let obj = ray.put(&42i32).expect("put(42)");
    let id = obj.id().to_vec();
    println!("  id_hex={}", obj.id_hex());
    read_before_drop(&obj, 42, "put");
    drop(obj);
    assert_survives_drop("put", id, 42);

    println!("\n--- 2. put(7) 加 Clone ---");
    let original = ray.put(&7i32).expect("put(7)");
    let clone = original.clone();
    let id = original.id().to_vec();
    drop(original);
    let value = clone.get().expect("clone 读不到值");
    assert_eq!(value, 7, "clone 读到的值不对");
    println!("  clone: 原引用 drop 之后仍读到 {}", value);
    drop(clone);
    assert_survives_drop("clone", id, 7);

    println!("\n--- 3. Rust 任务 add(10, 32) ---");
    let task_ref = add_remote(ray, 10, 32).expect("submit add(10, 32)");
    let id = task_ref.id().to_vec();
    read_before_drop(&task_ref, 42, "rust_task");
    drop(task_ref);
    assert_survives_drop("rust_task", id, 42);
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== Ray 引用计数 ({}) ===", mode);
    let config = match mode.as_str() {
        "local" => RayConfig::local(),
        "cluster" => cluster_config(),
        other => panic!("unknown mode {:?}, expected local or cluster", other),
    };
    let ray = Ray::connect(&config).expect("Ray::connect");

    if mode == "local" {
        run_local(&ray);
    } else {
        run_cluster(&ray);
    }

    // ── 6. 最后一个句柄 drop 之后 drop 活着的引用 ──────────
    println!("\n--- 收尾：最后一个句柄 drop 之后 drop 活着的引用 ---");
    let survivor = ray.put(&5i32).expect("put(5)");
    drop(ray);
    println!("  最后一个 Ray 句柄已 drop");
    drop(survivor);
    println!("  活着的引用 drop 完成，引用计数被代次守卫跳过");

    println!("\n✓ 引用计数检查全部通过");
}

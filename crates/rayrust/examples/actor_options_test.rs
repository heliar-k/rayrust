//! Example: `ActorOptions` 的 placement group 字段（`placement_group_id` 与
//! `bundle_index`）。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/actor_options_test cluster
//! ./target/release/examples/actor_options_test local
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! # 环境前提
//!
//! - 集群必须是 **2 个 CPU**，并带自定义资源 `Custom` 8 个：启动参数
//!   `--num-cpus=2 --resources='{"Custom":8}'`。第 7 项要用一个两 bundle（各 `CPU 1`）的
//!   组——长任务占住 bundle 0，默认 `bundle_index = -1` 的 actor 只能在 bundle 1 上跑；
//!   1 CPU 集群上两个 bundle 都建不出来。第 11 项同时活着 5 个 actor，其中 3 个各要 1 个
//!   `Custom`。
//! - 同一集群上别的 job 正在占着 CPU 时会干扰第 7 项（依赖「组内只剩 bundle 1 空着」）与
//!   第 11 项（要先把两个 CPU 全占住）。非 detached 的 placement group 随创建它的 job 结束
//!   而销毁，本示例自己上一轮留下的组不需要手动清理。
//! - 示例启动时清掉自己的日志目录 `actor_options_test_logs/`，结束时再清一次，不在
//!   工作树里留痕迹。
//!
//! # 检查项
//!
//! 1. 默认值：`ActorOptions::default()`、`ActorOptions::new()` 与结构体更新语法
//!    （`ActorOptions { placement_group_id, ..Default::default() }`）的 `bundle_index`
//!    都是 -1；`ActorOptions::new()` 建的 actor 能调通方法。`to_json` 的形状由
//!    `crates/rayrust/src/runtime.rs` 的单元测试覆盖。
//! 2. id 长度与格式：17 字节、19 字节、空、非 hex 四种 id 各自 `Err(RayError::Ffi(_))`，
//!    进程存活；四次拒绝之后一个普通 actor 仍能建出来并调通。空 id 与非法 hex 由
//!    C 侧 `parse_actor_options` 拒绝，只有手写 options JSON 才能把它们送进去（见
//!    `create_actor_with_raw_options` 的说明）；这条路径同时断言错误类别是
//!    `ErrorKind::Generic`，即 `ray_c.h` 承诺的 `RAY_ERROR_KIND_GENERIC`。
//! 3. `bundle_index` 的范围：类型化 API 用真组 + `bundle_index = -2` →
//!    `Err(RayError::Ffi(_))`，消息含 "-1 or between"，进程存活；再补五条原始 ABI 的
//!    越界输入（`-4294967296`、`-4294967297`、`-2147483649`、`9999999999999`、
//!    `18446744073709551615`），这些值在 nlohmann 3.9.1 的 `get<int>()` 下会被截断成
//!    合法值。两种模式都跑：范围检查发生在组解码与本地模式检查之前，所以本地模式用
//!    任意 18 字节 id 同样可达。
//! 4. 未知组：随机 18 字节 id → `Err(RayError::Ffi(_))`，消息含
//!    "placement group not found for id" 与该 hex，actor 不会在组外静默建出来。
//! 5. 孤立 `bundle_index`：没有 group 而 `bundle_index = 2` → `Err(RayError::Ffi(_))`，
//!    消息说明字段会被丢掉。`to_json` 不输出这个字段，拒绝来自 Rust 侧的
//!    `check_actor_options`。
//! 6. 两个入口都覆盖：第 5 条分别在 `Ray::actor_create`（Rust actor）与
//!    `Ray::actor_create_python`（Python 模块 `rayrust_test` 的 `Counter`）上各断言
//!    一次，证明 Python actor 入口也走了同一套校验。
//! 7. 默认 `bundle_index` 真的不钉在 bundle 0：建两 bundle 的组 → 绑 bundle 0 的短任务
//!    完成（证明组已生效）→ 绑 bundle 0 的长任务（`sleep_for(30)`）占住它 →
//!    `ActorOptions { placement_group_id, resources: CPU 1.0, ..Default::default() }`
//!    的 actor 第一次方法调用在期限内完成（只能在 bundle 1 上跑）→ 同组
//!    `bundle_index = 0` 的 actor 第一次方法调用在期限内超时（bundle 0 被占住）→
//!    cancel 长任务、移除组 → 不绑组的 actor 仍能建出来并调通（证明前面的超时来自绑定，
//!    集群本身正常）→ 建一个新的两 bundle 组 B，bundle 0 空着，绑 `bundle_index = 0`
//!    的 actor 第一次调用必须很快完成（证明同一条绑定路径在 bundle 空着时不是启动慢）。
//!    两个 actor 都要显式要 1 个 CPU：不写 `resources` 的 actor 不申请 CPU，占住
//!    bundle 的长任务挡不住它。
//! 8. 本地模式：`Ray::actor_create` 带组的 `ActorOptions` → `Err(RayError::Ffi(_))`，
//!    消息含 "local mode"，进程存活；不带组照常建 actor 并调通。`Ray::actor_create_python`
//!    在本地模式无论带不带组都报创建被拒（#68），由 `runtime_api_test` 第 7 项断言
//!    （那里只传名字、不带组；带组的拒绝由同一守卫先于 options 解析生效）。
//! 9. 只有手写 options JSON 才能到达的 C 侧分支：`bundle_index` 没有组、
//!    `bundle_index` 不是 JSON 整数、`placement_group_id` 不是 JSON 字符串，三条都是
//!    `Ffi` + `Generic` 类别 + 进程存活。
//! 10. actor 的语言判定（#71）：一个 16 字节未登记 id 的 `actor_call` → `Ffi` 且消息含
//!    "is not registered in this process"（两种模式都跑：登记表在两种模式下都查）。
//!    集群模式另外六条：用 `actor_create` 建出来的 C++ actor 调 `actor_call_python` →
//!    `Ffi` 且消息含 "is a C++ actor"；用 Python `Counter` 调 `actor_call` → `Ffi` 且
//!    消息含 "is a Python actor"；同一个 Python actor 的 `actor_call_python` 照常返回数值
//!    （对照，证明拒绝只针对语言不匹配）；两次拒绝之后 C++ actor 的普通调用仍完成
//!    （进程与运行时存活）。最后三条用原始 C ABI 建两个带名字的 actor（C++ 一个、Python
//!    `Counter` 一个），它们的 id 不在登记表里、`actor_call` 先报未登记，再用
//!    `get_actor` 取回：取回的 C++ handle 调 `actor_call_python` → "is a C++ actor"、
//!    调 `actor_call` 读到值；取回的 Python handle 调 `actor_call` → "is a Python actor"、
//!    调 `actor_call_python` 读到值。这两条证明 `get_actor` 集群路径从
//!    `SerializeActorHandle` 字节里读出的语言与创建描述符类型是对的（改成恒报 CPP，
//!    Python 那条会在 `actor_call` 的断言上失败）。
//! 11. actor 的默认 CPU（#55）：两个默认 CPU 的任务占住两个 CPU 之后，
//!    `resources = [("Custom",1.0)]` 的 actor 第一次调用必须在 `BLOCKED_TIMEOUT_MS` 内
//!    超时（本缺陷的判别项），对照是 `[("Custom",1.0), ("CPU",1.0)]` 的 actor 同样超时
//!    （证明 CPU 真的被占住）；`[("Custom",1.0), ("CPU",0.0)]`、不写 `resources`、只写
//!    `[("memory",1.0)]` 三个 actor 的第一次调用都要完成并读到构造参数（显式退出、没写
//!    资源、只有内存键都不补 CPU）；cancel 两个占 CPU 的任务之后，前面那两条排队的调用都
//!    要完成并读到构造参数（前面的超时来自 CPU 被占住）。这一项结尾把它建的 actor 全部
//!    kill。
//!
//! 本地模式（`local`）跳过第 4、7、11 项与第 10 项的集群部分并打印原因：第 4 项要查组，
//! 本地模式在查组之前就报错；第 7 项要真组与 bundle 资源；第 11 项要真资源账本，本地模式
//! 给任务规格传的是空资源表，资源不参与调度；第 10 项的集群部分要真的
//! Python actor，本地模式建不出来（`actor_create_python` 拒绝本地模式）。第 3、9 项的
//! 拒绝都发生在查组之前，两种模式都跑。

use std::time::{Duration, Instant};

use rayrust::prelude::*;

/// 与 worker .so 的 `sleep_for` 同名同签名，第 7 项用它占住 bundle 0。
#[rayrust::remote]
fn sleep_for(seconds: f64) -> i64 {
    std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
    0
}

/// Rust actor `Counter` 的工厂名（与 `crates/rayrust-example-worker` 注册的同一个）。
const COUNTER_FACTORY: &str = "__rayrust_actor_factory_counter";

/// 本示例自己的日志目录（集群模式）。
const DRIVER_LOG_DIR: &str = "actor_options_test_logs";

/// 普通 actor 方法调用的期限（毫秒）。
const GET_TIMEOUT_MS: i32 = 5000;

/// 必须完成的 actor 调用的期限（毫秒）：包含 actor 进程启动的时间。
const ACTOR_START_TIMEOUT_MS: i32 = 20000;

/// 必须超时的 actor 调用的期限（毫秒）：长得足够排除启动延迟。
const BLOCKED_TIMEOUT_MS: i32 = 15000;

/// 第 7 项里证明组已生效的短任务时长（秒）。
const SHORT_SLEEP_S: f64 = 1.0;

/// 第 7 项里占住 bundle 0 的长任务时长（秒）。
const LONG_SLEEP_S: f64 = 30.0;

/// 第 11 项里占住两个 CPU 的任务时长（秒）：两条 `BLOCKED_TIMEOUT_MS` 与三条
/// `ACTOR_START_TIMEOUT_MS` 最坏加起来是 90 s，占住 CPU 的任务必须活得更久。
const CPU_HOLD_S: f64 = 180.0;

/// 第 11 项里等两个占 CPU 的任务真的跑起来的时间（秒）。
const CPU_HOLD_SETTLE_S: u64 = 5;

/// 第 11 项里 cancel 之后等 raylet 把两个 CPU 还给调度器的时间（秒）。
const CPU_RELEASE_SETTLE_S: u64 = 3;

/// 与 worker .so 的 `Counter` 同名同实现：集群模式下由工人执行，本地模式下由这个
/// 进程执行。
struct Counter {
    value: i64,
}

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }
}

/// 清掉示例自己产生的日志目录。
fn clean_own_output() {
    let _ = std::fs::remove_dir_all(DRIVER_LOG_DIR);
}

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
        .log_dir(DRIVER_LOG_DIR)
}

/// 小写 hex，与 C 侧 `hex_encode` 的输出一致（第 4 项断言消息里的 id）。
fn hex_of(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// 断言调用返回 `RayError::Ffi`，且消息里含 `needle`；顺带证明进程存活。
fn expect_ffi_containing<T>(what: &str, result: Result<T, RayError>, needle: &str) {
    match result {
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains(needle),
                "{} 的错误文本里没有 {:?}: {}",
                what,
                needle,
                msg
            );
            println!("  {} → Ffi({})", what, msg);
        }
        Err(other) => panic!("{} 返回了别的错误变体: {}", what, other),
        Ok(_) => panic!("{} 返回了 Ok，本该被拒", what),
    }
}

/// 用 `Ray::actor_create` 建一个 `Counter`（构造函数参数 1）。
fn create_counter(ray: &Ray, opts: &ActorOptions) -> Result<ActorHandle, RayError> {
    let start = serialize(&1i64).expect("serialize the constructor argument");
    ray.actor_create(COUNTER_FACTORY, &[&start[..]], opts)
}

/// 用 `Ray::actor_create_python` 建一个 Python `Counter`（`start` 取默认 0）。
fn create_counter_python(ray: &Ray, opts: &ActorOptions) -> Result<ActorHandle, RayError> {
    ray.actor_create_python("rayrust_test", "Counter", &[], opts)
}

/// 直接调用 C ABI 的 `ray_actor_create_with_options`。
///
/// `ActorOptions::placement_group_id` 是字节，`to_json` 总是把它编成合法的小写 hex，
/// 为空时不输出这个键，`bundle_index` 又是 `i32`，所以「空串 hex」「非 hex」「超出 int
/// 的 `bundle_index`」这些输入只能靠手写的 options JSON 才能进到
/// `parse_actor_options`。这条路径测的是 `ray_c.h` 里 `ray_actor_create_with_options`
/// 承诺的行为：拒绝时错误类别必须是 `RAY_ERROR_KIND_GENERIC`，所以这里直接把类别
/// 读出来断言，不用示例自己造的错误自证。
fn create_actor_with_raw_options(options_json: &str) -> Result<Vec<u8>, RayError> {
    let factory = std::ffi::CString::new(COUNTER_FACTORY).expect("factory 名不含 NUL");
    let options = std::ffi::CString::new(options_json).expect("options JSON 不含 NUL");
    // 与本进程其它创建路径一样传一个构造函数参数：拒绝逻辑失效时工人真的会去构造
    // Counter，缺参数会让它在反序列化处 panic 并留下一份 core。
    let start = serialize(&1i64).expect("serialize the constructor argument");
    let args = rayrust::sys::build_args_array(&[&start]);
    let mut bytes = unsafe {
        rayrust::sys::ray_actor_create_with_options(
            factory.as_ptr(),
            args.as_ptr(),
            args.len(),
            options.as_ptr(),
        )
    };
    if bytes.data.is_null() {
        let kind = rayrust::sys::last_error_kind();
        assert_eq!(
            kind,
            rayrust::sys::ErrorKind::Generic,
            "options JSON {} 的错误类别不是 Generic: {:?}",
            options_json,
            kind
        );
        let message = rayrust::sys::last_error().unwrap_or_else(|| "没有留下错误".to_string());
        return Err(RayError::Ffi(message));
    }
    let id = unsafe { std::slice::from_raw_parts(bytes.data as *const u8, bytes.len) }.to_vec();
    unsafe { rayrust::sys::ray_free_bytes(&mut bytes) };
    Ok(id)
}

/// 直接调用 C ABI 的 `ray_actor_create_python_with_options`。
///
/// 第 10 项用它建一个带名字的 Python actor：这个入口同样不经过 crate 的登记表，
/// `get_actor` 因此是首次登记，读出的语言与创建描述符类型才能被断言。
fn create_python_actor_with_raw_options(options_json: &str) -> Result<Vec<u8>, RayError> {
    let module = std::ffi::CString::new("rayrust_test").expect("module 名不含 NUL");
    let class = std::ffi::CString::new("Counter").expect("class 名不含 NUL");
    let options = std::ffi::CString::new(options_json).expect("options JSON 不含 NUL");
    let mut bytes = unsafe {
        rayrust::sys::ray_actor_create_python_with_options(
            module.as_ptr(),
            class.as_ptr(),
            std::ptr::null(),
            0,
            options.as_ptr(),
        )
    };
    if bytes.data.is_null() {
        let message = rayrust::sys::last_error().unwrap_or_else(|| "没有留下错误".to_string());
        return Err(RayError::Ffi(message));
    }
    let id = unsafe { std::slice::from_raw_parts(bytes.data as *const u8, bytes.len) }.to_vec();
    unsafe { rayrust::sys::ray_free_bytes(&mut bytes) };
    Ok(id)
}

/// 直接调用 C ABI 的 `ray_task_call_with_options`。
///
/// 任务路径与 actor 路径共用 `parse_placement_group_fields`，但两个解码器各自有一行
/// 调用；这条入口就是任务那一行的运行时覆盖。函数名在本地模式与集群模式都不会被执行：
/// 拒绝发生在提交之前。
fn create_task_with_raw_options(options_json: &str) -> Result<Vec<u8>, RayError> {
    let func = std::ffi::CString::new("__rayrust_no_such_function").expect("函数名不含 NUL");
    let options = std::ffi::CString::new(options_json).expect("options JSON 不含 NUL");
    let mut bytes = unsafe {
        rayrust::sys::ray_task_call_with_options(
            func.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            options.as_ptr(),
        )
    };
    if bytes.data.is_null() {
        let kind = rayrust::sys::last_error_kind();
        assert_eq!(
            kind,
            rayrust::sys::ErrorKind::Generic,
            "options JSON {} 的错误类别不是 Generic: {:?}",
            options_json,
            kind
        );
        let message = rayrust::sys::last_error().unwrap_or_else(|| "没有留下错误".to_string());
        return Err(RayError::Ffi(message));
    }
    let id = unsafe { std::slice::from_raw_parts(bytes.data as *const u8, bytes.len) }.to_vec();
    unsafe { rayrust::sys::ray_free_bytes(&mut bytes) };
    Ok(id)
}

/// 1. 默认值：两个构造入口与结构体更新语法的 `bundle_index` 都是 -1。
async fn defaults_check(ray: &Ray) {
    println!("\n--- 1. 默认值 ---");
    assert_eq!(
        ActorOptions::default().bundle_index,
        -1,
        "ActorOptions::default() 的 bundle_index 不是 -1"
    );
    assert_eq!(
        ActorOptions::new().bundle_index,
        -1,
        "ActorOptions::new() 的 bundle_index 不是 -1"
    );
    let grouped = ActorOptions {
        placement_group_id: vec![0x01u8; 18],
        ..Default::default()
    };
    assert_eq!(
        grouped.bundle_index, -1,
        "只给 placement_group_id 时 bundle_index 不是 -1"
    );
    println!("  default()/new()/结构体更新语法的 bundle_index = -1（组内任意 bundle）");

    let handle = counter_actor_create(ray, &ActorOptions::new(), 7i64).expect("创建 Counter");
    let value: i64 = counter_get_sync(ray, &handle)
        .expect("submit get")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read get");
    assert_eq!(value, 7, "默认 options 的 actor 读到 {}", value);
    println!("  ActorOptions::new() 建的 actor → get() = 7");
    println!("  to_json 的形状（默认空串、带组时两个键都在）由单元测试覆盖");
}

/// 2. id 长度与格式：四次拒绝，随后普通 actor 照常工作。
async fn id_format_check(ray: &Ray) {
    println!("\n--- 2. id 长度与格式 ---");
    // 17 与 19 字节：C 侧的长度检查。
    let short = ActorOptions::new().placement_group(vec![0u8; 17], 0);
    expect_ffi_containing("17 字节 id", create_counter(ray, &short), "18 bytes");
    let long = ActorOptions::new().placement_group(vec![0u8; 19], 0);
    expect_ffi_containing("19 字节 id", create_counter(ray, &long), "18 bytes");

    // 空串与非法 hex：`hex_decode` 对两者都返回空串，C 侧报「必须是 18 字节 hex」。
    expect_ffi_containing(
        "空串 id",
        create_actor_with_raw_options(r#"{"placement_group_id":"","bundle_index":0}"#),
        "hex",
    );
    expect_ffi_containing(
        "非 hex id",
        create_actor_with_raw_options(
            r#"{"placement_group_id":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz","bundle_index":0}"#,
        ),
        "hex",
    );

    // 四次拒绝之后运行时仍然可用。
    let handle = counter_actor_create(ray, &ActorOptions::new(), 21i64).expect("创建 Counter");
    let value: i64 = counter_get(ray, &handle)
        .await
        .expect("submit get")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read get");
    assert_eq!(value, 21, "拒绝之后的 actor 读到 {}", value);
    println!("  四次拒绝之后 → 普通 actor 的 get() = 21（进程与运行时都存活）");
}

/// 3. `bundle_index` 的范围：-1，或 0 .. INT_MAX。两种模式都跑。
async fn bundle_index_range_check(ray: &Ray, cluster: bool) {
    println!("\n--- 3. bundle_index 的范围 ---");
    // 范围检查在组解码与本地模式检查之前，所以本地模式同样可达：集群模式用一个真组，
    // 本地模式用任意 18 字节 id，这条分支不需要组存在。
    let pg_id = if cluster {
        let id = ray
            .placement_group_create("rayrust_actor_options_neg", r#"[{"CPU":1}]"#, 0)
            .expect("create the single-bundle placement group");
        println!("  placement_group_create([CPU 1]) → {} 字节 id", id.len());
        id
    } else {
        println!("  本地模式：用任意 18 字节 id（下面这条拒绝来自 bundle_index 的范围检查，");
        println!("  发生在组解码与本地模式检查之前）");
        vec![0u8; 18]
    };
    let pg_hex = hex_of(&pg_id);

    let opts = ActorOptions::new().placement_group(pg_id.clone(), -2);
    let label = if cluster {
        "真组 + bundle_index=-2（类型化 API）"
    } else {
        "任意 id + bundle_index=-2（类型化 API）"
    };
    expect_ffi_containing(label, create_counter(ray, &opts), "-1 or between");

    // 下面五条只能由原始 ABI 造出来：类型化 API 的 bundle_index 是 i32。它们在
    // nlohmann 3.9.1 的 `get<int>()` 下会被截断成合法值，现在必须按原始值拒绝。
    let out_of_range = [
        (
            "-4294967296",
            "= -2^32；get<int>() 截断成 0，会钉在第一个 bundle",
        ),
        (
            "-4294967297",
            "= -(2^32+1)；get<int>() 截断成 -1，会当成任意 bundle",
        ),
        ("-2147483649", "= -(2^31+1)；get<int>() 截断成 2147483647"),
        ("9999999999999", "get<int>() 截断成 1316134911"),
        (
            "18446744073709551615",
            "= 2^64-1；get<int>() 截断成 -1（get<int64_t>() 也会回绕成 -1），会当成任意 bundle",
        ),
    ];
    for (raw, note) in out_of_range {
        let json = format!(
            r#"{{"placement_group_id":"{}","bundle_index":{}}}"#,
            pg_hex, raw
        );
        expect_ffi_containing(
            &format!("原始 ABI bundle_index={}（{}，现在被拒绝）", raw, note),
            create_actor_with_raw_options(&json),
            "-1 or between",
        );
    }

    // 任务路径与 actor 路径共用同一个解码器，但各有一行调用；这条检查任务这一行。
    let task_json = format!(
        r#"{{"placement_group_id":"{}","bundle_index":-4294967296}}"#,
        pg_hex
    );
    expect_ffi_containing(
        "原始 ABI 的任务提交 bundle_index=-4294967296（任务路径共用一个解码器）",
        create_task_with_raw_options(&task_json),
        "-1 or between",
    );

    if cluster {
        ray.placement_group_remove(&pg_id)
            .expect("remove the placement group after the raw ABI check");
        println!("  placement group 已 remove；进程存活");
    } else {
        println!("  进程存活（本地模式没有组需要移除）");
    }
}

/// 4. 未知的合法 18 字节 id：不静默在组外建 actor。
async fn unknown_group_check(ray: &Ray, cluster: bool) {
    println!("\n--- 4. 未知 placement group ---");
    if !cluster {
        println!("  本地模式跳过：本地模式在查组之前就报错（见第 8 项），");
        println!("  查不到组这条分支走不到。");
        return;
    }
    let unknown: Vec<u8> = (0..18u8).collect();
    let unknown_hex = hex_of(&unknown);
    let opts = ActorOptions::new().placement_group(unknown, 0);
    match create_counter(ray, &opts) {
        Err(RayError::Ffi(msg)) => {
            assert!(
                msg.contains("placement group not found for id"),
                "错误文本没有指出查不到组: {}",
                msg
            );
            assert!(
                msg.contains(&unknown_hex),
                "错误文本里没有 id {}: {}",
                unknown_hex,
                msg
            );
            println!("  随机 18 字节 id {} → Ffi({})", unknown_hex, msg);
        }
        Err(other) => panic!("未知组返回了别的错误变体: {}", other),
        Ok(_) => panic!("未知组的 actor 被建出来了（静默在组外创建）"),
    }
    println!("  进程存活：错误发生在创建侧，没有 abort");
}

/// 5 + 6. 孤立的 `bundle_index`：两个入口都在创建之前拒绝。
async fn standalone_bundle_index_check(ray: &Ray) {
    println!("\n--- 5+6. 孤立 bundle_index（两个入口） ---");
    let opts = ActorOptions {
        bundle_index: 2,
        ..Default::default()
    };
    expect_ffi_containing(
        "Ray::actor_create 的孤立 bundle_index=2",
        create_counter(ray, &opts),
        "would be dropped",
    );
    expect_ffi_containing(
        "Ray::actor_create_python 的孤立 bundle_index=2",
        create_counter_python(ray, &opts),
        "would be dropped",
    );
    println!("  两个入口都在进 C 之前拒绝，字段不会被静默丢掉");
}

/// 7. 默认 `bundle_index = -1` 真的不钉在 bundle 0。
async fn default_bundle_index_not_pinned_check(ray: &Ray, cluster: bool) {
    println!("\n--- 7. 默认 bundle_index 不钉在 bundle 0 ---");
    if !cluster {
        println!("  本地模式跳过：placement groups are not supported in local mode，");
        println!("  bundle 与资源都没有意义。");
        return;
    }

    let pg_id = ray
        .placement_group_create(
            "rayrust_actor_options_two_bundles",
            r#"[{"CPU":1},{"CPU":1}]"#,
            0,
        )
        .expect("create the two-bundle placement group");
    println!(
        "  placement_group_create([CPU 1, CPU 1]) → {} 字节 id",
        pg_id.len()
    );

    // a) 绑 bundle 0 的短任务完成：raylet 已经把 bundle 建好、CPU 也从节点扣掉了。
    //    不做这一步就提交长任务，会在「组刚创建、资源还没扣」的窗口里踩空。
    let short_opts = TaskOptions::new()
        .placement_group(pg_id.clone(), 0)
        .resource("CPU", 1.0);
    let short = sleep_for_remote_with_options(ray, &short_opts, SHORT_SLEEP_S)
        .expect("submit the short task bound to bundle 0");
    let started = Instant::now();
    let value = short
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("the short task bound to bundle 0 must finish");
    assert_eq!(value, 0, "sleep_for 返回了 {}", value);
    println!(
        "  绑 bundle 0 的短任务 → 完成，耗时 {:?}（组已生效）",
        started.elapsed()
    );

    // b) 绑 bundle 0 的长任务占住它。
    let long_opts = TaskOptions::new()
        .placement_group(pg_id.clone(), 0)
        .resource("CPU", 1.0);
    let long = sleep_for_remote_with_options(ray, &long_opts, LONG_SLEEP_S)
        .expect("submit the long task bound to bundle 0");
    println!(
        "  绑 bundle 0 的长任务已提交（sleep_for({}s)）",
        LONG_SLEEP_S
    );

    // c) 默认 bundle_index（-1）的 actor 只能在空着的 bundle 1 上跑起来。
    //
    // 两个 actor 都显式要 1 个 CPU：不写 resources 的 actor 不申请 CPU，占住 bundle 的
    // 长任务挡不住它，bundle 0 与 bundle 1 就没有区别了。
    let default_opts = ActorOptions {
        placement_group_id: pg_id.clone(),
        resources: vec![("CPU".to_string(), 1.0)],
        ..Default::default()
    };
    assert_eq!(default_opts.bundle_index, -1, "默认 bundle_index 不是 -1");
    let unpinned =
        counter_actor_create(ray, &default_opts, 3i64).expect("创建默认 options 的 actor");
    let started = Instant::now();
    let value: i64 = match counter_get(ray, &unpinned)
        .await
        .expect("submit get")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
    {
        Ok(value) => value,
        Err(error) => panic!(
            "默认 bundle_index 的 actor 第一次调用没有在期限内完成（bundle 0 已被占住，只有 bundle 1 能跑）: {}",
            error
        ),
    };
    assert_eq!(value, 3, "默认 bundle_index 的 actor 读到 {}", value);
    println!(
        "  默认 bundle_index 的 actor → get() = {}，耗时 {:?}（只能在 bundle 1 上跑）",
        value,
        started.elapsed()
    );

    // d) 显式 bundle_index = 0 的 actor 拿不到被占住的 bundle，第一次调用超时。
    let pinned_opts = ActorOptions::new()
        .placement_group(pg_id.clone(), 0)
        .resource("CPU", 1.0);
    let pinned = counter_actor_create(ray, &pinned_opts, 4i64).expect("创建绑 bundle 0 的 actor");
    let started = Instant::now();
    match counter_get(ray, &pinned)
        .await
        .expect("submit get")
        .get_timeout(BLOCKED_TIMEOUT_MS)
    {
        Err(RayError::Timeout(msg)) => println!(
            "  绑 bundle 0 的 actor get_timeout({}ms) → Timeout（{:?}）：{}",
            BLOCKED_TIMEOUT_MS,
            started.elapsed(),
            msg
        ),
        Err(other) => panic!("绑 bundle 0 的 actor 返回了别的错误变体: {}", other),
        Ok(value) => panic!(
            "bundle 0 被长任务占着，绑 bundle 0 的 actor 却完成了（{}）——绑定的对照不成立",
            value
        ),
    }

    // e) 收尾：cancel 长任务，移除组。
    ray.cancel(long.id(), false, true)
        .expect("cancel the long task with its handle alive");
    println!("  长任务已 cancel");
    ray.placement_group_remove(&pg_id)
        .expect("remove the placement group after the cancellation");
    println!("  placement group 已 remove");

    // f) 正向对照：组没了之后，不绑组的 actor 仍然能建并调通。
    let plain = counter_actor_create(ray, &ActorOptions::new(), 5i64).expect("创建不绑组的 actor");
    let value: i64 = counter_get(ray, &plain)
        .await
        .expect("submit get")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("read get");
    assert_eq!(value, 5, "不绑组的 actor 读到 {}", value);
    println!("  移除组后不绑组的 actor → get() = 5（上面的超时来自绑定）");

    // g) 对照：换一个全新的两 bundle 组，bundle 0 空着。同一条绑定路径（显式
    //    bundle_index = 0 + CPU 1.0）应该很快就绪，所以 d) 的超时来自「bundle 被占住」，
    //    不来自启动慢。
    let fresh_id = ray
        .placement_group_create("rayrust_actor_options_fresh", r#"[{"CPU":1},{"CPU":1}]"#, 0)
        .expect("create the fresh two-bundle placement group");
    println!(
        "  新组 placement_group_create([CPU 1, CPU 1]) → {} 字节 id",
        fresh_id.len()
    );
    let fresh_opts = ActorOptions::new()
        .placement_group(fresh_id.clone(), 0)
        .resource("CPU", 1.0);
    let fresh = counter_actor_create(ray, &fresh_opts, 6i64).expect("创建绑新组 bundle 0 的 actor");
    let started = Instant::now();
    let value: i64 = counter_get(ray, &fresh)
        .await
        .expect("submit get")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("bundle 0 空着时，绑 bundle 0 的 actor 第一次调用必须在期限内完成");
    assert_eq!(value, 6, "新组里绑 bundle 0 的 actor 读到 {}", value);
    println!(
        "  新组里 bundle 0 空着，同一个绑定路径 → get() = {}，耗时 {:?}",
        value,
        started.elapsed()
    );
    ray.placement_group_remove(&fresh_id)
        .expect("remove the fresh placement group");
    println!("  新组已 remove");
}

/// 8. 本地模式：任何带组的 options 都在进 SDK 之前报错。
async fn local_mode_check(ray: &Ray, cluster: bool) {
    println!("\n--- 8. 本地模式 ---");
    if cluster {
        println!("  集群模式跳过：这条断言只在 RayConfig::local() 下成立。");
        return;
    }
    let opts = ActorOptions::new().placement_group(vec![0u8; 18], 0);
    expect_ffi_containing(
        "本地模式 + 带组的 ActorOptions",
        create_counter(ray, &opts),
        "local mode",
    );

    let handle = counter_actor_create(ray, &ActorOptions::new(), 11i64).expect("本地创建 Counter");
    let value: i64 = counter_get(ray, &handle)
        .await
        .expect("submit get")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read get");
    assert_eq!(value, 11, "本地模式的 actor 读到 {}", value);
    println!("  不带组照常：本地 actor 的 get() = 11，进程存活");
}

/// 9. 只有手写 options JSON 才能到达的 C 侧分支：类型错误与缺组。
fn raw_decode_branches_check() {
    println!("\n--- 9. 只能由原始 ABI 到达的解码分支 ---");
    let pg_hex = hex_of(&vec![0u8; 18]);

    expect_ffi_containing(
        "bundle_index 没有组（{\"bundle_index\":0}）",
        create_actor_with_raw_options(r#"{"bundle_index":0}"#),
        "bundle_index requires placement_group_id",
    );
    expect_ffi_containing(
        "bundle_index 不是 JSON 整数（1.5）",
        create_actor_with_raw_options(&format!(
            r#"{{"placement_group_id":"{}","bundle_index":1.5}}"#,
            pg_hex
        )),
        "must be a JSON integer",
    );
    expect_ffi_containing(
        "placement_group_id 不是字符串（123）",
        create_actor_with_raw_options(r#"{"placement_group_id":123}"#),
        "must be a JSON string of hex",
    );
    println!("  三条都在进 SDK 之前拒绝，进程存活（类别都是 Generic）");
}

/// 10. actor 的语言判定：入口要求的语言与 handle 登记的语言必须一致。
async fn actor_language_checks(ray: &Ray, cluster: bool) {
    println!("\n--- 10. actor 的语言判定 ---");

    // 两种模式都跑：16 字节未登记的 id 在 crate 侧被拒，两种模式都要求登记。
    let unknown = [0x11u8; 16];
    expect_ffi_containing(
        "actor_call(16 字节未登记 id)",
        ray.actor_call(&unknown, "increment", &[], &[]),
        "is not registered in this process",
    );

    if !cluster {
        println!(
            "  本地模式到此为止：本地模式没有 Python actor 可建（actor_create_python 拒绝），"
        );
        println!("  actor_call 在本地模式不做语言判定（见 Ray::actor_call 的文档）。");
        return;
    }

    // C++ actor（`actor_create` 建出来的就是 C++ 语言）：Python 入口必须拒。
    let cpp = counter_actor_create(ray, &ActorOptions::new(), 31i64).expect("创建 C++ actor");
    expect_ffi_containing(
        "actor_call_python(C++ actor)",
        ray.actor_call_python(cpp.id(), "get", &[], &[]),
        "is a C++ actor",
    );

    // Python actor：C++ 入口必须拒，Python 入口照常。
    let python = create_counter_python(ray, &ActorOptions::new()).expect("创建 Python Counter");
    expect_ffi_containing(
        "actor_call(Python actor)",
        ray.actor_call(python.id(), "increment", &[], &[]),
        "is a Python actor",
    );

    // 对照：被拒的只是语言不匹配，同一个 Python actor 的 Python 入口照常返回数值。
    let increment_arg = serialize(&2i64).expect("serialize the increment argument");
    let call = ray
        .actor_call_python(python.id(), "increment", &[&increment_arg[..]], &[])
        .expect("submit increment on the Python actor");
    let value: i64 = call
        .cast()
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("read increment");
    assert_eq!(value, 2, "Python actor 的 increment 读到 {}", value);
    println!(
        "  对照：同一个 Python actor 的 actor_call_python → increment(2) = {}",
        value
    );

    // 两次拒绝之后进程与运行时都还在：C++ actor 的普通调用仍然完成。
    let value: i64 = counter_get(ray, &cpp)
        .await
        .expect("submit get")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("read get");
    assert_eq!(value, 31, "拒绝之后 C++ actor 的 get() 读到 {}", value);
    println!(
        "  拒绝之后 C++ actor 的 get() = {}（进程与运行时都存活）",
        value
    );

    // `get_actor` 的集群路径：语言与创建描述符类型都从 SerializeActorHandle 的字节里
    // 读。两个 actor 都走原始 C ABI 建，所以它们的 id 不在 crate 的登记表里，`get_actor`
    // 拿到的是首次登记：把 `ray_get_actor` 读出的语言改坏（例如恒报 CPP），下面两条对
    // 每种语言的断言都会失败。
    let cpp_name = "__rayrust_named_cpp_actor";
    let cpp_raw_id = create_actor_with_raw_options(&format!(r#"{{"name":"{}"}}"#, cpp_name))
        .expect("原始 ABI 建带名字的 C++ actor");
    expect_ffi_containing(
        "原始 C ABI 建出来的 actor 的 actor_call",
        ray.actor_call(&cpp_raw_id, &format!("{}::get", COUNTER_FACTORY), &[], &[]),
        "is not registered in this process",
    );
    let cpp_named = ray
        .get_actor(cpp_name, "")
        .expect("get_actor(C++ 名字)")
        .expect("找不到刚建的带名字 C++ actor");
    assert_eq!(
        cpp_named.id(),
        &cpp_raw_id[..],
        "get_actor 拿到的 C++ actor id 与创建时不同"
    );
    expect_ffi_containing(
        "get_actor 取回的 C++ actor → actor_call_python",
        ray.actor_call_python(cpp_named.id(), "get", &[], &[]),
        "is a C++ actor",
    );
    let value: i64 = counter_get(ray, &cpp_named)
        .await
        .expect("提交 get")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("读 get");
    assert_eq!(
        value, 1,
        "get_actor 取回的 C++ actor 的 get() 读到 {}",
        value
    );
    println!("  对照：get_actor 取回的 C++ actor → get() = {}", value);

    let python_name = "__rayrust_named_python_actor";
    let python_raw_id =
        create_python_actor_with_raw_options(&format!(r#"{{"name":"{}"}}"#, python_name))
            .expect("原始 ABI 建带名字的 Python actor");
    expect_ffi_containing(
        "原始 C ABI 建出来的 Python actor 的 actor_call",
        ray.actor_call(&python_raw_id, "increment", &[], &[]),
        "is not registered in this process",
    );
    let python_named = ray
        .get_actor(python_name, "")
        .expect("get_actor(Python 名字)")
        .expect("找不到刚建的带名字 Python actor");
    assert_eq!(
        python_named.id(),
        &python_raw_id[..],
        "get_actor 拿到的 Python actor id 与创建时不同"
    );
    expect_ffi_containing(
        "get_actor 取回的 Python actor → actor_call",
        ray.actor_call(python_named.id(), "increment", &[], &[]),
        "is a Python actor",
    );
    let increment_arg = serialize(&4i64).expect("serialize the increment argument");
    let call = ray
        .actor_call_python(python_named.id(), "increment", &[&increment_arg[..]], &[])
        .expect("提交 get_actor 取回的 Python actor 的 increment");
    let value: i64 = call
        .cast()
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("读 increment");
    assert_eq!(
        value, 4,
        "get_actor 取回的 Python actor 的 increment(4) 读到 {}",
        value
    );
    println!(
        "  对照：get_actor 取回的 Python actor → increment(4) = {}",
        value
    );
}

/// 11. actor 写了别的资源时要预留 1 个 CPU（#55）。
async fn default_actor_cpu_check(ray: &Ray, cluster: bool) {
    println!("\n--- 11. actor 的默认 CPU ---");
    if !cluster {
        println!("  本地模式跳过：本地模式给任务规格传的是空资源表，资源不参与调度，");
        println!("  两个 CPU 占不占住都是同一个结果。");
        return;
    }

    // a) 两个默认 CPU 的任务（`Ray::task_call` 默认要 1.0）占住两个 CPU。
    let first = sleep_for_remote_with_options(ray, &TaskOptions::new(), CPU_HOLD_S)
        .expect("submit the first CPU-holding task");
    let second = sleep_for_remote_with_options(ray, &TaskOptions::new(), CPU_HOLD_S)
        .expect("submit the second CPU-holding task");
    println!(
        "  两个默认 CPU 的任务已提交（sleep_for({}s)），等 {} s 让它们各占一个 CPU",
        CPU_HOLD_S, CPU_HOLD_SETTLE_S
    );
    tokio::time::sleep(Duration::from_secs(CPU_HOLD_SETTLE_S)).await;

    let mut actors: Vec<ActorHandle> = Vec::new();
    // 排队的调用：名、结果、构造参数（释放 CPU 后应当读到它）。
    let mut queued: Vec<(&str, ObjectRef<i64>, i64)> = Vec::new();

    // b) 对照：显式写 CPU 1 的 actor 拿不到 CPU，第一次调用超时。
    let explicit_opts = ActorOptions::new()
        .resource("Custom", 1.0)
        .resource("CPU", 1.0);
    let explicit =
        counter_actor_create(ray, &explicit_opts, 41i64).expect("创建显式 CPU 1 的 actor");
    actors.push(explicit.clone());
    let call = counter_get_sync(ray, &explicit).expect("submit get on the CPU 1 actor");
    let started = Instant::now();
    match call.get_timeout(BLOCKED_TIMEOUT_MS) {
        Err(RayError::Timeout(msg)) => println!(
            "  显式 Custom 1 + CPU 1 的 actor → Timeout（{:?}）：对照成立，两个 CPU 都被占住（{}）",
            started.elapsed(),
            msg
        ),
        Err(other) => panic!("显式 CPU 1 的 actor 返回了别的错误变体: {}", other),
        Ok(value) => panic!(
            "显式 CPU 1 的 actor 在两个 CPU 都被占住时完成了（{}）——占 CPU 的对照不成立",
            value
        ),
    }
    queued.push(("显式 CPU 1", call, 41i64));

    // c) 本缺陷的判别项：只写 Custom 1 的 actor 要被补上 1 个 CPU。
    let custom_only = ActorOptions::new().resource("Custom", 1.0);
    let custom =
        counter_actor_create(ray, &custom_only, 42i64).expect("创建只有 Custom 1 的 actor");
    actors.push(custom.clone());
    let call = counter_get_sync(ray, &custom).expect("submit get on the Custom 1 actor");
    let started = Instant::now();
    match call.get_timeout(BLOCKED_TIMEOUT_MS) {
        Err(RayError::Timeout(msg)) => println!(
            "  只有 Custom 1 的 actor → Timeout（{:?}）：补上的 1 个 CPU 拿不到（{}）",
            started.elapsed(),
            msg
        ),
        Err(other) => panic!("只有 Custom 1 的 actor 返回了别的错误变体: {}", other),
        Ok(value) => panic!(
            "只有 Custom 1 的 actor 在两个 CPU 都被占住时完成了（{}）——它没有被补上 CPU",
            value
        ),
    }
    queued.push(("只有 Custom 1", call, 42i64));

    // d) 显式 CPU 0 退出这一默认：不占 CPU，两个核被占住也照样完成。
    let cpu_zero_opts = ActorOptions::new()
        .resource("Custom", 1.0)
        .resource("CPU", 0.0);
    let cpu_zero =
        counter_actor_create(ray, &cpu_zero_opts, 43i64).expect("创建显式 CPU 0 的 actor");
    actors.push(cpu_zero.clone());
    let value: i64 = counter_get_sync(ray, &cpu_zero)
        .expect("submit get on the CPU 0 actor")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("显式 CPU 0 的 actor 不占 CPU，第一次调用必须在期限内完成");
    assert_eq!(value, 43, "显式 CPU 0 的 actor 读到 {}", value);
    println!(
        "  显式 Custom 1 + CPU 0 的 actor → get() = {}（不占 CPU）",
        value
    );

    // e) 不写 resources：不占 CPU，照样完成。
    let plain = counter_actor_create(ray, &ActorOptions::new(), 44i64)
        .expect("创建不写 resources 的 actor");
    actors.push(plain.clone());
    let value: i64 = counter_get_sync(ray, &plain)
        .expect("submit get on the actor without resources")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("不写 resources 的 actor 不占 CPU，第一次调用必须在期限内完成");
    assert_eq!(value, 44, "不写 resources 的 actor 读到 {}", value);
    println!("  不写 resources 的 actor → get() = {}（不占 CPU）", value);

    // f) 只有内存键：不补 CPU，照样完成。
    let memory_only_opts = ActorOptions::new().resource("memory", 1.0);
    let memory_only = counter_actor_create(ray, &memory_only_opts, 45i64)
        .expect("创建只有 memory 1 的 actor");
    actors.push(memory_only.clone());
    let value: i64 = counter_get_sync(ray, &memory_only)
        .expect("submit get on the memory-only actor")
        .get_timeout(ACTOR_START_TIMEOUT_MS)
        .expect("只有 memory 的 actor 不补 CPU，第一次调用必须在期限内完成");
    assert_eq!(value, 45, "只有 memory 的 actor 读到 {}", value);
    println!("  只有 memory 1 的 actor → get() = {}（不补 CPU）", value);

    // g) 把两个 CPU 还回去，前面两条排队的调用都要完成。
    ray.cancel(first.id(), false, true)
        .expect("cancel the first CPU-holding task with its handle alive");
    ray.cancel(second.id(), false, true)
        .expect("cancel the second CPU-holding task with its handle alive");
    println!(
        "  两个占 CPU 的任务已 cancel，等 {} s",
        CPU_RELEASE_SETTLE_S
    );
    tokio::time::sleep(Duration::from_secs(CPU_RELEASE_SETTLE_S)).await;
    for (label, call, expected) in queued {
        let value: i64 = call.get_timeout(ACTOR_START_TIMEOUT_MS).unwrap_or_else(|error| {
            panic!("释放 CPU 之后 {} 的 actor 调用仍没完成: {}", label, error)
        });
        assert_eq!(value, expected, "{} 的 actor 读到 {}", label, value);
        println!(
            "  释放 CPU 之后 {} 的 actor → get() = {}（前面的超时来自 CPU 被占住）",
            label, value
        );
    }

    // h) 收尾：这一项建的 actor 全 kill，不给下一项留下活 actor。
    for handle in &actors {
        ray.kill_actor(handle, false)
            .unwrap_or_else(|error| panic!("kill_actor 失败: {}", error));
    }
    println!("  第 11 项建的 {} 个 actor 已 kill", actors.len());
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== ActorOptions 的 placement group 字段 ({}) ===", mode);
    clean_own_output();
    let cluster = mode == "cluster";
    let config = if cluster {
        cluster_config()
    } else {
        RayConfig::local()
    };
    let ray = Ray::connect(&config).expect("Ray::connect");

    defaults_check(&ray).await;
    id_format_check(&ray).await;
    bundle_index_range_check(&ray, cluster).await;
    unknown_group_check(&ray, cluster).await;
    standalone_bundle_index_check(&ray).await;
    default_bundle_index_not_pinned_check(&ray, cluster).await;
    local_mode_check(&ray, cluster).await;
    raw_decode_branches_check();
    actor_language_checks(&ray, cluster).await;
    default_actor_cpu_check(&ray, cluster).await;

    // 让 tokio 的 blocking pool 里的 FFI 调用有机会收尾，避免退出竞态。
    tokio::time::sleep(Duration::from_millis(200)).await;

    clean_own_output();
    println!("\n✓ ActorOptions 检查全部通过");
}

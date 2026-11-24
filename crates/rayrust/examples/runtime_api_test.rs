//! Example: #8 补齐的 `RayRuntime` 接口——`is_local_mode`、placement group 的等待与
//! 查询、actor handle 的序列化、`exit_actor`。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/runtime_api_test cluster
//! ./target/release/examples/runtime_api_test local
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP、RAY_WORKER_SO。
//!
//! # 检查项
//!
//! 1. `is_local_mode`：集群 `false`、本地 `true`。同一项还断言 `namespace()`：集群返回
//!    job config 的 namespace，本地返回 init 时配置的那一个（两边都配了固定名字）。
//! 2. placement group 全流程（集群）：建带名字的单 bundle 组（`CPU 0.5`、
//!    `STRICT_PACK`）→ `wait_ready(10)` 为 `true` → `by_id` 的 id 与建组返回一致、
//!    name 一致、`state == 2`（CREATED）、`strategy` 与建组时传的一致 → `by_name`
//!    同上 → `placement_groups()` 里按 id 找到它 → 移除 → remove 返回后的第一次查询里
//!    `by_id` 的 `state == 3`（REMOVED）、`by_name` 报错、`placement_groups()` 里
//!    那一项也是 REMOVED（移除是同步 RPC，GCS 先删名字映射、改状态、写回表项，回调里
//!    才回复）。
//! 3. 非法与未知（集群）：17 / 19 / 空字节 id → `Ffi` 错误，随后合法调用照常完成；
//!    未知 18 字节 id → 错误且消息里带 id；未知名字 → 错误且消息里带名字。
//!    另加一条 C ABI 直接调用：`rayrust::sys::ray_actor_kill` 传 3 字节 actor id →
//!    `last_error()` 说明 16 字节，进程存活（`Ray::kill_actor` 只收 `&ActorHandle`，
//!    id 恒为 16 字节，短 id 只能从 C ABI 传进来；这个集群里它在修复前会命中
//!    `ActorID::FromBinary` 的 RAY_CHECK 终止进程）；随后第 4 项合法 actor 的
//!    `kill_actor` 照旧工作。最后是 actor 名字的边界：空名字与带内部 NUL 的名字都报
//!    `Ffi`（错误说明名字为空、进程存活），只有空白字符的名字合法且按原样查（`Ok(None)`）。
//! 3c. `placement_group_remove` 的守卫与错误出口（两个模式都跑）：3 字节与空字节 id →
//!    `Ffi` 且消息说明 18 字节（crate 内侧先拒，进不了 FFI）；从 C ABI 直接调同一入口传
//!    3 字节与 `(NULL, 0)`/`(NULL, 18)` → 返回 -1、`last_error()` 说明原因、进程存活（C 入口
//!    那道检查与空指针检查）；未知的 18 字节 id → `Ok(())`；同一个组 remove 两次 → 两次都
//!    `Ok(())`。两项在不同模式下的分量不同：集群模式第一次走到「从登记表移除」、第二次走
//!    「已经移除的分支」；本地模式没有查询能看那个 map，这一段只证明建组返回的 18 字节 id
//!    能连续走两次 erase 而不报错。
//! 4. `exit_actor`：`max_restarts=2` 的 actor，方法里调用 `exit_actor()`。
//!    集群模式：`exit()` 的对象用 `get_timeout` 取（退出路径不写结果，无界 `get` 会永远等），
//!    随后同一 handle 的普通方法必须失败；再用一个独立 actor 做正向对照——
//!    `increment(2)` 把计数推到 9、`kill_actor(no_restart = false)` 之后有界重试
//!    （200ms × 15）直到计数回到初值 7，证明这个集群真的会重启、
//!    「`exit()` 之后拿不到值」不是因为重启机制失效。本地模式：`exit_actor()` 是空操作，
//!    `increment_and_exit(2)` 返回自己的值 9、actor 仍然可用且状态不变。
//! 5. 任务里调 `exit_actor()`：集群模式任务失败且消息里含 `exit_actor`；本地模式
//!    `exit_actor()` 是空操作，任务照常返回它自己的值（1）。
//! 6. handle 交接（集群）：actor 方法返回数值；`handle.serialize()` 的字节与裸
//!    `handle.id()` 一起交给工人任务、原样交回；驱动从**交回来的字节**复原 handle、
//!    调方法、断言数值。另加载荷校验的覆盖：畸形字节、`actor_id` 3 字节、
//!    `creation_job_id` 3 字节、缺 `actor_id`、缺 `creation_job_id`、5 字节 key 的值
//!    超过 32 位（上游会将其截成 `actor_id`）、非最短编码的长度前缀、嵌套消息与
//!    `string` 内容损坏（`owner_address`、`FunctionDescriptor` 及其
//!    `python_function_descriptor`、`labels` entry 与它的 value、`name`，每一条都先放
//!    3 字节 `actor_id`、末尾再放 16 字节的，以及一条只出现一次的剥离型），长度合法但
//!    内容全 0x00 或全 0xff，都报 `Ffi` 且错误文本各自能区分；手写的结构良好的载荷能
//!    登记成 16 字节 id。
//! 7. 本地模式：第 2、3 项与第 6 项的集群部分跳过并打印原因；第 1 项、第 4、5、8 项的
//!    本地模式断言、本地模式下 `wait_ready` 与三个查询的报错、`kill_actor`、`cancel`、
//!    `actor_create_python` 与 `actor_call_python` 的报错（这些调用被拒之后同一个 actor
//!    仍然可用，计数逐次上涨）、`actor_create_python` 带名字被拒之后 `get_actor` 读不到
//!    那个名字、两个 handle 接口的报错实际跑；`from_serialized` 对结构良好的载荷报本地
//!    模式、对畸形载荷报载荷错误，actor 名字的边界与第 3 项跑同一组断言。
//! 8. 构造函数里调 `exit_actor()`：集群模式 `actor_create` 照常返回 handle、第一次调用
//!    报 `RayError::Actor`；本地模式那个调用是空操作，actor 正常创建、方法拿到值 11。
//!
//! # 实测出来的上游行为（第 2、4、6 项按它写）
//!
//! - 移除一个 placement group 之后 GCS **不会删掉表项**：`RemovePlacementGroup`
//!   把行改成 `REMOVED` 之后 `PlacementGroupTable().Put(...)`
//!   （`src/ray/gcs/gcs_placement_group_manager.cc:458-469`），表项一直留着，
//!   所以移除后 `by_id` 仍然成功、`state` 是 3；名字映射在这一步被删掉
//!   （同文件 414-427），`by_name` 因此报错。
//! - `wait_ready` 的时限原样交给 GCS：正数与 `0` 等这次 GCS 查询（`0` 在已就绪的组上
//!   实测仍然返回 `true`），负数的截止时间已经过期，即使组已就绪也返回 `false`。
//! - 移除一个不存在或已经移除的组两个模式都返回 `Ok(())`：GCS 在表里找不到这个 id 时
//!   直接回 `Status::OK()`（`src/ray/gcs/gcs_placement_group_manager.cc:404-408`），本地
//!   模式则是在 map 上 `erase`（`local_mode_task_submitter.cc:182-184`），未知 id 不命中
//!   任何表项。`CoreWorker::RemovePlacementGroup` 的 status 只在 GCS RPC 本身失败时非
//!   OK（超时被改写成带 id 的文本，`core_worker.cc:2293-2306`），也就是第 3c 项的
//!   `Ffi` 出口在正常集群上不会被这两种输入触发。
//! - actor id 是 16 字节（12 个唯一字节 + 4 字节 JobID，`src/ray/common/id.h:124-142`），
//!   不是 ObjectID 的 28 字节。
//! - 工人进程里 `ActorHandle::from_serialized` 把 handle 登记进本工人的
//!   `ActorManager`，紧接着按同一个裸 id 调用 `ray_actor_call` 也能提交成功；第 6 项同
//!   时断言工人侧两条路径都返回 `Ok(...)` 以及驱动侧复原后的调用结果。
//! - `from_serialized` 的载荷里只有 `actor_id`（字段 1，16 字节）与
//!   `creation_job_id`（字段 4，4 字节）是定长 id，载荷里就这两个尺寸会被上游按
//!   `RAY_CHECK` 检查：`actor_id` 不对在登记阶段终止（`actor_manager.cc:33` →
//!   `id.h:435`），`creation_job_id` 不对在第一次调用时终止（`core_worker.cc:2374`）。
//!   缺 `creation_job_id` 不会终止调用方进程：nil job id 让 actor 的 worker 在
//!   `WorkerContext::SetCurrentTask` 的 job id 检查处终止（`context.cc:295`），调用方
//!   收到 `ActorError`；这个仓库里用一个建 detached actor 的进程与一个剥掉字段 4 再调用
//!   的进程实测过（一个进程建 detached actor 并写出载荷，另一个进程删掉字段 4 后登记
//!   并调用）。上游丢弃 `ParseFromString` 的结果
//!   （`actor_handle.cc:66`），中止之前写入的字段才是登记的那份，所以 C 侧在进 SDK 之前
//!   按 protobuf wire format 自己走一遍：已知字段按 Ray 2.51.1 的 schema 校验内容（嵌套
//!   消息逐层递归、proto3 的 `string` 校验 UTF-8），未知字段按 wire type 跳过。
//!   只量顶层长度挡不住的两类载荷：先放 3 字节 `actor_id`、再放一段损坏的嵌套消息或
//!   非法 UTF-8 的 `string`、最后放 16 字节 `actor_id`，上游登记的仍是那个 3 字节值并在
//!   `id.h:435` 终止进程；把 `creation_job_id` 排在损坏内容之后，上游从未写入它，登记出
//!   nil job id 的 handle（实测：一个进程登记后 `serialize()` 读回 20 字节，只有
//!   `actor_id` 与一个空的 owner_address）。
//!   两个编码层面的差异也实测过：5 字节但值超过 32 位的 tag 会被上游截成 `uint32`
//!   （`0x10000000a` 变成 `actor_id`，随后的短内容在登记阶段终止进程）；超过 5 字节
//!   的 key、达到 2^31 的长度与超过 5 字节的长度前缀让上游的 `ParseFromString` 失败，
//!   5 字节以内但非最短编码的 key 与长度上游会接受。C 侧把这些编码都当载荷畸形拒掉。
//! - 只有字段 1 与 4 的载荷会被登记：字段 5 缺席按 proto3 的默认值算 `PYTHON`，字段 6
//!   缺席则 handle 里没有创建描述符。这种 handle 的 `actor_call` 报语言不符，
//!   `actor_call_python` 必须在进 SDK 之前被拒：`AbstractRayRuntime::CallActor` 把创建
//!   描述符按 `PythonFunctionDescriptor` 读
//!   （`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`，`As` 是 `reinterpret_cast`，
//!   `src/ray/common/function_descriptor.h:67-70`），字段 6 缺席时描述符是
//!   `EmptyFunctionDescriptor`，`ModuleName()`/`ClassName()` 读越过对象末尾；实测
//!   SIGSEGV，`PC` 在 `CallActor()`，退出码 139。第 6 项断言拒掉它并且进程存活。
//! - 集群 `get_actor("")` 与 `get_actor("a\0b")`（`to_cstring` 把带内部 NUL 的名字
//!   变成空串，#27）没有守卫时会在 `core_worker.cc:2594` 的 `RAY_CHECK(!name.empty())`
//!   终止进程，本地模式则返回 `Ok(None)`；现在两种模式都报 `Ffi`。
//!   `get_actor(" ", "")` 是合法的未命中，返回 `Ok(None)`。空 namespace 合法：
//!   `NativeTaskSubmitter::GetActor` 用 job config 的 namespace 顶替它。
//! - 本地模式没有 core worker，`exit_actor()` 在那里是空操作：退出状态异常会被
//!   `TaskExecutor::Invoke` 的 `catch (std::exception &)` 打包成错误载荷，返回 `()` 的
//!   方法看到 `Ok(())`、返回值的方法看到一条与退出无关的 `Serialization` 错误。

use std::time::Duration;

use rayrust::prelude::*;

/// 与 worker .so 的 `exit_from_task` 同名同签名：集群模式下由工人执行。
#[rayrust::remote]
fn exit_from_task() -> i64 {
    rayrust::exit_actor();
    1
}

/// 与 worker .so 的 `handle_handoff` 同名同签名：收下两段字节、原样交回，并报告
/// 工人进程里复原/调用的结果。
#[rayrust::remote]
fn handle_handoff(serialized: Vec<u8>, raw_id: Vec<u8>) -> (Vec<u8>, Vec<u8>, String, String) {
    let restored = match ActorHandle::from_serialized(&serialized) {
        Ok(handle) => format!("Ok({} 字节 id)", handle.id().len()),
        Err(error) => format!("Err({})", error),
    };
    (
        serialized,
        raw_id,
        restored,
        "本地进程不重复尝试".to_string(),
    )
}

/// `Counter` 的方法名，第 6 项的裸 id 对照要用完整名字提交。
const COUNTER_GET: &str = "__rayrust_actor_factory_counter::get";

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

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }
}

/// 与 worker .so 的 `SelfExiting` 同名同实现：`exit()` 里调用 `exit_actor()`。
struct SelfExiting {
    value: i64,
}

#[rayrust::actor]
impl SelfExiting {
    fn new(start: i64) -> Self {
        SelfExiting { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn exit(&self) {
        rayrust::exit_actor();
    }

    // 返回值的方法里调用 `exit_actor()`：本地模式用它验证「调用是空操作」这条，
    // 因为方法自己的返回值会被解出来（退出状态路径上那里得到的是 `Serialization` 错误）。
    fn increment_and_exit(&mut self, n: i64) -> i64 {
        self.value += n;
        rayrust::exit_actor();
        self.value
    }
}

/// 与 worker .so 的 `CtorExiting` 同名同实现：`new()` 里调用 `exit_actor()`。
struct CtorExiting {
    value: i64,
}

#[rayrust::actor]
impl CtorExiting {
    fn new(start: i64) -> Self {
        rayrust::exit_actor();
        CtorExiting { value: start }
    }

    fn get(&self) -> i64 {
        self.value
    }
}

/// 读一个对象的期限（毫秒）。
const GET_TIMEOUT_MS: i32 = 5000;

/// 等 actor 退出、等任务失败、等交接任务的期限（毫秒）。
const EXIT_TIMEOUT_MS: i32 = 15000;

/// 建组时传的 strategy：2 = STRICT_PACK。取非默认值，`strategy` 断言才有区分力。
const PG_STRATEGY: i32 = 2;

/// `PlacementGroupState::CREATED`。
const PG_STATE_CREATED: i32 = 2;

/// `PlacementGroupState::REMOVED`。
const PG_STATE_REMOVED: i32 = 3;

/// 移除走的是同步 RPC：`SyncRemovePlacementGroup` 在 GCS 把状态改成 `REMOVED`、
/// 删掉名字映射、写回表项之后才回复，所以调用返回后的第一次查询就该看到结果，
/// 不需要轮询。

/// 重启对照的轮数与间隔：`kill_actor(no_restart = false)` 之后等状态回到初值。
const RESTART_POLL_TIMES: usize = 15;
const RESTART_POLL_INTERVAL_MS: u64 = 200;

/// 重启对照里单次读数的期限（毫秒）：重启期间提交的调用可能要多等一会儿。
const RESTART_GET_TIMEOUT_MS: i32 = 3000;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// `rpc::ActorHandle` 的字段号：`actor_id = 1`、`creation_job_id = 4`
/// （`src/ray/protobuf/core_worker.proto:27-79`）。
const ACTOR_HANDLE_ACTOR_ID: u32 = 1;
const ACTOR_HANDLE_CREATION_JOB_ID: u32 = 4;

/// protobuf 的长度前缀字段编码：字段号左移 3 位再加 wire type 2（长度前缀），
/// 后面是一字节长度与载荷。这里用到的载荷都短于 128 字节，一字节长度就够。
fn proto_bytes_field(field_number: u32, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() < 128, "载荷长度必须能用一字节的长度前缀表示");
    let mut out = vec![((field_number << 3) | 2) as u8, payload.len() as u8];
    out.extend_from_slice(payload);
    out
}

/// 结构良好的 handle 载荷：`actor_id` 16 字节 + `creation_job_id` 4 字节。
fn well_formed_handle_payload(actor_id_byte: u8, job_id_byte: u8) -> Vec<u8> {
    let mut payload = proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[actor_id_byte; 16]);
    payload.extend(proto_bytes_field(
        ACTOR_HANDLE_CREATION_JOB_ID,
        &[job_id_byte; 4],
    ));
    payload
}

/// 嵌套消息的字段号：`owner_address = 3`、`actor_creation_task_function_descriptor =
/// 6`、`name = 10`、`labels = 15`（`src/ray/protobuf/core_worker.proto:34-75`）。
const ACTOR_HANDLE_OWNER_ADDRESS: u32 = 3;
const ACTOR_HANDLE_FUNCTION_DESCRIPTOR: u32 = 6;
const ACTOR_HANDLE_NAME: u32 = 10;
const ACTOR_HANDLE_LABELS: u32 = 15;

/// `rpc::FunctionDescriptor` 的 `python_function_descriptor` 字段
/// （`src/ray/protobuf/common.proto:158-164`）。
const FUNCTION_DESCRIPTOR_PYTHON: u32 = 2;

/// `map<string, string> labels` entry 的 `value` 字段
/// （`src/ray/protobuf/core_worker.proto:73-75`）。
const LABELS_ENTRY_VALUE: u32 = 2;

/// 断言载荷被 `from_serialized` 拒掉，且错误文本含指定的每一段。
fn assert_payload_rejected(label: &str, payload: &[u8], expected: &[&str]) {
    match ActorHandle::from_serialized(payload) {
        Err(RayError::Ffi(message)) => {
            for word in expected {
                assert!(
                    message.contains(word),
                    "{} 的错误里没有「{}」: {}",
                    label,
                    word,
                    message
                );
            }
            println!("  {} → Ffi({})", label, message);
        }
        Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
        Ok(handle) => panic!("{} 被收下了，得到 {} 字节 id", label, handle.id().len()),
    }
}

/// handle 载荷的校验：畸形、字段长度不对、缺字段、嵌套消息与字符串内容损坏、
/// 长度合法但 nil、手写的合法载荷。除了全 `0x00`、全 `0xff` 与对照三条，其余都在
/// 载荷校验处被拒（不需要运行时）；那三条过了载荷校验，会真的登记，需要集群运行时。
fn actor_handle_payload_checks(ray: &Ray) {
    println!("\n--- 载荷校验（rpc.ActorHandle 的两个 id 字段）---");
    // 0x5a 是字段 11、wire type 2，长度前缀 90 超出剩下的 62 字节。
    assert_payload_rejected(
        "from_serialized(64 字节 0x5a)",
        &[0x5au8; 64],
        &["well-formed", "90"],
    );
    // 字段 1 `actor_id` 只有 3 字节：登记阶段会命中 ActorID::FromBinary 的 RAY_CHECK。
    assert_payload_rejected(
        "from_serialized(actor_id 3 字节)",
        &proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[1, 2, 3]),
        &["actor_id", "3 bytes", "16 bytes"],
    );
    // 字段 4 `creation_job_id` 只有 3 字节：第一次调用会命中 JobID::FromBinary 的
    // RAY_CHECK，所以这条只能在 from_serialized 断言。
    let mut short_job_id = proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[0x11; 16]);
    short_job_id.extend(proto_bytes_field(ACTOR_HANDLE_CREATION_JOB_ID, &[1, 2, 3]));
    assert_payload_rejected(
        "from_serialized(actor_id 16 字节 + creation_job_id 3 字节)",
        &short_job_id,
        &["creation_job_id", "3 bytes", "4 bytes"],
    );
    // 缺字段 4：上游接受，但 nil job id 让 actor 的 worker 在 job id 检查处终止，
    // 所以这条只能在 from_serialized 断言。
    assert_payload_rejected(
        "from_serialized(只有 actor_id 16 字节)",
        &proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[0x11; 16]),
        &["no creation_job_id field"],
    );
    // 缺字段 1：上游把它登记成 nil actor。
    assert_payload_rejected(
        "from_serialized(只有 creation_job_id 4 字节)",
        &proto_bytes_field(ACTOR_HANDLE_CREATION_JOB_ID, &[0x22; 4]),
        &["no actor_id field"],
    );
    // 长度都合法、内容全 0：不属于任何 actor，由登记后的检查拒掉。
    assert_payload_rejected(
        "from_serialized(actor_id 16 个 0x00 + creation_job_id 4 个 0x00)",
        &well_formed_handle_payload(0x00, 0x00),
        &["all 0x00", "no actor has"],
    );
    // 长度都合法、内容全 0xff：就是 nil id，同样被登记后的检查拒掉。
    assert_payload_rejected(
        "from_serialized(actor_id 16 个 0xff + creation_job_id 4 个 0x00)",
        &well_formed_handle_payload(0xff, 0x00),
        &["all 0xff", "the nil id"],
    );
    // 字段号超出 32 位的 key：上游把 tag 读成 uint32，`0x10000000a`（5 字节编码）
    // 会被截成 `actor_id`（字段 1），随后的 3 字节覆盖前面的合法值。这种 key 必须在
    // 这里就被拒掉，否则集群下会在登记阶段终止进程。
    let mut overflowing_key = well_formed_handle_payload(0x11, 0x22);
    overflowing_key.extend_from_slice(&[0x8a, 0x80, 0x80, 0x80, 0x10, 0x03, 1, 2, 3]);
    assert_payload_rejected(
        "from_serialized(5 字节 key，值超过 32 位，随后 3 字节)",
        &overflowing_key,
        &["field key", "32 bits"],
    );
    // 非最短编码的长度前缀：protobuf 自己的序列化不会产生这种编码，长度 16 用 4 个字节
    // 表示（`0x90 0x80 0x80 0x00`）。
    let mut non_minimal_length = vec![0x0au8];
    non_minimal_length.extend_from_slice(&[0x90, 0x80, 0x80, 0x00]);
    non_minimal_length.extend_from_slice(&[0x11u8; 16]);
    non_minimal_length.extend(proto_bytes_field(ACTOR_HANDLE_CREATION_JOB_ID, &[0x22; 4]));
    assert_payload_rejected(
        "from_serialized(长度前缀 4 字节，非最短编码)",
        &non_minimal_length,
        &["length prefix", "not minimally encoded"],
    );
    // 嵌套消息与 `string` 字段的内容：上游的 `ParseFromString` 在第一条损坏的嵌套消息
    // 或非法 UTF-8 的 `string` 处中止，登记进去的是中止之前写入的字段。下面几条都在
    // 中间放损坏内容、末尾再放一个 16 字节的 `actor_id`：只看最后一次出现的长度会放行，
    // 上游登记的却是前面那个 3 字节值，登记阶段在 `id.h:435` 终止进程。
    let bypass = |broken: Vec<u8>| {
        let mut payload = proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[1, 2, 3]);
        payload.extend(broken);
        payload.extend(proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[0x11; 16]));
        payload.extend(proto_bytes_field(ACTOR_HANDLE_CREATION_JOB_ID, &[0x22; 4]));
        payload
    };
    assert_payload_rejected(
        "from_serialized(3 字节 actor_id + 损坏 owner_address + 16 字节 actor_id)",
        &bypass(proto_bytes_field(ACTOR_HANDLE_OWNER_ADDRESS, &[0xff])),
        &["owner_address", "truncated"],
    );
    assert_payload_rejected(
        "from_serialized(3 字节 actor_id + 损坏 FunctionDescriptor + 16 字节 actor_id)",
        &bypass(proto_bytes_field(ACTOR_HANDLE_FUNCTION_DESCRIPTOR, &[0xff])),
        // 冒号紧跟字段名，才能与下面那条「子消息里损坏」的路径区分开。
        &["actor_creation_task_function_descriptor: ", "truncated"],
    );
    assert_payload_rejected(
        "from_serialized(3 字节 actor_id + 损坏 PythonFunctionDescriptor + 16 字节 actor_id)",
        &bypass(proto_bytes_field(
            ACTOR_HANDLE_FUNCTION_DESCRIPTOR,
            &proto_bytes_field(FUNCTION_DESCRIPTOR_PYTHON, &[0xff]),
        )),
        &["python_function_descriptor: ", "truncated"],
    );
    assert_payload_rejected(
        "from_serialized(3 字节 actor_id + 损坏 labels entry + 16 字节 actor_id)",
        &bypass(proto_bytes_field(ACTOR_HANDLE_LABELS, &[0xff])),
        &["labels: ", "truncated"],
    );
    assert_payload_rejected(
        "from_serialized(3 字节 actor_id + 非 UTF-8 的 labels entry value + 16 字节 actor_id)",
        &bypass(proto_bytes_field(
            ACTOR_HANDLE_LABELS,
            &proto_bytes_field(LABELS_ENTRY_VALUE, &[0xff]),
        )),
        &["labels.value", "UTF-8"],
    );
    assert_payload_rejected(
        "from_serialized(3 字节 actor_id + 非 UTF-8 的 name + 16 字节 actor_id)",
        &bypass(proto_bytes_field(ACTOR_HANDLE_NAME, &[0xff])),
        &["name", "UTF-8"],
    );
    // 剥离型：每个字段只出现一次，损坏的 owner_address 让上游读不到后面的
    // `creation_job_id`，登记的 handle 带着 nil job id。
    let mut stripped_job_id = proto_bytes_field(ACTOR_HANDLE_ACTOR_ID, &[0x11; 16]);
    stripped_job_id.extend(proto_bytes_field(ACTOR_HANDLE_OWNER_ADDRESS, &[0xff]));
    stripped_job_id.extend(proto_bytes_field(ACTOR_HANDLE_CREATION_JOB_ID, &[0x22; 4]));
    assert_payload_rejected(
        "from_serialized(16 字节 actor_id + 损坏 owner_address + creation_job_id)",
        &stripped_job_id,
        &["owner_address", "truncated"],
    );
    // 对照：同样结构、非 nil 的载荷能登记，说明上面几条拒的是长度、编码、嵌套内容
    // 与 nil，结构良好的载荷本身不在拒绝之列。
    match ActorHandle::from_serialized(&well_formed_handle_payload(0x11, 0x22)) {
        Ok(handle) => {
            println!(
                "  对照 from_serialized(actor_id 16 个 0x11 + creation_job_id 4 个 0x22) → Ok({} 字节 id)",
                handle.id().len()
            );
            // 这份载荷没有字段 6，handle 里因此没有创建描述符；Python 入口必须在进 SDK
            // 之前拒掉（见模块文档）。
            match ray.actor_call_python(handle.id(), "get", &[], &[]) {
                Err(RayError::Ffi(message)) => {
                    assert!(
                        message.contains("no Python creation descriptor"),
                        "缺创建描述符的 handle 没有报描述符: {}",
                        message
                    );
                    println!("  对照 handle 的 actor_call_python → Ffi({})", message);
                }
                Err(other) => panic!("缺创建描述符的 handle 返回了别的错误变体: {}", other),
                Ok(_) => panic!("缺创建描述符的 handle 被 Python 入口提交了"),
            }
        }
        Err(error) => panic!("结构良好的载荷被拒了: {}", error),
    }
    println!("  进程存活：以上载荷要么在进 Ray 之前被拒，要么在登记之后被拒；最后一条对照成功登记，它的 Python 入口也在进 SDK 之前被拒");
}

/// actor 名字的边界：空名字与带内部 NUL 的名字都在 C 侧被拒（集群模式里它们会命中
/// `CoreWorker::GetNamedActorHandle` 的 `RAY_CHECK(!name.empty())`），只有空白字符的
/// 名字合法、按原样去查。两种模式判定相同，集群与本地各跑一遍。
fn actor_name_boundary_checks(ray: &Ray) {
    println!("\n--- actor 名字的边界 ---");
    for (label, name) in [
        ("get_actor(\"\", \"\")", ""),
        ("get_actor(\"a\\0b\", \"\")", "a\0b"),
    ] {
        match ray.get_actor(name, "") {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("empty"),
                    "{} 的错误里没有说明名字为空: {}",
                    label,
                    message
                );
                println!("  {} → Ffi({})", label, message);
            }
            Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
            Ok(found) => panic!("{} 返回了 Ok({} 个结果)", label, found.is_some()),
        }
    }
    match ray.get_actor(" ", "") {
        Ok(None) => {
            println!("  get_actor(\" \", \"\") → Ok(None)（只有空白字符的名字合法、不 trim）")
        }
        Ok(Some(handle)) => panic!(
            "get_actor(\" \") 找到了 {} 字节 id，这个名字没有注册过",
            handle.id().len()
        ),
        Err(error) => panic!("get_actor(\" \") 报错: {}", error),
    }
    println!("  进程存活：空名字在进 Ray 之前被拒");
}

/// 本地模式 init 时配的 namespace，第 1 项的断言要拿它对比。
const LOCAL_NAMESPACE: &str = "rayrust_local_ns";

/// 集群模式 init 时配的 namespace。
const CLUSTER_NAMESPACE: &str = "rayrust_cluster_ns";

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP");
    let worker_so = std::env::var("RAY_WORKER_SO").expect("RAY_WORKER_SO");
    RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
        .namespace(CLUSTER_NAMESPACE)
}

/// 1. is_local_mode：集群 false、本地 true；namespace：两边都与配置的相同。
fn is_local_mode_check(ray: &Ray, cluster: bool) {
    println!("\n--- 1. is_local_mode ---");
    let reported = ray.is_local_mode();
    println!("  is_local_mode() → {}", reported);
    assert_eq!(
        reported, !cluster,
        "is_local_mode() 报 {}，与本次运行的模式（cluster = {}）不符",
        reported, cluster
    );

    let expected = if cluster {
        CLUSTER_NAMESPACE
    } else {
        LOCAL_NAMESPACE
    };
    let namespace = ray.namespace().expect("namespace query");
    println!("  namespace() → {:?}", namespace);
    assert_eq!(
        namespace, expected,
        "namespace() 报 {:?}，配置的是 {:?}（cluster = {}）",
        namespace, expected, cluster
    );
}

/// 2. placement group 全流程。
async fn placement_group_flow(ray: &Ray) {
    println!("\n--- 2. placement group 全流程 ---");
    let name = format!("rayrust_runtime_api_pg_{}", std::process::id());
    let group_id = ray
        .placement_group_create(&name, r#"[{"CPU":0.5}]"#, PG_STRATEGY)
        .expect("placement_group_create");
    assert_eq!(
        group_id.len(),
        18,
        "group id 是 {} 字节，PlacementGroupID 的长度是 18",
        group_id.len()
    );
    println!(
        "  placement_group_create({}, CPU 0.5, STRICT_PACK) → {} 字节 id {}",
        name,
        group_id.len(),
        hex(&group_id)
    );

    let ready = ray
        .placement_group_wait_ready(&group_id, 10)
        .expect("placement_group_wait_ready");
    assert!(ready, "组在 10 秒内没有就绪");
    println!("  placement_group_wait_ready(group, 10) → true");

    let by_id = ray
        .placement_group_by_id(&group_id)
        .expect("placement_group_by_id");
    assert_eq!(by_id.id, group_id, "by_id 拿到的 id 与建组返回的不一样");
    assert_eq!(by_id.name, name, "by_id 拿到的名字不对");
    assert_eq!(
        by_id.state, PG_STATE_CREATED,
        "by_id 的 state 是 {}，CREATED 是 {}",
        by_id.state, PG_STATE_CREATED
    );
    assert_eq!(
        by_id.strategy, PG_STRATEGY,
        "by_id 的 strategy 是 {}，建组时传的是 {}",
        by_id.strategy, PG_STRATEGY
    );
    println!(
        "  placement_group_by_id → id 一致、name {:?}、state {} (CREATED)、strategy {} (STRICT_PACK)",
        by_id.name, by_id.state, by_id.strategy
    );

    let by_name = ray
        .placement_group_by_name(&name)
        .expect("placement_group_by_name");
    assert_eq!(
        (
            by_name.id.clone(),
            by_name.name.clone(),
            by_name.state,
            by_name.strategy
        ),
        (
            group_id.clone(),
            name.clone(),
            PG_STATE_CREATED,
            PG_STRATEGY
        ),
        "by_name 拿到的四个字段与 by_id 不一致"
    );
    println!("  placement_group_by_name → 同样四个字段");

    let all = ray.placement_groups().expect("placement_groups");
    let listed = all
        .iter()
        .find(|group| group.id == group_id)
        .unwrap_or_else(|| {
            panic!(
                "placement_groups() 的 {} 个组里没有本轮的 {}",
                all.len(),
                hex(&group_id)
            )
        });
    assert_eq!(listed.state, PG_STATE_CREATED);
    assert_eq!(listed.strategy, PG_STRATEGY);
    println!(
        "  placement_groups() → {} 个组，其中有它（state {}、strategy {}）",
        all.len(),
        listed.state,
        listed.strategy
    );

    ray.placement_group_remove(&group_id)
        .expect("placement_group_remove");
    println!("  placement_group_remove → 已返回");

    // 移除是同步 RPC：GCS 先删名字映射、把状态改成 REMOVED、写回表项，回调里才回复，
    // 所以 remove 返回后的第一次查询就该看到 state 3 与 by_name 报错。表项不会消失
    // （GCS 保留历史行），晚一些才归还的是节点上预留的那份资源。
    let by_id = ray
        .placement_group_by_id(&group_id)
        .expect("remove 返回后 by_id");
    let by_name = ray.placement_group_by_name(&name);
    let all = ray.placement_groups().expect("placement_groups");
    let listed_state = all
        .iter()
        .find(|group| group.id == group_id)
        .map(|group| group.state);
    assert_eq!(
        by_id.state, PG_STATE_REMOVED,
        "remove 返回后 by_id 的 state 是 {}",
        by_id.state
    );
    assert_eq!(
        listed_state,
        Some(PG_STATE_REMOVED),
        "remove 返回后 placement_groups() 里这一行的 state 是 {:?}",
        listed_state
    );
    let by_name_error = match by_name {
        Err(error) => error.to_string(),
        Ok(found) => panic!("remove 返回后 by_name 还查到了 state {}", found.state),
    };
    assert!(
        by_name_error.contains(&name),
        "by_name 的错误里没有查询用的名字: {}",
        by_name_error
    );
    println!(
        "  remove 返回后第一次查询：by_id state {} (REMOVED)、placement_groups() 里 state {:?}、by_name 报错",
        by_id.state, listed_state
    );
    println!("  by_name 的错误文本带上名字: {}", by_name_error);
}

/// 3. 非法长度与查不到：全部是提交侧的错误，进程存活。
fn placement_group_rejections(ray: &Ray) {
    println!("\n--- 3. 非法与未知的 placement group ---");
    for (label, id) in [
        ("17 字节", vec![0u8; 17]),
        ("19 字节", vec![0u8; 19]),
        ("空字节", Vec::new()),
    ] {
        match ray.placement_group_by_id(&id) {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("18"),
                    "{} 的 by_id 错误里没有说明长度: {}",
                    label,
                    message
                );
                println!("  by_id({}) → Ffi({})", label, message);
            }
            Err(other) => panic!("by_id({}) 返回了别的错误变体: {}", label, other),
            Ok(group) => panic!("by_id({}) 成功返回了 {:?}", label, group),
        }
        match ray.placement_group_wait_ready(&id, 1) {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("18"),
                    "{} 的 wait_ready 错误里没有说明长度: {}",
                    label,
                    message
                );
                println!("  wait_ready({}) → Ffi({})", label, message);
            }
            Err(other) => panic!("wait_ready({}) 返回了别的错误变体: {}", label, other),
            Ok(ready) => panic!("wait_ready({}) 成功返回了 {}", label, ready),
        }
    }

    // 非法长度之后合法调用照常完成：失败在进 SDK 之前，运行时不处于坏状态。
    let all = ray
        .placement_groups()
        .expect("非法长度之后 placement_groups()");
    println!(
        "  进程存活，随后 placement_groups() 照常返回 {} 个组",
        all.len()
    );

    let unknown = [0xabu8; 18];
    match ray.placement_group_by_id(&unknown) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains(&hex(&unknown)),
                "未知 id 的错误里没有查询用的 id: {}",
                message
            );
            println!("  by_id(未知 18 字节) → Ffi({})", message);
        }
        Err(other) => panic!("未知 18 字节 id 返回了别的错误变体: {}", other),
        Ok(group) => panic!("未知 18 字节 id 成功返回了 {:?}", group),
    }

    let unknown_name = "rayrust_no_such_placement_group";
    match ray.placement_group_by_name(unknown_name) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains(unknown_name),
                "未知名字的错误里没有查询用的名字: {}",
                message
            );
            println!("  by_name(未知名字) → Ffi({})", message);
        }
        Err(other) => panic!("未知名字返回了别的错误变体: {}", other),
        Ok(group) => panic!("未知名字成功返回了 {:?}", group),
    }
}

/// 3c. `placement_group_remove` 的宽度检查与错误出口，两个模式都跑。
fn placement_group_remove_checks(ray: &Ray) {
    println!("\n--- 3c. placement_group_remove 的守卫与错误出口 ---");

    // a) 宽度不对：两道检查里 crate 内侧那一道先返回，进不了 FFI。
    for (label, id) in [("3 字节", vec![0u8; 3]), ("空字节", Vec::new())] {
        match ray.placement_group_remove(&id) {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("18 bytes") && message.contains(&id.len().to_string()),
                    "{} 的 remove 错误里没有说明长度: {}",
                    label,
                    message
                );
                println!("  remove({}) → Ffi({})", label, message);
            }
            Err(other) => panic!("remove({}) 返回了别的错误变体: {}", label, other),
            Ok(()) => panic!("remove({}) 成功了", label),
        }
    }

    // b) C 入口上还有一道同样的宽度检查，crate 内侧那道挡在它前面，所以这一道只能从
    //    C ABI 直接调。返回值是 -1，消息在 last_error()。
    rayrust::sys::clear_error();
    let short = [0u8; 3];
    let ret = unsafe {
        rayrust::sys::ray_placement_group_remove(short.as_ptr() as *const _, short.len())
    };
    assert_eq!(ret, -1, "C 入口对 3 字节 id 返回了 {}", ret);
    match rayrust::sys::last_error() {
        Some(message) => {
            assert!(
                message.contains("placement group id must be 18 bytes"),
                "C 入口的错误文本不对: {}",
                message
            );
            println!("  C ABI remove(3 字节) → {}，last_error: {}", ret, message);
        }
        None => panic!("C 入口接受了 3 字节 id"),
    }

    // b2) 空指针：入口先看指针再看长度，不能拿它构造 std::string。
    for len in [0usize, 18] {
        rayrust::sys::clear_error();
        let ret = unsafe { rayrust::sys::ray_placement_group_remove(std::ptr::null(), len) };
        assert_eq!(ret, -1, "C 入口对 (NULL, {}) 返回了 {}", len, ret);
        match rayrust::sys::last_error() {
            Some(message) => assert!(
                message.contains("empty"),
                "C 入口对 (NULL, {}) 的错误文本不对: {}",
                len,
                message
            ),
            None => panic!("C 入口接受了 (NULL, {})", len),
        }
        println!(
            "  C ABI remove(NULL, {}) → -1，last_error 说明 id 为空",
            len
        );
    }
    println!("  进程存活：两道宽度检查与空指针检查都在进 Ray 之前返回");

    // c) 未知的 18 字节 id：两个模式都返回 Ok(())，上游按「已经移除」处理。
    let unknown = [0x5au8; 18];
    match ray.placement_group_remove(&unknown) {
        Ok(()) => println!(
            "  remove(未知 18 字节 {}) → Ok(())（GCS 与本地 map 都把它当已经移除）",
            hex(&unknown)
        ),
        Err(error) => panic!("未知 18 字节 id 返回了 {}", error),
    }

    // d) 同一个组 remove 两次：两次都 Ok(())，进程存活。集群模式下两次走的是不同的上游分支
    //    （第一次从登记表移除、第二次命中「已经移除」），本地模式下两者都是同一个 map 上的
    //    erase，且本地没有查询能观察它。
    let name = format!("rayrust_runtime_api_remove_twice_{}", std::process::id());
    let group_id = ray
        .placement_group_create(&name, r#"[{"CPU":0.5}]"#, PG_STRATEGY)
        .expect("remove_twice: placement_group_create");
    assert_eq!(group_id.len(), 18, "建组返回了 {} 字节 id", group_id.len());
    println!(
        "  建组 {} → {} 字节 id {}",
        name,
        group_id.len(),
        hex(&group_id)
    );
    ray.placement_group_remove(&group_id)
        .expect("第一次 remove");
    println!("  第一次 remove → Ok(())");
    match ray.placement_group_remove(&group_id) {
        Ok(()) => println!("  第二次 remove → Ok(())"),
        Err(error) => panic!("第二次 remove 返回了 {}", error),
    }
    println!("  进程存活：重复移除同一个组没有报错");
}

/// 3b. actor id 宽度：短 id 只能从 C ABI 传进 `ray_actor_kill`。
///
/// `Ray::kill_actor` 只收 `&ActorHandle`，而 `ActorHandle` 的 id 恒为 16 字节，所以
/// 「16 字节以下」这个形状只能从 C ABI 传进来。集群模式下这条路径在修复前会命中
/// `ActorID::FromBinary` 的 RAY_CHECK 终止进程
/// （`cpp/src/ray/runtime/abstract_ray_runtime.cc:265-267`），RAY_CATCH 抓不到，
/// 所以这条断言在集群里是承重的。合法的 `kill_actor` 由第 4 项的重启对照证实仍按
/// 原样工作。
fn actor_kill_id_width_c_abi() {
    println!("\n--- 3b. actor id 宽度（C ABI 直接调用 ray_actor_kill）---");
    rayrust::sys::clear_error();
    let short_actor_id = [1u8, 2, 3];
    // 指针与长度描述的是这个函数内的数组本身，数组在调用返回之前一直活着。
    unsafe {
        rayrust::sys::ray_actor_kill(short_actor_id.as_ptr() as *const _, 3, false);
    }
    match rayrust::sys::last_error() {
        Some(message) => {
            assert!(
                message.contains("actor id must be 16 bytes"),
                "ray_actor_kill 的错误里没有长度要求: {}",
                message
            );
            println!(
                "  ray_actor_kill(3 字节 actor id) → last_error({})",
                message
            );
        }
        None => panic!("ray_actor_kill 接受了 3 字节 actor id"),
    }
    println!("  进程存活：错误在进 Ray 之前返回");
}

/// 4. exit_actor：集群模式下 actor 自己结束且不被 max_restarts 重启；本地模式下这个调用
/// 是空操作，方法返回它自己的值、actor 仍然可用。
async fn exit_actor_check(ray: &Ray, cluster: bool) {
    println!("\n--- 4. exit_actor ---");
    let name = format!("rayrust_runtime_api_exiting_{}", std::process::id());
    let opts = ActorOptions::new().name(name.clone()).max_restarts(2);
    let handle = selfexiting_actor_create(ray, &opts, 7).expect("创建 SelfExiting");
    let before = selfexiting_get_sync(ray, &handle)
        .expect("调用 exit 之前的 get")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读 exit 之前的 get");
    assert_eq!(before, 7, "actor 的初值不对");
    println!("  exit 之前 get() → {}", before);

    if !cluster {
        // 本地模式：先给 actor 留下一个可辨认的状态，再证明 exit_actor() 什么都没做。
        // 用带返回值的方法：退出状态路径上它的返回值会被错误载荷顶掉（`Serialization`），
        // 空操作路径上拿到的就是 9。
        let bumped = selfexiting_increment_and_exit_sync(ray, &handle, 2)
            .expect("调用 increment_and_exit")
            .get_timeout(EXIT_TIMEOUT_MS)
            .expect("本地模式里 exit_actor() 是空操作，方法必须返回它自己的值");
        assert_eq!(bumped, 9, "方法自己的返回值不对");
        println!(
            "  increment_and_exit(2) → {}（本地模式 exit_actor() 是空操作，方法返回自己的值）",
            bumped
        );

        let exit_value = selfexiting_exit_sync(ray, &handle)
            .expect("调用 exit()")
            .get_timeout(EXIT_TIMEOUT_MS)
            .expect("本地模式的 () 方法必须正常返回");
        println!(
            "  exit() 的对象 → Ok({:?})（本地模式 () 方法正常返回）",
            exit_value
        );

        let after = selfexiting_get_sync(ray, &handle)
            .expect("调用 exit 之后的 get")
            .get_timeout(GET_TIMEOUT_MS)
            .expect("本地模式的 exit_actor() 之后 actor 必须还能用");
        assert_eq!(after, 9, "本地模式的 exit_actor() 不该改变 actor 的状态");
        println!("  exit 之后 get() → {}（actor 仍然可用，状态没变）", after);
        return;
    }

    let exited = selfexiting_exit_sync(ray, &handle).expect("调用 exit()");
    match exited.get_timeout(EXIT_TIMEOUT_MS) {
        Ok(()) => panic!("exit() 的对象拿到结果了，退出路径不该写结果"),
        Err(error) => println!("  exit() 的对象 → {}（退出路径不写结果）", error),
    }

    let after = match selfexiting_get_sync(ray, &handle) {
        Ok(object) => object.get_timeout(GET_TIMEOUT_MS),
        Err(error) => Err(error),
    };
    match after {
        Err(RayError::Actor(message)) => println!("  exit 之后 get() → Actor({})", message),
        Err(other) => panic!("exit 之后 get() 返回了别的错误变体: {}", other),
        Ok(value) => panic!(
            "actor 退出之后 get() 还返回了 {}——说明它被 max_restarts=2 重启了",
            value
        ),
    }

    match ray.get_actor(&name, "") {
        Ok(Some(found)) => println!(
            "  get_actor → 拿到 {} 字节 id，与原 handle 相同: {}（驱动进程的 core worker 缓存了名字映射，actor 本身已经没了）",
            found.id().len(),
            found.id() == handle.id()
        ),
        Ok(None) => println!("  get_actor → None（名字随 actor 一起消失）"),
        Err(error) => println!("  get_actor → {}（查询本身失败）", error),
    }

    // 正向对照：同一个集群、同样的 `max_restarts=2`，`kill_actor(no_restart = false)`
    // 之后这个 actor 会被重启，状态回到初值。没有这条，「exit 之后调用失败」区分不了
    // 「actor 永远没了」与「这次调用正好在重启窗口里」。
    let control_name = format!("rayrust_runtime_api_restart_{}", std::process::id());
    let control_opts = ActorOptions::new().name(control_name).max_restarts(2);
    let control = selfexiting_actor_create(ray, &control_opts, 7).expect("创建重启对照 actor");
    let before_kill = selfexiting_increment_sync(ray, &control, 2)
        .expect("对照 actor 的 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读对照 actor 的 increment");
    assert_eq!(before_kill, 9, "对照 actor 的计数不对");
    ray.kill_actor(&control, false)
        .expect("kill_actor(no_restart = false)");
    println!(
        "  对照：increment(2) → {}，kill_actor(no_restart = false) 已提交",
        before_kill
    );

    // 重启的判据是「状态回到初值 7」，不是「某次调用成功」：重启窗口里成功的调用
    // 也可能拿到被杀之前的状态，甚至一次既不是之前值也不是初值的过渡读数（实测恒为
    // 97，与 actor 的初值、增量都无关）。
    let mut restarted = None;
    for attempt in 0..RESTART_POLL_TIMES {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(RESTART_POLL_INTERVAL_MS)).await;
        }
        match selfexiting_get_sync(ray, &control)
            .expect("对照 actor 提交 get")
            .get_timeout(RESTART_GET_TIMEOUT_MS)
        {
            Ok(value) if value == 7 => {
                println!("  第 {} 次轮询：get() → {}（回到初值）", attempt, value);
                restarted = Some(value);
                break;
            }
            Ok(value) => println!(
                "  第 {} 次轮询：get() → {}（还没回到初值，重启窗口里的读数不能当判据）",
                attempt, value
            ),
            Err(error) => println!("  第 {} 次轮询：get() → {}（还没就绪）", attempt, error),
        }
    }
    let restarted = restarted.unwrap_or_else(|| {
        panic!(
            "kill_actor(no_restart = false) 之后 {} 次轮询（{}ms 一次）没看到计数回到初值 7",
            RESTART_POLL_TIMES, RESTART_POLL_INTERVAL_MS
        )
    });
    println!(
        "  重启前计数 {} → 重启后计数 {}（这个集群确实会重启这个 actor）",
        before_kill, restarted
    );
}

/// 5. 任务里调 exit_actor()：集群模式下该次调用失败、消息里是我们自己抛的文案；
/// 本地模式下这个调用是空操作，任务照常返回它自己的值。
async fn exit_from_task_check(ray: &Ray, cluster: bool) {
    println!("\n--- 5. 任务里调 exit_actor() ---");
    let object = exit_from_task_remote(ray).expect("提交 exit_from_task");
    if !cluster {
        let value = object
            .get_timeout(EXIT_TIMEOUT_MS)
            .expect("本地模式里 exit_actor() 是空操作，任务必须正常返回");
        assert_eq!(value, 1, "exit_from_task 的返回值不对");
        println!(
            "  exit_from_task → Ok({})（本地模式 exit_actor() 是空操作，任务照常返回）",
            value
        );
        return;
    }
    match object.get_timeout(EXIT_TIMEOUT_MS) {
        Err(RayError::Task(message)) => {
            assert!(
                message.contains("exit_actor"),
                "任务失败的消息里没有我们自己抛的文案: {}",
                message
            );
            println!("  exit_from_task → Task({})", message);
        }
        Err(other) => panic!("任务失败返回了别的错误变体: {}", other),
        Ok(value) => panic!("任务里调用 exit_actor() 还拿到了结果 {}", value),
    }
    println!("  进程存活：这次调用只是失败，没有停在任务里");
}

/// 8. 构造函数里调 exit_actor()：集群模式创建任务失败、第一次调用报 `Actor`；
/// 本地模式那个调用是空操作，actor 正常创建。
async fn ctor_exit_check(ray: &Ray, cluster: bool) {
    println!("\n--- 8. 构造函数里调 exit_actor() ---");
    let name = format!("rayrust_runtime_api_ctor_exiting_{}", std::process::id());
    let opts = ActorOptions::new().name(name).max_restarts(0);
    let handle = ctorexiting_actor_create(ray, &opts, 11).expect("actor_create 照常返回 handle");
    println!("  actor_create → {} 字节 id", handle.id().len());

    if !cluster {
        let value = ctorexiting_get_sync(ray, &handle)
            .expect("本地模式提交 get")
            .get_timeout(GET_TIMEOUT_MS)
            .expect("本地模式构造里的 exit_actor() 是空操作，actor 必须能用");
        assert_eq!(value, 11, "本地模式构造出来的 actor 值不对");
        println!(
            "  第一次 get() → {}（本地模式构造里的 exit_actor() 同样是空操作）",
            value
        );
        return;
    }

    let outcome = match ctorexiting_get_sync(ray, &handle) {
        Ok(object) => object.get_timeout(EXIT_TIMEOUT_MS),
        Err(error) => Err(error),
    };
    match outcome {
        Err(RayError::Actor(message)) => println!(
            "  第一次 get() → Actor({})（构造任务失败，actor 从未启动）",
            message
        ),
        Err(other) => panic!("构造函数退出之后 get() 返回了别的错误变体: {}", other),
        Ok(value) => panic!("构造函数里 exit_actor() 之后 get() 还拿到了 {}", value),
    }
    println!("  进程存活：actor_create 与 get 都没有把进程停住");
}

/// 6. handle 交接：字节穿过工人进程再回来，驱动用它复原 handle 并调用。
async fn handle_handoff_check(ray: &Ray) {
    println!("\n--- 6. actor handle 交接 ---");
    let handle = counter_actor_create(ray, &ActorOptions::new(), 21).expect("创建 Counter");
    let incremented = counter_increment_sync(ray, &handle, 4)
        .expect("调用 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读 increment");
    assert_eq!(incremented, 25, "actor 的计数不对");
    println!("  increment(4) → {}", incremented);

    let serialized = handle.serialize().expect("serialize");
    assert!(!serialized.is_empty(), "serialize 返回了空字节");
    println!(
        "  handle.serialize() → {} 字节（actor id 是 {} 字节）",
        serialized.len(),
        handle.id().len()
    );

    let object = handle_handoff_remote(ray, serialized.clone(), handle.id().to_vec())
        .expect("提交 handle_handoff");
    let (back_serialized, back_id, restored_in_worker, called_in_worker) = object
        .get_timeout(EXIT_TIMEOUT_MS)
        .expect("读 handle_handoff 的结果");
    assert_eq!(
        back_serialized, serialized,
        "序列化字节穿过工人进程之后变了"
    );
    assert_eq!(back_id, handle.id(), "裸 id 穿过工人进程之后变了");
    println!(
        "  工人交回 {} 字节序列化 handle 与 {} 字节裸 id，都与原值相同",
        back_serialized.len(),
        back_id.len()
    );
    println!("  工人侧 from_serialized → {}", restored_in_worker);
    println!("  工人侧裸 id 调用 → {}", called_in_worker);
    assert_eq!(
        restored_in_worker, "Ok(16 字节 id)",
        "工人侧复原 handle 失败"
    );
    assert_eq!(
        called_in_worker, "Ok(28 字节 object id)",
        "工人侧裸 id 调用失败"
    );

    let restored = ActorHandle::from_serialized(&back_serialized).expect("驱动侧复原 handle");
    assert_eq!(
        restored.id(),
        handle.id(),
        "复原出来的 actor id 与原 handle 不同"
    );
    println!(
        "  驱动侧 from_serialized → {} 字节 id，与原 handle 相同",
        restored.id().len()
    );
    let value = counter_get_sync(ray, &restored)
        .expect("调用复原的 handle")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读复原 handle 的 get");
    assert_eq!(value, 25, "复原的 handle 拿到的值不对");
    println!("  复原的 handle 调 get() → {}", value);

    // 对照：同一个 actor 的裸 id 在驱动侧同样能调用——驱动进程登记过这个 handle。
    let by_raw_id: i64 = ray
        .actor_call(handle.id(), COUNTER_GET, &[], &[])
        .expect("用裸 id 提交")
        .cast()
        .get_timeout(GET_TIMEOUT_MS)
        .expect("读裸 id 的结果");
    assert_eq!(by_raw_id, 25, "裸 id 调用拿到的值不对");
    println!(
        "  对照：驱动侧直接用裸 id 提交同一个方法 → {}（驱动登记过这个 handle）",
        by_raw_id
    );

    // 载荷校验：畸形字节、两个 id 字段的长度不对、缺字段、长度合法但 nil，
    // 以及结构良好的载荷能登记、它的 Python 入口因为没有创建描述符被拒。
    actor_handle_payload_checks(ray);
}

/// 7. 本地模式：这四条查询/等待、两个 handle 接口、kill_actor、cancel、
/// actor_create_python 与 actor_call_python 都必须报错，进程存活。
async fn local_mode_rejections(ray: &Ray) {
    println!("\n--- 7. 本地模式的拒绝 ---");
    let group_id = [0xa1u8; 18];
    match ray.placement_group_wait_ready(&group_id, 1) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "wait_ready 的错误没有说明本地模式: {}",
                message
            );
            println!("  wait_ready → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 wait_ready 返回了别的错误变体: {}", other),
        Ok(ready) => panic!("本地模式 wait_ready 返回了 {}", ready),
    }
    match ray.placement_groups() {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "placement_groups 的错误没有说明本地模式: {}",
                message
            );
            println!("  placement_groups → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 placement_groups 返回了别的错误变体: {}", other),
        Ok(groups) => panic!("本地模式 placement_groups 返回了 {} 个组", groups.len()),
    }
    match ray.placement_group_by_id(&group_id) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "by_id 的错误没有说明本地模式: {}",
                message
            );
            println!("  placement_group_by_id → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 by_id 返回了别的错误变体: {}", other),
        Ok(group) => panic!("本地模式 by_id 返回了 {:?}", group),
    }
    match ray.placement_group_by_name("rayrust_local") {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "by_name 的错误没有说明本地模式: {}",
                message
            );
            println!("  placement_group_by_name → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 by_name 返回了别的错误变体: {}", other),
        Ok(group) => panic!("本地模式 by_name 返回了 {:?}", group),
    }

    let handle = counter_actor_create(ray, &ActorOptions::new(), 1).expect("本地创建 Counter");
    let incremented = counter_increment_sync(ray, &handle, 0)
        .expect("本地 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地读 increment");
    assert_eq!(incremented, 1, "本地的 Counter 计数不对");
    println!(
        "  本地模式的 actor 本身可用：increment(0) → {}",
        incremented
    );
    match ray.kill_actor(&handle, true) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "kill_actor 的错误没有说明本地模式: {}",
                message
            );
            println!("  kill_actor → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 kill_actor 返回了别的错误变体: {}", other),
        Ok(()) => panic!("本地模式 kill_actor 返回了 Ok"),
    }
    let after_kill = counter_increment_sync(ray, &handle, 1)
        .expect("本地 kill 被拒之后 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地 kill 被拒之后读 increment");
    assert_eq!(after_kill, 2, "本地 kill 被拒之后的计数不对");
    println!(
        "  kill 被拒之后 actor 仍可用：increment(1) → {}",
        after_kill
    );
    match handle.serialize() {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "serialize 的错误没有说明本地模式: {}",
                message
            );
            println!("  serialize → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 serialize 返回了别的错误变体: {}", other),
        Ok(bytes) => panic!("本地模式 serialize 返回了 {} 字节", bytes.len()),
    }
    // 载荷畸形的：载荷校验在本地模式判定之前，错误文本里没有 local mode。
    match ActorHandle::from_serialized(&[1u8, 2, 3]) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("well-formed") && !message.contains("local mode"),
                "畸形载荷的错误应该说明载荷本身有问题: {}",
                message
            );
            println!(
                "  ActorHandle::from_serialized(畸形载荷) → Ffi({})",
                message
            );
        }
        Err(other) => panic!("本地模式 from_serialized 返回了别的错误变体: {}", other),
        Ok(handle) => panic!(
            "本地模式 from_serialized 收下了畸形载荷，返回 {} 字节 id",
            handle.id().len()
        ),
    }
    // 结构良好的：载荷过了校验，本地模式那条报错才出现。
    match ActorHandle::from_serialized(&well_formed_handle_payload(0x11, 0x22)) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "from_serialized 的错误没有说明本地模式: {}",
                message
            );
            println!(
                "  ActorHandle::from_serialized(结构良好的载荷) → Ffi({})",
                message
            );
        }
        Err(other) => panic!("本地模式 from_serialized 返回了别的错误变体: {}", other),
        Ok(handle) => panic!(
            "本地模式 from_serialized 返回了 {} 字节 id",
            handle.id().len()
        ),
    }
    let put_ref = ray.put(&7i64).expect("本地 put");
    match ray.cancel(put_ref.id(), false, false) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "cancel 的错误没有说明本地模式: {}",
                message
            );
            println!("  cancel → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 cancel 返回了别的错误变体: {}", other),
        Ok(()) => panic!("本地模式 cancel 返回了 Ok"),
    }
    let after_cancel = counter_increment_sync(ray, &handle, 2)
        .expect("本地 cancel 被拒之后 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地 cancel 被拒之后读 increment");
    assert_eq!(after_cancel, 4, "本地 cancel 被拒之后的计数不对");
    println!(
        "  cancel 被拒之后进程仍可用：increment(2) → {}",
        after_cancel
    );
    // 一个活着的本地 actor 的 id：入口在拿 id 之前就拒绝，所以传哪个 id 都一样。
    match ray.actor_call_python(handle.id(), "increment", &[], &[]) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "actor_call_python 的错误没有说明本地模式: {}",
                message
            );
            println!("  actor_call_python → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 actor_call_python 返回了别的错误变体: {}", other),
        Ok(reference) => panic!(
            "本地模式 actor_call_python 返回了 {} 字节 ref",
            reference.id().len()
        ),
    }
    let after_py_call = counter_increment_sync(ray, &handle, 3)
        .expect("本地 actor_call_python 被拒之后 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地 actor_call_python 被拒之后读 increment");
    assert_eq!(
        after_py_call, 7,
        "本地 actor_call_python 被拒之后的计数不对"
    );
    println!(
        "  actor_call_python 被拒之后 actor 仍可用：increment(3) → {}",
        after_py_call
    );
    let python_name = "rayrust_local_py";
    match ray.actor_create_python(
        "rayrust_test",
        "Counter",
        &[],
        &ActorOptions::new().name(python_name),
    ) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("local mode"),
                "actor_create_python 的错误没有说明本地模式: {}",
                message
            );
            println!("  actor_create_python → Ffi({})", message);
        }
        Err(other) => panic!("本地模式 actor_create_python 返回了别的错误变体: {}", other),
        Ok(handle) => panic!(
            "本地模式 actor_create_python 返回了 {} 字节 id",
            handle.id().len()
        ),
    }
    match ray.get_actor(python_name, "") {
        Ok(None) => println!("  get_actor({python_name}) → None（名字没有进本地名字表）"),
        Ok(Some(handle)) => panic!(
            "被拒的创建登记了名字，get_actor 拿到 {} 字节 id",
            handle.id().len()
        ),
        Err(error) => panic!("本地 get_actor({python_name}) 报错: {}", error),
    }
    let after_py_create = counter_increment_sync(ray, &handle, 4)
        .expect("本地 actor_create_python 被拒之后 increment")
        .get_timeout(GET_TIMEOUT_MS)
        .expect("本地 actor_create_python 被拒之后读 increment");
    assert_eq!(
        after_py_create, 11,
        "本地 actor_create_python 被拒之后的计数不对"
    );
    println!(
        "  actor_create_python 被拒之后 actor 仍可用：increment(4) → {}",
        after_py_create
    );
    println!("  进程存活：以上被拒的调用都是提交侧的报错，没有 abort");

    actor_name_boundary_checks(ray);
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "cluster".to_string());
    println!("=== RayRuntime 接口 ({}) ===", mode);
    let cluster = mode == "cluster";
    let config = if cluster {
        cluster_config()
    } else {
        RayConfig::local().namespace(LOCAL_NAMESPACE)
    };
    let ray = Ray::connect(&config).expect("Ray::connect");

    is_local_mode_check(&ray, cluster);
    if cluster {
        placement_group_flow(&ray).await;
        placement_group_rejections(&ray);
        actor_kill_id_width_c_abi();
        actor_name_boundary_checks(&ray);
    } else {
        println!("\n--- 2、3. placement group 流程与非法输入 ---");
        println!("  本地模式跳过：三个查询在本地模式没有 GCS accessor，C 侧直接报错；");
        println!("  第 7 项会实际跑这四条报错。");
        println!("  第 3b 项（C ABI 传 3 字节 actor id）也跳过：它的承重性来自集群模式下");
        println!("  修复前会终止进程；本地模式那条报错由第 7 项的 kill_actor 覆盖。");
        println!("  第 3 项的 actor 名字边界与第 7 项跑同一组断言。");
    }
    placement_group_remove_checks(&ray);
    exit_actor_check(&ray, cluster).await;
    exit_from_task_check(&ray, cluster).await;
    ctor_exit_check(&ray, cluster).await;
    if cluster {
        handle_handoff_check(&ray).await;
    } else {
        println!("\n--- 6. handle 交接的集群部分 ---");
        println!("  本地模式跳过：本地模式不启动 core worker 进程，两个 handle 接口都报错");
        println!("  （第 7 项实际跑这两个报错）；交接到工人进程的流程需要真实工人。");
        local_mode_rejections(&ray).await;
    }

    // 让 blocking pool 里的 FFI 调用收尾，避免退出竞态。
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("\n✓ RayRuntime 接口检查全部通过");
}

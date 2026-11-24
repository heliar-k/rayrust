//! Example Rust cdylib worker for Ray cluster mode.
//!
//! This crate compiles to `librayrust_worker.so`.
//! When the Ray C++ worker process loads this .so via `boost::dll`:
//!   1. `#[ctor]` constructors auto-register each `#[remote]` function
//!   2. `boost::dll` resolves `TaskExecutionHandler` / `GetRemoteFunctions` /
//!      `InitRayRuntime` via `libray_api.so` (`DT_NEEDED`)
//!   3. `GetRemoteFunctions()` returns the registered functions
//!   4. Worker executes Rust remote tasks and actors, and callbacks inside
//!      this `.so` reach the worker runtime via `AbstractRayRuntime::GetInstance()`

use rayrust::remote;

/// Simple addition task.
#[remote]
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Greeting task.
#[remote]
pub fn greet(name: String) -> String {
    format!("Hello, {} from Rust worker!", name)
}

/// Multiply task.
#[remote]
pub fn multiply(a: i64, b: i64) -> i64 {
    a * b
}

/// Non-commutative task: a wrong argument order gives a different result.
#[remote]
pub fn sub(a: i32, b: i32) -> i32 {
    a - b
}

/// CPU-intensive: sum of 0..n.
#[remote]
pub fn compute(n: i64) -> i64 {
    (0..n).sum()
}

/// Async task: simulates I/O with sleep, then returns sum.
#[remote]
pub async fn async_sum(a: i64, b: i64) -> i64 {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    a + b
}

/// 把收到的 id 字节原样交回。交回的是 28 个字节，不是引用：驱动侧没有这个 id 的
/// owner 路径，读它得到 `ObjectUnknownOwner`，与对象是否还活着无关。
#[remote]
pub fn return_id_bytes(id: Vec<u8>) -> Vec<u8> {
    id
}

/// 读进程环境变量；任务级 runtime_env 的 env_vars 是否真的到了工人进程，
/// 由这个任务交回的值判定。
#[remote]
pub fn env_check(name: String) -> Option<String> {
    std::env::var(&name).ok()
}

/// 睡眠任务：占住 CPU，用来验证 placement group 绑定与资源需求是否真的承重。
#[remote]
pub fn sleep_for(seconds: f64) -> i64 {
    std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
    0
}

/// 任务里调用 `exit_actor()`：退出语义只在 actor 方法里成立，这次调用必须失败。
#[remote]
pub fn exit_from_task() -> i64 {
    rayrust::exit_actor();
    1
}

/// actor handle 交接：收下序列化 handle 与裸 actor id，把两段字节原样交回，并报告
/// 在工人进程里复原 handle 与按裸 id 提交方法调用的结果。
///
/// `ActorHandle::from_serialized` 先把 handle 登记进工人进程的 `ActorManager`，
/// 随后的 `ray_actor_call` 按同一个 actor id 提交 `Counter::get` 调用并返回 28 字节
/// object id，两条路径都返回 `Ok(...)`。
#[remote]
pub fn handle_handoff(serialized: Vec<u8>, raw_id: Vec<u8>) -> (Vec<u8>, Vec<u8>, String, String) {
    let restored = match rayrust::ActorHandle::from_serialized(&serialized) {
        Ok(handle) => format!("Ok({} 字节 id)", handle.id().len()),
        Err(error) => format!("Err({})", error),
    };
    let called = call_actor_by_raw_id(&raw_id);
    (serialized, raw_id, restored, called)
}

/// 用裸 actor id 提交一次方法调用：走 `Ray::actor_call` 用的同一个 C 入口。工人进程
/// 里没有 `Ray` 句柄可用，所以这里直接调 FFI。
fn call_actor_by_raw_id(actor_id: &[u8]) -> String {
    let method = rayrust::sys::to_cstring("__rayrust_actor_factory_counter::get");
    let mut out = unsafe {
        rayrust::sys::ray_actor_call(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            method.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    if out.data.is_null() {
        let message = rayrust::sys::last_error().unwrap_or_else(|| "没有留下错误".to_string());
        return format!("Err({})", message);
    }
    let len = out.len;
    unsafe { rayrust::sys::ray_free_bytes(&mut out) };
    format!("Ok({} 字节 object id)", len)
}

/// 在工人进程里经 `ObjectRef::from(id).get()` 读取驱动写入的对象。
/// `_pinned` 按引用传入同一对象：`CoreWorker::GetAndPinArgsForExecutor`
/// （`src/ray/core_worker/core_worker.cc:3257-3350`）在 `:3283-3284` 为该 `ObjectID`
/// 登记 borrowed owner 地址，并在 `:3341-3346` 取出对象载荷交给
/// `TaskExecutor::ExecuteTask`（`cpp/src/ray/runtime/task/task_executor.cc:120-291`，
/// `:160-189` 写入参数字节），因此生成的回调把第 0 个参数按 msgpack `i64` 解码；函数体
/// 忽略 `_pinned`，由 `ObjectRef::from(id).get()` 经 `ray_get` 读取对象值。
#[remote]
pub fn read_driver_object(_pinned: i64, id: Vec<u8>) -> Result<i64, String> {
    rayrust::ObjectRef::<i64>::from(id)
        .get()
        .map_err(|e| format!("{:?}", e))
}

/// 在工人进程里调用 `ray_put` 写入 `i64`，并用返回的 28 字节 object id 读回该值。
#[remote]
pub fn put_in_worker(value: i64) -> Result<(Vec<u8>, i64), String> {
    let data = rayrust::serialize(&value).map_err(|e| format!("{:?}", e))?;
    rayrust::sys::clear_error();
    let raw =
        unsafe { rayrust::sys::ray_put(data.as_ptr() as *const std::os::raw::c_char, data.len()) };
    let guard = rayrust::sys::CBytesGuard::from(raw).ok_or_else(|| {
        let msg = rayrust::sys::last_error().unwrap_or_else(|| "unknown FFI error".to_string());
        format!("ray_put: {}", msg)
    })?;
    let id = guard.as_slice().to_vec();
    let read_back = rayrust::ObjectRef::<i64>::from(id.clone())
        .get()
        .map_err(|e| format!("{:?}", e))?;
    Ok((id, read_back))
}

/// 在工人进程里调用 `ray_put_xlang` 写入带 9 字节 XLANG 头与 `"XLANG"` metadata 的对象，
/// 再用 `ray_get` 与 `deserialize_xlang::<i64>` 读回该值。
#[remote]
pub fn put_xlang_in_worker(value: i64) -> Result<(Vec<u8>, i64), String> {
    let data = rayrust::serialize_xlang(&value).map_err(|e| format!("{:?}", e))?;
    rayrust::sys::clear_error();
    let raw = unsafe {
        rayrust::sys::ray_put_xlang(data.as_ptr() as *const std::os::raw::c_char, data.len())
    };
    let id_guard = rayrust::sys::CBytesGuard::from(raw).ok_or_else(|| {
        let msg = rayrust::sys::last_error().unwrap_or_else(|| "unknown FFI error".to_string());
        format!("ray_put_xlang: {}", msg)
    })?;
    let id = id_guard.as_slice().to_vec();
    rayrust::sys::clear_error();
    let got = unsafe {
        rayrust::sys::ray_get(id.as_ptr() as *const std::os::raw::c_char, id.len(), 10_000)
    };
    let val_guard = rayrust::sys::CBytesGuard::from(got).ok_or_else(|| {
        let msg = rayrust::sys::last_error().unwrap_or_else(|| "unknown FFI error".to_string());
        format!("ray_get: {}", msg)
    })?;
    let read_back =
        rayrust::deserialize_xlang::<i64>(val_guard.as_slice()).map_err(|e| format!("{:?}", e))?;
    Ok((id, read_back))
}

/// 在工人进程里调用 `ray_task_call_with_options` 提交已注册的 `multiply(value, 7)`，
/// 并用返回的 28 字节 object id 读出乘积。
#[remote]
pub fn submit_nested(value: i64) -> Result<(Vec<u8>, i64), String> {
    let arg_a = rayrust::serialize(&value).map_err(|e| format!("{:?}", e))?;
    let arg_b = rayrust::serialize(&7i64).map_err(|e| format!("{:?}", e))?;
    let args = rayrust::sys::build_args_array(&[&arg_a, &arg_b]);
    let func_c = rayrust::sys::to_cstring("multiply");
    rayrust::sys::clear_error();
    let raw = unsafe {
        rayrust::sys::ray_task_call_with_options(
            func_c.as_ptr(),
            args.as_ptr(),
            args.len(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    let guard = rayrust::sys::CBytesGuard::from(raw).ok_or_else(|| {
        let msg = rayrust::sys::last_error().unwrap_or_else(|| "unknown FFI error".to_string());
        format!("ray_task_call_with_options: {}", msg)
    })?;
    let id = guard.as_slice().to_vec();
    let product = rayrust::ObjectRef::<i64>::from(id.clone())
        .get_timeout(10_000)
        .map_err(|e| format!("{:?}", e))?;
    Ok((id, product))
}

/// 在工人进程里复原 `Counter` 的序列化 handle，调用 `ray_actor_call` 执行
/// `Counter::increment`，并用返回的 28 字节 object id 读出更新后的计数值。
#[remote]
pub fn call_actor_in_worker(serialized_handle: Vec<u8>, n: i64) -> Result<(Vec<u8>, i64), String> {
    let handle = rayrust::ActorHandle::from_serialized(&serialized_handle)
        .map_err(|e| format!("{:?}", e))?;
    let arg_n = rayrust::serialize(&n).map_err(|e| format!("{:?}", e))?;
    let args = rayrust::sys::build_args_array(&[&arg_n]);
    let method = rayrust::sys::to_cstring("__rayrust_actor_factory_counter::increment");
    rayrust::sys::clear_error();
    let raw = unsafe {
        rayrust::sys::ray_actor_call(
            handle.id().as_ptr() as *const std::os::raw::c_char,
            handle.id().len(),
            method.as_ptr(),
            args.as_ptr(),
            args.len(),
            std::ptr::null(),
        )
    };
    let guard = rayrust::sys::CBytesGuard::from(raw).ok_or_else(|| {
        let msg = rayrust::sys::last_error().unwrap_or_else(|| "unknown FFI error".to_string());
        format!("ray_actor_call: {}", msg)
    })?;
    let id = guard.as_slice().to_vec();
    let updated = rayrust::ObjectRef::<i64>::from(id.clone())
        .get_timeout(10_000)
        .map_err(|e| format!("{:?}", e))?;
    Ok((id, updated))
}

// ─── 健全性与回调错误通道测试用任务与 actor ──────────────────

/// 在同步 `#[remote]` 任务体内触发 `panic!`，验证错误回传且工人进程存活。
#[remote]
pub fn soundness_panic_sync(msg: String) -> i64 {
    panic!("{}", msg);
}

/// 在异步 `#[remote] async fn` 跨越 `.await` 之后触发 `panic!`。
#[remote]
pub async fn soundness_panic_async(msg: String) -> i64 {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    panic!("{}", msg);
}

/// 回显 `i32`：用于测试错误参数类型/个数与验证工人进程在错误后仍然存活。
#[remote]
pub fn soundness_echo_i32(x: i32) -> i32 {
    x
}

/// 并发互斥与错误/panic 恢复测试用 actor。
struct SoundnessActor {
    value: i64,
    in_flight: std::sync::atomic::AtomicI64,
    peak_in_flight: std::sync::atomic::AtomicI64,
}

#[rayrust::actor]
impl SoundnessActor {
    fn new(start: i64) -> Self {
        SoundnessActor {
            value: start,
            in_flight: std::sync::atomic::AtomicI64::new(0),
            peak_in_flight: std::sync::atomic::AtomicI64::new(0),
        }
    }

    fn sleep_and_add(&mut self, n: i64, sleep_ms: u64) -> (i64, i64) {
        use std::sync::atomic::Ordering;
        let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(cur, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
        self.value += n;
        let val = self.value;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        (val, self.peak_in_flight.load(Ordering::SeqCst))
    }

    fn read_state(&self) -> (i64, i64) {
        use std::sync::atomic::Ordering;
        let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(cur, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let res = (self.value, self.peak_in_flight.load(Ordering::SeqCst));
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        res
    }

    fn add_and_pid(&mut self, n: i64) -> (i64, u32) {
        self.value += n;
        (self.value, std::process::id())
    }

    fn get_value(&self) -> i64 {
        self.value
    }

    fn panic_method(&mut self, msg: String) -> i64 {
        panic!("{}", msg);
    }

    fn worker_pid() -> u32 {
        std::process::id()
    }
}

/// 构造函数必然 `panic!` 的 actor，用于测试构造失败时的异常传播与进程存活。
struct PanickingCtorActor {
    _unused: i64,
}

#[rayrust::actor]
impl PanickingCtorActor {
    fn new(msg: String) -> Self {
        panic!("{}", msg);
    }

    fn ping(&self) -> i64 {
        1
    }
}

// ─── Rust Actor: Counter (using #[rayrust::actor] macro) ───────

/// A simple counter actor.
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

    fn reset(&mut self) {
        self.value = 0;
    }
}

// ─── 读环境变量的 actor ─────────────────────────────────────

/// `read` 直接读工人进程自己的环境变量：actor 创建时带的 runtime_env 里的
/// `env_vars` 是否真的装到了工人进程，由它交回的值判定。这个 actor 没有状态，
/// 每个方法都在同一个进程里读同一份环境。
struct EnvReader;

#[rayrust::actor]
impl EnvReader {
    fn new() -> Self {
        EnvReader
    }

    fn read(&self, name: String) -> Option<String> {
        std::env::var(&name).ok()
    }
}

// ─── 能结束自己的 actor ──────────────────────────────────────

/// `exit()` 调用 `rayrust::exit_actor()` 结束自己；`get()` 是接管普通方法的对照组：
/// actor 退出之后它必须失败，并借此证明没有按 `max_restarts` 重启。
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

// ─── 构造函数里退出 ─────────────────────────────────────────

/// 在 `new()` 里调用 `rayrust::exit_actor()` 的 actor。集群模式下创建任务因此失败，
/// 本地模式下那个调用是空操作、actor 正常创建。
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

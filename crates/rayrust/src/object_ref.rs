//! Object reference — a handle to a future result in the Ray object store.
//!
//! ## Async architecture
//!
//! `get_async()` runs one bounded synchronous `Get` on tokio's blocking pool:
//! - the call blocks inside `core_worker.Get(ids, timeout_ms, …)` in the C++
//!   SDK, so the tokio worker thread awaiting the future stays free
//! - Ray enforces the deadline inside `core_worker.Get`, starting when a pool
//!   thread picks the call up; a call queued behind a full pool waits for a
//!   free thread first and can take up to one more deadline to return
//! - the pool defaults to `max_blocking_threads` = 512 threads on top of the
//!   runtime's own threads; calls beyond that wait for a free thread instead
//!   of failing
//! - dropping the future does not cancel the `Get`: the pool thread stays on
//!   it until the deadline; shutting the runtime down does not wait for it, so
//!   a `Get` that already entered C++ runs to its own deadline, and one with no
//!   deadline never returns
//! - a failed task surfaces as `RayError::Task`/`Actor`/`Worker`/
//!   `Unreconstructable`/`Unclassified`, an expired wait as `RayError::Timeout`;
//!   `Unclassified` covers a zero-length object and an untranslated error frame
//!   (the frame is recognised before decoding, so the read entries return the
//!   text Ray recorded instead of the frame bytes)
//!
//! ## Runtime generation
//!
//! 运行时初始化成功时发布一个代次，关闭时把它撤掉；没有活着的运行时的时候
//! 代次是 0。每个引用记下创建时的代次（没有活着的运行时就是 0）。最后一个
//! `Ray` 句柄 drop 之后运行时关闭，但它创建的引用还能被持有：这些引用上的
//! `get`/`get_async`/`wait`/批量取在进入 FFI 之前就返回 `RayError::Runtime`，
//! 消息里带引用自己的代次与活着的运行时的代次。
//!
//! ## id 宽度
//!
//! 引用里存的就是 28 字节的 ObjectID。`ObjectRef::from`（走 `from_id`）接受任意长度
//! 的字节，因为 `From` 不能失败；宽度在每个使用点检查：所有读取入口（`get`、
//! `get_timeout`、`get_value*`、`get_raw_bytes`、`get_*_async`、`get_many*`、`wait`、
//! `wait_async`）与作为引用参数提交时，不是 28 字节的 id 一律返回 `RayError::Ffi`，
//! 进程不终止。上游的 `ObjectID::FromBinary` 对长度不符的输入直接 RAY_CHECK 终止进程
//! （`src/ray/common/id.h:430-441`），0 字节则变成永不就绪的 nil id。
//!
//! ## 引用计数
//!
//! 调用结果（`put`、`put_xlang`、`put_async`、`task_call*`、`actor_call*`）持有
//! core worker 给每个返回 id 加的那个初始引用（`task_manager.cc` 的
//! `AddOwnedObject(..., add_local_ref=true)`、`core_worker.cc` 的
//! `CoreWorker::Put`）。构造时既不加也不减，`Clone` 加一个、`Drop` 减一个，
//! 与 C++ 前端在 `ray/api/object_ref.h:42-49` 做的事相同。净计数也相同
//! （core worker 初始 1 → C++ 构造 `ObjectRef` 加到 2 再去掉初始引用 → 1；
//! 这里保持 1），省掉两次 FFI 调用。
//!
//! 从 id 字节重建的引用（`from_id`，`impl From<Vec<u8>>` 走这条）不持有 Ray 的
//! 本地引用：构造时不加、`Clone` 时不加、`Drop` 时不减，与 Python 前端的
//! `ray.ObjectRef(id_bytes)` 一致。对象能不能活到你读它的时候，取决于别处是否
//! 还有引用。已释放的 id 在本地 core worker 的引用表里不存在，读取会立即返回
//! `RayError::Ffi`（`ObjectUnknownOwner`）；对可能已释放的对象用 `get_timeout`，
//! 没有期限的 `get` 只在对象迟迟不就绪时不返回。
//!
//! 转成字节（`impl From<ObjectRef<T>> for Vec<u8>`）之后引用不再由 Rust 管理，
//! 计数保持不变、对象活到作业结束：调用方拿着 id 提交任务时，靠这一个引用保证
//! 对象不会被提前释放。
//!
//! 只有属于当前活着的运行时、且 id 长度是 28 字节的引用才计数。运行时关闭
//! （代次撤掉）之后 drop 引用只是跳过：core worker 已经关掉，引用表不存在，
//! 没有可减的引用。这个跳过省掉一次进入 C++ 的调用；进入 C++ 的引用计数调用
//! 与关闭之间的互斥在 C 包装层（`crates/rayrust-sys/wrapper/ray_c.cc` 的引用
//! 计数闸门）：关闭开始时闸门拒绝新的调用，并等已经进入的那些退出（微秒级），
//! 所以关闭一定能推进。闸门不覆盖 `get`/`put`/`wait` 这些阻塞调用。
//! 工人进程不经过 `Ray::connect`，代次恒为 0，工人侧的引用不参与引用计数。
//!
//! 嵌套引用（返回的对象里装着别的对象引用）在 Rust 侧不可见：Ray 的
//! `RayObject::GetNestedRefs()` 不在 wheel 附带的头文件里，`RayRuntime::Get`
//! 也只交回载荷本身。Python 返回这样的值时，引用位于 pickle5 段里，Rust 解不开
//! （`RayError::Serialization`）；它们既不会因为这个句柄而保持存活，也没有公开
//! 途径读到那份列表（#19）。
//!
//! ## 作为参数
//!
//! 引用可以顶替一个值参数传给 Rust 任务或 actor 方法（`ObjectRef<T>` 或
//! `&ObjectRef<T>`，见 [`crate::TaskArg`]）：驱动不必先读出来，集群模式下 core
//! worker 把对象的值投递给工人，本地模式下提交前取成值。调用器自己持有一份引用
//! 直到提交的 FFI 调用返回，调用方不需要保证句柄活到某个时刻。传进来的句柄必须是
//! 本进程当前持有的非跨语言引用（[`crate::TaskArg`] 检查 28 字节宽度、持有权以及
//! `!obj.is_xlang()`；跨语言引用取 `.id()` 传给 `task_call_python` / `actor_call_python`）。
//!
//! Rust 任务不能把可用的引用交回来。`TaskExecutor` 给 `AllocateReturnObject` 传
//! 的是空的嵌套引用表（`cpp/src/ray/runtime/task/task_executor.cc:251-258`），
//! 所以 Rust 任务交回的 id 没有附带的嵌套引用；驱动侧的 core worker 只在它的引用
//! 表里有这个 id 的 owner 时才能读（`core_worker.cc` 的 `GetObjects` 先查
//! `HasOwner`，没有就 `ObjectUnknownOwner`，与对象是否还活着无关）。任务想把对象
//! 交给驱动，就在驱动侧 put，或者让任务把值本身交回。Python 任务不受这条限制：
//! 它返回的嵌套引用由 Ray 保活（`nested_inlined_refs`），只是 Rust 侧读不出那份
//! 列表（见上一段）。
//!
//! For cross-language (Python) results, the data has a 9-byte XLANG header
//! that is stripped before deserialization.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::RayError;
use crate::serialize::{deserialize, deserialize_xlang};

/// 下一代次号：只在初始化成功时递增，关闭时不递增。
static NEXT_EPOCH: AtomicU64 = AtomicU64::new(1);

/// 活着的运行时的代次，0 表示此刻没有活着的运行时。
static LIVE_EPOCH: AtomicU64 = AtomicU64::new(0);

/// 单元测试里发布/撤掉代次是进程级状态，测试线程并行跑会互相踩：拿这把锁串起来。
#[cfg(test)]
pub(crate) static EPOCH_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn live_epoch() -> u64 {
    LIVE_EPOCH.load(Ordering::SeqCst)
}

/// 初始化成功之后发布这一代；返回发布的代次。
pub(crate) fn publish_live_epoch() -> u64 {
    let epoch = NEXT_EPOCH.fetch_add(1, Ordering::SeqCst);
    LIVE_EPOCH.store(epoch, Ordering::SeqCst);
    epoch
}

/// 运行时开始关闭：先撤掉活着的代次。此后重建的引用拿到 0，旧引用也不再相等。
pub(crate) fn retract_live_epoch() {
    LIVE_EPOCH.store(0, Ordering::SeqCst);
}

/// Ray 2.51.1 的 ObjectID 二进制长度（`src/ray/common/id.h`）。
const OBJECT_ID_LEN: usize = 28;

/// 只有属于当前活着的运行时、且 id 长度正确的引用才参与引用计数。
///
/// 工人进程里 `live_epoch()` 恒为 0，运行时关闭之后引用自己的代次也不再相等，
/// 这两种情况下都不进入 Ray 的引用计数接口；长度不对的 id 直接拒绝，因为
/// `ObjectID::FromBinary` 对非空且长度不符的输入用 `RAY_CHECK` 终止进程，对空串
/// 返回 nil（`src/ray/common/id.h:430-440`）。
fn ref_counting_live(epoch: u64, id: &[u8]) -> bool {
    epoch != 0 && epoch == live_epoch() && id.len() == OBJECT_ID_LEN
}

/// 加一个本地引用。调用方已经确认 `ref_counting_live` 成立；失败意味着守卫与
/// C++ 侧的状态不一致，就地崩溃。
fn add_local_reference(id: &[u8]) {
    let rc = unsafe {
        rayrust_sys::ray_add_local_reference(id.as_ptr() as *const std::os::raw::c_char, id.len())
    };
    assert_eq!(
        rc,
        0,
        "ray_add_local_reference failed: {:?}",
        rayrust_sys::last_error()
    );
}

/// 减一个本地引用。失败条件与 `add_local_reference` 相同。
fn remove_local_reference(id: &[u8]) {
    let rc = unsafe {
        rayrust_sys::ray_remove_local_reference(
            id.as_ptr() as *const std::os::raw::c_char,
            id.len(),
        )
    };
    assert_eq!(
        rc,
        0,
        "ray_remove_local_reference failed: {:?}",
        rayrust_sys::last_error()
    );
}

/// 拒绝没有活着的运行时、或者属于已经关闭的运行时的引用。
///
/// 工人进程（`librayrust_worker.so`）由 `default_worker` 调用 `ray::Init` 初始化，
/// 不经过 `init_with_config`，`LIVE_EPOCH` 恒为 0。当 `LIVE_EPOCH` 为 0、没有正在
/// 进行的关闭，且 `runtime_is_borrowed()` 为真（本 .so 内联的 `RayRuntimeHolder`
/// 为空而进程里已有别人建的运行时，也就是工人进程）时，代次为 0 的重建引用放行
/// 进入 C 包装层。
/// 其他进程里的代次 0 引用仍被拒：直接调 `ray_init` 的原始 C 驱动里
/// `ray::IsInitialized()` 同样为真，但那份运行时就是本 .so 的 holder 填的，
/// `runtime_is_borrowed()` 在那里为假。
pub(crate) fn check_epoch(epoch: u64) -> Result<(), RayError> {
    let live = live_epoch();
    if epoch != 0 && epoch == live {
        return Ok(());
    }
    if live == 0 {
        if epoch == 0 && !crate::runtime::shutting_down() && crate::runtime::runtime_is_borrowed() {
            return Ok(());
        }
        return Err(RayError::Runtime(format!(
            "object ref belongs to Ray runtime epoch {} and no runtime is alive",
            epoch
        )));
    }
    Err(RayError::Runtime(format!(
        "object ref was created by Ray runtime epoch {} which has been shut down, the live runtime is epoch {}",
        epoch, live
    )))
}

/// A reference to an object stored in the Ray object store.
///
/// `ObjectRef` methods (`get`, `get_async`, `wait`) call the global Ray
/// runtime directly rather than going through `&Ray`. This is intentional:
/// `ObjectRef` is a value handle (like a file descriptor), not a context
/// reference. It can be freely cloned, stored, and awaited without holding
/// a borrow on `Ray`. The underlying C++ SDK is a global singleton, so
/// there is no ambiguity about which runtime instance is used.
///
/// 引用带创建时的运行时代次。运行时只在最后一个 `Ray` 句柄 drop 时关闭，
/// 引用可以活过它：这样的引用上调用 `get`/`get_async`/`get_timeout`/
/// `wait`/批量取会返回 `RayError::Runtime`，消息里带引用自己的代次与活着的
/// 运行时的代次，不会把调用交给已经关闭的 C++ 运行时。关闭之后从 id 字节
/// 重建的引用（`impl From<Vec<u8>>` 走 `from_id`）拿到代次 0，同样被拒。
/// 调用已经开始、另一个线程随后 drop 最后一个句柄时，那次已经进入 Ray SDK 的调用
/// 仍然会与关闭竞争，可能让进程以退出码 1 终止：关闭里的 `CoreWorker::Disconnect()`
/// （`src/ray/core_worker/core_worker.cc:563-596`）先关掉与 raylet 的 IPC 连接，在途
/// `Get` 析构 `ipc::ScopedResponse`（`src/ray/raylet_ipc_client/raylet_ipc_client_interface.h:67-72`）
/// 时在 `:70` 对 `Broken pipe` 执行 `RAY_CHECK`。这条风险覆盖 `get`、`get_timeout`
/// 与 `get_async`（`get_async` 在 `spawn_blocking` 里调同一个
/// `runtime::get_raw_timeout`）；这次调用也可能以错误返回（实测到过
/// `Get object error: IOError: Broken pipe`）。关闭开始之后的新调用由代次与关闭闸门
/// 挡成 `RayError::Runtime`。
///
/// 调用结果构造的引用同时持有 Ray 的一个本地引用：clone 时加一个，drop 时减一个。
/// 集群模式（core worker 已初始化）下最后一个引用 drop 之后对象可能被释放，要按
/// id 继续读取就得持有引用；本地模式与工人进程不参与引用计数，对象不会被释放。
/// 从 id 字节重建的引用（`impl From<Vec<u8>>`）不持有引用，见 `from_id`。
///
/// 引用可以顶替一个值参数传给任务或 actor 方法（`ObjectRef<T>` 或
/// `&ObjectRef<T>`，见 [`crate::TaskArg`]），任务拿到的是那个引用的值；传进来的
/// 句柄必须是本进程当前持有的引用。Rust 任务不能把可用的引用交回来：交回的 id
/// 没有嵌套引用，驱动侧也没有它的 owner 路径，详见模块文档的「作为参数」。
#[derive(Debug)]
pub struct ObjectRef<T> {
    pub(crate) id: Vec<u8>,
    pub(crate) is_xlang: bool,
    /// 创建时的运行时代次，用于拒绝属于已经关闭的运行时的引用。
    pub(crate) epoch: u64,
    /// 这个句柄是否持有 Ray 的本地引用：调用结果持有，从 id 字节重建的不持有。
    /// 只有持有的引用在 `Clone` / `Drop` 上增减计数。
    pub(crate) holds_reference: bool,
    _marker: PhantomData<T>,
}

/// 手写 Clone：派生版本会额外要求 `T: Clone`，而引用本身只需要复制 id。
///
/// 持有 Ray 本地引用的引用复制之后再加一个，不持有的复制也不加。
impl<T> Clone for ObjectRef<T> {
    fn clone(&self) -> Self {
        let cloned = ObjectRef {
            id: self.id.clone(),
            is_xlang: self.is_xlang,
            epoch: self.epoch,
            holds_reference: self.holds_reference,
            _marker: PhantomData,
        };
        if cloned.counts_reference() {
            add_local_reference(&cloned.id);
        }
        cloned
    }
}

/// 减掉这个引用持有的那一个本地引用；最后一个引用 drop 之后对象可能被 Ray 释放。
/// 从 id 字节重建的引用不持有引用，什么都不做。
///
/// 引用计数调用失败时就地崩溃。在 unwinding 过程中 drop 引用而 panic 会终止
/// 进程，这条不变量被破坏时就是这样的结果。
impl<T> Drop for ObjectRef<T> {
    fn drop(&mut self) {
        if self.counts_reference() {
            remove_local_reference(&self.id);
        }
    }
}

/// 解码一个值；载荷是未翻译错误对象的帧时返回 `RayError::Unclassified`。
///
/// 判定排在解码之前：帧本身是合法 msgpack，对 `Vec<u8>` 这类目标类型解码会成功，
/// 只看解码结果会把它当成正常值返回。
fn decode_value<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    is_xlang: bool,
) -> Result<T, RayError> {
    if let Some(err) = crate::runtime::untranslated_error("ray_get", bytes) {
        return Err(err);
    }
    if is_xlang {
        deserialize_xlang(bytes)
    } else {
        deserialize(bytes)
    }
}

/// 解码通用 msgpack `Value`；载荷是未翻译错误对象的帧时返回 `RayError::Unclassified`。
/// 判定同样排在解码之前：剥掉 9 字节头后的 `bin` 能解成 `Value::Binary`。
fn decode_value_generic(bytes: &[u8], is_xlang: bool) -> Result<rmpv::Value, RayError> {
    if let Some(err) = crate::runtime::untranslated_error("ray_get", bytes) {
        return Err(err);
    }
    if is_xlang {
        crate::serialize::deserialize_xlang_value(bytes)
    } else {
        crate::serialize::deserialize_value(bytes)
    }
}

impl<T> ObjectRef<T> {
    /// 这个引用是否要在增减时调用 Ray 的引用计数接口。
    ///
    /// 同时看句柄自己的代次与 `holds_reference`：从 id 字节重建的句柄、
    /// 属于已经关闭的运行时的句柄都为假。`TaskArg` 用它拒绝来路不明的 id。
    pub(crate) fn counts_reference(&self) -> bool {
        self.holds_reference && ref_counting_live(self.epoch, &self.id)
    }

    /// 从 id 字节重建引用（`impl From<Vec<u8>>` 走这条）。
    ///
    /// 不持有 Ray 的本地引用：构造时不加、`Clone` 时不加、`Drop` 时不减，与 Python
    /// 前端的 `ray.ObjectRef(id_bytes)` 一致。对象能不能活到你读它的时候取决于别处
    /// 是否还有引用。已释放的 id 在本地 core worker 的引用表里不存在，读取立即返回
    /// `RayError::Ffi`；对可能已释放的对象用 `get_timeout`，没有期限的 `get` 只在
    /// 对象迟迟不就绪时不返回。
    ///
    /// 任意长度的字节都接受（`From` 不能失败），宽度在使用时检查：所有读取入口与
    /// 作为引用参数提交时，不是 28 字节的 id 返回 `RayError::Ffi`，进程不终止。
    /// 见模块文档的「id 宽度」。
    ///
    /// 打戳取当前活着的运行时的代次；没有活着的运行时就是 0，此后恒被拒。
    pub(crate) fn from_id(id: Vec<u8>) -> Self {
        ObjectRef {
            id,
            is_xlang: false,
            epoch: live_epoch(),
            holds_reference: false,
            _marker: PhantomData,
        }
    }

    /// 调用结果（`put`、`task_call*`、`actor_call*`）的引用：沿用 core worker
    /// 已经加好的初始引用，构造时既不加也不减，`Clone` 加一个、`Drop` 减一个。
    ///
    /// core worker 给每个返回 id 加一个初始引用（`task_manager.cc` 的
    /// `AddOwnedObject(..., add_local_ref=true)`、`core_worker.cc` 的
    /// `CoreWorker::Put`），C++ 前端在 `ObjectRef(id)` 构造时加一、随后用
    /// `RemoveLocalReference` 去掉那个初始引用（`task_caller.h:105`、
    /// `actor_task_caller.h:92`、`api.h:216`），净计数 1。这里保持那一个初始
    /// 引用，`Drop` 时减掉它，净计数相同而省掉两次 FFI 调用。
    pub(crate) fn from_call_id(id: Vec<u8>) -> Self {
        ObjectRef {
            id,
            is_xlang: false,
            epoch: live_epoch(),
            holds_reference: true,
            _marker: PhantomData,
        }
    }

    pub(crate) fn from_call_xlang(id: Vec<u8>) -> Self {
        ObjectRef {
            id,
            is_xlang: true,
            epoch: live_epoch(),
            holds_reference: true,
            _marker: PhantomData,
        }
    }

    /// Get the raw object ID as bytes.
    ///
    /// 字节本身不持有引用：引用 drop 之后对象可能被 Ray 释放，之后拿这些字节
    /// 重建的引用（`impl From<Vec<u8>>`）同样不持有 Ray 的本地引用，对象能不能活到
    /// 你读它的时候取决于别处是否还有引用。对可能已经释放的对象用 `get_timeout`；
    /// 要在别处继续按 id 读取并保住对象，把 `ObjectRef` 一起交出去。
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// Get the object ID as a hex string (for debugging).
    pub fn id_hex(&self) -> String {
        self.id.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Whether this reference carries a cross-language payload (9-byte XLANG
    /// header and `"XLANG"` / `"PYTHON"` metadata).
    ///
    /// Returns `true` for handles created by [`Ray::put_xlang`](crate::Ray::put_xlang),
    /// [`Ray::task_call_python`](crate::Ray::task_call_python),
    /// [`Ray::task_call_python_with_options`](crate::Ray::task_call_python_with_options),
    /// and [`Ray::actor_call_python`](crate::Ray::actor_call_python).
    /// Returns `false` for handles created by [`Ray::put`](crate::Ray::put),
    /// [`Ray::put_async`](crate::Ray::put_async),
    /// [`Ray::task_call`](crate::Ray::task_call),
    /// [`Ray::task_call_async`](crate::Ray::task_call_async),
    /// [`Ray::actor_call`](crate::Ray::actor_call),
    /// [`Ray::actor_call_async`](crate::Ray::actor_call_async), and
    /// `ObjectRef::from`.
    pub fn is_xlang(&self) -> bool {
        self.is_xlang
    }

    /// Retrieve the object value (blocks until ready).
    ///
    /// 没有期限：`get` 走 `get_raw`（`timeout = -1`），对象永远不会就绪时它不返回。
    /// 需要期限用 `get_timeout` 或 `get_timeout_async`。id 不是 28 字节时返回
    /// `RayError::Ffi`，见模块文档的「id 宽度」。
    pub fn get(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        check_epoch(self.epoch)?;
        let bytes = crate::runtime::get_raw(&self.id)?;
        decode_value(&bytes, self.is_xlang)
    }

    /// Retrieve the object as a generic msgpack Value (blocks until ready).
    ///
    /// Useful when the exact type is unknown at compile time, e.g. when
    /// calling a Python function that returns a complex/nested structure.
    /// The `is_xlang` flag is respected automatically.
    ///
    /// 没有期限，与 `get` 共用 `get_raw`（`timeout = -1`）。需要期限用
    /// `get_value_timeout_async`。
    pub fn get_value(&self) -> Result<rmpv::Value, RayError> {
        check_epoch(self.epoch)?;
        let bytes = crate::runtime::get_raw(&self.id)?;
        decode_value_generic(&bytes, self.is_xlang)
    }

    /// Retrieve the object value with a timeout (in milliseconds).
    ///
    /// `-1` waits forever; other negative values return `RayError::Ffi`. An id
    /// that is not 28 bytes returns `RayError::Ffi` too, before the id reaches
    /// Ray.
    pub fn get_timeout(&self, timeout_ms: i32) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        check_epoch(self.epoch)?;
        let bytes = crate::runtime::get_raw_timeout(&self.id, timeout_ms)?;
        decode_value(&bytes, self.is_xlang)
    }

    /// Asynchronously retrieve the object value.
    ///
    /// The blocking `Get` runs on tokio's blocking pool; the awaiting task does
    /// not hold a tokio worker thread. The deadline is 300 seconds by default;
    /// `get_timeout_async` describes how it is applied.
    pub async fn get_async(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.get_timeout_async(std::time::Duration::from_secs(300))
            .await
    }

    /// Asynchronously retrieve the object value with a custom timeout.
    ///
    /// The deadline is handed to Ray's `Get`, which bounds the whole wait,
    /// including the wait for a lost object to be reconstructed. The deadline
    /// starts when a blocking-pool thread picks the call up, so a call queued
    /// behind a full pool waits for a free thread first and can take up to one
    /// more deadline to return. Dropping the future does not cancel the `Get`:
    /// the pool thread stays on it until the deadline. Shutting the runtime
    /// down does not wait for that thread, so a `Get` that already entered C++
    /// runs to its own deadline; the epoch check runs inside the blocking-pool
    /// closure and rejects a call that starts after the shutdown.
    pub async fn get_timeout_async(&self, timeout: std::time::Duration) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let id = self.id.clone();
        let epoch = self.epoch;
        let timeout_ms = crate::runtime::duration_to_ms(timeout);
        let bytes = tokio::task::spawn_blocking(move || {
            check_epoch(epoch)?;
            crate::runtime::get_raw_timeout(&id, timeout_ms)
        })
        .await
        .map_err(|e| RayError::Runtime(format!("spawn_blocking join error: {}", e)))??;

        decode_value(&bytes, self.is_xlang)
    }

    /// Asynchronously retrieve the object as a generic msgpack Value.
    ///
    /// Same blocking-pool `Get` as `get_async()`.
    /// Useful for Python cross-language calls with unknown return types.
    pub async fn get_value_async(&self) -> Result<rmpv::Value, RayError> {
        self.get_value_timeout_async(std::time::Duration::from_secs(300))
            .await
    }

    /// Asynchronously retrieve the object as a generic msgpack Value with timeout.
    pub async fn get_value_timeout_async(
        &self,
        timeout: std::time::Duration,
    ) -> Result<rmpv::Value, RayError> {
        let id = self.id.clone();
        let epoch = self.epoch;
        let timeout_ms = crate::runtime::duration_to_ms(timeout);
        let bytes = tokio::task::spawn_blocking(move || {
            check_epoch(epoch)?;
            crate::runtime::get_raw_timeout(&id, timeout_ms)
        })
        .await
        .map_err(|e| RayError::Runtime(format!("spawn_blocking join error: {}", e)))??;

        decode_value_generic(&bytes, self.is_xlang)
    }

    /// Get raw bytes from the object store (for debugging).
    ///
    /// 没有期限，与 `get` 共用 `get_raw`（`timeout = -1`），对象永远不会就绪时它
    /// 不返回。要原始字节同时要期限，用 `get_timeout` / `get_value_timeout_async`，
    /// 或者先用 `wait(..., timeout_ms)` 确认就绪再读。
    pub fn get_raw_bytes(&self) -> Result<Vec<u8>, RayError> {
        check_epoch(self.epoch)?;
        crate::runtime::get_raw(&self.id)
    }

    /// Cast the phantom type parameter.
    ///
    /// 引用计数随引用一起转移：原引用不执行 `Drop`，计数不变；「是否持有引用」
    /// 一并带到新的引用上。
    pub fn cast<U>(self) -> ObjectRef<U> {
        let mut this = std::mem::ManuallyDrop::new(self);
        ObjectRef {
            id: std::mem::take(&mut this.id),
            is_xlang: this.is_xlang,
            epoch: this.epoch,
            holds_reference: this.holds_reference,
            _marker: PhantomData,
        }
    }
}

impl<T> From<Vec<u8>> for ObjectRef<T> {
    /// 从 id 字节重建引用。不持有 Ray 的本地引用，见 `ObjectRef::from_id`。
    /// 任意长度的字节都接受，宽度在用到时检查：所有读取入口与作为引用参数提交时，
    /// 不是 28 字节的 id 返回 `RayError::Ffi`，进程不终止（模块文档的「id 宽度」）。
    fn from(id: Vec<u8>) -> Self {
        ObjectRef::from_id(id)
    }
}

impl<T> From<ObjectRef<T>> for Vec<u8> {
    /// 字节交出去之后引用不再由 Rust 管理，计数保持不变、对象活到作业结束。
    /// 这是有意的保留：调用方拿着 id 去提交任务时，靠这一个引用保证对象不会被
    /// 提前释放；这个引用没有任何 API 可以交还。
    fn from(obj: ObjectRef<T>) -> Vec<u8> {
        let mut obj = std::mem::ManuallyDrop::new(obj);
        std::mem::take(&mut obj.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 四个不计数的情况与一个计数的情况。这个测试会发布再撤掉代次：先把五种取值
    /// 算出来，再撤代次，最后统一断言，断言失败也不会把代次留给后面的测试。
    #[test]
    fn ref_counting_live_needs_the_live_epoch_and_a_28_byte_id() {
        let _guard = EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = [0u8; OBJECT_ID_LEN];
        let epoch = publish_live_epoch();
        let cases = [
            // 没有活着的运行时，代次是 0
            (!ref_counting_live(0, &id), "代次 0 不计数"),
            // 代次与活着的运行时不同
            (!ref_counting_live(epoch + 1, &id), "代次不同不计数"),
            // id 长度不对
            (
                !ref_counting_live(epoch, &id[..OBJECT_ID_LEN - 1]),
                "27 字节不计数",
            ),
            (!ref_counting_live(epoch, b""), "空 id 不计数"),
            // 代次匹配且 id 长度正确
            (ref_counting_live(epoch, &id), "代次匹配且 28 字节才计数"),
        ];
        retract_live_epoch();
        let after_retract = !ref_counting_live(epoch, &id);
        for (ok, what) in cases {
            assert!(ok, "{}", what);
        }
        assert!(after_retract, "撤掉代次之后不计数");
    }

    #[test]
    fn decode_value_checks_the_frame_before_decoding() {
        // 1. 正常跨语言值：剥头后是 msgpack 数组，按 `Vec<u8>` 读出原值。
        let ok_bytes =
            crate::serialize::serialize_xlang(&vec![1u8, 2, 3]).expect("serialize bytes");
        assert_eq!(
            decode_value::<Vec<u8>>(&ok_bytes, true).expect("decode bytes"),
            vec![1, 2, 3]
        );

        let text = b"ValueError: runtime_env failed";
        let mut pb = Vec::new();
        pb.push((5 << 3) | 2);
        pb.push(text.len() as u8);
        pb.extend_from_slice(text);
        pb.push((11 << 3) | 0);
        pb.push(7);
        let mut body = vec![0xc4, pb.len() as u8];
        body.extend_from_slice(&pb);
        let mut frame = vec![body.len() as u8];
        frame.extend_from_slice(&[0u8; 8]);
        frame.extend_from_slice(&body);

        // 2. 错误帧按 `Vec<u8>` 读：剥头后的 `bin` 本身可解，判定必须在解码之前。
        match decode_value::<Vec<u8>>(&frame, true) {
            Err(RayError::Unclassified(msg)) => {
                assert!(msg.starts_with(
                    "ray_get: the object carries an error the C++ SDK does not translate: "
                ));
                assert!(msg.ends_with("ValueError: runtime_env failed"));
            }
            other => panic!("xlang 错误帧按 Vec<u8> 读出 {:?}", other),
        }

        // 3. 同一个帧按 `rmpv::Value` 读，同样不得返回 `Value::Binary`。
        assert!(matches!(
            decode_value_generic(&frame, true),
            Err(RayError::Unclassified(_))
        ));

        // 4. 截断的 `bin8`：不是帧，仍是反序列化错误。
        let broken = [0xc4u8]; // 截断的 bin8：解码失败且不满足 9 + N 错误帧
        assert!(matches!(
            decode_value::<i64>(&broken, false),
            Err(RayError::Serialization(_))
        ));
        assert!(matches!(
            decode_value_generic(&broken, false),
            Err(RayError::Serialization(_))
        ));
    }

    #[test]
    fn check_epoch_rejects_uninitialized_rebuilt_and_stale_refs() {
        let _guard = EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "测试进程未启动 Ray runtime"
        );
        assert_eq!(live_epoch(), 0);
        assert!(
            !crate::runtime::runtime_is_borrowed(),
            "单元测试进程里没有别人建的运行时，谓词却为真"
        );

        // 1. LIVE_EPOCH == 0、谓词为假时，重建引用仍被拒成 RayError::Runtime
        let rebuilt_before_init: ObjectRef<i64> = ObjectRef::from(vec![0x11u8; OBJECT_ID_LEN]);
        let before_init_res = rebuilt_before_init.get_timeout(50);

        // 2. 发布代次后模拟驱动关闭（retract_live_epoch）：旧代次引用与关闭后重建引用均被拒
        let epoch1 = publish_live_epoch();
        let ref_epoch1: ObjectRef<i64> = ObjectRef::from(vec![0x22u8; OBJECT_ID_LEN]);
        let live_ok = check_epoch(ref_epoch1.epoch);
        let zero_while_live = check_epoch(0);

        retract_live_epoch();
        let stale_after_shutdown = ref_epoch1.get_timeout(50);
        let rebuilt_after_shutdown: ObjectRef<i64> = ObjectRef::from(vec![0x22u8; OBJECT_ID_LEN]);
        let after_shutdown_res = rebuilt_after_shutdown.get_timeout(50);

        // 3. 再次发布新代次后，第一代的旧引用仍被拒
        let epoch2 = publish_live_epoch();
        let stale_in_new_epoch = ref_epoch1.get_timeout(50);
        retract_live_epoch();

        assert!(epoch1 != 0 && epoch2 > epoch1);
        assert!(live_ok.is_ok(), "当前活跃代次检查未通过: {:?}", live_ok);
        assert!(matches!(
            before_init_res,
            Err(RayError::Runtime(ref msg)) if msg.contains("epoch 0 and no runtime is alive")
        ));
        assert!(matches!(
            zero_while_live,
            Err(RayError::Runtime(ref msg)) if msg.contains("epoch 0 which has been shut down")
        ));
        assert!(matches!(
            stale_after_shutdown,
            Err(RayError::Runtime(ref msg)) if msg.contains("no runtime is alive")
        ));
        assert!(matches!(
            after_shutdown_res,
            Err(RayError::Runtime(ref msg)) if msg.contains("epoch 0 and no runtime is alive")
        ));
        assert!(matches!(
            stale_in_new_epoch,
            Err(RayError::Runtime(ref msg)) if msg.contains("has been shut down")
        ));
    }

    #[test]
    fn decode_value_rejects_a_non_xlang_frame_before_reading_the_length() {
        // 非跨语言读：帧首值是 msgpack 整数 N（正文长度），后跟头内填充与 `bin` 正文；
        // 前置判定让它返回携带错误文本的 `RayError::Unclassified`，避免进入解码器被
        // 报告为尾随字节的 `RayError::Serialization`。
        let text = b"ValueError: unschedulable";
        let mut pb = Vec::new();
        pb.push((5 << 3) | 2);
        pb.push(text.len() as u8);
        pb.extend_from_slice(text);
        let mut body = vec![0xc4, pb.len() as u8];
        body.extend_from_slice(&pb);
        let mut frame = vec![body.len() as u8];
        frame.extend_from_slice(&[0u8; 8]);
        frame.extend_from_slice(&body);

        match decode_value::<i64>(&frame, false) {
            Err(RayError::Unclassified(msg)) => {
                assert!(msg.contains("ray_get: the object carries an error"));
                assert!(msg.ends_with("ValueError: unschedulable"));
            }
            other => panic!("非跨语言错误帧按 i64 读出 {:?}", other),
        }
        assert!(matches!(
            decode_value_generic(&frame, false),
            Err(RayError::Unclassified(_))
        ));
    }

    #[test]
    fn is_xlang_tracks_constructor_clone_and_cast() {
        let _guard = EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(live_epoch(), 0);

        let rebuilt: ObjectRef<i64> = ObjectRef::from_id(vec![1u8; OBJECT_ID_LEN]);
        assert!(!rebuilt.is_xlang());
        assert!(!rebuilt.clone().is_xlang());
        assert!(!rebuilt.cast::<i32>().is_xlang());

        let rust_call: ObjectRef<i64> = ObjectRef::from_call_id(vec![2u8; OBJECT_ID_LEN]);
        assert!(!rust_call.is_xlang());
        assert!(!rust_call.clone().is_xlang());
        assert!(!rust_call.cast::<i32>().is_xlang());

        let xlang_call: ObjectRef<i64> = ObjectRef::from_call_xlang(vec![3u8; OBJECT_ID_LEN]);
        assert!(xlang_call.is_xlang());
        assert!(xlang_call.clone().is_xlang());
        assert!(xlang_call.cast::<i32>().is_xlang());
    }
}

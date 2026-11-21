//! Ray runtime — init, shutdown, put, get, wait, task/actor operations.
//!
//! IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
//! We store them as `Vec<u8>` internally, not as Rust `String`.

use std::os::raw::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use rayrust_sys::{self, build_args_array, to_cstring, CBytesGuard, ErrorKind, RayBytes};

use crate::actor_registry::{self, ActorLanguage};
use crate::error::RayError;
use crate::object_ref::{check_epoch, ObjectRef};
use crate::serialize::serialize;
use crate::task_args::TaskArgs;

/// Result of `wait()`: (ready, unready) object references.
pub type WaitResult<T> = Result<(Vec<ObjectRef<T>>, Vec<ObjectRef<T>>), RayError>;

/// Map the class Ray recorded for a failure onto a `RayError` variant.
fn error_from_kind(kind: ErrorKind, msg: String) -> RayError {
    match kind {
        ErrorKind::None | ErrorKind::Generic | ErrorKind::Ray => RayError::Ffi(msg),
        ErrorKind::Actor => RayError::Actor(msg),
        ErrorKind::Task => RayError::Task(msg),
        ErrorKind::Worker => RayError::Worker(msg),
        ErrorKind::Unreconstructable => RayError::Unreconstructable(msg),
        ErrorKind::Timeout => RayError::Timeout(msg),
    }
}

/// Build an FFI error with the C-side last_error message if available.
fn ffi_error(context: &str) -> RayError {
    let msg = rayrust_sys::last_error()
        .unwrap_or_else(|| "unknown FFI error".to_string());
    error_from_kind(rayrust_sys::last_error_kind(), format!("{}: {}", context, msg))
}

/// Build an FFI error with context and a formatted detail string.
fn ffi_error_detail(context: &str, detail: impl std::fmt::Display) -> RayError {
    let msg = rayrust_sys::last_error()
        .unwrap_or_else(|| "unknown FFI error".to_string());
    error_from_kind(
        rayrust_sys::last_error_kind(),
        format!("{} ({}): {}", context, detail, msg),
    )
}

// ─── Config ───────────────────────────────────────────────────

/// Actor lifetime determines whether an actor outlives the job that created it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum ActorLifetime {
    /// The actor is destroyed when the job that created it ends (default).
    #[default]
    NonDetached = 0,
    /// The actor outlives the job that created it; must be killed manually.
    Detached = 1,
}

/// Configuration for connecting to a Ray cluster.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RayConfig {
    /// Address of the head node, e.g. "192.168.42.141:6379".
    ///
    /// The run mode comes from [`RayConfig::local_mode`] alone; an empty address
    /// does not select local mode.
    ///
    /// Local mode ignores this field: nothing of it reaches the SDK and nothing of
    /// it is validated. It still takes part in the config comparison of
    /// [`Ray::connect`], so two local-mode configs whose addresses differ are
    /// reported as different configs even though neither address was used.
    ///
    /// Cluster mode with a non-empty address requires `host:port`: a bracketed
    /// IPv6 host, a non-empty host, and a port of decimal digits in
    /// `0..=65535`. The port is read as the whole string, so a suffix the SDK
    /// would silently drop (`"host:6379\n"`) is refused too. Anything else makes
    /// the first [`Ray::connect`] return [`RayError::Ffi`] naming this field,
    /// before the SDK is entered.
    ///
    /// What upstream did with each shape this entry refuses, measured on Ray
    /// 2.51.1: a shape `ParseAddress` cannot split (no `:`, an IPv6 host without
    /// brackets) stopped the driver in `ConfigInternal::SetBootstrapAddress`
    /// (cpp/src/ray/config_internal.cc:255); an empty port, or one that does not
    /// start with decimal digits, stopped it at `:261`; a port above `65535`, and
    /// a port whose suffix `std::from_chars` drops, reached the GCS and stopped it
    /// about 35 seconds later (cpp/src/ray/util/process_helper.cc:110); a negative
    /// port stopped it in `GetNodeIpAddress` (cpp/src/ray/util/util.cc:42); an
    /// empty host made upstream take the bootstrap address as unset and start a
    /// head node instead (cpp/src/ray/util/process_helper.cc:84-92).
    ///
    /// Cluster mode with an empty address leaves the bootstrap address unset: the
    /// SDK reads `RAY_ADDRESS` (cpp/src/ray/config_internal.cc:207-215) and, when
    /// that is unset too, starts a head node itself
    /// (cpp/src/ray/util/process_helper.cc:80-92).
    pub address: String,
    /// Run in local mode (single process, no cluster).
    /// This is the field that decides the run mode; `address` is ignored while it
    /// is true.
    pub local_mode: bool,
    /// Code search path for dynamic libraries (worker side).
    pub code_search_path: Vec<String>,
    /// Namespace for this job.
    pub namespace: String,
    /// Node IP address to use when registering with the cluster.
    /// If empty, the C++ SDK auto-detects (may fail with multiple NICs).
    pub node_ip: String,
    /// Runtime environment JSON string (e.g. `{"pip": ["pkg1", "pkg2"]}`).
    /// If empty, no runtime_env is set.
    pub runtime_env: String,
    /// Directory for Ray logs. If empty, uses default.
    pub log_dir: String,
    /// Default actor lifetime for actors created in this job.
    /// `Detached` actors outlive the job that created them.
    pub default_actor_lifetime: ActorLifetime,
}

impl RayConfig {
    /// Create a new config pointing to a remote cluster.
    ///
    /// `address` must be `host:port`; a malformed one is rejected by the first
    /// [`Ray::connect`] with [`RayError::Ffi`]. Use [`RayConfig::local`] for a
    /// single process.
    pub fn new(address: impl Into<String>) -> Self {
        RayConfig {
            address: address.into(),
            ..Default::default()
        }
    }

    /// Use local mode (no cluster, single process).
    pub fn local() -> Self {
        RayConfig {
            local_mode: true,
            ..Default::default()
        }
    }

    /// Set the node IP address.
    pub fn node_ip(mut self, ip: impl Into<String>) -> Self {
        self.node_ip = ip.into();
        self
    }

    /// Set the code search path (directories or .so files for the worker).
    pub fn code_search_path(mut self, paths: Vec<String>) -> Self {
        self.code_search_path = paths;
        self
    }

    /// Set the runtime environment JSON string.
    /// Example: `{"pip": ["numpy", "pandas"]}` or `{"env_vars": {"FOO": "bar"}}`
    pub fn runtime_env(mut self, json: impl Into<String>) -> Self {
        self.runtime_env = json.into();
        self
    }

    /// Set the log directory.
    pub fn log_dir(mut self, dir: impl Into<String>) -> Self {
        self.log_dir = dir.into();
        self
    }

    /// Set the default actor lifetime.
    /// Pass `ActorLifetime::Detached` so actors outlive the job.
    pub fn default_actor_lifetime(mut self, lifetime: ActorLifetime) -> Self {
        self.default_actor_lifetime = lifetime;
        self
    }

    /// Set the job-level namespace for named actors.
    /// Named actors are registered in this namespace by default.
    /// Use `ActorOptions::ray_namespace()` to override per-actor.
    ///
    /// Local mode accepts the value and reports it back ([`Ray::namespace`]), but
    /// its actor registration and lookup ignore it: the local runtime keeps one
    /// flat name table.
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = ns.into();
        self
    }
}

// ─── Ray Context (RAII) ────────────────────────────────────────

/// Ray runtime context — owns the init→shutdown lifecycle.
///
/// Created via [`Ray::connect`] or [`Ray::local`]. All handles in a process
/// share one runtime: Ray is initialized once for as long as the runtime lives,
/// and it is shut down when the last handle is dropped; a `connect` after that
/// initializes a new runtime. A second `connect` with the same config returns
/// a handle to the live runtime without re-initializing it; a config that
/// differs returns [`RayError::Runtime`] naming the fields that differ.
///
/// `Ray` is `!Clone`: [`Ray::connect`] is the only way to obtain a handle, and
/// it is what checks the config against the live runtime. Share one handle by
/// passing `&Ray`.
///
/// # Local-mode shutdown
///
/// In local mode, dropping the last [`Ray`] handle waits for every task
/// submitted through the current runtime to finish (including its result
/// becoming ready in the local object store) before shutting Ray down. Cluster
/// mode does not wait for submitted tasks on drop. A local-mode task that never
/// returns causes dropping the last handle to wait indefinitely. The same applies
/// to dropping the last handle from inside a local-mode task body: the drain
/// waits for that task's own result, which cannot become ready while the task
/// is inside the drop. A Rust `panic!` inside a local-mode task crosses an
/// `extern "C"` callback boundary and aborts the process immediately, so it
/// does not leave shutdown waiting forever.
/// Skipping `Drop` (for example with `std::mem::forget` or
/// `std::process::exit` while a handle is still alive) bypasses this drain.
///
/// While the last handle is being dropped, which in local mode lasts until the
/// submitted tasks have finished, [`Ray::connect`] returns [`RayError::Runtime`]
/// instead of waiting for the shutdown to finish; a call from inside a task body
/// (for example `Ray::local()`) gets the same error. That is what lets the drain
/// make progress when a task tries to obtain a handle of its own.
///
/// # Example
/// ```no_run
/// use rayrust::prelude::*;
///
/// let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379")).unwrap();
/// let obj = ray.put(&42i32).unwrap();
/// let val: i32 = obj.get().unwrap();
/// // drop(ray) → shutdown when this is the last handle
/// ```
pub struct Ray {
    // 只为在最后一个句柄 drop 时触发 `RayInner` 的析构而持有，没有读取方。
    #[allow(dead_code)]
    inner: Arc<RayInner>,
}

/// 句柄共享的运行时。最后一个 `Arc` 释放时关闭 Ray。
pub(crate) struct RayInner {
    /// 初始化时用的配置，第二次 `connect` 用它判断配置是否一致。
    config: RayConfig,
}

impl Drop for RayInner {
    fn drop(&mut self) {
        // 先置位，再动登记：`Ray::connect` 的第二道检查依赖这个顺序。
        SHUTTING_DOWN.store(true, Ordering::SeqCst);
        {
            let mut live = LIVE.lock().expect("live runtime registry poisoned");
            *live = None;
            // 早一点唤醒：正在等旧运行时消失的 `connect` 会拿到「正在关闭」的错误，
            // 而不是继续等一个可能正在等它自己的排空。
            LIVE_CHANGED.notify_all();
        }
        // 关闭之前先撤掉活着的代次：从这一刻起这个运行时的引用全部过期。
        // 代次守卫只负责提前跳过；引用计数调用与 C++ 侧关闭之间的互斥在 C 包装层
        // （`crates/rayrust-sys/wrapper/ray_c.cc` 的引用计数闸门）：关闭开始时闸门
        // 拒绝新的引用计数调用，并等已经进入的那些退出（微秒级），所以关闭一定能
        // 推进。闸门只覆盖引用计数两个函数，`get`/`put`/`wait` 这些阻塞调用不在
        // 其中（阻塞调用不能让关闭等）。
        crate::object_ref::retract_live_epoch();
        // 登记表跟着运行时清空：旧运行时的 handle 在新运行时里查不到，几个调用入口
        // 会因此在进 SDK 之前把它们拒掉。
        actor_registry::clear();
        // 本地模式下 `ray_shutdown` 会在关闭引用计数闸门与 `ray::Shutdown()` 之前
        // 等待本次运行时提交的所有任务就绪。排空可能在等一个任务，而本地模式的任务体
        // 在驱动进程里执行、任务体里可能调用 `Ray::local()`，所以这里不能持有 `LIVE`。
        shutdown();
        // 关闭完成之后重新拿锁：只清掉已经失效的登记，别把之后建出来的运行时抹掉。
        let mut live = LIVE.lock().expect("live runtime registry poisoned");
        if let Some(handle) = live.as_ref() {
            if handle.upgrade().is_none() {
                *live = None;
            }
        }
        SHUTTING_DOWN.store(false, Ordering::SeqCst);
        LIVE_CHANGED.notify_all();
    }
}

/// 进程里活着的运行时。`Weak` 让登记表不延长运行时的存活期。
static LIVE: Mutex<Option<Weak<RayInner>>> = Mutex::new(None);

/// 登记表清空时唤醒正在等它的 `connect`。
static LIVE_CHANGED: Condvar = Condvar::new();

/// 最后一个句柄正在关闭。`Drop` 在碰 `LIVE` 之前置位，在 `shutdown()` 返回并重新
/// 拿到 `LIVE` 之后复位。`connect` 看到它就直接返回错误：关闭期间的排空可能在等
/// 一个任务，而任务体里可能调用 `Ray::local()`，让任务体去等 `LIVE` 会让两边互等。
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

fn shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::SeqCst)
}

/// 关闭期间请求运行时统一用的错误。
fn shutting_down_error() -> RayError {
    RayError::Runtime(
        "the last Ray handle is being dropped; a new runtime cannot be initialized until the \
         shutdown finishes"
            .to_string(),
    )
}

impl Ray {
    /// Connect to a Ray cluster with the given config.
    ///
    /// A second call with the same config returns a handle to the runtime that
    /// is already live. A config that differs returns [`RayError::Runtime`]
    /// listing the fields that differ. While the last handle is being dropped,
    /// every call returns [`RayError::Runtime`] without waiting for the shutdown
    /// to finish, including a call made from inside a local-mode task.
    ///
    /// # Address validation
    ///
    /// Cluster mode requires [`RayConfig::address`] to be `host:port` when it is
    /// non-empty, and `RAY_ADDRESS` to be `host:port` when the address is empty
    /// and the variable is set (an empty value counts as set, and is refused the
    /// same way). Either one that does not match returns [`RayError::Ffi`] naming
    /// which one, before Ray is initialized. An empty address with no
    /// `RAY_ADDRESS` is accepted: it means no bootstrap address is configured.
    ///
    /// A well-formed address that leads nowhere stays outside what this crate
    /// rejects, and it terminates the process. With `node_ip` unset and a host
    /// that does not resolve, it stops in `GetNodeIpAddress`
    /// (cpp/src/ray/util/util.cc:42, `RAY_LOG(FATAL)`); with `node_ip` set the
    /// resolution is skipped and the address joins a GCS that does not answer, so
    /// the driver stops about 35 seconds later at
    /// cpp/src/ray/util/process_helper.cc:110 (`RAY_CHECK_OK` on its
    /// `GetNodeToConnectForDriver`).
    pub fn connect(config: &RayConfig) -> Result<Self, RayError> {
        // 取锁之前先看一眼：关闭线程在排空期间不持有 LIVE（见 `Drop for RayInner`），
        // 这一眼让任务体在绝大多数情况下连锁都不用碰。
        if shutting_down() {
            return Err(shutting_down_error());
        }
        let mut live = LIVE.lock().expect("live runtime registry poisoned");
        // 拿到锁之后再确认一次：`Drop` 先置位再清空登记，所以看到「没有登记」时这一眼
        // 必然为真。
        if shutting_down() {
            return Err(shutting_down_error());
        }
        let existing = loop {
            match live.as_ref() {
                // 没有登记运行时，下面初始化。
                None => {
                    // 等到唤醒之后关闭可能已经开始（`Drop` 先置位、再清登记、再唤醒）。
                    if shutting_down() {
                        return Err(shutting_down_error());
                    }
                    break None;
                }
                Some(handle) => match handle.upgrade() {
                    Some(inner) => break Some(inner),
                    // 强引用计数归零时就升级不到，而那时 `Drop for RayInner`
                    // 可能还没拿到锁：等它把 C++ 侧关掉、清空登记表再初始化，
                    // 否则新运行时会随后被那个迟到的 `shutdown()` 关掉。
                    None => {
                        live = LIVE_CHANGED
                            .wait(live)
                            .expect("live runtime registry poisoned")
                    }
                },
            }
        };
        if let Some(inner) = existing {
            let live_config = inner.config.clone();
            // 先放锁：这里的 `Arc` 可能是最后一个，在锁内释放会让
            // `Drop for RayInner` 再来取同一把锁。
            drop(live);
            if live_config == *config {
                return Ok(Ray { inner });
            }
            return Err(RayError::Runtime(config_mismatch_message(
                &live_config,
                config,
            )));
        }
        init_with_config(config)?;
        let inner = Arc::new(RayInner {
            config: config.clone(),
        });
        *live = Some(Arc::downgrade(&inner));
        Ok(Ray { inner })
    }

    /// Connect in local mode (no cluster, single process).
    pub fn local() -> Result<Self, RayError> {
        Self::connect(&RayConfig::local())
    }

    // ── Object Store ──────────────────────────────────────────

    /// Put an object into the object store.
    pub fn put<T: serde::Serialize>(&self, value: &T) -> Result<ObjectRef<T>, RayError> {
        put(value)
    }

    /// Put a value with a 9-byte XLANG header in front of the data (the msgpack
    /// layout Python uses). The object itself carries no metadata: Put through
    /// the C++ SDK stores `nullptr` metadata, so a Python task or actor rejects
    /// this object when it arrives as a reference argument
    /// (`_private/serialization.py` 的 `_deserialize_object` 拒收无 metadata 的
    /// 非空对象，见 #21)。
    pub fn put_xlang<T: serde::Serialize>(&self, value: &T) -> Result<ObjectRef<T>, RayError> {
        put_xlang(value)
    }

    /// Asynchronously put an object into the object store.
    pub async fn put_async<T>(&self, value: T) -> Result<ObjectRef<T>, RayError>
    where
        T: serde::Serialize + Send + 'static,
    {
        put_async(value).await
    }

    /// Get an object from the object store (blocks until ready).
    pub fn get<T: serde::de::DeserializeOwned>(&self, obj_ref: &ObjectRef<T>) -> Result<T, RayError> {
        obj_ref.get()
    }

    /// Asynchronously get an object from the object store.
    pub async fn get_async<T>(&self, obj_ref: &ObjectRef<T>) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        obj_ref.get_async().await
    }

    /// Wait for objects to be locally available.
    ///
    /// `timeout_ms` 是 `-1`（永久等待）或非负的毫秒数，其他值返回
    /// `RayError::Ffi`。每个 id 必须正好 28 字节，其他长度（包括 0 字节）同样返回
    /// `RayError::Ffi`，进程存活：这种 id 进入 `ObjectID::FromBinary` 会触发
    /// RAY_CHECK 终止进程，而 0 字节会变成永不就绪的 nil id。
    ///
    /// `num_objects` 必须在 `1..=object_refs.len()`，且 `object_refs` 里的 id 必须
    /// 两两不同。集群模式的 `CoreWorker::Wait` 不看对象是否就绪，两类参数一律拒绝
    /// （`src/ray/core_worker/core_worker.cc:1475-1483`）；`ray_wait` 在碰 runtime 之前按同样
    /// 的规则拒掉，返回 `RayError::Ffi`，两种模式给出同一句话。本地模式没有 core worker，
    /// 它的 `Wait` 把 id 收进集合去重却把原数量传下去
    /// （`cpp/src/ray/runtime/object/local_mode_object_store.cc:82-97`）：数量大于去重后的
    /// 集合大小、且还有 id 没进本地存储时，构造出的 `GetRequest` 撞上
    /// `src/ray/core_worker/store_provider/memory_store/memory_store.cc:89` 的 RAY_CHECK，
    /// 整个进程终止；全部 id 已经在本地存储里时它在构造 `GetRequest` 之前就返回
    /// `Status::OK`，同一个输入得到的是 `Ok`。
    /// 返回的 `ready` 最多 `num_objects` 个：两个引用都
    /// 已就绪而 `num_objects` 为 1 时，另一个在 `unready` 里。
    pub fn wait<T>(
        &self,
        object_refs: &[ObjectRef<T>],
        num_objects: usize,
        timeout_ms: i32,
    ) -> WaitResult<T> {
        wait(object_refs, num_objects, timeout_ms)
    }

    /// 异步等待一组引用在本地就绪。
    ///
    /// 一次 `ray_wait` 跑在 tokio blocking pool 上，等待的 tokio 工作线程不被占用。
    /// `timeout` 限制整次等待。每个 id 必须正好 28 字节，其他长度（包括 0 字节）
    /// 返回 `RayError::Ffi`。`num_objects` 必须在 `1..=object_refs.len()`，id 必须
    /// 两两不同，违反时同样返回 `RayError::Ffi`；这两种拒绝由同一次 `ray_wait` 做出，
    /// 与 [`Ray::wait`] 相同，两种模式一致。
    pub async fn wait_async<T>(
        &self,
        object_refs: &[ObjectRef<T>],
        num_objects: usize,
        timeout: std::time::Duration,
    ) -> WaitResult<T>
    where
        T: Send + 'static,
    {
        let refs: Vec<ObjectRef<T>> = object_refs.to_vec();
        let timeout_ms = duration_to_ms(timeout);
        tokio::task::spawn_blocking(move || wait(&refs, num_objects, timeout_ms))
            .await
            .map_err(|e| RayError::Runtime(format!("wait join error: {}", e)))?
    }

    /// Get multiple objects from the object store (blocks until all ready).
    ///
    /// 每个引用必须正好 28 字节，其他长度（包括 0 字节）返回 `RayError::Ffi`，
    /// 进程存活。
    ///
    /// If any element's cross-language payload carries a pickle5 section, the
    /// whole batch returns `RayError::Serialization`: the plain values already
    /// read from the same batch are dropped too, because
    /// `get_many_with_timeout` returns on the first error.
    pub fn get_many<T: serde::de::DeserializeOwned>(
        &self,
        refs: &[ObjectRef<T>],
    ) -> Result<Vec<T>, RayError> {
        get_many(refs)
    }

    /// 批量取，超过 `timeout_ms` 毫秒就返回。
    ///
    /// `timeout_ms` 是 `-1`（永久等待）或非负的毫秒数，其他值返回
    /// `RayError::Ffi`。每个引用必须正好 28 字节，其他长度（包括 0 字节）同样返回
    /// `RayError::Ffi`，进程存活。批里任一元素的载荷带 pickle5 段时整批返回
    /// `RayError::Serialization`，同一批里已经取到的普通值也拿不到
    /// （`get_many_with_timeout` 用 `?` 直接返回）。
    pub fn get_many_timeout<T: serde::de::DeserializeOwned>(
        &self,
        refs: &[ObjectRef<T>],
        timeout_ms: i32,
    ) -> Result<Vec<T>, RayError> {
        get_many_with_timeout(refs, timeout_ms)
    }

    /// 异步批量取。
    ///
    /// 默认期限 300 秒，自定义期限用 `get_many_timeout_async`。整批是 blocking
    /// pool 上的一次 `Get`。批里任一元素的载荷带 pickle5 段时整批返回
    /// `RayError::Serialization`，同一批里已经取到的普通值也拿不到。
    pub async fn get_many_async<T>(&self, refs: &[ObjectRef<T>]) -> Result<Vec<T>, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.get_many_timeout_async(refs, std::time::Duration::from_secs(300))
            .await
    }

    /// 异步批量取，带自定义期限。
    ///
    /// Ray 在一次 `Get` 内部限制整批的等待，期限覆盖每一个元素。与
    /// `get_timeout_async` 一样，期限从 blocking pool 的线程接到这次调用时开始计时。
    /// 每个引用必须正好 28 字节，其他长度（包括 0 字节）返回 `RayError::Ffi`。
    /// 批里任一元素的载荷带 pickle5 段时整批返回 `RayError::Serialization`，
    /// 同一批里已经取到的普通值也拿不到。
    pub async fn get_many_timeout_async<T>(
        &self,
        refs: &[ObjectRef<T>],
        timeout: std::time::Duration,
    ) -> Result<Vec<T>, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let refs: Vec<ObjectRef<T>> = refs.to_vec();
        let timeout_ms = duration_to_ms(timeout);
        tokio::task::spawn_blocking(move || get_many_with_timeout(&refs, timeout_ms))
            .await
            .map_err(|e| RayError::Runtime(format!("get_many join error: {}", e)))?
    }

    // ── Tasks ─────────────────────────────────────────────────

    /// Call a remote task by function name.
    ///
    /// `is_ref[i] = true` means `args[i]` is a binary ObjectRef ID (pass by
    /// reference). The id must be a reference this process holds and whose owner
    /// Ray knows (a handle returned by `put` or by a task/actor call); a 28-byte
    /// id of unknown origin makes Ray's `GetOwnerAddressOrDie` terminate the
    /// process, and local mode waits for it forever in `build_task_args_cpp`'s
    /// undeadlined read. Use
    /// [`TaskArg`](crate::TaskArg) with the macro-generated callers
    /// to get that check for free. A reference argument's id must also be exactly
    /// 28 bytes: any other length returns [`RayError::Ffi`] before submission,
    /// with the process still alive. Pass `&TaskOptions::new()` for no options;
    /// [`TaskOptions`] carries the task name, resource requirements, placement
    /// group binding and runtime env. No options means one CPU, Ray's default
    /// for a task, so in cluster mode `--num-cpus` limits how many tasks run at
    /// once (local mode drops resource requirements).
    pub fn task_call(
        &self,
        func_name: &str,
        args: &[&[u8]],
        is_ref: &[bool],
        opts: &TaskOptions,
    ) -> Result<ObjectRef<()>, RayError> {
        task_call_inner(func_name, args, is_ref, opts)
    }

    /// Asynchronously call a remote task.
    ///
    /// Returns a `'static` future — does not borrow `&self`.
    /// This allows the future to be spawned on `JoinSet` or similar.
    /// The future does not keep the runtime alive either: the epoch is taken
    /// when the future is created, and the blocking-pool closure checks it
    /// again right before the call to C++, so a future polled after the last
    /// `Ray` handle was dropped fails with `RayError::Runtime`.
    /// Pass `&TaskOptions::new()` for no options (see [`TaskOptions`]). No
    /// options means one CPU, Ray's default for a task (cluster mode; local mode
    /// drops resource requirements).
    ///
    /// `is_ref[i] = true` means `args[i]` is a binary ObjectRef ID (pass by
    /// reference). Such an id must be a reference this process currently holds
    /// and whose owner Ray knows (a handle returned by `put` or by a task/actor
    /// call). A 28-byte id of unknown origin makes Ray's `TransformArgs` →
    /// `GetOwnerAddressOrDie` terminate the driver process, and local mode waits
    /// for it forever. Its length must be exactly 28 bytes; any other length
    /// returns [`RayError::Ffi`] before submission, with the process still alive.
    pub fn task_call_async(
        &self,
        func_name: &str,
        args: Vec<Vec<u8>>,
        is_ref: Vec<bool>,
        opts: &TaskOptions,
    ) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
        submit_task_async(func_name, TaskArgs::from_raw(args, is_ref), opts)
    }

    /// [`Ray::task_call_async`]，参数由 [`TaskArgs`] 收好。
    ///
    /// 参数准备过程中的错误（值参数序列化失败、引用参数的 id 长度不对）在这里
    /// 直接返回，任务不会被提交。宏生成的异步调用用它。
    #[doc(hidden)]
    pub fn task_call_async_with_args(
        &self,
        func_name: &str,
        args: TaskArgs,
        opts: &TaskOptions,
    ) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
        submit_task_async(func_name, args, opts)
    }

    /// Call a Python remote function.
    ///
    /// This entry point takes no [`TaskOptions`]: a Python task submitted here
    /// cannot be named, cannot request resources, cannot be bound to a placement
    /// group bundle and cannot be given a runtime env.
    /// The task requests one CPU, Ray's default for a task; with no options there
    /// is no way to opt out of it. Local mode drops the requirement.
    ///
    /// `module` must be non-empty. An empty one — and a name with an interior NUL
    /// byte, which reaches the C entry as an empty C string — returns
    /// [`RayError::Ffi`] before any task is submitted. That check comes before the
    /// runtime lookup and before the local mode refusal below, so an empty module
    /// name reports the name error in both modes and without a runtime.
    /// An empty module name can never run. The worker imports the module by name
    /// (`python/ray/_private/function_manager.py:138`, `importlib.import_module`)
    /// and importlib raises `ValueError: Empty module name` for an empty string —
    /// it raises before any lookup, so a module planted in `sys.modules` under the
    /// empty name does not change that — and the worker exits with `SYSTEM_ERROR`.
    /// The first read answers only an unknown FFI error and the worker pool starts
    /// a replacement worker.
    /// `function` is not checked. The loader splits it on dots and throws the empty
    /// pieces away (`function_manager.py:139`), so an empty name resolves to the
    /// module object itself, which the loader registers as the task's function
    /// (`:391-395`). An ordinary module is not callable, so the task then fails
    /// with `TypeError: 'module' object is not callable`: an ordinary task failure,
    /// where the worker stays alive and the first read answers only an unknown FFI
    /// error. A module object can be callable, though — a `types.ModuleType`
    /// subclass with `__call__` assigned to the module's `__class__`, or any
    /// callable placed in `sys.modules` under that name — and then the task runs
    /// and returns that object's result (measured on 2.51.1: such a module
    /// returned 3 for a call with an empty function name). Refusing the name would
    /// reject that call, so the entry submits it. An interior NUL byte makes it an
    /// empty function name as well, and the submission goes through the same way.
    /// A non-empty name nobody registered keeps Ray's asynchronous semantics: the
    /// driver holds no function table, so the submission returns an `ObjectRef` and
    /// the first read waits until it times out. A whitespace-only name such as
    /// `" "` is not trimmed and is submitted as it is.
    ///
    /// `is_ref[i] = true` means `args[i]` is a binary ObjectRef ID (pass by
    /// reference). Three requirements apply to such an id:
    /// - it must be exactly 28 bytes; any other length returns [`RayError::Ffi`]
    ///   before submission, with the process still alive.
    /// - it must be a reference this process currently holds and whose owner Ray
    ///   knows. In cluster mode a 28-byte id of unknown origin makes Ray's
    ///   `TransformArgs` → `GetOwnerAddressOrDie` terminate the driver process;
    ///   local mode sends it without an owner lookup
    ///   (`cpp/src/ray/runtime/abstract_ray_runtime.cc:152-155` only asks a core
    ///   worker in cluster mode).
    /// - it must come from a Python worker: objects put through the C++ SDK carry
    ///   no metadata and Python rejects them when they arrive as an argument (see
    ///   [`Ray::put_xlang`], #21).
    ///
    /// Local mode returns [`RayError::Ffi`] instead of submitting when `function`
    /// is also the name a `#[rayrust::remote]` function registered in this
    /// process: `LocalModeTaskSubmitter::Submit` drops the module and the
    /// language and resolves the function name in the C++ remote function map
    /// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40`), so that
    /// Rust function would run in place of the Python one. A local submission
    /// under any other name — including a method of a `#[rayrust::actor]` type,
    /// which registers in the member function map — is submitted as a C++ task
    /// and fails at the C++ function lookup.
    pub fn task_call_python(
        &self,
        module: &str,
        function: &str,
        args: &[&[u8]],
        is_ref: &[bool],
    ) -> Result<ObjectRef<()>, RayError> {
        let id = task_call_python_inner(module, function, args, is_ref)?;
        Ok(ObjectRef::from_call_xlang(id))
    }

    // ── Actors ────────────────────────────────────────────────

    /// Create an actor with full creation options.
    ///
    /// Pass `&ActorOptions::new()` for defaults (no name, no restarts, etc.).
    ///
    /// `func_name` is the name a `#[rayrust::actor]` type registered: the
    /// constructor as `__rayrust_actor_factory_<type>`, a method as
    /// `__rayrust_actor_factory_<type>::<method>`. Both are non-empty, and an empty
    /// name returns [`RayError::Ffi`] before any task is submitted, because it can
    /// never run. Local mode constructs only a driver
    /// (`cpp/src/ray/runtime/local_mode_ray_runtime.cc:30-34`), runs the creation task
    /// in the calling thread before `CreateActor` returns
    /// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:115-121`, `:143-151`)
    /// and throws `Task function name is empty` before any lookup
    /// (`cpp/src/ray/runtime/task/task_executor.cc:32-34`); the error text goes into
    /// the creation task's return value and no actor context is registered (same
    /// file, `:336-339`, so the branch at `:327-331` is skipped), and the first call
    /// throws a bare `_Map_base::at` from `actor_contexts_.at`
    /// (`local_mode_task_submitter.cc:110-112`). Cluster mode returns as soon as the
    /// creation task is submitted, and the worker fails it with `RayFunctionNotFound`
    /// (`cpp/src/ray/util/function_helper.cc:163-168`, caught at
    /// `task_executor.cc:101-111`): the actor worker stops at
    /// `src/ray/core_worker/core_worker_shutdown_executor.cc:40` and the driver reads
    /// only `Actor error ... unknown FFI error`. A name with an interior NUL byte
    /// reaches the C entry as an empty C string, the same as in [`Ray::get_actor`],
    /// and is refused with it.
    ///
    /// A non-empty name that nobody registered keeps Ray's asynchronous semantics:
    /// the driver has no way to learn which names are registered, so the creation
    /// returns `Ok` and the failure surfaces on the first call or when its value is
    /// read. A whitespace-only name is not trimmed and is submitted as it is.
    pub fn actor_create(
        &self,
        func_name: &str,
        args: &[&[u8]],
        opts: &ActorOptions,
    ) -> Result<ActorHandle, RayError> {
        actor_create_with_options_inner(func_name, args, opts)
    }

    /// Create a Python actor with full creation options.
    ///
    /// Pass `&ActorOptions::new()` for defaults (no name, no restarts, etc.).
    ///
    /// `module` and `class` must be non-empty, and either one empty returns
    /// [`RayError::Ffi`] before any task is submitted. A Python actor is named by
    /// both and the worker resolves them in Python, so an empty name hands out an
    /// actor that never runs: the driver only submits the creation task and the
    /// caller gets `Ok` with a 16-byte id whose first read answers only
    /// `Actor error ... unknown FFI error`. An empty module name fails in
    /// `python/ray/_private/function_manager.py:531` (`load_actor_class`) → `:589`
    /// (`_load_actor_class_from_local`) → `:138`
    /// (`load_function_or_class_from_local`), where
    /// `importlib.import_module(module_name)` raises `ValueError: Empty module name`;
    /// an empty class name fails at `python/ray/_raylet.pyx:2253`, which raises
    /// `TypeError: module.__new__(X): X is not a type object (module)` before
    /// `load_actor_class` is reached (the measured traceback holds only
    /// `_raylet.pyx:2432` and `:2253`). In both cases the worker exits with
    /// `SYSTEM_ERROR`. A name with an interior NUL byte reaches the C entry as an
    /// empty C string, the same as in [`Ray::get_actor`], and is refused with it
    /// (#27).
    ///
    /// The two name checks run before the local mode refusal below, so an empty
    /// name reports the name error in local mode too.
    ///
    /// Cluster mode creates the actor as usual.
    ///
    /// Local mode returns [`RayError::Ffi`] when both names are non-empty. That
    /// refusal runs before the options and the arguments are looked at, so in local
    /// mode this entry rejects every call with non-empty names whatever the options
    /// and the arguments are. The reason is the local submitter:
    /// `LocalModeTaskSubmitter::Submit` ignores the language and the module and
    /// builds every task as a C++ descriptor from the function name alone
    /// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40`), and the
    /// creation function name this entry sends is `"__init__"`. Without the refusal
    /// such a task reaches `TaskExecutor`, which finds no C++ function of that name
    /// and stores `PackError("unknown function: __init__")` as the actor state
    /// (`task_executor.cc:44-52`, `:313-331`) without raising anything: the caller
    /// would get `Ok` with a 16-byte id for an actor whose constructor never ran and
    /// whose Python calls are all refused (see [`Ray::actor_call_python`]), plus a
    /// name, when one is given, that `LocalModeTaskSubmitter::CreateActor` registers
    /// (`:143-152`) for [`Ray::get_actor`] to hand back (`:159-168`) (#68). With the
    /// refusal nothing is submitted and nothing is registered, so after a refused
    /// call [`Ray::get_actor`] still answers whatever the name held before it, which
    /// is `Ok(None)` when the name was free.
    ///
    /// The refusal covers one more case: a process that registers
    /// `#[rayrust::remote] fn __init__`. Without it, that Rust function would run in
    /// place of the Python constructor and its serialized return value would become
    /// the actor state.
    ///
    /// [`ActorOptions`] combinations that [`RayError::Ffi`] rejects before the FFI
    /// call keep their message: an isolated `bundle_index` reports
    /// `bundle_index N requires placement_group_id` in local mode too. The option
    /// diagnostics produced on the C side (a placement group in local mode, a group
    /// id of the wrong length, a `bundle_index` below -1) do not appear on this entry
    /// in local mode, because the refusal comes first; [`Ray::actor_create`] keeps
    /// all of them.
    ///
    /// `args` hold msgpack-serialized values. There is no `is_ref` here: constructor
    /// arguments cannot be passed by reference.
    ///
    /// `module` and `class` are put into the holder in cluster mode only, because
    /// local mode refuses a non-empty pair before the holder is built; the name
    /// checks above read them in both modes.
    pub fn actor_create_python(
        &self,
        module: &str,
        class: &str,
        args: &[&[u8]],
        opts: &ActorOptions,
    ) -> Result<ActorHandle, RayError> {
        actor_create_python_with_options_inner(module, class, args, opts)
    }

    /// Call a method on an actor (sync).
    ///
    /// `actor_id` must be exactly 16 bytes (the Ray 2.51.1 ActorID length); any
    /// other length returns [`RayError::Ffi`] before submission, with the process
    /// still alive.
    ///
    /// Beyond its length, the id must be a handle this process registered:
    /// [`Ray::actor_create`], [`Ray::actor_create_python`], [`Ray::get_actor`] or
    /// [`ActorHandle::from_serialized`]. Any other 16 bytes — garbage, a handle
    /// left over from a runtime that was shut down, the nil id of 16 `0xff`
    /// bytes — return [`RayError::Ffi`] naming the id. The reason is the
    /// core worker: `ActorManager::GetActorHandle` RAY_CHECKs an id it holds no
    /// handle for (`src/ray/core_worker/actor_manager.cc:52-59`), and a RAY_CHECK
    /// ends the process, which `RAY_CATCH` cannot catch.
    ///
    /// An actor created through the raw C ABI
    /// (`rayrust::sys::ray_actor_create_with_options`) does not enter that
    /// registry, so the typed call entries refuse its id as unregistered. In
    /// cluster mode, take the bytes from `rayrust::sys::ray_actor_handle_serialize`
    /// and hand them to [`ActorHandle::from_serialized`] to register it here;
    /// local mode has no such path back, so such an actor can only be called
    /// through the C ABI.
    ///
    /// The call this entry submits is a C++ one, so in cluster mode the handle
    /// must be a C++ actor. A Python actor returns [`RayError::Ffi`] naming its
    /// language and pointing at [`Ray::actor_call_python`]; a Java actor returns
    /// one saying this crate can neither create nor call Java actors. Local mode
    /// requires a registered handle but skips the language decision: no Python
    /// actor can be created there ([`Ray::actor_create_python`] refuses local
    /// mode) and the local submitter runs the registered Rust functions
    /// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:39-40`).
    ///
    /// `is_ref[i] = true` means `args[i]` is a binary ObjectRef ID
    /// (pass by reference); such an id must be exactly 28 bytes, and it must be a
    /// reference this process holds and whose owner Ray knows; see
    /// [`Ray::task_call`].
    pub fn actor_call(
        &self,
        actor_id: &[u8],
        func_name: &str,
        args: &[&[u8]],
        is_ref: &[bool],
    ) -> Result<ObjectRef<()>, RayError> {
        actor_call_inner(actor_id, func_name, args, is_ref)
    }

    /// Asynchronously call a method on an actor.
    ///
    /// Returns a `'static` future — does not borrow `&self`.
    /// This allows the future to be spawned on `JoinSet` or similar.
    /// The future does not keep the runtime alive either: the epoch is taken
    /// when the future is created, and the blocking-pool closure checks it
    /// again right before the call to C++, so a future polled after the last
    /// `Ray` handle was dropped fails with `RayError::Runtime`.
    ///
    /// `is_ref[i] = true` means `args[i]` is a binary ObjectRef ID (pass by
    /// reference). Such an id must be exactly 28 bytes, and it must be a
    /// reference this process currently holds and whose owner Ray knows (a handle
    /// returned by `put` or by a task/actor call). A 28-byte id of unknown origin
    /// makes Ray's `TransformArgs` → `GetOwnerAddressOrDie` terminate the driver
    /// process, and local mode waits for it forever.
    ///
    /// `actor_id` must be exactly 16 bytes; any other length returns
    /// [`RayError::Ffi`] before submission, with the process still alive.
    ///
    /// The handle and language requirements are the ones [`Ray::actor_call`]
    /// documents: a handle this process registered ([`Ray::actor_create`],
    /// [`Ray::actor_create_python`], [`Ray::get_actor`] or
    /// [`ActorHandle::from_serialized`]), and in cluster mode a C++ actor. A
    /// handle from a runtime that is gone is refused before the call reaches the
    /// core worker, which would end the process on it.
    pub fn actor_call_async(
        &self,
        actor_id: &[u8],
        func_name: &str,
        args: Vec<Vec<u8>>,
        is_ref: Vec<bool>,
    ) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
        submit_actor_call_async(actor_id, func_name, TaskArgs::from_raw(args, is_ref))
    }

    /// [`Ray::actor_call_async`]，参数由 [`TaskArgs`] 收好。
    ///
    /// 参数准备过程中的错误在这里直接返回，方法不会被调用。宏生成的异步调用用它。
    #[doc(hidden)]
    pub fn actor_call_async_with_args(
        &self,
        actor_id: &[u8],
        func_name: &str,
        args: TaskArgs,
    ) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
        submit_actor_call_async(actor_id, func_name, args)
    }

    /// Call a method on a Python actor.
    ///
    /// `actor_id` must be exactly 16 bytes; any other length returns
    /// [`RayError::Ffi`] before the call reaches the SDK, with the process still
    /// alive. That width check runs before the local mode rejection below, so
    /// local mode with a short actor id reports the width error.
    ///
    /// In cluster mode the id must also be a handle this process registered
    /// ([`Ray::actor_create`], [`Ray::actor_create_python`], [`Ray::get_actor`] or
    /// [`ActorHandle::from_serialized`]) whose language is Python. An unregistered
    /// id returns [`RayError::Ffi`] naming it, because the SDK reads the actor's
    /// module and class from the core worker's actor handle, and that lookup
    /// RAY_CHECKs an id it holds no handle for
    /// (`src/ray/core_worker/actor_manager.cc:52-59`). A C++ actor's id is refused
    /// by the language check too: the SDK would read its creation descriptor as a
    /// `PythonFunctionDescriptor`
    /// (`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`), and the C++ worker
    /// it lands on ends at
    /// `RAY_CHECK(ray_function.GetLanguage() == ray::Language::CPP)`
    /// (`cpp/src/ray/runtime/task/task_executor.cc:145`). A Java actor's id is
    /// refused as well: this crate can neither create nor call Java actors.
    ///
    /// A Python actor's id is accepted only when its handle carries a Python
    /// creation descriptor (field 6 of the serialized `rpc.ActorHandle`,
    /// `src/ray/protobuf/common.proto:158-164`). An id this process registered
    /// through [`ActorHandle::from_serialized`] with a handle whose descriptor is
    /// absent or of another type returns [`RayError::Ffi`] before the SDK runs,
    /// because that path reads the descriptor as a `PythonFunctionDescriptor`
    /// whatever the handle holds.
    ///
    /// `method_name` must be a method the Python actor really has; an empty one is
    /// refused before submission and a non-empty unknown one still ends the actor's
    /// worker.
    ///
    /// An empty `method_name` returns [`RayError::Ffi`] with a message starting
    /// `the method name is empty`, before the runtime lookup and before the local
    /// mode rejection below, so local mode with an empty method name reports that
    /// error while local mode with a non-empty one still reports the local mode
    /// error. An empty name is not always fatal: a Python instance can carry an
    /// attribute named with the empty string (`setattr(Class, "", method)`), and a
    /// cross-language call with that name reaches it (measured on Ray 2.51.1: the
    /// call read 42 and the actor kept answering). This process cannot tell such an
    /// actor apart from one without that attribute, and there the call ends the
    /// actor's worker: it exits with `SYSTEM_ERROR` (`Worker exits unexpectedly`) and
    /// a traceback holding `python/ray/_raylet.pyx:2432` (`task_execution_handler`),
    /// `:2291` (which calls `execute_task`) and `:1873`, where `execute_task` runs
    /// `function_name = execution_info.function_name` and raises
    /// `AttributeError: 'dict' object has no attribute 'function_name'`; the first
    /// read answers only `Actor error ... unknown FFI error` and that actor cannot be
    /// called again. The empty name is refused in both cases, including the one where
    /// the method exists. A method name with an interior NUL byte is refused by the
    /// same check, because `to_cstring` (`crates/rayrust-sys/src/lib.rs:253-255`,
    /// #27) turns it into an empty C string.
    ///
    /// A non-empty method name the actor does not have cannot be told apart from a
    /// valid one here: this process holds no method table for the actor. Measured on
    /// 2.51.1, such a name ends the actor's worker with the same traceback as an
    /// empty one, so a caller must pass a method name that exists.
    ///
    /// `is_ref[i] = true` means `args[i]` is a binary ObjectRef ID (pass by
    /// reference). Three requirements apply to such an id:
    /// - it must be exactly 28 bytes; any other length returns [`RayError::Ffi`]
    ///   before submission, with the process still alive.
    /// - it must be a reference this process currently holds and whose owner Ray
    ///   knows. A 28-byte id of unknown origin makes Ray's `TransformArgs` →
    ///   `GetOwnerAddressOrDie` terminate the driver process.
    /// - it must come from a Python worker: objects put through the C++ SDK carry
    ///   no metadata and Python rejects them when they arrive as an argument (see
    ///   [`Ray::put_xlang`], #21).
    ///
    /// Local mode returns [`RayError::Ffi`] before any of the reference
    /// requirements are looked at:
    /// the SDK reads the actor's module and class from the core worker's actor
    /// handle before it builds the call
    /// (`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`), and local mode
    /// never starts a core worker process. This entry receives only the actor id
    /// and the method name, so there is nothing to build that descriptor from.
    pub fn actor_call_python(
        &self,
        actor_id: &[u8],
        method_name: &str,
        args: &[&[u8]],
        is_ref: &[bool],
    ) -> Result<ObjectRef<()>, RayError> {
        let id = actor_call_python_inner(actor_id, method_name, args, is_ref)?;
        Ok(ObjectRef::from_call_xlang(id))
    }

    /// Kill an actor by its handle.
    ///
    /// Returns `Ok(())` on success, `Err` if the C++ side reports an error.
    ///
    /// The actor id width needs no check here: `&ActorHandle` always carries a
    /// 16-byte id. The width check sits at the C entry, which a C caller can reach
    /// with a shorter id.
    ///
    /// A handle this process did not register — one left over from a runtime that
    /// was shut down — returns [`RayError::Ffi`] naming the id, in both modes. The
    /// core worker's `AbstractRayRuntime::KillActor` reads the id as an `ActorID`
    /// and asks the core worker for it; a cluster core worker that holds no such
    /// handle cannot report the failure without ending the process.
    ///
    /// Local mode returns [`RayError::Ffi`]: `AbstractRayRuntime::KillActor`
    /// starts with `CoreWorkerProcess::GetCoreWorker()`, whose
    /// `EnsureInitialized` calls `QuickExit()` (`_Exit(1)`,
    /// `src/ray/util/process.cc:845-848`) because local mode never starts a core
    /// worker process. A local actor lives in the calling process and ends with
    /// it, so there is nothing to kill.
    pub fn kill_actor(&self, handle: &ActorHandle, no_restart: bool) -> Result<(), RayError> {
        check_registered_actor("ray_actor_kill", handle.id())?;
        rayrust_sys::clear_error();
        actor_kill_inner(handle.id(), no_restart);
        // The C ABI returns void and reports both the local mode rejection and
        // exceptions through last_error, so this check is the designed path.
        if rayrust_sys::last_error().is_some() {
            return Err(ffi_error("ray_actor_kill"));
        }
        Ok(())
    }

    // ── Placement Groups ──────────────────────────────────────

    /// Create a placement group.
    /// `bundles_json` is a JSON array like `[{"CPU":1},{"CPU":1}]`.
    /// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
    pub fn placement_group_create(
        &self,
        name: &str,
        bundles_json: &str,
        strategy: i32,
    ) -> Result<Vec<u8>, RayError> {
        placement_group_create_inner(name, bundles_json, strategy)
    }

    /// 移除一个 placement group。
    ///
    /// `group_id` 是 [`Ray::placement_group_create`] 返回的二进制 id，必须正好 18
    /// 字节；其他长度（包括空字节）返回 [`RayError::Ffi`]，进程存活。这个宽度在 crate
    /// 内侧与 C 入口各查一次：`PlacementGroupID::FromBinary` 对非空且长度不符的输入
    /// `RAY_CHECK` 终止进程（`src/ray/common/id.h:435`），`RAY_CATCH` 抓不到。
    ///
    /// 移除一个不存在或已经移除的组返回 `Ok(())`，两个模式实测都如此：上游按「已经
    /// 移除」处理，什么也不做。集群模式的判断在 GCS（表里没有这个 id 时直接回
    /// `Status::OK()`，`src/ray/gcs/gcs_placement_group_manager.cc:404-408`），本地模式是
    /// 自己 map 的 `erase`（`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:182-184`，
    /// 未知 id 不命中任何表项）。
    ///
    /// 本地模式不拒绝这个调用：`CreatePlacementGroup` 把组插进自己的 map
    /// （`local_mode_task_submitter.cc:170-180`，`PlacementGroupID::Of(job_id)`，18 字节），
    /// 移除就是从这个 map 里删掉那一项。这个 map 在本地模式下没有读取方——三个查询都抛
    /// 异常，`wait_ready` 用接口默认实现，任务与 actor 直接丢掉组绑定——所以删除与否在外
    /// 部看不到，`Ok(())` 是这里全部可观察的结果。`wait_ready` 被拒绝是因为它会报「任何
    /// id 都已就绪」，那是一个看得见的假答案。
    ///
    /// [`RayError::Ffi`] 的另一条来路是上游真的失败：
    /// `CoreWorker::RemovePlacementGroup` 把 GCS 调用的 status 交回
    /// （超时被改写成带 id 的文本，`src/ray/core_worker/core_worker.cc:2293-2306`），
    /// `NativeTaskSubmitter::RemovePlacementGroup` 在 status 非 OK 时抛出 `RayException`
    /// （`cpp/src/ray/runtime/task/native_task_submitter.cc:219-226`）。实测：把 GCS 挂起
    /// （`SIGSTOP`）并把 `RAY_gcs_server_request_timeout_seconds` 调到 2 秒，调用返回
    /// `Ffi`，消息为 `ray_placement_group_remove (<id 的 hex>): There was timeout in removing
    /// the placement group of id <...>`；直接把 GCS 杀掉则走不到这里，驱动会先在上游
    /// `rpc_client.h:202-214` 的 `_Exit` 终止。
    /// C 入口在 runtime 未初始化时也返回 `-1`，但本方法拿 `&self`，拿到 `&self` 就已经
    /// `connect` 过，所以这条只在 C 调用方与尚未 `ray_init` 的工人进程里出现。
    ///
    /// 调用返回时 GCS 的表项已经是 `REMOVED`：`SyncRemovePlacementGroup` 是同步 RPC，
    /// GCS 先删名字映射、把状态改成 `REMOVED`、写回表项，回调里才回复
    /// （`src/ray/gcs/gcs_placement_group_manager.cc:411-469`）。表项不会消失（GCS 保留历史
    /// 行），节点上预留的那份资源晚一些才归还——[`Ray::placement_groups`] 与
    /// [`Ray::placement_group_by_id`] 的文档写了表项保留这件事。
    pub fn placement_group_remove(&self, group_id: &[u8]) -> Result<(), RayError> {
        placement_group_remove_inner(group_id)
    }

    /// 等一个 placement group 就绪。
    ///
    /// `group_id` 是 [`Ray::placement_group_create`] 返回的二进制 id，必须正好 18
    /// 字节；其他长度（包括空字节）返回 [`RayError::Ffi`]，进程存活。
    ///
    /// `timeout_seconds` 原样交给 GCS，换算成 RPC 的截止时间：正数等这么久，`0` 把截止
    /// 时间设成现在，负数的截止时间已经过期。实测（已就绪的组上三跑一致）：`0` 仍然
    /// 返回 `Ok(true)`（应答在毫秒级内到达），**负数即使组已就绪也返回 `Ok(false)`**
    /// （不查现状）。未就绪的组在期限内返回 `Ok(false)`。
    ///
    /// 本地模式返回 [`RayError::Ffi`]：本地模式的等待实现没有覆盖接口默认值，上游
    /// 会对任何 id 都答「ready」。
    pub fn placement_group_wait_ready(
        &self,
        group_id: &[u8],
        timeout_seconds: i32,
    ) -> Result<bool, RayError> {
        placement_group_wait_ready_inner(group_id, timeout_seconds)
    }

    /// 列出集群里的全部 placement group。
    ///
    /// 列表是 GCS 的表项，包含 `state == 3`（`REMOVED`）的历史条目：上游
    /// `RemovePlacementGroup` 把行改成 `REMOVED` 之后继续写回去
    /// （`src/ray/gcs/gcs_placement_group_manager.cc:458-469`），不删行。所以「全部组」
    /// 不等于「全部存活的组」，判断存活要看 [`PlacementGroupInfo::state`]。
    ///
    /// 本地模式返回 [`RayError::Ffi`]：这三个查询直接解引用 GCS accessor，本地模式
    /// 里它是空指针。
    pub fn placement_groups(&self) -> Result<Vec<PlacementGroupInfo>, RayError> {
        placement_groups_inner()
    }

    /// 按二进制 id 查一个 placement group。
    ///
    /// `group_id` 必须正好 18 字节。已移除的组**仍然返回**，`state` 是 3（`REMOVED`）
    /// —— GCS 保留表项（同 [`Ray::placement_groups`] 的理由）；只有从未存在过的 id
    /// 才返回 [`RayError::Ffi`]，消息里带查询用的 id。本地模式同
    /// [`Ray::placement_groups`]。
    pub fn placement_group_by_id(&self, group_id: &[u8]) -> Result<PlacementGroupInfo, RayError> {
        placement_group_by_id_inner(group_id)
    }

    /// 按名字查一个 placement group。
    ///
    /// 只在**本 job 的 namespace 里**找：上游用
    /// `GetPlacementGroupByName(name, GetJobConfig().ray_namespace())`
    /// （`cpp/src/ray/runtime/abstract_ray_runtime.cc:363-372`），所以别的 namespace
    /// 里建的同名组会得到「not found」。这是查询范围，不是错误条件。
    ///
    /// 移除一个组时名字映射立刻被删（`gcs_placement_group_manager.cc:414-427`），所以
    /// 移除之后本方法马上报 [`RayError::Ffi`]，比按 id 的查询早（后者仍然成功、
    /// `state` 是 `REMOVED`）。查不到时消息里带查询用的名字。本地模式同
    /// [`Ray::placement_groups`]。
    pub fn placement_group_by_name(&self, name: &str) -> Result<PlacementGroupInfo, RayError> {
        placement_group_by_name_inner(name)
    }

    // ── Misc ──────────────────────────────────────────────────

    /// Get the namespace of this job.
    ///
    /// Cluster mode returns the job config's namespace, which for a job that
    /// configured none is the random value Ray generates. The namespace scopes
    /// named actors — registration ([`RayConfig::namespace`]'s default and
    /// [`ActorOptions::ray_namespace`]) and lookup ([`Ray::get_actor`]) — and
    /// named placement groups ([`Ray::placement_group_by_name`]).
    ///
    /// Local mode returns whatever `RayConfig::namespace` gave `ray_init`, and the
    /// empty string when that was none: local mode never starts a core worker, so
    /// there is no job config to ask. Ray does record a namespace for a driver job
    /// regardless of the run mode (`ConfigInternal::ray_namespace`, generated when
    /// the caller configured none), but nothing in local mode reads it, so the
    /// anonymous namespace a cluster would report is not observable here. Local
    /// mode also keeps one flat name table: `CreateActor` registers by name only
    /// (`cpp/src/ray/runtime/task/local_mode_task_submitter.cc:143-152`) and
    /// `GetActor` ignores its `ray_namespace` argument (same file, `:159-167`),
    /// and its placement group queries are rejected outright.
    pub fn namespace(&self) -> Result<String, RayError> {
        get_namespace()
    }

    /// Get a named actor by name and namespace.
    ///
    /// `name` must not be empty: an empty name returns [`RayError::Ffi`] without
    /// touching Ray, because the core worker's
    /// `CoreWorker::GetNamedActorHandle` starts with `RAY_CHECK(!name.empty())`
    /// (`src/ray/core_worker/core_worker.cc:2594`) and RAY_CHECK terminates the
    /// process. A name containing an interior NUL byte is rejected the same way:
    /// it reaches the C side as an empty C string (#27). A name of only
    /// whitespace characters is legal and is not trimmed, so it is looked up as
    /// it is. `namespace` may be empty: the C++ submitter falls back to the job
    /// config's namespace for an empty one
    /// (`cpp/src/ray/runtime/task/native_task_submitter.cc:181-186`).
    ///
    /// Returns `Ok(Some(handle))` when the name is found and `Ok(None)` when it is
    /// not. A found handle joins the actor registry this process keeps, with the
    /// language and creation descriptor type the core worker recorded for the
    /// actor (`rpc.ActorHandle.actor_language`, field 5, and the oneof member in
    /// `actor_creation_task_function_descriptor`, field 6, of
    /// `src/ray/protobuf/core_worker.proto:27-79`; local mode reports C++ with the
    /// cpp descriptor), because the core worker hands out only the 16-byte id.
    /// [`Ray::actor_call`], [`Ray::actor_call_python`], [`Ray::kill_actor`] and
    /// [`ActorHandle::serialize`] then accept it under the requirements each of
    /// them documents: an entry whose language differs from the actor's refuses
    /// it, and [`Ray::actor_call_python`] additionally needs the Python creation
    /// descriptor.
    ///
    /// Local mode ignores `namespace` and looks the name up in its flat table. The
    /// empty-name check runs before the mode decision, so local mode returns
    /// [`RayError::Ffi`] for an empty name as well; before the check it answered
    /// `Ok(None)` there.
    pub fn get_actor(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<Option<ActorHandle>, RayError> {
        get_actor(name, namespace)
    }

    /// Cancel a remote task by object ID.
    /// `obj_id` must be exactly 28 bytes (the Ray 2.51.1 ObjectID length).
    ///
    /// Local mode returns [`RayError::Ffi`]: `CancelTask` exists only on the core
    /// worker, which local mode never starts. Its submitter keeps no handle to
    /// withdraw a task once it is submitted, so there is nothing to cancel.
    pub fn cancel(&self, obj_id: &[u8], force_kill: bool, recursive: bool) -> Result<(), RayError> {
        cancel(obj_id, force_kill, recursive)
    }

    /// Returns true if the current actor was restarted.
    ///
    /// A `bool` has no error channel, so a failure reads as `false`. Local mode
    /// always reports `false`; it takes the SINGLE_PROCESS branch, which does not
    /// touch the core worker.
    pub fn was_current_actor_restarted(&self) -> bool {
        was_current_actor_restarted()
    }

    /// Check if Ray is initialized.
    pub fn is_initialized(&self) -> bool {
        is_initialized()
    }

    /// 这个进程是不是以本地模式初始化的（`RayConfig::local_mode`）。
    pub fn is_local_mode(&self) -> bool {
        is_local_mode()
    }
}

// ─── Lifecycle ────────────────────────────────────────────────

/// 列出两份配置里不同的字段，用来说明第二次 `connect` 为什么被拒。
fn config_mismatch_message(live: &RayConfig, requested: &RayConfig) -> String {
    let mut differences: Vec<String> = Vec::new();
    if live.address != requested.address {
        differences.push(format!(
            "address: {} -> {}",
            live.address, requested.address
        ));
    }
    if live.local_mode != requested.local_mode {
        differences.push(format!(
            "local_mode: {} -> {}",
            live.local_mode, requested.local_mode
        ));
    }
    if live.node_ip != requested.node_ip {
        differences.push(format!(
            "node_ip: {} -> {}",
            live.node_ip, requested.node_ip
        ));
    }
    if live.namespace != requested.namespace {
        differences.push(format!(
            "namespace: {} -> {}",
            live.namespace, requested.namespace
        ));
    }
    if live.runtime_env != requested.runtime_env {
        differences.push(format!(
            "runtime_env: {} -> {}",
            live.runtime_env, requested.runtime_env
        ));
    }
    if live.log_dir != requested.log_dir {
        differences.push(format!(
            "log_dir: {} -> {}",
            live.log_dir, requested.log_dir
        ));
    }
    if live.code_search_path != requested.code_search_path {
        differences.push(format!(
            "code_search_path: {:?} -> {:?}",
            live.code_search_path, requested.code_search_path
        ));
    }
    if live.default_actor_lifetime != requested.default_actor_lifetime {
        differences.push(format!(
            "default_actor_lifetime: {:?} -> {:?}",
            live.default_actor_lifetime, requested.default_actor_lifetime
        ));
    }
    format!(
        "Ray is already initialized with a different config: {}",
        differences.join(", ")
    )
}

/// Initialize Ray runtime with full config.
///
/// 成功之后发布新的运行时代次：此前创建的 `ObjectRef` 全部属于已经关闭的运行时。
pub(crate) fn init_with_config(config: &RayConfig) -> Result<(), RayError> {
    let address_c = to_cstring(&config.address);
    let local_mode = if config.local_mode { 1 } else { 0 };
    let node_ip_c = to_cstring(&config.node_ip);
    let code_search_path_c = to_cstring(&config.code_search_path.join(":"));
    let runtime_env_c = to_cstring(&config.runtime_env);
    let log_dir_c = to_cstring(&config.log_dir);

    let namespace_c = to_cstring(&config.namespace);

    let ret = unsafe {
        rayrust_sys::ray_init(
            address_c.as_ptr(),
            local_mode,
            node_ip_c.as_ptr(),
            code_search_path_c.as_ptr(),
            runtime_env_c.as_ptr(),
            log_dir_c.as_ptr(),
            config.default_actor_lifetime as i32,
            namespace_c.as_ptr(),
        )
    };
    if ret != 0 {
        return Err(ffi_error("ray_init"));
    }

    crate::object_ref::publish_live_epoch();

    // Warn if C++ SDK (default_worker) is not installed on this node.
    // This is needed for cluster-mode Rust actors — raylet launches default_worker
    // on worker nodes, and if it's missing, actors will silently die (NODE_DIED).
    if !config.local_mode {
        check_cpp_sdk();
    }

    Ok(())
}

/// Check if the Ray C++ SDK is properly installed on this node.
/// Prints a warning if `default_worker` is missing — this node cannot run C++ actors.
fn check_cpp_sdk() {
    let ray_path = std::process::Command::new("python3")
        .args(["-c", "import ray, os; print(os.path.dirname(ray.__file__))"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());

    if let Some(ray_dir) = ray_path {
        let default_worker = std::path::Path::new(&ray_dir)
            .join("cpp")
            .join("default_worker");
        if !default_worker.exists() {
            eprintln!("\n⚠️  WARNING: Ray C++ SDK not found on this node (missing {}).", default_worker.display());
            eprintln!("   Rust actors require 'pip install ray[cpp]' on ALL worker nodes.");
            eprintln!("   Without it, actor_create() succeeds but actor_call() hangs (NODE_DIED).");
            eprintln!("   Use local mode (RayConfig::local()) if C++ SDK is unavailable.\n");
        } else {
            // SDK exists on driver node — remind user to check worker nodes too
            eprintln!("ℹ️  Note: Rust actors in cluster mode require 'pip install ray[cpp]' on ALL worker nodes,");
            eprintln!("   not just this node. If actor_call() hangs, check that worker nodes have the C++ SDK.");
        }
    }
}

/// Check if Ray is initialized.
pub(crate) fn is_initialized() -> bool {
    unsafe { rayrust_sys::ray_is_initialized() }
}

/// Shutdown Ray runtime.
pub(crate) fn shutdown() {
    unsafe { rayrust_sys::ray_shutdown() }
}

// ─── Object Store ─────────────────────────────────────────────

/// Put an object into the object store.
/// Returns an ObjectRef that can be retrieved with `get`.
pub(crate) fn put<T: serde::Serialize>(value: &T) -> Result<ObjectRef<T>, RayError> {
    let data = serialize(value)?;
    let id = put_raw(&data)?;
    Ok(ObjectRef::from_call_id(id))
}

/// Put a value into the object store with XLANG header wrapping.
///
/// 数据前带 9 字节 xlang 头，与 Python 的 msgpack 段格式一致。驱动 put 出来的
/// 对象没有 metadata（`NativeObjectStore::PutRaw` 给 `CoreWorker::Put` 传
/// `nullptr`），所以把它当引用参数交给 Python 任务/actor 时，Python 侧会因为
/// 缺 metadata 拒收（`_private/serialization.py` 的 `_deserialize_object`）；
/// 这是既有行为，见 #21。
pub(crate) fn put_xlang<T: serde::Serialize>(value: &T) -> Result<ObjectRef<T>, RayError> {
    let data = crate::serialize::serialize_xlang(value)?;
    let id = put_raw(&data)?;
    Ok(ObjectRef::from_call_xlang(id))
}

/// Asynchronously put an object into the object store.
pub(crate) async fn put_async<T>(value: T) -> Result<ObjectRef<T>, RayError>
where
    T: serde::Serialize + Send + 'static,
{
    let bytes = tokio::task::spawn_blocking(move || serialize(&value))
        .await
        .map_err(|e| RayError::Runtime(format!("serialize join error: {}", e)))??;
    let id = tokio::task::spawn_blocking(move || put_raw(&bytes))
        .await
        .map_err(|e| RayError::Runtime(format!("put_raw join error: {}", e)))??;
    Ok(ObjectRef::from_call_id(id))
}

/// Ray 的 `Get` / `Wait` 只接受 `-1` 或非负的毫秒期限，其他负值会让内部的
/// `RAY_CHECK` 终止整个进程，必须在进入 FFI 之前拒绝。
fn validate_timeout(timeout_ms: i32) -> Result<(), RayError> {
    if timeout_ms < -1 {
        return Err(RayError::Ffi(format!(
            "timeout_ms must be -1 (wait forever) or non-negative, got {}",
            timeout_ms
        )));
    }
    Ok(())
}

/// Convert a duration to the millisecond timeout Ray's `Get` takes.
///
/// Capped at `i32::MAX` (about 24 days). A `Duration` is never negative, so the
/// result can never become Ray's -1, which means wait forever.
pub(crate) fn duration_to_ms(timeout: std::time::Duration) -> i32 {
    timeout.as_millis().min(i32::MAX as u128) as i32
}

/// Wait for objects to be locally available.
///
/// # Arguments
/// * `object_refs` - References to wait for. Their ids must be pairwise distinct.
/// * `num_objects` - Minimum number of objects to wait for. Must be in
///   `1..=object_refs.len()`: Ray's own `CoreWorker::Wait` requires it whether or
///   not the objects are ready (src/ray/core_worker/core_worker.cc:1475-1483). In
///   local mode the ids are deduplicated while the count is passed on unchanged, so
///   when the count is above the deduplicated set size and at least one deduplicated
///   id is not in the local store yet, the request built there hits the RAY_CHECK in
///   src/ray/core_worker/store_provider/memory_store/memory_store.cc:89 and ends the
///   process; with every id already there, the store returns OK before building that
///   request and the same input answers Ok. Either way the wrapper rejects it first,
///   matching the cluster mode rule.
/// * `timeout_ms` - Timeout in milliseconds. -1 for infinite, other negative
///   values are rejected with `RayError::Ffi`.
///
/// Both the count and the duplicate-id check happen in `ray_wait` before the
/// runtime is touched, so local mode and cluster mode report the same sentence
/// and the process survives.
///
/// # Returns
/// A tuple of (ready, unready) object references.
pub(crate) fn wait<T>(
    object_refs: &[ObjectRef<T>],
    num_objects: usize,
    timeout_ms: i32,
) -> WaitResult<T> {
    for obj_ref in object_refs {
        check_epoch(obj_ref.epoch)?;
    }
    validate_timeout(timeout_ms)?;

    let ids: Vec<RayBytes> = object_refs
        .iter()
        .map(|r| RayBytes {
            data: r.id.as_ptr() as *const std::os::raw::c_char,
            len: r.id.len(),
        })
        .collect();

    let result = unsafe { rayrust_sys::ray_wait(ids.as_ptr(), ids.len(), num_objects, timeout_ms) };

    if result.is_null() {
        return Err(ffi_error("ray_wait"));
    }

    let mut ready = Vec::new();
    let mut unready = Vec::new();
    for (i, obj) in object_refs.iter().enumerate() {
        let is_ready = unsafe { *result.add(i) };
        if is_ready {
            ready.push(obj.clone());
        } else {
            unready.push(obj.clone());
        }
    }
    unsafe { rayrust_sys::ray_free_bools(result) };

    Ok((ready, unready))
}

// ─── Raw object store operations ──────────────────────────────

/// Put raw bytes into the object store. Returns the binary object ID.
pub(crate) fn put_raw(data: &[u8]) -> Result<Vec<u8>, RayError> {
    let bytes = unsafe {
        rayrust_sys::ray_put(data.as_ptr() as *const std::os::raw::c_char, data.len())
    };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error("ray_put"))?;
    Ok(guard.as_slice().to_vec())
}

/// Get raw bytes from the object store by binary ID. Blocks until ready.
pub(crate) fn get_raw(id: &[u8]) -> Result<Vec<u8>, RayError> {
    get_raw_timeout(id, -1)
}

/// Get raw bytes from the object store by binary ID with timeout.
pub(crate) fn get_raw_timeout(id: &[u8], timeout_ms: i32) -> Result<Vec<u8>, RayError> {
    validate_timeout(timeout_ms)?;

    // Clear first so the kind read below belongs to this call.
    rayrust_sys::clear_error();
    let bytes = unsafe {
        rayrust_sys::ray_get(
            id.as_ptr() as *const std::os::raw::c_char,
            id.len(),
            timeout_ms as c_int,
        )
    };
    let guard = CBytesGuard::from(bytes).ok_or_else(|| {
        if rayrust_sys::last_error_kind() == ErrorKind::None {
            RayError::Ffi(format!(
                "ray_get returned no data and recorded no error for a {} byte id",
                id.len()
            ))
        } else {
            ffi_error("ray_get")
        }
    })?;
    Ok(guard.as_slice().to_vec())
}

// ─── Task ─────────────────────────────────────────────────────

/// `task_call_async` 与 `task_call_async_with_args` 的共同主体。
///
/// 代次在创建 future 时取，参数准备失败时在这里返回、不提交任务。保活集合被移进
/// `spawn_blocking` 的闭包，活到 FFI 调用返回为止：future 在排队期间被丢弃时，
/// 参数引用仍然键在闭包里。
fn submit_task_async(
    func_name: &str,
    args: TaskArgs,
    opts: &TaskOptions,
) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
    let func_name = func_name.to_string();
    let epoch = crate::object_ref::live_epoch();
    // 校验与 options JSON 都在创建 future 之前算好：字段组合不合法时连 JSON 都
    // 不生成，错误在第一次轮询时返回，任务不会被提交（与 `into_parts` 的错误同一
    // 时机）。
    let options_json = check_task_options(opts).map(|()| opts.to_json());
    async move {
        let options_json = options_json?;
        let (args, is_ref, held) = args.into_parts()?;
        task_call_inner_async(func_name, args, is_ref, held, epoch, options_json).await
    }
}

/// `is_ref` 要么为空（所有参数都是值），要么与 `args` 一一对应。
///
/// 长度对不上时 C 侧会按 `arg_count` 越界读：读到的那一字节会让参数被静默地按错误
/// 形状解释，或者让一个短 buffer 被当成对象 id 而终止进程。
fn check_is_ref_len(args_len: usize, is_ref: &[bool]) -> Result<(), RayError> {
    if is_ref.is_empty() || is_ref.len() == args_len {
        return Ok(());
    }
    Err(RayError::Ffi(format!(
        "is_ref has {} entries but there are {} arguments; pass an empty slice to treat every argument as a value",
        is_ref.len(),
        args_len
    )))
}

/// Call a remote task by function name.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
/// `is_ref` can be empty slice to treat all args as values.
pub(crate) fn task_call_inner(
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
    opts: &TaskOptions,
) -> Result<ObjectRef<()>, RayError> {
    check_is_ref_len(args.len(), is_ref)?;
    check_task_options(opts)?;
    task_call_json_inner(func_name, args, is_ref, &opts.to_json())
}

/// `task_call_inner` 的主体，`options_json` 已由调用方算好（异步路径在闭包外算，
/// 随闭包 move 进来）。空字符串与 NULL 等价：不设任何选项。
pub(crate) fn task_call_json_inner(
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
    options_json: &str,
) -> Result<ObjectRef<()>, RayError> {
    check_is_ref_len(args.len(), is_ref)?;
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };
    let options_json_c = to_cstring(options_json);

    let bytes = unsafe {
        rayrust_sys::ray_task_call_with_options(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
            if options_json_c.as_bytes().is_empty() {
                std::ptr::null()
            } else {
                options_json_c.as_ptr()
            },
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_task_call_with_options", func_name))?;
    Ok(ObjectRef::from_call_id(guard.as_slice().to_vec()))
}

/// Asynchronously call a remote task by function name.
///
/// `held` 是参数引用的保活集合：它被移进 `spawn_blocking` 的闭包，活到 FFI 调用
/// 返回为止，所以提交期间对象不会被释放。`options_json` 在闭包外算好：进入
/// 异步块之后只有 `&TaskOptions` 的借用，拿不到它了。
pub(crate) async fn task_call_inner_async(
    func_name: String,
    args: Vec<Vec<u8>>,
    is_ref: Vec<bool>,
    held: Vec<ObjectRef<()>>,
    epoch: u64,
    options_json: String,
) -> Result<ObjectRef<()>, RayError> {
    tokio::task::spawn_blocking(move || {
        let _held = held;
        check_epoch(epoch)?;
        let args_ref: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        task_call_json_inner(&func_name, &args_ref, &is_ref, &options_json)
    })
    .await
    .map_err(|e| RayError::Runtime(format!("task_call join error: {}", e)))?
}

/// Call a Python remote function.
/// `module` is the Python module name, `function` is the function name.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
/// Returns an ObjectRef with is_xlang=true (results need xlang header stripping).
pub(crate) fn task_call_python_inner(
    module: &str,
    function: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<Vec<u8>, RayError> {
    check_is_ref_len(args.len(), is_ref)?;
    let module_c = to_cstring(module);
    let func_c = to_cstring(function);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_task_call_python(
            module_c.as_ptr(),
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_task_call_python", format!("{}.{}", module, function)))?;
    Ok(guard.as_slice().to_vec())
}

// ─── Actor ────────────────────────────────────────────────────

/// A handle to a remote actor.
#[derive(Debug, Clone)]
pub struct ActorHandle {
    pub(crate) id: Vec<u8>,
}

impl ActorHandle {
    /// Get the actor's ID as raw bytes.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// 把 handle 序列化成可以跨进程传递的字节。
    ///
    /// 返回的是 core worker 的 `rpc::ActorHandle` protobuf：actor id、owner id 与 owner
    /// 地址、创建 job id、actor 语言、创建任务的函数描述符、actor cursor、
    /// `max_task_retries`、名字、namespace、`max_pending_calls` 等。带着 owner 地址
    /// 与函数描述符，所以拿到字节的进程不必自己查 GCS 就能向这个 actor 提交方法调用。
    /// [`ActorHandle::from_serialized`] 是它的逆操作。
    ///
    /// 只对「这个进程登记过的 handle」成立：[`Ray::actor_create`]、
    /// [`Ray::actor_create_python`]、[`Ray::get_actor`]、
    /// [`ActorHandle::from_serialized`] 都会在 core worker 的 actor manager 里登记。
    /// 本方法在进 SDK 之前查登记表：SDK 的 `ActorManager::GetActorHandle` 对查不到的 id
    /// 直接 RAY_CHECK 终止进程、没有错误可以返回，那条终止因此不可达。
    ///
    /// 本地模式返回 [`RayError::Ffi`]：`AbstractRayRuntime::SerializeActorHandle` 的第一句
    /// 就是 `CoreWorkerProcess::GetCoreWorker()`，本地模式从不启动 core worker 进程，
    /// 那里直接 `QuickExit()`（`_Exit(1)`，`src/ray/util/process.cc`）。
    ///
    /// 上一个运行时留下的 handle（同一个进程里 `drop` 掉 `Ray` 之后重新 `connect`）
    /// 返回 [`RayError::Ffi`]，消息里带这个 id 并说明它没有登记：登记表随运行时清空，
    /// 它在新的运行时里查不到，两个模式下都在进 SDK 之前被拒掉。[`Ray::actor_call`] 与
    /// [`Ray::kill_actor`] 拿同一个陈旧 handle 时报同一个错。
    pub fn serialize(&self) -> Result<Vec<u8>, RayError> {
        actor_handle_serialize_inner(&self.id)
    }

    /// 从一个 handle 字节复原并登记 handle。
    ///
    /// 空字节返回 [`RayError::Ffi`]。字节必须是一段结构良好的 `rpc::ActorHandle`
    /// protobuf（`src/ray/protobuf/core_worker.proto:27-79`），且 `actor_id`（字段 1）
    /// 正好 16 字节、`creation_job_id`（字段 4）正好 4 字节。已知字段按 Ray 2.51.1 的
    /// schema 校验内容：嵌套消息（`owner_address`、
    /// `actor_creation_task_function_descriptor`、`labels` 的每个 entry）逐层递归，
    /// proto3 的 `string` 字段校验 UTF-8；未知字段与 wire type 与 schema 不符的字段按
    /// wire type 跳过，与上游一致。同一字段出现多次时按最后一次。载荷畸形、缺字段、
    /// 字段长度不对都先在这里报 [`RayError::Ffi`]，错误文本能区分这三种情形。
    ///
    /// 嵌套消息与 `string` 的内容必须校验，否则两个长度都不可信：上游的
    /// `ParseFromString` 在第一条损坏的嵌套消息或非法 UTF-8 的 `string` 处中止，
    /// `CreateInnerActorHandleFromString` 丢弃这个结果
    /// （`src/ray/core_worker/actor_handle.cc:66`），登记进去的是中止之前写入的那些
    /// 字段。所以「3 字节 `actor_id` + 损坏的 `owner_address` + 16 字节 `actor_id`」
    /// 在上游登记的仍是那个 3 字节值，登记阶段在 `id.h:435` 终止进程；排在损坏内容
    /// 之后的 `creation_job_id` 上游从未写入，登记出 nil job id 的 handle。两类载荷
    /// 都由这里拒掉。递归按 schema 的树走，最深三层（`rpc.ActorHandle` →
    /// `rpc.FunctionDescriptor` → `rpc.PythonFunctionDescriptor`）；超过 8 层的走法
    /// 按载荷畸形拒绝，构造出来的载荷进不到更深处。
    ///
    /// 字段 key 与长度前缀还要求最短编码且能放进 32 位：上游把 tag 读成 `uint32`、把
    /// 长度前缀读成 `int32`，5 字节但超过 32 位的 key 会被截成别的字段（实测
    /// `0x10000000a` 被当成 `actor_id`，随后的短内容终止进程），超过 5 字节的 key、
    /// 达到 2^31 的长度与超过 5 字节的长度前缀都让上游的 `ParseFromString` 失败；
    /// 5 字节以内但非最短编码的 key 与长度上游会接受，这里当载荷畸形拒掉。这套校验
    /// 比上游严的地方正好是这三类编码：wire type 3/4/6/7、非最短编码、超过 32 位的
    /// key 与长度前缀；protobuf 自己的序列化不会产生它们，所以
    /// [`ActorHandle::serialize`] 交出来的 handle 不会被拒。
    ///
    /// 上面那张 schema 表按 Ray 2.51.1 固定：新增字段两侧都当未知字段跳过，不需要改；
    /// 若某个已有字段的类型变了，表要跟着改，否则新类型的损坏内容会重新变成「只按
    /// wire type 跳过」。
    /// 上游丢弃 `ParseFromString` 的返回值，所以「合法字段后面跟损坏字节」的载荷原本
    /// 能登记成 handle，现在直接报 [`RayError::Ffi`]。
    /// 三个长度问题都在上游终止进程或让调用失败：`actor_id` 在登记阶段被读成
    /// `ActorID`（`src/ray/core_worker/actor_manager.cc:33`
    /// → `src/ray/common/id.h:430-441`）、`creation_job_id` 在第一次调用时被读成
    /// `JobID`（`src/ray/core_worker/core_worker.cc:2374`，`JobID::kLength` 是 4）。
    /// 缺 `creation_job_id` 时 nil job id 让 actor 的 worker 在
    /// `WorkerContext::SetCurrentTask` 的 job id 检查处终止
    /// （`src/ray/core_worker/context.cc:295`，调用方收到 `ActorError`；实测：一个进程
    /// 建 detached actor 并写出载荷，另一个进程删掉字段 4 后登记并调用）。载荷校验在
    /// 本地模式判定之前，所以两种模式下畸形载荷都报载荷错误。
    ///
    /// 载荷校验只保证长度：`actor_id` 的 16 个字节原样登记，内容全 `0xff` 时是 nil id、
    /// 全 `0x00` 时是不属于任何 actor 的字节（合法 actor id 是散列），两者都在登记之后
    /// 被拒成 [`RayError::Ffi`]。这种 handle 在调用方手里没有任何明确的失败：调用提交成功
    /// 但永远不就绪（`get` 要等到期限才报超时），`serialize()` 把登记的那份 protobuf 再
    /// 交出来（长度与登记时相同，protobuf 重新序列化可能改写长度前缀；重新序列化结果为
    /// 空时报错），所以 C 侧看到这两种内容就直接报 [`RayError::Ffi`]。
    ///
    /// 结构良好的（`actor_id` 16 字节、`creation_job_id` 4 字节）载荷即使 `actor_id`
    /// 是 16 个 `0x11` 也会被收下并返回 16 字节 id：上游不验证 id 是否属于真实 actor。
    /// 这个 id 进本 crate 的 actor 登记表，语言就是载荷的 `actor_language` 字段
    /// （`src/ray/protobuf/core_worker.proto:42`；字段缺席时是 proto3 的默认值
    /// `PYTHON = 0`），创建描述符类型是字段 6 里那个 oneof 成员的字段号（`java` 1、
    /// `python` 2、`cpp` 3，字段 6 缺席时是「没有」）。两个调用入口按这两项判定，实测
    /// 的四种形态（集群上的运行；「提交成功」指进了队，`get` 到期报超时，因为 actor id
    /// 不属于任何真实 actor）：
    ///
    /// | 载荷 | [`Ray::actor_call`] | [`Ray::actor_call_python`] |
    /// | --- | --- | --- |
    /// | 字段 1 + 4 | 语言不符而拒（`is a Python actor`） | 没有 Python 创建描述符而拒 |
    /// | 字段 1 + 4 + 字段 5 = `CPP` | 提交成功，`get` 到期报超时 | 语言不符而拒 |
    /// | 字段 1 + 4 + 字段 6 = `python_function_descriptor` | 语言不符而拒 | 提交成功，`get` 到期报超时 |
    /// | 字段 1 + 4 + 字段 5 = `PYTHON` + 字段 6 = `cpp_function_descriptor` | 语言不符而拒 | 没有 Python 创建描述符而拒 |
    ///
    /// 第二行提交成功是因为 C++ 分支不读创建描述符
    /// （`cpp/src/ray/runtime/task/native_task_submitter.cc:81-82` 只读
    /// `MaxTaskRetries`）。第一、四行被 Python 入口拒掉，是因为字段 6 缺席或放的是 cpp
    /// 描述符，而 Python 分支会把它按 `PythonFunctionDescriptor` 读
    /// （`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`，`As` 是
    /// `reinterpret_cast`）；两行都停在 crate 的守卫里，没有进 SDK。
    ///
    /// 同一个 id 已经登记过时保留首次登记的语言与创建描述符类型，载荷里的语言字段只在
    /// 首次登记时生效，与上游 actor manager 的 `emplace` 一致
    /// （`src/ray/core_worker/actor_manager.cc:179`）。
    ///
    /// 本地模式返回 [`RayError::Ffi`]：登记要读 `CoreWorkerProcess::GetCoreWorker()`，
    /// 本地模式从不启动 core worker 进程，那里直接 `QuickExit()`（`_Exit(1)`）。
    pub fn from_serialized(bytes: &[u8]) -> Result<ActorHandle, RayError> {
        actor_handle_from_serialized_inner(bytes)
    }
}

// ─── Actor Creation Options (builder) ─────────────────────────

/// Escape a string for safe inclusion as a JSON string value.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Hex-encode raw bytes (for binary-safe transport inside JSON).
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Build a resources JSON string from (name, value) pairs.
fn resources_to_json(resources: &[(String, f64)]) -> String {
    if resources.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = resources.iter().map(|(n, v)| {
        let val = if *v == v.trunc() {
            format!("{}", *v as i64)
        } else {
            format!("{}", v)
        };
        format!("\"{}\":{}", json_escape(n), val)
    }).collect();
    format!("{{{}}}", parts.join(","))
}

// ─── Task Options (builder) ────────────────────────────────────

/// Builder for Ray `CallOptions`，与 C++ SDK 的 `internal::CallOptions` 五个字段
/// 一一对应：
/// - `name` — 任务名，空字符串表示不设，默认由函数名生成。名字进入任务规格；
///   任务失败或被取消时，驱动进程的 core worker 日志里会以 `task_name=<名字>`
///   打出这一行（`task_manager.cc`）。
/// - `resources` — 资源需求，例如 `[("CPU", 2.0), ("GPU", 0.5)]`。空表表示用
///   Ray 的默认值：任务要 1 个 CPU（与 Python `@ray.remote` 一致），所以
///   `--num-cpus` 限制同时运行的任务数。零需求必须显式写 `("CPU", 0.0)`；给了
///   别的资源而没给 `CPU` 时 1.0 会合并进资源表。
/// - `placement_group_id` — 绑定到 placement group 的二进制 id
///   （[`Ray::placement_group_create`] 返回的字节），空表示不绑。非空时必须
///   正好 18 字节（`PlacementGroupID` 的长度）；查不到的 group 报
///   [`RayError::Ffi`]，不会退回默认调度。
/// - `bundle_index` — 绑定到 group 内哪个 bundle；-1（默认）表示该 group 内任意
///   bundle 都可。必须是 -1 或非负值；只设 bundle_index 而不设 group 是错误（两者
///   都报 [`RayError::Ffi`]）。
/// - `runtime_env` — runtime env 对象的 JSON 文本，接受 Python
///   `ray.runtime_env.RuntimeEnv` 那种对象 JSON，例如
///   `{"env_vars":{"FOO":"bar"}}` 或 `{"pip":["..."],"env_vars":{...}}`；
///   空字符串表示不设。必须是 JSON 对象，非法 JSON 或非对象报
///   [`RayError::Ffi`]。底层由 SDK 的
///   `ray::RuntimeEnv::Deserialize(...).SerializeToRuntimeEnvInfo()` 转成 Ray 的
///   runtime env info。
///
/// 本地模式（[`RayConfig::local`]）下这些选项基本无效：
/// `LocalModeTaskSubmitter::SubmitTask` 把 `CallOptions` 整个丢弃
/// （`Submit(invocation, {})`），所以 `name`、`resources`、`runtime_env` 都不生效；
/// `placement_group_id` 则直接报 [`RayError::Ffi`]（本地模式没有 GCS accessor，
/// SDK 的 `GetPlacementGroupById` 在那里解引用空指针）。
///
/// # Example
/// ```no_run
/// use rayrust::prelude::*;
/// # let ray = Ray::local().unwrap();
/// let opts = TaskOptions::new()
///     .name("my_task")
///     .resource("CPU", 2.0)
///     .runtime_env(r#"{"env_vars":{"FOO":"bar"}}"#);
/// let obj = ray.task_call("my_task", &[], &[], &opts).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct TaskOptions {
    /// Task name. Empty = the default name derived from the function name.
    pub name: String,
    /// Resource requirements, e.g. `[("CPU", 2.0), ("GPU", 0.5)]`.
    /// Empty = Ray's default for a task, one CPU. A zero-CPU task must give
    /// `("CPU", 0.0)` explicitly; a list with other resources and no `CPU` gets
    /// CPU 1.0 merged in.
    pub resources: Vec<(String, f64)>,
    /// Binary placement group ID, exactly 18 bytes when non-empty.
    /// Empty = do not bind to a placement group.
    pub placement_group_id: Vec<u8>,
    /// Bundle index within the placement group: -1 (default) = any bundle in the
    /// group, otherwise non-negative. Only meaningful together with
    /// `placement_group_id`.
    pub bundle_index: i32,
    /// Runtime env object JSON, e.g. `{"env_vars":{"FOO":"bar"}}`. Empty = none.
    pub runtime_env: String,
}

impl Default for TaskOptions {
    /// `bundle_index` 的值是 -1（group 内任意 bundle），与 [`TaskOptions::new`]
    /// 相同。派生实现会给 0，则 `TaskOptions { placement_group_id, ..Default::default() }`
    /// 会把任务固定到第 0 个 bundle。
    fn default() -> Self {
        Self {
            name: String::new(),
            resources: Vec::new(),
            placement_group_id: Vec::new(),
            bundle_index: -1,
            runtime_env: String::new(),
        }
    }
}

impl TaskOptions {
    /// Create a new builder with no options (all defaults).
    ///
    /// The empty resource list means one CPU, the default Python's
    /// `@ray.remote` applies to every task.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the task name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Add a resource requirement, e.g. `("CPU", 2.0)`.
    ///
    /// Adding another resource leaves the task at one CPU: the default is merged
    /// in at submission, so zero CPU needs an explicit `("CPU", 0.0)`.
    pub fn resource(mut self, name: impl Into<String>, value: f64) -> Self {
        self.resources.push((name.into(), value));
        self
    }

    /// Set resource requirements as a slice of `(name, value)` pairs.
    ///
    /// The same default as [`TaskOptions::resource`]: no `CPU` key means one CPU.
    pub fn resources(mut self, resources: Vec<(String, f64)>) -> Self {
        self.resources = resources;
        self
    }

    /// Bind the task to a placement group bundle.
    /// `group_id` is the binary ID returned by `placement_group_create`; it must
    /// be exactly 18 bytes and must name an existing group, otherwise the call
    /// fails with [`RayError::Ffi`] instead of running the task outside the
    /// group. `bundle_index` is the index of the bundle within the group: -1 for
    /// any bundle in the group, otherwise non-negative.
    pub fn placement_group(mut self, group_id: Vec<u8>, bundle_index: i32) -> Self {
        self.placement_group_id = group_id;
        self.bundle_index = bundle_index;
        self
    }

    /// Set the runtime env from its object JSON, e.g.
    /// `{"env_vars":{"FOO":"bar"}}`. The text must be a single JSON object;
    /// malformed JSON and non-objects fail the call with [`RayError::Ffi`].
    pub fn runtime_env(mut self, json: impl Into<String>) -> Self {
        self.runtime_env = json.into();
        self
    }

    /// Whether any resources are set.
    ///
    /// This reports the list that was written. The requirement that reaches the
    /// scheduler can differ: an empty list still means one CPU, and a list without
    /// `CPU` gets 1.0 merged in.
    pub fn has_resources(&self) -> bool {
        !self.resources.is_empty()
    }

    /// Serialize to a JSON string for the C ABI `options_json` parameter.
    /// Returns empty string if no options are set (all defaults).
    /// `runtime_env` is embedded as a JSON string value holding the object text,
    /// so a runtime env string can never splice keys into the outer document.
    fn to_json(&self) -> String {
        let mut parts: Vec<String> = Vec::new();

        if !self.name.is_empty() {
            parts.push(format!("\"name\":\"{}\"", json_escape(&self.name)));
        }
        if !self.resources.is_empty() {
            parts.push(format!(
                "\"resources\":{}",
                resources_to_json(&self.resources)
            ));
        }
        if !self.placement_group_id.is_empty() {
            parts.push(format!(
                "\"placement_group_id\":\"{}\",\"bundle_index\":{}",
                hex_encode(&self.placement_group_id),
                self.bundle_index
            ));
        }
        if !self.runtime_env.is_empty() {
            parts.push(format!(
                "\"runtime_env\":\"{}\"",
                json_escape(&self.runtime_env)
            ));
        }

        if parts.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", parts.join(","))
        }
    }
}

/// `TaskOptions` 里互相依赖的字段的构造期校验。
///
/// `to_json` 只在 `placement_group_id` 非空时输出 `bundle_index`，所以孤立的
/// `bundle_index`（没有 group）不会进 options JSON，C 侧的 `parse_call_options` 也就
/// 看不到它。那个组合在文档里承诺报错，必须在提交前拦下，否则字段被静默丢弃、任务
/// 以默认 options 执行。
fn check_task_options(opts: &TaskOptions) -> Result<(), RayError> {
    if opts.placement_group_id.is_empty() && opts.bundle_index != -1 {
        return Err(RayError::Ffi(format!(
            "bundle_index {} requires placement_group_id; without a placement group the field would be dropped",
            opts.bundle_index
        )));
    }
    Ok(())
}

// ─── Actor Creation Options (builder) ─────────────────────────
///
/// Exposes all fields of the C++ SDK's `ActorCreationOptions`:
/// - `name` — named actor (looked up later via [`Ray::get_actor`])
/// - `ray_namespace` — namespace for named actor lookup
/// - `resources` — CPU/GPU requirements
/// - `max_restarts` — restart on failure (default 0 = no restart)
/// - `max_concurrency` — max concurrent method calls (default 1)
/// - `serialized_runtime_env_info` — runtime environment info string
/// - `placement_group` — bind actor to a placement group bundle
///
/// `resources` carries no task-like default here: an actor reserves exactly what
/// this list names, so an actor without resources reserves no CPU.
///
/// Ray's Python driver chooses between two actor presets (`ray/actor.py:1660-1674`,
/// `ray/_private/ray_constants.py:174-179`): with no resource options the actor
/// reserves 0 CPU for its lifetime and its method requirement is set to 1 CPU
/// (`DEFAULT_ACTOR_METHOD_CPU_SIMPLE`) instead of reserving anything; with any
/// other resource named, the actor reserves its resources plus 1 CPU for its
/// lifetime (`DEFAULT_ACTOR_CREATION_CPU_SPECIFIED`). Measured in 2.51.1, a
/// default Python actor's method still ran in 0.01 s while a task held the only
/// CPU for 20 s, so the per-method figure does not make methods wait. The C++
/// SDK's `ActorCreationOptions` has no method-CPU field
/// (`include/ray/api/task_options.h:104-112`), so the first preset can only be
/// approximated by reserving nothing; the second is expressible here but is not
/// applied, which means an actor that names e.g. a GPU reserves no CPU where
/// Python reserves one (#55).
///
/// # Example
/// ```no_run
/// use rayrust::ActorOptions;
/// let opts = ActorOptions::new()
///     .name("my_actor")
///     .max_restarts(3)
///     .max_concurrency(10)
///     .resource("CPU", 2.0);
/// ```
#[derive(Debug, Clone)]
pub struct ActorOptions {
    /// Named actor name. Empty = anonymous actor.
    pub name: String,
    /// Namespace for named actor lookup. Empty = current namespace.
    /// Ignored in local mode, where actors are registered and looked up by name
    /// only.
    pub ray_namespace: String,
    /// Resource requirements, e.g. `[("CPU", 1.0), ("GPU", 0.5)]`.
    pub resources: Vec<(String, f64)>,
    /// Maximum number of restarts on failure. 0 = no restart (default).
    pub max_restarts: i32,
    /// Maximum number of concurrent method calls. 1 = serial (default).
    pub max_concurrency: i32,
    /// Serialized runtime environment info string.
    pub serialized_runtime_env_info: String,
    /// Binary placement group ID, exactly 18 bytes when non-empty.
    /// Empty = do not bind to a placement group.
    pub placement_group_id: Vec<u8>,
    /// Bundle index within the placement group: -1 (default) = any bundle in the
    /// group, otherwise non-negative; -1 is the smallest negative value. Only
    /// meaningful together with `placement_group_id`.
    pub bundle_index: i32,
}

impl Default for ActorOptions {
    /// Same as [`ActorOptions::new`]: `bundle_index` is -1 (any bundle in the
    /// group). A derived `Default` would give 0, so
    /// `ActorOptions { placement_group_id, ..Default::default() }` would pin the
    /// actor to the first bundle. `max_concurrency` is 1, the documented and
    /// SDK-side default (#29); a derived 0 keeps running serially in 2.51.1 only
    /// because `NeedDefaultExecutor` returns false for 0
    /// (src/ray/core_worker/task_execution/thread_pool.h:37-39) and the queue then
    /// runs the method in place (`actor_scheduling_queue.cc:277-284`). The SDK's
    /// own `RAY_CHECK(max_concurrency > 0)` (`thread_pool.cc:32`) is why 0 is not
    /// a supported concurrency value.
    fn default() -> Self {
        Self {
            name: String::new(),
            ray_namespace: String::new(),
            resources: Vec::new(),
            max_restarts: 0,
            max_concurrency: 1,
            serialized_runtime_env_info: String::new(),
            placement_group_id: Vec::new(),
            bundle_index: -1,
        }
    }
}

impl ActorOptions {
    /// Create a new builder with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the actor name (named actor).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the ray namespace for named actor lookup.
    pub fn ray_namespace(mut self, ns: impl Into<String>) -> Self {
        self.ray_namespace = ns.into();
        self
    }

    /// Add a resource requirement, e.g. `("CPU", 2.0)`.
    pub fn resource(mut self, name: impl Into<String>, value: f64) -> Self {
        self.resources.push((name.into(), value));
        self
    }

    /// Set resource requirements as a slice of `(name, value)` pairs.
    pub fn resources(mut self, resources: Vec<(String, f64)>) -> Self {
        self.resources = resources;
        self
    }

    /// Set the maximum number of restarts on failure.
    pub fn max_restarts(mut self, n: i32) -> Self {
        self.max_restarts = n;
        self
    }

    /// Set the maximum number of concurrent method calls.
    pub fn max_concurrency(mut self, n: i32) -> Self {
        self.max_concurrency = n;
        self
    }

    /// Set the serialized runtime environment info string.
    pub fn runtime_env_info(mut self, info: impl Into<String>) -> Self {
        self.serialized_runtime_env_info = info.into();
        self
    }

    /// Bind the actor to a placement group bundle.
    /// `group_id` is the binary ID returned by `placement_group_create`. An empty
    /// id means "no placement group" (same as the field); a non-empty one must be
    /// exactly 18 bytes and must name an existing group, otherwise the call fails
    /// with [`RayError::Ffi`] instead of creating the actor outside the group.
    /// `bundle_index` is the index of the bundle within the group: -1 for any
    /// bundle in the group, otherwise non-negative. An index at or beyond the
    /// group's bundle count is not rejected here: Ray's
    /// `AddPlacementGroupConstraint` only checks that it is -1 or non-negative
    /// (`src/ray/common/bundle_spec.cc:167`), so the actor simply never gets a
    /// bundle to run in. [`TaskOptions::placement_group`] has the same limit.
    pub fn placement_group(mut self, group_id: Vec<u8>, bundle_index: i32) -> Self {
        self.placement_group_id = group_id;
        self.bundle_index = bundle_index;
        self
    }

    /// Serialize to a JSON string for the C ABI `options_json` parameter.
    /// Returns empty string if no options are set (all defaults).
    fn to_json(&self) -> String {
        let mut parts: Vec<String> = Vec::new();

        if !self.name.is_empty() {
            parts.push(format!("\"name\":\"{}\"", json_escape(&self.name)));
        }
        if !self.ray_namespace.is_empty() {
            parts.push(format!("\"ray_namespace\":\"{}\"", json_escape(&self.ray_namespace)));
        }
        if !self.resources.is_empty() {
            parts.push(format!("\"resources\":{}", resources_to_json(&self.resources)));
        }
        if self.max_restarts != 0 {
            parts.push(format!("\"max_restarts\":{}", self.max_restarts));
        }
        // max_concurrency default is 1; only include if different
        if self.max_concurrency != 1 {
            parts.push(format!("\"max_concurrency\":{}", self.max_concurrency));
        }
        if !self.serialized_runtime_env_info.is_empty() {
            parts.push(format!(
                "\"serialized_runtime_env_info\":\"{}\"",
                json_escape(&self.serialized_runtime_env_info)
            ));
        }
        if !self.placement_group_id.is_empty() {
            parts.push(format!(
                "\"placement_group_id\":\"{}\",\"bundle_index\":{}",
                hex_encode(&self.placement_group_id),
                self.bundle_index
            ));
        }

        if parts.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", parts.join(","))
        }
    }
}

/// `ActorOptions` 里互相依赖的字段的构造期校验。
///
/// `to_json` 只在 `placement_group_id` 非空时输出 `bundle_index`，所以孤立的
/// `bundle_index`（没有 group）不会进 options JSON，C 侧的 `parse_actor_options` 也就
/// 看不到它。那个组合在文档里承诺报错，必须在创建之前拦下，否则字段被静默丢弃、
/// actor 以默认 options 建出来。
fn check_actor_options(opts: &ActorOptions) -> Result<(), RayError> {
    if opts.placement_group_id.is_empty() && opts.bundle_index != -1 {
        return Err(RayError::Ffi(format!(
            "bundle_index {} requires placement_group_id; without a placement group the field would be dropped",
            opts.bundle_index
        )));
    }
    Ok(())
}

/// Create an actor with full creation options.
pub(crate) fn actor_create_with_options_inner(
    func_name: &str,
    args: &[&[u8]],
    options: &ActorOptions,
) -> Result<ActorHandle, RayError> {
    // 先校验再进 FFI：字段组合不合法时连 options JSON 都不生成。
    check_actor_options(options)?;
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let options_json = to_cstring(&options.to_json());

    let bytes = unsafe {
        rayrust_sys::ray_actor_create_with_options(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            if options_json.as_bytes().is_empty() {
                std::ptr::null()
            } else {
                options_json.as_ptr()
            },
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_create_with_options", func_name))?;
    // 这个入口建的永远是 C++ actor（holder 的语言固定为 CPP，见 ray_c.cc）。
    actor_registry::record(
        guard.as_slice(),
        ActorLanguage::Cpp,
        actor_registry::CREATION_DESCRIPTOR_CPP,
    );
    Ok(ActorHandle { id: guard.as_slice().to_vec() })
}

/// Create a Python actor with full creation options.
pub(crate) fn actor_create_python_with_options_inner(
    module: &str,
    class: &str,
    args: &[&[u8]],
    options: &ActorOptions,
) -> Result<ActorHandle, RayError> {
    // 先校验再进 FFI：字段组合不合法时连 options JSON 都不生成。
    check_actor_options(options)?;
    let module_c = to_cstring(module);
    let class_c = to_cstring(class);
    let args_arr = build_args_array(args);
    let options_json = to_cstring(&options.to_json());

    let bytes = unsafe {
        rayrust_sys::ray_actor_create_python_with_options(
            module_c.as_ptr(),
            class_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            if options_json.as_bytes().is_empty() {
                std::ptr::null()
            } else {
                options_json.as_ptr()
            },
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail(
            "ray_actor_create_python_with_options",
            format!("{}.{}", module, class),
        ))?;
    actor_registry::record(
        guard.as_slice(),
        ActorLanguage::Python,
        actor_registry::CREATION_DESCRIPTOR_PYTHON,
    );
    Ok(ActorHandle { id: guard.as_slice().to_vec() })
}

/// actor id 的宽度与登记状态，返回登记项（语言码与创建描述符类型）。
///
/// 宽度最先查：`ActorID::FromBinary` 对长度不符的输入 RAY_CHECK 终止进程
/// （`src/ray/common/id.h:430-441`），0 字节的 id 还会变成 nil id。登记表排在宽度
/// 之后，短 id 因此永远先拿到宽度错误。
///
/// 登记表只认本 crate 发出的四类 handle，理由见 `crate::actor_registry`。
fn check_registered_actor(
    entry: &str,
    actor_id: &[u8],
) -> Result<actor_registry::Registration, RayError> {
    if actor_id.len() != actor_registry::ACTOR_ID_LEN {
        return Err(RayError::Ffi(format!(
            "{}: actor id must be {} bytes, got {} bytes",
            entry,
            actor_registry::ACTOR_ID_LEN,
            actor_id.len()
        )));
    }
    actor_registry::registration_of(actor_id).ok_or_else(|| {
        RayError::Ffi(format!(
            "{}: actor {} is not registered in this process; this crate only accepts the \
             handles it handed out (Ray::actor_create, Ray::actor_create_python, \
             Ray::get_actor or ActorHandle::from_serialized), because the core worker's \
             lookup of an id it does not hold ends the process \
             (src/ray/core_worker/actor_manager.cc:52-59)",
            entry,
            hex_encode(actor_id)
        ))
    })
}

/// 登记的语言与入口要求不符时的错误。
fn language_mismatch(entry: &str, actor_id: &[u8], actual: ActorLanguage, hint: &str) -> RayError {
    RayError::Ffi(format!(
        "{}: actor {} is a {} actor; {}",
        entry,
        hex_encode(actor_id),
        actual.name(),
        hint
    ))
}

/// 登记表里有这个 id，但上游给的语言码不是这个 crate 认识的三个。
fn unknown_language(entry: &str, actor_id: &[u8], code: i32) -> RayError {
    RayError::Ffi(format!(
        "{}: actor {} reports language code {}; this crate knows CPP (2), PYTHON (0) and \
         JAVA (1)",
        entry,
        hex_encode(actor_id),
        code
    ))
}

/// `actor_call` 要求的语言：C++ actor。
///
/// `registration` 是 `check_registered_actor` 交出的那一份登记：语言判定直接用
/// 它，不第二次查表。
fn check_cpp_actor_language(
    entry: &str,
    actor_id: &[u8],
    registration: actor_registry::Registration,
) -> Result<(), RayError> {
    match ActorLanguage::from_code(registration.language) {
        Some(ActorLanguage::Cpp) => Ok(()),
        Some(ActorLanguage::Python) => Err(language_mismatch(
            entry,
            actor_id,
            ActorLanguage::Python,
            "this entry submits a C++ method call, use Ray::actor_call_python for a Python \
             actor",
        )),
        Some(ActorLanguage::Java) => Err(language_mismatch(
            entry,
            actor_id,
            ActorLanguage::Java,
            "this crate can neither create nor call Java actors",
        )),
        None => Err(unknown_language(entry, actor_id, registration.language)),
    }
}

/// `actor_call_python` 要求的语言：Python actor，且 handle 带 Python 创建描述符。
///
/// `registration` 是 `check_registered_actor` 交出的那一份登记：语言判定直接用
/// 它，不第二次查表。
fn check_python_actor_language(
    entry: &str,
    actor_id: &[u8],
    registration: actor_registry::Registration,
) -> Result<(), RayError> {
    match ActorLanguage::from_code(registration.language) {
        Some(ActorLanguage::Python) => {
            // 语言对还不够：SDK 的 Python 分支把 handle 的创建描述符按
            // `PythonFunctionDescriptor` 读（cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211，
            // `As` 是 reinterpret_cast），描述符缺席或是另一种类型时读越过对象末尾。
            if registration.creation_descriptor != actor_registry::CREATION_DESCRIPTOR_PYTHON {
                return Err(RayError::Ffi(format!(
                    "{}: actor {} is a Python actor whose handle carries no Python creation \
                     descriptor; the Python call path reads that descriptor from the handle \
                     (cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211) and an empty or \
                     differently typed one makes the driver read past the end of the \
                     descriptor object",
                    entry,
                    hex_encode(actor_id)
                )));
            }
            Ok(())
        }
        Some(ActorLanguage::Cpp) => Err(language_mismatch(
            entry,
            actor_id,
            ActorLanguage::Cpp,
            "this entry submits a Python method call, use Ray::actor_call for a C++ actor",
        )),
        Some(ActorLanguage::Java) => Err(language_mismatch(
            entry,
            actor_id,
            ActorLanguage::Java,
            "this crate can neither create nor call Java actors",
        )),
        None => Err(unknown_language(entry, actor_id, registration.language)),
    }
}

/// Call a method on an actor by binary actor ID and function name.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
pub(crate) fn actor_call_inner(
    actor_id: &[u8],
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<ObjectRef<()>, RayError> {
    let registration = check_registered_actor("ray_actor_call", actor_id)?;
    // 本地模式没有 Python actor（`actor_create_python` 在那里被拒），本地 submitter 又只
    // 执行注册过的 Rust 函数（local_mode_task_submitter.cc:39-40），所以本地模式不做语言
    // 判定；登记照查，旧运行时的 handle 在那里一样是错的。
    if !is_local_mode() {
        check_cpp_actor_language("ray_actor_call", actor_id, registration)?;
    }
    check_is_ref_len(args.len(), is_ref)?;
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_actor_call(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_call", func_name))?;
    Ok(ObjectRef::from_call_id(guard.as_slice().to_vec()))
}

/// `actor_call_async` 与 `actor_call_async_with_args` 的共同主体。
///
/// 代次在创建 future 时取，参数准备失败时在这里返回，方法不会被调用。保活集合被
/// 移进 `spawn_blocking` 的闭包，活到 FFI 调用返回为止。
fn submit_actor_call_async(
    actor_id: &[u8],
    func_name: &str,
    args: TaskArgs,
) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
    let actor_id = actor_id.to_vec();
    let func_name = func_name.to_string();
    let epoch = crate::object_ref::live_epoch();
    async move {
        let (args, is_ref, held) = args.into_parts()?;
        actor_call_inner_async(actor_id, func_name, args, is_ref, held, epoch).await
    }
}

/// Asynchronously call a method on an actor.
///
/// `held` 是参数引用的保活集合：它被移进 `spawn_blocking` 的闭包，活到 FFI 调用
/// 返回为止。
pub(crate) async fn actor_call_inner_async(
    actor_id: Vec<u8>,
    func_name: String,
    args: Vec<Vec<u8>>,
    is_ref: Vec<bool>,
    held: Vec<ObjectRef<()>>,
    epoch: u64,
) -> Result<ObjectRef<()>, RayError> {
    tokio::task::spawn_blocking(move || {
        let _held = held;
        check_epoch(epoch)?;
        let args_ref: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        actor_call_inner(&actor_id, &func_name, &args_ref, &is_ref)
    })
    .await
    .map_err(|e| RayError::Runtime(format!("actor_call join error: {}", e)))?
}

/// Call a method on a Python actor.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
pub(crate) fn actor_call_python_inner(
    actor_id: &[u8],
    method_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<Vec<u8>, RayError> {
    // 本地模式保持既有行为：C 入口在读 actor handle 之前就拒掉整个调用（见 ray_c.h），
    // 所以那里连宽度检查都不在 crate 侧。
    if !is_local_mode() {
        let registration = check_registered_actor("ray_actor_call_python", actor_id)?;
        check_python_actor_language("ray_actor_call_python", actor_id, registration)?;
    }
    check_is_ref_len(args.len(), is_ref)?;
    let method_c = to_cstring(method_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_actor_call_python(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            method_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_call_python", method_name))?;
    Ok(guard.as_slice().to_vec())
}

/// Kill an actor.
pub(crate) fn actor_kill_inner(actor_id: &[u8], no_restart: bool) {
    unsafe {
        rayrust_sys::ray_actor_kill(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            no_restart,
        )
    };
}

/// 序列化一个已登记的 actor handle。
///
/// 登记表在进 SDK 之前查：`ActorManager::GetActorHandle` 对查不到的 id 是 RAY_CHECK，
/// `RAY_CATCH` 拦不住，所以陈旧 handle 只能在这里拒。宽度由 `&ActorHandle` 保证，C 入口
/// 另有一道给 C 调用方。
pub(crate) fn actor_handle_serialize_inner(actor_id: &[u8]) -> Result<Vec<u8>, RayError> {
    check_registered_actor("ray_actor_handle_serialize", actor_id)?;
    let bytes = unsafe {
        rayrust_sys::ray_actor_handle_serialize(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
        )
    };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_handle_serialize", hex_encode(actor_id)))?;
    Ok(guard.as_slice().to_vec())
}

/// 从 handle 字节复原并登记 handle，同时把它的语言与创建描述符类型记进登记表。
pub(crate) fn actor_handle_from_serialized_inner(bytes: &[u8]) -> Result<ActorHandle, RayError> {
    let mut meta = rayrust_sys::RayActorHandleMeta {
        language: 0,
        creation_descriptor: 0,
    };
    let out = unsafe {
        rayrust_sys::ray_actor_handle_deserialize(
            bytes.as_ptr() as *const std::os::raw::c_char,
            bytes.len(),
            &mut meta,
        )
    };
    let guard = CBytesGuard::from(out).ok_or_else(|| ffi_error("ray_actor_handle_deserialize"))?;
    actor_registry::record_codes(guard.as_slice(), meta.language, meta.creation_descriptor);
    Ok(ActorHandle {
        id: guard.as_slice().to_vec(),
    })
}

// ─── Placement Group ──────────────────────────────────────────

/// `PlacementGroupID` 的长度：14 个唯一字节 + 4 字节 JobID
/// （`src/ray/common/id.h:300-306`）。`PlacementGroupID::FromBinary` 对非空且长度
/// 不符的输入 RAY_CHECK 终止进程，所以带 id 的三个入口在进 FFI 之前就按它检查。
const PLACEMENT_GROUP_ID_LEN: usize = 18;

/// 一个 placement group 的查询结果。
///
/// `state` 与 `strategy` 保持上游的整数编码
/// （`cpp/include/ray/api/task_options.h:44-58`）：
/// - `state`：`PENDING` 0、`PREPARED` 1、`CREATED` 2、`REMOVED` 3、
///   `RESCHEDULING` 4、`UNRECOGNIZED` -1；
/// - `strategy`：`PACK` 0、`SPREAD` 1、`STRICT_PACK` 2、`STRICT_SPREAD` 3。
///
/// `state == 3`（`REMOVED`）的条目会一直留在 [`Ray::placement_groups`] 与
/// [`Ray::placement_group_by_id`] 的结果里：GCS 把表行改成 `REMOVED` 之后保留
/// （`src/ray/gcs/gcs_placement_group_manager.cc:458-469`）。只有按名字的查询会在
/// 移除之后报错（名字映射在同一函数前面被删）。
///
/// 没有 `bundles` 字段：上游 `GeneratePlacementGroup` 迭代的是刚建的空 vector
/// （`cpp/src/ray/runtime/abstract_ray_runtime.cc:326-340`），查询回来的组
/// `GetBundles()` 永远是空的，读不到建组时传的 bundle。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementGroupInfo {
    /// 二进制 id，18 字节。
    pub id: Vec<u8>,
    /// 建组时给的名字；没给就是空字符串。
    pub name: String,
    /// `PlacementGroupState` 的整数值。
    pub state: i32,
    /// `PlacementStrategy` 的整数值。
    pub strategy: i32,
}

/// 带 group id 的三个入口的共同前置检查。
fn check_placement_group_id(group_id: &[u8]) -> Result<(), RayError> {
    if group_id.len() != PLACEMENT_GROUP_ID_LEN {
        return Err(RayError::Ffi(format!(
            "placement group id must be {} bytes, got {} bytes",
            PLACEMENT_GROUP_ID_LEN,
            group_id.len()
        )));
    }
    Ok(())
}

/// 解一个组：4 元数组 [id(bin), name(str), state(int), strategy(int)]，与 C 侧的
/// `pack_placement_group` 对应。
fn decode_placement_group(value: &rmpv::Value) -> Result<PlacementGroupInfo, RayError> {
    let fields = value.as_array().ok_or_else(|| {
        RayError::Serialization(format!(
            "a placement group must be a 4 element array, got {:?}",
            value
        ))
    })?;
    if fields.len() != 4 {
        return Err(RayError::Serialization(format!(
            "a placement group must have 4 fields, got {}",
            fields.len()
        )));
    }
    let id = match &fields[0] {
        rmpv::Value::Binary(bytes) => bytes.clone(),
        other => {
            return Err(RayError::Serialization(format!(
                "the placement group id must be msgpack bin, got {:?}",
                other
            )))
        }
    };
    let name = fields[1]
        .as_str()
        .ok_or_else(|| {
            RayError::Serialization(format!(
                "the placement group name must be a UTF-8 msgpack str, got {:?}",
                fields[1]
            ))
        })?
        .to_string();
    Ok(PlacementGroupInfo {
        id,
        name,
        state: decode_placement_group_int(&fields[2], "state")?,
        strategy: decode_placement_group_int(&fields[3], "strategy")?,
    })
}

/// 解一个整数字段，超出 i32 不截断。
fn decode_placement_group_int(value: &rmpv::Value, field: &str) -> Result<i32, RayError> {
    let number = value.as_i64().ok_or_else(|| {
        RayError::Serialization(format!(
            "the placement group {} must be a msgpack integer, got {:?}",
            field, value
        ))
    })?;
    i32::try_from(number).map_err(|_| {
        RayError::Serialization(format!(
            "the placement group {} is {} and does not fit in i32",
            field, number
        ))
    })
}

/// 解 C 侧返回的「组的数组」。
fn decode_placement_groups(bytes: &[u8]) -> Result<Vec<PlacementGroupInfo>, RayError> {
    let value = crate::serialize::deserialize_value(bytes)?;
    let items = value.as_array().ok_or_else(|| {
        RayError::Serialization(format!(
            "expected a msgpack array of placement groups, got {:?}",
            value
        ))
    })?;
    items.iter().map(decode_placement_group).collect()
}

/// Create a placement group.
/// `bundles_json` is a JSON array like `[{"CPU":1},{"CPU":1}]`.
/// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
pub(crate) fn placement_group_create_inner(
    name: &str,
    bundles_json: &str,
    strategy: i32,
) -> Result<Vec<u8>, RayError> {
    let name_c = to_cstring(name);
    let json_c = to_cstring(bundles_json);
    let bytes = unsafe {
        rayrust_sys::ray_placement_group_create(
            name_c.as_ptr(),
            json_c.as_ptr(),
            strategy,
        )
    };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error("ray_placement_group_create"))?;
    Ok(guard.as_slice().to_vec())
}

/// 移除一个 placement group。
pub(crate) fn placement_group_remove_inner(group_id: &[u8]) -> Result<(), RayError> {
    check_placement_group_id(group_id)?;
    let ret = unsafe {
        rayrust_sys::ray_placement_group_remove(
            group_id.as_ptr() as *const std::os::raw::c_char,
            group_id.len(),
        )
    };
    if ret != 0 {
        return Err(ffi_error_detail(
            "ray_placement_group_remove",
            hex_encode(group_id),
        ));
    }
    Ok(())
}

/// 等一个 placement group 就绪。
pub(crate) fn placement_group_wait_ready_inner(
    group_id: &[u8],
    timeout_seconds: i32,
) -> Result<bool, RayError> {
    check_placement_group_id(group_id)?;
    let ret = unsafe {
        rayrust_sys::ray_placement_group_wait_ready(
            group_id.as_ptr() as *const std::os::raw::c_char,
            group_id.len(),
            timeout_seconds as c_int,
        )
    };
    match ret {
        1 => Ok(true),
        0 => Ok(false),
        _ => Err(ffi_error_detail(
            "ray_placement_group_wait_ready",
            hex_encode(group_id),
        )),
    }
}

/// 列出集群里的全部 placement group。
pub(crate) fn placement_groups_inner() -> Result<Vec<PlacementGroupInfo>, RayError> {
    let bytes = unsafe { rayrust_sys::ray_placement_group_get_all() };
    let guard = CBytesGuard::from(bytes).ok_or_else(|| ffi_error("ray_placement_group_get_all"))?;
    decode_placement_groups(guard.as_slice())
}

/// 按二进制 id 查一个 placement group。
pub(crate) fn placement_group_by_id_inner(group_id: &[u8]) -> Result<PlacementGroupInfo, RayError> {
    check_placement_group_id(group_id)?;
    let bytes = unsafe {
        rayrust_sys::ray_placement_group_get_by_id(
            group_id.as_ptr() as *const std::os::raw::c_char,
            group_id.len(),
        )
    };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_placement_group_get_by_id", hex_encode(group_id)))?;
    unwrap_single_group(decode_placement_groups(guard.as_slice())?, || {
        format!("placement group not found for id {}", hex_encode(group_id))
    })
}

/// 按名字查一个 placement group。
pub(crate) fn placement_group_by_name_inner(name: &str) -> Result<PlacementGroupInfo, RayError> {
    let name_c = to_cstring(name);
    let bytes = unsafe { rayrust_sys::ray_placement_group_get_by_name(name_c.as_ptr()) };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_placement_group_get_by_name", name))?;
    unwrap_single_group(decode_placement_groups(guard.as_slice())?, || {
        format!("placement group not found for name {:?}", name)
    })
}

/// 按 id/按名字的查询返回 0 或 1 个组：0 个就是查不到（上游返回空组，这里必须报错，
/// 静默成功就是丢信息）；多于 1 个是本仓库自己的打包出了错。
fn unwrap_single_group(
    mut groups: Vec<PlacementGroupInfo>,
    not_found: impl FnOnce() -> String,
) -> Result<PlacementGroupInfo, RayError> {
    match groups.len() {
        1 => Ok(groups.remove(0)),
        0 => Err(RayError::Ffi(not_found())),
        n => Err(RayError::Serialization(format!(
            "a placement group lookup returned {} groups instead of at most 1",
            n
        ))),
    }
}

// ─── Misc ─────────────────────────────────────────────────────

/// Returns true if the current actor was restarted.
pub(crate) fn was_current_actor_restarted() -> bool {
    unsafe { rayrust_sys::ray_was_current_actor_restarted() }
}

/// 这个进程是不是以本地模式初始化的。
pub(crate) fn is_local_mode() -> bool {
    unsafe { rayrust_sys::ray_is_local_mode() }
}

/// Get the namespace of this job.
pub(crate) fn get_namespace() -> Result<String, RayError> {
    if !is_initialized() {
        return Ok(String::new());
    }
    let bytes = unsafe { rayrust_sys::ray_get_namespace() };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error("ray_get_namespace"))?;
    String::from_utf8(guard.as_slice().to_vec())
        .map_err(|e| RayError::Ffi(format!("namespace not valid UTF-8: {}", e)))
}

/// Get a named actor by name.
/// `namespace` can be empty string for current namespace.
/// Returns Ok(Some(handle)) if found, Ok(None) if not found.
pub(crate) fn get_actor(name: &str, namespace: &str) -> Result<Option<ActorHandle>, RayError> {
    let name_c = to_cstring(name);
    let ns_c = to_cstring(namespace);
    rayrust_sys::clear_error();
    let mut meta = rayrust_sys::RayActorHandleMeta {
        language: 0,
        creation_descriptor: 0,
    };
    let bytes = unsafe { rayrust_sys::ray_get_actor(name_c.as_ptr(), ns_c.as_ptr(), &mut meta) };
    let guard = CBytesGuard::from(bytes);
    match guard {
        Some(g) => {
            let id = g.as_slice().to_vec();
            if id.is_empty() {
                Ok(None)
            } else {
                // 语言与创建描述符类型由 C 侧从 handle 的序列化字节里读出来
                // （本地模式是 C++ / cpp）。
                actor_registry::record_codes(&id, meta.language, meta.creation_descriptor);
                Ok(Some(ActorHandle { id }))
            }
        }
        None => {
            // C returns null both for "not found" and "error".
            // Check last_error to distinguish.
            if rayrust_sys::last_error().is_some() {
                return Err(ffi_error("ray_get_actor"));
            }
            Ok(None)
        }
    }
}

/// Cancel a remote task by object ID.
/// `obj_id` must be exactly 28 bytes; the C side rejects any other length.
/// `force_kill` kills the worker process if true.
/// `recursive` cancels dependent tasks.
/// Local mode rejects with [`RayError::Ffi`]: there is no core worker to take the
/// `CancelTask` call, and no handle to withdraw a task once it is submitted.
pub(crate) fn cancel(obj_id: &[u8], force_kill: bool, recursive: bool) -> Result<(), RayError> {
    let ret = unsafe {
        rayrust_sys::ray_cancel(
            obj_id.as_ptr() as *const std::os::raw::c_char,
            obj_id.len(),
            force_kill,
            recursive,
        )
    };
    if ret != 0 {
        return Err(ffi_error("ray_cancel"));
    }
    Ok(())
}

/// `ray_get_many` 分配的 `ray_bytes_t` 数组的守卫。
///
/// C 侧先把数组与每个元素都填好才返回，因此数组与全部 buffer 必须一起释放，
/// 包括解码到一半失败的那条路径。
struct CBytesArrayGuard {
    array: *mut RayBytes,
    count: usize,
}

impl CBytesArrayGuard {
    fn from_raw(array: *mut RayBytes, count: usize) -> Result<Self, RayError> {
        if array.is_null() {
            Err(ffi_error("ray_get_many"))
        } else {
            Ok(CBytesArrayGuard { array, count })
        }
    }

    fn get(&self, index: usize) -> &RayBytes {
        debug_assert!(index < self.count, "index {} outside the array", index);
        unsafe { &*self.array.add(index) }
    }
}

impl Drop for CBytesArrayGuard {
    fn drop(&mut self) {
        unsafe { rayrust_sys::ray_free_bytes_array(self.array, self.count) }
    }
}

/// 批量取，超过 `timeout_ms` 毫秒就返回。
///
/// `timeout_ms` 是 `-1`（永久等待）或非负的毫秒数，其他值返回 `RayError::Ffi`。
pub(crate) fn get_many_with_timeout<T: serde::de::DeserializeOwned>(
    refs: &[ObjectRef<T>],
    timeout_ms: i32,
) -> Result<Vec<T>, RayError> {
    for obj_ref in refs {
        check_epoch(obj_ref.epoch)?;
    }
    validate_timeout(timeout_ms)?;

    // 空 slice 的 as_ptr() 得到的是非空却不指向任何元素的指针，交给 C 侧做 0
    // 个元素的 malloc 没有意义。
    if refs.is_empty() {
        return Ok(Vec::new());
    }

    let ids: Vec<RayBytes> = refs.iter().map(|r| RayBytes {
        data: r.id().as_ptr() as *const std::os::raw::c_char,
        len: r.id().len(),
    }).collect();

    let array = unsafe { rayrust_sys::ray_get_many(ids.as_ptr(), ids.len(), timeout_ms) };
    let array = CBytesArrayGuard::from_raw(array, refs.len())?;

    let mut results = Vec::with_capacity(refs.len());
    for (i, obj_ref) in refs.iter().enumerate() {
        let bytes = array.get(i);
        if bytes.data.is_null() {
            return Err(RayError::ObjectNotFound(format!("object {} not found", i)));
        }
        let slice = unsafe { std::slice::from_raw_parts(bytes.data as *const u8, bytes.len) };
        let val = if obj_ref.is_xlang {
            crate::serialize::deserialize_xlang(slice)?
        } else {
            crate::serialize::deserialize(slice)?
        };
        results.push(val);
    }
    Ok(results)
}

/// Get multiple objects from the object store.
/// Blocks until all objects are available.
pub(crate) fn get_many<T: serde::de::DeserializeOwned>(refs: &[ObjectRef<T>]) -> Result<Vec<T>, RayError> {
    get_many_with_timeout(refs, -1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `is_ref` 长度与参数个数对不上时必须拒绝，不进入 FFI。
    fn assert_len_rejected<T>(label: &str, result: Result<T, RayError>) {
        match result {
            Err(RayError::Ffi(msg)) => assert!(
                msg.contains("is_ref has 1 entries but there are 2 arguments"),
                "{} 的消息看不出长度对不上: {}",
                label,
                msg
            ),
            Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
            Ok(_) => panic!("{}：长度对不上却提交了", label),
        }
    }

    #[test]
    fn every_entry_point_rejects_a_mismatched_is_ref() {
        let arg = [0u8; 4];
        let args: [&[u8]; 2] = [&arg, &arg];
        let flags = [true];
        // actor 入口的登记表与语言判定都排在 `is_ref` 长度检查之前，所以这里的 id 先用
        // 各自入口要求的语言登记（含 Python 创建描述符）：没登记的 id 会在长度检查之前
        // 被拒。
        let cpp_actor = [0x21u8; 16];
        let python_actor = [0x22u8; 16];
        actor_registry::record(
            &cpp_actor,
            ActorLanguage::Cpp,
            actor_registry::CREATION_DESCRIPTOR_CPP,
        );
        actor_registry::record(
            &python_actor,
            ActorLanguage::Python,
            actor_registry::CREATION_DESCRIPTOR_PYTHON,
        );

        assert_len_rejected(
            "task_call_inner",
            task_call_inner("add", &args, &flags, &TaskOptions::new()),
        );
        assert_len_rejected(
            "actor_call_inner",
            actor_call_inner(&cpp_actor, "counter::increment", &args, &flags),
        );
        assert_len_rejected(
            "task_call_python_inner",
            task_call_python_inner("m", "f", &args, &flags),
        );
        assert_len_rejected(
            "actor_call_python_inner",
            actor_call_python_inner(&python_actor, "increment", &args, &flags),
        );
    }

    /// 登记表里的语言码不是这个 crate 认识的三个时，两个入口都报出实际取值。
    #[test]
    fn an_unknown_language_code_is_reported_by_both_entries() {
        let actor = [0x23u8; 16];
        actor_registry::record_codes(&actor, 7, actor_registry::CREATION_DESCRIPTOR_PYTHON);
        let registration = actor_registry::registration_of(&actor).expect("the id was registered");
        for (entry, result) in [
            (
                "ray_actor_call",
                check_cpp_actor_language("ray_actor_call", &actor, registration),
            ),
            (
                "ray_actor_call_python",
                check_python_actor_language("ray_actor_call_python", &actor, registration),
            ),
        ] {
            match result {
                Err(RayError::Ffi(msg)) => assert!(
                    msg.contains("reports language code 7"),
                    "{} 的错误里没有实际语言码: {}",
                    entry,
                    msg
                ),
                Err(other) => panic!("{} 返回了别的错误变体: {}", entry, other),
                Ok(()) => panic!("{} 收下了语言码 7", entry),
            }
        }
    }

    /// 同一个 id 重复登记时保留首次记录：一个语言字段说谎的载荷改变不了已经发出去的
    /// handle 被哪些入口接受（上游 `ActorManager::AddActorHandle` 的 `emplace` 同样如此）。
    #[test]
    fn a_second_registration_keeps_the_first_record() {
        let actor = [0x24u8; 16];
        actor_registry::record(
            &actor,
            ActorLanguage::Cpp,
            actor_registry::CREATION_DESCRIPTOR_CPP,
        );
        actor_registry::record(
            &actor,
            ActorLanguage::Python,
            actor_registry::CREATION_DESCRIPTOR_PYTHON,
        );
        let registration = actor_registry::registration_of(&actor).expect("the id was registered");
        assert_eq!(registration.language, ActorLanguage::Cpp.code());
        assert_eq!(
            registration.creation_descriptor,
            actor_registry::CREATION_DESCRIPTOR_CPP
        );
    }

    /// Python 入口要求 handle 带 Python 创建描述符：字段 6 缺席的载荷按 PYTHON 登记，
    /// 那条路径会读越过描述符对象，所以在调用门前拒掉。
    #[test]
    fn actor_call_python_requires_a_python_creation_descriptor() {
        let actor = [0x25u8; 16];
        actor_registry::record(&actor, ActorLanguage::Python, 0);
        let registration = actor_registry::registration_of(&actor).expect("the id was registered");
        match check_python_actor_language("ray_actor_call_python", &actor, registration) {
            Err(RayError::Ffi(msg)) => assert!(
                msg.contains("no Python creation descriptor"),
                "错误里没有说明缺描述符: {}",
                msg
            ),
            Err(other) => panic!("返回了别的错误变体: {}", other),
            Ok(()) => panic!("缺少 Python 创建描述符的 handle 被接受了"),
        }
    }

    /// Java handle：两个入口都拒，消息里点名 Java。语言码 1 只可能来自 handle 的
    /// 序列化字节（[`ActorHandle::from_serialized`] 与 [`crate::Ray::get_actor`]），
    /// crate 自己建不出 Java actor。
    #[test]
    fn a_java_handle_is_refused_by_both_entries() {
        let actor = [0x26u8; 16];
        actor_registry::record_codes(&actor, 1, 1);
        let registration = actor_registry::registration_of(&actor).expect("the id was registered");
        for (entry, result) in [
            (
                "ray_actor_call",
                check_cpp_actor_language("ray_actor_call", &actor, registration),
            ),
            (
                "ray_actor_call_python",
                check_python_actor_language("ray_actor_call_python", &actor, registration),
            ),
        ] {
            match result {
                Err(RayError::Ffi(msg)) => {
                    assert!(
                        msg.contains("is a Java actor"),
                        "{} 的错误里没有点出 Java: {}",
                        entry,
                        msg
                    );
                    assert!(
                        msg.contains("can neither create nor call Java actors"),
                        "{} 的错误没有说明这个 crate 不支持 Java: {}",
                        entry,
                        msg
                    );
                }
                Err(other) => panic!("{} 返回了别的错误变体: {}", entry, other),
                Ok(()) => panic!("{} 收下了一个 Java handle", entry),
            }
        }
    }

    #[test]
    fn task_options_to_json_empty_is_empty_string() {
        assert_eq!(TaskOptions::new().to_json(), "");
        assert_eq!(TaskOptions::default().to_json(), "");
    }

    #[test]
    fn task_options_to_json_name_only() {
        let opts = TaskOptions::new().name("my_task");
        assert_eq!(opts.to_json(), r#"{"name":"my_task"}"#);
    }

    #[test]
    fn task_options_to_json_resources_only() {
        let opts = TaskOptions::new().resource("CPU", 1.0).resource("GPU", 0.5);
        assert_eq!(opts.to_json(), r#"{"resources":{"CPU":1,"GPU":0.5}}"#);
    }

    #[test]
    fn task_options_to_json_placement_group_with_bundle_index() {
        let id = vec![0x0a, 0x1b, 0x2c];
        let opts = TaskOptions::new().placement_group(id.clone(), 1);
        assert_eq!(
            opts.to_json(),
            format!(
                "{{\"placement_group_id\":\"{}\",\"bundle_index\":1}}",
                hex_encode(&id)
            )
        );
    }

    #[test]
    fn task_options_to_json_placement_group_without_bundle_index() {
        // 两个构造入口的默认 bundle_index 都是 -1，未显式指定时照它输出。
        let id = vec![0xffu8; 4];
        let opts = TaskOptions::new().placement_group(id.clone(), -1);
        assert_eq!(
            opts.to_json(),
            format!(
                "{{\"placement_group_id\":\"{}\",\"bundle_index\":-1}}",
                hex_encode(&id)
            )
        );
    }

    #[test]
    fn task_options_default_carries_bundle_index_minus_one() {
        assert_eq!(TaskOptions::default().bundle_index, -1);
        assert_eq!(TaskOptions::new().bundle_index, -1);
        // 结构体更新语法不留缝：只给 group 时 bundle_index 也是 -1。
        let id = vec![0x07u8; 18];
        let opts = TaskOptions {
            placement_group_id: id.clone(),
            ..Default::default()
        };
        assert_eq!(opts.bundle_index, -1);
        assert_eq!(
            opts.to_json(),
            format!(
                "{{\"placement_group_id\":\"{}\",\"bundle_index\":-1}}",
                hex_encode(&id)
            )
        );
    }

    #[test]
    fn task_options_to_json_runtime_env_is_a_string_value() {
        let opts = TaskOptions::new().runtime_env(r#"{"env_vars":{"FOO":"bar"}}"#);
        assert_eq!(
            opts.to_json(),
            r#"{"runtime_env":"{\"env_vars\":{\"FOO\":\"bar\"}}"}"#
        );
    }

    #[test]
    fn task_options_to_json_escapes_runtime_env() {
        // 环境变量值里的引号、反斜杠与换行必须按 JSON 字符串转义，而不是把内容
        // 原样嵌进外层文档。
        let env = "line1\n\"q\" \\ end";
        let opts = TaskOptions::new().runtime_env(env);
        assert_eq!(
            opts.to_json(),
            "{\"runtime_env\":\"line1\\n\\\"q\\\" \\\\ end\"}"
        );
    }

    #[test]
    fn task_options_to_json_runtime_env_and_resources_without_a_group() {
        let opts = TaskOptions::new()
            .resource("CPU", 1.0)
            .runtime_env(r#"{"env_vars":{"FOO":"bar"}}"#);
        assert_eq!(
            opts.to_json(),
            r#"{"resources":{"CPU":1},"runtime_env":"{\"env_vars\":{\"FOO\":\"bar\"}}"}"#
        );
    }

    #[test]
    fn task_options_to_json_empty_strings_clear_set_fields() {
        let opts = TaskOptions::new().name("x").name("");
        assert_eq!(opts.to_json(), "");
        let opts = TaskOptions::new()
            .runtime_env(r#"{"env_vars":{"FOO":"bar"}}"#)
            .runtime_env("");
        assert_eq!(opts.to_json(), "");
    }

    #[test]
    fn task_options_to_json_keeps_runtime_env_inside_its_string() {
        // 注入串只能留在 runtime_env 的字符串值里，外层不会多出第二个 name 键。
        let opts = TaskOptions::new()
            .name("intended")
            .runtime_env(r#"{"env_vars":{}},"name":"hijack""#);
        assert_eq!(
            opts.to_json(),
            "{\"name\":\"intended\",\"runtime_env\":\"{\\\"env_vars\\\":{}},\\\"name\\\":\\\"hijack\\\"\"}"
        );
    }

    #[test]
    fn task_options_standalone_bundle_index_is_rejected() {
        let opts = TaskOptions {
            bundle_index: 0,
            ..Default::default()
        };
        match check_task_options(&opts) {
            Err(RayError::Ffi(msg)) => assert!(
                msg.contains("bundle_index") && msg.contains("placement_group_id"),
                "错误文本没有指出缺 placement_group_id: {}",
                msg
            ),
            Err(other) => panic!("孤立 bundle_index 返回了别的错误变体: {}", other),
            Ok(()) => panic!("孤立 bundle_index 通过了校验，字段会被静默丢掉"),
        }
    }

    #[test]
    fn task_options_default_bundle_index_without_group_is_accepted() {
        assert!(check_task_options(&TaskOptions::default()).is_ok());
        assert!(check_task_options(&TaskOptions::new()).is_ok());
    }

    #[test]
    fn task_options_bundle_index_with_group_is_accepted() {
        let opts = TaskOptions::new().placement_group(vec![0x01; 18], 0);
        assert!(check_task_options(&opts).is_ok());
        let opts = TaskOptions::new().placement_group(vec![0x01; 18], -1);
        assert!(check_task_options(&opts).is_ok());
    }

    #[test]
    fn task_options_to_json_combined_fields() {
        let id = vec![0x01, 0x02];
        let opts = TaskOptions::new()
            .name("combined")
            .resource("CPU", 2.0)
            .placement_group(id.clone(), 0)
            .runtime_env(r#"{"pip":["numpy"]}"#);
        assert_eq!(
            opts.to_json(),
            format!(
                "{{\"name\":\"combined\",\"resources\":{{\"CPU\":2}},\
                 \"placement_group_id\":\"{}\",\"bundle_index\":0,\
                 \"runtime_env\":\"{{\\\"pip\\\":[\\\"numpy\\\"]}}\"}}",
                hex_encode(&id)
            )
        );
    }

    #[test]
    fn task_options_to_json_escapes_name() {
        let opts = TaskOptions::new().name("quote\"back\\slash\n");
        assert_eq!(opts.to_json(), "{\"name\":\"quote\\\"back\\\\slash\\n\"}");
    }

    // ── ActorOptions ─────────────────────────────────────

    #[test]
    fn actor_options_default_carries_bundle_index_minus_one() {
        assert_eq!(ActorOptions::default().bundle_index, -1);
        assert_eq!(ActorOptions::new().bundle_index, -1);
        // max_concurrency 默认 1：派生实现给 0。0 在 2.51.1 里等价于 1，靠的是
        // NeedDefaultExecutor 在 0 时提前返回、队列就地串行执行；SDK 自己的
        // RAY_CHECK(max_concurrency > 0) 说明它不是受支持的并发值（#29）。
        assert_eq!(ActorOptions::default().max_concurrency, 1);
        assert_eq!(ActorOptions::new().max_concurrency, 1);
        // 结构体更新语法不留缝：只给 group 时 bundle_index 也是 -1。
        let opts = ActorOptions {
            placement_group_id: vec![0x07u8; 18],
            ..Default::default()
        };
        assert_eq!(opts.bundle_index, -1);
    }

    #[test]
    fn actor_options_standalone_bundle_index_is_rejected() {
        let opts = ActorOptions {
            bundle_index: 2,
            ..Default::default()
        };
        match check_actor_options(&opts) {
            Err(RayError::Ffi(msg)) => assert!(
                msg.contains("bundle_index")
                    && msg.contains("placement_group_id")
                    && msg.contains('2'),
                "错误文本没有指出字段与实际值: {}",
                msg
            ),
            Err(other) => panic!("孤立 bundle_index 返回了别的错误变体: {}", other),
            Ok(()) => panic!("孤立 bundle_index 通过了校验，字段会被静默丢掉"),
        }
    }

    #[test]
    fn actor_options_default_bundle_index_without_group_is_accepted() {
        assert!(check_actor_options(&ActorOptions::default()).is_ok());
        assert!(check_actor_options(&ActorOptions::new()).is_ok());
    }

    #[test]
    fn actor_options_bundle_index_with_group_is_accepted() {
        for bundle_index in [-1, 0, 2] {
            let opts = ActorOptions::new().placement_group(vec![0x01; 18], bundle_index);
            assert!(
                check_actor_options(&opts).is_ok(),
                "组 + bundle_index {} 被拒了",
                bundle_index
            );
        }
    }

    #[test]
    fn actor_options_to_json_empty_is_empty_string() {
        assert_eq!(ActorOptions::new().to_json(), "");
        assert_eq!(ActorOptions::default().to_json(), "");
    }

    #[test]
    fn actor_options_to_json_name_only() {
        let opts = ActorOptions::new().name("my_actor");
        assert_eq!(opts.to_json(), r#"{"name":"my_actor"}"#);
    }

    #[test]
    fn actor_options_to_json_placement_group_with_bundle_index() {
        let id = vec![0x0a, 0x1b, 0x2c];
        let opts = ActorOptions::new().placement_group(id.clone(), 2);
        assert_eq!(
            opts.to_json(),
            format!(
                "{{\"placement_group_id\":\"{}\",\"bundle_index\":2}}",
                hex_encode(&id)
            )
        );
    }

    #[test]
    fn actor_options_to_json_placement_group_with_default_bundle_index() {
        // 默认 bundle_index 是 -1，未显式指定时照它输出：两个键都在。
        let id = vec![0xffu8; 18];
        let opts = ActorOptions::new().placement_group(id.clone(), -1);
        assert_eq!(
            opts.to_json(),
            format!(
                "{{\"placement_group_id\":\"{}\",\"bundle_index\":-1}}",
                hex_encode(&id)
            )
        );
    }

    #[test]
    fn actor_options_to_json_skips_fields_at_their_defaults() {
        // max_restarts 默认 0、max_concurrency 默认 1，与默认值相同时不出现。
        let opts = ActorOptions::new().max_restarts(0).max_concurrency(1);
        assert_eq!(opts.to_json(), "");
        let opts = ActorOptions::new().max_restarts(3).max_concurrency(10);
        assert_eq!(opts.to_json(), r#"{"max_restarts":3,"max_concurrency":10}"#);
    }

    // ── placement group 查询结果的解码 ───────────────────

    /// 构造一个与 C 侧 `pack_placement_group` 同形的 msgpack 组：
    /// [id(bin), name(str), state(int), strategy(int)]。
    fn group_value(id: &[u8], name: &str, state: i64, strategy: i64) -> rmpv::Value {
        rmpv::Value::Array(vec![
            rmpv::Value::Binary(id.to_vec()),
            rmpv::Value::String(name.into()),
            rmpv::Value::Integer(state.into()),
            rmpv::Value::Integer(strategy.into()),
        ])
    }

    fn decode(groups: Vec<rmpv::Value>) -> Result<Vec<PlacementGroupInfo>, RayError> {
        let bytes = crate::serialize::serialize(&rmpv::Value::Array(groups))?;
        decode_placement_groups(&bytes)
    }

    #[test]
    fn decode_placement_groups_reads_every_field() {
        let first_id = vec![0x01u8; 18];
        let second_id: Vec<u8> = (0..18u8).collect();
        let groups = decode(vec![
            group_value(&first_id, "named", 2, 3),
            group_value(&second_id, "", 0, 0),
        ])
        .expect("两个组的数组");
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0],
            PlacementGroupInfo {
                id: first_id,
                name: "named".to_string(),
                state: 2,
                strategy: 3,
            }
        );
        // 建组时没给名字就是空串，与 C 侧的 pack_str(0) 对应。
        assert_eq!(
            groups[1],
            PlacementGroupInfo {
                id: second_id,
                name: String::new(),
                state: 0,
                strategy: 0,
            }
        );
    }

    #[test]
    fn decode_placement_groups_keeps_negative_state() {
        // UNRECOGNIZED 是 -1，不能当成解码失败。
        let groups = decode(vec![group_value(&[0x02u8; 18], "x", -1, -1)]).expect("一个组");
        assert_eq!((groups[0].state, groups[0].strategy), (-1, -1));
    }

    #[test]
    fn decode_placement_groups_rejects_a_short_array() {
        let value = rmpv::Value::Array(vec![
            rmpv::Value::Binary(vec![0u8; 18]),
            rmpv::Value::String("x".into()),
            rmpv::Value::Integer(2.into()),
        ]);
        match decode(vec![value]) {
            Err(RayError::Serialization(message)) => assert!(
                message.contains("4 fields"),
                "错误文本没有指出字段数: {}",
                message
            ),
            other => panic!("三字段的组返回了 {:?}", other),
        }
    }

    #[test]
    fn decode_placement_groups_rejects_a_textual_id() {
        let mut value = group_value(&[0u8; 18], "x", 2, 0);
        if let rmpv::Value::Array(fields) = &mut value {
            fields[0] = rmpv::Value::String("not a bin".into());
        }
        match decode(vec![value]) {
            Err(RayError::Serialization(message)) => assert!(
                message.contains("bin"),
                "错误文本没有指出 id 的 msgpack 类型: {}",
                message
            ),
            other => panic!("文本 id 的组返回了 {:?}", other),
        }
    }

    #[test]
    fn decode_placement_groups_rejects_a_state_outside_i32() {
        let value = group_value(&[0u8; 18], "x", i64::from(i32::MAX) + 1, 0);
        match decode(vec![value]) {
            Err(RayError::Serialization(message)) => assert!(
                message.contains("does not fit in i32"),
                "错误文本没有指出溢出: {}",
                message
            ),
            other => panic!("state 溢出的组返回了 {:?}", other),
        }
    }

    #[test]
    fn decode_placement_groups_rejects_a_non_array_payload() {
        let bytes = crate::serialize::serialize(&rmpv::Value::Nil).unwrap();
        match decode_placement_groups(&bytes) {
            Err(RayError::Serialization(message)) => assert!(
                message.contains("msgpack array"),
                "错误文本没有指出载荷形状: {}",
                message
            ),
            other => panic!("非数组载荷返回了 {:?}", other),
        }
    }

    #[test]
    fn unwrap_single_group_reports_a_missing_group() {
        let key = vec![0x0au8; 18];
        match unwrap_single_group(Vec::new(), || {
            format!("placement group not found for id {}", hex_encode(&key))
        }) {
            Err(RayError::Ffi(message)) => assert!(
                message.contains(&hex_encode(&key)),
                "错误文本没有带上查询键: {}",
                message
            ),
            other => panic!("空数组返回了 {:?}", other),
        }
        let two = decode(vec![
            group_value(&[0u8; 18], "a", 2, 0),
            group_value(&[1u8; 18], "b", 2, 0),
        ])
        .expect("两个组");
        match unwrap_single_group(two, || "not found".to_string()) {
            Err(RayError::Serialization(message)) => assert!(
                message.contains("2 groups"),
                "错误文本没有指出组数: {}",
                message
            ),
            other => panic!("两个组返回了 {:?}", other),
        }
    }

    #[test]
    fn check_placement_group_id_only_accepts_18_bytes() {
        assert!(check_placement_group_id(&[0u8; 18]).is_ok());
        for len in [0usize, 1, 17, 19, 28] {
            match check_placement_group_id(&vec![0u8; len]) {
                Err(RayError::Ffi(message)) => assert!(
                    message.contains("18 bytes") && message.contains(&len.to_string()),
                    "{} 字节的长度错误文本不对: {}",
                    len,
                    message
                ),
                other => panic!("{} 字节的 id 返回了 {:?}", len, other),
            }
        }
    }

    #[test]
    fn placement_group_remove_rejects_a_wrong_width_before_the_ffi_boundary() {
        for len in [0usize, 3, 17, 19] {
            match placement_group_remove_inner(&vec![0u8; len]) {
                // 这条文本来自 crate 内侧的检查，没有入口名前缀；进了 FFI 会拿到
                // C 入口的 `ray_placement_group_remove: ...`。
                Err(RayError::Ffi(message)) => assert_eq!(
                    message,
                    format!("placement group id must be 18 bytes, got {} bytes", len),
                    "{} 字节的 remove 错误文本不对",
                    len
                ),
                other => panic!("{} 字节的 id 返回了 {:?}", len, other),
            }
        }
    }

    #[test]
    fn placement_group_remove_reports_the_c_entry_error() {
        // 单元测试进程不 connect，所以进程里没有 Ray runtime：C 入口在
        // `GetRayRuntime()` 为空时返回 -1，错误经 Result 报出来，消息由 C 侧写入。
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        match placement_group_remove_inner(&[0u8; 18]) {
            Err(RayError::Ffi(message)) => assert!(
                message.contains("ray_placement_group_remove")
                    && message.contains("Ray runtime is not initialized"),
                "错误文本没有带上入口名与原因: {}",
                message
            ),
            other => panic!("没有 runtime 时返回了 {:?}", other),
        }
    }

    /// 手工拼 28 字节的 id 直接调 C 入口。这些用例不 connect，进程里没有 runtime，
    /// 所以只要前面几条参数检查放过去，错误就一定是 runtime 判空那条。
    fn wait_with_hand_built_ids(ids: &[[u8; 28]], num_objects: usize) -> String {
        let bytes: Vec<rayrust_sys::RayBytes> = ids
            .iter()
            .map(|id| rayrust_sys::RayBytes {
                data: id.as_ptr() as *const std::os::raw::c_char,
                len: id.len(),
            })
            .collect();
        rayrust_sys::clear_error();
        let result = unsafe { rayrust_sys::ray_wait(bytes.as_ptr(), bytes.len(), num_objects, 20) };
        assert!(
            result.is_null(),
            "{} 个 id、num_objects = {} 时 ray_wait 没有返回空指针",
            ids.len(),
            num_objects
        );
        rayrust_sys::last_error().expect("C 入口没有记录错误")
    }

    #[test]
    fn wait_rejects_a_count_above_the_id_count() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let message = wait_with_hand_built_ids(&[[0x11u8; 28]], 2);
        assert!(
            message
                .contains("Number of objects to wait for must be between 1 and the number of ids."),
            "错误文本不是数量那一句: {}",
            message
        );
    }

    #[test]
    fn wait_rejects_a_zero_count() {
        let message = wait_with_hand_built_ids(&[[0x12u8; 28]], 0);
        assert!(
            message
                .contains("Number of objects to wait for must be between 1 and the number of ids."),
            "错误文本不是数量那一句: {}",
            message
        );
    }

    #[test]
    fn wait_rejects_duplicate_ids() {
        let id = [0x13u8; 28];
        let message = wait_with_hand_built_ids(&[id, id], 2);
        assert!(
            message.contains("Duplicate object IDs not supported in wait."),
            "错误文本不是重复 id 那一句: {}",
            message
        );
    }

    #[test]
    fn wait_lets_a_valid_count_and_distinct_ids_through_to_the_runtime_check() {
        // 对照：参数合法时前面几条不拦，错误来自 runtime 判空。
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let message = wait_with_hand_built_ids(&[[0x14u8; 28], [0x15u8; 28]], 2);
        assert!(
            message.contains("Ray runtime is not initialized"),
            "参数合法时错误不是 runtime 判空那条: {}",
            message
        );
    }

    /// 直调 C 入口提交一个地址，其余字段全不传。被拒的调用在进 SDK 之前就返回，
    /// 进程里不会建出运行时。
    fn ray_init_with_address(address: &str, local_mode: bool) -> i32 {
        let address_c = to_cstring(address);
        rayrust_sys::clear_error();
        unsafe {
            rayrust_sys::ray_init(
                address_c.as_ptr(),
                local_mode as i32,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                0,
                std::ptr::null(),
            )
        }
    }

    /// 集群模式下不合规的地址在进 SDK 之前被拒：返回 -1，消息点名 `address` 与该形态的
    /// 判别子串。这些形状在上游的结局不同：`ParseAddress` 解析不出来的停在
    /// `config_internal.cc:255`，端口为空或不是十进制数字的停在 `:261`，端口超出 65535
    /// 或带尾缀被 `from_chars` 截断的约 35 秒后停在 `process_helper.cc:110`，负端口停在
    /// `util.cc:42`，空 host 让上游把 bootstrap 地址当成没设置、转头去起 head 节点。
    #[test]
    fn ray_init_rejects_an_address_that_is_not_host_port() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let cases: &[(&str, &str)] = &[
            ("local", "no ':' separator"),
            ("127.0.0.1", "no ':' separator"),
            ("::1", "an IPv6 host must be bracketed"),
            ("[::1", "an IPv6 host must be bracketed"),
            ("[::1]", "an IPv6 host must be bracketed"),
            ("[::1]junk:6379", "an IPv6 host must be bracketed"),
            (":6399", "the host is empty"),
            ("127.0.0.1:", "the port is empty"),
            ("127.0.0.1:abc", "the port must be decimal digits"),
            ("127.0.0.1:-1", "the port must be decimal digits"),
            ("127.0.0.1:+6379", "the port must be decimal digits"),
            ("127.0.0.1:6399abc", "the port must be decimal digits"),
            ("127.0.0.1:  6399", "the port must be decimal digits"),
            ("127.0.0.1:65536", "the port must be 0..=65535"),
            ("127.0.0.1:65536000", "the port must be 0..=65535"),
        ];
        for (address, sentence) in cases {
            let ret = ray_init_with_address(address, false);
            assert_eq!(ret, -1, "集群模式下的地址 {:?} 没有被拒", address);
            let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
            assert!(
                message.contains("address \""),
                "地址 {:?} 的错误消息没有点名 address: {}",
                address,
                message
            );
            assert!(
                message.contains(sentence),
                "地址 {:?} 的错误消息不是那一句: {}",
                address,
                message
            );
        }
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "被拒的调用建出了运行时"
        );
    }

    /// 地址为空时 `RAY_ADDRESS` 被设置（包括空串）就按同一规则校验，消息点名
    /// `RAY_ADDRESS`。上游在 config_internal.cc:207-215 读这个变量，空串同样走
    /// `ParseAddress` 并在 :255 终止进程。
    #[test]
    fn ray_init_rejects_an_invalid_ray_address_when_the_address_is_empty() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let cases: &[(&str, &str)] = &[
            ("local", "no ':' separator"),
            ("", "no ':' separator"),
            ("127.0.0.1", "no ':' separator"),
            ("127.0.0.1:abc", "the port must be decimal digits"),
            (":6379", "the host is empty"),
        ];
        for (env_value, sentence) in cases {
            std::env::set_var("RAY_ADDRESS", env_value);
            let ret = ray_init_with_address("", false);
            std::env::remove_var("RAY_ADDRESS");
            assert_eq!(ret, -1, "RAY_ADDRESS={:?} 没有被拒", env_value);
            let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
            assert!(
                message.contains("RAY_ADDRESS \""),
                "RAY_ADDRESS={:?} 的错误消息没有点名这个变量: {}",
                env_value,
                message
            );
            assert!(
                message.contains(sentence),
                "RAY_ADDRESS={:?} 的错误消息不是那一句: {}",
                env_value,
                message
            );
        }
        assert!(
            std::env::var_os("RAY_ADDRESS").is_none(),
            "用例跑完 RAY_ADDRESS 还在环境里"
        );
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "被拒的调用建出了运行时"
        );
    }

    /// 空函数名在 C 入口被拒，进程里没有运行时也能测：参数检查排在取运行时之前。
    /// `to_cstring` 把带内部 NUL 的名字（`"a\0b"`）折成空串（#27），所以两条用例
    /// 拿到的 C 串都是空的。
    #[test]
    fn actor_create_rejects_an_empty_function_name() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        for (raw, label) in [("", "空串"), ("a\0b", "带内部 NUL 的 \"a\\0b\"")] {
            let name = to_cstring(raw);
            assert!(
                name.as_bytes().is_empty(),
                "{} 经过 to_cstring 不是空串：这一条被拒的理由是它到 C 侧成了空串",
                label
            );
            let bytes = unsafe {
                rayrust_sys::ray_actor_create_with_options(
                    name.as_ptr(),
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                )
            };
            assert!(
                bytes.data.is_null(),
                "{} 的函数名被接受了（返回 {:#p}）",
                label,
                bytes.data
            );
            let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
            assert!(
                message.contains("the function name is empty"),
                "{} 的错误消息不是空函数名那一句: {}",
                label,
                message
            );
            assert!(
                !unsafe { rayrust_sys::ray_is_initialized() },
                "{} 被拒之后建出了运行时",
                label
            );
        }
    }

    /// 非空函数名不被这条检查拦住：守卫排在运行时检查之前，所以这里停在「运行时没有
    /// 初始化」那一条。
    #[test]
    fn actor_create_lets_a_non_empty_function_name_through_to_the_runtime_check() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let name = to_cstring("no_such_function");
        let bytes = unsafe {
            rayrust_sys::ray_actor_create_with_options(
                name.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null(),
            )
        };
        assert!(bytes.data.is_null(), "没有运行时却交回了 id");
        let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
        assert!(
            message.contains("Ray runtime is not initialized"),
            "非空函数名没有走到运行时检查: {}",
            message
        );
    }

    /// 空 module 名与空 class 名在 C 入口被拒，进程里没有运行时也能测：两条名字检查
    /// 都排在取运行时之前。`to_cstring` 把带内部 NUL 的名字（`"a\0b"`）折成空串
    /// （#27），所以那两条用例拿到的 C 串都是空的。
    #[test]
    fn actor_create_python_rejects_an_empty_module_or_class_name() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let cases: [(&str, &str, &str, &str); 5] = [
            ("", "Counter", "the module name is empty", "空 module 名"),
            (
                "",
                "",
                "the module name is empty",
                "两个都空（module 先报）",
            ),
            (
                "a\0b",
                "Counter",
                "the module name is empty",
                "带内部 NUL 的 module 名 \"a\\0b\"",
            ),
            ("rayrust_test", "", "the class name is empty", "空 class 名"),
            (
                "rayrust_test",
                "a\0b",
                "the class name is empty",
                "带内部 NUL 的 class 名 \"a\\0b\"",
            ),
        ];
        for (module, class, needle, label) in cases {
            let module_c = to_cstring(module);
            let class_c = to_cstring(class);
            if module.contains('\0') {
                assert!(
                    module_c.as_bytes().is_empty(),
                    "{} 经过 to_cstring 不是空串",
                    label
                );
            }
            if class.contains('\0') {
                assert!(
                    class_c.as_bytes().is_empty(),
                    "{} 经过 to_cstring 不是空串",
                    label
                );
            }
            let bytes = unsafe {
                rayrust_sys::ray_actor_create_python_with_options(
                    module_c.as_ptr(),
                    class_c.as_ptr(),
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                )
            };
            assert!(
                bytes.data.is_null(),
                "{} 被接受了（返回 {:#p}）",
                label,
                bytes.data
            );
            let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
            assert!(
                message.contains(needle),
                "{} 的错误消息不是 {:?}: {}",
                label,
                needle,
                message
            );
            assert!(
                !unsafe { rayrust_sys::ray_is_initialized() },
                "{} 被拒之后建出了运行时",
                label
            );
        }
    }

    /// 合法名字不被这两条检查拦住：守卫排在运行时检查之前，所以这里停在「运行时没有
    /// 初始化」那一条。
    #[test]
    fn actor_create_python_lets_valid_names_through_to_the_runtime_check() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let module = to_cstring("rayrust_test");
        let class = to_cstring("Counter");
        let bytes = unsafe {
            rayrust_sys::ray_actor_create_python_with_options(
                module.as_ptr(),
                class.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null(),
            )
        };
        assert!(bytes.data.is_null(), "没有运行时却交回了 id");
        let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
        assert!(
            message.contains("Ray runtime is not initialized"),
            "合法名字没有走到运行时检查: {}",
            message
        );
    }

    /// 空方法名在 C 入口被拒，进程里没有运行时也能测：两条方法名检查都排在取运行时
    /// 之前。`to_cstring` 把带内部 NUL 的方法名（`"in\0crement"`）折成空串（#27），
    /// 所以那条用例拿到的 C 串也是空的。id 宽度检查排在方法名前面，15 字节 id + 空
    /// 方法名报的是宽度错误。
    #[test]
    fn actor_call_python_rejects_an_empty_method_name() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let full_id = [0u8; 16];
        let short_id = [0u8; 15];
        let cases: [(&[u8], &str, &str, &str); 3] = [
            (&full_id, "", "the method name is empty", "空方法名"),
            (
                &full_id,
                "in\0crement",
                "the method name is empty",
                "带内部 NUL 的方法名 \"in\\0crement\"",
            ),
            (
                &short_id,
                "",
                "actor id must be 16 bytes",
                "15 字节 id + 空方法名",
            ),
        ];
        for (id, method_name, needle, label) in cases {
            let method_c = to_cstring(method_name);
            if method_name.contains('\0') {
                assert!(
                    method_c.as_bytes().is_empty(),
                    "{} 经过 to_cstring 不是空串",
                    label
                );
            }
            let bytes = unsafe {
                rayrust_sys::ray_actor_call_python(
                    id.as_ptr() as *const std::os::raw::c_char,
                    id.len(),
                    method_c.as_ptr(),
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                )
            };
            assert!(
                bytes.data.is_null(),
                "{} 被接受了（返回 {:#p}）",
                label,
                bytes.data
            );
            let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
            assert!(
                message.contains(needle),
                "{} 的错误消息不是 {:?}: {}",
                label,
                needle,
                message
            );
            assert!(
                !unsafe { rayrust_sys::ray_is_initialized() },
                "{} 被拒之后建出了运行时",
                label
            );
        }
    }

    /// 非空方法名不被这两条检查拦住：守卫排在取运行时之前，所以这里停在「运行时没有
    /// 初始化」那一条。
    #[test]
    fn actor_call_python_lets_a_non_empty_method_name_through_to_the_runtime_check() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let id = [0u8; 16];
        let method_c = to_cstring("get");
        let bytes = unsafe {
            rayrust_sys::ray_actor_call_python(
                id.as_ptr() as *const std::os::raw::c_char,
                id.len(),
                method_c.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null(),
            )
        };
        assert!(bytes.data.is_null(), "没有运行时却交回了调用结果");
        let message = rayrust_sys::last_error().expect("C 入口没有记录错误");
        assert!(
            message.contains("Ray runtime is not initialized"),
            "非空方法名没有走到运行时检查: {}",
            message
        );
    }

    /// 空 module 名在 C 入口被拒，进程里没有运行时也能测：这条检查排在取运行时之前。
    /// `to_cstring` 把带内部 NUL 的 module 名（`"a\0b"`）折成空串（#27），所以那条
    /// 用例拿到的 C 串也是空的。
    #[test]
    fn task_call_python_rejects_an_empty_module_name() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let cases: [(&str, &str, &str); 2] = [
            ("", "add", "空 module 名"),
            ("a\0b", "add", "带内部 NUL 的 module 名 \"a\\0b\""),
        ];
        for (module, function, label) in cases {
            if module.contains('\0') {
                assert!(
                    to_cstring(module).as_bytes().is_empty(),
                    "{} 经过 to_cstring 不是空串",
                    label
                );
            }
            match task_call_python_inner(module, function, &[], &[]) {
                Err(RayError::Ffi(message)) => assert!(
                    message.contains("the module name is empty"),
                    "{} 的错误消息里没有 module 名那条: {}",
                    label,
                    message
                ),
                Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
                Ok(_) => panic!("{} 被接受了", label),
            }
            assert!(
                !unsafe { rayrust_sys::ray_is_initialized() },
                "{} 被拒之后建出了运行时",
                label
            );
        }

        // 两个名字都空时报的是 module 那条：function 名根本不检查。
        match task_call_python_inner("", "", &[], &[]) {
            Err(RayError::Ffi(message)) => assert!(
                message.contains("the module name is empty"),
                "两个名字都空时没有报 module 名那条: {}",
                message
            ),
            Err(other) => panic!("两个名字都空时返回了别的错误变体: {}", other),
            Ok(_) => panic!("两个名字都空时被接受了"),
        }
    }

    /// 空 function 名不被拒绝，与空 module 名不同：加载器把它解析成 module 对象
    /// 本身（`function_manager.py:139`），而 module 对象可以可调用，所以这个输入能
    /// 跑通（`ray_c.h` 里有实测）。`to_cstring` 把带内部 NUL 的 function 名（`"a\0b"`）
    /// 折成空串（#27），两条用例都走到运行时检查。
    #[test]
    fn task_call_python_lets_an_empty_function_name_through_to_the_runtime_check() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let cases: [(&str, &str); 2] = [
            ("", "空 function 名"),
            ("a\0b", "带内部 NUL 的 function 名 \"a\\0b\""),
        ];
        for (function, label) in cases {
            if function.contains('\0') {
                assert!(
                    to_cstring(function).as_bytes().is_empty(),
                    "{} 经过 to_cstring 不是空串",
                    label
                );
            }
            match task_call_python_inner("rayrust_test", function, &[], &[]) {
                Err(RayError::Ffi(message)) => assert!(
                    message.contains("Ray runtime is not initialized"),
                    "{} 没有走到运行时检查: {}",
                    label,
                    message
                ),
                Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
                Ok(_) => panic!("{} 在没有运行时的进程里被提交了", label),
            }
        }
    }

    /// 非空名字不被前置检查拦住：守卫排在取运行时之前，所以这里停在「运行时没有
    /// 初始化」那一条。只有空白字符的名字不 trim，同样走到运行时检查。
    #[test]
    fn task_call_python_lets_non_empty_names_through_to_the_runtime_check() {
        assert!(
            !unsafe { rayrust_sys::ray_is_initialized() },
            "这个测试要求进程里没有 Ray runtime"
        );
        let cases: [(&str, &str, &str); 2] = [
            ("rayrust_test", "add", "正常的 module 名与 function 名"),
            (" ", "add", "只有空白字符的 module 名 \" \""),
        ];
        for (module, function, label) in cases {
            match task_call_python_inner(module, function, &[], &[]) {
                Err(RayError::Ffi(message)) => assert!(
                    message.contains("Ray runtime is not initialized"),
                    "{} 没有走到运行时检查: {}",
                    label,
                    message
                ),
                Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
                Ok(_) => panic!("{} 在没有运行时的进程里被提交了", label),
            }
        }
    }
}

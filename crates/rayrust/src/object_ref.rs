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
//!   `Unreconstructable`, an expired wait as `RayError::Timeout`
//!
//! ## Runtime generation
//!
//! 运行时初始化成功时发布一个代次，关闭时把它撤掉；没有活着的运行时的时候
//! 代次是 0。每个引用记下创建时的代次（没有活着的运行时就是 0）。最后一个
//! `Ray` 句柄 drop 之后运行时关闭，但它创建的引用还能被持有：这些引用上的
//! `get`/`get_async`/`wait`/批量取在进入 FFI 之前就返回 `RayError::Runtime`，
//! 消息里带引用自己的代次与活着的运行时的代次。
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

/// 拒绝没有活着的运行时、或者属于已经关闭的运行时的引用。
///
/// 工人进程（`librayrust_worker.so`）不经过 `init_with_config`，`LIVE_EPOCH`
/// 恒为 0。宏生成的 `#[remote]` 与 actor 回调只做序列化，不构造 `ObjectRef`，
/// 所以它们不受影响；用户函数里自己调用 `ObjectRef::from` 重建引用时，这个
/// 引用拿到的代次是 0，会被这里拒成 `RayError::Runtime`。那条调用在基线版本
/// 上同样失败（工人进程里 `GetRayRuntime()` 是 per-DSO 副本，#38），区别只是
/// 错误变体。
pub(crate) fn check_epoch(epoch: u64) -> Result<(), RayError> {
    let live = live_epoch();
    if epoch != 0 && epoch == live {
        return Ok(());
    }
    if live == 0 {
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
#[derive(Debug)]
pub struct ObjectRef<T> {
    pub(crate) id: Vec<u8>,
    pub(crate) is_xlang: bool,
    /// 创建时的运行时代次，用于拒绝属于已经关闭的运行时的引用。
    pub(crate) epoch: u64,
    _marker: PhantomData<T>,
}

/// 手写 Clone：派生版本会额外要求 `T: Clone`，而引用本身只需要复制 id。
impl<T> Clone for ObjectRef<T> {
    fn clone(&self) -> Self {
        ObjectRef {
            id: self.id.clone(),
            is_xlang: self.is_xlang,
            epoch: self.epoch,
            _marker: PhantomData,
        }
    }
}

impl<T> ObjectRef<T> {
    /// 打戳取当前活着的运行时的代次；没有活着的运行时就是 0，此后恒被拒。
    pub(crate) fn from_id(id: Vec<u8>) -> Self {
        ObjectRef {
            id,
            is_xlang: false,
            epoch: live_epoch(),
            _marker: PhantomData,
        }
    }

    pub(crate) fn from_id_xlang(id: Vec<u8>) -> Self {
        ObjectRef {
            id,
            is_xlang: true,
            epoch: live_epoch(),
            _marker: PhantomData,
        }
    }

    /// Get the raw object ID as bytes.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// Get the object ID as a hex string (for debugging).
    pub fn id_hex(&self) -> String {
        self.id.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Retrieve the object value (blocks until ready).
    pub fn get(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        check_epoch(self.epoch)?;
        let bytes = crate::runtime::get_raw(&self.id)?;
        if self.is_xlang {
            deserialize_xlang(&bytes)
        } else {
            deserialize(&bytes)
        }
    }

    /// Retrieve the object as a generic msgpack Value (blocks until ready).
    ///
    /// Useful when the exact type is unknown at compile time, e.g. when
    /// calling a Python function that returns a complex/nested structure.
    /// The `is_xlang` flag is respected automatically.
    pub fn get_value(&self) -> Result<rmpv::Value, RayError> {
        check_epoch(self.epoch)?;
        let bytes = crate::runtime::get_raw(&self.id)?;
        if self.is_xlang {
            crate::serialize::deserialize_xlang_value(&bytes)
        } else {
            crate::serialize::deserialize_value(&bytes)
        }
    }

    /// Retrieve the object value with a timeout (in milliseconds).
    ///
    /// `-1` waits forever; other negative values return `RayError::Ffi`.
    pub fn get_timeout(&self, timeout_ms: i32) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        check_epoch(self.epoch)?;
        let bytes = crate::runtime::get_raw_timeout(&self.id, timeout_ms)?;
        if self.is_xlang {
            deserialize_xlang(&bytes)
        } else {
            deserialize(&bytes)
        }
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
        self.get_timeout_async(std::time::Duration::from_secs(300)).await
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

        if self.is_xlang {
            deserialize_xlang(&bytes)
        } else {
            deserialize(&bytes)
        }
    }

    /// Asynchronously retrieve the object as a generic msgpack Value.
    ///
    /// Same blocking-pool `Get` as `get_async()`.
    /// Useful for Python cross-language calls with unknown return types.
    pub async fn get_value_async(&self) -> Result<rmpv::Value, RayError> {
        self.get_value_timeout_async(std::time::Duration::from_secs(300)).await
    }

    /// Asynchronously retrieve the object as a generic msgpack Value with timeout.
    pub async fn get_value_timeout_async(&self, timeout: std::time::Duration) -> Result<rmpv::Value, RayError> {
        let id = self.id.clone();
        let epoch = self.epoch;
        let timeout_ms = crate::runtime::duration_to_ms(timeout);
        let bytes = tokio::task::spawn_blocking(move || {
            check_epoch(epoch)?;
            crate::runtime::get_raw_timeout(&id, timeout_ms)
        })
        .await
        .map_err(|e| RayError::Runtime(format!("spawn_blocking join error: {}", e)))??;

        if self.is_xlang {
            crate::serialize::deserialize_xlang_value(&bytes)
        } else {
            crate::serialize::deserialize_value(&bytes)
        }
    }

    /// Get raw bytes from the object store (for debugging).
    pub fn get_raw_bytes(&self) -> Result<Vec<u8>, RayError> {
        check_epoch(self.epoch)?;
        crate::runtime::get_raw(&self.id)
    }

    /// Cast the phantom type parameter.
    pub fn cast<U>(self) -> ObjectRef<U> {
        ObjectRef {
            id: self.id,
            is_xlang: self.is_xlang,
            epoch: self.epoch,
            _marker: PhantomData,
        }
    }
}

impl<T> From<Vec<u8>> for ObjectRef<T> {
    fn from(id: Vec<u8>) -> Self {
        ObjectRef::from_id(id)
    }
}

impl<T> From<ObjectRef<T>> for Vec<u8> {
    fn from(obj: ObjectRef<T>) -> Vec<u8> {
        obj.id
    }
}

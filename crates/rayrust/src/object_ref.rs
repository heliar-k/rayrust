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
//!   it until the deadline, and shutting the runtime down waits for that
//!   thread
//! - a failed task surfaces as `RayError::Task`/`Actor`/`Worker`/
//!   `Unreconstructable`, an expired wait as `RayError::Timeout`
//!
//! For cross-language (Python) results, the data has a 9-byte XLANG header
//! that is stripped before deserialization.

use std::marker::PhantomData;

use crate::error::RayError;
use crate::serialize::{deserialize, deserialize_xlang};

/// A reference to an object stored in the Ray object store.
///
/// `ObjectRef` methods (`get`, `get_async`, `wait`) call the global Ray
/// runtime directly rather than going through `&Ray`. This is intentional:
/// `ObjectRef` is a value handle (like a file descriptor), not a context
/// reference. It can be freely cloned, stored, and awaited without holding
/// a borrow on `Ray`. The underlying C++ SDK is a global singleton, so
/// there is no ambiguity about which runtime instance is used.
///
/// The `Ray` context only manages init/shutdown lifecycle. Once `Ray` is
/// dropped, the runtime shuts down and `ObjectRef` operations will fail
/// at runtime — but this is not enforced at compile time, matching the
/// semantics of Ray in Python and C++.
#[derive(Debug)]
pub struct ObjectRef<T> {
    pub(crate) id: Vec<u8>,
    pub(crate) is_xlang: bool,
    _marker: PhantomData<T>,
}

/// 手写 Clone：派生版本会额外要求 `T: Clone`，而引用本身只需要复制 id。
impl<T> Clone for ObjectRef<T> {
    fn clone(&self) -> Self {
        ObjectRef {
            id: self.id.clone(),
            is_xlang: self.is_xlang,
            _marker: PhantomData,
        }
    }
}

impl<T> ObjectRef<T> {
    pub(crate) fn from_id(id: Vec<u8>) -> Self {
        ObjectRef { id, is_xlang: false, _marker: PhantomData }
    }

    pub(crate) fn from_id_xlang(id: Vec<u8>) -> Self {
        ObjectRef { id, is_xlang: true, _marker: PhantomData }
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
    /// the pool thread stays on it until the deadline, and shutting the runtime
    /// down waits for that thread too.
    pub async fn get_timeout_async(&self, timeout: std::time::Duration) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let id = self.id.clone();
        let timeout_ms = crate::runtime::duration_to_ms(timeout);
        let bytes = tokio::task::spawn_blocking(move || {
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
        let timeout_ms = crate::runtime::duration_to_ms(timeout);
        let bytes = tokio::task::spawn_blocking(move || {
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
        crate::runtime::get_raw(&self.id)
    }

    /// Cast the phantom type parameter.
    pub fn cast<U>(self) -> ObjectRef<U> {
        ObjectRef {
            id: self.id,
            is_xlang: self.is_xlang,
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

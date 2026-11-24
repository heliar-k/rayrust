//! rayrust — Rust SDK for Ray distributed computing.
//!
//! This crate wraps the Ray C++ SDK via a C ABI layer, providing
//! idiomatic Rust APIs for Ray's core distributed primitives:
//! - Object store (put/get/wait) — sync and async
//! - Remote tasks — sync and async
//! - Cross-language calls (Python tasks + actors)
//! - Actors
//! - Placement groups
//!
//! ## Quick start
//! ```no_run
//! use rayrust::prelude::*;
//!
//! let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;
//! let obj = ray.put(&42i32)?;
//! let val: i32 = obj.get()?;
//! // drop(ray) → automatic shutdown
//! # Ok::<(), rayrust::RayError>(())
//! ```
//!
//! ## Actor 约束（编译期检查）
//!
//! 正常的 `Send + 'static` 结构体与 `&self` / `&mut self` / 无 `self` 方法可通过编译：
//! ```no_run
//! struct SafeActor {
//!     count: i64,
//! }
//!
//! #[rayrust::actor]
//! impl SafeActor {
//!     fn new(start: i64) -> Self {
//!         SafeActor { count: start }
//!     }
//!     fn inc(&mut self, delta: i64) -> i64 {
//!         self.count += delta;
//!         self.count
//!     }
//!     fn get(&self) -> i64 {
//!         self.count
//!     }
//!     fn version() -> i64 {
//!         1
//!     }
//! }
//! fn main() {}
//! ```
//!
//! 含 `!Send` 字段（如 `std::rc::Rc<i32>`）的结构体无法跨线程包装进 `Box<Mutex<T>>`：
//! ```compile_fail
//! use std::rc::Rc;
//!
//! struct NotSendActor {
//!     inner: Rc<i32>,
//! }
//!
//! #[rayrust::actor]
//! impl NotSendActor {
//!     fn new() -> Self {
//!         NotSendActor { inner: Rc::new(1) }
//!     }
//!     fn get(&self) -> i32 {
//!         *self.inner
//!     }
//! }
//! fn main() {}
//! ```
//!
//! 按值接收 `self` 的方法无法在锁守卫内借用：
//! ```compile_fail
//! struct OwnedMethodActor {
//!     value: i32,
//! }
//!
//! #[rayrust::actor]
//! impl OwnedMethodActor {
//!     fn new() -> Self {
//!         OwnedMethodActor { value: 0 }
//!     }
//!     fn consume(self) -> i32 {
//!         self.value
//!     }
//! }
//! fn main() {}
//! ```

pub mod actor_exit;
pub(crate) mod actor_registry;
pub mod async_runtime;
pub mod callback;
pub mod error;
pub mod object_ref;
pub mod runtime;
pub mod serialize;
pub mod task_args;

/// Re-export of the raw FFI crate for macro-generated code.
pub use rayrust_sys as sys;

/// Re-export ctor for macro-generated code.
pub use ctor;

pub use error::RayError;
pub use object_ref::ObjectRef;
pub use runtime::{
    ActorHandle, ActorLifetime, ActorOptions, PlacementGroupInfo, Ray, RayConfig, TaskOptions,
};
pub use task_args::{TaskArg, TaskArgs};

/// 在当前 actor 的方法里结束它。
pub use actor_exit::exit_actor;

/// Re-export block_on_async for the `#[remote]` proc macro.
pub use async_runtime::block_on_async;

/// Re-export rmpv so users can use `rmpv::Value` for dynamic deserialization
/// without adding rmpv as their own dependency.
pub use rmpv;

/// Re-export the proc macros.
pub use rayrust_macros::{actor, remote};

// ── Serialization (pub — used by macro callbacks in worker process) ──

pub use serialize::{
    deserialize, deserialize_value, deserialize_xlang, deserialize_xlang_value, serialize,
    serialize_xlang,
};

/// Register a Rust actor member function with Ray's FunctionManager.
/// This is used by the `#[rayrust::actor]` macro for member function registration.
/// Must be called before `Ray::connect` or before the first actor call.
pub fn register_member_function(
    func_name: &str,
    callback: extern "C" fn(
        u64,
        *const rayrust_sys::RayBytes,
        usize,
    ) -> rayrust_sys::RayCallbackResult,
) {
    let name_c = rayrust_sys::to_cstring(func_name);
    unsafe {
        rayrust_sys::ray_register_member_function(name_c.as_ptr(), callback);
    }
}

/// Convenience module for common imports.
pub mod prelude {
    pub use crate::error::RayError;
    pub use crate::object_ref::ObjectRef;
    pub use crate::runtime::{
        ActorHandle, ActorLifetime, ActorOptions, PlacementGroupInfo, Ray, RayConfig, TaskOptions,
    };
    pub use crate::serialize::{
        deserialize, deserialize_value, deserialize_xlang, deserialize_xlang_value, serialize,
        serialize_xlang,
    };
    pub use crate::task_args::{TaskArg, TaskArgs};
    pub use crate::{exit_actor, register_member_function, rmpv};
    pub use rayrust_macros::{actor, remote};
}

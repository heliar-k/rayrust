//! FFI bindings to the Ray C++ SDK via a C ABI wrapper.
//!
//! This crate provides raw `unsafe` bindings. The `rayrust` crate wraps
//! these in safe Rust APIs.
//!
//! IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
//! All ID parameters use `ray_bytes_t` (ptr + len), not null-terminated strings.

use std::ffi::CString;
use std::os::raw::{c_char, c_int};

pub mod error_payload;

// ─── C struct definitions ─────────────────────────────────────

#[repr(C)]
pub struct RayBytes {
    pub data: *const c_char,
    pub len: usize,
}

/// 一个 actor handle 的语言与创建描述符类型（`wrapper/ray_c.h` 的
/// `ray_actor_handle_meta_t`）。`creation_descriptor` 是 `rpc.FunctionDescriptor`
/// 那个 oneof 成员的字段号：0 没有、java 1、python 2、cpp 3。
#[repr(C)]
pub struct RayActorHandleMeta {
    pub language: c_int,
    pub creation_descriptor: c_int,
}

/// Opaque C handle to a `ray::Metric` instance (`wrapper/ray_c.h` `ray_metric_t`).
#[repr(C)]
pub struct RayMetric {
    _private: [u8; 0],
}

/// Single `(key, value)` tag entry (`wrapper/ray_c.h` `ray_metric_tag_t`).
#[repr(C)]
pub struct RayMetricTag {
    pub key: RayBytes,
    pub value: RayBytes,
}

// ─── Rust 回调状态与结果结构体 ────────────────────────────────

/// 与 `wrapper/ray_c.h` 的 `ray_callback_status_t` 及 `ray_callback_result_t` 一致。
/// 正常返回时 `data`/`len` 为 msgpack 序列化的返回值；失败时为不含内部 NUL 字节的
/// UTF-8 错误文本；两种退出状态传 `data = NULL, len = 0`。所有非空 `data` 缓冲区
/// 由回调方通过 `libc::malloc` 分配（独立于 `#[global_allocator]`），由 C 侧
/// 调用 `free()` 释放。
#[repr(C)]
pub struct RayCallbackResult {
    pub status: c_int,
    pub data: *const c_char,
    pub len: usize,
}

pub const RAY_CALLBACK_OK: c_int = 0;
pub const RAY_CALLBACK_ERROR: c_int = 1;
pub const RAY_CALLBACK_INTENTIONAL_EXIT: c_int = 2;
pub const RAY_CALLBACK_EXIT_OUTSIDE_ACTOR: c_int = 3;

// ─── extern "C" function declarations ─────────────────────────

extern "C" {
    // Lifecycle
    pub fn ray_init(
        address: *const c_char,
        local_mode: c_int,
        node_ip: *const c_char,
        code_search_path: *const c_char,
        runtime_env_json: *const c_char,
        log_dir: *const c_char,
        actor_lifetime: c_int,
        namespace: *const c_char,
    ) -> c_int;
    pub fn ray_is_initialized() -> bool;
    pub fn ray_shutdown();

    // Object Store
    pub fn ray_put(data: *const c_char, len: usize) -> RayBytes;
    pub fn ray_put_xlang(data: *const c_char, len: usize) -> RayBytes;
    pub fn ray_get(id_data: *const c_char, id_len: usize, timeout_ms: c_int) -> RayBytes;
    pub fn ray_add_local_reference(id_data: *const c_char, id_len: usize) -> c_int;
    pub fn ray_remove_local_reference(id_data: *const c_char, id_len: usize) -> c_int;
    pub fn ray_wait(
        ids: *const RayBytes,
        count: usize,
        num_objects: usize,
        timeout_ms: c_int,
    ) -> *mut bool;

    // Task
    pub fn ray_task_call_with_options(
        func_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        is_ref: *const bool,
        options_json: *const c_char,
    ) -> RayBytes;
    pub fn ray_task_call_python(
        module_name: *const c_char,
        function_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        is_ref: *const bool,
    ) -> RayBytes;
    pub fn ray_task_call_python_with_options(
        module_name: *const c_char,
        function_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        is_ref: *const bool,
        options_json: *const c_char,
    ) -> RayBytes;

    // Actor
    pub fn ray_actor_create_with_options(
        func_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        options_json: *const c_char,
    ) -> RayBytes;
    pub fn ray_actor_create_python_with_options(
        module_name: *const c_char,
        class_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        options_json: *const c_char,
    ) -> RayBytes;
    pub fn ray_actor_call(
        actor_id_data: *const c_char,
        actor_id_len: usize,
        func_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        is_ref: *const bool,
    ) -> RayBytes;
    pub fn ray_actor_call_python(
        actor_id_data: *const c_char,
        actor_id_len: usize,
        method_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
        is_ref: *const bool,
    ) -> RayBytes;
    pub fn ray_actor_kill(actor_id_data: *const c_char, actor_id_len: usize, no_restart: bool);
    pub fn ray_actor_handle_serialize(
        actor_id_data: *const c_char,
        actor_id_len: usize,
    ) -> RayBytes;
    pub fn ray_actor_handle_deserialize(
        serialized_data: *const c_char,
        serialized_len: usize,
        out_meta: *mut RayActorHandleMeta,
    ) -> RayBytes;

    // Placement Group
    pub fn ray_placement_group_create(
        name: *const c_char,
        bundles_json: *const c_char,
        strategy: c_int,
    ) -> RayBytes;
    pub fn ray_placement_group_remove(group_id_data: *const c_char, group_id_len: usize) -> c_int;
    pub fn ray_placement_group_wait_ready(
        group_id_data: *const c_char,
        group_id_len: usize,
        timeout_seconds: c_int,
    ) -> c_int;
    pub fn ray_placement_group_get_all() -> RayBytes;
    pub fn ray_placement_group_get_by_id(
        group_id_data: *const c_char,
        group_id_len: usize,
    ) -> RayBytes;
    pub fn ray_placement_group_get_by_name(name: *const c_char) -> RayBytes;

    // Function Registration
    pub fn ray_register_function(
        func_name: *const c_char,
        callback: extern "C" fn(args: *const RayBytes, arg_count: usize) -> RayCallbackResult,
    );
    pub fn ray_register_member_function(
        func_name: *const c_char,
        callback: extern "C" fn(
            actor_ptr: u64,
            args: *const RayBytes,
            arg_count: usize,
        ) -> RayCallbackResult,
    );

    // Misc
    pub fn ray_was_current_actor_restarted() -> bool;
    pub fn ray_is_local_mode() -> bool;
    /// 运行时是不是由别的 DSO 建的（`wrapper/ray_c.h` 的 `ray_runtime_is_borrowed`）。
    /// 1 表示本 .so 内联的 holder 为空而进程里已有别人建的运行时，也就是工人进程。
    pub fn ray_runtime_is_borrowed() -> c_int;
    pub fn ray_get_namespace() -> RayBytes;
    pub fn ray_get_actor(
        name: *const c_char,
        ray_namespace: *const c_char,
        out_meta: *mut RayActorHandleMeta,
    ) -> RayBytes;
    pub fn ray_cancel(
        id_data: *const c_char,
        id_len: usize,
        force_kill: bool,
        recursive: bool,
    ) -> c_int;
    pub fn ray_get_many(ids: *const RayBytes, count: usize, timeout_ms: c_int) -> *mut RayBytes;
    pub fn ray_free_bytes_array(array: *mut RayBytes, count: usize);

    // Metrics
    pub fn ray_gauge_create(
        name: RayBytes,
        description: RayBytes,
        unit: RayBytes,
        tag_keys: *const RayBytes,
        tag_key_count: usize,
    ) -> *mut RayMetric;
    pub fn ray_counter_create(
        name: RayBytes,
        description: RayBytes,
        unit: RayBytes,
        tag_keys: *const RayBytes,
        tag_key_count: usize,
    ) -> *mut RayMetric;
    pub fn ray_histogram_create(
        name: RayBytes,
        description: RayBytes,
        unit: RayBytes,
        boundaries: *const f64,
        boundary_count: usize,
        tag_keys: *const RayBytes,
        tag_key_count: usize,
    ) -> *mut RayMetric;
    pub fn ray_sum_create(
        name: RayBytes,
        description: RayBytes,
        unit: RayBytes,
        tag_keys: *const RayBytes,
        tag_key_count: usize,
    ) -> *mut RayMetric;
    pub fn ray_metric_get_name(metric: *const RayMetric) -> RayBytes;
    pub fn ray_metric_destroy(metric: *mut RayMetric);
    pub fn ray_gauge_set(
        metric: *mut RayMetric,
        value: f64,
        tags: *const RayMetricTag,
        tag_count: usize,
    ) -> c_int;
    pub fn ray_counter_inc(
        metric: *mut RayMetric,
        value: f64,
        tags: *const RayMetricTag,
        tag_count: usize,
    ) -> c_int;
    pub fn ray_histogram_observe(
        metric: *mut RayMetric,
        value: f64,
        tags: *const RayMetricTag,
        tag_count: usize,
    ) -> c_int;
    pub fn ray_metric_record(
        metric: *mut RayMetric,
        value: f64,
        tags: *const RayMetricTag,
        tag_count: usize,
    ) -> c_int;

    // Memory Management
    pub fn ray_free_bytes(ptr: *mut RayBytes);
    pub fn ray_free_bools(ptr: *mut bool);

    // Error Handling
    pub fn ray_last_error_bytes(data: *mut *const c_char, len: *mut usize);
    pub fn ray_last_error_kind() -> c_int;
    pub fn ray_clear_error();
}

// ─── Error kinds ──────────────────────────────────────────────

/// Class of the last error, mirroring `ray_error_kind_t` in wrapper/ray_c.h.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// No error recorded on this thread.
    None = 0,
    /// A plain `std::exception`, or a message set without an exception.
    Generic = 1,
    /// `RayException`, the base of Ray's exception hierarchy.
    Ray = 2,
    Actor = 3,
    Task = 4,
    Worker = 5,
    Unreconstructable = 6,
    Timeout = 7,
    /// 运行时正在关闭：`ray_shutdown` 已经开始或已经结束，等下一次 `ray_init`。
    Runtime = 8,
    /// Ray 判定该对象失败，且错误类型不在 C++ SDK 翻译的七种之内；由 `ray_get` 与
    /// `ray_get_many` 在收到零长度缓冲区时写入。
    Unclassified = 9,
}

impl ErrorKind {
    /// Map a `ray_error_kind_t` constant; `Generic` for a value this enum does
    /// not know, so the accompanying message keeps its class instead of being
    /// dropped.
    pub fn from_raw(raw: c_int) -> ErrorKind {
        match raw {
            0 => ErrorKind::None,
            2 => ErrorKind::Ray,
            3 => ErrorKind::Actor,
            4 => ErrorKind::Task,
            5 => ErrorKind::Worker,
            6 => ErrorKind::Unreconstructable,
            7 => ErrorKind::Timeout,
            8 => ErrorKind::Runtime,
            9 => ErrorKind::Unclassified,
            _ => ErrorKind::Generic,
        }
    }
}

// ─── Safe wrapper helpers ─────────────────────────────────────

/// RAII guard for a C-allocated byte buffer (ray_bytes_t).
/// Frees the underlying memory on Drop.
pub struct CBytesGuard {
    inner: RayBytes,
}

impl CBytesGuard {
    pub fn from(inner: RayBytes) -> Option<Self> {
        if inner.data.is_null() {
            None
        } else {
            Some(Self { inner })
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.inner.data as *const u8, self.inner.len) }
    }
}

impl Drop for CBytesGuard {
    fn drop(&mut self) {
        unsafe { ray_free_bytes(&mut self.inner) }
    }
}

/// Helper to convert a Rust string to CString, handling null.
pub fn to_cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("").unwrap())
}

/// 读取当前线程在 C 包装层记录的最后一条错误消息。
///
/// 能从 Ray 二进制错误载荷中提取出格式化异常文本时直接返回该文本，否则按 UTF-8
/// lossy 呈现全部原始字节；只有当前线程没有记录错误时才返回 `None`。
pub fn last_error() -> Option<String> {
    let mut data: *const c_char = std::ptr::null();
    let mut len: usize = 0;
    unsafe { ray_last_error_bytes(&mut data, &mut len) };
    if data.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(data as *const u8, len) };
    Some(error_payload::render(bytes))
}

/// Clear the last error message and its kind.
pub fn clear_error() {
    unsafe { ray_clear_error() }
}

/// Kind of the last error recorded by the C wrapper on this thread.
pub fn last_error_kind() -> ErrorKind {
    ErrorKind::from_raw(unsafe { ray_last_error_kind() })
}

/// Build a `Vec<RayBytes>` from a slice of byte slices.
/// The returned vector must be kept alive for the duration of the FFI call.
pub fn build_args_array(args: &[&[u8]]) -> Vec<RayBytes> {
    args.iter()
        .map(|b| RayBytes {
            data: b.as_ptr() as *const c_char,
            len: b.len(),
        })
        .collect()
}

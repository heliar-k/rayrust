//! Rust 侧 `extern "C"` 回调运行时支撑：参数解包、反序列化、Actor 状态互斥与异常捕获。
//!
//! 本模块把过程宏生成的回调逻辑收敛成普通 Rust 函数：
//! - 缓冲区分配集中在单一私有函数，便于后续替换分配器；
//! - `unpack_args` 与 `deserialize_arg` 将空指针、参数个数不符与解码失败转为 `Err(String)`；
//! - `into_actor_ptr` / `with_actor_ref` / `with_actor_mut` 将 actor 状态包在
//!   `Box<std::sync::Mutex<T>>` 内，约束 `T: Send + 'static` 并串行借用；
//! - `run_task_callback`、`run_factory_callback` 与 `run_member_callback` 以 `catch_unwind`
//!   包裹整个执行与序列化过程，确保任何 `panic!` 都不会跨越 `extern "C"` 边界展开。

use std::os::raw::{c_char, c_int};

use rayrust_sys::{RayBytes, RayCallbackResult};

/// 将字节切片拷入 C 侧 `free()` 可释放的堆缓冲区（#28 的单一修改点）。
///
/// 当 `bytes` 为空时直接返回 `(null, 0)`，不调用零大小 `std::alloc::alloc`；布局计算或
/// 内存分配失败时返回 `Err(())`，由调用方回退为无缓冲区的 `RAY_CALLBACK_ERROR`。
fn alloc_callback_bytes(bytes: &[u8]) -> Result<(*const c_char, usize), ()> {
    if bytes.is_empty() {
        return Ok((std::ptr::null(), 0));
    }
    let layout = std::alloc::Layout::array::<u8>(bytes.len()).map_err(|_| ())?;
    let ptr = unsafe { std::alloc::alloc(layout) };
    if ptr.is_null() {
        return Err(());
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
    }
    Ok((ptr as *const c_char, bytes.len()))
}

fn ok_result(bytes: &[u8]) -> RayCallbackResult {
    match alloc_callback_bytes(bytes) {
        Ok((data, len)) => RayCallbackResult {
            status: rayrust_sys::RAY_CALLBACK_OK,
            data,
            len,
        },
        Err(()) => RayCallbackResult {
            status: rayrust_sys::RAY_CALLBACK_ERROR,
            data: std::ptr::null(),
            len: 0,
        },
    }
}

fn error_result(message: &str) -> RayCallbackResult {
    // 替换内部 NUL 字节，避免 C++ 侧 `std::exception::what()` 在首个 NUL 处截断。
    let sanitized = message.replace('\0', "\\0");
    match alloc_callback_bytes(sanitized.as_bytes()) {
        Ok((data, len)) => RayCallbackResult {
            status: rayrust_sys::RAY_CALLBACK_ERROR,
            data,
            len,
        },
        Err(()) => RayCallbackResult {
            status: rayrust_sys::RAY_CALLBACK_ERROR,
            data: std::ptr::null(),
            len: 0,
        },
    }
}

/// 校验参数数组指针、参数个数及各元素数据指针，返回借用自输入缓冲区的切片列表。
///
/// # Safety
///
/// 当 `arg_count > 0` 且 `args` 非空时，`args` 必须指向至少 `arg_count` 个有效的
/// `RayBytes` 元素；其中 `len > 0` 且 `data` 非空的元素必须指向至少 `len` 字节的可读内存，
/// 且生命周期覆盖返回的切片。
pub unsafe fn unpack_args<'a>(
    args: *const RayBytes,
    arg_count: usize,
    expected: usize,
    context: &str,
) -> Result<Vec<&'a [u8]>, String> {
    if arg_count > 0 && args.is_null() {
        return Err(format!(
            "{context} received a null argument array pointer for {arg_count} argument(s)"
        ));
    }
    if arg_count != expected {
        return Err(format!(
            "{context} expected {expected} argument(s), got {arg_count}"
        ));
    }
    if arg_count == 0 {
        return Ok(Vec::new());
    }
    let args_slice = std::slice::from_raw_parts(args, arg_count);
    let mut out = Vec::with_capacity(arg_count);
    for (i, entry) in args_slice.iter().enumerate() {
        if entry.len == 0 {
            out.push(&[][..]);
        } else if entry.data.is_null() {
            return Err(format!(
                "argument {i} of {context} has a null data pointer with length {}",
                entry.len
            ));
        } else {
            out.push(std::slice::from_raw_parts(
                entry.data as *const u8,
                entry.len,
            ));
        }
    }
    Ok(out)
}

/// 反序列化单个参数字节切片，失败时返回带参数下标与调用上下文的错误文本。
pub fn deserialize_arg<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    index: usize,
    context: &str,
) -> Result<T, String> {
    crate::serialize::deserialize(bytes)
        .map_err(|e| format!("failed to deserialize argument {index} of {context}: {e}"))
}

/// 将新构造的 actor 实例包装进 `Box<std::sync::Mutex<T>>` 并返回裸指针整数值。
///
/// `T: Send + 'static` 在编译期阻止 `!Send` 类型跨线程共享。
pub fn into_actor_ptr<T: Send + 'static>(instance: T) -> u64 {
    Box::into_raw(Box::new(std::sync::Mutex::new(instance))) as u64
}

unsafe fn actor_mutex<'a, T: Send + 'static>(
    actor_ptr: u64,
    context: &str,
) -> Result<&'a std::sync::Mutex<T>, String> {
    if actor_ptr == 0 {
        return Err(format!("actor pointer is null in {context}"));
    }
    let ptr = actor_ptr as *const std::sync::Mutex<T>;
    let align = std::mem::align_of::<std::sync::Mutex<T>>();
    if (ptr as usize) % align != 0 {
        return Err(format!(
            "actor pointer {actor_ptr:#x} is not aligned to {align} bytes in {context}"
        ));
    }
    Ok(&*ptr)
}

/// 持互斥锁以共享引用借用 actor 实例并执行 `f`，锁在 `f` 返回后立即释放。
///
/// # Safety
///
/// `actor_ptr` 若非空且满足对齐要求，必须是由 [`into_actor_ptr::<T>`] 产生且尚未释放的
/// `Box<std::sync::Mutex<T>>` 指针。
pub unsafe fn with_actor_ref<T: Send + 'static, R>(
    actor_ptr: u64,
    context: &str,
    f: impl FnOnce(&T) -> R,
) -> Result<R, String> {
    let mutex = actor_mutex::<T>(actor_ptr, context)?;
    let guard = mutex.lock().map_err(|_| {
        format!(
            "actor state mutex is poisoned in {context} after a panic in a previous method call"
        )
    })?;
    Ok(f(&*guard))
}

/// 持互斥锁以独占引用借用 actor 实例并执行 `f`，锁在 `f` 返回后立即释放。
///
/// # Safety
///
/// `actor_ptr` 若非空且满足对齐要求，必须是由 [`into_actor_ptr::<T>`] 产生且尚未释放的
/// `Box<std::sync::Mutex<T>>` 指针。
pub unsafe fn with_actor_mut<T: Send + 'static, R>(
    actor_ptr: u64,
    context: &str,
    f: impl FnOnce(&mut T) -> R,
) -> Result<R, String> {
    let mutex = actor_mutex::<T>(actor_ptr, context)?;
    let mut guard = mutex.lock().map_err(|_| {
        format!(
            "actor state mutex is poisoned in {context} after a panic in a previous method call"
        )
    })?;
    Ok(f(&mut *guard))
}

fn panic_payload_message(payload: &dyn std::any::Any) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn run_callback_with<T, R: serde::Serialize>(
    context: &str,
    exit_status: c_int,
    body: impl FnOnce() -> Result<T, String>,
    on_ok: impl FnOnce(T) -> R,
) -> RayCallbackResult {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> Result<Option<Vec<u8>>, String> {
            crate::actor_exit::clear_exit_request();
            let value = body()?;
            if crate::actor_exit::take_exit_request() {
                return Ok(None);
            }
            let mapped = on_ok(value);
            let bytes = crate::serialize::serialize(&mapped)
                .map_err(|e| format!("failed to serialize result of {context}: {e}"))?;
            Ok(Some(bytes))
        },
    ));
    match outcome {
        Ok(Ok(None)) => RayCallbackResult {
            status: exit_status,
            data: std::ptr::null(),
            len: 0,
        },
        Ok(Ok(Some(bytes))) => ok_result(&bytes),
        Ok(Err(msg)) => {
            let safe = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::actor_exit::clear_exit_request();
                error_result(&msg)
            }));
            match safe {
                Ok(res) => res,
                Err(double_panic) => {
                    std::mem::forget(double_panic);
                    RayCallbackResult {
                        status: rayrust_sys::RAY_CALLBACK_ERROR,
                        data: std::ptr::null(),
                        len: 0,
                    }
                }
            }
        }
        Err(payload) => {
            let safe = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::actor_exit::clear_exit_request();
                let msg = format!("panic in {context}: {}", panic_payload_message(&*payload));
                drop(payload);
                error_result(&msg)
            }));
            match safe {
                Ok(res) => res,
                Err(double_panic) => {
                    std::mem::forget(double_panic);
                    RayCallbackResult {
                        status: rayrust_sys::RAY_CALLBACK_ERROR,
                        data: std::ptr::null(),
                        len: 0,
                    }
                }
            }
        }
    }
}

/// 在 `catch_unwind` 边界内执行 `#[remote]` 任务回调。
///
/// 若函数体内部调用了 [`crate::exit_actor`]，返回 `RAY_CALLBACK_EXIT_OUTSIDE_ACTOR`。
pub fn run_task_callback<R: serde::Serialize>(
    context: &str,
    body: impl FnOnce() -> Result<R, String>,
) -> RayCallbackResult {
    run_callback_with(
        context,
        rayrust_sys::RAY_CALLBACK_EXIT_OUTSIDE_ACTOR,
        body,
        |v| v,
    )
}

/// 在 `catch_unwind` 边界内执行 `#[actor]` 构造函数回调。
///
/// 在检测到 [`crate::exit_actor`] 时直接析构新构造的实例并返回
/// `RAY_CALLBACK_EXIT_OUTSIDE_ACTOR`，不会将其包装为裸指针而泄漏；仅在未请求退出时
/// 调用 [`into_actor_ptr`] 并序列化 `u64`。
pub fn run_factory_callback<T: Send + 'static>(
    context: &str,
    body: impl FnOnce() -> Result<T, String>,
) -> RayCallbackResult {
    run_callback_with(
        context,
        rayrust_sys::RAY_CALLBACK_EXIT_OUTSIDE_ACTOR,
        body,
        into_actor_ptr::<T>,
    )
}

/// 在 `catch_unwind` 边界内执行 `#[actor]` 成员方法回调。
///
/// 若方法体内部调用了 [`crate::exit_actor`]，返回 `RAY_CALLBACK_INTENTIONAL_EXIT`。
pub fn run_member_callback<R: serde::Serialize>(
    context: &str,
    body: impl FnOnce() -> Result<R, String>,
) -> RayCallbackResult {
    run_callback_with(
        context,
        rayrust_sys::RAY_CALLBACK_INTENTIONAL_EXIT,
        body,
        |v| v,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    unsafe fn take_result(res: RayCallbackResult) -> (c_int, Vec<u8>) {
        let bytes = if res.data.is_null() || res.len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(res.data as *const u8, res.len).to_vec()
        };
        if !res.data.is_null() {
            libc::free(res.data as *mut libc::c_void);
        }
        (res.status, bytes)
    }

    #[test]
    fn unpack_args_checks_pointers_counts_and_element_buffers() {
        let err = unsafe { unpack_args(std::ptr::null(), 1, 1, "fn_a") }.unwrap_err();
        assert!(err.contains("null argument array pointer"), "got: {err}");

        let empty_entry = RayBytes {
            data: std::ptr::null(),
            len: 0,
        };
        let err = unsafe { unpack_args(&empty_entry, 1, 2, "fn_a") }.unwrap_err();
        assert!(err.contains("expected 2 argument(s), got 1"), "got: {err}");

        let err = unsafe { unpack_args(std::ptr::null(), 0, 1, "fn_a") }.unwrap_err();
        assert!(err.contains("expected 1 argument(s), got 0"), "got: {err}");

        // 零长度且 data 为空指针的条目合法，产出空切片。
        let ok_empty = unsafe { unpack_args(&empty_entry, 1, 1, "fn_a") }.unwrap();
        assert_eq!(ok_empty, vec![&[][..]]);

        // 非零长度且 data 为空指针必须报错。
        let bad_entry = RayBytes {
            data: std::ptr::null(),
            len: 4,
        };
        let err = unsafe { unpack_args(&bad_entry, 1, 1, "fn_a") }.unwrap_err();
        assert!(
            err.contains("null data pointer with length 4"),
            "got: {err}"
        );
    }

    #[test]
    fn deserialize_arg_failure_keeps_actor_mutex_unpoisoned() {
        let ptr = into_actor_ptr(10i64);
        let bad_bytes = crate::serialize::serialize(&"not-an-i64").unwrap();
        let bad_arg = RayBytes {
            data: bad_bytes.as_ptr() as *const c_char,
            len: bad_bytes.len(),
        };
        let res = run_member_callback("Actor::add", || {
            let raw = unsafe { unpack_args(&bad_arg, 1, 1, "Actor::add")? };
            let delta: i64 = deserialize_arg(raw[0], 0, "Actor::add")?;
            unsafe {
                with_actor_mut::<i64, i64>(ptr, "Actor::add", |v| {
                    *v += delta;
                    *v
                })
            }
        });
        let (status, bytes) = unsafe { take_result(res) };
        assert_eq!(status, rayrust_sys::RAY_CALLBACK_ERROR);
        let msg = String::from_utf8(bytes).unwrap();
        assert!(msg.contains("argument 0 of Actor::add"), "got: {msg}");

        // 紧接着合法调用照常修改状态，互斥锁未中毒。
        let val = unsafe {
            with_actor_mut::<i64, i64>(ptr, "Actor::add", |v| {
                *v += 5;
                *v
            })
        }
        .unwrap();
        assert_eq!(val, 15);
        unsafe { drop(Box::from_raw(ptr as *mut std::sync::Mutex<i64>)) };
    }

    #[test]
    fn run_callbacks_catch_all_panic_payloads_and_sanitize_nul() {
        // &'static str panic
        let (st, bytes) = unsafe {
            take_result(run_task_callback::<i64>("t_str", || {
                panic!("static str boom");
            }))
        };
        assert_eq!(st, rayrust_sys::RAY_CALLBACK_ERROR);
        assert!(String::from_utf8(bytes)
            .unwrap()
            .contains("panic in t_str: static str boom"));

        // String panic
        let (st, bytes) = unsafe {
            take_result(run_member_callback::<i64>("m_string", || {
                let n = 7;
                panic!("owned boom {n}");
            }))
        };
        assert_eq!(st, rayrust_sys::RAY_CALLBACK_ERROR);
        assert!(String::from_utf8(bytes)
            .unwrap()
            .contains("panic in m_string: owned boom 7"));

        // non-string panic_any(42i32)
        let (st, bytes) = unsafe {
            take_result(run_task_callback::<i64>("t_any", || {
                std::panic::panic_any(42i32);
            }))
        };
        assert_eq!(st, rayrust_sys::RAY_CALLBACK_ERROR);
        assert!(String::from_utf8(bytes)
            .unwrap()
            .contains("non-string panic payload"));

        // interior NUL byte in panic message is escaped, preserving suffix
        let (st, bytes) = unsafe {
            take_result(run_task_callback::<i64>("t_nul", || {
                panic!("before\0after");
            }))
        };
        assert_eq!(st, rayrust_sys::RAY_CALLBACK_ERROR);
        assert!(!bytes.contains(&0));
        let msg = String::from_utf8(bytes).unwrap();
        assert!(msg.contains("before\\0after"), "got: {msg}");

        // 调用 exit_actor() 之后又发生 panic：返回错误且同线程 EXIT_REQUESTED 干净。
        let (st, _) = unsafe {
            take_result(run_member_callback::<i64>("m_exit_then_panic", || {
                crate::actor_exit::exit_actor();
                panic!("boom after exit");
            }))
        };
        assert_eq!(st, rayrust_sys::RAY_CALLBACK_ERROR);
        let (st2, bytes2) =
            unsafe { take_result(run_member_callback::<i64>("m_clean", || Ok(99i64))) };
        assert_eq!(st2, rayrust_sys::RAY_CALLBACK_OK);
        let back: i64 = crate::serialize::deserialize(&bytes2).unwrap();
        assert_eq!(back, 99);

        // 自定义 panic 载荷在 Drop 内再次 panic!：仍安全返回 RAY_CALLBACK_ERROR。
        struct DropPanic;
        impl Drop for DropPanic {
            fn drop(&mut self) {
                panic!("second panic inside payload drop");
            }
        }
        let (st_drop_panic, _) = unsafe {
            take_result(run_task_callback::<i64>("t_drop_panic", || {
                std::panic::panic_any(DropPanic);
            }))
        };
        assert_eq!(st_drop_panic, rayrust_sys::RAY_CALLBACK_ERROR);

        // 调用 exit_actor() 后返回 Err("...")：错误优先于退出状态，且清除同线程退出标志。
        let (st_err, bytes_err) = unsafe {
            take_result(run_member_callback::<i64>("m_exit_then_err", || {
                crate::actor_exit::exit_actor();
                Err("explicit error after exit".to_string())
            }))
        };
        assert_eq!(st_err, rayrust_sys::RAY_CALLBACK_ERROR);
        assert!(String::from_utf8(bytes_err)
            .unwrap()
            .contains("explicit error after exit"));
        let (st_after_err, bytes_after_err) =
            unsafe { take_result(run_member_callback::<i64>("m_after_err", || Ok(77i64))) };
        assert_eq!(st_after_err, rayrust_sys::RAY_CALLBACK_OK);
        let val_after_err: i64 = crate::serialize::deserialize(&bytes_after_err).unwrap();
        assert_eq!(val_after_err, 77);
    }

    #[test]
    fn run_factory_callback_drops_instance_on_exit_request() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        struct DropTracked {
            drops: Arc<AtomicUsize>,
        }
        impl Drop for DropTracked {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let drops_clone = Arc::clone(&drops);
        let (status, bytes) = unsafe {
            take_result(run_factory_callback("Factory::new_exit", || {
                crate::actor_exit::exit_actor();
                Ok(DropTracked { drops: drops_clone })
            }))
        };
        assert_eq!(status, rayrust_sys::RAY_CALLBACK_EXIT_OUTSIDE_ACTOR);
        assert!(bytes.is_empty());
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        // 对照：未调用 exit_actor() 时返回 RAY_CALLBACK_OK，实例直到手动释放才析构。
        let drops_ok = Arc::new(AtomicUsize::new(0));
        let drops_ok_clone = Arc::clone(&drops_ok);
        let (status_ok, bytes_ok) = unsafe {
            take_result(run_factory_callback("Factory::new_ok", || {
                Ok(DropTracked {
                    drops: drops_ok_clone,
                })
            }))
        };
        assert_eq!(status_ok, rayrust_sys::RAY_CALLBACK_OK);
        assert_eq!(drops_ok.load(Ordering::SeqCst), 0);
        let actor_ptr: u64 = crate::serialize::deserialize(&bytes_ok).unwrap();
        unsafe {
            drop(Box::from_raw(
                actor_ptr as *mut std::sync::Mutex<DropTracked>,
            ))
        };
        assert_eq!(drops_ok.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn with_actor_ref_panic_also_poisons_mutex() {
        let ptr = into_actor_ptr(42i64);
        let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            with_actor_ref::<i64, ()>(ptr, "read_boom", |_| {
                panic!("inside with_actor_ref");
            })
        }));
        assert!(panic_res.is_err());

        let err_ref = unsafe { with_actor_ref::<i64, i64>(ptr, "after_ref", |v| *v) }.unwrap_err();
        assert!(err_ref.contains("poisoned"), "got: {err_ref}");
        let err_mut = unsafe { with_actor_mut::<i64, i64>(ptr, "after_mut", |v| *v) }.unwrap_err();
        assert!(err_mut.contains("poisoned"), "got: {err_mut}");

        unsafe { drop(Box::from_raw(ptr as *mut std::sync::Mutex<i64>)) };
    }

    #[test]
    fn with_actor_serializes_concurrent_access_and_reports_poison_and_bad_ptrs() {
        struct ProbeState {
            count: i64,
            in_flight: AtomicI64,
            peak: AtomicI64,
        }

        let ptr = into_actor_ptr(ProbeState {
            count: 0,
            in_flight: AtomicI64::new(0),
            peak: AtomicI64::new(0),
        });

        std::thread::scope(|s| {
            for t in 0..8 {
                s.spawn(move || {
                    for i in 0..200 {
                        if (t + i) % 4 == 0 {
                            unsafe {
                                with_actor_ref::<ProbeState, _>(ptr, "read", |st| {
                                    let cur = st.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                                    st.peak.fetch_max(cur, Ordering::SeqCst);
                                    std::hint::spin_loop();
                                    st.in_flight.fetch_sub(1, Ordering::SeqCst);
                                })
                                .unwrap();
                            }
                        } else {
                            unsafe {
                                with_actor_mut::<ProbeState, _>(ptr, "inc", |st| {
                                    let cur = st.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                                    st.peak.fetch_max(cur, Ordering::SeqCst);
                                    st.count += 1;
                                    st.in_flight.fetch_sub(1, Ordering::SeqCst);
                                })
                                .unwrap();
                            }
                        }
                    }
                });
            }
        });

        let (final_count, peak) = unsafe {
            with_actor_ref::<ProbeState, _>(ptr, "check", |st| {
                (st.count, st.peak.load(Ordering::SeqCst))
            })
            .unwrap()
        };
        assert_eq!(peak, 1, "并发进入数超出 1");
        assert_eq!(final_count, 1200);

        // 锁内 panic 使互斥锁中毒，后续读/写均返回包含 "poisoned" 的错误。
        let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            with_actor_mut::<ProbeState, ()>(ptr, "boom", |_| {
                panic!("inside mutex");
            })
        }));
        assert!(panic_res.is_err());

        let err_ref =
            unsafe { with_actor_ref::<ProbeState, i64>(ptr, "after", |s| s.count) }.unwrap_err();
        assert!(err_ref.contains("poisoned"), "got: {err_ref}");
        let err_mut = unsafe {
            with_actor_mut::<ProbeState, i64>(ptr, "after_mut", |s| {
                s.count += 1;
                s.count
            })
        }
        .unwrap_err();
        assert!(err_mut.contains("poisoned"), "got: {err_mut}");

        unsafe { drop(Box::from_raw(ptr as *mut std::sync::Mutex<ProbeState>)) };

        // 空指针与未对齐指针返回错误。
        let null_err = unsafe { with_actor_ref::<i64, i64>(0, "null_ctx", |v| *v) }.unwrap_err();
        assert!(null_err.contains("null"), "got: {null_err}");
        let unaligned_err =
            unsafe { with_actor_mut::<i64, i64>(1, "unaligned_ctx", |v| *v) }.unwrap_err();
        assert!(unaligned_err.contains("aligned"), "got: {unaligned_err}");
    }
}

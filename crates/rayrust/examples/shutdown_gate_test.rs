//! Example: `ray_shutdown` 关闭闸门在本地模式与集群模式下的行为。
//!
//! 一次运行只跑一种模式：
//! ```text
//! ./target/release/examples/shutdown_gate_test local
//! ./target/release/examples/shutdown_gate_test cluster
//! ```
//! 集群模式的环境：RAY_ADDRESS、RAY_NODE_IP。
//!
//! 机制：`ray_shutdown` 调用 `ray::Shutdown()`（`cpp/src/ray/api.cc:42-46`），进入
//! `AbstractRayRuntime::DoShutdown()`（`cpp/src/ray/runtime/abstract_ray_runtime.cc:72-77`）。
//! 集群模式下 `CoreWorkerProcess::Shutdown()`
//! （`src/ray/core_worker/core_worker_process.cc:101-108`）分两步销毁驱动端：先调
//! `ShutdownDriver()`（同文件 `:945-957`）把 `core_worker_` 置空，再执行
//! `core_worker_process.reset()`。两步之间 `CoreWorkerProcessImpl::GetCoreWorker()`
//! （同文件 `:964-984`）见到空 `core_worker_` 直接调 `QuickExit()`；`reset()` 之后
//! `CoreWorkerProcess::EnsureInitialized`（同文件 `:842-856`）同样调 `QuickExit()`
//! （`_Exit(1)`，`src/ray/util/process.cc:845-848`），`RAY_CATCH` 无法捕获。与此同时，
//! `RayRuntimeHolder::runtime_`（`cpp/include/ray/api/ray_runtime_holder.h:22-45`）在
//! `DoShutdown()` 里不会被清空，`GetRayRuntime()` 在关闭开始与结束之后仍返回非空指针。
//!
//! 为什么通过 `rayrust::sys` 直接调 C 入口：安全 API 的 `Ray` 方法需要持有活着的 `&Ray`
//! 句柄，而 `ObjectRef` 方法带有代次检查（`check_epoch`），句柄释放后在 Rust 层就提前
//! 返回，走不到 C 层；C 侧关闭闸门覆盖的是 C ABI 本身，以及多线程并发时「通过 Rust 层
//! 检查之后、进入 C 层之前」的竞争窗口。
//!
//! 检查项，每一项都打印实际结果：
//! 1. 前置对照：建立运行时，`put` 一个整数再 `get` 回来等于原值。
//! 2. 释放句柄后，`rayrust::sys::ray_is_initialized()` 返回 `false`。
//! 3. 29 个取运行时的 C 入口逐个调用：均返回空值或错误码，`ray_last_error_kind()` 为
//!    `RAY_ERROR_KIND_RUNTIME`（8，`ErrorKind::Runtime`），`ray_last_error_bytes()` 包含关闭
//!    拒绝消息。每个入口使用合法参数，排除参数校验提前拦截的干扰。
//! 4. 两个引用计数入口（`ray_add_local_reference`、`ray_remove_local_reference`）在关闭
//!    后返回 0，且 `ray_last_error_bytes()` 写出空指针与零长度。
//! 5. `ray_is_local_mode()` 以及指标构造/名称读取/销毁入口（`ray_gauge_create`、
//!    `ray_metric_get_name`、`ray_metric_destroy`）在关闭后不受关闭闸门拒绝且不记录错误。
//! 6. 关闭进行中的拒绝（仅在 local 模式运行）：提交一个 `sleep_for(1.5)` 任务，在另一
//!    个线程释放句柄触发本地排空，主线程等待 300 ms 后调用 `ray_put`、`ray_put_xlang` 与
//!    `ray_gauge_set`，验证排空期间已被关闭闸门以 `RAY_ERROR_KIND_RUNTIME` 拒绝，随后会合
//!    关闭线程并打印 `DROP_MS=`。
//! 7. 复位：再次建立运行时，`put`/`get` 成功，验证 `ray_init` 已将关闭标志清除。

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::time::{Duration, Instant};

use rayrust::prelude::*;
use rayrust::sys::{self, ErrorKind, RayActorHandleMeta, RayBytes};

const RAY_ERROR_KIND_RUNTIME_RAW: c_int = 8;
const EXPECTED_CLOSING_MESSAGE: &str =
    "the Ray runtime is shutting down or has shut down: calls are refused until a new runtime is initialized";

#[rayrust::remote]
fn sleep_for(seconds: f64) -> i64 {
    std::thread::sleep(Duration::from_secs_f64(seconds));
    42
}

fn fail(reason: String) -> ! {
    println!("  FAIL: {}", reason);
    std::process::exit(1);
}

fn cluster_config() -> RayConfig {
    let address = std::env::var("RAY_ADDRESS").expect("RAY_ADDRESS must be set");
    let node_ip = std::env::var("RAY_NODE_IP").expect("RAY_NODE_IP must be set");
    RayConfig::new(&address).node_ip(&node_ip)
}

/// 校验一次应当被关闭闸门拒绝的 C 调用，记录失败详情以供第 3 项汇总。
fn check_gate_rejection(name: &str, returned_empty: bool, failures: &mut Vec<String>) {
    let raw_kind = unsafe { sys::ray_last_error_kind() };
    let kind = sys::last_error_kind();
    let msg = sys::last_error();

    if !returned_empty {
        let detail = format!(
            "{}: returned non-empty result (raw_kind={}, msg={:?})",
            name, raw_kind, msg
        );
        println!("  FAIL {}", detail);
        failures.push(detail);
        return;
    }
    if raw_kind != RAY_ERROR_KIND_RUNTIME_RAW || kind != ErrorKind::Runtime {
        let detail = format!(
            "{}: error kind mismatch, expected raw={} ({:?}), got raw={} ({:?}), msg={:?}",
            name,
            RAY_ERROR_KIND_RUNTIME_RAW,
            ErrorKind::Runtime,
            raw_kind,
            kind,
            msg
        );
        println!("  FAIL {}", detail);
        failures.push(detail);
        return;
    }
    match msg {
        Some(ref text) if text.contains(EXPECTED_CLOSING_MESSAGE) => {
            println!("  {} -> refused (Runtime: {})", name, text);
        }
        other => {
            let detail = format!(
                "{}: error message mismatch, expected {:?}, got {:?}",
                name, EXPECTED_CLOSING_MESSAGE, other
            );
            println!("  FAIL {}", detail);
            failures.push(detail);
        }
    }
}

fn raw_str_bytes(s: &[u8]) -> RayBytes {
    RayBytes {
        data: s.as_ptr() as *const c_char,
        len: s.len(),
    }
}

fn empty_ray_bytes() -> RayBytes {
    RayBytes {
        data: std::ptr::null(),
        len: 0,
    }
}

fn check_29_entries() {
    let mut failures: Vec<String> = Vec::new();

    let put_payload: [u8; 2] = [0x91, 0x01];
    let put_xlang_payload = rayrust::serialize_xlang(&1i64).expect("serialize_xlang(1)");
    assert_eq!(put_xlang_payload.len(), 10);
    let obj_id: [u8; 28] = [0x01; 28];
    let actor_id: [u8; 16] = [0x02; 16];
    let pg_id: [u8; 18] = [0x03; 18];
    let obj_ray_bytes = RayBytes {
        data: obj_id.as_ptr() as *const c_char,
        len: obj_id.len(),
    };

    let c_f = CString::new("f").unwrap();
    let c_m = CString::new("m").unwrap();
    let c_cap_c = CString::new("C").unwrap();
    let c_g = CString::new("g").unwrap();
    let c_a = CString::new("a").unwrap();

    // 最小合法 `rpc.ActorHandle` protobuf：字段 1 (actor_id, 16 字节) + 字段 4 (creation_job_id, 4 字节)。
    let mut minimal_handle = Vec::with_capacity(24);
    minimal_handle.extend_from_slice(&[0x0a, 0x10]);
    minimal_handle.extend_from_slice(&[0x11u8; 16]);
    minimal_handle.extend_from_slice(&[0x22, 0x04]);
    minimal_handle.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]);

    // 1. ray_put
    sys::clear_error();
    let mut res = unsafe { sys::ray_put(put_payload.as_ptr() as *const c_char, put_payload.len()) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_put", empty, &mut failures);

    // 2. ray_put_xlang
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_put_xlang(
            put_xlang_payload.as_ptr() as *const c_char,
            put_xlang_payload.len(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_put_xlang", empty, &mut failures);

    // 3. ray_get
    sys::clear_error();
    let mut res = unsafe { sys::ray_get(obj_id.as_ptr() as *const c_char, obj_id.len(), 1000) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_get", empty, &mut failures);

    // 4. ray_wait
    sys::clear_error();
    let ptr = unsafe { sys::ray_wait(&obj_ray_bytes, 1, 1, 1000) };
    let empty = ptr.is_null();
    if !empty {
        unsafe { sys::ray_free_bools(ptr) };
    }
    check_gate_rejection("ray_wait", empty, &mut failures);

    // 5. ray_get_many
    sys::clear_error();
    let ptr = unsafe { sys::ray_get_many(&obj_ray_bytes, 1, 1000) };
    let empty = ptr.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes_array(ptr, 1) };
    }
    check_gate_rejection("ray_get_many", empty, &mut failures);

    // 6. ray_task_call_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_task_call_with_options(
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_task_call_with_options", empty, &mut failures);

    // 7. ray_task_call_python
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_task_call_python(
            c_m.as_ptr(),
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_task_call_python", empty, &mut failures);

    // 8. ray_task_call_python_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_task_call_python_with_options(
            c_m.as_ptr(),
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_task_call_python_with_options", empty, &mut failures);

    // 9. ray_actor_create_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_create_with_options(c_f.as_ptr(), std::ptr::null(), 0, std::ptr::null())
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_create_with_options", empty, &mut failures);

    // 10. ray_actor_create_python_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_create_python_with_options(
            c_m.as_ptr(),
            c_cap_c.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_create_python_with_options", empty, &mut failures);

    // 11. ray_actor_call
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_call(
            actor_id.as_ptr() as *const c_char,
            actor_id.len(),
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_call", empty, &mut failures);

    // 12. ray_actor_call_python
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_call_python(
            actor_id.as_ptr() as *const c_char,
            actor_id.len(),
            c_m.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_call_python", empty, &mut failures);

    // 13. ray_actor_kill
    sys::clear_error();
    unsafe {
        sys::ray_actor_kill(actor_id.as_ptr() as *const c_char, actor_id.len(), true);
    }
    check_gate_rejection("ray_actor_kill", true, &mut failures);

    // 14. ray_actor_handle_serialize
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_handle_serialize(actor_id.as_ptr() as *const c_char, actor_id.len())
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_handle_serialize", empty, &mut failures);

    // 15. ray_actor_handle_deserialize
    let mut meta = RayActorHandleMeta {
        language: 0,
        creation_descriptor: 0,
    };
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_handle_deserialize(
            minimal_handle.as_ptr() as *const c_char,
            minimal_handle.len(),
            &mut meta,
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_handle_deserialize", empty, &mut failures);

    // 16. ray_placement_group_create
    sys::clear_error();
    let mut res = unsafe { sys::ray_placement_group_create(std::ptr::null(), std::ptr::null(), 0) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_create", empty, &mut failures);

    // 17. ray_placement_group_remove
    sys::clear_error();
    let rc =
        unsafe { sys::ray_placement_group_remove(pg_id.as_ptr() as *const c_char, pg_id.len()) };
    check_gate_rejection("ray_placement_group_remove", rc == -1, &mut failures);

    // 18. ray_placement_group_wait_ready
    sys::clear_error();
    let rc = unsafe {
        sys::ray_placement_group_wait_ready(pg_id.as_ptr() as *const c_char, pg_id.len(), 0)
    };
    check_gate_rejection("ray_placement_group_wait_ready", rc == -1, &mut failures);

    // 19. ray_placement_group_get_all
    sys::clear_error();
    let mut res = unsafe { sys::ray_placement_group_get_all() };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_get_all", empty, &mut failures);

    // 20. ray_placement_group_get_by_id
    sys::clear_error();
    let mut res =
        unsafe { sys::ray_placement_group_get_by_id(pg_id.as_ptr() as *const c_char, pg_id.len()) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_get_by_id", empty, &mut failures);

    // 21. ray_placement_group_get_by_name
    sys::clear_error();
    let mut res = unsafe { sys::ray_placement_group_get_by_name(c_g.as_ptr()) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_get_by_name", empty, &mut failures);

    // 22. ray_was_current_actor_restarted
    sys::clear_error();
    let restarted = unsafe { sys::ray_was_current_actor_restarted() };
    check_gate_rejection("ray_was_current_actor_restarted", !restarted, &mut failures);

    // 23. ray_get_namespace
    sys::clear_error();
    let mut res = unsafe { sys::ray_get_namespace() };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_get_namespace", empty, &mut failures);

    // 24. ray_get_actor
    let mut meta2 = RayActorHandleMeta {
        language: 0,
        creation_descriptor: 0,
    };
    sys::clear_error();
    let mut res = unsafe { sys::ray_get_actor(c_a.as_ptr(), std::ptr::null(), &mut meta2) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_get_actor", empty, &mut failures);

    // 25. ray_cancel
    sys::clear_error();
    let rc =
        unsafe { sys::ray_cancel(obj_id.as_ptr() as *const c_char, obj_id.len(), true, false) };
    check_gate_rejection("ray_cancel", rc == -1, &mut failures);

    // 预先构造四个合法指标句柄，验证关闭闸门拒绝四个录制入口
    let hist_bounds = [1.0f64, 10.0f64];
    let gauge_h = unsafe {
        sys::ray_gauge_create(
            raw_str_bytes(b"gate_gauge"),
            empty_ray_bytes(),
            empty_ray_bytes(),
            std::ptr::null(),
            0,
        )
    };
    let counter_h = unsafe {
        sys::ray_counter_create(
            raw_str_bytes(b"gate_counter"),
            empty_ray_bytes(),
            empty_ray_bytes(),
            std::ptr::null(),
            0,
        )
    };
    let hist_h = unsafe {
        sys::ray_histogram_create(
            raw_str_bytes(b"gate_hist"),
            empty_ray_bytes(),
            empty_ray_bytes(),
            hist_bounds.as_ptr(),
            hist_bounds.len(),
            std::ptr::null(),
            0,
        )
    };
    let sum_h = unsafe {
        sys::ray_sum_create(
            raw_str_bytes(b"gate_sum"),
            empty_ray_bytes(),
            empty_ray_bytes(),
            std::ptr::null(),
            0,
        )
    };
    if gauge_h.is_null() || counter_h.is_null() || hist_h.is_null() || sum_h.is_null() {
        fail("failed to construct metric handles for shutdown gate test".to_string());
    }

    // 26. ray_gauge_set
    sys::clear_error();
    let rc = unsafe { sys::ray_gauge_set(gauge_h, 1.0, std::ptr::null(), 0) };
    check_gate_rejection("ray_gauge_set", rc == -1, &mut failures);

    // 27. ray_counter_inc
    sys::clear_error();
    let rc = unsafe { sys::ray_counter_inc(counter_h, 1.0, std::ptr::null(), 0) };
    check_gate_rejection("ray_counter_inc", rc == -1, &mut failures);

    // 28. ray_histogram_observe
    sys::clear_error();
    let rc = unsafe { sys::ray_histogram_observe(hist_h, 1.0, std::ptr::null(), 0) };
    check_gate_rejection("ray_histogram_observe", rc == -1, &mut failures);

    // 29. ray_metric_record
    sys::clear_error();
    let rc = unsafe { sys::ray_metric_record(sum_h, 1.0, std::ptr::null(), 0) };
    check_gate_rejection("ray_metric_record", rc == -1, &mut failures);

    unsafe {
        sys::ray_metric_destroy(gauge_h);
        sys::ray_metric_destroy(counter_h);
        sys::ray_metric_destroy(hist_h);
        sys::ray_metric_destroy(sum_h);
    }

    if !failures.is_empty() {
        fail(format!(
            "{} of 29 entries were not refused as expected: {}",
            failures.len(),
            failures.join("; ")
        ));
    }
    println!("  all 29 entries refused with RAY_ERROR_KIND_RUNTIME");
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "local".to_string());
    if mode != "local" && mode != "cluster" {
        fail(format!(
            "unknown mode {:?}, expected local or cluster",
            mode
        ));
    }
    let is_local = mode == "local";
    println!("=== shutdown_gate_test ({}) ===", mode);

    // 1. 前置对照
    println!("\n--- 1. Baseline: initialize runtime and put/get ---");
    let config = if is_local {
        RayConfig::local()
    } else {
        cluster_config()
    };
    let ray =
        Ray::connect(&config).unwrap_or_else(|e| fail(format!("initial connect failed: {}", e)));
    let obj = ray
        .put(&12345i64)
        .unwrap_or_else(|e| fail(format!("initial put failed: {}", e)));
    let val = obj
        .get()
        .unwrap_or_else(|e| fail(format!("initial get failed: {}", e)));
    if val != 12345 {
        fail(format!("initial get expected 12345, got {}", val));
    }
    println!("  put(12345) -> get() = {}", val);

    // 2. 丢掉句柄，检查 ray_is_initialized()
    println!("\n--- 2. After handle drop, ray_is_initialized() is false ---");
    drop(ray);
    let initialized = unsafe { sys::ray_is_initialized() };
    println!("  ray_is_initialized() = {}", initialized);
    if initialized {
        fail("ray_is_initialized() remained true after handle drop".to_string());
    }

    // 3. 29 个入口逐个调用
    println!("\n--- 3. Call all 29 entries after shutdown ---");
    check_29_entries();

    // 4. 引用计数两个入口不被拒
    println!("\n--- 4. Both ref-counting entries return 0 with no error ---");
    let ref_id = [0x01u8; 28];
    sys::clear_error();
    let rc_add =
        unsafe { sys::ray_add_local_reference(ref_id.as_ptr() as *const c_char, ref_id.len()) };
    let mut err_add: *const c_char = std::ptr::null();
    let mut err_add_len: usize = 0;
    unsafe { sys::ray_last_error_bytes(&mut err_add, &mut err_add_len) };
    println!(
        "  ray_add_local_reference -> rc={}, last_error_is_null={}, len={}",
        rc_add,
        err_add.is_null(),
        err_add_len
    );
    if rc_add != 0 || !err_add.is_null() || err_add_len != 0 {
        fail(format!(
            "ray_add_local_reference expected rc=0 and no error, got rc={}, len={}, err={:?}",
            rc_add,
            err_add_len,
            sys::last_error()
        ));
    }

    sys::clear_error();
    let rc_rem =
        unsafe { sys::ray_remove_local_reference(ref_id.as_ptr() as *const c_char, ref_id.len()) };
    let mut err_rem: *const c_char = std::ptr::null();
    let mut err_rem_len: usize = 0;
    unsafe { sys::ray_last_error_bytes(&mut err_rem, &mut err_rem_len) };
    println!(
        "  ray_remove_local_reference -> rc={}, last_error_is_null={}, len={}",
        rc_rem,
        err_rem.is_null(),
        err_rem_len
    );
    if rc_rem != 0 || !err_rem.is_null() || err_rem_len != 0 {
        fail(format!(
            "ray_remove_local_reference expected rc=0 and no error, got rc={}, len={}, err={:?}",
            rc_rem,
            err_rem_len,
            sys::last_error()
        ));
    }

    // 5. ray_is_local_mode() 仍然答得出
    println!("\n--- 5. ray_is_local_mode() stays readable without error ---");
    sys::clear_error();
    let reported_local = unsafe { sys::ray_is_local_mode() };
    let mut err_mode: *const c_char = std::ptr::null();
    let mut err_mode_len: usize = 0;
    unsafe { sys::ray_last_error_bytes(&mut err_mode, &mut err_mode_len) };
    println!(
        "  ray_is_local_mode() = {}, last_error_is_null={}, len={}",
        reported_local,
        err_mode.is_null(),
        err_mode_len
    );
    if reported_local != is_local || !err_mode.is_null() || err_mode_len != 0 {
        fail(format!(
            "ray_is_local_mode expected {} with no error, got {}, len={}, err={:?}",
            is_local,
            reported_local,
            err_mode_len,
            sys::last_error()
        ));
    }

    // 检查指标构造、名称读取与销毁不受关闭闸门拒绝且不留 last_error
    sys::clear_error();
    let post_gauge = unsafe {
        sys::ray_gauge_create(
            raw_str_bytes(b"post_close_gauge"),
            empty_ray_bytes(),
            empty_ray_bytes(),
            std::ptr::null(),
            0,
        )
    };
    let mut err_create: *const c_char = std::ptr::null();
    let mut err_create_len: usize = 0;
    unsafe { sys::ray_last_error_bytes(&mut err_create, &mut err_create_len) };
    if post_gauge.is_null() || !err_create.is_null() || err_create_len != 0 {
        fail(format!(
            "post-close ray_gauge_create failed or left error: ptr_is_null={}, len={}, err={:?}",
            post_gauge.is_null(),
            err_create_len,
            sys::last_error()
        ));
    }

    sys::clear_error();
    let raw_name = unsafe { sys::ray_metric_get_name(post_gauge) };
    let mut err_name: *const c_char = std::ptr::null();
    let mut err_name_len: usize = 0;
    unsafe { sys::ray_last_error_bytes(&mut err_name, &mut err_name_len) };
    let name_guard = sys::CBytesGuard::from(raw_name)
        .unwrap_or_else(|| fail("post-close ray_metric_get_name returned null".to_string()));
    if name_guard.as_slice() != b"post_close_gauge" || !err_name.is_null() || err_name_len != 0 {
        fail(format!(
            "post-close ray_metric_get_name unexpected result or error: got={:?}, len={}, err={:?}",
            std::str::from_utf8(name_guard.as_slice()),
            err_name_len,
            sys::last_error()
        ));
    }
    drop(name_guard);

    sys::clear_error();
    unsafe { sys::ray_metric_destroy(post_gauge) };
    let mut err_destroy: *const c_char = std::ptr::null();
    let mut err_destroy_len: usize = 0;
    unsafe { sys::ray_last_error_bytes(&mut err_destroy, &mut err_destroy_len) };
    if !err_destroy.is_null() || err_destroy_len != 0 {
        fail(format!(
            "post-close ray_metric_destroy left error: len={}, err={:?}",
            err_destroy_len,
            sys::last_error()
        ));
    }
    println!(
        "  ray_gauge_create / ray_metric_get_name / ray_metric_destroy succeeded after shutdown with no error"
    );

    // 检查三个 ABI/版本诊断入口在关闭后不受关闭闸门拒绝且不留错误
    sys::clear_error();
    let raw_build_ver = unsafe { sys::ray_build_sdk_version() };
    let build_ver_guard = sys::CBytesGuard::from(raw_build_ver)
        .unwrap_or_else(|| fail("post-close ray_build_sdk_version returned null".to_string()));
    if build_ver_guard.as_slice() != sys::BUILD_RAY_VERSION.as_bytes()
        || sys::last_error().is_some()
    {
        fail(format!(
            "post-close ray_build_sdk_version failed: err={:?}",
            sys::last_error()
        ));
    }
    drop(build_ver_guard);

    sys::clear_error();
    let raw_loaded_ver = unsafe { sys::ray_loaded_sdk_version() };
    let loaded_ver_guard = sys::CBytesGuard::from(raw_loaded_ver)
        .unwrap_or_else(|| fail("post-close ray_loaded_sdk_version returned null".to_string()));
    if loaded_ver_guard.as_slice() != sys::BUILD_RAY_VERSION.as_bytes()
        || sys::last_error().is_some()
    {
        fail(format!(
            "post-close ray_loaded_sdk_version failed: err={:?}",
            sys::last_error()
        ));
    }
    drop(loaded_ver_guard);

    let sym_fm = CString::new("_ZN3ray8internal18GetFunctionManagerEv").unwrap();
    sys::clear_error();
    let sym_rc = unsafe { sys::ray_check_private_symbol(sym_fm.as_ptr()) };
    if sym_rc != 0 || sys::last_error().is_some() {
        fail(format!(
            "post-close ray_check_private_symbol failed: rc={}, err={:?}",
            sym_rc,
            sys::last_error()
        ));
    }
    println!(
        "  ray_build_sdk_version / ray_loaded_sdk_version / ray_check_private_symbol succeeded after shutdown with no error"
    );

    // 6. 关闭进行中的拒绝（仅在 local 模式跑）
    println!("\n--- 6. Refusal during shutdown drain ---");
    if is_local {
        let drain_gauge = unsafe {
            sys::ray_gauge_create(
                raw_str_bytes(b"drain_gate_gauge"),
                empty_ray_bytes(),
                empty_ray_bytes(),
                std::ptr::null(),
                0,
            )
        };
        if drain_gauge.is_null() {
            fail("failed to construct case 6 drain_gate_gauge".to_string());
        }
        let ray_drain =
            Ray::local().unwrap_or_else(|e| fail(format!("case 6 Ray::local failed: {}", e)));
        let _pending = sleep_for_remote(&ray_drain, 1.5)
            .unwrap_or_else(|e| fail(format!("submit sleep_for(1.5) failed: {}", e)));
        let drop_thread = std::thread::spawn(move || {
            let t0 = Instant::now();
            drop(ray_drain);
            t0.elapsed().as_secs_f64() * 1000.0
        });

        std::thread::sleep(Duration::from_millis(300));
        let put_payload: [u8; 2] = [0x91, 0x01];
        sys::clear_error();
        let mut mid_res =
            unsafe { sys::ray_put(put_payload.as_ptr() as *const c_char, put_payload.len()) };
        let mid_empty = mid_res.data.is_null();
        if !mid_empty {
            unsafe { sys::ray_free_bytes(&mut mid_res) };
        }
        let mid_raw_kind = unsafe { sys::ray_last_error_kind() };
        let mid_kind = sys::last_error_kind();
        let mid_msg = sys::last_error();

        let put_xlang_payload = rayrust::serialize_xlang(&1i64).expect("serialize_xlang(1)");
        sys::clear_error();
        let mut mid_xlang_res = unsafe {
            sys::ray_put_xlang(
                put_xlang_payload.as_ptr() as *const c_char,
                put_xlang_payload.len(),
            )
        };
        let mid_xlang_empty = mid_xlang_res.data.is_null();
        if !mid_xlang_empty {
            unsafe { sys::ray_free_bytes(&mut mid_xlang_res) };
        }
        let mid_xlang_raw_kind = unsafe { sys::ray_last_error_kind() };
        let mid_xlang_kind = sys::last_error_kind();
        let mid_xlang_msg = sys::last_error();

        sys::clear_error();
        let mid_gauge_rc = unsafe { sys::ray_gauge_set(drain_gauge, 2.5, std::ptr::null(), 0) };
        let mid_gauge_raw_kind = unsafe { sys::ray_last_error_kind() };
        let mid_gauge_kind = sys::last_error_kind();
        let mid_gauge_msg = sys::last_error();
        unsafe { sys::ray_metric_destroy(drain_gauge) };

        let drop_ms = drop_thread.join().expect("join drop thread");
        println!("  DROP_MS={:.2}", drop_ms);

        if drop_ms < 1000.0 {
            fail(format!(
                "drain elapsed ({:.2} ms < 1000 ms), cannot confirm calls ran during drain",
                drop_ms
            ));
        }
        if !mid_empty {
            fail(
                "in-flight ray_put during drain was not refused (returned non-empty id)"
                    .to_string(),
            );
        }
        if mid_raw_kind != RAY_ERROR_KIND_RUNTIME_RAW || mid_kind != ErrorKind::Runtime {
            fail(format!(
                "in-flight ray_put error kind mismatch: raw={} ({:?}), msg={:?}",
                mid_raw_kind, mid_kind, mid_msg
            ));
        }
        match mid_msg {
            Some(ref text) if text.contains(EXPECTED_CLOSING_MESSAGE) => {
                println!("  ray_put during drain refused: Runtime({})", text);
            }
            other => fail(format!(
                "in-flight ray_put error message mismatch: {:?}",
                other
            )),
        }

        if !mid_xlang_empty {
            fail(
                "in-flight ray_put_xlang during drain was not refused (returned non-empty id)"
                    .to_string(),
            );
        }
        if mid_xlang_raw_kind != RAY_ERROR_KIND_RUNTIME_RAW || mid_xlang_kind != ErrorKind::Runtime
        {
            fail(format!(
                "in-flight ray_put_xlang error kind mismatch: raw={} ({:?}), msg={:?}",
                mid_xlang_raw_kind, mid_xlang_kind, mid_xlang_msg
            ));
        }
        match mid_xlang_msg {
            Some(ref text) if text.contains(EXPECTED_CLOSING_MESSAGE) => {
                println!("  ray_put_xlang during drain refused: Runtime({})", text);
            }
            other => fail(format!(
                "in-flight ray_put_xlang error message mismatch: {:?}",
                other
            )),
        }

        if mid_gauge_rc != -1 {
            fail(format!(
                "in-flight ray_gauge_set during drain was not refused (rc={})",
                mid_gauge_rc
            ));
        }
        if mid_gauge_raw_kind != RAY_ERROR_KIND_RUNTIME_RAW || mid_gauge_kind != ErrorKind::Runtime
        {
            fail(format!(
                "in-flight ray_gauge_set error kind mismatch: raw={} ({:?}), msg={:?}",
                mid_gauge_raw_kind, mid_gauge_kind, mid_gauge_msg
            ));
        }
        match mid_gauge_msg {
            Some(ref text) if text.contains(EXPECTED_CLOSING_MESSAGE) => {
                println!("  ray_gauge_set during drain refused: Runtime({})", text);
            }
            other => fail(format!(
                "in-flight ray_gauge_set error message mismatch: {:?}",
                other
            )),
        }
    } else {
        println!("  cluster mode shutdown does not wait for in-flight tasks; case 6 only runs in local mode");
    }

    // 7. 复位：再次建立运行时并 put/get
    println!("\n--- 7. Re-initialize to reset shutdown flag and put/get ---");
    let ray2 = Ray::connect(&config).unwrap_or_else(|e| fail(format!("reconnect failed: {}", e)));
    let obj2 = ray2
        .put(&67890i64)
        .unwrap_or_else(|e| fail(format!("put after reset failed: {}", e)));
    let val2 = obj2
        .get()
        .unwrap_or_else(|e| fail(format!("get after reset failed: {}", e)));
    if val2 != 67890 {
        fail(format!("get after reset expected 67890, got {}", val2));
    }
    println!("  reconnect succeeded: put(67890) -> get() = {}", val2);
    drop(ray2);

    println!("\nPASS: shutdown_gate_test ({}) all checks passed", mode);
}

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
//! 3. 24 个取运行时的 C 入口逐个调用：均返回空值或错误码，`ray_last_error_kind()` 为
//!    `RAY_ERROR_KIND_RUNTIME`（8，`ErrorKind::Runtime`），`ray_last_error()` 包含关闭
//!    拒绝消息。每个入口使用合法参数，排除参数校验提前拦截的干扰。
//! 4. 两个引用计数入口（`ray_add_local_reference`、`ray_remove_local_reference`）在关闭
//!    后返回 0，且 `ray_last_error()` 为空指针。
//! 5. `ray_is_local_mode()` 在关闭后照常返回对应模式的布尔值，且不记录错误。
//! 6. 关闭进行中的拒绝（仅在 local 模式运行）：提交一个 `sleep_for(1.5)` 任务，在另一
//!    个线程释放句柄触发本地排空，主线程等待 300 ms 后调用 `ray_put`，验证排空期间已被
//!    关闭闸门以 `RAY_ERROR_KIND_RUNTIME` 拒绝，随后会合关闭线程并打印 `DROP_MS=`。
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
            "{}: 返回了非空结果 (raw_kind={}, msg={:?})",
            name, raw_kind, msg
        );
        println!("  FAIL {}", detail);
        failures.push(detail);
        return;
    }
    if raw_kind != RAY_ERROR_KIND_RUNTIME_RAW || kind != ErrorKind::Runtime {
        let detail = format!(
            "{}: 错误类别不符, 预期 raw={} ({:?}), 实际 raw={} ({:?}), msg={:?}",
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
            println!("  {} → refused (Runtime: {})", name, text);
        }
        other => {
            let detail = format!(
                "{}: 错误消息不符, 预期包含 {:?}, 实际 {:?}",
                name, EXPECTED_CLOSING_MESSAGE, other
            );
            println!("  FAIL {}", detail);
            failures.push(detail);
        }
    }
}

fn check_24_entries() {
    let mut failures: Vec<String> = Vec::new();

    let put_payload: [u8; 2] = [0x91, 0x01];
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

    // 2. ray_get
    sys::clear_error();
    let mut res = unsafe { sys::ray_get(obj_id.as_ptr() as *const c_char, obj_id.len(), 1000) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_get", empty, &mut failures);

    // 3. ray_wait
    sys::clear_error();
    let ptr = unsafe { sys::ray_wait(&obj_ray_bytes, 1, 1, 1000) };
    let empty = ptr.is_null();
    if !empty {
        unsafe { sys::ray_free_bools(ptr) };
    }
    check_gate_rejection("ray_wait", empty, &mut failures);

    // 4. ray_get_many
    sys::clear_error();
    let ptr = unsafe { sys::ray_get_many(&obj_ray_bytes, 1, 1000) };
    let empty = ptr.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes_array(ptr, 1) };
    }
    check_gate_rejection("ray_get_many", empty, &mut failures);

    // 5. ray_task_call_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_task_call_with_options(
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_task_call_with_options", empty, &mut failures);

    // 6. ray_task_call_python
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_task_call_python(
            c_m.as_ptr(),
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_task_call_python", empty, &mut failures);

    // 7. ray_task_call_python_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_task_call_python_with_options(
            c_m.as_ptr(),
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_task_call_python_with_options", empty, &mut failures);

    // 8. ray_actor_create_with_options
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_create_with_options(c_f.as_ptr(), std::ptr::null(), 0, std::ptr::null())
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_create_with_options", empty, &mut failures);

    // 9. ray_actor_create_python_with_options
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

    // 10. ray_actor_call
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_call(
            actor_id.as_ptr() as *const c_char,
            actor_id.len(),
            c_f.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_call", empty, &mut failures);

    // 11. ray_actor_call_python
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_call_python(
            actor_id.as_ptr() as *const c_char,
            actor_id.len(),
            c_m.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_call_python", empty, &mut failures);

    // 12. ray_actor_kill
    sys::clear_error();
    unsafe {
        sys::ray_actor_kill(actor_id.as_ptr() as *const c_char, actor_id.len(), true);
    }
    check_gate_rejection("ray_actor_kill", true, &mut failures);

    // 13. ray_actor_handle_serialize
    sys::clear_error();
    let mut res = unsafe {
        sys::ray_actor_handle_serialize(actor_id.as_ptr() as *const c_char, actor_id.len())
    };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_actor_handle_serialize", empty, &mut failures);

    // 14. ray_actor_handle_deserialize
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

    // 15. ray_placement_group_create
    sys::clear_error();
    let mut res = unsafe { sys::ray_placement_group_create(std::ptr::null(), std::ptr::null(), 0) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_create", empty, &mut failures);

    // 16. ray_placement_group_remove
    sys::clear_error();
    let rc =
        unsafe { sys::ray_placement_group_remove(pg_id.as_ptr() as *const c_char, pg_id.len()) };
    check_gate_rejection("ray_placement_group_remove", rc == -1, &mut failures);

    // 17. ray_placement_group_wait_ready
    sys::clear_error();
    let rc = unsafe {
        sys::ray_placement_group_wait_ready(pg_id.as_ptr() as *const c_char, pg_id.len(), 0)
    };
    check_gate_rejection("ray_placement_group_wait_ready", rc == -1, &mut failures);

    // 18. ray_placement_group_get_all
    sys::clear_error();
    let mut res = unsafe { sys::ray_placement_group_get_all() };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_get_all", empty, &mut failures);

    // 19. ray_placement_group_get_by_id
    sys::clear_error();
    let mut res =
        unsafe { sys::ray_placement_group_get_by_id(pg_id.as_ptr() as *const c_char, pg_id.len()) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_get_by_id", empty, &mut failures);

    // 20. ray_placement_group_get_by_name
    sys::clear_error();
    let mut res = unsafe { sys::ray_placement_group_get_by_name(c_g.as_ptr()) };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_placement_group_get_by_name", empty, &mut failures);

    // 21. ray_was_current_actor_restarted
    sys::clear_error();
    let restarted = unsafe { sys::ray_was_current_actor_restarted() };
    check_gate_rejection("ray_was_current_actor_restarted", !restarted, &mut failures);

    // 22. ray_get_namespace
    sys::clear_error();
    let mut res = unsafe { sys::ray_get_namespace() };
    let empty = res.data.is_null();
    if !empty {
        unsafe { sys::ray_free_bytes(&mut res) };
    }
    check_gate_rejection("ray_get_namespace", empty, &mut failures);

    // 23. ray_get_actor
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

    // 24. ray_cancel
    sys::clear_error();
    let rc =
        unsafe { sys::ray_cancel(obj_id.as_ptr() as *const c_char, obj_id.len(), true, false) };
    check_gate_rejection("ray_cancel", rc == -1, &mut failures);

    if !failures.is_empty() {
        fail(format!(
            "24 个入口中有 {} 个未按预期拒绝: {}",
            failures.len(),
            failures.join("; ")
        ));
    }
    println!("  24 个入口全部以 RAY_ERROR_KIND_RUNTIME 拒绝");
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
    println!("\n--- 1. 前置对照：建立运行时并 put/get ---");
    let config = if is_local {
        RayConfig::local()
    } else {
        cluster_config()
    };
    let ray = Ray::connect(&config).unwrap_or_else(|e| fail(format!("首次连接失败: {}", e)));
    let obj = ray
        .put(&12345i64)
        .unwrap_or_else(|e| fail(format!("首次 put 失败: {}", e)));
    let val = obj
        .get()
        .unwrap_or_else(|e| fail(format!("首次 get 失败: {}", e)));
    if val != 12345 {
        fail(format!("首次 get 预期 12345, 实际 {}", val));
    }
    println!("  put(12345) -> get() = {}", val);

    // 2. 丢掉句柄，检查 ray_is_initialized()
    println!("\n--- 2. 释放句柄后 ray_is_initialized() 为 false ---");
    drop(ray);
    let initialized = unsafe { sys::ray_is_initialized() };
    println!("  ray_is_initialized() = {}", initialized);
    if initialized {
        fail("释放句柄后 ray_is_initialized() 仍为 true".to_string());
    }

    // 3. 24 个入口逐个调用
    println!("\n--- 3. 关闭后 24 个入口逐个调用 ---");
    check_24_entries();

    // 4. 引用计数两个入口不被拒
    println!("\n--- 4. 引用计数两个入口返回 0 且无错误 ---");
    let ref_id = [0x01u8; 28];
    sys::clear_error();
    let rc_add =
        unsafe { sys::ray_add_local_reference(ref_id.as_ptr() as *const c_char, ref_id.len()) };
    let err_add = unsafe { sys::ray_last_error() };
    println!(
        "  ray_add_local_reference -> rc={}, last_error_is_null={}",
        rc_add,
        err_add.is_null()
    );
    if rc_add != 0 || !err_add.is_null() {
        fail(format!(
            "ray_add_local_reference 预期 rc=0 且无错误, 实际 rc={}, err={:?}",
            rc_add,
            sys::last_error()
        ));
    }

    sys::clear_error();
    let rc_rem =
        unsafe { sys::ray_remove_local_reference(ref_id.as_ptr() as *const c_char, ref_id.len()) };
    let err_rem = unsafe { sys::ray_last_error() };
    println!(
        "  ray_remove_local_reference -> rc={}, last_error_is_null={}",
        rc_rem,
        err_rem.is_null()
    );
    if rc_rem != 0 || !err_rem.is_null() {
        fail(format!(
            "ray_remove_local_reference 预期 rc=0 且无错误, 实际 rc={}, err={:?}",
            rc_rem,
            sys::last_error()
        ));
    }

    // 5. ray_is_local_mode() 仍然答得出
    println!("\n--- 5. ray_is_local_mode() 保持可读且不报错 ---");
    sys::clear_error();
    let reported_local = unsafe { sys::ray_is_local_mode() };
    let err_mode = unsafe { sys::ray_last_error() };
    println!(
        "  ray_is_local_mode() = {}, last_error_is_null={}",
        reported_local,
        err_mode.is_null()
    );
    if reported_local != is_local || !err_mode.is_null() {
        fail(format!(
            "ray_is_local_mode 预期 {} 且无错误, 实际 {}, err={:?}",
            is_local,
            reported_local,
            sys::last_error()
        ));
    }

    // 6. 关闭进行中的拒绝（仅在 local 模式跑）
    println!("\n--- 6. 关闭排空进行中的拒绝 ---");
    if is_local {
        let ray_drain =
            Ray::local().unwrap_or_else(|e| fail(format!("第 6 项 Ray::local 失败: {}", e)));
        let _pending = sleep_for_remote(&ray_drain, 1.5)
            .unwrap_or_else(|e| fail(format!("提交 sleep_for(1.5) 失败: {}", e)));
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

        let drop_ms = drop_thread.join().expect("join drop thread");
        println!("  DROP_MS={:.2}", drop_ms);

        if drop_ms < 1000.0 {
            fail(format!(
                "排空耗时过短 ({:.2} ms < 1000 ms), 无法证明调用发生在排空期间",
                drop_ms
            ));
        }
        if !mid_empty {
            fail("排空进行中的 ray_put 未被拒绝（返回了非空 id）".to_string());
        }
        if mid_raw_kind != RAY_ERROR_KIND_RUNTIME_RAW || mid_kind != ErrorKind::Runtime {
            fail(format!(
                "排空进行中的 ray_put 错误类别不符: raw={} ({:?}), msg={:?}",
                mid_raw_kind, mid_kind, mid_msg
            ));
        }
        match mid_msg {
            Some(ref text) if text.contains(EXPECTED_CLOSING_MESSAGE) => {
                println!("  排空期间 ray_put 被拒绝: Runtime({})", text);
            }
            other => fail(format!("排空进行中的 ray_put 错误消息不符: {:?}", other)),
        }
    } else {
        println!("  cluster 模式关闭不等待在途任务，第 6 项仅在 local 模式运行");
    }

    // 7. 复位：再次建立运行时并 put/get
    println!("\n--- 7. 重新初始化复位关闭标志并 put/get ---");
    let ray2 = Ray::connect(&config).unwrap_or_else(|e| fail(format!("重新连接失败: {}", e)));
    let obj2 = ray2
        .put(&67890i64)
        .unwrap_or_else(|e| fail(format!("复位后 put 失败: {}", e)));
    let val2 = obj2
        .get()
        .unwrap_or_else(|e| fail(format!("复位后 get 失败: {}", e)));
    if val2 != 67890 {
        fail(format!("复位后 get 预期 67890, 实际 {}", val2));
    }
    println!("  重新连接成功: put(67890) -> get() = {}", val2);
    drop(ray2);

    println!("\nPASS: shutdown_gate_test ({}) 全部通过", mode);
}

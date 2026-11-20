//! #69 + #71：crate 自己记的 actor 登记表，以及按语言判定的入口。
//!
//! 这些检查要 `connect` Ray 的本地运行时，而运行时是进程级的：`crates/rayrust/src/runtime.rs`
//! 的单元测试里有一条断言「进程里没有运行时」
//! （`placement_group_remove_reports_the_c_entry_error`），跑在同一个二进制里会互相打掉，
//! 所以会 connect 的检查放在这个测试二进制里（`cargo test` 每个 tests/*.rs 是一个独立进程）。
//!
//! 检查项：
//! 1. 16 字节未登记 id 的 `actor_call` 报「没有登记」，进程与运行时照常工作；
//! 2. 3 字节与 0 字节 actor id 报宽度错误，宽度排在登记表之前；
//! 3. 登记表随运行时生命周期：`actor_create` 的 handle 在同一个运行时里被认出，`drop`
//!    `Ray` 之后同一个 id 变成未登记；
//! 4. 陈旧 handle 的 `ActorHandle::serialize` 报未登记（本地模式下这条检查排在本地模式
//!    拒绝之前）；
//! 5. 本地模式下 `task_call_python` 与已注册 Rust 函数同名时被拒，不提交任务。

use std::sync::{Mutex, MutexGuard};

use rayrust::prelude::*;

/// Ray 的运行时与登记表都是进程级的，libtest 又并行跑测试：这几个检查串行执行。
static RAY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_ray() -> MutexGuard<'static, ()> {
    RAY_TEST_LOCK.lock().expect("ray test lock poisoned")
}

/// 小写 hex，与 C 侧 `hex_encode` 的输出一致。
fn hex_of(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

fn local_ray() -> Ray {
    Ray::connect(&RayConfig::local()).expect("local Ray")
}

#[test]
fn actor_call_rejects_an_unregistered_actor_id() {
    let _lock = lock_ray();
    let ray = local_ray();
    let unknown = [0x11u8; 16];
    match ray.actor_call(&unknown, "__rayrust_actor_factory_counter::get", &[], &[]) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("is not registered in this process"),
                "未登记的 id 没有报未登记: {}",
                message
            );
            assert!(
                message.contains(&hex_of(&unknown)),
                "错误文本里没有 id 的 hex: {}",
                message
            );
        }
        Err(other) => panic!("未登记的 id 返回了别的错误变体: {}", other),
        Ok(_) => panic!("未登记的 id 被提交了"),
    }
    // 拒绝之后进程与运行时都还能用。
    let value = ray.put(&7i32).expect("put after the rejection");
    assert_eq!(
        value.get_timeout(5000).expect("get after the rejection"),
        7,
        "拒绝之后 put/get 读回的值不对"
    );
}

#[test]
fn actor_call_reports_a_short_actor_id_before_the_registry() {
    let _lock = lock_ray();
    let ray = local_ray();
    for (label, actor_id) in [("3 字节", vec![1u8, 2, 3]), ("0 字节", Vec::new())] {
        match ray.actor_call(&actor_id, "counter::increment", &[], &[]) {
            Err(RayError::Ffi(message)) => {
                assert!(
                    message.contains("actor id must be 16 bytes"),
                    "{} actor id 没有报宽度: {}",
                    label,
                    message
                );
                assert!(
                    message.contains(&format!("got {} bytes", actor_id.len())),
                    "{} actor id 的错误里没有实际长度: {}",
                    label,
                    message
                );
                assert!(
                    !message.contains("is not registered in this process"),
                    "{} actor id 先命中了登记表: {}",
                    label,
                    message
                );
            }
            Err(other) => panic!("{} actor id 返回了别的错误变体: {}", label, other),
            Ok(_) => panic!("{}的 actor id 被接受了", label),
        }
    }
}

/// 本地模式不查工厂函数是否存在：`CreateActor` 会返回 16 字节 id，登记表按创建入口的
/// 语言（C++）记下它。下面用「调用不被登记表拒」证明它在表里。
#[test]
fn the_registry_forgets_the_handles_of_a_dropped_runtime() {
    let _lock = lock_ray();
    let ray = local_ray();
    let handle = ray
        .actor_create("__rayrust_no_such_actor_factory", &[], &ActorOptions::new())
        .expect("actor_create in local mode");
    assert_eq!(handle.id().len(), 16, "actor id 的长度不是 16");
    // 同一个运行时里，调用要么被本地模式自己拒掉，要么提交；不能是「未登记」。
    match ray.actor_call(
        handle.id(),
        "__rayrust_no_such_actor_factory::get",
        &[],
        &[],
    ) {
        Err(RayError::Ffi(message)) => assert!(
            !message.contains("is not registered in this process"),
            "同一个运行时里的 handle 被登记表拒了: {}",
            message
        ),
        Err(other) => panic!("调用本地 actor 返回了别的错误变体: {}", other),
        Ok(_) => {}
    }
    let stale = handle.id().to_vec();
    drop(ray);
    let ray = local_ray();
    match ray.actor_call(&stale, "__rayrust_no_such_actor_factory::get", &[], &[]) {
        Err(RayError::Ffi(message)) => assert!(
            message.contains("is not registered in this process"),
            "重连之后的陈旧 handle 没有报未登记: {}",
            message
        ),
        Err(other) => panic!("陈旧 handle 返回了别的错误变体: {}", other),
        Ok(_) => panic!("重连之后的陈旧 handle 被提交了"),
    }
}

#[test]
fn serialize_reports_a_handle_from_a_dropped_runtime_as_unregistered() {
    let _lock = lock_ray();
    let ray = local_ray();
    let handle = ray
        .actor_create("__rayrust_no_such_actor_factory", &[], &ActorOptions::new())
        .expect("actor_create in local mode");
    let stale_id = handle.id().to_vec();
    drop(ray);
    // 重连一个本地运行时：登记表如果没有先拒，这个调用会走到 C 入口的本地模式拒绝。
    let _ray = local_ray();
    match handle.serialize() {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("is not registered in this process"),
                "陈旧 handle 的 serialize 没有报未登记: {}",
                message
            );
            assert!(
                !message.contains("local mode"),
                "本地模式拒绝排在了登记表之前: {}",
                message
            );
            assert!(
                message.contains(&hex_of(&stale_id)),
                "错误文本里没有 id 的 hex: {}",
                message
            );
        }
        Err(other) => panic!("陈旧 handle 返回了别的错误变体: {}", other),
        Ok(_) => panic!("陈旧 handle 被序列化了"),
    }
}

#[test]
fn task_call_python_rejects_a_name_registered_as_a_rust_function() {
    let _lock = lock_ray();
    // 这个回调永远不会被调用：拒绝发生在提交之前。
    extern "C" fn never_called(
        _args: *const rayrust::sys::RayBytes,
        _arg_count: usize,
    ) -> rayrust::sys::RayBytes {
        panic!("the registered function must not run")
    }
    let registered = "__rayrust_registered_rust_function";
    let name_c = rayrust::sys::to_cstring(registered);
    unsafe { rayrust::sys::ray_register_function(name_c.as_ptr(), never_called) };
    let ray = local_ray();
    match ray.task_call_python("some_module", registered, &[], &[]) {
        Err(RayError::Ffi(message)) => {
            assert!(
                message.contains("would run instead of the Python function"),
                "错误文本没有说明撞名: {}",
                message
            );
            assert!(
                message.contains(&format!("some_module.{}", registered)),
                "错误文本里没有模块与函数名: {}",
                message
            );
        }
        Err(other) => panic!("撞名的 task_call_python 返回了别的错误变体: {}", other),
        Ok(_) => panic!("撞名的 Python 任务被提交了"),
    }
}

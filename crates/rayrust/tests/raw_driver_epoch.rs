//! 原始 C 入口驱动的代次检查：进程里没有 `Ray::connect`，运行时由直接调用的
//! `ray_init` 建立。
//!
//! `LIVE_EPOCH` 在这个进程里恒为 0（只有 `Ray::connect` 会发布代次），所以从 id
//! 字节重建的引用带代次 0。放行代次 0 的条件必须限定到工人进程（本 .so 内联的
//! `RayRuntimeHolder` 为空、运行时由别的 DSO 建立）：普通驱动里的代次 0 引用仍要拒成
//! `RayError::Runtime`。
//!
//! 单独一个测试二进制：运行时是进程级的，`cargo test` 每个 `tests/*.rs` 是一个
//! 独立进程，这条检查要求 `ray_init` 之前进程里没有运行时。

use rayrust::prelude::*;

fn direct_init_local() -> i32 {
    unsafe {
        rayrust::sys::ray_init(
            std::ptr::null(),
            1,
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            std::ptr::null(),
        )
    }
}

fn raw_put_i64(value: i64) -> Vec<u8> {
    let data = serialize(&value).expect("serialize the value");
    rayrust::sys::clear_error();
    let bytes =
        unsafe { rayrust::sys::ray_put(data.as_ptr() as *const std::os::raw::c_char, data.len()) };
    let guard = rayrust::sys::CBytesGuard::from(bytes).unwrap_or_else(|| {
        panic!(
            "ray_put failed: {:?}",
            rayrust::sys::last_error().unwrap_or_else(|| "没有留下错误".to_string())
        )
    });
    guard.as_slice().to_vec()
}

/// 直接调 `ray_get`，证明写入本身是成功的（拒绝来自代次检查，与对象是否存在无关）。
fn raw_get_it(id: &[u8]) -> i64 {
    rayrust::sys::clear_error();
    let bytes = unsafe {
        rayrust::sys::ray_get(id.as_ptr() as *const std::os::raw::c_char, id.len(), 5000)
    };
    let guard = rayrust::sys::CBytesGuard::from(bytes).unwrap_or_else(|| {
        panic!(
            "ray_get failed: {:?}",
            rayrust::sys::last_error().unwrap_or_else(|| "没有留下错误".to_string())
        )
    });
    deserialize(guard.as_slice()).expect("decode the value ray_put wrote")
}

#[test]
fn raw_c_driver_rejects_a_rebuilt_reference_with_epoch_zero() {
    assert!(
        !unsafe { rayrust::sys::ray_is_initialized() },
        "这个测试要求进程里没有 Ray runtime"
    );

    assert_eq!(direct_init_local(), 0, "直接调 ray_init 失败了");
    assert!(
        unsafe { rayrust::sys::ray_is_initialized() },
        "ray_init 成功之后运行时仍未初始化"
    );
    assert_eq!(
        unsafe { rayrust::sys::ray_runtime_is_borrowed() },
        0,
        "本进程的运行时由自己的 ray_init 建立，不该被当成借来的"
    );

    let id = raw_put_i64(73);
    assert_eq!(id.len(), 28, "ray_put 交回的 id 不是 28 字节");
    assert_eq!(raw_get_it(&id), 73, "ray_put 写入的对象读不回来");

    // 代次为 0 的重建引用必须被拒：本进程的运行时不是别人建的。
    let rebuilt: ObjectRef<i64> = ObjectRef::from(id.clone());
    match rebuilt.get_timeout(1000) {
        Err(RayError::Runtime(message)) => {
            assert!(
                message.contains("epoch 0"),
                "拒绝消息里没有代次 0: {}",
                message
            );
            println!("RAW_DRIVER_zero_rebuilt_get -> Err(Runtime({}))", message);
        }
        Ok(value) => panic!("RAW_DRIVER_zero_rebuilt_get -> Ok({})", value),
        Err(other) => panic!("RAW_DRIVER_zero_rebuilt_get -> {:?}", other),
    }

    unsafe { rayrust::sys::ray_shutdown() };
    assert!(
        !unsafe { rayrust::sys::ray_is_initialized() },
        "ray_shutdown 之后运行时仍在"
    );
    match rebuilt.get_timeout(1000) {
        Err(RayError::Runtime(message)) => {
            println!("RAW_DRIVER_after_shutdown -> Err(Runtime({}))", message)
        }
        Ok(value) => panic!("RAW_DRIVER_after_shutdown -> Ok({})", value),
        Err(other) => panic!("RAW_DRIVER_after_shutdown -> {:?}", other),
    }
}

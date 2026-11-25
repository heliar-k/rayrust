//! 本地模式生命周期前后的内部 NUL 边界校验集成测试，以及 `register_member_function`
//! 对含内部 NUL 函数名的立即触发 panic 检查。

use std::sync::{Mutex, MutexGuard};

use rayrust::prelude::*;

static RAY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_ray() -> MutexGuard<'static, ()> {
    RAY_TEST_LOCK.lock().expect("ray test lock poisoned")
}

struct LocalCounter {
    value: i64,
}

#[rayrust::actor]
impl LocalCounter {
    fn new(start: i64) -> Self {
        LocalCounter { value: start }
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn get(&self) -> i64 {
        self.value
    }
}

fn c_namespace() -> Option<String> {
    let mut bytes = unsafe { rayrust::sys::ray_get_namespace() };
    if bytes.data.is_null() {
        return None;
    }
    let text = unsafe {
        let slice = std::slice::from_raw_parts(bytes.data as *const u8, bytes.len);
        std::str::from_utf8(slice)
            .expect("namespace returned from C entry must be valid UTF-8")
            .to_string()
    };
    unsafe { rayrust::sys::ray_free_bytes(&mut bytes) };
    Some(text)
}

fn assert_nul_ffi_error<T>(
    label: &str,
    result: Result<T, RayError>,
    expected_context: &str,
    expected_field: &str,
    expected_pos: usize,
) {
    match result {
        Err(RayError::Ffi(message)) => {
            let prefix = format!(
                "{}: {} contains an interior NUL byte at byte {}",
                expected_context, expected_field, expected_pos
            );
            assert!(
                message.starts_with(&prefix),
                "{}: error message {:?} did not start with {:?}",
                label,
                message,
                prefix
            );
            assert!(
                message.contains("cannot cross the C string FFI boundary"),
                "{}: error message missing FFI boundary note: {:?}",
                label,
                message
            );
        }
        Err(other) => panic!("{}: expected Err(RayError::Ffi), got {:?}", label, other),
        Ok(_) => panic!("{}: input with interior NUL returned Ok", label),
    }
}

fn rejected_config_cases() -> Vec<(&'static str, RayConfig, &'static str, usize)> {
    vec![
        (
            "local address",
            RayConfig {
                address: "127.0.0.1:6379\0bad".to_string(),
                local_mode: true,
                namespace: "uncommitted_ns".to_string(),
                ..Default::default()
            },
            "address",
            14,
        ),
        (
            "local node_ip",
            RayConfig::local()
                .namespace("uncommitted_ns")
                .node_ip("127.0.\0.1"),
            "node_ip",
            6,
        ),
        (
            "local code_search_path[1]",
            RayConfig::local()
                .namespace("uncommitted_ns")
                .code_search_path(vec!["/valid/a.so".to_string(), "/bad\0/b.so".to_string()]),
            "code_search_path[1]",
            4,
        ),
        (
            "local runtime_env",
            RayConfig::local()
                .namespace("uncommitted_ns")
                .runtime_env("{\"env_vars\":{}}\0"),
            "runtime_env",
            15,
        ),
        (
            "local log_dir",
            RayConfig::local()
                .namespace("uncommitted_ns")
                .log_dir("ray\0logs"),
            "log_dir",
            3,
        ),
        (
            "local namespace",
            RayConfig::local().namespace("ns\0bad"),
            "namespace",
            2,
        ),
        (
            "cluster address",
            RayConfig::new("127.0.0.1:6379\0bad").namespace("uncommitted_ns"),
            "address",
            14,
        ),
        (
            "cluster node_ip",
            RayConfig::new("127.0.0.1:6379")
                .namespace("uncommitted_ns")
                .node_ip("10.0.\0.1"),
            "node_ip",
            5,
        ),
        (
            "cluster code_search_path[0]",
            RayConfig::new("127.0.0.1:6379")
                .namespace("uncommitted_ns")
                .code_search_path(vec!["worker\0.so".to_string()]),
            "code_search_path[0]",
            6,
        ),
        (
            "cluster runtime_env",
            RayConfig::new("127.0.0.1:6379")
                .namespace("uncommitted_ns")
                .runtime_env("{\"pip\":[]}\0"),
            "runtime_env",
            10,
        ),
        (
            "cluster log_dir",
            RayConfig::new("127.0.0.1:6379")
                .namespace("uncommitted_ns")
                .log_dir("dir\0one"),
            "log_dir",
            3,
        ),
        (
            "cluster namespace",
            RayConfig::new("127.0.0.1:6379").namespace("cluster\0ns"),
            "namespace",
            7,
        ),
    ]
}

#[test]
#[should_panic(expected = "interior NUL byte")]
fn register_member_function_panics_on_interior_nul() {
    extern "C" fn dummy_cb(
        _actor_ptr: u64,
        _args: *const rayrust::sys::RayBytes,
        _arg_count: usize,
    ) -> rayrust::sys::RayCallbackResult {
        rayrust::sys::RayCallbackResult {
            status: rayrust::sys::RAY_CALLBACK_OK,
            data: std::ptr::null(),
            len: 0,
        }
    }

    rayrust::register_member_function("bad\0fn", dummy_cb);
}

#[test]
fn local_lifecycle_rejects_interior_nul_configs_and_calls_cleanly() {
    let _lock = lock_ray();

    // 1. 初始化前：进程内无运行时，`RayConfig` 全部 6 个字符串字段含内部 NUL 均被拒绝，
    //    不启动运行时、不写入本地模式 namespace。
    assert!(
        !unsafe { rayrust::sys::ray_is_initialized() },
        "initial state requires no active Ray runtime"
    );
    assert_eq!(c_namespace(), None);

    for (label, cfg, field, pos) in rejected_config_cases() {
        assert_nul_ffi_error(label, Ray::connect(&cfg), "ray_init", field, pos);
        assert!(
            !unsafe { rayrust::sys::ray_is_initialized() },
            "{}: runtime started despite rejection",
            label
        );
        assert_eq!(
            c_namespace(),
            None,
            "{}: readable namespace left behind despite rejection",
            label
        );
    }

    // 2. 正常 `Ray::local()` 初始化后，本地 Actor 调用含内部 NUL 的方法名在两个入口均被点名拒绝，
    //    原 Actor 后续调用与 `put`/`get` 保持正常。
    let ray = Ray::local().expect("Ray::local init");
    assert!(ray.is_local_mode());
    assert_eq!(ray.namespace().expect("local default namespace"), "");

    let handle =
        localcounter_actor_create(&ray, &ActorOptions::new(), 10).expect("create LocalCounter");
    let first = localcounter_increment_sync(&ray, &handle, 5)
        .expect("submit increment(5)")
        .get_timeout(5_000)
        .expect("read increment(5)");
    assert_eq!(first, 15);

    assert_nul_ffi_error(
        "actor_call with interior NUL",
        ray.actor_call(handle.id(), "bad\0method", &[], &[]),
        "ray_actor_call",
        "func_name",
        3,
    );
    assert_nul_ffi_error(
        "actor_call_python with interior NUL",
        ray.actor_call_python(handle.id(), "bad\0method", &[], &[]),
        "ray_actor_call_python",
        "method_name",
        3,
    );

    let async_res = rayrust::block_on_async(ray.actor_call_async(
        handle.id(),
        "bad\0async",
        Vec::new(),
        Vec::new(),
    ));
    assert_nul_ffi_error(
        "actor_call_async with interior NUL",
        async_res,
        "ray_actor_call",
        "func_name",
        3,
    );

    let after = localcounter_increment_sync(&ray, &handle, 7)
        .expect("submit increment(7) after NUL rejections")
        .get_timeout(5_000)
        .expect("read increment(7)");
    assert_eq!(after, 22);
    assert_eq!(
        localcounter_get_sync(&ray, &handle)
            .expect("submit get()")
            .get_timeout(5_000)
            .expect("read get()"),
        22
    );

    let obj = ray.put(&99i64).expect("put 99");
    assert_eq!(obj.get_timeout(5_000).expect("get 99"), 99);

    // 3. 关闭当前运行时后，再次验证 `RayConfig` 全部 6 个字段含内部 NUL 被拒绝且不改写状态，
    //    随后带自定义 namespace 重新初始化并正常关闭。
    drop(ray);
    assert!(
        !unsafe { rayrust::sys::ray_is_initialized() },
        "runtime still active after drop(ray)"
    );

    for (label, cfg, field, pos) in rejected_config_cases() {
        assert_nul_ffi_error(
            &format!("after shutdown: {}", label),
            Ray::connect(&cfg),
            "ray_init",
            field,
            pos,
        );
        assert!(
            !unsafe { rayrust::sys::ray_is_initialized() },
            "after shutdown {}: runtime started despite rejection",
            label
        );
        assert_eq!(
            c_namespace(),
            None,
            "after shutdown {}: namespace written despite rejection",
            label
        );
    }

    let ray2 = Ray::connect(&RayConfig::local().namespace("second_ns"))
        .expect("re-initialize local Ray with namespace");
    assert_eq!(ray2.namespace().expect("read second_ns"), "second_ns");
    assert_eq!(c_namespace().as_deref(), Some("second_ns"));
    let obj2 = ray2.put(&123i64).expect("put 123 on second runtime");
    assert_eq!(obj2.get_timeout(5_000).expect("get 123"), 123);
    drop(ray2);
    assert!(
        !unsafe { rayrust::sys::ray_is_initialized() },
        "runtime still active after drop(ray2)"
    );
}

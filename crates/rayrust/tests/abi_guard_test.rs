//! Integration tests for ABI probing, private `dlsym` symbol checks, SDK version
//! reporting, and C-side `is_ref_count` boundary checks across all five task and
//! actor call entry points.

use std::ffi::CString;
use std::os::raw::c_char;

use rayrust::sys::{self, CBytesGuard, ErrorKind, RayBytes};
use rayrust::Ray;

fn private_symbols_for_current_abi() -> [&'static str; 12] {
    let (create_gsa, get_internal_kv, status_to_string) = match sys::BUILD_GLIBCXX_CXX11_ABI {
        "0" => (
            "_ZN3ray8internal13ProcessHelper25CreateGlobalStateAccessorERKSsi",
            "_ZN3ray3gcs19GlobalStateAccessor13GetInternalKVERKSsS3_",
            "_ZNK3ray6Status8ToStringEv",
        ),
        "1" => (
            "_ZN3ray8internal13ProcessHelper25CreateGlobalStateAccessorERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEi",
            "_ZN3ray3gcs19GlobalStateAccessor13GetInternalKVERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES9_",
            "_ZNK3ray6Status8ToStringB5cxx11Ev",
        ),
        other => panic!("unexpected BUILD_GLIBCXX_CXX11_ABI value: {other}"),
    };
    [
        "_ZN3ray8internal18GetFunctionManagerEv",
        "_ZN3ray8internal18AbstractRayRuntime11GetInstanceEv",
        "_ZN3ray8internal18AbstractRayRuntime22GetGlobalStateAccessorEv",
        create_gsa,
        get_internal_kv,
        "_ZN3ray3gcs19GlobalStateAccessorD1Ev",
        "_ZN3ray4core17CoreWorkerProcess13IsInitializedEv",
        "_ZN3ray4core17CoreWorkerProcess13GetCoreWorkerEv",
        "_ZN3ray4core10CoreWorker3PutERKNS_9RayObjectERKSt6vectorINS_8ObjectIDESaIS6_EEPS6_",
        "_ZN3ray4core10CoreWorker10CancelTaskERKNS_8ObjectIDEbb",
        "_ZN3ray6StatusD1Ev",
        status_to_string,
    ]
}

#[test]
fn sdk_version_and_cxx11_abi_constants_match_loaded_library() {
    let sdk_ver = Ray::sdk_version();
    assert!(!sdk_ver.is_empty(), "Ray::sdk_version() must be non-empty");
    assert_eq!(sdk_ver, sys::BUILD_RAY_VERSION);

    assert!(
        sys::BUILD_GLIBCXX_CXX11_ABI == "0" || sys::BUILD_GLIBCXX_CXX11_ABI == "1",
        "BUILD_GLIBCXX_CXX11_ABI must be '0' or '1', got {:?}",
        sys::BUILD_GLIBCXX_CXX11_ABI
    );

    sys::clear_error();
    let raw_build = unsafe { sys::ray_build_sdk_version() };
    let build_guard =
        CBytesGuard::from(raw_build).expect("ray_build_sdk_version returned null data");
    assert_eq!(build_guard.as_slice(), sdk_ver.as_bytes());
    assert_eq!(sys::last_error(), None);

    let loaded_ver = Ray::loaded_sdk_version().expect("Ray::loaded_sdk_version() failed");
    assert_eq!(loaded_ver, sdk_ver);
}

#[test]
fn private_symbols_resolve_and_missing_or_empty_names_report_diagnostics() {
    for sym in private_symbols_for_current_abi() {
        let c_sym = CString::new(sym).expect("valid symbol CString");
        sys::clear_error();
        let rc = unsafe { sys::ray_check_private_symbol(c_sym.as_ptr()) };
        assert_eq!(
            rc,
            0,
            "expected private symbol {sym} to resolve, err={:?}",
            sys::last_error()
        );
        assert_eq!(sys::last_error(), None);
        assert_eq!(sys::last_error_kind(), ErrorKind::None);
    }

    // Missing symbol reports symbol name, loaded library path, and build SDK version.
    let missing_name = "_ZN3ray8internal25NonExistentPrivateSymbolEv";
    let missing_c = CString::new(missing_name).unwrap();
    sys::clear_error();
    let rc_missing = unsafe { sys::ray_check_private_symbol(missing_c.as_ptr()) };
    assert_eq!(rc_missing, -1);
    assert_eq!(sys::last_error_kind(), ErrorKind::Generic);
    let err_missing = sys::last_error().expect("missing symbol must record error");
    assert!(
        err_missing.contains(missing_name),
        "error should mention symbol name, got: {err_missing}"
    );
    assert!(
        err_missing.contains("libray_api"),
        "error should mention loaded SDK library path, got: {err_missing}"
    );
    assert!(
        err_missing.contains(Ray::sdk_version()),
        "error should mention build SDK version {}, got: {err_missing}",
        Ray::sdk_version()
    );

    // Empty C string is rejected.
    let empty_c = CString::new("").unwrap();
    sys::clear_error();
    let rc_empty = unsafe { sys::ray_check_private_symbol(empty_c.as_ptr()) };
    assert_eq!(rc_empty, -1);
    assert_eq!(sys::last_error_kind(), ErrorKind::Generic);
    let err_empty = sys::last_error().expect("empty symbol must record error");
    assert!(
        err_empty.contains("mangled_name is empty"),
        "unexpected error for empty symbol: {err_empty}"
    );

    // Null pointer is rejected.
    sys::clear_error();
    let rc_null = unsafe { sys::ray_check_private_symbol(std::ptr::null()) };
    assert_eq!(rc_null, -1);
    assert_eq!(sys::last_error_kind(), ErrorKind::Generic);
    let err_null = sys::last_error().expect("null symbol must record error");
    assert!(
        err_null.contains("mangled_name is null"),
        "unexpected error for null symbol: {err_null}"
    );
}

type RawCallFn = unsafe fn(*const RayBytes, usize, *const bool, usize) -> sys::RayBytes;

unsafe fn call_task_cpp(
    args: *const RayBytes,
    arg_count: usize,
    is_ref: *const bool,
    is_ref_count: usize,
) -> sys::RayBytes {
    let fn_name = CString::new("f").unwrap();
    sys::ray_task_call_with_options(
        fn_name.as_ptr(),
        args,
        arg_count,
        is_ref,
        is_ref_count,
        std::ptr::null(),
    )
}

unsafe fn call_task_py(
    args: *const RayBytes,
    arg_count: usize,
    is_ref: *const bool,
    is_ref_count: usize,
) -> sys::RayBytes {
    let mod_name = CString::new("m").unwrap();
    let fn_name = CString::new("f").unwrap();
    sys::ray_task_call_python(
        mod_name.as_ptr(),
        fn_name.as_ptr(),
        args,
        arg_count,
        is_ref,
        is_ref_count,
    )
}

unsafe fn call_task_py_opts(
    args: *const RayBytes,
    arg_count: usize,
    is_ref: *const bool,
    is_ref_count: usize,
) -> sys::RayBytes {
    let mod_name = CString::new("m").unwrap();
    let fn_name = CString::new("f").unwrap();
    sys::ray_task_call_python_with_options(
        mod_name.as_ptr(),
        fn_name.as_ptr(),
        args,
        arg_count,
        is_ref,
        is_ref_count,
        std::ptr::null(),
    )
}

unsafe fn call_actor_cpp(
    args: *const RayBytes,
    arg_count: usize,
    is_ref: *const bool,
    is_ref_count: usize,
) -> sys::RayBytes {
    let actor_id = [0x11u8; 16];
    let fn_name = CString::new("f").unwrap();
    sys::ray_actor_call(
        actor_id.as_ptr() as *const c_char,
        actor_id.len(),
        fn_name.as_ptr(),
        args,
        arg_count,
        is_ref,
        is_ref_count,
    )
}

unsafe fn call_actor_py(
    args: *const RayBytes,
    arg_count: usize,
    is_ref: *const bool,
    is_ref_count: usize,
) -> sys::RayBytes {
    let actor_id = [0x11u8; 16];
    let method_name = CString::new("m").unwrap();
    sys::ray_actor_call_python(
        actor_id.as_ptr() as *const c_char,
        actor_id.len(),
        method_name.as_ptr(),
        args,
        arg_count,
        is_ref,
        is_ref_count,
    )
}

#[test]
fn all_five_is_ref_entries_enforce_is_ref_count_and_args_bounds_in_c() {
    assert!(
        !unsafe { sys::ray_is_initialized() },
        "boundary checks must run without an initialized Ray runtime"
    );

    let entries: [(&str, RawCallFn); 5] = [
        ("ray_task_call_with_options", call_task_cpp),
        ("ray_task_call_python", call_task_py),
        ("ray_task_call_python_with_options", call_task_py_opts),
        ("ray_actor_call", call_actor_cpp),
        ("ray_actor_call_python", call_actor_py),
    ];

    let payload_a = [0x01u8];
    let payload_b = [0x02u8];
    let two_args = [
        RayBytes {
            data: payload_a.as_ptr() as *const c_char,
            len: payload_a.len(),
        },
        RayBytes {
            data: payload_b.as_ptr() as *const c_char,
            len: payload_b.len(),
        },
    ];
    let short_is_ref = [false; 1];
    let long_is_ref = [false; 3];

    for (entry_name, func) in entries {
        // 1. is_ref = null, is_ref_count = 1 -> rejected with "non-zero is_ref_count"
        sys::clear_error();
        let mut res = unsafe { func(std::ptr::null(), 0, std::ptr::null(), 1) };
        if !res.data.is_null() {
            unsafe { sys::ray_free_bytes(&mut res) };
            panic!("{entry_name} accepted null is_ref with non-zero is_ref_count");
        }
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic, "{entry_name}");
        let msg1 = sys::last_error().expect("error recorded");
        assert!(
            msg1.contains("non-zero is_ref_count"),
            "{entry_name} unexpected message: {msg1}"
        );

        // 2. is_ref != null, arg_count = 2, is_ref_count = 1 (short array)
        sys::clear_error();
        let mut res = unsafe {
            func(
                two_args.as_ptr(),
                two_args.len(),
                short_is_ref.as_ptr(),
                short_is_ref.len(),
            )
        };
        if !res.data.is_null() {
            unsafe { sys::ray_free_bytes(&mut res) };
            panic!("{entry_name} accepted short is_ref array");
        }
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic, "{entry_name}");
        let msg2 = sys::last_error().expect("error recorded");
        assert!(
            msg2.contains("is_ref_count (1) does not match arg_count (2)"),
            "{entry_name} unexpected message: {msg2}"
        );

        // 3. is_ref != null, arg_count = 1, is_ref_count = 3 (long array)
        sys::clear_error();
        let mut res = unsafe {
            func(
                two_args.as_ptr(),
                1,
                long_is_ref.as_ptr(),
                long_is_ref.len(),
            )
        };
        if !res.data.is_null() {
            unsafe { sys::ray_free_bytes(&mut res) };
            panic!("{entry_name} accepted overlong is_ref array");
        }
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic, "{entry_name}");
        let msg3 = sys::last_error().expect("error recorded");
        assert!(
            msg3.contains("is_ref_count (3) does not match arg_count (1)"),
            "{entry_name} unexpected message: {msg3}"
        );

        // 4. args = null, arg_count = 1, is_ref = null, is_ref_count = 0
        sys::clear_error();
        let mut res = unsafe { func(std::ptr::null(), 1, std::ptr::null(), 0) };
        if !res.data.is_null() {
            unsafe { sys::ray_free_bytes(&mut res) };
            panic!("{entry_name} accepted null args with arg_count = 1");
        }
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic, "{entry_name}");
        let msg4 = sys::last_error().expect("error recorded");
        assert!(
            msg4.contains("argument array is null"),
            "{entry_name} unexpected message: {msg4}"
        );

        assert!(
            !unsafe { sys::ray_is_initialized() },
            "{entry_name} must leave ray_is_initialized() false"
        );
    }
}

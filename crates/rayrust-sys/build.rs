use std::path::PathBuf;

fn main() {
    let sdk = rayrust_build::resolve_or_download();
    let lib_so = sdk.lib_dir().join("libray_api.so");

    // 探测 `libray_api.so` 中 `CoreWorker::CreateOwnedAndIncrementLocalRef` 的导出符号签名，
    // 以匹配上游 `src/ray/common/ray_object.h` 中 `ray::RayObject` 的尾部成员布局：
    // - Ray 2.49.0 / 2.51.1：尾部为 4 字节枚举 `rpc::TensorTransport`，符号不含 `St8optionalISsE`；
    // - Ray 2.55.0 / 2.60.0：尾部为两个 `std::optional<std::string>`，符号含 `St8optionalISsE`。
    const SYMBOL_PREFIX: &[u8] = b"_ZN3ray4core10CoreWorker31CreateOwnedAndIncrementLocalRefE";
    const OPTIONAL_STR_MARKER: &[u8] = b"St8optionalISsE";
    let so_bytes = std::fs::read(&lib_so)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", lib_so.display(), e));
    let matched_symbol = so_bytes
        .split(|&b| b == 0)
        .find(|sym| sym.starts_with(SYMBOL_PREFIX) && sym.iter().all(|&c| c.is_ascii_graphic()))
        .unwrap_or_else(|| {
            panic!(
                "failed to detect ray::RayObject layout from {}: symbol CoreWorker::CreateOwnedAndIncrementLocalRef not found",
                lib_so.display()
            )
        });
    let uses_optional_str = matched_symbol
        .windows(OPTIONAL_STR_MARKER.len())
        .any(|w| w == OPTIONAL_STR_MARKER);

    let wrapper_dir = PathBuf::from("wrapper");

    // ── Compile the C ABI wrapper (.cc) + worker export ────────
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .file(wrapper_dir.join("ray_c.cc"))
        .file(wrapper_dir.join("ray_worker_export.cc"))
        .include(&wrapper_dir)
        .include(sdk.include_dir());

    // Warnings are non-fatal
    build.warnings(false);

    // Ray C++ SDK (libray_api.so) is built with Bazel which sets
    // _GLIBCXX_USE_CXX11_ABI=0 for maximum compatibility. We must match.
    build.define("_GLIBCXX_USE_CXX11_ABI", "0");
    if uses_optional_str {
        build.define("RAYRUST_RAY_OBJECT_LAYOUT_OPTIONAL_STR", "1");
    } else {
        build.define("RAYRUST_RAY_OBJECT_LAYOUT_ENUM", "1");
    }

    build.compile("ray_c");

    // ── Link against libray_api.so ─────────────────────────────
    println!("cargo:rustc-link-search=native={}", sdk.lib_dir().display());
    println!("cargo:rustc-link-lib=dylib=ray_api");

    // ── Tell cargo where to find the wrapper header ─────────────
    println!("cargo:rerun-if-changed={}", lib_so.display());
    println!("cargo:rerun-if-changed=wrapper/ray_c.h");
    println!("cargo:rerun-if-changed=wrapper/ray_c.cc");
    println!("cargo:rerun-if-changed=wrapper/ray_worker_export.cc");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RAY_CPP_DIR");
    println!("cargo:rerun-if-env-changed=RAY_VERSION");
    println!("cargo:rerun-if-env-changed=RAYRUST_CACHE_DIR");
    println!("cargo:rerun-if-env-changed=RAYRUST_DOWNLOAD");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-env-changed=PWD");
    println!("cargo:rerun-if-env-changed=PATH");
    println!("cargo:rerun-if-env-changed=PYTHONPATH");
}

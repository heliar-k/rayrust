use std::path::PathBuf;

fn main() {
    let sdk = rayrust_build::resolve_or_download();

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

    build.compile("ray_c");

    // ── Link against libray_api.so ─────────────────────────────
    println!("cargo:rustc-link-search=native={}", sdk.lib_dir().display());
    println!("cargo:rustc-link-lib=dylib=ray_api");

    // ── Tell cargo where to find the wrapper header ─────────────
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

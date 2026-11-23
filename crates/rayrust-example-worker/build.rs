fn main() {
    let sdk = rayrust_build::resolve_or_download();
    let lib_dir = sdk.lib_dir();

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RAY_CPP_DIR");
    println!("cargo:rerun-if-env-changed=RAY_VERSION");
    println!("cargo:rerun-if-env-changed=RAYRUST_CACHE_DIR");
    println!("cargo:rerun-if-env-changed=RAYRUST_DOWNLOAD");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-env-changed=PWD");
    println!("cargo:rerun-if-env-changed=PATH");
    println!("cargo:rerun-if-env-changed=PYTHONPATH");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    // ── RPATH: ensure librayrust_worker.so can find libray_api.so at runtime ──
    //
    // Ray's default_worker dlopens librayrust_worker.so. The dynamic linker
    // searches for NEEDED libs using the loaded .so's RPATH, not the calling
    // process. Without RPATH, libray_api.so is not found.
    //
    // Two entries, searched in order:
    //   1. $ORIGIN — .so's own directory (deployment: copy libray_api.so alongside)
    //   2. absolute path to ray/cpp/lib (dev: same machine builds and runs)
    //
    // DT_RPATH (--disable-new-dtags) is used instead of DT_RUNPATH because
    // RPATH is inherited by transitive dlopen dependencies, while RUNPATH is not.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,$ORIGIN");
        println!("cargo:rustc-link-arg=-Wl,--disable-new-dtags");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }

    // Force libray_api.so into NEEDED.
    // Without --no-as-needed, the linker may drop libray_api.so from
    // the NEEDED list because no Rust code directly references its symbols.
    println!("cargo:rustc-link-arg-cdylib=-Wl,--no-as-needed");
    println!("cargo:rustc-link-arg-cdylib=-lray_api");
    println!("cargo:rustc-link-arg-cdylib=-Wl,--as-needed");
}

fn main() {
    let sdk = rayrust_build::resolve_or_download();
    println!("cargo:rerun-if-env-changed=RAY_CPP_DIR");
    println!("cargo:rerun-if-env-changed=RAY_VERSION");
    println!("cargo:rerun-if-env-changed=RAYRUST_CACHE_DIR");
    println!("cargo:rerun-if-env-changed=RAYRUST_DOWNLOAD");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-env-changed=PWD");
    println!("cargo:rerun-if-env-changed=PATH");
    println!("cargo:rerun-if-env-changed=PYTHONPATH");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-arg=-Wl,--disable-new-dtags");
        println!(
            "cargo:rustc-link-arg=-Wl,-rpath,{}",
            sdk.lib_dir().display()
        );
    } else {
        println!("cargo:warning=No RPATH set on this target OS; set LD_LIBRARY_PATH at run time");
    }
}

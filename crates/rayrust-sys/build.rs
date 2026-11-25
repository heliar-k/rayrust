use std::path::PathBuf;

fn symbol_matches_versioned(sym: &[u8], base: &[u8]) -> bool {
    sym.starts_with(base)
        && (sym.len() == base.len() || sym[base.len()] == b'@')
        && sym.iter().all(|&c| c.is_ascii_graphic())
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn main() {
    let sdk = rayrust_build::resolve_or_download();
    let lib_so = sdk.lib_dir().join("libray_api.so");

    let env_ray_cpp_dir_set = std::env::var_os("RAY_CPP_DIR").is_some_and(|v| !v.is_empty());
    let build_ray_version = match rayrust_build::probe_sdk_version(&sdk.cpp_dir) {
        Some(probed) => {
            if !env_ray_cpp_dir_set && probed != sdk.version {
                panic!(
                    "Ray C++ SDK version mismatch in {}: libray_api.so embeds Ray {probed}, expected Ray {}",
                    sdk.cpp_dir.display(),
                    sdk.version
                );
            }
            probed
        }
        None => {
            if env_ray_cpp_dir_set {
                println!(
                    "cargo:warning=Could not extract kRayVersion from {}; recording RAY_VERSION={} as RAYRUST_BUILD_RAY_VERSION",
                    lib_so.display(),
                    sdk.version
                );
            }
            sdk.version.clone()
        }
    };
    rayrust_build::check_minimum_ray_version(&build_ray_version).unwrap_or_else(|e| panic!("{e}"));

    let so_bytes = std::fs::read(&lib_so)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", lib_so.display(), e));

    // 探测 `libray_api.so` 中 `ray::RuntimeEnv::Deserialize` 的导出符号，
    // 确定 C++ 标准库 `std::string` 的 `_GLIBCXX_USE_CXX11_ABI` 取值。
    const DESERIALIZE_LEGACY: &[u8] = b"_ZN3ray10RuntimeEnv11DeserializeERKSs";
    const DESERIALIZE_CXX11: &[u8] =
        b"_ZN3ray10RuntimeEnv11DeserializeERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE";
    let mut has_legacy_abi = false;
    let mut has_cxx11_abi = false;
    for sym in so_bytes.split(|&b| b == 0) {
        if symbol_matches_versioned(sym, DESERIALIZE_LEGACY) {
            has_legacy_abi = true;
        }
        if symbol_matches_versioned(sym, DESERIALIZE_CXX11) {
            has_cxx11_abi = true;
        }
    }
    let glibcxx_cxx11_abi = match (has_legacy_abi, has_cxx11_abi) {
        (true, false) => "0",
        (false, true) => "1",
        (false, false) => panic!(
            "failed to detect std::string ABI from {}: symbol ray::RuntimeEnv::Deserialize not found",
            lib_so.display()
        ),
        (true, true) => panic!(
            "ambiguous std::string ABI in {}: both legacy and cxx11 ray::RuntimeEnv::Deserialize symbols are present",
            lib_so.display()
        ),
    };

    // 校验 `libray_api.so` 中 `CoreWorker::CreateOwnedAndIncrementLocalRef` 的导出符号签名，
    // 确认 `src/ray/common/ray_object.h` 中 `ray::RayObject` 尾部包含 `std::optional<std::string>`（136 字节，Ray >= 2.56.1）。
    const SYMBOL_PREFIX: &[u8] = b"_ZN3ray4core10CoreWorker31CreateOwnedAndIncrementLocalRefE";
    const OPTIONAL_STR_LEGACY: &[u8] = b"St8optionalISsE";
    const OPTIONAL_STR_CXX11: &[u8] =
        b"St8optionalINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEE";
    let matched_symbol = so_bytes
        .split(|&b| b == 0)
        .find(|sym| sym.starts_with(SYMBOL_PREFIX) && sym.iter().all(|&c| c.is_ascii_graphic()))
        .unwrap_or_else(|| {
            panic!(
                "failed to detect ray::RayObject layout from {}: symbol CoreWorker::CreateOwnedAndIncrementLocalRef not found",
                lib_so.display()
            )
        });
    let uses_optional_str = contains_subslice(matched_symbol, OPTIONAL_STR_LEGACY)
        || contains_subslice(matched_symbol, OPTIONAL_STR_CXX11);
    if !uses_optional_str {
        panic!(
            "unsupported ray::RayObject layout in {}: symbol CoreWorker::CreateOwnedAndIncrementLocalRef lacks optional<std::string> (rayrust requires Ray >= {})",
            lib_so.display(),
            rayrust_build::MIN_SUPPORTED_RAY_VERSION
        );
    }

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

    build.define("_GLIBCXX_USE_CXX11_ABI", glibcxx_cxx11_abi);
    build.define("RAYRUST_GLIBCXX_CXX11_ABI", glibcxx_cxx11_abi);
    build.define(
        "RAYRUST_BUILD_RAY_VERSION",
        format!("\"{build_ray_version}\"").as_str(),
    );

    build.compile("ray_c");

    println!("cargo:rustc-env=RAYRUST_BUILD_RAY_VERSION={build_ray_version}");
    println!("cargo:rustc-env=RAYRUST_GLIBCXX_CXX11_ABI={glibcxx_cxx11_abi}");

    // ── Link against libray_api.so ─────────────────────────────
    println!("cargo:rustc-link-search=native={}", sdk.lib_dir().display());
    println!("cargo:rustc-link-lib=dylib=ray_api");

    // ── Tell cargo where to find the wrapper header ─────────────
    println!("cargo:rerun-if-changed={}", lib_so.display());
    println!("cargo:rerun-if-changed=wrapper/ray_c.h");
    println!("cargo:rerun-if-changed=wrapper/ray_private_abi.h");
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

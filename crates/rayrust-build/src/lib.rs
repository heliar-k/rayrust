use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};

pub const DEFAULT_RAY_VERSION: &str = "2.51.1";

const FIX_HINT: &str = "Set RAY_CPP_DIR=/path/to/ray/cpp or install via `pip install ray[cpp]` \
(supported PyPI wheel platforms: linux/x86_64, linux/aarch64, macos/aarch64, windows/x86_64).";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sdk {
    pub cpp_dir: PathBuf,
}

impl Sdk {
    pub fn lib_dir(&self) -> PathBuf {
        self.cpp_dir.join("lib")
    }

    pub fn include_dir(&self) -> PathBuf {
        self.cpp_dir.join("include")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Env,
    Cache,
    Python,
    Download,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Env => f.write_str("RAY_CPP_DIR"),
            Source::Cache => f.write_str("cache"),
            Source::Python => f.write_str("python3"),
            Source::Download => f.write_str("download"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Args {
    pub ray_cpp_dir: Option<PathBuf>,
    pub ray_version: String,
    pub cache_root: PathBuf,
    pub python3: Option<PathBuf>,
}

fn find_workspace_root() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").ok()?);
    for ancestor in manifest_dir.ancestors().skip(1) {
        let cargo_toml = ancestor.join("Cargo.toml");
        if let Ok(contents) = fs::read_to_string(&cargo_toml) {
            if contents.contains("[workspace]") {
                return Some(ancestor.to_path_buf());
            }
        }
    }
    Some(manifest_dir)
}

fn env_base_dir() -> Option<PathBuf> {
    env::var("PWD")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(find_workspace_root)
}

fn resolve_relative(raw: &Path, base: Option<&Path>) -> PathBuf {
    match base {
        Some(base) if !raw.is_absolute() => base.join(raw),
        _ => raw.to_path_buf(),
    }
}

// Cargo runs build.rs with cwd = CARGO_MANIFEST_DIR, so relative env paths use PWD (the cargo invocation dir) instead.
fn resolve_env_path(raw: &str) -> PathBuf {
    resolve_relative(Path::new(raw), env_base_dir().as_deref())
}

pub fn from_env() -> Result<Args, String> {
    let ray_cpp_dir = env::var("RAY_CPP_DIR")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| resolve_env_path(&s));
    let ray_version = env::var("RAY_VERSION")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_RAY_VERSION.to_string());
    let cache_root = match env::var("RAYRUST_CACHE_DIR").ok().filter(|s| !s.is_empty()) {
        Some(dir) => resolve_env_path(&dir),
        None => match env::var("HOME").ok().filter(|s| !s.is_empty()) {
            Some(home) => PathBuf::from(home).join(".cache").join("rayrust"),
            None => {
                return Err(
                    "HOME is not set; please set RAYRUST_CACHE_DIR to specify the Ray C++ SDK cache directory"
                        .to_string(),
                );
            }
        },
    };
    Ok(Args {
        ray_cpp_dir,
        ray_version,
        cache_root,
        python3: Some(PathBuf::from("python3")),
    })
}

pub fn platform_tag(os: &str, arch: &str) -> Result<&'static str, String> {
    match (os, arch) {
        ("linux", "x86_64") => Ok("manylinux2014_x86_64"),
        ("linux", "aarch64") => Ok("manylinux2014_aarch64"),
        ("macos", "aarch64") => Ok("macosx_12_0_arm64"),
        ("windows", "x86_64") => Ok("win_amd64"),
        _ => Err(format!(
            "unsupported platform '{os}/{arch}' for PyPI ray-cpp wheels \
             (supported: linux/x86_64, linux/aarch64, macos/aarch64, windows/x86_64); \
             or set RAY_CPP_DIR to point to your own Ray C++ SDK"
        )),
    }
}

pub fn cache_dir(cache_root: &Path, version: &str, platform: &str) -> PathBuf {
    cache_root.join(format!("ray-cpp-{version}-{platform}"))
}

fn check_sdk_dir(dir: &Path) -> Result<(), String> {
    let header = dir.join("include").join("ray").join("api.h");
    if !header.is_file() {
        return Err(format!("missing {}", header.display()));
    }

    let lib_dir = dir.join("lib");
    let has_lib = fs::read_dir(&lib_dir)
        .ok()
        .map(|entries| {
            entries.flatten().any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("libray_api")
                    && entry.path().is_file()
            })
        })
        .unwrap_or(false);

    if !has_lib {
        return Err(format!("missing `libray_api*` in {}", lib_dir.display()));
    }

    Ok(())
}

pub fn validate(dir: &Path) -> Result<(), String> {
    check_sdk_dir(dir).map_err(|reason| {
        format!(
            "invalid Ray C++ SDK at {}: {reason}. {FIX_HINT}",
            dir.display()
        )
    })
}

fn build_sdk(dir: &Path) -> Result<Sdk, String> {
    check_sdk_dir(dir)?;
    let canonical = fs::canonicalize(dir).map_err(|e| {
        format!(
            "failed to canonicalize Ray C++ SDK path {}: {e}",
            dir.display()
        )
    })?;
    Ok(Sdk { cpp_dir: canonical })
}

pub fn version_matches(installed: &str, requested: &str) -> bool {
    let inst = installed.trim();
    let req = requested.trim();
    // Empty installed version means ray.__version__ is unavailable, so ABI compatibility cannot be verified.
    !inst.is_empty() && !req.is_empty() && inst == req
}

const PYTHON_PROBE_SCRIPT: &str = "\
import ray, os, sys
ver = getattr(ray, '__version__', '')
for p in sys.path:
    c = os.path.join(p, 'ray', 'cpp')
    h = os.path.join(c, 'include', 'ray', 'api.h')
    ld = os.path.join(c, 'lib')
    if os.path.exists(h) and os.path.isdir(ld) and any(f.startswith('libray_api') for f in os.listdir(ld)):
        print(f'{c}\\t{ver}'); break
";

fn probe_python(python3: &Path) -> Result<Option<(PathBuf, String)>, String> {
    let output = Command::new(python3)
        .args(["-c", PYTHON_PROBE_SCRIPT])
        .output()
        .map_err(|e| format!("failed to run {}: {e}", python3.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim();
        if detail.is_empty() {
            return Err(format!("exited with {}", output.status));
        }
        return Err(format!("exited with {}: {detail}", output.status));
    }
    let stdout = String::from_utf8(output.stdout).map_err(|e| format!("non-UTF-8 output: {e}"))?;
    let trimmed = stdout.trim_matches(&['\r', '\n'][..]);
    if trimmed.trim().is_empty() {
        return Ok(None);
    }
    let (dir, ver) = match trimmed.split_once('\t') {
        Some((d, v)) => (d.trim(), v.trim()),
        None => (trimmed.trim(), ""),
    };
    if dir.is_empty() {
        Ok(None)
    } else {
        Ok(Some((PathBuf::from(dir), ver.to_string())))
    }
}

pub fn resolve(args: &Args) -> Result<(Sdk, Source), String> {
    if let Some(ref dir) = args.ray_cpp_dir {
        return build_sdk(dir)
            .map(|sdk| (sdk, Source::Env))
            .map_err(|reason| format!("invalid RAY_CPP_DIR ({}): {reason}", dir.display()));
    }

    let mut checked = Vec::new();
    checked.push("RAY_CPP_DIR: not set".to_string());

    match platform_tag(env::consts::OS, env::consts::ARCH) {
        Ok(platform) => {
            let cached = cache_dir(&args.cache_root, &args.ray_version, platform);
            match build_sdk(&cached) {
                Ok(sdk) => return Ok((sdk, Source::Cache)),
                Err(e) => checked.push(format!("cache ({}): {e}", cached.display())),
            }
        }
        Err(e) => checked.push(format!("cache: skipped ({e})")),
    }

    match args.python3 {
        Some(ref py) => match probe_python(py) {
            Ok(Some((dir, installed_ver))) => {
                if version_matches(&installed_ver, &args.ray_version) {
                    match build_sdk(&dir) {
                        Ok(sdk) => return Ok((sdk, Source::Python)),
                        Err(e) => checked.push(format!("python3 ({}): {e}", py.display())),
                    }
                } else {
                    let shown = if installed_ver.trim().is_empty() {
                        "<unknown>"
                    } else {
                        installed_ver.trim()
                    };
                    checked.push(format!(
                        "python3 ({}): ray {shown} at {}, but RAY_VERSION={} was requested",
                        py.display(),
                        dir.display(),
                        args.ray_version.trim()
                    ));
                }
            }
            Ok(None) => checked.push(format!(
                "python3 ({}): ray/cpp not found in sys.path",
                py.display()
            )),
            Err(e) => checked.push(format!("python3 ({}): {e}", py.display())),
        },
        None => checked.push("python3: not configured".to_string()),
    }

    Err(not_found_error(&checked))
}

fn not_found_error(checked: &[String]) -> String {
    format!(
        "Ray C++ SDK not found.\nChecked:\n  - {}",
        checked.join("\n  - ")
    )
}

fn unsupported_platform_panic(resolve_err: &str) -> String {
    format!("{resolve_err}\nAuto-download needs a supported platform.\n{FIX_HINT}")
}

pub fn download(args: &Args, platform: &str) -> Result<Sdk, String> {
    let target_dir = cache_dir(&args.cache_root, &args.ray_version, platform);
    fs::create_dir_all(&args.cache_root).map_err(|e| {
        format!(
            "failed to create cache root {}: {e}",
            args.cache_root.display()
        )
    })?;

    let pid = process::id();
    let wheel_path = args
        .cache_root
        .join(format!("ray-{}-{pid}.whl", args.ray_version));
    let tmp_extract = args
        .cache_root
        .join(format!("ray-extract-{}-{pid}", args.ray_version));

    let result = download_into(
        &args.ray_version,
        platform,
        &wheel_path,
        &tmp_extract,
        &target_dir,
    );
    let _ = fs::remove_file(&wheel_path);
    let _ = fs::remove_dir_all(&tmp_extract);
    result
}

fn download_into(
    version: &str,
    platform: &str,
    wheel_path: &Path,
    tmp_extract: &Path,
    target_dir: &Path,
) -> Result<Sdk, String> {
    let wheel_url = fetch_wheel_url(version, platform)?;

    let dl_out = Command::new("curl")
        .args([
            "-sS",
            "--fail",
            "--connect-timeout",
            "60",
            "--retry",
            "3",
            "-L",
            "-o",
        ])
        .arg(wheel_path)
        .arg(&wheel_url)
        .output()
        .map_err(|e| format!("failed to run curl to download {wheel_url}: {e}"))?;

    if !dl_out.status.success() {
        return Err(format!(
            "failed to download Ray C++ wheel from {wheel_url} ({}): {}",
            dl_out.status,
            String::from_utf8_lossy(&dl_out.stderr).trim()
        ));
    }

    let _ = fs::remove_dir_all(tmp_extract);
    fs::create_dir_all(tmp_extract).map_err(|e| {
        format!(
            "failed to create temporary extract dir {}: {e}",
            tmp_extract.display()
        )
    })?;

    let unzip_out = Command::new("unzip")
        .args(["-q", "-o"])
        .arg(wheel_path)
        .args(["ray/cpp/*", "-d"])
        .arg(tmp_extract)
        .output()
        .map_err(|e| format!("failed to run unzip on {}: {e}", wheel_path.display()))?;

    if !unzip_out.status.success() {
        return Err(format!(
            "failed to extract ray/cpp/* from {} ({}): {}",
            wheel_path.display(),
            unzip_out.status,
            String::from_utf8_lossy(&unzip_out.stderr).trim()
        ));
    }

    let extracted_cpp = tmp_extract.join("ray").join("cpp");
    check_sdk_dir(&extracted_cpp)?;

    // Concurrent cold-start build scripts may download in parallel and race on rename; keep existing valid dir.
    if let Err(rename_err) = fs::rename(&extracted_cpp, target_dir) {
        if check_sdk_dir(target_dir).is_err() {
            return Err(format!(
                "failed to move extracted SDK from {} to {}: {rename_err}",
                extracted_cpp.display(),
                target_dir.display()
            ));
        }
    }

    build_sdk(target_dir)
}

fn fetch_wheel_url(version: &str, platform: &str) -> Result<String, String> {
    let api_url = format!("https://pypi.org/pypi/ray-cpp/{version}/json");
    let output = Command::new("curl")
        .args(["-sS", "--fail", "--connect-timeout", "30", "-L", &api_url])
        .output()
        .map_err(|e| format!("curl not available or failed when fetching {api_url}: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "failed to fetch PyPI metadata from {api_url} ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let json = String::from_utf8_lossy(&output.stdout);
    for pytag in ["cp312", "cp311", "cp310"] {
        let filename = format!("ray_cpp-{version}-{pytag}-{pytag}-{platform}.whl");
        if let Some(url) = extract_url_for_filename(&json, &filename) {
            return Ok(url);
        }
    }

    Err(format!(
        "no ray_cpp wheel found for version {version} on platform {platform} (tried cp312, cp311, cp310)"
    ))
}

fn extract_url_for_filename(json: &str, filename: &str) -> Option<String> {
    let needle = format!("\"filename\":\"{filename}\"");
    let idx = json
        .find(&needle)
        .or_else(|| json.find(&format!("\"filename\": \"{filename}\"")))
        .or_else(|| json.find(filename))?;
    let after = &json[idx..];
    let url_key = after.find("\"url\"")?;
    let after_url = &after[url_key..];
    let colon = after_url.find(':')?;
    let after_colon = &after_url[colon + 1..];
    let q1 = after_colon.find('"')?;
    let after_quote = &after_colon[q1 + 1..];
    let q2 = after_quote.find('"')?;
    Some(after_quote[..q2].to_string())
}

pub fn resolve_or_download() -> Sdk {
    let args = from_env().unwrap_or_else(|e| panic!("Failed to configure Ray C++ SDK lookup: {e}"));
    match resolve(&args) {
        Ok((sdk, source)) => {
            println!(
                "cargo:warning=Using Ray C++ SDK at {} (from {})",
                sdk.cpp_dir.display(),
                source
            );
            sdk
        }
        Err(resolve_err) => {
            if args.ray_cpp_dir.is_some() {
                panic!("{resolve_err}\n{FIX_HINT}");
            }
            let platform = match platform_tag(env::consts::OS, env::consts::ARCH) {
                Ok(p) => p,
                // resolve already recorded the platform reason in its checked list.
                Err(_) => panic!("{}", unsupported_platform_panic(&resolve_err)),
            };
            match download(&args, platform) {
                Ok(sdk) => {
                    println!(
                        "cargo:warning=Using Ray C++ SDK at {} (from {})",
                        sdk.cpp_dir.display(),
                        Source::Download
                    );
                    sdk
                }
                Err(dl_err) => {
                    panic!(
                        "{resolve_err}\nAuto-download of ray-cpp {} ({platform}) failed: {dl_err}\n{FIX_HINT}",
                        args.ray_version
                    );
                }
            }
        }
    }
}

pub fn require() -> Sdk {
    let args = from_env().unwrap_or_else(|e| panic!("Failed to configure Ray C++ SDK lookup: {e}"));
    match resolve(&args) {
        Ok((sdk, source)) => {
            println!(
                "cargo:warning=Using Ray C++ SDK at {} (from {})",
                sdk.cpp_dir.display(),
                source
            );
            sdk
        }
        Err(err) => panic!("{err}\n{FIX_HINT}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_case_dir(case_name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-data")
            .join(case_name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create test case dir");
        dir
    }

    fn populate_fake_sdk(dir: &Path) {
        let include_ray = dir.join("include").join("ray");
        let lib_dir = dir.join("lib");
        fs::create_dir_all(&include_ray).expect("create include/ray");
        fs::create_dir_all(&lib_dir).expect("create lib");
        fs::write(include_ray.join("api.h"), "// fake api.h\n").expect("write api.h");
        fs::write(lib_dir.join("libray_api.so"), b"fake").expect("write libray_api.so");
    }

    #[test]
    fn cache_dir_includes_version_and_platform() {
        let root = Path::new("cache-root");
        let dir = cache_dir(root, "2.51.1", "manylinux2014_x86_64");
        assert_eq!(
            dir,
            PathBuf::from("cache-root/ray-cpp-2.51.1-manylinux2014_x86_64")
        );
    }

    #[test]
    fn platform_tag_supported_and_unsupported() {
        assert_eq!(
            platform_tag("linux", "x86_64").unwrap(),
            "manylinux2014_x86_64"
        );
        assert_eq!(
            platform_tag("linux", "aarch64").unwrap(),
            "manylinux2014_aarch64"
        );
        assert_eq!(
            platform_tag("macos", "aarch64").unwrap(),
            "macosx_12_0_arm64"
        );
        assert_eq!(platform_tag("windows", "x86_64").unwrap(), "win_amd64");

        let err = platform_tag("macos", "x86_64").unwrap_err();
        assert!(err.contains("linux/x86_64"), "err was: {err}");
        assert!(err.contains("linux/aarch64"), "err was: {err}");
        assert!(err.contains("macos/aarch64"), "err was: {err}");
        assert!(err.contains("windows/x86_64"), "err was: {err}");
        assert!(err.contains("RAY_CPP_DIR"), "err was: {err}");
    }

    #[test]
    fn resolve_prefers_ray_cpp_dir_over_cache() {
        let base = test_case_dir("resolve_prefers_env");
        let env_sdk = base.join("env-sdk");
        let cache_root = base.join("cache");
        populate_fake_sdk(&env_sdk);

        let host_platform =
            platform_tag(env::consts::OS, env::consts::ARCH).expect("host platform supported");
        let cached_sdk = cache_dir(&cache_root, DEFAULT_RAY_VERSION, host_platform);
        populate_fake_sdk(&cached_sdk);

        let args = Args {
            ray_cpp_dir: Some(env_sdk.clone()),
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root: cache_root.clone(),
            python3: None,
        };
        let (sdk, source) = resolve(&args).expect("resolve env sdk");
        assert_eq!(source, Source::Env);
        assert_eq!(sdk.cpp_dir, fs::canonicalize(&env_sdk).unwrap());

        let cache_args = Args {
            ray_cpp_dir: None,
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root,
            python3: None,
        };
        let (sdk_cached, source_cached) = resolve(&cache_args).expect("resolve cached sdk");
        assert_eq!(source_cached, Source::Cache);
        assert_eq!(sdk_cached.cpp_dir, fs::canonicalize(&cached_sdk).unwrap());
    }

    #[test]
    fn resolve_canonicalizes_non_normalized_path() {
        let base = test_case_dir("resolve_canonicalize");
        let real_sdk = base.join("nested").join("sdk");
        populate_fake_sdk(&real_sdk);

        let unnormalized = base.join("nested").join("..").join("nested").join("sdk");
        let args = Args {
            ray_cpp_dir: Some(unnormalized),
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root: base.join("cache"),
            python3: None,
        };
        let (sdk, source) = resolve(&args).expect("resolve unnormalized sdk");
        assert_eq!(source, Source::Env);
        assert!(sdk.cpp_dir.is_absolute());
        assert_eq!(sdk.cpp_dir, fs::canonicalize(&real_sdk).unwrap());
    }

    #[test]
    fn resolve_relative_path_uses_pwd_base() {
        let base = test_case_dir("resolve_relative_base");
        populate_fake_sdk(&base.join("relative-sdk"));

        let resolved = resolve_relative(Path::new("relative-sdk"), Some(&base));
        assert_eq!(resolved, base.join("relative-sdk"));

        let sdk = build_sdk(&resolved).expect("build relative sdk");
        assert!(sdk.cpp_dir.is_absolute());
        assert_eq!(
            sdk.cpp_dir,
            fs::canonicalize(base.join("relative-sdk")).unwrap()
        );

        assert_eq!(
            resolve_relative(Path::new("relative-sdk"), None),
            PathBuf::from("relative-sdk")
        );
        assert_eq!(
            resolve_relative(&base.join("absolute-sdk"), Some(Path::new("other"))),
            base.join("absolute-sdk")
        );
    }

    #[test]
    fn resolve_invalid_ray_cpp_dir_errors_without_fallback() {
        let base = test_case_dir("resolve_invalid_env");
        let empty_env_sdk = base.join("empty-sdk");
        fs::create_dir_all(&empty_env_sdk).expect("create empty-sdk");

        let cache_root = base.join("cache");
        if let Ok(host_platform) = platform_tag(env::consts::OS, env::consts::ARCH) {
            populate_fake_sdk(&cache_dir(&cache_root, DEFAULT_RAY_VERSION, host_platform));
        }

        let args = Args {
            ray_cpp_dir: Some(empty_env_sdk.clone()),
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root,
            python3: None,
        };
        let err = resolve(&args).unwrap_err();
        assert!(
            err.contains(&empty_env_sdk.display().to_string()),
            "err should mention empty_env_sdk path, got: {err}"
        );
    }

    #[test]
    fn resolve_cache_requires_matching_platform() {
        let base = test_case_dir("cache_platform_mismatch");
        let cache_root = base.join("cache");
        let other_platform = if env::consts::OS == "macos" && env::consts::ARCH == "aarch64" {
            "manylinux2014_x86_64"
        } else {
            "macosx_12_0_arm64"
        };
        let foreign_sdk = cache_dir(&cache_root, DEFAULT_RAY_VERSION, other_platform);
        populate_fake_sdk(&foreign_sdk);

        let args = Args {
            ray_cpp_dir: None,
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root: cache_root.clone(),
            python3: None,
        };
        let err = resolve(&args).unwrap_err();
        let host_platform = platform_tag(env::consts::OS, env::consts::ARCH).unwrap();
        let expected = cache_dir(&cache_root, DEFAULT_RAY_VERSION, host_platform);
        assert!(
            err.contains(&expected.display().to_string()),
            "err should mention the host platform cache dir {}, got: {err}",
            expected.display()
        );
    }

    #[test]
    fn unsupported_platform_message_mentions_platform_once() {
        let plat_err = platform_tag("macos", "x86_64").unwrap_err();
        let checked = vec![
            "RAY_CPP_DIR: not set".to_string(),
            format!("cache: skipped ({plat_err})"),
            "python3 (python3): ray/cpp not found in sys.path".to_string(),
        ];
        let message = unsupported_platform_panic(&not_found_error(&checked));
        let expected = concat!(
            "Ray C++ SDK not found.\n",
            "Checked:\n",
            "  - RAY_CPP_DIR: not set\n",
            "  - cache: skipped (unsupported platform 'macos/x86_64' for PyPI ray-cpp wheels (supported: linux/x86_64, linux/aarch64, macos/aarch64, windows/x86_64); or set RAY_CPP_DIR to point to your own Ray C++ SDK)\n",
            "  - python3 (python3): ray/cpp not found in sys.path\n",
            "Auto-download needs a supported platform.\n",
            "Set RAY_CPP_DIR=/path/to/ray/cpp or install via `pip install ray[cpp]` (supported PyPI wheel platforms: linux/x86_64, linux/aarch64, macos/aarch64, windows/x86_64).",
        );
        assert_eq!(message, expected);
        assert_eq!(message.matches("unsupported platform").count(), 1);
    }

    #[test]
    fn validate_checks_header_and_library() {
        let base = test_case_dir("validate_checks");

        let missing_header = base.join("missing-header");
        fs::create_dir_all(missing_header.join("lib")).expect("create lib");
        fs::write(missing_header.join("lib").join("libray_api.so"), b"fake")
            .expect("write libray_api.so");
        let err_header = validate(&missing_header).unwrap_err();
        assert!(err_header.contains("api.h"), "err was: {err_header}");

        let missing_lib = base.join("missing-lib");
        fs::create_dir_all(missing_lib.join("include").join("ray")).expect("create include/ray");
        fs::write(
            missing_lib.join("include").join("ray").join("api.h"),
            "// header\n",
        )
        .expect("write api.h");
        fs::create_dir_all(missing_lib.join("lib")).expect("create empty lib");
        let err_lib = validate(&missing_lib).unwrap_err();
        assert!(err_lib.contains("libray_api"), "err was: {err_lib}");

        let valid_dir = base.join("valid");
        populate_fake_sdk(&valid_dir);
        validate(&valid_dir).expect("valid sdk passes");
    }

    #[test]
    fn version_matches_cases() {
        assert!(version_matches("2.51.1", "2.51.1"));
        assert!(!version_matches("2.51.1", "2.50.0"));
        assert!(!version_matches("", "2.51.1"));
        assert!(!version_matches("2.51.1", ""));
        assert!(!version_matches("", ""));
        assert!(!version_matches("   ", "2.51.1"));
        assert!(version_matches("  2.51.1\n", "2.51.1  "));
    }
}

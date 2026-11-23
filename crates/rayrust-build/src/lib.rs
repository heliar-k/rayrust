//! Build-time Ray C++ SDK locator and opt-in PyPI wheel fetcher.
//!
//! SDK lookup order:
//! 1. `RAY_CPP_DIR` (used as given; version is not checked)
//! 2. Shared cache `<cache_root>/ray-cpp-<version>-<platform>/` (with optional `.sha256` pin file)
//! 3. `python3` (`ray/cpp` on `sys.path` whose `ray.__version__` equals `RAY_VERSION`)
//! 4. Opt-in PyPI wheel download (`RAYRUST_DOWNLOAD=1`), verified against `digests.sha256`
//!
//! Runtime version handshake boundary (issue #12):
//! - The public headers shipped in `ray-cpp` (`include/ray/api.h`, `include/ray/api/*.h`) declare
//!   neither a version query nor a GCS entry point.
//! - `libray_api.so` does export C++ symbols for `ray::gcs::GlobalStateAccessor` (`Connect`,
//!   `GetInternalKV`, `GetAllNodeInfo`, etc.) and `GcsClient` (inspectable via
//!   `nm -D --defined-only libray_api.so | grep -E 'GlobalStateAccessor|GcsClient' | c++filt`),
//!   all versioned with `@@VERSION_1.0` as private symbols whose declarations are omitted from the
//!   SDK; calling them requires hand-written private declarations and matching the `std::string`
//!   ABI tracked in #19.
//! - No exported symbol carries the Ray version string
//!   (`nm -D --defined-only libray_api.so | grep -i version | grep -Ev 'VERSION_1.0|FLAGS_'`).
//! - Upstream records the cluster version on internal paths: GCS fills
//!   `CheckAliveReply.ray_version` in `GcsNodeManager::HandleCheckAlive`
//!   (`src/ray/gcs/gcs_node_manager.cc:154-166`, set at `:158`), which only the raylet calls
//!   (`src/ray/raylet/node_manager.cc:442`); Python reads cluster metadata from GCS internal KV in
//!   `get_cluster_metadata` (`ray/_common/usage/usage_lib.py:841-862`) and compares it against
//!   `ray.__version__` in `_check_ray_version` (`ray/scripts/scripts.py:69-79`).
//!
//! Automatic runtime cluster version comparison is therefore deferred to #19; build scripts emit a
//! `cargo:warning` recording the resolved SDK path, source, and Ray version for manual comparison
//! with `ray.__version__` on the cluster.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::env;
use std::fmt::{self, Write as _};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{self, Command};

pub const DEFAULT_RAY_VERSION: &str = "2.51.1";

const FIX_HINT: &str = "Set RAY_CPP_DIR=/path/to/ray/cpp, install via `pip install ray[cpp]`, \
or set RAYRUST_DOWNLOAD=1 to download the ray-cpp wheel from PyPI \
(supported PyPI wheel platforms: linux/x86_64, linux/aarch64, macos/aarch64, windows/x86_64).";

const FIX_HINT_NO_DOWNLOAD: &str =
    "Set RAY_CPP_DIR=/path/to/ray/cpp or install via `pip install ray[cpp]`.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sdk {
    pub cpp_dir: PathBuf,
    pub version: String,
    pub sha256: Option<String>,
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

pub fn format_warning(sdk: &Sdk, source: Source) -> String {
    let dir = sdk.cpp_dir.display();
    let detail = match (source, sdk.sha256.as_deref()) {
        (Source::Env, _) => format!("from {source}, version not checked"),
        (Source::Cache, Some(sha)) => {
            format!("from {source}, Ray {}, sha256 {sha}", sdk.version)
        }
        (Source::Cache, None) => format!("from {source}, Ray {}", sdk.version),
        (Source::Python, _) => format!("from {source}, Ray {}", sdk.version),
        (Source::Download, Some(sha)) => {
            format!("from {source}, Ray {}, sha256 {sha}", sdk.version)
        }
        (Source::Download, None) => format!("from {source}, Ray {}", sdk.version),
    };
    format!("cargo:warning=Using Ray C++ SDK at {dir} ({detail})")
}

#[derive(Debug, Clone)]
pub struct Args {
    pub ray_cpp_dir: Option<PathBuf>,
    pub ray_version: String,
    pub cache_root: PathBuf,
    pub python3: Option<PathBuf>,
    pub download: bool,
}

pub fn parse_download_flag(raw: Option<&str>) -> Result<bool, String> {
    let Some(raw) = raw else {
        return Ok(false);
    };
    if raw.is_empty() {
        return Ok(false);
    }
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(format!(
            "invalid RAYRUST_DOWNLOAD value '{raw}' (expected 1/true/yes or 0/false/no)"
        )),
    }
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
    let download = match env::var("RAYRUST_DOWNLOAD") {
        Ok(val) => parse_download_flag(Some(&val))?,
        Err(env::VarError::NotPresent) => parse_download_flag(None)?,
        Err(env::VarError::NotUnicode(os)) => {
            return Err(format!(
                "invalid RAYRUST_DOWNLOAD value '{}' (expected 1/true/yes or 0/false/no)",
                os.to_string_lossy()
            ));
        }
    };
    Ok(Args {
        ray_cpp_dir,
        ray_version,
        cache_root,
        python3: Some(PathBuf::from("python3")),
        download,
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

pub fn pin_file_path(cache_root: &Path, version: &str, platform: &str) -> PathBuf {
    cache_root.join(format!("ray-cpp-{version}-{platform}.sha256"))
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn format_hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

pub fn verify_sha256(path: &Path, expected_hex: &str) -> Result<(), String> {
    let mut file = fs::File::open(path).map_err(|e| {
        format!(
            "failed to open {} for sha256 verification: {e}",
            path.display()
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| {
            format!(
                "failed to read {} for sha256 verification: {e}",
                path.display()
            )
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let actual = format_hex_lower(&hasher.finalize());
    if !actual.eq_ignore_ascii_case(expected_hex) {
        return Err(format!(
            "sha256 mismatch for {}: expected {expected_hex}, got {actual}",
            path.display()
        ));
    }
    Ok(())
}

fn write_pin_file(pin_path: &Path, sha256: &str, pid: u32) -> Result<(), String> {
    let normalized = sha256.trim().to_ascii_lowercase();
    if !is_sha256_hex(&normalized) {
        return Err(format!(
            "invalid sha256 digest for {}: expected a 64-character sha256, got {sha256}",
            pin_path.display()
        ));
    }
    let tmp_path = PathBuf::from(format!("{}.tmp-{pid}", pin_path.display()));
    if let Err(e) = fs::write(&tmp_path, format!("{normalized}\n")) {
        let _ = fs::remove_file(&tmp_path);
        return Err(format!(
            "failed to write temporary pin file {}: {e}",
            tmp_path.display()
        ));
    }
    if let Err(e) = fs::rename(&tmp_path, pin_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(format!(
            "failed to move temporary pin file {} to {}: {e}",
            tmp_path.display(),
            pin_path.display()
        ));
    }
    Ok(())
}

fn read_pin_file(pin_path: &Path) -> Result<Option<String>, String> {
    let contents = match fs::read_to_string(pin_path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!(
                "failed to read pin file {path}: {e}. Delete {path} and rebuild; with RAYRUST_DOWNLOAD=1 the wheel is downloaded again.",
                path = pin_path.display()
            ));
        }
    };
    let trimmed = contents.trim_end_matches(&['\r', '\n'][..]);
    if !is_sha256_hex(trimmed) {
        return Err(format!(
            "invalid pin file {path}: expected a 64-character sha256, got {trimmed}. Delete {path} and rebuild; with RAYRUST_DOWNLOAD=1 the wheel is downloaded again.",
            path = pin_path.display()
        ));
    }
    Ok(Some(trimmed.to_ascii_lowercase()))
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

fn build_sdk(dir: &Path, version: String, sha256: Option<String>) -> Result<Sdk, String> {
    check_sdk_dir(dir)?;
    let canonical = fs::canonicalize(dir).map_err(|e| {
        format!(
            "failed to canonicalize Ray C++ SDK path {}: {e}",
            dir.display()
        )
    })?;
    Ok(Sdk {
        cpp_dir: canonical,
        version,
        sha256,
    })
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

#[derive(Debug)]
enum ResolveFailure {
    Fatal(String),
    NotFound(String),
}

impl ResolveFailure {
    fn into_message(self) -> String {
        match self {
            ResolveFailure::Fatal(msg) | ResolveFailure::NotFound(msg) => msg,
        }
    }
}

fn resolve_inner(
    args: &Args,
    report_download_disabled: bool,
) -> Result<(Sdk, Source), ResolveFailure> {
    if let Some(ref dir) = args.ray_cpp_dir {
        return build_sdk(dir, args.ray_version.clone(), None)
            .map(|sdk| (sdk, Source::Env))
            .map_err(|reason| {
                ResolveFailure::Fatal(format!(
                    "invalid RAY_CPP_DIR ({}): {reason}. {FIX_HINT}",
                    dir.display()
                ))
            });
    }

    let mut checked = Vec::new();
    checked.push("RAY_CPP_DIR: not set".to_string());

    let platform_supported = match platform_tag(env::consts::OS, env::consts::ARCH) {
        Ok(platform) => {
            let cached = cache_dir(&args.cache_root, &args.ray_version, platform);
            match build_sdk(&cached, args.ray_version.clone(), None) {
                Ok(mut sdk) => {
                    let pin_path = pin_file_path(&args.cache_root, &args.ray_version, platform);
                    sdk.sha256 = read_pin_file(&pin_path).map_err(ResolveFailure::Fatal)?;
                    return Ok((sdk, Source::Cache));
                }
                Err(e) => checked.push(format!("cache ({}): {e}", cached.display())),
            }
            true
        }
        Err(e) => {
            checked.push(format!("cache: skipped ({e})"));
            false
        }
    };

    match args.python3 {
        Some(ref py) => match probe_python(py) {
            Ok(Some((dir, installed_ver))) => {
                if version_matches(&installed_ver, &args.ray_version) {
                    let resolved_ver = installed_ver.trim().to_string();
                    match build_sdk(&dir, resolved_ver, None) {
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

    if report_download_disabled && platform_supported && !args.download {
        checked.push(format!(
            "download: disabled (set RAYRUST_DOWNLOAD=1 to download ray-cpp {} from PyPI)",
            args.ray_version
        ));
    }

    Err(ResolveFailure::NotFound(not_found_error(&checked)))
}

pub fn resolve(args: &Args) -> Result<(Sdk, Source), String> {
    resolve_inner(args, true).map_err(ResolveFailure::into_message)
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
    let pin_path = pin_file_path(&args.cache_root, &args.ray_version, platform);
    fs::create_dir_all(&args.cache_root).map_err(|e| {
        format!(
            "failed to create cache root {}: {e}",
            args.cache_root.display()
        )
    })?;

    let pid = process::id();
    let wheel_path = args
        .cache_root
        .join(format!("ray_cpp-{}-{platform}-{pid}.whl", args.ray_version));
    let tmp_extract = args.cache_root.join(format!(
        "ray_cpp-extract-{}-{platform}-{pid}",
        args.ray_version
    ));

    let result = download_into(
        &args.ray_version,
        platform,
        pid,
        &wheel_path,
        &tmp_extract,
        &target_dir,
        &pin_path,
    );
    let _ = fs::remove_file(&wheel_path);
    let _ = fs::remove_dir_all(&tmp_extract);
    result
}

fn download_into(
    version: &str,
    platform: &str,
    pid: u32,
    wheel_path: &Path,
    tmp_extract: &Path,
    target_dir: &Path,
    pin_path: &Path,
) -> Result<Sdk, String> {
    let index_json = fetch_index(version)?;
    let wheel = select_wheel(&index_json, version, platform)?;

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
        .arg(&wheel.url)
        .output()
        .map_err(|e| format!("failed to run curl to download {}: {e}", wheel.url))?;

    if !dl_out.status.success() {
        return Err(format!(
            "failed to download Ray C++ wheel from {} ({}): {}",
            wheel.url,
            dl_out.status,
            String::from_utf8_lossy(&dl_out.stderr).trim()
        ));
    }

    verify_sha256(wheel_path, &wheel.sha256)?;

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

    move_extracted_sdk(&extracted_cpp, target_dir)?;

    write_pin_file(pin_path, &wheel.sha256, pid)?;
    build_sdk(target_dir, version.to_string(), Some(wheel.sha256))
}

fn move_extracted_sdk(extracted_cpp: &Path, target_dir: &Path) -> Result<(), String> {
    // Concurrent cold-start build scripts may download in parallel and race on rename; keep existing valid dir.
    if let Err(rename_err) = fs::rename(extracted_cpp, target_dir) {
        if check_sdk_dir(target_dir).is_err() {
            if target_dir.exists() {
                return Err(format!(
                    "failed to move extracted SDK from {} to {}: {rename_err}; {} exists but is not a usable SDK — delete it and rebuild",
                    extracted_cpp.display(),
                    target_dir.display(),
                    target_dir.display()
                ));
            }
            return Err(format!(
                "failed to move extracted SDK from {} to {}: {rename_err}",
                extracted_cpp.display(),
                target_dir.display()
            ));
        }
    }
    Ok(())
}

fn fetch_index(version: &str) -> Result<String, String> {
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

    String::from_utf8(output.stdout)
        .map_err(|e| format!("non-UTF-8 PyPI metadata from {api_url}: {e}"))
}

#[derive(Deserialize)]
struct PypiIndex {
    urls: Vec<IndexFile>,
}

#[derive(Deserialize)]
struct IndexFile {
    filename: String,
    url: String,
    #[serde(default)]
    digests: Digests,
}

#[derive(Default, Deserialize)]
struct Digests {
    sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wheel {
    pub filename: String,
    pub url: String,
    pub sha256: String,
}

const WHEEL_TAGS: &[&str] = &["py3-none", "cp312-cp312", "cp311-cp311", "cp310-cp310"];

pub fn select_wheel(json: &str, version: &str, platform: &str) -> Result<Wheel, String> {
    let index: PypiIndex = serde_json::from_str(json)
        .map_err(|e| format!("failed to parse PyPI metadata for ray-cpp {version}: {e}"))?;

    for tag in WHEEL_TAGS {
        let filename = format!("ray_cpp-{version}-{tag}-{platform}.whl");
        if let Some(file) = index.urls.iter().find(|u| u.filename == filename) {
            let sha256 = file
                .digests
                .sha256
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("PyPI metadata for {} has no sha256 digest", file.filename))?
                .to_ascii_lowercase();
            return Ok(Wheel {
                filename: file.filename.clone(),
                url: file.url.clone(),
                sha256,
            });
        }
    }

    Err(format!(
        "no ray_cpp wheel found for version {version} on platform {platform} (tried {})",
        WHEEL_TAGS.join(", ")
    ))
}

pub fn resolve_or_download() -> Sdk {
    let args = from_env().unwrap_or_else(|e| panic!("Failed to configure Ray C++ SDK lookup: {e}"));
    match resolve_inner(&args, true) {
        Ok((sdk, source)) => {
            println!("{}", format_warning(&sdk, source));
            sdk
        }
        Err(ResolveFailure::Fatal(err)) => {
            panic!("{err}");
        }
        Err(ResolveFailure::NotFound(resolve_err)) => {
            if !args.download {
                panic!("{resolve_err}\n{FIX_HINT}");
            }
            let platform = match platform_tag(env::consts::OS, env::consts::ARCH) {
                Ok(p) => p,
                // resolve already recorded the platform reason in its checked list.
                Err(_) => panic!("{}", unsupported_platform_panic(&resolve_err)),
            };
            match download(&args, platform) {
                Ok(sdk) => {
                    println!("{}", format_warning(&sdk, Source::Download));
                    sdk
                }
                Err(dl_err) => {
                    panic!(
                        "{resolve_err}\nDownload of ray-cpp {} ({platform}) failed: {dl_err}\n{FIX_HINT}",
                        args.ray_version
                    );
                }
            }
        }
    }
}

fn format_require_failure(failure: ResolveFailure) -> String {
    match failure {
        ResolveFailure::Fatal(err) => err,
        ResolveFailure::NotFound(err) => format!("{err}\n{FIX_HINT_NO_DOWNLOAD}"),
    }
}

pub fn require() -> Sdk {
    let args = from_env().unwrap_or_else(|e| panic!("Failed to configure Ray C++ SDK lookup: {e}"));
    match resolve_inner(&args, false) {
        Ok((sdk, source)) => {
            println!("{}", format_warning(&sdk, source));
            sdk
        }
        Err(failure) => panic!("{}", format_require_failure(failure)),
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
            download: false,
        };
        let (sdk, source) = resolve(&args).expect("resolve env sdk");
        assert_eq!(source, Source::Env);
        assert_eq!(sdk.cpp_dir, fs::canonicalize(&env_sdk).unwrap());
        assert_eq!(sdk.version, DEFAULT_RAY_VERSION);
        assert_eq!(sdk.sha256, None);

        let cache_args = Args {
            ray_cpp_dir: None,
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root,
            python3: None,
            download: false,
        };
        let (sdk_cached, source_cached) = resolve(&cache_args).expect("resolve cached sdk");
        assert_eq!(source_cached, Source::Cache);
        assert_eq!(sdk_cached.cpp_dir, fs::canonicalize(&cached_sdk).unwrap());
        assert_eq!(sdk_cached.version, DEFAULT_RAY_VERSION);
        assert_eq!(sdk_cached.sha256, None);
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
            download: false,
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

        let sdk =
            build_sdk(&resolved, DEFAULT_RAY_VERSION.to_string(), None).expect("build relative");
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
            download: false,
        };
        let err = resolve(&args).unwrap_err();
        assert!(
            err.contains(&empty_env_sdk.display().to_string()),
            "err should mention empty_env_sdk path, got: {err}"
        );
        assert!(
            err.contains("RAY_CPP_DIR") && err.contains("ray[cpp]") && err.contains(FIX_HINT),
            "err should include FIX_HINT keywords, got: {err}"
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
            download: false,
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
            "Set RAY_CPP_DIR=/path/to/ray/cpp, install via `pip install ray[cpp]`, or set RAYRUST_DOWNLOAD=1 to download the ray-cpp wheel from PyPI (supported PyPI wheel platforms: linux/x86_64, linux/aarch64, macos/aarch64, windows/x86_64).",
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

    #[test]
    fn select_wheel_url_matches_py3_none_and_cp_wheels() {
        let pypi_255 = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.55.0-py3-none-macosx_12_0_arm64.whl","url":"https://files.pythonhosted.org/packages/8f/38/640ada368f4d6554ffa5ed0719ee89d8787d1309448a39046aec44f5a3bc/ray_cpp-2.55.0-py3-none-macosx_12_0_arm64.whl","digests":{"sha256":"129e3862fbf6938c670c3b1bb91fe01d6001287c12cb1787750fec989cee434d"}},"#,
            r#"{"filename":"ray_cpp-2.55.0-py3-none-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/61/64/4de4ad58dd50532aa2774462af8531395af55ff54c19e6d270c643611823/ray_cpp-2.55.0-py3-none-manylinux2014_x86_64.whl","digests":{"sha256":"fabf15f1403cfb3b4cc707b21eaaf7b80081763386393a87f9e392dd137c5849"}}"#,
            r#"]}"#,
        );
        let w_255 = select_wheel(pypi_255, "2.55.0", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            w_255,
            Wheel {
                filename: "ray_cpp-2.55.0-py3-none-manylinux2014_x86_64.whl".to_string(),
                url: "https://files.pythonhosted.org/packages/61/64/4de4ad58dd50532aa2774462af8531395af55ff54c19e6d270c643611823/ray_cpp-2.55.0-py3-none-manylinux2014_x86_64.whl".to_string(),
                sha256: "fabf15f1403cfb3b4cc707b21eaaf7b80081763386393a87f9e392dd137c5849".to_string(),
            }
        );

        let pypi_260 = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/97/e0/099d1ea4558d3d7115a0b4143f25c20830d36dfd3e0f8200868d63a2aadf/ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl","digests":{"sha256":"43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0"}}"#,
            r#"]}"#,
        );
        let w_260 = select_wheel(pypi_260, "2.60.0", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            w_260,
            Wheel {
                filename: "ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl".to_string(),
                url: "https://files.pythonhosted.org/packages/97/e0/099d1ea4558d3d7115a0b4143f25c20830d36dfd3e0f8200868d63a2aadf/ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl".to_string(),
                sha256: "43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0".to_string(),
            }
        );

        let pypi_2511 = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.51.1-cp310-cp310-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/cp310/ray_cpp-2.51.1-cp310-cp310-manylinux2014_x86_64.whl","digests":{"sha256":"0000000000000000000000000000000000000000000000000000000000000310"}},"#,
            r#"{"filename":"ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/1c/84/c7aecb7867ca3b85dbcfa20808f698727c35163b76ad2baae94fcad9531c/ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl","digests":{"sha256":"9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6"}}"#,
            r#"]}"#,
        );
        let w_2511 = select_wheel(pypi_2511, "2.51.1", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            w_2511,
            Wheel {
                filename: "ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl".to_string(),
                url: "https://files.pythonhosted.org/packages/1c/84/c7aecb7867ca3b85dbcfa20808f698727c35163b76ad2baae94fcad9531c/ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl".to_string(),
                sha256: "9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6".to_string(),
            }
        );

        let err = select_wheel(pypi_260, "2.60.0", "win_amd64").unwrap_err();
        assert!(err.contains("py3-none"), "err was: {err}");
    }

    #[test]
    fn select_wheel_tag_priority_falls_back_to_cp312_when_py3_none_absent() {
        let only_cp = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.51.1-cp311-cp311-manylinux2014_x86_64.whl","url":"https://example.invalid/cp311.whl","digests":{"sha256":"1111111111111111111111111111111111111111111111111111111111111111"}},"#,
            r#"{"filename":"ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/1c/84/c7aecb7867ca3b85dbcfa20808f698727c35163b76ad2baae94fcad9531c/ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl","digests":{"sha256":"9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6"}}"#,
            r#"]}"#,
        );
        let selected = select_wheel(only_cp, "2.51.1", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            selected.filename,
            "ray_cpp-2.51.1-cp312-cp312-manylinux2014_x86_64.whl"
        );
        assert_eq!(
            selected.sha256,
            "9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6"
        );

        let both_py3_and_cp = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.60.0-cp312-cp312-manylinux2014_x86_64.whl","url":"https://example.invalid/cp312.whl","digests":{"sha256":"2222222222222222222222222222222222222222222222222222222222222222"}},"#,
            r#"{"filename":"ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/97/e0/099d1ea4558d3d7115a0b4143f25c20830d36dfd3e0f8200868d63a2aadf/ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl","digests":{"sha256":"43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0"}}"#,
            r#"]}"#,
        );
        let preferred = select_wheel(both_py3_and_cp, "2.60.0", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            preferred.filename,
            "ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl"
        );
    }

    #[test]
    fn select_wheel_invalid_json_mentions_package_and_version() {
        let err = select_wheel("{not-json", "2.60.0", "manylinux2014_x86_64").unwrap_err();
        assert!(
            err.contains("ray-cpp 2.60.0"),
            "err should mention 'ray-cpp 2.60.0', got: {err}"
        );
    }

    #[test]
    fn select_wheel_missing_sha256_mentions_filename_and_sha256() {
        let no_digests = r#"{"urls":[{"filename":"ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl","url":"https://example.invalid/w.whl"}]}"#;
        let err1 = select_wheel(no_digests, "2.60.0", "manylinux2014_x86_64").unwrap_err();
        assert!(
            err1.contains("ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl"),
            "err was: {err1}"
        );
        assert!(err1.contains("sha256"), "err was: {err1}");

        let empty_sha = r#"{"urls":[{"filename":"ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl","url":"https://example.invalid/w.whl","digests":{"sha256":""}}]}"#;
        let err2 = select_wheel(empty_sha, "2.60.0", "manylinux2014_x86_64").unwrap_err();
        assert!(
            err2.contains("ray_cpp-2.60.0-py3-none-manylinux2014_x86_64.whl"),
            "err was: {err2}"
        );
        assert!(err2.contains("sha256"), "err was: {err2}");
    }

    #[test]
    fn verify_sha256_matches_and_reports_mismatch_digests() {
        let base = test_case_dir("verify_sha256");
        let file_path = base.join("payload.bin");
        // Exceed 1 MiB to exercise multi-chunk streaming reads.
        let mut payload = vec![b'a'; 1024 * 1024 + 37];
        fs::write(&file_path, &payload).expect("write payload");

        let expected = format_hex_lower(&Sha256::digest(&payload));
        verify_sha256(&file_path, &expected).expect("matching digest passes");

        payload[1024 * 1024] = b'b';
        fs::write(&file_path, &payload).expect("write mutated payload");
        let actual = format_hex_lower(&Sha256::digest(&payload));
        let err = verify_sha256(&file_path, &expected).unwrap_err();
        assert!(
            err.contains("sha256 mismatch"),
            "err should say sha256 mismatch, got: {err}"
        );
        assert!(
            err.contains(&expected),
            "err should contain expected digest {expected}, got: {err}"
        );
        assert!(
            err.contains(&actual),
            "err should contain actual digest {actual}, got: {err}"
        );
    }

    #[test]
    fn parse_download_flag_cases() {
        assert!(!parse_download_flag(None).unwrap());
        assert!(!parse_download_flag(Some("")).unwrap());
        assert!(parse_download_flag(Some("1")).unwrap());
        assert!(parse_download_flag(Some("true")).unwrap());
        assert!(parse_download_flag(Some("YES")).unwrap());
        assert!(parse_download_flag(Some(" 1 ")).unwrap());
        assert!(!parse_download_flag(Some("0")).unwrap());
        assert!(!parse_download_flag(Some("false")).unwrap());
        assert!(!parse_download_flag(Some("no")).unwrap());
        assert!(!parse_download_flag(Some(" NO ")).unwrap());

        for bad in ["2", "auto", "   "] {
            let err = parse_download_flag(Some(bad)).unwrap_err();
            assert!(
                err.contains(&format!("invalid RAYRUST_DOWNLOAD value '{bad}'")),
                "err was: {err}"
            );
        }
    }

    #[test]
    fn pin_file_roundtrip_and_invalid_pin_fails_cache_lookup() {
        let base = test_case_dir("pin_file");
        let cache_root = base.join("cache");
        fs::create_dir_all(&cache_root).expect("create cache_root");
        let host_platform = platform_tag(env::consts::OS, env::consts::ARCH).unwrap();
        let cached_sdk = cache_dir(&cache_root, "2.60.0", host_platform);
        populate_fake_sdk(&cached_sdk);

        let pin = pin_file_path(&cache_root, "2.60.0", host_platform);
        let digest = "43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0";
        write_pin_file(&pin, digest, 4242).expect("write pin file");
        assert_eq!(fs::read_to_string(&pin).unwrap(), format!("{digest}\n"));
        assert_eq!(read_pin_file(&pin).unwrap().as_deref(), Some(digest));

        let args = Args {
            ray_cpp_dir: None,
            ray_version: "2.60.0".to_string(),
            cache_root: cache_root.clone(),
            python3: None,
            download: false,
        };
        let (sdk, source) = resolve(&args).expect("resolve cached sdk with pin");
        assert_eq!(source, Source::Cache);
        assert_eq!(sdk.version, "2.60.0");
        assert_eq!(sdk.sha256.as_deref(), Some(digest));

        fs::write(&pin, "deadbeef\n").expect("corrupt pin file");
        let err = resolve(&args).unwrap_err();
        assert!(
            err.contains("invalid pin file"),
            "err should say invalid pin file, got: {err}"
        );
        assert!(
            err.contains(&pin.display().to_string()),
            "err should mention pin path {}, got: {err}",
            pin.display()
        );
        assert!(
            err.contains("deadbeef"),
            "err should mention deadbeef, got: {err}"
        );
        assert!(
            err.contains("Delete"),
            "err should instruct user to Delete the pin file, got: {err}"
        );
        assert!(
            err.contains("RAYRUST_DOWNLOAD=1"),
            "err should mention rebuilding with RAYRUST_DOWNLOAD=1, got: {err}"
        );
    }

    #[test]
    fn require_hint_excludes_rayrust_download_and_preserves_fatal() {
        assert!(FIX_HINT_NO_DOWNLOAD.contains("RAY_CPP_DIR"));
        assert!(FIX_HINT_NO_DOWNLOAD.contains("ray[cpp]"));
        assert!(!FIX_HINT_NO_DOWNLOAD.contains("RAYRUST_DOWNLOAD"));

        let base = test_case_dir("require_hint");
        let args = Args {
            ray_cpp_dir: None,
            ray_version: "2.60.0".to_string(),
            cache_root: base.join("empty-cache"),
            python3: None,
            download: false,
        };
        let failure = resolve_inner(&args, false).expect_err("missing SDK fails");
        let msg = format_require_failure(failure);
        assert!(msg.contains("Ray C++ SDK not found."), "msg was: {msg}");
        assert!(msg.ends_with(FIX_HINT_NO_DOWNLOAD), "msg was: {msg}");
        assert!(
            !msg.contains("RAYRUST_DOWNLOAD"),
            "require() failure message must not mention RAYRUST_DOWNLOAD, got: {msg}"
        );

        let fatal_msg = format_require_failure(ResolveFailure::Fatal(
            "invalid pin file /x: bad. Delete /x and rebuild.".to_string(),
        ));
        assert_eq!(
            fatal_msg,
            "invalid pin file /x: bad. Delete /x and rebuild."
        );
    }

    #[test]
    fn move_extracted_sdk_reuses_valid_target_and_reports_corrupt_target() {
        let base = test_case_dir("move_extracted_sdk");

        let extracted_corrupt = base.join("extracted-1");
        populate_fake_sdk(&extracted_corrupt);
        let corrupt_target = base.join("target-corrupt");
        fs::create_dir_all(&corrupt_target).expect("create corrupt target");
        fs::write(corrupt_target.join("leftover"), b"partial").expect("write leftover");

        let err = move_extracted_sdk(&extracted_corrupt, &corrupt_target).unwrap_err();
        assert!(
            err.contains(&format!(
                "{} exists but is not a usable SDK — delete it and rebuild",
                corrupt_target.display()
            )),
            "err was: {err}"
        );

        let extracted_race = base.join("extracted-2");
        populate_fake_sdk(&extracted_race);
        let valid_target = base.join("target-valid");
        populate_fake_sdk(&valid_target);
        move_extracted_sdk(&extracted_race, &valid_target)
            .expect("existing valid target succeeds on concurrent rename race");
    }

    #[test]
    fn resolve_reports_download_disabled_and_formats_all_warnings() {
        let base = test_case_dir("download_disabled_and_warnings");
        let disabled_args = Args {
            ray_cpp_dir: None,
            ray_version: "2.60.0".to_string(),
            cache_root: base.join("empty-cache"),
            python3: None,
            download: false,
        };
        let err_disabled = resolve(&disabled_args).unwrap_err();
        assert!(
            err_disabled.contains(
                "download: disabled (set RAYRUST_DOWNLOAD=1 to download ray-cpp 2.60.0 from PyPI)"
            ),
            "err was: {err_disabled}"
        );

        let enabled_args = Args {
            download: true,
            ..disabled_args
        };
        let err_enabled = resolve(&enabled_args).unwrap_err();
        assert!(
            !err_enabled.contains("download: disabled"),
            "err was: {err_enabled}"
        );

        let dir = PathBuf::from("/opt/ray/cpp");
        let digest = "43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0";
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir.clone(),
                    version: "2.51.1".to_string(),
                    sha256: None,
                },
                Source::Env,
            ),
            "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from RAY_CPP_DIR, version not checked)"
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir.clone(),
                    version: "2.60.0".to_string(),
                    sha256: None,
                },
                Source::Cache,
            ),
            "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from cache, Ray 2.60.0)"
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir.clone(),
                    version: "2.60.0".to_string(),
                    sha256: Some(digest.to_string()),
                },
                Source::Cache,
            ),
            format!(
                "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from cache, Ray 2.60.0, sha256 {digest})"
            )
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir.clone(),
                    version: "2.51.1".to_string(),
                    sha256: None,
                },
                Source::Python,
            ),
            "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from python3, Ray 2.51.1)"
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir,
                    version: "2.60.0".to_string(),
                    sha256: Some(digest.to_string()),
                },
                Source::Download,
            ),
            format!(
                "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from download, Ray 2.60.0, sha256 {digest})"
            )
        );
    }
}

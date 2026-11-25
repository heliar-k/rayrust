//! Build-time Ray C++ SDK locator and opt-in PyPI wheel fetcher (requires Ray >= 2.56.1).
//!
//! SDK lookup order:
//! 1. `RAY_CPP_DIR` (probed for minimum supported version `>= 2.56.1`)
//! 2. Shared cache `<cache_root>/ray-cpp-<version>-<platform>/` (with optional `.sha256` pin file)
//! 3. `python3` (`ray/cpp` on `sys.path` whose `ray.__version__` equals `RAY_VERSION`)
//! 4. Opt-in PyPI wheel download (`RAYRUST_DOWNLOAD=1`), verified against `digests.sha256`
//!
//! Build-time probing and runtime version handshake:
//! - [`probe_sdk_version`] inspects `lib/libray_api.so` (reading `_ZL11kRayVersion` from ELF64
//!   `SHT_SYMTAB`, embedded from `src/ray/common/constants.h:68` at
//!   `src/ray/core_worker/core_worker_process.cc:780-785`) and falls back to `../_version.py` when
//!   available.
//! - `rayrust-sys/build.rs` records the probed version as `RAYRUST_BUILD_RAY_VERSION` and probes
//!   `ray::RuntimeEnv::Deserialize` in `libray_api.so` to match `_GLIBCXX_USE_CXX11_ABI`.
//! - At runtime, `ray_init` and worker callback entries verify `kRayVersion` in the loaded
//!   `libray_api.so`; in cluster mode, `ray_init` also queries GCS internal KV
//!   `("cluster", "CLUSTER_METADATA")` via `dlsym`-resolved
//!   `ray::gcs::GlobalStateAccessor::GetInternalKV`
//!   (`src/ray/gcs_rpc_client/global_state_accessor.cc:375-382`) to verify `ray_version` written
//!   by `Node._write_cluster_info_to_kv` (`python/ray/_private/node.py:1295-1344`),
//!   `put_cluster_metadata` (`python/ray/_common/usage/usage_lib.py:521-543`), and
//!   `_generate_cluster_metadata` (same file `:401-434`), matching `get_cluster_metadata`
//!   (`:841-862`) and `_check_ray_version` (`python/ray/scripts/scripts.py:69-78`).

use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::env;
use std::fmt::{self, Write as _};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{self, Command};

pub const DEFAULT_RAY_VERSION: &str = "2.56.1";
pub const MIN_SUPPORTED_RAY_VERSION: &str = "2.56.1";
pub const MIN_SUPPORTED_RAY_VERSION_TUPLE: (u32, u32, u32) = (2, 56, 1);

pub fn parse_ray_version(raw: &str) -> Result<(u32, u32, u32), String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(format!(
            "invalid Ray version '{trimmed}': expected '<major>.<minor>.<patch>'"
        ));
    }

    let mut parts = trimmed.split('.');
    let (Some(major_str), Some(minor_str), Some(patch_raw)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return Err(format!(
            "invalid Ray version '{trimmed}': expected '<major>.<minor>.<patch>'"
        ));
    };
    for extra in parts {
        if extra.is_empty() {
            return Err(format!(
                "invalid Ray version '{trimmed}': empty component after patch version"
            ));
        }
    }

    let parse_component = |s: &str, name: &str| -> Result<u32, String> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!(
                "invalid Ray version '{trimmed}': expected numeric {name} component, got '{s}'"
            ));
        }
        s.parse::<u32>().map_err(|e| {
            format!("invalid Ray version '{trimmed}': {name} component '{s}' out of range: {e}")
        })
    };

    let major = parse_component(major_str, "major")?;
    let minor = parse_component(minor_str, "minor")?;
    let patch_digits_len = patch_raw.bytes().take_while(|b| b.is_ascii_digit()).count();
    if patch_digits_len == 0 {
        return Err(format!(
            "invalid Ray version '{trimmed}': expected numeric patch component, got '{patch_raw}'"
        ));
    }
    let patch = parse_component(&patch_raw[..patch_digits_len], "patch")?;

    Ok((major, minor, patch))
}

pub fn check_minimum_ray_version(version: &str) -> Result<(), String> {
    let parsed = parse_ray_version(version)?;
    if parsed < MIN_SUPPORTED_RAY_VERSION_TUPLE {
        return Err(format!(
            "Ray {} is unsupported; rayrust requires Ray >= {MIN_SUPPORTED_RAY_VERSION}",
            version.trim()
        ));
    }
    Ok(())
}

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

fn read_u16_le(buf: &[u8], pos: usize) -> Option<u16> {
    let slice = buf.get(pos..pos.checked_add(2)?)?;
    Some(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32_le(buf: &[u8], pos: usize) -> Option<u32> {
    let slice = buf.get(pos..pos.checked_add(4)?)?;
    Some(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_u64_le(buf: &[u8], pos: usize) -> Option<u64> {
    let slice = buf.get(pos..pos.checked_add(8)?)?;
    Some(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

#[derive(Clone, Copy)]
struct Elf64Shdr {
    sh_type: u32,
    sh_addr: u64,
    sh_offset: u64,
    sh_size: u64,
    sh_link: u32,
    sh_entsize: u64,
}

fn parse_elf64_shdr(raw: &[u8]) -> Option<Elf64Shdr> {
    if raw.len() < 64 {
        return None;
    }
    Some(Elf64Shdr {
        sh_type: read_u32_le(raw, 4)?,
        sh_addr: read_u64_le(raw, 16)?,
        sh_offset: read_u64_le(raw, 24)?,
        sh_size: read_u64_le(raw, 32)?,
        sh_link: read_u32_le(raw, 40)?,
        sh_entsize: read_u64_le(raw, 56)?,
    })
}

fn is_valid_version_ascii(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes.iter().all(|&b| b.is_ascii_graphic())
}

fn probe_elf64_kray_version(so_path: &Path) -> Option<String> {
    const ELF_MAGIC: &[u8; 4] = b"\x7fELF";
    const ELFCLASS64: u8 = 2;
    const ELFDATA2LSB: u8 = 1;
    const SHT_SYMTAB: u32 = 2;
    const SHT_STRTAB: u32 = 3;
    const SHT_NOBITS: u32 = 8;
    const TARGET_SYM: &[u8] = b"_ZL11kRayVersion\0";

    let mut file = fs::File::open(so_path).ok()?;
    let file_len = file.metadata().ok()?.len();
    if file_len < 64 {
        return None;
    }

    let mut ehdr = [0u8; 64];
    file.read_exact(&mut ehdr).ok()?;
    if &ehdr[0..4] != ELF_MAGIC || ehdr[4] != ELFCLASS64 || ehdr[5] != ELFDATA2LSB {
        return None;
    }

    let e_shoff = read_u64_le(&ehdr, 40)?;
    let e_shentsize = u64::from(read_u16_le(&ehdr, 58)?);
    let e_shnum = usize::from(read_u16_le(&ehdr, 60)?);
    if e_shoff == 0 || e_shentsize < 64 || e_shnum == 0 {
        return None;
    }

    let sh_table_bytes = e_shentsize.checked_mul(e_shnum as u64)?;
    if e_shoff.checked_add(sh_table_bytes)? > file_len {
        return None;
    }
    let sh_table_len = usize::try_from(sh_table_bytes).ok()?;
    let mut sh_table = vec![0u8; sh_table_len];
    file.seek(SeekFrom::Start(e_shoff)).ok()?;
    file.read_exact(&mut sh_table).ok()?;

    let shentsize = usize::try_from(e_shentsize).ok()?;
    let mut sections = Vec::with_capacity(e_shnum);
    for idx in 0..e_shnum {
        let start = idx.checked_mul(shentsize)?;
        let raw = sh_table.get(start..start.checked_add(shentsize)?)?;
        sections.push(parse_elf64_shdr(raw)?);
    }

    for sec in &sections {
        if sec.sh_type != SHT_SYMTAB {
            continue;
        }
        if sec.sh_entsize < 24 || sec.sh_size < sec.sh_entsize {
            continue;
        }
        if sec.sh_offset.checked_add(sec.sh_size)? > file_len {
            return None;
        }
        let str_idx = sec.sh_link as usize;
        let str_sec = sections.get(str_idx)?;
        if str_sec.sh_type != SHT_STRTAB || str_sec.sh_size == 0 {
            continue;
        }
        if str_sec.sh_offset.checked_add(str_sec.sh_size)? > file_len {
            return None;
        }

        let sym_len = usize::try_from(sec.sh_size).ok()?;
        let str_len = usize::try_from(str_sec.sh_size).ok()?;
        let sym_step = usize::try_from(sec.sh_entsize).ok()?;

        let mut symtab = vec![0u8; sym_len];
        file.seek(SeekFrom::Start(sec.sh_offset)).ok()?;
        file.read_exact(&mut symtab).ok()?;

        let mut strtab = vec![0u8; str_len];
        file.seek(SeekFrom::Start(str_sec.sh_offset)).ok()?;
        file.read_exact(&mut strtab).ok()?;

        let mut pos = 0usize;
        while let Some(end) = pos.checked_add(sym_step) {
            if end > symtab.len() {
                break;
            }
            let entry = &symtab[pos..end];
            pos = end;

            let st_name = read_u32_le(entry, 0)? as usize;
            let st_shndx = read_u16_le(entry, 6)? as usize;
            let st_value = read_u64_le(entry, 8)?;

            let Some(name_slice) = strtab.get(st_name..st_name.checked_add(TARGET_SYM.len())?)
            else {
                continue;
            };
            if name_slice != TARGET_SYM {
                continue;
            }
            if st_shndx == 0 {
                return None;
            }
            let target_sec = sections.get(st_shndx)?;
            if target_sec.sh_type == SHT_NOBITS {
                return None;
            }
            if target_sec.sh_offset.checked_add(target_sec.sh_size)? > file_len {
                return None;
            }
            let rel = st_value.checked_sub(target_sec.sh_addr)?;
            if rel >= target_sec.sh_size {
                return None;
            }
            let str_file_pos = target_sec.sh_offset.checked_add(rel)?;
            let remaining = target_sec.sh_size - rel;
            let read_cap = usize::try_from(remaining.min(256)).ok()?;
            let mut raw_ver = vec![0u8; read_cap];
            file.seek(SeekFrom::Start(str_file_pos)).ok()?;
            file.read_exact(&mut raw_ver).ok()?;

            let nul_pos = raw_ver.iter().position(|&b| b == 0)?;
            let ver_bytes = &raw_ver[..nul_pos];
            if !is_valid_version_ascii(ver_bytes) {
                return None;
            }
            return std::str::from_utf8(ver_bytes).ok().map(str::to_string);
        }
    }
    None
}

fn probe_parent_version_py(cpp_dir: &Path) -> Option<String> {
    let version_py = cpp_dir.parent()?.join("_version.py");
    let contents = fs::read_to_string(version_py).ok()?;
    for line in contents.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("version") else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        let rhs = rest.trim();
        let unquoted = rhs
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .or_else(|| rhs.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')));
        if let Some(ver) = unquoted {
            if is_valid_version_ascii(ver.as_bytes()) {
                return Some(ver.to_string());
            }
        }
    }
    None
}

/// Probe the exact Ray version embedded in `cpp_dir`.
///
/// Priority:
/// 1. `cpp_dir/lib/libray_api.so` ELF64 `SHT_SYMTAB` local symbol `_ZL11kRayVersion`.
/// 2. `cpp_dir/../_version.py` (`version = "..."` or `version = '...'`).
///
/// Unreadable, non-ELF, or stripped files without `../_version.py` return `None`.
pub fn probe_sdk_version(cpp_dir: &Path) -> Option<String> {
    let so_path = cpp_dir.join("lib").join("libray_api.so");
    if so_path.is_file() {
        if let Some(ver) = probe_elf64_kray_version(&so_path) {
            return Some(ver);
        }
    }
    probe_parent_version_py(cpp_dir)
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
        let mut sdk = build_sdk(dir, args.ray_version.clone(), None).map_err(|reason| {
            ResolveFailure::Fatal(format!(
                "invalid RAY_CPP_DIR ({}): {reason}. {FIX_HINT}",
                dir.display()
            ))
        })?;
        if let Some(probed) = probe_sdk_version(&sdk.cpp_dir) {
            check_minimum_ray_version(&probed).map_err(|err| {
                ResolveFailure::Fatal(format!(
                    "unsupported RAY_CPP_DIR ({}): {err}",
                    dir.display()
                ))
            })?;
            sdk.version = probed;
        } else {
            check_minimum_ray_version(&args.ray_version).map_err(|err| {
                ResolveFailure::Fatal(format!(
                    "unsupported RAY_CPP_DIR ({}): {err}",
                    dir.display()
                ))
            })?;
        }
        return Ok((sdk, Source::Env));
    }

    check_minimum_ray_version(&args.ray_version).map_err(ResolveFailure::Fatal)?;

    let mut checked = Vec::new();
    checked.push("RAY_CPP_DIR: not set".to_string());

    let platform_supported = match platform_tag(env::consts::OS, env::consts::ARCH) {
        Ok(platform) => {
            let cached = cache_dir(&args.cache_root, &args.ray_version, platform);
            match build_sdk(&cached, args.ray_version.clone(), None) {
                Ok(mut sdk) => {
                    if let Some(probed) = probe_sdk_version(&sdk.cpp_dir) {
                        check_minimum_ray_version(&probed).map_err(|err| {
                            ResolveFailure::Fatal(format!(
                                "unsupported cached Ray C++ SDK ({}): {err}",
                                cached.display()
                            ))
                        })?;
                    }
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
                        Ok(sdk) => {
                            if let Some(probed) = probe_sdk_version(&sdk.cpp_dir) {
                                check_minimum_ray_version(&probed).map_err(|err| {
                                    ResolveFailure::Fatal(format!(
                                        "unsupported Python Ray C++ SDK ({}): {err}",
                                        dir.display()
                                    ))
                                })?;
                            }
                            return Ok((sdk, Source::Python));
                        }
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
    check_minimum_ray_version(&args.ray_version)?;
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
    check_minimum_ray_version(version)?;
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
    check_minimum_ray_version(version)?;
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
        let dir = cache_dir(root, "2.56.1", "manylinux2014_x86_64");
        assert_eq!(
            dir,
            PathBuf::from("cache-root/ray-cpp-2.56.1-manylinux2014_x86_64")
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
        assert!(version_matches("2.56.1", "2.56.1"));
        assert!(!version_matches("2.56.1", "2.58.2"));
        assert!(!version_matches("", "2.56.1"));
        assert!(!version_matches("2.56.1", ""));
        assert!(!version_matches("", ""));
        assert!(!version_matches("   ", "2.56.1"));
        assert!(version_matches("  2.56.1\n", "2.56.1  "));
    }

    #[test]
    fn select_wheel_url_matches_py3_none_and_cp_wheels() {
        let pypi_2561 = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.56.1-py3-none-macosx_12_0_arm64.whl","url":"https://files.pythonhosted.org/packages/mac/ray_cpp-2.56.1-py3-none-macosx_12_0_arm64.whl","digests":{"sha256":"129e3862fbf6938c670c3b1bb91fe01d6001287c12cb1787750fec989cee434d"}},"#,
            r#"{"filename":"ray_cpp-2.56.1-py3-none-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/linux/ray_cpp-2.56.1-py3-none-manylinux2014_x86_64.whl","digests":{"sha256":"fabf15f1403cfb3b4cc707b21eaaf7b80081763386393a87f9e392dd137c5849"}}"#,
            r#"]}"#,
        );
        let w_2561 = select_wheel(pypi_2561, "2.56.1", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            w_2561,
            Wheel {
                filename: "ray_cpp-2.56.1-py3-none-manylinux2014_x86_64.whl".to_string(),
                url: "https://files.pythonhosted.org/packages/linux/ray_cpp-2.56.1-py3-none-manylinux2014_x86_64.whl".to_string(),
                sha256: "fabf15f1403cfb3b4cc707b21eaaf7b80081763386393a87f9e392dd137c5849".to_string(),
            }
        );

        let pypi_2601 = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/2601/ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl","digests":{"sha256":"43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0"}}"#,
            r#"]}"#,
        );
        let w_2601 = select_wheel(pypi_2601, "2.60.1", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            w_2601,
            Wheel {
                filename: "ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl".to_string(),
                url: "https://files.pythonhosted.org/packages/2601/ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl".to_string(),
                sha256: "43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0".to_string(),
            }
        );

        let pypi_2582_cp = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.58.2-cp310-cp310-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/cp310/ray_cpp-2.58.2-cp310-cp310-manylinux2014_x86_64.whl","digests":{"sha256":"0000000000000000000000000000000000000000000000000000000000000310"}},"#,
            r#"{"filename":"ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/cp312/ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl","digests":{"sha256":"9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6"}}"#,
            r#"]}"#,
        );
        let w_2582 = select_wheel(pypi_2582_cp, "2.58.2", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            w_2582,
            Wheel {
                filename: "ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl".to_string(),
                url: "https://files.pythonhosted.org/packages/cp312/ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl".to_string(),
                sha256: "9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6".to_string(),
            }
        );

        let err = select_wheel(pypi_2601, "2.60.1", "win_amd64").unwrap_err();
        assert!(err.contains("py3-none"), "err was: {err}");
    }

    #[test]
    fn select_wheel_tag_priority_falls_back_to_cp312_when_py3_none_absent() {
        let only_cp = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.58.2-cp311-cp311-manylinux2014_x86_64.whl","url":"https://example.invalid/cp311.whl","digests":{"sha256":"1111111111111111111111111111111111111111111111111111111111111111"}},"#,
            r#"{"filename":"ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/cp312/ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl","digests":{"sha256":"9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6"}}"#,
            r#"]}"#,
        );
        let selected = select_wheel(only_cp, "2.58.2", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            selected.filename,
            "ray_cpp-2.58.2-cp312-cp312-manylinux2014_x86_64.whl"
        );
        assert_eq!(
            selected.sha256,
            "9b9fd02af0e750a3dd8c4702d82551e83393d89fda9baa75c2513d22b079a9f6"
        );

        let both_py3_and_cp = concat!(
            r#"{"urls":["#,
            r#"{"filename":"ray_cpp-2.60.1-cp312-cp312-manylinux2014_x86_64.whl","url":"https://example.invalid/cp312.whl","digests":{"sha256":"2222222222222222222222222222222222222222222222222222222222222222"}},"#,
            r#"{"filename":"ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl","url":"https://files.pythonhosted.org/packages/2601/ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl","digests":{"sha256":"43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0"}}"#,
            r#"]}"#,
        );
        let preferred = select_wheel(both_py3_and_cp, "2.60.1", "manylinux2014_x86_64").unwrap();
        assert_eq!(
            preferred.filename,
            "ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl"
        );
    }

    #[test]
    fn select_wheel_invalid_json_mentions_package_and_version() {
        let err = select_wheel("{not-json", "2.60.1", "manylinux2014_x86_64").unwrap_err();
        assert!(
            err.contains("ray-cpp 2.60.1"),
            "err should mention 'ray-cpp 2.60.1', got: {err}"
        );
    }

    #[test]
    fn select_wheel_missing_sha256_mentions_filename_and_sha256() {
        let no_digests = r#"{"urls":[{"filename":"ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl","url":"https://example.invalid/w.whl"}]}"#;
        let err1 = select_wheel(no_digests, "2.60.1", "manylinux2014_x86_64").unwrap_err();
        assert!(
            err1.contains("ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl"),
            "err was: {err1}"
        );
        assert!(err1.contains("sha256"), "err was: {err1}");

        let empty_sha = r#"{"urls":[{"filename":"ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl","url":"https://example.invalid/w.whl","digests":{"sha256":""}}]}"#;
        let err2 = select_wheel(empty_sha, "2.60.1", "manylinux2014_x86_64").unwrap_err();
        assert!(
            err2.contains("ray_cpp-2.60.1-py3-none-manylinux2014_x86_64.whl"),
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
        let cached_sdk = cache_dir(&cache_root, "2.60.1", host_platform);
        populate_fake_sdk(&cached_sdk);

        let pin = pin_file_path(&cache_root, "2.60.1", host_platform);
        let digest = "43433cf59e2c5f41c0a4b2dbe3aa87a664a8ed1f00cbccf06967fd9819e1dac0";
        write_pin_file(&pin, digest, 4242).expect("write pin file");
        assert_eq!(fs::read_to_string(&pin).unwrap(), format!("{digest}\n"));
        assert_eq!(read_pin_file(&pin).unwrap().as_deref(), Some(digest));

        let args = Args {
            ray_cpp_dir: None,
            ray_version: "2.60.1".to_string(),
            cache_root: cache_root.clone(),
            python3: None,
            download: false,
        };
        let (sdk, source) = resolve(&args).expect("resolve cached sdk with pin");
        assert_eq!(source, Source::Cache);
        assert_eq!(sdk.version, "2.60.1");
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
            ray_version: "2.60.1".to_string(),
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
            ray_version: "2.60.1".to_string(),
            cache_root: base.join("empty-cache"),
            python3: None,
            download: false,
        };
        let err_disabled = resolve(&disabled_args).unwrap_err();
        assert!(
            err_disabled.contains(
                "download: disabled (set RAYRUST_DOWNLOAD=1 to download ray-cpp 2.60.1 from PyPI)"
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
                    version: "2.56.1".to_string(),
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
                    version: "2.60.1".to_string(),
                    sha256: None,
                },
                Source::Cache,
            ),
            "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from cache, Ray 2.60.1)"
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir.clone(),
                    version: "2.60.1".to_string(),
                    sha256: Some(digest.to_string()),
                },
                Source::Cache,
            ),
            format!(
                "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from cache, Ray 2.60.1, sha256 {digest})"
            )
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir.clone(),
                    version: "2.56.1".to_string(),
                    sha256: None,
                },
                Source::Python,
            ),
            "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from python3, Ray 2.56.1)"
        );
        assert_eq!(
            format_warning(
                &Sdk {
                    cpp_dir: dir,
                    version: "2.60.1".to_string(),
                    sha256: Some(digest.to_string()),
                },
                Source::Download,
            ),
            format!(
                "cargo:warning=Using Ray C++ SDK at /opt/ray/cpp (from download, Ray 2.60.1, sha256 {digest})"
            )
        );
    }

    fn build_minimal_elf64_with_kray_version(version: &str) -> Vec<u8> {
        let mut rodata = Vec::new();
        rodata.extend_from_slice(b"pad\0");
        let ver_offset_in_rodata = rodata.len() as u64;
        rodata.extend_from_slice(version.as_bytes());
        rodata.push(0);

        let mut strtab = vec![0u8];
        let sym_name_offset = strtab.len() as u32;
        strtab.extend_from_slice(b"_ZL11kRayVersion\0");

        // Section indices: 0 = SHT_NULL, 1 = .rodata, 2 = .strtab, 3 = .symtab
        let rodata_vaddr: u64 = 0x400000;
        let mut symtab = vec![0u8; 48]; // two 24-byte Elf64_Sym entries (0 = null, 1 = _ZL11kRayVersion)
        symtab[24..28].copy_from_slice(&sym_name_offset.to_le_bytes());
        symtab[28] = 0x01; // STB_LOCAL | STT_OBJECT
        symtab[30..32].copy_from_slice(&1u16.to_le_bytes()); // st_shndx = 1 (.rodata)
        let sym_val = rodata_vaddr + ver_offset_in_rodata;
        symtab[32..40].copy_from_slice(&sym_val.to_le_bytes());
        symtab[40..48].copy_from_slice(&((version.len() + 1) as u64).to_le_bytes());

        let ehdr_size = 64usize;
        let rodata_file_off = ehdr_size;
        let strtab_file_off = rodata_file_off + rodata.len();
        let symtab_file_off = strtab_file_off + strtab.len();
        let shoff = symtab_file_off + symtab.len();

        let write_shdr = |out: &mut Vec<u8>,
                          sh_type: u32,
                          sh_addr: u64,
                          sh_offset: u64,
                          sh_size: u64,
                          sh_link: u32,
                          sh_entsize: u64| {
            let mut sh = [0u8; 64];
            sh[4..8].copy_from_slice(&sh_type.to_le_bytes());
            sh[16..24].copy_from_slice(&sh_addr.to_le_bytes());
            sh[24..32].copy_from_slice(&sh_offset.to_le_bytes());
            sh[32..40].copy_from_slice(&sh_size.to_le_bytes());
            sh[40..44].copy_from_slice(&sh_link.to_le_bytes());
            sh[56..64].copy_from_slice(&sh_entsize.to_le_bytes());
            out.extend_from_slice(&sh);
        };

        let mut elf = vec![0u8; ehdr_size];
        elf[0..4].copy_from_slice(b"\x7fELF");
        elf[4] = 2; // ELFCLASS64
        elf[5] = 1; // ELFDATA2LSB
        elf[6] = 1; // EV_CURRENT
        elf[40..48].copy_from_slice(&(shoff as u64).to_le_bytes());
        elf[58..60].copy_from_slice(&64u16.to_le_bytes()); // e_shentsize
        elf[60..62].copy_from_slice(&4u16.to_le_bytes()); // e_shnum

        elf.extend_from_slice(&rodata);
        elf.extend_from_slice(&strtab);
        elf.extend_from_slice(&symtab);

        // shdr 0: SHT_NULL
        write_shdr(&mut elf, 0, 0, 0, 0, 0, 0);
        // shdr 1: .rodata (SHT_PROGBITS = 1)
        write_shdr(
            &mut elf,
            1,
            rodata_vaddr,
            rodata_file_off as u64,
            rodata.len() as u64,
            0,
            0,
        );
        // shdr 2: .strtab (SHT_STRTAB = 3)
        write_shdr(
            &mut elf,
            3,
            0,
            strtab_file_off as u64,
            strtab.len() as u64,
            0,
            0,
        );
        // shdr 3: .symtab (SHT_SYMTAB = 2, sh_link = 2, sh_entsize = 24)
        write_shdr(
            &mut elf,
            2,
            0,
            symtab_file_off as u64,
            symtab.len() as u64,
            2,
            24,
        );
        elf
    }

    #[test]
    fn probe_sdk_version_reads_elf64_and_parent_version_py() {
        let base = test_case_dir("probe_sdk_version");
        let ray_pkg = base.join("ray");
        let cpp_dir = ray_pkg.join("cpp");
        populate_fake_sdk(&cpp_dir);

        // Fake non-ELF .so without _version.py returns None.
        assert_eq!(probe_sdk_version(&cpp_dir), None);

        // Fallback to ../_version.py with double and single quotes.
        let version_py = ray_pkg.join("_version.py");
        fs::write(&version_py, "commit = \"abc\"\nversion = \"2.60.1\"\n")
            .expect("write _version.py");
        assert_eq!(probe_sdk_version(&cpp_dir).as_deref(), Some("2.60.1"));

        fs::write(&version_py, "version = '2.56.1'\n").expect("write single-quoted _version.py");
        assert_eq!(probe_sdk_version(&cpp_dir).as_deref(), Some("2.56.1"));

        // Valid ELF64 .symtab takes priority over ../_version.py.
        let elf_bytes = build_minimal_elf64_with_kray_version("2.58.2");
        let so_path = cpp_dir.join("lib").join("libray_api.so");
        fs::write(&so_path, &elf_bytes).expect("write minimal ELF64");
        assert_eq!(probe_sdk_version(&cpp_dir).as_deref(), Some("2.58.2"));

        // Truncated ELF prefixes fall back cleanly without panicking.
        let _ = fs::remove_file(&version_py);
        for len in [0usize, 4, 63, 64, elf_bytes.len() - 1] {
            fs::write(&so_path, &elf_bytes[..len]).expect("write truncated ELF");
            assert_eq!(
                probe_sdk_version(&cpp_dir),
                None,
                "truncated length {len} must return None"
            );
        }

        // If RAY_CPP_DIR points to a real SDK on this host, verify probe_sdk_version succeeds.
        if let Ok(real_dir) = env::var("RAY_CPP_DIR") {
            let p = Path::new(&real_dir);
            if p.join("lib").join("libray_api.so").is_file() {
                let probed = probe_sdk_version(p).expect("probe real RAY_CPP_DIR");
                assert!(!probed.is_empty());
            }
        }
    }

    #[test]
    fn parse_ray_version_and_check_minimum_ray_version_cases() {
        assert_eq!(parse_ray_version("2.56.1").unwrap(), (2, 56, 1));
        assert_eq!(parse_ray_version("  2.58.2\n").unwrap(), (2, 58, 2));
        assert_eq!(parse_ray_version("2.60.0rc1").unwrap(), (2, 60, 0));
        assert_eq!(parse_ray_version("2.60.1.post1").unwrap(), (2, 60, 1));
        assert_eq!(parse_ray_version("3.0.0dev0").unwrap(), (3, 0, 0));

        for malformed in [
            "",
            "   ",
            "2",
            "2.56",
            "2.56.",
            "2.56.rc1",
            "+2.56.1",
            "2.56.1.",
            "2.56.1 foo",
            "v2.56.1",
        ] {
            assert!(
                parse_ray_version(malformed).is_err(),
                "expected parse_ray_version({malformed:?}) to fail"
            );
            assert!(
                check_minimum_ray_version(malformed).is_err(),
                "expected check_minimum_ray_version({malformed:?}) to fail"
            );
        }

        for unsupported in ["1.13.0", "2.49.0", "2.51.1", "2.55.0", "2.56.0"] {
            let err = check_minimum_ray_version(unsupported).unwrap_err();
            assert_eq!(
                err,
                format!(
                    "Ray {unsupported} is unsupported; rayrust requires Ray >= {MIN_SUPPORTED_RAY_VERSION}"
                )
            );
        }

        for supported in ["2.56.1", "2.58.2", "2.60.0", "2.60.0rc1", "2.60.1", "3.0.0"] {
            check_minimum_ray_version(supported)
                .unwrap_or_else(|e| panic!("expected {supported} to pass, got: {e}"));
        }
    }

    #[test]
    fn resolve_and_download_reject_versions_below_minimum() {
        let base = test_case_dir("reject_below_minimum");
        let cache_root = base.join("cache");
        let host_platform = platform_tag(env::consts::OS, env::consts::ARCH).unwrap();

        // Populate a cache entry for 2.51.1 to prove version rejection happens before cache hit.
        populate_fake_sdk(&cache_dir(&cache_root, "2.51.1", host_platform));

        for old_ver in ["2.49.0", "2.51.1", "2.55.0", "2.56.0"] {
            let args = Args {
                ray_cpp_dir: None,
                ray_version: old_ver.to_string(),
                cache_root: cache_root.clone(),
                python3: None,
                download: false,
            };
            let err = resolve(&args).unwrap_err();
            assert!(
                err.contains(&format!(
                    "Ray {old_ver} is unsupported; rayrust requires Ray >= 2.56.1"
                )),
                "unexpected error for RAY_VERSION={old_ver}: {err}"
            );

            let dl_err = download(&args, host_platform).unwrap_err();
            assert!(
                dl_err.contains("requires Ray >= 2.56.1"),
                "unexpected download error for {old_ver}: {dl_err}"
            );
            let wheel_err = select_wheel(r#"{"urls":[]}"#, old_ver, host_platform).unwrap_err();
            assert!(
                wheel_err.contains("requires Ray >= 2.56.1"),
                "unexpected select_wheel error for {old_ver}: {wheel_err}"
            );
        }

        // RAY_CPP_DIR with embedded ELF64 _ZL11kRayVersion = "2.51.1" is rejected even when RAY_VERSION is 2.56.1.
        let elf_old_sdk = base.join("elf-2511").join("ray").join("cpp");
        populate_fake_sdk(&elf_old_sdk);
        fs::write(
            elf_old_sdk.join("lib").join("libray_api.so"),
            build_minimal_elf64_with_kray_version("2.51.1"),
        )
        .expect("write 2.51.1 ELF");
        let elf_old_args = Args {
            ray_cpp_dir: Some(elf_old_sdk.clone()),
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root: cache_root.clone(),
            python3: None,
            download: false,
        };
        let elf_err = resolve(&elf_old_args).unwrap_err();
        assert!(
            elf_err.contains("unsupported RAY_CPP_DIR")
                && elf_err.contains("Ray 2.51.1 is unsupported; rayrust requires Ray >= 2.56.1"),
            "unexpected error for 2.51.1 ELF RAY_CPP_DIR: {elf_err}"
        );

        // RAY_CPP_DIR with parent _version.py = "2.56.0" is rejected.
        let py_old_pkg = base.join("py-2560").join("ray");
        let py_old_sdk = py_old_pkg.join("cpp");
        populate_fake_sdk(&py_old_sdk);
        fs::write(py_old_pkg.join("_version.py"), "version = \"2.56.0\"\n")
            .expect("write 2.56.0 _version.py");
        let py_old_args = Args {
            ray_cpp_dir: Some(py_old_sdk),
            ray_version: DEFAULT_RAY_VERSION.to_string(),
            cache_root: cache_root.clone(),
            python3: None,
            download: false,
        };
        let py_err = resolve(&py_old_args).unwrap_err();
        assert!(
            py_err.contains("unsupported RAY_CPP_DIR")
                && py_err.contains("Ray 2.56.0 is unsupported; rayrust requires Ray >= 2.56.1"),
            "unexpected error for 2.56.0 _version.py RAY_CPP_DIR: {py_err}"
        );

        // Stripped RAY_CPP_DIR (probe_sdk_version == None) falls back to checking args.ray_version.
        let stripped_sdk = base.join("stripped-sdk");
        populate_fake_sdk(&stripped_sdk);
        let stripped_old_args = Args {
            ray_cpp_dir: Some(stripped_sdk),
            ray_version: "2.51.1".to_string(),
            cache_root: cache_root.clone(),
            python3: None,
            download: false,
        };
        let stripped_err = resolve(&stripped_old_args).unwrap_err();
        assert!(
            stripped_err.contains("unsupported RAY_CPP_DIR")
                && stripped_err
                    .contains("Ray 2.51.1 is unsupported; rayrust requires Ray >= 2.56.1"),
            "unexpected error for stripped RAY_CPP_DIR with RAY_VERSION=2.51.1: {stripped_err}"
        );

        // Cache directory named 2.56.1 containing an old 2.51.1 ELF binary is also rejected.
        let poisoned_cache = cache_dir(&cache_root, "2.56.1", host_platform);
        populate_fake_sdk(&poisoned_cache);
        fs::write(
            poisoned_cache.join("lib").join("libray_api.so"),
            build_minimal_elf64_with_kray_version("2.51.1"),
        )
        .expect("write poisoned cache ELF");
        let poisoned_args = Args {
            ray_cpp_dir: None,
            ray_version: "2.56.1".to_string(),
            cache_root,
            python3: None,
            download: false,
        };
        let poisoned_err = resolve(&poisoned_args).unwrap_err();
        assert!(
            poisoned_err.contains("Ray 2.51.1 is unsupported; rayrust requires Ray >= 2.56.1"),
            "unexpected error for poisoned cache: {poisoned_err}"
        );
    }
}

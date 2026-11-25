//! 对象读取路径的内存分配次数与物理内存峰值回归测试。
//!
//! 覆盖两层保证：
//! 1. Rust 层读取入口直接在 `CBytesGuard` 借用切片上完成反序列化，零拷贝目标类型不触发
//!    大块 Rust 堆分配，拥有所有权的目标类型（`String`、`rmpv::Value`、原始字节 `Vec<u8>`）
//!    仅分配最终返回对象本身那一份缓冲区；
//! 2. C++ 包装层 `ray_get` 与 `ray_get_many` 通过 `msgpack::sbuffer::release()` 直接移交
//!    底层缓冲区所有权，16 MiB 对象读取期间的进程 `VmHWM` 增量保持在单份载荷范围内。

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rayrust::prelude::*;

const HEADER_MAGIC: usize = 0x5241_5952_5553_5430;
const LARGE_ALLOC_THRESHOLD: usize = 512 * 1024;
const TWO_MIB: usize = 2 * 1024 * 1024;
const SIXTEEN_MIB: usize = 16 * 1024 * 1024;
const MAX_SINGLE_COPY_2M_BYTES: usize = 3 * 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_SINGLE_COPY_16M_HWM_KIB: usize = 24 * 1024;

const SEED_2M: usize = 37;
#[cfg(target_os = "linux")]
const SEED_16M: usize = 91;

static TRACK_EPOCH: AtomicUsize = AtomicUsize::new(0);
static NEXT_TRACK_EPOCH: AtomicUsize = AtomicUsize::new(1);
static LARGE_ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static LIVE_LARGE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_LIVE_LARGE_BYTES: AtomicUsize = AtomicUsize::new(0);

#[repr(C)]
struct AllocHeader {
    magic: usize,
    base: *mut u8,
    total_size: usize,
    total_align: usize,
    track_epoch: usize,
    counted_bytes: usize,
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align().max(std::mem::align_of::<AllocHeader>());
        let offset = align.max(std::mem::size_of::<AllocHeader>()).max(64);
        let total_size = match offset.checked_add(layout.size()) {
            Some(n) => n,
            None => return std::ptr::null_mut(),
        };
        let sys_layout = match Layout::from_size_align(total_size, align) {
            Ok(l) => l,
            Err(_) => return std::ptr::null_mut(),
        };
        let base = System.alloc(sys_layout);
        if base.is_null() {
            return std::ptr::null_mut();
        }

        let epoch = TRACK_EPOCH.load(Ordering::SeqCst);
        let counted_bytes = if epoch != 0 && layout.size() >= LARGE_ALLOC_THRESHOLD {
            LARGE_ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
            let prev = LIVE_LARGE_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
            PEAK_LIVE_LARGE_BYTES.fetch_max(prev + layout.size(), Ordering::SeqCst);
            layout.size()
        } else {
            0
        };

        let user_ptr = base.add(offset);
        let header_ptr = (user_ptr as *mut AllocHeader).sub(1);
        header_ptr.write(AllocHeader {
            magic: HEADER_MAGIC,
            base,
            total_size,
            total_align: align,
            track_epoch: epoch,
            counted_bytes,
        });
        user_ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ptr.is_null() {
            libc::abort();
        }
        let header_ptr = (ptr as *mut AllocHeader).sub(1);
        let header = header_ptr.read();
        let expected_offset = header
            .total_align
            .max(std::mem::size_of::<AllocHeader>())
            .max(64);
        if header.magic != HEADER_MAGIC
            || header.total_align != layout.align().max(std::mem::align_of::<AllocHeader>())
            || header.total_size != expected_offset + layout.size()
        {
            libc::abort();
        }
        (*header_ptr).magic = 0;

        if header.counted_bytes > 0 && header.track_epoch == TRACK_EPOCH.load(Ordering::SeqCst) {
            LIVE_LARGE_BYTES.fetch_sub(header.counted_bytes, Ordering::SeqCst);
        }

        let sys_layout = Layout::from_size_align_unchecked(header.total_size, header.total_align);
        System.dealloc(header.base, sys_layout);
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Debug, Clone, Copy)]
struct AllocStats {
    name: &'static str,
    alloc_count: usize,
    peak_live_bytes: usize,
}

fn measure_large_allocs<R>(name: &'static str, f: impl FnOnce() -> R) -> (R, AllocStats) {
    LARGE_ALLOC_COUNT.store(0, Ordering::SeqCst);
    LIVE_LARGE_BYTES.store(0, Ordering::SeqCst);
    PEAK_LIVE_LARGE_BYTES.store(0, Ordering::SeqCst);
    let epoch = NEXT_TRACK_EPOCH.fetch_add(1, Ordering::SeqCst);
    TRACK_EPOCH.store(epoch, Ordering::SeqCst);

    let result = f();

    let stats = AllocStats {
        name,
        alloc_count: LARGE_ALLOC_COUNT.load(Ordering::SeqCst),
        peak_live_bytes: PEAK_LIVE_LARGE_BYTES.load(Ordering::SeqCst),
    };
    TRACK_EPOCH.store(0, Ordering::SeqCst);
    println!(
        "[alloc] {:<34} large_alloc_count={} peak_live_large_bytes={}",
        stats.name, stats.alloc_count, stats.peak_live_bytes
    );
    (result, stats)
}

async fn measure_large_allocs_async<F, R>(name: &'static str, fut: F) -> (R, AllocStats)
where
    F: Future<Output = R>,
{
    LARGE_ALLOC_COUNT.store(0, Ordering::SeqCst);
    LIVE_LARGE_BYTES.store(0, Ordering::SeqCst);
    PEAK_LIVE_LARGE_BYTES.store(0, Ordering::SeqCst);
    let epoch = NEXT_TRACK_EPOCH.fetch_add(1, Ordering::SeqCst);
    TRACK_EPOCH.store(epoch, Ordering::SeqCst);

    let result = fut.await;

    let stats = AllocStats {
        name,
        alloc_count: LARGE_ALLOC_COUNT.load(Ordering::SeqCst),
        peak_live_bytes: PEAK_LIVE_LARGE_BYTES.load(Ordering::SeqCst),
    };
    TRACK_EPOCH.store(0, Ordering::SeqCst);
    println!(
        "[alloc] {:<34} large_alloc_count={} peak_live_large_bytes={}",
        stats.name, stats.alloc_count, stats.peak_live_bytes
    );
    (result, stats)
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_bytes(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// 借用输入字节原地计算长度与 FNV-1a 校验和，自身零堆分配。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PayloadDigest {
    len: usize,
    fnv1a: u64,
}

impl PayloadDigest {
    fn of(bytes: &[u8]) -> Self {
        Self {
            len: bytes.len(),
            fnv1a: fnv1a_bytes(bytes),
        }
    }
}

struct PayloadDigestVisitor;

impl<'de> serde::de::Visitor<'de> for PayloadDigestVisitor {
    type Value = PayloadDigest;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a string or byte slice payload")
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        Ok(PayloadDigest::of(v.as_bytes()))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(PayloadDigest::of(v.as_bytes()))
    }

    fn visit_borrowed_bytes<E: serde::de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        Ok(PayloadDigest::of(v))
    }

    fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(PayloadDigest::of(v))
    }
}

impl<'de> serde::Deserialize<'de> for PayloadDigest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(PayloadDigestVisitor)
    }
}

const ASCII_ALPHABET: &[u8; 64] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";

#[inline]
fn expected_ascii_byte(index: usize, seed: usize) -> u8 {
    let slot = (index
        .wrapping_mul(131)
        .wrapping_add(seed)
        .wrapping_add(index >> 11))
        & 63;
    ASCII_ALPHABET[slot]
}

fn build_ascii_payload(len: usize, seed: usize) -> String {
    let mut bytes = Vec::with_capacity(len);
    for i in 0..len {
        bytes.push(expected_ascii_byte(i, seed));
    }
    String::from_utf8(bytes).expect("ASCII alphabet produces valid UTF-8")
}

fn verify_ascii_payload(label: &str, actual: &str, expected_len: usize, seed: usize) {
    assert_eq!(actual.len(), expected_len, "{label}: payload length");
    for (i, &b) in actual.as_bytes().iter().enumerate() {
        let want = expected_ascii_byte(i, seed);
        if b != want {
            panic!("{label}: byte mismatch at index {i}: got {b:#04x}, expected {want:#04x}");
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
struct HwmStats {
    name: &'static str,
    rss_before_kib: usize,
    hwm_after_kib: usize,
    delta_hwm_kib: usize,
    elapsed: Duration,
}

#[cfg(target_os = "linux")]
fn read_proc_status_kib(field_prefix: &str) -> usize {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field_prefix) {
            let digits = rest
                .split_whitespace()
                .next()
                .expect("numeric value in /proc/self/status");
            return digits.parse::<usize>().expect("parse KiB value");
        }
    }
    panic!("field {field_prefix} missing from /proc/self/status");
}

#[cfg(target_os = "linux")]
fn reset_vm_hwm() {
    #[cfg(target_env = "gnu")]
    unsafe {
        libc::malloc_trim(0);
    }
    std::fs::write("/proc/self/clear_refs", b"5\n").expect("write 5 to /proc/self/clear_refs");
}

#[cfg(target_os = "linux")]
fn measure_hwm_delta<R>(name: &'static str, f: impl FnOnce() -> R) -> (R, HwmStats) {
    reset_vm_hwm();
    let rss_before_kib = read_proc_status_kib("VmRSS:");
    let started = Instant::now();
    let out = std::hint::black_box(f());
    let elapsed = started.elapsed();
    let hwm_after_kib = read_proc_status_kib("VmHWM:");
    let delta_hwm_kib = hwm_after_kib.saturating_sub(rss_before_kib);
    let stats = HwmStats {
        name,
        rss_before_kib,
        hwm_after_kib,
        delta_hwm_kib,
        elapsed,
    };
    println!(
        "[vmhwm] {:<34} rss_before={} KiB hwm_after={} KiB delta_hwm={} KiB elapsed={:?}",
        stats.name, stats.rss_before_kib, stats.hwm_after_kib, stats.delta_hwm_kib, stats.elapsed
    );
    (out, stats)
}

#[test]
fn get_paths_avoid_intermediate_payload_copies() {
    // 固定 glibc 的 M_MMAP_THRESHOLD 为 1 MiB（同时关闭动态阈值上调），使每次 >= 1 MiB 的
    // C 堆分配均独立走 mmap 并在 free 时立即 munmap，消除跨调用的 arena 空闲页残留干扰。
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        const M_MMAP_THRESHOLD: libc::c_int = -3;
        libc::mallopt(M_MMAP_THRESHOLD, 1024 * 1024);
    }

    // 自检自定义全局分配器的大块追踪钩子处于激活状态。
    {
        let (probe_len, probe_stats) = measure_large_allocs("allocator_self_check", || {
            let v = std::hint::black_box(vec![0x5au8; LARGE_ALLOC_THRESHOLD]);
            v.len()
        });
        assert_eq!(probe_len, LARGE_ALLOC_THRESHOLD);
        assert_eq!(
            probe_stats.alloc_count, 1,
            "CountingAllocator must record >= 512 KiB allocations"
        );
        assert_eq!(probe_stats.peak_live_bytes, LARGE_ALLOC_THRESHOLD);
    }

    let config = RayConfig {
        local_mode: true,
        ..Default::default()
    };
    let ray = Ray::connect(&config).expect("init local Ray");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    // 预热运行时的本地存储读取路径与 tokio blocking 线程池，使后续统计聚焦于被测读取调用。
    let warm_ref = ray.put(&12345i64).expect("put warmup value");
    assert_eq!(warm_ref.get().expect("warmup sync get"), 12345i64);
    let warm_async = rt.block_on(warm_ref.get_async()).expect("warmup async get");
    assert_eq!(warm_async, 12345i64);
    drop(warm_ref);

    // ── 第一部分：2 MiB 字符串对象的 Rust 大块堆分配精确计数 ─────────────
    let payload_2m = build_ascii_payload(TWO_MIB, SEED_2M);
    let expected_digest_2m = PayloadDigest::of(payload_2m.as_bytes());
    let obj_str: ObjectRef<String> = ray.put(&payload_2m).expect("put 2 MiB string");
    let obj_digest: ObjectRef<PayloadDigest> = obj_str.clone().cast();
    drop(payload_2m);

    let mut zero_alloc_checks: Vec<AllocStats> = Vec::new();
    let mut single_alloc_checks: Vec<AllocStats> = Vec::new();

    // 1. 借用切片的零分配类型连续读取：同步、带超时、异步、带超时异步。
    let (d1, s) = measure_large_allocs("digest.get#1", || obj_digest.get().expect("digest.get #1"));
    assert_eq!(d1, expected_digest_2m);
    zero_alloc_checks.push(s);

    let (d2, s) = measure_large_allocs("digest.get#2", || obj_digest.get().expect("digest.get #2"));
    assert_eq!(d2, expected_digest_2m);
    zero_alloc_checks.push(s);

    let (d3, s) = measure_large_allocs("digest.get_timeout", || {
        obj_digest
            .get_timeout(5_000)
            .expect("digest.get_timeout(5_000)")
    });
    assert_eq!(d3, expected_digest_2m);
    zero_alloc_checks.push(s);

    let (d4, s) = rt.block_on(measure_large_allocs_async(
        "digest.get_async",
        obj_digest.get_async(),
    ));
    assert_eq!(d4.expect("digest.get_async"), expected_digest_2m);
    zero_alloc_checks.push(s);

    let (d5, s) = rt.block_on(measure_large_allocs_async(
        "digest.get_timeout_async",
        obj_digest.get_timeout_async(Duration::from_secs(5)),
    ));
    assert_eq!(d5.expect("digest.get_timeout_async"), expected_digest_2m);
    zero_alloc_checks.push(s);

    // 2. 含有重复 ID 的同步与异步批量读取。
    let batch_sync_refs = [obj_digest.clone(), obj_digest.clone(), obj_digest.clone()];
    let (batch_sync, s) = measure_large_allocs("digest.get_many(x3_dup)", || {
        ray.get_many(&batch_sync_refs)
            .expect("get_many with duplicate digest refs")
    });
    assert_eq!(batch_sync, vec![expected_digest_2m; 3]);
    zero_alloc_checks.push(s);
    drop(batch_sync_refs);

    let batch_async_refs = [obj_digest.clone(), obj_digest.clone()];
    let (batch_async, s) = rt.block_on(measure_large_allocs_async(
        "digest.get_many_async(x2_dup)",
        ray.get_many_async(&batch_async_refs),
    ));
    assert_eq!(
        batch_async.expect("get_many_async with duplicate digest refs"),
        vec![expected_digest_2m; 2]
    );
    zero_alloc_checks.push(s);
    drop(batch_async_refs);

    // 3. 拥有所有权的 String 读取：每次仅返回对象本身 1 次大块分配。
    {
        let (got, s) = measure_large_allocs("string.get", || obj_str.get().expect("string.get"));
        verify_ascii_payload("string.get", &got, TWO_MIB, SEED_2M);
        single_alloc_checks.push(s);
    }
    {
        let (got, s) = measure_large_allocs("string.get_timeout", || {
            obj_str
                .get_timeout(5_000)
                .expect("string.get_timeout(5_000)")
        });
        verify_ascii_payload("string.get_timeout", &got, TWO_MIB, SEED_2M);
        single_alloc_checks.push(s);
    }
    {
        let (got, s) = rt.block_on(measure_large_allocs_async(
            "string.get_async",
            obj_str.get_async(),
        ));
        let got = got.expect("string.get_async");
        verify_ascii_payload("string.get_async", &got, TWO_MIB, SEED_2M);
        single_alloc_checks.push(s);
    }

    // 4. 通用 rmpv::Value 读取：同步与异步。
    {
        let (val, s) = measure_large_allocs("string.get_value", || {
            obj_str.get_value().expect("string.get_value")
        });
        let text = val.as_str().expect("rmpv::Value must hold str");
        verify_ascii_payload("string.get_value", text, TWO_MIB, SEED_2M);
        single_alloc_checks.push(s);
    }
    {
        let (val, s) = rt.block_on(measure_large_allocs_async(
            "string.get_value_async",
            obj_str.get_value_async(),
        ));
        let val = val.expect("string.get_value_async");
        let text = val.as_str().expect("rmpv::Value must hold str");
        verify_ascii_payload("string.get_value_async", text, TWO_MIB, SEED_2M);
        single_alloc_checks.push(s);
    }

    // 5. 原始 msgpack 字节读取。
    {
        let (raw, s) = measure_large_allocs("string.get_raw_bytes", || {
            obj_str.get_raw_bytes().expect("string.get_raw_bytes")
        });
        // 2 MiB 字符串编码为 msgpack str32：1 字节标记 0xdb + 4 字节大端长度 + 正文。
        assert_eq!(raw.len(), 5 + TWO_MIB);
        assert_eq!(raw[0], 0xdb);
        assert_eq!(
            u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize,
            TWO_MIB
        );
        let decoded_digest: PayloadDigest =
            deserialize(&raw).expect("deserialize PayloadDigest from raw bytes");
        assert_eq!(decoded_digest, expected_digest_2m);
        let raw_str = std::str::from_utf8(&raw[5..]).expect("raw msgpack str32 body UTF-8");
        verify_ascii_payload("string.get_raw_bytes", raw_str, TWO_MIB, SEED_2M);
        single_alloc_checks.push(s);
    }

    drop(obj_digest);
    drop(obj_str);

    // ── 第二部分：Linux 16 MiB 对象物理内存峰值 VmHWM 增量核验 ──────────
    #[cfg(target_os = "linux")]
    let hwm_checks: Vec<HwmStats> = {
        let payload_16m = build_ascii_payload(SIXTEEN_MIB, SEED_16M);
        let expected_digest_16m = PayloadDigest::of(payload_16m.as_bytes());
        let digest_16m: ObjectRef<PayloadDigest> = ray
            .put(&payload_16m)
            .expect("put 16 MiB string")
            .cast::<PayloadDigest>();
        drop(payload_16m);

        let (got_sync, hwm_sync) = measure_hwm_delta("16MiB digest.get", || {
            digest_16m.get().expect("16 MiB sync get")
        });
        assert_eq!(got_sync, expected_digest_16m);

        let (got_async, hwm_async) = measure_hwm_delta("16MiB digest.get_async", || {
            rt.block_on(digest_16m.get_async())
                .expect("16 MiB async get")
        });
        assert_eq!(got_async, expected_digest_16m);

        let (got_batch, hwm_batch) = measure_hwm_delta("16MiB ray.get_many(x1)", || {
            ray.get_many(std::slice::from_ref(&digest_16m))
                .expect("16 MiB get_many")
        });
        assert_eq!(got_batch, vec![expected_digest_16m]);

        drop(digest_16m);
        vec![hwm_sync, hwm_async, hwm_batch]
    };

    drop(rt);
    drop(ray);

    // ── 统一核验所有分配计数与物理内存峰值不变量 ────────────────────────
    for s in &zero_alloc_checks {
        assert_eq!(
            s.alloc_count, 0,
            "{}: expected 0 large Rust heap allocations, got {} (peak={} bytes)",
            s.name, s.alloc_count, s.peak_live_bytes
        );
        assert_eq!(
            s.peak_live_bytes, 0,
            "{}: expected 0 peak live large Rust bytes, got {}",
            s.name, s.peak_live_bytes
        );
    }

    for s in &single_alloc_checks {
        assert_eq!(
            s.alloc_count, 1,
            "{}: expected exactly 1 large Rust heap allocation for the returned value, got {} (peak={} bytes)",
            s.name, s.alloc_count, s.peak_live_bytes
        );
        assert!(
            s.peak_live_bytes < MAX_SINGLE_COPY_2M_BYTES,
            "{}: peak live large Rust bytes {} exceeded single-copy bound {}",
            s.name,
            s.peak_live_bytes,
            MAX_SINGLE_COPY_2M_BYTES
        );
    }

    #[cfg(target_os = "linux")]
    for h in &hwm_checks {
        assert!(
            h.delta_hwm_kib < MAX_SINGLE_COPY_16M_HWM_KIB,
            "{}: VmHWM delta {} KiB (rss_before={} KiB, hwm_after={} KiB) exceeded single-copy bound {} KiB",
            h.name,
            h.delta_hwm_kib,
            h.rss_before_kib,
            h.hwm_after_kib,
            MAX_SINGLE_COPY_16M_HWM_KIB
        );
    }
}

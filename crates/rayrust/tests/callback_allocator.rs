//! 带头部间距的自定义 `#[global_allocator]` 下回调缓冲区双向内存管理回归测试。
//!
//! 本测试进程启用 `OffsetAllocator`，将每次 Rust 堆分配的用户指针相对底层
//! `std::alloc::System` 块首地址向后移动至少 64 字节。C++ 包装层释放回调
//! 返回的 `data` 缓冲区时调用 `std::free`，只有通过 C 标准库 `malloc` 分配的
//! 缓冲区才能被安全释放。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use rayrust::prelude::*;

const HEADER_MAGIC: usize = 0x5241_5952_5553_5428;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

#[repr(C)]
struct Header {
    magic: usize,
    base: *mut u8,
    total_size: usize,
    total_align: usize,
}

struct OffsetAllocator;

unsafe impl GlobalAlloc for OffsetAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align().max(std::mem::align_of::<Header>());
        let offset = align.max(std::mem::size_of::<Header>()).max(64);
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
        let user_ptr = base.add(offset);
        let header_ptr = (user_ptr as *mut Header).sub(1);
        header_ptr.write(Header {
            magic: HEADER_MAGIC,
            base,
            total_size,
            total_align: align,
        });
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        user_ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ptr.is_null() {
            libc::abort();
        }
        let header_ptr = (ptr as *mut Header).sub(1);
        let header = header_ptr.read();
        let expected_offset = header
            .total_align
            .max(std::mem::size_of::<Header>())
            .max(64);
        if header.magic != HEADER_MAGIC
            || header.total_align != layout.align().max(std::mem::align_of::<Header>())
            || header.total_size != expected_offset + layout.size()
        {
            libc::abort();
        }
        (*header_ptr).magic = 0;
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        let sys_layout = Layout::from_size_align_unchecked(header.total_size, header.total_align);
        System.dealloc(header.base, sys_layout);
    }
}

#[global_allocator]
static GLOBAL: OffsetAllocator = OffsetAllocator;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct PayloadInfo {
    tag: String,
    items: Vec<u64>,
}

#[rayrust::remote]
fn alloc_unit() {}

#[rayrust::remote]
fn alloc_struct(tag: String, count: u64) -> PayloadInfo {
    PayloadInfo {
        tag,
        items: (0..count).collect(),
    }
}

#[rayrust::remote]
fn alloc_large_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

#[rayrust::remote]
async fn alloc_async_message(prefix: String, len: usize) -> String {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    format!("{prefix}:{}", "x".repeat(len))
}

#[rayrust::remote]
fn alloc_panic_task() -> i64 {
    panic!("boom with custom allocator");
}

struct AllocActor {
    chunks: Vec<Vec<u8>>,
}

#[rayrust::actor]
impl AllocActor {
    fn new(initial: Vec<u8>) -> Self {
        AllocActor {
            chunks: vec![initial],
        }
    }

    fn push_and_concat(&mut self, next: Vec<u8>) -> Vec<u8> {
        self.chunks.push(next);
        let mut out = Vec::new();
        for c in &self.chunks {
            out.extend_from_slice(c);
        }
        out
    }

    fn summary(&self) -> String {
        let total: usize = self.chunks.iter().map(|c| c.len()).sum();
        format!("chunks={},bytes={}", self.chunks.len(), total)
    }

    fn actor_version() -> String {
        "alloc-actor-v1".to_string()
    }

    fn trigger_panic(&mut self) -> i64 {
        panic!("actor panic with custom allocator");
    }
}

fn assert_local_pack_error(label: &str, bytes: &[u8], expected_substrings: &[&str]) {
    assert!(
        bytes.starts_with(&[0xc0, 0x92, 0x03]),
        "{label}: expected PackError prefix [0xc0, 0x92, 0x03], got {:?}",
        &bytes[..bytes.len().min(16)]
    );
    let text = String::from_utf8_lossy(&bytes[3..]);
    for needle in expected_substrings {
        assert!(
            text.contains(needle),
            "{label}: error payload does not contain {needle:?}; payload={text:?}"
        );
    }
}

#[test]
fn callback_buffers_survive_offset_global_allocator() {
    let base_allocs = ALLOC_COUNT.load(Ordering::Relaxed);
    let base_deallocs = DEALLOC_COUNT.load(Ordering::Relaxed);
    {
        let probe = vec![7u8; 256];
        assert_eq!(probe.len(), 256);
    }
    assert!(
        ALLOC_COUNT.load(Ordering::Relaxed) > base_allocs,
        "OffsetAllocator alloc hook must be active"
    );
    assert!(
        DEALLOC_COUNT.load(Ordering::Relaxed) > base_deallocs,
        "OffsetAllocator dealloc hook must be active"
    );

    let ray = Ray::local().expect("init local Ray");

    // 1. 同步 #[rayrust::remote] 正常返回：单元类型、结构体与 64 KiB 大载荷
    alloc_unit_remote(&ray)
        .expect("submit alloc_unit")
        .get_timeout(5_000)
        .expect("get alloc_unit");

    let info = alloc_struct_remote(&ray, "t44".to_string(), 16)
        .expect("submit alloc_struct")
        .get_timeout(5_000)
        .expect("get alloc_struct");
    assert_eq!(
        info,
        PayloadInfo {
            tag: "t44".to_string(),
            items: (0..16).collect(),
        }
    );

    let large_len = 64 * 1024;
    let large_back = alloc_large_bytes_remote(&ray, large_len, 19)
        .expect("submit alloc_large_bytes")
        .get_timeout(5_000)
        .expect("get alloc_large_bytes");
    let expected_large: Vec<u8> = (0..large_len)
        .map(|i| 19u8.wrapping_add((i % 251) as u8))
        .collect();
    assert_eq!(large_back, expected_large);

    // 2. 异步 #[rayrust::remote] async fn 正常返回
    let async_msg = alloc_async_message_remote(&ray, "async".to_string(), 512)
        .expect("submit alloc_async_message")
        .get_timeout(5_000)
        .expect("get alloc_async_message");
    assert_eq!(async_msg, format!("async:{}", "x".repeat(512)));

    // 3. #[rayrust::remote] panic! 与参数错误路径（经过 error_result）
    let panic_raw = alloc_panic_task_remote(&ray)
        .expect("submit alloc_panic_task")
        .get_raw_bytes()
        .expect("raw bytes for panic task");
    assert_local_pack_error(
        "remote panic task",
        &panic_raw,
        &["boom with custom allocator"],
    );

    let bad_count_raw = ray
        .task_call("alloc_struct", &[], &[], &TaskOptions::new())
        .expect("submit alloc_struct with wrong arg count")
        .get_raw_bytes()
        .expect("raw bytes for wrong arg count");
    assert_local_pack_error(
        "remote wrong arg count",
        &bad_count_raw,
        &["expected 2 argument(s), got 0"],
    );

    let wrong_type_arg = serialize(&12345i64).unwrap();
    let second_arg = serialize(&4u64).unwrap();
    let bad_type_raw = ray
        .task_call(
            "alloc_struct",
            &[&wrong_type_arg, &second_arg],
            &[],
            &TaskOptions::new(),
        )
        .expect("submit alloc_struct with wrong arg type")
        .get_raw_bytes()
        .expect("raw bytes for wrong arg type");
    assert_local_pack_error(
        "remote wrong arg type",
        &bad_type_raw,
        &["failed to deserialize argument 0 of alloc_struct"],
    );

    let after_err = alloc_struct_remote(&ray, "recovered".to_string(), 4)
        .expect("submit alloc_struct after errors")
        .get_timeout(5_000)
        .expect("get alloc_struct after errors");
    assert_eq!(after_err.tag, "recovered");

    // 4. #[rayrust::actor] 构造、&mut self、&self、并发线程、无 self 关联函数与 panic
    let initial = vec![1u8; 4096];
    let actor =
        allocactor_actor_create(&ray, &ActorOptions::new(), initial.clone()).expect("create actor");

    let chunk_a = vec![2u8; 8192];
    let combined = allocactor_push_and_concat_sync(&ray, &actor, chunk_a.clone())
        .expect("submit push_and_concat")
        .get_timeout(5_000)
        .expect("get push_and_concat");
    assert_eq!(combined.len(), 4096 + 8192);
    assert!(combined[..4096].iter().all(|&b| b == 1));
    assert!(combined[4096..].iter().all(|&b| b == 2));

    let summary = allocactor_summary_sync(&ray, &actor)
        .expect("submit summary")
        .get_timeout(5_000)
        .expect("get summary");
    assert_eq!(summary, "chunks=2,bytes=12288");

    // 多线程并发调用 &mut self 与 &self
    std::thread::scope(|s| {
        for tid in 0..4u8 {
            let ray_ref = &ray;
            let actor_ref = &actor;
            s.spawn(move || {
                let extra = vec![tid + 10; 1024];
                let out = allocactor_push_and_concat_sync(ray_ref, actor_ref, extra)
                    .expect("concurrent push_and_concat")
                    .get_timeout(5_000)
                    .expect("get concurrent push_and_concat");
                assert!(out.len() >= 12288 + 1024);
                let cur_summary = allocactor_summary_sync(ray_ref, actor_ref)
                    .expect("concurrent summary")
                    .get_timeout(5_000)
                    .expect("get concurrent summary");
                assert!(cur_summary.starts_with("chunks="));
            });
        }
    });

    let ver = allocactor_actor_version_sync(&ray, &actor)
        .expect("submit actor_version")
        .get_timeout(5_000)
        .expect("get actor_version");
    assert_eq!(ver, "alloc-actor-v1");

    let actor_panic_raw = allocactor_trigger_panic_sync(&ray, &actor)
        .expect("submit trigger_panic")
        .get_raw_bytes()
        .expect("raw bytes for actor panic");
    assert_local_pack_error(
        "actor method panic",
        &actor_panic_raw,
        &["actor panic with custom allocator"],
    );

    let ver_after_panic = allocactor_actor_version_sync(&ray, &actor)
        .expect("submit actor_version after panic")
        .get_timeout(5_000)
        .expect("get actor_version after panic");
    assert_eq!(ver_after_panic, "alloc-actor-v1");

    drop(ray);

    let total_allocs = ALLOC_COUNT.load(Ordering::Relaxed);
    let total_deallocs = DEALLOC_COUNT.load(Ordering::Relaxed);
    println!("OffsetAllocator totals: alloc={total_allocs}, dealloc={total_deallocs}");
    assert!(
        total_allocs > base_allocs + 100,
        "expected significant OffsetAllocator allocations, got {total_allocs} (base {base_allocs})"
    );
    assert!(
        total_deallocs > base_deallocs + 100,
        "expected significant OffsetAllocator deallocations, got {total_deallocs} (base {base_deallocs})"
    );
}

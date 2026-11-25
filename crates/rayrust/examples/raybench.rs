//! Benchmark: rayrust SDK dispatch overhead and task-body execution on Ray.
//!
//! Measures:
//! 1. SDK dispatch: Rust -> Rust sequential sync (N=500, add)
//! 2. SDK dispatch: Rust -> Rust concurrent + batched get_many_async (N=500, add)
//! 3. SDK dispatch: Rust -> Rust concurrent + per-ref get_async (N=500, add)
//! 4. SDK xlang dispatch: Rust -> Python concurrent + batched get_many_async (N=500, add)
//! 5. Single-task round-trip latency (N=100, median & p99)
//! 6. Async worker concurrency (100 tasks x 50ms tokio sleep)
//! 7. Task-body language comparison: sum(0..1_000_000) x 10 concurrent tasks
//! 8. Complex payload serialization breakdown (100x100 nested Vec<Vec<i64>>, N=50)
//!
//! Build and run from the repository root:
//!   cargo build --release --all-targets
//!   RAY_ADDRESS=127.0.0.1:6379 RAY_NODE_IP=127.0.0.1 \
//!   cargo run --release --example raybench
//!
//! Optional: set `RAY_WORKER_SO` to override the default
//! `target/release/librayrust_worker.so` path. `PYTHONPATH` for `rayrust_test`
//! (`tests/python`) is passed automatically via `runtime_env`.

use rayrust::prelude::*;
use std::time::{Duration, Instant};

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[rayrust::remote]
async fn async_sum(a: i64, b: i64) -> i64 {
    tokio::time::sleep(Duration::from_millis(50)).await;
    a + b
}

/// CPU-intensive: sum of 0..n.
#[rayrust::remote]
fn compute(n: i64) -> i64 {
    (0..n).sum()
}

/// Worker-side directory of `rayrust_test`, passed through `runtime_env`.
fn python_module_dir() -> String {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/python");
    let path = std::fs::canonicalize(dir).expect("canonicalize the Python test module directory");
    path.to_str()
        .expect("the Python module path is valid UTF-8")
        .to_string()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000_000.0
}

/// Match `ray start`'s `resolve_ip_for_localhost`: when `node_ip` is loopback,
/// query the kernel routing table via a UDP socket (zero packets transmitted)
/// so the C++ driver matches the local raylet registered by `ray start`.
fn resolve_node_ip(host: &str) -> String {
    if host == "127.0.0.1" || host == "::1" || host == "localhost" {
        std::net::UdpSocket::bind("0.0.0.0:0")
            .and_then(|sock| {
                sock.connect("8.8.8.8:53")?;
                Ok(sock.local_addr()?.ip().to_string())
            })
            .unwrap_or_else(|_| "127.0.0.1".to_string())
    } else {
        host.to_string()
    }
}

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());
    let raw_node_ip = std::env::var("RAY_NODE_IP")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let node_ip = resolve_node_ip(&raw_node_ip);
    let worker_so = std::env::var("RAY_WORKER_SO")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../target/release/librayrust_worker.so"
            )
            .to_string()
        });
    if !std::path::Path::new(&worker_so).exists() {
        eprintln!(
            "ERROR: worker library missing at {}. Build it first:",
            worker_so
        );
        eprintln!("  cargo build --release --all-targets");
        std::process::exit(1);
    }
    let worker_so = std::fs::canonicalize(&worker_so)
        .expect("canonicalize worker library path")
        .to_str()
        .expect("worker library path is valid UTF-8")
        .to_string();

    println!("=== Rayrust Benchmark ===");
    println!("address: {}", address);

    let python_dir = python_module_dir();
    let runtime_env = serde_json::json!({
        "env_vars": {
            "PYTHONPATH": python_dir,
        }
    })
    .to_string();
    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so])
        .runtime_env(runtime_env);
    let ray = Ray::connect(&config).expect("init failed");
    println!("status: Ray initialized");

    // Complete worker warmup before starting any benchmark timer.
    let w_add = add_remote(&ray, 1, 1)
        .expect("warmup add submit")
        .get()
        .expect("warmup add get");
    assert_eq!(w_add, 2);

    let w_comp = compute_remote(&ray, 1_000)
        .expect("warmup compute submit")
        .get()
        .expect("warmup compute get");
    assert_eq!(w_comp, 499_500);

    let w_async = async_sum_remote_async(&ray, 1, 1)
        .await
        .expect("warmup async_sum submit")
        .get_async()
        .await
        .expect("warmup async_sum get");
    assert_eq!(w_async, 2);

    let warmup_a = rayrust::serialize(&1i64).unwrap();
    let warmup_b = rayrust::serialize(&1i64).unwrap();
    let w_py_add: i64 = ray
        .task_call_python("rayrust_test", "add", &[&warmup_a, &warmup_b], &[])
        .expect("warmup python add submit")
        .cast::<i64>()
        .get()
        .expect("warmup python add get");
    assert_eq!(w_py_add, 2);

    let warmup_n = rayrust::serialize(&1_000i64).unwrap();
    let w_py_comp: i64 = ray
        .task_call_python("rayrust_test", "compute", &[&warmup_n], &[])
        .expect("warmup python compute submit")
        .cast::<i64>()
        .get()
        .expect("warmup python compute get");
    assert_eq!(w_py_comp, 499_500);

    let warmup_list = rayrust::serialize(&vec![vec![1i64]]).unwrap();
    let _w_py_echo = ray
        .task_call_python("rayrust_test", "echo_list", &[&warmup_list], &[])
        .expect("warmup python echo_list submit")
        .get_value()
        .expect("warmup python echo_list get");

    // Warm all 16 C++ and 16 Python workers concurrently so timed arms measure
    // steady-state execution after worker process startup and module loading.
    let warmup_cpp_sleep = rayrust::serialize(&0.25f64).unwrap();
    let mut w_rust_pool: Vec<ObjectRef<i64>> = Vec::with_capacity(16);
    for _ in 0..16 {
        let r = ray
            .task_call("sleep_for", &[&warmup_cpp_sleep], &[], &TaskOptions::new())
            .expect("warmup rust pool submit")
            .cast::<i64>();
        w_rust_pool.push(r);
    }
    let _ = ray
        .get_many_async(&w_rust_pool)
        .await
        .expect("warmup rust pool get");

    let warmup_sleep = rayrust::serialize(&0.30f64).unwrap();
    let mut w_py_pool: Vec<ObjectRef<i64>> = Vec::with_capacity(16);
    for _ in 0..16 {
        let r = ray
            .task_call_python("rayrust_test", "sleep_for", &[&warmup_sleep], &[])
            .expect("warmup python pool submit")
            .cast::<i64>();
        w_py_pool.push(r);
    }
    let _ = ray
        .get_many_async(&w_py_pool)
        .await
        .expect("warmup python pool get");

    println!("status: Warmup completed\n");

    // [1] SDK dispatch: Rust -> Rust sequential sync
    println!("[1] SDK dispatch: Rust -> Rust sequential sync (N=500, add(i, 1))");
    let n: i32 = 500;
    let t0 = Instant::now();
    let mut sum = 0i64;
    for i in 0..n {
        let r = add_remote(&ray, i, 1).expect("submit add");
        let v: i32 = r.get().expect("get failed");
        sum += v as i64;
    }
    let sync_elapsed = t0.elapsed();
    assert_eq!(sum, 125_250);
    let sync_tps = n as f64 / sync_elapsed.as_secs_f64();
    println!(
        "  {} tasks in {:.2}ms -> {:.0} tasks/sec, {:.2}ms/task (checksum={})\n",
        n,
        ms(sync_elapsed),
        sync_tps,
        ms(sync_elapsed) / n as f64,
        sum
    );

    // [2] SDK dispatch: Rust -> Rust concurrent + batched get
    println!("[2] SDK dispatch: Rust -> Rust concurrent + batched get (N=500, add(i, 1))");
    let t0 = Instant::now();
    let mut futs = Vec::with_capacity(n as usize);
    for i in 0..n {
        futs.push(add_remote_async(&ray, i, 1));
    }
    let mut refs = Vec::with_capacity(n as usize);
    for f in futs {
        refs.push(f.await.expect("submit failed"));
    }
    let vals = ray
        .get_many_async(&refs)
        .await
        .expect("get_many_async failed");
    let sum: i64 = vals.iter().map(|&v| v as i64).sum();
    let batched_elapsed = t0.elapsed();
    assert_eq!(sum, 125_250);
    let batched_tps = n as f64 / batched_elapsed.as_secs_f64();
    println!(
        "  {} tasks in {:.2}ms -> {:.0} tasks/sec, {:.2}ms/task (checksum={})\n",
        n,
        ms(batched_elapsed),
        batched_tps,
        ms(batched_elapsed) / n as f64,
        sum
    );

    // [3] SDK dispatch: Rust -> Rust concurrent + per-ref get_async
    println!("[3] SDK dispatch: Rust -> Rust concurrent + per-ref get_async (N=500, add(i, 1))");
    let t0 = Instant::now();
    let mut futs = Vec::with_capacity(n as usize);
    for i in 0..n {
        futs.push(add_remote_async(&ray, i, 1));
    }
    let mut refs = Vec::with_capacity(n as usize);
    for f in futs {
        refs.push(f.await.expect("submit failed"));
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.get_async().await.expect("get_async failed") });
    }
    let mut sum = 0i64;
    while let Some(res) = set.join_next().await {
        sum += res.unwrap() as i64;
    }
    let per_ref_elapsed = t0.elapsed();
    assert_eq!(sum, 125_250);
    let per_ref_tps = n as f64 / per_ref_elapsed.as_secs_f64();
    println!(
        "  {} tasks in {:.2}ms -> {:.0} tasks/sec, {:.2}ms/task (checksum={})\n",
        n,
        ms(per_ref_elapsed),
        per_ref_tps,
        ms(per_ref_elapsed) / n as f64,
        sum
    );

    // [4] SDK xlang dispatch: Rust -> Python concurrent + batched get
    println!(
        "[4] SDK xlang dispatch: Rust -> Python concurrent + batched get (N=500, rayrust_test.add)"
    );
    let t0 = Instant::now();
    let arg_one = rayrust::serialize(&1i64).unwrap();
    let mut py_refs: Vec<ObjectRef<i64>> = Vec::with_capacity(n as usize);
    for i in 0..n {
        let arg_i = rayrust::serialize(&(i as i64)).unwrap();
        let args: Vec<&[u8]> = vec![&arg_i, &arg_one];
        let r = ray
            .task_call_python("rayrust_test", "add", &args, &[])
            .expect("python task failed")
            .cast::<i64>();
        py_refs.push(r);
    }
    let py_vals = ray
        .get_many_async(&py_refs)
        .await
        .expect("python get_many_async failed");
    let sum: i64 = py_vals.iter().sum();
    let xlang_elapsed = t0.elapsed();
    assert_eq!(sum, 125_250);
    let xlang_tps = n as f64 / xlang_elapsed.as_secs_f64();
    println!(
        "  {} tasks in {:.2}ms -> {:.0} tasks/sec, {:.2}ms/task (checksum={})\n",
        n,
        ms(xlang_elapsed),
        xlang_tps,
        ms(xlang_elapsed) / n as f64,
        sum
    );

    // [5] Single-task round-trip latency (N=100, median & p99)
    println!("[5] Single-task round-trip latency (N=100, median & p99)");
    let n_lat = 100usize;

    let mut times_rust_sync = Vec::with_capacity(n_lat);
    for _ in 0..n_lat {
        let t0 = Instant::now();
        let r = add_remote(&ray, 1, 2).expect("submit add");
        let v: i32 = r.get().unwrap();
        assert_eq!(v, 3);
        times_rust_sync.push(t0.elapsed());
    }
    times_rust_sync.sort();

    let mut times_rust_async = Vec::with_capacity(n_lat);
    for _ in 0..n_lat {
        let t0 = Instant::now();
        let r = add_remote_async(&ray, 1, 2).await.unwrap();
        let v: i32 = r.get_async().await.unwrap();
        assert_eq!(v, 3);
        times_rust_async.push(t0.elapsed());
    }
    times_rust_async.sort();

    let mut times_python = Vec::with_capacity(n_lat);
    for _ in 0..n_lat {
        let t0 = Instant::now();
        let arg_a = rayrust::serialize(&1i64).unwrap();
        let arg_b = rayrust::serialize(&2i64).unwrap();
        let args: Vec<&[u8]> = vec![&arg_a, &arg_b];
        let r = ray
            .task_call_python("rayrust_test", "add", &args, &[])
            .unwrap();
        let v: i64 = r.cast::<i64>().get_async().await.unwrap();
        assert_eq!(v, 3);
        times_python.push(t0.elapsed());
    }
    times_python.sort();

    let median = |v: &[Duration]| v[v.len() / 2];
    let p99 = |v: &[Duration]| v[v.len() * 99 / 100];
    println!(
        "  Rust sync:          median {:>6.2}ms  p99 {:>6.2}ms",
        ms(median(&times_rust_sync)),
        ms(p99(&times_rust_sync))
    );
    println!(
        "  Rust async:         median {:>6.2}ms  p99 {:>6.2}ms",
        ms(median(&times_rust_async)),
        ms(p99(&times_rust_async))
    );
    println!(
        "  Rust -> Python:     median {:>6.2}ms  p99 {:>6.2}ms\n",
        ms(median(&times_python)),
        ms(p99(&times_python))
    );

    // [6] Async worker concurrency (100 tasks x 50ms tokio sleep)
    println!("[6] Async worker concurrency (100 tasks x 50ms tokio sleep)");
    let n_sleep: i64 = 100;
    let t0 = Instant::now();
    let mut sleep_refs = Vec::with_capacity(n_sleep as usize);
    for i in 0..n_sleep {
        sleep_refs.push(async_sum_remote(&ray, i, 0).expect("submit async_sum"));
    }
    let sleep_vals = ray
        .get_many_async(&sleep_refs)
        .await
        .expect("async_sum get_many_async failed");
    let sleep_elapsed = t0.elapsed();
    let sleep_sum: i64 = sleep_vals.iter().sum();
    assert_eq!(sleep_sum, 4_950);
    let serial_expected = Duration::from_millis(50 * n_sleep as u64);
    let parallel_speedup = serial_expected.as_secs_f64() / sleep_elapsed.as_secs_f64();
    println!(
        "  {} tasks x 50ms in {:.2}ms (serial baseline {:.0}ms) -> {:.2}x parallel speedup\n",
        n_sleep,
        ms(sleep_elapsed),
        ms(serial_expected),
        parallel_speedup
    );

    // [7] Task-body language comparison: sum(0..1_000_000) x 10 concurrent tasks
    println!("[7] Task-body language comparison: sum(0..1_000_000) x 10 concurrent tasks");
    println!("  Language execution comparison (compiled Rust vs interpreted CPython, end-to-end for 10 tasks)");
    let n_tasks = 10usize;
    let n_compute: i64 = 1_000_000;
    let expected_compute: i64 = 499_999_500_000;

    let t0 = Instant::now();
    let mut rust_comp_refs = Vec::with_capacity(n_tasks);
    for _ in 0..n_tasks {
        rust_comp_refs.push(compute_remote(&ray, n_compute).expect("submit compute"));
    }
    let rust_comp_vals = ray
        .get_many_async(&rust_comp_refs)
        .await
        .expect("compute get_many_async failed");
    let rust_compute_elapsed = t0.elapsed();
    assert!(rust_comp_vals.iter().all(|&v| v == expected_compute));

    let t0 = Instant::now();
    let arg_comp = rayrust::serialize(&n_compute).unwrap();
    let mut py_comp_refs: Vec<ObjectRef<i64>> = Vec::with_capacity(n_tasks);
    for _ in 0..n_tasks {
        let args: Vec<&[u8]> = vec![&arg_comp];
        let r = ray
            .task_call_python("rayrust_test", "compute", &args, &[])
            .expect("python compute failed")
            .cast::<i64>();
        py_comp_refs.push(r);
    }
    let py_comp_vals = ray
        .get_many_async(&py_comp_refs)
        .await
        .expect("python compute get_many_async failed");
    let python_compute_elapsed = t0.elapsed();
    assert!(py_comp_vals.iter().all(|&v| v == expected_compute));

    let compute_speedup = python_compute_elapsed.as_secs_f64() / rust_compute_elapsed.as_secs_f64();
    println!(
        "  Rust (compiled):          {:>7.2}ms ({:.0} tasks/sec)",
        ms(rust_compute_elapsed),
        n_tasks as f64 / rust_compute_elapsed.as_secs_f64()
    );
    println!(
        "  Python xlang (CPython):   {:>7.2}ms ({:.0} tasks/sec)",
        ms(python_compute_elapsed),
        n_tasks as f64 / python_compute_elapsed.as_secs_f64()
    );
    println!("  Task-body speedup:        {:.2}x\n", compute_speedup);

    // [8] Complex payload serialization breakdown (100x100 nested Vec<Vec<i64>>, N=50)
    println!("[8] Complex payload serialization breakdown (100x100 nested Vec<Vec<i64>>, N=50)");
    let big_list: Vec<Vec<i64>> = (0..100)
        .map(|i| (0..100).map(|j| i * 100 + j).collect())
        .collect();
    let n_iter = 50u32;

    let t0 = Instant::now();
    for _ in 0..n_iter {
        let bytes = rayrust::serialize(&big_list).unwrap();
        let _back: Vec<Vec<i64>> = rayrust::deserialize(&bytes).unwrap();
    }
    let pure_serde = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..n_iter {
        let _obj = ray.put(&big_list).unwrap();
    }
    let put_elapsed = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..n_iter {
        let arg = rayrust::serialize(&big_list).unwrap();
        let args: Vec<&[u8]> = vec![&arg];
        let r = ray
            .task_call_python("rayrust_test", "echo_list", &args, &[])
            .unwrap();
        let _v = r.get_value_async().await.unwrap();
    }
    let full_xlang = t0.elapsed();

    let ser_per = pure_serde / n_iter;
    let put_per = put_elapsed / n_iter;
    let full_per = full_xlang / n_iter;
    let ray_overhead = full_per.saturating_sub(put_per);

    let full_us = full_xlang.as_micros().max(1);
    let ser_pct = pure_serde.as_micros() as f64 * 100.0 / full_us as f64;
    let ray_pct =
        full_xlang.saturating_sub(put_elapsed).as_micros() as f64 * 100.0 / full_us as f64;

    println!("  Pure msgpack serde:       {:>7.2}us/iter", us(ser_per));
    println!(
        "  Rust put (object store):  {:>7.2}us/iter ({:.2}ms/iter)",
        us(put_per),
        ms(put_per)
    );
    println!("  Full Rust -> Python echo: {:>7.2}ms/iter", ms(full_per));
    println!(
        "  Serialization share:      {:>6.2}% ({:.2}us/iter)",
        ser_pct,
        us(ser_per)
    );
    println!(
        "  Ray + xlang share:        {:>6.2}% ({:.2}ms/iter)\n",
        ray_pct,
        ms(ray_overhead)
    );

    println!("=== Summary ===");
    println!("rust_sync_tps={:.1}", sync_tps);
    println!("rust_batched_tps={:.1}", batched_tps);
    println!("rust_per_ref_tps={:.1}", per_ref_tps);
    println!("rust_xlang_batched_tps={:.1}", xlang_tps);
    println!(
        "rust_sync_lat_median_ms={:.2}",
        ms(median(&times_rust_sync))
    );
    println!("rust_sync_lat_p99_ms={:.2}", ms(p99(&times_rust_sync)));
    println!(
        "rust_async_lat_median_ms={:.2}",
        ms(median(&times_rust_async))
    );
    println!("rust_async_lat_p99_ms={:.2}", ms(p99(&times_rust_async)));
    println!("rust_xlang_lat_median_ms={:.2}", ms(median(&times_python)));
    println!("rust_xlang_lat_p99_ms={:.2}", ms(p99(&times_python)));
    println!("async_sleep_100x50ms_ms={:.2}", ms(sleep_elapsed));
    println!("async_sleep_speedup={:.2}", parallel_speedup);
    println!("compute_rust_10x1m_ms={:.2}", ms(rust_compute_elapsed));
    println!("compute_xlang_10x1m_ms={:.2}", ms(python_compute_elapsed));
    println!("compute_xlang_speedup={:.2}", compute_speedup);
    println!("serde_pure_us={:.2}", us(ser_per));
    println!("serde_put_us={:.2}", us(put_per));
    println!("serde_echo_ms={:.2}", ms(full_per));

    drop(ray);
}

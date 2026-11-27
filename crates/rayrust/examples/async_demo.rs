//! Example: Async remote tasks with tokio + Ray cluster.
//!
//! Uses one bounded `Get` on the tokio blocking pool for async gets.

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[rayrust::remote]
fn greet(name: String) -> String {
    format!("Hello, {} from async Rust!", name)
}

#[tokio::main]
async fn main() {
    let address =
        std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| {
        eprintln!("ERROR: RAY_WORKER_SO not set.");
        std::process::exit(1);
    });

    println!("=== Ray Async Demo (bounded Get on the blocking pool) ===");
    println!("Address: {}, Node: {}", address, node_ip);

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);

    let ray = Ray::connect(&config).expect("init failed");
    println!("✓ Ray initialized\n");

    // ── Concurrent Remote Tasks (tokio::join!) ──────────────
    println!("--- Concurrent Remote Tasks ---");

    let (r1, r2, r3) = tokio::join!(
        add_remote_async(&ray, 10, 32),
        add_remote_async(&ray, 100, 200),
        greet_remote_async(&ray, "Tokio".to_string()),
    );

    let r1: ObjectRef<i32> = r1.expect("add(10,32) failed");
    let r2: ObjectRef<i32> = r2.expect("add(100,200) failed");
    let r3: ObjectRef<String> = r3.expect("greet failed");

    // get_async blocks one blocking-pool thread inside Get(id, timeout_ms); the tokio side stays free
    let (v1, v2, v3) = tokio::join!(r1.get_async(), r2.get_async(), r3.get_async(),);

    let v1: i32 = v1.expect("add(10, 32) failed");
    let v2: i32 = v2.expect("add(100, 200) failed");
    let v3: String = v3.expect("greet(Tokio) failed");

    println!("add(10, 32) = {} ✓", v1);
    println!("add(100, 200) = {} ✓", v2);
    println!("greet(\"Tokio\") = {} ✓", v3);
    assert_eq!(v1, 42, "add(10, 32)");
    assert_eq!(v2, 300, "add(100, 200)");
    assert_eq!(v3, "Hello, Tokio from Rust worker!");

    // ── Batch Concurrent Tasks (10 tasks) ───────────────────
    println!("\n--- Batch Concurrent (10 tasks) ---");

    let mut submit_futs = Vec::new();
    for i in 0..10i32 {
        submit_futs.push(add_remote_async(&ray, i, i * 2));
    }

    let obj_refs: Vec<ObjectRef<i32>> = join_all(submit_futs)
        .await
        .into_iter()
        .map(|r| r.expect("task failed"))
        .collect();

    let mut get_futs = Vec::new();
    for obj_ref in obj_refs {
        get_futs.push(async move { obj_ref.get_async().await });
    }
    let results: Vec<i32> = join_all(get_futs)
        .await
        .into_iter()
        .map(|r| r.expect("task result failed"))
        .collect();

    let mut sum = 0i32;
    for (i, val) in results.iter().enumerate() {
        assert_eq!(
            *val,
            i as i32 * 3,
            "task[{}] printed a value that belongs to another submission",
            i
        );
        sum += val;
        if i < 3 || i >= results.len() - 1 {
            println!("  task[{}] add({}, {}) = {}", i, i, i * 2, val);
        } else if i == 3 {
            println!("  ...");
        }
    }
    println!("Sum of all results: {} ✓", sum);

    // ── Shutdown (automatic on drop) ──────────────────────
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ Ray shutdown");
}

async fn join_all<F, T>(futs: Vec<F>) -> Vec<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut set = tokio::task::JoinSet::new();
    for (idx, f) in futs.into_iter().enumerate() {
        set.spawn(async move { (idx, f.await) });
    }
    let mut indexed = Vec::with_capacity(set.len());
    while let Some(res) = set.join_next().await {
        indexed.push(res.unwrap());
    }
    indexed.sort_by_key(|(idx, _)| *idx);
    indexed.into_iter().map(|(_, val)| val).collect()
}

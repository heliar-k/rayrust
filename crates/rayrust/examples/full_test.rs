//! Comprehensive test: all features.

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO")
        .unwrap_or_else(|_| {
            eprintln!("ERROR: RAY_WORKER_SO not set.");
            std::process::exit(1);
        });

    println!("=== Comprehensive Feature Test ===\n");

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);
    let ray = Ray::connect(&config).expect("init failed");
    println!("✓ Ray initialized\n");

    // ── 1. Wait ──────────────────────────────────────────────
    println!("--- 1. Wait ---");
    let obj1 = ray.put(&10i32).unwrap();
    let obj2 = ray.put(&20i32).unwrap();
    let (ready, unready) = ray.wait(&[obj1, obj2], 2, 5000)
        .expect("wait failed");
    println!("wait: {} ready, {} unready ✓", ready.len(), unready.len());
    assert_eq!(
        (ready.len(), unready.len()),
        (2, 0),
        "wait over two put objects returns both ready and none unready"
    );

    // ── 2. Remote Task (Rust → Rust) ─────────────────────────
    println!("\n--- 2. Remote Task (Rust) ---");
    let r = add_remote(&ray, 3, 4).expect("submit add(3, 4)");
    let v: i32 = r.get_async().await.expect("add failed");
    println!("add(3, 4) = {} ✓", v);
    assert_eq!(v, 7, "add(3, 4)");

    // ── 3. Batch Get (get_many) ─────────────────────────────
    println!("\n--- 3. Batch Get ---");
    let r1 = ray.put(&100i32).unwrap();
    let r2 = ray.put(&200i32).unwrap();
    let r3 = ray.put(&300i32).unwrap();
    let vals = ray.get_many(&[r1, r2, r3]).expect("get_many failed");
    println!("get_many: {:?} ✓", vals);
    assert_eq!(vals, vec![100, 200, 300], "get_many over three puts");

    // ── 4. Cross-language: Python Task ──────────────────────
    println!("\n--- 4. Cross-language: Python Task ---");
    let arg_a = rayrust::serialize(&5i64).unwrap();
    let arg_b = rayrust::serialize(&3i64).unwrap();
    let args: Vec<&[u8]> = vec![&arg_a, &arg_b];

    let obj_ref = ray
        .task_call_python("rayrust_test", "add", &args, &[])
        .expect("task_call_python(rayrust_test.add)");
    println!("Python add(5, 3) task submitted");
    let obj_ref: ObjectRef<i64> = obj_ref.cast();
    let val: i64 = obj_ref
        .get_async()
        .await
        .expect("Python add(5, 3) result failed");
    println!("Python add(5, 3) = {} ✓ (auto xlang deserialization)", val);
    assert_eq!(val, 8, "Python add(5, 3)");

    // ── 5. Cross-language: Python Actor ─────────────────────
    println!("\n--- 5. Cross-language: Python Actor ---");
    let arg_start = rayrust::serialize(&10i64).unwrap();
    let args_actor: Vec<&[u8]> = vec![&arg_start];

    let actor = ray
        .actor_create_python("rayrust_test", "Counter", &args_actor, &ActorOptions::new())
        .expect("actor_create_python(rayrust_test.Counter)");
    println!("Python Counter actor created ✓");

    let arg_n = rayrust::serialize(&5i64).unwrap();
    let args_inc: Vec<&[u8]> = vec![&arg_n];
    let obj_ref = ray
        .actor_call_python(actor.id(), "increment", &args_inc, &[])
        .expect("actor_call_python(Counter.increment)");
    let obj_ref: ObjectRef<i64> = obj_ref.cast();
    let val: i64 = obj_ref
        .get_async()
        .await
        .expect("Counter.increment(5) result failed");
    println!("Counter.increment(5) = {} ✓ (auto xlang)", val);
    assert_eq!(val, 15, "Python Counter(10).increment(5)");

    let _ = ray.kill_actor(&actor, true);
    println!("Counter killed ✓");

    // ── 6. Rust Actor (Rust → Rust) ─────────────────────────
    println!("\n--- 6. Rust Actor ---");
    let arg_start = rayrust::serialize(&100i64).unwrap();
    let args_actor: Vec<&[u8]> = vec![&arg_start];

    let actor = ray
        .actor_create(
            "__rayrust_actor_factory_counter",
            &args_actor,
            &ActorOptions::new(),
        )
        .expect("actor_create(__rayrust_actor_factory_counter)");
    println!("Rust Counter actor created ✓ (id_len={})", actor.id().len());

    let arg_n = rayrust::serialize(&5i64).unwrap();
    let args_inc: Vec<&[u8]> = vec![&arg_n];
    let obj_ref = ray
        .actor_call(
            actor.id(),
            "__rayrust_actor_factory_counter::increment",
            &args_inc,
            &[],
        )
        .expect("actor_call(Counter.increment)");
    let obj_ref: ObjectRef<i64> = obj_ref.cast();
    let val: i64 = obj_ref
        .get_async()
        .await
        .expect("Counter.increment(5) result failed");
    println!("Counter.increment(5) = {} ✓", val);
    assert_eq!(val, 105, "Rust Counter(100).increment(5)");

    let obj_ref = ray
        .actor_call(actor.id(), "__rayrust_actor_factory_counter::get", &[], &[])
        .expect("actor_call(Counter.get)");
    let obj_ref: ObjectRef<i64> = obj_ref.cast();
    let val: i64 = obj_ref
        .get_async()
        .await
        .expect("Counter.get() result failed");
    println!("Counter.get() = {} ✓", val);
    assert_eq!(val, 105, "Rust Counter(100).get() after increment(5)");

    let _ = ray.kill_actor(&actor, true);
    println!("Counter killed ✓");

    // ── 7. Placement Group ──────────────────────────────────
    println!("\n--- 7. Placement Group ---");
    let bundles_json = r#"[{"CPU": 1}, {"CPU": 1}]"#;
    let pg_id = ray
        .placement_group_create("test_pg", bundles_json, 0)
        .expect("placement_group_create(test_pg)");
    println!("PlacementGroup created ✓ (id_len={})", pg_id.len());
    // PlacementGroupID::kLength is 14 unique bytes plus JobID::kLength (4).
    assert_eq!(pg_id.len(), 18, "placement group id length");
    ray.placement_group_remove(&pg_id)
        .expect("placement_group_remove failed");
    println!("PlacementGroup removed ✓");

    // ── 8. Debug helpers ───────────────────────────────────
    println!("\n--- 8. Debug helpers ---");
    let obj = ray.put(&42i32).unwrap();
    println!("ObjectRef id_hex: {} ✓", obj.id_hex());
    println!("is_initialized: {} ✓", ray.is_initialized());

    // ── Shutdown (automatic on drop) ──────────────────────
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ Ray shutdown");
}

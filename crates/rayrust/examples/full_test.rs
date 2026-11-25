//! Comprehensive test: all features.

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
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
    let (ready, unready) = ray.wait(&[obj1, obj2], 2, 5000).expect("wait failed");
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
    let build_ver = Ray::sdk_version();
    let loaded_ver = Ray::loaded_sdk_version().expect("loaded_sdk_version failed");
    assert!(
        !build_ver.is_empty(),
        "Ray::sdk_version() must be non-empty"
    );
    assert_eq!(
        loaded_ver, build_ver,
        "loaded libray_api.so version must match Ray::sdk_version()"
    );
    println!("SDK version: build={} loaded={} ✓", build_ver, loaded_ver);

    // ── 9. Large Payload & Repeated Reads ──────────────────
    println!("\n--- 9. Large Payload & Repeated Reads ---");
    let build_ascii = |len: usize, seed: usize| -> String {
        const ALPHABET: &[u8; 64] =
            b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let idx = (i.wrapping_mul(131).wrapping_add(seed).wrapping_add(i >> 11)) & 63;
            out.push(ALPHABET[idx]);
        }
        String::from_utf8(out).expect("ASCII payload")
    };

    let four_mib = 4 * 1024 * 1024;
    let payload_4m = build_ascii(four_mib, 17);
    let r = ray.put(&payload_4m).expect("put 4 MiB string");
    assert_eq!(r.get().expect("4 MiB get()"), payload_4m);
    assert_eq!(
        r.get_timeout(10_000).expect("4 MiB get_timeout(10_000)"),
        payload_4m
    );
    assert_eq!(r.get_async().await.expect("4 MiB get_async()"), payload_4m);
    assert_eq!(
        r.get_value().expect("4 MiB get_value()").as_str(),
        Some(payload_4m.as_str())
    );
    assert_eq!(
        r.get_value_async()
            .await
            .expect("4 MiB get_value_async()")
            .as_str(),
        Some(payload_4m.as_str())
    );
    let raw_4m = r.get_raw_bytes().expect("4 MiB get_raw_bytes()");
    let expected_raw_4m = rayrust::serialize(&payload_4m).expect("serialize 4 MiB string");
    assert_eq!(raw_4m, expected_raw_4m, "4 MiB raw msgpack bytes");
    drop(raw_4m);
    drop(expected_raw_4m);

    let dup_sync = ray
        .get_many(&[r.clone(), r.clone()])
        .expect("4 MiB get_many duplicate refs");
    assert_eq!(dup_sync.len(), 2);
    assert_eq!(dup_sync[0], payload_4m);
    assert_eq!(dup_sync[1], payload_4m);
    drop(dup_sync);

    let dup_async = ray
        .get_many_async(&[r.clone(), r])
        .await
        .expect("4 MiB get_many_async duplicate refs");
    assert_eq!(dup_async.len(), 2);
    assert_eq!(dup_async[0], payload_4m);
    assert_eq!(dup_async[1], payload_4m);
    drop(dup_async);
    drop(payload_4m);
    println!("4 MiB put + repeated sync/async/batch reads verified ✓");

    let one_mib = 1024 * 1024;
    let payload_1m = build_ascii(one_mib, 53);
    let xlang_ref = ray.put_xlang(&payload_1m).expect("put_xlang 1 MiB string");
    assert!(xlang_ref.is_xlang(), "put_xlang must mark is_xlang");
    assert_eq!(xlang_ref.get().expect("1 MiB xlang get()"), payload_1m);
    assert_eq!(
        xlang_ref
            .get_async()
            .await
            .expect("1 MiB xlang get_async()"),
        payload_1m
    );
    assert_eq!(
        xlang_ref
            .get_value()
            .expect("1 MiB xlang get_value()")
            .as_str(),
        Some(payload_1m.as_str())
    );
    let xlang_batch = ray
        .get_many(&[xlang_ref.clone(), xlang_ref.clone()])
        .expect("1 MiB xlang get_many()");
    assert_eq!(xlang_batch.len(), 2);
    assert_eq!(xlang_batch[0], payload_1m);
    assert_eq!(xlang_batch[1], payload_1m);
    drop(xlang_batch);

    let py_greet: ObjectRef<String> = ray
        .task_call_python("rayrust_test", "greet", &[xlang_ref.id()], &[true])
        .expect("submit rayrust_test.greet(&put_xlang 1 MiB)")
        .cast();
    let py_out = py_greet
        .get_async()
        .await
        .expect("await rayrust_test.greet(&put_xlang 1 MiB)");
    assert_eq!(py_out, format!("Hello, {} from Python!", payload_1m));
    drop(py_out);
    drop(xlang_ref);
    drop(payload_1m);
    println!("1 MiB put_xlang + Rust/Python reads verified ✓");

    // ── 10. Metrics (Driver, Remote Task, MetricActor) ─────
    println!("\n--- 10. Metrics ---");
    assert!(matches!(
        Gauge::new("1invalid_driver_metric", "bad", "u", &[]),
        Err(RayError::Ffi(_))
    ));
    assert!(matches!(
        Histogram::new("driver_bad_hist", "bad", "ms", &[5.0, 1.0], &[]),
        Err(RayError::Ffi(_))
    ));

    let drv_gauge =
        Gauge::new("driver_active_jobs", "Active driver jobs", "jobs", &["env"]).unwrap();
    let drv_counter = Counter::new(
        "driver_requests_total",
        "Total driver requests",
        "count",
        &["env"],
    )
    .unwrap();
    let drv_hist = Histogram::new(
        "driver_latency_ms",
        "Driver operation latency",
        "ms",
        &[1.0, 5.0, 10.0, 50.0],
        &["env"],
    )
    .unwrap();
    let drv_sum = Sum::new("driver_payload_bytes", "Payload delta", "bytes", &["env"]).unwrap();

    let drv_tags = &[("env", "cluster_e2e")];
    drv_gauge.set(4.0, drv_tags).expect("driver gauge.set");
    drv_counter.inc(1.0, drv_tags).expect("driver counter.inc");
    drv_hist
        .observe(7.5, drv_tags)
        .expect("driver hist.observe");
    drv_sum.record(128.0, drv_tags).expect("driver sum.record");
    assert!(matches!(
        drv_counter.inc(-1.0, drv_tags),
        Err(RayError::Ffi(_))
    ));
    assert!(matches!(
        drv_gauge.set(f64::NAN, drv_tags),
        Err(RayError::Ffi(_))
    ));
    println!("Driver metrics recorded & invalid inputs rejected ✓");

    let worker_val = rayrust::serialize(&12.5f64).unwrap();
    let worker_metric_ref: ObjectRef<String> = ray
        .task_call(
            "record_worker_metrics",
            &[&worker_val],
            &[],
            &TaskOptions::new(),
        )
        .expect("submit record_worker_metrics")
        .cast();
    let worker_metric_out = worker_metric_ref
        .get_async()
        .await
        .expect("record_worker_metrics result");
    assert_eq!(worker_metric_out, "ok:rayrust_worker_calls_total:12.50");
    println!("Remote task record_worker_metrics: {} ✓", worker_metric_out);

    let prefix_arg = rayrust::serialize(&"cluster_actor".to_string()).unwrap();
    let metric_actor = ray
        .actor_create(
            "__rayrust_actor_factory_metricactor",
            &[&prefix_arg],
            &ActorOptions::new(),
        )
        .expect("create MetricActor");

    for (delta, expected_total) in [(10.5f64, 10.5f64), (-3.0f64, 7.5f64), (4.25f64, 11.75f64)] {
        let arg_d = rayrust::serialize(&delta).unwrap();
        let step_ref: ObjectRef<f64> = ray
            .actor_call(
                metric_actor.id(),
                "__rayrust_actor_factory_metricactor::record_step",
                &[&arg_d],
                &[],
            )
            .expect("call MetricActor::record_step")
            .cast();
        let got_total = step_ref.get_async().await.expect("record_step result");
        assert!(
            (got_total - expected_total).abs() < 1e-9,
            "MetricActor total mismatch: got {got_total}, expected {expected_total}"
        );
    }

    // 验证 MetricActor 内错误输入拒绝且 actor 继续可用
    for bad_inc in [-2.0f64, f64::NAN] {
        let arg_bad = rayrust::serialize(&bad_inc).unwrap();
        let rej_ref: ObjectRef<Result<(), String>> = ray
            .actor_call(
                metric_actor.id(),
                "__rayrust_actor_factory_metricactor::try_counter_inc",
                &[&arg_bad],
                &[],
            )
            .expect("call MetricActor::try_counter_inc")
            .cast();
        let rej_res = rej_ref.get_async().await.expect("try_counter_inc result");
        assert!(
            rej_res.is_err(),
            "MetricActor::try_counter_inc({bad_inc}) should be rejected"
        );
    }

    let arg_final = rayrust::serialize(&1.25f64).unwrap();
    let final_ref: ObjectRef<f64> = ray
        .actor_call(
            metric_actor.id(),
            "__rayrust_actor_factory_metricactor::record_step",
            &[&arg_final],
            &[],
        )
        .expect("call MetricActor::record_step after rejection")
        .cast();
    let final_total = final_ref.get_async().await.expect("final record_step");
    assert!((final_total - 13.0).abs() < 1e-9);
    let _ = ray.kill_actor(&metric_actor, true);
    println!(
        "MetricActor multi-step recording & input rejection verified (total={}) ✓",
        final_total
    );

    // ── Shutdown (automatic on drop) ──────────────────────
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ Ray shutdown");
}

//! Metrics API（`Gauge`、`Counter`、`Histogram`、`Sum`）参数校验、原始 FFI 边界与完整生命周期集成测试。

use std::os::raw::c_char;
use std::sync::{Arc, LazyLock};

use rayrust::prelude::*;
use rayrust::sys::{self, CBytesGuard, ErrorKind, RayBytes, RayMetricTag};

static GLOBAL_LAZY_COUNTER: LazyLock<Counter> = LazyLock::new(|| {
    Counter::new(
        "rayrust_lazy_counter_total",
        "Process-wide lazy counter",
        "events",
        &["worker"],
    )
    .expect("LazyLock Counter::new before Ray init")
});

fn raw_bytes(s: &[u8]) -> RayBytes {
    RayBytes {
        data: s.as_ptr() as *const c_char,
        len: s.len(),
    }
}

fn empty_bytes() -> RayBytes {
    RayBytes {
        data: std::ptr::null(),
        len: 0,
    }
}

fn bad_ptr_bytes() -> RayBytes {
    RayBytes {
        data: std::ptr::null(),
        len: 3,
    }
}

fn check_valid_and_invalid_names() {
    for valid in [
        "ray_test_gauge",
        "_private:metric_1",
        ":leading_colon_9",
        "UpperCase_Metric_0",
    ] {
        let g = Gauge::new(valid, "desc", "unit", &["env"]).expect("valid Gauge name");
        let c = Counter::new(valid, "desc", "unit", &["env"]).expect("valid Counter name");
        let h = Histogram::new(valid, "desc", "unit", &[-1.5, 0.0, 2.5, 10.0], &["env"])
            .expect("valid Histogram name");
        let s = Sum::new(valid, "desc", "unit", &["env"]).expect("valid Sum name");

        assert_eq!(g.name(), valid);
        assert_eq!(c.name(), valid);
        assert_eq!(h.name(), valid);
        assert_eq!(s.name(), valid);
    }

    for invalid in [
        "",
        "1starts_with_digit",
        "has-dash",
        "has.dot",
        "has space",
        "非ASCII",
        "bad\0name",
    ] {
        match Gauge::new(invalid, "desc", "unit", &[]) {
            Err(RayError::Ffi(_)) => {}
            other => panic!("Gauge::new({invalid:?}) expected Err(Ffi), got {other:?}"),
        }
        match Counter::new(invalid, "desc", "unit", &[]) {
            Err(RayError::Ffi(_)) => {}
            other => panic!("Counter::new({invalid:?}) expected Err(Ffi), got {other:?}"),
        }
        match Histogram::new(invalid, "desc", "unit", &[1.0, 2.0], &[]) {
            Err(RayError::Ffi(_)) => {}
            other => panic!("Histogram::new({invalid:?}) expected Err(Ffi), got {other:?}"),
        }
        match Sum::new(invalid, "desc", "unit", &[]) {
            Err(RayError::Ffi(_)) => {}
            other => panic!("Sum::new({invalid:?}) expected Err(Ffi), got {other:?}"),
        }
    }
}

fn check_metadata_tag_keys_and_boundaries() {
    // description 与 unit 含内部 NUL 字节时拒绝
    for bad_desc in ["desc\0tail", "\0"] {
        assert!(matches!(
            Gauge::new("valid_meta_g", bad_desc, "u", &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            Counter::new("valid_meta_c", bad_desc, "u", &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            Histogram::new("valid_meta_h", bad_desc, "u", &[1.0], &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            Sum::new("valid_meta_s", bad_desc, "u", &[]),
            Err(RayError::Ffi(_))
        ));
    }

    for bad_unit in ["ms\0s", "\0"] {
        assert!(matches!(
            Gauge::new("valid_unit_g", "d", bad_unit, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            Counter::new("valid_unit_c", "d", bad_unit, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            Histogram::new("valid_unit_h", "d", bad_unit, &[1.0], &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            Sum::new("valid_unit_s", "d", bad_unit, &[]),
            Err(RayError::Ffi(_))
        ));
    }

    // 空 tag_key、含内部 NUL 的 tag_key、重复 tag_keys 全部拒绝
    let invalid_tag_key_sets: &[&[&str]] = &[
        &[""],
        &["env", ""],
        &["bad\0key"],
        &["env", "env"],
        &["a", "b", "a"],
    ];
    for keys in invalid_tag_key_sets {
        assert!(
            matches!(
                Gauge::new("tag_check_g", "d", "u", keys),
                Err(RayError::Ffi(_))
            ),
            "Gauge::new should reject tag_keys {keys:?}"
        );
        assert!(
            matches!(
                Counter::new("tag_check_c", "d", "u", keys),
                Err(RayError::Ffi(_))
            ),
            "Counter::new should reject tag_keys {keys:?}"
        );
        assert!(
            matches!(
                Histogram::new("tag_check_h", "d", "u", &[1.0, 2.0], keys),
                Err(RayError::Ffi(_))
            ),
            "Histogram::new should reject tag_keys {keys:?}"
        );
        assert!(
            matches!(
                Sum::new("tag_check_s", "d", "u", keys),
                Err(RayError::Ffi(_))
            ),
            "Sum::new should reject tag_keys {keys:?}"
        );
    }

    // Histogram::new 非法桶边界全部拒绝
    let invalid_boundaries: &[&[f64]] = &[
        &[],
        &[f64::NAN],
        &[0.0, f64::NAN],
        &[f64::INFINITY],
        &[f64::NEG_INFINITY],
        &[-1.0, f64::INFINITY],
        &[1.0, 1.0],
        &[0.0, 2.0, 2.0],
        &[5.0, 2.0],
        &[-1.0, -2.0],
    ];
    for bounds in invalid_boundaries {
        assert!(
            matches!(
                Histogram::new("hist_bounds_bad", "d", "u", bounds, &[]),
                Err(RayError::Ffi(_))
            ),
            "Histogram::new should reject boundaries {bounds:?}"
        );
    }

    let ok_hist = Histogram::new(
        "hist_bounds_ok",
        "signed monotonic buckets",
        "ms",
        &[-1.5, 0.0, 2.5, 10.0],
        &["stage"],
    )
    .expect("strictly increasing finite boundaries");
    assert_eq!(ok_hist.name(), "hist_bounds_ok");
}

fn check_raw_ffi_boundaries() {
    unsafe {
        // 空句柄检查
        sys::clear_error();
        let null_name = sys::ray_metric_get_name(std::ptr::null());
        assert!(null_name.data.is_null());
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic);

        // destroy(null) 为空操作且不写入错误
        sys::clear_error();
        sys::ray_metric_destroy(std::ptr::null_mut());
        assert_eq!(sys::last_error_kind(), ErrorKind::None);
        assert!(sys::last_error().is_none());

        assert_eq!(
            sys::ray_gauge_set(std::ptr::null_mut(), 1.0, std::ptr::null(), 0),
            -1
        );
        assert_eq!(
            sys::ray_counter_inc(std::ptr::null_mut(), 1.0, std::ptr::null(), 0),
            -1
        );
        assert_eq!(
            sys::ray_histogram_observe(std::ptr::null_mut(), 1.0, std::ptr::null(), 0),
            -1
        );
        assert_eq!(
            sys::ray_metric_record(std::ptr::null_mut(), 1.0, std::ptr::null(), 0),
            -1
        );

        // 非零长度配空指针
        assert!(sys::ray_gauge_create(
            bad_ptr_bytes(),
            empty_bytes(),
            empty_bytes(),
            std::ptr::null(),
            0
        )
        .is_null());
        assert!(sys::ray_gauge_create(
            raw_bytes(b"raw_ffi_metric"),
            bad_ptr_bytes(),
            empty_bytes(),
            std::ptr::null(),
            0
        )
        .is_null());
        assert!(sys::ray_gauge_create(
            raw_bytes(b"raw_ffi_metric"),
            empty_bytes(),
            bad_ptr_bytes(),
            std::ptr::null(),
            0
        )
        .is_null());
        assert!(sys::ray_gauge_create(
            raw_bytes(b"raw_ffi_metric"),
            empty_bytes(),
            empty_bytes(),
            std::ptr::null(),
            1
        )
        .is_null());
        assert!(sys::ray_histogram_create(
            raw_bytes(b"raw_ffi_metric"),
            empty_bytes(),
            empty_bytes(),
            std::ptr::null(),
            2,
            std::ptr::null(),
            0
        )
        .is_null());

        // 构造四个合法原始句柄，验证 ray_metric_get_name 与跨类型误调拦截
        let bounds = [1.0f64, 5.0f64];
        let g_name = b"raw_gauge_handle";
        let c_name = b"raw_counter_handle";
        let h_name = b"raw_hist_handle";
        let s_name = b"raw_sum_handle";

        let g_raw = sys::ray_gauge_create(
            raw_bytes(g_name),
            empty_bytes(),
            empty_bytes(),
            std::ptr::null(),
            0,
        );
        let c_raw = sys::ray_counter_create(
            raw_bytes(c_name),
            empty_bytes(),
            empty_bytes(),
            std::ptr::null(),
            0,
        );
        let h_raw = sys::ray_histogram_create(
            raw_bytes(h_name),
            empty_bytes(),
            empty_bytes(),
            bounds.as_ptr(),
            bounds.len(),
            std::ptr::null(),
            0,
        );
        let s_raw = sys::ray_sum_create(
            raw_bytes(s_name),
            empty_bytes(),
            empty_bytes(),
            std::ptr::null(),
            0,
        );
        assert!(!g_raw.is_null() && !c_raw.is_null() && !h_raw.is_null() && !s_raw.is_null());

        for (handle, expected) in [
            (g_raw as *const _, &g_name[..]),
            (c_raw as *const _, &c_name[..]),
            (h_raw as *const _, &h_name[..]),
            (s_raw as *const _, &s_name[..]),
        ] {
            let got = CBytesGuard::from(sys::ray_metric_get_name(handle))
                .expect("ray_metric_get_name non-null");
            assert_eq!(got.as_slice(), expected);
        }

        // 跨类型误调全部在取运行时之前返回 -1（Generic）
        assert_eq!(sys::ray_gauge_set(c_raw, 1.0, std::ptr::null(), 0), -1);
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic);
        assert!(sys::last_error()
            .unwrap_or_default()
            .contains("requires a Gauge handle"));

        assert_eq!(
            sys::ray_histogram_observe(c_raw, 1.0, std::ptr::null(), 0),
            -1
        );
        assert_eq!(sys::last_error_kind(), ErrorKind::Generic);
        assert!(sys::last_error()
            .unwrap_or_default()
            .contains("requires a Histogram handle"));

        assert_eq!(sys::ray_counter_inc(g_raw, 1.0, std::ptr::null(), 0), -1);
        assert_eq!(
            sys::ray_histogram_observe(g_raw, 1.0, std::ptr::null(), 0),
            -1
        );
        assert_eq!(sys::ray_gauge_set(h_raw, 1.0, std::ptr::null(), 0), -1);
        assert_eq!(sys::ray_counter_inc(h_raw, 1.0, std::ptr::null(), 0), -1);
        assert_eq!(sys::ray_gauge_set(s_raw, 1.0, std::ptr::null(), 0), -1);
        assert_eq!(sys::ray_counter_inc(s_raw, 1.0, std::ptr::null(), 0), -1);
        assert_eq!(
            sys::ray_histogram_observe(s_raw, 1.0, std::ptr::null(), 0),
            -1
        );

        // tags 指针异常检查（同样先于运行时检查）
        assert_eq!(sys::ray_gauge_set(g_raw, 1.0, std::ptr::null(), 1), -1);
        let null_val_tag = RayMetricTag {
            key: raw_bytes(b"env"),
            value: bad_ptr_bytes(),
        };
        assert_eq!(sys::ray_gauge_set(g_raw, 1.0, &null_val_tag, 1), -1);

        sys::ray_metric_destroy(g_raw);
        sys::ray_metric_destroy(c_raw);
        sys::ray_metric_destroy(h_raw);
        sys::ray_metric_destroy(s_raw);
    }
}

#[test]
fn metrics_validation_raw_ffi_and_full_lifecycle() {
    assert!(
        !unsafe { sys::ray_is_initialized() },
        "metrics_test requires starting in a fresh process before Ray initialization"
    );

    check_valid_and_invalid_names();
    check_metadata_tag_keys_and_boundaries();
    check_raw_ffi_boundaries();

    // ── 阶段 1：Ray::local() 之前构造四个指标与 LazyLock 静态指标 ──────
    let gauge = Arc::new(
        Gauge::new(
            "lifecycle_gauge",
            "Lifecycle test gauge",
            "items",
            &["env", "stage"],
        )
        .expect("Gauge::new before init"),
    );
    let counter = Arc::new(
        Counter::new(
            "lifecycle_counter",
            "Lifecycle test counter",
            "count",
            &["env", "stage"],
        )
        .expect("Counter::new before init"),
    );
    let hist = Arc::new(
        Histogram::new(
            "lifecycle_hist",
            "Lifecycle test histogram",
            "ms",
            &[-1.5, 0.0, 2.5, 10.0],
            &["env", "stage"],
        )
        .expect("Histogram::new before init"),
    );
    let sum = Arc::new(
        Sum::new(
            "lifecycle_sum",
            "Lifecycle test sum",
            "bytes",
            &["env", "stage"],
        )
        .expect("Sum::new before init"),
    );

    assert_eq!(GLOBAL_LAZY_COUNTER.name(), "rayrust_lazy_counter_total");

    for res in [
        gauge.set(1.0, &[]),
        gauge.record(1.0, &[]),
        counter.inc(1.0, &[]),
        counter.record(1.0, &[]),
        hist.observe(1.0, &[]),
        hist.record(1.0, &[]),
        sum.record(1.0, &[]),
        GLOBAL_LAZY_COUNTER.inc(1.0, &[("worker", "pre")]),
    ] {
        match res {
            Err(RayError::Ffi(msg)) => assert!(
                msg.contains("Ray runtime is not initialized"),
                "unexpected pre-init message: {msg}"
            ),
            other => panic!("expected pre-init Err(RayError::Ffi), got {other:?}"),
        }
    }

    // 参数校验先于未初始化检查
    match counter.inc(-0.5, &[]) {
        Err(RayError::Ffi(msg)) => assert!(
            msg.contains("non-negative"),
            "expected non-negative error before runtime check, got: {msg}"
        ),
        other => panic!("expected Err(Ffi) for negative counter inc, got {other:?}"),
    }

    // ── 阶段 2：初始化 Ray::local()，验证正常录制、边界值与多线程并发 ──
    let ray = Ray::local().expect("first Ray::local()");

    let tags = &[("env", "test"), ("stage", "1")];
    gauge.set(0.0, &[]).expect("gauge.set(0.0)");
    gauge.set(-42.5, tags).expect("gauge.set(-42.5, tags)");
    gauge
        .set(99.25, &[("env", "")])
        .expect("gauge empty tag value");
    gauge
        .set(12.0, &[("env", "a"), ("env", "b")])
        .expect("duplicate tag key in single record slice");
    gauge.record(-3.5, tags).expect("gauge.record(-3.5)");

    counter.inc(0.0, &[]).expect("counter.inc(0.0)");
    counter.inc(3.5, tags).expect("counter.inc(3.5, tags)");
    counter
        .record(0.0, &[("env", "a"), ("env", "b")])
        .expect("counter.record(0.0)");
    counter.record(2.25, tags).expect("counter.record(2.25)");
    GLOBAL_LAZY_COUNTER
        .inc(1.0, &[("worker", "main")])
        .expect("LazyLock counter inc");

    hist.observe(-5.0, &[]).expect("hist.observe(-5.0)");
    hist.observe(0.0, tags).expect("hist.observe(0.0)");
    hist.observe(4.2, &[("env", "a"), ("env", "b")])
        .expect("hist duplicate tag key");
    hist.record(15.0, tags).expect("hist.record(15.0)");

    sum.record(-10.0, &[]).expect("sum.record(-10.0)");
    sum.record(0.0, tags).expect("sum.record(0.0)");
    sum.record(25.5, &[("env", "a"), ("env", "b")])
        .expect("sum duplicate tag key");

    // 非有限数值在全部四种指标与 .record() 上均报错
    for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(gauge.set(non_finite, &[]), Err(RayError::Ffi(_))));
        assert!(matches!(
            gauge.record(non_finite, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            counter.inc(non_finite, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            counter.record(non_finite, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            hist.observe(non_finite, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(
            hist.record(non_finite, &[]),
            Err(RayError::Ffi(_))
        ));
        assert!(matches!(sum.record(non_finite, &[]), Err(RayError::Ffi(_))));
    }

    // Counter::inc 与 Counter::record 拒绝负数
    assert!(matches!(counter.inc(-0.001, &[]), Err(RayError::Ffi(_))));
    assert!(matches!(counter.record(-0.001, &[]), Err(RayError::Ffi(_))));

    // 录制时非法 tag key / tag value 均拒绝
    for bad_tags in [
        &[("", "v")][..],
        &[("k\0a", "v")][..],
        &[("env", "v\0b")][..],
    ] {
        assert!(matches!(gauge.set(1.0, bad_tags), Err(RayError::Ffi(_))));
        assert!(matches!(counter.inc(1.0, bad_tags), Err(RayError::Ffi(_))));
        assert!(matches!(hist.observe(1.0, bad_tags), Err(RayError::Ffi(_))));
        assert!(matches!(sum.record(1.0, bad_tags), Err(RayError::Ffi(_))));
    }

    // 4 线程 Arc 并发录制 200 次
    let mut handles = Vec::with_capacity(4);
    for tid in 0..4usize {
        let g = Arc::clone(&gauge);
        let c = Arc::clone(&counter);
        let h = Arc::clone(&hist);
        let s = Arc::clone(&sum);
        handles.push(std::thread::spawn(move || {
            let stage = tid.to_string();
            for step in 0..200usize {
                let v = (step as f64) * 0.25;
                let t = [("env", "concurrent"), ("stage", stage.as_str())];
                g.set(v - 10.0, &t).expect("concurrent gauge.set");
                c.inc(1.0, &t).expect("concurrent counter.inc");
                h.observe(v, &t).expect("concurrent hist.observe");
                s.record(v - 5.0, &t).expect("concurrent sum.record");
                GLOBAL_LAZY_COUNTER
                    .inc(1.0, &[("worker", stage.as_str())])
                    .expect("concurrent lazy counter");
            }
        }));
    }
    for jh in handles {
        jh.join().expect("join metric worker thread");
    }

    // ── 阶段 3：drop(ray) 关闭后，关闭闸门拒绝录制并优先报告参数错误 ──
    drop(ray);

    for res in [
        gauge.set(1.0, &[]),
        gauge.record(1.0, &[]),
        counter.inc(1.0, &[]),
        counter.record(1.0, &[]),
        hist.observe(1.0, &[]),
        hist.record(1.0, &[]),
        sum.record(1.0, &[]),
        GLOBAL_LAZY_COUNTER.inc(1.0, &[("worker", "post_shutdown")]),
    ] {
        match res {
            Err(RayError::Runtime(msg)) => assert!(
                msg.contains("shutting down or has shut down"),
                "unexpected post-shutdown message: {msg}"
            ),
            other => panic!("expected post-shutdown Err(RayError::Runtime), got {other:?}"),
        }
    }

    // 关闭状态下参数同时非法时优先返回 RayError::Ffi
    assert!(matches!(gauge.set(f64::NAN, &[]), Err(RayError::Ffi(_))));
    assert!(matches!(counter.inc(-1.0, &[]), Err(RayError::Ffi(_))));
    assert!(matches!(
        hist.observe(1.0, &[("", "v")]),
        Err(RayError::Ffi(_))
    ));
    assert!(matches!(
        sum.record(1.0, &[("env", "a\0b")]),
        Err(RayError::Ffi(_))
    ));

    // ── 阶段 4：重新初始化 Ray::local()，同一批指标实例恢复录制 ──────
    let ray2 = Ray::local().expect("second Ray::local()");
    gauge
        .set(7.5, &[("env", "reinit"), ("stage", "2")])
        .expect("gauge.set after reinit");
    counter
        .inc(2.0, &[("env", "reinit"), ("stage", "2")])
        .expect("counter.inc after reinit");
    hist.observe(3.3, &[("env", "reinit"), ("stage", "2")])
        .expect("hist.observe after reinit");
    sum.record(4.4, &[("env", "reinit"), ("stage", "2")])
        .expect("sum.record after reinit");
    GLOBAL_LAZY_COUNTER
        .inc(1.0, &[("worker", "reinit")])
        .expect("lazy counter after reinit");

    drop(ray2);
    drop(gauge);
    drop(counter);
    drop(hist);
    drop(sum);
}

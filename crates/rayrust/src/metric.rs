//! Ray C++ SDK `<ray/api/metric.h>` 指标封装：[`Gauge`]、[`Counter`]、[`Histogram`]、[`Sum`]。
//!
//! 四个指标类型对应 `libray_api.so` 公开导出的 `ray::Gauge`、`ray::Counter`、
//! `ray::Histogram` 与 `ray::Sum`（`cpp/include/ray/api/metric.h:23-133`、
//! `cpp/src/ray/runtime/metric/metric.cc:22-91`），可在驱动进程、`#[rayrust::remote]`
//! 远程任务（含 `std::sync::LazyLock` 静态变量）以及 `#[rayrust::actor]` 状态结构体内构造并并发上报。
//!
//! ## 后端聚合语义
//!
//! - [`Gauge`]：OpenCensus 路径注册 `Aggregation::LastValue()`（`src/ray/stats/metric.cc:189-197`），
//!   OpenTelemetry 路径调用 `RegisterGaugeMetric`（`src/ray/observability/open_telemetry_metric_recorder.cc:122-154`），
//!   保留最新记录的数值。
//! - [`Counter`]：OpenCensus 路径注册 `Aggregation::Count()` 按事件次数计数
//!   （`src/ray/stats/metric.cc:220-229`），OpenTelemetry 路径调用 `RegisterCounterMetric`
//!   累加非负增量（`src/ray/observability/open_telemetry_metric_recorder.cc:161-172`）。
//!   录制值要求有限且 `>= 0.0`。
//! - [`Histogram`]：OpenCensus 路径注册 `Aggregation::Distribution(BucketBoundaries::Explicit(boundaries))`
//!   （`src/ray/stats/metric.cc:204-214`），OpenTelemetry 路径调用 `RegisterHistogramMetric`
//!   （`src/ray/observability/open_telemetry_metric_recorder.cc:187-225`），按严格单调递增的桶边界统计分布。
//! - [`Sum`]：OpenCensus 路径注册 `Aggregation::Sum()` 按传入的 `f64` 数值累加求和
//!   （`src/ray/stats/metric.cc:235-244`），OpenTelemetry 路径调用 `RegisterSumMetric`
//!   注册 `DoubleUpDownCounter`（`src/ray/observability/open_telemetry_metric_recorder.cc:174-185`）。
//!
//! ## 运行模式、关闭闸门与上游端口限制
//!
//! - **构造与析构独立于运行时生命周期**：`new`、`.name()` 与 `Drop` 仅操作 `ray::Metric`
//!   实例本身，可在 [`crate::Ray::connect`] 或 [`crate::Ray::local`] 之前完成初始化，并可跨越运行时关闭与重建周期。
//! - **关闭闸门保护**：`ray::stats::Shutdown()`（`src/ray/stats/stats.h:133-150`）停止导出线程与
//!   `DeltaProducer` / `StatsExporter` / `OpenTelemetryMetricRecorder` 时保持
//!   `StatsConfig::instance().is_stats_disabled_` 原值不变。所有录制调用在完成参数校验后统一经运行时关闭闸门校验：
//!   运行时未初始化时返回 [`RayError::Ffi`]，关闭进行中或已关闭时返回 [`RayError::Runtime`]。
//! - **本地模式行为**：本地模式未启动 `CoreWorkerProcess`，`StatsConfig::instance().IsStatsDisabled()`
//!   保持初始值 `true`（`src/ray/stats/metric.h:90`、`src/ray/stats/metric.cc:62`），录制调用在活跃运行时上检查后作为空操作返回 `Ok(())`。
//! - **集群模式上游端口限制**：上游 `ProcessHelper::RayStart`（`cpp/src/ray/util/process_helper.cc:80-179`，
//!   第 `:153` 行）将 `CoreWorkerOptions::metrics_agent_port` 固定设为 `-1`，进程内 OpenCensus 与
//!   OpenTelemetry 视图正常完成注册与聚合，向节点 metrics agent 建立 gRPC 连接导出时因端口 `-1` 无法连通。
//!
//! ## 示例
//!
//! ```
//! use rayrust::{Counter, Gauge, Histogram, Sum};
//!
//! let gauge = Gauge::new("queue_depth", "Pending items", "items", &["worker"])?;
//! let counter = Counter::new("requests_total", "Processed requests", "count", &["status"])?;
//! let hist = Histogram::new(
//!     "request_latency_ms",
//!     "Request latency",
//!     "ms",
//!     &[1.0, 5.0, 10.0, 50.0],
//!     &["route"],
//! )?;
//! let sum = Sum::new("bytes_transferred", "Net payload delta", "bytes", &[])?;
//!
//! assert_eq!(gauge.name(), "queue_depth");
//! assert_eq!(counter.name(), "requests_total");
//! assert_eq!(hist.name(), "request_latency_ms");
//! assert_eq!(sum.name(), "bytes_transferred");
//! # Ok::<(), rayrust::RayError>(())
//! ```

use std::fmt;
use std::os::raw::c_char;
use std::ptr::NonNull;

use rayrust_sys::{self, RayBytes, RayMetric, RayMetricTag};

use crate::error::RayError;
use crate::runtime::ffi_error;

#[inline]
fn str_to_ray_bytes(s: &str) -> RayBytes {
    RayBytes {
        data: s.as_ptr() as *const c_char,
        len: s.len(),
    }
}

fn build_raw_tag_keys(tag_keys: &[&str]) -> Vec<RayBytes> {
    tag_keys.iter().map(|k| str_to_ray_bytes(k)).collect()
}

fn build_raw_tags(tags: &[(&str, &str)]) -> Vec<RayMetricTag> {
    tags.iter()
        .map(|(k, v)| RayMetricTag {
            key: str_to_ray_bytes(k),
            value: str_to_ray_bytes(v),
        })
        .collect()
}

struct MetricInner {
    ptr: NonNull<RayMetric>,
    name: String,
}

// 安全性说明：底层 `ray::stats::Metric::Record`（`src/ray/stats/metric.cc:111-162`）在
// OpenCensus 路径全程持有全局静态互斥锁 `registration_mutex_`（`src/ray/stats/metric.h:156`），
// 在 OpenTelemetry 路径经 `OpenTelemetryMetricRecorder::SetMetricValue`
// （`src/ray/observability/open_telemetry_metric_recorder.cc:227-237`）全程持有 `mutex_`，
// `TagKey::Register` 内部持有独立互斥锁；`name` 在构造后只读不变，析构仅在唯一所有者释放时执行一次。
unsafe impl Send for MetricInner {}
unsafe impl Sync for MetricInner {}

impl Drop for MetricInner {
    fn drop(&mut self) {
        unsafe {
            rayrust_sys::ray_metric_destroy(self.ptr.as_ptr());
        }
    }
}

impl MetricInner {
    fn from_raw(raw: *mut RayMetric, name: &str, context: &str) -> Result<Self, RayError> {
        let ptr = NonNull::new(raw).ok_or_else(|| ffi_error(context))?;
        Ok(Self {
            ptr,
            name: name.to_string(),
        })
    }

    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    fn record(&self, value: f64, tags: &[(&str, &str)], context: &str) -> Result<(), RayError> {
        let raw_tags = build_raw_tags(tags);
        let tags_ptr = if raw_tags.is_empty() {
            std::ptr::null()
        } else {
            raw_tags.as_ptr()
        };
        let rc = unsafe {
            rayrust_sys::ray_metric_record(self.ptr.as_ptr(), value, tags_ptr, raw_tags.len())
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(ffi_error(context))
        }
    }
}

impl fmt::Debug for MetricInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricInner")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// 瞬时值指标（对应 `ray::Gauge`），保留最近一次写入的有限 `f64` 数值，支持正数、零与负数。
pub struct Gauge {
    inner: MetricInner,
}

impl Gauge {
    /// 创建 `Gauge` 指标。
    ///
    /// `name` 须满足正则 `^[a-zA-Z_:][a-zA-Z0-9_:]*$`；`tag_keys` 中每个键须非空、无内部
    /// NUL 字节且互不重复。
    pub fn new(
        name: &str,
        description: &str,
        unit: &str,
        tag_keys: &[&str],
    ) -> Result<Self, RayError> {
        let raw_keys = build_raw_tag_keys(tag_keys);
        let keys_ptr = if raw_keys.is_empty() {
            std::ptr::null()
        } else {
            raw_keys.as_ptr()
        };
        let raw = unsafe {
            rayrust_sys::ray_gauge_create(
                str_to_ray_bytes(name),
                str_to_ray_bytes(description),
                str_to_ray_bytes(unit),
                keys_ptr,
                raw_keys.len(),
            )
        };
        let inner = MetricInner::from_raw(raw, name, "Gauge::new")?;
        Ok(Self { inner })
    }

    /// 返回指标名称。
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// 将当前 `Gauge` 设为 `value`（须为有限 `f64`）。
    pub fn set(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        let raw_tags = build_raw_tags(tags);
        let tags_ptr = if raw_tags.is_empty() {
            std::ptr::null()
        } else {
            raw_tags.as_ptr()
        };
        let rc = unsafe {
            rayrust_sys::ray_gauge_set(self.inner.ptr.as_ptr(), value, tags_ptr, raw_tags.len())
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(ffi_error("Gauge::set"))
        }
    }

    /// 通过基类 `ray::Metric::Record` 录制 `value`（须为有限 `f64`）。
    pub fn record(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        self.inner.record(value, tags, "Gauge::record")
    }
}

impl fmt::Debug for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gauge")
            .field("name", &self.inner.name)
            .finish()
    }
}

/// 单调递增计数器指标（对应 `ray::Counter`）。
///
/// OpenCensus 路径使用 `Aggregation::Count()` 按事件调用次数计数，OpenTelemetry 路径使用
/// `RegisterCounterMetric` 按传入的非负值累加。`value` 须有限且 `>= 0.0`。
pub struct Counter {
    inner: MetricInner,
}

impl Counter {
    /// 创建 `Counter` 指标。
    ///
    /// `name` 须满足正则 `^[a-zA-Z_:][a-zA-Z0-9_:]*$`；`tag_keys` 中每个键须非空、无内部
    /// NUL 字节且互不重复。
    pub fn new(
        name: &str,
        description: &str,
        unit: &str,
        tag_keys: &[&str],
    ) -> Result<Self, RayError> {
        let raw_keys = build_raw_tag_keys(tag_keys);
        let keys_ptr = if raw_keys.is_empty() {
            std::ptr::null()
        } else {
            raw_keys.as_ptr()
        };
        let raw = unsafe {
            rayrust_sys::ray_counter_create(
                str_to_ray_bytes(name),
                str_to_ray_bytes(description),
                str_to_ray_bytes(unit),
                keys_ptr,
                raw_keys.len(),
            )
        };
        let inner = MetricInner::from_raw(raw, name, "Counter::new")?;
        Ok(Self { inner })
    }

    /// 返回指标名称。
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// 递增计数器，`value` 须为有限且非负（`>= 0.0`）的 `f64`。
    pub fn inc(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        let raw_tags = build_raw_tags(tags);
        let tags_ptr = if raw_tags.is_empty() {
            std::ptr::null()
        } else {
            raw_tags.as_ptr()
        };
        let rc = unsafe {
            rayrust_sys::ray_counter_inc(self.inner.ptr.as_ptr(), value, tags_ptr, raw_tags.len())
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(ffi_error("Counter::inc"))
        }
    }

    /// 通过基类 `ray::Metric::Record` 录制增量，`value` 须有限且 `>= 0.0`。
    pub fn record(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        self.inner.record(value, tags, "Counter::record")
    }
}

impl fmt::Debug for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counter")
            .field("name", &self.inner.name)
            .finish()
    }
}

/// 分桶分布直方图指标（对应 `ray::Histogram`）。
pub struct Histogram {
    inner: MetricInner,
}

impl Histogram {
    /// 创建 `Histogram` 指标。
    ///
    /// `name` 须满足正则 `^[a-zA-Z_:][a-zA-Z0-9_:]*$`；`boundaries` 须非空、所有元素为有限
    /// `f64` 且严格单调递增（`boundaries[i - 1] < boundaries[i]`）；`tag_keys` 中每个键须非空、
    /// 无内部 NUL 字节且互不重复。
    pub fn new(
        name: &str,
        description: &str,
        unit: &str,
        boundaries: &[f64],
        tag_keys: &[&str],
    ) -> Result<Self, RayError> {
        let raw_keys = build_raw_tag_keys(tag_keys);
        let keys_ptr = if raw_keys.is_empty() {
            std::ptr::null()
        } else {
            raw_keys.as_ptr()
        };
        let bounds_ptr = if boundaries.is_empty() {
            std::ptr::null()
        } else {
            boundaries.as_ptr()
        };
        let raw = unsafe {
            rayrust_sys::ray_histogram_create(
                str_to_ray_bytes(name),
                str_to_ray_bytes(description),
                str_to_ray_bytes(unit),
                bounds_ptr,
                boundaries.len(),
                keys_ptr,
                raw_keys.len(),
            )
        };
        let inner = MetricInner::from_raw(raw, name, "Histogram::new")?;
        Ok(Self { inner })
    }

    /// 返回指标名称。
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// 记录一次直方图观测值 `value`（须为有限 `f64`）。
    pub fn observe(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        let raw_tags = build_raw_tags(tags);
        let tags_ptr = if raw_tags.is_empty() {
            std::ptr::null()
        } else {
            raw_tags.as_ptr()
        };
        let rc = unsafe {
            rayrust_sys::ray_histogram_observe(
                self.inner.ptr.as_ptr(),
                value,
                tags_ptr,
                raw_tags.len(),
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(ffi_error("Histogram::observe"))
        }
    }

    /// 通过基类 `ray::Metric::Record` 录制观测值 `value`（须为有限 `f64`）。
    pub fn record(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        self.inner.record(value, tags, "Histogram::record")
    }
}

impl fmt::Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Histogram")
            .field("name", &self.inner.name)
            .finish()
    }
}

/// 数值累加和指标（对应 `ray::Sum`）。
///
/// OpenCensus 路径使用 `Aggregation::Sum()` 按传入的 `f64` 数值累加求和，OpenTelemetry 路径使用
/// `RegisterSumMetric` 注册 `DoubleUpDownCounter`，支持正数、零与负数。
pub struct Sum {
    inner: MetricInner,
}

impl Sum {
    /// 创建 `Sum` 指标。
    ///
    /// `name` 须满足正则 `^[a-zA-Z_:][a-zA-Z0-9_:]*$`；`tag_keys` 中每个键须非空、无内部
    /// NUL 字节且互不重复。
    pub fn new(
        name: &str,
        description: &str,
        unit: &str,
        tag_keys: &[&str],
    ) -> Result<Self, RayError> {
        let raw_keys = build_raw_tag_keys(tag_keys);
        let keys_ptr = if raw_keys.is_empty() {
            std::ptr::null()
        } else {
            raw_keys.as_ptr()
        };
        let raw = unsafe {
            rayrust_sys::ray_sum_create(
                str_to_ray_bytes(name),
                str_to_ray_bytes(description),
                str_to_ray_bytes(unit),
                keys_ptr,
                raw_keys.len(),
            )
        };
        let inner = MetricInner::from_raw(raw, name, "Sum::new")?;
        Ok(Self { inner })
    }

    /// 返回指标名称。
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// 录制一次求和点 `value`（须为有限 `f64`）。
    pub fn record(&self, value: f64, tags: &[(&str, &str)]) -> Result<(), RayError> {
        self.inner.record(value, tags, "Sum::record")
    }
}

impl fmt::Debug for Sum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sum")
            .field("name", &self.inner.name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn metric_types_are_send_and_sync() {
        assert_send_sync::<Gauge>();
        assert_send_sync::<Counter>();
        assert_send_sync::<Histogram>();
        assert_send_sync::<Sum>();
    }

    #[test]
    fn construct_and_debug_without_runtime() {
        let g = Gauge::new("unit_gauge", "desc", "u", &["k1"]).expect("Gauge::new");
        let c = Counter::new("_unit:counter_1", "", "", &[]).expect("Counter::new");
        let h = Histogram::new(":unit_hist", "lat", "ms", &[-1.0, 0.0, 2.5], &["k"]).expect("Hist");
        let s = Sum::new("UnitSum_9", "sum", "bytes", &["a", "b"]).expect("Sum::new");

        assert_eq!(g.name(), "unit_gauge");
        assert_eq!(c.name(), "_unit:counter_1");
        assert_eq!(h.name(), ":unit_hist");
        assert_eq!(s.name(), "UnitSum_9");
        assert!(format!("{:?}", g).contains("unit_gauge"));
        assert!(format!("{:?}", c).contains("_unit:counter_1"));
        assert!(format!("{:?}", h).contains(":unit_hist"));
        assert!(format!("{:?}", s).contains("UnitSum_9"));
    }

    #[test]
    fn reject_invalid_names_and_boundaries_without_aborting() {
        for bad in ["", "1digit", "a-b", "a.b", "a b", "指标", "bad\0name"] {
            assert!(matches!(
                Gauge::new(bad, "d", "u", &[]),
                Err(RayError::Ffi(_))
            ));
            assert!(matches!(
                Counter::new(bad, "d", "u", &[]),
                Err(RayError::Ffi(_))
            ));
            assert!(matches!(
                Histogram::new(bad, "d", "u", &[1.0], &[]),
                Err(RayError::Ffi(_))
            ));
            assert!(matches!(
                Sum::new(bad, "d", "u", &[]),
                Err(RayError::Ffi(_))
            ));
        }

        for bad_bounds in [
            &[][..],
            &[f64::NAN][..],
            &[f64::INFINITY][..],
            &[f64::NEG_INFINITY][..],
            &[1.0, 1.0][..],
            &[5.0, 2.0][..],
        ] {
            assert!(matches!(
                Histogram::new("valid_hist", "d", "u", bad_bounds, &[]),
                Err(RayError::Ffi(_))
            ));
        }
    }
}

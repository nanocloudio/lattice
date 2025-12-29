//! Metrics and health checks.
//!
//! Per §14, metrics namespaces:
//! - lattice.kv.*
//! - lattice.adapter.etcd.*
//! - lattice.quota.*
//! - plus inherited clustor.*

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Readiness status for the /readyz endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessStatus {
    /// Overall ready state.
    pub ready: bool,
    /// Tenant routing epoch cache age in seconds.
    pub routing_cache_age_seconds: f64,
    /// Adapter enablement states.
    pub adapters: AdapterStatus,
    /// Digest of the active tenant manifest.
    pub manifest_digest: String,
    /// Individual component status.
    pub components: ComponentStatus,
}

/// Adapter enablement status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterStatus {
    /// etcd adapter enabled.
    pub etcd: bool,
    /// Redis adapter enabled.
    pub redis: bool,
    /// Memcached adapter enabled.
    pub memcached: bool,
}

/// Component health status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    /// Storage layer healthy.
    pub storage: bool,
    /// Control plane cache healthy.
    pub control_plane: bool,
    /// KPG runtime healthy.
    pub kpg_runtime: bool,
}

impl Default for ReadinessStatus {
    fn default() -> Self {
        Self {
            ready: false,
            routing_cache_age_seconds: 0.0,
            adapters: AdapterStatus {
                etcd: false,
                redis: false,
                memcached: false,
            },
            manifest_digest: String::new(),
            components: ComponentStatus {
                storage: false,
                control_plane: false,
                kpg_runtime: false,
            },
        }
    }
}

/// Health check result for /healthz.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Overall healthy state.
    pub healthy: bool,
    /// Status message.
    pub message: String,
}

impl HealthStatus {
    /// Create a healthy status.
    pub fn healthy() -> Self {
        Self {
            healthy: true,
            message: "OK".to_string(),
        }
    }

    /// Create an unhealthy status.
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            healthy: false,
            message: message.into(),
        }
    }
}

/// Metric names per §14 specification.
pub mod metrics {
    /// LIN-BOUND can_linearize gauge.
    pub const LIN_BOUND_CAN_LINEARIZE: &str = "lattice.lin_bound.can_linearize";
    /// LIN-BOUND failed clause counter.
    pub const LIN_BOUND_FAILED_CLAUSE: &str = "lattice.lin_bound.failed_clause";
    /// Current KV revision gauge.
    pub const KV_REVISION: &str = "lattice.kv.revision";
    /// Compaction floor revision gauge.
    pub const KV_COMPACTION_FLOOR: &str = "lattice.kv.compaction_floor_revision";
    /// TTL expiry queue depth gauge.
    pub const TTL_EXPIRY_QUEUE_DEPTH: &str = "lattice.ttl.expiry_queue_depth";
    /// Active watch streams gauge.
    pub const WATCH_ACTIVE_STREAMS: &str = "lattice.watch.active_streams";
    /// Active leases gauge.
    pub const LEASE_ACTIVE_LEASES: &str = "lattice.lease.active_leases";
    /// Adapter requests total counter.
    pub const ADAPTER_REQUESTS_TOTAL: &str = "lattice.adapter.{name}.requests_total";
    /// Adapter errors total counter.
    pub const ADAPTER_ERRORS_TOTAL: &str = "lattice.adapter.{name}.errors_total";
    /// Quota throttle total counter.
    pub const QUOTA_THROTTLE_TOTAL: &str = "lattice.quota.throttle_total";
    /// Dirty epoch total counter.
    pub const DIRTY_EPOCH_TOTAL: &str = "lattice.dirty_epoch_total";
}

/// Metrics registry for Lattice.
#[derive(Debug)]
pub struct MetricsRegistry {
    /// Counter metrics.
    counters: RwLock<HashMap<String, AtomicU64>>,
    /// Gauge metrics.
    gauges: RwLock<HashMap<String, AtomicU64>>,
    /// Histogram observations (simplified: just count and sum).
    histograms: RwLock<HashMap<String, HistogramData>>,
    /// Labels for metrics.
    labels: RwLock<HashMap<String, HashMap<String, String>>>,
}

impl MetricsRegistry {
    /// Create a new metrics registry.
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            labels: RwLock::new(HashMap::new()),
        }
    }

    /// Increment a counter.
    pub fn counter_inc(&self, name: &str) {
        self.counter_add(name, 1);
    }

    /// Add to a counter.
    pub fn counter_add(&self, name: &str, value: u64) {
        let counters = self.counters.read().unwrap();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
            return;
        }
        drop(counters);

        let mut counters = self.counters.write().unwrap();
        counters
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(value, Ordering::Relaxed);
    }

    /// Get counter value.
    pub fn counter_get(&self, name: &str) -> u64 {
        let counters = self.counters.read().unwrap();
        counters
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Set a gauge value.
    pub fn gauge_set(&self, name: &str, value: u64) {
        let gauges = self.gauges.read().unwrap();
        if let Some(gauge) = gauges.get(name) {
            gauge.store(value, Ordering::Relaxed);
            return;
        }
        drop(gauges);

        let mut gauges = self.gauges.write().unwrap();
        gauges
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .store(value, Ordering::Relaxed);
    }

    /// Get gauge value.
    pub fn gauge_get(&self, name: &str) -> u64 {
        let gauges = self.gauges.read().unwrap();
        gauges
            .get(name)
            .map(|g| g.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Increment a gauge.
    pub fn gauge_inc(&self, name: &str) {
        let gauges = self.gauges.read().unwrap();
        if let Some(gauge) = gauges.get(name) {
            gauge.fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(gauges);

        let mut gauges = self.gauges.write().unwrap();
        gauges
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement a gauge.
    pub fn gauge_dec(&self, name: &str) {
        let gauges = self.gauges.read().unwrap();
        if let Some(gauge) = gauges.get(name) {
            gauge.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record a histogram observation.
    pub fn histogram_observe(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.write().unwrap();
        let data = histograms
            .entry(name.to_string())
            .or_insert_with(HistogramData::new);
        data.observe(value);
    }

    /// Get histogram data.
    pub fn histogram_get(&self, name: &str) -> Option<HistogramSnapshot> {
        let histograms = self.histograms.read().unwrap();
        histograms.get(name).map(|h| h.snapshot())
    }

    /// Set labels for a metric.
    pub fn set_labels(&self, metric: &str, labels: HashMap<String, String>) {
        let mut all_labels = self.labels.write().unwrap();
        all_labels.insert(metric.to_string(), labels);
    }

    /// Get all counter names.
    pub fn counter_names(&self) -> Vec<String> {
        let counters = self.counters.read().unwrap();
        counters.keys().cloned().collect()
    }

    /// Get all gauge names.
    pub fn gauge_names(&self) -> Vec<String> {
        let gauges = self.gauges.read().unwrap();
        gauges.keys().cloned().collect()
    }

    /// Export metrics in Prometheus format.
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Export counters
        let counters = self.counters.read().unwrap();
        for (name, value) in counters.iter() {
            let prometheus_name = name.replace('.', "_");
            output.push_str(&format!(
                "# TYPE {} counter\n{} {}\n",
                prometheus_name,
                prometheus_name,
                value.load(Ordering::Relaxed)
            ));
        }

        // Export gauges
        let gauges = self.gauges.read().unwrap();
        for (name, value) in gauges.iter() {
            let prometheus_name = name.replace('.', "_");
            output.push_str(&format!(
                "# TYPE {} gauge\n{} {}\n",
                prometheus_name,
                prometheus_name,
                value.load(Ordering::Relaxed)
            ));
        }

        // Export histograms
        let histograms = self.histograms.read().unwrap();
        for (name, data) in histograms.iter() {
            let prometheus_name = name.replace('.', "_");
            let snapshot = data.snapshot();
            output.push_str(&format!(
                "# TYPE {} histogram\n{}_count {}\n{}_sum {}\n",
                prometheus_name, prometheus_name, snapshot.count, prometheus_name, snapshot.sum
            ));
        }

        output
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Histogram data storage.
#[derive(Debug)]
struct HistogramData {
    count: AtomicU64,
    sum: std::sync::Mutex<f64>,
    min: std::sync::Mutex<f64>,
    max: std::sync::Mutex<f64>,
}

impl HistogramData {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: std::sync::Mutex::new(0.0),
            min: std::sync::Mutex::new(f64::MAX),
            max: std::sync::Mutex::new(f64::MIN),
        }
    }

    fn observe(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        *self.sum.lock().unwrap() += value;
        let mut min = self.min.lock().unwrap();
        if value < *min {
            *min = value;
        }
        let mut max = self.max.lock().unwrap();
        if value > *max {
            *max = value;
        }
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            count: self.count.load(Ordering::Relaxed),
            sum: *self.sum.lock().unwrap(),
            min: *self.min.lock().unwrap(),
            max: *self.max.lock().unwrap(),
        }
    }
}

/// Histogram snapshot.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Number of observations.
    pub count: u64,
    /// Sum of all observations.
    pub sum: f64,
    /// Minimum value observed.
    pub min: f64,
    /// Maximum value observed.
    pub max: f64,
}

impl HistogramSnapshot {
    /// Calculate mean.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Lattice-specific metrics collector.
#[derive(Debug)]
pub struct LatticeMetrics {
    /// Core registry.
    registry: MetricsRegistry,
}

impl LatticeMetrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            registry: MetricsRegistry::new(),
        }
    }

    /// Get the underlying registry.
    pub fn registry(&self) -> &MetricsRegistry {
        &self.registry
    }

    // === LIN-BOUND Metrics (Task 152) ===

    /// Record can_linearize status.
    pub fn record_can_linearize(&self, available: bool) {
        self.registry.gauge_set(
            metrics::LIN_BOUND_CAN_LINEARIZE,
            if available { 1 } else { 0 },
        );
    }

    /// Record LIN-BOUND failure.
    pub fn record_lin_bound_failure(&self, clause: &str) {
        let metric = format!("{}.{}", metrics::LIN_BOUND_FAILED_CLAUSE, clause);
        self.registry.counter_inc(&metric);
    }

    // === KV Metrics (Task 152, 153) ===

    /// Update KV revision gauge.
    pub fn set_kv_revision(&self, revision: u64) {
        self.registry.gauge_set(metrics::KV_REVISION, revision);
    }

    /// Update compaction floor gauge.
    pub fn set_compaction_floor(&self, floor: u64) {
        self.registry.gauge_set(metrics::KV_COMPACTION_FLOOR, floor);
    }

    /// Update TTL expiry queue depth.
    pub fn set_ttl_queue_depth(&self, depth: u64) {
        self.registry
            .gauge_set(metrics::TTL_EXPIRY_QUEUE_DEPTH, depth);
    }

    /// Update active watch streams.
    pub fn set_active_watches(&self, count: u64) {
        self.registry
            .gauge_set(metrics::WATCH_ACTIVE_STREAMS, count);
    }

    /// Update active leases.
    pub fn set_active_leases(&self, count: u64) {
        self.registry.gauge_set(metrics::LEASE_ACTIVE_LEASES, count);
    }

    // === Adapter Metrics (Task 154) ===

    /// Record adapter request.
    pub fn record_adapter_request(&self, adapter: &str, operation: &str) {
        let metric = format!("lattice.adapter.{}.requests_total.{}", adapter, operation);
        self.registry.counter_inc(&metric);
    }

    /// Record adapter error.
    pub fn record_adapter_error(&self, adapter: &str, error_type: &str) {
        let metric = format!("lattice.adapter.{}.errors_total.{}", adapter, error_type);
        self.registry.counter_inc(&metric);
    }

    /// Record adapter latency.
    pub fn record_adapter_latency(&self, adapter: &str, operation: &str, latency_ms: f64) {
        let metric = format!("lattice.adapter.{}.latency_ms.{}", adapter, operation);
        self.registry.histogram_observe(&metric, latency_ms);
    }

    // === Quota Metrics (Task 155) ===

    /// Record quota throttle.
    pub fn record_quota_throttle(&self, tenant_id: &str) {
        self.registry.counter_inc(metrics::QUOTA_THROTTLE_TOTAL);
        let tenant_metric = format!("lattice.quota.throttle_total.{}", tenant_id);
        self.registry.counter_inc(&tenant_metric);
    }

    /// Set tenant usage gauge.
    pub fn set_tenant_usage(&self, tenant_id: &str, requests: u64, bytes: u64) {
        let req_metric = format!("lattice.quota.{}.requests", tenant_id);
        let bytes_metric = format!("lattice.quota.{}.bytes", tenant_id);
        self.registry.gauge_set(&req_metric, requests);
        self.registry.gauge_set(&bytes_metric, bytes);
    }

    // === Dirty Epoch Metrics (Task 156) ===

    /// Record dirty epoch.
    pub fn record_dirty_epoch(&self, tenant_id: &str) {
        self.registry.counter_inc(metrics::DIRTY_EPOCH_TOTAL);
        let tenant_metric = format!("lattice.dirty_epoch.{}", tenant_id);
        self.registry.counter_inc(&tenant_metric);
    }

    /// Export all metrics.
    pub fn export(&self) -> String {
        self.registry.export_prometheus()
    }
}

impl Default for LatticeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Readiness probe handler.
#[derive(Debug)]
pub struct ReadinessProbe {
    /// Current status.
    status: RwLock<ReadinessStatus>,
}

impl ReadinessProbe {
    /// Create a new readiness probe.
    pub fn new() -> Self {
        Self {
            status: RwLock::new(ReadinessStatus::default()),
        }
    }

    /// Get current readiness status.
    pub fn status(&self) -> ReadinessStatus {
        self.status.read().unwrap().clone()
    }

    /// Update readiness status.
    pub fn update(&self, status: ReadinessStatus) {
        *self.status.write().unwrap() = status;
    }

    /// Mark as ready.
    pub fn set_ready(&self, ready: bool) {
        self.status.write().unwrap().ready = ready;
    }

    /// Update routing cache age.
    pub fn set_routing_cache_age(&self, age_seconds: f64) {
        self.status.write().unwrap().routing_cache_age_seconds = age_seconds;
    }

    /// Update adapter status.
    pub fn set_adapter_status(&self, adapters: AdapterStatus) {
        self.status.write().unwrap().adapters = adapters;
    }

    /// Update manifest digest.
    pub fn set_manifest_digest(&self, digest: impl Into<String>) {
        self.status.write().unwrap().manifest_digest = digest.into();
    }

    /// Update component status.
    pub fn set_component_status(&self, components: ComponentStatus) {
        self.status.write().unwrap().components = components;
    }

    /// Check if ready.
    pub fn is_ready(&self) -> bool {
        self.status.read().unwrap().ready
    }
}

impl Default for ReadinessProbe {
    fn default() -> Self {
        Self::new()
    }
}

/// Health probe handler.
#[derive(Debug)]
pub struct HealthProbe {
    /// Current status.
    status: RwLock<HealthStatus>,
}

impl HealthProbe {
    /// Create a new health probe.
    pub fn new() -> Self {
        Self {
            status: RwLock::new(HealthStatus::healthy()),
        }
    }

    /// Get current health status.
    pub fn status(&self) -> HealthStatus {
        self.status.read().unwrap().clone()
    }

    /// Mark as healthy.
    pub fn set_healthy(&self) {
        *self.status.write().unwrap() = HealthStatus::healthy();
    }

    /// Mark as unhealthy.
    pub fn set_unhealthy(&self, message: impl Into<String>) {
        *self.status.write().unwrap() = HealthStatus::unhealthy(message);
    }

    /// Check if healthy.
    pub fn is_healthy(&self) -> bool {
        self.status.read().unwrap().healthy
    }
}

impl Default for HealthProbe {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_counters() {
        let registry = MetricsRegistry::new();

        registry.counter_inc("test.counter");
        assert_eq!(registry.counter_get("test.counter"), 1);

        registry.counter_add("test.counter", 5);
        assert_eq!(registry.counter_get("test.counter"), 6);
    }

    #[test]
    fn test_metrics_registry_gauges() {
        let registry = MetricsRegistry::new();

        registry.gauge_set("test.gauge", 100);
        assert_eq!(registry.gauge_get("test.gauge"), 100);

        registry.gauge_inc("test.gauge");
        assert_eq!(registry.gauge_get("test.gauge"), 101);

        registry.gauge_dec("test.gauge");
        assert_eq!(registry.gauge_get("test.gauge"), 100);
    }

    #[test]
    fn test_metrics_registry_histograms() {
        let registry = MetricsRegistry::new();

        registry.histogram_observe("test.histogram", 10.0);
        registry.histogram_observe("test.histogram", 20.0);
        registry.histogram_observe("test.histogram", 30.0);

        let snapshot = registry.histogram_get("test.histogram").unwrap();
        assert_eq!(snapshot.count, 3);
        assert_eq!(snapshot.sum, 60.0);
        assert_eq!(snapshot.min, 10.0);
        assert_eq!(snapshot.max, 30.0);
        assert_eq!(snapshot.mean(), 20.0);
    }

    #[test]
    fn test_lattice_metrics_kv() {
        let metrics = LatticeMetrics::new();

        metrics.set_kv_revision(100);
        metrics.set_compaction_floor(50);
        metrics.set_ttl_queue_depth(25);

        assert_eq!(metrics.registry.gauge_get(metrics::KV_REVISION), 100);
        assert_eq!(metrics.registry.gauge_get(metrics::KV_COMPACTION_FLOOR), 50);
        assert_eq!(
            metrics.registry.gauge_get(metrics::TTL_EXPIRY_QUEUE_DEPTH),
            25
        );
    }

    #[test]
    fn test_lattice_metrics_adapter() {
        let metrics = LatticeMetrics::new();

        metrics.record_adapter_request("etcd", "range");
        metrics.record_adapter_request("etcd", "range");
        metrics.record_adapter_request("etcd", "put");

        assert_eq!(
            metrics
                .registry
                .counter_get("lattice.adapter.etcd.requests_total.range"),
            2
        );
        assert_eq!(
            metrics
                .registry
                .counter_get("lattice.adapter.etcd.requests_total.put"),
            1
        );
    }

    #[test]
    fn test_lattice_metrics_quota() {
        let metrics = LatticeMetrics::new();

        metrics.record_quota_throttle("tenant1");
        metrics.record_quota_throttle("tenant1");
        metrics.record_quota_throttle("tenant2");

        assert_eq!(
            metrics.registry.counter_get(metrics::QUOTA_THROTTLE_TOTAL),
            3
        );
        assert_eq!(
            metrics
                .registry
                .counter_get("lattice.quota.throttle_total.tenant1"),
            2
        );
    }

    #[test]
    fn test_readiness_probe() {
        let probe = ReadinessProbe::new();
        assert!(!probe.is_ready());

        probe.set_ready(true);
        assert!(probe.is_ready());

        probe.set_routing_cache_age(5.5);
        assert_eq!(probe.status().routing_cache_age_seconds, 5.5);
    }

    #[test]
    fn test_health_probe() {
        let probe = HealthProbe::new();
        assert!(probe.is_healthy());

        probe.set_unhealthy("test error");
        assert!(!probe.is_healthy());
        assert_eq!(probe.status().message, "test error");

        probe.set_healthy();
        assert!(probe.is_healthy());
    }

    #[test]
    fn test_prometheus_export() {
        let registry = MetricsRegistry::new();
        registry.counter_inc("test.counter");
        registry.gauge_set("test.gauge", 42);

        let output = registry.export_prometheus();
        assert!(output.contains("test_counter 1"));
        assert!(output.contains("test_gauge 42"));
    }
}

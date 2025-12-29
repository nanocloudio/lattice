//! Telemetry collection.
//!
//! Structured logging and tracing integration per §14.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant;

/// Telemetry configuration.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Log level filter.
    pub log_level: String,
    /// Enable JSON output.
    pub json_output: bool,
    /// Include source location in logs.
    pub include_source_location: bool,
    /// Include thread ID in logs.
    pub include_thread_id: bool,
    /// Trace sampling rate (0.0 - 1.0).
    pub trace_sample_rate: f64,
    /// Maximum span attributes.
    pub max_span_attributes: usize,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            json_output: false,
            include_source_location: false,
            include_thread_id: false,
            trace_sample_rate: 1.0,
            max_span_attributes: 32,
        }
    }
}

impl TelemetryConfig {
    /// Create a production configuration.
    pub fn production() -> Self {
        Self {
            log_level: "info".to_string(),
            json_output: true,
            include_source_location: false,
            include_thread_id: false,
            trace_sample_rate: 0.1, // 10% sampling
            max_span_attributes: 32,
        }
    }

    /// Create a development configuration.
    pub fn development() -> Self {
        Self {
            log_level: "debug".to_string(),
            json_output: false,
            include_source_location: true,
            include_thread_id: true,
            trace_sample_rate: 1.0, // 100% sampling
            max_span_attributes: 64,
        }
    }

    /// Parse log level to filter.
    pub fn log_level_filter(&self) -> LogLevel {
        match self.log_level.to_lowercase().as_str() {
            "trace" => LogLevel::Trace,
            "debug" => LogLevel::Debug,
            "info" => LogLevel::Info,
            "warn" | "warning" => LogLevel::Warn,
            "error" => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }
}

/// Log levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trace => write!(f, "TRACE"),
            Self::Debug => write!(f, "DEBUG"),
            Self::Info => write!(f, "INFO"),
            Self::Warn => write!(f, "WARN"),
            Self::Error => write!(f, "ERROR"),
        }
    }
}

/// Structured log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp in RFC3339 format.
    pub timestamp: String,
    /// Log level.
    pub level: String,
    /// Message.
    pub message: String,
    /// Target module.
    pub target: String,
    /// Additional fields.
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

impl LogEntry {
    /// Create a new log entry.
    pub fn new(level: LogLevel, target: &str, message: impl Into<String>) -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: level.to_string(),
            message: message.into(),
            target: target.to_string(),
            fields: HashMap::new(),
        }
    }

    /// Add a field.
    pub fn with_field(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        if let Ok(v) = serde_json::to_value(value) {
            self.fields.insert(key.into(), v);
        }
        self
    }

    /// Format as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| self.to_text())
    }

    /// Format as plain text.
    pub fn to_text(&self) -> String {
        let fields = if self.fields.is_empty() {
            String::new()
        } else {
            let parts: Vec<String> = self
                .fields
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            format!(" {}", parts.join(" "))
        };
        format!(
            "{} {} [{}] {}{}",
            self.timestamp, self.level, self.target, self.message, fields
        )
    }
}

/// Telemetry collector.
#[derive(Debug)]
pub struct TelemetryCollector {
    /// Configuration.
    config: TelemetryConfig,
    /// Log count by level.
    log_counts: LogCounts,
    /// Span tracking.
    span_counts: SpanCounts,
    /// Start time.
    start_time: Instant,
}

#[derive(Debug)]
struct LogCounts {
    trace: AtomicU64,
    debug: AtomicU64,
    info: AtomicU64,
    warn: AtomicU64,
    error: AtomicU64,
}

impl LogCounts {
    fn new() -> Self {
        Self {
            trace: AtomicU64::new(0),
            debug: AtomicU64::new(0),
            info: AtomicU64::new(0),
            warn: AtomicU64::new(0),
            error: AtomicU64::new(0),
        }
    }

    fn increment(&self, level: LogLevel) {
        match level {
            LogLevel::Trace => self.trace.fetch_add(1, Ordering::Relaxed),
            LogLevel::Debug => self.debug.fetch_add(1, Ordering::Relaxed),
            LogLevel::Info => self.info.fetch_add(1, Ordering::Relaxed),
            LogLevel::Warn => self.warn.fetch_add(1, Ordering::Relaxed),
            LogLevel::Error => self.error.fetch_add(1, Ordering::Relaxed),
        };
    }

    fn snapshot(&self) -> LogCountSnapshot {
        LogCountSnapshot {
            trace: self.trace.load(Ordering::Relaxed),
            debug: self.debug.load(Ordering::Relaxed),
            info: self.info.load(Ordering::Relaxed),
            warn: self.warn.load(Ordering::Relaxed),
            error: self.error.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct SpanCounts {
    started: AtomicU64,
    ended: AtomicU64,
    dropped: AtomicU64,
    sampled: AtomicU64,
}

impl SpanCounts {
    fn new() -> Self {
        Self {
            started: AtomicU64::new(0),
            ended: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            sampled: AtomicU64::new(0),
        }
    }
}

impl TelemetryCollector {
    /// Create a new telemetry collector.
    pub fn new(config: TelemetryConfig) -> Self {
        Self {
            config,
            log_counts: LogCounts::new(),
            span_counts: SpanCounts::new(),
            start_time: Instant::now(),
        }
    }

    /// Get configuration.
    pub fn config(&self) -> &TelemetryConfig {
        &self.config
    }

    /// Check if level is enabled.
    pub fn is_enabled(&self, level: LogLevel) -> bool {
        level >= self.config.log_level_filter()
    }

    /// Log a message.
    pub fn log(&self, entry: LogEntry) {
        let level = match entry.level.as_str() {
            "TRACE" => LogLevel::Trace,
            "DEBUG" => LogLevel::Debug,
            "INFO" => LogLevel::Info,
            "WARN" => LogLevel::Warn,
            "ERROR" => LogLevel::Error,
            _ => LogLevel::Info,
        };

        if !self.is_enabled(level) {
            return;
        }

        self.log_counts.increment(level);

        let output = if self.config.json_output {
            entry.to_json()
        } else {
            entry.to_text()
        };

        // In a real implementation, this would go to the configured output
        eprintln!("{}", output);
    }

    /// Log at trace level.
    pub fn trace(&self, target: &str, message: impl Into<String>) {
        self.log(LogEntry::new(LogLevel::Trace, target, message));
    }

    /// Log at debug level.
    pub fn debug(&self, target: &str, message: impl Into<String>) {
        self.log(LogEntry::new(LogLevel::Debug, target, message));
    }

    /// Log at info level.
    pub fn info(&self, target: &str, message: impl Into<String>) {
        self.log(LogEntry::new(LogLevel::Info, target, message));
    }

    /// Log at warn level.
    pub fn warn(&self, target: &str, message: impl Into<String>) {
        self.log(LogEntry::new(LogLevel::Warn, target, message));
    }

    /// Log at error level.
    pub fn error(&self, target: &str, message: impl Into<String>) {
        self.log(LogEntry::new(LogLevel::Error, target, message));
    }

    // === Span Tracking ===

    /// Record span start.
    pub fn record_span_start(&self, sampled: bool) {
        self.span_counts.started.fetch_add(1, Ordering::Relaxed);
        if sampled {
            self.span_counts.sampled.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record span end.
    pub fn record_span_end(&self) {
        self.span_counts.ended.fetch_add(1, Ordering::Relaxed);
    }

    /// Record span drop.
    pub fn record_span_drop(&self) {
        self.span_counts.dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if a span should be sampled.
    pub fn should_sample(&self) -> bool {
        if self.config.trace_sample_rate >= 1.0 {
            return true;
        }
        if self.config.trace_sample_rate <= 0.0 {
            return false;
        }
        // Simple sampling based on time
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let random = (nanos as f64) / (u32::MAX as f64);
        random < self.config.trace_sample_rate
    }

    /// Get telemetry statistics.
    pub fn stats(&self) -> TelemetryStats {
        TelemetryStats {
            uptime_seconds: self.start_time.elapsed().as_secs(),
            log_counts: self.log_counts.snapshot(),
            spans_started: self.span_counts.started.load(Ordering::Relaxed),
            spans_ended: self.span_counts.ended.load(Ordering::Relaxed),
            spans_dropped: self.span_counts.dropped.load(Ordering::Relaxed),
            spans_sampled: self.span_counts.sampled.load(Ordering::Relaxed),
        }
    }
}

impl Default for TelemetryCollector {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}

/// Log count snapshot.
#[derive(Debug, Clone)]
pub struct LogCountSnapshot {
    /// Trace log count.
    pub trace: u64,
    /// Debug log count.
    pub debug: u64,
    /// Info log count.
    pub info: u64,
    /// Warn log count.
    pub warn: u64,
    /// Error log count.
    pub error: u64,
}

impl LogCountSnapshot {
    /// Total log count.
    pub fn total(&self) -> u64 {
        self.trace + self.debug + self.info + self.warn + self.error
    }
}

/// Telemetry statistics.
#[derive(Debug, Clone)]
pub struct TelemetryStats {
    /// Uptime in seconds.
    pub uptime_seconds: u64,
    /// Log counts by level.
    pub log_counts: LogCountSnapshot,
    /// Total spans started.
    pub spans_started: u64,
    /// Total spans ended.
    pub spans_ended: u64,
    /// Total spans dropped.
    pub spans_dropped: u64,
    /// Total spans sampled.
    pub spans_sampled: u64,
}

impl TelemetryStats {
    /// Get span sample rate.
    pub fn sample_rate(&self) -> f64 {
        if self.spans_started == 0 {
            0.0
        } else {
            self.spans_sampled as f64 / self.spans_started as f64
        }
    }
}

/// Trace span for timing operations.
#[derive(Debug)]
pub struct Span {
    /// Span name.
    name: String,
    /// Start time.
    start: Instant,
    /// Attributes.
    attributes: HashMap<String, String>,
    /// Whether this span is sampled.
    sampled: bool,
    /// Parent span ID.
    parent_id: Option<String>,
}

impl Span {
    /// Create a new span.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start: Instant::now(),
            attributes: HashMap::new(),
            sampled: true,
            parent_id: None,
        }
    }

    /// Create a child span.
    pub fn child(&self, name: impl Into<String>) -> Self {
        let mut span = Self::new(name);
        span.parent_id = Some(format!("{:p}", self));
        span.sampled = self.sampled;
        span
    }

    /// Set whether sampled.
    pub fn with_sampled(mut self, sampled: bool) -> Self {
        self.sampled = sampled;
        self
    }

    /// Add an attribute.
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Get span name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get elapsed time.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    /// Get elapsed time in milliseconds.
    pub fn elapsed_ms(&self) -> f64 {
        self.elapsed().as_secs_f64() * 1000.0
    }

    /// Check if sampled.
    pub fn is_sampled(&self) -> bool {
        self.sampled
    }

    /// End the span and return elapsed time.
    pub fn end(self) -> std::time::Duration {
        self.elapsed()
    }
}

/// Telemetry catalog entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    /// Metric name.
    pub name: String,
    /// Metric type (counter, gauge, histogram).
    pub metric_type: String,
    /// Description.
    pub description: String,
    /// Unit.
    pub unit: Option<String>,
    /// Labels.
    pub labels: Vec<String>,
}

/// Telemetry catalog for schema validation.
#[derive(Debug)]
pub struct TelemetryCatalog {
    /// Metric entries.
    metrics: RwLock<HashMap<String, CatalogEntry>>,
    /// Trace entries.
    traces: RwLock<HashMap<String, CatalogEntry>>,
    /// Log fields.
    log_fields: RwLock<HashMap<String, String>>,
}

impl TelemetryCatalog {
    /// Create a new catalog.
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
            traces: RwLock::new(HashMap::new()),
            log_fields: RwLock::new(HashMap::new()),
        }
    }

    /// Register a metric.
    pub fn register_metric(&self, entry: CatalogEntry) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.insert(entry.name.clone(), entry);
    }

    /// Register a trace.
    pub fn register_trace(&self, entry: CatalogEntry) {
        let mut traces = self.traces.write().unwrap();
        traces.insert(entry.name.clone(), entry);
    }

    /// Register a log field.
    pub fn register_log_field(&self, name: &str, description: &str) {
        let mut fields = self.log_fields.write().unwrap();
        fields.insert(name.to_string(), description.to_string());
    }

    /// Get all metrics.
    pub fn metrics(&self) -> Vec<CatalogEntry> {
        let metrics = self.metrics.read().unwrap();
        metrics.values().cloned().collect()
    }

    /// Get all traces.
    pub fn traces(&self) -> Vec<CatalogEntry> {
        let traces = self.traces.read().unwrap();
        traces.values().cloned().collect()
    }

    /// Validate a metric exists.
    pub fn validate_metric(&self, name: &str) -> bool {
        let metrics = self.metrics.read().unwrap();
        metrics.contains_key(name)
    }

    /// Export catalog as JSON.
    pub fn export_json(&self) -> String {
        #[derive(Serialize)]
        struct Catalog {
            metrics: Vec<CatalogEntry>,
            traces: Vec<CatalogEntry>,
            log_fields: HashMap<String, String>,
        }

        let catalog = Catalog {
            metrics: self.metrics(),
            traces: self.traces(),
            log_fields: self.log_fields.read().unwrap().clone(),
        };

        serde_json::to_string_pretty(&catalog).unwrap_or_default()
    }

    /// Load catalog from JSON.
    pub fn load_json(&self, json: &str) -> Result<(), serde_json::Error> {
        #[derive(Deserialize)]
        struct Catalog {
            metrics: Vec<CatalogEntry>,
            traces: Vec<CatalogEntry>,
            log_fields: HashMap<String, String>,
        }

        let catalog: Catalog = serde_json::from_str(json)?;

        for entry in catalog.metrics {
            self.register_metric(entry);
        }
        for entry in catalog.traces {
            self.register_trace(entry);
        }
        for (name, desc) in catalog.log_fields {
            self.register_log_field(&name, &desc);
        }

        Ok(())
    }
}

impl Default for TelemetryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_config_default() {
        let config = TelemetryConfig::default();
        assert_eq!(config.log_level, "info");
        assert!(!config.json_output);
        assert_eq!(config.log_level_filter(), LogLevel::Info);
    }

    #[test]
    fn test_telemetry_config_production() {
        let config = TelemetryConfig::production();
        assert!(config.json_output);
        assert_eq!(config.trace_sample_rate, 0.1);
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn test_log_entry_fields() {
        let entry = LogEntry::new(LogLevel::Info, "test", "test message")
            .with_field("key", "value")
            .with_field("number", 42);

        assert_eq!(entry.fields.len(), 2);
        assert_eq!(entry.fields["key"], serde_json::json!("value"));
        assert_eq!(entry.fields["number"], serde_json::json!(42));
    }

    #[test]
    fn test_log_entry_to_json() {
        let entry = LogEntry::new(LogLevel::Info, "test", "test message");
        let json = entry.to_json();
        assert!(json.contains("INFO"));
        assert!(json.contains("test message"));
    }

    #[test]
    fn test_log_entry_to_text() {
        let entry = LogEntry::new(LogLevel::Info, "test", "test message");
        let text = entry.to_text();
        assert!(text.contains("INFO"));
        assert!(text.contains("[test]"));
        assert!(text.contains("test message"));
    }

    #[test]
    fn test_telemetry_collector_levels() {
        let config = TelemetryConfig {
            log_level: "warn".to_string(),
            ..Default::default()
        };
        let collector = TelemetryCollector::new(config);

        assert!(!collector.is_enabled(LogLevel::Debug));
        assert!(!collector.is_enabled(LogLevel::Info));
        assert!(collector.is_enabled(LogLevel::Warn));
        assert!(collector.is_enabled(LogLevel::Error));
    }

    #[test]
    fn test_telemetry_stats() {
        let collector = TelemetryCollector::default();
        collector.record_span_start(true);
        collector.record_span_start(false);
        collector.record_span_end();

        let stats = collector.stats();
        assert_eq!(stats.spans_started, 2);
        assert_eq!(stats.spans_sampled, 1);
        assert_eq!(stats.spans_ended, 1);
        assert_eq!(stats.sample_rate(), 0.5);
    }

    #[test]
    fn test_span_basic() {
        let span = Span::new("test_operation");
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(span.elapsed_ms() >= 10.0);
    }

    #[test]
    fn test_span_child() {
        let parent = Span::new("parent");
        let child = parent.child("child");
        assert!(child.parent_id.is_some());
        assert_eq!(child.sampled, parent.sampled);
    }

    #[test]
    fn test_span_attributes() {
        let mut span = Span::new("test");
        span.set_attribute("key", "value");
        assert_eq!(span.attributes.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_catalog_registration() {
        let catalog = TelemetryCatalog::new();

        catalog.register_metric(CatalogEntry {
            name: "test_counter".to_string(),
            metric_type: "counter".to_string(),
            description: "A test counter".to_string(),
            unit: None,
            labels: vec![],
        });

        assert!(catalog.validate_metric("test_counter"));
        assert!(!catalog.validate_metric("nonexistent"));
    }

    #[test]
    fn test_catalog_export_import() {
        let catalog = TelemetryCatalog::new();
        catalog.register_metric(CatalogEntry {
            name: "test".to_string(),
            metric_type: "gauge".to_string(),
            description: "Test".to_string(),
            unit: Some("bytes".to_string()),
            labels: vec!["label1".to_string()],
        });

        let json = catalog.export_json();

        let catalog2 = TelemetryCatalog::new();
        catalog2.load_json(&json).unwrap();

        assert!(catalog2.validate_metric("test"));
    }

    #[test]
    fn test_log_count_snapshot() {
        let counts = LogCounts::new();
        counts.increment(LogLevel::Info);
        counts.increment(LogLevel::Info);
        counts.increment(LogLevel::Error);

        let snapshot = counts.snapshot();
        assert_eq!(snapshot.info, 2);
        assert_eq!(snapshot.error, 1);
        assert_eq!(snapshot.total(), 3);
    }
}

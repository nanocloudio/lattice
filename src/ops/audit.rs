//! Security audit logging.
//!
//! Per §14, audit logging for security-sensitive operations.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Audit event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    // Authentication events
    /// User authentication attempt.
    AuthAttempt,
    /// User authentication success.
    AuthSuccess,
    /// User authentication failure.
    AuthFailure,
    /// Token issued.
    TokenIssued,
    /// Token revoked.
    TokenRevoked,
    /// Token expired.
    TokenExpired,

    // Authorization events
    /// Permission check.
    PermissionCheck,
    /// Permission denied.
    PermissionDenied,
    /// Role assigned.
    RoleAssigned,
    /// Role revoked.
    RoleRevoked,

    // Administrative events
    /// User created.
    UserCreated,
    /// User deleted.
    UserDeleted,
    /// User modified.
    UserModified,
    /// Tenant created.
    TenantCreated,
    /// Tenant deleted.
    TenantDeleted,
    /// Tenant modified.
    TenantModified,
    /// Configuration changed.
    ConfigChanged,

    // Break-glass events
    /// Break-glass requested.
    BreakGlassRequested,
    /// Break-glass approved.
    BreakGlassApproved,
    /// Break-glass activated.
    BreakGlassActivated,
    /// Break-glass revoked.
    BreakGlassRevoked,

    // Certificate events
    /// Certificate loaded.
    CertificateLoaded,
    /// Certificate expired.
    CertificateExpired,
    /// Certificate expiring soon.
    CertificateExpiringSoon,
    /// Certificate reloaded.
    CertificateReloaded,

    // Data events
    /// Data exported.
    DataExported,
    /// Data deleted.
    DataDeleted,
    /// Snapshot created.
    SnapshotCreated,
    /// Snapshot restored.
    SnapshotRestored,
}

impl std::fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AuthAttempt => write!(f, "auth.attempt"),
            Self::AuthSuccess => write!(f, "auth.success"),
            Self::AuthFailure => write!(f, "auth.failure"),
            Self::TokenIssued => write!(f, "token.issued"),
            Self::TokenRevoked => write!(f, "token.revoked"),
            Self::TokenExpired => write!(f, "token.expired"),
            Self::PermissionCheck => write!(f, "authz.check"),
            Self::PermissionDenied => write!(f, "authz.denied"),
            Self::RoleAssigned => write!(f, "role.assigned"),
            Self::RoleRevoked => write!(f, "role.revoked"),
            Self::UserCreated => write!(f, "user.created"),
            Self::UserDeleted => write!(f, "user.deleted"),
            Self::UserModified => write!(f, "user.modified"),
            Self::TenantCreated => write!(f, "tenant.created"),
            Self::TenantDeleted => write!(f, "tenant.deleted"),
            Self::TenantModified => write!(f, "tenant.modified"),
            Self::ConfigChanged => write!(f, "config.changed"),
            Self::BreakGlassRequested => write!(f, "breakglass.requested"),
            Self::BreakGlassApproved => write!(f, "breakglass.approved"),
            Self::BreakGlassActivated => write!(f, "breakglass.activated"),
            Self::BreakGlassRevoked => write!(f, "breakglass.revoked"),
            Self::CertificateLoaded => write!(f, "cert.loaded"),
            Self::CertificateExpired => write!(f, "cert.expired"),
            Self::CertificateExpiringSoon => write!(f, "cert.expiring"),
            Self::CertificateReloaded => write!(f, "cert.reloaded"),
            Self::DataExported => write!(f, "data.exported"),
            Self::DataDeleted => write!(f, "data.deleted"),
            Self::SnapshotCreated => write!(f, "snapshot.created"),
            Self::SnapshotRestored => write!(f, "snapshot.restored"),
        }
    }
}

/// Audit event severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuditSeverity {
    /// Informational event.
    Info,
    /// Warning event.
    Warning,
    /// Error event.
    Error,
    /// Critical event requiring immediate attention.
    Critical,
}

impl std::fmt::Display for AuditSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Error => write!(f, "error"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Event ID.
    pub event_id: u64,
    /// Event type.
    pub event_type: AuditEventType,
    /// Severity.
    pub severity: AuditSeverity,
    /// Timestamp (RFC3339).
    pub timestamp: String,
    /// Tenant ID.
    pub tenant_id: Option<String>,
    /// Actor (user, service, etc.).
    pub actor: String,
    /// Target resource.
    pub target: Option<String>,
    /// Action performed.
    pub action: String,
    /// Result (success/failure).
    pub result: AuditResult,
    /// Additional details.
    pub details: Option<String>,
    /// Source IP address.
    pub source_ip: Option<String>,
    /// Request ID for correlation.
    pub request_id: Option<String>,
}

/// Audit result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditResult {
    /// Operation succeeded.
    Success,
    /// Operation failed.
    Failure,
    /// Operation denied.
    Denied,
    /// Operation pending.
    Pending,
}

impl std::fmt::Display for AuditResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Failure => write!(f, "failure"),
            Self::Denied => write!(f, "denied"),
            Self::Pending => write!(f, "pending"),
        }
    }
}

impl AuditEvent {
    /// Create a new audit event.
    pub fn new(
        event_type: AuditEventType,
        actor: impl Into<String>,
        action: impl Into<String>,
    ) -> Self {
        Self {
            event_id: 0,
            event_type,
            severity: Self::default_severity(event_type),
            timestamp: chrono::Utc::now().to_rfc3339(),
            tenant_id: None,
            actor: actor.into(),
            target: None,
            action: action.into(),
            result: AuditResult::Success,
            details: None,
            source_ip: None,
            request_id: None,
        }
    }

    /// Get default severity for event type.
    fn default_severity(event_type: AuditEventType) -> AuditSeverity {
        match event_type {
            AuditEventType::AuthFailure
            | AuditEventType::PermissionDenied
            | AuditEventType::CertificateExpired => AuditSeverity::Warning,
            AuditEventType::BreakGlassActivated
            | AuditEventType::UserDeleted
            | AuditEventType::TenantDeleted
            | AuditEventType::DataDeleted => AuditSeverity::Critical,
            _ => AuditSeverity::Info,
        }
    }

    /// Set tenant ID.
    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set target resource.
    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }

    /// Set result.
    pub fn with_result(mut self, result: AuditResult) -> Self {
        self.result = result;
        // Adjust severity for failures
        if (result == AuditResult::Failure || result == AuditResult::Denied)
            && self.severity < AuditSeverity::Warning
        {
            self.severity = AuditSeverity::Warning;
        }
        self
    }

    /// Set severity.
    pub fn with_severity(mut self, severity: AuditSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Set details.
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Set source IP.
    pub fn with_source_ip(mut self, ip: impl Into<String>) -> Self {
        self.source_ip = Some(ip.into());
        self
    }

    /// Set request ID.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Format as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Audit log configuration.
#[derive(Debug, Clone)]
pub struct AuditLogConfig {
    /// Enable audit logging.
    pub enabled: bool,
    /// Maximum events in memory buffer.
    pub buffer_size: usize,
    /// Minimum severity to log.
    pub min_severity: AuditSeverity,
    /// Include sensitive details.
    pub include_details: bool,
    /// Retention period in hours.
    pub retention_hours: u32,
}

impl Default for AuditLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            buffer_size: 10_000,
            min_severity: AuditSeverity::Info,
            include_details: true,
            retention_hours: 168, // 7 days
        }
    }
}

/// Audit log statistics.
#[derive(Debug, Clone)]
pub struct AuditLogStats {
    /// Total events logged.
    pub total_events: u64,
    /// Events by severity.
    pub events_by_severity: SeverityCounts,
    /// Events by type.
    pub events_by_type: TypeCounts,
    /// Buffer utilization.
    pub buffer_utilization: f64,
    /// Dropped events.
    pub dropped_events: u64,
}

/// Event counts by severity.
#[derive(Debug, Clone, Default)]
pub struct SeverityCounts {
    /// Info events.
    pub info: u64,
    /// Warning events.
    pub warning: u64,
    /// Error events.
    pub error: u64,
    /// Critical events.
    pub critical: u64,
}

/// Event counts by type category.
#[derive(Debug, Clone, Default)]
pub struct TypeCounts {
    /// Authentication events.
    pub auth: u64,
    /// Authorization events.
    pub authz: u64,
    /// Administrative events.
    pub admin: u64,
    /// Break-glass events.
    pub break_glass: u64,
    /// Certificate events.
    pub cert: u64,
    /// Data events.
    pub data: u64,
}

impl TypeCounts {
    fn increment(&mut self, event_type: AuditEventType) {
        match event_type {
            AuditEventType::AuthAttempt
            | AuditEventType::AuthSuccess
            | AuditEventType::AuthFailure
            | AuditEventType::TokenIssued
            | AuditEventType::TokenRevoked
            | AuditEventType::TokenExpired => self.auth += 1,
            AuditEventType::PermissionCheck
            | AuditEventType::PermissionDenied
            | AuditEventType::RoleAssigned
            | AuditEventType::RoleRevoked => self.authz += 1,
            AuditEventType::UserCreated
            | AuditEventType::UserDeleted
            | AuditEventType::UserModified
            | AuditEventType::TenantCreated
            | AuditEventType::TenantDeleted
            | AuditEventType::TenantModified
            | AuditEventType::ConfigChanged => self.admin += 1,
            AuditEventType::BreakGlassRequested
            | AuditEventType::BreakGlassApproved
            | AuditEventType::BreakGlassActivated
            | AuditEventType::BreakGlassRevoked => self.break_glass += 1,
            AuditEventType::CertificateLoaded
            | AuditEventType::CertificateExpired
            | AuditEventType::CertificateExpiringSoon
            | AuditEventType::CertificateReloaded => self.cert += 1,
            AuditEventType::DataExported
            | AuditEventType::DataDeleted
            | AuditEventType::SnapshotCreated
            | AuditEventType::SnapshotRestored => self.data += 1,
        }
    }
}

/// Audit log manager.
#[derive(Debug)]
pub struct AuditLog {
    /// Configuration.
    config: AuditLogConfig,
    /// Event buffer.
    buffer: RwLock<VecDeque<AuditEvent>>,
    /// Next event ID.
    next_event_id: AtomicU64,
    /// Total events logged.
    total_events: AtomicU64,
    /// Dropped events.
    dropped_events: AtomicU64,
    /// Severity counts.
    severity_counts: RwLock<SeverityCounts>,
    /// Type counts.
    type_counts: RwLock<TypeCounts>,
}

impl AuditLog {
    /// Create a new audit log.
    pub fn new(config: AuditLogConfig) -> Self {
        Self {
            config,
            buffer: RwLock::new(VecDeque::new()),
            next_event_id: AtomicU64::new(1),
            total_events: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            severity_counts: RwLock::new(SeverityCounts::default()),
            type_counts: RwLock::new(TypeCounts::default()),
        }
    }

    /// Log an audit event.
    pub fn log(&self, mut event: AuditEvent) {
        if !self.config.enabled {
            return;
        }

        if event.severity < self.config.min_severity {
            return;
        }

        // Assign event ID
        event.event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);

        // Clear details if not included
        if !self.config.include_details {
            event.details = None;
        }

        // Update statistics
        self.total_events.fetch_add(1, Ordering::Relaxed);
        {
            let mut counts = self.severity_counts.write().unwrap();
            match event.severity {
                AuditSeverity::Info => counts.info += 1,
                AuditSeverity::Warning => counts.warning += 1,
                AuditSeverity::Error => counts.error += 1,
                AuditSeverity::Critical => counts.critical += 1,
            }
        }
        {
            let mut counts = self.type_counts.write().unwrap();
            counts.increment(event.event_type);
        }

        // Add to buffer
        let mut buffer = self.buffer.write().unwrap();
        if buffer.len() >= self.config.buffer_size {
            buffer.pop_front();
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
        buffer.push_back(event);
    }

    /// Log an authentication event.
    pub fn log_auth(&self, actor: &str, success: bool, details: Option<&str>) {
        let event_type = if success {
            AuditEventType::AuthSuccess
        } else {
            AuditEventType::AuthFailure
        };
        let mut event =
            AuditEvent::new(event_type, actor, "authenticate").with_result(if success {
                AuditResult::Success
            } else {
                AuditResult::Failure
            });
        if let Some(d) = details {
            event = event.with_details(d);
        }
        self.log(event);
    }

    /// Log a permission check.
    pub fn log_permission_check(&self, actor: &str, resource: &str, action: &str, allowed: bool) {
        let event_type = if allowed {
            AuditEventType::PermissionCheck
        } else {
            AuditEventType::PermissionDenied
        };
        let event = AuditEvent::new(event_type, actor, action)
            .with_target(resource)
            .with_result(if allowed {
                AuditResult::Success
            } else {
                AuditResult::Denied
            });
        self.log(event);
    }

    /// Log a break-glass event.
    pub fn log_break_glass(
        &self,
        event_type: AuditEventType,
        actor: &str,
        tenant_id: &str,
        details: &str,
    ) {
        let event = AuditEvent::new(event_type, actor, "break_glass")
            .with_tenant(tenant_id)
            .with_details(details);
        self.log(event);
    }

    /// Get recent events.
    pub fn get_recent(&self, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().unwrap();
        buffer.iter().rev().take(count).cloned().collect()
    }

    /// Get events by severity.
    pub fn get_by_severity(&self, severity: AuditSeverity, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().unwrap();
        buffer
            .iter()
            .rev()
            .filter(|e| e.severity == severity)
            .take(count)
            .cloned()
            .collect()
    }

    /// Get events by type.
    pub fn get_by_type(&self, event_type: AuditEventType, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().unwrap();
        buffer
            .iter()
            .rev()
            .filter(|e| e.event_type == event_type)
            .take(count)
            .cloned()
            .collect()
    }

    /// Get events by tenant.
    pub fn get_by_tenant(&self, tenant_id: &str, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().unwrap();
        buffer
            .iter()
            .rev()
            .filter(|e| e.tenant_id.as_deref() == Some(tenant_id))
            .take(count)
            .cloned()
            .collect()
    }

    /// Get events by actor.
    pub fn get_by_actor(&self, actor: &str, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().unwrap();
        buffer
            .iter()
            .rev()
            .filter(|e| e.actor == actor)
            .take(count)
            .cloned()
            .collect()
    }

    /// Clear the buffer.
    pub fn clear(&self) {
        let mut buffer = self.buffer.write().unwrap();
        buffer.clear();
    }

    /// Get statistics.
    pub fn stats(&self) -> AuditLogStats {
        let buffer = self.buffer.read().unwrap();
        let buffer_utilization = if self.config.buffer_size > 0 {
            buffer.len() as f64 / self.config.buffer_size as f64
        } else {
            0.0
        };

        AuditLogStats {
            total_events: self.total_events.load(Ordering::Relaxed),
            events_by_severity: self.severity_counts.read().unwrap().clone(),
            events_by_type: self.type_counts.read().unwrap().clone(),
            buffer_utilization,
            dropped_events: self.dropped_events.load(Ordering::Relaxed),
        }
    }

    /// Get buffer length.
    pub fn len(&self) -> usize {
        self.buffer.read().unwrap().len()
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.read().unwrap().is_empty()
    }

    /// Export events as JSON array.
    pub fn export_json(&self) -> String {
        let buffer = self.buffer.read().unwrap();
        let events: Vec<&AuditEvent> = buffer.iter().collect();
        serde_json::to_string_pretty(&events).unwrap_or_default()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(AuditLogConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_new() {
        let event = AuditEvent::new(AuditEventType::AuthSuccess, "user1", "login");
        assert_eq!(event.actor, "user1");
        assert_eq!(event.action, "login");
        assert_eq!(event.result, AuditResult::Success);
    }

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::new(AuditEventType::PermissionDenied, "user1", "read")
            .with_tenant("tenant1")
            .with_target("/data/secret")
            .with_result(AuditResult::Denied)
            .with_source_ip("192.168.1.1");

        assert_eq!(event.tenant_id, Some("tenant1".to_string()));
        assert_eq!(event.target, Some("/data/secret".to_string()));
        assert_eq!(event.result, AuditResult::Denied);
        assert_eq!(event.source_ip, Some("192.168.1.1".to_string()));
    }

    #[test]
    fn test_audit_event_severity_adjustment() {
        let event = AuditEvent::new(AuditEventType::AuthAttempt, "user1", "login")
            .with_result(AuditResult::Failure);
        assert_eq!(event.severity, AuditSeverity::Warning);
    }

    #[test]
    fn test_audit_log_basic() {
        let log = AuditLog::new(AuditLogConfig::default());

        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user1",
            "login",
        ));
        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user2",
            "login",
        ));

        assert_eq!(log.len(), 2);
        let stats = log.stats();
        assert_eq!(stats.total_events, 2);
    }

    #[test]
    fn test_audit_log_buffer_overflow() {
        let config = AuditLogConfig {
            buffer_size: 2,
            ..Default::default()
        };
        let log = AuditLog::new(config);

        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user1",
            "login",
        ));
        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user2",
            "login",
        ));
        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user3",
            "login",
        ));

        assert_eq!(log.len(), 2);
        assert_eq!(log.stats().dropped_events, 1);
    }

    #[test]
    fn test_audit_log_auth() {
        let log = AuditLog::default();

        log.log_auth("user1", true, None);
        log.log_auth("user2", false, Some("invalid password"));

        let events = log.get_recent(10);
        assert_eq!(events.len(), 2);

        let failures = log.get_by_type(AuditEventType::AuthFailure, 10);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].actor, "user2");
    }

    #[test]
    fn test_audit_log_permission_check() {
        let log = AuditLog::default();

        log.log_permission_check("user1", "/data/public", "read", true);
        log.log_permission_check("user1", "/data/secret", "write", false);

        let denied = log.get_by_type(AuditEventType::PermissionDenied, 10);
        assert_eq!(denied.len(), 1);
        assert_eq!(denied[0].target, Some("/data/secret".to_string()));
    }

    #[test]
    fn test_audit_log_by_tenant() {
        let log = AuditLog::default();

        log.log(
            AuditEvent::new(AuditEventType::UserCreated, "admin", "create").with_tenant("tenant1"),
        );
        log.log(
            AuditEvent::new(AuditEventType::UserCreated, "admin", "create").with_tenant("tenant2"),
        );
        log.log(
            AuditEvent::new(AuditEventType::UserCreated, "admin", "create").with_tenant("tenant1"),
        );

        let tenant1_events = log.get_by_tenant("tenant1", 10);
        assert_eq!(tenant1_events.len(), 2);
    }

    #[test]
    fn test_audit_log_by_actor() {
        let log = AuditLog::default();

        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user1",
            "login",
        ));
        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user2",
            "login",
        ));
        log.log(AuditEvent::new(
            AuditEventType::ConfigChanged,
            "user1",
            "change",
        ));

        let user1_events = log.get_by_actor("user1", 10);
        assert_eq!(user1_events.len(), 2);
    }

    #[test]
    fn test_audit_log_severity_filter() {
        let config = AuditLogConfig {
            min_severity: AuditSeverity::Warning,
            ..Default::default()
        };
        let log = AuditLog::new(config);

        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user1",
            "login",
        )); // Info - filtered
        log.log(AuditEvent::new(
            AuditEventType::AuthFailure,
            "user1",
            "login",
        )); // Warning

        assert_eq!(log.len(), 1);
    }

    #[test]
    fn test_audit_event_type_display() {
        assert_eq!(format!("{}", AuditEventType::AuthSuccess), "auth.success");
        assert_eq!(
            format!("{}", AuditEventType::PermissionDenied),
            "authz.denied"
        );
        assert_eq!(
            format!("{}", AuditEventType::BreakGlassActivated),
            "breakglass.activated"
        );
    }

    #[test]
    fn test_audit_log_stats() {
        let log = AuditLog::default();

        log.log(AuditEvent::new(
            AuditEventType::AuthSuccess,
            "user1",
            "login",
        ));
        log.log(
            AuditEvent::new(AuditEventType::AuthFailure, "user2", "login")
                .with_severity(AuditSeverity::Warning),
        );
        log.log(
            AuditEvent::new(AuditEventType::BreakGlassActivated, "admin", "activate")
                .with_severity(AuditSeverity::Critical),
        );

        let stats = log.stats();
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.events_by_severity.info, 1);
        assert_eq!(stats.events_by_severity.warning, 1);
        assert_eq!(stats.events_by_severity.critical, 1);
        assert_eq!(stats.events_by_type.auth, 2);
        assert_eq!(stats.events_by_type.break_glass, 1);
    }

    #[test]
    fn test_type_counts_increment() {
        let mut counts = TypeCounts::default();
        counts.increment(AuditEventType::AuthSuccess);
        counts.increment(AuditEventType::AuthFailure);
        counts.increment(AuditEventType::PermissionDenied);
        counts.increment(AuditEventType::UserCreated);

        assert_eq!(counts.auth, 2);
        assert_eq!(counts.authz, 1);
        assert_eq!(counts.admin, 1);
    }
}

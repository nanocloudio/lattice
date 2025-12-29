//! etcd lease operations.
//!
//! Implements Lease per §10.1.7.
//! - Grant/revoke/keepalive are WAL-backed mutations
//! - Expiry uses deterministic ticks
//! - Keepalive responses reflect committed lease state

use super::kv::ResponseHeader;
use serde::{Deserialize, Serialize};

/// Lease grant request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseGrantRequest {
    /// Requested TTL in seconds.
    pub ttl: i64,
    /// Requested lease ID (0 for auto-assign).
    pub id: i64,
}

/// Lease grant response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseGrantResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Assigned lease ID.
    pub id: i64,
    /// Granted TTL in seconds.
    pub ttl: i64,
    /// Error message (if any).
    pub error: String,
}

/// Lease revoke request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRevokeRequest {
    /// Lease ID to revoke.
    pub id: i64,
}

/// Lease revoke response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRevokeResponse {
    /// Response header.
    pub header: ResponseHeader,
}

/// Lease keepalive request (stream message).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseKeepAliveRequest {
    /// Lease ID to keep alive.
    pub id: i64,
}

/// Lease keepalive response (stream message).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseKeepAliveResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Lease ID.
    pub id: i64,
    /// Remaining TTL in seconds.
    pub ttl: i64,
}

/// Lease time-to-live request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseTimeToLiveRequest {
    /// Lease ID to query.
    pub id: i64,
    /// Include attached keys in response.
    pub keys: bool,
}

/// Lease time-to-live response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseTimeToLiveResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Lease ID.
    pub id: i64,
    /// Remaining TTL in seconds.
    pub ttl: i64,
    /// Granted TTL in seconds.
    pub granted_ttl: i64,
    /// Attached keys (if requested).
    pub keys: Vec<Vec<u8>>,
}

/// Lease leases request (list all leases).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseLeasesRequest {}

/// Lease status (for listing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseStatus {
    /// Lease ID.
    pub id: i64,
}

/// Lease leases response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseLeasesResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// List of active leases.
    pub leases: Vec<LeaseStatus>,
}

// ============================================================================
// Lease Service Implementation (Tasks 123-132)
// ============================================================================

use crate::core::error::{LatticeError, LatticeResult};
use crate::core::time::Tick;
use crate::kpg::lease::LeaseManager;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::RwLock;

/// Lease service configuration.
#[derive(Debug, Clone)]
pub struct LeaseServiceConfig {
    /// Default TTL in seconds if not specified.
    pub default_ttl_seconds: i64,
    /// Minimum TTL in seconds.
    pub min_ttl_seconds: i64,
    /// Maximum TTL in seconds.
    pub max_ttl_seconds: i64,
    /// Whether to clamp TTL to bounds (vs reject).
    pub clamp_ttl: bool,
}

impl Default for LeaseServiceConfig {
    fn default() -> Self {
        Self {
            default_ttl_seconds: 60,
            min_ttl_seconds: 5,
            max_ttl_seconds: 7200, // 2 hours
            clamp_ttl: true,
        }
    }
}

/// Lease service for the etcd adapter.
///
/// Wraps the KPG LeaseManager and provides etcd-compatible
/// lease operations with TTL bounds enforcement.
pub struct LeaseService {
    /// Configuration.
    config: LeaseServiceConfig,
    /// Underlying lease manager.
    manager: RwLock<LeaseManager>,
    /// Header information.
    cluster_id: u64,
    member_id: u64,
    /// Current revision (updated externally).
    current_revision: AtomicI64,
    /// Current raft term.
    raft_term: AtomicU64,
    /// Statistics.
    stats: LeaseServiceStats,
}

/// Lease service statistics.
struct LeaseServiceStats {
    grants: AtomicU64,
    revokes: AtomicU64,
    keepalives: AtomicU64,
    expirations: AtomicU64,
}

impl LeaseServiceStats {
    fn new() -> Self {
        Self {
            grants: AtomicU64::new(0),
            revokes: AtomicU64::new(0),
            keepalives: AtomicU64::new(0),
            expirations: AtomicU64::new(0),
        }
    }
}

impl LeaseService {
    /// Create a new lease service.
    pub fn new(config: LeaseServiceConfig, cluster_id: u64, member_id: u64) -> Self {
        Self {
            config,
            manager: RwLock::new(LeaseManager::new()),
            cluster_id,
            member_id,
            current_revision: AtomicI64::new(0),
            raft_term: AtomicU64::new(0),
            stats: LeaseServiceStats::new(),
        }
    }

    /// Update the current revision.
    pub fn set_revision(&self, revision: i64) {
        self.current_revision.store(revision, Ordering::Release);
    }

    /// Update the raft term.
    pub fn set_raft_term(&self, term: u64) {
        self.raft_term.store(term, Ordering::Release);
    }

    /// Create a response header.
    fn make_header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: self.current_revision.load(Ordering::Acquire),
            raft_term: self.raft_term.load(Ordering::Acquire),
        }
    }

    /// Enforce TTL bounds.
    fn enforce_ttl(&self, requested_ttl: i64) -> LatticeResult<i64> {
        if requested_ttl <= 0 {
            return Ok(self.config.default_ttl_seconds);
        }

        if self.config.clamp_ttl {
            // Clamp to bounds
            let clamped = requested_ttl
                .max(self.config.min_ttl_seconds)
                .min(self.config.max_ttl_seconds);
            Ok(clamped)
        } else {
            // Reject out-of-bounds
            if requested_ttl < self.config.min_ttl_seconds {
                return Err(LatticeError::InvalidRequest {
                    message: format!(
                        "TTL {} is below minimum {}",
                        requested_ttl, self.config.min_ttl_seconds
                    ),
                });
            }
            if requested_ttl > self.config.max_ttl_seconds {
                return Err(LatticeError::InvalidRequest {
                    message: format!(
                        "TTL {} exceeds maximum {}",
                        requested_ttl, self.config.max_ttl_seconds
                    ),
                });
            }
            Ok(requested_ttl)
        }
    }

    /// Grant a new lease.
    pub fn grant(
        &self,
        req: &LeaseGrantRequest,
        current_tick: Tick,
    ) -> LatticeResult<LeaseGrantResponse> {
        let ttl_seconds = self.enforce_ttl(req.ttl)?;
        let ttl_ms = (ttl_seconds * 1000) as u64;

        let mut manager = self.manager.write().unwrap();

        // Check if lease ID already exists
        if req.id != 0 && manager.exists(req.id) {
            return Err(LatticeError::InvalidRequest {
                message: format!("lease {} already exists", req.id),
            });
        }

        let lease_id = manager.grant(req.id, ttl_ms, current_tick);
        self.stats.grants.fetch_add(1, Ordering::Relaxed);

        Ok(LeaseGrantResponse {
            header: self.make_header(),
            id: lease_id,
            ttl: ttl_seconds,
            error: String::new(),
        })
    }

    /// Revoke a lease.
    ///
    /// Returns the keys that were attached to the lease for cascade deletion.
    pub fn revoke(
        &self,
        req: &LeaseRevokeRequest,
    ) -> LatticeResult<(LeaseRevokeResponse, Vec<Vec<u8>>)> {
        let mut manager = self.manager.write().unwrap();

        let lease = manager
            .revoke(req.id)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("lease {} not found", req.id),
            })?;

        self.stats.revokes.fetch_add(1, Ordering::Relaxed);

        // Return attached keys for cascade deletion
        let attached_keys: Vec<Vec<u8>> = lease.attached_keys.into_iter().collect();

        Ok((
            LeaseRevokeResponse {
                header: self.make_header(),
            },
            attached_keys,
        ))
    }

    /// Send a keepalive to extend a lease.
    pub fn keepalive(
        &self,
        req: &LeaseKeepAliveRequest,
        current_tick: Tick,
    ) -> LatticeResult<LeaseKeepAliveResponse> {
        let mut manager = self.manager.write().unwrap();

        if !manager.keepalive(req.id, current_tick) {
            return Err(LatticeError::InvalidRequest {
                message: format!("lease {} not found", req.id),
            });
        }

        self.stats.keepalives.fetch_add(1, Ordering::Relaxed);

        // Get remaining TTL
        let remaining_ttl = manager
            .remaining_ttl(req.id, current_tick)
            .map(|ms| (ms / 1000) as i64)
            .unwrap_or(0);

        Ok(LeaseKeepAliveResponse {
            header: self.make_header(),
            id: req.id,
            ttl: remaining_ttl,
        })
    }

    /// Get time-to-live information for a lease.
    pub fn time_to_live(
        &self,
        req: &LeaseTimeToLiveRequest,
        current_tick: Tick,
    ) -> LatticeResult<LeaseTimeToLiveResponse> {
        let manager = self.manager.read().unwrap();

        let lease = manager
            .get(req.id)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("lease {} not found", req.id),
            })?;

        let remaining_ms = lease.remaining_ttl_at(current_tick);
        let remaining_ttl = (remaining_ms / 1000) as i64;
        let granted_ttl = (lease.ttl_ms / 1000) as i64;

        let keys = if req.keys {
            lease.attached_keys.iter().cloned().collect()
        } else {
            Vec::new()
        };

        Ok(LeaseTimeToLiveResponse {
            header: self.make_header(),
            id: req.id,
            ttl: remaining_ttl,
            granted_ttl,
            keys,
        })
    }

    /// List all active leases.
    pub fn leases(&self) -> LeaseLeasesResponse {
        let manager = self.manager.read().unwrap();

        let leases = manager
            .lease_ids()
            .into_iter()
            .map(|id| LeaseStatus { id })
            .collect();

        LeaseLeasesResponse {
            header: self.make_header(),
            leases,
        }
    }

    /// Process tick to expire leases.
    ///
    /// Returns expired lease IDs and their attached keys for cascade deletion.
    pub fn process_tick(&self, current_tick: Tick) -> Vec<LeaseExpiration> {
        let mut manager = self.manager.write().unwrap();

        let expired_ids = manager.collect_expired(current_tick);
        let mut expirations = Vec::new();

        for lease_id in expired_ids {
            if let Some(lease) = manager.revoke(lease_id) {
                self.stats.expirations.fetch_add(1, Ordering::Relaxed);
                expirations.push(LeaseExpiration {
                    lease_id,
                    attached_keys: lease.attached_keys.into_iter().collect(),
                });
            }
        }

        expirations
    }

    /// Attach a key to a lease.
    pub fn attach_key(&self, lease_id: i64, key: Vec<u8>) -> bool {
        let mut manager = self.manager.write().unwrap();
        manager.attach_key(lease_id, key)
    }

    /// Detach a key from a lease.
    pub fn detach_key(&self, lease_id: i64, key: &[u8]) -> bool {
        let mut manager = self.manager.write().unwrap();
        manager.detach_key(lease_id, key)
    }

    /// Check if a lease exists and is valid.
    pub fn lease_exists(&self, lease_id: i64) -> bool {
        let manager = self.manager.read().unwrap();
        manager.exists(lease_id)
    }

    /// Get lease metrics.
    pub fn metrics(&self) -> LeaseServiceMetrics {
        let manager = self.manager.read().unwrap();
        LeaseServiceMetrics {
            active_leases: manager.lease_count(),
            grants_total: self.stats.grants.load(Ordering::Relaxed),
            revokes_total: self.stats.revokes.load(Ordering::Relaxed),
            keepalives_total: self.stats.keepalives.load(Ordering::Relaxed),
            expirations_total: self.stats.expirations.load(Ordering::Relaxed),
        }
    }

    /// Get the underlying lease manager for snapshot serialization.
    pub fn snapshot_state(&self) -> HashMap<i64, LeaseSnapshotEntry> {
        let manager = self.manager.read().unwrap();
        let mut state = HashMap::new();

        for lease in manager.all_leases() {
            state.insert(
                lease.lease_id,
                LeaseSnapshotEntry {
                    lease_id: lease.lease_id,
                    ttl_ms: lease.ttl_ms,
                    granted_at: lease.granted_at,
                    keepalive_deadline: lease.keepalive_deadline,
                    attached_keys: lease.attached_keys.iter().cloned().collect(),
                    epoch: lease.epoch,
                },
            );
        }

        state
    }

    /// Restore lease state from snapshot.
    pub fn restore_snapshot(&self, state: HashMap<i64, LeaseSnapshotEntry>) {
        let mut manager = self.manager.write().unwrap();

        // Clear existing leases
        for id in manager.lease_ids() {
            manager.revoke(id);
        }

        // Restore from snapshot
        for (lease_id, entry) in state {
            // Grant with the original parameters
            manager.grant(lease_id, entry.ttl_ms, entry.granted_at);

            // Attach keys
            for key in entry.attached_keys {
                manager.attach_key(lease_id, key);
            }
        }
    }
}

/// Lease expiration event.
#[derive(Debug, Clone)]
pub struct LeaseExpiration {
    /// Expired lease ID.
    pub lease_id: i64,
    /// Keys that were attached to the lease.
    pub attached_keys: Vec<Vec<u8>>,
}

/// Lease service metrics.
#[derive(Debug, Clone)]
pub struct LeaseServiceMetrics {
    /// Number of active leases.
    pub active_leases: usize,
    /// Total grants.
    pub grants_total: u64,
    /// Total revokes.
    pub revokes_total: u64,
    /// Total keepalives.
    pub keepalives_total: u64,
    /// Total expirations.
    pub expirations_total: u64,
}

/// Lease snapshot entry for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseSnapshotEntry {
    /// Lease ID.
    pub lease_id: i64,
    /// TTL in milliseconds.
    pub ttl_ms: u64,
    /// Tick when granted.
    pub granted_at: Tick,
    /// Keepalive deadline tick.
    pub keepalive_deadline: Tick,
    /// Attached keys.
    pub attached_keys: Vec<Vec<u8>>,
    /// Epoch counter.
    pub epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tick(ms: u64) -> Tick {
        Tick::new(ms)
    }

    #[test]
    fn test_lease_grant() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let req = LeaseGrantRequest { ttl: 60, id: 0 };
        let resp = service.grant(&req, make_tick(0)).expect("should grant");

        assert!(resp.id > 0);
        assert_eq!(resp.ttl, 60);
        assert!(resp.error.is_empty());
    }

    #[test]
    fn test_lease_grant_with_id() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let req = LeaseGrantRequest { ttl: 60, id: 12345 };
        let resp = service.grant(&req, make_tick(0)).expect("should grant");

        assert_eq!(resp.id, 12345);
    }

    #[test]
    fn test_lease_grant_duplicate() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let req = LeaseGrantRequest { ttl: 60, id: 12345 };
        service.grant(&req, make_tick(0)).expect("should grant");

        // Try to grant again with same ID
        let result = service.grant(&req, make_tick(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_lease_ttl_clamp() {
        let config = LeaseServiceConfig {
            min_ttl_seconds: 10,
            max_ttl_seconds: 100,
            clamp_ttl: true,
            ..Default::default()
        };
        let service = LeaseService::new(config, 1, 1);

        // Below minimum - should clamp
        let req = LeaseGrantRequest { ttl: 1, id: 0 };
        let resp = service.grant(&req, make_tick(0)).expect("should grant");
        assert_eq!(resp.ttl, 10);

        // Above maximum - should clamp
        let req = LeaseGrantRequest { ttl: 1000, id: 0 };
        let resp = service.grant(&req, make_tick(0)).expect("should grant");
        assert_eq!(resp.ttl, 100);
    }

    #[test]
    fn test_lease_ttl_reject() {
        let config = LeaseServiceConfig {
            min_ttl_seconds: 10,
            max_ttl_seconds: 100,
            clamp_ttl: false,
            ..Default::default()
        };
        let service = LeaseService::new(config, 1, 1);

        // Below minimum - should reject
        let req = LeaseGrantRequest { ttl: 1, id: 0 };
        assert!(service.grant(&req, make_tick(0)).is_err());

        // Above maximum - should reject
        let req = LeaseGrantRequest { ttl: 1000, id: 0 };
        assert!(service.grant(&req, make_tick(0)).is_err());
    }

    #[test]
    fn test_lease_revoke() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let req = LeaseGrantRequest { ttl: 60, id: 0 };
        let grant_resp = service.grant(&req, make_tick(0)).unwrap();

        let revoke_req = LeaseRevokeRequest { id: grant_resp.id };
        let (_, keys) = service.revoke(&revoke_req).expect("should revoke");

        assert!(keys.is_empty());

        // Try to revoke again - should fail
        assert!(service.revoke(&revoke_req).is_err());
    }

    #[test]
    fn test_lease_keepalive() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let grant_req = LeaseGrantRequest { ttl: 60, id: 0 };
        let grant_resp = service.grant(&grant_req, make_tick(0)).unwrap();

        // Keepalive at 30 seconds
        let keepalive_req = LeaseKeepAliveRequest { id: grant_resp.id };
        let keepalive_resp = service
            .keepalive(&keepalive_req, make_tick(30_000))
            .expect("should keepalive");

        assert_eq!(keepalive_resp.id, grant_resp.id);
        assert_eq!(keepalive_resp.ttl, 60); // Should be full TTL after keepalive
    }

    #[test]
    fn test_lease_time_to_live() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let grant_req = LeaseGrantRequest { ttl: 60, id: 0 };
        let grant_resp = service.grant(&grant_req, make_tick(0)).unwrap();

        // Attach a key
        service.attach_key(grant_resp.id, b"key1".to_vec());

        // Query TTL at 30 seconds with keys
        let ttl_req = LeaseTimeToLiveRequest {
            id: grant_resp.id,
            keys: true,
        };
        let ttl_resp = service
            .time_to_live(&ttl_req, make_tick(30_000))
            .expect("should get ttl");

        assert_eq!(ttl_resp.id, grant_resp.id);
        assert_eq!(ttl_resp.ttl, 30); // 30 seconds remaining
        assert_eq!(ttl_resp.granted_ttl, 60);
        assert_eq!(ttl_resp.keys.len(), 1);
    }

    #[test]
    fn test_lease_leases() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        // Grant multiple leases
        for _ in 0..3 {
            let req = LeaseGrantRequest { ttl: 60, id: 0 };
            service.grant(&req, make_tick(0)).unwrap();
        }

        let resp = service.leases();
        assert_eq!(resp.leases.len(), 3);
    }

    #[test]
    fn test_lease_expiration() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        let grant_req = LeaseGrantRequest { ttl: 60, id: 0 };
        let grant_resp = service.grant(&grant_req, make_tick(0)).unwrap();

        // Attach keys
        service.attach_key(grant_resp.id, b"key1".to_vec());
        service.attach_key(grant_resp.id, b"key2".to_vec());

        // Process tick at 60+ seconds - should expire
        let expirations = service.process_tick(make_tick(61_000));

        assert_eq!(expirations.len(), 1);
        assert_eq!(expirations[0].lease_id, grant_resp.id);
        assert_eq!(expirations[0].attached_keys.len(), 2);
    }

    #[test]
    fn test_lease_metrics() {
        let service = LeaseService::new(LeaseServiceConfig::default(), 1, 1);

        // Grant
        let req = LeaseGrantRequest { ttl: 60, id: 0 };
        let resp = service.grant(&req, make_tick(0)).unwrap();

        // Keepalive
        let keepalive_req = LeaseKeepAliveRequest { id: resp.id };
        service.keepalive(&keepalive_req, make_tick(1000)).unwrap();

        // Revoke
        let revoke_req = LeaseRevokeRequest { id: resp.id };
        service.revoke(&revoke_req).unwrap();

        let metrics = service.metrics();
        assert_eq!(metrics.grants_total, 1);
        assert_eq!(metrics.keepalives_total, 1);
        assert_eq!(metrics.revokes_total, 1);
    }
}

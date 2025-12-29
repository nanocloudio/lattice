//! Lease management.
//!
//! Per §7.1, lease records track TTL and attached keys:
//! `LeaseID → LeaseState{ttl_ms, granted_at_ms, keepalive_deadline_ms, attached_keys[], epoch}`
//!
//! Lease operations are WAL-backed mutations per §10.1.7.

use crate::core::time::Tick;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A lease record in the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Unique lease identifier.
    pub lease_id: i64,

    /// TTL in milliseconds.
    pub ttl_ms: u64,

    /// Tick when the lease was granted.
    pub granted_at: Tick,

    /// Tick when the lease expires unless renewed.
    pub keepalive_deadline: Tick,

    /// Keys attached to this lease.
    pub attached_keys: HashSet<Vec<u8>>,

    /// Epoch counter, increments on revoke/recreate.
    pub epoch: u64,
}

impl LeaseRecord {
    /// Create a new lease.
    pub fn new(lease_id: i64, ttl_ms: u64, granted_at: Tick) -> Self {
        let keepalive_deadline = granted_at.add_ms(ttl_ms);
        Self {
            lease_id,
            ttl_ms,
            granted_at,
            keepalive_deadline,
            attached_keys: HashSet::new(),
            epoch: 1,
        }
    }

    /// Check if the lease has expired at the given tick.
    pub fn is_expired_at(&self, tick: Tick) -> bool {
        tick.is_at_or_after(self.keepalive_deadline)
    }

    /// Calculate remaining TTL at the given tick.
    pub fn remaining_ttl_at(&self, tick: Tick) -> u64 {
        tick.ms_until(self.keepalive_deadline)
    }

    /// Extend the keepalive deadline (renew the lease).
    pub fn keepalive(&mut self, current_tick: Tick) {
        self.keepalive_deadline = current_tick.add_ms(self.ttl_ms);
    }

    /// Attach a key to this lease.
    pub fn attach_key(&mut self, key: Vec<u8>) {
        self.attached_keys.insert(key);
    }

    /// Detach a key from this lease.
    pub fn detach_key(&mut self, key: &[u8]) -> bool {
        self.attached_keys.remove(key)
    }

    /// Get the number of attached keys.
    pub fn attached_key_count(&self) -> usize {
        self.attached_keys.len()
    }

    /// Check if this lease has any attached keys.
    pub fn has_attached_keys(&self) -> bool {
        !self.attached_keys.is_empty()
    }

    /// Get all attached keys.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.attached_keys.iter()
    }
}

/// Lease grant request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseGrantRequest {
    /// Requested TTL in seconds.
    pub ttl_seconds: i64,
    /// Client-provided lease ID (0 for auto-generation).
    pub id: i64,
}

/// Lease grant response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseGrantResponse {
    /// Assigned lease ID.
    pub id: i64,
    /// Granted TTL in seconds.
    pub ttl: i64,
}

/// Lease revoke request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRevokeRequest {
    /// Lease ID to revoke.
    pub id: i64,
}

/// Lease keepalive request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseKeepaliveRequest {
    /// Lease ID to keep alive.
    pub id: i64,
}

/// Lease keepalive response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseKeepaliveResponse {
    /// Lease ID.
    pub id: i64,
    /// Remaining TTL in seconds.
    pub ttl: i64,
}

/// Lease time-to-live query response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseTimeToLiveResponse {
    /// Lease ID.
    pub id: i64,
    /// Remaining TTL in seconds.
    pub ttl: i64,
    /// Granted TTL in seconds.
    pub granted_ttl: i64,
    /// Keys attached to the lease (if requested).
    pub keys: Vec<Vec<u8>>,
}

/// Lease manager for a KPG.
///
/// Manages lease lifecycle including grants, keepalives, revocations,
/// and expiration tracking per §7.1.
pub struct LeaseManager {
    /// Active leases indexed by lease ID.
    leases: std::collections::HashMap<i64, LeaseRecord>,

    /// Next lease ID for auto-generation.
    next_lease_id: i64,
}

impl LeaseManager {
    /// Create a new lease manager.
    pub fn new() -> Self {
        Self {
            leases: std::collections::HashMap::new(),
            next_lease_id: 1,
        }
    }

    /// Get the number of active leases.
    pub fn lease_count(&self) -> usize {
        self.leases.len()
    }

    /// Check if a lease exists.
    pub fn exists(&self, lease_id: i64) -> bool {
        self.leases.contains_key(&lease_id)
    }

    /// Get a lease by ID.
    pub fn get(&self, lease_id: i64) -> Option<&LeaseRecord> {
        self.leases.get(&lease_id)
    }

    /// Get a mutable lease by ID.
    pub fn get_mut(&mut self, lease_id: i64) -> Option<&mut LeaseRecord> {
        self.leases.get_mut(&lease_id)
    }

    /// Grant a new lease.
    ///
    /// If lease_id is 0, auto-generates a new ID.
    pub fn grant(&mut self, lease_id: i64, ttl_ms: u64, current_tick: Tick) -> i64 {
        let id = if lease_id == 0 {
            let id = self.next_lease_id;
            self.next_lease_id += 1;
            id
        } else {
            // Update next_lease_id if needed
            if lease_id >= self.next_lease_id {
                self.next_lease_id = lease_id + 1;
            }
            lease_id
        };

        let lease = LeaseRecord::new(id, ttl_ms, current_tick);
        self.leases.insert(id, lease);
        id
    }

    /// Revoke a lease.
    ///
    /// Returns the revoked lease if it existed.
    pub fn revoke(&mut self, lease_id: i64) -> Option<LeaseRecord> {
        self.leases.remove(&lease_id)
    }

    /// Send keepalive to extend a lease.
    ///
    /// Returns true if the lease was found and extended.
    pub fn keepalive(&mut self, lease_id: i64, current_tick: Tick) -> bool {
        if let Some(lease) = self.leases.get_mut(&lease_id) {
            lease.keepalive(current_tick);
            true
        } else {
            false
        }
    }

    /// Attach a key to a lease.
    pub fn attach_key(&mut self, lease_id: i64, key: Vec<u8>) -> bool {
        if let Some(lease) = self.leases.get_mut(&lease_id) {
            lease.attach_key(key);
            true
        } else {
            false
        }
    }

    /// Detach a key from a lease.
    pub fn detach_key(&mut self, lease_id: i64, key: &[u8]) -> bool {
        if let Some(lease) = self.leases.get_mut(&lease_id) {
            lease.detach_key(key)
        } else {
            false
        }
    }

    /// Collect expired leases at the given tick.
    ///
    /// Returns the IDs of expired leases (does not remove them).
    pub fn collect_expired(&self, current_tick: Tick) -> Vec<i64> {
        self.leases
            .iter()
            .filter(|(_, lease)| lease.is_expired_at(current_tick))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get remaining TTL for a lease.
    pub fn remaining_ttl(&self, lease_id: i64, current_tick: Tick) -> Option<u64> {
        self.leases
            .get(&lease_id)
            .map(|l| l.remaining_ttl_at(current_tick))
    }

    /// Get all lease IDs.
    pub fn lease_ids(&self) -> Vec<i64> {
        self.leases.keys().copied().collect()
    }

    /// Get all leases.
    pub fn all_leases(&self) -> impl Iterator<Item = &LeaseRecord> {
        self.leases.values()
    }

    /// Get keys attached to a lease.
    pub fn attached_keys(&self, lease_id: i64) -> Option<impl Iterator<Item = &Vec<u8>>> {
        self.leases.get(&lease_id).map(|l| l.keys())
    }

    /// Get statistics about leases.
    pub fn stats(&self, current_tick: Tick) -> LeaseStats {
        let mut total_attached_keys = 0;
        let mut expiring_soon = 0;
        let soon_threshold_ms = 10_000; // 10 seconds

        for lease in self.leases.values() {
            total_attached_keys += lease.attached_key_count();
            let remaining = lease.remaining_ttl_at(current_tick);
            if remaining <= soon_threshold_ms {
                expiring_soon += 1;
            }
        }

        LeaseStats {
            active_leases: self.leases.len(),
            total_attached_keys,
            expiring_soon,
        }
    }
}

impl Default for LeaseManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Lease statistics.
#[derive(Debug, Clone)]
pub struct LeaseStats {
    /// Number of active leases.
    pub active_leases: usize,
    /// Total keys attached across all leases.
    pub total_attached_keys: usize,
    /// Leases expiring within threshold.
    pub expiring_soon: usize,
}

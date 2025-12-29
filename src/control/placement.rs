//! KPG placement management.
//!
//! CP-Raft assigns KPGs to nodes and performs membership changes
//! via Clustor §9.9. This module tracks placement and handles
//! rebalance coordination.

use crate::control::routing::KpgId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Placement record for a KPG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementRecord {
    /// KPG identifier.
    pub kpg_id: KpgId,

    /// Current leader node (if known).
    pub leader: Option<String>,

    /// Voter replicas.
    pub voters: Vec<String>,

    /// Learner replicas (catching up).
    pub learners: Vec<String>,

    /// Placement epoch for this KPG.
    pub placement_epoch: u64,
}

impl PlacementRecord {
    /// Check if a node is a voter for this KPG.
    pub fn is_voter(&self, node_id: &str) -> bool {
        self.voters.iter().any(|v| v == node_id)
    }

    /// Check if a node is a learner for this KPG.
    pub fn is_learner(&self, node_id: &str) -> bool {
        self.learners.iter().any(|l| l == node_id)
    }

    /// Check if a node is involved in this KPG (voter or learner).
    pub fn involves_node(&self, node_id: &str) -> bool {
        self.is_voter(node_id) || self.is_learner(node_id)
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self, node_id: &str) -> bool {
        self.leader.as_ref().is_some_and(|l| l == node_id)
    }
}

/// Placement table for a tenant.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlacementTable {
    /// Placement records indexed by KPG partition.
    pub placements: HashMap<u32, PlacementRecord>,

    /// Tenant identifier.
    pub tenant_id: String,
}

impl PlacementTable {
    /// Create a new placement table for a tenant.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            placements: HashMap::new(),
            tenant_id: tenant_id.into(),
        }
    }

    /// Get placement for a specific partition.
    pub fn get(&self, partition: u32) -> Option<&PlacementRecord> {
        self.placements.get(&partition)
    }

    /// Update placement for a partition.
    pub fn update(&mut self, partition: u32, record: PlacementRecord) {
        self.placements.insert(partition, record);
    }

    /// Get all partitions this node is involved in.
    pub fn partitions_for_node(&self, node_id: &str) -> Vec<u32> {
        self.placements
            .iter()
            .filter(|(_, r)| r.involves_node(node_id))
            .map(|(p, _)| *p)
            .collect()
    }

    /// Get all partitions this node leads.
    pub fn partitions_led_by_node(&self, node_id: &str) -> Vec<u32> {
        self.placements
            .iter()
            .filter(|(_, r)| r.is_leader(node_id))
            .map(|(p, _)| *p)
            .collect()
    }
}

/// Rebalance phase for placement changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalancePhase {
    /// CP-Raft planning the rebalance.
    Planning,
    /// Learner catching up to leader.
    LearnerCatchUp,
    /// Routing epoch flip in progress.
    EpochFlip,
    /// Clients reconnecting after flip.
    ClientReconnect,
    /// Rebalance complete.
    Complete,
}

impl RebalancePhase {
    /// Check if this phase blocks writes.
    pub fn blocks_writes(&self) -> bool {
        matches!(self, Self::EpochFlip)
    }

    /// Check if this phase allows new connections.
    pub fn allows_new_connections(&self) -> bool {
        !matches!(self, Self::EpochFlip | Self::ClientReconnect)
    }
}

/// Membership change type per Clustor §9.9.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipChangeType {
    /// Add a new voter.
    AddVoter,
    /// Remove a voter.
    RemoveVoter,
    /// Add a learner (non-voting replica).
    AddLearner,
    /// Remove a learner.
    RemoveLearner,
    /// Promote a learner to voter.
    PromoteLearner,
}

/// A pending membership change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMembershipChange {
    /// The type of change.
    pub change_type: MembershipChangeType,
    /// Target node ID.
    pub node_id: String,
    /// KPG partition.
    pub partition: u32,
    /// Tenant ID.
    pub tenant_id: String,
    /// When the change was initiated (ms since epoch).
    pub initiated_at_ms: u64,
    /// Current phase of the change.
    pub phase: RebalancePhase,
}

impl PendingMembershipChange {
    /// Create a new pending change.
    pub fn new(
        change_type: MembershipChangeType,
        node_id: impl Into<String>,
        tenant_id: impl Into<String>,
        partition: u32,
    ) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            change_type,
            node_id: node_id.into(),
            tenant_id: tenant_id.into(),
            partition,
            initiated_at_ms: now_ms,
            phase: RebalancePhase::Planning,
        }
    }

    /// Advance to the next phase.
    pub fn advance_phase(&mut self) {
        self.phase = match self.phase {
            RebalancePhase::Planning => RebalancePhase::LearnerCatchUp,
            RebalancePhase::LearnerCatchUp => RebalancePhase::EpochFlip,
            RebalancePhase::EpochFlip => RebalancePhase::ClientReconnect,
            RebalancePhase::ClientReconnect => RebalancePhase::Complete,
            RebalancePhase::Complete => RebalancePhase::Complete,
        };
    }

    /// Check if the change is complete.
    pub fn is_complete(&self) -> bool {
        self.phase == RebalancePhase::Complete
    }
}

/// Placement manager that coordinates with CP-Raft for KPG placement.
///
/// This manager tracks current placement, pending changes, and
/// coordinates membership changes per Clustor §9.9.
pub struct PlacementManager {
    /// Current placement tables per tenant.
    placements: std::sync::RwLock<HashMap<String, PlacementTable>>,

    /// Pending membership changes.
    pending_changes: std::sync::RwLock<Vec<PendingMembershipChange>>,

    /// Local node ID.
    local_node_id: String,
}

impl PlacementManager {
    /// Create a new placement manager.
    pub fn new(local_node_id: impl Into<String>) -> Self {
        Self {
            placements: std::sync::RwLock::new(HashMap::new()),
            pending_changes: std::sync::RwLock::new(Vec::new()),
            local_node_id: local_node_id.into(),
        }
    }

    /// Get the local node ID.
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Get placement table for a tenant.
    pub fn get_placement(&self, tenant_id: &str) -> Option<PlacementTable> {
        self.placements.read().unwrap().get(tenant_id).cloned()
    }

    /// Update placement table for a tenant.
    pub fn update_placement(&self, table: PlacementTable) {
        self.placements
            .write()
            .unwrap()
            .insert(table.tenant_id.clone(), table);
    }

    /// Remove placement for a tenant.
    pub fn remove_placement(&self, tenant_id: &str) {
        self.placements.write().unwrap().remove(tenant_id);
    }

    /// Get placement record for a specific KPG.
    pub fn get_kpg_placement(&self, tenant_id: &str, partition: u32) -> Option<PlacementRecord> {
        self.placements
            .read()
            .unwrap()
            .get(tenant_id)
            .and_then(|t| t.get(partition).cloned())
    }

    /// Check if local node is leader for a KPG.
    pub fn is_local_leader(&self, tenant_id: &str, partition: u32) -> bool {
        self.get_kpg_placement(tenant_id, partition)
            .is_some_and(|r| r.is_leader(&self.local_node_id))
    }

    /// Check if local node is a voter for a KPG.
    pub fn is_local_voter(&self, tenant_id: &str, partition: u32) -> bool {
        self.get_kpg_placement(tenant_id, partition)
            .is_some_and(|r| r.is_voter(&self.local_node_id))
    }

    /// Get all KPGs where local node is leader.
    pub fn local_leader_kpgs(&self) -> Vec<(String, u32)> {
        let placements = self.placements.read().unwrap();
        let mut result = Vec::new();

        for (tenant_id, table) in placements.iter() {
            for partition in table.partitions_led_by_node(&self.local_node_id) {
                result.push((tenant_id.clone(), partition));
            }
        }

        result
    }

    /// Get all KPGs where local node participates.
    pub fn local_participating_kpgs(&self) -> Vec<(String, u32)> {
        let placements = self.placements.read().unwrap();
        let mut result = Vec::new();

        for (tenant_id, table) in placements.iter() {
            for partition in table.partitions_for_node(&self.local_node_id) {
                result.push((tenant_id.clone(), partition));
            }
        }

        result
    }

    /// Add a pending membership change.
    pub fn add_pending_change(&self, change: PendingMembershipChange) {
        self.pending_changes.write().unwrap().push(change);
    }

    /// Get pending changes for a tenant/partition.
    pub fn get_pending_changes(
        &self,
        tenant_id: &str,
        partition: u32,
    ) -> Vec<PendingMembershipChange> {
        self.pending_changes
            .read()
            .unwrap()
            .iter()
            .filter(|c| c.tenant_id == tenant_id && c.partition == partition)
            .cloned()
            .collect()
    }

    /// Remove completed changes.
    pub fn cleanup_completed_changes(&self) {
        self.pending_changes
            .write()
            .unwrap()
            .retain(|c| !c.is_complete());
    }

    /// Check if a partition is in a rebalance phase that blocks writes.
    pub fn is_write_blocked(&self, tenant_id: &str, partition: u32) -> bool {
        self.pending_changes.read().unwrap().iter().any(|c| {
            c.tenant_id == tenant_id && c.partition == partition && c.phase.blocks_writes()
        })
    }

    /// Get placement statistics.
    pub fn stats(&self) -> PlacementStats {
        let placements = self.placements.read().unwrap();
        let pending = self.pending_changes.read().unwrap();

        let mut total_kpgs = 0;
        let mut local_leader_count = 0;
        let mut local_voter_count = 0;

        for table in placements.values() {
            for record in table.placements.values() {
                total_kpgs += 1;
                if record.is_leader(&self.local_node_id) {
                    local_leader_count += 1;
                }
                if record.is_voter(&self.local_node_id) {
                    local_voter_count += 1;
                }
            }
        }

        PlacementStats {
            tenant_count: placements.len(),
            total_kpgs,
            local_leader_count,
            local_voter_count,
            pending_changes: pending.len(),
        }
    }
}

/// Placement statistics.
#[derive(Debug, Clone)]
pub struct PlacementStats {
    /// Number of tenants with placement.
    pub tenant_count: usize,
    /// Total number of KPGs tracked.
    pub total_kpgs: usize,
    /// KPGs where local node is leader.
    pub local_leader_count: usize,
    /// KPGs where local node is voter.
    pub local_voter_count: usize,
    /// Number of pending membership changes.
    pub pending_changes: usize,
}

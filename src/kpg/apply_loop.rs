//! WAL entry processing and apply loop.
//!
//! The apply loop processes WAL entries in order, maintaining determinism.
//! All state changes derive from WAL entries per the KVSOURCE invariant.
//! TTL evaluation occurs only during committed tick processing.

use serde::{Deserialize, Serialize};

/// WAL entry types for the KV state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    /// Put a key-value pair.
    Put(PutEntry),

    /// Delete a key or range.
    Delete(DeleteEntry),

    /// Transaction (compare-then-execute).
    Txn(TxnEntry),

    /// Lease grant.
    LeaseGrant(LeaseGrantEntry),

    /// Lease revoke.
    LeaseRevoke(LeaseRevokeEntry),

    /// Lease keepalive.
    LeaseKeepalive(LeaseKeepaliveEntry),

    /// Deterministic tick for TTL evaluation.
    Tick(TickEntry),

    /// Compaction marker.
    Compact(CompactEntry),

    /// Idempotency table eviction.
    IdemEvict(IdemEvictEntry),
}

/// Put entry for a single key-value mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lease_id: Option<i64>,
    pub prev_kv: bool,
    pub idempotency_key: Option<Vec<u8>>,
}

/// Delete entry for key or range deletion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteEntry {
    pub key: Vec<u8>,
    pub range_end: Option<Vec<u8>>,
    pub prev_kv: bool,
    pub idempotency_key: Option<Vec<u8>>,
}

/// Transaction entry with compare-then-execute semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnEntry {
    pub compares: Vec<Compare>,
    pub success_ops: Vec<TxnOp>,
    pub failure_ops: Vec<TxnOp>,
    pub idempotency_key: Option<Vec<u8>>,
}

/// Compare predicate for transactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Compare {
    pub key: Vec<u8>,
    pub target: CompareTarget,
    pub result: CompareResult,
}

/// Compare target type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompareTarget {
    /// Compare mod_revision.
    ModRevision(u64),
    /// Compare create_revision.
    CreateRevision(u64),
    /// Compare version.
    Version(u64),
    /// Compare value.
    Value(Vec<u8>),
    /// Compare lease.
    Lease(i64),
}

/// Compare result type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompareResult {
    Equal,
    NotEqual,
    Greater,
    Less,
}

/// Transaction operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnOp {
    Put(PutEntry),
    Delete(DeleteEntry),
    Range(RangeOp),
}

/// Range operation within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeOp {
    pub key: Vec<u8>,
    pub range_end: Option<Vec<u8>>,
    pub limit: u64,
    pub revision: Option<u64>,
}

/// Lease grant entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseGrantEntry {
    pub lease_id: i64,
    pub ttl_ms: u64,
}

/// Lease revoke entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRevokeEntry {
    pub lease_id: i64,
}

/// Lease keepalive entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseKeepaliveEntry {
    pub lease_id: i64,
}

/// Deterministic tick entry for TTL evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickEntry {
    /// Tick value in milliseconds.
    pub tick_ms: u64,
}

/// Compaction marker entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactEntry {
    /// Revision to compact up to.
    pub revision: u64,
}

/// Idempotency table eviction entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdemEvictEntry {
    /// Idempotency keys to evict.
    pub keys: Vec<Vec<u8>>,
}

/// Result of applying a WAL entry.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    /// Resulting revision after application.
    pub revision: u64,
    /// Whether the operation succeeded.
    pub success: bool,
    /// Number of keys affected.
    pub affected_count: usize,
    /// Previous key-value pairs (if requested).
    pub prev_kvs: Vec<super::state_machine::KvRecord>,
}

impl ApplyResult {
    /// Create a successful result.
    pub fn success(revision: u64, affected_count: usize) -> Self {
        Self {
            revision,
            success: true,
            affected_count,
            prev_kvs: Vec::new(),
        }
    }

    /// Create a successful result with previous KVs.
    pub fn success_with_prev(
        revision: u64,
        affected_count: usize,
        prev_kvs: Vec<super::state_machine::KvRecord>,
    ) -> Self {
        Self {
            revision,
            success: true,
            affected_count,
            prev_kvs,
        }
    }

    /// Create a failed result (e.g., transaction condition failed).
    pub fn failed(revision: u64) -> Self {
        Self {
            revision,
            success: false,
            affected_count: 0,
            prev_kvs: Vec::new(),
        }
    }
}

/// Compare evaluation against the state machine.
pub struct CompareEvaluator;

impl CompareEvaluator {
    /// Evaluate a compare predicate against the state machine.
    pub fn evaluate(compare: &Compare, state: &super::state_machine::KvStateMachine) -> bool {
        let record = state.get(&compare.key);

        match &compare.target {
            CompareTarget::ModRevision(expected) => Self::compare_u64(
                record.map(|r| r.mod_revision).unwrap_or(0),
                *expected,
                compare.result,
            ),
            CompareTarget::CreateRevision(expected) => Self::compare_u64(
                record.map(|r| r.create_revision).unwrap_or(0),
                *expected,
                compare.result,
            ),
            CompareTarget::Version(expected) => Self::compare_u64(
                record.map(|r| r.version).unwrap_or(0),
                *expected,
                compare.result,
            ),
            CompareTarget::Value(expected) => {
                let actual = record.map(|r| r.value.as_slice()).unwrap_or(&[]);
                Self::compare_bytes(actual, expected, compare.result)
            }
            CompareTarget::Lease(expected) => Self::compare_i64(
                record.and_then(|r| r.lease_id).unwrap_or(0),
                *expected,
                compare.result,
            ),
        }
    }

    /// Compare two u64 values.
    fn compare_u64(actual: u64, expected: u64, result: CompareResult) -> bool {
        match result {
            CompareResult::Equal => actual == expected,
            CompareResult::NotEqual => actual != expected,
            CompareResult::Greater => actual > expected,
            CompareResult::Less => actual < expected,
        }
    }

    /// Compare two i64 values.
    fn compare_i64(actual: i64, expected: i64, result: CompareResult) -> bool {
        match result {
            CompareResult::Equal => actual == expected,
            CompareResult::NotEqual => actual != expected,
            CompareResult::Greater => actual > expected,
            CompareResult::Less => actual < expected,
        }
    }

    /// Compare two byte slices.
    fn compare_bytes(actual: &[u8], expected: &[u8], result: CompareResult) -> bool {
        match result {
            CompareResult::Equal => actual == expected,
            CompareResult::NotEqual => actual != expected,
            CompareResult::Greater => actual > expected,
            CompareResult::Less => actual < expected,
        }
    }

    /// Evaluate all compares in a transaction.
    pub fn evaluate_all(
        compares: &[Compare],
        state: &super::state_machine::KvStateMachine,
    ) -> bool {
        compares.iter().all(|c| Self::evaluate(c, state))
    }
}

/// Transaction executor for atomically executing transaction operations.
pub struct TxnExecutor;

impl TxnExecutor {
    /// Execute a transaction entry against the state machine.
    ///
    /// Returns the apply result and whether the success branch was taken.
    pub fn execute(
        txn: &TxnEntry,
        state: &mut super::state_machine::KvStateMachine,
        revision: u64,
    ) -> (ApplyResult, bool) {
        // Evaluate all compare predicates (CAS-FENCE)
        let success = CompareEvaluator::evaluate_all(&txn.compares, state);

        // Select ops based on compare result
        let ops = if success {
            &txn.success_ops
        } else {
            &txn.failure_ops
        };

        let mut affected = 0;
        let mut prev_kvs = Vec::new();

        // Execute selected operations
        for op in ops {
            match op {
                TxnOp::Put(put) => {
                    if let Some(prev) =
                        state.put(put.key.clone(), put.value.clone(), revision, put.lease_id)
                    {
                        if put.prev_kv {
                            prev_kvs.push(prev);
                        }
                    }
                    affected += 1;
                }
                TxnOp::Delete(del) => {
                    if del.range_end.is_some() {
                        let deleted =
                            state.delete_range(&del.key, del.range_end.as_deref(), revision);
                        if del.prev_kv {
                            prev_kvs.extend(deleted.clone());
                        }
                        affected += deleted.len();
                    } else if let Some(prev) = state.delete(&del.key, revision) {
                        if del.prev_kv {
                            prev_kvs.push(prev);
                        }
                        affected += 1;
                    }
                }
                TxnOp::Range(_) => {
                    // Range ops are read-only, don't modify state
                }
            }
        }

        let result = ApplyResult::success_with_prev(revision, affected, prev_kvs);
        (result, success)
    }
}

/// Apply loop for processing WAL entries.
///
/// This is the core apply loop that processes committed WAL entries
/// in strict order, maintaining determinism per the KVSOURCE invariant.
pub struct ApplyLoop {
    /// The KV state machine.
    state_machine: super::state_machine::KvStateMachine,

    /// Lease manager.
    lease_manager: super::lease::LeaseManager,

    /// Idempotency table.
    idempotency_table: super::idempotency::IdempotencyTable,

    /// Last applied revision.
    last_applied: u64,
}

impl ApplyLoop {
    /// Create a new apply loop.
    pub fn new() -> Self {
        Self {
            state_machine: super::state_machine::KvStateMachine::new(),
            lease_manager: super::lease::LeaseManager::new(),
            idempotency_table: super::idempotency::IdempotencyTable::new(),
            last_applied: 0,
        }
    }

    /// Get a reference to the state machine.
    pub fn state_machine(&self) -> &super::state_machine::KvStateMachine {
        &self.state_machine
    }

    /// Get a mutable reference to the state machine.
    pub fn state_machine_mut(&mut self) -> &mut super::state_machine::KvStateMachine {
        &mut self.state_machine
    }

    /// Get a reference to the lease manager.
    pub fn lease_manager(&self) -> &super::lease::LeaseManager {
        &self.lease_manager
    }

    /// Get a mutable reference to the lease manager.
    pub fn lease_manager_mut(&mut self) -> &mut super::lease::LeaseManager {
        &mut self.lease_manager
    }

    /// Get a reference to the idempotency table.
    pub fn idempotency_table(&self) -> &super::idempotency::IdempotencyTable {
        &self.idempotency_table
    }

    /// Get the last applied revision.
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Apply a WAL entry at the given revision.
    ///
    /// This MUST be called in strict revision order per KVSOURCE invariant.
    pub fn apply(&mut self, entry: WalEntry, revision: u64) -> ApplyResult {
        debug_assert!(
            revision > self.last_applied,
            "APPLY-ORDER violation: {} <= {}",
            revision,
            self.last_applied
        );

        let result = match entry {
            WalEntry::Put(put) => self.apply_put(put, revision),
            WalEntry::Delete(del) => self.apply_delete(del, revision),
            WalEntry::Txn(txn) => self.apply_txn(txn, revision),
            WalEntry::LeaseGrant(grant) => self.apply_lease_grant(grant, revision),
            WalEntry::LeaseRevoke(revoke) => self.apply_lease_revoke(revoke, revision),
            WalEntry::LeaseKeepalive(keepalive) => self.apply_lease_keepalive(keepalive, revision),
            WalEntry::Tick(tick) => self.apply_tick(tick, revision),
            WalEntry::Compact(compact) => self.apply_compact(compact, revision),
            WalEntry::IdemEvict(evict) => self.apply_idem_evict(evict, revision),
        };

        self.last_applied = revision;
        result
    }

    /// Apply a put entry.
    fn apply_put(&mut self, put: PutEntry, revision: u64) -> ApplyResult {
        let prev = self
            .state_machine
            .put(put.key.clone(), put.value, revision, put.lease_id);

        // Attach to lease if specified
        if let Some(lease_id) = put.lease_id {
            self.lease_manager.attach_key(lease_id, put.key);
        }

        if put.prev_kv && prev.is_some() {
            ApplyResult::success_with_prev(revision, 1, vec![prev.unwrap()])
        } else {
            ApplyResult::success(revision, 1)
        }
    }

    /// Apply a delete entry.
    fn apply_delete(&mut self, del: DeleteEntry, revision: u64) -> ApplyResult {
        if del.range_end.is_some() {
            let deleted =
                self.state_machine
                    .delete_range(&del.key, del.range_end.as_deref(), revision);
            let count = deleted.len();
            if del.prev_kv {
                ApplyResult::success_with_prev(revision, count, deleted)
            } else {
                ApplyResult::success(revision, count)
            }
        } else {
            let prev = self.state_machine.delete(&del.key, revision);
            if del.prev_kv && prev.is_some() {
                ApplyResult::success_with_prev(revision, 1, vec![prev.unwrap()])
            } else {
                ApplyResult::success(revision, if prev.is_some() { 1 } else { 0 })
            }
        }
    }

    /// Apply a transaction entry with CAS-FENCE evaluation.
    fn apply_txn(&mut self, txn: TxnEntry, revision: u64) -> ApplyResult {
        let (result, _success) = TxnExecutor::execute(&txn, &mut self.state_machine, revision);
        result
    }

    /// Apply a lease grant entry.
    fn apply_lease_grant(&mut self, grant: LeaseGrantEntry, revision: u64) -> ApplyResult {
        let tick = self.state_machine.current_tick();
        self.lease_manager.grant(grant.lease_id, grant.ttl_ms, tick);
        ApplyResult::success(revision, 1)
    }

    /// Apply a lease revoke entry.
    fn apply_lease_revoke(&mut self, revoke: LeaseRevokeEntry, revision: u64) -> ApplyResult {
        // Get attached keys before revoking
        let keys_to_delete = self
            .lease_manager
            .get(revoke.lease_id)
            .map(|l| l.attached_keys.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        // Delete all attached keys
        for key in &keys_to_delete {
            self.state_machine.delete(key, revision);
        }

        self.lease_manager.revoke(revoke.lease_id);
        ApplyResult::success(revision, keys_to_delete.len())
    }

    /// Apply a lease keepalive entry.
    fn apply_lease_keepalive(
        &mut self,
        keepalive: LeaseKeepaliveEntry,
        revision: u64,
    ) -> ApplyResult {
        let tick = self.state_machine.current_tick();
        let success = self.lease_manager.keepalive(keepalive.lease_id, tick);
        ApplyResult {
            revision,
            success,
            affected_count: if success { 1 } else { 0 },
            prev_kvs: Vec::new(),
        }
    }

    /// Apply a tick entry for deterministic TTL evaluation.
    fn apply_tick(&mut self, tick: TickEntry, revision: u64) -> ApplyResult {
        use crate::core::time::Tick;
        let new_tick = Tick::new(tick.tick_ms);
        self.state_machine.advance_tick(new_tick);

        // Check for expired leases
        let expired = self.lease_manager.collect_expired(new_tick);
        let mut deleted_count = 0;

        for lease_id in expired {
            if let Some(lease) = self.lease_manager.get(lease_id) {
                let keys: Vec<_> = lease.attached_keys.iter().cloned().collect();
                for key in keys {
                    self.state_machine.delete(&key, revision);
                    deleted_count += 1;
                }
            }
            self.lease_manager.revoke(lease_id);
        }

        ApplyResult::success(revision, deleted_count)
    }

    /// Apply a compaction entry.
    fn apply_compact(&mut self, compact: CompactEntry, revision: u64) -> ApplyResult {
        let removed = self.state_machine.compact(compact.revision);
        ApplyResult::success(revision, removed)
    }

    /// Apply an idempotency eviction entry.
    fn apply_idem_evict(&mut self, evict: IdemEvictEntry, revision: u64) -> ApplyResult {
        let mut evicted = 0;
        for key in evict.keys {
            if self.idempotency_table.remove(&key) {
                evicted += 1;
            }
        }
        ApplyResult::success(revision, evicted)
    }

    /// Take pending events from the state machine.
    pub fn take_events(&mut self) -> Vec<super::state_machine::KvEvent> {
        self.state_machine.take_events()
    }

    /// Get apply loop statistics.
    pub fn stats(&self) -> ApplyLoopStats {
        ApplyLoopStats {
            last_applied: self.last_applied,
            state_machine_stats: self.state_machine.stats(),
            lease_count: self.lease_manager.lease_count(),
            idempotency_entries: self.idempotency_table.len(),
        }
    }
}

impl Default for ApplyLoop {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for the apply loop.
#[derive(Debug, Clone)]
pub struct ApplyLoopStats {
    /// Last applied revision.
    pub last_applied: u64,
    /// State machine statistics.
    pub state_machine_stats: super::state_machine::KvStateMachineStats,
    /// Number of active leases.
    pub lease_count: usize,
    /// Number of idempotency entries.
    pub idempotency_entries: usize,
}

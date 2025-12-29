//! Watch stream state.
//!
//! Per §7.1, watch state includes:
//! `WatchID → WatchState{key_range, start_revision, filters, progress_notify, last_sent_revision}`
//!
//! For etcd-conformant adapters, `last_sent_revision` MUST be persisted
//! for replay correctness per the specification.

use serde::{Deserialize, Serialize};

/// Watch state for a single watch stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchState {
    /// Unique watch identifier.
    pub watch_id: i64,

    /// Start of the key range to watch.
    pub key: Vec<u8>,

    /// End of the key range (exclusive), or empty for single key.
    pub range_end: Vec<u8>,

    /// Starting revision for the watch.
    pub start_revision: u64,

    /// Event filters applied to this watch.
    pub filters: WatchFilters,

    /// Whether to send progress notifications.
    pub progress_notify: bool,

    /// Revision of the last event delivered to the client.
    ///
    /// This MUST be persisted for etcd-conformant replay correctness.
    pub last_sent_revision: u64,

    /// Watch fragment flag (for large responses).
    pub fragment: bool,

    /// Previous key-value inclusion flag.
    pub prev_kv: bool,
}

bitflags::bitflags! {
    /// Event filters for a watch stream.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct WatchFilters: u32 {
        /// Filter out PUT events.
        const NOPUT = 0b0000_0001;
        /// Filter out DELETE events.
        const NODELETE = 0b0000_0010;
    }
}

impl Default for WatchFilters {
    fn default() -> Self {
        Self::empty()
    }
}

impl WatchState {
    /// Create a new watch state.
    pub fn new(watch_id: i64, key: Vec<u8>, range_end: Vec<u8>, start_revision: u64) -> Self {
        Self {
            watch_id,
            key,
            range_end,
            start_revision,
            filters: WatchFilters::empty(),
            progress_notify: false,
            last_sent_revision: start_revision,
            fragment: false,
            prev_kv: false,
        }
    }

    /// Check if this watch matches a key.
    pub fn matches_key(&self, key: &[u8]) -> bool {
        if self.range_end.is_empty() {
            // Single key watch
            key == self.key.as_slice()
        } else if self.range_end == [0] {
            // Prefix watch (range_end is \x00)
            key.starts_with(&self.key)
        } else {
            // Range watch
            key >= self.key.as_slice() && key < self.range_end.as_slice()
        }
    }

    /// Check if PUT events should be filtered.
    pub fn filters_put(&self) -> bool {
        self.filters.contains(WatchFilters::NOPUT)
    }

    /// Check if DELETE events should be filtered.
    pub fn filters_delete(&self) -> bool {
        self.filters.contains(WatchFilters::NODELETE)
    }

    /// Update the last sent revision.
    pub fn update_last_sent(&mut self, revision: u64) {
        if revision > self.last_sent_revision {
            self.last_sent_revision = revision;
        }
    }
}

/// Watch creation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchCreateRequest {
    /// Key to watch.
    pub key: Vec<u8>,
    /// Range end (empty for single key).
    pub range_end: Vec<u8>,
    /// Starting revision (0 for "now").
    pub start_revision: u64,
    /// Send progress notifications.
    pub progress_notify: bool,
    /// Event filters.
    pub filters: WatchFilters,
    /// Include previous key-value in events.
    pub prev_kv: bool,
    /// Watch ID (0 for auto-assign).
    pub watch_id: i64,
    /// Fragment large responses.
    pub fragment: bool,
}

/// Watch cancel request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchCancelRequest {
    /// Watch ID to cancel.
    pub watch_id: i64,
}

/// Watch semantics indicator.
///
/// Per §10.1.6, if linearizable start is not available, the watch
/// MUST be labeled as snapshot-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchSemantics {
    /// Watch started at a linearizable point.
    Linearizable,
    /// Watch started at a snapshot-only point (not linearizable).
    SnapshotOnly,
}

/// Watch event for delivery to a client.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    /// Watch ID this event is for.
    pub watch_id: i64,
    /// The underlying KV event.
    pub event: super::state_machine::KvEvent,
    /// Revision of this event.
    pub revision: u64,
}

/// Watch response to send to a client.
#[derive(Debug, Clone)]
pub struct WatchResponse {
    /// Watch ID.
    pub watch_id: i64,
    /// Whether this is a create response.
    pub created: bool,
    /// Whether this is a cancel response.
    pub canceled: bool,
    /// Cancel reason (if canceled).
    pub cancel_reason: Option<String>,
    /// Compaction revision (if watch was compacted).
    pub compact_revision: u64,
    /// Events in this response.
    pub events: Vec<super::state_machine::KvEvent>,
    /// Header revision (highest revision in batch).
    pub header_revision: u64,
    /// Whether more events are coming (fragmented response).
    pub fragment: bool,
}

impl WatchResponse {
    /// Create a watch created response.
    pub fn created(watch_id: i64, revision: u64) -> Self {
        Self {
            watch_id,
            created: true,
            canceled: false,
            cancel_reason: None,
            compact_revision: 0,
            events: Vec::new(),
            header_revision: revision,
            fragment: false,
        }
    }

    /// Create a watch canceled response.
    pub fn canceled(watch_id: i64, reason: Option<String>) -> Self {
        Self {
            watch_id,
            created: false,
            canceled: true,
            cancel_reason: reason,
            compact_revision: 0,
            events: Vec::new(),
            header_revision: 0,
            fragment: false,
        }
    }

    /// Create an events response.
    pub fn events(
        watch_id: i64,
        events: Vec<super::state_machine::KvEvent>,
        revision: u64,
    ) -> Self {
        Self {
            watch_id,
            created: false,
            canceled: false,
            cancel_reason: None,
            compact_revision: 0,
            events,
            header_revision: revision,
            fragment: false,
        }
    }

    /// Create a compacted response.
    pub fn compacted(watch_id: i64, compact_revision: u64) -> Self {
        Self {
            watch_id,
            created: false,
            canceled: true,
            cancel_reason: Some("compacted".to_string()),
            compact_revision,
            events: Vec::new(),
            header_revision: 0,
            fragment: false,
        }
    }

    /// Create a progress notification response.
    pub fn progress(watch_id: i64, revision: u64) -> Self {
        Self {
            watch_id,
            created: false,
            canceled: false,
            cancel_reason: None,
            compact_revision: 0,
            events: Vec::new(),
            header_revision: revision,
            fragment: false,
        }
    }
}

/// Watch manager for a KPG.
///
/// Manages watch lifecycle including creation, cancellation, event
/// filtering, and delivery per §7.1.
pub struct WatchManager {
    /// Active watches indexed by watch ID.
    watches: std::collections::HashMap<i64, WatchState>,

    /// Next watch ID for auto-generation.
    next_watch_id: i64,

    /// Pending responses to send to clients.
    pending_responses: Vec<WatchResponse>,
}

impl WatchManager {
    /// Create a new watch manager.
    pub fn new() -> Self {
        Self {
            watches: std::collections::HashMap::new(),
            next_watch_id: 1,
            pending_responses: Vec::new(),
        }
    }

    /// Get the number of active watches.
    pub fn watch_count(&self) -> usize {
        self.watches.len()
    }

    /// Create a new watch.
    ///
    /// Returns the assigned watch ID and whether it was created successfully.
    pub fn create(
        &mut self,
        request: WatchCreateRequest,
        current_revision: u64,
        semantics: WatchSemantics,
    ) -> (i64, bool) {
        let watch_id = if request.watch_id == 0 {
            let id = self.next_watch_id;
            self.next_watch_id += 1;
            id
        } else {
            // Check if ID is already in use
            if self.watches.contains_key(&request.watch_id) {
                return (request.watch_id, false);
            }
            if request.watch_id >= self.next_watch_id {
                self.next_watch_id = request.watch_id + 1;
            }
            request.watch_id
        };

        // Determine start revision
        let start_revision = if request.start_revision == 0 {
            current_revision + 1 // Start from next revision
        } else {
            request.start_revision
        };

        let mut state = WatchState::new(watch_id, request.key, request.range_end, start_revision);
        state.filters = request.filters;
        state.progress_notify = request.progress_notify;
        state.prev_kv = request.prev_kv;
        state.fragment = request.fragment;

        self.watches.insert(watch_id, state);

        // Queue created response
        self.pending_responses
            .push(WatchResponse::created(watch_id, current_revision));

        // Mark semantics if not linearizable
        if semantics == WatchSemantics::SnapshotOnly {
            // Could store this for later reference
        }

        (watch_id, true)
    }

    /// Cancel a watch.
    pub fn cancel(&mut self, watch_id: i64, reason: Option<String>) -> bool {
        if self.watches.remove(&watch_id).is_some() {
            self.pending_responses
                .push(WatchResponse::canceled(watch_id, reason));
            true
        } else {
            false
        }
    }

    /// Get a watch by ID.
    pub fn get(&self, watch_id: i64) -> Option<&WatchState> {
        self.watches.get(&watch_id)
    }

    /// Get a mutable watch by ID.
    pub fn get_mut(&mut self, watch_id: i64) -> Option<&mut WatchState> {
        self.watches.get_mut(&watch_id)
    }

    /// Process events and generate watch responses.
    ///
    /// This filters events based on watch key ranges and filters,
    /// then queues appropriate responses.
    pub fn process_events(&mut self, events: &[super::state_machine::KvEvent], revision: u64) {
        for (watch_id, watch) in &mut self.watches {
            let matching_events: Vec<_> = events
                .iter()
                .filter(|e| {
                    // Check key match
                    if !watch.matches_key(&e.kv.key) {
                        return false;
                    }

                    // Check filters
                    match e.event_type {
                        super::state_machine::EventType::Put => !watch.filters_put(),
                        super::state_machine::EventType::Delete => !watch.filters_delete(),
                    }
                })
                .cloned()
                .collect();

            if !matching_events.is_empty() {
                let response = WatchResponse::events(*watch_id, matching_events, revision);
                self.pending_responses.push(response);
                watch.update_last_sent(revision);
            }
        }
    }

    /// Generate progress notifications for watches that requested them.
    pub fn generate_progress_notifications(&mut self, revision: u64) {
        for (watch_id, watch) in &self.watches {
            if watch.progress_notify && watch.last_sent_revision < revision {
                self.pending_responses
                    .push(WatchResponse::progress(*watch_id, revision));
            }
        }
    }

    /// Handle compaction - cancel watches that are behind the compaction point.
    pub fn handle_compaction(&mut self, compact_revision: u64) {
        let watches_to_cancel: Vec<i64> = self
            .watches
            .iter()
            .filter(|(_, w)| w.start_revision < compact_revision)
            .map(|(id, _)| *id)
            .collect();

        for watch_id in watches_to_cancel {
            self.watches.remove(&watch_id);
            self.pending_responses
                .push(WatchResponse::compacted(watch_id, compact_revision));
        }
    }

    /// Take pending responses for delivery.
    pub fn take_responses(&mut self) -> Vec<WatchResponse> {
        std::mem::take(&mut self.pending_responses)
    }

    /// Peek at pending responses.
    pub fn peek_responses(&self) -> &[WatchResponse] {
        &self.pending_responses
    }

    /// Get all watch IDs.
    pub fn watch_ids(&self) -> Vec<i64> {
        self.watches.keys().copied().collect()
    }

    /// Get statistics.
    pub fn stats(&self) -> WatchStats {
        let mut total_key_ranges = 0;
        let mut with_prev_kv = 0;
        let mut with_progress = 0;

        for watch in self.watches.values() {
            total_key_ranges += 1;
            if watch.prev_kv {
                with_prev_kv += 1;
            }
            if watch.progress_notify {
                with_progress += 1;
            }
        }

        WatchStats {
            active_watches: self.watches.len(),
            total_key_ranges,
            watches_with_prev_kv: with_prev_kv,
            watches_with_progress: with_progress,
            pending_responses: self.pending_responses.len(),
        }
    }
}

impl Default for WatchManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Watch statistics.
#[derive(Debug, Clone)]
pub struct WatchStats {
    /// Number of active watches.
    pub active_watches: usize,
    /// Total key ranges being watched.
    pub total_key_ranges: usize,
    /// Watches with prev_kv enabled.
    pub watches_with_prev_kv: usize,
    /// Watches with progress notifications enabled.
    pub watches_with_progress: usize,
    /// Number of pending responses.
    pub pending_responses: usize,
}

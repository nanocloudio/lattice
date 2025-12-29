//! Deterministic time utilities.
//!
//! Lattice defines a deterministic tick source per §9.6:
//! - `Tick(ms)` entries committed periodically to the WAL
//! - OR derived from Clustor snapshot manifest time fences (feature-gated)
//!
//! Exactly one source MUST be active per KPG at any time.
//! No non-WAL time source is permitted.

use serde::{Deserialize, Serialize};

/// A deterministic tick representing WAL-committed time.
///
/// Ticks are the sole source of time for TTL evaluation and lease deadlines.
/// Wall-clock sampling outside the apply loop MUST NOT directly delete keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tick {
    /// Milliseconds since an epoch (implementation-defined).
    pub ms: u64,
}

impl Tick {
    /// Create a new tick with the given millisecond value.
    pub const fn new(ms: u64) -> Self {
        Self { ms }
    }

    /// Create a tick representing zero (epoch start).
    pub const fn zero() -> Self {
        Self { ms: 0 }
    }

    /// Add milliseconds to this tick.
    pub const fn add_ms(self, ms: u64) -> Self {
        Self { ms: self.ms + ms }
    }

    /// Subtract milliseconds from this tick, saturating at zero.
    pub const fn sub_ms(self, ms: u64) -> Self {
        Self {
            ms: self.ms.saturating_sub(ms),
        }
    }

    /// Check if this tick is at or after the given deadline.
    pub const fn is_at_or_after(self, deadline: Tick) -> bool {
        self.ms >= deadline.ms
    }

    /// Check if this tick is before the given deadline.
    pub const fn is_before(self, deadline: Tick) -> bool {
        self.ms < deadline.ms
    }

    /// Milliseconds until a deadline.
    ///
    /// Returns 0 if the deadline has already passed.
    pub fn ms_until(self, deadline: Tick) -> u64 {
        deadline.ms.saturating_sub(self.ms)
    }
}

impl std::fmt::Display for Tick {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tick({}ms)", self.ms)
    }
}

/// Tick source type for configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TickSourceType {
    /// Periodic WAL-committed ticks (default).
    WalTick,
    /// Derived from Clustor snapshot manifest time fences (feature-gated).
    SnapshotManifest,
}

impl Default for TickSourceType {
    fn default() -> Self {
        Self::WalTick
    }
}

/// Configuration for the tick source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickSourceConfig {
    /// Type of tick source.
    pub source_type: TickSourceType,

    /// Tick emission period in milliseconds (for WalTick source).
    pub period_ms: u64,
}

impl Default for TickSourceConfig {
    fn default() -> Self {
        Self {
            source_type: TickSourceType::WalTick,
            period_ms: 1000, // 1 second default per specification
        }
    }
}

impl TickSourceConfig {
    /// Validate the tick source configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.period_ms == 0 {
            anyhow::bail!("tick source period_ms must be > 0");
        }
        Ok(())
    }
}

// ============================================================================
// Tick Source Trait and Implementations
// ============================================================================

/// Trait for generating deterministic ticks.
///
/// Exactly one tick source MUST be active per KPG at any time.
/// The tick source is responsible for emitting Tick WAL entries
/// that drive TTL evaluation and lease deadline enforcement.
pub trait TickSource: Send + Sync {
    /// Get the current tick value.
    ///
    /// This should only be called during WAL entry generation,
    /// never during apply loop processing.
    fn current_tick(&self) -> Tick;

    /// Get the tick source type.
    fn source_type(&self) -> TickSourceType;

    /// Get the configured emission period in milliseconds.
    fn period_ms(&self) -> u64;

    /// Check if a tick should be emitted based on the last emitted tick.
    ///
    /// Returns Some(Tick) if a new tick should be emitted, None otherwise.
    fn should_emit(&self, last_emitted: Option<Tick>) -> Option<Tick>;
}

/// WAL-committed tick source (default).
///
/// Emits periodic Tick entries to the WAL based on wall-clock time.
/// The emitted ticks become the sole source of time for TTL evaluation.
pub struct WalTickSource {
    /// Emission period in milliseconds.
    period_ms: u64,
}

impl WalTickSource {
    /// Create a new WAL tick source with the given period.
    pub fn new(period_ms: u64) -> Self {
        Self { period_ms }
    }

    /// Create a WAL tick source from configuration.
    pub fn from_config(config: &TickSourceConfig) -> Self {
        Self {
            period_ms: config.period_ms,
        }
    }
}

impl TickSource for WalTickSource {
    fn current_tick(&self) -> Tick {
        // Sample wall-clock time for tick generation
        // This is only called during WAL entry creation, not during apply
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Tick::new(now)
    }

    fn source_type(&self) -> TickSourceType {
        TickSourceType::WalTick
    }

    fn period_ms(&self) -> u64 {
        self.period_ms
    }

    fn should_emit(&self, last_emitted: Option<Tick>) -> Option<Tick> {
        let current = self.current_tick();
        match last_emitted {
            None => Some(current),
            Some(last) => {
                if current.ms >= last.ms + self.period_ms {
                    Some(current)
                } else {
                    None
                }
            }
        }
    }
}

/// Snapshot manifest tick source (feature-gated).
///
/// Derives ticks from Clustor snapshot manifest time fences.
/// This is an alternative to WAL-committed ticks for deployments
/// that prefer snapshot-based time synchronization.
pub struct SnapshotManifestTickSource {
    /// Emission period in milliseconds.
    period_ms: u64,
    /// Last known snapshot manifest time.
    last_manifest_time_ms: std::sync::atomic::AtomicU64,
}

impl SnapshotManifestTickSource {
    /// Create a new snapshot manifest tick source.
    pub fn new(period_ms: u64) -> Self {
        Self {
            period_ms,
            last_manifest_time_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Update the last known manifest time.
    pub fn update_manifest_time(&self, time_ms: u64) {
        self.last_manifest_time_ms
            .store(time_ms, std::sync::atomic::Ordering::Release);
    }
}

impl TickSource for SnapshotManifestTickSource {
    fn current_tick(&self) -> Tick {
        let ms = self
            .last_manifest_time_ms
            .load(std::sync::atomic::Ordering::Acquire);
        Tick::new(ms)
    }

    fn source_type(&self) -> TickSourceType {
        TickSourceType::SnapshotManifest
    }

    fn period_ms(&self) -> u64 {
        self.period_ms
    }

    fn should_emit(&self, last_emitted: Option<Tick>) -> Option<Tick> {
        let current = self.current_tick();
        if current.ms == 0 {
            // No manifest time available yet
            return None;
        }
        match last_emitted {
            None => Some(current),
            Some(last) => {
                if current.ms >= last.ms + self.period_ms {
                    Some(current)
                } else {
                    None
                }
            }
        }
    }
}

// ============================================================================
// Tick Source Exclusivity (Task 20)
// ============================================================================

/// Tick source registry ensuring exactly one source per KPG.
///
/// This struct enforces the invariant that exactly one tick source
/// MUST be active per KPG at any time (§9.6).
pub struct TickSourceRegistry {
    /// Active tick source.
    source: Box<dyn TickSource>,
}

impl TickSourceRegistry {
    /// Create a new registry with a WAL tick source (default).
    pub fn new_wal(period_ms: u64) -> Self {
        Self {
            source: Box::new(WalTickSource::new(period_ms)),
        }
    }

    /// Create a new registry with a snapshot manifest tick source.
    pub fn new_snapshot_manifest(period_ms: u64) -> Self {
        Self {
            source: Box::new(SnapshotManifestTickSource::new(period_ms)),
        }
    }

    /// Create a registry from configuration.
    pub fn from_config(config: &TickSourceConfig) -> Self {
        match config.source_type {
            TickSourceType::WalTick => Self::new_wal(config.period_ms),
            TickSourceType::SnapshotManifest => Self::new_snapshot_manifest(config.period_ms),
        }
    }

    /// Get the active tick source.
    pub fn source(&self) -> &dyn TickSource {
        self.source.as_ref()
    }

    /// Get the source type.
    pub fn source_type(&self) -> TickSourceType {
        self.source.source_type()
    }

    /// Check if a tick should be emitted.
    pub fn should_emit(&self, last_emitted: Option<Tick>) -> Option<Tick> {
        self.source.should_emit(last_emitted)
    }
}

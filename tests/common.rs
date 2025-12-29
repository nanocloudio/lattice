//! Common test utilities.
//!
//! This module contains shared helpers for integration tests.
//! Import with `mod common;` in test files.

use lattice::core::config::Config;
use lattice::core::time::Tick;
use lattice::kpg::state_machine::KvStateMachine;
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;

/// Create a minimal valid configuration file.
pub fn create_minimal_config() -> NamedTempFile {
    let config_content = r#"
[control_plane]
mode = "embedded"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"
"#;

    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(config_content.as_bytes())
        .expect("Failed to write config");
    file
}

/// Create a configuration with custom settings.
pub fn create_config_with_settings(
    mode: &str,
    durability_mode: &str,
    log_level: &str,
) -> NamedTempFile {
    let config_content = format!(
        r#"
[control_plane]
mode = "{}"

[listeners]

[durability]
durability_mode = "{}"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"

[telemetry]
log_level = "{}"
"#,
        mode, durability_mode, log_level
    );

    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(config_content.as_bytes())
        .expect("Failed to write config");
    file
}

/// Create a config with paths.
pub fn create_config_with_paths(storage_dir: &str) -> NamedTempFile {
    let config_content = format!(
        r#"
[control_plane]
mode = "embedded"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"

[paths]
storage_dir = "{}"
"#,
        storage_dir
    );

    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(config_content.as_bytes())
        .expect("Failed to write config");
    file
}

/// Load a config from a temp file.
pub fn load_config(file: &NamedTempFile) -> Config {
    Config::from_file(file.path()).expect("Failed to load config")
}

/// Create a pre-populated KV state machine.
pub fn create_populated_state_machine(num_keys: usize) -> KvStateMachine {
    let mut sm = KvStateMachine::new();
    for i in 0..num_keys {
        let key = format!("key-{:05}", i).into_bytes();
        let value = format!("value-{}", i).into_bytes();
        sm.put(key, value, (i + 1) as u64, None);
    }
    // Clear events so tests start fresh
    sm.take_events();
    sm
}

/// Create a state machine with structured keys for range testing.
pub fn create_range_test_state_machine() -> KvStateMachine {
    let mut sm = KvStateMachine::new();

    // Create hierarchical key structure
    let keys = [
        ("users/alice", "alice-data"),
        ("users/bob", "bob-data"),
        ("users/charlie", "charlie-data"),
        ("config/app", "app-config"),
        ("config/db", "db-config"),
        ("data/records/1", "record-1"),
        ("data/records/2", "record-2"),
        ("data/records/3", "record-3"),
        ("metrics/cpu", "cpu-data"),
        ("metrics/memory", "memory-data"),
    ];

    for (i, (key, value)) in keys.iter().enumerate() {
        sm.put(
            key.as_bytes().to_vec(),
            value.as_bytes().to_vec(),
            (i + 1) as u64,
            None,
        );
    }

    sm.take_events();
    sm
}

/// Generate a test tick at the given milliseconds.
pub fn test_tick(ms: u64) -> Tick {
    Tick::new(ms)
}

/// Generate a sequence of test ticks.
pub fn test_tick_sequence(start_ms: u64, count: usize, interval_ms: u64) -> Vec<Tick> {
    (0..count)
        .map(|i| Tick::new(start_ms + (i as u64 * interval_ms)))
        .collect()
}

/// Create a temporary directory for test data.
pub fn temp_test_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp directory")
}

/// Create a test storage path.
pub fn test_storage_path() -> PathBuf {
    let dir = temp_test_dir();
    dir.path().join("storage")
}

/// Assert that two byte slices are equal with a helpful message.
#[track_caller]
pub fn assert_bytes_eq(expected: &[u8], actual: &[u8]) {
    if expected != actual {
        panic!(
            "Byte slices not equal:\n  expected: {:?}\n  actual:   {:?}",
            String::from_utf8_lossy(expected),
            String::from_utf8_lossy(actual)
        );
    }
}

/// Assert that a result is Ok and return the value.
#[track_caller]
pub fn assert_ok<T, E: std::fmt::Debug>(result: Result<T, E>) -> T {
    match result {
        Ok(v) => v,
        Err(e) => panic!("Expected Ok, got Err: {:?}", e),
    }
}

/// Assert that a result is Err.
#[track_caller]
pub fn assert_err<T: std::fmt::Debug, E>(result: Result<T, E>) -> E {
    match result {
        Ok(v) => panic!("Expected Err, got Ok: {:?}", v),
        Err(e) => e,
    }
}

/// Measure the execution time of a closure.
pub fn measure_time<F, R>(f: F) -> (R, std::time::Duration)
where
    F: FnOnce() -> R,
{
    let start = std::time::Instant::now();
    let result = f();
    (result, start.elapsed())
}

/// Generate random bytes for testing.
pub fn random_bytes(len: usize) -> Vec<u8> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let mut bytes = Vec::with_capacity(len);
    let mut state = seed;
    for _ in 0..len {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        bytes.push((state >> 56) as u8);
    }
    bytes
}

/// Generate a random key for testing.
pub fn random_key(prefix: &str) -> Vec<u8> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", prefix, ts).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_minimal_config() {
        let file = create_minimal_config();
        let config = load_config(&file);
        assert_eq!(config.control_plane.mode, "embedded");
    }

    #[test]
    fn test_create_populated_state_machine() {
        let sm = create_populated_state_machine(10);
        assert_eq!(sm.current_revision(), 10);
        assert!(sm.get(b"key-00005").is_some());
    }

    #[test]
    fn test_create_range_test_state_machine() {
        let sm = create_range_test_state_machine();

        // Test prefix range
        let users = sm.range(b"users/", Some(b"users0"));
        assert_eq!(users.len(), 3);

        let configs = sm.range(b"config/", Some(b"config0"));
        assert_eq!(configs.len(), 2);
    }

    #[test]
    fn test_tick_sequence_helper() {
        let ticks = super::test_tick_sequence(1000, 5, 100);
        assert_eq!(ticks.len(), 5);
        assert_eq!(ticks[0].ms, 1000);
        assert_eq!(ticks[1].ms, 1100);
        assert_eq!(ticks[4].ms, 1400);
    }

    #[test]
    fn test_random_bytes() {
        let bytes1 = random_bytes(32);
        let bytes2 = random_bytes(32);

        assert_eq!(bytes1.len(), 32);
        assert_eq!(bytes2.len(), 32);
        // Very unlikely to be equal (but not impossible)
    }

    #[test]
    fn test_measure_time() {
        let (result, duration) = measure_time(|| {
            std::thread::sleep(std::time::Duration::from_millis(10));
            42
        });

        assert_eq!(result, 42);
        assert!(duration.as_millis() >= 10);
    }

    #[test]
    fn test_create_config_with_settings() {
        let file = create_config_with_settings("embedded", "strict", "debug");
        let config = load_config(&file);
        assert_eq!(config.control_plane.mode, "embedded");
    }

    #[test]
    fn test_create_config_with_paths() {
        let file = create_config_with_paths("/tmp/test-storage");
        let config = load_config(&file);
        assert_eq!(config.control_plane.mode, "embedded");
    }

    #[test]
    fn test_test_tick() {
        let tick = test_tick(1000);
        assert_eq!(tick.ms, 1000);
    }

    #[test]
    fn test_temp_test_dir() {
        let dir = temp_test_dir();
        assert!(dir.path().exists());
    }

    #[test]
    fn test_test_storage_path() {
        let path = test_storage_path();
        assert!(path.to_string_lossy().contains("storage"));
    }

    #[test]
    fn test_assert_bytes_eq() {
        assert_bytes_eq(b"hello", b"hello");
    }

    #[test]
    fn test_assert_ok() {
        let result: Result<i32, &str> = Ok(42);
        assert_eq!(assert_ok(result), 42);
    }

    #[test]
    fn test_assert_err() {
        let result: Result<i32, &str> = Err("error");
        assert_eq!(assert_err(result), "error");
    }

    #[test]
    fn test_random_key() {
        let key1 = random_key("test");
        let key2 = random_key("test");
        // Keys should start with the prefix
        assert!(key1.starts_with(b"test-"));
        assert!(key2.starts_with(b"test-"));
    }
}

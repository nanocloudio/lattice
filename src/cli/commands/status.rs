//! Status command implementation.

use anyhow::Result;
use clap::Args;

/// Show server status.
#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Admin endpoint URL.
    #[arg(short, long, default_value = "http://localhost:9090")]
    pub endpoint: String,

    /// Output format (text, json).
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Show verbose output.
    #[arg(short, long)]
    pub verbose: bool,
}

/// Run the status command.
pub fn run_status(args: StatusArgs) -> Result<()> {
    match args.format.as_str() {
        "json" => show_status_json(&args),
        _ => show_status_text(&args), // Default to text output
    }
}

fn show_status_text(args: &StatusArgs) -> Result<()> {
    println!("Lattice Server Status");
    println!("=====================");
    println!("Endpoint: {}", args.endpoint);
    println!();

    // Server info
    println!("Server:");
    println!("  Version:      0.1.0");
    println!("  Node ID:      node-1");
    println!("  Uptime:       2h 34m 12s");
    println!();

    // Cluster info
    println!("Cluster:");
    println!("  ID:           lattice-default");
    println!("  Role:         Leader");
    println!("  Term:         42");
    println!("  Members:      3");
    println!("  Healthy:      3");
    println!();

    // Storage info
    println!("Storage:");
    println!("  Revision:     12345");
    println!("  Compaction:   10000");
    println!("  Keys:         1,234,567");
    println!("  Size:         256 MiB");
    println!();

    if args.verbose {
        // Additional details
        println!("Adapters:");
        println!("  etcd:         enabled (2379)");
        println!("  redis:        disabled");
        println!("  memcached:    disabled");
        println!();

        println!("Connections:");
        println!("  Active:       42");
        println!("  Total:        1,234");
        println!();

        println!("Metrics:");
        println!("  Requests/s:   1,234");
        println!("  Latency P50:  1.2ms");
        println!("  Latency P99:  15.3ms");
        println!();

        println!("Security:");
        println!("  mTLS:         enabled");
        println!("  Auth:         enabled");
        println!("  Audit:        enabled");
        println!();
    }

    println!("Note: This is placeholder data. Connect to a running server for real status.");

    Ok(())
}

fn show_status_json(args: &StatusArgs) -> Result<()> {
    let status = serde_json::json!({
        "endpoint": args.endpoint,
        "server": {
            "version": "0.1.0",
            "node_id": "node-1",
            "uptime_seconds": 9252
        },
        "cluster": {
            "id": "lattice-default",
            "role": "leader",
            "term": 42,
            "members": 3,
            "healthy_members": 3
        },
        "storage": {
            "revision": 12345,
            "compaction_floor": 10000,
            "keys": 1234567,
            "size_bytes": 268435456
        },
        "adapters": {
            "etcd": {
                "enabled": true,
                "port": 2379
            },
            "redis": {
                "enabled": false
            },
            "memcached": {
                "enabled": false
            }
        },
        "connections": {
            "active": 42,
            "total": 1234
        },
        "security": {
            "mtls_enabled": true,
            "auth_enabled": true,
            "audit_enabled": true
        }
    });

    println!("{}", serde_json::to_string_pretty(&status)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    /// Server status information.
    #[derive(Debug, Clone)]
    pub struct ServerStatus {
        /// Server version.
        pub version: String,
        /// Node ID.
        pub node_id: String,
        /// Uptime in seconds.
        pub uptime_seconds: u64,
        /// Cluster role.
        pub role: ClusterRole,
        /// Current term.
        pub term: u64,
        /// Current revision.
        pub revision: u64,
        /// Compaction floor.
        pub compaction_floor: u64,
        /// Number of keys.
        pub key_count: u64,
        /// Storage size in bytes.
        pub storage_bytes: u64,
        /// Active connections.
        pub active_connections: u64,
        /// Is healthy.
        pub is_healthy: bool,
    }

    /// Cluster role.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClusterRole {
        /// Leader of the cluster.
        Leader,
        /// Follower in the cluster.
        Follower,
        /// Candidate during election.
        Candidate,
        /// Learner (non-voting member).
        Learner,
    }

    impl std::fmt::Display for ClusterRole {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Leader => write!(f, "leader"),
                Self::Follower => write!(f, "follower"),
                Self::Candidate => write!(f, "candidate"),
                Self::Learner => write!(f, "learner"),
            }
        }
    }

    impl ServerStatus {
        /// Format uptime as human-readable string.
        pub fn uptime_human(&self) -> String {
            let secs = self.uptime_seconds;
            let hours = secs / 3600;
            let mins = (secs % 3600) / 60;
            let secs = secs % 60;

            if hours > 0 {
                format!("{}h {}m {}s", hours, mins, secs)
            } else if mins > 0 {
                format!("{}m {}s", mins, secs)
            } else {
                format!("{}s", secs)
            }
        }

        /// Format storage size as human-readable string.
        pub fn storage_human(&self) -> String {
            let bytes = self.storage_bytes;
            if bytes >= 1024 * 1024 * 1024 {
                format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
            } else if bytes >= 1024 * 1024 {
                format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
            } else if bytes >= 1024 {
                format!("{:.1} KiB", bytes as f64 / 1024.0)
            } else {
                format!("{} B", bytes)
            }
        }
    }

    #[test]
    fn test_cluster_role_display() {
        assert_eq!(format!("{}", ClusterRole::Leader), "leader");
        assert_eq!(format!("{}", ClusterRole::Follower), "follower");
        assert_eq!(format!("{}", ClusterRole::Candidate), "candidate");
        assert_eq!(format!("{}", ClusterRole::Learner), "learner");
    }

    #[test]
    fn test_uptime_human_hours() {
        let status = ServerStatus {
            version: "0.1.0".to_string(),
            node_id: "node-1".to_string(),
            uptime_seconds: 9252, // 2h 34m 12s
            role: ClusterRole::Leader,
            term: 42,
            revision: 12345,
            compaction_floor: 10000,
            key_count: 1000,
            storage_bytes: 1024 * 1024,
            active_connections: 10,
            is_healthy: true,
        };

        assert_eq!(status.uptime_human(), "2h 34m 12s");
    }

    #[test]
    fn test_uptime_human_minutes() {
        let status = ServerStatus {
            version: "0.1.0".to_string(),
            node_id: "node-1".to_string(),
            uptime_seconds: 125, // 2m 5s
            role: ClusterRole::Follower,
            term: 42,
            revision: 12345,
            compaction_floor: 10000,
            key_count: 1000,
            storage_bytes: 1024 * 1024,
            active_connections: 10,
            is_healthy: true,
        };

        assert_eq!(status.uptime_human(), "2m 5s");
    }

    #[test]
    fn test_storage_human() {
        let mut status = ServerStatus {
            version: "0.1.0".to_string(),
            node_id: "node-1".to_string(),
            uptime_seconds: 100,
            role: ClusterRole::Leader,
            term: 42,
            revision: 12345,
            compaction_floor: 10000,
            key_count: 1000,
            storage_bytes: 1024 * 1024 * 1024, // 1 GiB
            active_connections: 10,
            is_healthy: true,
        };

        assert_eq!(status.storage_human(), "1.0 GiB");

        status.storage_bytes = 256 * 1024 * 1024; // 256 MiB
        assert_eq!(status.storage_human(), "256.0 MiB");

        status.storage_bytes = 512 * 1024; // 512 KiB
        assert_eq!(status.storage_human(), "512.0 KiB");

        status.storage_bytes = 512; // 512 B
        assert_eq!(status.storage_human(), "512 B");
    }

    #[test]
    fn test_server_status_fields() {
        let status = ServerStatus {
            version: "0.1.0".to_string(),
            node_id: "node-1".to_string(),
            uptime_seconds: 3600,
            role: ClusterRole::Leader,
            term: 42,
            revision: 12345,
            compaction_floor: 10000,
            key_count: 1000,
            storage_bytes: 1024 * 1024,
            active_connections: 10,
            is_healthy: true,
        };

        assert_eq!(status.version, "0.1.0");
        assert_eq!(status.node_id, "node-1");
        assert_eq!(status.role, ClusterRole::Leader);
        assert_eq!(status.term, 42);
        assert_eq!(status.revision, 12345);
        assert_eq!(status.compaction_floor, 10000);
        assert_eq!(status.key_count, 1000);
        assert_eq!(status.active_connections, 10);
        assert!(status.is_healthy);
    }
}

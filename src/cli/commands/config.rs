//! Config command implementation.

use anyhow::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;

/// Configuration operations.
#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

/// Config subcommands.
#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Validate configuration file.
    Validate {
        /// Config file path.
        #[arg(short, long, default_value = "config/lattice.toml")]
        config: PathBuf,
    },
    /// Print configuration with defaults.
    Show {
        /// Config file path.
        #[arg(short, long, default_value = "config/lattice.toml")]
        config: PathBuf,
        /// Output format (toml, json).
        #[arg(long, default_value = "toml")]
        format: String,
    },
    /// Generate a configuration template.
    Generate {
        /// Output file path.
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Environment (dev, staging, prod).
        #[arg(long, default_value = "dev")]
        env: String,
    },
    /// Show configuration diff between two files.
    Diff {
        /// First config file.
        file1: PathBuf,
        /// Second config file.
        file2: PathBuf,
    },
}

/// Run the config command.
pub fn run_config(args: ConfigArgs) -> Result<()> {
    match args.command {
        ConfigCommand::Validate { config } => validate_config(&config),
        ConfigCommand::Show { config, format } => show_config(&config, &format),
        ConfigCommand::Generate { output, env } => generate_config(output.as_deref(), &env),
        ConfigCommand::Diff { file1, file2 } => diff_configs(&file1, &file2),
    }
}

fn validate_config(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("Config file not found: {:?}", path);
    }

    let content = std::fs::read_to_string(path)?;

    // Try to parse as TOML
    match toml::from_str::<toml::Value>(&content) {
        Ok(_) => {
            println!("✓ Config file is valid TOML");

            // Additional validation logic would go here
            // For now we just check the structure
            let config: toml::Value = toml::from_str(&content)?;

            if let Some(server) = config.get("server") {
                if server.get("node_id").is_none() {
                    println!("  ⚠ Warning: server.node_id not set");
                }
            } else {
                println!("  ⚠ Warning: [server] section not found");
            }

            if config.get("storage").is_none() {
                println!("  ⚠ Warning: [storage] section not found");
            }

            if config.get("tls").is_none() {
                println!("  ⚠ Warning: [tls] section not found (required for production)");
            }

            println!("✓ Configuration validation complete");
            Ok(())
        }
        Err(e) => {
            anyhow::bail!("Invalid TOML: {}", e);
        }
    }
}

fn show_config(path: &PathBuf, format: &str) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("Config file not found: {:?}", path);
    }

    let content = std::fs::read_to_string(path)?;
    let config: toml::Value = toml::from_str(&content)?;

    match format {
        "json" => {
            let json = serde_json::to_string_pretty(&config)?;
            println!("{}", json);
        }
        _ => {
            // Default to TOML output
            println!("{}", content);
        }
    }

    Ok(())
}

fn generate_config(output: Option<&std::path::Path>, env: &str) -> Result<()> {
    let template = match env {
        "prod" | "production" => generate_prod_template(),
        "staging" => generate_staging_template(),
        _ => generate_dev_template(), // Default to dev for "dev", "development", or anything else
    };

    match output {
        Some(path) => {
            std::fs::write(path, &template)?;
            println!("Generated {} config template: {:?}", env, path);
        }
        None => {
            println!("{}", template);
        }
    }

    Ok(())
}

fn generate_dev_template() -> String {
    r#"# Lattice Development Configuration

[server]
node_id = "node-1"
bind_addr = "127.0.0.1:2379"
admin_addr = "127.0.0.1:9090"
cluster_name = "lattice-dev"

[storage]
data_dir = "data"
wal_dir = "data/wal"
snapshot_dir = "data/snapshots"

[cluster]
peers = []
election_timeout_ms = 1000
heartbeat_interval_ms = 100

[adapters.etcd]
enabled = true
listen_addr = "127.0.0.1:2379"

[logging]
level = "debug"
format = "text"
"#
    .to_string()
}

fn generate_staging_template() -> String {
    r#"# Lattice Staging Configuration

[server]
node_id = "node-1"
bind_addr = "0.0.0.0:2379"
admin_addr = "0.0.0.0:9090"
cluster_name = "lattice-staging"

[storage]
data_dir = "/var/lib/lattice"
wal_dir = "/var/lib/lattice/wal"
snapshot_dir = "/var/lib/lattice/snapshots"

[cluster]
peers = ["node-2:2380", "node-3:2380"]
election_timeout_ms = 500
heartbeat_interval_ms = 50

[tls]
cert_chain = "/etc/lattice/certs/server.pem"
private_key = "/etc/lattice/certs/server-key.pem"
client_ca = "/etc/lattice/certs/ca.pem"

[adapters.etcd]
enabled = true
listen_addr = "0.0.0.0:2379"

[logging]
level = "info"
format = "json"
"#
    .to_string()
}

fn generate_prod_template() -> String {
    r#"# Lattice Production Configuration

[server]
node_id = "node-1"
bind_addr = "0.0.0.0:2379"
admin_addr = "0.0.0.0:9090"
cluster_name = "lattice-prod"

[storage]
data_dir = "/var/lib/lattice"
wal_dir = "/var/lib/lattice/wal"
snapshot_dir = "/var/lib/lattice/snapshots"
compaction_batch_size = 1000
compaction_interval_secs = 3600

[cluster]
peers = ["node-2:2380", "node-3:2380"]
election_timeout_ms = 300
heartbeat_interval_ms = 30

[tls]
cert_chain = "/etc/lattice/certs/server.pem"
private_key = "/etc/lattice/certs/server-key.pem"
client_ca = "/etc/lattice/certs/ca.pem"
require_client_cert = true
min_tls_version = "1.3"

[security]
require_mtls = true
audit_logging = true

[adapters.etcd]
enabled = true
listen_addr = "0.0.0.0:2379"
max_request_bytes = 1572864

[quotas]
max_keys_per_tenant = 10000000
max_value_size = 1572864
max_requests_per_second = 10000

[logging]
level = "info"
format = "json"
output = "/var/log/lattice/server.log"
"#
    .to_string()
}

fn diff_configs(file1: &PathBuf, file2: &PathBuf) -> Result<()> {
    if !file1.exists() {
        anyhow::bail!("File not found: {:?}", file1);
    }
    if !file2.exists() {
        anyhow::bail!("File not found: {:?}", file2);
    }

    let content1 = std::fs::read_to_string(file1)?;
    let content2 = std::fs::read_to_string(file2)?;

    let config1: toml::Value = toml::from_str(&content1)?;
    let config2: toml::Value = toml::from_str(&content2)?;

    println!("Comparing {:?} and {:?}", file1, file2);
    println!();

    diff_toml_values("", &config1, &config2);

    Ok(())
}

fn diff_toml_values(path: &str, v1: &toml::Value, v2: &toml::Value) {
    match (v1, v2) {
        (toml::Value::Table(t1), toml::Value::Table(t2)) => {
            // Keys in t1 but not in t2
            for key in t1.keys() {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };

                match t2.get(key) {
                    Some(v2_val) => {
                        diff_toml_values(&new_path, &t1[key], v2_val);
                    }
                    None => {
                        println!("- {}: {:?}", new_path, t1[key]);
                    }
                }
            }

            // Keys in t2 but not in t1
            for key in t2.keys() {
                if !t1.contains_key(key) {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    println!("+ {}: {:?}", new_path, t2[key]);
                }
            }
        }
        (v1, v2) if v1 != v2 => {
            println!("~ {}: {:?} -> {:?}", path, v1, v2);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_dev_template() {
        let template = generate_dev_template();
        assert!(template.contains("[server]"));
        assert!(template.contains("node_id"));
        assert!(template.contains("debug"));
    }

    #[test]
    fn test_generate_prod_template() {
        let template = generate_prod_template();
        assert!(template.contains("[server]"));
        assert!(template.contains("[tls]"));
        assert!(template.contains("require_client_cert"));
        assert!(template.contains("[security]"));
    }

    #[test]
    fn test_generate_staging_template() {
        let template = generate_staging_template();
        assert!(template.contains("[server]"));
        assert!(template.contains("[tls]"));
        assert!(template.contains("staging"));
    }
}

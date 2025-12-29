# Lattice Deployment Guide

This document covers production deployment of Lattice, including configuration, TLS setup, and operational considerations.

## Prerequisites

- Linux x86_64 or ARM64 host
- Rust 1.75+ (for building from source)
- Valid TLS certificates (mTLS required in production)
- Network access to clustor control plane nodes

## Building

```bash
# Development build
make build

# Release build with optimizations
make build-release

# Run tests
make test

# Check formatting and lints
make lint
```

## Configuration

Lattice uses TOML configuration files. See `config/lattice.toml` for a complete reference.

### Minimal Configuration

```toml
[control_plane]
mode = "clustor"
endpoints = ["https://cp1:2379", "https://cp2:2379", "https://cp3:2379"]
cache_ttl_seconds = 300

[listeners.grpc]
bind = "0.0.0.0:2379"
tls_chain_path = "/etc/lattice/tls/server.crt"
tls_key_path = "/etc/lattice/tls/server.key"
client_ca_path = "/etc/lattice/tls/ca.crt"

[durability]
mode = "quorum"
quorum_size = 2

[paths]
data_dir = "/var/lib/lattice/data"
wal_dir = "/var/lib/lattice/wal"
```

### Configuration Sections

#### Control Plane

```toml
[control_plane]
# Mode: "standalone" for development, "clustor" for production
mode = "clustor"

# CP-Raft endpoints
endpoints = ["https://cp1:2379", "https://cp2:2379", "https://cp3:2379"]

# Cache TTL for CP data (manifests, routing, policies)
cache_ttl_seconds = 300

# Grace period before treating cached data as stale
cache_grace_ms = 30000
```

#### Listeners

```toml
[listeners.grpc]
# Bind address for etcd v3 gRPC
bind = "0.0.0.0:2379"

# TLS configuration (required in production)
tls_chain_path = "/etc/lattice/tls/server.crt"
tls_key_path = "/etc/lattice/tls/server.key"
client_ca_path = "/etc/lattice/tls/ca.crt"

# Minimum TLS version
min_tls_version = "1.2"

# Maximum concurrent connections
max_connections = 10000

# Connection idle timeout
idle_timeout_ms = 300000
```

#### Durability

```toml
[durability]
# Mode: "sync", "quorum", "async"
mode = "quorum"

# Quorum size for quorum mode
quorum_size = 2

# fsync policy
fsync_policy = "always"
```

#### Tenants

```toml
[tenants]
# Default tenant for connections without explicit tenant ID
default_tenant = "default"

# Per-tenant QPS limits
default_qps_limit = 1000

# Per-tenant bytes/second limits
default_bytes_limit = 104857600

# Lease TTL bounds
lease_ttl_default_ms = 60000
lease_ttl_max_ms = 86400000
```

#### Telemetry

```toml
[telemetry]
# Enable metrics export
enabled = true

# Prometheus metrics endpoint
metrics_bind = "0.0.0.0:9090"

# Log level: error, warn, info, debug, trace
log_level = "info"

# JSON log output
json_logs = true
```

## TLS Setup

Lattice requires mTLS for production deployments.

### Generate CA and Certificates

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
    -subj "/CN=Lattice CA"

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/CN=lattice.example.com"
openssl x509 -req -days 365 -in server.csr \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt \
    -extfile <(printf "subjectAltName=DNS:lattice.example.com,DNS:localhost,IP:127.0.0.1")

# Generate client certificate
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
    -subj "/CN=client@example.com"
openssl x509 -req -days 365 -in client.csr \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client.crt
```

### Certificate Rotation

Lattice monitors certificate files for changes and reloads automatically. To rotate certificates:

1. Generate new certificates
2. Replace certificate files atomically (rename)
3. Lattice detects change and reloads within 30 seconds

## Running

### Systemd Service

Create `/etc/systemd/system/lattice.service`:

```ini
[Unit]
Description=Lattice Key-Value Store
After=network.target

[Service]
Type=simple
User=lattice
Group=lattice
ExecStart=/usr/local/bin/lattice start --config /etc/lattice/lattice.toml
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

### Container

```bash
docker run -d \
    --name lattice \
    -p 2379:2379 \
    -p 9090:9090 \
    -v /etc/lattice:/etc/lattice:ro \
    -v /var/lib/lattice:/var/lib/lattice \
    lattice:latest start --config /etc/lattice/lattice.toml
```

## Verifying Deployment

### Health Check

```bash
# Liveness
curl -k https://localhost:2379/healthz

# Readiness (includes CP cache status)
curl -k https://localhost:2379/readyz
```

### Metrics

```bash
# Prometheus metrics
curl http://localhost:9090/metrics
```

### etcd Client Test

```bash
# Using etcdctl with mTLS
ETCDCTL_API=3 etcdctl \
    --endpoints=https://localhost:2379 \
    --cacert=/etc/lattice/tls/ca.crt \
    --cert=/etc/lattice/tls/client.crt \
    --key=/etc/lattice/tls/client.key \
    put foo bar

ETCDCTL_API=3 etcdctl \
    --endpoints=https://localhost:2379 \
    --cacert=/etc/lattice/tls/ca.crt \
    --cert=/etc/lattice/tls/client.crt \
    --key=/etc/lattice/tls/client.key \
    get foo
```

## Troubleshooting

### Common Issues

#### LIN-BOUND Failures

If operations fail with `UNAVAILABLE` and "linearizability unavailable":

1. Check CP-Raft connectivity: `curl https://cp1:2379/health`
2. Verify CP cache is not stale: check `lattice_cp_cache_state` metric
3. Check if node is leader: only leaders can serve linearizable reads

#### Routing Epoch Mismatch

If operations fail with `FAILED_PRECONDITION` and "routing epoch changed":

1. Client is using stale routing information
2. Retry with updated routing metadata
3. Check for recent CP-Raft manifest updates

#### Watch Stream Disconnects

If watches disconnect frequently:

1. Check network stability
2. Verify `watch_idle_timeout_ms` configuration
3. Enable progress notifications: `progress_notify=true`

### Logs

```bash
# View logs with systemd
journalctl -u lattice -f

# Log levels
lattice start --config /etc/lattice/lattice.toml --log-level debug
```

### Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `lattice_lin_bound_can_linearize` | LIN-BOUND availability | < 1 for > 30s |
| `lattice_cp_cache_state` | CP cache freshness | != Fresh for > 60s |
| `lattice_adapter_etcd_requests_total` | Request rate | Varies |
| `lattice_adapter_etcd_errors_total` | Error rate | > 1% of requests |
| `lattice_kv_revision` | Current revision | Stalled growth |
| `lattice_watch_active_streams` | Active watches | Capacity planning |
| `lattice_lease_active_leases` | Active leases | Capacity planning |

## See Also

- [High Availability](high_availability.md) - HA deployment patterns
- [Performance](performance.md) - Tuning and latency targets
- [Interoperability](interop.md) - etcd client compatibility

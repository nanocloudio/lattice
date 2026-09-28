# etcd surface

The `etcd_edge_anchor` module serves the etcd v3 gRPC API over HTTP/2.
Sources: `modules/app/etcd_edge_anchor/mod.rs` (HTTP/2, dispatch, the
Maintenance/Cluster/Auth services), `modules/common/etcd_codec.rs`
(protobuf and gRPC framing), `modules/common/etcd_auth.rs` (users,
roles, permissions, passwords). Leases are served by `lease_manager`,
watches by `watch_registry` and `watch_fanout`, the data by
`kv_state_worker` through `kv_request_router`.

It is verified against the reference server: `tools/e2e/etcd_diff.sh`
runs one etcdctl command matrix against lattice and `/usr/bin/etcd` and
compares the outputs line for line (52 cases: KV, Txn, leases, watch
with prev_kv, compaction, snapshot, cluster, maintenance, auth).

## Graphs

| Graph | What it adds |
|---|---|
| `configs/single-minimal-etcd.yaml` | KV and Txn on the memory store |
| `configs/single-minimal-etcd-lease.yaml` | Leases |
| `configs/single-minimal-etcd-watch.yaml` | Watches |
| `configs/single-etcd-watch-disk.yaml` | The disk store: history, so a Range at a past revision and a watch from a past revision are served |
| `configs/single-minimal-etcd-tls.yaml` | etcd behind TLS |
| `configs/single.yaml` | The full composition: etcd and Redis behind TLS on one consensus group with a WAL |

The anchor binds `listen_port` (default 2379):

```yaml
  - name: etcd_edge_anchor
    params:
      listen_port: 2379
```

## Transport

Plaintext HTTP/2, or TLS with fluxor's `tls` module in front of the
anchor — one `tls` instance per TLS anchor, since an instance claims the
connections of the one listener its anchor binds:

```yaml
  - name: tls
    mode: 1
    verify_peer: 0
    cert_file: "certs/dev/server.der"
    key_file: "certs/dev/server.key.der"
```

`tools/dev-certs.sh` mints a development CA and server certificate into
`certs/dev/`. Client-certificate authentication (etcd's
`--client-cert-auth`) is `verify_peer: 1` with `peer_auth: 2` and the CA
as `trust`. `tools/e2e/etcd_tls.sh` drives the TLS graph.

## Supported RPCs

| Service | RPCs |
|---|---|
| KV | Range, Put, DeleteRange, Txn, Compact |
| Watch | Watch (create, cancel, progress; filters, ranges, `start_revision`, `prev_kv`, fragment-free) |
| Lease | LeaseGrant, LeaseRevoke, LeaseKeepAlive, LeaseTimeToLive, LeaseLeases |
| Maintenance | Status, Alarm, Defragment, Hash, HashKV, Snapshot, MoveLeader; Downgrade is refused |
| Cluster | MemberList; MemberAdd/Update are refused, MemberRemove/Promote answer etcd's refusals |
| Auth | AuthEnable, AuthDisable, AuthStatus, Authenticate, User\*, Role\* |

The etcd `v3lock` and `v3election` gRPC services are not served;
etcdctl's `lock` and `elect` use the client-side recipes over KV, Lease
and Watch, which are. Anything unserved answers status 12
(UNIMPLEMENTED).

## Semantics, and where they differ from etcd

- **One member.** Lattice's membership and replication are its graph's
  (clustor), not etcd's: MemberList names the one anchor ("default"),
  MoveLeader accepts only it, and MemberAdd/Update are refused.
- **Consistency.** A Range is linearizable unless it sets
  `serializable`, as in etcd: on a consensus graph the router serves it
  behind the ReadIndex fence; a serializable Range reads the local
  applied state. (A graph without the fence serves every read locally.)
- **Revisions and history.** Every write takes one revision (a Txn one
  for all its writes). The disk store keeps history: Range at a
  revision, watches from a past revision (replayed with each event's
  metadata and `prev_kv`, deletes as deletes), and Compact (reads below
  it are refused at once; the physical reclamation follows). The memory
  store keeps none, and answers such requests as compacted.
- **Leases.** Server-assigned IDs are positive. A lease that is revoked
  or expires deletes its keys at one revision, with watch events.
  Revoking needs write permission on every attached key, and
  TimeToLive with keys read permission on each (under auth).
- **Auth.** Users and roles are stored in the store (a reserved keyspace),
  so they persist and replicate with the data. Passwords are
  PBKDF2-HMAC-SHA-256 with a random salt. Tokens are etcd "simple"
  tokens (5-minute TTL, refreshed on use), held per anchor; the anchor
  reads the `token` or `authorization` metadata. Enforcement follows
  etcd: admin RPCs need the root role, KV and Watch need the user's
  permissions over the range (the union of their roles).
- **Alarms** are held by the anchor; a raised NOSPACE refuses writes.
- **Leases are durable.** A granted lease is written to a record (a
  reserved keyspace) before the grant is answered, and deleted after
  its keys; at start the anchor re-grants every recorded lease with its
  full TTL, as etcd extends its leases when it restarts. The anchor's
  records are read linearizably, so a restarted graph reads them after
  its log has replayed; requests wait until they are loaded.
- **Snapshot** streams the live keys at one revision in lattice's own
  entry format, zero-padded to 512-byte pages and ending with its
  SHA-256 (what etcd's clients verify). `etcdctl snapshot save` works;
  the file is not a bbolt database, so `etcdutl snapshot restore` does
  not read it.
- **Hash / HashKV** are lattice's digests of the live keys: comparable
  across lattice members holding the same state, not with etcd's.

## Large values

A value over one chunk (1969 bytes) is stored as a chunk tree and read
back whole wherever etcd answers one: a Range, a Put's `prev_kv`, a
Txn's branches, a DeleteRange's `prev_kv`, a watch event and its
`prev_kv`, and a watch replayed from history. Up to 8 such requests are
carried at once; more wait their turn. A Range sorted by value over a
large value is refused.

## Limits

| Limit | Value | Past it |
|---|---|---|
| Request body | 4608 bytes (a Put of a 4 KiB value) | "etcdserver: request is too large" (InvalidArgument) |
| Replicated write (consensus graphs) | one log entry: a command of about 2 KiB. A value over one chunk (1969 bytes) is stored as a chunk tree first, so this bounds a Txn's other content (keys, compares, small values), not a value | "etcdserver: request is too large" |
| One Range answer | 8 KiB of keys and values | "lattice: the range does not fit one response; set a limit and page" (ResourceExhausted); `count` is always the whole range, `more` means the limit cut it |
| One response | 16000 bytes | ResourceExhausted |
| Watch event | a value and its `prev_kv`, each up to about 6.3 KiB once fetched whole | the watch is canceled with the reason |
| Watch replay window | 64 KiB / 256 events | refused as compacted: the client relists |
| Users, roles | 32 each; 8 roles per user, 8 permissions per role | ResourceExhausted |
| Watches per stream | 8 | refused (created and canceled at once) |

`docs/architecture/limit_register.md` lists each with its source.

## Errors

Every refusal carries etcd's `grpc-message` text where etcd has one, so
clients print and match it as they would against etcd.

| Condition | Status | Message |
|---|---|---|
| Read below the compaction | 11 (OUT_OF_RANGE) | etcdserver: mvcc: required revision has been compacted |
| Read ahead of the store | 11 | etcdserver: mvcc: required revision is a future revision |
| Txn writing one key twice | 3 (INVALID_ARGUMENT) | etcdserver: duplicate key given in txn request |
| Put with `ignore_value` of a missing key | 3 | etcdserver: key not found |
| Unknown lease | 5 (NOT_FOUND) | etcdserver: requested lease not found |
| NOSPACE alarm raised | 8 (RESOURCE_EXHAUSTED) | etcdserver: mvcc: database space exceeded |
| No user / bad token / not permitted | 3 / 16 / 7 | etcd's auth messages |
| Store unavailable at start (auth records unread) | 14 (UNAVAILABLE) | etcdserver: the store is not answering (auth records unread) |
| No authority answered in time (a follower, an unresolved read fence) | 14 | etcdserver: request timed out |
| Routing epoch moved under the request | 14 | etcdserver: leader changed |
| Throttled or backpressured | 8 | etcdserver: too many requests |
| Operation spans ranges the graph keeps apart | 9 (FAILED_PRECONDITION) | lattice: the operation spans ranges this graph keeps apart |
| Unserved RPC | 12 (UNIMPLEMENTED) | — |

## Client configuration

```sh
etcdctl --endpoints=http://127.0.0.1:2379 put demo-key demo-value
etcdctl --endpoints=https://127.0.0.1:2379 --cacert=certs/dev/ca.pem get demo-key
etcdctl --endpoints=https://127.0.0.1:2379 --cacert=certs/dev/ca.pem --user=root:pw get demo-key
```

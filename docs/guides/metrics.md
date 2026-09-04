# Metrics surface

The `prometheus_edge_anchor` module serves a Prometheus-compatible
metrics interface: remote-write ingest plus the query endpoints a
Prometheus datasource (Grafana included) speaks. Three query dialects
— PromQL, MetricsQL, and KQL — are parsed by per-language front-ends
and lowered onto one language-agnostic engine, so every dialect
answers from the same series data with the same semantics. Sources:
`modules/app/prometheus_edge_anchor/mod.rs` (HTTP transport, series
resolution, folding), `modules/common/tsquery_core.rs` (the engine),
and `modules/common/promql_front.rs` / `metricsql_front.rs` /
`kql_front.rs` (the dialects).

Samples are ordinary records in the canonical KV store's time-series
keyspace: ingest goes through the request router like every other
write, so on a replicated graph a sample is Raft-committed before it
is acknowledged, and queries read the same applied state as every
other surface.

## Endpoints

| Route | Method | Purpose |
|---|---|---|
| `/api/v1/write` | POST | Remote-write ingest: a Snappy-framed protobuf `WriteRequest`. Each series' identity and samples become a series-directory record and sample puts in one transaction. |
| `/api/v1/query_range` | GET | PromQL/MetricsQL `query` evaluated over a `start`/`end`/`step` grid (seconds; `step` defaults to 15s); answers a Prometheus `matrix`. |
| `/api/v1/query` | GET | Instant form: evaluates at `time`; answers a `vector`. |
| `/api/v1/kql` | GET | A KQL `query` over the same `start`/`end`/`step` parameters; answers the same Prometheus JSON. |
| `/api/v1/labels` | GET | Distinct label names across the series directory. |
| `/api/v1/series` | GET | Series discovery for a `match[]` selector: the matching label sets. |
| `/-/healthy` | GET | Liveness; `200 ok`. |

Responses use the standard Prometheus envelope
(`{"status":"success","data":{...}}`); errors are JSON with an HTTP
status and a reason naming the refusal.

## Query dialects

Every dialect lowers onto the same three-operation core: series
selection (metric name plus label matchers), a per-series temporal
fold over a time window, then an optional cross-series aggregation
grouped by labels. Anything a front-end cannot express in those
operations is **refused by name** with `400`, never silently
mis-evaluated.

**PromQL** (`promql_front.rs`):

```text
<agg> ( <selector-or-rate> ) [by|without (<labels>)]
<selector-or-rate>
rate ( <selector> [ <duration> ] )
<selector> := metric | metric{ l op "v", ... }
<agg>      := sum | avg | min | max | count
op         := = | !=
duration   := <int>(ms|s|m|h)
```

The grid comes from the API parameters, not the expression; a
`rate(...)` duration sets the per-step lookback window. Outside the
subset — subqueries, `@`/`offset`, regex matchers, binary-operator
trees, `histogram_quantile`, nested functions — the query is refused.

**MetricsQL** (`metricsql_front.rs`) is accepted on the same
endpoints as PromQL, as a superset: a rollup like `rate(m)` without
an explicit `[range]` is legal and the lookback defaults to `5m`.
Everything else behaves exactly as PromQL.

**KQL** (`kql_front.rs`), on `/api/v1/kql`:

```text
<table> [ | where <pred> [and <pred>]* ]
         [ | summarize <agg>(<col>) [by <keys>] ]
<pred> := <ident> (== | !=) "string"
<agg>  := sum | avg | min | max | count
```

The table name is the metric; `where` lowers to label matchers;
`summarize ... by` is the cross-series aggregation; a
`bin(Timestamp, ...)` group key is the time axis and is satisfied by
the API grid. `join`, `mv-expand`, `project`, `extend`, string
operators, and non-metric tables are refused.

## Bounds

Queries are bounded work end to end; the ceilings and their
exhaustion behaviour are in the
[limit register](../architecture/limit_register.md). Three are worth
knowing when sizing dashboards: a query matching more than 32 series
is refused (`too many series`), a grid may have at most 128 steps
(`query grid too large`), and a single series' window denser than
2048 samples is refused (`window too dense`) — tighten the matchers,
narrow the range, or coarsen the step. Exhaustion is always a
refusal, never a silently partial answer.

## Configuration

The anchor is enabled by listing `prometheus_edge_anchor` in the
graph config with its `listen_port` param (default 9090), wired to a
`kv_request_router` ingress pair. `configs/single-metrics.yaml` is
the canonical graph, validated end to end by the live harness with
real remote-write bodies and dashboard-shaped queries in all three
dialects.

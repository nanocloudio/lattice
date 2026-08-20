# Tools

Host-side helpers that live alongside the PIC module tree but never run on
`fluxor`. One directory per role: a file is named for what it is, its directory
for what it is *for* — the same convention wave uses.

The language split is deliberate. Rust holds the tools that parse structured
input — the wire and telemetry catalogs, the load generator's codecs — where a
real parser earns its keep.

| Directory | Role |
| --- | --- |
| `load/` | Throughput: the off-DUT open-loop load generator |

## `load/`

`lattice-bench/` is the off-DUT load driver (`lattice-loadgen` +
`lattice-scrape`). It runs on the driver host, never on `fluxor`, and is
dependency-free by design so it builds on an offline Pi — networking is raw
`std::net`, and the JSON, RESP client, and latency histogram are hand-rolled.
Like wave's equivalent, it shares **no code** with the database it
drives: a load driver built from the DUT's own parser cannot fail on a shared
codec defect, because the bug cancels out on both sides and the run goes green.

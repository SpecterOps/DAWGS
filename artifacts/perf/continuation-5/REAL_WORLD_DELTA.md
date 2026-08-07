# Real-world live-v2 to continuation-5 delta

Date: 2026-08-07

## Verdict

`sp-static-v3` is not qualified for release on this dataset as implemented.
It fixes the catastrophic hidden-fan-in cases, but the containment boundary is
too broad for direct inbound searches and the existing-graph read-only session
cannot initialize `SP-S0`'s temporary workspace.

The restored graph matched the frozen baseline before the run and retained the
same cardinalities afterward: graph 24 (`default`), 1,845,833 nodes,
44,133,029 edges, and 8,742,373 `MemberOf` edges.

## Protocol

The preserved live-v2 harness and its exact 147 stable case names, anchors,
timeouts, warmups, and adaptive sample counts were rerun against the same
PostgreSQL database. The original `results.jsonl` is the baseline.

The strict ordinary run kept `default_transaction_read_only=on`. It exposed
22 `SP-S0` initialization errors (`DROP TABLE` is prohibited in a read-only
transaction) and one timeout. The unchanged 16 fallback controls ran through
the baseline's guarded temporary-workspace session. A second, explicitly
diagnostic guarded run measured only the 23 strict failures/timeouts so search
latency could be separated from the read-only integration defect.

The composite performance view substitutes those guarded records only for the
23 strict failures. It is not a release pass.

## Matched result

The diagnostic composite has all 147 baseline keys:

| Status | Count |
|---|---:|
| `ok` | 142 |
| `timeout` | 2 |
| `unsupported` | 2 |
| `expected_error` | 1 |

Among 142 comparable successful medians, 40 improved by at least 20%, 32
regressed by at least 20%, and 70 stayed within 20%. Median case ratios by
family (current/baseline) were: shortest 0.928, horizontal 0.881,
materialization 0.961, ADCS 0.963, count 1.007, and fallback 1.003.

## Shortest-path deltas

| Case | Baseline | Current | Ratio | Result |
|---|---:|---:|---:|---|
| Outbound F987 distance | 1.492 ms | 1.867 ms | 1.251 | 25% regression |
| Outbound F987 path | 1.754 ms | 1.575 ms | 0.898 | stable/improved |
| Direct inbound F128 distance | 0.462 ms | 5.777 ms | 12.50 | over-contained |
| Direct inbound F1,025 path | 2.279 ms | 11.617 ms | 5.10 | over-contained |
| Hidden-fan-in D3 distance | 117.998 ms | 7.356 ms | 0.062 | 16.0x faster |
| Hidden-fan-in D3 path | 154.445 ms | 9.472 ms | 0.061 | 16.3x faster |
| Hidden-fan-in D64 distance | 596.545 ms | 7.407 ms | 0.012 | 80.5x faster |
| Hidden-fan-in D64 path | 646.992 ms | 9.186 ms | 0.014 | 70.4x faster |
| Parallel K1/D1 distance | 236.017 ms | 227.405 ms | 0.964 | stable |
| Parallel K1/D1 path | 220.175 ms | 216.817 ms | 0.985 | stable |
| Parallel K7/D2 distance | 2,387.204 ms | 2,388.589 ms | 1.001 | unchanged |
| Parallel K7/D2 path | 8,070.438 ms | 13,120.538 ms | 1.626 | 63% regression |

The two status regressions were:

- `all_shortest_diamond_paths`: 462.323 ms baseline to a five-second timeout;
- `shortest_parallel_path_k7_d1`: 989.414 ms baseline to a five-second
  timeout in the guarded v3 run.

Containment therefore solves the original hidden-intermediate fan-in defect,
but using `SP-S0` for every physical-inbound cap greater than one sacrifices
the previously qualified direct-inbound envelope. Multi-kind singleton path
fallback also removes the former S3 latency advantage without solving the
absolute resource problem.

## Concurrency delta

All 18 guarded concurrency records completed successfully. The decisive
changes at concurrency four were:

| Case | Baseline QPS | Current QPS | QPS ratio | Baseline p95 | Current p95 |
|---|---:|---:|---:|---:|---:|
| Outbound F987 path | 1,804 | 1,917 | 1.06 | 2.866 ms | 2.903 ms |
| Direct inbound F1,025 path | 1,652 | 282 | 0.17 | 3.076 ms | 16.316 ms |
| Outbound true-depth path | 5,267 | 5,169 | 0.98 | 1.072 ms | 1.220 ms |
| Hidden-fan-in D64 path | 4.29 | 323.99 | 75.6 | 947.154 ms | 14.764 ms |
| Outbound F987 full rows | 218 | 221 | 1.01 | 20.840 ms | 19.129 ms |
| Inbound F1,025 full rows | 117 | 122 | 1.04 | 37.544 ms | 35.826 ms |

The hidden-fan-in concurrency recovery is substantial, but direct-inbound
throughput falls by 83% because the static selector cannot distinguish a cheap
one-hop result from dangerous downstream reverse fan-in using query shape
alone.

## Non-shortest controls

Counts and ADCS were essentially unchanged. The all-node count moved from
145.763 to 146.855 ms, and `MemberOf` count from 1,878.504 to 1,893.230 ms.
Hydrating 1,000 indexed nodes improved from 6.743 to 4.634 ms; the 1,000-user
full-node scan was stable at 20.288 versus 19.502 ms.

## Required disposition

1. Do not call the current existing-graph strict protocol complete while
   production fallback requires temporary DDL that the read-only GUC rejects.
2. Do not activate blanket deep-inbound containment without a direct-inbound
   exception or a bounded topology/runtime decision that passes regret gates.
3. Keep multi-kind singleton path on an explicitly rejected/closed boundary
   until a non-spilling candidate or accepted fallback latency envelope exists.
4. Preserve the hidden-fan-in containment evidence: it fixes the principal
   live-v2 failure and should not be lost when refining the boundary.

## Artifacts

| Artifact | SHA-256 |
|---|---|
| `real-world-live-v3-ordinary.jsonl` | `439c16f643511ff1480e114d996d1c3492203c01c0698ef2eff09fb4cdc619db` |
| `real-world-live-v3-contained-temp.jsonl` | `0b187e84148030c2a0148a87e79a62c309f2f33f6e99bb7112a9f00d2c54cf8e` |
| `real-world-live-v3-fallback.jsonl` | `3632b4b1fec57170bd7ae4a9b4320c267457277a9fb3187a8083027a2d597368` |
| `real-world-live-v3-delta.json` | `3ac2abf7a303211eaaa1ee6c5bb217a43bc83dd6ecbc831c2b173eeb4c480ce5` |
| `real-world-live-v3-concurrency.jsonl` | `65dc4521d4e81b01c5c9e4b0a6d13094e54267e042d3c64bee14c4693a4752a3` |
| `real-world-live-v3-concurrency-delta.json` | `9218548381a6ff71bfc0e794db1b20954f184b09a7bbac18754738cf911ab9e6` |

No connection string or credential is present in these artifacts.

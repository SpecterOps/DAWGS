# Continuation-5 follow-up qualification

Date: 2026-08-07

## Verdict

The strict read-only workspace defect is fixed and qualified on PostgreSQL.
`SP-S0-DIRECT` is exact on the generated direct-hit and incumbent-fallback
boundaries, materially improves direct multi-kind searches, remains stable on
hidden-fan-in fallback, passes concurrency and a 10,000-operation-per-case
soak, and emits no candidate spill, local workspace, or WAL on direct hits.

Keep `SP-S0-DIRECT` tool-only until the 147-case restored real-world dataset is
available for repeated fixed confirmation. The real-world dataset was dropped
before this follow-up, so the prior live-v2 delta cannot be superseded. Keep
S4 and ASP-A1 reference-only for the same reason and because they do not yet
have executable-candidate concurrency, cancellation, and soak evidence.

## Validation

- PostgreSQL `make test_all`: pass, using the reachable IPv4 loopback endpoint.
- Neo4j `make test_all`: pass.
- PostgreSQL forced direct plan invariants: exact direct inbound and multi-kind
  paths; `bidirectional_sp_harness` `Actual Loops = 0` on direct hits and
  positive loops on fallback.
- Existing-graph fixed confirmation: four of four cases `ok` under strict
  `default_transaction_read_only=on`, including concurrency 1/4/8; graph
  cardinality remained 183 nodes and 276 edges.
- Existing-graph string redaction scan: no connection string, credential, or
  physical anchor ID in durable records or checkpoint.

## Direct preflight comparison

The matched diagnostic used five warmups, 20 measured samples, pool size four,
and concurrency 1/4/8. The baseline explicitly forced exact `SP-S0`; the
candidate forced `SP-S0-DIRECT`.

| Case | SP-S0 median | Direct median | Ratio | QPS ratio at c4 | QPS ratio at c8 |
|---|---:|---:|---:|---:|---:|
| Hidden fan-in distance | 1.308 ms | 1.336 ms | 1.02 | 1.07 | 0.94 |
| Hidden fan-in path | 1.935 ms | 1.844 ms | 0.95 | 1.18 | 0.93 |
| Parallel-kind direct distance | 0.957 ms | 0.071 ms | 0.074 | 5.24 | 9.51 |
| Parallel-kind direct path | 1.463 ms | 0.702 ms | 0.48 | 2.74 | 2.99 |

At concurrency eight, hidden-fan-in fallback stayed within 7% of incumbent
throughput. Direct distance improved from 1,694 to 16,104 QPS and direct path
from 1,373 to 4,100 QPS. The direct arm resource report passes: hidden-fan-in
records are truthfully attributed to exact `SP-S0` fallback, while direct-hit
records show no local workspace use.

## Soak

Each direct-arm case completed 10,000 measured operations after 20 warmups:

| Case | Median | p95 | p99 | Max | Status |
|---|---:|---:|---:|---:|---|
| Hidden fan-in distance | 1.427 ms | 1.793 ms | 2.124 ms | 3.245 ms | `ok` |
| Hidden fan-in path | 2.014 ms | 2.446 ms | 2.943 ms | 3.649 ms | `ok` |
| Parallel-kind direct distance | 0.073 ms | 0.146 ms | 0.253 ms | 0.600 ms | `ok` |
| Parallel-kind direct path | 0.681 ms | 0.870 ms | 1.096 ms | 2.036 ms | `ok` |

## S4 and all-shortest reference tournament

All reference arms returned the exact public observation and passed their own
nested resource checks. The resource gate now evaluates full-comparator
references rather than only the outer production record.

| Boundary | Incumbent | Reference | Speedup | Rows | Resource gate |
|---|---:|---:|---:|---:|---|
| Hidden fan-in distance, `SP-S4-C-D` | 1.344 ms | 0.099 ms | 13.6x | 1 | pass |
| Hidden fan-in path, `SP-S4-C-WE+MAT-M0` | 2.092 ms | 0.463 ms | 4.5x | 1 | pass |
| Diamond all-shortest, `ASP-A1-DAG` | 10.658 ms | 0.759 ms | 14.0x | 2 | pass |

These are strong architecture signals, not production activation evidence.
They require restored-data holdouts and executable-arm concurrency,
cancellation, and soak before selector changes.

## Durable artifacts

| Artifact | SHA-256 |
|---|---|
| `followup-generated-direct.jsonl` | `230839b9170f149a809e8d072e4ad5dc4bd192da66352bb607345580d212e713` |
| `followup-generated-direct-soak.jsonl` | `998a12b001f44bff50506103162e2327fa4f669e9f8499a84ff26e5ae0c95f75` |
| `followup-generated-direct-resources.json` | `4c8594aeb930694928ed990d0aa6cff7d5ba67511dad8fdb58bcfb18f778628c` |
| `followup-generated-s4-distance.jsonl` | `538f2738d019f738bfa5a267aef6bc4dd293ebd3be98b02b16567b53cb9c5455` |
| `followup-generated-s4-distance-resources.json` | `5655a21456d42496fa32be9e5bf0f50538d0787449ed9b94c0da934825bbfec8` |
| `followup-generated-s4-witness.jsonl` | `87e2eac3d96a257d5f2fe9bc3c253ae3c92f4722fb24eea210f0100c811f3d7e` |
| `followup-generated-s4-witness-resources.json` | `02a5b41e82be24b063d3b5b03dff62dc41146303eb78381c0c7201e0a3ee2e66` |
| `followup-generated-asp-a1.jsonl` | `36d6d78ae9a8833cb1038a707edc77c822ddcdb508413cfc3181cd24ad849993` |
| `followup-generated-asp-a1-resources.json` | `0266ff9a51ec5f92d578e7162b3dd42037fe763648c50dc3a6cc40fbed4c5a7f` |
| `followup-existing-readonly-v2.jsonl` | `6a610721701e0cc46a37633f9a744605f8f362624b4f1eaea1b4feaaecf77104` |

No artifact contains a supplied credential or unredacted connection string.

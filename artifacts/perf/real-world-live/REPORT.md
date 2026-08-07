# Real-world PostgreSQL benchmark qualification

Date: 2026-08-07

## Verdict

The activated shortest-path production paths are qualified on this sanitized
PostgreSQL dataset for the exercised outbound, typed, fixed-endpoint envelope.
`SP-S3-U-D` and `SP-S3-U-E+MAT-M0` were both selected and applied through the
public Cypher/driver boundary. A sampled two-hop-only pair proved that the
result is not limited to direct-edge hits: depth 1 returned no result and depth
2 or greater returned exactly one result.

This run also found two important limits. Typed relationship counting is at the
2 second cutoff on 8.74 million `MemberOf` edges and is not qualified. ADCS
cannot be performance-qualified on this dataset because the required suffix is
absent (`TrustedForNTAuth` has zero rows); the production selector correctly
retained `ADCS-INCUMBENT-STEPWISE` with `tournament_unqualified`.

No real-data Neo4j comparison was run. The sanitized dataset was available at
the configured PostgreSQL connection only. The Neo4j values below are the
existing synthetic release baseline, not measurements of this dataset.

## Dataset boundary

The `default` graph contains 1,845,833 nodes and approximately 42,876,356
relationships. Its node partition occupies 1.52 GiB including indexes and its
edge partition 16.21 GiB. Autoanalyze completed for the node partition about 25
minutes and for the edge partition about 12 minutes before capture. The node
estimate is still 3.1% below the exact count.

The relationship topology is heavily skewed: exact counts include 8,742,373
`MemberOf` and 5,732,248 `AZMemberOf` relationships. The high-fanout
`MemberOf` anchor used for the cap-stability probe has 987 direct neighbors.
The separate two-hop anchor was selected from a 0.01% physical sample and then
validated by indexed lookups.

All database access was read-only. Sessions set
`default_transaction_read_only=on`, `statement_timeout=2s`,
`lock_timeout=250ms`, and a 5 second idle-transaction timeout. The fixture
loading benchmark was deliberately not used because it clears and reloads its
target graph.

## Production-boundary latency

Medians exclude one untimed cold execution. Normally five warm executions were
retained; work above 500 ms dropped to two and work above 1 second to one.
Every individual query also had a 2.5 second client context deadline.

| Probe | Rows | Warm samples | Median | Maximum | Disposition |
|---|---:|---:|---:|---:|---|
| Indexed node ID | 1 | 5 | 0.165 ms | 0.194 ms | qualified |
| High-fanout distance, cap 16 | 1 | 5 | 1.697 ms | 2.016 ms | qualified |
| High-fanout path, cap 16 | 1 | 5 | 2.700 ms | 3.228 ms | qualified |
| Two-hop-only distance, cap 1 | 0 | 5 | 0.425 ms | 0.666 ms | correct miss |
| Two-hop-only distance, cap 2 | 1 | 5 | 0.386 ms | 0.405 ms | qualified |
| Two-hop-only path, cap 1 | 0 | 5 | 0.459 ms | 0.493 ms | correct miss |
| Two-hop-only path, cap 2 | 1 | 5 | 0.642 ms | 1.054 ms | qualified |
| `AZMemberOf` distance, cap 8 | 1 | 5 | 0.983 ms | 1.329 ms | qualified |
| `AZMemberOf` path, cap 8 | 1 | 5 | 1.734 ms | 1.891 ms | qualified |
| ADCS missing-suffix control | 0 | 5 | 15.841 ms | 16.503 ms | semantic control only |
| All-node count | 1 aggregate | 5 | 165.624 ms | 176.368 ms | scale-sensitive |
| Typed user count | 1 aggregate | 5 | 116.561 ms | 124.180 ms | scale-sensitive |
| Typed group count | 1 aggregate | 5 | 92.260 ms | 96.322 ms | scale-sensitive |
| `MemberOf` count | 1 aggregate | 1 | 2,000.168 ms | 2,000.168 ms | not qualified; cutoff-bound |

The relationship count succeeded once at the statement-timeout boundary in the
consolidated run and timed out in the preceding pass. It is classified as a
timeout/cutoff result, not a stable 2 second benchmark.

## Plan evidence

`EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF, FORMAT JSON)` was run
once per selected probe under the same read-only 2 second statement limit.

| Probe | Planning | Execution | Recursive rows | Shared hit/read blocks | Temp blocks | WAL records |
|---|---:|---:|---:|---:|---:|---:|
| All-node count | 0.167 ms | 148.317 ms | 0 | 38,266 / 230,131 | 0 | 0 |
| High-fanout distance, cap 16 | 0.467 ms | 2.094 ms | 988 | 3,590 / 384 | 0 | 0 |
| Two-hop-only path, cap 8 | 1.373 ms | 0.339 ms | 27 | 107 / 58 | 0 | 0 |
| ADCS missing-suffix control | 10.442 ms | 7.359 ms | 988 | 13,318 / 1,437 | 0 | 0 |

Shortest-path expansion used
`edge_24_start_id_kind_id_id_end_id_idx`; endpoint and hydration work used the
node and edge primary keys. The two-hop plan performed 29 edge-index loops and
did not spill. The high-fanout plan performed 988 recursive rows and remained
under 2.1 ms of server execution. This directly supports the production-path
qualification.

The all-node count is a parallel index-only scan of the complete node primary
key with four workers plus the leader, not a metadata/count-store operation. It
touches roughly 2.05 GiB of 8 KiB blocks and explains why synthetic count
timings do not extrapolate to this dataset.

## Comparison with the synthetic release corpus

The comparison is diagnostic only: graph size, topology, cache state, and
server state differ. Synthetic values are the median of the five retained
round medians in the checksum-bound cumulative release corpus.

| Shape | Real PG median | Synthetic PG | Real / synthetic PG | Synthetic Neo4j |
|---|---:|---:|---:|---:|
| Two-hop-only D2 distance | 0.386 ms | 0.214 ms | 1.80x | 1.001 ms |
| Two-hop-only D2 path | 0.642 ms | 0.375 ms | 1.71x | 0.978 ms |
| High-fanout D16 distance | 1.697 ms | 0.278 ms | 6.10x | 0.987 ms |
| High-fanout D16 path | 2.700 ms | 0.504 ms | 5.36x | 1.061 ms |
| All-node count | 165.624 ms | 0.092 ms | 1,806x | 0.616 ms |
| Typed edge count | cutoff at ~2,000 ms | 0.056 ms | at least 35,800x | 0.596 ms |

The shortest-path production gains survive the real topology, although the
high-fanout anchor is 5-6x slower than the small synthetic PostgreSQL fixture.
Absolute latency remains below 3 ms median for path materialization. Count
queries are the clear scale gap and should not inherit conclusions from the
small release fixture.

## Remaining gaps and next gates

- Load the identical sanitized graph into Neo4j before claiming a real-data
  backend delta. Cross-backend synthetic numbers are context only.
- Add a count-store, maintained summary, or other explicitly consistent count
  strategy if large typed counts are part of the production objective. The
  current scan is cutoff-bound.
- Qualify inbound shortest paths, disconnected high-fanout searches, deeper
  true paths, ties, and cycles from real anchors. The synthetic semantic corpus
  covers those shapes, but this dataset pass exercised outbound reachable
  paths only.
- ADCS requires data with a complete exact suffix and both sparse and
  high-reverse-fan-in controls. This dataset cannot reopen the closed A3
  selector gate.
- Repeat under controlled cold-cache and concurrency conditions if deployment
  capacity, rather than single-session warm latency, is the decision target.

## Artifact manifest

| Artifact | SHA-256 |
|---|---|
| `dataset.json` | `f09198dbfa2190a5afd12e929cc1a7c8e83e2fa8cfbf65d775f30e170a4dc824` |
| `harness.go.txt` | `1905795be50e9333f1aff6423bb68fb98cd5034dcb6bd160a62dbb3e809912a1` |
| `postgres-results.jsonl` | `c29a5c17daed53e41e5b95e9afc4ed0c933108b0a21e52496eaaa44b3e6ca6cf` |
| `postgres-plans.jsonl` | `3ff58b97ca743fd4b9a96b5c6a85ae4d418fb65a2c6b7c0b6e8505b80496e6ec` |
| Synthetic cumulative corpus | `b3a0e81e603ff6424ae87a26b1745b61b90d02bfa25df7bbb85037035e42c0d6` |
| Production-lift final report | `f728e2c82d2f8da095e093f5581444963a0c541e047aa64f7c746d6f00a1f19a` |

The dataset metadata is separately bound by schema signature MD5
`3bab9deff6fea785a5914601b6a2d8af`; it intentionally contains no connection
credentials.

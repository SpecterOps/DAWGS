# Expanded real-world PostgreSQL benchmark qualification

Date: 2026-08-07

## Verdict

The expanded dataset pass narrows the earlier qualification. The activated
shortest-path executors remain strong for outbound fanout, direct inbound
fan-in, ordinary true-depth paths, missing endpoints, and disconnected
searches. They are **not fully qualified for this real dataset**, however:

- A three-hop inbound path crosses a node with 170,593 incoming `MemberOf`
  relationships. `SP-S3-U-D`/`SP-S3-U-E+MAT-M0` are 18-20x slower than `SP-S0`
  at cap 3 and 75-78x slower at cap 64.
- A seven-kind, edge-distinct path at cap 2 preserves 9.53 million recursive
  states, takes 8.07 seconds end to end (8.96 seconds in the instrumented
  plan), and spills 48,380/114,253 temp blocks read/written. It is still faster
  than the 12.25 second incumbent, but fails the normal-tier absolute-latency
  and no-spill gates.
- `allShortestPaths` remains expensive: a ten-path `MemberOf` diamond is
  462 ms median and seven parallel one-hop paths are 8.15 seconds median.

The practical release implication is that the current static selector cannot
infer intermediate reverse fan-in from query shape. Outbound activation remains
supported by this dataset; deep inbound activation needs a conservative
fallback or a separately qualified bounded runtime/topology decision.

Horizontal ID lookup and bounded hydration paths behave well. Large aggregate
counts remain scan-bound. ADCS still cannot be performance-qualified because
the dataset contains no `TrustedForNTAuth` relationship.

No real-data Neo4j comparison is claimed. Neo4j values in this report come from
the existing synthetic release corpus, not from an identical copy of this
sanitized graph.

## Scope and safety

The `default` graph contains exactly 1,845,833 nodes and 44,133,029
relationships, including 8,742,373 `MemberOf` and 5,732,248 `AZMemberOf`
relationships. The node and edge partitions occupy 1.52 GiB and 16.21 GiB,
including indexes.

Anchor discovery used 0.01-1.0% physical samples, a five-second statement cap,
and indexed validation. The matrix covers:

- outbound fanout 1/16/128/439/987;
- direct inbound fan-in 1/16/128/524/1,025;
- a true depth-three path in both directions and caps through 64;
- disconnected, missing-endpoint, zero-depth, endpoint-label, self-loop, and
  materialization-projection controls;
- a ten-branch equal-length diamond;
- one/two/seven relationship kinds at caps one and two over a real parallel
  edge pair;
- ID-set lookup and hydration at 10/100/1,000 rows;
- bounded node/edge scans, one-hop hydration, aggregate counts, and ADCS
  missing-suffix controls;
- semantically equivalent `SP-S0` controls forced by an unused relationship
  variable.

Normal cases ran with `default_transaction_read_only=on`. `SP-S0` and
`allShortestPaths` require DAWGS' reusable `pg_temp.bsp_*` workspace, so those
hard-coded fallback cases ran in a separately guarded session that permitted
only temporary workspace writes. No Cypher mutation was allowed. Exact
post-run counts remained 1,845,833 nodes, 44,133,029 relationships, and
8,742,373 `MemberOf` relationships.

Fast cases used two-second caps; bounded scans and topology controls used five
seconds; counts and deliberately heavy fallback/parallel cases used fifteen
seconds. Each case had one cold diagnostic followed by up to 15 warm samples.
Warm effort dropped to five samples above 50 ms, three above 250 ms, two above
one second, and one above five seconds. Progress was emitted before every case
and after every sample.

## Matrix result

The final matrix contains 147 records:

| Status | Count | Meaning |
|---|---:|---|
| `ok` | 144 | Stable row/scalar expectations passed |
| `unsupported` | 2 | Directionless variable-length expansion; declared PostgreSQL limitation |
| `expected_error` | 1 | Min-depth-one shortest path with identical endpoints |

The 144 successful records comprise 84 selected shortest cases, 16 `SP-S0` or
all-shortest controls, 16 horizontal cases, 15 materialization cases, eight
ADCS controls, and five counts. No unexpected timeout or semantic mismatch
remained after adaptive timeout escalation. The pilot capture preserves the
initial two-second inbound and five-second parallel-path timeouts.

## Shortest-path production envelope

### Qualified real-data shapes

| Shape | Distance median | Path median | Path p95/max | Result |
|---|---:|---:|---:|---|
| Outbound F1, cap 16 | 0.474 ms | 1.988 ms | 3.138 ms | qualified |
| Outbound F128, cap 16 | 0.592 ms | 0.760 ms | 0.958 ms | qualified |
| Outbound F439, cap 16 | 0.962 ms | 1.085 ms | 1.399 ms | qualified |
| Outbound F987, cap 16 | 1.492 ms | 1.754 ms | 2.445 ms | qualified |
| Direct inbound F128, cap 16 | 0.462 ms | 0.840 ms | 1.005 ms | qualified |
| Direct inbound F1,025, cap 16 | 2.207 ms | 2.279 ms | 2.806 ms | qualified |
| Outbound true depth 3, cap 3 | 0.576 ms | 1.810 ms | 2.757 ms | qualified |
| Outbound true depth 3, cap 64 | 0.802 ms | 1.969 ms | 2.822 ms | qualified |
| Disconnected F987, cap 64 | 2.053 ms | 1.788 ms | 2.594 ms | qualified |
| Missing endpoint, cap 64 | 0.297 ms | 0.440 ms | 0.676 ms | qualified |

All selected cases above recorded `SP-S3-U-D` or
`SP-S3-U-E+MAT-M0` as both selected and applied. The F987 reachable plan
produced 988 recursive rows; the true-depth plan produced 27. Neither spilled
or emitted WAL.

### Candidate versus incumbent controls

Ratios below are production candidate / `SP-S0`; values below 1 favor the
candidate.

| Shape | Candidate median | `SP-S0` median | Ratio | Disposition |
|---|---:|---:|---:|---|
| Outbound F987 D16 distance | 1.492 ms | 10.492 ms | 0.142 | candidate wins 7.0x |
| Outbound F987 D16 path | 1.754 ms | 10.548 ms | 0.166 | candidate wins 6.0x |
| Inbound true-depth D3 distance | 117.998 ms | 6.027 ms | 19.58 | regression |
| Inbound true-depth D3 path | 154.445 ms | 8.413 ms | 18.36 | regression |
| Inbound true-depth D64 distance | 596.545 ms | 7.983 ms | 74.73 | regression |
| Inbound true-depth D64 path | 646.992 ms | 8.248 ms | 78.44 | regression |
| Parallel K1/D1 distance | 236.017 ms | 4,126.454 ms | 0.057 | candidate wins 17.5x |
| Parallel K1/D1 path | 220.175 ms | 3,887.278 ms | 0.057 | candidate wins 17.7x |
| Parallel K7/D2 distance | 2,387.204 ms | 13,302.470 ms | 0.179 | candidate wins 5.6x |
| Parallel K7/D2 path | 8,070.438 ms | 12,249.235 ms | 0.659 | candidate wins 1.5x; gate failure |

The inbound chain begins with only two incoming edges, but its second
intermediate has 170,593 incoming `MemberOf` edges. The selected D64 path plan
retains 348,667 recursive rows, performs 348,670 edge loops, and touches
1,306,199/90,493 shared hit/read blocks. The incumbent plan completes in
6.57 ms server time with 1,584/63 shared hit/read blocks. A root-degree-only
probe would therefore miss this crossover.

The parallel root has 657,302 outgoing `GenericWrite` relationships. Across all
seven selected kinds it has 2,810,036 physical outgoing edges but 657,349
distinct next nodes. Distance mode deduplicates node state; full-path mode must
preserve edge-distinct state. At K7/D2 the selected path plan reaches 9,527,404
recursive rows and 2,810,044 edge loops, explaining the 5.68 second
distance-to-path tax and temp spill.

### All-shortest fallback

| Shape | Rows | Median | Server execution | Notes |
|---|---:|---:|---:|---|
| Ten-branch `MemberOf` diamond | 10 | 462.323 ms | 379.212 ms | `SP-S0`, no temp spill |
| Seven parallel one-hop edges | 7 | 8,149.182 ms | 8,350.846 ms | `SP-S0`, absolute gap |

These cases use session-local workspace tables and are outside the activated
singleton selector envelope.

## Horizontal and materialization paths

| Shape | ID/scalar median | Full-object median | Materialization delta |
|---|---:|---:|---:|
| 1,000 indexed node IDs | 1.228 ms | 6.743 ms | +5.514 ms |
| 1,000 typed user scan rows | 1.950 ms | 20.288 ms | +18.337 ms |
| Outbound one-hop F987 | 1.384 ms | 10.860 ms | +9.476 ms |
| Inbound one-hop F1,025 | 1.312 ms | 20.721 ms | +19.409 ms |

Single-node ID lookup is 0.152 ms median. One hundred indexed IDs are 0.239 ms
and one hundred fully hydrated nodes are 1.040 ms. The bounded hydration paths
are usable, but rich real user/relationship payloads expose a much larger
client decoding tax than the synthetic fixtures.

The 1,000-user ID scan had a 1.950 ms median but a 79.839 ms maximum, so its
tail requires more repetitions before a strict p95 gate. Row order is
intentionally not asserted for these unordered scans.

## Counts and ADCS

| Probe | Median | Plan/server observation |
|---|---:|---|
| All nodes | 145.763 ms | Parallel full primary-key index scan |
| Users | 105.529 ms | Typed node scan |
| Groups | 90.124 ms | Typed node scan |
| `MemberOf` relationships | 1,878.504 ms | 2,108.775 ms instrumented execution |
| All relationships | 3,061.493 ms | One retained warm sample |

The `MemberOf` count joins all 8.74 million edges to both endpoint node
partitions. Its plan performs about 1.62 million memoized node index scans and
touches 5.94 million/1.15 million shared hit/read blocks. The small synthetic
typed-count result does not extrapolate to this topology.

ADCS endpoint/path controls remain between 10.21 and 15.31 ms median. A selected
root reaches a real `Enroll` edge, proving more suffix work than the first pass,
but global `TrustedForNTAuth` cardinality is zero. Every case correctly retains
`ADCS-INCUMBENT-STEPWISE`; none can qualify A3 on this dataset.

## Concurrency

All 665 fast-path operations and all 21 bounded slow-inbound operations
completed without error across the retained concurrency blocks.

| Shape | Concurrency | QPS | p95 |
|---|---:|---:|---:|
| Outbound F987 path | 1 / 2 / 4 | 361 / 983 / 1,804 | 4.969 / 2.804 / 2.866 ms |
| Direct inbound F1,025 path | 1 / 2 / 4 | 384 / 906 / 1,652 | 3.779 / 3.302 / 3.076 ms |
| Outbound true-depth path | 1 / 2 / 4 | 801 / 2,036 / 5,267 | 2.432 / 2.517 / 1.072 ms |
| Outbound F987 full one-hop rows | 1 / 2 / 4 | 69 / 126 / 218 | 41.467 / 22.713 / 20.840 ms |
| Inbound F1,025 full one-hop rows | 1 / 2 / 4 | 45 / 78 / 117 | 26.793 / 28.242 / 37.544 ms |
| Slow inbound D64 path | 1 / 2 / 4 | 1.55 / 2.71 / 4.29 | 686.020 / 740.674 / 947.154 ms |

The fast selected paths scale without errors in this four-connection test. The
slow inbound mutation loses latency as concurrency rises: p95 increases 38%
from one to four workers while throughput reaches only 2.77x. Its blocks used
three operations per worker to bound load; the other shortest blocks used 25.

## Synthetic-corpus comparison

This is diagnostic rather than a backend comparison. Synthetic values are the
median of five PostgreSQL round medians from the checksum-bound release corpus.

| Mutation | Real PG | Synthetic PG | Real / synthetic |
|---|---:|---:|---:|
| Inbound D8 distance | 603.776 ms | 0.268 ms | 2,252x |
| Inbound D8 path | 641.875 ms | 0.293 ms | 2,192x |
| Two-kind direct path, cap 2, distance | 1,273.983 ms | 0.347 ms | 3,672x |
| Two-kind direct path, cap 2, full path | 1,309.833 ms | 0.278 ms | 4,711x |
| Disconnected F987/F1000, cap 64 | 2.053 ms | 1.336 ms | 1.54x |
| All-node count | 145.763 ms | 0.092 ms | 1,590x |
| Typed relationship count | 1,878.504 ms | 0.056 ms | 33,660x |

The current generated shortest fixture places fanout in outbound dead ends and
does not model a low-degree inbound root whose next level has extreme reverse
fan-in. Its parallel control has fanout 16 rather than hundreds of thousands.
Those are now explicit corpus gaps, not evidence that the production paths are
uniformly safe.

## Required follow-up

1. Add generated shortest fixtures for hidden intermediate reverse fan-in and
   high-cardinality multi-kind edge-distinct state. Gate both candidate and
   `SP-S0` with p50/p95, search-state, spill, and concurrency evidence.
2. Until that gate passes, fail closed for deep inbound singleton shortest
   shapes or introduce a bounded topology-aware decision that can detect more
   than root degree. The real D64 case demonstrates that the current static
   query-shape selector is insufficient.
3. Add a state/resource guard for multi-kind full-path materialization. The
   candidate is faster than `SP-S0`, but a nine-second spilling plan is not a
   qualified production tier.
4. Keep `allShortestPaths` outside the singleton lift and pursue it as a
   separate workspace/search program.
5. Add a maintained count strategy only if exact large-graph counts are a
   production objective; the current endpoint-preserving scans are inherently
   scale-sensitive.
6. Load this exact sanitized graph into Neo4j before publishing real-data
   backend deltas.
7. Revisit ADCS only with a dataset containing a complete trust suffix plus
   sparse and high-reverse-fan-in controls.

## Validation and artifact manifest

The harness tests cover matrix uniqueness, absence of graph-mutation clauses,
adaptive sample reduction, filtering, percentile selection, and complex-value
redaction. `go test ./.coverage/read-only-live-v2` passed. Every JSON/JSONL
artifact parses, all 147 compiled cases have a matching final result, and
`git diff --check` is clean.

| Artifact | SHA-256 |
|---|---|
| `anchors.json` | `f21585a966f927d6945bd114593cfd53e84d4fde59dba844eb0394b9e8f83945` |
| `dataset.json` | `fdcb4d6d36f3eb34ab0d201a818c05a5984e50e5e896c48cade6324adb76c423` |
| `harness.go.txt` | `b025791705ea45c3b191534477bb5eb138853bc452a46fb2191e5c577663075d` |
| `harness_test.go.txt` | `849a8c5ed467aa5dccebb8da82e48b8b0663e65e5204a72fd99e3295dc5a2a90` |
| `compile.jsonl` | `93b4f7829ed2c673751c317659dcf6657bb4806146dc64ae544ce9ce0d79c5a5` |
| `results.jsonl` | `b9993373b9d390acb9a992ffdc347fc1d7dcb41a326b605c437856ce8825d7fa` |
| `plans.jsonl` | `4d54c5d9ba403b47ecac37ab8f6416d2af8867d597b2eea0d6f549274ed0a7d6` |
| `concurrency.jsonl` | `1681c57404cac126e12bf155b87cd693bfc9a276f066214e0271e7a9ecb7bd6a` |
| `pilot-edge-cases.jsonl` | `2a44b210ab508a3f0406a8029aae63f0fc5944e61c1fb9603fb9f5b290a4d9d4` |
| Synthetic cumulative corpus | `b3a0e81e603ff6424ae87a26b1745b61b90d02bfa25df7bbb85037035e42c0d6` |

Connection credentials are not present in any retained artifact.

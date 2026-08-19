# PostgreSQL versus Neo4j performance summary

The latest broad automatic-production benchmark shows that PostgreSQL now
wins most of the measured DAWGS corpus. The gains are shape-dependent:
qualified ordinary shortest paths have largely moved from severe losses to
PostgreSQL wins, while unbounded shortest paths, hidden fan-in, all-shortest
enumeration, and sparse fixed-suffix traversal remain the principal Neo4j
advantages.

## Headline

| Metric | Result |
| --- | ---: |
| Matched cases | 132 |
| PostgreSQL wins | 90 |
| Neo4j wins | 42 |
| Median PostgreSQL/Neo4j ratio | `0.305` |
| Geometric-mean ratio | `0.399` |
| Median interpretation | PostgreSQL about `3.28x` faster |
| Geometric-mean interpretation | PostgreSQL about `2.51x` faster |
| Semantic failures | 0 |

This is a marked change from the earlier 75-case capture, where PostgreSQL won
40 cases, the median ratio was `0.947`, and the geometric mean was `1.45`,
favoring Neo4j.

Ratios below are PostgreSQL latency divided by Neo4j latency. Values below
`1.0` favor PostgreSQL; values above `1.0` favor Neo4j.

## Current automatic-production landscape

| Workload family | Cases | PG wins | Median PG/Neo4j | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Fixed one-hop `ExpandInto` | 11 | 11 | `0.036` | PostgreSQL about `28x` faster |
| Counts | 5 | 4 | `0.074` | PostgreSQL about `13.5x` faster |
| Lookups | 9 | 8 | `0.125` | PostgreSQL about `8.0x` faster |
| Generated ordinary shortest path | 24 | 21 | `0.132` | PostgreSQL about `7.6x` faster |
| Trust reconciliation | 3 | 3 | `0.137` | PostgreSQL about `7.3x` faster |
| Pruning selections | 4 | 4 | `0.141` | PostgreSQL about `7.1x` faster |
| Relationship scans | 9 | 9 | `0.347` | PostgreSQL about `2.9x` faster |
| Endpoint-seeded expansion | 3 | 2 | `0.390` | PostgreSQL about `2.6x` faster overall |
| Standalone one-hop | 7 | 6 | `0.444` | PostgreSQL about `2.3x` faster |
| Pruning mutations | 2 | 2 | `0.478` | PostgreSQL about `2.1x` faster |
| Reconciliation mutations | 6 | 5 | `0.578` | PostgreSQL about `1.7x` faster |
| Small fixed-suffix controls | 2 | 1 | `0.893` | Near parity |
| All-shortest control | 1 | 0 | `1.798` | Neo4j about `1.8x` faster |
| Path-observed generic traversal | 1 | 0 | `1.932` | Neo4j about `1.9x` faster |
| Generated fixed-suffix expansion | 23 | 7 | `2.094` | Neo4j about `2.1x` faster |
| SP-v2 topology suite | 18 | 5 | `2.976` | Neo4j about `3.0x` faster |
| Base unbounded shortest path | 2 | 0 | `7.019` | Neo4j about `7.0x` faster |

## Original-to-current shortest-path delta

The original PostgreSQL public path used the workspace shortest-path executor.
For qualified bounded shapes, the current automatic lowering removes roughly
two orders of magnitude of PostgreSQL latency and changes the backend winner.

| Case | Original PG | Current PG | PG reduction | Current Neo4j | Current PG/Neo4j |
| --- | ---: | ---: | ---: | ---: | ---: |
| D2 distance | 5.948 ms | 0.059 ms | `99.0%` | 0.984 ms | `0.060` |
| D2 path | 6.861 ms | 0.097 ms | `98.6%` | 1.020 ms | `0.095` |
| D16 distance | 23.205 ms | 0.131 ms | `99.4%` | 1.025 ms | `0.128` |
| D16 path | 25.299 ms | 0.187 ms | `99.3%` | 1.078 ms | `0.174` |
| D32/F512 path | 67.989 ms | 0.727 ms | `98.9%` | 0.856 ms | `0.850` |

These original-to-current values come from separate diagnostic captures, not
one frozen paired study. The effect is nevertheless far larger than observed
host or Neo4j control drift.

## Canonical I1 qualification

For the protected inbound, typed, single-kind, one-path `1..64` cohort,
canonical I1 produced:

- median reductions of `75.9-94.2%` versus S4;
- p95 reductions of `70.2-89.7%`;
- 3,500 unique timed candidate receipts and zero fallback;
- maximum state and predecessor observations of 281 and 280 rows;
- all four training and all three protected holdout cases passing.

This was an I1-versus-S4 qualification, not a matched Neo4j comparison. It
supports lowering the qualified S4-heavy envelope but does not itself provide
an exact I1/Neo4j ratio.

## Largest remaining gaps

| Workload | Current PG/Neo4j |
| --- | ---: |
| Hidden-fan-in distance stress | `60.34x` slower |
| Sparse fixed-suffix path | `57.79x` slower |
| V2 sparse fixed-suffix path | `49.53x` slower |
| V2 sparse endpoint IDs | `46.97x` slower |
| Sparse endpoint IDs | `45.94x` slower |
| Base unbounded one-path shortest | `7.74x` slower |
| Base unbounded shortest distance | `6.30x` slower |
| All-shortest depth-16 stress | `5.40x` slower |
| Inbound all-shortest depth 8 | `5.08x` slower |
| Outbound all-shortest depth 8 | `4.84x` slower |

Forced suffix-seeded reverse reduced sparse PostgreSQL latency by
`95.4-96.8%`, but remained slower than Neo4j on the sparse cases and regressed
the already-fast high-reverse-fan-in control by `75.7%`. The crossover is why
broad reverse dispatch remains disabled.

The subsequent ordered-ID hydration work removed the remaining exact-reverse
path-materialization deficit. An adversarial evidence audit invalidated the
first static same-statement guard capture because its physical order did not
match its doubled-Williams labels. After the gate gained process chronology and
artifact-bound A/A checks, a compliant recapture retained the large win over
exact forward but added estimated median overhead of `21-35%` over exact
reverse. Both cases failed the preregistered guard-overhead stop gate, so
`suffix-reverse-guard-v1` is rejected before holdout, manifest, or
driver-policy work.

The new `SP-I2-C-D` distance executor remains very promising on hidden fan-in.
Its cycle regression was traced to expansion out of an already-reached target,
which repeated a two-node cycle through depth 64. Target-terminal pruning
reduced that diagnostic plan from 65 recursive states to two. A fresh
five-round dirty-tree training rehearsal then produced pooled median ratios of
`0.033-0.759` on the five target workloads, with zero candidate fallback.

Commit `3865cbc` subsequently supplied the first clean-source discovery. The
capture ran the protocol maximum of 20 order-balanced training rounds and
passed source, binary, corpus, chronology, receipt, exactness, and resource
validation. All five target cases passed. The cycle control's median ratio was
`0.908`; its median-overhead upper bound was about `67.3us`, inside the frozen
`100us` alternative. Its p95 estimate was `0.972`, but the confidence upper
bound was `1.253`, exceeding the immutable `1.05` limit. Training therefore
failed and no freeze was created. No protected holdout was opened. The
100,000-row state and frontier limits remain preregistered production-form
inputs, not qualified caps.

That failed V1 result motivated the separate `sp-i2-distance-v2` successor.
V2 added a fresh corpus, stronger A/A checks, and a frozen 40-round design, but
it stopped at its required power study before any formal candidate timing. The
clean V1 cycle-control trace was rescaled to V2's fixed design and used in
20,000 deterministic trials per scenario. Even a same-implementation
comparison produced a plausible p95 ratio range of about `0.946-1.063` and an
absolute range of about `-116us` to `+133us`, both wider than V2 allowed. The
full target decision succeeded in 47.94% of trials and the control decision in
51.23%; V2 required a 90% lower confidence bound. Coverage and false-positive
checks passed, so this was not a faulty statistical test: the fixed study was
too noisy to make the required decision reliably. V2 is therefore terminal,
with no formal A/A, capture plan, sealed preregistration, holdout access, or
production activation. Any continuation requires a newly named successor
protocol with a prospectively fixed design.

ASP I1 improved over A1 by a median `57.2%` on the focused all-shortest
tournament, but regressed shallow diamond and parallel-kind cases by about
`8-10%`. A1 therefore remains the automatic default.

## P5 architectural follow-on

The shadow adjacency-materialization feasibility run passed its physical
oracles but was terminally not advanced. At 2,000 targets it consumed about
`1.95x` combined/base storage, produced about `2.22-3.00x` structural-write
WAL, and caused destructive-write median regressions ranging from roughly
`524x` to `1,324x`. Its faster raw lookup was not a Cypher result and did not
justify those costs.

The prospectively frozen successor is `P5-NATIVE-ADJACENCY-SCAN-V1`: one
read-only PostgreSQL extension primitive over the existing edge indexes. Its
first gate is matched-major build/install/drop/reinstall on PostgreSQL 17 and
18, followed by exact snapshot and raw-adjacency feasibility. It maintains no
graph copy and authorizes no native traversal candidate until the complete
non-candidate protocol passes.

## Bottom line

- PostgreSQL now dominates counts, lookups, scans, pruning, one-hop,
  `ExpandInto`, and qualified ordinary shortest paths.
- The original bounded shortest-path deficit has been almost completely
  reversed.
- Canonical I1 convincingly lowers the remaining qualified S4-heavy inbound
  witness envelope.
- SP-I2 V1 removed most hidden-fan-in distance cost on its target shapes, but
  its clean 20-round discovery failed the frozen cycle p95 confidence bound.
- SP-I2 V2 then failed its mandatory pre-timing power gate: target and control
  decisions were reliable only about half the time, not the required 90%. V2
  never entered formal A/A or opened a holdout and is now retired.
- The static suffix-reverse guard has a valid failed training stop-gate
  decision: its chronology-compliant recapture exceeded the immutable overhead
  bound on both cases, so it cannot authorize holdout or production. Exact
  reverse hydration remains a component rather than an automatic policy.
- The expansion driver can still reproduce orientation-probe v1/v2 guarded
  statements structurally, but authorization rejects v1's insufficient
  evidence schema and v2's failed immutable overhead generation. Separately,
  SP-I2 V2 is retired for inadequate prospective power and must not be
  recaptured or advanced to its unopened holdouts.
- Neo4j remains materially ahead for unbounded shortest paths, hidden-fan-in
  distance, all-shortest enumeration, and sparse fixed-suffix traversal.
- The remaining gaps are architectural and topology-specific rather than
  general driver or decoding overhead.
- P5's duplicate adjacency architecture is terminally stopped; its native
  read-only successor is planned but not implemented or performance-qualified.

## Evidence boundaries

The broad cross-backend capture is decision-quality diagnostic evidence for
ranking work, not rollout authorization. The canonical-I1 confirmation has a
clean committed source and frozen binary identity, but production activation
still requires exact production-statement, reference-closure, operational,
and manifest evidence.

The current uncommitted `sp-static-v6` boundary has not yet been benchmarked
from a clean committed source. Its performance basis is the preceding frozen
`6d56a609` canonical-I1 confirmation.

The SP-I2 V1 report is clean-source training evidence, but its failed cycle p95
decision prevented freeze creation and terminated V1 before its protected
cohort. The V2 successor has only open-development and prospective-calibration
evidence: its inadequate-power result stopped it before formal timing. The
checked-in V2 rejection tombstone records that no A/A, capture plan, sealed
preregistration, holdout access, or activation occurred. The suffix-guard
figures come from a dirty-tree but chronology-valid training stop-gate report
whose failed decision likewise terminates that generation before any protected
cohort. Neither result is rollout authorization.

Final evidence closure requires each reference workload to match exactly one
native PostgreSQL A/A workload and requires performance receipts to equal the
complete per-round resource receipt set. Two producer limitations remain
explicit: confirmation/performance typed reports do not embed raw samples for
independent bootstrap replay, and the operational gate validates an assembled
32-record native input for which no standalone capture producer yet exists.

Primary local sources:

- [`docs/experiments/remaining_outlier_delivery_v1.md`](docs/experiments/remaining_outlier_delivery_v1.md)
- [`docs/experiments/p5_native_adjacency_extension_feasibility_v1.md`](docs/experiments/p5_native_adjacency_extension_feasibility_v1.md)
- [`cmd/graphbench/README.md`](cmd/graphbench/README.md)
- [`benchmark/testdata/scale/protocols/sp_i2_distance_v2.json`](benchmark/testdata/scale/protocols/sp_i2_distance_v2.json)
- [`benchmark/testdata/scale/protocols/sp_i2_distance_v2_rejection.json`](benchmark/testdata/scale/protocols/sp_i2_distance_v2_rejection.json)
- [`.coverage/live-backend-current-v1.md`](.coverage/live-backend-current-v1.md)
- [`.coverage/cypher-sql-delta-20260807/REPORT.md`](.coverage/cypher-sql-delta-20260807/REPORT.md)
- [`.coverage/sp-i1-inbound-v1-6d56a60/confirmation-report.json`](.coverage/sp-i1-inbound-v1-6d56a60/confirmation-report.json)

# CySQL Performance Continuation Plan 1

## Purpose

This document continues `perf_rework_plan.md` from the validated working-tree
state captured on 2026-08-05. It changes the optimization objective:

- The goal is the best practical CySQL/PostgreSQL performance that preserves
  Cypher semantics, scales across the supported workload envelope, and remains
  operable under realistic pool concurrency.
- Neo4j is a semantic and implementation oracle. Its latency is useful context,
  but it is not a target, lower bound, acceptance threshold, or stopping rule.
- Every optimization competes against the best CySQL predecessor and against a
  measured PostgreSQL-native reference that performs the same necessary work.

The correctness, graph-scoping, backend-equivalent integration, repository
workflow, and destructive-benchmark safeguards in `perf_rework_plan.md` remain
in force. Where the older plan defines Neo4j-relative or arbitrary cumulative
percentage gates, this continuation replaces them with the reference-gap and
optimality rules below.

## State entering this continuation

The authoritative result is
`.coverage/live-bench-rerun-20260805/REPORT.md`. It compares the clean
`05e70a18d7c6` engine, instrumented with the current measurement harness, with
the current working tree over five independently reloaded rounds and 150 warm
samples per target/backend series.

| Target | Matched PG baseline | Current PG | Current change | Diagnostic reading |
|---|---:|---:|---:|---|
| Bound-pair shortest path | 10.915 ms | 5.278 ms | -51.6% | Real gain, but search remains the dominant cost |
| ADCS P1 endpoint IDs | 0.908 ms | 0.878 ms | -3.3% | Already a small server query; remaining headroom is unquantified |
| ADCS P1 observed path | 1.834 ms | 1.661 ms | -9.4% | Search is cheap; path construction and end-to-end overhead remain |

All exact target observations matched their declarations and matched across
PostgreSQL and Neo4j in every final round. Target p95 improved by 46-52%.
Across the complete comparable corpus, 96 of 101 backend/case series passed the
existing regression gate.

The current working tree already contains:

- graph-scoped traversal and hydration;
- reusable, versioned shortest-path workspace relations;
- proven-singleton endpoint arrays and shortest-path limit handling;
- edge-ID-only `length(shortestPath(...))` observation;
- graph-scoped ordered edge-ID path materialization;
- suffix, projection, and staged field-requirement lowering;
- exact observations, retained raw samples, cold/warm classification, and a
  seeded bootstrap gate.

These changes are an incumbent implementation, not the assumed final design.
They must be preserved while the continuation baseline is captured; do not
reset or recreate the dirty working tree from `05e70a1`.

### Current bottleneck evidence

The five candidate shortest-path `EXPLAIN ANALYZE` captures report 3.76-5.40
ms of server execution, with a median near 4.18 ms, versus the 5.28 ms warm
end-to-end median. `length(shortestPath(...))` avoids full hydration but remains
close to the full-path latency. The shortest search engine, not path output or
client compilation, is therefore the first critical path.

The singleton route still performs all of the following:

- checks and resets five indexed temporary workspace relations;
- dynamically plans primer and recursive fragments at each layer;
- deletes rejected rows, copies and deduplicates frontiers, truncates frontier
  slots, and maintains visited indexes;
- carries and concatenates complete edge-ID trails in frontier rows;
- passes fragments already rewritten at translation time through the
  server-side runtime rewriter again.

A representative two-edge candidate plan still recorded roughly one thousand
shared-buffer hits plus local temporary reads, writes, and dirtying. This makes
the existing workspace harness an entrant in the next design comparison, not
the destination.

Path materialization is the next confirmed server-side cost. Across the five
candidate diagnostic plans:

- ADCS P1 server execution is approximately 0.215 ms for endpoint IDs and
  0.911 ms for the full path, an output-shape gap near 0.70 ms;
- the small generic variable-length traversal is approximately 0.109 ms for
  ID-only output and 0.574 ms for observed-path output, an output-shape gap
  near 0.47 ms.

Those figures are diagnostic single-plan observations, not substitutes for a
matched repeated component benchmark. They are strong enough to establish the
measurement work that must come next.

By contrast, the latest HOP-05, HOP-09, and LOOKUP-11 plans execute in well
under one millisecond while their end-to-end medians are much larger and they
return 128-1,024 hydrated rows. Those cases must be decomposed through result
decoding and allocation before their SQL is rewritten.

## What "optimal" means

No single cross-engine ratio can establish optimality. This plan uses three
CySQL/PostgreSQL references for every target:

1. **Immediate predecessor**: the artifact produced by the last accepted
   increment. It gives isolated attribution.
2. **Continuation baseline**: the frozen, checksummed current working-tree
   artifact produced in Phase C0. It gives cumulative progress.
3. **Best correct PostgreSQL reference**: the fastest measured implementation
   that performs the same required work and returns the same representation
   through the same pgx transaction and drain path. This can be hand-written
   SQL, a direct helper invocation, or an experimental executor. It is a moving
   engineering reference, not a theoretical bound.

Each target also receives smaller component floors:

- open-session protocol and prepared-statement round trip;
- endpoint validation and required graph-partition access;
- search returning ordered scalar IDs only;
- path hydration from precomputed ordered IDs;
- result transfer, composite decoding, and drain;
- parse, optimize, translate, render, bind/prepare, and plan costs.

For a continuation baseline `B`, candidate `C`, and current reference `R`,
report addressable-gap closure when `B > R` as:

```text
gap_closed = (B - C) / (B - R)
remaining_gap = C - R
```

Do not optimize a ratio when the absolute gap is below measurement resolution.
Do not call a specialized reference a floor for a broader semantic form that it
does not implement.

### Optimization dimensions

Warm serial median remains useful, but optimality is Pareto-based across:

- end-to-end p50, p95, and sufficiently sampled p99;
- PostgreSQL planning and execution time;
- client compilation, parameter binding, transfer, decode, and drain time;
- shared, local, and temporary buffer activity;
- temporary relation and file bytes;
- rows and edges examined relative to rows and paths returned;
- allocations and bytes allocated in the Go client path;
- cold-session and whole-pool cold-start cost;
- throughput, pool wait, CPU, memory, and error rate under concurrency;
- depth, fanout, output-cardinality, payload, and parameter-cardinality slopes.

Latency cannot be bought with unbounded per-session workspace, a p99 collapse,
or worse asymptotic behavior.

### Experiment acceptance

Before selecting a universal fixed percentage, Phase C0 must run A/A trials and
publish the statistical measurement resolution/minimum detectable effect for
every metric. Each experiment must also predeclare a practical materiality
threshold based on absolute savings, workload frequency, or operational
resource value. Statistical distinguishability and practical materiality are
separate requirements. An implementation experiment may ship only when all of
the following are true:

- exact semantics and the required plan/shape invariants pass;
- its matched 95% interval demonstrates an improvement larger than the A/A
  measurement resolution and the predeclared materiality threshold, or
  equivalent latency with a material resource win;
- it passes the normal and largest applicable scale tiers;
- it introduces no confirmed p95, p99, throughput, memory, or temporary-space
  regression outside the approved phase budget;
- the complete declared corpus is present; missing, newly unsupported, or
  non-`ok` PostgreSQL records cannot disappear through intersection-only
  comparison;
- rejected alternatives and their artifacts are recorded, and rejected
  production code is removed.

As initial materiality defaults, with statistical thresholds calibrated by
A/A:

- a target optimization should have a median-ratio upper bound at most `0.95`
  or an absolute saving whose lower bound is at least 0.10 ms;
- an architecture replacement should normally improve its target by at least
  10-15%, not merely add complexity for a marginal point estimate;
- an affected-family regression is confirmed when the lower interval bound is
  above `1.05`; a point estimate above `1.05` with an inconclusive interval
  requires more rounds;
- `1.20` is an emergency whole-corpus ceiling, not permission to ship a
  confirmed 5-19% regression. Any confirmed regression beyond the
  A/A-supported non-inferiority budget requires a named maintainer-approved
  exception with cause, magnitude, operational trade, and rollback decision;
- normal-tier traversal should not spill to disk;
- any accepted resource-only trade must name the saved resource and its
  operational value.

### Workstream completion rule

A workload is "optimal within the current architecture and measurement
resolution" only when all of these are true:

- the candidate/reference upper confidence bound is at most `1.10`, or the
  absolute gap is below the A/A-derived measurement resolution;
- its fixed cost and depth/fanout/output slopes are explained by necessary
  work, with no duplicate traversal, hydration, dynamic planning, workspace
  churn, or avoidable wide state left in the measured hot path;
- expected scaling, concurrency, p95/p99, memory, and soak gates pass;
- at least two independently plausible alternatives fail to produce a
  statistically distinguishable and materially useful improvement, unless one
  candidate already reaches the reference within measurement resolution;
- the selected design is not Pareto-dominated by another correct candidate.

Reopen a completed workload when PostgreSQL, the DAWGS schema, production
workload weights, or the best reference changes materially.

## Scope and guardrails

In scope:

- Cypher optimization and PostgreSQL lowering;
- PostgreSQL traversal algorithms and helper functions;
- graph-partition access and planner behavior;
- intermediate row shape, path representation, hydration, and result decoding;
- translation/template caching after SQL shapes stabilize;
- benchmark instrumentation, scale generation, concurrency, and artifact
  publication needed to prove the result;
- a documented portability decision if SQL/PLpgSQL reaches a measured plateau.

Not automatically in scope:

- weakening Cypher relationship uniqueness, multiplicity, path order, null,
  zero-depth, or same-endpoint behavior;
- global planner settings chosen for a single query;
- replacing CySQL with the unimplemented local traversal mode;
- adopting a native PostgreSQL extension without an explicit packaging,
  deployment, upgrade, and security decision;
- optimizing a query solely because it is slower than Neo4j;
- keeping dormant experimental implementations in production.

All optimized persistent reads must remain graph-scoped. Shared integration
cases remain backend-equivalent; PostgreSQL-only physical plan and resource
assertions belong in PostgreSQL-scoped tests.

## Sequenced delivery plan

| Phase | Outcome | Depends on | Critical path |
|---|---|---|---|
| C0 | Complete continuation baseline and trustworthy gate | Current working tree | Yes |
| C1 | PostgreSQL references and component cost model | C0 | Yes |
| C2 | Shortest-path executor tournament | C1 | Yes |
| C3 | Selected singleton shortest executor and observation modes | C2 | Yes |
| C3G | Generic, correlated, multi-pair, and all-shortest optimization | C1 and C3 | Required for whole-family optimality |
| C4 | Minimal linear/batched path materialization | C1; may prototype beside C2 | Yes after C3 |
| C5A | Slim variable traversal and staged scalar state | C1; coordinate observed paths with C4 | No |
| C5B | Large-result decode and list-cardinality path | C1; may run beside C2-C5A | No |
| C6 | ADCS suffix, scalar-state, and combined-query convergence | C4 and C5A | No |
| C7 | Conditional stable-template compilation and plan-cache work | C3, C3G, C4-C6, and C5B SQL stabilized | Only if C1 proves addressable cost |
| CX | Native-extension portability decision/prototype | Portable C3/C3G results | Conditional before shortest completion |
| C8 | Pool, concurrency, memory, cancellation, and soak qualification | C3-C7 plus any triggered CX decision | Yes |
| C9 | Cost-weighted complete-corpus optimization loop | C8 | Ongoing |

C4 reference work may proceed while C2 evaluates search algorithms, but the
shortest executor and path materializer must first be measured separately.
C7 must not begin with complete-template caching until emitted SQL and
parameter signatures are stable. C3 completes the proven-singleton target;
shortest-path performance as a whole is not complete until C3G also satisfies
the workstream completion rule.

## Phase C0: Freeze a complete continuation baseline

### Complete and protect the corpus

- Resolve the PostgreSQL `SCAN-02` and `SCAN-03` `Meta` kind-mapping errors.
- Re-run PostgreSQL `LOOKUP-05` and the Neo4j-only `TRUST-03` tail outliers in
  isolated matched rounds. Treat Neo4j latency only as noise diagnosis; a
  PostgreSQL code change cannot be justified by a Neo4j-only timing movement.
- Add a declared case/backend manifest to the performance gate. Fail when a
  required PostgreSQL key is missing, changes from `ok` to another status, or
  lacks enough samples. Require every declared Neo4j oracle case to remain
  present and exact-result-correct, without applying a Neo4j latency gate.
  Unsupported cases must be explicit versioned entries.
- Add a destructive-run lock or unique database/graph allocation so two
  GraphBench processes cannot clear or load the same target concurrently.
- Keep preflight and postflight exact observations outside timed blocks.
- Use a fresh disposable PostgreSQL database and `VACUUM (ANALYZE)` after each
  fixture load. Abort on maintenance failure.

### Activate the generated scale fixtures

`generated_shortest_paths` and `generated_adcs` are registered today, but the
benchmark corpus does not execute cases against them. Add parameterized or
versioned deterministic variants instead of keeping one unused fixed
configuration.

Normal shortest matrix:

| Dimension | Normal points | Largest/soak points |
|---|---|---|
| Depth | 1, 2, 4, 8, 16 | 32, 64 |
| Fanout | 1, 16, 128 | 512, 1000 |
| Shape | direct, linear, diamond, dead-end, cycle, disconnected | dense disconnected |
| Direction | outbound, inbound, directionless | mixed fallback |
| Kinds | untyped, one, several | 30 kinds where supported |
| Observation | distance, full path | all-shortest tie set |
| Endpoint form | singleton IDs | correlated and multi-pair fallback |

Normal ADCS/path matrix:

| Dimension | Points |
|---|---|
| `MemberOf` depth | 0, 1, 2, 4, 8, 16 |
| Fanout | 1, 10, 100, 1000 |
| Valid suffix density | none, sparse, half, all |
| Decoy | kind, direction, endpoint kind, disconnected |
| Payload | empty, normal, 4 KiB node/edge properties |
| Projection | endpoint IDs, P1 path, P2 path, combined paths |
| Output cardinality | 0, 1, 4, 32, 1000 paths |

Use a documented pairwise subset in normal CI and the full largest tier on a
dedicated performance runner. Every generated fixture records configuration,
cardinality, checksum, and repeatability.

Internal raw ordered node/edge-ID observations belong in PostgreSQL-only C1
component probes, not the shared Cypher corpus. Cypher does not expose that
representation, and a shared case must remain backend-equivalent.

Extend `generated_adcs` before using it for P2 or combined coverage. The current
generator models only the P1 `Enroll`/`TrustedForNTAuth`/`NTAuthStoreFor`
suffix. Add independent P1/P2 valid-density controls, the required
certificate-template publication and CA/root/domain chains, branch-specific
decoys, and exact Cartesian result declarations.

### Extend measurements

GraphBench must record:

- source commit plus dirty-diff hash and binary hash;
- fixture checksum/cardinalities and graph partition count;
- hardware, OS, Go, PostgreSQL, Neo4j, and relevant server settings;
- exact invocation, pool settings, backend PID, plan mode, and cache state;
- declared per-session and whole-pool memory/workspace ceilings derived from
  the supported pool configuration and deployment budget;
- SQL template/fingerprint and optimizer/lowering decisions;
- raw latency samples and pool wait, plus versioned output fields that C1 can
  populate with client component timings and allocations;
- shared, local, and temporary buffers, temporary bytes/files, and workspace
  relation sizes;
- examined and returned row counts where they can be observed safely.

Add a concurrency-capable measurement mode in C0 that can retain a configured
pool, drive concurrency 1, pool size, and twice pool size, and classify pool
wait and per-session cold state. C2 uses it for algorithm selection; C8 remains
the full qualification rather than the first availability of concurrency
tooling.

The current PostgreSQL plan summary must be extended to retain local-buffer
activity; that activity is central to the shortest workspace diagnosis.

Run baseline-versus-baseline A/A trials with the same alternation and reload
protocol. Publish per-metric measurement resolution and the number of rounds and
samples required for p50, p95, and p99. Do not declare p99 from the current 150
samples. A gated p99 needs the A/A-derived sample size and at least roughly 100
expected observations in the top one percent, normally at least 10,000 samples
per gated series across independent blocks; otherwise p99 remains diagnostic.

### Freeze and publish

After the corpus is complete, capture the current working tree as continuation
baseline `C0`. Publish a durable bundle containing:

- an environment and corpus manifest;
- raw JSONL and summary reports;
- translated SQL and PostgreSQL/Neo4j plans;
- A/A measurements and any already available package microbenchmarks; C1
  publishes the required component/reference bundle separately;
- gate JSON, checksums, and exact commands;
- source commit, dirty-diff hash, and binary checksums.

`.coverage` may remain a local staging location but cannot be the only durable
record. The old clean `05e70a1` artifact remains historical context; C0 becomes
the immutable cumulative baseline for this continuation.

### C0 exit criteria

- Every declared PostgreSQL case/backend key is present. Every required
  supported case is `ok`; any intentionally unsupported form has an explicit,
  approved, versioned declaration.
- Every declared Neo4j oracle case is present and exact-result-correct; its
  latency remains informational.
- A/A measurement resolutions are published alongside predeclared materiality
  thresholds for the active workloads.
- Generated shortest and ADCS normal tiers execute real cases.
- The concurrency runner can execute serial, pool-sized, and oversubscribed
  smoke blocks while retaining backend/session identities.
- Destructive overlap is prevented rather than detected after corruption.
- The C0 bundle is reproducible and durable.
- `make test`, `go test -race ./cmd/graphbench`, PostgreSQL `make test_all`,
  Neo4j `make test_all`, formatting, and diff checks pass after the final
  benchmark changes.

## Phase C1: Build PostgreSQL-native references and a cost model

### Reference ladder

For each target, execute the following through the same pinned pgx connection,
transaction behavior, parameter encoding, result representation, and drain
path as CySQL:

1. A constant prepared query to measure protocol and transaction overhead.
2. Endpoint validation only.
3. The minimum required graph access returning scalar IDs.
4. Search returning ordered node/edge IDs without hydration.
5. Hydration from precomputed ordered IDs without search.
6. A complete hand-written, parameterized PostgreSQL reference with identical
   semantics and output representation.
7. The translated CySQL query.

References must be graph-scoped and use the same schema and indexes. A
reference that omits relationship uniqueness, duplicates, path order, payload,
or decoding work is a component floor, not a full-query comparator.

### Client waterfall

Add repeatable benchmarks for:

- parse;
- optimize/lowering analysis;
- PostgreSQL AST translation;
- SQL formatting and parameter mapping;
- pool acquisition and transaction setup;
- parameter encode/bind and prepare/plan behavior;
- server execution;
- row transfer, composite decode, graph value construction, and drain;
- allocations and bytes allocated for each client stage.

Record cache miss, first prepared execution, executions 2-5, and steady-state
cache hit separately on the same backend PID.

Build the waterfall from mutually exclusive intervals where instrumentation can
measure them directly and from controlled one-variable deltas elsewhere.
`EXPLAIN ANALYZE` planning/execution, client wall time, transfer, and decode
observations are not automatically additive; never obtain the attribution
percentage by summing overlapping measurements. Report the unexplained
residual explicitly.

### Shortest server attribution

Create benchmark-only probes for at least:

- endpoint validation;
- workspace ensure and reset;
- runtime fragment rewriting and dynamic planning;
- forward/backward primer and recursive execution;
- rejected-row pruning;
- frontier deduplication/copy and slot reset;
- visited maintenance;
- midpoint/direct-hit detection;
- path reconstruction and hydration.

Run isolated comparisons of multi-table `TRUNCATE`, indexed `DELETE`, a single
compact trace relation, and generation-tagged rows with bounded cleanup. Also
measure removal of runtime fragment rewriting and every singleton scratch
index. Attribute at least 90% of the captured server time before selecting an
executor; do not spend multiple production increments polishing an incumbent
whose architecture may lose the tournament.

### C1 exit criteria

- Each active target has versioned component floors and a full correct
  PostgreSQL reference.
- At least 90% of shortest server time and 90% of end-to-end time for the large
  result cases is assigned by mutually exclusive measurements or controlled
  deltas; overlap and the unexplained residual are reported explicitly.
- Reports rank work by addressable absolute cost, not a Neo4j ratio.
- Neo4j latency is informational; Neo4j exact-result disagreement remains a
  correctness failure.

## Phase C2: Shortest-path executor tournament

Prototype additive singleton executors behind the same semantic test adapter.
Keep prototypes out of the production dispatcher until the tournament is
complete.

### Candidate S0: optimized incumbent workspace

Use the current bidirectional harness as the control and test only measured
changes:

- generate final workspace names once instead of rewriting fragments again at
  execution;
- compare reset strategies and remove only proven-unhelpful indexes;
- avoid repeated `EXISTS`/return scans and redundant frontier passes;
- replace full edge-ID trails with predecessor state where semantics allow;
- evaluate one compact relation keyed by run generation and side instead of
  five copied frontier/visited relations.

Generation-tagged state must have deterministic bounded cleanup and a soak test;
it cannot trade latency for unbounded session bloat.

### Candidate S1: array-resident singleton BFS

Evaluate a typed PL/pgSQL helper for small frontiers that holds frontier,
visited, and predecessor state in memory. It should accept typed graph ID,
endpoint IDs, direction, kind IDs, depth bounds, and observation mode rather
than arbitrary SQL fragments.

This candidate is eligible only where its state model proves the required
relationship uniqueness and path semantics. It must have an explicit frontier
or memory threshold and fall back before array growth becomes pathological.

### Candidate S2: compact bidirectional trace

Evaluate one trace relation containing a run generation, side, node, parent,
edge, and depth. Expand the smaller frontier, insert each eligible discovered
state once, detect intersection against the opposite side, and reconstruct one
path only after success.

This design should eliminate per-layer deletion, full frontier copies, and
edge-array concatenation. Its uniqueness key must encode enough state for the
eligible minimum-depth and predicate semantics; node-only visited pruning is
not universally safe.

### Candidate S3: inline recursive CTE

Generate an inline CTE against the concrete graph partition with stable typed
parameters. Test unidirectional and, if representable without duplicate work,
bidirectional forms.

Do not depend on PostgreSQL's implementation output order for shortest
semantics. `ORDER BY depth LIMIT 1` is correct only if its complete search and
worst-case behavior pass the disconnected and dense fanout tiers. Carrying path
arrays, global visited semantics, and equal-depth ties must be accounted for
explicitly.

### Tournament method

Every candidate runs the same matrix:

- direct, linear, diamond, cycle, dead-end, wrong-direction, and disconnected;
- depth 1, 2, 4, 8, 16, 32, and largest-tier 64;
- fanout 1, 16, 128, 512, and largest-tier 1000;
- outbound, inbound, directionless;
- untyped, one kind, and multiple kinds;
- distance-only and one-path observation;
- warm session, cold session, full-pool cold fan-out, and concurrent calls;
- missing, null, contradictory, and same endpoints;
- generic correlated/multi-pair controls.

Compare search candidates first at an identical raw-output boundary: depth and
ordered scalar node/edge IDs. Full-path tournament comparisons must use the
same materializer and decoder so C2 cannot select a search engine because it
quietly exercised a different C4 output path.

Rank candidates by end-to-end latency, server latency, edges examined, shared
and local buffers, temporary bytes, memory, cold cost, concurrency throughput,
and scaling slope. If candidates win in different measured regimes, define a
small evidence-backed hybrid dispatcher. Do not select on the three-node base
fixture alone.

After subtracting the measured fixed cost, the upper confidence bound for time
per examined edge and bytes per discovered state between adjacent normal tiers
must remain within `1.25` times the prior tier. Dense disconnected cases must
complete within their timeout without normal-tier spill.

### Provisional pinned-host budgets

These budgets guide the first tournament on the 2026-08-05 report host; Phase
C1 references supersede them when available:

- upper confidence bound for distance-only singleton server execution below
  0.25 ms on the tiny case;
- no local/temp I/O for the tiny singleton fast path unless the temp-backed
  candidate Pareto-dominates every temp-free candidate;
- upper confidence bound for full-path server time no greater than search plus
  1.2 times isolated hydration;
- stable outer SQL; any retained dynamic SQL must be measured as part of the
  Pareto-winning implementation rather than excluded by assumption;
- no superlinear unexplained cost over depth and examined-edge tiers.

### C2 exit criteria

- At least the incumbent and two fundamentally different executors have valid
  complete artifacts.
- A winner or measured hybrid is selected on the complete envelope.
- The winner produces a statistically distinguishable and materially useful
  improvement over C0, or C0 itself satisfies the workstream completion rule
  after the alternatives fail. The selected result is not Pareto-dominated.
- Rejected prototypes are documented and removed from production code.

## Phase C3: Ship the selected singleton shortest executor

### Explicit lowering and eligibility

Add a named optimizer/lowering decision with the selected executor and fallback
reason. Initial fast-path eligibility requires:

- `shortestPath`, not `allShortestPaths`;
- exactly one validated endpoint ID on each side;
- no correlated or multi-row endpoint source;
- no path-dependent predicate unsupported by the executor;
- supported direction, relationship kinds, and depth bounds;
- minimum depth 0 or 1 unless the executor's state also proves the required
  node-depth, relationship-history, and predicate semantics.

Validate label, property, and additional ID predicates before invoking search.
Missing, null, or contradictory endpoints must invoke no executor. Preserve the
same-endpoint error and zero-depth behavior before allocating search state.

Use stable typed parameters, graph scope, and the existing outer limit when
safe. Declare `ROWS 1` only if the selected helper is set-returning; omit it if
the helper returns one scalar composite. Different endpoint values must produce
the same SQL template.

### Observation-specific modes

Use distinct state/result shapes:

- **distance** returns depth only, retains only the minimal frontier/visited
  node-depth state required by the selected algorithm, and never retains
  predecessor or path arrays;
- **one path** uses the tournament-winning bounded state representation and
  returns ordered node and edge IDs; a compact predecessor chain is preferred,
  but a full-trail array is allowed in a measured bounded regime if it
  Pareto-dominates the alternatives;
- **all shortest paths** remains on the generic fallback until a separate
  predecessor-DAG implementation preserves every valid equal-depth predecessor
  edge, including parallel-edge-distinct ties.

Do not make `length(p)` pay for the one-path representation when field
requirements prove every downstream use of `p`, including aliases and `WITH`
propagation, is distance-only. A full-path result should pass ordered node and
edge IDs directly to the observation boundary so the materializer does not
rediscover connectivity.

### Semantic gate

The selected path must preserve:

- one valid result from an equal-length diamond;
- post-filter semantics without substituting an invalid longer path;
- relationship uniqueness, parallel edges, cycles, and repeated nodes;
- outbound, inbound, directionless, and exact edge order;
- depth bounds including `*0..0` and `*0..`;
- null, missing, contradictory, and same endpoints;
- graph-scoped colliding IDs;
- two shortest calls in one statement and success/error/rollback reuse;
- conservative fallback for correlated, multi-pair, path-predicate, and
  unsupported forms.

### C3 exit criteria

- The fast path performs no generic filter/pair bookkeeping.
- Distance mode carries no predecessor/path state.
- One-path state matches the selected tournament regime; any full-trail array
  has a proven bound and measured advantage over predecessor-state alternatives.
- SQL templates are stable and partition pruning is proven under the chosen
  custom/generic plan behavior.
- Warm, cold, scale, and concurrent gates pass against immediate predecessor,
  C0, and the best PostgreSQL reference.
- The generic harness remains correct and has no confirmed regression.

## Phase C3G: Optimize generic and all-shortest forms

C3 establishes optimality only for the proven-singleton bound-pair envelope.
Measure and optimize the remaining shortest family separately rather than
broadening singleton assumptions.

Required workload classes:

- terminal-filtered searches with multiple possible roots;
- materialized endpoint-pair searches;
- correlated endpoints produced by earlier query parts;
- repeated pairs and batches sharing a root or terminal;
- multiple shortest calls in one statement;
- `allShortestPaths` with node-, relationship-, and parallel-edge-distinct
  equal-depth ties;
- supported path-dependent predicates and conservative dynamic fallbacks.

Build complete PostgreSQL references and apply the same depth, fanout,
direction, kind, disconnected, cold/warm, and concurrency matrices. Compare at
least:

- the current pair-aware workspace;
- endpoint-pair deduplication with exact multiplicity restoration;
- shared expansion for pairs with a common root or terminal;
- compact trace state keyed by the necessary pair/search state;
- a predecessor DAG for `allShortestPaths` that retains every valid equal-depth
  predecessor edge, including parallel edges.

Runtime degree/frontier sampling may choose between measured strategies, but
the decision must be bounded, observable, and stable under the declared
parameter envelope. Path-dependent or otherwise unsupported semantics retain a
correct fallback; a fallback is not performance-complete until its production
importance and remaining reference gap are reported.

### C3G exit criteria

- Pair deduplication and shared expansion preserve duplicate input and output
  multiplicity exactly.
- `allShortestPaths` retains all valid ties without substituting the one-path
  executor.
- State is isolated across pairs, calls, transactions, errors, cancellations,
  and physical connections.
- Every material generic workload is within `1.10` times its best correct
  PostgreSQL reference or below measurement resolution, or has an explicit
  portability/product decision describing why it remains outside the current
  architecture boundary.
- Singleton performance does not regress, and the generic family satisfies the
  same scale, resource, concurrency, and workstream completion rules.

## Phase C4: Minimize ordered-path materialization

The current translator already concatenates raw edge-ID components into one
graph-scoped `ordered_edge_ids_to_path` call for eligible read paths. The helper
hydrates edges once, but still walks them recursively and repeatedly appends a
node-ID array. The next work must compare materializer architectures rather
than repeat the already completed consolidation.

### Component cases

For an identical search result, measure:

- scalar distance/row count;
- ordered edge IDs only;
- ordered node and edge IDs;
- relationship composites only;
- complete path composite and normal client decoding.

Run path lengths 0, 1, 2, 4, 8, 16, 32, and 64; output counts 1, 4, 32, 128,
and 1000; and empty, normal, and 4 KiB properties.

Define paired server path tax for this phase as:

```text
path_tax = server_execution(full path composite)
         - server_execution(raw ordered node/edge IDs)
```

Both arms must consume the same search relation on the same physical
connection, return the same row cardinality, and belong to the same matched
round. Summarize the paired deltas directly; do not subtract independently
aggregated medians. The raw-ID arm is a PostgreSQL-only component probe, not a
public Cypher or Neo4j corpus case.

### Materializer M0: directed set-based reconstruction

For a proven directed path, derive ordered nodes directly from the root and
ordered hydrated edge endpoints without recursive `path_walk`. Retain the
recursive fallback for directionless, mixed, legacy, and mutation-returning
paths until each form has a proven linear alternative.

### Materializer M1: carry ordered node IDs

For observed read paths, compare carrying ordered node IDs beside ordered edge
IDs against reconstructing nodes at the boundary. Hydrate each stream in one
ordinal join. Do not add node-ID arrays to endpoint-only or distance-only
queries.

### Materializer M2: batch across result rows

Key output paths by a stable row ordinal, unnest their node/edge IDs once,
hydrate distinct entities set-wise, and reconstruct each result with exact row
multiplicity and order. This is especially relevant to ADCS paths that share a
fixed suffix. Compare it with the simpler one-path-at-a-time helper at low and
high output cardinalities; choose by measured envelope rather than assuming
batching always wins.

### Wide-state comparison

A/B full composites already joined during traversal against scalar IDs plus
boundary hydration under small and 4 KiB payloads. The selected representation
must account for transfer, PostgreSQL row width, TOAST access, and Go decode
allocations, not server execution alone.

### C4 provisional gates

Phase C1 references replace these pinned-host budgets when stricter or better
grounded:

- upper confidence bound for paired generic path server tax at most 0.25 ms on
  the small fixture;
- upper confidence bound for paired ADCS P1 path server tax at most 0.35 ms;
- upper confidence bound for ADCS P1 total server execution at most 0.60 ms;
- no hydration for `length(p)` and exactly one hydration boundary per returned
  path variable;
- zero path-materialization temp reads/writes in the normal tier;
- upper confidence bound for path-only added shared hits at most 30 for the
  four-row ADCS P1 fixture;
- upper confidence bound at most `2.2` for both execution and bytes when path
  length grows from 32 to 64;
- exact order, multiplicity, null, zero-edge, and graph-scope semantics.

## Phase C5A: Slim variable traversal and staged scalar state

### Consume staged field requirements

Field-requirement analysis exists, but ID-only lowering currently applies only
at limited terminal positions. Extend it stage by stage:

- retain labels/properties until their last validation;
- convert roots, terminals, fixed-suffix nodes, and relationships to scalar IDs
  immediately afterward;
- omit unused `satisfied`, entity, property, and kind columns from specialized
  recursive records;
- keep ordered edge-ID trails where ordinary result multiplicity and
  relationship uniqueness require them;
- use node/global visited state only for formally cardinality-insensitive forms
  such as eligible `EXISTS` or proven deduplicated reachability.

Ordinary endpoint projection can contain duplicate endpoint rows reached by
different paths. It must not be converted to simple visited-node reachability
without an explicit semantic proof.

Initial gates:

- upper confidence bounds for base ID-only variable traversal server execution
  at most 0.15 ms and 20 shared hits on the pinned host;
- no property heap/TOAST fetch after the last property use;
- recursive plan row width contains only required scalars and arrays;
- no normal-tier temp I/O;
- cost normalized by expanded edge/path instances does not rise unexpectedly
  by more than 25% between adjacent scale tiers;
- exact duplicate multiplicity remains unchanged.

## Phase C5B: Optimize large-result decode and list-cardinality paths

### Decompose before SQL changes

HOP-05, HOP-09, and LOOKUP-11 currently have sub-millisecond diagnostic server
execution but much larger end-to-end latency. Measure:

- pgx transfer and composite codec cost;
- per-row field-key construction;
- `Values` and JSON/property copying;
- graph value allocation and row drain;
- result retention versus streaming/discarding;
- allocations and bytes per returned node/relationship/property byte.

First A/B cached field metadata, removal of unconditional per-row slice/map
copies, specialized composite codecs, and safe streaming. Preserve ownership
semantics: a decoded value cannot alias mutable pgx buffers after row advance.

Only after the client floor is known should SQL variants compete:

- `= ANY(typed_array)`;
- deduplicated `unnest` plus hash/semi-join;
- adjacency-first plans followed by endpoint-list filtering;
- anchor from the smaller side for two-sided ID sets;
- custom versus generic plans across list size and match density.

List matrix: 0, 1, 8, 32, 1000, and 10,000 values; absent, sparse, half, and
dense matches; a null list parameter; arrays containing null; duplicate
matching IDs; one-sided and two-sided anchors; one and 30 edge kinds. `ANY`,
`unnest`, and semi-join variants must preserve Cypher three-valued filtering.
Duplicate input IDs must not multiply Cypher result rows unless the surrounding
Cypher construct requires that multiplicity.

Pinned-host upper-confidence-bound server guardrails while decomposing the
client path:

- HOP-05 at most 0.35 ms;
- HOP-09 at most 0.40 ms;
- LOOKUP-11 at most 0.60 ms;
- zero spill.

Set end-to-end ceilings only after the identical raw-pgx decode floor exists.
The final target is no more than `1.15` times that floor, with allocations and
decoded bytes no more than `1.10` times the direct-pgx reference. An SQL rewrite
must reduce measured work and latency; a different-looking plan is not a win.

## Phase C6: Converge ADCS from its own measured floor

The endpoint query is a control, not a mandate for another arbitrary percentage
reduction. Sequence ADCS work as follows.

### C6.1 Scalar staged bindings

Apply C5A field requirements to P1 endpoint and path forms. Carry entity IDs
after label/property validation and retain edge IDs needed for whole-pattern
relationship uniqueness. Endpoint projection must not carry full node/edge
properties past last use.

Gates:

- the endpoint server-execution upper confidence bound remains at most 0.25 ms
  on the pinned fixture;
- the payload differential satisfies
  `(candidate_4KiB - candidate_empty) <=
  (reference_4KiB - reference_empty) + A/A measurement resolution`; the required
  `objectid` lookup may necessarily access or detoast its JSONB value;
- no payload is fetched or carried after its last predicate use;
- exact four-row multiplicity remains;
- no full entity/path hydration occurs in the endpoint form.

### C6.2 Select suffix strategy by measured density

The current observed three-hop suffix shape omits the supplemental prefilter.
Compare:

1. current result-producing suffix only;
2. a supplemental satisfaction prefilter;
3. a single consumed suffix relation that produces the required bindings.

Run the full depth, fanout, density, decoy, and payload matrix. The prefilter may
win for sparse high-fanout inputs even if it loses on the small fixture. If
different variants win stable regimes, add a simple shape/statistics decision;
otherwise keep the universal winner. A consumed relation must preserve one row
per suffix path and whole-pattern relationship uniqueness.

The selected strategy must remain within 10% of the best correct PostgreSQL
reference at each declared tier or below the A/A measurement resolution.

### C6.3 P2 and combined queries

Add standalone P2 and combined P1/P2 cases to GraphBench with exact stable
observations. Apply batch hydration before attempting shared expansion.

Share an anchored `MemberOf*` closure only when both branches have identical
graph, anchor, direction, kinds, depth, predicates, uniqueness requirements,
and required state. Branch independently into P1/P2 suffixes and preserve their
Cartesian multiplicity.

Structural and performance gates:

- the shared closure is expanded once;
- P1 and P2 suffix semantics and relationship uniqueness remain independent;
- combined output multiplicity is exact;
- combined server execution is within `1.10` times the best correct combined
  PostgreSQL reference or below A/A measurement resolution;
- suffix and hydration work normalized by returned path-pair rows and bytes is
  within the reference envelope;
- no unbounded materialization or temporary-space increase.

Compare combined time with the sum of isolated branches only on a fixture whose
output rows and bytes are demonstrably equivalent. The usual P1/P2 Cartesian
result performs unavoidable output and hydration work that isolated `m+n`
queries do not.

Stop ADCS server rewriting when its candidate is within 10% of the best correct
reference or below A/A measurement resolution and no duplicate physical work
remains.

## Phase C7: Conditionally remove client compilation and plan overhead

Evaluate this phase only after C3, C3G, C4-C6, and C5B have stable SQL
templates and parameter signatures. It is triggered when C1/C7 remeasurement
shows that client compilation or repeated PostgreSQL planning exceeds both
measurement resolution and the predeclared materiality threshold, or accounts
for at least 10% of the remaining end-to-end reference gap. If the trigger does
not fire, publish that decision and omit production cache/policy changes.
Server, decode, or transfer may still dominate a given case.

### Bounded CySQL template cache

Add caches in measured increments:

1. immutable parsed/optimized representation;
2. complete SQL template plus parameter mapping for proven value-insensitive
   shapes.

The complete-template key includes:

- Cypher text or canonical fingerprint;
- graph relation and generation;
- kind/schema generation;
- parameter type signature;
- optimizer/translator generation and relevant feature flags.

Requirements:

- deterministic memory bound and eviction;
- concurrent request safety and race coverage;
- explicit invalidation metrics and tests;
- no parameter values in a stable singleton key;
- no shared mutable AST, scope, frame, or parameter state;
- hit, miss, eviction, invalidation, and compile-stage telemetry.

Target a cache-hit compile upper confidence bound at most 10% of the uncached
pipeline or 0.05 ms on the pinned host, whichever is supported by the measured
reference. Report cold miss and steady hit separately.

### PostgreSQL plan policy

On pinned connections compare `auto`, forced custom, and forced generic plans
for stable templates over endpoint selectivity, traversal direction/kinds,
list cardinality, and match density. Record prepared statement identity and
executions 1-5 separately from steady state.

Do not set a global plan policy for one shape. Use a query-local or connection
policy only if it is stable across its declared parameter envelope and the
complete corpus/concurrency gates pass.

### C7 exit criteria

- If the trigger does not fire, a published component report closes the phase
  without a production cache or plan-policy change.

For a triggered implementation:

- End-to-end target latency is within 15% of `protocol + cached execution +
  identical decode` or below measurement resolution.
- Cache memory is bounded and stable in the soak test.
- Schema/kind/graph changes cannot reuse stale templates.
- No plan policy depends on the benchmark's particular parameter values.

## Phase CX: Native-extension portability decision

Portable SQL/PLpgSQL is the default boundary, not an unquestioned permanent
constraint. Trigger CX before declaring shortest search complete when all of
the following hold:

- the best portable candidate/reference upper confidence bound remains above
  `1.10` and its absolute gap exceeds measurement resolution and materiality;
- at least two plausible portable alternatives have failed;
- profiling attributes the residual to unavoidable SPI, recursive-CTE,
  hashing, or relation bookkeeping;
- a native extension is a product/deployment option rather than a prohibited
  portability trade.

When triggered, produce an explicit architecture decision record and measured
prototype comparison covering:

- the best portable executor;
- a native C or Rust PostgreSQL extension with in-backend adjacency/visited
  structures;
- deployment and upgrade support across required PostgreSQL environments;
- managed-service compatibility;
- packaging, ABI, security, observability, and rollback costs;
- measured latency, throughput, memory, and scale gains.

Do not add a native extension speculatively. Do not declare portable performance
optimal if the remaining measured gap is material and native execution is a
permitted product option that has not been evaluated.

CX exits with either an accepted implementation that passes the C3/C3G and C8
gates, a measured rejection, or an explicit product decision that native code
is outside the supported portability boundary. The last outcome permits the
claim "optimal within the declared portable architecture," not an unqualified
claim of absolute optimality.

## Phase C8: Concurrency, memory, cancellation, and soak qualification

Serial one-connection performance is necessary but insufficient. Run:

- concurrency 1;
- concurrency equal to half the configured pool size;
- concurrency equal to pool size;
- concurrency twice pool size to expose queue behavior;
- cold first call on one open session;
- cold fan-out across every physical pool connection;
- mixed shortest, generic traversal, ADCS, and lookup traffic;
- cancellation during shallow, deep, and disconnected searches;
- success, error/rollback, then success on the same session;
- at least 10,000 calls for workspace/cache growth and bloat detection.

Capture QPS, p50/p95/p99, pool acquisition wait, errors, cancellations,
timeouts, backend count, server CPU where available, Go allocations/heap,
temporary I/O, and per-session/whole-pool workspace bytes.

Before capture, declare absolute byte ceilings for one session and the complete
configured pool. Derive the per-session allowance from the deployment's total
performance-memory budget, maximum physical connections, and reserved server
headroom. Stable-but-excessive memory is a failure; "bounded" alone is not an
operational budget.

Rollout requires:

- no state leakage or semantic mismatch;
- no unbounded workspace, cache, catalog, or prepared-statement growth;
- per-session and whole-pool peak/steady memory remain below the declared
  absolute ceilings;
- no normal-tier temp spill;
- expected throughput scaling until the measured database or pool saturation
  point;
- no confirmed p95/p99 or throughput regression outside the A/A-derived
  allowance;
- prompt cleanup and session reuse after cancellation/error.

## Phase C9: Cost-weighted complete-corpus optimization loop

After the traversal critical path passes C8, rerun the complete PostgreSQL
corpus and rank remaining work by addressable cost:

```text
priority = workload_frequency
         * max(cysql_latency - best_correct_reference_latency, 0)
         * confidence
         * concurrency_or_resource_amplifier
```

Production workload frequency is preferred. If it is unavailable, publish an
equal-weight ranking plus sensitivity tables rather than pretending benchmark
case count is production frequency.

The current candidate suggests relationship count, large-list adjacency, and
some reconciliation/delete forms may be next, but each must receive a
PostgreSQL reference and component waterfall before implementation begins.
Repeat the same loop:

1. prove addressable cost;
2. compare at least two plausible designs for a material hotspot;
3. ship the Pareto winner in an isolated change;
4. validate scale, resources, concurrency, and the complete corpus;
5. publish accepted and rejected artifacts;
6. stop only under the workstream completion rule.

## Cross-phase correctness matrix

Every affected executor, representation, hydration, cache, and plan strategy
must preserve:

- exact graph scoping, including colliding entity IDs in another partition;
- direct, linear, diamond, dead-end, disconnected, and cyclic graphs;
- outbound, inbound, directionless, self-loop, parallel-edge, and mixed paths;
- relationship uniqueness within one path and permitted reuse across rows or
  independent pattern paths;
- repeated nodes where legal;
- exact node/relationship order and direction;
- one valid `shortestPath` tie and every valid `allShortestPaths` tie;
- minimum and maximum depth, including zero-depth behavior;
- same-endpoint error behavior;
- missing, null, contradictory, literal, parameter, and safe-cast endpoints;
- duplicate endpoint/path multiplicity and ADCS Cartesian multiplicity;
- `OPTIONAL MATCH` null preservation;
- property/kind predicates before scalarization;
- no silent substitution of a longer path when a selected shortest-path
  post-filter fails;
- path functions, aliases, composed projections, and mutation-returning
  conservative fallback;
- multiple calls in one statement, sequential transactions, rollback,
  cancellation, and concurrent physical connections;
- stable template invalidation after graph/schema/kind changes.

Mutation and translation fixture requirements from `AGENTS.md` and
`perf_rework_plan.md` remain mandatory.

## Statistical and reporting protocol

For every runtime behavior increment:

1. Predeclare target cases, control cases, primary metrics, and expected
   direction before capturing candidate results.
2. Use fresh, equivalent, analyzed fixtures and a pinned physical connection
   for serial session-state measurements.
3. Capture at least five independently reloaded matched rounds with 30-50 warm
   observations per round for p50/p95. Use enough rounds/samples for the
   A/A-derived resolution.
4. Treat p99 as a gate only at the A/A-derived sample size and with at least
   roughly 100 expected top-one-percent observations, normally 10,000 or more
   samples per gated series across independent blocks. Otherwise report p99 as
   diagnostic.
5. Alternate candidate/predecessor order for every PostgreSQL A/B. Run Neo4j
   exact-result checks for every increment, but capture full Neo4j latency and
   alternate backend order only for C0, periodic context snapshots, and release
   qualification. Never overlap destructive batches.
6. Run exact untimed preflight/postflight observations.
7. Compare immediate predecessor, C0, and PostgreSQL reference separately.
8. Screen the complete corpus, then confirm an apparent regression in isolated
   matched rounds before changing production code.
9. Keep Neo4j latency in the report but outside CySQL performance pass/fail.
   Neo4j result disagreement remains a semantic failure.
10. Publish every experiment bundle, including rejected experiments.

Each result table includes at least:

| Metric | Predecessor | C0 | PG reference | Candidate | Candidate/reference |
|---|---:|---:|---:|---:|---:|
| End-to-end p50 | | | | | |
| End-to-end p95 | | | | | |
| End-to-end p99 | | | | | |
| Client compile | | | | | |
| Pool/transaction | | | | | |
| PostgreSQL planning | | | | | |
| PostgreSQL execution | | | | | |
| Transfer/decode/drain | | | | | |
| Shared/local/temp buffers | | | | | |
| Temp/workspace bytes | | | | | |
| Allocations/bytes | | | | | |
| Rows/edges examined | | | | | |
| Cold first call | | | | | |
| QPS/pool wait | | | | | |

## Pull-request and experiment sequence

Keep each production behavior change independently attributable.

1. **Continuation benchmark completeness**
   - Required-key/status manifest, `Meta` mapping, destructive lock, local/temp
     metrics, environment manifest, A/A mode, concurrency-runner scaffolding,
     and durable artifact workflow.
2. **Generated traversal matrices**
   - Real shortest/ADCS generated cases, configuration/checksum reporting,
     timeouts, and normal/largest tier selection.
3. **PostgreSQL references and component probes**
   - Round-trip, search-only, hydration-only, identical-decode references, and
     shortest/client waterfall reports. No production strategy change.
4. **Shortest executor tournament record**
   - Test adapters and experimental artifacts for S0-S3. No dormant production
     dispatcher branches.
5. **Selected singleton executor**
   - Typed helper/SQL, explicit lowering decision, stable template, distance and
     one-path modes, generic fallback, schema down/up coverage.
6. **C3G generic/all-shortest optimization**
   - Pair batching/sharing, compact state, predecessor-DAG alternatives, and
     performance-qualified dynamic fallbacks.
7. **Path materializer comparison**
   - M0/M1 results first; ship the selected linear representation separately
     from batch-across-row hydration.
8. **Batched path hydration, if it wins**
   - Output-row ordinals, deduplicated hydration, exact duplicate/order tests.
9. **C5A staged scalar traversal state**
   - Last-use lowering and specialized record shapes, with multiplicity
     negative tests.
10. **C5B result decode/allocation path**
   - Field metadata, copy/ownership, composite codec, or streaming changes.
11. **C5B list-cardinality strategies, if still addressable**
    - `ANY`/`unnest`/adjacency and plan-policy comparison after decode work.
12. **C6 ADCS suffix and combined branches**
    - Density-aware suffix decision, then exact expansion sharing only if it
      remains addressable.
13. **Conditional translation/template cache**
    - Parsed/optimized cache before complete templates; invalidation and race
      coverage in each increment.
14. **Conditional CX portability decision**
    - Native-extension ADR and measured prototype only if the portable gap
      triggers it.
15. **Concurrency and soak qualification**
    - Pool fan-out, QPS/tails, resource footprint, cancellation, and long-run
      stability.
16. **Complete-corpus reprioritization**
    - Cost-weighted next-work report and the next continuation plan if needed.

An experiment may use a temporary benchmark-only branch or helper, but rejected
code must not remain behind an unused feature flag.

## Immediate next actions

Execute these in order:

1. Preserve and checksum the current working tree and rerun the final required
   PostgreSQL/Neo4j validation after benchmark normalization.
2. Complete C0: fix missing PostgreSQL cases, add required-key validation,
   prevent destructive overlap, and publish A/A measurement resolution plus
   materiality thresholds.
3. Wire real cases to `generated_shortest_paths` and `generated_adcs` before
   choosing another search or suffix implementation.
4. Add C1 raw PostgreSQL and component references.
5. Attribute at least 90% of shortest server time.
6. Run S0-S3 as an executor tournament and select by the complete envelope.
7. Ship the selected typed singleton path with distance/path specialization.
8. Measure and optimize C3G generic/multi-pair/all-shortest forms before
   claiming whole-family shortest optimality.
9. In parallel after C1, run M0/M1 materializer comparisons; integrate the
   winner only after shortest search is measured independently.

Do not start with translation caching, another ADCS percentage target, or an
unmeasured rewrite of list-heavy SQL.

## Definition of done

This continuation is complete when:

- Neo4j has no latency threshold in the CySQL performance gate; it remains an
  exact-result and informational implementation oracle.
- Every declared PostgreSQL benchmark key is present and included in corpus
  completeness checks; every required supported key is successful and included
  in performance comparisons.
- C0, PostgreSQL references, A/A measurement resolution and materiality
  thresholds, raw samples, environment manifests,
  and all accepted/rejected experiment bundles are durably published.
- The selected proven-singleton shortest executor is optimal under the
  workstream completion rule across small, deep, high-fanout, disconnected,
  cold, warm, and concurrent cases.
- C3G generic, correlated, multi-pair, and all-shortest workloads independently
  satisfy the completion rule; the plan does not infer whole-family optimality
  from singleton results.
- Distance-only shortest carries no predecessor/path state. One-path shortest
  uses the tournament-winning bounded representation; any complete trail array
  has a measured advantage and explicit bound rather than being retained by
  default.
- Path materialization is linear, graph-scoped, performed once per path boundary
  or once per winning batch, and satisfies the C4 paired-tax and PostgreSQL
  reference gates.
- Variable traversal carries only fields required at each stage and preserves
  path/endpoint multiplicity.
- ADCS endpoint, path, suffix, P2, and combined forms are within `1.10` times
  their best correct PostgreSQL references or below measurement resolution,
  without duplicate traversal or hydration work.
- Large-result traversal is within `1.15` times the raw-pgx identical-decode
  reference or below measurement resolution, with bounded allocations and no
  speculative SQL rewrite.
- C7 is either not triggered by measured material cost or stable CySQL
  compilation and PostgreSQL plan overhead are within 15% of their component
  references or below measurement resolution.
- Any triggered CX native-extension decision has an accepted/rejected measured
  result or an explicit portable-architecture boundary.
- Normal and largest scale tiers, full-pool cold fan-out, concurrency,
  cancellation, error/rollback, and 10,000-call soak gates pass with bounded
  cache, prepared statement, catalog, and workspace growth and with memory
  below the declared per-session and whole-pool ceilings.
- The complete corpus has no unapproved confirmed latency/resource regression,
  and remaining work is reprioritized by addressable production cost.
- Formatting, unit/race tests, generated fixtures/goldens, schema down/up
  round-trips, and separate PostgreSQL and Neo4j `make test_all` runs pass.

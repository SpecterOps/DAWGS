# PostgreSQL traversal performance plan

Date: 2026-08-19

Status: proposed; no candidate in this document is authorized for production

## Objective

Close the largest remaining PostgreSQL-versus-Neo4j traversal gaps without
reviving a terminal experiment, weakening exact Cypher semantics, or moving
unbounded work into an unmeasured fallback. The work is ordered by expected
portfolio impact, evidence already available, and the likelihood that a new
architecture can beat the current PostgreSQL incumbent.

The immediate objective is not to make PostgreSQL win every synthetic case.
It is to replace the worst production-relevant execution paths with bounded,
observable candidates that improve their declared targets while preserving
controls, cancellation, resource limits, and exact results.

## Current position

PostgreSQL already wins most of the broad corpus, but the remaining losses are
concentrated:

| Priority signal | Current PostgreSQL/Neo4j result | Relevant evidence |
| --- | ---: | --- |
| Hidden-fan-in distance stress | `60.34x` slower | An exact reverse-distance component previously made PostgreSQL `6-10x` faster, but SP-I2 V1 and V2 are terminal |
| Sparse fixed-suffix path | `57.79x` slower | Exact suffix reverse plus ordered-ID hydration ran at `0.278ms` versus Neo4j at `1.117ms` |
| Other sparse fixed-suffix forms | `45.94-49.53x` slower | Automatic forward dispatch remains the problem |
| Base unbounded shortest path | `7.74x` slower | B1/B2 compact kernels exist but are unqualified |
| Base unbounded shortest distance | `6.30x` slower | Same kernel and selection boundary |
| All-shortest stress cases | `4.84-5.40x` slower | ASP-I1 improved the focused median by `57.2%` but regressed shallow controls |

The evidence establishes three boundaries:

1. Exact suffix-seeded reverse search and ordered-ID hydration are already fast
   enough. Same-statement topology probes and dual-arm dispatch erase too much
   of that win.
2. The edge table already has covering start/kind, end/kind, and kind-first
   indexes. Another minor variation of those indexes is unlikely to close a
   `5-50x` traversal gap.
3. Query probes remain useful for diagnosis, but repeated attempts show that
   paying for selection inside every statement is not a viable fast path for
   shallow or selective traversal.

## Optimization ceiling assessment

The project is near the useful limit of **in-statement query probing for
runtime routing**, not the limit of PostgreSQL performance work. Existing
probes have successfully identified topology and protected exact fallback, but
their fixed scans, materialization, and dual-arm planning costs now dominate
the fastest candidates. Adding more probe terms is a diagnostic activity, not
the default optimization strategy.

Generated SQL and stored-function work still have room where an execution
kernel can avoid workspace setup, repeated scans, hydration, or unnecessary
output. Beyond that, the remaining levers are architectural: transaction-local
retry, generation-keyed decisions, maintained topology data, and adjacency
layout. This plan escalates through those layers only after a cheaper layer
reaches a measured stop condition.

## Priority order

| Priority | Workstream | Expected impact | Confidence | Start condition |
| --- | --- | --- | --- | --- |
| P0 | Re-establish a clean measurement baseline | Prevents optimizing stale or unrepresentative deltas | High | Immediate |
| P1 | Probe-free fixed-suffix reverse with transaction-local retry | Addresses four of the five largest raw gaps | High | After P0 |
| P2 | New hidden-fan-in distance successor | Addresses the largest raw gap using a proven component | Medium | Only after prospective power passes under a new identity |
| P3 | Compact unbounded shortest-path kernel | Addresses stable `6-8x` losses | Medium | After P1 reaches a terminal decision |
| P4 | Bucketed all-shortest predecessor-DAG execution | Addresses `5x` losses without broad shallow regressions | Medium-low | After P3 search telemetry is reusable |
| P5 | No successor selected | The duplicate-adjacency architecture is terminal and native-extension work is withdrawn | N/A | A distinct non-native architecture needs a new protocol |

P1 and P2 may share diagnostic infrastructure, but formal evidence must remain
serial. Do not run multiple protected studies concurrently on one host or use
one study's holdout to tune another.

## Rules that apply to every workstream

- Preserve exact row, path, trail-multiplicity, direction, relationship-kind,
  self-loop, cycle, no-path, and mutation semantics.
- PostgreSQL and Neo4j core corpus cases remain backend-equivalent. Candidate-
  specific capability tests belong in driver-scoped suites.
- No timed candidate row may become public until its overflow, fallback, and
  semantic status is final.
- A retry or fallback must use the same Repeatable Read transaction snapshot.
- Candidate, retry, and incumbent work must have distinct runtime receipts and
  independently attributable resource counters.
- Missing or contradictory counters fail closed; they are never interpreted as
  zero work.
- No protected holdout is selected, instantiated, or timed before a clean
  training freeze authorizes it.
- Freeze sample counts, thresholds, scenario seeds, schedules, and power
  calculations before formal candidate timing.
- Terminal identities remain immutable. In particular, do not revive or retune
  `orientation-probe-v2`, `suffix-reverse-guard-v1`, `sp-i2-distance-v1`, or
  `sp-i2-distance-v2`.
- Every new automatic policy has an evidence-free rollback switch and leaves
  the current incumbent available.
- Benchmark planning time, execution time, first-session cost, buffers, WAL,
  temporary bytes, workspace high-water memory, and fallback frequency
  separately. A faster median cannot hide a resource or cold-session failure.

## P0: clean baseline and opportunity accounting

### 2026-08-19 recapture

P0 was recaptured from clean committed source `a4b29f2` with binary SHA-256
`6745fca9c3b79b48875a0d5555f375f458af34d938e20e545bb6bce27379fca2`.
Two independently reloaded rounds used one warm-up and three timed iterations
with a one-connection PostgreSQL pool. The ignored raw artifacts are
`.coverage/p0-sql-routing-20260819/round-1.jsonl`
(`a50836f08eaf85fac839f09d15420f060445ffb5312bf3bf609552fc86ad26ca`)
and `.coverage/p0-sql-routing-20260819/round-2.jsonl`
(`fb6198257d15c71f2dcb348e1d2c3d7a9579b27ef44d983b3d5fc50f0336e262`).

All 704 backend records were `ok`; 184 complete observations were comparable
and equal across the two backends. The refreshed descriptive ledger identifies
`GFSE-V2-D16-F1000-R1-X1-M1-sparse_path` as the largest repeated loss at
`91.06x` PostgreSQL/Neo4j, followed by hidden-fan-in distance at `79.03x`.
All-shortest cases now make up the broadest high-loss cluster, with several
`26-36x` losses. This remains opportunity accounting only: it opens no
protected corpus, production selector, or terminal identity.

### Purpose

Ensure the priority order still reflects a clean, current binary and identify
whether production frequency changes the raw-ratio ranking.

### Work

1. Build one clean GraphBench binary and capture the broad PostgreSQL/Neo4j
   corpus with destructive integration guards enabled.
2. Use at least two independently reloaded descriptive rounds for ranking;
   formal candidate protocols will define their own larger counts.
3. Recompute per-family median and geometric-mean ratios, worst cases, semantic
   failures, planning/execution split, buffers, and result sizes.
4. If representative production frequency data is available, add a separate
   frequency-weighted opportunity score. Never replace the unweighted corpus
   report with it.
5. Record the exact incumbent SQL digest and structured plan for every target
   that enters P1-P4.
6. Add or refresh component measurements that isolate search, hydration,
   guarded dispatch, transaction setup, and decoding.

### Exit

- The clean report still identifies fixed-suffix traversal as the largest
  multi-case opportunity, or this document is revised before candidate work.
- Every selected target has an exact semantic oracle and stable component
  decomposition.
- No candidate policy has been enabled.

## P1: probe-free fixed-suffix reverse

### Hypothesis

The proven exact suffix-reverse kernel can remove the `46-58x` sparse deficits
if the successful fast path executes neither topology probes nor the forward
body. Resource overflow can be handled as a transaction-local retry instead of
embedding both arms in one SQL statement.

### Candidate identity

Create a new identity and protocol; names below are placeholders until the
preregistration commit:

- executor: `EXPANSION-SUFFIX-REVERSE-OPTIMISTIC-V1`;
- policy: `suffix-reverse-retry-v1`;
- incumbent: `EXPANSION-STEPWISE-FORWARD`;
- component: the existing exact suffix-seeded reverse search with ordered node
  and edge ID hydration.

The new policy must not share a selector or evidence identity with either
retired orientation generation or `suffix-reverse-guard-v1`.

### Execution contract

1. Admit only read-only, complete-path fixed-suffix shapes whose direction,
   endpoint binding, relationship kinds, and observation boundary are fully
   understood by the existing exact reverse emitter.
2. Start a Repeatable Read transaction before candidate execution.
3. Execute a reverse-only statement with explicit state, suffix, output, and
   byte sentinels. Do not include topology probes or a forward CTE/body.
4. Buffer candidate output until the statement returns a complete status and
   every sentinel passes. Overflow, cancellation, receipt failure, or semantic
   uncertainty exposes zero candidate rows.
5. On declared overflow only, clear candidate-local state and execute the exact
   forward incumbent in the same transaction. Record `reverse_complete` or a
   precise `forward_retry_<reason>` receipt.
6. Commit only after the chosen arm drains and validates. Any error rolls back
   the whole attempt.

This retry is an execution strategy, not a correctness shortcut: both arms are
exact, and the stable snapshot prevents candidate/fallback drift.

### Routing experiments

Evaluate routing in this order:

1. **Static structural enrollment.** Run optimistic reverse for the narrow
   complete-path family and rely on bounded retry. This is the minimum viable
   candidate and avoids a metadata dependency.
2. **Generation-keyed decision cache.** Cache completed reverse/overflow
   outcomes by graph mutation epoch, normalized query digest, relevant bound
   endpoint IDs, direction, kinds, and depth. A miss follows the static policy;
   stale entries are unusable.
3. **Topology synopsis shadow.** Reopen the deferred synopsis only under a new
   version. Compare its prediction with exact-arm results and runtime counters;
   it may improve routing but may not prove correctness or suppress retry.

Do not begin with persistent synopsis tables. First prove that static retry
closes the long poles and measure whether its remaining regret justifies
schema, refresh, cache-invalidation, and mutation write amplification.

### Development tournament

Compare, in fixed balanced order:

- exact forward incumbent;
- exact reverse component;
- optimistic reverse with retry disabled on cases proven below the caps;
- optimistic reverse with real retry;
- optional cache/synopsis routing only after the preceding arms are stable.

The open corpus must include sparse wins, shallow controls, high reverse fan-in,
dense suffixes, cap-1/cap/cap+1 boundaries, no path, cycles, self-loops,
parallel kinds, duplicated endpoints, multiple paths, cancellation, and a
forced retry.

### Early stop gate

Stop this policy generation before holdout or driver work unless all open
training cases satisfy:

- exact observations and complete receipts;
- successful reverse fast-path overhead versus exact reverse within `1.10` or
  `100us` at the median, with p95 ratio upper at most `1.05`;
- target improvement versus exact forward with median-ratio upper at most
  `0.95` or saving lower at least `100us`, with p95 upper at most `1.05`;
- control/retry median-ratio upper at most `1.10` or worst plausible overhead
  at most `100us`, with p95 upper at most `1.05`;
- zero forward executor work on `reverse_complete` and zero public candidate
  rows before every retry;
- bounded memory, no unexpected read-path WAL, and no leaked session state.

If transaction setup or the retry protocol itself cannot meet the exact-reverse
overhead gate, terminate the generation. Do not add another same-statement
probe to rescue it.

### Formal sequence

1. Freeze a fresh training/holdout corpus and power simulation.
2. Pass clean host A/A and prospective power before candidate timing.
3. Capture training only, including resource, cold-session, cancellation, and
   retry evidence.
4. Freeze only if every case passes.
5. Authorize and capture the untouched holdout once.
6. Recompute training from raw artifacts during confirmation.
7. Produce reference closure, operational evidence, promotion manifest, and a
   rollback canary limited to the passing buckets.

### P1 success definition

- The broad automatic-production benchmark no longer reports the sparse
  fixed-suffix family among its largest losses.
- At least the declared sparse target cases reach parity with Neo4j while
  controls stay within PostgreSQL incumbent bounds.
- The successful reverse path contains no selection scan and no inactive
  forward body.

### P1 disposition

The clean six-round `suffix-reverse-retry-v1` capture is terminally rejected:
the retry fast path exceeded the frozen exact-reverse overhead gate on every
open case. Its exact receipt and timing evidence is recorded in
[`docs/experiments/suffix_reverse_retry_v1.md`](docs/experiments/suffix_reverse_retry_v1.md).
No P1 holdout was opened. The subsequently frozen P2 V3 power study is also
terminal, so no existing P1 or P2 candidate may proceed.

### P1-SR1: externally selected single-arm reverse feasibility

The fresh P0 record demonstrates that fixed-suffix remains the highest single
SQL-only opportunity, but it does not authorize another retry, probe, or
always-reverse policy. The separately named, default-off
`sql-strategy-routing-preflight-v1` generation isolates only the existing
exact reverse component and the cost of an external decision boundary. It
cannot use terminal P1 receipts as qualifying evidence and cannot enable a
production cache or selector. Its frozen preimplementation contract is
[`docs/experiments/sql_strategy_routing_preflight_v1.md`](docs/experiments/sql_strategy_routing_preflight_v1.md)
with machine-readable inputs in
[`benchmark/testdata/scale/protocols/sql_strategy_routing_preflight_v1.json`](benchmark/testdata/scale/protocols/sql_strategy_routing_preflight_v1.json).
The direct component's clean recapture passed its descriptive target and
resource checks, but its first boundary/workspace artifact predates per-sample
raw-PGX public-observation hashes. Recapture that closure before freezing or
implementing any cache-feasibility work.

## P2: hidden-fan-in distance successor

### Rationale

Hidden fan-in is the largest raw gap, and the reverse-distance component has
already shown large wins. The blocker is not lack of another SQL variant: V1
failed its cycle tail and V2's fixed design had inadequate prospective power.
Further work therefore begins with a new evidence design, not a V2 recapture.

### P2 disposition

The distinct V3 prospective power study is terminally rejected before any
candidate or corpus work: its frozen 800-block design could not establish the
required physical-order A/A admission power. The source, scenario vectors, and
terminal record are in
[`docs/experiments/sp_i2_successor_power_study_v3.md`](docs/experiments/sp_i2_successor_power_study_v3.md).
Do not enlarge or retune that study. P3 is the next eligible workstream.

### Preconditions

1. Assign a new generation, executor, policy, selector, corpus, and rollback
   identity. Reusable code components do not imply reusable evidence.
2. Use the archived V1/V2 open traces to freeze a design whose 95% Wilson lower
   power bound meets the declared requirement for A/A, targets, controls, and
   arm-order strata.
3. Freeze sample counts large enough for the observed cycle-tail variance. If
   the required study is operationally unreasonable, stop before coding.
4. Retain fresh adverse controls for direct acyclic, direct cyclic,
   post-target-cycle, dense reconvergence, and no-path exhaustion.

### Architecture tournament

Use open cases to compare the smallest reusable alternatives:

- the current S4 incumbent;
- the retired algorithms as read-only descriptive references;
- reverse-physical ID-only distance with target-terminal pruning;
- a reverse-only optimistic candidate with transaction-local S4 retry, if that
  removes material same-statement admission overhead;
- direct one/two-hop floors only when their inactive recursive work is proven
  zero.

Select the smallest candidate that meets semantic, tail, and resource gates.
Do not select an arm solely because it wins the stress target.

### Stop gate

- Prospective power failure terminates the new protocol before formal timing.
- Any cycle/control p95 upper above `1.05`, unexplained fallback, incomplete
  receipt, or unbounded state terminates the generation.
- No threshold, sample count, or cohort changes after formal timing begins.

### P2 success definition

The automatic policy removes the hidden-fan-in distance long pole on its exact
passing buckets, retains bounded cycle/control behavior, and leaves all other
shortest-path shapes on the incumbent.

## P3: unbounded one-path and distance search

P3's telemetry-first preflight is frozen in
`docs/experiments/sp_bidirectional_p3_preflight_v2.md` and
`benchmark/testdata/scale/protocols/sp_bidirectional_p3_preflight_v2.json`.
The current B1/B2 function-workspace arms are terminally rejected: complete
telemetry showed them materially slower than S4 on every frozen target. A new
P3 executor must use a distinct generation and power study; V2 cannot authorize
component work, formal timing, a selector, or a holdout.

### Hypothesis

The remaining `6-8x` unbounded losses require a compact bidirectional execution
kernel and predictable reusable workspace, not additional outer SQL probes.

### Work

1. Complete trustworthy B1/B2 telemetry first: per-level frontier/seen rows,
   predecessor rows, meeting depth, scheduler decisions, fallback reason,
   workspace high-water bytes, spill, WAL, and inactive-arm work.
2. Benchmark S0/S3/S4, B1 strict alternation, and B2 smaller-current-level on
   the same open corpus and component boundaries.
3. Isolate workspace reset, temporary-table access, search, witness recovery,
   hydration, and result decoding.
4. Prefer one reusable typed workspace per session. Avoid catalog churn,
   unbounded arrays, and repeated full-table truncation where generation-stamped
   logical clearing is measurably cheaper and bounded.
5. Add exact zero/one/two-hop floors only where they bypass workspace setup and
   cannot alter self-endpoint semantics.
6. Select B2 only if it stably beats both incumbent and B1 across the frozen
   target buckets. Otherwise keep the simpler B1 or incumbent.

### Corpus

Cover outbound/inbound, typed/untyped, one/multiple kinds, shallow/deep,
sparse/dense, asymmetric frontiers, early/late meeting, disconnected, cycles,
self-loops, parallel kinds, cap boundaries, and path versus distance output.

### Stop gate

Terminate a candidate that cannot materially beat the incumbent on every
declared target without regressing controls beyond `1.10` or `100us`, contain
p95 at `1.05`, and pass measured memory/spill/WAL limits. Hidden function work
or unavailable workspace counters cannot qualify.

### P3 success definition

The selected exact buckets reduce the base unbounded shortest-path and distance
ratios materially, ideally to PostgreSQL/Neo4j parity, without broadening to
untyped or multi-kind shapes that did not independently pass.

## P4: all-shortest paths

### Hypothesis

All-shortest performance can improve through output-sensitive predecessor-DAG
discovery and batched hydration, but shallow shapes should remain on A1.

### Work

1. Reuse P3's trustworthy search/workspace telemetry rather than create a new
   opaque counter family.
2. Re-run A1, ASP-I1, ASP-B1, and ASP-B2 on open cases with complete path-
   multiset equality and separate discovery, predecessor, enumeration, output,
   and hydration timing.
3. Preserve all relationship-distinct predecessors at minimum depth; never
   reduce the multiset to one witness.
4. Stream or batch ordered-ID hydration only after enumeration/output-byte
   sentinels pass.
5. Prefer static, evidence-backed buckets such as deeper typed singleton pairs.
   Keep shallow diamond and parallel-kind regressions on A1.
6. Do not add a runtime selector unless its lookup/dispatch cost independently
   clears the same overhead gate used for P1.

### Stop gate

No candidate advances if it changes the exact path multiset, saturates an
undeclared limit, hides enumeration/output memory, or fails any shallow adverse
control. A focused median win cannot average away one failed case.

### P4 success definition

The passing deep/stress buckets reduce the current `4.8-5.4x` gaps, while A1
remains automatic for shallow or high-multiplicity shapes that did not pass.

### P4 disposition

The one-authorized `ASP-I1-U-DAG+MAT-M0` preflight is terminally rejected for
the current P4 generation. It improved the selected disconnected no-path
target, but regressed every frozen shallow/reconvergent control at the pooled
median. Its exact observations and complete telemetry are recorded in
[`docs/experiments/asp_p4_i1_disconnected_preflight_v1.md`](docs/experiments/asp_p4_i1_disconnected_preflight_v1.md),
but they authorize no power study, extra I1 timing, holdout, selector, or
manifest work. A future P4 successor must have a distinct executor and a new
frozen clean-source roster.

## P5: architectural escalation beyond generated SQL

Begin this lane only when P1 or P3 shows that the best exact SQL/function
candidate remains materially behind Neo4j after search, hydration, routing,
and workspace costs have been isolated.

Evaluate independently:

- a graph-generation-scoped topology synopsis with atomic refresh and explicit
  staleness;
- transactionally maintained typed degree/heavy-hitter summaries;
- a denormalized adjacency representation optimized for ordered traversal;
- an application-side traversal service only if transaction snapshot and
  consistency semantics can be preserved.

Each option must report read improvement alongside mutation latency, WAL,
storage amplification, refresh cost, upgrade/downgrade behavior, backup and
restore implications, and operational complexity. Planner estimates or sampled
statistics may guide performance routing, but they never establish correctness
or remove exact fallback.

### P5 feasibility disposition

The initial read-only feasibility inventory is deferred before implementation:
the existing partitioned edge layout already has covering indexes in both
physical directions, but it has no mutation epoch or cache-generation contract
for a maintained copy. No P5 successor is selected. See
[`docs/experiments/p5_architecture_feasibility_v1.md`](docs/experiments/p5_architecture_feasibility_v1.md).
The separately frozen
[`docs/experiments/p5_adjacency_materialization_feasibility_v1.md`](docs/experiments/p5_adjacency_materialization_feasibility_v1.md)
has an opt-in, PostgreSQL-integration-validated shadow implementation and
authorizes only its mutation/storage inventory. No Cypher candidate, selector,
cache-key change, protected timing, or production schema is authorized until a
distinct budget decision follows that report.

## Validation matrix

Every implementation checkpoint runs:

1. `make format` or the narrow equivalent for touched generated/Go/SQL files;
2. `make test` during development;
3. mutation and template tests for every parser, renderer, or query-semantic
   change;
4. destructive PostgreSQL `make test_all` with the disposable target allowlist;
5. destructive Neo4j `make test_all` for backend-equivalent semantic cases;
6. focused race tests for policy/cache/session state;
7. schema upgrade/downgrade, graph reload/drop, cancellation, rollback, pool
   reuse, concurrent reader/writer, and stale-cache tests where applicable;
8. clean-source bundle reconstruction and credential scans before evidence.

Performance artifacts remain ignored and checksummed. Checked-in declarations,
protocols, terminal tombstones, and corpus sources are the durable record.

## Decision checkpoints

After each priority lane, update `report_summary.md` with:

- exact source and binary identities;
- cases and backend versions;
- semantic disposition;
- median and p95 intervals versus the PostgreSQL incumbent;
- descriptive PostgreSQL/Neo4j ratios;
- resource and cold-session disposition;
- holdout status;
- promotion or terminal decision;
- the next priority after accounting for the new broad benchmark.

Do not begin the next formal lane until the prior lane has one of three explicit
outcomes: promoted for exact buckets, terminally rejected with a tombstone, or
deferred before protected timing with a documented reason.

## Evidence inputs

- [`report_summary.md`](report_summary.md) records the current broad deltas and
  terminal SP-I2 results.
- [`docs/experiments/remaining_outlier_delivery_v1.md`](docs/experiments/remaining_outlier_delivery_v1.md)
  records the exact-reverse, hydration, orientation, and static-guard evidence.
- [`docs/experiments/fixed_suffix_cardinality_metadata_audit.md`](docs/experiments/fixed_suffix_cardinality_metadata_audit.md)
  explains why existing translator and planner metadata cannot prove the
  fixed-suffix state ceilings.
- [`docs/experiments/traversal_topology_synopsis_adr_v1.md`](docs/experiments/traversal_topology_synopsis_adr_v1.md)
  defines the deferred synopsis, staleness, mutation, and cache-key boundary.
- [`docs/experiments/traversal_priority_implementation_status_v1.md`](docs/experiments/traversal_priority_implementation_status_v1.md)
  inventories the existing SP/ASP kernels, telemetry, and promotion gaps.
- [`docs/experiments/asp_i1_inline_v1.md`](docs/experiments/asp_i1_inline_v1.md)
  defines the current all-shortest correctness and resource boundary.
- [`docs/experiments/asp_p4_open_baseline_v1.md`](docs/experiments/asp_p4_open_baseline_v1.md)
  freezes the fresh training-only P4 baseline needed to choose an all-shortest
  target without reusing protected historical captures.
- [`docs/experiments/asp_p4_i1_disconnected_preflight_v1.md`](docs/experiments/asp_p4_i1_disconnected_preflight_v1.md)
  freezes the sole authorized A1-versus-I1 PostgreSQL telemetry preflight for
  that selected disconnected target.

## Immediate next actions

1. Retain the completed P4 A1 baseline and terminal I1 preflight; do not run a
   power study, broader I1 timing, protected cases, or selector work from them.
2. Retain the passed SQL-routing boundary/workspace closure from clean commit
   `c490f3c` at `.coverage/sql-routing-component-closure-v1-c490f3c`; keep the
   earlier `94fe902` and failed `36b1bac` artifacts unchanged as diagnostics.
3. Retain the frozen transaction-scoped
   `suffix-route-cache-feasibility-v1` protocol. Before any separately
   authorized feasibility implementation, review its key, same-snapshot
   ownership, incumbent-only misses, invalidation, rollback, bounded-memory,
   and zero read-path-WAL requirements.

# CySQL Performance Continuation Plan 2

## Purpose

This document follows `perf_cont_1.md` from the live benchmark state captured
on 2026-08-05. It turns the completed measurement work and provisional shortest
comparator result into the next implementation sequence.

The immediate objectives are:

1. determine whether the two live gate failures are reproducible regressions or
   temporal/environmental drift;
2. finish the missing server-cost attribution needed by the previous plan;
3. reconcile the benchmark candidate names with the S0-S3 architectures from
   the prior plan and complete the singleton executor tournament;
4. ship only the proven singleton subset with distinct distance and path state;
5. optimize path materialization independently from search;
6. return to generic shortest, large-result decoding, ADCS, caching,
   concurrency, and soak work only in evidence-ranked order.

This is a continuation, not a replacement, of `perf_rework_plan.md` and
`perf_cont_1.md`. Their correctness, backend-equivalence, graph-scoping,
mutation/template coverage, statistical, artifact, and operational safeguards
remain in force unless this document makes a narrower rule stricter.

Neo4j remains an exact-result and implementation oracle. Neo4j latency is
diagnostic only and is never a CySQL performance target or gate.

## State entering this continuation

### Implemented measurement foundation

The current working tree contains the prerequisite benchmark work from C0-C2:

- a versioned case/backend declaration used by the executable performance
  gate;
- explicit unsupported-backend declarations with reasons;
- complete-key and status enforcement for PostgreSQL;
- Neo4j exact-result oracle enforcement without a latency threshold;
- a non-blocking destructive benchmark lock;
- deterministic generated shortest and ADCS normal-tier cases with fixture
  configuration, checksums, and cardinalities;
- PostgreSQL `VACUUM (ANALYZE)` after fixture loading;
- source commit, dirty-tree, executable, environment, database, SQL, and
  fixture fingerprints;
- credential-redacted command manifests;
- shared, local, and temporary plan-buffer accounting;
- retained raw cold/warm observations;
- pool wait, transaction setup, execute/decode/drain, backend PID, session
  classification, QPS, and opt-in concurrency blocks;
- alternating-sample A/A resolution reports;
- a C1 ladder containing prepared round trip, endpoint validation, minimum
  graph access, ordered-ID search, isolated hydration, two end-to-end inline-CTE
  comparators, and translated CySQL;
- client parse/optimization/translation/render timing and allocation samples.

These facilities are part of the benchmark contract for every phase below.
They do not, by themselves, qualify an experimental executor for production.

### Authoritative artifacts

The current local evidence is:

| Artifact | Purpose | SHA-256 |
|---|---|---|
| `.coverage/c0/baseline.jsonl` | Five-round C0 baseline | `5ba48428e4fe358b80ab75ca396882c28f70f0bd06c2f24ea73ed3d77d3201d1` |
| `.coverage/live-current/candidate.jsonl` | Five-round live rerun | `a1c2985c5ebfe6c137653759712a972c4ebbfa63a49c12dc6845ce1897899af3` |
| `.coverage/live-current/gate.json` | Complete 151-key comparison | `3744f4f1eac42550e921f91ea99c1f2b546ff3fbab1d4288e554f788b6dcf99c` |
| `.coverage/live-current/aa-resolution.json` | Current A/A resolution | `804a8c7c46a9c7bb008a725baaf1c0fa4219332a2b9c387885eafb05523c3b7f` |
| `.coverage/c2/tournament-summary.json` | Provisional inline-CTE comparison; legacy labels say S1/S2 | `5fc75f1b8a1ceb6b62020b64381daef6c70ae19cf92e718ff7fb484dc7afd32c` |

The source commit recorded for this uncommitted continuation is
`ec7f9abcf1b26fe589e46bf9dbfea8bf1282d100`. Dirty-tree and executable hashes
inside each record distinguish the exact builds.

`.coverage` is staging, not durable publication. Before any production change,
copy the accepted evidence into a reviewed, immutable artifact location and
retain enough source material to reproduce the binary. A commit hash plus a
dirty-tree hash is not sufficient if the dirty patch and executable are lost.

### Live rerun result

Five independently reloaded rounds with 30 warm observations per case produced:

- 375 of 375 declared PostgreSQL records with status `ok`;
- 380 of 380 declared Neo4j records with status `ok`;
- exact result agreement in every declared oracle record;
- 149 of 151 complete performance-gate entries passing;
- no gated p99 series: each A/A arm has 75 samples, far below the required
  10,000 samples per arm.

The two PostgreSQL failures are:

| Case | Baseline p50 | Current p50 | Pooled p50 change | Gated p95 ratio, 95% interval | Reading |
|---|---:|---:|---:|---:|---|
| `LOOKUP-05_repeated_case_insensitive_prefix` | 0.365 ms | 0.634 ms | +73.5% | 1.642, 1.373-1.964 | Screening alert; end-to-end increase is much larger than server-plan movement |
| `GSP-D02-F016_distance` | 6.246 ms | 7.330 ms | +17.4% | 1.303, 1.219-1.540 | Screening alert; PostgreSQL execution also increased |

Important invariants match between C0 and the live rerun for both failures:

- SQL fingerprints are identical;
- fixture checksums are identical;
- `plan_cache_mode`, `work_mem`, and `temp_file_limit` are identical;
- result cardinality and exact observations are identical;
- the shortest case retains the same local-buffer footprint class.

These are failed screening gates, not yet confirmed code regressions. The two
captures were not a contemporaneous matched A/B comparison. Although both
record source commit `ec7f9abcf1b26fe589e46bf9dbfea8bf1282d100`, their
dirty-tree hashes differ (`c8ad5d0e...` versus `0c951f58...`), their executable
hashes differ (`08ad8a31...` versus `800a3cf3...`), and the shared `bhe`
database reports 20 versus 21 graph partitions. The current alternating-sample
A/A report measures jitter inside a capture; it does not measure binary
reload, fixture reload, database-instance, or capture-to-capture drift.

The limitation is visible in the per-capture p95 resolution: C0/current is
approximately 36.1%/23.3% for `LOOKUP-05` and 7.9%/27.1% for the depth-2
shortest case. A fixed 20% screen cannot substitute for contemporaneous,
case-specific noise calibration.

`LOOKUP-05` plan execution moved from approximately 0.11-0.15 ms to
0.13-0.19 ms, while its client-visible tail moved much more. Its first triage
target is therefore scheduling, transaction, transfer/drain, and host noise,
not an assumed SQL-plan regression.

`GSP-D02-F016_distance` plan execution moved from approximately 3.6-5.5 ms to
5.6-7.4 ms. Its first triage target is the incumbent shortest workspace and
server execution path. Current p95 A/A resolution for this case is about 27%,
so its 30% p95 point movement is material but close enough to the resolution
boundary to require an isolated confirmation block.

The one-shot `EXPLAIN` means moved in the same direction despite essentially
unchanged plan/buffer shapes: approximately 0.124 to 0.153 ms for `LOOKUP-05`
and 4.656 to 6.235 ms for the depth-2 shortest case. This supports a
server/environment-drift hypothesis, but the isolated protocol below must
decide it.

### Other observed movements

The complete gate did not confirm a broad regression, but several shortest
cases shifted upward:

- generated depth-1 distance and path: about +14-16% pooled p50;
- generated depth-2 distance and path: about +17% pooled p50;
- generated depth-4/fanout-128 distance: about +28% pooled p50 with a wide
  interval;
- generated depth-4/fanout-128 path: about +25% pooled p50 with a wide
  interval;
- base distance and path: about +11% and +7% pooled p50;
- depth-16 distance and path: about +4% and +8% pooled p50;
- depth-8 inbound distance improved about 8% pooled p50.

Neo4j simultaneously showed informational 26-36% increases on several small
base traversals. Because no production executor changed and both backends saw
some upward movement, temporal host/server drift is a credible contributor.
That inference does not waive the two PostgreSQL failures; it determines the
matched rerun protocol needed to classify them.

### Provisional inline-CTE result and naming correction

The artifact labels do not match the architectures defined in
`perf_cont_1.md`:

| Prior-plan name | Intended architecture | Current implementation/evidence |
|---|---|---|
| S0 | incumbent bidirectional workspace | Measured production control |
| S1 | typed array-resident singleton BFS helper with bounded in-memory state | Not implemented or measured |
| S2 | compact generation-tagged bidirectional trace relation | Not implemented or measured |
| S3 | stable inline recursive CTE | Both current experimental comparators are in this class |

`complete_reference_s1_array_cte` is a unidirectional recursive CTE that
carries `node_ids` and `edge_ids` on every recursive row. The artifact's
distance form still carries those full trails. `candidate_s2_bidirectional_cte`
is a pair of trail-carrying recursive CTEs joined at a midpoint, not the compact
trace-relation S2. In the next artifact schema, call them S3-U and S3-B while
retaining a legacy-name mapping for old reports.

The newest live data nevertheless establishes a valuable provisional result:

- S3-U distance is approximately 19-65 times faster than the incumbent
  workspace harness;
- S3-U full-path output is approximately 4-19 times faster than the incumbent;
- S3-B is slower than S3-U on every measured normal-tier case;
- the current adapter reports the declared row count on disconnected, shallow,
  deep, high-fanout, inbound, distance, and path cases.

This rejects S3-B for the measured normal tier, not the unimplemented S2
architecture. Preserve its artifact and remove any production prototype. S3-U
is a strong provisional comparator, but it is not an exact-result-qualified
executor: `fullComparator` currently checks only row count. It also lacks the
complete semantic, trail-free distance, fallback, memory/spill, cancellation,
depth-32/64, fanout-512/1000, dense-disconnected, and concurrency envelopes.

### ADCS and path findings

The current hand-written ADCS recursive reference is slower than translated
CySQL: roughly 8.1 times the endpoint-ID query and 3.8 times the observed-path
query in the live run. It is not a useful performance floor and cannot justify
an ADCS rewrite.

Shortest S3-U search-only distance is generally 0.17-0.52 ms, while S3-U
full-path output is generally 1.5-1.9 ms. Path construction and hydration are
therefore the next addressable component after singleton search. Search and
materialization must continue to be measured separately.

The current base and depth-16 S3-U pairs leave roughly 1.2-1.3 ms between
distance and full path. The ordinary base traversal shows the same shape:
approximately 0.059 ms and 10 shared hits for its ID-only server work versus
1.615 ms and 130 shared hits when the path is observed. This makes the M0/M1
materializer tournament the first evidence-backed step after singleton search,
ahead of a general traversal-state rewrite.

Large-result cases point first to the client boundary rather than SQL:

| Case | End-to-end median | Diagnostic server execution |
|---|---:|---:|
| `HOP-05_thousand_endpoint_IDs_with_sparse_matches` | 1.910 ms | 0.272 ms |
| `HOP-09_dense_two_sided_ID_sets` | 4.600 ms | 1.319 ms |
| `LOOKUP-11_tenant_adjacency_thousand_property_list` | 10.470 ms | 0.547 ms |

These one-shot server values are attribution hints, not independently sampled
performance gates. C1R must create an identical-SQL raw-pgx boundary before C5
changes query shapes.

## Decisions fixed by the current evidence

The following decisions are predeclared for this continuation:

1. Do not optimize `LOOKUP-05` until an isolated run separates PostgreSQL
   execution from client/host tail cost.
2. Do not treat the depth-2 shortest failure as a candidate regression; the live
   production path is still the incumbent workspace harness.
3. Continue singleton qualification with S3-U as the provisional performance
   leader, but do not call the current artifact S1 or claim exactness from its
   row-count-only comparator.
4. Reject only S3-B for the measured normal tier. True S1 and S2 remain
   unmeasured candidates until built or explicitly closed by a predeclared
   tournament stop rule.
5. Do not start translation/template caching before the selected SQL shapes
   stabilize and C1 proves an addressable client compilation cost.
6. Do not rewrite ADCS from the current hand-written comparator. First build a
   correct competitive reference or show a component gap.
7. Keep directionless, correlated, multi-pair, path-predicate, mutation-return,
   and `allShortestPaths` forms on the generic path until their independent
   phases qualify them.
8. Keep p99 diagnostic until the A/A-derived sample requirement and the minimum
   top-one-percent population are both met.

## Optimization and acceptance rules

The reference-gap, Pareto, and workstream-completion definitions from
`perf_cont_1.md` remain authoritative. This continuation adds these rules:

- A historical-versus-current movement is not a code regression until the
  compared executable/source states are reconstructible or the movement is
  reproduced in an interleaved controlled block.
- A targeted diagnostic corpus may omit unrelated cases only when its artifact
  is marked diagnostic. It must never be accepted by the complete-corpus gate.
- A production fast path must expose an explicit eligibility decision and an
  explicit fallback reason. Absence of a decision is not an acceptable
  fallback contract.
- Distance-only execution must carry no path or predecessor state. Returning a
  dummy or zero-filled path array to satisfy the old projection is not a valid
  specialization.
- Full-path execution must preserve ordered node and relationship identity and
  must not rediscover connectivity when the search already has ordered IDs.
- A specialized helper must be graph-scoped in every query and collision test.
- A performance win cannot compensate for a confirmed semantic, cancellation,
  memory-ceiling, or complete-corpus failure.

## Sequenced delivery plan

| Phase | Outcome | Depends on | Ship decision |
|---|---|---|---|
| C0R | Reconcile live regressions and freeze a reconstructible baseline | Current artifacts | Blocks production performance claims |
| C1R | Complete shortest and client cost attribution | C0R tooling | Blocks final executor selection |
| C2Q | Repair candidate identity and qualify the S0-S3 singleton tournament | C1R | Selects or rejects a ship candidate |
| C3S | Ship selected singleton distance and path lowering | C2Q | First production performance increment |
| C4M | Select minimal path materialization | C3S search stabilized | Second production increment if material |
| C3G | Optimize generic, correlated, multi-pair, directionless, and all-shortest forms | C3S; coordinate with C4M | Required for shortest-family completion |
| C5 | Optimize variable traversal, decoding, and list-cardinality work | C1R; C4M where paths are observed | Evidence-ranked |
| C6 | Rebuild ADCS references and optimize only a measured gap | C4M/C5 as applicable | Conditional |
| C7/CX | Cache stable compilation stages or evaluate a native extension | Stable C3G-C6 SQL and measured gap | Conditional |
| C8 | Concurrency, memory, cancellation, and soak qualification | All accepted production increments | Blocks completion |
| C9 | Cost-weighted complete-corpus reprioritization | C8 | Defines the next continuation or stop |

Phases C0R and C1R may share benchmark instrumentation work. C4M prototypes
may run beside C2Q, but no materializer should be coupled to executor selection
until search-only results are independently stable. C5B decode work may proceed in
parallel when it touches neither shortest SQL nor shared benchmark state.

Primary implementation seams are:

- GraphBench selection/lifecycle: `cmd/graphbench/main.go`, `corpus.go`,
  `environment.go`, `results.go`, and `types.go`;
- comparison/noise reports: `cmd/graphbench/perf_gate.go`, `aa_report.go`, and a
  new paired confirmation report beside them;
- reference identity/exactness: `cmd/graphbench/references.go` and its tests;
- optimizer decision: `cypher/models/pgsql/optimize/lowering_plan.go` and
  `lowering.go`;
- translation and observation lineage: the PostgreSQL translator traversal,
  function, path-function, projection, tracking, and summary models;
- helper boundary, if selected: `cypher/models/pgsql/functions.go` and
  `drivers/pg/query/sql/schema_up.sql`/`schema_down.sql`;
- public semantics: translation goldens plus backend-equivalent integration
  cases/templates; PostgreSQL-only plan/resource behavior stays driver-scoped.

## Phase C0R: Reconcile regressions and freeze a reconstructible baseline

### Add safe targeted diagnostic selection

Add an exact case-selection facility to GraphBench before spending more full
corpus time. Requirements:

- accept stable case names and optionally dataset/category/tag selectors;
- reject unknown selectors and duplicate ambiguous names;
- record requested and resolved selectors in every environment manifest;
- retain the destructive lock and normal fixture reload/analyze behavior;
- retain exact preflight and postflight observations outside timed intervals;
- support PostgreSQL-only or Neo4j-only diagnostics without changing the
  versioned full-corpus declaration;
- mark filtered artifacts `diagnostic_only` and record the omitted declaration
  count;
- refuse to use a filtered artifact in the ordinary complete performance gate;
- provide an explicitly filtered diagnostic comparison mode whose declaration
  checksum includes the resolved subset;
- keep serial pool size one unless the diagnostic explicitly targets
  concurrency.

The initial exact filter set is:

```text
LOOKUP-05_repeated_case_insensitive_prefix
GSP-D02-F016_distance
```

Include these controls in the same diagnostic block:

```text
LOOKUP-02_repeated_exact_objectid_lookup
LOOKUP-04_suffix_kind_and_domain_filter
LOOKUP-15_all_node_count
GSP-D01-F001_distance
GSP-D02-F016_path
GSP-D04-F128_distance
GSP-D08-F001_distance_inbound
GSP-D16-F016_distance
```

The lookup controls exercise exact property lookup, a related suffix/filter
shape, and a same-fixture scan/protocol floor. The shortest controls exercise
the same fixture/path boundary, a shallow fixed-cost case, and depth/fanout
slope. Capture S3-U/raw references in an adjacent attribution block, not in the
primary alert-timing block.

Extend the harness/report format at the same time:

- add an explicit untimed `warmup_iterations` setting and record every warmup
  count while excluding it from reported samples;
- record arm label, arm order, run UUID, block/round number, and start/end
  timestamps;
- make the confirmation report accept two named artifacts and emit paired
  absolute and relative p50/p95 differences, not only median savings;
- preserve ordinary full-manifest behavior when no selector is supplied;
- fail on unknown or duplicate exact names rather than silently selecting an
  empty or different corpus.

### Make future baselines reconstructible

For each accepted baseline or candidate bundle, retain:

- source commit;
- tracked-source patch and a manifest/checksum of untracked source;
- reproducible build command and Go module checksum state;
- built executable or a content-addressed durable binary;
- executable SHA-256;
- sanitized invocation;
- corpus declaration and checksum;
- raw JSONL, summaries, plans, reference SQL, A/A report, and gate report;
- PostgreSQL and Neo4j versions/settings;
- fixture configuration, cardinality, and checksum;
- host/kernel/CPU topology and any available frequency/governor/cgroup limits;
- database identity, graph count, pool configuration, backend PID, and cache
  classification;
- start/end timestamps and a run-series identifier.

Do not publish credentials, connection URLs, arbitrary environment variables,
or host credential paths. Preserve connection identities only as sanitized
backend/session IDs.

### Isolated rerun protocol

Calibrate two distinct kinds of noise before comparing source states:

1. Keep the existing alternating-sample A/A split to measure within-session
   jitter.
2. Add same-binary block A/A: independently reload equivalent databases for
   the two arms and reverse arm order each round. This measures fixture reload,
   process, database, and capture-to-capture drift that the existing report
   cannot see.

Use the larger within-session or block/reload resolution for each case and
metric. Do not assume the old and new A/A reports are interchangeable; their
observed p95 resolution changed materially between captures.

Run the causal predecessor/candidate confirmation as follows:

1. Build one `-trimpath` GraphBench executable per arm before measurement,
   retain it, and verify its SHA-256. Do not use transient `go run` binaries for
   a causal comparison.
2. Give each arm a fresh disposable database or verified clean clone. Apply the
   same migrations, independently load the fixture, and run a verified
   `VACUUM (ANALYZE)` before timing.
3. Pin pool size and concurrency to one, use one physical PostgreSQL connection
   per case, and run no concurrent Neo4j capture or unrelated GraphBench job.
4. Run 20 fixed untimed warmups followed by 50 timed warm observations per
   case. Keep cold executions as separate diagnostics.
5. Start with 10 matched rounds, running A then B in odd rounds and B then A in
   even rounds. Alternate case/control order as well.
6. Extend only in predeclared five-round batches, to at most 20 rounds, when CI
   precision is insufficient. Never add samples because a point estimate is
   inconvenient.
7. If the C0 source and executable can be reconstructed, use them as the
   predecessor arm. If not, classify C0 as historical-only, freeze a new
   reconstructible predecessor, and do not claim causality from the old
   artifact.
8. Capture server plan/execution, buffers, client transaction/setup,
   execute/decode/drain, and end-to-end intervals for every selected case.
9. Verify source/binary, SQL, fixture, result, schema/migration, settings,
   relation/index-size, and intended plan-shape fingerprints before comparing
   timing.

Record the postmaster start identity, database OID, backend PID, graph partition
count, autovacuum/analyze state, `plan_cache_mode`, `work_mem`,
`temp_file_limit`, host load, CPU frequency/governor, and cgroup limits where
available. Abort the block on a fingerprint mismatch, failed maintenance,
competing destructive-lock holder, connection replacement, or predeclared host
saturation. References and Neo4j exact-result oracles run beside the primary
block, never interleaved into its timing.

### `LOOKUP-05` diagnosis

Measure these boundaries separately on the same connection:

- prepared `select 1`;
- transaction begin/rollback;
- parameter bind/encode;
- server planning and execution;
- first-row time;
- row decode/drain;
- total client wall time;
- scheduler/pool wait, even with pool size one;
- cold first prepared execution, executions 2-5, and steady state.

Add an identical-SQL raw-pgx control. If that control is stable while CySQL
end-to-end moves, investigate the CySQL/pool/decode boundary. If it moves with
the same plan, investigate PostgreSQL/host/index/collation state before any
translator change.

Capture `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON)` where supported
and preserve the text plan already used by the repository. Compare index usage,
row estimates, heap/index fetches, buffer hits/reads, and plan time. A stable
0.1-0.2 ms server plan with a much larger client p95 is a client/host finding,
not a reason to rewrite the SQL predicate.

### Depth-2 shortest diagnosis

For `GSP-D02-F016_distance`, capture:

- incumbent harness total server time;
- workspace ensure/reset time;
- local relation reads, writes, dirtying, and relation sizes;
- dynamic fragment rewrite/plan time;
- forward/backward primer and recursive layer time;
- frontier/visited row counts by layer;
- examined edge count;
- rejected, deduplicated, and copied rows;
- cold versus warm session state;
- the same S3-U and S3-B comparator samples in the same round.

Compare depth 1, 2, 4, 8, and 16 controls. A depth-2-only movement suggests
noise or a threshold effect; a common incumbent increase with stable S3-U points
to workspace/server state; a common increase across incumbent and references
points to the database host.

### Classification gates

Use p95 as the primary alert-confirmation metric. Treat p50 as a secondary
diagnostic and non-inferiority safeguard, and p99 as diagnostic. For each case
and metric define:

```text
noise_ratio = max(0.05, within_run_AA_ratio, block_reload_AA_ratio)
noise_abs   = max(within_run_AA_abs, block_reload_AA_abs, 0.10 ms)
```

Because two hypotheses were selected from the complete-corpus screen, use only
fresh confirmation samples and either Holm-adjust the two primary p95 tests or
use a conservative 97.5% interval per case. Never reuse the screening samples
as confirmation evidence.

Classify an alert as **confirmed** only when the fresh matched interval has:

- ratio lower bound greater than `1 + noise_ratio`;
- absolute slowdown lower bound greater than `noise_abs`;
- identical correctness/status and comparable source, SQL, fixture, schema,
  settings, relation sizes, and intended plan/resource fingerprint.

Classify it as **cleared/non-inferior** when the ratio upper bound is no more
than `1 + noise_ratio` and the absolute slowdown upper bound is no more than
`noise_abs`.

Classify it as **inconclusive** when neither rule holds. Extend by independent
rounds under the cap; at 20 rounds publish the inconclusive result and do not
change production behavior or silently waive the alert.

After statistical classification, assign a causal disposition:

- same-binary block A/A failure: runner/host unqualified;
- changed SQL, plan, or resource fingerprint: translator/planner investigation;
- stable plan/buffers with PostgreSQL execution and controls moving together:
  server/environment drift;
- stable server time with end-to-end movement: pool, transaction, transfer, or
  decode path;
- incumbent-only shortest movement with stable S3-U/raw reference: workspace or
  session-state sensitivity;
- old state not reconstructible and no fresh reproduction: historical-only.

Preserve the failed historical gate in every case; do not delete or relabel it
as a pass. Any correctness, status, checksum, or cardinality mismatch is an
immediate failure regardless of timing.

If `LOOKUP-05` is confirmed in server execution, open a scoped lookup plan
experiment. If it is client/host-only, fix the measured layer or document the
operational environment; do not alter Cypher lowering.

If the depth-2 shortest failure is confirmed only on the incumbent, record it
as additional urgency for C3S, not as permission to weaken the incumbent gate
before a replacement executor ships.

### C0R exit criteria

- Targeted diagnostic artifacts cannot pass as complete-corpus artifacts.
- Both live failures have cleared, confirmed-code, confirmed-environment, or
  capped-inconclusive dispositions backed by fresh matched data.
- A reconstructible current baseline bundle is durably published.
- The complete 151-key gate passes against identical C0R A/A arms.
- A/A p50/p95 resolution is published for every PostgreSQL case.
- p99 remains explicitly diagnostic.
- Raw paired samples, within-run and block A/A reports, environment diffs,
  plans, exact sanitized commands, and saved binary checksums are published.
- A full-corpus rerun follows any fix or newly frozen baseline; a targeted
  artifact never replaces it.
- No production optimization is introduced in this phase.

## Phase C1R: Complete cost attribution

The current reference ladder establishes large gaps but does not yet attribute
90% of shortest server time or 90% of large-result end-to-end time. Do not mark
C1 complete until the missing intervals are measured.

### Shortest incumbent probes

Add benchmark-only probes for:

1. endpoint validation;
2. workspace schema/version check;
3. workspace allocation on a cold session;
4. workspace reset alone;
5. multi-table `TRUNCATE` versus indexed `DELETE` versus generation tagging;
6. runtime fragment rewrite;
7. dynamic fragment prepare/plan;
8. forward primer;
9. backward primer;
10. each recursive layer;
11. rejected-row pruning;
12. frontier copy/deduplication and slot reset;
13. visited maintenance and indexes;
14. midpoint/direct-hit detection;
15. ordered-ID reconstruction;
16. full path hydration;
17. transfer/decode/drain;
18. unexplained residual.

Use mutually exclusive instrumentation where possible. Where instrumentation
would perturb the hot loop, use controlled one-variable deltas. Never sum
overlapping `EXPLAIN`, wall-clock, and client waterfall intervals into a false
attribution percentage.

Report for every probe:

- p50/p95 and raw observations;
- server/client boundary;
- shared/local/temp buffers and bytes;
- rows and edges examined/returned;
- allocation count/bytes where Go is involved;
- cold/warm session classification;
- whether the interval is exclusive, inclusive, or a controlled delta.

### Repair candidate identity and comparator exactness

Version the reference-result schema and rename the current candidates:

```text
complete_reference_s1_array_cte  -> s3_unidirectional_trail_cte
candidate_s2_bidirectional_cte   -> s3_bidirectional_trail_cte
```

Readers may map the legacy names for historical artifacts, but new records must
also declare architecture, implementation ID, state shape, observation shape,
and semantic-validation level. A report must not group unlike implementations
because their legacy labels share an S-number.

Replace the current `fullComparator` row-count check with exact semantic
validation outside the timed interval:

- distance must equal the independently declared/oracle minimum;
- ordered node/edge IDs must resolve inside the active graph;
- consecutive entities must be adjacent in the requested direction and use an
  allowed kind;
- endpoints, minimum/maximum depth, relationship uniqueness, and zero-edge
  behavior must hold;
- a returned `shortestPath` tie may be any member of the independently
  validated minimum-length set, but may not be a longer substitute after a
  post-filter;
- null, empty, error, and multiplicity observations must match the public
  Cypher result, not merely its row count.

Then normalize every S0-S3 candidate at the same boundary:

- give candidates the same endpoint validation and helper-call boundary;
- apply the same edge-kind, direction, graph, depth, and uniqueness semantics;
- return the same scalar or raw ordered-ID representation;
- use the same pgx transaction, parameter encoding, binary formats, and drain
  path;
- precompute hydration inputs outside timed hydration blocks;
- pair search and hydration samples by round and physical connection;
- report cold prepare/plan separately from steady state;
- record examined-edge and retained-state slopes.

The measured S3-B loss closes only that implementation. It does not close the
compact trace-relation S2 from the prior plan.

### Client and large-result attribution

For these exact cases, separate the following costs:

```text
HOP-05_thousand_endpoint_IDs_with_sparse_matches
HOP-09_dense_two_sided_ID_sets
LOOKUP-09_thousand_ID_full_node_hydration
LOOKUP-11_tenant_adjacency_thousand_property_list
```

- pool acquisition;
- transaction setup;
- bind/prepare;
- server time;
- first-row transfer;
- all-row transfer;
- composite decode;
- graph value construction;
- result ownership/copying;
- drain and close;
- allocations and bytes;
- unexplained residual.

Do not rewrite list-heavy SQL while server time is below measurement resolution
and decode/transfer dominates.

### Cost-model report

Produce a versioned machine-readable and Markdown report with:

| Component | Inclusive/exclusive | Median | p95 | Buffers/bytes | Rows/edges | Share of E2E | Confidence |
|---|---|---:|---:|---:|---:|---:|---|
| Protocol/transaction | Exclusive | | | | | | |
| Endpoint validation | Controlled delta | | | | | | |
| Search | Exclusive | | | | | | |
| Hydration | Exclusive | | | | | | |
| Transfer/decode/drain | Exclusive | | | | | | |
| Client compilation | Overlapping unless isolated | | | | | | |
| Unexplained residual | Derived | | | | | | |

Rank opportunities by addressable absolute time multiplied by documented
workload weight. Keep Neo4j out of the ranking formula.

### C1R exit criteria

- At least 90% of incumbent shortest server time is attributed.
- At least 90% of selected large-result end-to-end time is attributed.
- Candidate names map unambiguously to the prior plan's S0-S3 architectures.
- Every full comparator validates exact semantics, not only row count.
- S0-S3 comparisons use identical boundaries and semantics.
- Search and hydration are paired, separate measurements.
- Residual and overlapping intervals are explicit.
- The report supplies normalized C2Q inputs and closes an unbuilt candidate
  only with a concrete, predeclared feasibility reason.

## Phase C2Q: Qualify the singleton executor tournament

### Candidate definitions

Evaluate the architectures promised by the prior plan rather than treating
artifact labels as implementations:

- **S0:** the incumbent workspace control, with only separately measured
  workspace/reset changes;
- **S1:** typed PL/pgSQL array-resident singleton BFS with an explicit state
  limit and a correct overflow fallback;
- **S2:** one compact, generation-tagged bidirectional trace relation, if a
  benchmark-only prototype can satisfy bounded cleanup and uniqueness;
- **S3-U:** the measured inline unidirectional recursive CTE, renamed and made
  exact;
- **S3-B:** the measured inline bidirectional trail CTE, retained as a rejected
  normal-tier artifact unless new envelope evidence overturns it.

Every viable candidate must have two genuinely distinct result shapes:

- **distance**: depth only, with no predecessor, ordered-node, or ordered-edge
  state;
- **one path**: the minimum bounded state needed to return ordered node and edge
  IDs for exactly one shortest path.

S1 should expose two additive typed `RETURNS TABLE ... ROWS 1` helpers without
new composite types: one for distance and one for ordered path IDs. Return a
found/overflow indication and diagnostic counters; path mode additionally
returns ordered IDs. Add exact schema-down definitions, idempotent schema-up
tests, and up/down/up coverage. Do not mark the functions parallel-safe without
evidence.

An S1 state limit is not a semantic failure mode. Overflow must transparently
restart a correct fallback in the same statement/session; it must never become
an empty result or transaction-aborting error. If no qualified restart is
possible, restrict S1 further or select S3-U. S3-U requires no schema migration
but must prove its trail-array memory/spill and dense-disconnected behavior.

### Semantic adapter

Run the same candidate through a table-driven adapter covering:

| Dimension | Required cases |
|---|---|
| Shape | direct, linear, diamond, cycle, repeated node, dead end, disconnected |
| Edge identity | parallel edges, self-loop, repeated relationship rejection |
| Direction | outbound, inbound; directionless remains fallback unless separately proven |
| Kinds | untyped, one kind, several kinds, no matching kind |
| Depth | `*0..0`, `*0..1`, `*1..1`, bounded 2/4/8/16/32/64, open upper bound policy |
| Endpoints | missing, null, contradictory, same ID, graph-colliding IDs |
| Predicates | endpoint label/kind/property/ID, path-independent edge predicate, unsupported path predicate |
| Result | distance, one full path, alias/`WITH`, composed projection, downstream path function |
| Statement | two shortest calls, sequential transactions, rollback, cancellation |
| Source | literal/parameter singleton, correlated row, multi-row source, multi-pair source |
| Concurrency | one connection, pool-sized connections, session reuse after error/cancel |

For equal-length diamonds, one valid shortest path is sufficient for
`shortestPath`; the candidate may not substitute a longer path when a selected
shortest path fails a post-filter. `allShortestPaths` remains a separate
predecessor-DAG problem.

Test exact node order, relationship order, direction, duplicate multiplicity,
properties, null behavior, and errors. Row count alone is insufficient.

### Resource and slope envelope

Measure normal and largest tiers:

- depths 1, 2, 4, 8, 16, 32, and 64;
- fanout 1, 16, 128, 512, and 1000;
- connected and dense-disconnected shapes;
- empty, normal, and 4 KiB payloads for path output;
- cold and warm sessions;
- concurrency 1, configured pool size, and twice pool size.

For every tier record:

- examined edges;
- frontier rows;
- retained path/predecessor bytes;
- server memory/workspace;
- shared/local/temp buffers;
- temp spill files/bytes;
- p50/p95 and throughput;
- cancellation cleanup.

Reject or restrict any candidate whose state has an unacceptable depth/fanout
slope. S3 trail arrays and S1 in-memory state require separate byte ceilings.
A bounded eligibility regime is acceptable only when its bound is explicit,
tested at and beyond the boundary, and paired with a correct fallback.

### Candidate comparison

Compare at least:

- S0 incumbent workspace harness;
- S1 array-resident singleton search;
- S2 compact bidirectional trace relation;
- S3-U inline unidirectional trail CTE;
- S3-B inline bidirectional trail CTE as the preserved rejected control;
- the best correct full PostgreSQL reference.

Do not count the preserved S3-B artifact as S2 evidence. A candidate may close
without a full implementation only when a documented feasibility result shows
that its required correctness/state model cannot meet a predeclared bound; raw
implementation effort is not a performance stop rule.

The selected executor must not be Pareto-dominated on latency, tail, memory,
temp space, examined edges, cold cost, or concurrency. If different candidates
win stable tiers, choose a measured, observable hybrid eligibility boundary
rather than a universal claim. A runtime selector may use only bounded inputs
available without performing the search.

### C2Q exit criteria

- Every semantic adapter case passes.
- Every unsupported form records a tested fallback reason.
- Distance state contains no path/predecessor representation.
- Path state has an explicit memory/depth bound.
- The selected executor or measured hybrid wins the complete eligible
  envelope; every rejected implementation and reason remains in the report.
- At least S0 and two fundamentally different executor architectures have
  exact complete artifacts.
- Five independently reloaded rounds with 30-50 warm samples show a material
  improvement over C0R beyond A/A resolution, or C0R itself satisfies the
  workstream completion rule after alternatives fail.
- Candidate/reference upper confidence bound is at most `1.10` or the absolute
  gap is below A/A resolution for each declared target.
- Normal tiers have no temp-file spill; the tiny singleton fast path has no
  local/temp I/O unless a temp-backed candidate Pareto-dominates every
  temp-free alternative. Dense-disconnected cases finish within their timeout,
  and adjacent-tier time-per-edge and bytes-per-state upper bounds grow by no
  more than `1.25` without an explained regime change.
- Rejected prototypes are documented and absent from production code.
- No production dispatcher branch is added before this gate passes.

## Phase C3S: Ship the singleton executor

### Explicit optimizer/lowering decision

Add a typed decision such as `ShortestPathExecutorDecision` to the lowering
plan, containing:

- query/traversal target;
- selected executor and observation mode;
- eligibility facts;
- maximum supported depth/fanout or state bound, if any;
- fallback executor;
- fallback reason when not selected.

Expose planned/applied/skipped decisions in translation diagnostics and
GraphBench records. Static eligibility must not depend on runtime endpoint
values; an S1 runtime `state_limit` overflow is a separately recorded fallback
event.

### Initial eligibility

The production fast path requires all of the following:

- `shortestPath`, not `allShortestPaths`;
- one three-element variable-length traversal step;
- exactly one static literal/parameter integer-ID equality on each endpoint;
- no correlated or multi-row endpoint source;
- no optional match or mutation/update dependency;
- a supported outbound or inbound direction;
- supported relationship-kind predicates;
- minimum depth zero or one and a qualified bounded maximum;
- no relationship variable, relationship-property predicate, or path-dependent
  predicate;
- no interaction with another path call that changes semantics;
- a proven distance-only or full-path observation classification;
- graph-scoped access using the active graph ID.

Directionless, mixed-direction, correlated, multi-pair, `allShortestPaths`, and
unsupported post-filter forms must record a conservative generic fallback.

Use stable fallback codes, including at least:

```text
all_shortest_paths
correlated_endpoints
multiple_endpoint_pairs
non_singleton_id
multiple_id_equalities
path_predicate
relationship_predicate
relationship_variable
directionless
optional_match
unsupported_depth
mutation
multiple_path_calls
state_limit
```

Validate endpoint ID, kind/label, property, null, and contradiction predicates
before invoking search. Missing endpoints invoke no executor. Preserve
same-endpoint error for minimum depth one and zero-edge success for minimum
depth zero before allocating recursive state. Endpoint-local labels,
properties, and additional predicates remain eligible only through the existing
singleton endpoint-validation CTE; plans/tests must show the executor is never
called when validation returns no row.

### Stable SQL boundary

Preserve the architecture that actually won C2Q:

- if S3-U wins, emit a stable recursive CTE in the PostgreSQL AST and document
  explicitly that it introduces no schema migration;
- if S1 wins, call its two typed, graph-scoped helpers;
- if a bounded hybrid wins, make its threshold and overflow restart observable
  and test both sides of the boundary;
- if S0 or S2 wins, land only the qualified stable boundary from its tournament
  implementation.

Compare viable inline/helper boundaries only when they implement the same state
model and semantics. Include planning, prepared-statement reuse, schema
evolution, cancellation, partition pruning, and debugging. Never pass runtime
SQL text or rewritten fragments into the selected executor.

If a helper wins:

- add schema-up and schema-down coverage;
- use fully typed parameters and return columns;
- declare realistic row estimates only where PostgreSQL uses them correctly;
- avoid session-global mutable state;
- test upgrade, downgrade, and repeated `AssertSchema` behavior.

Different endpoint values must produce the same SQL fingerprint. Relationship
kind and depth shapes may produce distinct stable templates only when their
types and planner behavior require it.

Implement through the existing seams: lowering decision/model files under
`cypher/models/pgsql/optimize`, a focused singleton translator beside the
generic shortest traversal lowering, optimization-summary reporting, typed
PostgreSQL function identifiers/schema files when S1 wins, and the existing
translation/integration fixture workflows. Keep the generic harness intact as
the fallback until C3G independently replaces any of its other cases.

### Distance mode

Distance mode returns depth directly. It must:

- carry no ordered edge IDs;
- carry no node IDs beyond the current frontier/visited requirement;
- allocate no predecessor chain;
- invoke no path materializer;
- avoid constructing a synthetic array merely so `cardinality()` returns the
  desired depth;
- survive aliases and `WITH` propagation when every downstream use remains
  distance-only.

Add negative tests proving that any downstream path/node/relationship/property
observation prevents distance specialization.

Track this observation through aliases and `WITH`: a path used only beneath
`length()` remains distance mode, while direct path output, `nodes()`,
`relationships()`, an unknown function, collection use, or a path predicate
requires path mode or fallback. Node-visited pruning is permitted only for the
proven singleton envelope where it preserves relationship-unique shortest-path
semantics; broader minimum-depth or predicate forms fall back.

### One-path mode

One-path mode initially returns the minimal ordered IDs required by the
qualified search/materializer boundary. C4M may add ordered node IDs only if M1
wins its later paired tournament. One-path mode must:

- preserve relationship uniqueness;
- preserve exact order and direction;
- return one valid equal-length tie;
- avoid re-running search during materialization;
- keep search state distinct from hydrated composites;
- preserve null/error behavior and transaction cleanup.

### Test requirements

Add or update:

- optimizer decision tests;
- translation golden/template cases;
- PostgreSQL schema up/down tests if a helper is introduced;
- PostgreSQL integration semantics;
- shared backend-equivalent Cypher cases for supported public semantics;
- exact raw distance/node-ID/edge-ID comparator tests, including adjacency,
  graph scope, kind, direction, uniqueness, and valid equal-depth ties;
- PostgreSQL-scoped plan/resource assertions;
- mutation/template coverage required by `AGENTS.md` for affected translation
  behavior;
- cancellation, rollback, sequential reuse, and concurrent-connection tests;
- race tests for any shared analysis/cache state.

Do not add driver-specific expected results or skips to the shared integration
corpus.

### Performance experiment

Predeclare as primary targets:

```text
shortest_distance_bound_pair
one_shortest_path_bound_pair
GSP-D01-F001_distance
GSP-D01-F001_path
GSP-D02-F016_distance
GSP-D02-F016_path
GSP-D04-F128_distance
GSP-D04-F128_path
GSP-D04-F128_disconnected
GSP-D08-F001_distance_inbound
GSP-D16-F016_distance
GSP-D16-F016_path
```

Use `all-shortest`, directionless, generic variable traversal, lookup, count,
mutation, and ADCS cases as controls.

Capture at least five independently reloaded matched rounds with 30-50 warm
observations. Require:

- exact PostgreSQL and Neo4j oracle results;
- target median materiality beyond A/A resolution;
- candidate/reference upper confidence bound at most `1.10` or an absolute gap
  below measurement resolution;
- no confirmed affected-family regression above the 5% non-inferiority budget;
- no complete-corpus emergency regression;
- normal-tier no-spill behavior;
- improved or bounded local-buffer/workspace activity;
- concrete graph-partition pruning under representative `auto`, custom, and
  generic planning modes;
- cold-session and pool-sized concurrency results within declared budgets.

Compare the immediate predecessor, C0R, and best exact PostgreSQL reference
separately. Keep `LOOKUP-05` as a predeclared control and resolve the historical
depth-2 alert under C0R before attributing any new movement. A small pool-cold,
concurrency, cancellation, and session-reuse smoke blocks each production
increment; the full soak remains C8.

### Rollout and rollback

The selected lowering is the production behavior for eligible queries after
acceptance. Do not retain a dormant permanent feature flag. Preserve the
generic executor as the semantic fallback.

Rollback consists of reverting the new lowering/helper in a forward change and
returning eligible queries to the generic harness; schema-down must remove any
new helper safely. Never rewrite repository history or use `git revert` as an
agent workflow.

### C3S exit criteria

- The typed/stable singleton lowering ships with explicit decisions.
- Distance and one-path modes use distinct state.
- Every ineligible form has a tested generic fallback.
- Target performance and complete-corpus gates pass.
- Schema, template, mutation, integration, race, cancellation, and concurrency
  tests pass.
- Accepted artifacts are durable and reconstructible.

## Phase C4M: Minimize path materialization

### Re-establish paired path tax

For each search shape, measure in the same round and physical connection:

```text
path_tax = server_execution(full_path_composite)
         - server_execution(raw_ordered_IDs)
```

Both arms must share the same search representation and row cardinality.
Summarize paired deltas directly; do not subtract independent medians.

Cover path lengths 0, 1, 2, 4, 8, 16, 32, and 64; output cardinalities 1, 4,
32, 128, and 1000; and empty, normal, and 4 KiB properties.

### M0: directed reconstruction

For a proven directed path, hydrate ordered edges once and derive ordered nodes
from the root and edge endpoints. Avoid recursive `path_walk` and connectivity
rediscovery. Retain the generic recursive materializer for directionless,
mixed, legacy, and mutation-returning paths.

### M1: carry ordered node IDs

Compare carrying ordered node IDs beside ordered edge IDs with deriving nodes at
the boundary. Hydrate node and edge streams with ordinal joins and reconstruct
the exact composite order. Do not add node-ID arrays to distance-only or
endpoint-only queries.

### M2: batch across rows

Only after M0/M1, compare batching across output rows for high-cardinality
results:

- attach a stable output-row ordinal;
- unnest ordered IDs once;
- hydrate distinct entities set-wise;
- reconstruct every row with exact duplicates and order;
- preserve rows sharing suffixes or complete paths;
- measure low-cardinality overhead against high-cardinality benefit.

Do not ship M2 if its fixed cost regresses the common one-path case beyond the
non-inferiority budget.

### C4M exit criteria

- Search is unchanged between materializer arms.
- The selected implementation beats its predecessor beyond both A/A resolution
  and absolute materiality, and is within `1.10` of the best identical-boundary
  PostgreSQL reference or below absolute resolution.
- The upper confidence bound for paired path tax is at most 0.25 ms on the
  small generic fixture and 0.35 ms on ADCS P1; C1R may replace these with a
  stricter evidence-backed budget.
- The selected implementation is linear in path/output size within the tested
  envelope, and execution plus bytes grow by at most `2.2` from length 32 to
  64.
- Exact order, direction, duplicates, properties, and graph scope pass.
- Distance queries perform zero hydration.
- Normal-tier materialization has no temp I/O, and the four-row ADCS P1 path
  adds at most 30 shared hits at its upper confidence bound.
- No selected candidate is Pareto-dominated on server execution, transfer,
  decode, or allocations.
- The chosen materializer closes a material part of the paired path tax without
  a low-cardinality regression.

## Phase C3G: Generic and all-shortest completion

Singleton success does not establish shortest-family optimality. Freeze a new
generic baseline after C3S and treat these as independent workstreams:

1. bound but correlated endpoint pairs;
2. multi-row and multi-pair endpoint sources;
3. directionless and mixed-direction paths;
4. path-dependent predicates and post-filters;
5. multiple shortest calls in one statement;
6. `allShortestPaths` and equal-depth predecessor multiplicity;
7. zero-depth/open-upper-bound forms outside C3S eligibility.

The current corpus has only one generated all-shortest case; its roughly
13.26 ms end-to-end and 10.18 ms diagnostic server execution justify a focused
workstream but cannot select an architecture. Extend the matrix with multiple
roots, terminal-filtered searches, materialized/correlated/duplicate endpoint
pairs, batches sharing a root or terminal, repeated pairs, multiple calls in
one statement, and node-, relationship-, and parallel-edge-distinct shortest
ties. Cross direction, kinds, depth, fanout, disconnected results, cold/warm
sessions, and concurrency.

### Generic alternatives

Measure:

- batching endpoint pairs into one stable relation;
- sharing search only where semantics and pair identity permit it;
- compact state versus the incumbent multi-table workspace;
- stable generated SQL versus runtime fragment rewriting;
- unidirectional versus bidirectional search by pair density;
- generation-tagged workspace cleanup where a workspace remains necessary.

Tournament pair deduplication with exact multiplicity restoration, shared
expansion for common roots/terminals, and pair-keyed trace state. Any runtime
strategy selector must use bounded observable inputs, remain stable over its
declared envelope, and record its choice/fallback in the artifact.

Do not scalarize a multi-row source, merge duplicate endpoint pairs, or lose row
multiplicity.

### All-shortest alternatives

Keep the current generic fallback until a predecessor-DAG candidate proves:

- every equal-depth predecessor edge is retained;
- parallel-edge-distinct paths remain distinct;
- cycles and relationship uniqueness are correct;
- deterministic output comparison can canonicalize without changing public
  multiplicity;
- memory is bounded or spills within declared limits;
- enumeration is cancellation-safe.

### C3G exit criteria

- Every generic family has its own reference, baseline, targets, and controls.
- Singleton results are not reused as generic performance evidence.
- Correlation and multiplicity negative tests pass.
- `allShortestPaths` tie sets are exact.
- Material generic classes are within `1.10` of their best correct references
  or below resolution, without regressing the qualified singleton path.
- Pair/call/session state cannot leak across success, error, cancellation,
  rollback, or physical-connection reuse.
- Each family meets the workstream completion rule or retains a documented
  incumbent with failed alternatives removed.

## Phase C5: Variable traversal, decoding, and list cardinality

### C5A: slim staged traversal state

Use field requirements and last-use analysis to avoid carrying values that are
not observed after a stage:

- ID-only state for endpoint projections;
- depth-only state for counts/distances where semantics permit;
- relationship composites only when observed;
- full path composites only at the final observation boundary;
- no property hydration before its last necessary stage.

Preserve duplicate and row multiplicity across `WITH`, aggregation, `UNWIND`,
optional matches, aliases, and multiple expansions. Add negative tests before
shipping any scalarization.

The live `variable_length_id_only_from_bound_id` improvement and
`variable_length_path_observed_from_bound_id` increase are diagnostic. Confirm
them under C0R selection before using them as C5A evidence.

The base ID-only plan already measures approximately 0.059 ms with 10 shared
hits, inside the prior 0.15 ms/20-hit budget. Close the small case as a measured
no-op unless scale or payload probes expose an addressable gap. C4 path
materialization therefore precedes a broad traversal-state rewrite. For any
larger tier that does justify C5A, require no post-last-use heap/TOAST fetch,
exact duplicate multiplicity, no normal-tier spill, and no unexplained greater
than 25% normalized-work increase between adjacent tiers.

### C5B: decode and ownership

For large-result cases, profile and compare:

- field metadata reuse;
- composite codec allocations;
- copying versus safe ownership transfer;
- graph value construction;
- streaming/drain behavior;
- reusable decode buffers with explicit lifetime rules;
- client backpressure and cancellation.

Any ownership optimization must have race, use-after-release, retained-memory,
and cancellation tests.

Build an identical-SQL raw-pgx reference before changing SQL. A/B, in order:
immutable field-metadata reuse, removal of ownership-safe unconditional
slice/map copies, specialized composite codecs, then safe streaming/discard
modes. Attribute transfer, field-key construction, property copying, graph
value allocation, retention, streaming, and drain separately.

### List-cardinality strategies

Only if server access remains addressable after decode work, compare:

- `ANY` arrays;
- typed `unnest` relations with ordinality;
- temporary input relations at large cardinality;
- adjacency-first versus parameter-first joins;
- generic versus custom plan policy under representative cardinalities.

Cover 0, 1, 8, 32, 1000, and 10,000 values; null list parameters; null members;
duplicate IDs; sparse, half, and dense matches; one- and two-sided anchors; and
one versus 30 relationship kinds. Preserve Cypher three-valued filtering and
do not let duplicate input IDs multiply rows unless the surrounding construct
requires it. Do not choose global PostgreSQL settings for one case.

### C5 exit criteria

- Selected row shapes contain only semantically required fields.
- Large-result end-to-end attribution exceeds 90%.
- Allocation/byte reductions are material and lifetime-safe.
- End-to-end latency is within `1.15` of the identical raw-pgx reference and
  allocations/decoded bytes are within `1.10`, or the gaps are below
  resolution.
- List strategy is selected by cardinality envelope, not one point.
- Complete-corpus and concurrency gates pass.

## Phase C6: Rebuild ADCS evidence before optimization

The current ADCS reference is not a floor. In a representative live round,
translated CySQL was approximately 0.913 ms versus 4.638 ms for the handwritten
endpoint comparator and 2.510 ms versus 6.263 ms for the observed-path
comparator. Broad base-fixture ADCS rewriting is therefore deprioritized.

First absorb applicable C4 materialization, C5A scalar-state, and C5B decode
improvements, then:

- verify identical P1 semantics, uniqueness, path order, payload, and decoding;
- profile why its recursive search is slower than translated CySQL;
- add a direct component reference for the already-efficient suffix strategy;
- separate scalar endpoint binding, variable `MemberOf` expansion, fixed suffix,
  hydration, transfer, and decode;
- construct a best correct full reference before calculating addressable gap.

Extend `generated_adcs` before P2 or combined-query work:

- independent P1 and P2 valid-density controls;
- certificate-template publication and CA/root/domain chains;
- branch-specific kind, direction, endpoint-kind, and disconnected decoys;
- exact Cartesian result declarations;
- endpoint, P1 path, P2 path, and combined projections;
- output cardinalities through 1000 and 4 KiB payloads.

Proceed with suffix-density or expansion-sharing work only when the rebuilt cost
model shows a gap larger than A/A resolution and materiality. A slower reference
is a diagnostic failure, not evidence that production is optimal.

Keep the generated D16/F1000 sparse tier open: it currently costs roughly
57-64 ms and about 158,000 shared hits. Before optimizing it, establish its
workload frequency and a correct competitive reference; it must not make the
small, already-efficient ADCS shape drive a broad rewrite.

### C6 exit criteria

- A competitive correct reference exists or ADCS is explicitly deferred.
- P1/P2/combined semantics and cardinalities are exact.
- Any density-aware decision is stable and recorded.
- No optimization is justified by a Neo4j ratio.
- Accepted changes pass low/high density, payload, output, and concurrency
  envelopes.

## Phase C7: Conditional compilation and plan-cache work

Do not begin until C3S, C3G, C4M, C5, and any accepted C6 SQL shapes are stable.

Use the C1R waterfall to decide whether work is warranted. Prefer this order:

1. parsed Cypher cache;
2. optimized/lowering-plan cache;
3. translated AST or stable-template cache;
4. rendered SQL/parameter-layout cache only if invalidation can be proven.

Trigger implementation only when isolated compilation or repeated planning
exceeds both A/A resolution and materiality, or accounts for at least 10% of
the remaining end-to-end reference gap. If the trigger does not fire, publish a
no-change decision and close C7.

Cache keys must include every semantic dependency, including query text,
parameter type/shape where relevant, graph/schema/kind generation, optimizer
configuration, and any feature/lowering version. Test:

- graph/schema/kind changes;
- concurrent misses and hits;
- cancellation and errors;
- bounded size and eviction;
- mutable AST/value ownership;
- race detector;
- stable prepared-statement behavior.

Ship a cache only if its end-to-end saving exceeds A/A resolution for a
documented workload frequency and does not retain unacceptable memory.

## Phase CX: Conditional native-extension decision

After portable singleton/C3G work, compare the best correct PostgreSQL
implementation with its references. Current S3-U ratios do not trigger native
work. Open a native-extension ADR only when all of these hold:

- the portable candidate/reference upper bound remains above `1.10`;
- the absolute gap exceeds A/A resolution and materiality;
- two plausible portable alternatives have failed;
- profiling attributes the residual to unavoidable PostgreSQL/SPI/recursive
  bookkeeping;
- native deployment is an accepted product option.

The ADR must cover packaging, supported PostgreSQL versions/platforms,
deployment, managed-service compatibility, upgrades, rollback, security,
observability, crash isolation, CI, and a portable fallback. A prototype must
run the same semantic/resource/concurrency envelope. Native code is not a
shortcut around an unqualified portable candidate.

## Phase C8: Concurrency, memory, cancellation, and soak

Run accepted executors/materializers with:

- pool sizes one and the configured supported size;
- concurrency one, half-pool, full-pool, and twice-pool;
- cold whole-pool initialization;
- repeated cancellation and rollback;
- mixed shortest, lookup, mutation, and large-result traffic.

Predeclare per-session and whole-pool memory ceilings from the supported
deployment budget. Record:

- QPS and pool wait;
- p50/p95 and sufficiently sampled p99;
- backend/session identity and cold/warm state;
- CPU and memory high-water marks;
- shared/local/temp buffers and temp files/bytes;
- workspace relation sizes and generation counts;
- errors, cancellations, transaction aborts, and cleanup latency;
- state visible on a reused connection after success, error, rollback, and
  cancellation.

Run at least 10,000 mixed soak calls to expose workspace growth, prepared
statement churn, cache leaks, retained decode buffers, and session-state
corruption. Use success -> error/rollback -> success sequences on the same
physical connection and cancel shallow, deep, and disconnected searches. p99
becomes gated only after current A/A analysis establishes the required sample
count and each gated arm has at least 10,000 observations.

### C8 exit criteria

- Throughput scales acceptably to the supported pool size.
- Oversubscription is expressed as bounded pool wait, not memory explosion or
  state corruption.
- Per-session and whole-pool ceilings pass.
- Cancellation/rollback leave reusable sessions correct.
- Soak shows no unbounded memory, workspace, cache, or prepared-statement
  growth.
- Normal-tier queries do not spill unexpectedly.

## Phase C9: Cost-weighted complete-corpus loop

After C8, produce a report ranking each case/family by:

```text
addressable_cost = max(candidate - best_correct_reference, 0)
weighted_cost = addressable_cost
              * documented_workload_frequency
              * confidence
              * concurrency_or_resource_amplifier
```

Include confidence, A/A resolution, server/client attribution, resource slope,
and operational risk. Use production workload frequency where available;
otherwise publish both an equal-weight ranking and a sensitivity analysis. Do
not rank by Neo4j ratio.

Define `confidence` on a published 0-1 scale from reference exactness,
attribution completeness, and independent-round reproducibility. Define the
amplifier from measured concurrency, memory, I/O, or tail impact and publish a
unit-amplifier view so a subjective factor cannot hide raw addressable cost.

For each high-ranked item, either:

- open a scoped experiment with targets, controls, alternatives, and stop
  conditions;
- declare it complete under the workstream rule;
- defer it with a named missing capability or workload input.

Remove rejected production experiments. Preserve their code only in patches or
artifact bundles when needed for historical reproducibility.

## Cross-phase correctness matrix

Every affected traversal/path increment must cover, as applicable:

- graph-scoped colliding node and edge IDs;
- null, missing, contradictory, and same endpoints;
- zero-depth, lower/upper bounds, and open bounds;
- outbound, inbound, directionless, and mixed paths;
- direct, linear, diamond, dead-end, cycle, and disconnected shapes;
- parallel edges, relationship uniqueness, repeated nodes, and self-loops;
- exact node/relationship order and direction;
- duplicate rows and correlated source multiplicity;
- label/kind/property/ID predicates;
- shortest-path post-filter semantics;
- path functions, aliases, `WITH`, aggregation, and composed projections;
- multiple path calls in one statement;
- mutations and mutation-returning conservative fallback;
- sequential transactions, rollback, cancellation, and physical-session reuse;
- concurrent connections;
- stable schema/kind/template invalidation.

Shared Cypher semantics belong in backend-equivalent integration cases.
PostgreSQL-specific helper, plan, buffer, and workspace behavior belongs in
driver-scoped tests selected only by a PostgreSQL connection string.

## Statistical protocol

For every production behavior increment:

1. Predeclare target cases, controls, metrics, expected direction, materiality,
   and resource budgets before candidate capture.
2. Use fresh equivalent analyzed fixtures and pinned physical connections for
   serial session-state measurements.
3. Alternate baseline/candidate order across independently reloaded rounds.
4. Capture at least five rounds and 30-50 warm observations per round for
   p50/p95; use more rounds when reload variance dominates.
5. Bootstrap matched round medians and stratified p95 with a recorded seed and
   confidence level.
6. Compare movements against case/metric A/A resolution and absolute
   materiality.
7. Publish both within-session alternating A/A and independently reloaded
   block A/A; use the worse applicable ratio and absolute resolution.
8. Keep p99 diagnostic until the A/A-derived requirement and at least 10,000
   observations per gated series are satisfied.
9. Require every declared PostgreSQL case and every Neo4j oracle record to be
   present and exact.
10. Report incomplete, unsupported, and non-`ok` records; never drop them by
   intersecting successful series.
11. Preserve raw samples, not only percentiles.

Use ratio and absolute intervals together so sub-resolution microsecond noise
cannot fail a change and a large absolute tail cannot hide behind a percentage.
When cases are selected after a complete-corpus screen, use a fresh data set and
predeclared multiplicity correction. A full-corpus emergency gate identifies
alerts; only the matched confirmation protocol assigns causality.

The 20% complete-corpus threshold is an emergency ceiling. A confirmed 5-19%
affected-family regression still requires diagnosis, mitigation, or an explicit
maintainer-approved trade with rollback criteria.

## Artifact layout and commands

Use a durable bundle layout similar to:

```text
artifacts/perf/<run-series>/
  manifest.json
  source.patch
  source-untracked-manifest.json
  bin/
    predecessor-graphbench
    candidate-graphbench
    checksums.sha256
  corpus-declaration.json
  predeclaration.json
  baseline/
    round-1.jsonl ... round-N.jsonl
    combined.jsonl
  candidate/
    round-1.jsonl ... round-N.jsonl
    combined.jsonl
  block-aa/
  plans/
  references/
  aa-resolution.json
  gate.json
  report.md
  checksums.sha256
```

Local staging may remain under `.coverage`, but completion requires the durable
bundle.

Canonical full capture shape:

```bash
go build -trimpath -o .coverage/<series>/bin/graphbench ./cmd/graphbench

.coverage/<series>/bin/graphbench \
  -round 1 \
  -iterations 30 \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -postgres-references \
  -jsonl-output .coverage/<series>/round-1.jsonl
```

Canonical gate shape:

```bash
make perf_gate \
  PERF_BASELINE=.coverage/<series>/baseline.jsonl \
  PERF_CANDIDATE=.coverage/<series>/candidate.jsonl \
  PERF_TARGETS='<predeclared comma-separated targets>'
```

Canonical A/A shape:

```bash
make perf_aa PERF_AA_ARTIFACT=.coverage/<series>/candidate.jsonl
```

After the C0R flags/report exist, the targeted confirmation shape is:

```bash
go build -trimpath -o .coverage/<series>/bin/candidate-graphbench \
  ./cmd/graphbench
sha256sum .coverage/<series>/bin/candidate-graphbench

.coverage/<series>/bin/candidate-graphbench \
  -round 1 \
  -modes postgres_sql \
  -cases '<exact comma-separated targets and controls>' \
  -warmup-iterations 20 \
  -iterations 50 \
  -pool-size 1 \
  -pg-connection "$PG_CONNECTION_STRING" \
  -arm candidate \
  -jsonl-output .coverage/<series>/confirm/round-01-candidate.jsonl
```

Run the saved predecessor binary against its equivalent reloaded database in
the other arm, reversing order on even rounds. The proposed paired report shape
is:

```bash
make perf_confirm \
  PERF_LEFT=.coverage/<series>/confirm/predecessor.jsonl \
  PERF_RIGHT=.coverage/<series>/confirm/candidate.jsonl \
  PERF_AA=.coverage/<series>/block-aa/report.json \
  PERF_CASES='<exact primary target names>'
```

Connection strings must come from approved environment input and must be
redacted from artifacts. Use IPv4 loopback where the sandbox resolves
`localhost` only to an unavailable IPv6 listener.

## Pull-request and experiment sequence

Keep each behavior change independently attributable:

1. **Targeted diagnostic, paired report, and reconstructible bundle workflow**
   - Exact filters, untimed warmups, arm/order metadata, diagnostic-only
     declaration, two-level A/A, source/binary bundle, tests/docs.
2. **Regression reconciliation report**
   - Matched isolated blocks, multiplicity-adjusted classification, no
     production change.
3. **Candidate-name repair and exact reference comparator**
   - Versioned S3-U/S3-B names, legacy mapping, raw semantic validation.
4. **Shortest component attribution**
   - Workspace/runtime planning/frontier/visited/reconstruction probes.
5. **Large-result client attribution**
   - Transfer/decode/ownership/allocation waterfall.
6. **Singleton semantic adapter and largest-tier generator coverage**
   - No production dispatcher branch.
7. **True S1/S2 benchmark prototypes and normalized S3 controls**
   - Distinct distance/path state; no production dispatcher branch.
8. **S0-S3 final tournament record**
   - Exact semantics, resource envelope, references, selection decision.
9. **Singleton optimizer decision and schema/helper boundary**
   - Translation/schema tests; still benchmark-gated.
10. **Distance-only singleton mode**
   - No path state; exact fallback tests; matched candidate artifact.
11. **One-path singleton mode**
   - Ordered IDs; materialization boundary; matched candidate artifact.
12. **M0/M1 materializer comparison**
    - Search fixed; paired path-tax report.
13. **M2 batched hydration, only if it wins**
    - High-output benefit and low-output non-inferiority.
14. **C3G generic/correlated/multi-pair work**
    - Independent baselines and multiplicity tests.
15. **All-shortest predecessor-DAG experiment**
    - Exact tie and parallel-edge semantics.
16. **C5A staged traversal state**
    - Last-use lowering and multiplicity negatives.
17. **C5B decode/ownership work**
    - Race/lifetime/cancellation gates.
18. **List-cardinality strategy, if still addressable**
19. **ADCS reference rebuild and conditional optimization**
20. **Conditional compilation cache, only if the C7 trigger fires**
21. **Conditional native-extension ADR/prototype, only if the CX trigger fires**
22. **Concurrency and soak qualification**
23. **Cost-weighted corpus report and next-plan/stop decision**

Do not combine the selected singleton search change, path materializer, generic
shortest rewrite, and cache in one production increment. Their effects and
rollback boundaries must remain separable.

## Immediate next actions

Execute in this order:

1. Add exact case filtering, fixed untimed warmups, arm/order metadata, paired
   p50/p95 reporting, and diagnostic-only artifact enforcement.
2. Add same-binary block/reload A/A and reconstructible source/binary bundle
   generation.
3. Run matched isolated `LOOKUP-05`/depth-2/control blocks and classify the
   alerts.
4. Freeze and publish C0R, then rerun the complete corpus.
5. Rename the legacy CTE candidates S3-U/S3-B and replace row-count-only
   `fullComparator` validation with exact semantic observations.
6. Add the missing incumbent shortest server probes and close the 90%
   attribution requirement.
7. Implement benchmark-only true S1/S2 candidates, normalize S0-S3 boundaries,
   and run the full semantic/largest-tier adapter.
8. Select the winning executor or bounded hybrid with every rejection recorded.
9. Ship its distance-only form first with explicit lowering/fallback diagnostics.
10. Ship its one-path form separately.
11. Run M0/M1 with fixed search and integrate only the material winner.

Do not begin with a `LOOKUP-05` SQL rewrite, translation cache, ADCS rewrite, or
universal dispatcher based on the mislabeled current reference.

## Definition of done

This continuation is complete when:

- the two live gate failures have durable evidence-backed classifications;
- accepted baselines and candidates are reconstructible, not identified only
  by hashes;
- at least 90% of shortest server and selected large-result end-to-end cost is
  attributed;
- the selected S0-S3 executor or measured hybrid passes the complete singleton
  semantic, scale, resource, cancellation, and concurrency envelope;
- distance-only shortest carries no path/predecessor state;
- one-path shortest returns minimal ordered IDs and uses the selected linear
  materializer;
- singleton eligibility and every fallback reason are explicit and tested;
- generic/correlated/multi-pair/directionless and `allShortestPaths` workstreams
  independently meet the workstream rule or retain documented incumbents;
- ADCS work is based on a competitive correct reference or explicitly deferred;
- any cache or native extension is justified by measured remaining cost;
- complete PostgreSQL and Neo4j oracle manifests are exact;
- p50/p95, cold/warm, pool, memory, cancellation, and soak gates pass;
- p99 is gated only with sufficient A/A-derived samples;
- rejected production experiments are removed and their evidence retained;
- `make format`, `make test`, `go test -race ./cmd/graphbench`, PostgreSQL
  `make test_all`, Neo4j `make test_all`, generated fixture/template workflows,
  and `git diff --check` pass;
- a cost-weighted C9 report either declares completion within current
  architecture/resolution or defines the next bounded continuation.

# PostgreSQL Traversal Performance Rework Plan

## Purpose

Close the isolated PostgreSQL performance gaps for these query shapes without weakening Cypher semantics or regressing the general traversal path:

- Bound-pair `shortestPath` with one statically identified start and end node.
- ADCS P1 endpoint projection: `RETURN id(ca), id(d)`.
- ADCS P1 path projection: `RETURN p`.

The original request named ADCS path materialization twice. This plan treats it as one workstream and also covers the combined P1/P2 query where the same materialization behavior is amplified.

This is an implementation plan, not a claim that the proposed gains have already been realized. Measured results, expected improvements, and hypotheses requiring A/B validation are identified separately throughout.

## Current handoff status — 2026-08-05

The implementation is present in the working tree and the first statistically complete live validation pass is finished. The automated gate **failed**. Do not treat the rework as complete, and do not reuse the rejected/overlapped intermediate rounds described in the report.

The final comparison used clean commit `05e70a18d7c6` engine sources with the current GraphBench instrumentation as the matched baseline, the current working tree as the candidate, five independently reloaded rounds, and 30 warm observations per case/backend/round. Backend order reversed on even rounds and baseline/candidate order also alternated. The final target series each contain 150 baseline and 150 candidate warm samples.

The complete handoff report is `.coverage/live-bench-rerun-20260805/REPORT.md`; raw aggregates are `baseline.jsonl` and `candidate.jsonl`, and the seeded bootstrap result is `perf-gate.json` in the same directory.

All implementation changes are still uncommitted in a deliberately dirty working tree. Preserve them: do not reset, checkout, revert, or attempt to recreate the work from the baseline commit. The instrumented baseline binary was built from a temporary `git archive` of `05e70a18d7c6` with only the current GraphBench measurement harness layered on top; that temporary source tree has been removed, while the binary and checksummed output remain in the artifact directory.

Implemented landmarks already present in the working tree include graph-scoped traversal/hydration, reusable versioned shortest-path workspace, the proven singleton endpoint array path, shortest-path limit and length observation work, graph-scoped ordered edge-ID path materialization, conservative suffix/field-requirement lowering, exact target observations, raw cold/warm samples, and the executable bootstrap regression gate. Inspect and refine these paths rather than restarting the plan from Phase 0.

| Target | Matched PG baseline | PG candidate | Median delta | Candidate/baseline ratio, 95% CI | Candidate PG/Neo4j ratio, 95% CI | Gate status |
|---|---:|---:|---:|---:|---:|---|
| Bound-pair shortest path | 10.915 ms | 5.278 ms | -51.6% | 0.484 (0.468–0.520) | 6.075x (4.533–6.503) | Fail: improvement and backend-ratio gates |
| ADCS P1 endpoint IDs | 0.908 ms | 0.878 ms | -3.3% | 0.967 (0.857–1.016) | 0.817x (0.599–1.053) | Fail: improvement gate |
| ADCS P1 path | 1.834 ms | 1.661 ms | -9.4% | 0.906 (0.672–0.968) | 1.754x (1.392–2.109) | Fail: improvement gate |

All exact target rows and paths matched their declarations and matched across PostgreSQL and Neo4j in every final round. PostgreSQL target p95 improved by 46–52%, but the configured gates remain conjunctive and therefore fail on the median criteria above. Across the complete comparable corpus, 96 of 101 series passed. PostgreSQL `LOOKUP-05_repeated_case_insensitive_prefix` and Neo4j `TRUST-03_directional_branch_local_kinds` also failed their p95 regression gates.

### Baseline interpretation

The original capture below and the matched validation use different GraphBench session behavior. In particular, the current harness pins one PostgreSQL physical connection per runner, resets it per case, separates cold samples, and warms pgx/PostgreSQL statement state consistently. That change reduced the remeasured clean-HEAD ADCS baseline from 4.964/6.073 ms to 0.908/1.834 ms. The historical candidate deltas (-82.3% for endpoint IDs and -72.7% for the P1 path) are useful context but are not a valid A/B acceptance result; the automated gate correctly uses the matched re-instrumented baseline and reports only -3.3%/-9.4%.

With the matched point estimates, the percentage gates imply ceilings of 4.366 ms for shortest path, 0.545 ms for endpoint IDs, and 1.284 ms for the P1 path. The candidate-backend point estimates imply separate ceilings of about 2.606, 2.150, and 2.367 ms respectively. Confidence-interval upper bounds, rather than point estimates alone, remain authoritative.

### Priorities for the next pass

1. Focus first on bound-pair shortest path. Its candidate `EXPLAIN ANALYZE` execution median is about 4.18 ms while end-to-end warm median is 5.28 ms, so the remaining 6x PostgreSQL/Neo4j gap is predominantly server-side. Profile the proven-singleton array path through `_bidirectional_sp_harness`, especially workspace reset and dynamic frontier execution, before adding more client compilation work. The backend-ratio gate is currently stricter than the 60% improvement gate.
2. Resolve the ADCS acceptance-baseline question explicitly before more optimization. Both ADCS backend-ratio gates already pass and server execution is small; the matched percentage gates fail because the clean baseline benefits strongly from the new session harness. Do not silently weaken the gate or claim the historical cross-method numbers as an A/B pass.
3. Reproduce the two tail failures in isolated matched rounds before changing production code. The Neo4j-only `TRUST-03` regression is evidence of environmental tail noise; PostgreSQL `LOOKUP-05` improved at the median but regressed at p95.
4. Fix or account for the two consistently non-`ok` PostgreSQL scan records (`SCAN-02` and `SCAN-03`, missing `Meta` kind mapping) before claiming full-corpus completion. Clean HEAD additionally cannot execute the newly added `length()` shortest-distance case. The current gate excludes these records.
5. Use a new disposable PostgreSQL database for another destructive GraphBench pass and run only one benchmark batch at a time. The database used for this pass was dropped. In this environment PostgreSQL `localhost` selected an unavailable IPv6 listener, so the equivalent IPv4 host was required. Neo4j remains loaded with the final benchmark fixture.

Post-validation checks passed with `make test`, `go test -race ./cmd/graphbench`, and `git diff --check`. Separate PostgreSQL and Neo4j `make test_all` runs passed before the final observation-normalization adjustment; rerun both after any next implementation change. `make format` could not find the expected `goimports` executable in this sandbox, so the touched files were formatted with `go run golang.org/x/tools/cmd/goimports@v0.47.0` instead.

## Historical baseline (original plan)

The live benchmark baseline was captured on 2026-08-05 from DAWGS commit `05e70a18d7c6`, PostgreSQL 17.10, and Neo4j 4.4.44. The comparison used a fresh PostgreSQL database with one graph partition so the residual gaps were not caused by cross-partition planning.

The complete report and raw captures are under `.coverage/live-bench-20260805/`; the summary is `.coverage/live-bench-20260805/REPORT.md`.

PostgreSQL planning and execution values below came from separate `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` executions. They diagnose the dominant work but do not add exactly to the end-to-end median.

| Case | PostgreSQL median | Neo4j median | Ratio | PostgreSQL execution | PostgreSQL planning |
|---|---:|---:|---:|---:|---:|
| Bound-pair shortest path | 12.396 ms | 1.166 ms | 10.63x | 9.016 ms | 0.345 ms |
| ADCS P1 endpoint IDs | 4.964 ms | 1.029 ms | 4.83x | 0.676 ms | 4.548 ms |
| ADCS P1 path | 6.073 ms | 1.118 ms | 5.43x | 1.614 ms | 3.899 ms |

Independent scenarios confirmed the same shape:

- Bound-pair shortest path was 13.10x slower.
- Diamond and disconnected shortest paths were 16.18x and 12.71x slower.
- ADCS P1 path was 2.03x slower.
- Combined ADCS paths were 3.28x slower.
- Combined ADCS endpoint projection was 2.27x slower.

### Shortest-path evidence

The shortest-path diagnostics strongly implicate fixed harness overhead:

- `bidirectional_sp_harness` accounted for 4,876 of 5,575 shared-buffer hits in a representative isolated plan.
- It also performed local temporary-buffer reads, writes, and dirtying.
- The two-edge result hydration used only six shared-buffer hits.
- The harness currently creates approximately ten temporary tables and 21 indexes per invocation across pathspace, visited, filter, unresolved-pair, and resolved-pair state.
- `VACUUM (ANALYZE)` barely changed shortest-path latency, ruling out persistent-table statistics as the main cause.
- Ordinary recursive traversal on the same small graph was sub-millisecond server-side, demonstrating that graph access itself is not the principal cost.

Because plan capture used `TIMING OFF`, the artifacts attribute buffer and temporary-state activity rather than per-node elapsed time. The several-millisecond workspace benefit remains a hypothesis until a controlled A/B run.

### ADCS evidence

Planning/custom-plan behavior is the leading hypothesis for the endpoint query, not yet a proven per-request cost:

- Its generated SQL was 3,303 bytes and its captured plan had 193 lines.
- Standalone server execution was only about 0.6-0.8 ms in the cleanest rounds, while standalone `EXPLAIN` reported much more planning than execution time. A warmed pgx prepared execution does not necessarily pay that exact `EXPLAIN` planning time, so Phase 0 must measure repeated executions on one physical connection under `auto`, forced-custom, and forced-generic plan modes.
- The fixed suffix is evaluated once in an `EXISTS` satisfaction probe and again to produce the suffix bindings.
- PostgreSQL can prune some unused fixed-suffix fields physically, so syntactic node composites do not translate one-for-one into heap materialization. Field-sensitive lowering is still useful for simplifying the plan but is not, by itself, a four-millisecond execution fix.
- The endpoint form must continue carrying raw relationship IDs for whole-path relationship uniqueness even when it does not return a path.

The path query adds distinct materialization work:

- Its generated SQL was 4,727 bytes and its captured plan had 228 lines.
- P1 performs four correlated edge-hydration subplans: one for the variable segment and one for each of the three fixed relationships.
- The combined P1/P2 query performs nine such subplans.
- `ordered_edges_to_path` repeatedly searches the remaining edge array to reconstruct connectivity, making its generic reconstruction approximately quadratic in path length.

### Client compilation evidence

The PostgreSQL driver reparses, optimizes, translates, and renders Cypher for every request. A local pipeline microbenchmark measured approximately:

- 0.36 ms for the bound-pair shortest query.
- 0.55-0.59 ms for the ADCS endpoint and path queries.

Compilation caching is therefore worthwhile, but it cannot explain or close the multi-millisecond PostgreSQL planning gap alone. Pgx statement caching is already enabled. PostgreSQL may still produce custom plans before switching to a generic plan, and the query can be spread over the pool's five minimum physical connections.

### Existing prior art

Review non-ancestor commit `ffa0f83` on `upstream/kpom/fix-benchmarks` before changing the harness. It contains selectively reusable ideas such as fewer scratch indexes, cached frontier sizes, single-pass frontier splitting, and one-scan path hydration. Current HEAD has absorbed some but not all of that work; do not cherry-pick the commit wholesale because it also contains unrelated and superseded schema changes.

Commit `bc9c4ca` demonstrates adding post-load `VACUUM (ANALYZE)` to the benchmark. Reimplement the relevant source change cleanly rather than copying its generated binary artifact.

## Goals

1. Reduce warm bound-pair shortest-path latency by at least 60% and bring the PostgreSQL/Neo4j ratio to 3x or less on the clean baseline.
2. Reduce ADCS endpoint latency by at least 40% and bring the ratio to 2x or less.
3. Reduce ADCS P1 path latency by at least 30% and bring the ratio to 2.5x or less.
4. Remove at least 50% of the path-specific server tax for observed ADCS paths, with 75% as the stretch target.
5. Preserve exact path order, direction, endpoint multiplicity, zero-depth behavior, relationship uniqueness, and fallback behavior.
6. Avoid statistically significant median or p95 regressions greater than 20% in the rest of the clean comparable corpus.
7. Keep connection establishment and cold per-session setup visible as separate metrics rather than hiding either inside warm-only results.

These are initial acceptance gates, not portable absolute latency guarantees. They must be evaluated with repeated rounds and confidence intervals, not a single 15-iteration run. Each percentage and ratio gate is conjunctive; on the recorded baseline, the ratio gates imply the stricter effective thresholds:

| Case | Percentage gate | Ratio gate | Implied PostgreSQL ceiling | Implied reduction |
|---|---:|---:|---:|---:|
| Bound-pair shortest path | at least 60% | at most 3x | 3.498 ms | 71.8% |
| ADCS P1 endpoint IDs | at least 40% | at most 2x | 2.058 ms | 58.5% |
| ADCS P1 path | at least 30% | at most 2.5x | 2.795 ms | 54.0% |

The absolute ceilings are baseline-specific and must be recomputed when a published baseline changes.

## Scope and guardrails

In scope:

- PostgreSQL optimizer decisions and Cypher-to-SQL lowering.
- PostgreSQL traversal helper functions and their session-local working state.
- Path representation and final materialization.
- Query compilation and prepared-plan experiments after SQL shape is stabilized.
- Exact-result, translation, integration, plan-invariant, and scale coverage for the affected forms.
- Benchmark hygiene needed to make the comparison reproducible.

Out of scope for the initial delivery:

- Replacing PostgreSQL storage with another graph engine.
- A global rewrite of all variable-length traversal.
- Globally forcing PostgreSQL generic plans.
- Treating a direct recursive CTE as production-ready before it passes cyclic, disconnected, high-fanout, and tie-semantics gates.
- Optimizing unrelated count, mutation, or reconciliation gaps.
- Using multi-partition overhead to explain the residual one-partition measurements.

Cross-cutting requirements:

- Every affected existing or new SQL path and helper function must be graph-scoped. Node and edge IDs are only unique with `graph_id`; partition pruning is a performance benefit, while preventing cross-graph hydration is a correctness requirement.
- Shared integration cases remain backend-equivalent. PostgreSQL-only plan assertions belong in PostgreSQL-scoped tests.
- New fast paths must be additive and retain the current generic implementation as a conservative fallback.
- Do not infer singleton endpoint semantics merely from `LIMIT 1`; prove that the requested endpoint universe contains exactly one pair.
- Keep performance changes in separable pull requests so each can be benchmarked and reverted independently.

## Design principles

### Carry IDs, hydrate at the observation boundary

Traversal, relationship uniqueness, endpoint filtering, and suffix joining generally need IDs rather than full node and relationship composites. Carry compact IDs through intermediate frames and hydrate only when a returned value or path function requires a complete entity.

### Pay setup once per physical connection

The shortest-path harness needs indexed mutable state, but it should not rebuild identical temporary relations on every request. Session-local PostgreSQL objects match pgxpool's physical-connection model and avoid cross-session interference.

### Specialize only when eligibility is provable

The singleton shortest path, ID-only projection, suffix reuse, and linear path materializer must each have narrow eligibility rules and explicit fallback tests.

### Avoid duplicate semantic work

An optimization must not prove suffix existence and then independently traverse the same suffix to return it. Path edge segments must not be hydrated independently when they can be concatenated and hydrated once.

### Fix query shape before planner policy

Reduce joins, subplans, row width, and value-sensitive SQL first. Evaluate generic plans and compilation caches only after the emitted SQL is stable and smaller.

## Delivery sequence

| Phase | Outcome | Depends on |
|---|---|---|
| 0 | Correctness assertions and reproducible baselines exist. | None |
| G | Every affected shortest/ADCS read and fallback is constrained to the target graph. | Phase 0 |
| 1 | Shortest-path LIMIT and endpoint lookup estimates are corrected. | Phases 0 and G |
| 2 | Shortest-path workspaces are reused safely per connection. | Phases G and 1 |
| 3 | Proven singleton endpoint pairs use a lean harness mode. | Phase 2 |
| 3L | `length(shortestPath(...))` observes ordered edge IDs without path hydration. | Phase 3 |
| 4 | Observed paths hydrate one ordered edge-ID stream. | Phases 0 and G |
| 5A | A shape-based gate avoids a redundant suffix prefilter where it is not worthwhile. | Phases 0 and G |
| 6A | Field-requirement metadata exists without changing SQL semantics. | Phase 0 |
| 5B | An eligible suffix is produced once with conservative complete bindings, exact traversal semantics, and multiplicity. | Phase 5A |
| 6B | Endpoint-only queries carry field-sensitive scalar state. | Phase 6A and a resolved Phase 5B experiment |
| 7 (conditional) | Stable SQL benefits from bounded compilation and plan-cache work if its trigger fires. | Phases 3 and 6B |
| 8 (conditional) | Identical multi-branch expansions are shared if their trigger fires. | Phases 4-6B |
| 9 | Full corpus, scale, cold/warm, and rollout gates are satisfied. | Phases G-6B and any triggered conditional phase |

Phase G is a shared correctness prerequisite and its one-partition performance effect is reported separately. Phases 1-3L are the shortest-path track. In the ADCS track, Phase 4, Phase 5A, and Phase 6A can start after their listed prerequisites; Phase 5B emits conservative complete suffix bindings, and Phase 6B then uses stage-sensitive requirements to make those and other eligible bindings scalar. A rejected Phase 5B experiment is still a resolved decision: Phase 6B applies to the retained Phase 5A/legacy suffix shape. The two tracks can otherwise proceed in parallel. Phases 7 and 8 are not on the critical path unless their numeric triggers fire.

## Phase 0: Correctness and measurement prerequisites

### Benchmark correctness

- Extend GraphBench read validation beyond row count for these cases. Perform one untimed exact-result preflight and one untimed postflight around each timed block; do not mix decoding/comparison work into latency samples.
- Add an expected-output schema such as `expected.id_rows` whose values are fixture node names. Reverse-map returned node IDs through the dataset's complete `opengraph.IDMap`, then compare endpoint rows as a multiset of stable fixture identities plus kinds/properties where observed. `node_params` remains input-parameter resolution only; the bound-pair cases must explicitly declare `node_params: {start_id: ..., end_id: ...}`. Do not compare backend-generated relationship IDs across PostgreSQL and Neo4j. For paths, compare ordered stable node identities and ordered relationship kinds/properties, and separately assert that a relationship ID is not reused within one returned path.
- For equal-length diamonds, accept the explicit set of valid shortest results instead of fixing one arbitrary route.
- Add expected observations to the standalone ADCS scenarios; they currently do not declare exact expected rows.
- Extend GraphBench's machine-readable result format to retain every raw latency sample with round, case, backend, connection/session identifier, and cold/warm classification; the current median/p95/maximum summaries are insufficient for confidence intervals or an automated regression gate.
- Continue recording translated SQL, optimizer/lowering metadata, plan operators, execution time, planning time, buffer activity, and the compilation-pipeline microbenchmark method and raw output.
- Record fixture cardinalities and checksum, graph partition count, hardware/OS, PostgreSQL settings, PostgreSQL/Neo4j versions, DAWGS commit, pool settings, and whether a sample is a cold or warm physical-connection call.
- Run `VACUUM (ANALYZE)` through the PostgreSQL pool outside any transaction after fixture loading and before timed PostgreSQL reads. Treat any failure as a benchmark failure rather than logging and continuing. Fixture reloads must not accumulate dead tuples across rounds.
- Use a disposable database or graph for destructive GraphBench runs.
- Alternate backend order across independent rounds.
- Publish a stable baseline report plus raw-capture checksums as a committed benchmark artifact or durable CI artifact. `.coverage/live-bench-20260805` is gitignored local evidence and cannot be the only review record.

### Required semantic cases

Add a backend-equivalent integration case for the exact bound shape:

```cypher
MATCH p = shortestPath((s)-[*1..]->(e))
WHERE id(s) = $start_id AND id(e) = $end_id
RETURN p
LIMIT 1
```

It must cover:

- Direct edge versus a longer route.
- Equal-length diamond paths, accepting one valid shortest path.
- Disconnected endpoints.
- Wrong direction.
- Relationship-kind and depth bounds.
- Cycles and relationship uniqueness.
- Missing and null endpoint parameters.
- Same-endpoint behavior, including the existing error contract.
- `*0..0` and `*0..` behavior, including same-endpoint handling and fallback eligibility.
- Exact ordered node and relationship hydration.

Add isolated ADCS P1 cases alongside the existing combined coverage:

- Endpoint projection returns four rows without accidental `DISTINCT` collapse.
- P1 path projection returns four paths with lengths 3, 4, 4, and 5.
- The `MemberOf*0..` zero-depth result remains present.
- Node and relationship order and relationship kinds are exact.
- Decoy suffixes fail independently by direction, kind, and endpoint kind.

Keep the combined ADCS cases as sentinels:

- Combined P1/P2 remains eight rows.
- P1 and P2 Cartesian multiplicity is preserved.
- Shared endpoint bindings do not collapse distinct path pairs.

### Scale matrices

Generate fixtures rather than committing large handwritten JSON. Implement deterministic generators in a shared benchmark test utility, register the resulting datasets in `cmd/graphbench/datasets.go`, and add generator cardinality, checksum, and repeatability tests. Execute a documented orthogonal/pairwise subset for normal CI rather than the Cartesian product; reserve the largest depth/fanout cases for a separately timed scale gate with explicit per-case timeouts.

Shortest-path matrix:

| Dimension | Required points |
|---|---|
| Depth | 1, 2, 4, 8, 16 |
| Fanout | 1, moderate, dense |
| Shape | linear, diamond, dead-end, cycle, disconnected |
| Direction | outbound, inbound, directionless fallback |
| Relationship kinds | untyped, one kind, several kinds |
| Endpoint state | valid, missing, null, contradictory constraints |
| Connection state | first call, warm reused workspace |

ADCS matrix:

| Dimension | Required points |
|---|---|
| `MemberOf` depth | 0, 1, 2, 4, 8 |
| `MemberOf` fanout | 1x, 10x, 100x, 1000x |
| Valid suffix density | none, sparse, half, all |
| Decoy cause | edge kind, direction, endpoint kind, disconnected suffix |
| Projection | endpoint IDs, P1 path, P2 path, combined paths |
| Property payload | small and large node/edge properties |

### Phase 0 exit criteria

- A deliberately reordered or partially hydrated path fails an assertion.
- A deliberately deduplicated endpoint result fails an assertion.
- Each benchmark round begins from equivalent analyzed fixture state.
- A pinned PostgreSQL run can identify a physical connection by backend PID and report cold and warm shortest-path calls on that same session.
- Five independent rounds with at least 30-50 timed observations per case can be compared automatically from retained raw samples.
- The baseline report, environment manifest, raw-capture checksums, and exact GraphBench invocation are available to reviewers outside the local gitignored directory.

## Phase G: Graph-scope the affected reads and fallbacks

The clean latency baseline uses one graph partition, but the affected SQL currently reads partitioned parent `node` and `edge` relations without consistently constraining `graph_id`. Because the schema keys entities by `(id, graph_id)`, this is both a multi-partition planning problem and a possible cross-graph correctness problem when explicitly assigned IDs collide. Do not make a new fast path depend on the accidental global uniqueness of sequence-generated fixture IDs.

Implementation:

- Thread the translator's known target graph through every node/edge source reachable from the bound-pair shortest and ADCS endpoint/path shapes, including generated BFS primer/recursive fragments, suffix traversals, endpoint hydration, `EdgeArrayFromPathIDs`, and every retained fallback helper such as `ordered_edges_to_path`, `nodes_to_path`, or `edges_to_path` that can hydrate these results.
- First prefer an explicit typed `graph_id` predicate on both sides of each ID join. Verify static/startup partition pruning under prepared `auto` and generic plans. If many-partition planning remains above the recorded budget, A/B rendering concrete target-partition relations; doing so makes graph/relation generation part of the SQL-template cache key.
- Ensure endpoint edges and nodes are constrained to the same target graph, not merely filtered independently after an ID-only join has multiplied rows.
- Keep this as a separate correctness/performance pull request. Report both its many-partition benefit and its one-partition overhead, but do not credit removal of unrelated 15-partition planning overhead toward the isolated hotspot targets.

Tests:

- A PostgreSQL-scoped end-to-end fixture creates two graph partitions with deliberately colliding node and edge IDs and distinguishable kinds/properties. Running each affected query against one selected graph must never observe the decoy graph.
- Translation tests assert a target-graph predicate or concrete target relation for every affected `node`/`edge` source, including dynamic harness fragments and generic materialization fallbacks.
- Plan tests prove that only the selected graph partition is scanned under the supported prepared-plan modes.

Phase G exit criteria:

- Colliding-ID correctness passes on the fast paths and every fallback reachable from the target queries.
- The selected partition is pruned in the many-partition plan corpus.
- The one-partition warm median/p95 regression intervals are not wholly above 1.20, and the clean Phase 0 baseline remains the cumulative reference for final goals.

## Phase 1: Correct shortest-path cardinality estimates

The existing LIMIT lowering passes `path_limit` into the PL/pgSQL harness but does not place a SQL `LIMIT` on the SELECT containing the function scan. Adding the outer SQL limit does not change the set-returning function's declared/default estimate of 1,000 rows; it gives the containing `Limit` node, and therefore downstream joins, an at-most-one-row estimate. That can prevent full endpoint scans, sorts, merge joins, or hash joins when only one result is requested.

Implementation:

- Extend `appendLimitToShortestPathHarness` in `cypher/models/pgsql/translate/projection.go` to set the containing query's `Limit` as well as appending the harness argument.
- Retain the current safety checks: one harness call, a transparent tail projection, no ordering, grouping, aggregation, skip, mutation, or nontransparent predicate.
- Preserve the internal function argument. The argument stops the BFS; the SQL limit bounds the containing relation for downstream planning.
- Use the existing indexed lateral endpoint lookup shape from ordinary traversal for the final root and terminal node hydration.
- Do not redeclare the general multi-pair function as `ROWS 1`.

Tests:

- Translation tests assert both the harness `path_limit` argument and the FunctionScan-containing SELECT limit, including literal `LIMIT 0`, `LIMIT 1`, and parameterized limits where pushdown is supported. The internal harness convention treats `path_limit = 0` as unlimited, so only the outer SQL `LIMIT 0` may be relied upon to prevent execution.
- Negative tests retain no pushdown for ordering, aggregation, multiple harness calls, mutation, or filtering that can change the selected row.
- Structural translation tests assert the lateral endpoint-lookup shape. A PostgreSQL plan test uses a sufficiently large analyzed fixture before asserting indexed endpoint access; a tiny fixture may legitimately choose a sequential scan.
- Benchmark SQL-limit pushdown and lateral endpoint hydration as separate A/B increments before measuring them together.

Expected impact:

- Approximately 0.5-1 ms on the current small fixture is a reasonable hypothesis.
- The larger benefit is protecting latency as node cardinality grows.
- This phase does not address internal temporary-table setup and cannot meet the shortest-path target alone.

Phase 1 exit criteria:

- For a constant `LIMIT 1`, or a value-aware custom plan, the containing `Limit` reports the pushed bound and downstream estimates reflect it; the test does not require the function scan itself to report `ROWS 1`. A generic plan for `LIMIT $n` may use PostgreSQL's heuristic estimate, so its translation is tested without asserting an at-most-one plan estimate.
- The semantic shortest-path corpus is unchanged.
- No plan regression occurs for multi-pair shortest queries.

## Phase 2: Reusable session-local shortest-path workspace

### Runtime design

Split workspace management by capability:

1. `ensure_bsp_core_workspace()` creates the frontier/visited core once; the singleton array path calls only this operation.
2. `ensure_bsp_generic_workspace()` lazily adds the root/terminal/pair filters and unresolved/resolved pair state required by text-filter and pair-aware generic modes.
3. `reset_bsp_workspace(mode)` clears only the objects required by the next invocation.

Use `pg_temp` objects with `ON COMMIT PRESERVE ROWS`. Prefer lazy initialization in the first shortest-path call rather than eagerly creating all relations on every pooled connection, because many connections may never execute shortest paths.

Use a dedicated `bsp_*` physical name prefix for the first implementation. The ensured workspace should include reusable forms of:

- `forward_front`.
- `backward_front`.
- `next_front`.
- `forward_visited`.
- `backward_visited`.
- In the lazy generic extension: root/terminal/pair filters and unresolved/resolved pair state required by the existing generic harness.

Phase 2 must retain the generic pair-aware behavior. Phase 3, after it proves singleton eligibility, initializes only the core and omits creation, initialization, and access to filter and pair-resolution objects. A later generic call on the same session lazily ensures the missing generic extension.

Scope Phase 2 strictly to `_bidirectional_sp_harness`/`shortestPath`. The `bsp_*` namespace must isolate it from unidirectional SP and all `allShortestPaths`/ASP helpers, which remain on their legacy workspaces and fallbacks in this plan. ASP has additional `resolved_pair_depths`/`resolved_paths` state and different frontier swapping; do not partially migrate it. If sharing is later desirable, enumerate that state and migrate every producer and consumer atomically to a versioned compatible superset.

The dynamic primer and recursive SQL emitted by `cypher/models/pgsql/translate/expansion.go` currently hardcodes frontier, visited, filter, and constraint names. Add a shortest-workspace naming context to fragment generation so the SP fragments reference `pg_temp.bsp_*` consistently, including `ON CONFLICT ON CONSTRAINT ...` identifiers. Renaming only the SQL helper's tables would otherwise make generated fragments read the wrong workspace or fail.

Use stable physical frontier slots rather than renaming tables to exchange logical roles. PostgreSQL indexes move with a renamed table, so `ALTER TABLE ... RENAME` followed by `CREATE INDEX IF NOT EXISTS` can silently attach the wrong logical index set. Prefer a role flag or explicit clear-and-copy/swap strategy whose table and index OIDs remain stable, and benchmark its row-movement cost before adoption.

### Index and statement audit

Workspace reuse removes DDL churn but not index-maintenance cost. Measure every current scratch index against the statements that probe it.

- Benchmark one multi-relation `TRUNCATE` against indexed `DELETE` for tiny warm workspaces. `TRUNCATE` can change relfilenodes and takes stronger locks; `ANALYZE` writes statistics. Neither should be described as zero catalog churn.
- Remove an index only after a plan/scale A/B proves it is unused or more expensive to maintain than to scan.
- Pay particular attention to partial `satisfied`/`is_cycle` indexes and root/next compound indexes on small frontiers.
- Preserve indexes required for high-fanout and multi-pair fallback even if the singleton fixture does not use them.
- Keep index-removal measurements separate from workspace-reuse measurements.
- Inventory dynamic `EXECUTE` planning inside each BFS iteration after DDL is removed; static SQL or stable prepared fragments are a later optimization if dynamic planning becomes the next dominant cost.

### Lifecycle rules

- Clear at the start of every call, including after the previous transaction committed successfully, using the reset strategy selected by the preceding A/B.
- After a transaction error, PostgreSQL must first roll back; the next valid call then performs the reset before reading any retained state.
- Inside an invoked generic harness, reset all reusable tables before any internal early return caused by empty endpoint materialization. Phase 3's outer validation must avoid invoking the harness at all when an endpoint is absent.
- Schema-qualify all workspace relations through `pg_temp` to prevent search-path ambiguity.
- Add a small workspace-version marker. If the expected version or table shape differs, drop and rebuild only the known `pg_temp` workspace objects.
- Keep table and index object identities stable during warm calls; do not implement the logical frontier swap by renaming persistent workspace tables.
- Verify whether PL/pgSQL set-returning results are fully materialized before a second shortest invocation can reset shared session state. Do not rely on this without an integration test.
- Do not use `ON COMMIT DELETE ROWS` as the only cleanup mechanism; start-of-call reset is still required after error and shape changes, and commit-time cleanup adds work.

### Statistics policy

The current filter helpers run `ANALYZE` after loading small filter tables.

- Benchmark small, medium, and large filter cardinalities before selecting a threshold for multi-pair materialized filters.
- Do not reuse stale frontier statistics as if they describe a new traversal. Prefer query shapes and indexes that are robust to the small workspace relations.
- Defer any singleton-specific `ANALYZE` omission to Phase 3, where the endpoint cardinality is actually proven.

### Integration with the pool

- Keep workspace initialization inside database functions initially so it works for all driver-created physical connections.
- If a later `AfterConnect` optimization is justified, compose it with the existing composite-type registration hook instead of replacing the hook.
- Measure the number and memory footprint of persistent temporary relations at the configured minimum and maximum pool sizes.

Tests:

- Pin or acquire one physical pgx connection, record `pg_backend_pid()`, and run two different shortest pairs across separate transactions on that same connection.
- Success, error/rollback, then success with the same recorded backend PID.
- Connected followed by disconnected and the reverse.
- Two shortest expansions in one SQL statement.
- Multiple sequential harness calls in one transaction.
- Concurrent physical connections with different pairs.
- Workspace version mismatch and rebuild.
- Multi-pair fallback remains complete.
- Bidirectional and unidirectional `allShortestPaths` retain their legacy behavior and object set.
- Table/index OIDs and object counts are stable across warm calls, with no repeated `CREATE`, `DROP`, or `CREATE INDEX` execution.
- If two harness calls in one statement can observe a reset before the first set-returning result is fully consumed, retain the current isolated legacy workspace for that shape instead of shipping shared state there.

Expected impact:

- Several milliseconds on warm physical connections is plausible because most captured buffer and temporary-state activity sits inside the harness; elapsed-time attribution still requires the controlled A/B.
- The first call on each physical connection still pays creation cost and must be reported separately.
- The measured A/B result, not the 9 ms diagnostic upper bound, determines whether Phase 2 meets the target.

Phase 2 exit criteria:

- Warm calls execute no repeated table/index `CREATE` or `DROP`, and table/index OIDs remain stable.
- Reset and statistics costs are reported explicitly; the plan does not claim that `TRUNCATE` or `ANALYZE` is catalog-free.
- No state leaks across calls, transactions, failures, or connections.
- No comparable shortest-path median or adequately sampled p95 regression interval is wholly above 1.20.

## Phase 3: Singleton bound-pair shortest-path mode

### Eligibility analysis

Add an explicit optimizer/lowering decision for a singleton shortest pair. Initial eligibility must require:

- `shortestPath`, not `allShortestPaths`.
- Exactly one selected anchor equality on each endpoint ID. Additional conjunctive endpoint-ID equalities are validation predicates, not extra anchors; they must be evaluated before search and may reduce the endpoint relation to zero rows.
- The equality operand is a literal, parameter, or explicitly whitelisted safe cast.
- No previously bound multi-row or correlated endpoint source.
- No `UNWIND`-dependent endpoint expression.
- Supported direction and min/max depth.
- No path or relationship predicate that the specialized harness cannot evaluate.
- No `OR`, `IN`, volatile function, or identifier-free expression merely classified as static.

Additional endpoint label and property predicates are allowed only if a one-row endpoint-validation CTE applies them before invoking the harness.

### SQL and harness design

Deliver this in two increments:

1. Reuse the existing array-parameter control path in `_bidirectional_sp_harness` (`root_ids` and `terminal_ids`). Feed it validated one-element typed arrays, bypass the dynamic pair-filter insertion path, and change this proven-singleton branch to skip `create_traversal_filter_tables`, filter-table `ANALYZE`, and unresolved/resolved pair state because its primer/recursive statements already bind `$1`/`$2` directly. Return at the first valid shortest intersection.
2. If that increment remains above the Phase 3 target, add an additive table-returning singleton SRF such as `bidirectional_sp_single_harness(root_id, terminal_id, ...)`, declared `ROWS 1`, and only then evaluate removing constant root columns from frontier/visited state. If the helper instead returns one composite scalar, omit the invalid `ROWS` clause.

The singleton form should:

- Accept endpoint IDs as typed parameters instead of embedding them in a dynamic text `INSERT`.
- Emit stable outer SQL across different ID values so pgx/PostgreSQL statement caching can work.
- Avoid root/terminal/pair filter tables.
- Avoid root columns in frontier and visited state where they are constant.
- Avoid unresolved/resolved pair bookkeeping.
- Preserve relationship kinds, direction, maximum depth, cycle handling, path edge order, and the same-endpoint error contract.
- Return raw ordered edge IDs; leave full path hydration to the observation boundary.

Materialize and validate each endpoint in an at-most-one-row CTE, choose the anchor equality, and apply every remaining label/property/ID conjunct there. Invoke the harness through a dependent `CROSS JOIN LATERAL`; zero endpoint rows must cause zero harness invocations and no workspace initialization. Put the same-endpoint check at the top of the singleton wrapper/array control path, before `ensure_bsp_core_workspace()`, rather than relying on SQL expression evaluation order outside the function.

### Adjacent projection optimization

Deliver Phase 3L as an adjacent, separately reviewable correctness pull request for shortest-path `length(p)`. Mark `length(path)` as an edge-ID-only observation in requirement analysis and lower an unmaterialized path to `cardinality(raw_ordered_edge_ids)` without hydrating it first. When only a materialized path value is available, lower to `cardinality((p).edges)`. PostgreSQL currently rejects this form as an unknown function, so unsupported forms must keep that explicit error rather than claiming an existing execution fallback.

### Direct recursive-CTE experiment

Prototype a direct recursive CTE only after the reusable singleton harness is measured. It has the highest theoretical upside but is not the default production recommendation because:

- Recursive output breadth-first order is not a semantic guarantee.
- `ORDER BY depth LIMIT 1` may still complete the expansion.
- Carrying path arrays prevents simple global `UNION` deduplication.
- Cyclic, disconnected, and high-fanout graphs can expand catastrophically without global visited state.
- Tie and relationship-uniqueness semantics must match Cypher exactly.

Adopt it only if its warm median is at least 15% below the reusable singleton harness and the lower bound of the p95 regression interval is not above 1.20 across the required scale set. Otherwise retain it as an abandoned experiment, not dormant production code.

Tests:

- Literal and parameter IDs, commuted equality, parentheses, and safe casts.
- Additional label/property predicates.
- Contradictory equalities, null IDs, and missing endpoints.
- A plan/execution invariant that missing, null, or contradictory endpoints invoke the harness zero times and do not initialize the workspace.
- Same valid endpoint with and without incident edges raises the existing error before any core workspace initialization.
- Fallback for `IN`, `OR`, volatile expressions, correlated bindings, directionless unsupported forms, and multiple requested pairs.
- Continued generic behavior for `allShortestPaths`.
- Direct, multi-hop, diamond, cycle, dead-end, disconnected, `*0..0`, and `*0..` graphs.

Phase 3 exit criteria:

- Different ID values produce the same SQL template and different parameter bags.
- The singleton path creates, analyzes, or scans no root, terminal, pair-filter, or pair-resolution tables.
- The upper 95% confidence bound for candidate/clean-baseline median ratio is at most `0.40`, and the separate upper bound for PostgreSQL/Neo4j median ratio is at most `3.0`; on the recorded baseline the latter ceiling is 3.498 ms.
- The generic multi-pair corpus remains complete, with no median or adequately sampled p95 regression interval wholly above `1.20`.
- Phase 3L passes literal/parameter-bound shortest paths, aliases, composed projections, `*0..0`, `*0..`, and null/optional cases without path hydration when only length is observed.

## Phase 4: Consolidated ADCS path materialization

### Increment 1: Hydrate one edge-ID stream per path

Change path projection construction so consecutive raw-ID path components are concatenated before conversion to `edgecomposite[]`.

For P1:

```text
ep0 || ARRAY[e1, e2, e3]
```

must be passed to one `EdgeArrayFromPathIDs` expression instead of four expressions whose composite arrays are concatenated afterward.

For combined P1/P2, the initial target is one correlated hydration expression per projected path variable, evaluated for each result row, reducing nine edge-hydration expressions in the generated plan to two. This is not a claim that all result rows are hydrated by one set-based query.

Preserve dependency order when a path mixes raw-ID and already materialized components. Implement this by coalescing contiguous raw-ID runs and flushing a run when a direct composite component is encountered; do not group all IDs globally and reorder interleaved dependencies.

Likely touchpoints:

- `cypher/models/pgsql/translate/projection.go`.
- `cypher/models/pgsql/model.go` for any richer path-ID expression.
- `cypher/models/pgsql/format/format.go`.
- Renaming/walker tests for any new PostgreSQL AST node.

### Increment 2: Linear ordered-ID materializer

Add an additive helper that accepts the target `graph_id` (or graph-scoped relations), a root ID or root composite, and one ordered edge-ID array. Node and edge IDs are keyed by `(id, graph_id)` and are not schema-enforced as globally unique, so a root-and-edge-only signature is unsafe. The existing `EdgeArrayFromPathIDs` formatter must receive the same graph scope instead of joining the parent `edge` relation by ID alone. The helper should:

- Fetch all required edge composites in one ordered relation using `WITH ORDINALITY`.
- Walk the already ordered edge sequence linearly from the root.
- Hydrate the derived node sequence once.
- Preserve directionless traversal, self-loop, repeated-node, and relationship-uniqueness semantics.
- Return a `pathcomposite` with exact node and relationship order.
- Remain graph-scoped during edge and node lookup.

The translator already knows segment order, so the common read-expansion path should not repeatedly search all remaining edges for the next connected edge. Keep `ordered_edges_to_path` as the fallback for legacy, mixed, mutation-returning, or otherwise unproven expressions.

### Increment 3: Carry node IDs when profitable

For observed read-only expansions, carry an ordered node-ID sequence beside the ordered edge-ID sequence when doing so is cheaper than reconstruction. Do not force this extra array into endpoint-only queries or unobserved paths.

Consider set-based hydration across output rows only after the one-path-at-a-time design is measured. A batched relation keyed by result-row ID can avoid repeated lookup of shared nodes and edges, but it is a larger planner change and must preserve duplicate rows.

Tests:

- Exact P1 lengths and ordered node/relationship sequences.
- P2 and combined paths.
- Zero-edge variable segment followed by fixed edges.
- A complete empty edge-ID stream produces the correct one-node/zero-relationship path; an empty array is distinguished from a `NULL` path.
- Inbound, outbound, directionless, and mixed-direction paths.
- Self-loops, cycles, and repeated nodes are preserved.
- Relationship reuse within one matched path is rejected; the same relationship may appear in distinct result rows or independent pattern paths where Cypher permits it.
- Optional/null paths.
- Multiple paths reaching the same endpoint.
- Path functions over the materialized result.
- Equivalence between the linear materializer and generic fallback.
- Mutation-returning paths continue using a safe representation that can observe newly written values.
- A multi-partition plan/semantic test with a decoy graph containing colliding explicitly assigned node and edge IDs proves that every hydration lookup is constrained to the target graph.
- Isolated helper scaling at 8, 16, 32, and 64 ordered edges; after warmup, the upper 95% confidence bound for the 64/32 server-execution ratio is at most 2.5, guarding against reintroducing quadratic reconstruction.

Phase 4 exit criteria:

- Increment 1 makes P1 emit one correlated edge-hydration expression rather than four and combined P1/P2 emit one per projected path variable rather than nine total; it is gated on structural reduction and semantic equivalence, not the 50-75% materializer target by itself.
- After the linear materializer and any independently justified Increment 3, the upper 95% confidence bound for candidate/clean-baseline paired path-tax ratio is at most `0.50`. Define that tax for each matched round as `P1 path server execution - P1 endpoint server execution` on the same fixture and connection protocol, then summarize the distribution of paired deltas; do not subtract independently aggregated medians.
- Exact path semantics remain backend-equivalent.

## Phase 5: Consume fixed suffixes once

### Current problem

Expansion suffix pushdown builds a correlated `EXISTS` over the fixed suffix, while the following traversal steps still join the same suffix to return bindings. This preserves semantics but duplicates edge/node lookup and expands the join tree the PostgreSQL planner must consider.

Merely moving the existing `EXISTS` expression into a recursive `satisfied` column does not remove duplication.

### Phase 5A: Short-term eligibility gate

Make suffix-pushdown eligibility consumption-aware:

- Treat the current `expansionSuffixTerminalSatisfaction` `EXISTS` expression only as a permissive supplemental prefilter. It may reject endpoints cheaply, but it is not the relation that produces suffix rows.
- Do not classify a suffix as existential merely because its variables are anonymous or not projected. Multiple suffix matches still multiply rows and affect later aggregation. Elide normal suffix-row production only when surrounding semantics are formally cardinality-insensitive, such as an explicit existence context proven by optimizer tests.
- Skip the supplemental `EXISTS` when the fixed suffix is the immediate continuation and normal suffix rows must still be produced. Retain it as a recorded exception only when the upper 95% confidence bound for prefilter/no-prefilter warm-median ratio is at most `0.90` on the sparse-decoy tier and no comparable case has a regression interval wholly above `1.20`.
- Record the choice and reason in lowering metadata.
- Do not remove suffix pushdown globally; high fanout with sparse valid suffixes may benefit from the supplemental prefilter.

The first gate should use deterministic query-shape information rather than pretending the compiler has database cardinality statistics. The scale matrix will determine whether later runtime/statistical costing is justified.

### Phase 5B: Consumed suffix relation

Lower an eligible fixed suffix into one graph-scoped anchored lateral relation that initially returns conservative complete bindings and one row per valid suffix path, without deduplication:

- Suffix start ID.
- Ordered suffix edge IDs.
- The complete suffix node/edge bindings required by ordinary continuation, plus ordered suffix edge IDs for path construction.
- Any predicate satisfaction needed by the variable expansion.

Both terminal satisfaction and final projection must consume that one multiplicity-preserving relation. Mark the original suffix steps consumed so the normal traversal renderer does not emit them again.

Do not reuse the current `EXISTS` AST as the consumed relation. Factor the ordinary traversal lowering so the produced relation preserves every node/relationship predicate, bound-variable constraint, graph constraint, ordering rule, null behavior, and whole-pattern relationship-uniqueness rule. In particular, each suffix edge must be absent from the variable expansion's edge-ID array, and fixed suffix edges must be pairwise distinct where the normal lowering requires it. These omissions are acceptable false positives in a supplemental prefilter but are incorrect in the result-producing relation.

Correlate the relation with the complete expansion result row, including its ordered edge-ID array and every outer binding referenced by suffix predicates, not only the expansion terminal ID. Two expansion paths may reach the same terminal while only one conflicts with a candidate suffix relationship. An endpoint-keyed CTE may precompute candidate suffixes, but per-expansion-path relationship-uniqueness filtering must occur before producing rows. Phase 6B may later replace conservative complete suffix bindings with scalar fields after its stage-sensitive analysis proves them sufficient.

Prefer an endpoint-anchored `LATERAL` relation over materializing every matching suffix in the graph. A globally materialized suffix can trade duplicate probes for an unbounded full-graph computation.

Likely touchpoints:

- `cypher/models/pgsql/optimize/lowering_plan.go`.
- `cypher/models/pgsql/optimize/lowering.go`.
- `cypher/models/pgsql/translate/expansion.go`.
- `cypher/models/pgsql/translate/traversal.go`.
- Optimizer and translation safety tests.

Tests:

- Suffix observed as a path and as endpoint IDs.
- A suffix in a formally explicit pattern-existence/cardinality-insensitive context where pushdown remains beneficial.
- Zero, one, and multiple suffix matches per expansion endpoint.
- Decoys at every suffix hop.
- Bound suffix endpoints and predicates.
- Duplicate paths and endpoint multiplicity.
- Pairwise fixed-edge inequality and suffix-edge exclusion from the variable expansion path.
- Two expansion paths reach the same terminal and only the path that already contains the candidate suffix edge rejects that suffix.
- `OPTIONAL MATCH` fallback and null preservation.
- Directionless suffixes retain the generic fallback.

Phase 5 exit criteria:

- Phase 5A removes the duplicate supplemental `EXISTS` for observed ADCS P1 unless a sparse-decoy A/B records a retained exception; all normal suffix rows are still produced.
- A shipped Phase 5B emits exactly one result-producing suffix traversal, with one output row per valid suffix path and no boolean or deduplicated substitute.
- Four endpoint rows and four P1 paths remain exact.
- Sparse-valid-suffix scale cases have no median or adequately sampled p95 regression interval wholly above `1.20`.
- Phase 5B ships only if the upper 95% confidence bound for its Phase-5A-relative warm endpoint median ratio is at most `0.90`, or the equivalent bound for repeated prepared-statement planning median is at most `0.85`, while meeting the regression gate. Otherwise retain Phase 5A and document the rejected experiment.

## Phase 6: Field-sensitive projection and ID-only traversal state

### Phase 6A: Requirement analysis

Current liveness is symbol-level: `id(ca)` keeps `ca` live as if the full node were required. Extend source-reference analysis to record required fields per binding and per frame/use location, including last-use information. A single query-wide binding bitset is insufficient: kinds or properties may be required to validate a pattern endpoint, while only its ID remains live after that validation.

Use a requirement lattice or bitset that can express:

- Entity ID.
- Node kinds.
- Properties.
- Full node or relationship composite.
- Ordered path edge IDs.
- Fully observed path.

Examples:

- `id(n)` requires ID only.
- `labels(n)` requires kinds.
- `n.property` requires properties and sufficient identity/null semantics.
- Returning `n` requires a full node.
- Relationship uniqueness requires edge IDs, not edge properties.
- Returning `p` requires ordered path IDs and final hydration.

Phase 6A adds the requirement lattice and lowering metadata without changing generated SQL. Phase 6B consumes it; Phase 5B deliberately remains conservative so suffix fusion does not depend on a scalar-binding representation that does not exist yet.

Treat pattern labels, property predicates, endpoint-existence joins, bound-variable constraints, and relationship-uniqueness arrays as internal staged uses even when the final source expression is only `id(binding)`. Drop a field only after its last validating use, never merely because it is absent from the final projection.

### Phase 6B: Staged lowering

1. Apply ID-only lowering to fixed-suffix terminal nodes used only by `id(...)`.
2. Apply it to variable expansion roots/endpoints after their last property/kind use.
3. Add combined ID+kinds or ID+properties shapes only if an isolated A/B lowers warm endpoint latency or repeated prepared planning median by at least 10%.

Prefer an explicit scalar binding type analogous to `PathEdge` over sparse node composites whose null fields can be mistaken for real values. The binding and frame system must know when later use requires hydration.

### Semantic constraints

- Do not remove endpoint node joins merely by assuming all edges have valid endpoints; preserve current existence semantics unless schema constraints prove equivalence.
- Preserve null behavior through `WITH`, aliases, optional matches, aggregation, ordering, and property access.
- Keep edge-ID arrays needed for path relationship uniqueness even in endpoint-only projection.
- Rehydrate at most once when a later query part upgrades an ID-only binding to a full entity.

Likely touchpoints:

- `cypher/models/pgsql/optimize/source_references.go`.
- `cypher/models/pgsql/optimize/lowering_plan.go`.
- `cypher/models/pgsql/translate/model.go`.
- `cypher/models/pgsql/translate/projection.go`.
- `cypher/models/pgsql/translate/traversal.go`.
- `cypher/models/pgsql/translate/expansion.go`.

Tests:

- ID-only, labels-only, property-only, and full-entity projections.
- Mixed uses of the same binding.
- Uses before and after `WITH` aliases.
- Optional/null bindings.
- Ordering/grouping by a field not present in the final projection.
- ID-only final projections whose pattern labels or property predicates reject wrong-label/property decoys before kinds/properties are dropped.
- Path uniqueness without path observation.
- Endpoint query contains no path materializer, edge-property hydration, or node properties after their last required use.

Expected impact:

- Executor gains on the tiny endpoint fixture may be only a few tenths of a millisecond.
- The primary immediate value is reducing planner work and intermediate row width.
- Gains should grow with fanout and larger property payloads.

Phase 6 exit criteria:

- Phase 6A requirement metadata correctly distinguishes ID, kinds, properties, relationship-uniqueness IDs, ordered path IDs, and full entity/path observation without changing SQL goldens.
- ADCS endpoint output carries scalar IDs through the suffix wherever full entities are not required.
- Exact duplicate endpoint rows are preserved.
- The endpoint SQL byte count or stable logical plan-node count falls by at least 10%, and the upper bound of the warm endpoint candidate/immediate-predecessor median-ratio interval is at most `1.05`. Evaluate the cumulative clean-baseline endpoint target in Phase 9, after deciding whether Phase 7's trigger fires.

## Phase 7: Compilation and PostgreSQL plan caching

This is a conditional shipping phase after value-sensitive shortest SQL and verbose ADCS SQL have been corrected. Run its diagnostics after Phase 6B; ship cache/policy changes if the warm ADCS endpoint remains above 2.058 ms, client compilation is at least 10% of warm end-to-end latency, or repeated prepared planning/custom-plan behavior is at least 15% of warm latency.

### DAWGS compilation cache

Add a bounded concurrent cache in stages:

1. Cache parsed/optimized query structures, copying before any mutable translation step.
2. Cache complete SQL templates and parameter mappings for proven value-insensitive translations.

The full-template cache key must include at least:

- Raw Cypher text or a canonical query fingerprint.
- Target graph ID or graph relation generation.
- Kind/schema generation because translated SQL embeds kind IDs.
- Parameter type signature where it changes SQL casts or shape.
- Translator/optimizer version or an equivalent invalidation generation.

Requirements:

- Bounded memory and deterministic eviction.
- Concurrent request safety.
- Invalidation after schema/kind changes.
- Cache hit/miss/eviction metrics in benchmark diagnostics.
- No parameter values in the key once the singleton shortest lowering emits stable typed SQL.
- No reuse of mutable AST or frame state across requests.

### PostgreSQL generic-plan experiment

Pgx already uses statement caching, but PostgreSQL may make custom-plan decisions per physical connection. On one pinned physical connection, compare repeated prepared executions under `plan_cache_mode=auto`, `force_custom_plan`, and `force_generic_plan` for the stable ADCS templates.

- Do not enable it globally by default.
- Test skewed property predicates, empty/large lists, and different endpoint selectivities.
- Compare first execution, executions 2-5, and steady state on each physical connection.
- Record backend PID, prepared-statement identity, plan mode, SQL template hash, client compilation time, and raw samples so standalone `EXPLAIN` planning time is not mistaken for warmed request planning cost.
- Prefer query-local or connection-policy changes only if the general corpus does not regress.

Tests:

- Capacity-plus-one insertion proves bounded deterministic eviction and hit/miss/eviction metrics.
- Concurrent hits, misses, and invalidations pass `go test -race` without duplicate mutable state.
- Graph/relation generation and kind/schema generation changes invalidate old entries.
- Different parameter type signatures do not alias one template when casts or SQL shape differ.
- Mutating a translated copy cannot affect a later cache hit, proving AST/frame isolation.
- Pinned-connection integration coverage exercises `auto`, forced-custom, and forced-generic plan modes across first and steady-state executions.

Phase 7 exit criteria:

- A shipped compilation cache has an upper 95% confidence bound of at most `0.92` for cache-hit warm candidate/immediate-predecessor median ratio, or a lower 95% confidence bound of at least 0.25 ms for absolute time saved, without semantic drift.
- Cache invalidation is deterministic and tested.
- Any generic-plan policy passes the complete corpus and selectivity matrix.
- The overall endpoint gate is evaluated in Phase 9 and does not depend on an unsafe global planner setting.

## Phase 8: Share identical ADCS expansions where profitable

The combined ADCS query computes the same anchored `MemberOf*` closure for P1 and P2. After path materialization, suffix reuse, and field liveness are stable, consider sharing exact duplicate expansion signatures.

An expansion signature must include:

- Anchor binding and graph.
- Direction.
- Relationship kinds.
- Minimum and maximum depth.
- Node/relationship predicates.
- Relationship uniqueness requirements.
- Required projected state.

Materialize or reuse only exact multi-use expansions. Forced materialization of a large single-use closure can regress performance.

The two branches must still independently join their suffixes and preserve the P1 x P2 Cartesian multiplicity. Sharing the closure must not deduplicate output paths or merge branch-local predicates.

Tests:

- Negative optimizer cases vary each signature field independently: anchor/graph, direction, relationship kinds, min/max depth, node predicate, relationship predicate, uniqueness requirement, and projected state. Every mismatch must prevent sharing.
- The positive combined P1/P2 case computes the closure once while preserving exact eight-row Cartesian multiplicity and every ordered path pair.
- Single-use and high-cardinality closures retain the unshared plan.

Phase 8 is not required to close the standalone P1 gaps. Trigger it only if the combined-path case remains above 2.5x Neo4j after Phase 6B or profiling attributes at least 15% of its server execution time or shared-buffer hits to duplicate closure computation. Ship it only if the closure is computed once, the upper 95% confidence bound for combined warm candidate/immediate-predecessor median ratio is at most `0.90`, and no comparable case has a median or adequately sampled p95 regression interval wholly above `1.20`; otherwise document and remove the experiment.

## Phase 9: Validation and rollout

Phase 9 applies the following test architecture and performance protocol to every completed workstream, publishes the final comparison, evaluates the Phase 7-8 triggers, and runs any triggered conditional work before final acceptance.

### Optimizer tests

Add decision and fallback coverage for:

- Singleton shortest-path eligibility.
- Field-sensitive requirements.
- Consolidated path materialization.
- Suffix gating and suffix reuse.
- Exact duplicate expansion recognition.

Every decision must appear in lowering metadata with an eligibility or fallback reason that can be inspected in benchmark output.

### Translation and golden tests

Assert stable structural invariants rather than full planner costs:

- Bound-pair fast path uses typed endpoint parameters and stable SQL.
- LIMIT appears both inside the harness arguments and on the containing SELECT.
- Warm-workspace functions do not contain unconditional per-call table/index creation.
- Endpoint ADCS contains no `ordered_edges_to_path` or eager edge properties.
- P1 path contains one consolidated edge-ID hydration expression.
- Reused suffix SQL contains one suffix traversal.
- Unsupported shapes retain the generic SQL.

Update the source translation cases and generated artifacts using the existing repository workflow. Because this work changes translation and query semantics, add source-template variants rather than relying only on focused inline cases:

- `integration/testdata/templates/pattern_shapes.json`: bound shortest, observed suffix, multiplicity, and fallback shapes.
- `integration/testdata/templates/parameter_shapes.json`: literal, parameter, commuted, null, missing, and contradictory endpoint forms.
- `integration/testdata/templates/scalar_shapes.json`: endpoint `id(...)`, `length(path)`, and full-path observation transitions.
- `integration/testdata/templates/optional_shapes.json`: null path, optional suffix, and later rehydration behavior.

Regenerate and review their owned artifacts together with focused cases in `integration/testdata/cases`; do not edit only generated output.

### Backend-equivalent integration tests

Put semantic assertions in `integration/testdata/cases` or templates without driver-specific skips or expected values. The PostgreSQL fast path and Neo4j query must return equivalent stable fixture values, ordering where specified, multiplicity, and errors. Backend-generated internal IDs are validated within a backend for path uniqueness but are not compared numerically across engines.

### PostgreSQL-scoped integration tests

Use driver-scoped tests for:

- Workspace reuse and failure lifecycle.
- Plan invariants.
- Cold/warm physical-connection behavior.
- Generic-plan experiments.
- SQL-function fallback equivalence.
- Schema up/down round trips, function signatures, and fresh-install versus upgraded-install equivalence for every added or changed SQL helper.

Do not assert brittle cost numbers or entire plan text. Assert stable properties such as one hydration subplan, absence of duplicate suffix work, and no repeated warm DDL. Test indexed endpoint access only on a sufficiently large analyzed fixture where that choice is expected.

### Automated negative-control tests

No mutation-testing runner is currently configured. For each semantic hazard below, temporarily make the named deliberate code mutation while developing the adjacent test and verify that the test fails; the committed deliverable is the ordinary automated regression test, not a claim of a repository-wide mutation score:

- Wrong shortest direction.
- Removed relationship kind or depth bound.
- Incorrectly applying singleton logic to `allShortestPaths` or multiple pairs.
- Removed same-endpoint guard.
- Endpoint deduplication.
- Changing `*0..` to `*1..`.
- Reordered path edges or nodes.
- Omitted suffix hop.
- Removed relationship-uniqueness checks.
- Eager or missing final hydration.

### Performance protocol

For every phase that changes runtime behavior:

1. Load a fresh fixture.
2. Through the PostgreSQL pool and outside a transaction, run `VACUUM (ANALYZE)`; abort the round on failure.
3. Run an untimed exact-result preflight against both backends.
4. Capture at least five independent rounds with 30-50 timed samples per case and backend, alternating backend order.
5. Run the same untimed exact-result check after each timed block.
6. Capture raw samples, median, p95, maximum, client compilation time, plan mode, server plan/execution time where measured, buffer activity, SQL, plan shape, and lowering metadata.
7. Compare the complete clean corpus, not only the target cases.
8. Run the deterministic pairwise scale set and its timeouts; run the largest scale tier separately.

For workspace measurements, configure `MaxConns=1` or explicitly acquire and retain one pgx connection. Record `pg_backend_pid()` before every block. Define a cold workspace sample as the first target query on an already-open fresh PostgreSQL session and warm samples as subsequent calls on that same session. Report connection establishment separately. Also label and independently reset, retain, or prewarm each cache layer: DAWGS compilation cache, pgx statement cache, PostgreSQL prepared/generic-plan state, PostgreSQL data cache, and the `pg_temp` workspace. A generic pool warmup is not evidence that two samples used the same session.

Make the statistical gate executable in GraphBench rather than leaving it as report prose:

- Add versioned raw-sample JSON and a comparison mode (plus a Make target such as `perf_gate`) accepting baseline artifact, candidate artifact, seed, confidence level, and `-regression-threshold=0.20`.
- For medians, pair baseline and candidate round medians by matched environment/fixture blocks, keep the blocks independent, and compute a seeded 95% bootstrap confidence interval by resampling those blocks. Fail a comparable-corpus case when the interval's lower bound exceeds `1.20`.
- For p95, bootstrap raw samples stratified by round. Apply the same lower-bound-above-`1.20` failure once at least 150 timed observations exist per side; otherwise report p95 as directional and require another round rather than declaring it passed.
- For target cases, calculate two separate upper 95% confidence bounds: candidate/clean-baseline median ratio must be at most `0.40` for shortest, `0.60` for endpoint IDs, and `0.70` for P1 path; PostgreSQL/Neo4j median ratio must independently be at most `3.0`, `2.0`, and `2.5`, respectively. Report both point estimates and intervals.
- Permit a regression exception only when a repository maintainer approves a recorded case name, magnitude, confidence interval, cause, and follow-up/rollback decision in the benchmark report.

Every phase artifact records two references: the immediate predecessor artifact for isolated attribution and the immutable clean Phase 0 artifact based on commit `05e70a18d7c6` for cumulative goals. Store artifact IDs/checksums in the comparison output. Phase-specific shipping gates compare against the immediate predecessor unless they explicitly say clean baseline; Definition of Done always uses the clean baseline.

Scale results are gates, not informational appendices. Every normal and largest-tier case must complete within its dataset-configured timeout. Apply the same median/p95 interval rule to comparable scale cases, and fail if per-session temporary bytes or measured workspace memory has a regression interval wholly above `1.20` without an approved exception. Phase 4 additionally enforces its 64/32 materializer scaling-ratio gate; no general asymptotic slope is inferred across unrelated graph shapes.

The primary gate is warm serial latency because that matches the original benchmark. Before rollout, add a concurrent run at representative pool occupancy and record throughput, latency, pool wait time, PostgreSQL backend count, and per-session temporary-space footprint. It must pass the same 20% regression rule so persistent workspaces cannot trade single-client latency for an unreported throughput or memory regression.

Report each phase as a before/after table:

| Metric | Baseline | Candidate | Change |
|---|---:|---:|---:|
| End-to-end median | | | |
| End-to-end p95 | | | |
| PostgreSQL planning | | | |
| PostgreSQL execution | | | |
| Shared buffers | | | |
| Local/temp buffers | | | |
| Cold first call | | | |
| Warm call | | | |
| Client compilation | | | |
| PostgreSQL plan mode | | | |
| SQL bytes | | | |
| Hydration subplans | | | |

Do not combine unrelated optimizations in the first A/B for a phase. For example, measure consolidated edge hydration before also replacing `ordered_edges_to_path`.

### Required repository workflow

For every implementation pull request:

1. Run `make format` after code edits.
2. Run `make test` for unit, optimizer, translation, formatter, and benchmark-runner tests.
3. When translation fixtures change, update them with `make test_update`, inspect the source and generated diffs, and add a CI stale-artifact check that runs the update workflow and fails if it creates a diff.
4. Run `CONNECTION_STRING="postgresql://..." make test_all` against PostgreSQL.
5. Run `CONNECTION_STRING="neo4j://..." make test_all` separately against Neo4j. The scheme selects the backend and the other backend's scoped tests must skip themselves.
6. For SQL schema changes, run the PostgreSQL schema up/down/up round-trip and function-signature tests on a fresh database as well as an upgrade-shaped database.

Do not put PostgreSQL-only expectations into shared integration cases. Put plan, workspace, and schema assertions in clearly PostgreSQL-scoped tests while keeping source cases/templates backend-equivalent.

## Proposed pull-request breakdown

1. **Benchmark correctness and isolated fixtures**
   - Exact result assertions, isolated P1 cases, bound-ID shortest case, statistics refresh, cold/warm labels.
2. **Affected-query graph scoping**
   - Target-graph predicates/relations across ordinary traversal, dynamic shortest fragments, path helpers, and fallbacks; colliding-ID test.
3. **Shortest LIMIT and endpoint lookup**
   - SQL limit plus internal path limit, indexed lateral hydration, plan invariant.
4. **Reusable shortest workspace**
   - Core/generic ensure functions, reset/version handling, generated-fragment naming context, lifecycle tests, cold/warm benchmark.
5. **Singleton shortest mode**
   - Eligibility decision, typed IDs, lean pair handling, stable SQL.
6. **Shortest path length observation (Phase 3L)**
   - Edge-ID-only `length(path)` lowering, materialized-path case, aliases/null/zero-hop coverage.
7. **Consolidated graph-scoped path-ID hydration**
   - Contiguous ID-run coalescing, P1 four-to-one and combined nine-to-two assertions.
8. **Linear ordered-ID path materializer**
   - Graph-scoped additive SQL helper, read-expansion lowering, fallback equivalence.
9. **Suffix prefilter gate (Phase 5A)**
   - Remove redundant supplemental probes by shape while preserving all result-producing suffix rows.
10. **Field-requirement analysis (Phase 6A)**
    - Add stage-sensitive requirement/last-use metadata without changing generated SQL.
11. **Consumed suffix relation (Phase 5B)**
    - Reuse ordinary traversal semantics in one anchored, multiplicity-preserving relation.
12. **ID-only field-sensitive projection (Phase 6B)**
    - Carry scalar identity only where Phase 6A proves it sufficient.
13. **Conditional compilation cache and plan-policy experiment**
    - Bounded cache, invalidation, generic-plan A/B.
14. **Conditional shared ADCS expansion follow-up**
    - Exact-signature reuse for combined P1/P2 only when the Phase 8 trigger fires.

Each pull request must include its semantic tests, translation/plan invariant where applicable, before/after benchmark artifact, and documentation update. Do not defer coverage to a later performance pull request.

## Code ownership map

| Concern | Primary locations |
|---|---|
| Target-graph scoping | `cypher/models/pgsql/translate/translator.go`, `traversal.go`, `expansion.go`, `projection.go`, and affected SQL path helpers |
| Optimizer decisions and liveness | `cypher/models/pgsql/optimize/lowering.go`, `lowering_plan.go`, `source_references.go` |
| Shortest strategy and suffix application | `cypher/models/pgsql/translate/traversal.go`, `expansion.go`, `pattern.go` |
| LIMIT lowering | `cypher/models/pgsql/translate/projection.go`, `limit_pushdown_test.go` |
| Path projection and materialization | `cypher/models/pgsql/translate/projection.go`, `path_functions.go`, `cypher/models/pgsql/format/format.go` |
| PostgreSQL AST types | `cypher/models/pgsql/model.go`, walkers and renamers |
| SQL functions and temporary workspace | `drivers/pg/query/sql/schema_up.sql`, `schema_down.sql` |
| SQL function identifiers | `cypher/models/pgsql/functions.go` |
| Driver compilation/cache boundary | `drivers/pg/transaction.go`, `driver.go`, `pg.go` |
| Optimizer/translation safety | `cypher/models/pgsql/optimize/*_test.go`, `cypher/models/pgsql/translate/*_test.go` |
| Backend-equivalent semantics | `integration/testdata/cases/optimizer_inline.json`, `integration/testdata/cases/shortest_paths_inline.json`, and focused new cases |
| Scale cases | `benchmark/testdata/scale/cases/shortest_paths.json`, `traversal.json` |
| Benchmark runner and plan gates | `cmd/graphbench`, especially `measure.go` and `postgresql_plan_invariants_integration_test.go` |

## Risk register

| Risk | Consequence | Mitigation |
|---|---|---|
| Reused temp state leaks between calls | Incorrect paths or missing results | Start-of-call reset, rollback tests, version marker, concurrent-session tests |
| Two harness calls interfere in one statement | Corrupt or truncated results | Prove set-return materialization; retain isolated fallback if unsafe |
| Persistent temp relations inflate pool footprint | Memory/catalog pressure | Lazy creation, measure min/max pool footprint, and let only the proven Phase 3 singleton path skip pair state |
| Warm reset takes strong locks or rewrites temp storage | Tail-latency regression | Benchmark multi-table `TRUNCATE` versus indexed `DELETE`; record locks, relfilenodes, and p95 |
| Singleton eligibility is too broad | Wrong results for correlated or multi-pair queries | Strict operand whitelist and comprehensive fallback tests |
| SQL LIMIT changes which row survives | Semantic drift | Retain current transparent-tail safety analysis and negative tests |
| Consolidating path pieces reorders dependencies | Incorrect path order | Coalesce only contiguous raw-ID runs and test mixed components |
| Hydration omits graph scope | Cross-graph node/edge leakage when IDs collide | Pass `graph_id` or scoped relations through every helper and test a colliding decoy partition |
| Linear path materializer mishandles directionless paths | Wrong node ordering | Generic fallback plus equivalence tests for every direction |
| Removing suffix `EXISTS` increases high-decoy work | Fanout regression | Shape gate, decoy-density scale matrix, reusable anchored suffix design |
| Consumed suffix loses multiplicity or relationship uniqueness | Wrong row counts or invalid paths | Produce one row per suffix path by factoring ordinary traversal lowering; never promote the permissive `EXISTS` AST to the producer |
| Reusable suffix relation materializes the whole graph | Large memory/runtime regression | Anchor with `LATERAL`; avoid unbounded global suffix CTEs |
| ID-only binding drops needed fields | Late property/label failures | Requirement lattice, staged rollout, rehydration tests across `WITH`/optional scopes |
| Cached translation uses stale kind/graph metadata | Incorrect SQL | Schema/kind generation in cache key and deterministic invalidation |
| Forced generic plan regresses skewed predicates | Broad query regression | A/B only; no global default without full selectivity matrix |
| Benchmark bloat/stale statistics masks results | False conclusions | Fresh fixture/database and `VACUUM (ANALYZE)` before measurement |

## Rollout and rollback

- Add new SQL functions and overloads before the translator emits calls to them.
- Keep existing generic functions during at least one compatibility window.
- Make optimizer eligibility conservative so disabling a decision returns to the known generic path.
- Expose lowering decisions and fallback reasons in GraphBench artifacts so production-like plans can be audited.
- Roll out workspace reuse and singleton search separately; a singleton bug must not require reverting the generic workspace improvement.
- Roll out the linear path materializer only for read expansions first. Mutation-returning paths stay on the generic composite-aware path until explicitly proven safe.
- Treat any global connection or PostgreSQL planner setting as a separate opt-in experiment with an immediate configuration rollback.

## Definition of done

The rework is complete when all of the following hold:

- Exact backend-equivalent semantics pass for the isolated and combined shortest/ADCS cases.
- For bound-pair shortest, the candidate/clean-baseline median-ratio upper bound is at most `0.40` and the separate PostgreSQL/Neo4j ratio upper bound is at most `3.0`; the recorded absolute ratio ceiling is 3.498 ms.
- For ADCS endpoint IDs, the candidate/clean-baseline median-ratio upper bound is at most `0.60` and the separate PostgreSQL/Neo4j ratio upper bound is at most `2.0`; the recorded absolute ratio ceiling is 2.058 ms.
- For ADCS P1 path, the candidate/clean-baseline median-ratio upper bound is at most `0.70` and the separate PostgreSQL/Neo4j ratio upper bound is at most `2.5`; the recorded absolute ratio ceiling is 2.795 ms.
- `length(shortestPath(...))` succeeds through edge-ID-only observation for the Phase 3L corpus and does not force path hydration.
- Warm shortest calls perform no repeated table/index creation and no state leaks are observable.
- P1 path uses one graph-scoped correlated edge-hydration expression per projected path variable, combined P1/P2 uses two total expressions, and the upper confidence bound for candidate/clean-baseline paired server path-tax ratio is at most `0.50`.
- A colliding-ID decoy partition cannot affect path hydration or suffix results.
- Observed fixed suffixes are not traversed twice unless lowering metadata records a sparse-decoy exception that met Phase 5A's A/B rule; all variants preserve one row per valid suffix path.
- Endpoint-only SQL carries no unnecessary full path or edge-property hydration.
- Every Phase 7/8 trigger is evaluated; each triggered phase either meets its numeric exit gate or has a published rejection record and no shipped code.
- Cold-session, connection-establishment, warm, scale, and concurrent-pool results plus the complete-corpus regression comparison are published with raw samples, checksums, environment manifest, and seeded statistical settings.
- Every configured scale case completes within its timeout, passes the same latency/temp-footprint regression gate, and the linear materializer passes its 64/32 ratio gate.
- No median or adequately sampled p95 has a 95% regression interval wholly above 1.20, except a fully recorded maintainer-approved exception.
- `README.md` and benchmark documentation describe any new workflow, configuration, or required statistics step.
- `make format`, `make test`, fixture/golden regeneration, and separate PostgreSQL and Neo4j `make test_all` runs pass.
- Schema down migrations and round trips, translation goldens, optimizer tests, backend-equivalent integration tests, PostgreSQL-scoped tests, and benchmark artifacts are updated with the implementation.

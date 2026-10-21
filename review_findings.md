# Translator review and remediation ledger

## Current state

- Review date: 2026-10-21.
- Reviewed checkout: `750ea96107f7889733b67001922a090f4c6b4279`.
- Scope: merged V2 history, starting with `8ffc4418`, through the reviewed checkout, including the later V2 builder port at `403c9289`.
- Result: **not qualified; five high-severity findings remain open (F1–F4, F7); F5 and F6 closed. No critical findings confirmed**.
- Implementation: stage 1 complete (F5, F6 closed); remaining stages not started. Overall qualification still fails on the two baseline adjacency permission errors.
- Review method: parallel subdirectory reviews plus focused PostgreSQL reproductions.
- Test backend: PostgreSQL 18.6, using `database.connection` from `/home/zinic/work/bhe-dawgs/integration-config-local.json`.
- The review left tracked source files unchanged. This ledger is the requested follow-up document.

Finding IDs are permanent. Source line numbers refer to the reviewed checkout and may move as repairs land. The findings and reproductions below are self-contained; local `.coverage` artifacts supplement them but are ignored by Git and are not the durable record.

| ID | Severity | Finding | Introduced by | Repair stage | Status | Owner |
| --- | --- | --- | --- | --- | --- | --- |
| F1 | High | Carried nodes acquire the wrong representation | `b78ed8ba` | 3 | Open | Unassigned |
| F2 | High | Inline property maps lose required node properties | `b78ed8ba` | 3 | Open | Unassigned |
| F3 | High | Suffix-reverse execution drops correlated predicates | `b78ed8ba` | 5 | Open | Unassigned |
| F4 | High | Expansions lose an already-bound terminal's identity | `2a043e64` | 2 | Open | Unassigned |
| F5 | High | Traversal-count optimization discards additional source labels | `9fe77970` | 1 | Closed | Codex |
| F6 | High | Traversal-count optimization treats outgoing paths as cycles | `9fe77970` | 1 | Closed | Codex |
| F7 | High | Leading `UNWIND` / `CREATE` followed by `WITH` generates self-referencing SQL | `2a043e64` | 4 | Open | Unassigned |

## Findings

### F1 — Carried nodes acquire the wrong representation

**Severity:** High. **Location:** [translate/traversal.go](cypher/models/pgsql/translate/traversal.go), reviewed lines 910–912, `applyIDOnlyNodeProjection`.

The translator marks an existing binding `IDOnly` without converting the composite already materialized by an earlier frame. Subsequent joins and projections then treat that composite as a scalar ID.

Reproduction, with a single `a-[:R]->b` edge:

```cypher
MATCH (a) WITH a
MATCH (a)-[:R*1..2]->(b)
RETURN id(a), id(b)
```

Expected: one row containing two integer IDs. Actual: PostgreSQL `42883`, `operator does not exist: bigint = nodecomposite`.

The fixed-hop equivalent also fails semantically:

```cypher
MATCH (a) WITH a
MATCH (a)-[:R]->(b)
RETURN id(a), id(b)
```

It returns a node composite for `id(a)` and an integer for `id(b)`. Both forms return the correct integer values with optimization disabled.

### F2 — Inline property maps lose required node properties

**Severity:** High. **Location:** [optimize/source_references.go](cypher/models/pgsql/optimize/source_references.go), reviewed lines 179–181, `fieldRequirementCollector`.

Field-requirement collection ignores variable references while inside a pattern. That also ignores expression consumers inside inline property maps, so an intermediate node needed for `m.x` can be reduced to its ID.

```cypher
MATCH (s)-[:R]->(m)-[:R]->(e {x: m.x})
RETURN id(e)
```

The relationship-map variant is affected too:

```cypher
MATCH (s)-[:R]->(m)-[:R {x: m.x}]->(e)
RETURN id(e)
```

Expected: evaluate the property comparison and return matching endpoint IDs. Actual: PostgreSQL `42809`, `column notation .properties applied to type bigint, which is not a composite type`. The error occurs during SQL type resolution even without matching rows. Both optimized and optimizer-disabled translation fail in the reviewed checkout.

### F3 — Suffix-reverse execution drops correlated predicates

**Severity:** High. **Location:** [translate/expansion_suffix_seeded.go](cypher/models/pgsql/translate/expansion_suffix_seeded.go), reviewed lines 889 and 894–899, `buildFixedSuffixCTEWithOptions`.

The suffix builder retains local predicates but discards the nonlocal remainder. A predicate that references a scalar carried through `WITH` therefore disappears from the reverse candidate. This affects enabled suffix-reverse/topology policies using this lowering; the database reproduction explicitly selected `ToolOptions{EnableExpansionSuffixReverseGuard: true}`.

```cypher
MATCH (root:ExpansionRoot)
WHERE root.root_key = $root_key
WITH root, 'wanted' AS wanted
MATCH path = (root)-[:Expand*0..16]->()
  -[:EnterSuffix]->(head:SuffixHead)
  -[:ContinueSuffix]->(:SuffixMiddle)
  -[:CompleteSuffix]->(terminal:SuffixTerminal)
WHERE terminal.name = wanted
RETURN path
```

Fixture: a selected root connected directly to `SuffixHead`, then `SuffixMiddle`, then `SuffixTerminal`, using the three fixed relationship kinds. Set the terminal's name to `'wrong'`. No `Expand` edge is needed because the expansion permits zero hops.

Expected: zero rows. Actual: the guarded reverse candidate returns one path; the incumbent returns zero. The forced guarded arm was executed; manifest-authorized production routing remains an additional repair acceptance test.

### F4 — Expansions lose an already-bound terminal's identity

**Severity:** High. **Location:** [translate/expansion.go](cypher/models/pgsql/translate/expansion.go), reviewed lines 4279–4287, `buildExpansionPatternStep`.

The expansion primer joins the terminal node only when a terminal satisfaction expression remains. Reusing a previously bound node still requires endpoint equality even when its other predicates have already been consumed by the prefix.

```cypher
MATCH (a)-[:X]->(mid)-[:R*1..2]->(a)
WHERE a.name = 'shared'
RETURN a
```

Fixture: distinct nodes `a1-[:X]->mid-[:R]->a2`, where `a1.name = 'shared'` and `a2.name = 'other'`, with no cycle.

Expected: zero rows, because the terminal must equal `a1`. Actual: one row returning `a2`, including its name `'other'`. Both optimized and optimizer-disabled translation produce the false positive. The ordinary expansion executor must be repaired independently of newer endpoint-seeded optimizations.

### F5 — Traversal-count optimization discards additional source labels

**Severity:** High. **Location:** [optimize/lowering_plan.go](cypher/models/pgsql/optimize/lowering_plan.go), reviewed lines 3198–3201, `aggregateTraversalMatch`.

The aggregate fast path accepts a second source pattern with additional labels, but its candidate-source filter uses only labels from the first match.

```cypher
MATCH (s:User)
MATCH (s:Group)-[:R*1..2]->(t)
WITH s, count(t) AS c
RETURN s, c ORDER BY c DESC LIMIT 10
```

Fixture: a `User` source without `Group`, with one outgoing `R` edge to another node.

Expected: zero rows. Actual: the optimized query returns the ineligible source with count `1`; optimization disabled returns zero rows. Counts and rankings silently include sources excluded by the authored query.

### F6 — Traversal-count optimization treats outgoing paths as cycles

**Severity:** High. **Location:** [optimize/lowering_plan.go](cypher/models/pgsql/optimize/lowering_plan.go), reviewed lines 3207–3208, `aggregateTraversalMatch`.

The aggregate fast path accepts the same source and terminal symbol without retaining their identity constraint. Its traversal counts outgoing paths without requiring `next_id = root_id`.

```cypher
MATCH (s:User)
MATCH (s)-[:R*1..2]->(s)
WITH s, count(s) AS c
RETURN s, c ORDER BY c DESC LIMIT 10
```

Fixture: a `User` source with one outgoing `R` edge to a different node and no cycle.

Expected: zero rows. Actual: the optimized query returns one row with count `1`; optimization disabled returns zero rows. This is distinct from F4: it concerns the specialized aggregate fast path, and its disabled-mode baseline behaves correctly.

### F7 — Leading `UNWIND` / `CREATE` followed by `WITH` generates self-referencing SQL

**Severity:** High. **Locations:** [translate/unwind.go](cypher/models/pgsql/translate/unwind.go), reviewed lines 61–70; [translate/create.go](cypher/models/pgsql/translate/create.go), reviewed lines 43–44.

A multipart placeholder frame is treated as an existing physical SQL source. The resulting CTE reads `s0` while defining `s0`.

```cypher
UNWIND [1, 2] AS x WITH x RETURN x
```

Expected: two rows containing `1` and `2`. Actual: PostgreSQL `42P01`, `relation "s0" does not exist`.

```cypher
CREATE (n) WITH n RETURN n
```

Expected: create and return one node. Actual: the same `42P01` error. Both were confirmed using PostgreSQL `EXPLAIN` on the actual emitted SQL, without executing the mutation. The CREATE repair must also respect PostgreSQL's restrictions on nested data-modifying CTEs; removing the self-reference alone is not a sufficient closure test.

## Recommended repair sequence

Default production queries that silently return incorrect results come first. Shared recognizers and representation code should be repaired together. Optional suffix policies are requalified after their ordinary traversal baseline is correct.

Before stage 1, record whether an affected suffix policy is active. If F3 is active in the target deployment, bring forward containment: reject unsupported correlated shapes or route them to the exact incumbent until repaired. Policy activation was not established by this review.

### Stage 1 — Repair aggregate fast-path admission and semantics: F5, then F6

**Status:** Complete. **Owner:** Codex, 2026-10-21 stage-1 session. **Dependency:** None.

- [x] Add minimal failing regressions for F5 and F6 before editing the recognizer.
- [x] Make eligibility account for every constraint on both source appearances. Either preserve the second source's labels with correct conjunction semantics, or decline the fast path when preservation is not supported.
- [x] Preserve repeated endpoint identity, or decline the fast path when source and terminal reuse a binding. Use the correct ordinary translation as the fallback for these aggregate shapes.
- [x] Test a source lacking the extra label, a source satisfying both labels, a noncyclic outgoing edge, and genuine cycles with multiple path lengths.
- [x] Assert exact source IDs, counts, aliases, and ranking membership. Verify eligible ordinary count queries still take the fast path.
- [x] Close F5 and F6 separately, with named tests and recorded results for each. See the stage 1 checkpoint below.

**Exit condition:** optimized and disabled-mode results agree on the fixtures, with correct labels, endpoint identity, counts, and result cardinality.

### Stage 2 — Restore bound endpoint identity in ordinary traversal: F4

**Status:** Not started. **Owner:** Unassigned. **Dependency:** Stage 1 in the recommended serial order; implementation is otherwise independent.

- [ ] Add the distinct-`a1`/`a2` fixture as a failing semantic regression.
- [ ] Carry the bound terminal's identity through primer, recursive, and final filtering even when no terminal property predicate remains.
- [ ] Audit automatic endpoint-seeded/reverse routes for the same binding contract. Decline an optimized route if it cannot preserve the constraint.
- [ ] Cover repeated bindings within one pattern and across query parts, positive cycles, false cycles, zero-depth eligibility, and directed/undirected forms supported by the translator.
- [ ] Check returned entity identity and full path endpoints, not just row counts, with optimization enabled and disabled.

**Exit condition:** no executor can substitute a different terminal for the bound node; ordinary traversal supplies a correct baseline for stage 5.

### Stage 3 — Repair node representation planning: F1, then F2

**Status:** Not started. **Owner:** Unassigned. **Dependency:** Earlier stages in the recommended order. F1 and F2 share scalar projection machinery and should have one coordinated owner.

- [ ] Add F1 regressions for fixed-hop and variable-hop continuations after `WITH`, asserting both returned ID values and integer result types.
- [ ] Track the representation actually materialized by each frame. Preserve a carried composite or explicitly project its ID before consumers treat the binding as scalar.
- [ ] Add F2 regressions for node and relationship inline property maps, including matching and nonmatching property values.
- [ ] Visit inline-map expression consumers while distinguishing them from pattern binding declarations. Retain properties/kinds/full entities whenever required downstream.
- [ ] Exercise both optimizer modes. A collector-only change can close F2 without repairing F1, so validate each finding independently.
- [ ] Confirm genuinely ID-only continuations still use scalar projections where safe; check representative generated SQL and focused performance coverage if the projection strategy changes.

**Exit condition:** carried-frame representations and all consumers agree, property maps execute correctly, and ID-returning expressions produce integer IDs.

### Stage 4 — Repair leading multipart clause initialization: F7

**Status:** Not started. **Owner:** Unassigned. **Dependency:** None technically; execute after stage 3 in the recommended order.

- [ ] Add failing cases for both leading `UNWIND ... WITH` and `CREATE ... WITH`.
- [ ] Distinguish an empty multipart placeholder from an already materialized source frame. Audit leading-clause frame initialization and projection assembly together.
- [ ] Verify emitted SQL has valid CTE dependencies and PostgreSQL-compatible placement of data-modifying CTEs.
- [ ] Execute UNWIND cases with empty, singleton, and multi-element lists, plus subsequent filtering and projection.
- [ ] Execute CREATE cases in rollback fixtures and assert exactly one created node per input row, returned identity/properties, and visibility to subsequent clauses. Include a committed fixture with cleanup if required to prove persistence.
- [ ] Retain coverage for MATCH/WITH followed by CREATE and for UNWIND attached to an existing source.

**Exit condition:** both original queries execute with correct rows and mutation effects; neither invalid self-references nor nested-write restrictions remain.

### Stage 5 — Preserve correlated suffix predicates and requalify the policy: F3

**Status:** Not started. **Owner:** Unassigned. **Dependency:** Stage 2's corrected ordinary traversal baseline; incorporate stage 3's representation fixes.

- [ ] Add the zero-expansion, three-fixed-hop suffix fixture and carry a scalar through `WITH` as in F3.
- [ ] Preserve every predicate remainder after locality partitioning. Attach nonlocal predicates where their dependencies are available, or reject the candidate shape and use the exact incumbent.
- [ ] Test matching and nonmatching terminal values and correlated predicates on suffix boundaries, intermediate nodes, and relationships.
- [ ] Compare exact candidate and incumbent row/path multisets, including duplicate handling, path ordering within each path, and repeated endpoint constraints.
- [ ] Exercise candidate completion and fallback at the applicable suffix/state/output guard boundaries.
- [ ] Cover the forced guarded arm and manifest-authorized production topology routing. Keep affected shapes on the incumbent until these checks pass.

**Exit condition:** authorized candidate and fallback execution preserve all authored predicates and return equivalent results across guard branches.

## Validation and closure gates

- Follow [AGENTS.md](AGENTS.md) for every implementation session. Add required mutation and template coverage for parsing, translation, rendering, or integration-semantic changes, and regenerate fixture artifacts through the repository workflow.
- Put backend-equivalent semantic cases in the shared integration corpus. Keep PostgreSQL optimizer/SQL-arm assertions in driver-scoped coverage; do not introduce backend-specific expectations or skips into shared cases.
- Record a failing reproduction before each repair and a passing result after it. SQL snapshot agreement alone does not establish correctness: check actual rows, identities, counts, scalar types, path structure, and mutation effects as applicable.
- Format edited code, run focused tests appropriate to the changed packages, then run `make test_all` for the backend selected by `CONNECTION_STRING`.
- Use the authorized local integration config to populate `CONNECTION_STRING` without printing credentials. Its PostgreSQL connection is a keyword DSN; the integration harness requires an equivalent PostgreSQL URI. Preserve its connection options when converting.
- Serialize database-mutating qualification runs. Parallel agents can review distinct directories and run independent unit checks, but must coordinate shared-file edits.
- Do not remove temporary test files while a Go test/build command may still have them in its file list. The original review had this interference once; use stable tracked regressions or isolated harness directories.
- A finding is **Closed** only when its acceptance tests pass and the ledger records the changed files, named tests, commands/results, and patch/commit reference when one exists. A proposed fix or changed SQL snapshot is insufficient.
- Track qualification failures separately. Do not mark the overall translator qualified while relevant failures or coverage gaps remain unexplained.

## Baseline qualification record

These are results from the reviewed checkout, not results for repaired code.

| Check | Recorded result | Evidence / follow-up |
| --- | --- | --- |
| `make test_all`, unit stage | Passed, with race detection | `.coverage/review-qualification.log` |
| PostgreSQL shared integration package | Passed | `github.com/specterops/dawgs/integration` |
| PostgreSQL translation validation package | Passed | `github.com/specterops/dawgs/cypher/models/pgsql/test` |
| PostgreSQL driver package | Passed | `github.com/specterops/dawgs/drivers/pg` |
| Translator package during the integration run | Interrupted by removal of a temporary review test file | Clean rerun below resolved this review-process failure |
| Clean translator rerun | Passed | `go test -tags 'manual_integration integration' -race -cover -count=1 -p=1 -parallel=1 ./cypher/models/pgsql/translate`; `.coverage/review-translate-clean.log` |
| `make test_all`, overall | Failed; make exit status 2 | Two adjacency tests failed with PostgreSQL `42501`, permission denied for table `p5_adjacency_v1`, in addition to the temporary-file interruption |
| Separately run `make test_bdd_integration` | Command exited 0, but reported 11 failed and 8 undefined scenarios | `.coverage/review-bdd.log`; the BDD `TestMain` only fails the command for DAWGS feature failures or invalid reports |
| Focused reproductions | Confirmed F1–F7 | Queries, fixtures, and actual results are recorded above |

The two adjacency failures were:

- `cmd/graphbench`: `TestP5AdjacencyCalibrationAttributesTaggedStatementWAL`.
- `drivers/pg/query`: `TestP5AdjacencyShadowLifecycle`.

BDD failures included unsupported side-effect assertions, returned-value formatting/comparison failures, and missing negative-query step definitions. Investigate and classify these qualification obstacles separately from F1–F7; an exit code of zero does not establish that the full BDD report passed.

Local review artifacts, if still present:

- `.coverage/reviewprobe/main.go` and `.coverage/review-reproductions.log`: F1, F2, F4, F5, and F6, plus controls.
- `.coverage/reviewsuffix/main.go` and `.coverage/review-suffix-reproduction.log`: F3 candidate/incumbent comparison.
- `.coverage/reviewexplain/main.go`: F7 emitted-SQL validation through PostgreSQL EXPLAIN.
- `.coverage/run_with_review_config.py`: local config-to-environment launcher with output redaction.

## Session handoff protocol

At the start of each session:

1. Read `AGENTS.md`, this ledger, the current Git status/diff, and the latest session entry. Compare the current checkout to the recorded baseline and preserve existing work.
2. Claim the next incomplete stage by recording owner/session identity, starting revision, and status. If another owner is active, agree on file boundaries before editing shared optimizer or translator files.
3. Reproduce the relevant finding on the current checkout using a durable regression. Do not assume ignored `.coverage` files survived from the review session.
4. Implement and validate the claimed work, updating the finding status and stage checkboxes as evidence accumulates. Use `Blocked` only with the specific missing prerequisite and the next action needed to resolve it.
5. Before ending or handing off, record changed files, tests and exit statuses, unresolved failures, and one concrete next action. Keep acceptance criteria unchecked until verified. Follow the repository's explicit-authorization requirement for creating commits.

Suggested statuses: `Open`, `Reproduced`, `In progress`, `Implemented; validation pending`, `Closed`, or `Blocked` with a reason.

### Initial handoff — 2026-10-21

- **Owner:** Review session.
- **Revision:** `750ea96107f7889733b67001922a090f4c6b4279`.
- **Completed:** parallel review, history tracing, PostgreSQL reproductions, baseline unit/integration qualification, clean translator rerun, separate BDD run, and this ledger.
- **Implementation changes:** none.
- **Open work:** F1–F7 and the qualification obstacles recorded above.
- **Next action:** claim stage 1; add failing shared semantic cases and PostgreSQL fast-path admission tests for F5 and F6 before changing `aggregateTraversalMatch`.

### Stage 1 implementation session — 2026-10-21

- **Owner:** Codex.
- **Starting revision:** `750ea96107f7889733b67001922a090f4c6b4279`; existing work is this untracked ledger, preserved and updated in place.
- **Claim:** stage 1, F5 then F6. No implementation work claimed for other stages.
- **Plan:** add failing optimizer admission and shared semantic/template regressions; decline aggregate counting when the second source has labels or the terminal reuses its source binding; verify positive fast-path controls and both optimizer modes; run qualification and record results.
- **F3 activation check:** the authorized local integration config supplies no suffix/topology policy setting; the integration harness opens the default driver without installing a traversal policy. The local qualification run therefore uses the incumbent. Target deployment activation remains unknown.
- **Reproduction before repair:** durable tests in `optimizer_test.go`, `optimizer_safety_test.go`, and `aggregate_traversal_count_shapes.json`. Admission tests failed for F5/F6 (exit 1); PostgreSQL semantic tests failed only in enabled mode (exit 1). F5 admitted `user-only` and let it displace `both-high` at LIMIT 1; F6 admitted a noncycle and counted 4 instead of 2 paths for `two-cycles`. All 17 disabled-mode variants passed. Supplementary logs: `.coverage/stage1-unit-before.log`, `.coverage/stage1-semantics-before.log`.
- **Repair:** `aggregateTraversalMatch` declines the fast path for any second-source labels and for identical source/terminal symbols. The ordinary executor retains the constraints. Existing unconstrained count shapes still plan and emit the fast path.
- **Focused validation:** `go test ./cypher/models/pgsql/optimize ./cypher/models/pgsql/translate -run 'AggregateTraversalCount' -count=1` passed (exit 0). `python3 .coverage/run_with_review_config.py go test -tags manual_integration ./integration -run '^TestPostgreSQLAggregateTraversalCountBindingSemantics$|^TestCypherTemplates$/aggregate_traversal_count_shapes' -count=1 -v` passed (exit 0): 17 variants per explicit optimizer mode plus 17 shared template variants. Three variants verify mutation selection/post-state in rollback fixtures. See `.coverage/stage1-unit-after.log`, `.coverage/stage1-semantics-after.log`.
- **Qualification:** `python3 .coverage/run_with_review_config.py make test_all > .coverage/stage1-qualification.log 2>&1` completed on PostgreSQL with **exit 2**. The complete unit phase passed with race detection. Shared integration, optimizer, translator, translation validation, and PostgreSQL driver packages passed. The only failures were the two baseline `42501` permission errors for `p5_adjacency_v1`: `cmd/graphbench/TestP5AdjacencyCalibrationAttributesTaggedStatementWAL` and `drivers/pg/query/TestP5AdjacencyShadowLifecycle`. BDD was not reached because make stopped at `test_integration`; the prior BDD qualification obstacles remain unresolved. No additional run was started after the user's checkpoint request. Supplementary evidence: `.coverage/stage1-qualification.log`, `.coverage/stage1-qualification.exit`.
- **Final checkpoint before session expiry:** stage 1 is complete. F5 and F6 are individually closed on passing semantic acceptance tests; the translator as a whole remains unqualified. No stage 2 work has started and no tests remain running.
- **F5 closure:** `TestAggregateTraversalCountAdmissionPreservesBindingConstraints/F5_*`, `TestAggregateTraversalCountFallsBackForBindingConstraints/F5_*`, the two shared `F5` template families, and their enabled/disabled PostgreSQL differential executions passed. Sources lacking Group are excluded, sources satisfying User and Group retain exact counts 2/1, and LIMIT 1 returns `both-high` instead of the higher-count ineligible decoy. Aliases, repeated labels, row-count form, and mutation post-state are covered.
- **F6 closure:** the corresponding `F6_*` admission/emission subtests, the two shared `F6` template families, and their enabled/disabled PostgreSQL differential executions passed. Directed outgoing and incoming noncycles are excluded; one/two-hop cycle counts are 2/1; the two-hop-only case returns just `two-cycles` with count 1. Zero-depth identity, row-count form, ranking, aliases, and mutation post-state are covered.
- **Changed files:** `cypher/models/pgsql/optimize/lowering_plan.go`, `cypher/models/pgsql/optimize/optimizer_test.go`, `cypher/models/pgsql/translate/optimizer_safety_test.go`, `integration/pgsql_aggregate_traversal_plan_test.go`, `integration/cypher_test.go`, new shared `integration/testdata/templates/aggregate_traversal_count_shapes.json`, `integration/testdata/README.md`, `docs/postgresql_translation.md`, `README.md`, and this ledger.
- **Formatting/review:** targeted `gofmt` on all five changed Go files and `git diff --check` passed. No generated fixtures changed; new cases are ordinary template JSON consumed directly by the existing runner. No commit created.
- **Coverage:** full unit phase passed with race detection. Baseline `.coverage/stage1-unit-baseline.out`: 31,599 / 48,254 statements (65.4847%); repaired `.coverage/unit.out`: 31,601 / 48,254 (65.4889%). Overall coverage did not decrease.
- **Next concrete action:** claim stage 2/F4 and add the distinct-bound-endpoint fixture before modifying ordinary expansion. Track the adjacency table permissions and BDD failures as separate qualification work; do not mark the translator qualified until those obstacles and the remaining findings are resolved.
- **Additional observations outside F1–F7:** the specialized count SQL always projects the source before its count, even for `RETURN c AS total, s AS source`. Also, raw `tx.Query` calls `translate.Translate` directly and does not honor `pg.SetOptimizedTranslation(false)` in this checkout. The new PostgreSQL differential test therefore uses explicit `translate.TranslateWithOptions` and executes the emitted SQL via `tx.Raw`, ensuring disabled-mode results are actual baselines. These observations remain separate follow-up work.

### Session entry template

```text
Date / session / owner:
Starting revision and existing work:
Claimed stage and finding IDs:
Reproduction before repair:
Changes and affected files:
Tests run (exact commands, backend, exit statuses, results):
Finding statuses and acceptance criteria completed:
Patch / commit reference, if any:
Remaining failures or blockers:
Next concrete action:
```

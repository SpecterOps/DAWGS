# Translator review and remediation ledger

## Current state

- Review date: 2026-10-21.
- Reviewed checkout: `750ea96107f7889733b67001922a090f4c6b4279`.
- Scope: merged V2 history, starting with `8ffc4418`, through the reviewed checkout, including the later V2 builder port at `403c9289`.
- Result: **F1–F7 closed on passing focused acceptance checks; translator remains unqualified because of PostgreSQL extension permissions and BDD/TCK gaps. No critical findings confirmed**.
- Implementation: stages 1–5 complete. F3 uses conservative admission: unsupported correlated suffixes, additional root constraints, and primer-only predicates retain the exact incumbent; forced reverse diagnostics decline them. Final-source unit and focused PostgreSQL checks pass; remaining full-suite qualification obstacles are classified below.
- Review method: parallel subdirectory reviews plus focused PostgreSQL reproductions.
- Test backend: PostgreSQL 18.6. Current qualification uses the user-supplied `CONNECTION_STRING`; the original review used its authorized local configuration.
- The review itself left tracked source files unchanged. Subsequent repairs are recorded below and currently form an uncommitted worktree patch.

Finding IDs are permanent. Source line numbers refer to the reviewed checkout and may move as repairs land. The findings and reproductions below are self-contained; local `.coverage` artifacts supplement them but are ignored by Git and are not the durable record.

| ID | Severity | Finding | Introduced by | Repair stage | Status | Owner |
| --- | --- | --- | --- | --- | --- | --- |
| F1 | High | Carried nodes acquire the wrong representation | `b78ed8ba` | 3 | Closed | Codex |
| F2 | High | Inline property maps lose required node properties | `b78ed8ba` | 3 | Closed | Codex |
| F3 | High | Suffix-reverse execution drops correlated predicates | `b78ed8ba` | 5 | Closed; correlated shapes use incumbent | Codex |
| F4 | High | Expansions lose an already-bound terminal's identity | `2a043e64` | 2 | Closed | Codex |
| F5 | High | Traversal-count optimization discards additional source labels | `9fe77970` | 1 | Closed | Codex |
| F6 | High | Traversal-count optimization treats outgoing paths as cycles | `9fe77970` | 1 | Closed | Codex |
| F7 | High | Leading `UNWIND` / `CREATE` followed by `WITH` generates self-referencing SQL | `2a043e64` | 4 | Closed | Codex |

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

**Status:** Complete. **Owner:** Codex, stage-2 implementation and 2026-10-21 resume session. **Dependency:** Stage 1 in the recommended serial order; implementation is otherwise independent.

- [x] Add the distinct-`a1`/`a2` fixture as a failing semantic regression. The original review reproduces the false positive; durable stage-2 unit assertions failed before repair, and shared database acceptance now passes.
- [x] Carry the bound terminal's identity through primer, recursive, and final filtering even when no terminal property predicate remains. Completed states retain their scalar endpoint and are compared with the prefix binding during projection; intermediate hops remain unrestricted.
- [x] Audit automatic endpoint-seeded/reverse routes for the same binding contract. Endpoint-seeded admission now rejects a terminal bound earlier in its own pattern, including reuse of the expansion root. Existing earlier-clause rejection and eligible unbound controls still pass. Suffix correlation remains under F3/stage 5.
- [x] Cover repeated bindings within one pattern and across query parts, positive cycles, false cycles, zero-depth eligibility, and directed/undirected forms supported by the translator.
- [x] Check returned entity identity and full path endpoints, not just row counts, with optimization enabled and disabled.

**Exit condition:** no executor can substitute a different terminal for the bound node; ordinary traversal supplies a correct baseline for stage 5.

### Stage 3 — Repair node representation planning: F1, then F2

**Status:** Complete. **Owner:** Codex, 2026-10-21 resume session. **Dependency:** Earlier stages in the recommended order. F1 and F2 share scalar projection machinery and were repaired together.

- [x] Add F1 regressions for fixed-hop and variable-hop continuations after `WITH`, asserting both returned ID values and integer result types.
- [x] Track the representation actually materialized by each frame. Preserve a carried composite or explicitly project its ID before consumers treat the binding as scalar.
- [x] Add F2 regressions for node and relationship inline property maps, including matching and nonmatching property values.
- [x] Visit inline-map expression consumers while distinguishing them from pattern binding declarations. Retain properties/kinds/full entities whenever required downstream.
- [x] Exercise both optimizer modes. A collector-only change can close F2 without repairing F1, so validate each finding independently.
- [x] Confirm genuinely ID-only continuations still use scalar projections where safe; representative generated SQL and existing ID-only continuation controls pass. No new executor or performance claim is introduced.

**Exit condition:** carried-frame representations and all consumers agree, property maps execute correctly, and ID-returning expressions produce integer IDs.

### Stage 4 — Repair leading multipart clause initialization: F7

**Status:** Complete. **Owner:** Codex, 2026-10-21 resume session. **Dependency:** None technically; executed after stage 3.

- [x] Add failing cases for both leading `UNWIND ... WITH` and `CREATE ... WITH`.
- [x] Distinguish an empty multipart placeholder from an already materialized source frame. Audit leading-clause frame initialization and projection assembly together.
- [x] Verify emitted SQL has valid CTE dependencies and PostgreSQL-compatible placement of data-modifying CTEs.
- [x] Execute UNWIND cases with empty, singleton, and multi-element lists, plus subsequent filtering and projection.
- [x] Execute CREATE cases in rollback fixtures and assert exactly one created node per input row, returned identity/properties, and visibility to subsequent clauses. A committed creation is read from a separate transaction and cleaned by the integration harness.
- [x] Retain coverage for MATCH/WITH followed by CREATE and for UNWIND attached to an existing source.

**Exit condition:** both original queries execute with correct rows and mutation effects; neither invalid self-references nor nested-write restrictions remain.

### Stage 5 — Preserve correlated suffix predicates and requalify the policy: F3

**Status:** Complete via conservative admission. **Owner:** Codex, 2026-10-21 resume session. **Dependency:** Stage 2's corrected ordinary traversal baseline and stage 3's representation fixes.

- [x] Add the zero-expansion, three-fixed-hop suffix fixture and carry a scalar through `WITH` as in F3.
- [x] Preserve every predicate remainder after locality partitioning by rejecting unsupported candidate shapes and using the exact incumbent. The SQL builder also rejects unexpected remainders; generated edge exclusions are accepted only when structurally identical to exclusions enforced by reverse path disjointness and pairwise suffix uniqueness.
- [x] Test matching and nonmatching terminal values and correlated predicates on suffix boundaries, intermediate nodes, and relationships.
- [x] Check exact incumbent row/path multisets for rejected shapes, including duplicate carried rows and ordered path nodes. Repeated suffix endpoints are rejected at admission; stage-2 cases verify their ordinary-executor baseline. Supported reverse controls retain existing exact-result coverage.
- [x] Verify supported candidate completion, state/output retry, no-path execution, cancellation, and pool reuse with the existing suffix integration controls. Correlated shapes are rejected before runtime cap selection, so no guard branch can admit them.
- [x] Reproduce the forced guarded arm's incorrect rows before repair, then verify forced diagnostics decline the shape. Manifest-authorized v4 and v5 policies both execute matching/nonmatching correlated queries on the incumbent; eligible uncorrelated production controls still execute the candidate.

**Exit condition:** authorized candidate and fallback execution preserve all authored predicates and return equivalent results across guard branches.

## Validation and closure gates

- Follow [AGENTS.md](AGENTS.md) for every implementation session. Add required mutation and template coverage for parsing, translation, rendering, or integration-semantic changes, and regenerate fixture artifacts through the repository workflow.
- Put backend-equivalent semantic cases in the shared integration corpus. Keep PostgreSQL optimizer/SQL-arm assertions in driver-scoped coverage; do not introduce backend-specific expectations or skips into shared cases.
- Record a failing reproduction before each repair and a passing result after it. SQL snapshot agreement alone does not establish correctness: check actual rows, identities, counts, scalar types, path structure, and mutation effects as applicable.
- Format edited code, run focused tests appropriate to the changed packages, then run `make test_all` for the backend selected by `CONNECTION_STRING`.
- Supply the authorized test connection through `CONNECTION_STRING` without printing or saving credentials in evidence. The integration harness requires a backend URI; preserve connection options when converting a keyword DSN.
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

### Stage 2 implementation session — 2026-10-21

- **Owner:** Codex.
- **Starting revision:** `c4776de`; clean worktree, with stage 1 and this ledger already committed by the user.
- **Claim:** stage 2/F4; preserve the remaining stages for their coordinated repairs.
- **Plan:** reproduce the missing bound-terminal equality in durable translation and shared semantic/template cases; correlate ordinary expansion results with the bound terminal independently of terminal property predicates; audit alternative expansion routes; verify exact nodes, path endpoints, zero-depth eligibility, and mutation post-state in both optimizer modes.
- **Validation prerequisite:** requested a test-backend `CONNECTION_STRING` or authorized local configuration path under `AGENTS.md`. The prior session's ignored `.coverage` launcher and reproduction files are absent. Continue unit verification while database access is pending.
- **Reproduction before repair:** `go test ./cypher/models/pgsql/translate ./cypher/models/pgsql/optimize -run 'TestExpansionStepPreservesBoundTerminalIdentity|TestGuardedEndpointSeededExpansionFallbackReasons' -count=1` failed (exit 1). Both modes omitted the bound-terminal equality. Enabled mode also admitted a same-pattern bound terminal to endpoint-seeded reverse, whose candidate seed had no remaining terminal predicate. The two new optimizer cases (`terminal_bound_by_prefix`, `terminal_repeats_expansion_root`) failed admission assertions. Supplementary evidence: `.coverage/stage2-unit-before.log`. Database reproduction is pending, so the first stage-2 checkbox remains open.
- **Repair:** `buildExpansionPatternStep` adds an unconditional identity gate for a bound terminal to completed zero-depth, primer, and recursive states; it reuses `expansionProjectionNodeJoins` to hydrate a shared expansion root/terminal once. Endpoint-seeded admission uses declarations visible before each endpoint rather than only declarations preceding the pattern.
- **Representation prerequisite brought forward:** path checks revealed that optimizer-disabled mode still scalarized intermediate nodes without a field-requirement plan, leaving composite consumers invalid. `applyIDOnlyNodeProjection` now retains composites when the query part has no field-requirement plan. This small prerequisite keeps the baseline usable for F4; it does **not** close F1 or F2, whose optimized-mode representation and inline-map issues remain for stage 3.
- **Durable coverage:** `TestExpansionStepPreservesBoundTerminalIdentity` covers seven shapes in each mode; `TestSelfLoopExpansionAfterPrefixHydratesSharedEndpointOnce` covers the shared-alias case in each mode. Shared `bound_expansion_endpoint_shapes.json` contains 43 variants in nine families, including six mutation/post-state variants, exact returned node records, ordered full-path node IDs, zero-depth success/rejection, and per-prefix-row correlation. Direction coverage includes outbound/inbound expansion and an undirected fixed prefix; fully undirected variable expansions remain unsupported. `TestPostgreSQLExpansionBoundTerminalSemantics` reuses the existing explicit-compiler-mode template runner to execute the same assertions in both modes.
- **Focused validation:** `go test ./cypher/models/pgsql/translate ./cypher/models/pgsql/optimize ./cypher/models/pgsql/test -count=1` passed (exit 0). All 86 template/mode combinations parsed, translated, and rendered in `.coverage/stage2-template-compile/main.go` (exit 0); these checks are not database semantic evidence. No generated translation fixtures changed.
- **Database validation:** `go test -tags manual_integration ./integration -run '^TestPostgreSQLExpansionBoundTerminalSemantics$' -count=1 -v` compiled, then **skipped** because `CONNECTION_STRING` is unset. The prior authorized config path `/home/zinic/work/bhe-dawgs/integration-config-local.json` is also unavailable. Neither shared semantic execution nor `make test_all` has been run in this session. Do not close F4 on compilation/SQL checks alone.
- **Unit environment:** the managed image defaults to `CGO_ENABLED=0` and exposes its C compiler as `cc`; race-enabled checks require `CGO_ENABLED=1 CC=cc`. The baseline full unit run then failed (make exit 2) only at unchanged `cypher/models/pgsql/TestValueToDataType`, where the UTC local timezone makes the expected local/UTC distinction ambiguous. `TZ=America/New_York go test ./cypher/models/pgsql -run '^TestValueToDataType$' -count=1` passed (exit 0).
- **Final unit qualification:** `TZ=America/New_York CGO_ENABLED=1 CC=cc make test COVERAGE_PROFILE=.coverage/stage2-unit-after.out` passed (exit 0), with race detection across all unit packages. Evidence: `.coverage/stage2-unit-after-full.log`. Coverage, merging duplicate profile blocks by source range, increased from **31,283 / 48,254 statements (64.8299%)** in this checkout's baseline run to **31,288 / 48,259 (64.8335%)** after repair. These current-environment measurements are distinct from stage 1's historical coverage capture.
- **Changed files:** `cypher/models/pgsql/translate/expansion.go`, `traversal.go`, new `expansion_binding_test.go`; `cypher/models/pgsql/optimize/lowering_plan.go`, `optimizer_test.go`; `integration/pgsql_aggregate_traversal_plan_test.go`, new shared `integration/testdata/templates/bound_expansion_endpoint_shapes.json`; `integration/testdata/README.md`, `docs/postgresql_translation.md`, `README.md`, and this ledger.
- **Formatting and patch:** targeted `gofmt` on all six changed/new Go files and `git diff --check` passed. Changes remain an uncommitted worktree patch; no commit created. No test/build command remains running.
- **Final checkpoint:** F4 is **Implemented; validation pending**, not closed. Unit/SQL checks pass; the three remaining semantic acceptance checkboxes still require database execution. F1–F3 and F7 remain open; F5/F6 remain closed. Overall translator qualification is still incomplete.
- **Next concrete action:** provide a test-backend connection, then run `go test -tags manual_integration ./integration -run '^TestPostgreSQLExpansionBoundTerminalSemantics$|^TestCypherTemplates$/bound_expansion_endpoint_shapes' -count=1 -v` and `TZ=America/New_York CGO_ENABLED=1 CC=cc make test_all` for its selected backend. Verify semantic acceptance before closing F4 or advancing to the coordinated F1/F2 repair. Existing adjacency/BDD qualification obstacles remain separate work.

### Stage 2 database qualification follow-up — 2026-10-21

- **Owner:** Codex.
- **Starting revision and work:** `c4776de`, preserving the prior stage-2 worktree patch and all existing changes.
- **Connection:** the user supplied a PostgreSQL connection targeting `127.0.0.1:5432`. The runner receives it through the child environment; its output launcher redacts the URI and password before writing evidence.
- **Attempt:** `TZ=America/New_York CGO_ENABLED=0 go test -tags manual_integration ./integration -run '^TestPostgreSQLExpansionBoundTerminalSemantics$|^TestCypherTemplates$/bound_expansion_endpoint_shapes' -count=1 -v` failed (exit 1). Both selected tests failed during schema initialization with `dial tcp 127.0.0.1:5432: connect: connection refused`. No fixture, query, or semantic assertion executed. Supplementary evidence: `.coverage/stage2-semantics-live.log`.
- **Toolchain observation:** an initial non-race build with `CGO_ENABLED=1 CC=cc` failed to link libc symbols with the image's musl compiler. The focused command above compiled successfully with `CGO_ENABLED=0`; the prior race-enabled unit run remains passing with `CGO_ENABLED=1 CC=cc`.
- **Qualification:** `make test_all` was not started because focused integration setup cannot connect. The existing passing unit/SQL checks are unchanged. F4 remains **Implemented; validation pending**, and all database acceptance criteria remain open.
- **Changes:** this ledger only; no production or test source changes, and no commit created.
- **Next concrete action:** supply a PostgreSQL endpoint reachable from this workspace, or start PostgreSQL on its loopback interface; rerun the focused shared/differential cases before full qualification. A replacement connection or a running local service is required to proceed.

### Resume and stage 3 session — 2026-10-21

- **Owner:** Codex. **Starting revision:** `c4776de`; all existing stage-2 changes preserved as an uncommitted patch.
- **Claim and plan:** validate stage 2/F4 against the supplied PostgreSQL connection, then repair F1/F2 together with durable failing unit, shared template, and mutation regressions; proceed through subsequent stages after their dependencies are verified.
- **Connection prerequisite:** PostgreSQL 18.6 is reachable. Focused stage-2 integration execution failed (exit 1) during schema initialization, before any semantic assertion: `25P02`. A direct execution of `schema_up.sql` inside a rolled-back diagnostic transaction exposed the originating error: `42501`, `must be owner of table node_24`. No diagnostic DDL was committed. Requested an owned disposable database or corrected test-schema ownership; no credentials are recorded in this ledger or evidence.
- **Focused command:** `TZ=America/New_York CGO_ENABLED=0 python3 .coverage/run_stage2_validation.py go test -tags manual_integration ./integration -run '^TestPostgreSQLExpansionBoundTerminalSemantics$|^TestCypherTemplates$/bound_expansion_endpoint_shapes' -count=1 -v` (supplied connection in child environment); `.coverage/stage2-resume-semantics.log`. F4 remains implemented with semantic validation pending.
- **Independent work:** stage 3/F1–F2 regression development can proceed while database ownership is resolved; semantic closure and advancing dependent stages still require successful database execution.

### Resume completion of stages 2–5 — 2026-10-21

- **Owner / revision:** Codex, starting at `c4776de`; preserved all existing stage-2 source, tests, documentation, and ledger edits. No commit created.
- **Database reset:** the user cleaned/reset the supplied PostgreSQL database after the ownership diagnostic. Schema initialization and fixture execution now succeed. The original connection-refusal and `node_24` ownership blockers are resolved for this session.
- **F4 closure:** the first live run executed all acceptance cases and exposed a zero-upper-bound defect: `*0..0` still emitted primer edges, yielding one unexpected row for genuine-cycle fixtures and an extra self-loop path. `ExpansionBuilder.Build` now disables its one-edge primer when maximum depth is zero. `TestPostgreSQLExpansionBoundTerminalSemantics` passed all **43 variants per optimizer mode**, and `TestCypherTemplates/bound_expansion_endpoint_shapes` passed all **43 shared variants**, including exact node/path identities, per-row correlation, both directions, undirected fixed prefixes, and six mutation/post-state cases. Before: `.coverage/stage2-resume-semantics.log` (exit 1); after: `.coverage/stage2-resume-semantics-after.log` (exit 0).
- **F1 reproduction / repair / closure:** `TestCarriedNodeRepresentation` failed in enabled mode for fixed and variable hops before repair. All nine enabled shared-template variants failed with PostgreSQL `42883` before repair; disabled controls passed. `applyIDOnlyNodeProjection` preserves bindings that have `LastProjection`; expansion decides representation before dematerializing endpoints so seed/correlation consumers still address the representation stored in the input frame. `TestCarriedNodeRepresentation`, the nine `F1` variants in `node_representation_shapes`, and `TestPostgreSQLCarriedNodeIntegerIDs` now pass. The last test checks raw values are `int64` and exact fixture IDs for fixed/variable hops in both modes. Aliases, zero hops, and mutation selection are covered.
- **F2 reproduction / repair / closure:** `TestInlineMapFieldRequirements` failed to find intermediate `m` requirements, and all eight enabled node/relationship-map variants failed with `42809` before repair. The field collector now visits inline expression consumers through the ordinary expression walker, which already omits pattern declaration variables. `TestInlineMapFieldRequirements`, both four-variant `F2` families, and their explicit enabled/disabled executions pass for matching/nonmatching values, arithmetic expressions, and mutation post-state. The stage-2 composite baseline prerequisite also keeps disabled-mode consumers safe.
- **Stage-3 evidence:** `.coverage/stage3-unit-before.log` and `.coverage/stage3-semantics-before.log` (exit 1); `.coverage/stage3-unit-after.log` and `.coverage/stage3-semantics-after.log` (exit 0). Existing `TestIDOnly*` SQL controls still pass, including scalar expansion-to-fixed and expansion-to-expansion continuation.
- **F7 reproduction / repair / closure:** `TestLeadingMultipartClauseSources` failed for both original queries in both modes; live template runs failed before repair on self-references or nested modifying CTEs. Creation source assembly resolves a multipart output placeholder to its actual input, carries newly unwound variables as unmaterialized values, and hoists modifying CTE dependencies above the multipart boundary. Inline WITH projection omits a nonexistent placeholder source. Empty UNWIND literals receive a concrete PostgreSQL array type. All **15 shared variants**, **30 explicit-mode variants**, and `TestLeadingMultipartClauseSources` pass. `TestPostgreSQLMultipartCreatePersists` commits one creation, then checks its exact ID/properties from a separate read transaction with harness cleanup. Coverage includes empty/singleton/multiple UNWIND, filtering, consecutive parts, attached sources, per-row CREATE, repeated CREATE boundaries, relationships, and post-state counts. Evidence: `.coverage/stage4-unit-before.log`, `.coverage/stage4-semantics-before.log` (exit 1), `.coverage/stage4-unit-after.log`, `.coverage/stage4-semantics-after.log` (exit 0 after the empty-array fix); final persistence evidence below.
- **F3 reproduction:** `TestSuffixSearchRejectsExternalPredicateDependencies` and `TestSuffixReverseRejectsCarriedPredicate` failed admission assertions. Live `TestPostgreSQLCorrelatedSuffixGuardAdmission` returned two paths instead of one/zero for terminal, head, middle, and boundary predicates, and six instead of two for duplicate carried rows. Evidence: `.coverage/stage5-unit-before.log`, `.coverage/stage5-semantics-before.log` (exit 1).
- **F3 repair / closure:** fixed-suffix admission now rejects outside dependencies in suffix WHERE predicates and inline maps, plus terminal bindings reused within the same pattern. Final audit also rejects comparisons between suffix bindings, inline-map reads from earlier suffix frames, additional constraints on a bound root, and constant/root/scalar-only WHERE terms that the ordinary executor attaches to its primer. Conjunctions and parenthesized terms are checked independently so a local suffix condition cannot hide a discarded constant term. Additional admission regressions failed before tightening (`.coverage/stage5-cross-suffix-before.log`, `.coverage/stage5-root-constraints-before.log`, exit 1). The root/scalar/constant/extra-label/property database cases returned two paths instead of zero, and a constant conjunct returned one instead of zero (`.coverage/stage5-root-semantics-before.log`, exit 1). All four affected unit packages pass afterward (`.coverage/resume-unit-focused-final.log`, exit 0), with existing fallback reason codes retained. The incumbent retains the original authored predicates. Forced guard/retry/component/orientation tools decline unsupported shapes. The SQL suffix builder rejects unrecognized external remainders rather than dropping them; it recognizes only generated relationship exclusions structurally identical to those enforced in the reverse plan. `TestFixedSuffixBuilderRejectsExternalRemainders` covers boundary/left/relationship/right remainders. All **22 shared F3 variants** and **44 explicit-mode variants** cover exact ordered paths, duplicate row bags, cross-suffix comparisons/maps, relationship-map predicates, root/constant constraints, and mutation selection. `TestV2TopologyFixedSuffixKeepsCorrelatedPredicatesOnIncumbent` authorizes manifests for both v4 and v5, executes matching/nonmatching values twice in a stable snapshot, and asserts zero candidate selection; existing eligible v4/v5 controls still select reverse.
- **Final focused integration command:** `TZ=America/New_York CGO_ENABLED=0 python3 .coverage/run_stage2_validation.py go test -tags manual_integration ./integration ./drivers/pg -run '^TestPostgreSQL(CorrelatedSuffix|NodeRepresentation|CarriedNodeIntegerIDs|LeadingMultipart|MultipartCreatePersists|ExpansionBoundTerminal)|^TestCypherTemplates$/(correlated_suffix_shapes|node_representation_shapes|leading_multipart_shapes|bound_expansion_endpoint_shapes)|^TestV2TopologyFixedSuffix' -count=1 -v -p=1` passed (exit 0): **97 shared variants**, **194 explicit-mode variants**, direct integer-type, committed persistence, forced-admission, and both production-policy checks. Evidence: `.coverage/resume-integration-final.log`.
- **Policy controls:** `go test -tags manual_integration ./drivers/pg ./cmd/graphbench -run '^TestV2TopologyFixedSuffix|^TestPostgreSQLSuffixReverse|^TestPostgreSQLSuffixRouteComponent' -count=1 -v -p=1` passed for both packages with the supplied connection and `CGO_ENABLED=0`. Named controls include `TestPostgreSQLSuffixReverseGuardPlanAttribution`, `TestPostgreSQLSuffixReverseRetryPreservesExactRowsAndReceipts`, and all five `TestPostgreSQLSuffixRouteComponent*` tests: candidate receipt, forced/natural state retry, output-byte retry, no-path completion, prepared/workspace closure, cancellation, and physical session reuse. Evidence: successful package sections of `.coverage/resume-focused-final.log`; its overall exit 1 records an integration-test compile typo corrected before the separate successful integration run above.
- **Changed production files:** optimizer `lowering_plan.go`, `source_references.go`; translator `traversal.go`, `expansion.go`, `create.go`, `unwind.go`, `projection.go`, `query.go`, `expansion_suffix_seeded.go`.
- **Changed coverage / documentation:** preserved stage-2 tests; added `representation_test.go`, `suffix_correlation_test.go`, new shared `node_representation_shapes.json`, `leading_multipart_shapes.json`, `correlated_suffix_shapes.json`, PostgreSQL persistence/admission tests, explicit-mode parameter support, and production topology-policy regressions. Updated `README.md`, `docs/postgresql_translation.md`, `integration/testdata/README.md`, and this ledger. New JSON templates are consumed directly by the existing runner; no generated translation snapshots changed.
- **Formatting:** targeted `gofmt` on all 20 edited/new Go files and `git diff --check` pass. The first `make test_all` run passed its complete race-enabled unit phase, then was deliberately stopped during integration after source-admission tightening (launcher exit 241). `.coverage/resume-qualification.log` preserves that partial run; it is not final qualification. The completed broad rerun is recorded below in `.coverage/resume-qualification-final.log`.
- **Final unit phase and coverage:** after the last bound-root/constant admission repair, `TZ=America/New_York CGO_ENABLED=1 CC=cc python3 .coverage/run_stage2_validation.py make test COVERAGE_PROFILE=.coverage/resume-unit-final.out` passed (exit 0), with race detection across all unit packages. Merging repeated profile blocks by source range gives **31,677 / 48,329 statements (65.5445%)**, versus the checkout baseline **31,283 / 48,254 (64.8299%)**. Overall coverage increased. Evidence: `.coverage/resume-unit-final.log`, `.coverage/resume-unit-final.out`, `.coverage/resume-coverage-summary.txt`.

### Qualification follow-up — 2026-10-21

- **Full-suite command:** with the supplied connection in the environment, `TZ=America/New_York CGO_ENABLED=1 CC=cc python3 .coverage/run_stage2_validation.py make test_all COVERAGE_PROFILE=.coverage/resume-unit-final.out` completed with **exit 2**. Unit tests passed with race detection. All integration packages passed except `cmd/graphbench`, where two tests failed while trying to install the capture-only `pg_stat_statements` extension: `TestP5AdjacencyTaggedPGXStatementVisibleToPGStatStatements` and `TestP5AdjacencyCalibrationAttributesTaggedStatementWAL`. Both reported PostgreSQL **42501**, permission denied to create that extension. Evidence: `.coverage/resume-qualification-final.log`. The original adjacency-table ownership errors no longer reproduce; `drivers/pg/query/TestP5AdjacencyShadowLifecycle` passes.
- **Environment action:** requested that an administrator install `pg_stat_statements` in the selected test database, with `shared_preload_libraries` configured as documented in `README.md`. The supplied test role cannot perform that installation. No privilege changes or administrator-credential access were attempted. README now states the administrator prerequisite for restricted test roles.
- **Final source checkpoint:** the bound-root/constant admission regressions were added after the broad run above. Their focused unit/semantic checks and the complete final-source race-enabled `make test` pass. The final focused PostgreSQL command was rerun after the last source change: all **97 shared variants and 194 explicit-mode executions** pass, along with raw integer ID types, committed CREATE persistence, forced reverse admission, and v4/v5 production policy routing (exit 0). No database-mutating runs overlap. No live test/build commands remain.
- **Separate BDD command:** `TZ=America/New_York CGO_ENABLED=0 python3 .coverage/run_stage2_validation.py make test_bdd_integration` completed with **exit 0**, but that status does not qualify the suite. All **8 DAWGS scenarios passed**. The imported TCK corpus has **2 passed, 11 failed, and 7 otherwise undefined scenarios**. The CLI reports 8 undefined cases because its failed undefined-variable scenario also contains an undefined negative-error assertion. Across both folders there are 28 unique scenarios: 10 passed and 18 nonpassing. This matches the original 11-failure/8-undefined CLI baseline. Evidence: `.coverage/resume-bdd.log`, `.coverage/opencypher-tck-results.json`, and `.coverage/opencypher-tck-report.html`.
- **BDD classification:** six failures request unsupported `+labels`/`+properties` side-effect assertions; one compares quoted versus unquoted string formatting; three encounter unsupported returned `float64` formatting (including the large-integer scenario, whose precision is therefore not established by the BDD assertion); one expected negative compile-error scenario is treated as a step failure rather than an expected error. Seven other negative scenarios lack error assertion step definitions. These are separate harness/qualification work, not evidence that F1–F7 remain open. `bdd/steps_test.go:TestMain` intentionally gates only DAWGS-folder failures or invalid reports and ignores the general `m.Run` status, explaining the zero command exit.
- **Next action:** after the extension prerequisite is satisfied, rerun the two named PostgreSQL failures. Then repair/classify the imported TCK harness gaps and revisit the two out-of-scope observations recorded in the stage-1 entry (count projection order and raw-query optimizer-switch behavior) before declaring overall translator qualification.
- **Handoff:** F1–F7 are closed; F3 deliberately routes unsupported shapes to the incumbent and introduces no new performance qualification claim. Overall translator qualification remains incomplete for the separately recorded extension and TCK prerequisites. All changes remain an uncommitted patch on `c4776de`; no commit, history rewrite, or unrelated user-work removal was performed.

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

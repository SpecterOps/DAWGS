# Prioritized variance findings and work plan

Prepared on **2026-10-22** from [variance_findings.md](variance_findings.md), validated against checkout **`0bf83edb8155d915c55f559bae212a4526e4607f`**. This document adds engineering judgment and delivery order to that evidence. It preserves the underlying dispositions: **90 retained findings, 4 needing runtime validation, 10 contract questions, and 9 resolved original findings**. The 22 additional observations have a separate queue.

**Prioritize the confirmed schema-selection and pool-acquisition repairs, the pending regex migration decision, and the selected open-ended traversal implementation.** The largest sustained work is compiler semantics: row identity, predicate lifetime, type preservation, and traversal correctness. Smaller utility and error-handling fixes can ship while those programs develop.

## Priority and sizing rules

| Priority | Scheduling meaning | Basis |
| --- | --- | --- |
| P1 | Start these repair or investigation tracks first. | Silent durable-write loss, wrong query reuse, incomplete core analysis, sensitive-data handling, compiler panic containment, or high-consequence source paths that need urgent confirmation. |
| P2 | Next correctness and reliability wave; pull small independent repairs forward when capacity permits. | Remaining value/query correctness, detectable query failures, lifecycle hardening, narrower backend paths, and important API decisions. |
| P3 | Explicit support/design backlog or a bounded investigation before implementation. | Accepted compatibility/model choices, clean feature rejections, lower-exposure validation failures, or historical measurements without a current fixture. |

These are proposed engineering priorities. The report does not establish current production incident rates or deployment exposure. P1 can mean **reproduce first**: it never upgrades source inspection or a contract question into a demonstrated backend defect. Backend, entry point, and data prerequisites still apply.

**Action labels:** Repair = implement the retained behavior correction; Contain = return a clear error, or decline unsafe lowering only when the fallback is valid, while fuller support is scoped separately; Reproduce = obtain the missing controlled fixture before choosing a repair; Decide = choose/document an API promise; Design = scope a semantic, lifecycle, model or performance change; Enable = improve the validation signal. These are proposed tasks, not implementation already completed.

**Effort describes the listed next step, including its focused regression and documentation:** S = localized change/check; M = coordinated subsystem work or a backend fixture; L = interacting compiler/driver/algorithm paths plus a broader regression matrix; XL = storage/public-behavior redesign. A small decision or containment step does not price the eventual implementation. Sizes are relative and do not imply calendar estimates, equal-sized findings, or additive project totals.

The ordering assumes both supported backends and the shared analysis/tooling APIs matter. Bring Neo4j-specific rows forward for a Neo4j deployment. Bring 098 forward when an exposed endpoint relies on the result budget, and 082 when temporal export fidelity is required. Scrub and directory-input rows matter when those workflows are used; no remote exploit or full application replay is established here. A read-only service may put mutation work behind its active read/analysis paths.

## Original triage baseline

The counts in this section describe the original triage baseline, including completed actions; they are not current backlog totals. The active queue now has **52 items: 4 P1, 39 P2 and 9 P3**. The [completion ledger](variance_findings_priority_complete.md) holds **52 completed actions** and the nine originally resolved regression controls.

Each numbered finding has **one primary workstream** below, so counts do not double-count shared code. Retained means Applies or Partially applies in the validated report. Questions combine its unverified and contract dispositions. Resolved findings remain regression controls.

| Code | Primary workstream | Retained | Questions | Resolved | Baseline next steps | Main effort concentration |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Q | Cypher parsing, patterns, clauses and mutations | 41 | 3 | 7 | 44 | L; XL for storage/resource policy: Predicate lifetime, input-row identity, path uniqueness, read boundaries; exact-length parsing is a smaller independent fix. |
| R | Result values, numeric types and budgets | 8 | 1 | 1 | 9 | M–L: Exact numbers and JSON/string identity across projection/decoding; nested result accounting has separate cost and completeness questions. |
| B | Query builders, rewriting and compilation cache | 6 | 0 | 0 | 6 | M–L: Semantic cache identity can be contained independently; formatter, builder and hand-built upsert emitters need separate coverage. |
| D | Drivers, schema, kinds and batching | 15 | 7 | 1 | 22 | M–L; some S repairs: Confirm destructive schema and pool paths early; coordinate manager locks, batch identity, deletion propagation and lifecycle contracts. |
| A | Analysis, traversal, cardinality and containers | 13 | 1 | 0 | 14 | M–L; several S helpers: Reachability, bitmap and lock invariants; traversal transitions; error/cancellation propagation. Several helpers can ship independently. |
| I | Import, export and scrubbing | 6 | 2 | 0 | 8 | S–M repairs; L/XL recovery choices: Nested scrub policy, exact import numbers and root-aware file reads can proceed together; temporal format and recovery guarantees need decisions. |
| V | BDD validation and reporting | 1 | 0 | 0 | 1 | M: Provide a reliable gate for the selected supported scenarios and distinguish upstream reporting from enforced conformance. |
| **Total** | | **90** | **14** | **9** | **104** | |

**55 of the 90 retained findings (61%) concern query semantics, builders/cache, or result values** across Q/R/B. Their shared implementations create the biggest coordination cost. Drivers account for 15, analysis/traversal/containers for 13, transfer tooling for 6, and validation for 1. Counts measure breadth; they do not measure engineering duration or independent root causes.

The baseline plan had **36 P1, 59 P2, and 9 P3 next steps**, plus 9 resolved controls. By next action there are **68 repairs, 9 containment tasks, 9 reproductions, 12 decisions, 5 designs, and 1 validation enabler**. This includes DAWGS-007's reclassification from Repair to Decide for a downstream audit and migration decision; its finding disposition, priority and effort remain unchanged. By next-step size there are **33 S, 58 M, 12 L, and 1 XL**. Many S rows are bounded rejection, contract or utility work; completed support for list operations, procedures, traversal policy, atomic recovery or multigraph storage can be much larger.

The original 36 P1 rows formed several parallel work packages. Start the first available item in each active lane, preserving the dependencies below; one engineer should not have to finish all compiler work before independent driver or tooling work can begin.

## Open work ledger

Completed actions, original queue details and their implementation/validation notes have moved to the [completion ledger](variance_findings_priority_complete.md). These four work items still have pending repairs or decisions. Reproduction and design completion do not close their runtime defects. Credentials are excluded from evidence.

| Order | Finding | Status | Work and validation |
| ---: | --- | --- | --- |
| 3 | DAWGS-063 | Reproduction complete; repair open | A fresh PostgreSQL manager selecting an existing graph by name through `ReadTransaction` drops both node and edge property indexes and uniqueness constraints (four objects total). The call succeeds and returns count 0. Selection/schema-reconciliation separation remains to implement. |
| 14 | DAWGS-007 | Downstream audit and migration decision — pending | Preserve the current PostgreSQL substring-matching default while auditing downstream raw and parameterized regex usage, including negated filters and mutation targets. Decide the migration policy before changing matching scope. Assess literal/parameter escaping separately because its correction can also change results. |
| 23 | DAWGS-019 | Design action complete; runtime repair open | Selected complete traversal semantics with explicit resource failure. A controlled chain confirms silent truncation in both PostgreSQL compiler modes: open queries omit depths 16/17 and SET/CREATE affect 15 rather than 17 rows. The design records the API guarantee, versioned migration, L implementation scope and resource/semantic acceptance gates. A separate optimized all-shortest scalar discrepancy remains open. See the policy and evidence below. |
| 25 | DAWGS-065 | Reproduction complete; repair open | Live PostgreSQL probes confirm the nested acquisition. On a verified one-connection pool, CreateNode with an uncached kind waits its full context budget and returns `context deadline exceeded` without committing. With all connections held by registering transactions, one registration holds the schema write lock while waiting for a second connection, others block on the lock, and an unrelated reader holding its own connection stalls until a registration context expires. MapKinds misses wedge through the nested read path; cached-kind concurrency at the pool maximum completes. The probes are pinned as bounded driver-scoped regressions the connection-ownership repair must invert. See below. |

### DAWGS-063 controlled fixture and outcome — 2026-10-22

Create an empty disposable PostgreSQL graph through schema assertion with this definition (use a unique name):

```go
graph.Graph{
    Name: "variance-schema-probe",
    NodeIndexes: []graph.Index{{Field: "name", Type: graph.BTreeIndex}},
    NodeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}},
    EdgeIndexes: []graph.Index{{Field: "name", Type: graph.BTreeIndex}},
    EdgeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}},
}
```

Record the four property indexes in `pg_indexes` for its `node_<id>` and `edge_<id>` partitions. Construct a fresh driver/manager, without asserting this graph into its cache, and run:

```go
fresh.ReadTransaction(ctx, func(tx graph.Transaction) error {
    _, err := tx.WithGraph(graph.Graph{Name: "variance-schema-probe"}).Nodes().Count()
    return err
})
```

Expected: count 0, all four property indexes/constraints retained. Observed: count 0, no error, all four objects removed and the removal visible from an independent PostgreSQL connection after the read transaction. The removed objects were `node_<id>_name_index`, `node_<id>_objectid_constraint`, `edge_<id>_name_index`, and `edge_<id>_objectid_constraint`. The graph and its partitions were removed after inspection. Repair remains open: graph selection must resolve existing metadata without schema reconciliation; explicit schema assertion must retain its reconciliation role.

### DAWGS-007 downstream audit and migration decision — 2026-10-23

At the user's request, the next action changes from **Repair** to **Decide**, retaining P1 priority, M effort and the finding's Applies disposition. Changing PostgreSQL regex matching from substring search to full-string matching is a behavior change for existing callers. It can remove positive matches, add matches under negation, and change the entities selected by SET/DELETE predicates. The current PostgreSQL default remains in place pending a downstream audit and an explicit migration decision. No regex implementation or default changes are part of this ledger update.

The audit must inventory actual downstream raw Cypher and parameterized patterns, identify callers relying on substring behavior, and compare returned rows and mutation selection under current and proposed matching semantics. Repository examples alone cannot establish downstream dependence. Cover flags, alternation, multiline values and backslashes, and assess the literal/parameter escaping defect independently; fixing escaping can also change results.

Completion requires a recorded compatibility policy and migration plan: choose whether full-string matching needs an opt-in policy before a versioned default change, define the supported regex behavior, document affected caller changes and rollback, and select the release boundary for any default switch. Audit evidence and the migration decision remain open; no opt-in mechanism or default switch has been implemented.

### DAWGS-019 open-ended traversal design and runtime evidence — 2026-10-24

The selected [open-ended traversal policy](docs/open_ended_traversal_policy.md) promises complete ordinary, shortest and all-shortest query semantics, with identifiable failure if execution resources prevent completion. Explicit Cypher upper bounds remain semantic limits. Execution budgets must not silently narrow a query or permit partial writes to commit. The document defines streaming/error behavior, budget/fallback accounting, schema/selector/cache migration, downstream result/write impact and the shared semantic plus driver-scoped resource acceptance matrix. This completes the queue's **M design action**; the coordinated runtime implementation is **L** and remains open. Current execution still caps omitted maxima at 15; no cap removal or rejection was implemented here.

An 18-node / 17-edge chain supplies **80 variants on Neo4j and 80 in each PostgreSQL compiler mode**, all in rollback transactions. Both PostgreSQL modes return no rows for open ordinary/shortest/all-shortest queries targeting depths 16 and 17, without errors. A minimum-only ordinary `*16..` also returns no rows. Explicit upper bounds reach those endpoints, and `RETURN p` checks the complete ordered nodes and relationship counts. Open SET and CREATE queries without RETURN affect **15 rows on PostgreSQL versus 17 on Neo4j**. Comparison identifies **28 truncation discrepancies per PostgreSQL mode**. The complete fixture, query matrix, observed results and limitations are in the policy document; this small chain is not resource/performance qualification or downstream service replay.

The positive controls also identify **eight scalar discrepancies from an independent projection issue with optimization enabled**: singleton `allShortestPaths` with minimum one returns `length(p) = 0` for a present 15–17-hop path. Disabled translation and Neo4j return the actual depth, and full `RETURN p` witnesses remain correct. The bounded 16-hop reproducer and separate repair/containment requirement are recorded without changing the original triage counts. The investigation-only overlay logs observations, so its successful execution is not a conformance pass; no shared expected assertions were weakened.

Baseline validation passes: the PostgreSQL-selected **`make test_all`** completes its race-enabled unit and integration stages and final BDD stage, with unit statement coverage unchanged at **66.4%** at reported precision. The complete Neo4j-selected **`make test_integration`** and subsequent **`make test_bdd_integration`** also pass. Both full shared integration suites pass on each backend. Each BDD gate reports **10 enforced / 0 failed**, **18 report-only / 18 failed**. Runs use each supplied connection separately, `TZ=Etc/UTC CGO_ENABLED=1 CC=/opt/codices/development/bin/cc GOFLAGS='-ldflags=-linkmode=external'`, and sequential BDD report writes. The investigation overlay was Go-formatted; new local Markdown links and `git diff --check` pass. The final patch changes documentation only. These existing gates do not enforce the investigation's new failing semantics or qualify resource behavior; both runtime defects remain open.

The next ordered work item is **DAWGS-072**, cancellation and error propagation for parallel fetch operations. DAWGS-019's runtime repair stays open under the selected design. DAWGS-007 remains pending downstream audit evidence and a migration decision; earlier reproduction-only rows 039/063 and the expression-sequencing follow-up under 045 remain open. Credentials are excluded from this ledger.

### DAWGS-065 kind-registration pool wedge — 2026-10-24

The retained source mechanism is confirmed live on PostgreSQL and pinned as driver-scoped regressions in `drivers/pg/kind_registration_pool_integration_test.go` (`manual_integration`; the probes skip unless `CONNECTION_STRING` selects PostgreSQL). Every probe builds its pool through `NewPoolWithRuntimeConfig` with explicit `RuntimeConfig` limits and asserts the effective `Stat().MaxConns()` first, because pools without explicit runtime configuration keep the v1 default of 50 connections. Fixtures warm all physical connections before timing so the measured windows contain lock and connection waits, not connection setup. Kind names use a `DAWGS065` prefix; cleanup removes the probe graph's nodes/edges and those kind rows, and the graph partition remains like the existing `pg_v2_integration` fixture.

| Probe / shape | Outcome |
| --- | --- |
| Single write transaction, effective MaxConns 1, CreateNode with an uncached kind | Waits the full 750 ms budget; returns `context deadline exceeded`; node count and kind rows unchanged. |
| Saturated pool, MaxConns 3: two registering writers plus one unrelated reader, all holding connections at a barrier | The schema write lock is sampled continuously unavailable; both writers fail with `context deadline exceeded` at their 2 s deadlines although each already holds a connection; the reader stalls in target-graph resolution behind the same lock and completes with count 0 only after those deadlines; nothing commits and no wedge kind is defined. A fresh registration afterwards succeeds, proving the wait cycle is released by context expiry/connection release rather than pool damage. |
| Read-side family: one connection held by an unrelated read, MapKinds for an uncached kind | The miss takes the schema write lock and refreshes through a nested read transaction; it waits its full budget and returns `context deadline exceeded`; after release the same miss completes promptly with `unable to map kinds` and defines nothing. |
| Control: two concurrent writes at MaxConns 2 with an already-cached kind | Both complete immediately and commit; the AssertKinds fast path needs no second connection. |

The saturated-pool probe samples the schema lock with `RWMutex.TryLock` polls rather than sleeps, so the unrelated reader is released only after a registration demonstrably holds the write lock; that reader already holds its own connection at release, isolating the lock wait from pool acquisition. All probes use bounded contexts and terminate deterministically; they pin current wedged behavior, and the connection-ownership repair must invert them so kind registration inside a transaction never needs a second connection while holding the schema lock, matching the finding's regression criterion. The batch registration paths in `drivers/pg/batch.go` share the same `AssertKinds` entry and were not separately replayed.

Validation: the four probes pass under the race detector with repeated runs (`-count=3`, and `-count=2` after the read-side addition). The full `drivers/pg` tagged integration package passes on PostgreSQL (**69.2%** package statement coverage) and on Neo4j (the PostgreSQL-scoped probes skip). The PostgreSQL-selected `make test` passes (**66.6%** overall unit statement coverage, unchanged from the last ledger gate), and both backends pass `make test_integration` and `make test_bdd_integration`; each gate reports **10 enforced / 0 failed**, **18 report-only scenarios / 18 failed**. The three-probe version of the file ran in the unrestricted broad gates; the later read-side addition re-ran the complete affected package on both backends. Runs use each supplied connection separately with `TZ=Etc/UTC CGO_ENABLED=1 CC=cc GOFLAGS='-ldflags=-linkmode=external'`. Repository lint, `gofmt`, default and integration-tagged vet, and `git diff --check` pass. No Go edits followed the final package retries.

The next ordered work item is **DAWGS-099**, the ThreadSafeDuplex cross-operation deadlock repair. DAWGS-019's runtime repair stays open under the selected design. DAWGS-007 remains pending downstream audit evidence and a migration decision; earlier reproduction-only rows 039/063 and the expression-sequencing follow-up under 045 remain open. Credentials are excluded from this ledger.

### Live acceptance completed — 2026-10-28

Both supplied database services are reachable. DAWGS-015, DAWGS-014, DAWGS-040,
DAWGS-021 and the next implementation item, DAWGS-036, now have live acceptance
on Neo4j and in both PostgreSQL compiler modes. Their implementation history and
current validation results have moved to the [completion ledger](variance_findings_priority_complete.md#dawgs-036-pattern-predicate-lifetime-and-live-acceptance--2026-10-28).
The current shared corpora contain **50, 60, 44, 57 and 22 variants**, respectively;
all **699 backend/mode variant executions** pass, as do the independent integer
type/boundary checks. Both backend integration and BDD gates pass, with **17
enforced scenarios and zero enforced failures** on each backend. The 18
report-only compatibility failures on each backend remain visible.

DAWGS-035 and DAWGS-046 now have live acceptance on both backends; their
implementation and validation notes are in the
[completion ledger](variance_findings_priority_complete.md#dawgs-046-multipart-read-boundaries--2026-10-28).

### DAWGS-047 live acceptance — 2026-10-29

DAWGS-047 now has live acceptance on both backends and in both PostgreSQL compiler
modes; its implementation and validation notes are in the
[completion ledger](variance_findings_priority_complete.md#dawgs-047-order-by-alias-expressions-and-binding-identity--2026-10-29).
The earlier P1 repairs and regex migration decision remain open in their existing
lanes.

### DAWGS-042 live acceptance — 2026-10-29

DAWGS-042 now has live acceptance on both backends and in both PostgreSQL compiler
modes. Its implementation, regression evidence and complete validation results
are in the [completion ledger](variance_findings_priority_complete.md#dawgs-042-property-list-unwind--2026-10-29).
DAWGS-043 is completed below. Earlier pending repairs and
separate follow-ups retain their existing status.

### DAWGS-043 live acceptance — 2026-10-29

Final DELETE projections now consume the complete input stream. SET/CREATE before
DELETE uses the existing transaction-scoped staged executor so PostgreSQL observes
the earlier writes. Implementation, shared mutation/template regressions and live
validation are recorded in the
[completion ledger](variance_findings_priority_complete.md#dawgs-043-delete-continuation-and-write-boundaries--2026-10-29).
**DAWGS-023 is the next implementation item.** Earlier pending repairs and
separate follow-ups retain their existing status.

### Open follow-ups to completed actions

| Finding | Remaining work | Completed evidence |
| --- | --- | --- |
| DAWGS-045 | Full sequential evaluation of assignment expressions, caller-row sequencing of non-idempotent writes, and writes through aliases of the same entity remain open. With initial `n.x = 0`, `SET n.x = 1 REMOVE n.x SET n.y = coalesce(n.x, -1) RETURN n.y` returns 0 on PostgreSQL and -1 on Neo4j; inserting `WITH n` before the last SET returns -1 on both. Preserve separate regressions. | [Ordered intent repair and sequencing probe](variance_findings_priority_complete.md#dawgs-045-ordered-intent-repair-and-checks--2026-10-22) |
| DAWGS-087 | Whole-batch rollback remains a separate lifecycle/recovery contract under 106/109/112. With `WithBatchSize(1)`, a delegate error leaves earlier PostgreSQL identity-upsert flushes committed; Neo4j rolls back the small fixture. The completed repair guarantees atomicity only for its two-write kind-removal flush. | [Flush boundary evidence](variance_findings_priority_complete.md#dawgs-087-reproduction-repair-and-flush-boundary--2026-10-22) |
| DAWGS-003 | Decide whether to promise full scalar-list quantifier projections; the completed PostgreSQL containment returns a normal translation error. Full support is separate M/L work. | [Quantifier containment](variance_findings_priority_complete.md#dawgs-003-scalar-list-quantifier-containment--2026-10-25) |
| DAWGS-011 | Complete temporal component support remains separate M work after explicit PostgreSQL rejection and compiler panic containment. | [Temporal containment](variance_findings_priority_complete.md#dawgs-011-temporal-component-rejection-and-panic-containment--2026-10-26) |
| DAWGS-030 | DAWGS-036 repairs generic pattern-predicate lifetime. The aggregate traversal-count optimization still explicitly rejects pattern predicates; qualify a safe generic fallback or implement the specialized lowering separately. | [Aggregate containment](variance_findings_priority_complete.md#dawgs-030-aggregate-pattern-predicate-containment--2026-10-26) |
| DAWGS-047 | Separate unary numeric typing remains: `MATCH (n:OrderSource) RETURN n.k AS x ORDER BY -x` reaches PostgreSQL's unsupported `- jsonb` operation after alias resolution. Qualify and repair numeric property/JSON unary operators separately. Native numeric alias negation is covered by the completed repair. | [Alias resolution repair and scope](variance_findings_priority_complete.md#dawgs-047-order-by-alias-expressions-and-binding-identity--2026-10-29) |

### Deferred performance follow-ups

These follow-ups are scheduled for completion **after the semantic fixes in the ordered queue**. They preserve finding counts and priority ordering. The DAWGS-073 correctness repair is complete; this performance follow-up remains open.

| Finding | Status | Work and completion criteria |
| --- | --- | --- |
| DAWGS-073 — bulk reachability throughput | Open; deferred until semantic fixes are complete | Recover throughput when querying many different roots while retaining complete cache entries, bounded working memory, and safe eviction/concurrent reads. Investigate selective prewarming or another reuse strategy without restoring partial intermediate results. |

Recorded on **2026-10-23** at the user's request. The [recorded 512-root, 64-entry-cache sweeps](variance_findings_priority_complete.md#dawgs-073-complete-requested-component-reach--2026-10-23) regress from **2.734 ms to 6.489 ms (2.37×)** for a chain and **2.714 ms to 9.434 ms (3.48×)** for converging shortcuts. Removing intermediate prewarming causes repeated traversal for newly requested roots. Cold-root queries improve and repeated-root hits remain comparable; application-scale impact is still unmeasured.

Completion requires preserving the DAWGS-073 semantic and race regressions, then repeating the cold-root, warm-root and all-roots benchmarks in `algo/reach_bench_test.go` against the repaired implementation. Add larger graphs, varied cache capacities and representative bulk reachability workloads; report runtime, allocations and peak memory. Measure against the historical baseline where its results are correct, including the chain, and verify every optimized result against an independent traversal. Record the recovered throughput, remaining regressions and explicit memory bounds before closing this follow-up.

## Ordered next-step queue

The 52 unfinished numbered work items appear once in this queue. Completed actions and their original queue entries are in the [completion ledger](variance_findings_priority_complete.md). Original order numbers are retained; separate follow-ups to completed actions remain visible above. The ID links to the original validated evidence, including corrected subcases and runtime limits. Track codes refer to the workstream table. The global order is a default next-start order; independent tasks can run concurrently.

### P1: Start first — 4 items

| Order | Finding | Track | Action | Effort | Next step and reason |
| ---: | --- | :---: | --- | :---: | --- |
| 3 | [DAWGS-063](variance_findings.md#dawgs-063) — WithGraph drops existing indexes and constraints | D | Repair | M | Separate graph selection from schema reconciliation; preserve existing indexes and constraints during reads. The destructive read-triggered behavior is reproduced above. |
| 14 | [DAWGS-007](variance_findings.md#dawgs-007) — Regex match is a substring search | Q | Decide | M | Downstream audit and migration decision: preserve the current default, audit raw/parameterized patterns and read/negated/mutation behavior, then choose an opt-in policy or versioned default migration. Assess literal/parameter escaping independently; both corrections can change results. |
| 23 | [DAWGS-019](variance_findings.md#dawgs-019) — Unbounded traversals stop at 15 hops | Q | Repair | L | Implement the selected complete-traversal policy with explicit resource failure (L scope). Preserve the recorded long-path and write-state controls; separately repair the optimized all-shortest scalar discrepancy. |
| 25 | [DAWGS-065](variance_findings.md#dawgs-065) — Kind mapping can deadlock the connection pool | D | Repair | M | Remove nested pool acquisition during kind registration and MapKinds misses; invert the bounded pool-wedge regressions recorded above. |

### P2: Next correctness and reliability wave — 39 items

| Order | Finding | Track | Action | Effort | Next step and reason |
| ---: | --- | :---: | --- | :---: | --- |
| 57 | [DAWGS-023](variance_findings.md#dawgs-023) — An unknown label raises an error | Q | Repair | M | Resolve an unregistered match kind as an empty match without registering it or broadening the query. |
| 58 | [DAWGS-001](variance_findings.md#dawgs-001) — List indexing returns the index | Q | Contain | S | Reject unsupported list indexing explicitly now; implement full null/negative/out-of-range indexing as separately scoped M/L work. |
| 59 | [DAWGS-002](variance_findings.md#dawgs-002) — List slicing returns the upper bound | Q | Contain | S | Reject unsupported slicing explicitly now; full slicing should share list AST/type work with 001. |
| 60 | [DAWGS-017](variance_findings.md#dawgs-017) — A standalone CALL parses to `(nil, nil)`; formatting that nil query panics | Q | Contain | S | Return an explicit standalone CALL rejection or a nonnil supported AST; never format a successful nil query. |
| 61 | [DAWGS-010](variance_findings.md#dawgs-010) — shortestPath used as an expression returns a node | Q | Contain | S | Reject unsupported shortestPath expression position instead of returning the terminal node. Full expression support is L feature work. |
| 62 | [DAWGS-013](variance_findings.md#dawgs-013) — `CREATE UNIQUE` and `LOAD CSV` are accepted and dropped | Q | Contain | S | Reject silently dropped CREATE UNIQUE/LOAD CSV at the text compiler boundary; formatter rejection alone does not close the raw-text path. |
| 63 | [DAWGS-103](variance_findings.md#dawgs-103) — In-query CALL omitted from generated SQL | Q | Contain | S | Reject accepted in-query CALL on unrestricted compiler paths; preserve existing DefaultCypherContext restrictions. Full procedure execution is separate XL feature work. |
| 64 | [DAWGS-059](variance_findings.md#dawgs-059) — Neo4j builder rewrite changes Boolean kind predicates | B | Repair | M | Repair the remaining relationship-kind conjunction becoming R|T. The original OR-based Enterprise caller is fixed and supplies no current residual exposure claim. |
| 65 | [DAWGS-060](variance_findings.md#dawgs-060) — tx.Query rewrite re-emits a lossy AST | B | Repair | L | Preserve namespaces, conjunctive WHERE labels and integral float types whenever pattern-map rewriting re-emits the AST. Vendor hints/prefixes need a separate support promise. |
| 66 | [DAWGS-085](variance_findings.md#dawgs-085) — Xor rendering loses builder grouping | B | Repair | S | Parenthesize/group XOR consistently when embedded in AND; cover both backend render/translation paths. |
| 67 | [DAWGS-055](variance_findings.md#dawgs-055) — Result aliases are emitted unquoted | R | Repair | S | Preserve case and decoded alias spelling when quoting result-column identifiers; unusual aliases now avoid the old syntax failure. |
| 68 | [DAWGS-061](variance_findings.md#dawgs-061) — Kind and identity-property names lack escaping | B | Repair | M | Escape valid kind and identity-property names in the separate hand-built Neo4j relationship-upsert emitter. |
| 69 | [DAWGS-071](variance_findings.md#dawgs-071) — Dumping an unknown graph name creates that graph | D | Repair | M | Make missing-graph dump selection intentional and non-mutating where required; share select/assert separation with 063 rather than opening a second schema redesign. |
| 70 | [DAWGS-067](variance_findings.md#dawgs-067) — AssertSchema never creates the non-default graphs it is given | D | Repair | M | Assert all requested non-default graph definitions after separating graph selection from schema reconciliation. |
| 71 | [DAWGS-089](variance_findings.md#dawgs-089) — Kind-map getter returns shared map after unlocking | D | Repair | S | Return owned kind-map snapshots and protect iteration. A concurrent runtime race has not been reproduced; shared mutable map exposure is source-supported. |
| 72 | [DAWGS-088](variance_findings.md#dawgs-088) — Concurrent kind insert can return no rows | D | Reproduce | M | Use a synchronized two-session kind insertion schedule before changing conflict/snapshot handling. |
| 73 | [DAWGS-069](variance_findings.md#dawgs-069) — SelectKindID loads the wrong SQL file | D | Repair | S | Load the intended SelectKindID SQL and bind its expected parameter; validate exact current executor behavior. |
| 74 | [DAWGS-090](variance_findings.md#dawgs-090) — Mixed-case index field causes repeated reconciliation | D | Reproduce | M | Confirm mixed-case index-name folding and repeated DDL, then repair naming compatibly with existing installations. |
| 75 | [DAWGS-068](variance_findings.md#dawgs-068) — Updating with no changes or nil properties panics | D | Repair | S | Normalize or reject nil properties and handle unchanged updates intentionally. Only the nil-node panic is currently reproduced; empty SQL dispatch is not a live database panic. |
| 76 | [DAWGS-092](variance_findings.md#dawgs-092) — Neo4j nil-properties dereference | D | Reproduce | S | Pin Neo4j nil-property behavior with builder/driver doubles, then implement a consistent reject-or-empty contract alongside 068. |
| 77 | [DAWGS-075](variance_findings.md#dawgs-075) — Relationship-cache missing-ID slice | A | Repair | S | Query missing relationship IDs safely and handle fewer fetched rows without negative slicing. |
| 78 | [DAWGS-079](variance_findings.md#dawgs-079) — Pagination helper panics beyond result bounds | A | Repair | S | Bound pagination slices for empty sets and oversized skip/limit, with explicit negative-input behavior. |
| 79 | [DAWGS-078](variance_findings.md#dawgs-078) — BFSTreeFile gzip reader and SerializedSegment reconstruction | A | Repair | M | Read decoded gzip bytes and reconstruct serialized paths without negative edge indexing; verify round trips and errors. |
| 80 | [DAWGS-096](variance_findings.md#dawgs-096) — Acyclic helpers replace caller ExpansionFilter | A | Repair | M | Compose the caller expansion filter with cycle control. Specify pruned-terminal, counting and limit semantics before altering those separate behaviors. |
| 81 | [DAWGS-097](variance_findings.md#dawgs-097) — BreadthFirst hides cancellation error | A | Repair | M | Expose cancellation consistently from BreadthFirst and specify whether any partial result is usable. |
| 82 | [DAWGS-091](variance_findings.md#dawgs-091) — Cursor Close does not join result producer | D | Reproduce | M | Control Next/Close overlap and cancellation under race detection before changing producer shutdown/join order. |
| 83 | [DAWGS-093](variance_findings.md#dawgs-093) — Neo4j execution uses context-free API | D | Design | M | Plan context-aware Neo4j session/transaction/result execution and then qualify live post-dispatch cancellation. The migration/repair may be L. |
| 84 | [DAWGS-094](variance_findings.md#dawgs-094) — DatabaseSwitch settings, driver check and switch-state synchronization | D | Repair | M | Forward size setters and correct raw/wrapped driver checks; reproduce and repair switch-state synchronization separately. |
| 85 | [DAWGS-098](variance_findings.md#dawgs-098) — FetchByQuery charges shallow result sizes | R | Repair | L | Define and enforce incremental accounting for literals and nested graph values. Promote to P1 where an exposed endpoint relies on this limit; no production peak-memory/OOM result is established. |
| 86 | [DAWGS-081](variance_findings.md#dawgs-081) — Case-insensitive domain scrub option unused | I | Repair | S | Honor requested case-insensitive domain identity consistently with references; review declaration-only scrub options in the same policy pass. |
| 87 | [DAWGS-082](variance_findings.md#dawgs-082) — Neo4j temporal values serialize as empty objects | I | Design | M | Choose a temporal export/import representation that preserves required type/zone semantics. Promote to P1 where Neo4j temporal dump fidelity is required; implementation may be M/L. |
| 88 | [DAWGS-083](variance_findings.md#dawgs-083) — Trailing slash unpack directory fails | I | Repair | S | Normalize the output directory before staging/promotion; cover trailing separators, existing paths and force safety. |
| 89 | [DAWGS-109](variance_findings.md#dawgs-109) — Neo4j write-count transaction rotation | D | Decide | M | Choose and document WriteTransaction atomicity versus counted-write rotation, then run a small-threshold tail-failure fixture. Changed lifecycle semantics may be L/XL. |
| 90 | [DAWGS-112](variance_findings.md#dawgs-112) — Failed multi-batch Load leaves state and prevents ordinary retry | I | Decide | M | Choose multi-batch load recovery/retry guarantees and test committed-prefix failure. Documentation/cleanup can precede any L/XL resume or staging design; 109 does not settle this promise. |
| 91 | [DAWGS-106](variance_findings.md#dawgs-106) — Batch flush groups operations by kind | D | Decide | S | Specify grouped versus caller-ordered batch mutations or rejection of order-sensitive mixtures. Preserving arbitrary order is potential L work. |
| 92 | [DAWGS-110](variance_findings.md#dawgs-110) — AssertSchema owns entire Neo4j schema and rejects composites | D | Decide | S | Define Neo4j schema ownership and document composite-index support; deliberate explicit reconciliation differs from read-triggered schema changes in 063. |
| 93 | [DAWGS-107](variance_findings.md#dawgs-107) — Explicit IDs do not adjust node sequence | D | Decide | M | Establish caller/driver ownership of explicit-ID sequence synchronization using isolated concurrent PostgreSQL allocation tests. |
| 94 | [DAWGS-113](variance_findings.md#dawgs-113) — ParseDocument permits unknown keys and trailing bytes | I | Decide | S | Choose graph-presence, unknown-field and EOF requirements independently; preserve valid empty graphs and implement lossless numeric decoding independently of strictness policy. |
| 95 | [DAWGS-066](variance_findings.md#dawgs-066) — Changing the default graph is silently ignored after the first one | D | Decide | S | Reconcile the public default-graph setter with internal immutability. An explicit documented rejection may be preferable to silently promising a successful change. |

### P3: Support, model and lower-urgency decisions — 9 items

| Order | Finding | Track | Action | Effort | Next step and reason |
| ---: | --- | :---: | --- | :---: | --- |
| 96 | [DAWGS-005](variance_findings.md#dawgs-005) — Required semantic validation is missing | Q | Repair | M | Add the recorded bound-CREATE and duplicate-alias semantic checks; do not treat these two examples as a complete semantic-validation program. |
| 97 | [DAWGS-012](variance_findings.md#dawgs-012) — A comment before an arithmetic operator shifts the operator sequence | Q | Repair | S | Correct arithmetic operator token collection around comments; the current trigger returns a visible translation error. |
| 98 | [DAWGS-049](variance_findings.md#dawgs-049) — CREATE retains a null-valued property | Q | Design | M | Specify property-null omission/storage and observable key/count semantics consistently with the existing null support boundary. |
| 99 | [DAWGS-104](variance_findings.md#dawgs-104) — Historical CREATE SQL expansion size | Q | Reproduce | M | Recover the missing many-CREATE fixture and measure current compilation output/resources before pricing or claiming a performance repair. |
| 100 | [DAWGS-022](variance_findings.md#dawgs-022) — Parallel same-type relationships are rejected | Q | Design | XL | Decide multigraph support and review schema migration, ingestion/upsert identity and downstream assumptions. Removing a uniqueness constraint alone is insufficient. |
| 101 | [DAWGS-037](variance_findings.md#dawgs-037) — Pattern map parameters use jsonb containment | Q | Decide | S | Choose equality versus containment for the whole-map MATCH extension before implementing a semantic change; follow-on work may be M/L. |
| 102 | [DAWGS-105](variance_findings.md#dawgs-105) — AsNumeric truncates and wraps | R | Decide | S | Document ordinary casts versus a checked/lossless numeric API; consider additive checked conversion rather than silently breaking existing callers. |
| 103 | [DAWGS-108](variance_findings.md#dawgs-108) — Neo4j relationship upsert sets endpoint lastseen | D | Decide | S | Choose caller versus ingestion ownership of endpoint lastseen updates on Neo4j relationship upserts. |
| 104 | [DAWGS-111](variance_findings.md#dawgs-111) — Kinds.Remove changes shared slice storage | A | Decide | S | Define constructor/slice ownership and copying for Kinds; an implementation change may be M and needs allocation/performance review. |

## Delivery packages, dependencies and parallelism

The original delivery packages below retain completed prerequisites as coordination context; use the active queue and open follow-ups above for remaining work. A practical allocation is four engineering lanes plus a shared validation owner. These are coordination boundaries; no headcount or staffing availability is assumed.

| Lane | Start with | Coordinate next | Acceptance and important limits |
| --- | --- | --- | --- |
| Compiler/query integrity | 058 cache identity; 006 Boolean context; 041/045 mutation state; 003/004/011/030 failure handling | 028/035/036 predicate lifetime; 021/024 optional rows; 026/027/031/033 binding/orientation; Q/R/B type and projection work | Use separate patches and acceptance tests per root cause. Share owners/reviews around expression, traversal, projection and frame bookkeeping. Cache containment can ship before complete formatter repair. |
| Analysis and containers | 073 reachability; 076 bitmap operations; both 077 mechanisms; 099 locking; 072 error propagation | 074/095 traversal transitions, 100 iterator error, then helpers 075/078/079 and cancellation 097 | Disjoint primitives can proceed in parallel. Reachability must be rechecked with repaired bitmap/lock behavior. Give one integration owner to traversal transition/error changes that share traversal.go. |
| Driver and persistence | 039/063/065 targeted backend probes; 064 key identity; 070/087 deletion intent | 071/067 graph selection/assertion; 069/088/089 kinds; switch/cursor/Neo4j lifecycle and batch contracts | Coordinate manager locks/connection ownership and selection versus schema mutation. Source-only probes are completion prerequisites for choosing consequential repairs, while clearly independent key/deletion fixes can progress. |
| Transfer integrity | 080 nested scrub, 101 exact numeric import, 102 root-aware fragment reads | 081 scrub consistency, 082 temporal representation, 083 unpack paths; recovery/strictness decisions 112/113 | Reuse exact numeric rules with result decoding, but test import binding/persistence separately. Define deliberate preserve overrides before changing recursive scrub behavior. No compiler dependency for isolated file/helper fixes. |
| Shared validation | 084 useful BDD status, focused reproductions, shared fixture discipline | Backend-equivalent mutation/template corpora, race/error schedules, neighboring resolved controls | A supported gate can run alongside report-only upstream compatibility. Keep unsupported/undefined states visible. New regression failures must not disappear behind the overall exit code. |

Specific sequencing constraints:

1. **020 can precede 019.** Exact-length normalization is bounded; the default traversal cap needs a resource/completeness policy, cancellation behavior and performance qualification before it changes.
2. **030 and 036 have separate failure paths.** An explicit translation error can contain the aggregate panic; declining optimization alone still reaches the generic pattern-predicate defect. Give both explicit completion checks, along with 028/035 predicate ownership.
3. **041, 045, 043 and 106 are separate promises.** Input-row preservation, Cypher clause order, post-DELETE projection, and Go batch ordering share plumbing but each needs its own acceptance tests. Swapping one flush order does not establish arbitrary caller order.
4. **063/071 should share graph-selection work.** Preserve fixed partition routing in 062. Preserve DAWGS-039's separate parent/partition trigger and connected-node deletion controls during graph-selection changes.
5. **051/057 and 101 share representation rules, with different boundaries.** Result decoding must preserve string identity and exact values; import must also produce values accepted by both drivers. 105's numeric-cast API decision does not justify corrupting JSON input while it is pending.
6. **059, 060, 061, 085 and 086 touch different emitters or rewrite paths.** Share Boolean/type/escaping controls without assuming one formatter patch fixes a hand-built upsert string. The repaired old OR cleanup caller in 059 stays out of the residual impact claim.
7. **Cancellation and locking need complete lifetimes.** Fixing 072 must retain the originating error; 091 needs a controlled shutdown schedule before adding waits; 099 must avoid a new inversion or uncontrolled allocation cost. Cover early close, cancellation and error draining.
8. **109 and 112 require independent decisions.** Driver transaction rotation, multi-batch import recovery, batch ordering 106 and schema ownership 110 are different contracts. Atomic recovery or resumability should receive a separate design after its guarantee is chosen.
9. **Containment is an independently shippable result.** Clear rejection for 001/002/010/013/017/103 and safe optimizer decline can stop misleading success or panics. Full feature support requires its own acceptance scope and should remain visible after containment.

## Resolved original findings: regression controls

The nine originally resolved findings and their protection notes are preserved in the [completion ledger](variance_findings_priority_complete.md#resolved-original-findings-keep-as-regression-controls). Keep those controls intact during adjacent work. DAWGS-062's parent-trigger caveat is repaired and covered by the [DAWGS-039 regression evidence](variance_findings_priority_complete.md#dawgs-039-node-delete-semantics-and-graph-scoped-cleanup--2026-10-28).

## Additional observations: attach to the owning workstream

These 22 observations are separate from the 113 numbered findings and the counts above. O01–O22 follow their source-report order and are reference labels for this plan. Where the files and tests overlap, address them alongside the owning package; each still needs its stated contract or reproduction. The links point to the exact source heading lines.

| Priority | Note | Observation | Track | Action | Effort | Next step / overlap |
| --- | --- | --- | :---: | --- | :---: | --- |
| P2 | O01 | [FetchRelationshipNodes rejects self-loop](variance_findings.md#L1633) | A | Repair | S | Add a self-loop endpoint helper regression, then accept one distinct endpoint where both relationship endpoints share identity. |
| P2 | O02 | [DirectionBoth origin and inconsistent deletion views](variance_findings.md#L1639) | A | Repair | M | Restore consistent adjacency/deletion views and exclude the origin where required; review with cache/container invariants. |
| P2 | O03 | [Cache and adjacency graph report node counts for edges](variance_findings.md#L1645) | A | Repair | S | Correct relationship/edge counts and cover unequal node/edge totals, empty state and deletions. |
| P2 | O04 | [PathSegment.Slice allocation/index mismatch](variance_findings.md#L1651) | A | Repair | S | Fix PathSegment.Slice root-inclusive allocation/indexing; pair with serialized-path reconstruction in 078. |
| P2 | O05 | [TSStatelessBFS emits maxDepth+1](variance_findings.md#L1657) | A | Repair | M | Pin inclusive depth semantics, then stop TSStatelessBFS before expanding a maximum-depth node. This is separate from SQL expansion 019/020. |
| P2 | O06 | [AcyclicNodeFilter comment promises global first visit](variance_findings.md#L1663) | A | Decide | S | Align the first-visit comment with per-path cycle behavior or explicitly choose a global-dedup helper; coordinate with 096. |
| P2 | O07 | [Node.Merge on nil receiver Properties](variance_findings.md#L1669) | A | Repair | S | Handle nil receiver Properties deliberately while preserving merge modification/deletion tracking; share nil-contract checks with 068/092. |
| P2 | O09 | [Identifier backtick round trips](variance_findings.md#L1681) | Q | Reproduce | M | Target surviving variable/label identifier paths; retain already-fixed property-key tests and coordinate escaping with 055/061. |
| P2 | O10 | [Formatter omits AND between Where.Expressions](variance_findings.md#L1687) | B | Repair | S | Define multi-expression WHERE rendering and add separators without changing normal explicitly grouped conjunctions; coordinate 085/086. |
| P2 | O12 | [ScanNextResult target arity unchecked](variance_findings.md#L1699) | R | Repair | S | Return a defined arity error from ScanNextResult instead of indexing beyond caller targets. |
| P2 | O13 | [Non-path map accepted as empty Path](variance_findings.md#L1705) | R | Reproduce | S | Pin mapper acceptance of a non-path map, then require path-shape/type evidence while preserving legitimate empty paths. |
| P2 | O15 | [SetBatchWriteSize unsynchronized package global](variance_findings.md#L1717) | D | Reproduce | M | Exercise concurrent setters and multiple driver instances under race detection before selecting instance-local or synchronized configuration. |
| P2 | O16 | [Neo4j errors may surface after initial check](variance_findings.md#L1723) | D | Reproduce | M | Simulate errors at Next/Consume and preserve attribution to the originating statement; coordinate result lifecycle 091/093/109. |
| P2 | O17 | [SinglePartQuery order sensitivity and replacing Filter](variance_findings.md#L1729) | B | Decide | M | Specify fluent clause-order and repeated-Filter composition, then correct or document behavior; share builder rendering coverage. |
| P2 | O18 | [DatabaseSwitch nested-transaction deadlock](variance_findings.md#L1735) | D | Reproduce | M | Control nested transactions, a waiting switch writer and inSwitch rejection together before changing locks; link to 094. |
| P2 | O22 | [Component/reachability cancellation completeness](variance_findings.md#L1759) | A | Decide | M | Choose cancellation completeness guarantees and reproduce partial construction. An additive error-returning API may require L follow-on work; coordinate 073/097. |
| P3 | O08 | [split of empty string](variance_findings.md#L1675) | Q | Reproduce | S | Establish the expected empty-string split result and execute the PostgreSQL scalar fixture before requiring a fix. |
| P3 | O11 | [Hex/octal, Unicode escapes and parameterized SKIP/LIMIT](variance_findings.md#L1693) | Q | Decide | S | Document supported literal/escape/pagination forms and their rejection phase. Implementation of each unsupported form is separate M feature work. |
| P3 | O14 | [MapStringAnyToJSONB modifies caller map](variance_findings.md#L1711) | R | Decide | S | Choose caller-map ownership separately from nil-slice encoding policy; coordinate with 111 without regressing array binding. |
| P3 | O19 | [IPv4 pseudonym space only 24 bits](variance_findings.md#L1741) | I | Decide | S | Choose required IPv4 pseudonym uniqueness/collision guarantees for expected cardinalities. A 24-bit output space alone does not prove a collision or plaintext recovery. |
| P3 | O20 | [Node MarshalJSON does not imply round-trip promise](variance_findings.md#L1747) | I | Decide | S | Document whether Node JSON promises round trips before adding an unmarshaler or choosing a stable interchange format. |
| P3 | O21 | [Low-level TSBFS/TSDFS caller cycle policy](variance_findings.md#L1753) | A | Decide | S | Specify low-level walk/trail/cycle policies and caller-filter responsibility before imposing new global visited behavior. |

## Completion and validation

Completion means the specific next action is reviewable: a repaired behavior has a focused regression and the applicable backend checks; a reproduction has a complete fixture, expected result and captured outcome; a decision states the API guarantee, migration impact and selected follow-on scope. A clean rejection or safe fallback must be reported as containment until the promised semantics are implemented.

When a work item is complete, move its work-ledger entry, original queue details and all related implementation notes and validation evidence from this file to [variance_findings_priority_complete.md](variance_findings_priority_complete.md). Preserve its finding ID, original order number, dated evidence and shared validation references. Remove the completed item from the active queue, update current backlog and completion counts, and fix links to moved notes. Keep separate unfinished follow-ups visible here with links to the completed evidence. Reproduction or design completion alone does not close an item whose runtime repair remains open; retain that item here and update its next action.

For implementation, follow AGENTS.md: inspect affected code, propose a plan before schema/interface/performance changes, format touched code, preserve backend-equivalent core cases, and add mutation/template coverage for parser/translation/rendering/query-semantics changes. Verify both PostgreSQL optimizer modes where relevant. Keep concurrency and driver-specific behavior in properly scoped tests.

Arrange the selected backend's `CONNECTION_STRING` before live validation, then use the required backend-selected checks. The earlier unit-suite success in variance_findings.md is historical evidence for the original triage; new runtime results are recorded in the open work ledger above or the [completion ledger](variance_findings_priority_complete.md). Gate changes should preserve visibility of unsupported TCK assertions while making the enforced supported set reliable.

The original prioritization pass checked complete ID/observation coverage, status and workstream counts, link targets, ordering and Markdown structure. Implementation progress and validation belong in the open work ledger and completion ledger; the active queue retains original order numbers, with completed actions removed and confirmed next actions updated. Calendar estimates should follow the initial high-consequence probes and contract choices; the present relative sizes support sequencing and ownership, not a delivery-date commitment.

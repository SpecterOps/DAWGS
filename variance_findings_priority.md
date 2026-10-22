# Prioritized variance findings and work plan

Prepared on **2026-10-22** from [variance_findings.md](variance_findings.md), validated against checkout **`0bf83edb8155d915c55f559bae212a4526e4607f`**. This document adds engineering judgment and delivery order to that evidence. It preserves the underlying dispositions: **90 retained findings, 4 needing runtime validation, 10 contract questions, and 9 resolved original findings**. The 22 additional observations have a separate queue.

**Start with a usable regression gate, urgent deletion/schema probes, and parallel repairs to mutation integrity, cache identity, reachability, and transfer integrity.** The largest sustained work is compiler semantics: row identity, predicate lifetime, type preservation, and traversal correctness. Smaller utility and error-handling fixes can ship while those programs develop.

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

## Spread of work

Each numbered finding has **one primary workstream** below, so counts do not double-count shared code. Retained means Applies or Partially applies in the validated report. Questions combine its unverified and contract dispositions. Resolved findings remain regression controls.

| Code | Primary workstream | Retained | Questions | Resolved | Open next steps | Main effort concentration |
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

The plan has **36 P1, 59 P2, and 9 P3 next steps**, plus 9 resolved controls. By next action there are **69 repairs, 9 containment tasks, 9 reproductions, 11 decisions, 5 designs, and 1 validation enabler**. By next-step size there are **33 S, 58 M, 12 L, and 1 XL**. Many S rows are bounded rejection, contract or utility work; completed support for list operations, procedures, traversal policy, atomic recovery or multigraph storage can be much larger.

The 36 P1 rows form several parallel work packages. Start the first available item in each active lane, preserving the dependencies below; one engineer should not have to finish all compiler work before independent driver or tooling work can begin.

## Work ledger

The queue and counts retain the original triage baseline. This ledger records implementation progress and new runtime evidence in queue order. Credentials are excluded from evidence.

| Order | Finding | Status | Work and validation |
| ---: | --- | --- | --- |
| 1 | DAWGS-084 | Repaired and validated; initial broad-gate failure recorded | Removed the `TestMain` exit override. Untagged scenarios are enforced by default; unsupported upstream cases have explicit `@report-only` tags and still execute. Reports distinguish enforcement and compatibility results. Sequential BDD runs on both backends enforce 10 passing scenarios and expose 18 report-only failures. Race-enabled BDD unit tests and vet pass; package coverage increases from 74.0% to 80.8%, with 100% coverage of gate decisions. Both shared integration suites pass. Initial broad PostgreSQL commands failed on an unchanged timezone-sensitive unit test; later validation under 041 uses a distinct local-location name. |
| 2 | DAWGS-039 | Reproduction complete; repair open | PostgreSQL in both optimizer modes commits plain and DETACH DELETE with one node and one stored orphan edge remaining. Both roll back correctly. Neo4j rejects connected-node plain DELETE at commit and correctly detaches on DETACH DELETE. The separate parent trigger deletes another graph's edge when explicit endpoint IDs overlap. See the controlled fixtures below. |
| 3 | DAWGS-063 | Reproduction complete; repair open | A fresh PostgreSQL manager selecting an existing graph by name through `ReadTransaction` drops both node and edge property indexes and uniqueness constraints (four objects total). The call succeeds and returns count 0. Selection/schema-reconciliation separation remains to implement. |
| 4 | DAWGS-041 | Row-stream repair validated; image linker workaround recorded | UPDATE returns only the changed entity; a separate left-joined projection retains the complete input stream, duplicates and null targets. Assignments bind against each target's actual input frame. All 19 shared variants pass on Neo4j and in both PostgreSQL compiler modes; WITH continuation passes on both. Complete PostgreSQL unit tests and both complete backend integration runs pass with the race detector. BDD gates pass on both backends; PostgreSQL's final non-race BDD step needs external linking in this image. Mutation/SQL fixtures, v2 builder expectations, coverage, scope and exact validation conditions are recorded below. |
| 5 | DAWGS-064 | Repaired and validated; aggregate retry recorded | Framed node/relationship identities retain kind, property names, value types and endpoint boundaries without sorting caller slices. Property hashing frames each key/type/value. Neo4j groups query shapes by structured strings, separating identity/add/remove roles and eliminating digest-only grouping. Focused race tests, vet and live kind-operation/relationship-identity regressions pass on both backends. Existing PostgreSQL relationship-create regression passes. Complete backend validation and the final graph-package retry are recorded below. |
| 6 | DAWGS-070 | Repaired and validated | Neo4j node-update parameters copy the property map only when deletions exist and represent each deleted key with null, using existing `SET n += p.properties` behavior. Caller maps remain unchanged and rows with different deletions still share one query shape. Shared stored-state regressions pass on both backends; the corrected 1,000-node Neo4j regression and focused race tests pass. Complete integration and BDD checks pass with the recorded environment settings and graph-package retry. |
| 7 | DAWGS-087 | Reproduced, repaired and validated | The shared stored-state regression fails before repair on PostgreSQL (removed kind retained) and passes on Neo4j. PostgreSQL now resolves upsert IDs and applies only pending kind removals through the existing bulk-update path. Flushes containing removals use an explicit transaction; a forced removal failure rolls back the associated upsert. Normal commits, coalescing and survivor isolation pass. Both backend integration suites and BDD checks pass with the recorded settings/retry. A separate pre-existing whole-batch rollback limitation was reproduced and remains open below. |
| 8 | DAWGS-045 | Ordered intent repaired and validated; expression sequencing open | Ordered operations fix property/kind post-state without changing one-write-per-target or row preservation. All 20 shared variants pass on Neo4j and in both PostgreSQL compiler modes. A separate live probe confirms that dependent assignment expressions still require a WITH boundary; that sequencing follow-up remains open below. |
| 9 | DAWGS-058 | Next implementation item | Qualify warm/cold compilation identity for distinct builder ASTs; consider conservative cache containment before a complete semantic fingerprint. |

### DAWGS-084 implementation and checks — 2026-10-22

The enforced set consists of eight DAWGS scenarios plus upstream CREATE scenarios 1–2. Upstream scenarios 3–10 need label/property side-effect assertions or scalar result formatting; 11–12 additionally cover retained null/precision findings; 13–20 need compile-time error assertions. Their 18 failures remain in Cucumber JSON and HTML. This is a supported-scenario gate, not full TCK conformance.

Regression coverage exercises actual Godog passing, failing and undefined steps in DAWGS and upstream feature locations; report-only failures; HTML visibility/escaping; invalid/empty reports; runner/report disagreement; and ordinary Go-test failure in a subprocess. New untagged scenarios fail the gate when broken. No parser, translator, renderer or query semantics changed.

Validation: `CGO_ENABLED=1 CC=cc go test -race -coverprofile=… ./bdd` and `go vet ./bdd` pass. Sequential runs with both supplied backend connections pass `CGO_ENABLED=0 make test_bdd_integration` with **10 enforced / 0 failed**, **18 report-only / 18 failed**. `CC=cc` selects the image's supplied C compiler for race-enabled tests; the default `gcc` executable is absent. Backend BDD runs must be sequential because they share report paths.

The PostgreSQL-selected `CGO_ENABLED=1 CC=cc make test_all` completes the unit run with one failure in unchanged `cypher/models/pgsql/pytypes_test.go:452`: `TestValueToDataType` expects an explicit UTC time to be `TimestampWithTimeZone`, but `ValueToDataType` compares the location's string against the local location's string, also `UTC` in this environment. The focused test fails independently and passes with `TZ=Etc/UTC` (same UTC offset, distinct local-location name). The remaining unit packages pass. This validation issue is recorded rather than changing temporal behavior outside the current work.

Neo4j-selected `TZ=Etc/UTC CGO_ENABLED=1 CC=cc make test_integration MAIN_PACKAGES='./drivers/neo4j ./integration ./opengraph'` passes, including the shared integration suite. The separate default PostgreSQL `CGO_ENABLED=1 CC=cc make test_integration` passes its shared integration suite, PostgreSQL driver/schema-query checks, graphbench integration checks, and import checks; its unchanged `TestValueToDataType` assertion fails under the original local UTC name. The gate repair has no observed failures in focused or backend checks. Full-suite green remains limited by that independent temporal-type test.

### DAWGS-039 controlled fixture and outcome — 2026-10-22

Use a disposable PostgreSQL graph with node kind `VarianceDeleteNode` and relationship kind `VarianceDeleteEdge`. Create two nodes and one outbound edge, equivalent to:

```cypher
CREATE (:VarianceDeleteNode {role: 'target'})
       -[:VarianceDeleteEdge]->(:VarianceDeleteNode {role: 'survivor'})
```

Reset to this fixture before each check. Execute each query in a separate write transaction, consuming its result and checking the transaction/commit error:

```cypher
MATCH (n:VarianceDeleteNode {role: 'target'}) DELETE n
MATCH (n:VarianceDeleteNode {role: 'target'}) DETACH DELETE n
```

For each query, repeat once with normal commit and once with a sentinel error returned from the transaction delegate after result consumption. On PostgreSQL repeat the whole matrix with `pg.SetOptimizedTranslation(false)` and `true`. Inspect physical storage through `SELECT count(*) FROM node WHERE graph_id = $1` and the equivalent query over `edge`; a Cypher relationship match would hide orphan edges. On Neo4j inspect fixture-label node and relationship counts after the transaction.

| Backend / path | Commit outcome | Persisted nodes / edges | Forced rollback nodes / edges |
| --- | --- | --- | --- |
| PostgreSQL plain DELETE, either optimizer mode | Success (incorrect) | 1 / 1; stored edge has a missing endpoint | 2 / 1 |
| PostgreSQL DETACH DELETE, either optimizer mode | Success with orphan edge (incorrect) | 1 / 1 | 2 / 1 |
| Neo4j plain DELETE | `Neo.ClientError.Schema.ConstraintValidationFailed` at commit | 2 / 1 | 2 / 1 |
| Neo4j DETACH DELETE | Success | 1 / 0 | 2 / 1 |

For the separate parent-trigger fixture, create graph A with nodes `a,b` and edge `a→b`. Create graph B with nodes `a,c`, deliberately reusing A's explicit `a.id`, and edge `a→c`. The shared node can be inserted without altering kinds/properties using:

```sql
INSERT INTO node (id, graph_id, kind_ids, properties)
SELECT id, $graph_b, kind_ids, properties
FROM node WHERE graph_id = $graph_a AND id = $shared_id;

DELETE FROM node WHERE graph_id = $graph_a AND id = $shared_id;
```

After deletion A has **1 node / 0 edges**; B has **2 nodes / 0 edges**. B should retain its edge. Repeating the parent deletion inside an explicit SQL transaction and rolling it back restores **2 nodes / 1 edge in each graph**. This confirms the previously source-only parent-delete caveat under resolved DAWGS-062; it does not reopen graph-partition routing. Repair must preserve plain-DELETE rejection, implement DETACH cleanup on partition paths, and scope parent-trigger edge removal by graph as well as endpoint ID. All probe graphs and their partitions were removed after inspection.

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

### DAWGS-041 row-stream repair and checks — 2026-10-22

PostgreSQL UPDATE cardinality is one row per physical target, so its RETURNING stream cannot stand in for the Cypher input. The repair makes each UPDATE return only its changed entity and follows it with a separate projection that left joins the original stream by target ID. Every matched input row, including duplicates and rows with null mutation targets, remains available to subsequent targets and RETURN. Entity bindings refresh by ID; captured scalars, paths and lists retain their representations. Assignment expressions stay unbound until the target's input frame is known, fixing the invalid earlier-frame reference in chained per-row assignments.

The shared `integration/testdata/templates/update_row_stream_shapes.json` has **19 variants** covering fan-out, genuine fan-in, repeated node and relationship targets, both target orders, mixed node/relationship writes, null targets first/last, an entirely null target set, duplicate scalar input values, empty input, per-row expressions, alias freshness, property removal and kind changes. Each checks returned rows and complete node/relationship post-state, including direction/kind/isolated-node decoys. `integration/testdata/cases/update_row_stream_inline.json` additionally checks preserved rows through a later WITH/SET statement. These fixtures use identical assertions on both backends; a PostgreSQL-scoped runner executes all template variants in each compiler mode.

Initial Neo4j execution passed all 12 original variants and the continuation case. Initial PostgreSQL execution in each mode exposed nine row-loss cases, an invalid earlier-frame reference, and an adjacent direct-UNWIND projection error. The duplicate-value regression now materializes UNWIND through WITH to isolate row preservation; the general UNWIND finding remains in its own queue. After the repair, all **38 PostgreSQL compiler-mode executions / 19 Neo4j variants**, the continuation case on each backend, and both full shared integration suites pass with the race detector. Broad backend results follow below.

Unit tests retain alias/scalar/path/list coverage and additionally check ID-only representation preservation, combined property/kind additions and removals, and mapping-error propagation. Five parser/analyzer mutation cases and their generated fitness values cover the same query shapes, including staged continuation. Four new single-statement SQL fixture queries cover fan-out, mixed writes and nullable nodes/relationships; their generated expectations and the existing SQL fixtures were refreshed through `CYSQL_UPDATE_CASES=true`. The v2 builder's update SQL expectation was also updated. The SQL golden harness only renders single statements, so staged continuation is covered by the mutation case and live integration fixture instead. `gofmt` and focused `go vet` pass. Translator unit coverage increases from **6270/8465 (74.0697%)** to **6270/8462 (74.0960%)** with race-enabled tests passing.

Scope: this repairs input-row preservation and assignment-frame resolution. Per-identifier assignment coalescing is unchanged; ordered SET/REMOVE behavior remains DAWGS-045. Repeated-target fixtures use consistent assignment values; they do not establish sequential per-row evaluation of non-idempotent expressions or writes through different variables to the same physical entity. The added projection introduces one left join per update target; correctness checks establish cardinality and post-state, not a performance improvement.

Broad validation uses `TZ=Etc/UTC CGO_ENABLED=1 CC=cc` with the supplied connection for each selected backend. PostgreSQL-selected `make test_all` passes its **complete unit and integration stages across all repository packages**, then its non-race BDD build hits an image linker failure: `runtime.gcdata: missing Go type information for global symbol .dynsym`, followed by unresolved native symbols. Re-running `make test_bdd_integration` with the additional `GOFLAGS='-ldflags=-linkmode=external'` passes. No test-behavior or linker configuration was changed in the repository.

The complete Neo4j-selected `make test_integration` passes across all repository packages. Separate sequential BDD runs on both backends pass with **10 enforced / 0 failed**, **18 report-only / 18 failed** each; Neo4j used `CGO_ENABLED=0`, PostgreSQL used the external-linking flag above. The historical timezone-sensitive unit test remains unchanged; using the distinct local-location name allows the broad checks to proceed. All required test stages therefore pass with these recorded environment workarounds, while the unmodified CGO-enabled aggregate command still requires the linker flag for its final stage.

### DAWGS-064 batch identity repair and checks — 2026-10-22

Regressions first reproduced distinct relationship identities collapsing to `R1234`, property maps `{"a":1,"b":2}` and `{"a1b":2}` sharing a digest, Neo4j add-kind/remove-kind grouping, and identity-shape role collisions. `NodeUpdate.Key` also sorted its caller-owned identity-property slice. The repair uses framed identities with explicit entity/kind, property name/type/value and endpoint boundaries; property hashes frame each key/type/value; and Neo4j query grouping uses canonical structured strings instead of digest-only equality. Identity field and kind-set permutations retain equality. Values unsupported by JSON encoding and missing/null identity properties return errors. Keys and digests are opaque and are not a persistent serialization contract.

Coverage includes graph identity/hash tests, PostgreSQL upsert coalescing/future tests, Neo4j generated-template/parameter tests, the existing PostgreSQL relationship-create regression, and shared live batch fixtures. Live fixtures check add/remove-kind separation and distinct relationship endpoint tuples plus duplicate-property merging on both backends. No parser or translator behavior changes; generated Cypher templates retain their text. Focused race tests and vet pass. Before/after unit coverage for the initial repair increases from **25.1% to 27.5%** in `graph`, **27.5% to 29.8%** in `drivers/neo4j`, and **5.2% to 16.9%** in `drivers/pg/query`; final combined validation is recorded below. Structured keys trade some allocation for explicit equality; no throughput improvement is claimed.

### DAWGS-070 Neo4j batch property-deletion repair and checks — 2026-10-22

Node-update rows now include null values for tracked deleted keys in their existing `properties` parameter. Neo4j's existing `SET n += p.properties` applies those deletions. The property map is copied only when deletions exist, preserving the caller's map. Nodes with different deletion sets can share the same generated query without sharing deletion intent; property names remain parameter data.

The parameter regression fails before repair and passes afterward. Shared integration fixtures cover grouped and individual submissions, deleting an existing or absent property, several deletions, keys containing punctuation/backticks, simultaneous writes, untouched sibling rows, and delete-then-set resetting deletion tracking. They pass on both backends. The previous Neo4j-specific 1,000-node test now requires the deleted property to be absent and passes. Generated-template and parameter assertions cover the unchanged Cypher template. This change covers batch update-by-ID, not identity-based property-deletion behavior.

### DAWGS-087 reproduction, repair and flush boundary — 2026-10-22

Create a node with kinds `WriteUpsertNode`, `WriteUpsertNodeA`, `WriteUpsertNodeB`, identity property `objectid`, and an unrelated preserved property. Submit `Batch.UpdateNodeBy` using that identity, adding `WriteUpsertNodeC`, setting another property, and tracking deletion of `WriteUpsertNodeA`. In the same batch, update a separate survivor without a removal. Before repair PostgreSQL commits successfully, preserves the node ID and properties, adds C, but incorrectly retains A. Neo4j removes A with the same fixture. This converts the source-supported finding into a live stored-state reproduction.

The repair pairs pending kind removals with the IDs returned by the upsert and applies them through the existing update-by-ID implementation. Its second write contains no property changes or kind additions. A flush containing removals wraps both writes in an explicit transaction; flushes without removals keep their previous path. Shared tests pass for normal commits, survivor isolation and add/remove sequences across flushes. Unit tests verify returned-ID association, property isolation and write-error propagation. A PostgreSQL-scoped trigger rejects the removal update after the upsert has changed properties/kinds: the operation returns that failure and leaves the original node unchanged, proving flush rollback. The temporary trigger/function are removed by test cleanup.

**Separate runtime evidence, repair open:** A delegate-error fixture with `WithBatchSize(1)` flushes two PostgreSQL identity upserts and then returns a sentinel error. The error is returned but the flushed property/kind changes remain committed. Neo4j rolls back this small fixture. PostgreSQL constructs its batch wrapper without an explicit transaction and normally executes flushes directly through the pooled connection. Therefore whole-batch rollback was never guaranteed by the existing implementation, despite the former internal comment. README and that comment now describe the observed boundary. The 087 repair makes its two-write flush atomic; it does not implement whole-batch atomicity, recovery or caller-order guarantees. Keep that broader contract/design follow-up with the driver lifecycle/recovery work (106/109/112); the original triage counts are unchanged.

### Combined batch validation — 2026-10-22

Commands use each supplied backend connection separately with `TZ=Etc/UTC CGO_ENABLED=1 CC=cc GOFLAGS='-ldflags=-linkmode=external'`, retaining the already-recorded image/timezone workarounds. The PostgreSQL-selected `make test_all` passes its complete race-enabled unit stage (**66.1% overall statement coverage**). Every integration package except `graph` passes in that invocation, including graphbench and the full shared suite. Neo4j-selected `make test_integration` passes across all repository packages, including the full shared suite. Its separate `make test_bdd_integration` passes with **10 enforced / 0 failed**, **18 report-only / 18 failed**.

**Aggregate retry:** The final missing-property error-compatibility edit was made while PostgreSQL's aggregate build was running. That build paired the newly strengthened `errors.Is` assertions with the already-compiled older graph package: the two graph integration-tagged tests observed the previous plain `missing identity property` error and failed. The unrestricted aggregate invocation consequently exits nonzero at its integration stage. After the source was fixed in place, a PostgreSQL-selected `make test_all MAIN_PACKAGES='./graph' COVERPKG=github.com/specterops/dawgs/graph` with dedicated coverage output paths passes the graph unit and integration stages and the final BDD stage. The BDD outcome is **10 enforced / 0 failed**, **18 report-only / 18 failed**. Together these runs verify all required unit/integration/BDD stages; the unrestricted aggregate command itself was not rerun. Backend BDD report writes are sequential. No code edits follow the successful final retry.

Final focused race-enabled unit coverage is **333/1213 (27.4526%)** in `graph`, **363/1200 (30.2500%)** in `drivers/neo4j`, **1802/3187 (56.5422%)** in `drivers/pg`, and **49/290 (16.8966%)** in `drivers/pg/query`. All four affected packages increase from their pre-change coverage (PostgreSQL was **55.2%** before 087). Focused vet and `git diff --check` pass. Final compatibility review preserved `errors.Is(err, graph.ErrPropertyNotFound)` for missing/null identity properties; the focused graph/query race tests and graph vet pass after that narrow error-path adjustment. All touched Go files were formatted with `gofmt`.

### DAWGS-045 ordered intent repair and checks — 2026-10-22

The PostgreSQL mutation model now retains each property/kind set or removal in source order instead of collecting four independent assignment/removal buckets. The renderer folds each target's property and kind expressions from left to right, with explicit parentheses at operator changes. Adjacent property sets still share one `jsonb_build_object`; adjacent removals and kind operations still share arrays. Final kind additions are sorted and deduplicated as before. Each identifier still produces one UPDATE plus its existing row-preserving projection, avoiding PostgreSQL's unsupported repeated updates of the same row in one statement.

The shared `integration/testdata/templates/update_clause_order_shapes.json` has **20 variants**. They cover node and relationship properties in both orders, alternating and repeated changes, initially missing properties, label changes and overlapping label lists, combined property/kind changes, mixed node/relationship targets, fan-out, nullable relationships, empty input, and continuation through WITH. Every variant checks complete stored node and relationship records, including direction, kind, property, missing-property, endpoint and isolated-node decoys. The PostgreSQL-scoped runner executes the same assertions with each compiler mode.

Before repair, **12 of 20 variants fail in each PostgreSQL compiler mode**; Neo4j passes all 20. After repair, all **40 PostgreSQL compiler-mode executions / 20 Neo4j variants** pass. Eight SQL-expression unit cases, each in both optimizer modes, pin operation order, parentheses and adjacent-operation grouping while checking one physical update. Seven parser/analyzer mutation cases and six SQL fixture queries cover alternating/mixed changes; generated fitness values and SQL expectations were refreshed using `CYSQL_UPDATE_CASES=true` and copied from the existing `updated_cases` output directories. The generated directories and temporary probes were removed. Focused race-enabled tests and coverage checks pass. Translator coverage increases from **6270/8462 (74.0960%)** to **6272/8460 (74.1371%)**. Touched Go files were formatted with `gofmt`; focused vet and `git diff --check` pass.

Broad validation uses each supplied connection separately with `TZ=Etc/UTC CGO_ENABLED=1 CC=cc GOFLAGS='-ldflags=-linkmode=external'`, retaining the previously recorded image/timezone workarounds. The unrestricted PostgreSQL-selected **`make test_all` passes**, including the complete race-enabled unit and integration stages and the final BDD stage; overall unit statement coverage is **66.1%**. The complete Neo4j-selected **`make test_integration`** and separate **`make test_bdd_integration`** pass. Both shared integration suites pass on each backend. Each BDD gate reports **10 enforced / 0 failed**, **18 report-only / 18 failed**; the report-only failures remain explicit compatibility results. BDD report writes were sequential. No Go edits followed these successful broad runs.

**Scope and open expression-sequencing follow-up:** This repairs ordered final mutation intent on each identifier. It does not establish arbitrary clause-by-clause evaluation of assignment expressions, caller-row sequencing of non-idempotent writes, or ordered writes through different identifiers bound to the same physical entity. A separate rollback-fixture probe starts with `n.x = 0` and runs:

```cypher
MATCH (n:OrderSource {objectid: 'source'})
SET n.x = 1 REMOVE n.x SET n.y = coalesce(n.x, -1)
RETURN n.y
```

PostgreSQL returns **0**, reading the original input frame; Neo4j returns **-1**, observing the earlier removal. Inserting `WITH n` between `REMOVE n.x` and the last SET returns **-1 on both backends**. README documents this boundary. Full sequential expression evaluation remains compiler work and must retain separate regressions; the constant-value ordered-intent fixtures do not close it. Query rendering remains a single statement unless an existing WITH write boundary requires staging. Alternating operations add expression nodes; adjacent operations keep their previous compact SQL shape. No performance improvement is claimed.

## Ordered next-step queue

Every unresolved numbered finding appears once in this queue. The ID links to the original validated evidence, including corrected subcases and runtime limits. Track codes refer to the workstream table. The global order is a default next-start order; independent tasks can run concurrently.

### P1: Start first — 36 items

| Order | Finding | Track | Action | Effort | Next step and reason |
| ---: | --- | :---: | --- | :---: | --- |
| 1 | [DAWGS-084](variance_findings.md#dawgs-084) — BDD gate ignores upstream failures | V | Enable | M | Establish a useful supported-scenario gate and preserve ordinary Go-test failures. Keep unsupported upstream reporting explicit so the gate can qualify later repairs. |
| 2 | [DAWGS-039](variance_findings.md#dawgs-039) — Plain DELETE silently detaches connected nodes | Q | Reproduce | M | Run connected-node plain/DETACH DELETE against the actual partition/parent trigger paths and inspect persistent edges and rollback. The old silent-detach outcome is unverified; include the separate parent-delete caveat under resolved 062. |
| 3 | [DAWGS-063](variance_findings.md#dawgs-063) — WithGraph drops existing indexes and constraints | D | Reproduce | M | Confirm whether selecting an uncached graph with a name-only definition removes existing indexes/constraints. Then separate selection from schema reconciliation; the source-supported repair is likely L. |
| 4 | [DAWGS-041](variance_findings.md#dawgs-041) — SET loses updates and drops rows | Q | Repair | L | Preserve the original input row stream through chained updates so fan-out, repeated targets and null targets cannot lose later writes or rows. |
| 5 | [DAWGS-064](variance_findings.md#dawgs-064) — Batch de-duplication keys collide | D | Repair | M | Repair remaining typed batch-key/digest distinctions, especially Neo4j add-kind versus remove-kind. The original PostgreSQL endpoint-pair create collision stays closed. |
| 6 | [DAWGS-070](variance_findings.md#dawgs-070) — Neo4j batch.UpdateNodes ignores property deletions | D | Repair | M | Carry tracked property deletions into Neo4j batch updates; replace the integration expectation that currently preserves the deleted property. |
| 7 | [DAWGS-087](variance_findings.md#dawgs-087) — UpdateNodeBy ignores DeletedKinds on PostgreSQL | D | Reproduce | M | Pin UpdateNodeBy deleted-kind post-state on PostgreSQL, then carry removal intent through the upsert path. Current evidence is source-supported, not a live stored-state replay. |
| 8 | [DAWGS-045](variance_findings.md#dawgs-045) — SET and REMOVE of the same property or label apply removals first | Q | Repair | M | Represent SET/REMOVE in clause order; remove-then-add folding currently changes persistent properties and labels. |
| 9 | [DAWGS-058](variance_findings.md#dawgs-058) — The translation cache returns another query's SQL | B | Repair | M | Make warm/cold compilation equivalent for distinct builder ASTs. A conservative cache bypass can land before a complete semantic fingerprint or formatter repair. |
| 10 | [DAWGS-073](variance_findings.md#dawgs-073) — ReachabilityCache returns incomplete reach sets | A | Repair | L | Guarantee complete reach sets for every cached component under diamonds, SCCs, both directions, query permutations and eviction; qualify memory and runtime effects. |
| 11 | [DAWGS-080](variance_findings.md#dawgs-080) — Nested sensitive keys survive preserved outer properties | I | Repair | M | Define default/unclassified nested-map handling separately from explicit preserve overrides, then apply full-scrub policy recursively where promised. |
| 12 | [DAWGS-101](variance_findings.md#dawgs-101) — JSON import numbers round through float64 | I | Repair | M | Preserve exact integers through both JSONL and document import and normalize values for driver binding. UseNumber alone does not establish lossless persistence. |
| 13 | [DAWGS-102](variance_findings.md#dawgs-102) — Directory manifests permit parent-relative fragment paths | I | Repair | M | Use one root-aware fragment access policy in checksum, validation and load reads. Prioritize where external manifests are accepted; current proof concerns local directory reads, not remote exfiltration. |
| 14 | [DAWGS-007](variance_findings.md#dawgs-007) — Regex match is a substring search | Q | Repair | M | Implement full-string regex compatibility and literal/parameter parity without corrupting flags, alternation, multiline behavior or backslashes. Historical caller exposure guides this rank. |
| 15 | [DAWGS-006](variance_findings.md#dawgs-006) — AND outside WHERE becomes a row filter | Q | Repair | M | Keep AND operands inside their expression outside WHERE; cover projected values and exact SET post-state. |
| 16 | [DAWGS-028](variance_findings.md#dawgs-028) — Endpoint predicates on a variable-length step are dropped | Q | Repair | L | Retain cross-endpoint predicates through variable expansion and fixed suffix continuation. Check both optimizer modes and mutation selection. |
| 17 | [DAWGS-024](variance_findings.md#dawgs-024) — OPTIONAL MATCH squares repeated input rows | Q | Repair | L | Preserve identity of duplicate OPTIONAL MATCH input rows; joining derived matches back by equal exported values must not multiply them. |
| 18 | [DAWGS-026](variance_findings.md#dawgs-026) — Reused relationship variables cross-join the edge table | Q | Repair | M | Correlate reused relationship identity to the newly joined edge; prevent an unrelated edge scan multiplying matches. |
| 19 | [DAWGS-027](variance_findings.md#dawgs-027) — Undirected later hops can reuse the near endpoint | Q | Repair | M | Pair undirected continuation orientations so the far endpoint cannot equal the near endpoint on a non-loop edge. |
| 20 | [DAWGS-031](variance_findings.md#dawgs-031) — Relationship uniqueness is incomplete | Q | Repair | L | Enforce edge uniqueness across comma parts and consecutive expansions, with correct duplicate-preserving path/cardinality semantics and performance checks. |
| 21 | [DAWGS-033](variance_findings.md#dawgs-033) — Undirected patterns mishandle self-loops | Q | Repair | M | Repair only the surviving self-reference pattern (x)-[r]-(x). Generic undirected self-loop omission and the old library-query attribution are already corrected. |
| 22 | [DAWGS-020](variance_findings.md#dawgs-020) — Fixed-length expansion is not equivalent to an exact range | Q | Repair | S | Normalize *k to an exact range and test lengths above 15. This bounded parsing fix can ship before changing the unbounded traversal policy. |
| 23 | [DAWGS-019](variance_findings.md#dawgs-019) — Unbounded traversals stop at 15 hops | Q | Design | M | Choose complete traversal versus explicit resource-bound/error behavior and qualify the implementation cost. The documented cap still truncates results; removing it alone is not the implementation plan. Execution changes may be L/XL. |
| 24 | [DAWGS-072](variance_findings.md#dawgs-072) — Parallel fetch operations hang when a worker fails | A | Repair | M | Stop and join the producer/workers on the first worker failure while preserving the originating error. Cancellation must unblock submission without swallowing the error. |
| 25 | [DAWGS-065](variance_findings.md#dawgs-065) — Kind mapping can deadlock the connection pool | D | Reproduce | M | Exercise effective finite-pool limits with transactions registering new kinds. Confirm the nested acquisition/lock wait before selecting a connection-ownership repair; source mechanism is current. |
| 26 | [DAWGS-099](variance_findings.md#dawgs-099) — ThreadSafeDuplex cross-operation deadlock | A | Repair | M | Remove the cross-wrapper lock-order cycle; cover concurrent cross-operations, self-operations and allocation/runtime costs of any snapshot strategy. |
| 27 | [DAWGS-076](variance_findings.md#dawgs-076) — Bitmap mutation during And/AndNot iteration | A | Repair | M | Avoid removing from a bitmap during its own non-native And/AndNot iteration; cover 32/64-bit, native/wrapped and empty/full operands. |
| 28 | [DAWGS-077](variance_findings.md#dawgs-077) — PackedMap merge loses values and EFSet.Find underflows | A | Repair | M | Treat PackedMap merged-value corruption and EFSet below-minimum search as two acceptance cases and likely separate patches. |
| 29 | [DAWGS-074](variance_findings.md#dawgs-074) — Traversal patterns changing direction never match | A | Repair | M | Use the next traversal step direction when fetching its endpoint. Coordinate the state-machine change with optional-step duplicate callbacks in 095. |
| 30 | [DAWGS-095](variance_findings.md#dawgs-095) — Optional following traversal step duplicates callbacks | A | Repair | M | Produce each optional-transition match once. Final consumer set deduplication is not a substitute for correct callback cardinality; external caller impact is unmeasured. |
| 31 | [DAWGS-100](variance_findings.md#dawgs-100) — FetchNodeProperties ignores iteration error | A | Repair | S | Return iteration failure from FetchNodeProperties instead of successful partial initialization. This is independently deliverable with a controlled result failure. |
| 32 | [DAWGS-003](variance_findings.md#dawgs-003) — Scalar-list quantifiers panic | Q | Contain | S | Return a normal unsupported/translation error instead of a scalar-list quantifier panic. Full scalar quantifier support is a separate M/L repair if promised. |
| 33 | [DAWGS-004](variance_findings.md#dawgs-004) — Literal null predicates underflow the expression stack | Q | Repair | M | Handle literal-null predicates without unchecked expression-stack underflow and establish correct null predicate results. |
| 34 | [DAWGS-011](variance_findings.md#dawgs-011) — New panics from valid input | Q | Contain | S | Guard compiler failure boundaries and reject unsupported temporal components explicitly. Complete temporal handling is additional M work; a recovery boundary alone does not fix producer bugs. |
| 35 | [DAWGS-030](variance_findings.md#dawgs-030) — Aggregate traversal count lowering miscounts | Q | Contain | S | Return a normal translation error for the unsafe aggregate/pattern-predicate shape until the generic 036 path is valid; declining optimization alone still reaches that path. Old source-label/repeated-terminal miscounts stay closed. |
| 36 | [DAWGS-086](variance_findings.md#dawgs-086) — Empty v2 KindIn drops relationship predicate | B | Repair | M | Settle empty-kind consistency and preserve an explicit restrictive predicate or clear error. Current v2 relationship output drops the condition and node output has a blank WHERE. |

### P2: Next correctness and reliability wave — 59 items

| Order | Finding | Track | Action | Effort | Next step and reason |
| ---: | --- | :---: | --- | :---: | --- |
| 37 | [DAWGS-008](variance_findings.md#dawgs-008) — Double negation collapses to a single NOT | Q | Repair | S | Preserve NOT count/parity instead of collapsing double negation. |
| 38 | [DAWGS-009](variance_findings.md#dawgs-009) — Whole-number float literals become integers | Q | Repair | S | Preserve integral floating-point literal types in both emitters, the Neo4j rewrite and cache preparation. |
| 39 | [DAWGS-051](variance_findings.md#dawgs-051) — Large integer properties lose precision in results | R | Repair | M | Decode JSON integers exactly with explicit supported ranges across scalar/map/entity results; coordinate numeric normalization with import 101. |
| 40 | [DAWGS-057](variance_findings.md#dawgs-057) — String values that look like JSON are decoded a second time | R | Repair | M | Distinguish driver JSON bytes/values from ordinary strings that happen to resemble JSON; retain legitimate structured decoding. |
| 41 | [DAWGS-016](variance_findings.md#dawgs-016) — Arithmetic between two properties is concatenation or trigram similarity | Q | Repair | L | Choose numeric arithmetic from value/type information rather than applying concatenation or text modulo to two properties. |
| 42 | [DAWGS-018](variance_findings.md#dawgs-018) — String predicates and ordered comparisons on non-string values use the value's text form | Q | Repair | L | Preserve type boundaries for non-string string predicates and numeric comparison. Current numeric ordering casts text; do not restore the obsolete lexicographic claim. |
| 43 | [DAWGS-052](variance_findings.md#dawgs-052) — Properties projected through WITH stay raw jsonb | R | Repair | L | Carry property value/type semantics through WITH before toString and later consumers; avoid raw-JSON quoting changes. |
| 44 | [DAWGS-053](variance_findings.md#dawgs-053) — Integer sum loses precision; empty-input sum is null | R | Repair | M | Preserve supported integer aggregate precision and the empty SUM identity; qualify AVG separately. |
| 45 | [DAWGS-054](variance_findings.md#dawgs-054) — collect(n.prop) returns strings for numbers and booleans | R | Repair | M | Keep property values typed through collect/UNWIND rather than converting numeric and Boolean elements into strings. |
| 46 | [DAWGS-056](variance_findings.md#dawgs-056) — Entity membership compares raw ids across entity types | R | Repair | M | Retain entity type during collected-ID membership lowering, including with optimization disabled and overlapping node/edge IDs. |
| 47 | [DAWGS-015](variance_findings.md#dawgs-015) — `toInteger` rounds instead of truncating | Q | Repair | S | Implement the chosen Cypher toInteger truncation behavior explicitly, including negative and boundary values. |
| 48 | [DAWGS-014](variance_findings.md#dawgs-014) — STARTS WITH / ENDS WITH / CONTAINS escape `%` and `_` only for a bare property operand | Q | Repair | M | Escape literal substring/prefix/suffix operands with functional left sides; keep this separate from regex handling. |
| 49 | [DAWGS-040](variance_findings.md#dawgs-040) — WITH ordering, skip, and limit are ignored | Q | Repair | M | Apply WITH ordering and pagination to read-only boundaries; retain already-correct staged mutation modifiers. |
| 50 | [DAWGS-021](variance_findings.md#dawgs-021) — Leading OPTIONAL MATCH loses the null row | Q | Repair | L | Seed leading OPTIONAL MATCH with a null-preserving input. Share row/cardinality design with 024, while keeping separate regressions. |
| 51 | [DAWGS-036](variance_findings.md#dawgs-036) — A pattern predicate in a MATCH followed by another MATCH gives invalid SQL and drops the predicate's type filter | Q | Repair | L | Bind pattern predicates to valid frames and retain their relationship-type filters when later MATCH clauses are present. |
| 52 | [DAWGS-035](variance_findings.md#dawgs-035) — Optimizer pattern reordering ignores pattern-predicate dependencies | Q | Repair | M | Include pattern-predicate dependencies in safe clause reordering; test that unresolved bindings remain pinned. |
| 53 | [DAWGS-046](variance_findings.md#dawgs-046) — WITH → MATCH → WITH builds a self-referencing CTE | Q | Repair | M | Fix read-only WITH/MATCH/WITH frame construction and out-of-scope/self CTE references. |
| 54 | [DAWGS-047](variance_findings.md#dawgs-047) — ORDER BY alias expressions and internal-name collisions | Q | Repair | M | Rewrite nested ORDER BY alias expressions safely and separate user identifiers from internal binding names. |
| 55 | [DAWGS-042](variance_findings.md#dawgs-042) — UNWIND generates invalid SQL for common forms | Q | Repair | M | Translate property-array UNWIND as an actual typed collection; leading literal and empty-alias pipelines are fixed controls. |
| 56 | [DAWGS-043](variance_findings.md#dawgs-043) — DELETE with several targets, or followed by RETURN, generates invalid SQL | Q | Repair | M | Define valid continuation/projection after final DELETE and preserve needed input rows. Duplicate multi-target delete CTE naming stays closed. |
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

A practical starting allocation is four engineering lanes plus a shared validation owner. These are coordination boundaries; no headcount or staffing availability is assumed.

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
4. **063/071 should share graph-selection work.** Preserve fixed partition routing in 062. Test the distinct parent-table trigger caveat as part of 039's connected-node matrix rather than reopening every graph-scoped read.
5. **051/057 and 101 share representation rules, with different boundaries.** Result decoding must preserve string identity and exact values; import must also produce values accepted by both drivers. 105's numeric-cast API decision does not justify corrupting JSON input while it is pending.
6. **059, 060, 061, 085 and 086 touch different emitters or rewrite paths.** Share Boolean/type/escaping controls without assuming one formatter patch fixes a hand-built upsert string. The repaired old OR cleanup caller in 059 stays out of the residual impact claim.
7. **Cancellation and locking need complete lifetimes.** Fixing 072 must retain the originating error; 091 needs a controlled shutdown schedule before adding waits; 099 must avoid a new inversion or uncontrolled allocation cost. Cover early close, cancellation and error draining.
8. **109 and 112 require independent decisions.** Driver transaction rotation, multi-batch import recovery, batch ordering 106 and schema ownership 110 are different contracts. Atomic recovery or resumability should receive a separate design after its guarantee is chosen.
9. **Containment is an independently shippable result.** Clear rejection for 001/002/010/013/017/103 and safe optimizer decline can stop misleading success or panics. Full feature support requires its own acceptance scope and should remain visible after containment.

## Resolved original findings: keep as regression controls

These nine original findings have no new repair slot. Their current corrected behavior must remain intact while adjacent work changes shared code. The caveat associated with 062 is already included in the urgent deletion investigation above.

| Finding | Track | Protect while changing |
| --- | :---: | --- |
| [DAWGS-025](variance_findings.md#dawgs-025) | Q | Dual-bound exact endpoint correlation and row counts during pattern/binding work. |
| [DAWGS-029](variance_findings.md#dawgs-029) | Q | Previously bound expansion terminal identity, including zero-hop and carried bindings. |
| [DAWGS-032](variance_findings.md#dawgs-032) | Q | Ordinary expansion continuation after a first-edge self-loop. |
| [DAWGS-034](variance_findings.md#dawgs-034) | Q | Zero-only expansion without one-edge neighbors. |
| [DAWGS-038](variance_findings.md#dawgs-038) | Q | Nonfinal DELETE visibility before aggregation and retained input projection. |
| [DAWGS-044](variance_findings.md#dawgs-044) | Q | Staged multipart SET/REMOVE/CREATE execution and write visibility. |
| [DAWGS-048](variance_findings.md#dawgs-048) | Q | Distinct aliases referencing the original WITH input frame. |
| [DAWGS-050](variance_findings.md#dawgs-050) | R | Implicit result expression names and correct SQL quoting. |
| [DAWGS-062](variance_findings.md#dawgs-062) | D | Graph-specific node/edge partition routing for reads and mutations; retain separate trigger-scope validation. |

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

For implementation, follow AGENTS.md: inspect affected code, propose a plan before schema/interface/performance changes, format touched code, preserve backend-equivalent core cases, and add mutation/template coverage for parser/translation/rendering/query-semantics changes. Verify both PostgreSQL optimizer modes where relevant. Keep concurrency and driver-specific behavior in properly scoped tests.

Arrange the selected backend's `CONNECTION_STRING` before live validation, then use the required backend-selected checks. The earlier unit-suite success in variance_findings.md is historical evidence for the original triage; new runtime results are recorded in the work ledger above. Gate changes should preserve visibility of unsupported TCK assertions while making the enforced supported set reliable.

The original prioritization pass checked complete ID/observation coverage, status and workstream counts, link targets, ordering and Markdown structure. Implementation progress and validation now belong in the work ledger; the queue retains its baseline ordering and estimates. Calendar estimates should follow the initial high-consequence probes and contract choices; the present relative sizes support sequencing and ownership, not a delivery-date commitment.

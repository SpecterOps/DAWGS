# DAWGS Cypher Variances

## Table of contents

- [Introduction and how to read this report](#introduction-and-how-to-read-this-report)
- [Priorities for BloodHound consumers](#priorities-for-bloodhound-consumers)
- [Findings index](#findings-index)
- [Query parsing and expression evaluation](#query-parsing-and-expression-evaluation)
- [Pattern matching and path traversal](#pattern-matching-and-path-traversal)
- [Clause ordering, projection, and updates](#clause-ordering-projection-and-updates)
- [Result values, types, and decoding](#result-values-types-and-decoding)
- [Query builders and translation caching](#query-builders-and-translation-caching)
- [Database drivers, graph isolation, and batching](#database-drivers-graph-isolation-and-batching)
- [Graph algorithms, caches, and utilities](#graph-algorithms-caches-and-utilities)
- [Import, export, and test tooling](#import-export-and-test-tooling)
- [Findings requiring runtime validation](#findings-requiring-runtime-validation)
- [Behaviors requiring an API contract decision](#behaviors-requiring-an-api-contract-decision)
- [Downstream impact and validation](#downstream-impact-and-validation)
- [Appendix A: Shared fixture for DAWGS-019 and DAWGS-020](#appendix-a-shared-fixture-for-dawgs-019-and-dawgs-020)
- [Appendix B: Shared fixture for DAWGS-025, DAWGS-028, and DAWGS-030](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030)
- [Appendix C: Full downstream query catalogue](#appendix-c-full-downstream-query-catalogue)
- [Appendix D: Specification and support boundaries](#appendix-d-specification-and-support-boundaries)

## Introduction and how to read this report

This report describes DAWGS behavior that can produce incorrect results, lose data, raise unexpected errors, or affect callers of its Go APIs. Findings are organized by the component developers would investigate. Start with **Expected** and **Observed**, then use the reproduction, source links, and regression criterion to understand the problem and check a fix.

**Versions tested:** DAWGS `c107686a5e8685693a7df660d2ee968c758c17af`; PostgreSQL 16.15. **Specification reference:** openCypher `677cbafabb8c3c5eed458fd3b1ec0daec8d67d23`. Source links point to fixed commits. Specification references use source lines where available and page numbers for the PDF.

The [consumer priorities](#priorities-for-bloodhound-consumers) identify which findings to address first for BloodHound deployments and link to affected queries. The findings index lists the affected component, backend, evidence status, and general impact priority. Finding IDs are cross-reference labels; use the status and priority fields to assess the evidence and impact. “Not assigned” means a priority has not been set. Each precondition supplies graph construction code, a shared-fixture link, or “Empty graph.” Additional setup lists configuration and API inputs separately. For pending findings, example graphs are proposed validation inputs; the runtime failure still needs to be demonstrated.

| Evidence status | What it establishes |
| --- | --- |
| Reproduced: PostgreSQL / Neo4j / Go | A query or API probe demonstrated the primary symptom. Related examples and suggested causes may still need investigation. |
| Generated output checked | Emitted Cypher or data was inspected. This does not establish execution behavior on Neo4j. |
| Source-supported / runtime validation required | The code suggests a problem, but a complete runtime reproduction is still needed. |
| Contract decision needed | The behavior depends on an API promise that must be decided before requiring a fix. |
| Related | A manifestation of another finding, rather than a separate root cause. |

Runtime-validation candidates and API contract questions have their own sections in the main body. Their status must be resolved before treating them as confirmed defects. Each regression criterion states the behavior a fix should establish; a likely cause is a starting point for investigation.

Some findings concern openCypher language rules; others concern DAWGS APIs, caching, data import, or query generation. A grammar citation establishes valid syntax, but may not define its behavior. Where no direct openCypher rule was identified, the finding states its compatibility expectation or API scope. See [specification scope](#specification-scope-of-the-findings) for these distinctions.

The appendices provide shared fixtures, the downstream query catalogue, and support boundaries. Reproductions and their necessary context are included here; no companion report or data file is needed to interpret them.

### Reproduction conventions

Execute and commit each **Precondition** Cypher block before its reproduction query, using a fresh graph for each case. “Empty graph.” means no existing nodes or relationships are required. Go blocks construct in-memory graphs or nodes with explicit IDs. **Additional setup** covers schema, parameters, caches, files, and concurrency controls. Examples labeled proposed do not establish runtime evidence.

Use these conventions to repeat the findings:

1. Execute Cypher through the DAWGS PostgreSQL driver, using `graph.Transaction.Query`; do not submit Cypher directly to PostgreSQL's SQL interface. The reproductions used write transactions for mutation statements and inspected `graph.Result.Keys()` and `Values()` for results. Application-specific BloodHound mutation controls were outside this driver-level test.
2. Start each finding with its own empty graph in an isolated test database unless another precondition is stated. Commit a setup block before executing its query block. Multiple CREATE clauses in one block form a single Cypher statement and must retain their variable scope.
3. Statements in separate code blocks are separate queries unless labeled otherwise. Queries involving mutations require write transactions. Record errors as well as rows, column names, and persistent graph changes.
4. Check side effects independently of the returned aggregate. Node/relationship additions and removals are identity-based; property changes compare entity/key/value entries; label changes count distinct labels present in the graph. Unspecified side effects must be zero.
5. The depth observations were repeated with `pg.SetOptimizedTranslation(false)` and `pg.SetOptimizedTranslation(true)` before query compilation. Other focused observations use the driver's normal translation mode.
6. Each entry names its backend and evidence status. The Cypher probes tested both optimizer modes; API observations do not imply an optimizer comparison. Source-supported entries have not all been executed. API-level examples are call sketches; undefined variables represent the preconditions stated in the finding.

## Priorities for BloodHound consumers

These assessments follow the actual application callers, query preparation, configuration, and result handling. **P1** is a demonstrated application correctness issue on the named backend. **P2** is a narrower correctness, resource-budget, or legacy-upgrade issue. **Conditional** requires a nondefault backend or gated feature.

[Enterprise default backend](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/cmd/bhe/main.go#L28-L36) selects PostgreSQL. [BloodHound default configuration](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/config/default.go#L82-L89) selects Neo4j, while [active-driver resolution](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/api/tools/dbswitch.go#L80-L110) can override configuration using the persisted driver selection. Confirm the active backend before applying a backend-specific priority; these source defaults are not deployment inventory. Enterprise links labeled test catalogue identify HoundStrike definitions, not production endpoints.

| Disposition | Finding | Application route | Affected query / caller | Validation and reason |
| --- | --- | --- | --- | --- |
| P1 | [DAWGS-007](#dawgs-007) | PostgreSQL | [Built-in AGT Global Administrators](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L394-L398) | Two edges through the application query service versus one with a full-match control; sample-derived library queries also differ. |
| P1 | [DAWGS-019](#dawgs-019) | PostgreSQL | [Built-in All Domain Admins](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L42-L46) | Application query service returned an empty graph; the 16-hop-bound control returned 17 nodes and 16 edges. |
| P1 | [DAWGS-073](#dawgs-073) | Shared analysis API | [CanRDP eligibility](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L583-L619) | Actual application function dropped user 5 after cache warming, including with the normal database-backed cache factory. |
| P2 | [DAWGS-098](#dawgs-098) | PostgreSQL query service | [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611) | A 64 KiB literal passed a 16 KiB result budget. Default-budget exhaustion and peak memory were not measured. |
| P2 — library queries | [DAWGS-033](#dawgs-033) | PostgreSQL Explore queries | [Computer-path library query](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20incoming%20and%20local%20paths%20for%20a%20specific%20computer.yml#L7-L12) | The query passed application preparation and omitted the qualifying self-loop; no matching built-in search was identified. |
| P2 — legacy upgrades | [DAWGS-072](#dawgs-072) | Migration error path | [Version_513_Migration](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L443-L460) | Six-worker migration waited for cancellation at max ID 140000 with injected worker failures; the smaller control returned promptly. |
| Conditional — Neo4j reconciliation | [DAWGS-059](#dawgs-059) | Enterprise, nondefault backend | [Enterprise trust cleanup](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L134-L169) | Live selection/deletion simulation found extra edges, but default Enterprise uses PostgreSQL and reconciliation is gated. |

Affected query and caller links are included in each finding and in the [downstream validation](#downstream-impact-and-validation) and [full query catalogue](#appendix-c-full-downstream-query-catalogue).

## Findings index

Use the component and backend columns to find the relevant code. Status describes the evidence; priority describes urgency. “Not assigned” does not mean low impact. Each ID links to its finding.

| ID | Problem | Component | Backend / scope | Evidence status | Priority |
| --- | --- | --- | --- | --- | --- |
| [DAWGS-001](#dawgs-001) | List indexing returns the index | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-002](#dawgs-002) | List slicing returns the upper bound | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-003](#dawgs-003) | Scalar-list quantifiers panic | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: unhandled library panic |
| [DAWGS-004](#dawgs-004) | Literal null predicates underflow the expression stack | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: unhandled library panic |
| [DAWGS-005](#dawgs-005) | Required semantic validation is missing | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Medium: invalid queries accepted |
| [DAWGS-006](#dawgs-006) | AND outside WHERE becomes a row filter | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result and stored value |
| [DAWGS-007](#dawgs-007) | Regex match is a substring search | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-008](#dawgs-008) | Double negation collapses to a single NOT | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-009](#dawgs-009) | Whole-number float literals become integers | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-010](#dawgs-010) | shortestPath used as an expression returns a node | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Medium: silent wrong result |
| [DAWGS-011](#dawgs-011) | New panics from valid input | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | High: unhandled library panic |
| [DAWGS-012](#dawgs-012) | A comment before an arithmetic operator shifts the operator sequence | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-013](#dawgs-013) | `CREATE UNIQUE` and `LOAD CSV` are accepted and dropped | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-014](#dawgs-014) | STARTS WITH / ENDS WITH / CONTAINS escape `%` and `_` only for a bare property operand | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-015](#dawgs-015) | `toInteger` rounds instead of truncating | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-016](#dawgs-016) | Arithmetic between two properties is concatenation or trigram similarity | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-017](#dawgs-017) | A standalone CALL parses to `(nil, nil)`; formatting that nil query panics | Query parsing and expression evaluation | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-018](#dawgs-018) | String predicates and ordered comparisons on non-string values use the value's text form | Query parsing and expression evaluation | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-019](#dawgs-019) | Unbounded traversals stop at 15 hops | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: incomplete results |
| [DAWGS-020](#dawgs-020) | Fixed-length expansion is not equivalent to an exact range | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: incorrect bound |
| [DAWGS-021](#dawgs-021) | Leading OPTIONAL MATCH loses the null row | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: wrong cardinality |
| [DAWGS-022](#dawgs-022) | Parallel same-type relationships are rejected | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: incompatible data model |
| [DAWGS-023](#dawgs-023) | An unknown label raises an error | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Medium: wrong empty-match behavior |
| [DAWGS-024](#dawgs-024) | OPTIONAL MATCH squares repeated input rows | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: wrong cardinality |
| [DAWGS-025](#dawgs-025) | Bound right nodes cross-join the node table | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: wrong cardinality |
| [DAWGS-026](#dawgs-026) | Reused relationship variables cross-join the edge table | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: wrong cardinality |
| [DAWGS-027](#dawgs-027) | Undirected later hops can reuse the near endpoint | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-028](#dawgs-028) | Endpoint predicates on a variable-length step are dropped | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-029](#dawgs-029) | Variable-length steps ignore an earlier binding of their terminal | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-030](#dawgs-030) | Aggregate traversal count lowering miscounts | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong count (optimizer only) |
| [DAWGS-031](#dawgs-031) | Relationship uniqueness is incomplete | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Medium-high: wrong cardinality |
| [DAWGS-032](#dawgs-032) | A first-hop self-loop stops variable-length expansion | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Medium-high: incomplete results |
| [DAWGS-033](#dawgs-033) | Undirected patterns mishandle self-loops | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Medium-high: silent wrong result |
| [DAWGS-034](#dawgs-034) | `*0..0` also returns one-hop neighbours | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-035](#dawgs-035) | Optimizer pattern reordering ignores pattern-predicate dependencies | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-036](#dawgs-036) | A pattern predicate in a MATCH followed by another MATCH gives invalid SQL and drops the predicate's type filter | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-037](#dawgs-037) | Pattern map parameters use jsonb containment | Pattern matching and path traversal | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-038](#dawgs-038) | DELETE before aggregating WITH is omitted | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | High: mutation silently skipped |
| [DAWGS-039](#dawgs-039) | Plain DELETE silently detaches connected nodes | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | High: unexpected data removal |
| [DAWGS-040](#dawgs-040) | WITH ordering, skip, and limit are ignored | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-041](#dawgs-041) | SET loses updates and drops rows | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | High: mutation silently skipped |
| [DAWGS-042](#dawgs-042) | UNWIND generates invalid SQL for common forms | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-043](#dawgs-043) | DELETE with several targets, or followed by RETURN, generates invalid SQL | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-044](#dawgs-044) | SET, REMOVE, or CREATE before WITH generates invalid SQL | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-045](#dawgs-045) | SET and REMOVE of the same property or label apply removals first | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-046](#dawgs-046) | WITH → MATCH → WITH builds a self-referencing CTE | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-047](#dawgs-047) | ORDER BY over an alias expression references an internal name; user aliases can collide with internal names | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-048](#dawgs-048) | `WITH n, n AS m` generates invalid SQL | Clause ordering, projection, and updates | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-049](#dawgs-049) | CREATE retains a null-valued property | Clause ordering, projection, and updates | PostgreSQL | Reproduced storage observation; related to documented null behavior | Not assigned |
| [DAWGS-050](#dawgs-050) | Unaliased expressions lose their result-column name | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Medium: result API mismatch |
| [DAWGS-051](#dawgs-051) | Large integer properties lose precision in results | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | High: silent numeric corruption |
| [DAWGS-052](#dawgs-052) | Properties projected through WITH stay raw jsonb | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-053](#dawgs-053) | Integer `sum` loses precision; empty-input `sum` is null (`avg` cast is source-supported) | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-054](#dawgs-054) | `collect(n.prop)` returns strings for numbers and booleans | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-055](#dawgs-055) | Result aliases are emitted unquoted | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-056](#dawgs-056) | Entity membership compares raw ids across entity types; the collect-ID rewrite runs with the optimizer disabled | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-057](#dawgs-057) | String values that look like JSON are decoded a second time | Result values, types, and decoding | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-058](#dawgs-058) | The translation cache returns another query's SQL | Query builders and translation caching | PostgreSQL | Reproduced: PostgreSQL | High: silent wrong result |
| [DAWGS-059](#dawgs-059) | The Neo4j builder rewrite turns OR into AND | Query builders and translation caching | Neo4j adapter / builder | Reproduced: Neo4j; other forms generated-output only | High: silent wrong result |
| [DAWGS-060](#dawgs-060) | The `tx.Query` rewrite re-emits a lossy AST whenever it applies | Query builders and translation caching | Neo4j adapter / builder | Generated output checked | Not assigned |
| [DAWGS-061](#dawgs-061) | Kind and identity-property names are emitted without identifier escaping | Query builders and translation caching | Neo4j adapter / builder | Generated output checked | Not assigned |
| [DAWGS-062](#dawgs-062) | PostgreSQL reads and deletes ignore the target graph | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | High: cross-graph data loss |
| [DAWGS-063](#dawgs-063) | WithGraph drops existing indexes and constraints | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | High: schema destruction |
| [DAWGS-064](#dawgs-064) | Batch de-duplication keys collide | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | High: silent data loss |
| [DAWGS-065](#dawgs-065) | Kind mapping can deadlock the connection pool | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | High: driver hang |
| [DAWGS-066](#dawgs-066) | Changing the default graph is silently ignored after the first one | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-067](#dawgs-067) | `AssertSchema` never creates the non-default graphs it is given | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-068](#dawgs-068) | Updating with no changes or nil properties panics | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-069](#dawgs-069) | `query.SelectKindID` loads the wrong SQL file and always fails | Database drivers, graph isolation, and batching | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-070](#dawgs-070) | `batch.UpdateNodes` ignores property deletions; an integration test asserts the defect | Database drivers, graph isolation, and batching | Neo4j adapter / builder | Generated output checked | Not assigned |
| [DAWGS-071](#dawgs-071) | Dumping an unknown graph name creates that graph | Database drivers, graph isolation, and batching | PostgreSQL | Related; source-supported | Not assigned |
| [DAWGS-072](#dawgs-072) | Parallel fetch operations hang when a worker fails | Graph algorithms, caches, and utilities | PostgreSQL | Reproduced: PostgreSQL | High: caller hang |
| [DAWGS-073](#dawgs-073) | ReachabilityCache returns incomplete reach sets | Graph algorithms, caches, and utilities | Go library / utility | Reproduced: Go | High: silent wrong result |
| [DAWGS-074](#dawgs-074) | Traversal patterns that change direction never match | Graph algorithms, caches, and utilities | PostgreSQL | Reproduced: PostgreSQL | Medium-high: missing results |
| [DAWGS-075](#dawgs-075) | `graphcache.FetchRelationshipsByID` queries all ids, then slices by the missing count | Graph algorithms, caches, and utilities | PostgreSQL | Reproduced: PostgreSQL | Not assigned |
| [DAWGS-076](#dawgs-076) | Bitmap `And` / `AndNot` with a non-native duplex mutate during iteration | Graph algorithms, caches, and utilities | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-077](#dawgs-077) | `PackedMap` corrupts values after inserting keys between compactions; `EFSet.Find` underflows below the minimum | Graph algorithms, caches, and utilities | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-078](#dawgs-078) | `BFSTreeFile` reads the compressed source instead of the gzip reader; `ToSegment` indexes edge -1 | Graph algorithms, caches, and utilities | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-079](#dawgs-079) | `SortAndSliceNodeSet` panics when skip or limit exceeds the set | Graph algorithms, caches, and utilities | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-098](#dawgs-098) | `FetchByQuery` undercounts nested path and literal memory | Graph algorithms, caches, and utilities | BloodHound query service / PostgreSQL | Reproduced: BloodHound/PostgreSQL literal; path variants pending | Medium: result-budget enforcement |
| [DAWGS-080](#dawgs-080) | Full scrub does not inspect sensitive nested keys beneath an unclassified outer property | Import, export, and test tooling | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-081](#dawgs-081) | Scrub configuration options are parsed but unused | Import, export, and test tooling | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-082](#dawgs-082) | Dumps from Neo4j write temporal properties as `{}` | Import, export, and test tooling | Neo4j adapter / builder | Generated output checked | Not assigned |
| [DAWGS-083](#dawgs-083) | Unpacking into an output directory with a trailing slash fails | Import, export, and test tooling | Go library / utility | Reproduced: Go | Not assigned |
| [DAWGS-084](#dawgs-084) | The bundled BDD command can report success with failing TCK cases | Import, export, and test tooling | BDD harness; PostgreSQL-selected run | Reproduced test-reporting issue | Not assigned |
| [DAWGS-085](#dawgs-085) | Builder `query.Xor` adds no parentheses, unlike `query.Or` | Query builders and translation caching | Neo4j adapter / builder | Runtime validation pending | Not assigned |
| [DAWGS-086](#dawgs-086) | v2 `KindIn` with no kinds on a relationship matches every relationship | Query builders and translation caching | Neo4j adapter / builder | Runtime validation pending | Not assigned |
| [DAWGS-087](#dawgs-087) | `UpdateNodeBy` ignores deleted kinds on PostgreSQL | Database drivers, graph isolation, and batching | PostgreSQL | Runtime validation pending | Not assigned |
| [DAWGS-088](#dawgs-088) | A concurrent kind insert from another session fails `AssertKinds` | Database drivers, graph isolation, and batching | PostgreSQL | Runtime validation pending | Not assigned |
| [DAWGS-089](#dawgs-089) | `GetKindIDsByKind` exposes an internal map; `FetchKinds` iterates it after unlocking | Database drivers, graph isolation, and batching | PostgreSQL | Runtime validation pending | Not assigned |
| [DAWGS-090](#dawgs-090) | Mixed-case index fields are dropped and rebuilt on every start | Database drivers, graph isolation, and batching | PostgreSQL | Runtime validation pending | Not assigned |
| [DAWGS-091](#dawgs-091) | Cursor `Close` races the producer goroutine | Database drivers, graph isolation, and batching | PostgreSQL | Runtime validation pending | Not assigned |
| [DAWGS-092](#dawgs-092) | Nil properties panic in the Neo4j create and update paths | Database drivers, graph isolation, and batching | Neo4j adapter / builder | Runtime validation pending | Not assigned |
| [DAWGS-093](#dawgs-093) | Running Neo4j operations do not receive the caller context | Database drivers, graph isolation, and batching | Neo4j adapter / builder | Runtime validation pending | Not assigned |
| [DAWGS-094](#dawgs-094) | `DatabaseSwitch` does not forward size setters, races on `inSwitch`, and `IsDriver` is wrong | Database drivers, graph isolation, and batching | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-095](#dawgs-095) | Traversal patterns report a match twice when the next step is optional | Graph algorithms, caches, and utilities | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-096](#dawgs-096) | Acyclic helpers replace the caller's ExpansionFilter; terminal/limit semantics need clarification | Graph algorithms, caches, and utilities | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-097](#dawgs-097) | `BreadthFirst` returns nil with partial results when the caller's context is cancelled | Graph algorithms, caches, and utilities | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-099](#dawgs-099) | `ThreadSafeDuplex` deadlocks on concurrent cross-operations | Graph algorithms, caches, and utilities | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-100](#dawgs-100) | `FetchNodeProperties` swallows mid-iteration errors | Graph algorithms, caches, and utilities | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-101](#dawgs-101) | Load decodes JSONL numbers as float64 | Import, export, and test tooling | JSON import; backend-independent | Runtime validation pending | Not assigned |
| [DAWGS-102](#dawgs-102) | Directory-based load and verify follow `../` manifest paths | Import, export, and test tooling | Go library / utility | Runtime validation pending | Not assigned |
| [DAWGS-103](#dawgs-103) | In-query CALL is silently omitted during compilation | Query parsing and expression evaluation | PostgreSQL | Generated output checked | Not assigned |
| [DAWGS-104](#dawgs-104) | A graph-building CREATE query expands to about 77.8 MB of SQL | Query builders and translation caching | PostgreSQL | Compilation output size measured; performance impact pending | Not assigned |
| [DAWGS-105](#dawgs-105) | `AsNumeric` truncates and overflows silently | Result values, types, and decoding | Go library / utility | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-106](#dawgs-106) | Batch flush reorders operations | Database drivers, graph isolation, and batching | PostgreSQL | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-107](#dawgs-107) | Preset node IDs do not advance the id sequence | Database drivers, graph isolation, and batching | PostgreSQL | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-108](#dawgs-108) | `UpdateRelationshipBy` overwrites `lastseen` on start and end nodes | Database drivers, graph isolation, and batching | Neo4j adapter / builder | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-109](#dawgs-109) | Write transactions commit every `writeFlushSize` writes | Database drivers, graph isolation, and batching | Neo4j adapter / builder | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-110](#dawgs-110) | `AssertSchema` drops indexes it did not create and errors on composite indexes | Database drivers, graph isolation, and batching | Neo4j adapter / builder | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-111](#dawgs-111) | `Kinds.Remove` mutates the caller's backing array | Graph algorithms, caches, and utilities | Go library / utility | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-112](#dawgs-112) | Load commits per batch; a failed load blocks its own retry | Import, export, and test tooling | Go library / utility | Contract decision needed | Not assigned: contract decision needed |
| [DAWGS-113](#dawgs-113) | `opengraph.ParseDocument` accepts misspelled top-level keys and trailing data | Import, export, and test tooling | Go library / utility | Contract decision needed | Not assigned: contract decision needed |

## Query parsing and expression evaluation

These findings concern how DAWGS reads a query and evaluates its expressions.

<a id="dawgs-001"></a>

### DAWGS-001: List indexing returns the index

**Status:** Reproduced on PostgreSQL.

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

Empty graph.

**openCypher specification:** Select the indexed element, rather than the index. [List indexing, printed p. 16–17](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=20); [Element-reference syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L743-L756).

```cypher
WITH [1,2] AS xs RETURN xs[0] AS x
```

**Expected:**

one row, `x = 1`.

**Observed:**

one row, `x = 0`.

The compiler emits `with s0 as (select array [1, 2]::int8[] as i0) select 0 as x from s0;`. It discards the subscript operation and projects the index literal. The live result confirms a value error, independently of column naming or numeric precision.

**Source evidence:** [frontend expression visitor](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/expression.go#L370); the base [list-operator callbacks](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/context.go#L427) are empty. The emitted SQL confirms lost semantics; the complete visitor correction has not been designed.

**Regression criterion:** first, last, negative, parameterized, and out-of-range subscripts must preserve Cypher element-selection and null/error behavior.

<a id="dawgs-002"></a>

### DAWGS-002: List slicing returns the upper bound

**Status:** Reproduced on PostgreSQL.

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

Empty graph.

**openCypher specification:** A slice returns the selected list range. [List slicing, printed p. 17–18](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=21); [Slice syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L758-L765).

```cypher
RETURN [1,2,3][0..2] AS xs
```

**Expected:**

`xs = [1,2]`.

**Observed:**

`xs = 2`; emitted SQL is `select 2 as xs;`.

The result changes both value and shape from a list to a scalar. This is recorded separately from indexing because fixing one syntax form does not establish correct slicing. Source evidence is the same expression/list-operator visitor path as DAWGS-001; a shared frontend cause is likely, not independently proven for every shape.

**Regression criterion:** lower/upper bounds, omitted bounds, negative bounds, and empty slices must produce the appropriate list.

<a id="dawgs-003"></a>

### DAWGS-003: Scalar-list quantifiers panic

**Status:** Reproduced on PostgreSQL.

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: unhandled library panic.

**Precondition:**

Empty graph.

**openCypher specification:** The current grammar explicitly admits scalar-list quantifier expressions. This is syntax support, not a line-addressable truth-definition for ALL; the historical Cypher 9 CIP excluded these functions. Treat the specification-version boundary explicitly. [Quantifier expressions and element source](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L900-L913); [Historical quantifier exclusion](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2017-10-17-Cypher-9.adoc#L45-L55).

```cypher
RETURN all(x IN [1] WHERE x = 1) AS value
```

**Expected:**

`true`.

**Observed:**

a nil-pointer panic during compilation. This reproduction uses a nonempty, homogeneous integer list to avoid the published empty-array inference caveat.

**Source evidence:** [prepareFilterExpression](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/quantifiers.go#L59) dereferences `currentPattern.Parts` without checking whether `currentPattern` exists. The relevant scalar RETURN query has no pattern. The scalar RETURN example above demonstrates this panic; related quantifier forms may fail through other paths.

**Impact:** a query can propagate a panic to library callers. The probe recovered the panic; production panic-handling behavior was not assessed.

**Regression criterion:** support the scalar-list predicate, and ensure all supported/unsupported quantifier shapes return a result or ordinary error rather than panic.

<a id="dawgs-004"></a>

### DAWGS-004: Literal null predicates underflow the expression stack

**Status:** Reproduced on PostgreSQL.

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: unhandled library panic.

**Precondition:**

Empty graph.

**openCypher specification:** IS NULL tests absence of a value; literal null must be testable. [Null values, printed p. 22–23](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=26); [IS NULL, printed p. 92](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=96); [Null-predicate syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L703-L705).

```cypher
RETURN null IS NULL AS value
```

**Expected:**

`true`.

**Observed:**

`runtime error: index out of range [-1]`. Upstream `Null1`, scenario 4 and the focused reproduction agree.

**Source evidence:** [Builder.PopOperand](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L446) indexes the last stack element without checking for an empty stack; [null-predicate construction](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/expression.go#L319) and null-literal translation are the relevant producer/consumer path. The complete operand-accounting correction remains to be determined. Twenty positive-query panics share this underflow site, including more complex predicates.

The page's broad null-difference caveat does not describe a compiler panic.

**Regression criterion:** correct boolean results for literal `IS NULL` / `IS NOT NULL`, with no stack-underflow panic for any malformed or unsupported input.

<a id="dawgs-005"></a>

### DAWGS-005: Required semantic validation is missing

**Status:** Reproduced on PostgreSQL.

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Medium: invalid queries accepted.

**Precondition:**

Empty graph.

**Additional setup:**

- Run the two validation examples separately.

**openCypher specification:** Projection aliases must be distinct. The exact bound-node CREATE rejection rule was not located in the line-addressable specification sources; that subcase has no direct specification citation established here. [Distinct projection aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L56).

Two independently reproduced examples:

```cypher
MATCH (a) CREATE (a)
```

**Expected:**

a compile-time `VariableAlreadyBound` syntax error, even if the graph is empty.

**Observed:**

the query succeeds. The emitted statement performs no creation.

```cypher
RETURN 1 AS a, 2 AS a
```

**Expected:**

a compile-time `ColumnNameConflict` syntax error.

**Observed:**

the query succeeds with duplicate result names.

Upstream references: `Create1`, scenario 13 and `Return4`, scenario 10. The first family has 57 successful negative cases expecting `VariableAlreadyBound`; duplicate columns account for one case. These are validation-gap families, not a claim that they have one shared implementation cause. Source areas include [CREATE binding handling](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/create.go#L193), [scope/projection construction](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/projection.go#L628), and frontend semantic validation; no single missing check has been proven sufficient for all cases.

**Regression criterion:** reject each query with the required compile-time category/detail before executing mutations. The rejection must match the required error category and detail.

The broader upstream run contained 200 queries that unexpectedly succeeded rather than raising their specified errors. Only the two representative validation behaviors above and the connected-node case below are isolated here. The remaining cases are not individually classified against the support page, and some involve expressly excluded constructs. They must not be counted as 200 undocumented defects.

<a id="dawgs-006"></a>

### DAWGS-006: AND outside WHERE becomes a row filter

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result and stored value.

**Precondition:**

Primary query: Empty graph.

SET, ORDER BY, and coalesce variants:

```cypher
CREATE (:A {k:1}), (:A {k:2})
```

**Additional setup:**

- Run each mutation on a fresh fixture.

**openCypher specification:** AND evaluates to its Boolean result; projecting that expression preserves rows. [Conjunction and disjunction truth tables](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc#L95-L124); [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69).


```cypher
UNWIND [1,2,3] AS x RETURN x, x > 1 AND x < 3 AS mid
```

**Expected:**

three rows; `mid` is `false`, `true`, `false`.

**Observed:**

two rows. The emitted SQL is `select i0 as x, i0 > 1 as mid from unnest(array [1, 2, 3]::int8[]) as i0 where (i0 < 3);`. The right operand of the conjunction became a row filter.

The same mechanism affects:

- `RETURN true AND false AS v`: zero rows instead of one `false`.
- Setup `CREATE (:A {k: 1}), (:A {k: 2})`, then `MATCH (n:A) SET n.v = n.k > 0 AND n.k < 2`: both nodes store `v = true`. The second conjunct is silently dropped from the update.
- `ORDER BY n.k > 1 AND n.k < 3`: becomes a filter plus an ordering.
- `WHERE coalesce(n.k > 5 AND n.k < 2, true)`: becomes `coalesce(k > 5, true) AND k < 2`.

**Source evidence:** [CompleteBinaryExpression](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L1466) calls `ConstrainConjoinedOperandPair()` whenever the parenthetical and disjunction depths are zero, regardless of clause context. The popped operand is consumed by whichever SELECT or UPDATE next collects constraints; for SET, nothing consumes it. Parenthesizing the expression avoids the defect.

**Impact:** rows disappear from projections, and stored values silently differ from the requested expression.

**Regression criterion:** AND in projections, ORDER BY, SET/CREATE values, and function arguments evaluates to a boolean value with Cypher's three-valued semantics. WHERE behavior is unchanged.

<a id="dawgs-007"></a>

### DAWGS-007: Regex match is a substring search

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Downstream priority:** P1 — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

BloodHound's built-in AGT Global Administrators query passed the actual [PrepareCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L580) checks and returned two edges through [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611) where the full-match control returned one. The query service does not remove the translator's regex difference. Query Library Service Principal and MicrosoftDNS queries also have sample-derived discrepancies. P1 applies to PostgreSQL deployments using these queries; the Enterprise HoundStrike definition is test-catalogue evidence, not a production endpoint.

**Affected queries / consumers:** [All privileged Azure Service Principals](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20privileged%20Azure%20Service%20Principals.yml#L8-L11); [Non-default delegation on MicrosoftDNS container](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Non-default%20delegation%20on%20MicrosoftDNS%20container.yml#L7-L13); [All Global Administrators](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Global%20Administrators.yml#L7-L11); [BloodHound AGT Global Administrators](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L394-L398); [Enterprise Global Administrators test catalogue](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L228-L229); [BloodHound query service](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L611).

**Precondition:**

```cypher
CREATE (:A {name: "abc"}), (:A {name: "x.y"})
```

**Additional setup:**

- For the backslash cases, compare identical regex values supplied as literals and parameters.

**openCypher specification:** No direct full-string regex semantic rule was identified in this specification checkout. The current grammar admits =~, while the historical Cypher 9 proposal excluded it. The expected result here is a Neo4j Cypher compatibility baseline, not an established openCypher semantic rule. [Regex operator syntax only](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L695-L701); [Historical regex exclusion](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2017-10-17-Cypher-9.adoc#L85).


**Specification scope:** Neo4j Cypher regex compatibility; the local openCypher grammar accepts the operator, but its regex TCK files provide no semantic scenarios. See [Specification scope of the findings](#specification-scope-of-the-findings). This is not an established upstream regex TCK failure.

```cypher
MATCH (n:A) WHERE n.name =~ 'b' RETURN n.name AS name
```

**Expected:**

zero rows under Neo4j Cypher's full-string `=~` semantics, the compatibility baseline for this finding.

**Observed:**

`abc`. The emitted predicate is `(n0.properties ->> 'name') ~ @p` with `p = 'b'`. As a result, `n.name =~ '(?i)Global Administrator.*'` also matches `'Not Global Administrator X'`.

Separately, literal regex patterns are escaped as LIKE patterns. `WHERE n.name =~ 'x\\.y'` (Cypher value `x\.y`) and `=~ '.*\\d.*'` never match, because the backslash is doubled. The same patterns passed as parameters behave correctly.

**Source evidence:** [OperatorCypherRegexMatch](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L1213) maps directly to PostgreSQL `~` without anchoring. The [string-operator rewrite](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L348) applies [rewriteStringWildCardLiteral](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L195) to regex operands.

**Regression criterion:** `=~` matches the full string, and literal and parameterized patterns behave identically. Any anchoring implementation must also preserve inline flags, alternation, and multiline behavior; a particular wrapper is not established as a complete fix here.

<a id="dawgs-008"></a>

### DAWGS-008: Double negation collapses to a single NOT

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

```cypher
CREATE (:A {x: 1}), (:A {x: 2})
```

**openCypher specification:** Applying Boolean negation twice restores the input truth value. [Boolean truth tables, printed p. 50–51](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=54); [Repeated NOT factors](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L659-L660).


```cypher
MATCH (n:A) WHERE NOT NOT n.x = 1 RETURN n.x AS x
```

**Expected:**

`x = 1`.

**Observed:**

`x = 2`. The emitted predicate contains a single `not`. Formatting `RETURN NOT NOT true` produces `return not true`. Any even number of NOT operators inverts the predicate.

**Source evidence:** the [expression visitor](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/expression.go#L123) (also [line 131](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/expression.go#L131)) and the [conjunction visitor](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/conjunction.go#L63) (also [line 71](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/conjunction.go#L71)) test `len(ctx.AllNOT()) > 0` and build exactly one `Negation`. The grammar allows `( NOT SP? )*`.

**Regression criterion:** *k* NOT operators produce *k* negations (or their parity).

<a id="dawgs-009"></a>

### DAWGS-009: Whole-number float literals become integers

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

Empty graph.

**openCypher specification:** Floating-point values must retain their type; division is a mathematical operator. [Exact INTEGER and IEEE-754 FLOAT types](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-09-16-public-type-system-type-annotation.adoc#L88-L103); [Mathematical operators, printed p. 48](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=52); [Decimal and approximate numeric literals](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L1136-L1159).


```cypher
RETURN 3.0 / 2.0 AS a
```

**Expected:**

`a = 1.5`.

**Observed:**

`a = 1`. The emitted SQL is `select 3 / 2 as a;`. `RETURN 3.0` returns an integer, and `toString(1.0)` returns `"1"`.

**Source evidence:** the [SQL emitter](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/format/format.go#L184) formats float64 with `strconv.FormatFloat(v, 'f', -1, 64)` and no `::float8`. The [Cypher emitter](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/cypher/format/format.go#L434) has the same flaw: `1e3` becomes `1000`, `[1.0, 2.0]` becomes `[1, 2]`, and very large floats become unparseable. That emitter feeds the Neo4j query rewrite and builder cache keys (DAWGS-058).

**Regression criterion:** float literals keep a floating-point type through both emitters, and float arithmetic is never performed as integer arithmetic.

<a id="dawgs-010"></a>

### DAWGS-010: shortestPath used as an expression returns a node

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Medium: silent wrong result.

**Precondition:**

```cypher
CREATE (:A {name: "a"})-[:R]->(:B {name: "b1"}), (:B {name: "b2"})
```

**openCypher specification:** The current grammar explicitly includes a shortest-path expression. The baseline describes path-valued shortest-path matching; it does not establish the exact unreachable RETURN-expression result inline here. Returning the terminal node loses the syntactic construct. [Shortest-path expression](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L945-L946); [Legacy shortest-path pattern](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L340-L345); [Shortest paths return paths, printed p. 67](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=71).


```cypher
MATCH (a:A) MATCH (b:B) RETURN shortestPath((a)-[:R*]->(b)) AS p
```

**Expected:**

a path for `b1`, and `null` for `b2`, which is unreachable. An explicit unsupported-feature error would also be acceptable.

**Observed:**

node `b` on every row, including `b2`. The parsed AST is the variable `b`, and the emitted SQL ends with `select s1.n1 as p from s1;`.

**Source evidence:** the [shortest-path pattern](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/context.go#L355) callback is an empty stub, and it is not registered as unsupported. The atom visitor has no handler for this form, so the nested node patterns reach [AtomVisitor.EnterOC_Variable](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/atom.go#L202), which overwrites the atom with the last variable seen. [AtomVisitor.EnterOC_Expression](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/atom.go#L157) behaves the same way.

List and pattern comprehensions are rewritten the same way, but the page lists them as unsupported, so they are not counted here (see [Documentation boundary](#documentation-boundary)). The page documents `shortestPath` only in a MATCH pattern.

**Regression criterion:** `shortestPath()` and `allShortestPaths()` in expression position are either implemented or rejected with an explicit error before execution.

<a id="dawgs-011"></a>

### DAWGS-011: New panics from valid input

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** High: unhandled library panic.

**Precondition:**

Literal temporal expression and builder criteria: Empty graph.

Node-property variants:

```cypher
CREATE (:A {d:'2020-01-01'})
```

**Additional setup:**

- Bind n in the node-property variants. Keep the literal component names exactly as shown in the reproduction.

**openCypher specification:** The temporal component-access specification describes epochMillis and epochSeconds (case-sensitive names); the reported lowercase spellings and empty builder criteria do not establish the same semantic-value claim. Panic handling is a DAWGS API issue. [Temporal component access (testable CIP)](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/2.testable/CIP2015-08-06-date-time.adoc#L819-L849); [Property lookup syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L743-L753).


```cypher
RETURN datetime('2020-01-01').epochmillis AS v
```

**Expected:**

a value, or an ordinary error.

**Observed:**

`panic: runtime error: index out of range [-1]`. `date(n.d).year` and `WHERE datetime(n.d).epochseconds > 1` panic the same way. `datetime().epochseconds` without an argument works. Builder criteria with zero operands (`Filter(query.Or())`, `Not(query.And())`, `And(query.Or(), x)`) reach the same stack underflow.

**Source evidence:** in [translatePropertyLookup](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L151), the `TypeCast` branch pops the operand and pushes nothing back unless the target is a node or edge composite. The next pop underflows at [PopOperand](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L447). Empty criteria reach it through [PopParenthetical](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/translator.go#L566). The translation cache [recovers](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/translation_cache.go#L288) the panic and then [re-panics](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/translation_cache.go#L267) into the caller. These triggers are distinct from DAWGS-003 and DAWGS-004.

**Regression criterion:** these inputs return a value or an error. The translation cache converts translation panics into errors rather than re-raising them.

<a id="dawgs-012"></a>

### DAWGS-012: A comment before an arithmetic operator shifts the operator sequence

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {x:1}), (:A {x:2})
```

**Reproduction:**

With `CREATE (:A {x:1}),(:A {x:2})`, `MATCH (n:A) RETURN n.x - 1 /*c*/ - 2 + 3 AS v` should return 1 and 2; instead translation rejects binary operator `""` as incompatible. Translation fails before SQL execution.

**Expected:**

The comment does not change arithmetic parsing. The two example rows return 1 and 2.

**Observed:**

Translation rejected an empty binary operator instead of evaluating the expression.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/frontend/expression.go:150](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/expression.go#L150)

**Specification reference:** Block comments are permitted in queries; arithmetic operators retain their expression structure. [Comment syntax, printed p. 195](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=199); [Arithmetic expression syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L723-L732).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-013"></a>

### DAWGS-013: `CREATE UNIQUE` and `LOAD CSV` are accepted and dropped

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A), (:B)
```

**Additional setup:**

- Register T before checking the relationship-count control.

- For LOAD CSV, use `LOAD CSV FROM 'file:///missing.csv' AS row MATCH (n:A) RETURN count(*) AS c`, with that file absent.

**Reproduction:**

`MATCH (a:A),(b:B) CREATE UNIQUE (a)-[:T]->(b)` emits `select 1` and creates nothing; `LOAD CSV … MATCH (n)` is a plain MATCH

**Expected:**

Implement the accepted operation or reject it explicitly before executing a different query.

**Observed:**

The accepted constructs disappeared from the generated operation.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/frontend/context.go:291](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/context.go#L291), [cypher/frontend/context.go:279](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/context.go#L279)

**Specification reference:** No direct openCypher semantic rule is established for these legacy/vendor constructs. Silent acceptance is a DAWGS parser/compatibility concern; it is not evidence of an openCypher feature-support requirement.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-014"></a>

### DAWGS-014: STARTS WITH / ENDS WITH / CONTAINS escape `%` and `_` only for a bare property operand

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {name:'abc'}), (:A {name:'x.y'})
```

**Reproduction:**

`toLower(n.name) STARTS WITH 'a_'` matches `abc` (pattern `a_%`)

**Expected:**

Treat percent and underscore as literal characters in these string-search operands.

**Observed:**

The functional-left search treated SQL wildcard characters as wildcards and matched abc.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/expression.go:1217](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L1217)

**Specification reference:** String-search operators compare literal text rather than SQL LIKE wildcards. [Textual prefix, suffix, and substring semantics (testable CIP)](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/2.testable/CIP2015-09-15-STARTS-WITH-and-ENDS-WITH.adoc#L83-L98); [String matching, printed p. 87–88](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=91).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-015"></a>

### DAWGS-015: `toInteger` rounds instead of truncating

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Reproduction:**

`toInteger(1.9)` returns 2 and `toInteger(-1.5)` returns -2 (Cypher: 1 and -1); the error for unparseable strings is documented and excluded

**Expected:**

Under the compatibility expectation used in this finding, convert 1.9 to 1 and -1.5 to -1. The exact truncation rule was not located in the cited specification.

**Observed:**

The replay returned 2 for 1.9 and -2 for -1.5.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/function.go:865](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/function.go#L865)

**Specification reference:** The baseline defines conversion of floating-point/string values to INTEGER. Neither this paragraph nor the conversion CIP explicitly states the rounding/truncation rule; a direct specification citation for the exact rounding discrepancy has not been established. [toInteger, printed p. 148–149](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=152); [Conversion semantics delegated to toInt](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/2.testable/CIP2016-07-07-Type-conversion-functions.adoc#L41-L45).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-016"></a>

### DAWGS-016: Arithmetic between two properties is concatenation or trigram similarity

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {x:5, y:2})
```

**Reproduction:**

`n.x + n.y` with 5 and 2 returns `"52"`; `n.x % n.y` returns boolean `false`

**Expected:**

Add numeric properties numerically and evaluate `%` as numeric modulo.

**Observed:**

The replay returned the string "52" for addition and Boolean false for modulo.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/expression.go:790](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L790)

**Specification reference:** Numeric addition and modulo are mathematical operations; properties do not become strings merely by participating in an expression. [Mathematical operators, printed p. 48](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=52); [Exact INTEGER and IEEE-754 FLOAT types](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-09-16-public-type-system-type-annotation.adoc#L88-L103); [Numeric additive and multiplicative forms](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L723-L732).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-017"></a>

### DAWGS-017: A standalone CALL parses to `(nil, nil)`; formatting that nil query panics

**Status:** Replayed Go (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- No database connection or installed procedure is required; invoke the parser and formatter directly.

**Reproduction:**

`frontend.ParseCypher(ctx, "CALL db.labels()")` followed by `format.RegularQuery`

**Expected:**

Parse the standalone CALL or return an ordinary unsupported/error result; do not return a successful nil query that the formatter cannot handle.

**Observed:**

Parsing returned (nil, nil), and formatting the nil query panicked.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/frontend/parse.go:38](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/parse.go#L38), [cypher/models/cypher/format/format.go:1202](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/cypher/format/format.go#L1202)

**Specification reference:** CALL can constitute an entire statement. The specification defines procedure invocation, not the Go parser/formatter nil-handling contract or availability of db.labels. [Standalone and in-statement calls](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-06-24-call-procedures.adoc#L80-L106); [Standalone CALL syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L156-L157).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-018"></a>

### DAWGS-018: String predicates and ordered comparisons on non-string values use the value's text form

**Status:** Replayed PG (primary trigger).

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

String-predicate case:

```cypher
CREATE (:A {x:123})
```

Ordered-comparison case (separate graph):

```cypher
CREATE (:A {x:'123'})
```

**Reproduction:**

`n.x STARTS WITH '1'` with `x = 123` matches; `n.x < 200` with `x = '123'` matches

**Expected:**

Use the language operand-type rules for string predicates and comparisons instead of converting non-string values to their text form.

**Observed:**

The replay accepted matches by comparing the operands as text.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/expression.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L1) (text `->>` lookup)

**Specification reference:** String predicates require string operands; ordering across incomparable scalar types does not use their textual representations. [String-search operand types (testable CIP)](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/2.testable/CIP2015-09-15-STARTS-WITH-and-ENDS-WITH.adoc#L83-L98); [Numeric/string comparability](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc#L156-L190).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

## Pattern matching and path traversal

These findings concern which graph paths match a query, how existing bindings constrain a match, and how traversal bounds and relationship uniqueness work. Use the shared fixtures in Appendices A and B where a finding references them.

<a id="dawgs-019"></a>

### DAWGS-019: Unbounded traversals stop at 15 hops

**Status:** Reproduced on PostgreSQL.

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: incomplete results.

**Downstream priority:** P1 — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

The actual [BloodHound query service](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L611) accepted the built-in Domain Admins query but returned an empty graph on the 16-hop membership fixture. The explicit-bound control, passed through the same service, returned 17 nodes and 16 edges. P1 is supported by a missing privileged-membership result after application query preparation and execution, rather than only direct driver behavior. It requires an eligible path longer than 15 hops and the PostgreSQL backend.

**Affected queries / consumers:** [All Domain Admins](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Domain%20Admins.yml#L7-L11); [Shortest Paths from Azure Users to Azure Keyvaults](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20Paths%20from%20Azure%20Users%20to%20Azure%20Keyvaults.yml#L8-L10); [BloodHound AGI Domain Admins](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L42-L46); [BloodHound AGT Domain Admins](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L42-L46); [Enterprise Domain Admins test catalogue](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L26-L28); [BloodHound query service](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L611).

**openCypher specification:** An omitted upper bound permits paths of any length; 15 is not an implicit language bound. [Path lengths, exact lengths, and omitted bounds, printed p. 11](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=15); [Optional path-length bounds](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L437-L441).

**Precondition:**

[Appendix A](#appendix-a-shared-fixture-for-dawgs-019-and-dawgs-020).

**Additional setup:**

- Run each query separately with optimized translation both enabled and disabled.

**Reproduction:**

```cypher
MATCH (a:AuditStart)-[:AuditR*]->(b:AuditEnd) RETURN b
MATCH p=shortestPath((a:AuditStart)-[:AuditR*]->(b:AuditEnd)) RETURN p
MATCH p=allShortestPaths((a:AuditStart)-[:AuditR*]->(b:AuditEnd)) RETURN p
```

Run each statement separately.

**Expected:**

each returns the one reachable endpoint/path.

**Observed:**

each returns zero rows, with no error. Changing `*` to `*..16` makes each return one row. Both optimizer modes behave this way.

**Source evidence:** [expansion.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expansion.go#L15) sets the default maximum to 15; ordinary recursive expansion and shortest-path harness parameters use it when no maximum was supplied.

**Impact:** reachable endpoints and valid shortest paths disappear silently.

**Regression criterion:** absent an explicit upper bound, this fixture must yield one row; larger finite trails must not be truncated by an undocumented default.

<a id="dawgs-020"></a>

### DAWGS-020: Fixed-length expansion is not equivalent to an exact range

**Status:** Reproduced on PostgreSQL.

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: incorrect bound.

**openCypher specification:** A fixed length denotes exactly that many relationships. [Path lengths, exact lengths, and omitted bounds, printed p. 11](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=15); [Fixed and ranged path lengths](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L437-L466).

**Precondition:**

[Appendix A](#appendix-a-shared-fixture-for-dawgs-019-and-dawgs-020).

**Additional setup:**

- Compare both forms in each optimizer mode.

**Reproduction:**

```cypher
MATCH (a:AuditStart)-[:AuditR*16]->(b:AuditEnd) RETURN b
MATCH (a:AuditStart)-[:AuditR*16..16]->(b:AuditEnd) RETURN b
```

**Expected:**

both return one row.

**Observed:**

`*16` returns zero rows; `*16..16` returns one. Both optimizer modes fail the fixed-length form.

**Source evidence:** [RangeLiteralVisitor](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/query.go#L168) puts a single integer into `StartIndex`; [expansion options](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/model.go#L51) derive minimum and maximum from start/end independently. The missing upper bound interacts with DAWGS-019's default. This explains the recorded 16-hop discrepancy; smaller fixed-length forms were not exhaustively checked.

**Regression criterion:** `*k` and `*k..k` must be equivalent, including `k > 15`.

<a id="dawgs-021"></a>

### DAWGS-021: Leading OPTIONAL MATCH loses the null row

**Status:** Reproduced on PostgreSQL.

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: wrong cardinality.

**openCypher specification:** An unmatched OPTIONAL MATCH extends the input with null bindings. [OPTIONAL MATCH null extension and outer-join semantics, printed p. 69–70](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=73); [OPTIONAL MATCH syntax](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L47-L58).

**Precondition:**

Empty graph.

**Reproduction:**

```cypher
OPTIONAL MATCH (n) RETURN n
```

**Expected:**

one row containing `null`.

**Observed:**

zero rows. The upstream case is `Match7`, scenario 1; focused case `DAWGS-021` confirms the cardinality failure rather than treating missing empty-result headers as an adapter limitation.

**Source evidence:** [translateMatch](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/match.go#L46) explicitly skips optional handling when no previous frame exists. Generated SQL has an ordinary node match and no null-preserving seed row.

**Impact:** pipelines beginning with an optional pattern can disappear instead of continuing with null bindings.

**Regression criterion:** preserve one null row on the empty graph, while retaining ordinary matches when nodes exist.

<a id="dawgs-022"></a>

### DAWGS-022: Parallel same-type relationships are rejected

**Status:** Reproduced on PostgreSQL.

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: incompatible data model.

**openCypher specification:** Relationships have their own identity; the graph is a multigraph. [Property graph: a multigraph with self-edges and edge identity, printed p. 4–5](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=8).

**Precondition:**

Empty graph.

**Additional setup:**

- The CREATE statement below is the trigger, not setup to run first. Keep its CREATE clauses in one statement so a and b remain in scope.

**Reproduction:**

```cypher
CREATE (a), (b)
CREATE (a)-[:OCReportParallel]->(b)
CREATE (a)-[:OCReportParallel]->(b)
```

**Expected:**

two distinct relationships; a subsequent `MATCH ()-[r:OCReportParallel]->() RETURN count(r) AS count` returns 2.

**Observed:**

setup fails with SQLSTATE `23505`, reporting a duplicate endpoint/type key. The setup transaction fails before its result assertion can run.

**Source evidence:** [schema_up.sql](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query/sql/schema_up.sql#L146) enforces uniqueness on `(start_id, kind_id, end_id, graph_id)`, excluding relationship identity from the uniqueness key. Upstream `Merge5`, scenario 3 encounters the same failure while creating its fixture, before the unsupported MERGE statement is reached.

**Impact:** an openCypher multigraph cannot be represented.

**Regression criterion:** allow two distinct relationship IDs for identical endpoints/type and preserve both in traversal/count results. This requires reviewing code that assumes endpoint/type uniqueness, not merely removing an index blindly.

<a id="dawgs-023"></a>

### DAWGS-023: An unknown label raises an error

**Status:** Reproduced on PostgreSQL.

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Medium: wrong empty-match behavior.

**openCypher specification:** A label is a constraint on matching graph data; no matching data produces no bindings. These sections do not prescribe a driver label-registration API. [Label constraints, printed p. 9](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=13); [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62).

**Precondition:**

Empty graph.

**Additional setup:**

- Use a fresh database whose kind registry has never contained OCReportNeverRegisteredLabel. Do not preregister that label.

**Reproduction:**

```cypher
MATCH (n:OCReportNeverRegisteredLabel) RETURN count(n) AS count
```

**Expected:**

one row with `count = 0`.

**Observed:**

`unable to map kinds: OCReportNeverRegisteredLabel`.

This reproduction has no optional match and no already-bound destination node, so the published bound-node-join defect cannot explain it. Nineteen upstream first-failure messages mention missing kind mappings; those include more complicated queries.

**Source evidence:** [SchemaManager.MapKinds](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/manager.go#L183) errors when the refreshed registry lacks a requested kind; matching translation propagates that failure, for example in [node.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/node.go#L88).

**Regression criterion:** an unknown label/type in a read pattern denotes an empty matching set. Do not automatically create a label just to make a read succeed; mutation validation is a separate concern.

<a id="dawgs-024"></a>

### DAWGS-024: OPTIONAL MATCH squares repeated input rows

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: wrong cardinality.

**Precondition:**

```cypher
CREATE (a:A {name:"a"})-[:R]->(:B {name:"b1"}),
       (a)-[:R]->(:B {name:"b2"}),
       (a)-[:R]->(:B {name:"b3"})
```

**Additional setup:**

- For the separate S-edge variants, use the following fixture instead.

```cypher
CREATE (a:A {name:"a"})-[:S]->(:C {name:"c1"}),
       (a)-[:S]->(:C {name:"c2"}),
       (a)-[:R]->(:B {name:"b1"})
```

**openCypher specification:** OPTIONAL MATCH extends each incoming binding; it does not independently rejoin duplicate inputs. [OPTIONAL MATCH null extension and outer-join semantics, printed p. 69–70](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=73); [Clause state and sequential data flow](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-10-27-State-visibility-between-clauses.adoc#L29-L40).


Setup: the DAWGS-041 setup.

```cypher
UNWIND [1,1,1] AS x MATCH (a:A {name: "a"}) OPTIONAL MATCH (a)-[:R]->(b:B {name: "b1"}) RETURN count(*) AS c
```

**Expected:**

`c = 3`.

**Observed:**

`c = 9`. With a node that has two `S` relationships and one `R` relationship, `MATCH (a:A)-[:S]->(c) WITH a OPTIONAL MATCH (a)-[:R]->(b) RETURN count(*)` returns 4 instead of 2. With the optimizer enabled, `MATCH (a:A)-[:S]->(c) OPTIONAL MATCH (a)-[:R]->(b) RETURN a.name, b.name` also returns 4 rows instead of 2, because projection pruning removes `c` from the join key.

**Source evidence:** the optional frame is derived from the origin rows, then joined back to them by [value equality on the exported columns](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/match.go#L104) (`IS NOT DISTINCT FROM`) rather than by row identity. See also the [aggregation source](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/match.go#L155). When *n* origin rows share the same exported values, the join yields *n²* rows.

**Impact:** counts and aggregates after OPTIONAL MATCH are inflated whenever input rows repeat.

**Regression criterion:** each input row appears exactly once when the optional pattern does not match, and once per match otherwise.

<a id="dawgs-025"></a>

### DAWGS-025: Bound right nodes cross-join the node table

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: wrong cardinality.

**openCypher specification:** Previously bound nodes constrain subsequent pattern matching. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62).


**Precondition:**

[Appendix B](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030).

**Additional setup:**

- Enable optimized translation for the primary failure; disable it for the control.

**Reproduction:**

```cypher
MATCH (a {name: "a1"}), (b {name: "c1"}) MATCH (a)-[:R*1..1]->(b) RETURN count(*) AS c
```

**Expected:**

`c = 1`.

**Observed:**

`c = 6` with the optimizer enabled and `c = 1` with it disabled. The exact-range lowering rewrites `*1..1` into a fixed step without an ExpandInto decision, so the step joins the full node table.

The same multiplication occurs, in the default optimized mode, when both endpoints are carried through WITH. In a four-node graph, `MATCH (x:A), (y:B) WITH x, y MATCH (x)-[:R]->(y) RETURN count(*)` returns 4 instead of 1. It also occurs for `OPTIONAL MATCH (x)-[:R]->(y)` with both endpoints bound. With the optimizer **disabled** (the documented rollback mode), it occurs for any second MATCH over bound endpoints and for cycles closed within one pattern: `MATCH (a:A),(b:B) MATCH (a)-[r]->(b)` returned 50 rows instead of 5, and `(a)-->(b)-->(a)` was multiplied by 10.

**Source evidence:** the [LeftNodeBound branch of buildTraversalPatternRoot](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L394) and the [step builder](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L561) always emit `join node nX` for the right node. Its ON clause references the previous frame's composite, so the new alias is unconstrained. ExpandInto planning in [appendExpandIntoDecisions](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/optimize/lowering_plan.go#L524) leaves the common cases to this fallback in three ways:

- It skips [OPTIONAL MATCH](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/optimize/lowering_plan.go#L533).
- It skips [ranged steps](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/optimize/lowering_plan.go#L545).
- It starts each query part with no declared symbols, so it does not see WITH-carried bindings.

**Impact:** results are multiplied by |V| without any error. Switching the optimizer off to work around an optimizer defect makes this class worse.

**Regression criterion:** a bound right node constrains the step's end column directly. Counts are equal in both optimizer modes, including for `*k..k`, WITH-carried endpoints, and OPTIONAL MATCH.

<a id="dawgs-026"></a>

### DAWGS-026: Reused relationship variables cross-join the edge table

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: wrong cardinality.

**Precondition:**

```cypher
CREATE (a:A {name: "a"})-[:R]->(b:B {name: "b"}), (c:C {name: "c"})-[:R]->(c), (:C {name: "d"}), (:C {name: "e"})
```

**openCypher specification:** Previously bound relationships retain their identity and endpoints in subsequent matches. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62); [Property graph: a multigraph with self-edges and edge identity, printed p. 4–5](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=8).


```cypher
MATCH (a)-[r:R]->(b) MATCH (a)-[r]->(c) RETURN count(*) AS c
```

**Expected:**

`c = 2`, one row per relationship.

**Observed:**

`c = 4`, multiplied by the edge count. With the optimizer disabled, the same shape with `(a)-[r]->(b)` in the second MATCH returned 32 in a four-edge graph. `MATCH ()-[r:R]->() WITH r MATCH (x)-[r]->(y) RETURN x.name` fails with `invalid reference to FROM-clause entry for table "s0"`.

**Source evidence:** the [AlreadyBound branch](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/relationship.go#L203) reuses the binding, but the traversal builders still join `edge eX` with join conditions rewritten to the previously bound composite. No `eX.id = previous.id` correlation is emitted.

**Regression criterion:** a re-used relationship variable constrains the step to that relationship's identity, its endpoints, and its direction.

<a id="dawgs-027"></a>

### DAWGS-027: Undirected later hops can reuse the near endpoint

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

```cypher
CREATE (a:A {name: "a"})-[:R]->(b:B {name: "b"})-[:T]->(d:C {name: "d"})
```

**openCypher specification:** An undirected relationship must still connect the nodes at its two endpoints. [Patterns for relationships, printed p. 10](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=14); [Property graph: a multigraph with self-edges and edge identity, printed p. 4–5](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=8); [Undirected relationship pattern](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L419-L420).


```cypher
MATCH (a:A)-[:R]->(b)-[:T]-(c) RETURN c.name AS n
```

**Expected:**

one row, `d`.

**Observed:**

two rows, `b` and `d`. The emitted step is `join edge e1 on ((s0.n1).id = e1.start_id or (s0.n1).id = e1.end_id) join node n2 on (n2.id = e1.end_id or n2.id = e1.start_id)`, so `n2` can match the same column as `n1`. `MATCH (x)-[]-(y)-[]-(z)` over the same graph returns 4 rows instead of 2, including `(a, b, b)`.

**Source evidence:** the `DirectionBoth` cases of the [right edge constraint](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/constraints.go#L76) and the [terminal node constraint](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/constraints.go#L153) are two independent disjunctions. They need the paired `(start, end)` / `(end, start)` form that the root builders use.

**Regression criterion:** each orientation of an undirected non-first hop binds the far node to the opposite endpoint of the near node.

<a id="dawgs-028"></a>

### DAWGS-028: Endpoint predicates on a variable-length step are dropped

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**openCypher specification:** WHERE predicates constrain the corresponding MATCH, including variable-length patterns. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62); [Path lengths, exact lengths, and omitted bounds, printed p. 11](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=15).


**Precondition:**

[Appendix B](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030).

**Reproduction:**

```cypher
MATCH (s)-[:R*1..2]->(g)-[:T]->(d) WHERE s.v = 99 OR g.v = 99 RETURN count(*) AS c
```

**Expected:**

`c = 0`; no node has `v = 99`.

**Observed:**

`c = 5` in both optimizer modes, the same as with no WHERE. The literal `99` does not appear in the emitted SQL. `WHERE s = g`, `s.name <> g.name`, and `s.v + g.v = 99` are dropped the same way. Rewriting the query as `… WITH s, g, d WHERE …` returns correct results. The optimizer's inbound-traversal reversal sometimes moves the expansion to the last step, which hides the defect. Optimizer modes therefore disagree on some shapes.

**Source evidence:** for recursive steps, [consumePatternConstraints](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/constraints.go#L508) deliberately leaves predicates over the root and terminal for the expansion's outer select. The following fixed step then consumes them as its [left-node constraint](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L972). That constraint is only [assigned](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L1001) for the first traversal step, so the predicate is discarded.

**Impact:** filters on traversal endpoints are silently ignored when a further hop follows the expansion.

**Regression criterion:** every WHERE conjunct appears in the emitted query exactly once and is evaluated, for every step position.

<a id="dawgs-029"></a>

### DAWGS-029: Variable-length steps ignore an earlier binding of their terminal

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

```cypher
CREATE (a1:A {name: "a1"})-[:R]->(c1:C {name: "c1"})-[:R]->(a1), (c1)-[:R]->(x:C {name: "x"})
```

**openCypher specification:** A repeated bound terminal denotes the existing node. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62); [Repeated node references in patterns, printed p. 9](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=13).


```cypher
MATCH (a)-[:R]->(b)-[:R*1..2]->(a) RETURN a.name AS a, b.name AS b
```

**Expected:**

two rows, `(a1, c1)` and `(c1, a1)`.

**Observed:**

four rows, including `(x, c1)` and `(x, a1)`. `a` is rebound to whatever node the expansion reaches.

**Source evidence:** when the step's right node is already bound earlier in the same pattern, [buildExpansionPatternStep](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expansion.go#L2910) and [buildExpansionProjectionConstraints](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expansion.go#L3335) re-project it from `next_id`. They do not add an equality to the existing binding.

**Regression criterion:** a variable-length step that closes onto an existing binding returns only paths ending at that node.

<a id="dawgs-030"></a>

### DAWGS-030: Aggregate traversal count lowering miscounts

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong count (optimizer only).

**openCypher specification:** Counts derive from the matches satisfying the complete pattern and its labels. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62); [COUNT semantics, printed p. 152–154](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=156).


**Precondition:**

[Appendix B](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030).

**Additional setup:**

- Enable optimized translation for the failure. Preserve both MATCH clauses and the exact WITH/RETURN/ORDER BY/LIMIT shape.

**Reproduction:**

```cypher
MATCH (s:A) MATCH (s:B)-[:R*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100
```

**Expected:**

one row, `a2 | 3`. `a2` is the only node labeled both A and B.

**Observed:**

with the optimizer enabled, `a1 | 9`, `c2 | 7`, `a2 | 3`. With it disabled, `a2 | 3`. The emitted `candidate_sources` CTE filters only on kind A.

Related optimizer behavior:

- `MATCH (s:A) MATCH (s)-[:R*1..]->(s) WITH s, count(s) AS c …` counts every path from `s`, because the terminal is never tied to the root (for example `a1|21` instead of `a1|3`).
- A pattern predicate in either MATCH's WHERE panics with `index out of range [-1]`, which the translation cache re-raises.

**Source evidence:** [aggregateTraversalMatch](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/optimize/lowering_plan.go#L1722) checks the second MATCH's source-node properties but not its kinds, and does not reject a right-node symbol equal to the source. The predicate panic is in [aggregateBindingPredicate](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/aggregate_traversal_count.go#L468), which walks the WHERE with a translator that has no current query part.

**Impact:** a "who reaches the most" query returns plausible but wrong rankings.

**Regression criterion:** the lowered and unlowered forms return identical rows for all accepted shapes. Shapes the lowering cannot represent are rejected by the shape check.

<a id="dawgs-031"></a>

### DAWGS-031: Relationship uniqueness is incomplete

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Medium-high: wrong cardinality.

**Precondition:**

```cypher
CREATE (a:A {name: "a"})-[:R]->(b:B {name: "b"})-[:T]->(d:C {name: "d"})
```

**Additional setup:**

For the separate variable-length self-loop case, replace the graph above with:

```cypher
CREATE (a)-[:R]->(a)
```

**openCypher specification:** One MATCH cannot reuse a graph relationship across its constituent paths; separate MATCH clauses have separate uniqueness scopes. [Relationship uniqueness across paths within one MATCH, printed p. 6–8](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=10).


```cypher
MATCH (a)-[r1:R]->(x), (a)-[r2:R]->(y) RETURN count(*) AS c
```

**Expected:**

`c = 0`. openCypher requires distinct relationships across all parts of one MATCH, and the graph has a single R relationship.

**Observed:**

`c = 1`. With two R edges from `a`, the query returns 4 rows, including `(b1, b1)`.

Between two variable-length steps in one pattern:

- Over one self-loop, `MATCH (x)-[:R*1..2]->(y)-[:R*1..2]->(z) RETURN count(*)` returns 1 instead of 0.
- `MATCH p=(a {name:'a1'})-[:R*1..2]->(b {name:'b1'})<-[:R*1..2]-(c) RETURN p` returns paths that reuse the same relationship. The exact-range lowering does enforce uniqueness, so optimizer modes disagree for `*1..1`.

**Source evidence:** [previousRelationshipUniquenessConstraint](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L773) scans only the current pattern part's steps, and the MATCH loop adds no cross-part constraint. It and its [companion](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L830) skip previous steps that are expansions (see also [line 788](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal.go#L788)).

**Regression criterion:** every relationship bound within one MATCH clause, across comma-separated parts and variable-length steps, is distinct.

<a id="dawgs-032"></a>

### DAWGS-032: A first-hop self-loop stops variable-length expansion

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Medium-high: incomplete results.

**Precondition:**

```cypher
CREATE (a:A {name: "a"})-[:R]->(a), (a)-[:T]->(b:B {name: "b"})
```

**Additional setup:**

- Compare both optimizer modes because traversal reversal can change the result.

**openCypher specification:** Self-edges are legal and eligible variable-length paths must be matched within the specified bounds. [Property graph: a multigraph with self-edges and edge identity, printed p. 4–5](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=8); [Path lengths, exact lengths, and omitted bounds, printed p. 11](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=15); [Relationship uniqueness across paths within one MATCH, printed p. 6–8](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=10).


```cypher
MATCH (x)-[*2..3]->(y) RETURN x.name AS x, y.name AS y
```

**Expected:**

one row, `(a, b)`, via the loop then the T relationship.

**Observed:**

zero rows. The optimizer's traversal reversal changes which expansions start at a loop, so optimizer modes disagree on some queries. The aggregate traversal count lowering should also be compared on this fixture; its different guard handling is source-supported, not an independently measured count here.

**Source evidence:** the [primer projection](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expansion.go#L3229) marks a seed whose first edge is a loop with `is_cycle`. The recursion then requires `not is_cycle` in [expansionConstraints](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expansion.go#L34). The trail check `e.id != all(path)` already prevents infinite recursion, so the extra guard only removes valid trails.

**Regression criterion:** trails that begin with a self-loop are extended like any other trail, subject to relationship uniqueness and the depth bound.

<a id="dawgs-033"></a>

### DAWGS-033: Undirected patterns mishandle self-loops

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Medium-high: silent wrong result.

**Downstream priority:** P2 — library queries — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

The hostname-substituted Query Library computer-path query passed the actual [BloodHound query service](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L611) and omitted the qualifying AdminTo self-loop; the same source query returned it on the Neo4j fixture. This remains a P2 correctness issue for library queries submitted through Explore when PostgreSQL is active. No matching built-in search was identified, and no effect is established for ordinary non-loop paths. The malformed domain-trust probe does not establish valid AD trust exposure.

**Affected queries / consumers:** [All incoming and local paths for a specific computer](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20incoming%20and%20local%20paths%20for%20a%20specific%20computer.yml#L7-L12); [BloodHound query service](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L611).

**Precondition:**

```cypher
CREATE (a:A {name: "a"})-[:R]->(b:B {name: "b"}), (c:C {name: "c"})-[:R]->(c)
```

**Additional setup:**

- Run the main queries separately. Disable optimized translation for the pattern-predicate variants.

**openCypher specification:** Self-edges are legal; an undirected step must connect its named endpoint nodes. [Property graph: a multigraph with self-edges and edge identity, printed p. 4–5](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=8); [Undirected relationship patterns, printed p. 10](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=14).


```cypher
MATCH ()-[r:R]-() RETURN count(r) AS c
MATCH (x)-[r:R]-(x) RETURN x.name AS n
```

Run each statement separately.

**Expected:**

`c = 3` (two orientations of `a–b` plus one row for the loop; TCK `Match2` self-loop case). The second query returns `c` only.

**Observed:**

`c = 2`. The second query returns `a`, `b`, and `c`: every node with any incident R relationship. Other symptoms:

- `MATCH (x:C)-[r:R]-(y)` returns no rows.
- With the optimizer disabled, `WHERE (x)-[:R]-()` excludes the loop node, and `WHERE NOT (x)-[:R]-()` includes it.

**Source evidence:** the directionless builders guard against both OR conditions matching the same column with an [endpoint inequality](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal_directionless.go#L248). That guard also excludes legitimate `start_id = end_id` edges. The [self-referential builder](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/traversal_directionless.go#L340) uses the same disjunction for both endpoints, so `start_id = end_id = x.id` is never required.

**Regression criterion:** undirected patterns return each self-loop once, and `(x)-[r]-(x)` matches only self-loops.

<a id="dawgs-034"></a>

### DAWGS-034: `*0..0` also returns one-hop neighbours

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (a:A {name:'a'})-[:R]->(:B {name:'b'})
```

**Reproduction:**

`MATCH (x {name:"a"})-[*0..0]->(y)` returns `a` and `b`

**Expected:**

Return only the starting node for an exact zero-length path.

**Observed:**

The replay returned both the origin and a one-hop neighbor.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/expansion.go:2075](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expansion.go#L2075), [cypher/models/pgsql/translate/model.go:55](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/model.go#L55)

**Specification reference:** A range with both bounds zero denotes a zero-length path, whose endpoints are the same node. [Path lengths, exact lengths, and omitted bounds, printed p. 11](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=15); [Zero-length paths, printed p. 65](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=69); [Explicit path bounds](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L437-L466).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-035"></a>

### DAWGS-035: Optimizer pattern reordering ignores pattern-predicate dependencies

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

[Appendix B](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030).

**Additional setup:**

- Enable optimized translation for the failure and disable it for the control.

**Reproduction:**

`MATCH (b:B) MATCH (c {name:'c1'}) WHERE (b)-[:R]->(c)` gives `missing FROM-clause entry` when optimized, correct when unoptimized

**Expected:**

Preserve pattern dependencies when reordering; the optimized query should succeed with the same meaning.

**Observed:**

The optimized query failed with a missing FROM-clause reference; the unoptimized form succeeded.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/optimize/analysis.go:492](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/optimize/analysis.go#L492), [cypher/models/walk/walk_cypher.go:698](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/walk/walk_cypher.go#L698)

**Specification reference:** Reordering cannot discard existing bindings or the predicates associated with a MATCH. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62); [Pattern predicates, printed p. 89–90](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=93).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-036"></a>

### DAWGS-036: A pattern predicate in a MATCH followed by another MATCH gives invalid SQL and drops the predicate's type filter

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

[Appendix B](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030).

**Reproduction:**

`MATCH (s:A) WHERE (s)-[:T]->() MATCH (s)-[:R]->(t)`

**Expected:**

Retain the pattern predicate and its type restriction when processing the following MATCH.

**Observed:**

The recorded form produced invalid SQL and the source showed a lost type restriction.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/predicate.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/predicate.go#L171)

**Specification reference:** A pattern predicate remains part of its MATCH restrictions, including its relationship type. [MATCH uses existing bindings and all associated predicates, printed p. 58](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=62); [Typed pattern predicates, printed p. 89–90](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=93).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-037"></a>

### DAWGS-037: Pattern map parameters use jsonb containment

**Status:** Replayed PG (primary trigger).

**Component:** Pattern matching and path traversal.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {arr:['x','y']})
```

**Additional setup:**

- Bind `props` to `{arr:['x']}`.

**Reproduction:**

`MATCH (n:A $props)` with `{arr:['x']}` matches `arr:['x','y']`

**Expected:**

If this implementation extension is supported, require the complete property value to match rather than treating an array as a contained subset. The baseline openCypher specification excludes the whole-map MATCH parameter form.

**Observed:**

The extension matched an array containing additional elements.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/property.go:107](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/property.go#L107)

**Specification reference:** The baseline explicitly allows a whole property-map parameter in CREATE but excludes it in other pattern clauses. This MATCH-map-parameter finding is an implementation-extension compatibility issue, not a demonstrated violation of that baseline. [Whole property-map parameter restriction, printed p. 10](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=14); [Current property specifications require key/value pairs](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L392-L399).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

## Clause ordering, projection, and updates

These findings concern the records passed between clauses, sorting and pagination, and the order and effects of updates. Check returned values and stored changes separately.

<a id="dawgs-038"></a>

### DAWGS-038: DELETE before aggregating WITH is omitted

**Status:** Reproduced on PostgreSQL.

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** High: mutation silently skipped.

**Precondition:**

Node case:

```cypher
CREATE (:OCReportDelete {num: 1}), (:OCReportDelete {num: 2})
```

Relationship case (separate graph):

```cypher
CREATE ()-[:OCReportDeleteEdge {num: 1}]->()
CREATE ()-[:OCReportDeleteEdge {num: 2}]->()
```

**openCypher specification:** Later clauses operate on the state produced by earlier updates. [Clause state and sequential data flow](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-10-27-State-visibility-between-clauses.adoc#L29-L40).

Query:

```cypher
MATCH (n:OCReportDelete)
WITH n, n.num AS num
DELETE n
WITH sum(num) AS total
RETURN total
```

**Expected:**

`total = 3`, two nodes and two properties are removed, and the now-absent distinct label contributes one label removal.

**Observed:**

`total = 3`, but all deletion side-effect counts are zero.

Relationship query:

```cypher
MATCH ()-[r:OCReportDeleteEdge]->()
WITH r, r.num AS num
DELETE r
WITH sum(num) AS total
RETURN total
```

**Expected:**

`total = 3`, two relationships removed, and two properties removed; nodes remain.

**Observed:**

`total = 3`, with zero relationship/property removals.

Upstream `Delete6`, scenario 7 and scenario 14 confirm the same behavior with five elements and aggregate 15. Their generated SQL contains no DELETE, so the discrepancy does not depend solely on the side-effect adapter.

**Source evidence:** [single-part deletion emission and multipart assembly](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/query.go#L43), [multipart boundary handling](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/query.go#L105), and [deletion builder](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/delete.go#L32). Deletions are recorded per part; these records are not emitted for the affected non-final part.

**Impact:** callers receive a plausible successful aggregate while the requested mutation is skipped.

**Regression criterion:** execute node and relationship deletions before the next WITH boundary and retain their observable effects regardless of later aggregation or result filtering.

<a id="dawgs-039"></a>

### DAWGS-039: Plain DELETE silently detaches connected nodes

**Status:** Reproduced on PostgreSQL.

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** High: unexpected data removal.

**Precondition:**

```cypher
CREATE (a:OCReportConnected)-[:OCReportConnection]->(b)
```

**Additional setup:**

- Run plain DELETE while the relationship still exists; do not use DETACH DELETE.

**openCypher specification:** Connected nodes require explicit deletion of incident relationships or DETACH DELETE. [DELETE versus DETACH DELETE, printed p. 106–107](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=110); [Optional DETACH modifier](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L138-L147).

Query:

```cypher
MATCH (n:OCReportConnected) DELETE n
```

**Expected:**

`DeleteConnectedNode` at runtime; the graph remains unchanged.

**Observed:**

the query succeeds rather than raising the required error. Upstream `Delete1`, scenario 7 exhibits the same behavior.

**Source evidence:** [translateDelete](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/delete.go#L11) does not distinguish the detach flag while scheduling deletions. The [delete_node_edges trigger](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query/sql/schema_up.sql#L188) deletes incident relationships whenever a node is deleted. This supplies detach behavior even for a plain DELETE.

**Impact:** a caller requesting plain deletion receives success despite connected relationships, with incident edges subject to removal. The focused TCK-style probe stops at the missing-error assertion; the trigger implementation explains the cascade. This is distinct from DAWGS-038, where requested deletions never execute.

**Regression criterion:** plain DELETE on a connected node fails atomically; DETACH DELETE succeeds and removes incident relationships. Both paths require separate checks.

<a id="dawgs-040"></a>

### DAWGS-040: WITH ordering, skip, and limit are ignored

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

```cypher
CREATE (:A {k: 1}), (:A {k: 2}), (:A {k: 3}), (:A {k: 4})
```

**Additional setup:**

- The additional grouped-pagination variant requires group properties; that variant has no complete fixture here.

**openCypher specification:** WITH ordering and pagination affect the table passed to subsequent clauses. [WITH ORDER/SKIP/LIMIT semantics](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L520-L545); [WITH includes ordering and pagination](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L65-L68).


Query:

```cypher
MATCH (n:A) WITH n ORDER BY n.k DESC LIMIT 2 RETURN n.k AS k
```

**Expected:**

two rows, `4` and `3`.

**Observed:**

all four rows. The emitted SQL contains no `order by` or `limit` in any CTE. `WITH n SKIP 1`, `WITH n LIMIT 2`, and `WITH n.grp AS g, count(*) AS c ORDER BY c DESC LIMIT 1 RETURN g, c` are likewise ignored.

**Source evidence:** [buildInlineProjection](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/projection.go#L601), used for non-final parts by [buildMultiPartQuery](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/query.go#L66), never applies the part's sort items, skip, or limit. Only the [tail projection](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/projection.go#L1423) does.

**Impact:** "top N, then continue" pipelines return every row.

**Regression criterion:** ORDER BY, SKIP, and LIMIT on WITH are applied at that boundary, including after aggregation and before subsequent MATCH clauses.

<a id="dawgs-041"></a>

### DAWGS-041: SET loses updates and drops rows

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** High: mutation silently skipped.

**Precondition:**

```cypher
CREATE (a:A {name: "a"})-[:R]->(:B {name: "b1"}), (a)-[:R]->(:B {name: "b2"}), (a)-[:R]->(:B {name: "b3"})
```

**Additional setup:**

- For the nullable-target variant, add the following node to a fresh copy of the fixture.

```cypher
CREATE (:A {name:"alone"})
```

**openCypher specification:** All SET items apply to the incoming records, before subsequent clauses execute. [SET item list](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L94-L118); [Clause state and sequential data flow](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-10-27-State-visibility-between-clauses.adoc#L29-L40); [Multiple SET properties, printed p. 111–112](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=115).


Query:

```cypher
MATCH (x:A)-[:R]->(y) SET x.src = true, y.dst = true
```

**Expected:**

all three B nodes have `dst = true`.

**Observed:**

only one B node has `dst = true`; the other two are unchanged.

Related shapes:

- Chain `a→b→c` with the same SET: `b` receives `src` but not `dst`.
- `MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b) SET b.x = 1 RETURN a.name`: rows whose `b` is null disappear.
- One A and two B nodes, `MATCH (n:A), (m:B) SET n.y = 1 RETURN count(*)`: returns 1 instead of 2.

**Source evidence:** [buildUpdates](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/update.go#L118) emits each SET target as `UPDATE … FROM <previous frame> … RETURNING`, and the [RETURNING projection](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/update.go#L340) becomes the next frame. UPDATE…FROM returns one row per updated target row and carries an arbitrary joined source row. PostgreSQL also applies at most one modification per row per statement. Later SET targets and the final projection therefore read from a collapsed row set.

**Impact:** writes are silently lost, and the returned rows do not match the matched rows.

**Regression criterion:** every matched row's SET items are applied. Row cardinality is preserved through SET. A null SET target skips only that item for that row.

<a id="dawgs-042"></a>

### DAWGS-042: UNWIND generates invalid SQL for common forms

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Literal-list case: Empty graph.

Property-array case:

```cypher
CREATE (:A {arr:[1,2,3]})
```

**Reproduction:**

`UNWIND [1,2,3] AS x WITH x WHERE x > 1 RETURN x` (`relation "s0" does not exist`); after `CREATE (:A {arr:[1,2,3]})`, `MATCH (n:A) UNWIND n.arr AS x RETURN x` fails with `unnest(text)`. Boolean-list and empty-list inference errors overlap the published array-inference caveat and are excluded from the new claim

**Expected:**

UNWIND each list element into a record and carry the resulting bindings through WITH.

**Observed:**

The recorded forms generated SQL referring to missing frames or calling unnest on text.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/unwind.go:22](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/unwind.go#L22), [cypher/models/pgsql/translate/unwind.go:31](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/unwind.go#L31)

**Specification reference:** UNWIND expands list values into records, including lists supplied by expressions. [UNWIND, printed p. 81–82](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=85); [UNWIND accepts a value expression](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L60-L63).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-043"></a>

### DAWGS-043: DELETE with several targets, or followed by RETURN, generates invalid SQL

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (a:A {name:"a"})-[:R]->(:B {name:"b1"}),
       (a)-[:R]->(:B {name:"b2"}),
       (a)-[:R]->(:B {name:"b3"})
```

**Additional setup:**

- Run each DELETE variant on a fresh copy.

**Reproduction:**

`DELETE r, y` (duplicate CTE name); `DELETE r RETURN count(*)` (CTE has no RETURNING)

**Expected:**

Delete every named target and allow a following projection or aggregate.

**Observed:**

The recorded forms produced duplicate-CTE or missing-RETURNING errors.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/delete.go:32](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/delete.go#L32), [cypher/models/pgsql/translate/delete.go:78](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/delete.go#L78)

**Specification reference:** DELETE permits multiple targets, and update effects precede subsequent clauses. [DELETE item list](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L138-L147); [Clause state and sequential data flow](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-10-27-State-visibility-between-clauses.adoc#L29-L40).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-044"></a>

### DAWGS-044: SET, REMOVE, or CREATE before WITH generates invalid SQL

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {k:0})
```

**Additional setup:**

- The SET example is the complete primary trigger; separate REMOVE/CREATE variants still need complete reproductions.

**Reproduction:**

`MATCH (n:A) SET n.k = 1 WITH n RETURN n` (data-modifying CTE not at top level); a different path from DAWGS-038

**Expected:**

Execute updates before WITH, then expose their results to the next query part.

**Observed:**

The replay failed because the data-modifying CTE was nested in an invalid location.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/query.go:69](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/query.go#L69)

**Specification reference:** Updates are visible after the WITH boundary. [Clause state and sequential data flow](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-10-27-State-visibility-between-clauses.adoc#L29-L40); [WITH separates update and reading parts, printed p. 79](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=83).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-045"></a>

### DAWGS-045: SET and REMOVE of the same property or label apply removals first

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Property case:

```cypher
CREATE (:A {x:0})
```

Label case (separate graph):

```cypher
CREATE (:A)
```

**Additional setup:**

- Register B before the label case. Run each mutation on a fresh fixture and read its effects in a separate transaction.

**Reproduction:**

`SET n.x = 1 REMOVE n.x` keeps `x`; `SET n:B REMOVE n:B` keeps B

**Expected:**

Apply clauses in order, so the later REMOVE removes the property or label just set.

**Observed:**

The property or label remained after the later REMOVE.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/update.go:118](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/update.go#L118) (folding at ≈240-330)

**Specification reference:** A later REMOVE observes and changes the state produced by the earlier SET. [Clause state and sequential data flow](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-10-27-State-visibility-between-clauses.adoc#L29-L40).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-046"></a>

### DAWGS-046: WITH → MATCH → WITH builds a self-referencing CTE

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

[Appendix B](#appendix-b-shared-fixture-for-dawgs-025-dawgs-028-and-dawgs-030).

**Reproduction:**

`MATCH (a) WITH a MATCH (a)-[:R]->(c) WITH c RETURN c` (`relation "s2" does not exist`)

**Expected:**

Carry bindings through both WITH boundaries without referring to an undefined SQL frame.

**Observed:**

The generated SQL referred to a query frame that did not exist.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/query.go:69](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/query.go#L69)

**Specification reference:** WITH chains query parts while carrying projected bindings forward. [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69); [WITH chaining and scope, printed p. 78–80](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=82).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-047"></a>

### DAWGS-047: ORDER BY over an alias expression references an internal name; user aliases can collide with internal names

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Alias-expression case:

```cypher
CREATE (:A {k:1}), (:A {k:2})
```

Internal-name collision case: Empty graph.

**Reproduction:**

`RETURN n.k AS x ORDER BY x + 0` (`column "i0" does not exist`); `RETURN 10 - x AS i0 ORDER BY x` orders by the wrong column

**Expected:**

Resolve ORDER BY expressions against the correct user-visible aliases and bindings.

**Observed:**

The replay either referred to an undefined internal column or ordered by a colliding alias.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/projection.go:1118](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/projection.go#L1118)

**Specification reference:** ORDER BY expressions can refer to projected aliases; shadowing uses the new binding. [ORDER BY alias availability](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L547-L566); [ORDER BY scope and shadowing, printed p. 93–94](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=97).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-048"></a>

### DAWGS-048: `WITH n, n AS m` generates invalid SQL

**Status:** Replayed PG (primary trigger).

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {name:'a'})
```

**Reproduction:**

`missing FROM-clause entry for table "s0"`

**Expected:**

Project the same node under both distinct aliases and allow the later query to use them.

**Observed:**

The replay failed with a missing FROM-clause reference.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/with.go:97](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/with.go#L97)

**Specification reference:** WITH may project one value under different distinct aliases. [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-049"></a>

### DAWGS-049: CREATE retains a null-valued property

**Status:** Reproduced storage observation; related to documented null behavior.

**Component:** Clause ordering, projection, and updates.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- The CREATE below is the trigger. Inspect persisted property entries, not just returned null values.

**openCypher specification:** Null used to update properties denotes absence/removal, not a stored property value. Observability of the retained JSON entry still needs the additional coverage described in this entry. [Missing-null property semantics](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc#L65-L76).

```cypher
CREATE (n {id: 12, name: null}) RETURN n.id AS id, n.name AS p
```

**Expected:**

values `(12, null)`, one created node, and one created property.

**Observed:**

the values match, but the snapshot records two properties. Upstream `Create1`, scenario 11 and the focused `DAWGS-049` case agree.

The [property object builder](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/create.go#L295) emits `jsonb_build_object` entries including null values. This is a storage/mutation discrepancy rather than merely a predicate truth-value difference. However, the page already warns generally about null differences, so this report marks it **related to an existing broad caveat**, rather than a separate support promise. The snapshot measures persisted entity/key/value entries; exact `keys(n)`/property-observability coverage would further strengthen the language-level characterization.

**Regression criterion:** creating a null property leaves no observable property entry and does not increment the property-addition metric.

## Result values, types, and decoding

These findings concern the values returned to callers: column names, numeric precision, list element types, identity comparisons, and decoding. A plausible-looking value can still have the wrong type or identity.

<a id="dawgs-050"></a>

### DAWGS-050: Unaliased expressions lose their result-column name

**Status:** Reproduced on PostgreSQL.

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Medium: result API mismatch.

**Precondition:**

Empty graph.

**Additional setup:**

- The CREATE below is the trigger. Inspect Result.Keys() as well as values; keep the RETURN expression unaliased.

**openCypher specification:** An unaliased property expression names its own field; AS explicitly renames it. [RETURN property field and aliasing, printed p. 76–77](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=80); [Optional RETURN alias](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L209-L213).

```cypher
CREATE (p {id: 12}) RETURN p.id
```

**Expected:**

column `p.id`, value 12.

**Observed:**

column `?column?`, value 12. The small integer separates this from DAWGS-051's precision defect. Explicit `RETURN p.id AS value` supplies a name and avoids this particular header mismatch.

**Source evidence:** [projection assembly](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/projection.go#L628) only applies an alias when one is present in the model; [result keys](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/result.go#L26) expose PostgreSQL field names. A complete fix may synthesize the required Cypher name or preserve a result-name mapping.

Upstream `Create1`, scenario 12 fails on the same header; 106 recorded first-failure messages are column mismatches, but they have not all been assigned this root cause.

**Impact:** consumers reading Cypher expression names cannot locate the returned column.

**Regression criterion:** correct names for unaliased property/function/operator expressions as well as explicit aliases.

<a id="dawgs-051"></a>

### DAWGS-051: Large integer properties lose precision in results

**Status:** Reproduced on PostgreSQL.

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent numeric corruption.

**Precondition:**

Empty graph.

**Additional setup:**

- The CREATE below is the trigger. Retain its alias and compare the expected int64 without float64 conversion.

**openCypher specification:** INTEGER values are exact; returning a property must preserve its value. [Exact INTEGER and IEEE-754 FLOAT types](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-09-16-public-type-system-type-annotation.adoc#L88-L103); [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69).

```cypher
CREATE (n {id: 4611686018427387905}) RETURN n.id AS value
```

**Expected:**

the exact integer `4611686018427387905`.

**Observed:**

the graph result is decoded through floating-point numeric handling; the recorded numeric serialization is `4611686018427388000`. An explicit alias removes DAWGS-050's column-name failure, exposing the value discrepancy directly.

A read-only PostgreSQL control, `SELECT jsonb_build_object('id',4611686018427387905)->>'id'`, returns the exact text `4611686018427387905`. Thus PostgreSQL can represent the property exactly; the problem is the driver result-conversion path, not an integer-storage limit. The expected value is compared exactly as an int64.

**Source evidence:** [queryResult.Next and JSON/JSONB decoding](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/result.go#L30). `rows.Values()` uses pgx's JSON decoding, and the additional `decodeJSONValue` paths use `encoding/json.Unmarshal` into `any`, which also defaults JSON numbers to float64. The driver does not maintain the exact Cypher integer through this path. The specific stage responsible for every composite/property representation has not been exhaustively mapped.

**Impact:** identifiers and counts stored as large integer properties can change silently in returned values. The page's stricter-comparison warning does not describe lossy retrieval.

**Regression criterion:** preserve integers above 2^53 and both int64 extrema through scalar property, map, and entity result paths.

<a id="dawgs-052"></a>

### DAWGS-052: Properties projected through WITH stay raw jsonb

**Status:** Replayed PG (primary trigger).

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {name:'abc'})
```

**Reproduction:**

`WITH n.name AS nm RETURN toString(nm)` returns `"\"abc\""`; most later comparisons fail with type errors

**Expected:**

Carry the original property value through WITH; toString of the string abc remains abc.

**Observed:**

The replay returned a quoted JSON string representation and related comparisons could fail.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/projection.go:1524](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/projection.go#L1524)

**Specification reference:** WITH projects the expression value under its alias; string conversion operates on that value. [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69); [toString conversion, printed p. 149](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=153).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-053"></a>

### DAWGS-053: Integer `sum` loses precision; empty-input `sum` is null (`avg` cast is source-supported)

**Status:** Replayed PG (primary trigger).

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Precision case:

```cypher
CREATE (:A {big:1234567890123456})
```

Zero-row case (separate graph):

```cypher
CREATE (:A {x:1}), (:A {x:2})
```

**Additional setup:**

- Use `WHERE n.x > 99` for the zero-row case. The avg subcase remains source-supported and needs its own runtime check.

**Reproduction:**

`sum(n.big)` with 1234567890123456 returns 1234567890123460; `sum` over zero rows returns null (Cypher: 0)

**Expected:**

Preserve exact integer sums and return zero for an empty SUM aggregate.

**Observed:**

The integer sum changed to 1234567890123460; empty-input SUM returned null.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/function.go:905](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/function.go#L905)

**Specification reference:** SUM returns the sum, defaulting to zero for an empty aggregate; INTEGER is exact. [Aggregation, including empty-input SUM](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc#L400-L427); [Exact INTEGER and IEEE-754 FLOAT types](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-09-16-public-type-system-type-annotation.adoc#L88-L103).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-054"></a>

### DAWGS-054: `collect(n.prop)` returns strings for numbers and booleans

**Status:** Replayed PG (primary trigger).

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Numeric case:

```cypher
CREATE (:A {x:1}), (:A {x:2})
```

Boolean case (separate graph):

```cypher
CREATE (:A {x:true}), (:A {x:false})
```

**Additional setup:**

- Inspect the Go element types before attempting arithmetic after UNWIND.

**Reproduction:**

`collect(n.x)` returns `["1","2"]`; `UNWIND` of the result then fails on arithmetic

**Expected:**

Retain numeric and Boolean element types in the collected list.

**Observed:**

The collected numeric values were returned as strings.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/function.go:501](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/function.go#L501)

**Specification reference:** COLLECT retains the values and their element types. [COLLECT returns aggregated values](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc#L400-L427); [COLLECT return types, printed p. 152](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=156).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-055"></a>

### DAWGS-055: Result aliases are emitted unquoted

**Status:** Replayed PG (primary trigger).

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {k:1})
```

**Additional setup:**

- Inspect result keys for each alias variant.

**Reproduction:**

`RETURN n.k AS objectId` yields key `objectid`; backtick aliases give a PostgreSQL syntax error

**Expected:**

Preserve the exact alias spelling and support names that require quoting.

**Observed:**

Mixed-case aliases were folded to lowercase; backtick aliases caused SQL errors.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/format/format.go:395](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/format/format.go#L395), [drivers/pg/result.go:33](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/result.go#L33)

**Specification reference:** Aliases retain their specified names and variables are case-sensitive; backquotes permit unusual characters. [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69); [Variable naming and quoting, printed p. 41](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=45); [Field aliasing, printed p. 77](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=81).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-056"></a>

### DAWGS-056: Entity membership compares raw ids across entity types; the collect-ID rewrite runs with the optimizer disabled

**Status:** Replayed PG (primary trigger).

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A)-[:R]->(:B)
```

**Additional setup:**

- Use a fresh database with node and relationship sequences starting at 1. Verify numeric ID overlap before running the membership query; a reused database may hide the bug.

**Reproduction:**

`WITH collect(n) AS ns MATCH ()-[r]->() WHERE r IN ns` returns relationships whose id equals a node id

**Expected:**

A relationship is not a member of a list of nodes, even when their numeric IDs overlap; count is zero.

**Observed:**

The controlled overlapping-ID replay returned count 1; disjoint IDs returned zero.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cypher/models/pgsql/translate/expression.go:689](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L689), [cypher/models/pgsql/translate/expression.go:735](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/expression.go#L735), [cypher/models/pgsql/translate/function.go:532](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/function.go#L532), [cypher/models/pgsql/translate/translator.go:887](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/translator.go#L887)

**Specification reference:** Membership compares entity values, not bare IDs from different entity types. IDs need only be unique within their respective sets. [Property graph: a multigraph with self-edges and edge identity, printed p. 4–5](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=8); [List membership, printed p. 23](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=27); [Separate node/relationship ID uniqueness](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2017-10-17-Cypher-9.adoc#L82).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

#### DAWGS-056 reproduction precondition

Use a fresh isolated database in which node and relationship sequences both start at 1, then commit:

```cypher
CREATE (:A)-[:R]->(:B)
```

Verify that one node and the relationship have the same numeric `id()`. Separate ID namespaces make this overlap legitimate. If the sequences do not overlap, the query can return the expected zero and conceal the defect.

```cypher
MATCH (n) WITH collect(n) AS ns
MATCH ()-[r]->() WHERE r IN ns
RETURN count(*) AS c
```

**Expected:**

`c = 0`; a relationship is never a member of a node list.

**Observed:**

`c = 1` with the controlled overlapping IDs, in both optimizer modes. A control graph with disjoint IDs returned zero, establishing the importance of overlapping IDs.

<a id="dawgs-057"></a>

### DAWGS-057: String values that look like JSON are decoded a second time

**Status:** Replayed PG (primary trigger).

**Component:** Result values, types, and decoding.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {x:$listText}), (:A {x:$quotedText}), (:A {x:$mapText})
```

**Additional setup:**

- Bind string parameters `listText = '[1, 2]'`, `quotedText = '"q"'`, and `mapText = '{"k":"v"}'`. These are strings, not list/map values.

**Reproduction:**

properties `'[1, 2]'`, `'"q"'`, `'{"k":"v"}'` return a list, `q`, and a map; `Scan(&string)` fails

**Expected:**

Return the stored string unchanged, even when its characters form valid JSON.

**Observed:**

JSON-looking strings were returned as decoded lists, maps, or differently quoted strings.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/result.go:81-96](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/result.go#L81)

**Specification reference:** A property value projected as STRING retains its string value. Driver JSON decoding is not an openCypher operation. [Exact INTEGER and IEEE-754 FLOAT types](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-09-16-public-type-system-type-annotation.adoc#L88-L103); [Projection preserves evaluated values and aliases](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-07-07-Grouping-keys-and-aggregation-expressions.adoc#L45-L69).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

## Query builders and translation caching

These findings concern how a query is rendered or reused after the caller constructs it. Generated-output findings require tests of the emitted form and do not by themselves show a live Neo4j result.

<a id="dawgs-058"></a>

### DAWGS-058: The translation cache returns another query's SQL

**Status:** Replayed PG (primary trigger).

**Component:** Query builders and translation caching.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent wrong result.

**Precondition:**

```cypher
CREATE (:A)
```

**Additional setup:**

- Use one driver and shared translation cache for both queries. Start cold; repeat with a new driver and the query order reversed.

**openCypher specification:** No direct openCypher rule governs translation-cache keys. This is a DAWGS implementation defect; query equivalence is the requirement, not a separately specified cache algorithm.


In one process, run both builder queries:

```go
tx.Nodes().Filter(query.KindIn(query.Node())).Fetch(...) // no kinds: should match nothing
tx.Nodes().Filter(query.And()).Fetch(...)                // empty conjunction: should match everything
```

**Expected:**

the first returns no nodes and the second returns all nodes, in either order.

**Observed:**

both format as `match (n) where  return n` and share one cache entry. Whichever compiles first decides both results:

- If `And()` runs first, `KindIn` with no kinds returns **every node**.
- If `KindIn` runs first, `And()` returns none.

An exclusive kind matcher (`n:A:B`) and `query.KindIn(n, A, B)` both format as `(n:A or n:B)` and also share an entry.

**Source evidence:** builder queries are keyed on [formatted Cypher](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/compiler.go#L34) at the [cache key](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/compiler.go#L105). The [KindMatcher emitter](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/cypher/format/format.go#L698) writes nothing for zero kinds and ignores `IsExclusive`, and an empty conjunction also renders as an empty string. This example establishes a canonical-text collision; it does not establish that every other cache-key dimension is correct. With the optimizer disabled, the cache is bypassed.

**Regression criterion:** semantically different ASTs never share a cache key. Either the emitter is lossless for every node type it accepts, or the key is derived from the AST.

<a id="dawgs-059"></a>

### DAWGS-059: The Neo4j builder rewrite turns OR into AND

**Status:** Reproduced on Neo4j for the primary OR example and Enterprise selection; other rewrite forms have generated-output evidence.

**Component:** Query builders and translation caching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** High: silent wrong result.

**Downstream priority:** Conditional — Neo4j reconciliation — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

[Enterprise's application default](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/cmd/bhe/main.go#L28-L36) is PostgreSQL, where the Neo4j kind-hoist defect does not apply. The affected reconciliation function is also gated by [reconciliationEnabled](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L31-L39). This is therefore not a default-Enterprise fix priority. It becomes an urgent conditional issue only if the active backend is Neo4j and reconciliation runs: live selection and a rollback-only deletion simulation already showed two unintended relationships selected and removed. The full Enterprise reconciliation function was not executed.

**Affected queries / consumers:** [Enterprise reconcileTrustEdges selection](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L134-L155); [deletion of selected IDs](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L163-L169); [Enterprise default backend](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/cmd/bhe/main.go#L28-L36); [active-driver resolution](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/api/tools/dbswitch.go#L80-L110); [reconciliation gate](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L31-L39).

**Precondition:**

Generated-output check: Empty graph.

Primary live check:

```cypher
CREATE ()-[:R {w:0}]->(), ()-[:T {w:2}]->()
```

Enterprise cleanup case:

```cypher
CREATE (a:Domain {name:'start'}), (b:Domain {name:'end'}),
       (a)-[:AbuseTGTDelegation]->(b),
       (b)-[:SpoofSIDHistory]->(a),
       (a)-[:SpoofSIDHistory]->(b),
       (b)-[:AbuseTGTDelegation]->(a)
```

**Additional setup:**

- Call Prepare then Render before executing the builder query. Run the two graphs as separate cases. For Enterprise cleanup, bind start/end IDs to a and b, and retain the type/direction correlation in each OR branch.

**openCypher specification:** The intended and generated queries differ under the specified Boolean truth tables. The rewrite itself is a DAWGS builder defect; this does not claim Neo4j violates the language. [Conjunction and disjunction truth tables](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc#L95-L124); [Boolean expression grouping](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L647-L660).


```go
tx.Relationships().Filter(query.Or(
	query.Kind(query.Relationship(), R),
	query.Equals(query.RelationshipProperty("w"), 2),
))
```

**Expected:**

relationships of kind R, or relationships with `w = 2`.

**Observed:**

Generated Cypher: `match ()-[r:R]->() where (r.w = $p0)`, which is an AND. `And(Kind(r, R), Kind(r, T))` becomes `[r:R|T]`, turning an AND into an OR. `Or(Kind(r, S), Kind(s, B))` becomes `(s)-[r:S]->() where (s:B)`. v2 queries executed through `neo4j.NewQueryBuilder(...).Prepare()` are rewritten the same way, although v2's own output is correct.

**Live consumer validation:**

The primary R-or-property query returned zero relationships where the intended predicate returned two on Neo4j 4.4.42. The Enterprise cleanup builder returned four relationship IDs where its intended predicate returned two. Passing those four IDs to a deletion-by-ID simulation removed all four, including two unintended relationships; that transaction was rolled back. This validates selection and its deletion consequence, not the full Enterprise reconciliation workflow. Other rewrite forms listed above retain their generated-output evidence.

**Source evidence:** the [ExpressionListRewriter KindMatcher case](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/query/neo4j/rewrite.go#L130) removes any non-negated relationship kind matcher from whatever expression list contains it, and appends it to the first relationship pattern. That hoist is only valid for a single top-level AND conjunct. The PostgreSQL driver evaluates the same criteria correctly.

**Regression criterion:** Neo4j and PostgreSQL return the same relationships for builder criteria with kind matchers inside OR, NOT, and nested AND.

<a id="dawgs-060"></a>

### DAWGS-060: The `tx.Query` rewrite re-emits a lossy AST whenever it applies

**Status:** Generated output checked.

**Component:** Query builders and translation caching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Generated-output check: Empty graph.

Proposed live label/prefix check:

```cypher
CREATE (:A:B {x:1})
```

**Additional setup:**

- Bind `props = {x:1}` and include a pattern map parameter to force rewriting. Inspect generated text without requiring a live APOC installation.

**Reproduction:**

When a pattern map parameter forces rewriting, `apoc.coll.sum` becomes `apoc.collsum`; `USING INDEX` and a `CYPHER 4.4` prefix are dropped; floats become integers (DAWGS-009). Inline `(n:A:B)` retains both labels. The separate `WHERE n:A:B` label-predicate formatter ignores exclusivity; inspect that form separately

**Expected:**

Rewriting preserves the meaning of supported names, labels, and numeric expressions; vendor prefixes and hints need their own compatibility guarantee.

**Observed:**

The checked formatter changed a qualified function name and some values/options; inline A:B labels were preserved.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/query_rewrite.go:51](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/query_rewrite.go#L51), [cypher/models/cypher/format/format.go:646](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/cypher/format/format.go#L646), [cypher/models/cypher/format/format.go:698](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/cypher/format/format.go#L698)

**Specification reference:** Rewriting must preserve qualified function names, labels, and numeric values. CYPHER 4.4 and vendor index hints have no openCypher-version guarantee established here. [Qualified function references](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L626-L633); [Conjunctive node labels](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L577-L578); [Exact INTEGER and IEEE-754 FLOAT types](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-09-16-public-type-system-type-annotation.adoc#L88-L103).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-061"></a>

### DAWGS-061: Kind and identity-property names are emitted without identifier escaping

**Status:** Generated output checked.

**Component:** Query builders and translation caching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Supply a relationship update identifying endpoints by objectids s and e. Use a kind or identity-property name requiring escaping, such as `User Account` or `object id`. Inspect generated MERGE text; live injection execution is unverified.

**Reproduction:**

The generated MERGE interpolates a supplied kind verbatim. Valid names requiring backticks can produce invalid Cypher; the generated output has not been shown to execute an injected update

**Expected:**

Quote and escape valid kind and property names that contain special characters.

**Observed:**

The checked output interpolated the supplied identifiers without the required escaping.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/cypher.go:103-189](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/cypher.go#L103)

**Specification reference:** Backquotes allow names containing otherwise invalid identifier characters. The escaping defect is in the DAWGS builder, not Neo4j execution. [Identifier quoting, printed p. 41](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=45); [Names used as labels and properties](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L988-L1010).

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

## Database drivers, graph isolation, and batching

These findings concern database selection, schema handling, batching, and driver helpers. DAWGS-071 is a related source-supported manifestation of DAWGS-063, not another confirmed family.

<a id="dawgs-062"></a>

### DAWGS-062: PostgreSQL reads and deletes ignore the target graph

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** High: cross-graph data loss.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


**Precondition:**

Graph `ga`:

```cypher
CREATE (:A)-[:R]->(:B)
```

Graph `gb`:

```cypher
CREATE (:C)
```

**Additional setup:**

- Use one isolated PostgreSQL database for ga and gb. Verify their initial contents by graph ID, not the affected unfiltered Count API.

**Reproduction:**

```go
db.WriteTransaction(ctx, func(tx graph.Transaction) error {
	return tx.WithGraph(gb).Nodes().Delete()
})
```

**Expected:**

only `gb`'s node is deleted.

**Observed:**

the nodes of both graphs are deleted. `WithGraph(gb).Nodes().Count()` likewise counts all graphs. A retriever dump of `gb` contains `ga`'s nodes (`node_count = 3` instead of 1). A two-graph dump and load duplicates every node, and `-verify-metrics` then fails.

**Source evidence:** the PostgreSQL live query [resolves the graph ID](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query.go#L31) and passes it to the translator. The translator uses it only for inserted rows ([create.go L235](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/create.go#L235), [L389](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/create.go#L389)). Every MATCH reads the partitioned parent `node`/`edge` tables without a `graph_id` predicate.

**Impact:** multi-graph deployments can destroy or leak another graph's data through graph-scoped APIs.

**Regression criterion:** graph-scoped reads, counts, updates, and deletes affect only the target graph's partition.

<a id="dawgs-063"></a>

### DAWGS-063: WithGraph drops existing indexes and constraints

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** High: schema destruction.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


**Precondition:**

Empty graph.

**Additional setup:**

- Before opening the driver under test, assert graph second with a name property index and an objectid uniqueness constraint. Use a fresh schema-manager instance for the WithGraph call; record schema objects before and after.

**Reproduction:**

```go
db.ReadTransaction(ctx, func(tx graph.Transaction) error {
	_, err := tx.WithGraph(graph.Graph{Name: "second"}).Nodes().Count()
	return err
})
```

**Expected:**

a count; the schema is unchanged.

**Observed:**

- The count succeeds.
- The graph's property index and unique constraint (in the reproduction, `node_3_name_index` and `node_3_objectid_constraint`) are dropped; only the primary key and kind index remain.
- Later `UpdateNodeBy` calls fail with `no unique or exclusion constraint matching the ON CONFLICT specification`.

**Source evidence:** for any graph not yet cached, [getTargetGraph](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/transaction.go#L115) calls `AssertGraph` with the caller's `graph.Graph`. [Query.AssertGraph](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query/query.go#L338) removes every present index and constraint that the passed definition lacks, and a name-only definition lacks all of them. ReadTransaction does not open a PostgreSQL transaction, so the DDL autocommits.

**Regression criterion:** selecting a graph never changes its schema. Schema changes happen only through explicit schema assertion.

<a id="dawgs-064"></a>

### DAWGS-064: Batch de-duplication keys collide

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** High: silent data loss.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


**Precondition:**

```go
err := db.BatchOperation(ctx, func(batch graph.Batch) error {
	for _, id := range []graph.ID{1, 3, 12, 23} {
		if err := batch.CreateNode(graph.NewNode(id, graph.NewProperties(), graph.StringKind("A"))); err != nil {
			return err
		}
	}
	return nil
})
```

**Additional setup:**

- Register R. Commit the node batch before the relationship batch; put both relationship creations in one subsequent batch.

**Reproduction:**

```go
db.BatchOperation(ctx, func(batch graph.Batch) error {
	_ = batch.CreateRelationshipByIDs(1, 23, R, graph.AsProperties(map[string]any{"name": "first"}))
	return batch.CreateRelationshipByIDs(12, 3, R, graph.AsProperties(map[string]any{"name": "second"}))
})
```

**Expected:**

two relationships, `1→23` and `12→3`.

**Observed:**

one relationship, `1→23 {name: second}`; `12→3` is never created. The trigger and the following key/hash builders omit structural boundaries:

| Location | Behavior |
| --- | --- |
| [relationshipCreateBatchBuilder.Add](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/batch.go#L742) (the trigger above) | Builds its key as `StartID + EndID + Kind` without delimiters, so relationships with different endpoints collide. The separate stored-edge-ID/slice-index problem is not counted again here. |
| [NodeUpdate.Key](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/graph.go#L167) and [RelationshipUpdate.Key](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/graph.go#L194) | Concatenate identity values. `UpdateRelationshipBy` with objectids `A→BC` and `AB→C` creates four nodes but one relationship. |
| Neo4j [newUpdateKey](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/cypher.go#L17) and [nodeUpdateByMap.add](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/cypher.go#L215) | Kind digests collide across add and remove sets. `AddKinds(A)` on one node and `DeleteKinds(A)` on another produce one statement applying the first node's kinds to both. Deleted kinds are omitted from the upsert key. |
| [Properties.HashInto](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/properties.go#L489) | Hashes `{"a":1,"b":2}` and `{"a1b":2}` identically. |

**Impact:** delimiter-free numeric or OpenGraph-style identifiers can silently merge unrelated entities.

**Regression criterion:** keys are delimiter- or length-prefixed, and batch tests cover colliding concatenations.

<a id="dawgs-065"></a>

### DAWGS-065: Kind mapping can deadlock the connection pool

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** High: driver hang.

**Precondition:**

Empty graph.

**Additional setup:**

- Initialize the schema first. Use a pool with effective MaxConns 1 and DAWGS connection hooks; register a fresh kind while the write transaction holds the connection.

- For N connections, hold all N at a barrier. pg.NewPool resets MaxConns to 50, so verify the actual pool configuration.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


Configure an actual pgx pool with `MaxConns = N`. Hold N concurrent write transactions at a barrier after they acquire their connections, then let them register previously unseen kinds. The label must be absent from the schema manager cache. **Important setup detail:** `pg.NewPool` overwrites `MaxConns` with 50; for a one-connection reproduction, construct the pgx pool directly with DAWGS connection hooks, or use a pool whose effective configuration is verified.

**Expected:**

all transactions complete.

**Observed:**

- With all N connections held, kind registration waits for a second connection. Without cancellation or another connection being released, it cannot progress.
- Unrelated ReadTransactions then time out, because target-graph resolution needs the same lock.
- The independent probe used an actual one-connection pool. `CreateNode` with a fresh kind waited until its 500 ms context expired and returned `context deadline exceeded`. A pool made with `pg.NewPool` did not reproduce this single-call case because its effective maximum was 50.

**Source evidence:** [assertKinds](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/manager.go#L271) acquires a second pool connection while holding the global schema write lock. `MapKind`, `MapKinds`, `MapKindIDs`, and the driver's kind resolution follow the same pattern.

**Regression criterion:** kind registration inside a transaction never needs a second connection while holding the schema lock. A concurrency test at `MaxConns` completes.

<a id="dawgs-066"></a>

### DAWGS-066: Changing the default graph is silently ignored after the first one

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Initialize one driver with default graph g1, request default g2 on that same instance, then create a probe node without WithGraph and inspect its graph ID.

**Reproduction:**

`AssertSchema(DefaultGraph=g2)` and `SetDefaultGraph(g2)` return nil; new nodes still go to `g1`

**Expected:**

Either change the selected default graph as requested or return an explicit error.

**Observed:**

The calls returned success, but later nodes still used the first default graph.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/manager.go:310](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/manager.go#L310)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-067"></a>

### DAWGS-067: `AssertSchema` never creates the non-default graphs it is given

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use a database containing neither main nor second. Assert DefaultGraph main and Graphs [main, second]; inspect graph metadata before WithGraph can create second.

**Reproduction:**

`Graphs=[main, second]` creates only `main`; the other graph's indexes apply only on a later `WithGraph` (see DAWGS-063)

**Expected:**

Create the non-default graphs included in the requested schema.

**Observed:**

Only the default graph was created.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/manager.go:403](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/manager.go#L403)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-068"></a>

### DAWGS-068: Updating with no changes or nil properties panics

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A)-[:R]->(:B)
```

**Additional setup:**

- Fetch the relationship with no tracked changes for UpdateRelationship. For the node variant, construct its update with Properties nil. Test each case separately.

**Reproduction:**

`UpdateRelationship` with no modified properties runs `Exec("")`; `UpdateNode` with nil properties dereferences nil

**Expected:**

Handle an empty change set or nil properties without an unhandled panic; an explicit error is acceptable where the API rejects the input.

**Observed:**

The empty-change or nil-properties paths panicked.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/transaction.go:223](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/transaction.go#L223)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-069"></a>

### DAWGS-069: `query.SelectKindID` loads the wrong SQL file and always fails

**Status:** Replayed PG (primary trigger).

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Register kind A before calling SelectKindID(A) through a transaction.

**Reproduction:**

it returns `-1, no rows` for an existing kind

**Expected:**

Return the existing kind ID using the intended SELECT statement.

**Observed:**

The helper returned -1 and a no-rows error for an existing kind.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/query/sql.go:49](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query/sql.go#L49)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-070"></a>

### DAWGS-070: `batch.UpdateNodes` ignores property deletions; an integration test asserts the defect

**Status:** Generated output checked.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Generated-output check: Empty graph.

Live check:

```cypher
CREATE (:A {gone:1})
```

**Additional setup:**

- Call Properties.Delete("gone") on the update object before batch.UpdateNodes. For the generated-output probe, an in-memory node with ID 1, kind A, and the tracked deletion suffices; live execution is unverified.

**Reproduction:**

`Properties.Delete("gone")` emits only `set n += p.properties`

**Expected:**

The generated update removes properties marked for deletion.

**Observed:**

The checked Cypher set the property map without emitting the requested deletions.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/cypher.go:306](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/cypher.go#L306), [drivers/neo4j/batch_integration_test.go:119](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/batch_integration_test.go#L119), [drivers/neo4j/batch_integration_test.go:137](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/batch_integration_test.go#L137)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-071"></a>

### DAWGS-071: Dumping an unknown graph name creates that graph

**Status:** Related to DAWGS-063; source-supported.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Ensure graph name typo is absent from the source catalogue before requesting its dump; inspect metadata afterward.

**Reproduction:**

a typo creates a graph row and its partitions in the source database during a read-only dump

**Expected:**

A graph read should follow the graph-selection contract; implicit creation is a related manifestation of DAWGS-063, not a separate proven family.

**Observed:**

Source-supported related behavior; no independent defect-family claim is made.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/transaction.go:105](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/transaction.go#L105)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

## Graph algorithms, caches, and utilities

These findings concern graph helper APIs and their supporting containers. Their expected behavior comes from the API or data structure, rather than an openCypher language rule.

<a id="dawgs-072"></a>

### DAWGS-072: Parallel fetch operations hang when a worker fails

**Status:** Replayed PG (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** PostgreSQL.

**Priority:** High: caller hang.

**Downstream priority:** P2 — legacy upgrades — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

The actual caller is [Version_513_Migration](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L443-L460), [registered for upgrades crossing version 5.13.0](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L704-L707), and it uses [six workers](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/post/operation.go#L28-L33). With max node ID 140000 and a controlled query failure in each worker, the migration waited until external cancellation; the max-ID-50000 control returned its errors without that wait. P2 is limited to affected legacy upgrade/error conditions, not ordinary current-version operation. The [migration-version check](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/graph.go#L212-L228) skips this step once its version is already applied.

**Affected queries / consumers:** [Version_513_Migration](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L458-L479); [5.13 migration registration](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L704-L707); [six-worker setting](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/post/operation.go#L28-L33); [migration-version check](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/graph.go#L212-L228).

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


**Precondition:**

```go
err := db.BatchOperation(ctx, func(batch graph.Batch) error {
	return batch.CreateNode(graph.NewNode(50000, graph.NewProperties(), graph.StringKind("A")))
})
```

Application migration case:

```go
err := db.BatchOperation(ctx, func(batch graph.Batch) error {
    return batch.CreateNode(graph.NewNode(140000, graph.NewProperties(), graph.StringKind("Base"), graph.StringKind("User")))
})
```

**Additional setup:**

- For the primary helper probe, keep NoSuchKind unregistered and use one worker.

- For the separate application migration fixture, inject a query error in all six migration workers after each receives a range. The max-ID-50000 control has only three ranges and returned errors promptly. Use an uncancelled context initially, then external cancellation to release a wait.

**Reproduction:**

```go
ops.ParallelFetchNodes(ctx, db, query.KindIn(query.Node(), graph.StringKind("NoSuchKind")), 1)
```

**Expected:**

an error.

**Observed:**

after the worker failed, the producer remained blocked submitting further ranges. The independent probe had to cancel the caller context to obtain the worker error. This is a hang with an uncancelled context, not proof that cancellation cannot release it. `ops.ParallelNodeQuery` has the same producer/worker structure; its separate delegate-error example remains source-supported.

**Source evidence:** a failing worker reports its error and exits. The producer loops in [parallelFetchNodes](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/ops.go#L571) and [parallelNodeQuery](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/parallel.go#L357), the helpers behind the public functions, keep doing a blocking send on an unbuffered channel that no worker reads, and the context is never cancelled.

**Regression criterion:** any worker error cancels the remaining work and is returned to the caller.

<a id="dawgs-073"></a>

### DAWGS-073: ReachabilityCache returns incomplete reach sets

**Status:** Replayed Go (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** High: silent wrong result.

**Downstream priority:** P1 — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

BloodHound's actual [FetchCanRDPEntityBitmapForComputer](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L583-L619) function returned eligible user 5 with a fresh cache and no eligible users after the same membership cache was warmed from group 1. The test used enforceURA=true, Citrix disabled, and the production cache-size formula on 106 membership nodes, including a database-fetched CSR membership graph. The returned IDs feed [CanRDP edge generation](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/local_groups.go#L114-L135), supporting P1 for analysis correctness. This demonstrates a wrong application eligibility set; persistence through the complete analysis pipeline was not tested.

**Affected queries / consumers:** [FetchAdminGroupIds](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/owns.go#L301-L320); [NTLM membership expansion](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/ntlm.go#L532-L550); [CanRDP membership expansion](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L853-L870); [CanRDP eligibility function](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L583-L619); [URA membership calculation](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L851-L883); [CanRDP edge writer input](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/local_groups.go#L114-L135); [shared cache across analysis stages](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L904-L922).

**Precondition:**

```go
digraph := container.NewAdjacencyMapGraph()
for _, edge := range [][2]uint64{{1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}} {
	digraph.AddEdge(edge[0], edge[1])
}
```

Inbound OrReach consumer check:

```go
membership := container.NewAdjacencyMapGraph()
for _, edge := range [][2]uint64{{2, 1}, {3, 1}, {4, 2}, {4, 3}, {5, 4}} {
    membership.AddEdge(edge[0], edge[1])
}
```

CanRDP application case:

```go
membership := container.NewAdjacencyMapGraph()
for _, edge := range [][2]uint64{{2, 1}, {3, 1}, {4, 2}, {4, 3}, {5, 4}, {3, 6}} {
    membership.AddEdge(edge[0], edge[1])
}
for id := uint64(10); id < 110; id += 2 {
    membership.AddEdge(id, id+1)
}
ril := container.NewAdjacencyMapGraph()
ril.AddEdge(5, 9)
```

**Additional setup:**

- Use one ReachabilityCache with capacity 100 for each graph. For the outbound example, query member 1 before member 3. For the inbound OrReach check, warm from member 1, then call OrReach(3, graph.DirectionInbound, result) into a fresh bitmap. Use a new cache for each control.

**Application setup:** Use `RemoteDesktopUsersLocalGroup.ID = 6`, `Computer = 9`, and the `ril` graph above. Construct the membership cache with `int(float64(membership.NumNodes()) * 0.15)`. Call `FetchCanRDPEntityBitmapForComputer(data, true, false)` once with a fresh cache and once after warming inbound reach from member 1. For the database-backed check, persist membership edges as MemberOf except 3→6 as MemberOfLocalGroup, and 5→9 as RemoteInteractiveLogonRight; build the cache with FetchFilteredReachabilityCache using those two membership kinds. User 5 is a member through group 3 and has a direct RIL right to computer 9; local group 6 itself has no RIL edge, so the caller takes its URA calculation path.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


Graph: edges `1→2`, `1→3`, `2→4`, `3→4`, `4→5`.

```go
cache := algo.NewReachabilityCache(ctx, digraph, 100)
cache.ReachOfComponentContainingMember(1, graph.DirectionOutbound)
cache.ReachOfComponentContainingMember(3, graph.DirectionOutbound)
```

**Expected:**

the reach of 3 is `[3 4 5]`.

**Observed:**

`[3 4]`. A fresh cache returns `[3 4 5]`. This result was reproduced on the stated five-node fixture.

**BloodHound caller validation:**

[FetchCanRDPEntityBitmapForComputer](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L583-L619) returned `[5]` with a fresh cache and `[]` with the warmed cache on the CanRDP application graph. The same difference reproduced with a database-fetched CSR graph containing 106 membership nodes and 56 membership edges, using the [normal cache factory](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/algo/reach.go#L295-L310). These are the IDs the post-processing writer uses for CanRDP edges. This is a direct call to the application eligibility function, not a complete analysis/persistence run.

**Consumer-style validation:**

On the inbound membership graph above, warming from member 1 and then calling `OrReach(3, graph.DirectionInbound, result)` produced `[4]`; a fresh cache produced `[4,5]`. OrReach intentionally excludes root 3. This uses the same API/direction as the linked BloodHound administrative-group expansion. It does not establish the final attack-edge difference in a complete analysis run.

**Source evidence:** `componentReachDFS` uses the root cursor's bitmap as the visited set for the whole search. A non-root cursor skips a component already visited through another branch, so that component's descendants are never merged into the cursor's reach. The cursor's incomplete reach is then [cached](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/algo/reach.go#L229) and served to later queries.

**Regression criterion:** cached and uncached reach are identical for every component, in either query order.

<a id="dawgs-074"></a>

### DAWGS-074: Traversal patterns that change direction never match

**Status:** Replayed PG (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** PostgreSQL.

**Priority:** Medium-high: missing results.

**Precondition:**

```cypher
CREATE (a:A {name:"a"})-[:R]->(b:B {name:"b"})<-[:T]-(c:C {name:"c"})
```

**Additional setup:**

- Bind R/T to the graph kinds. Run BreadthFirst from node a using the outbound-R then inbound-T pattern.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


Graph: `a-[:R]->b<-[:T]-c`.

```go
traversal.NewPattern().
	OutboundWithDepth(1, 1, query.KindIn(query.Relationship(), R)).
	InboundWithDepth(1, 1, query.KindIn(query.Relationship(), T))
```

Run it with `BreadthFirst` from `a`.

**Expected:**

one match, `a→b←c`.

**Observed:**

no matches and no error. Patterns that stay in one direction work.

**Source evidence:** the [fetch direction](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/traversal/traversal.go#L199) is computed from the current expansion and reused to fetch the next one. For an inbound step after an outbound step, the fetch returns the segment's own node, which is then discarded as a cycle.

**Regression criterion:** mixed-direction patterns return the same matches as the equivalent Cypher pattern.

<a id="dawgs-075"></a>

### DAWGS-075: `graphcache.FetchRelationshipsByID` queries all ids, then slices by the missing count

**Status:** Replayed PG (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use a fresh empty graphcache and request IDs 9999998 and 9999999; neither relationship may exist in the graph or cache.

**Reproduction:**

panics with `slice bounds out of range [-2:]`

**Expected:**

Fetch the missing relationship IDs and combine the results without an invalid slice.

**Observed:**

The controlled cache probe panicked on a negative slice bound.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [graphcache/cache.go:171](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graphcache/cache.go#L171), [graphcache/cache.go:181](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graphcache/cache.go#L181)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-076"></a>

### DAWGS-076: Bitmap `And` / `AndNot` with a non-native duplex mutate during iteration

**Status:** Replayed Go (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- No database is needed. Use a Bitmap64 containing 0–9 and an empty Bitmap64 wrapped in ThreadSafeDuplex; recreate operands for each operation. The HLL/simplex subcase remains unverified.

**Reproduction:**

`NewBitmap64With(0,1,2,3,4,5,6,7,8,9).And(ThreadSafeDuplex(NewBitmap64()))` leaves `[1,3,5,7]` (expected empty); the HLL/simplex subcase remains source-supported

**Expected:**

AND with an empty bitmap produces an empty set, including through the duplex wrapper.

**Observed:**

The wrapped-bitmap operation left elements in the result when the expected set was empty.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cardinality/roaring64.go:99](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cardinality/roaring64.go#L99), [cardinality/roaring64.go:137](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cardinality/roaring64.go#L137), [cardinality/roaring32.go:100](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cardinality/roaring32.go#L100), [cardinality/hyperloglog64.go:55](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cardinality/hyperloglog64.go#L55)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-077"></a>

### DAWGS-077: `PackedMap` corrupts values after inserting keys between compactions; `EFSet.Find` underflows below the minimum

**Status:** Replayed Go (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- No database is needed. Initialize PackedMap with BucketKeyStride 100, NumMaxEntries 100, NumBuckets 10. Use a separate EFSet containing 10, 20, 30 for Find(5), with a bounded execution budget.

**Reproduction:**

Put `(2,102)` and `(3,103)`, Compact, put `(1,101)`, Compact: `Get(1)` returns 102; `CompressToEFSet([]uint64{10,20,30}).Find(5)` did not finish within the probe budget

**Expected:**

Preserve each key/value association across insertion and compaction; a lookup below the minimum must terminate with no match.

**Observed:**

The controlled compaction returned a value for the wrong key; the below-minimum lookup did not finish within the probe budget.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [container/pacmap.go:116](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/pacmap.go#L116), [container/ef.go:158](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/ef.go#L158), [container/ef.go:169](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/ef.go#L169)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-078"></a>

### DAWGS-078: `BFSTreeFile` reads the compressed source instead of the gzip reader; `ToSegment` indexes edge -1

**Status:** Replayed Go (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- No database is needed for the SerializedSegment input below. The BFSTreeFile variant additionally needs a matching writer/reader gzip round-trip; it remains unverified.

**Reproduction:**

`SerializedSegment{Nodes: []uint64{1,2}, Edges: []uint64{1}}.ToSegment()` panics; file read/write mismatch is source-supported. Unlimited cyclic traversal is discussed separately as an API-contract question

**Expected:**

Read through the gzip decoder and reconstruct a valid segment without indexing edge -1.

**Observed:**

The segment probe panicked; the file-reader mismatch is supported by source rather than a separate replay.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [container/bfs.go:33](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/bfs.go#L33), [container/segment.go:141](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/segment.go#L141)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-079"></a>

### DAWGS-079: `SortAndSliceNodeSet` panics when skip or limit exceeds the set

**Status:** Replayed Go (primary trigger).

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Downstream priority:** Not prioritized — guarded caller — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

The helper defect is real, but the actual BloodHound entity-list service calls its delegate with skip=limit=0 and [applies bounds afterward](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1096-L1128). With one DCSyncer, [GetEntityResults](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1164-L1176) safely returned one row for limit 10 and returned an ordinary invalid-skip error for skip 2. Direct [FetchDCSyncers(0,10)](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L812-L825) still panicked. No unguarded product route was identified among the inspected callers, so this is not a demonstrated BloodHound endpoint fix priority.

**Affected queries / consumers:** [FetchDCSyncers pagination](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L812-L849); [HTTP fetch and pagination guard](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1096-L1128); [list-path bounds checks](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1096-L1128).

**Precondition:**

```go
nodes := graph.NewNodeSet(graph.NewNode(1, nil))
```

BloodHound entity-list case:

```cypher
CREATE (:User:Base {objectid:'u',name:'user'})-[:DCSync]->(:Domain:Base {objectid:'d',name:'domain'})
```

**Additional setup:**

- This is an in-memory NodeSet; no database is needed. Call SortAndSliceNodeSet(nodes, 0, 10).

**Reproduction:**

`SortAndSliceNodeSet` with skip 0 and limit 10 on a one-node set panics with `slice bounds out of range`; the requested limit exceeds the set size

**Expected:**

Handle skip/limit values beyond the result size without a slice-bounds panic.

**Observed:**

The out-of-range paging probe panicked.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**BloodHound caller validation:** With the Domain node as the target and [FetchDCSyncers](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L812-L825) as the list delegate, [GetEntityResults](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1164-L1176) returned one row for skip 0/limit 10 and an ordinary `invalid skip: 2` error for skip 2/limit 10. Calling the delegate directly with skip 0/limit 10 reproduced the panic. The application list guard is effective on these cases.

**Source:** [graph/util.go:24](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/util.go#L24)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-098"></a>

### DAWGS-098: `FetchByQuery` undercounts nested path and literal memory

**Status:** Reproduced through BloodHound on PostgreSQL for a large literal; nested/path variants remain source-supported.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** BloodHound query service / PostgreSQL; shared FetchByQuery accounting.

**Priority:** Medium: configured result-budget enforcement.

**Downstream priority:** P2 — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

BloodHound's actual [PrepareCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L580) and [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611) accepted a query returning a 65,536-byte string with a 16,384-byte driver result-memory budget, returning the literal without a memory-limit error. Query preparation did not block this case. P2 is supported by an application result-budget bypass, rather than a source-only concern. The test used a reduced budget; it did not measure peak memory or exhaust the [default 2 GiB budget](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/config/default.go#L82-L85), and nested/path variants still need separate measurement.

**Affected queries / consumers:** [RawCypherQuery → FetchByQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L592-L620); [BloodHound query service](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L611); [Cypher request route](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/api/v2/cypherquery.go#L124-L175); [default result-memory budget](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/config/default.go#L82-L85).

**Precondition:**

Path-result variant (not runtime-validated):

```cypher
CREATE (a:A {payload:$payload})-[:R]->(b:B {payload:$payload})-[:R]->(c:C {payload:$payload})
```

Reproduced literal case: Empty graph.

**Additional setup:**

- For the literal case, initialize the PostgreSQL driver with `pg.NewDriver(16 * size.Kibibyte, pool)`. Construct the normal BloodHound GraphQuery service with complexity checks enabled.

- For the separate path variant, bind payload to a large string and measure its accounting independently; that variant is not established by the literal reproduction.

**Reproduction:**

```go
payload := strings.Repeat("x", 65536)
prepared, err := service.PrepareCypherQuery("RETURN '" + payload + "' AS payload", queries.DefaultQueryFitnessLowerBoundExplore)
if err != nil {
    panic(err)
}
response, err := service.RawCypherQuery(ctx, graphschema.ValidKinds, prepared, true)
if err != nil {
    panic(err)
}
fmt.Println(len(response.Literals[0].Value.(string)))
```

**Expected:**

Charge nested values and path contents against the intended result-memory budget.

**Observed:**

The service returned the 65,536-byte string without a memory-limit error despite a 16,384-byte result budget. This demonstrates budget enforcement failure at the application query-service boundary. Peak memory and exhaustion of the default 2 GiB budget were not measured.

**Likely cause:**

FetchByQuery uses `size.Of(result.Literals)` for the literal collection, which counts the slice descriptor rather than the string allocation stored in its elements. The same function uses shallow path-container measurements; their full runtime effect needs separate tests.

**Source:** [ops/ops.go:232](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/ops.go#L232)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

The over-budget literal must return `ops.ErrGraphQueryMemoryLimit`. Verify path properties and nested values with separate fixtures, preserving the normal query-preparation path.

## Import, export, and test tooling

These findings concern data scrubbing, serialization, archive handling, and whether test failures reach the test-command exit status. They have different owners and validation needs from query translation.

<a id="dawgs-080"></a>

### DAWGS-080: Full scrub does not inspect sensitive nested keys beneath an unclassified outer property

**Status:** Replayed Go (primary trigger).

**Component:** Import, export, and test tooling.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

```go
node := graph.NewNode(1, graph.AsProperties(map[string]any{
	"extra": map[string]any{
		"email": "alice@example.com",
		"ssn":   "123-45-6789",
	},
}), graph.StringKind("A"))
```

**Additional setup:**

- No database is needed. Pass the nested extra map to a full-mode scrubber with salt review-salt and extra classified as preserve. Repeat with the same map under name as the control.

**Reproduction:**

`extra={"email":"alice@example.com","ssn":"123-45-6789"}` is preserved because `extra` is classified as preserve. The action count is correctly preserve. A map under `name` is recursively pseudonymized, so the outer property classification is necessary to the trigger

**Expected:**

Under the full-scrub expectation in this finding, apply the configured sensitive-field action to nested sensitive keys.

**Observed:**

The nested email and ssn values under extra were preserved; maps under name were recursively scrubbed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [retriever/scrubber.go:295](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/scrubber.go#L295)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-081"></a>

### DAWGS-081: Scrub configuration options are parsed but unused

**Status:** Replayed Go (primary trigger).

**Component:** Import, export, and test tooling.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- No database is needed. Set `[scrub.graph_rules] case_insensitive_domain_names = true` and salt review-salt; compare the domain property values CORP.LOCAL and corp.local.

**Reproduction:**

With salt `review-salt`, `scrubProperty("domain", "CORP.LOCAL")` and its lowercase variant receive different host pseudonyms despite `case_insensitive_domain_names = true`

**Expected:**

Honor the case-insensitive domain-name option when producing the pseudonym.

**Observed:**

Uppercase and lowercase domain variants produced different pseudonyms despite the option.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [retriever/scrubber.go:53](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/scrubber.go#L53), [retriever/scrubber.go:692](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/scrubber.go#L692)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-082"></a>

### DAWGS-082: Dumps from Neo4j write temporal properties as `{}`

**Status:** Generated output checked.

**Component:** Import, export, and test tooling.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Serialization check: Empty graph.

Live dump check:

```cypher
CREATE (:A {
  d:date('2020-01-01'),
  dt:localdatetime('2020-01-01T12:00:00'),
  lt:localtime('12:00:00'),
  t:time('12:00:00Z')
})
```

**Additional setup:**

- The recorded check marshals a converted Neo4j driver node containing native temporal Go values. The Cypher setup is for a live dump check; keep values temporal, not strings.

**Reproduction:**

`date`, `localdatetime`, `localtime`, and `time` values become empty objects

**Expected:**

Serialize temporal values in a representation that preserves the value rather than an empty object.

**Observed:**

The checked serialized temporal values were empty objects.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/node.go:56](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/node.go#L56), [retriever/dump.go:662](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/dump.go#L662)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-083"></a>

### DAWGS-083: Unpacking into an output directory with a trailing slash fails

**Status:** Replayed Go (primary trigger).

**Component:** Import, export, and test tooling.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use a writable temporary parent, output path `/tmp/example/out/`, and force disabled. For the recorded helper check, create staging, write one file, then promote staging. A full UnpackTar check also needs a valid archive.

**Reproduction:**

`output directory … is not empty` or `rename … invalid argument`

**Expected:**

A trailing path separator should not make an otherwise valid output-directory operation fail.

**Observed:**

The trailing-separator path produced an output-directory or rename error.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [retriever/archive_envelope.go:354](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/archive_envelope.go#L354)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

Repeat the recorded trigger with a complete fixture and assert the expected behavior above. Preserve relevant backend and optimizer coverage.

<a id="dawgs-084"></a>

### DAWGS-084: The bundled BDD command can report success with failing TCK cases

**Status:** Reproduced test-reporting issue.

**Component:** Import, export, and test tooling.

**Backend / scope:** BDD harness; PostgreSQL-selected run.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use an isolated PostgreSQL test database and set CONNECTION_STRING. Keep the bundled upstream feature cases and compare their outcomes with the suite process exit status.

**openCypher specification:** No direct openCypher language rule specifies the DAWGS test command exit status. This is a DAWGS test-harness reporting issue.

This is a **test-reporting issue**, separate from product query defects. The recorded `make test_all` exited 0. All eight DAWGS-owned BDD cases passed, but the 20 bundled upstream cases had 2 passes, 11 failures, and 7 undefined-step cases.

[TestMain](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/bdd/steps_test.go#L113) discards `m.Run()`'s return code and intentionally fails only for DAWGS-owned feature failures or invalid reports. The observed stage results are:

| Stage or case group | Observed outcome |
| --- | --- |
| Unit tests | Passed |
| Repository integration tests, PostgreSQL selected | Passed |
| DAWGS-owned BDD cases | 8 passed |
| Bundled upstream TCK cases | 2 passed, 11 failed, 7 undefined |
| Overall `make test_all` process | Exit code 0 |

An undefined step is a harness gap, not automatically an implementation failure.

**Regression criterion:** a conformance-gating command exits nonzero for failed or unimplemented upstream assertions. If a reporting-only mode is retained, identify it explicitly and provide a separate gate.

## Findings requiring runtime validation

These candidates have a source-supported mechanism but no complete independently reproduced runtime failure. They remain investigation work. First capture a complete fixture, the expected behavior or contract, and the actual result. The component and backend fields identify where to start; they do not promote a candidate to a confirmed defect. The procedure and compilation-size observations that follow have their own narrower evidence limits.

<a id="dawgs-085"></a>

### DAWGS-085: Builder `query.Xor` adds no parentheses, unlike `query.Or`

**Status:** Source-supported; runtime replay pending.

**Component:** Query builders and translation caching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Rendering check: Empty graph.

Proposed live check:

```cypher
CREATE (:A {a:true, b:false, c:false})
```

**Additional setup:**

- Bind builder predicates a, b, c to the corresponding property comparisons. The graph is a proposed runtime fixture; execution remains pending.

**Reproduction:**

`query.And(query.Xor(a, b), c)` renders as `a xor b and c`, which Cypher evaluates as `a xor (b and c)`; Neo4j, which supports XOR, returns the wrong rows (PostgreSQL XOR translation is excluded: documented as unsupported)

**Expected:**

Preserve the builder grouping `(a XOR b) AND c` when rendering the query.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [query/model.go:192](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/query/model.go#L192) (compare [query/model.go:186](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/query/model.go#L186))

**Specification reference:** AND has higher precedence than XOR. The emitted query changes the builder grouping; the source-supported builder claim remains pending runtime replay. [Logical precedence levels and rule](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2021-08-10-Operator-precedence.adoc#L202-L233).

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-086"></a>

### DAWGS-086: v2 `KindIn` with no kinds on a relationship matches every relationship

**Status:** Source-supported; runtime replay pending.

**Component:** Query builders and translation caching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Rendering check: Empty graph.

Proposed live check:

```cypher
CREATE (:A)-[:R]->(:B)
```

**Additional setup:**

- Call v2 KindIn on the relationship with zero kinds and compare legacy KindIn. The graph is a proposed runtime fixture; execution remains pending.

**Reproduction:**

`match ()-[r]->()`, whereas legacy `query.KindIn` matches none; `KindIn(Node())` renders `where  return`

**Expected:**

Establish the intended empty-kind behavior, then render a valid predicate consistent with that API contract rather than accidentally matching every relationship.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [query/v2/compat.go:88](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/query/v2/compat.go#L88)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-087"></a>

### DAWGS-087: `UpdateNodeBy` ignores deleted kinds on PostgreSQL

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A:B {objectid:'n'})
```

**Additional setup:**

- Assert the objectid identity constraint required by UpdateNodeBy; track B in DeletedKinds. Complete schema/update arguments and runtime evidence remain pending.

**Reproduction:**

kinds `[A,B]`, update with `DeleteKinds(B)`: kinds stay `[A,B]` (Neo4j removes B)

**Expected:**

Remove kind B when DeleteKinds(B) is requested, leaving kind A.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/query/format.go:155](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query/format.go#L155)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-088"></a>

### DAWGS-088: A concurrent kind insert from another session fails `AssertKinds`

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use two independent sessions and fresh kind K. Synchronize conflicting registrations so one insert commits while the other statement is in progress. A deterministic schedule and runtime evidence remain pending.

**Reproduction:**

`no rows in result set` although the kind exists

**Expected:**

Concurrent assertion of an already-created kind returns its ID rather than a spurious no-rows error.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/query/sql/insert_or_get_kind.sql:4](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/query/sql/insert_or_get_kind.sql#L4)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-089"></a>

### DAWGS-089: `GetKindIDsByKind` exposes an internal map; `FetchKinds` iterates it after unlocking

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Register A on a shared schema manager. Overlap GetKindIDsByKind/FetchKinds with adding fresh kind B. A synchronized race-detector test remains pending.

**Reproduction:**

The getter releases its lock before returning the shared map, and FetchKinds iterates outside that lock; a focused race-detector case is required

**Expected:**

Access shared kind mappings under an appropriate synchronization/ownership rule; establish this with a focused race test.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/manager.go:118](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/manager.go#L118)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-090"></a>

### DAWGS-090: Mixed-case index fields are dropped and rebuilt on every start

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Declare a node-property index on mixed-case objectId for graph main. Record its OID, then assert the identical schema using a fresh manager. Runtime evidence remains pending.

**Reproduction:**

the index OID changes on each `AssertSchema` (non-concurrent DDL)

**Expected:**

Repeated schema assertion should retain an equivalent existing index rather than rebuild it solely because of field-name case.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/model/format.go:27](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/model/format.go#L27)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-091"></a>

### DAWGS-091: Cursor `Close` races the producer goroutine

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A), (:A), (:A)
```

**Additional setup:**

- Overlap cursor Close with its active producer/Next goroutine. A synchronized race-detector test remains pending.

**Reproduction:**

Close cancels and closes the underlying result without joining the Next goroutine; a focused pgx race-detector case is required

**Expected:**

Coordinate cursor shutdown with the producer; the pending race test must establish the exact failure.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [graph/result.go:82](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/result.go#L82)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-092"></a>

### DAWGS-092: Nil properties panic in the Neo4j create and update paths

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (:A {name:'s'}), (:B {name:'e'})
```

**Additional setup:**

- Record the two node IDs and pass nil properties to CreateRelationshipByIDs. An empty map does not test nil handling. Live execution remains pending.

**Reproduction:**

`CreateRelationshipByIDs(s, e, kind, nil)` (PostgreSQL uses `MapOrEmpty`)

**Expected:**

Handle nil property inputs according to the API contract without an unhandled panic.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/batch.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/batch.go#L228), [drivers/neo4j/transaction.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/transaction.go#L265)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-093"></a>

### DAWGS-093: Running Neo4j operations do not receive the caller context

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use a data-independent long-running Neo4j query. Cancel after connection acquisition and query start; record the separate transaction timeout. The exact query, timing controls, and live result remain pending.

**Reproduction:**

After connection-slot acquisition, transaction/run calls use the context-free Neo4j API. A separate configured transaction timeout exists; a live cancellation/deadline reproduction is still needed

**Expected:**

Cancellation/deadline behavior must follow the caller-context guarantee; validate that guarantee against the live Neo4j driver.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/driver.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/driver.go#L88), [drivers/neo4j/transaction.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/transaction.go#L265)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-094"></a>

### DAWGS-094: `DatabaseSwitch` does not forward size setters, races on `inSwitch`, and `IsDriver` is wrong

**Status:** Source-supported; runtime replay pending.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Wrap an initialized driver or recording test double in DatabaseSwitch. Check setter forwarding and IsDriver; the overlapping inSwitch access still needs a synchronized race-detector test.

**Reproduction:**

Size setters update wrapper fields without forwarding to currentDB; IsDriver returns false for an unwrapped matching driver. The inSwitch access needs a focused race test

**Expected:**

Forward supported driver settings, report the actual driver type correctly, and synchronize switching state.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [graph/switch.go:62](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/switch.go#L62)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-095"></a>

### DAWGS-095: Traversal patterns report a match twice when the next step is optional

**Status:** Source-supported; runtime replay pending.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Downstream priority:** Not prioritized — no incorrect selection shown — see [consumer priorities](#priorities-for-bloodhound-consumers).

**BloodHound priority assessment:**

BloodHound's actual [FetchNodesFromSeeds](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L136-L145) parent-expansion path returned exactly seed+OU without GPLink and seed+OU+GPO with GPLink, with no errors. Its [parent-node collector deduplicates nodes by ID](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L540-L544). This does not support prioritizing the suspected duplicate-callback behavior as incorrect BloodHound membership. The DAWGS callback candidate remains unverified; a distinct performance cost or wrong final result would need evidence before assigning an application fix priority.

**Affected queries / consumers:** [fetchADParentNodes traversal](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L393-L411); [parent-node collector](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L540-L544).

**Precondition:**

```cypher
CREATE (a:A {name:"a"})-[:R]->(b:B {name:"b"})-[:T]->(c:C {name:"c"})
```

BloodHound parent-expansion case:

```cypher
CREATE (ou:OU:Base {objectid:'ou',name:'ou'})-[:Contains]->(:User:Base {objectid:'seed',name:'seed',isaclprotected:true}),
       (:GPO:Base {objectid:'gpo',name:'gpo'})-[:GPLink]->(ou)
```

Without-GPLink control (separate graph):

```cypher
CREATE (:OU:Base {objectid:'ou',name:'ou'})-[:Contains]->(:User:Base {objectid:'seed',name:'seed',isaclprotected:true})
```

**Additional setup:**

- Set root a. Use outbound R depth 1..1 followed by outbound T depth 0..0, counting duplicate callbacks. The graph is proposed; runtime evidence remains pending.

**Reproduction:**

`OutboundWithDepth(1,1,R).OutboundWithDepth(0,0,T)` on `a→b→c` returns the same match twice

**Expected:**

Report each completed match once; an optional following step must not duplicate completion.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**BloodHound caller validation:** [FetchNodesFromSeeds](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L136-L145) with object-ID seed `seed`, parent expansion, one traversal/expansion worker, and no node limit returned exactly three unique nodes (`seed`, `ou`, `gpo`); the without-GPLink control returned exactly two (`seed`, `ou`). Both calls completed without errors. The suspected callback duplication was not established as incorrect application membership.

**Source:** [traversal/traversal.go:219](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/traversal/traversal.go#L219)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-096"></a>

### DAWGS-096: Acyclic helpers replace the caller's ExpansionFilter; terminal/limit semantics need clarification

**Status:** Source-supported; runtime replay pending.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (a:A {name:"a"})-[:R]->(b:B {name:"b"})-[:R]->(c:C {name:"c"})
```

**Additional setup:**

- Supply an ExpansionFilter rejecting b→c. Define graph-terminal versus filtered-terminal semantics before checking counts/limits. Runtime evidence remains pending.

**Reproduction:**

Both helpers overwrite ExpansionFilter instead of composing it. Runtime validation needs a runnable fixture and a definition of graph terminals versus terminals of a pruned traversal; duplicate counting needs a separate regression.

**Expected:**

Define how caller filters compose and what a terminal means, then preserve the chosen behavior in the acyclic helpers.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [ops/traversal.go:179](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/traversal.go#L179), [ops/traversal.go:226](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/traversal.go#L226), [ops/traversal.go:258](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/traversal.go#L258)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-097"></a>

### DAWGS-097: `BreadthFirst` returns nil with partial results when the caller's context is cancelled

**Status:** Source-supported; runtime replay pending.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

```cypher
CREATE (a:A {name:"a"})-[:R]->(b:B {name:"b"})-[:R]->(c:C {name:"c"})
```

**Additional setup:**

- Synchronize cancellation after one callback and before traversal completes; inspect error and partial results. Runtime evidence and the completeness contract remain pending.

**Reproduction:**

Cancellation is filtered from collected worker errors and the function can return nil. The number of affected operations has not been established with a complete runtime fixture.

**Expected:**

Do not report a complete successful traversal when cancellation prevented completion, subject to the API cancellation contract.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [traversal/traversal.go:331](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/traversal/traversal.go#L331), [traversal/traversal.go:356](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/traversal/traversal.go#L356)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-099"></a>

### DAWGS-099: `ThreadSafeDuplex` deadlocks on concurrent cross-operations

**Status:** Source-supported; runtime replay pending.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- No database is needed. Use two distinct ThreadSafeDuplex bitmaps, e.g. {1} and {2}; synchronize opposing a.Or(b)/b.Or(a) calls after each acquires its own lock. Deadlock replay remains pending.

**Reproduction:**

Opposing `a.Or(b)` / `b.Or(a)` can acquire locks in opposite order and deadlock; completion is schedule-dependent

**Expected:**

Concurrent cross-operations on the two wrappers must not deadlock.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [cardinality/lock.go:61](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cardinality/lock.go#L61)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-100"></a>

### DAWGS-100: `FetchNodeProperties` swallows mid-iteration errors

**Status:** Source-supported; runtime replay pending.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

```go
nodes := graph.NewNodeSet(graph.NewNode(1, nil), graph.NewNode(2, nil))
```

**Additional setup:**

- Use a transaction/result test double returning one valid property row then an iteration error while filling nodes. Runtime evidence remains pending; a successful database query cannot simulate this condition.

**Reproduction:**

a result failing after one row returns nil, leaving nil properties

**Expected:**

Return a result-iteration error to the caller rather than silently accepting partial data.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [ops/ops.go:66](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/ops.go#L66)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-101"></a>

### DAWGS-101: Load decodes JSONL numbers as float64

**Status:** Source-supported; runtime replay pending.

**Component:** Import, export, and test tooling.

**Backend / scope:** JSON import; backend-independent.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

Input document:

```json
{"graph":{"nodes":[{"id":"n1","kinds":["A"],"properties":{"x":9007199254740993}}]}}
```

- An end-to-end retriever check also needs a valid manifest and compressed JSONL fragment. Runtime import evidence remains pending.

**Reproduction:**

`9007199254740993` loads as `9007199254740992`; `opengraph.ParseDocument` does the same; distinct from DAWGS-051's result path

**Expected:**

Preserve the exact integer 9007199254740993 through document/import decoding.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [retriever/compression.go:347](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/compression.go#L347), [opengraph/load.go:62](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/opengraph/load.go#L62)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-102"></a>

### DAWGS-102: Directory-based load and verify follow `../` manifest paths

**Status:** Source-supported; runtime replay pending.

**Component:** Import, export, and test tooling.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Use a directory manifest naming ../outside/nodes.jsonl, with that file present outside the collection root and all normal integrity checks satisfied. A one-node A collection suffices. Complete end-to-end load/verify evidence remains pending.

**Reproduction:**

Directory readers join the manifest path without the tar path sanitizer; `../outside/nodes.jsonl` can escape the collection directory if that file exists and passes integrity checks. End-to-end load/verify replay remains pending

**Expected:**

Keep manifest-based directory reads within the intended collection directory, or explicitly define and validate a broader path contract.

**Observed:**

Not independently reproduced. The source describes the possible behavior in the reproduction note below; a controlled runtime check is still needed.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [retriever/load.go:758](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/load.go#L758), [retriever/manifest.go:70](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/manifest.go#L70)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Regression criterion:**

First capture the complete fixture and actual runtime result, then test the expected behavior stated above.

<a id="dawgs-103"></a>

### DAWGS-103: In-query CALL is silently omitted during compilation

**Status:** Generated SQL checked; procedure execution pending.

**Component:** Query parsing and expression evaluation.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Inspect compilation without installing a procedure. A live semantic check needs an installed mock procedure and a nonempty MATCH input; neither is required for the compilation observation.

**openCypher specification:** A CALL invokes the procedure for each input record and yields the defined result records; it is not a no-op. [CALL invocation and row semantics](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2015-06-24-call-procedures.adoc#L80-L106).

```cypher
MATCH (n) CALL test.doNothing() RETURN n
```

The compiler emits the following SQL, containing only a node match and projection:

```sql
with s0 as (
  select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
  from node n0
)
select s0.n0 as n from s0;
```

No procedure operation survives. The originating upstream case is `Call1`, scenario 3. [Procedure visitor callbacks](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/context.go#L453) have no implementation for this flow. The live adapter cannot install the required mock procedure, so it does not claim this scenario was semantically executed.

CALL is not advertised on the BloodHound support page. This is therefore an **accepted-but-unimplemented, unadvertised feature observation**, not proof that a documented procedure-support promise was broken. A procedure with effects or row filtering must not be silently replaced by a no-op; implementing CALL or explicitly rejecting it would make behavior reviewable.

<a id="dawgs-104"></a>

### DAWGS-104: A graph-building CREATE query expands to about 77.8 MB of SQL

**Status:** Compilation output size measured; performance impact pending.

**Component:** Query builders and translation caching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned.

**Precondition:**

Empty graph.

**Additional setup:**

- Supply the complete multi-CREATE query identified below and measure untruncated SQL. That full input query is not reproduced in this document.

**openCypher specification:** No direct openCypher rule sets generated SQL size, compilation time, or resource budgets. This is a compilation scalability observation, not a semantic specification deviation.

Compiling upstream `Create4`, scenario 2 (Many CREATE clauses) has a **42,200-byte input query** and emits **77,765,308 bytes of SQL** (about 1,843 times the input size). `Create4`, scenario 1 emits 3,429,216 bytes from 28,162 input bytes. These are query steps, not setup fixtures.

These byte lengths were measured on the complete compiler output before truncating it for display. The oversized inputs were resource-limit exclusions in the live run. This is an **observed compilation scalability concern**, not a measured backend slowdown or a separately established semantic failure. Investigation should examine carried projections and repeated frame expansion in [create.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/create.go#L62) before assigning a complete cause.

### Other observations requiring validation

This section contains narrower implementation observations, unsupported syntax forms, and unverified notes that do not yet have the evidence or contract needed for a confirmed defect. Use them to plan focused investigations; they do not expand the confirmed finding count.

These lower-priority items are not a separately verified defect count. The Go API, container, cache, traversal, and formatter-contract observations below have no direct openCypher specification rule; language-related notes carry their specification references inline. The following source-supported candidates have concrete mechanisms but need complete regression cases:

- `ops.FetchRelationshipNodes` requires two fetched nodes and therefore rejects a self-loop, whose endpoints are one node ([ops/ops.go:430](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/ops.go#L430)).
- Triplestore direction-both adjacency can include the origin, and deletion tracking is not consistently applied by its readers ([container/triplestore.go:155](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/triplestore.go#L155)).
- `graphcache.RelationshipsCached` and `adjacencyMapDigraph.NumEdges` return node counts ([graphcache/cache.go:38](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graphcache/cache.go#L38), [container/adjacencymap.go:98](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/adjacencymap.go#L98)).
- `PathSegment.Slice()` allocates Depth entries for Depth+1 ancestors and indexes -1 ([graph/path.go:209](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/path.go#L209)).
- `TSStatelessBFS` expands a segment at the maximum depth and can emit depth max+1 ([container/traversal.go:99](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/container/traversal.go#L99)).
- `AcyclicNodeFilter` checks per-path cycles, whereas its comment promises a global first-visit filter. This is a documentation/contract mismatch, not independently proven traversal corruption ([traversal/traversal.go:466](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/traversal/traversal.go#L466)).
- `Node.Merge` dereferences receiver properties without a nil check; nullable node construction makes this worth a focused regression ([graph/node.go:50](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/node.go#L50)).
- The probe confirmed `RETURN split('', ',') AS v` returns `[]`, rather than the openCypher result `[""]` ([cypher/models/pgsql/translate/function.go:741](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/function.go#L741)). **Specification reference:** [String splitting, printed pp. 188–189](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=192) defines splitting into strings; this paragraph does not explicitly exemplify empty input, so the exact empty-string result has no direct specification example established here.
- The frontend retains backticks in some identifier paths; identifier round trips need a fixture distinguishing variable, label, and property-key handling ([cypher/frontend/literal.go:20](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/frontend/literal.go#L20)). **Specification reference:** [Variable/property-name quoting, printed p. 41](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=45).
- The builder formatter may omit AND between multiple WHERE expressions; a complete reproduction is needed. **Specification scope:** the builder method contract is outside openCypher; any emitted query must retain its intended predicate, but this candidate is not yet verified.

Clean rejections of hexadecimal/octal literals, Unicode escape strings, and parameterized SKIP/LIMIT are feature gaps, not silently incorrect execution. They belong in a support backlog, with the grammar/version stated. The probe confirmed rejection of `0x10`, `\u0041`, and SKIP parameters; `010` was also rejected but is not a valid proof of the grammar's `0o` octal form. No conformance defect count is assigned to these unsupported forms. Missing JSON round-trip support, low-level cyclic traversal behavior, and cancellation contracts are discussed under [API contract decisions](#behaviors-requiring-an-api-contract-decision).

### Unverified observations

These possible problems need a reproduction and an expected-behavior contract before they can be treated as confirmed defects:

- `graph.ScanNextResult` indexes targets without a length check.
- `pathComposite.FromMap` accepts non-path maps, so scanning a node into `*graph.Path` silently yields an empty path.
- `MapStringAnyToJSONB` mutates the caller's map.
- `SetBatchWriteSize` writes an unsynchronized package-level variable.
- Neo4j `result.Error()` is checked before the result is consumed, so errors may be attributed to the next statement.
- `query.SinglePartQuery` drops Limit/Skip/Order given before Return, and a repeated `Filter()` replaces rather than ANDs.
- `DatabaseSwitch` may deadlock on nested transactions under writer preference.
- IPv4 pseudonyms use only 24 digest bits.

## Behaviors requiring an API contract decision

These nine findings describe behavior that may be intentional or depends on an unclear API promise. Decide the ordering, ownership, atomicity, conversion, schema, or input-validation guarantee before assigning an implementation fix. Each finding includes the specific decision needed. Observable behavior is retained as evidence for that discussion; these entries are not counted as confirmed defects.

<a id="dawgs-105"></a>

### DAWGS-105: `AsNumeric` truncates and overflows silently

**Status:** Contract decision needed.

**Component:** Result values, types, and decoding.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

Empty graph.

**Additional setup:**

- No database is needed. Record both values and errors from the two scalar conversions.

**Reproduction:**

Replayed conversions: 1.9 → int 1 and int64(300) → int8 44 without error. Floating-point conversions outside the target range are not a portable exact-value guarantee

**Expected:**

First decide whether AsNumeric promises checked/lossless conversion or ordinary Go conversion behavior.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [graph/mapper.go:32](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/mapper.go#L32)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** Truncation/wrapping is reproducible and follows direct Go numeric casts. No checked/lossless conversion promise was found. Range checks would be a contract/design change, not an established bug fix.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-106"></a>

### DAWGS-106: Batch flush reorders operations

**Status:** Contract decision needed. Excluded from confirmed defects pending an ordering contract.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned: contract decision needed.

**openCypher specification:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.


**Precondition:**

```cypher
CREATE (a:A)-[:R {v:'old'}]->(b:B)
```

**Additional setup:**

- Record both endpoint IDs and relationship e's ID. Put delete and recreate in one batch with no intervening flush. Caller-order preservation is a contract question.

**Reproduction:**

```go
db.BatchOperation(ctx, func(batch graph.Batch) error {
	_ = batch.DeleteRelationship(e.ID)
	return batch.CreateRelationshipByIDs(a.ID, b.ID, R, graph.AsProperties(map[string]any{"v": "new"}))
})
```

**Expected:**

one relationship with `v = "new"`.

**Observed:**

no relationship remains. The create upserts into the old relationship, and the delete then removes it. Deleting a node and then calling `UpdateNodeBy` for the same identity also leaves no node.

**Source evidence:** [tryFlush](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/batch.go#L795) flushes buffers in a fixed order: node update-by, node update, relationship update-by, relationship create, node create, node delete, relationship delete. Caller order is not preserved.

**Contract decision:** caller-order preservation is not established by the Batch interface. The replay confirms the zero-relationship result, but this is an ordering-contract candidate rather than a confirmed defect. Document or decide the ordering guarantee before requiring an implementation change.

**Decision needed:** The delete/create replay leaves no relationship, but Batch does not promise caller-order execution. Decide whether order-sensitive operations are supported or should be rejected.

<a id="dawgs-107"></a>

### DAWGS-107: Preset node IDs do not advance the id sequence

**Status:** Contract decision needed.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** PostgreSQL.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

```go
err := db.BatchOperation(ctx, func(batch graph.Batch) error {
	return batch.CreateNode(graph.NewNode(k+1, graph.NewProperties(), graph.StringKind("A")))
})
```

**Additional setup:**

- In a fresh isolated database, register A, advance the actual node sequence once, and bind its returned value to k before the setup batch. Do not consume another sequence value before the subsequent automatic CreateNode.

**Reproduction:**

batch `CreateNode` with `ID = nextval + 1`; a later `CreateNode` fails with a duplicate key

**Expected:**

This depends on whether callers or the driver are responsible for synchronizing sequences after explicit-ID imports.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/pg/statements.go:18](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/pg/statements.go#L18)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** Explicit IDs can collide with later sequence allocation. Establish whether callers importing fixed IDs must synchronize sequences, or whether the driver owns this obligation. The source alone does not establish that guarantee.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-108"></a>

### DAWGS-108: `UpdateRelationshipBy` overwrites `lastseen` on start and end nodes

**Status:** Contract decision needed.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

Generated-output check: Empty graph.

Live check:

```cypher
CREATE (:A {objectid:'s',lastseen:'keep'}), (:A {objectid:'e',lastseen:'keep'})
```

**Additional setup:**

- Supply an update identifying both endpoints by objectid and carrying lastseen keep. The recorded check inspects generated Cypher; live changes and metadata ownership remain unverified.

**Reproduction:**

any caller-supplied `lastseen` is replaced with the server time (PostgreSQL does not do this)

**Expected:**

This depends on whether the generic update API promises to preserve endpoint metadata such as lastseen.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/cypher.go:193](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/cypher.go#L193)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** Generated Neo4j Cypher explicitly assigns endpoint `lastseen`. Decide whether this is intentional ingestion metadata or an unwanted side effect of the generic API. No live Neo4j execution is claimed.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-109"></a>

### DAWGS-109: Write transactions commit every `writeFlushSize` writes

**Status:** Contract decision needed.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

Empty graph.

**Additional setup:**

- Record writeFlushSize. A proposed small test sets it to 2, creates three A nodes through counted API writes, and fails after the third write; inspect persistence from a new transaction. Atomicity is a contract question; this small-threshold case is not recorded runtime evidence.

**Reproduction:**

after 100,000 writes, a later failure rolls back only the tail (PostgreSQL write transactions are atomic)

**Expected:**

This depends on whether the API promises one atomic transaction or allows documented commit boundaries at write-flush thresholds.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/transaction.go:169](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/transaction.go#L169)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** Neo4j write rotation commits at the configured write-flush threshold. The interface exposes `SetWriteFlushSize`; establish atomicity guarantees and distinguish graph API writes from raw Cypher statements before calling this a transaction defect.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-110"></a>

### DAWGS-110: `AssertSchema` drops indexes it did not create and errors on composite indexes

**Status:** Contract decision needed.

**Component:** Database drivers, graph isolation, and batching.

**Backend / scope:** Neo4j adapter / builder.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

Empty graph.

**Additional setup:**

Create a manually owned schema index before AssertSchema:

```cypher
CREATE INDEX external_name FOR (n:A) ON (n.name)
```

For a separate composite-index check:

```cypher
CREATE INDEX external_composite FOR (n:A) ON (n.name, n.objectid)
```

- Record server version and schema changes. Index ownership/support require a contract decision.

**Reproduction:**

any index not named `<kind>_<field>_index` or `_constraint` is dropped

**Expected:**

First decide which schema definitions AssertSchema owns and which index forms it supports.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [drivers/neo4j/index.go](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/drivers/neo4j/index.go#L222)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** Explicit AssertSchema reconciles the entire schema by design, as the PostgreSQL implementation also does. Dropping definitions absent from the requested schema is not enough to prove a defect. Composite indexes are explicitly rejected; their absence is a support gap. Implicit reconciliation during a read remains DAWGS-063.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-111"></a>

### DAWGS-111: `Kinds.Remove` mutates the caller's backing array

**Status:** Contract decision needed.

**Component:** Graph algorithms, caches, and utilities.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

```go
kinds := graph.Kinds{graph.StringKind("A"), graph.StringKind("B"), graph.StringKind("C")}
a := graph.NewNode(1, nil, kinds...)
b := graph.NewNode(2, nil, kinds...)
```

**Additional setup:**

- These are in-memory nodes; no database is needed. Preserve the shared kinds backing array, delete A from only a, then inspect b.

**Reproduction:**

two nodes built from one kinds slice: deleting a kind from one turns the other into `[B C C]`

**Expected:**

First establish whether callers must copy shared Kinds slices or whether Remove promises to avoid changing shared backing storage.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [graph/kind.go:76](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/graph/kind.go#L76)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** The `[B C C]` mutation is reproducible, but Kinds.Remove uses normal in-place Go slice removal and Kinds.Copy exists. A requirement to copy shared caller slices has not been established.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-112"></a>

### DAWGS-112: Load commits per batch; a failed load blocks its own retry

**Status:** Contract decision needed.

**Component:** Import, export, and test tooling.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

Empty graph.

**Additional setup:**

- Prepare a valid three-node A collection with no edges and batch size 2. Inject a database failure after one committed batch, then retry on the same target. Exact failure, commit boundaries, and recovery contract remain pending.

**Reproduction:**

If a database-side failure follows committed batches, committed rows remain and a retry encounters the nonempty-target check. A complete failure/retry fixture is needed to establish the affected batch and row counts.

**Expected:**

First decide the load atomicity/recovery promise, including what a caller must do before retrying a partial import.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [retriever/load.go:189](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/load.go#L189), [retriever/load.go:622](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/retriever/load.go#L622)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** The loader writes separate batches and refuses nonempty targets. Failure can leave partial state, but atomic or resumable load is not promised. Document cleanup/retry requirements or adopt a recovery guarantee before classifying it as corruption.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

<a id="dawgs-113"></a>

### DAWGS-113: `opengraph.ParseDocument` accepts misspelled top-level keys and trailing data

**Status:** Contract decision needed.

**Component:** Import, export, and test tooling.

**Backend / scope:** Go library / utility.

**Priority:** Not assigned: contract decision needed.

**Precondition:**

Empty graph.

**Additional setup:**

No database is needed for ParseDocument. Input:

```json
{"grpah":{"nodes":[{"id":"x"}]}} garbage
```

- Test trailing data separately with a valid graph key. Strictness remains a contract decision.

**Reproduction:**

`{"grpah": …}` parses as an empty graph and Load reports success

**Expected:**

First decide whether the document parser must reject unknown fields and trailing data; empty graphs are allowed.

**Observed:**

The recorded behavior is described in the reproduction note. Whether it is a defect depends on the contract decision below.

**Likely cause:**

Not isolated for this entry; the source links identify where to investigate.

**Source:** [opengraph/load.go:59-62](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/opengraph/load.go#L59)

**Specification reference:** Outside openCypher: DAWGS API, storage, algorithm, or utility behavior.

**Decision needed:** Unknown JSON keys and one decoded value followed by trailing bytes are accepted. Empty graphs are explicitly valid. A strict whole-document/schema requirement must be stated before treating permissive decoding as a correctness defect. Consider unknown-field and trailing-data rejection as a deliberate validation improvement.

**Regression criterion:**

After the contract is chosen, document it and test the recorded case against that guarantee.

### Additional contract questions

These behaviors need explicit API guarantees before they can be classified as defects:

- A JSON marshaler does not imply an unmarshaling/round-trip contract for `graph.Node`; absence of `UnmarshalJSON` alone is not a defect.
- TSBFS/TSDFS accept caller-supplied descent filters. Unlimited traversal of a cyclic graph without a cycle filter does not by itself prove a defect; decide whether these low-level APIs promise trails, simple paths, or arbitrary walks.
- Component/reachability construction returns no error slot. Partial results on cancellation need an explicit completeness/cancellation contract and a focused reproduction; they are not independently confirmed here.

## Downstream impact and validation

The clearest matches are unbounded traversals in built-in/saved searches (**[DAWGS-019](#dawgs-019)**), unanchored regex predicates (**[DAWGS-007](#dawgs-007)**), and a direction-sensitive Enterprise cleanup builder (**[DAWGS-059](#dawgs-059)**, Neo4j only). The tables below identify the actual queries and the conditions required for an effect. A matching query shape is exposure evidence, not proof that a customer's graph contains the triggering data.

### Application caller validation

Seventeen focused checks called the BloodHound functions from the referenced source, linked to the DAWGS revision in this report. PostgreSQL checks used an isolated graph with the BloodHound schema. Query preparation used the normal Explore fitness threshold with complexity checks enabled. CanRDP checks covered both an in-memory membership graph and the normal database-backed cache factory with 106 membership nodes and 56 edges; worker errors were injected only for the migration checks. These are direct application-function checks, not a complete HTTP/authentication or ingest/analysis run, and are separate from the query-engine and upstream TCK results.

| Finding | Application function / gate | Observed result | Disposition |
| --- | --- | --- | --- |
| [DAWGS-007](#dawgs-007) | [PrepareCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L580) → [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611); [built-in AGT Global Administrators](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L394-L398) | Source predicate returned two edges; full-match regex control returned one. | P1 on PostgreSQL. |
| [DAWGS-019](#dawgs-019) | [PrepareCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L580) → [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611); [built-in Domain Admins](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L42-L46) | Unbounded form returned no nodes/edges; explicit 16-hop bound returned 17 nodes and 16 edges. | P1 on PostgreSQL. |
| [DAWGS-073](#dawgs-073) | [FetchCanRDPEntityBitmapForComputer](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L583-L619); [normal cache factory](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/algo/reach.go#L295-L310); URA enforced / Citrix disabled | Fresh cache returned eligible user [5]; warmed cache returned []. | P1 analysis correctness. |
| [DAWGS-098](#dawgs-098) | [PrepareCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L580) → [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611); [literal-budget check](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/ops/ops.go#L232-L240) | A 65,536-byte string passed a 16,384-byte result budget without error. | P2; default 2 GiB exhaustion and peak memory not measured. |
| [DAWGS-033](#dawgs-033) | [PrepareCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L514-L580) → [RawCypherQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L584-L611); [computer query](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20incoming%20and%20local%20paths%20for%20a%20specific%20computer.yml#L7-L12) | Query was accepted and omitted the qualifying self-loop; the Neo4j fixture returned it. | P2 for library queries through Explore; no built-in match established. |
| [DAWGS-072](#dawgs-072) | [Version_513_Migration](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L443-L460); [six workers](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/post/operation.go#L28-L33) | Max ID 140000 plus six injected worker-query failures waited for external cancellation; max-ID-50000 control returned three worker errors promptly. | P2 for affected legacy upgrades. |
| [DAWGS-059](#dawgs-059) | [Enterprise backend default](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/cmd/bhe/main.go#L28-L36); [reconciliation gate](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L31-L39) | Default is PostgreSQL; the affected Neo4j function runs only with reconciliation enabled. | Conditional; no default-Enterprise PostgreSQL impact from this rewrite. |
| [DAWGS-079](#dawgs-079) | [GetEntityResults](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1164-L1176); [FetchDCSyncers](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L812-L825); [bounds guard](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1096-L1128) | Limit 10 on one result returned one row; skip 2 returned invalid-skip error. Direct delegate still panicked. | Not prioritized for the guarded application route. |
| [DAWGS-095](#dawgs-095) | [FetchNodesFromSeeds](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L136-L145); [parent expansion and collector](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L474-L545) | Two unique nodes without GPLink; three with GPLink; no errors. | Not prioritized as incorrect application selection. |

### Exact-query discrepancies reproduced on PostgreSQL

Each row describes an independent fixture. Expected counts follow the source query's Cypher semantics, including full-string regex matching; they do not assume that the query author's apparent intent overrides its predicate. The observed count was the same with the optimizer enabled and disabled.

| Source query | Finding | Fixture and expected behavior | Observed behavior |
| --- | --- | --- | --- |
| [All Domain Admins](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Domain%20Admins.yml#L7) | [DAWGS-019](#dawgs-019) | A chain of 16 MemberOf relationships, with one User at the start, 15 intermediate Groups, and a final Group whose objectid ends in -512. Exactly one qualifying user-to-group path. | 0 rows instead of 1. The same query is in both product catalogues. |
| [All Global Administrators](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Global%20Administrators.yml#L7) | [DAWGS-007](#dawgs-007) | One AZBase has AZHasRole edges to roles named `Global Administrator Good` and `Not Global Administrator X`. The predicate `(?i)Global Administrator.*` full-matches only the first. | 2 rows instead of 1. |
| [All privileged Azure Service Principals](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20privileged%20Azure%20Service%20Principals.yml#L8) | [DAWGS-007](#dawgs-007) | One AZServicePrincipal has AZHasRole edges to roles whose displaynames are `Application Administrator` and `Fake Application Administrator Foo`. Only the first full-matches a role-name alternative. | 2 rows instead of 1; an embedded role name is sufficient. |
| [Accounts related to AAD Entra Connect](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Accounts%20related%20to%20AAD%20Entra%20Connect.yml#L9) | [DAWGS-007](#dawgs-007) | One User has name `MSOL_test` and userprincipalname `person@example.test`. The first alternative `^MSOL_` has no trailing wildcard and does not full-match this name; the other alternatives do not match. | 1 row instead of 0. Add the intended trailing wildcard if prefix matching is desired; do not rely on PostgreSQL substring behavior. |
| [Devices with unsupported operating systems](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Devices%20with%20unsupported%20operating%20systems.yml#L7) | [DAWGS-007](#dawgs-007) | One AZDevice has operatingsystem `WINDOWS` and operatingsystemversion `PREFIX10.0.19044`. The version pattern cannot full-match from the beginning. | 1 row instead of 0; the version can match after the prefix. |
| [Non-default delegation on MicrosoftDNS container](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Non-default%20delegation%20on%20MicrosoftDNS%20container.yml#L7) | [DAWGS-007](#dawgs-007) | A Base named `NORMAL`, objectid `X-512`, has an Owns relationship with isacl=true to a Container whose distinguishedname is `CN=MICROSOFTDNS,CN=SYSTEM,DC=D`. The negative suffix-only regex `-(512\|544\|519\|9)$` does not full-match `X-512`, so Cypher retains the path. | 0 rows instead of 1. Substring matching changes an exclusion predicate; review the intended query and translator semantics together. |
[DAWGS-007](#dawgs-007) catalogue correspondence needs the exact variant: [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L394) and [Enterprise test](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L228) use the role-name regex. [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L394) instead follows AZGlobalAdmin edges to AZTenant and does not use that regex; its [DAWGS-019](#dawgs-019) shape remains conditional as listed below. Devices with unsupported operating systems has the tested regex in [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L492), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L492), and [Enterprise test](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L267). The other five [DAWGS-007](#dawgs-007) query names identified here were not found as corresponding active built-in entries in these product catalogues.

The domain-admin chain is a concrete false negative. The regex cases demonstrate both additional rows and omitted rows. A terminal LIMIT limits returned rows, not traversal depth, and does not repair [DAWGS-019](#dawgs-019).

### Focused validation of built-in and Enterprise catalogue queries

Nineteen focused query cases were executed on isolated fixtures using Neo4j 4.4.42 and DAWGS/PostgreSQL 16.15 with each optimizer setting: 57 query executions. Fourteen cases showed the predicted discrepancy; five controls returned their expected counts. All executions completed without query errors. These counts are separate from the sample-graph replay and upstream TCK results. Source text was checked against all 236 local Query Library YAML files and against the seven product/test-catalogue variants executed below. The computer query substitutes its documented HOSTNAME placeholder.

| Exact built-in / catalogue source | Finding | Neo4j rows | PostgreSQL rows, either optimizer mode | Trigger |
| --- | --- | ---: | ---: | --- |
| [All Domain Admins — AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L42-L46); [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L42-L46); [Enterprise test catalogue](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L26-L28) | [DAWGS-019](#dawgs-019) | 1 | 0 | One user reaches the Domain Admins group only after 16 MemberOf relationships. |
| [All Global Administrators — AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L394-L398); [Enterprise test catalogue](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L228-L229) | [DAWGS-007](#dawgs-007) | 1 | 2 | A role name containing, but not beginning with, Global Administrator is also matched. The AGI variant uses a different predicate and is excluded from this regex claim. |
| [Devices with unsupported operating systems — AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L492-L496); [Enterprise test catalogue](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L267-L268) | [DAWGS-007](#dawgs-007) | 0 | 1 | Deliberately nonstandard version PREFIX10.0.19044; this tests conditional matching, not ordinary version-data exposure. |

The other cases replayed the eleven library fixtures documented here and the hostname-substituted computer self-loop query. The five controls were Computers with non-default Primary Group membership, Map domain trusts and forest roots, Domain Admins logons to non-Domain Controllers, Computers with local administrator permissions over other computers, and Non-Tier Zero principals with access to enabled gMSA passwords. Their passing minimal fixtures do not disprove a different conditional trigger; see Appendix C for the exact attribution limits. Full BloodHound HTTP/UI and Enterprise reconciliation workflows were not executed.

### Enterprise Neo4j trust cleanup loses type/direction correlation

**Consumer:** [reconcileTrustEdges attack-edge lookup](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L134). **Finding:** [DAWGS-059](#dawgs-059). **Evidence:** the exact builder structure was executed on Neo4j 4.4.42 against the four-relationship fixture in DAWGS-059. It selected four IDs instead of two. A deletion-by-ID simulation removed those four inside a transaction that was rolled back. The SQL/Cypher illustration below uses domain IDs 10 and 20; the live probe bound the fixture’s actual IDs. The full Enterprise reconciliation function was not executed.

The intended lookup selects `AbuseTGTDelegation` from the expired trust's start to end **or** `SpoofSIDHistory` from its end to start. The emitted shape is:

```cypher
MATCH (s)-[r:AbuseTGTDelegation|SpoofSIDHistory]->(e)
WHERE s:Domain AND e:Domain
  AND ((id(s) = $p0 AND id(e) = $p1)
    OR (id(s) = $p2 AND id(e) = $p3))
RETURN id(r)
```

With p0/p3=10 and p1/p2=20, either type can now match either direction. For example, SpoofSIDHistory from 10 to 20 is selected although neither original branch allows it. The consumer appends these IDs to its deletion list and later deletes them in a batch ([selection and deletion](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/ad/post.go#L154)). The live selection and rollback-only deletion simulation confirm that two extra attack edges can be removed under this condition. PostgreSQL does not use this relationship-kind hoist. Enterprise defaults to PostgreSQL and gates this function on reconciliationEnabled. The deployment must actually use Neo4j with reconciliation enabled, and a relationship of the disallowed type/direction must exist for the difference to be observable.

### API-backed query and analysis consumers

These are affected-library call paths, not additional literal Cypher failures. The application-function checks above establish the stated caller results; other analysis consumers retain their per-row qualifications.

| Consumer | Finding | Possible effect and qualification |
| --- | --- | --- |
| [FetchAdminGroupIds / Owns processing](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/owns.go#L301); [NTLM administrative-principal expansion](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/ntlm.go#L532); [CanRDP local-group expansion](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/post.go#L853); [ADCS group expansion](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/esc_shared.go#L411) | [DAWGS-073](#dawgs-073) | These consumers use the shared ReachabilityCache. A warmed cache on a branching/converging membership graph can return incomplete members. This can change administrative membership/exclusion sets and derived attack relationships; the precise downstream edge difference needs a domain-specific fixture. The actual CanRDP eligibility function returned [] instead of [5] with a warmed cache. Full edge persistence was not exercised; the separate Owns/NTLM call paths remain source mappings. |
| [fetchADParentNodes Contains → optional GPLink traversal](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L393) | [DAWGS-095](#dawgs-095) (source-supported candidate) | The pattern has an optional next step, but the actual parent selector returned the expected unique nodes with and without GPLink. Its collector deduplicates nodes ([collector](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/agt.go#L540-L544)), so this is not proof of duplicated final group membership or an inflated displayed count. |
| [FetchDCSyncers](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L812); [FetchForeignGPOControllers](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L858); [FetchForeignAdmins](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L965); [CreateForeignEntityMembershipListDelegate](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/go/analysis/ad/queries.go#L1062) | [DAWGS-079](#dawgs-079) | All call SortAndSliceNodeSet. Direct delegate calls with skip/limit outside the result size can panic. Some API paths fetch with skip=limit=0 and page afterwards, so this does not establish that every corresponding HTTP request panics ([fetch-before-paging path](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L1096)). |
| [MetaTreeGenerator.Write graph-scoped transaction](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/analysis/metatree/generator.go#L364) | [DAWGS-062](#dawgs-062) | The transaction selects the meta graph, but subsequent reads/updates must not assume graph_id isolation. The consumer already states that reads are not subgraph-aware and adds Meta kind/ID filters. This is an existing source acknowledgement and workaround, not proof that its guarded lookup currently returns wrong rows. It also means [DAWGS-062](#dawgs-062) should not be described as unknown to all consumer developers. Passing the full graph definition here is not the name-only [DAWGS-063](#dawgs-063) trigger. |
| [RawCypherQuery → FetchByQuery](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L592) | [DAWGS-098](#dawgs-098) (literal-budget failure reproduced) | User/saved queries returning paths or large values reach the shallow memory accounting identified in [DAWGS-098](#dawgs-098). Complexity and response limits are additional controls, not proof that nested graph-result memory is accurately charged. The actual service returned a 64 KiB literal under a 16 KiB result budget. Peak application memory and path-payload accounting were not measured. |
| [Version_513_Migration → ParallelFetchNodes](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/migrations/manifest.go#L458) | [DAWGS-072](#dawgs-072) | The six-worker Version_513_Migration waited for external cancellation with max ID 140000 and injected worker-query failures. The smaller control returned errors promptly; this is an affected legacy-upgrade/error condition, not normal current-version migration. |

### Validation on sample-derived AD and Entra graphs

Sample-graph validation projected the public AD and companion Entra samples into 14,378 nodes and 30,415 relationships, then added 62 nodes, 61 relationships, and a Tier Zero label on an existing Domain Admins group. It executed 63 library queries against the baseline and extended graphs, plus two scoped diagnostic variants on the extended graph, using Neo4j 4.4.42 and both DAWGS PostgreSQL optimizer settings. BloodHound's attack-path relationship shortcuts were expanded using the referenced consumer source on both backends. This is query-engine validation on a core-relationship projection; the complete BloodHound ingestion, analysis, HTTP, and UI workflows were not executed.

| Query / check | Data | Neo4j rows | PostgreSQL rows, either optimizer setting | Finding |
| --- | --- | ---: | ---: | --- |
| All privileged Azure Service Principals | Sample projection before extension | 835 | 838 | DAWGS-007: three extra paths |
| Non-default delegation on MicrosoftDNS container | Sample projection before extension | 18 | 0 | DAWGS-007: omitted paths |
| All Domain Admins | Extended: new user reaches the existing sample group only after 16 MemberOf hops | 21 | 20 | DAWGS-019: missing user-to-group path |
| Shortest Paths from Azure Users to Azure Keyvaults | Extended: 16-hop AZMemberOf/AZGetSecrets path | 1 | 0 | DAWGS-019: missing shortest path |
| All incoming and local paths for a specific computer | Extended: AdminTo self-loop; HOSTNAME placeholder substituted | 1 | 0 | DAWGS-033: omitted self-loop |
| Uncommon permission — deep-leaf diagnostic | Original predicates plus one leaf-object filter | 1 | 0 | DAWGS-019: depth witness, scoped to avoid an unordered LIMIT hiding it |
| Uncommon permission — Exchange diagnostic | Original predicates plus one container-object filter | 1 | 0 | DAWGS-007: regex witness, scoped to avoid an unordered LIMIT hiding it |

The replay recorded 384 executions and asserted 19 targeted reproductions. Ten executions hit time or row budgets, leaving six query/phase comparisons inconclusive; partial rows were not treated as correctness results. Other path differences were not all attributed, and an unordered LIMIT or a different choice among equally short paths is insufficient evidence of a defect. The per-finding minimal reproductions remain the implementation regression targets; the sample replay adds downstream impact evidence.

The domain-trust self-loop and role-GUID-substring probes are malformed-data stress checks. They confirm the conditional mechanisms in DAWGS-033 and DAWGS-007 without establishing exposure under valid domain-trust or role-GUID invariants. The device-version probe also deliberately uses a nonstandard version string. These are separate from the ordinary sample-data discrepancies above. The full conditional query inventory and its attribution limits are in [Appendix C](#appendix-c-full-downstream-query-catalogue).

## Appendix A: Shared fixture for DAWGS-019 and DAWGS-020

This graph was loaded through the graph API. The equivalent single CREATE statement below was verified to create exactly 17 nodes and 16 relationships and to produce the successful explicit-bound control result. The start and end nodes have no alternative connecting path.

```cypher
CREATE
  (n0:AuditStart {position: 0})-[:AuditR]->
  (n1 {position: 1})-[:AuditR]->
  (n2 {position: 2})-[:AuditR]->
  (n3 {position: 3})-[:AuditR]->
  (n4 {position: 4})-[:AuditR]->
  (n5 {position: 5})-[:AuditR]->
  (n6 {position: 6})-[:AuditR]->
  (n7 {position: 7})-[:AuditR]->
  (n8 {position: 8})-[:AuditR]->
  (n9 {position: 9})-[:AuditR]->
  (n10 {position: 10})-[:AuditR]->
  (n11 {position: 11})-[:AuditR]->
  (n12 {position: 12})-[:AuditR]->
  (n13 {position: 13})-[:AuditR]->
  (n14 {position: 14})-[:AuditR]->
  (n15 {position: 15})-[:AuditR]->
  (n16:AuditEnd {position: 16})
```

After committing the fixture, these independent controls return `17`, `16`, and `16` respectively:

```cypher
MATCH (n) RETURN count(n) AS nodes
```

```cypher
MATCH ()-[r]->() RETURN count(r) AS relationships
```

```cypher
MATCH (a:AuditStart)-[:AuditR*16..16]->(b:AuditEnd)
RETURN b.position AS position
```

The table gives the row counts for this depth fixture. Each query is independent. The issue column identifies the failing reproduction or the passing control associated with that finding. The explicit-bound controls establish that the endpoint is present and reachable; none of the recorded queries returned an error.

| Issue and role | Query | Expected rows | Baseline rows | Optimized rows |
| --- | --- | ---: | ---: | ---: |
| [DAWGS-019](#dawgs-019): failing unbounded expansion | `MATCH (a:AuditStart)-[:AuditR*]->(b:AuditEnd) RETURN b` | 1 | 0 | 0 |
| [DAWGS-020](#dawgs-020): failing fixed-length expansion | `MATCH (a:AuditStart)-[:AuditR*16]->(b:AuditEnd) RETURN b` | 1 | 0 | 0 |
| [DAWGS-020](#dawgs-020): passing exact-range control | `MATCH (a:AuditStart)-[:AuditR*16..16]->(b:AuditEnd) RETURN b` | 1 | 1 | 1 |
| [DAWGS-019](#dawgs-019): passing bounded-expansion control | `MATCH (a:AuditStart)-[:AuditR*..16]->(b:AuditEnd) RETURN b` | 1 | 1 | 1 |
| [DAWGS-019](#dawgs-019): failing unbounded shortest path | `MATCH p=shortestPath((a:AuditStart)-[:AuditR*]->(b:AuditEnd)) RETURN p` | 1 | 0 | 0 |
| [DAWGS-019](#dawgs-019): passing bounded shortest-path control | `MATCH p=shortestPath((a:AuditStart)-[:AuditR*..16]->(b:AuditEnd)) RETURN p` | 1 | 1 | 1 |
| [DAWGS-019](#dawgs-019): failing unbounded all-shortest paths | `MATCH p=allShortestPaths((a:AuditStart)-[:AuditR*]->(b:AuditEnd)) RETURN p` | 1 | 0 | 0 |
| [DAWGS-019](#dawgs-019): passing bounded all-shortest-paths control | `MATCH p=allShortestPaths((a:AuditStart)-[:AuditR*..16]->(b:AuditEnd)) RETURN p` | 1 | 1 | 1 |

## Appendix B: Shared fixture for DAWGS-025, DAWGS-028, and DAWGS-030

Load and commit this single statement in an otherwise empty graph. It creates six nodes and ten relationships. `a2` is the only node labeled both A and B; `c2` is labeled both C and A; `b2` has no `v` property; and there are no self-loops.

```cypher
CREATE
  (a1:A {name: "a1", v: 1}), (a2:A:B {name: "a2", v: 2}),
  (b1:B {name: "b1", v: 2}), (b2:B {name: "b2"}),
  (c1:C {name: "c1", v: 3}), (c2:C:A {name: "c2", v: 3}),
  (a1)-[:R]->(b1), (a1)-[:R]->(b2), (a1)-[:T]->(b1),
  (b1)-[:R]->(c1), (b2)-[:R]->(c1), (a1)-[:R]->(c1),
  (c1)-[:R]->(c2), (c2)-[:R]->(a1),
  (a2)-[:R]->(b1), (b1)-[:T]->(a2)
```

Recorded results for this fixture:

| Issue | Query | Expected | Optimized | Unoptimized |
| --- | --- | --- | --- | --- |
| [DAWGS-025](#dawgs-025) | `MATCH (a {name: "a1"}), (b {name: "c1"}) MATCH (a)-[:R*1..1]->(b) RETURN count(*) AS c` | 1 | 6 | 1 |
| [DAWGS-028](#dawgs-028) | `MATCH (s)-[:R*1..2]->(g)-[:T]->(d) WHERE s.v = 99 OR g.v = 99 RETURN count(*) AS c` | 0 | 5 | 5 |
| [DAWGS-030](#dawgs-030) | `MATCH (s:A) MATCH (s:B)-[:R*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100` | `a2 \| 3` | `a1 \| 9`, `c2 \| 7`, `a2 \| 3` | `a2 \| 3` |

## Appendix C: Full downstream query catalogue

This appendix lists query-to-finding mappings and the conditions required for an effect. Query shape establishes possible exposure; the stated preconditions must exist before it demonstrates an effect. The catalogue is supporting prioritization material, not a count of confirmed application failures.

### Consumer revisions and analysis scope

| Repository | Source revision | What was inspected |
| --- | --- | --- |
| BloodHound | `e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9` | Both CommonSearches catalogues (96 entries each), UI-generated/help queries, graph-query preparation, shortest-path builders, and analysis consumers |
| bloodhound-enterprise | `ecf107871592a31aed70422f8391e36518b8fb64` | Production graph builders and consumers, Hunter Cypher policy, and 89 Cypher-valued assignments in the HoundStrike test catalogue; its `bhce` submodule is not populated in this checkout |
| BloodHoundQueryLibrary | `b1fee8f5715f8fb3358e187ca3f1bddbfd125f93` | All 236 query YAML files, including entries not marked prebuilt |

BloodHound declares DAWGS v0.9.2 ([dependency](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/go.mod#L66)); Enterprise declares v0.9.3 ([dependency](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/go.mod#L44)). DAWGS v0.9.3 resolves to `c107686a5e8685693a7df660d2ee968c758c17af`; v0.9.2 resolves to `d0640cfb421e2b9f4fc249de733c3d6e52f1a57f`. The implementations relevant to the principal mappings here—expansion, regex translation, reachability cache, pagination helper, traversal driver, and Neo4j expression rewriting—are unchanged between those tags. This establishes source applicability; it is not a complete deployed-release or HTTP/API compatibility test. Enterprise links require repository access.

The structural pass parsed the queries rather than treating every textual WITH, AND, or arrow as a defect trigger. All 236 library queries compiled through the tested PostgreSQL translator with an in-memory kind registry. Of 192 product CommonSearches entries, 190 compiled; two entries are comment-only instructions to enable an expensive query. The separate intentional UncommonSearches parser-error example was excluded. Of 89 Enterprise Python catalogue assignments, 85 compiled; four have their own syntax/name defects described below. TypeScript constants were resolved, and attack-path type sets were represented by BloodHound's AD_ATTACK_PATHS/AZ_ATTACK_PATHS shortcuts for structural analysis. Compilation success is not result correctness.

Eleven complete library queries were additionally executed on isolated PostgreSQL fixtures with both optimizer settings: six exhibited the predicted discrepancy and five controls returned the expected row counts. These 22 executions are focused downstream checks, separate from upstream TCK results. Kinds were registered before execution so that an unknown-kind error could not hide the target defect. No customer data, application instance, or live Neo4j server was used.

### Catalogue queries with an unbounded traversal

**Finding: [DAWGS-019](#dawgs-019).** These 56 library queries contain at least one `*`, `*..`, or `*k..` range without an upper bound. On PostgreSQL, an otherwise eligible path requiring more than 15 hops can be omitted. Queries returning counts/rankings can therefore undercount or change rank, not merely draw a smaller graph. This is conditional on eligible data; the query shape alone does not show that such a path exists in every AD/Azure relationship model.

The BloodHound column identifies all 23 unbounded query entries in each CommonSearches variant (46 entries). The Enterprise column identifies corresponding test-query families, not separate production endpoints; filters and names may differ. “Blocked” marks a catalogue typo that prevents reaching the traversal defect until corrected. A dash means no corresponding unbounded entry was identified in that catalogue, not that the product cannot run an imported query.

| Library query (pinned source) | BloodHound CommonSearches | Enterprise HoundStrike test catalogue |
| --- | --- | --- |
| [AD Groups nested more than 3 levels](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/AD%20Groups%20nested%20more%20than%203%20levels.yml#L7) | — | — |
| [Accounts with smart card required in domains where smart account passwords do not expire](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Accounts%20with%20smart%20card%20required%20in%20domains%20where%20smart%20account%20passwords%20do%20not%20expire.yml#L7) | [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L323) | [Test: ACCOUNTS_WITH_SMART_CARD_REQ_IN_DOMAINS_WHERE_SMART_ACCOUNT_PASSWORDS_DO_NOT_EXPIRE](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L165) |
| [AdminSDHolder protected Accounts and Groups](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/AdminSDHolder%20protected%20Accounts%20and%20Groups.yml#L7) | — | — |
| [All Domain Admins](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Domain%20Admins.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L42), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L42) | [Test: ALL_DOMAIN_ADMINS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L26) |
| [All GPOs applied to a specific computer](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20GPOs%20applied%20to%20a%20specific%20computer.yml#L7) | — | — |
| [All Global Administrators](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Global%20Administrators.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L394), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L394) | [Test: ALL_GLOBAL_ADMINISTRATORS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L228) |
| [All Schema Admins](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20Schema%20Admins.yml#L7) | — | — |
| [All members of Protected Users](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20members%20of%20Protected%20Users.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L579), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L579) | [Test: COMPUTERS_WITH_MEMBERSHIP_IN_PROTECTED_USERS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L212) |
| [Azure groups nested more than 3 levels](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Azure%20groups%20nested%20more%20than%203%20levels.yml#L7) | — | — |
| [Circular AD group memberships](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Circular%20AD%20group%20memberships.yml#L7) | — | — |
| [Circular AZ group memberships](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Circular%20AZ%20group%20memberships.yml#L7) | — | — |
| [Computers with local administrator permissions over other computers](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Computers%20with%20local%20administrator%20permissions%20over%20other%20computers.yml#L7) | — | — |
| [Computers with membership in default privileged groups](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Computers%20with%20membership%20in%20default%20privileged%20groups.yml#L7) | — | — |
| [Domain Admins logons to non-Domain Controllers](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Domain%20Admins%20logons%20to%20non-Domain%20Controllers.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L144), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L144) | [Test: DOMAIN_ADMINS_LOGONS_TO_NON_DOMAIN_CONTROLLERS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L45) |
| [Domains with more than 50 Tier Zero accounts](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Domains%20with%20more%20than%2050%20Tier%20Zero%20accounts.yml#L7) | — | — |
| [Domains with smart card accounts where smart account passwords do not expire](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Domains%20with%20smart%20card%20accounts%20where%20smart%20account%20passwords%20do%20not%20expire.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L323) | — |
| [Kerberoastable users with most admin privileges](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Kerberoastable%20users%20with%20most%20admin%20privileges.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L165), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L165) | [Test: KERBERROASTABLE_USERS_WITH_MOST_ADMIN_PRIVILEGES](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L55) |
| [Kerberos-enabled service account member of built-in Admins groups](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Kerberos-enabled%20service%20account%20member%20of%20built-in%20Admins%20groups.yml#L7) | — | — |
| [Location of AdminSDHolder Protected objects](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Location%20of%20AdminSDHolder%20Protected%20objects.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L62), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L62) | [Test: LOCATION_OF_ADMIN_SD_HOLDER_PROTECTED_OBJECTS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L30) |
| [Locations of Owned objects (AD)](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Locations%20of%20Owned%20objects%20-%20AD.yml#L7) | — | — |
| [Locations of Owned objects (AZ)](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Locations%20of%20Owned%20objects%20-%20AZ.yml#L7) | — | — |
| [Locations of Tier Zero / High Value objects](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Locations%20of%20Tier%20Zero%20High%20Value%20objects.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L52), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L52) | [Test: LOCATIONS_OF_TIER_ZERO_HIGH_VALUE_OBJECTS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L28) |
| [Map Azure Management structure](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Map%20Azure%20Management%20structure.yml#L7) | — | — |
| [Map OU structure](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Map%20OU%20structure.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L57), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L57) | [Test: MAP_OU_STRUCTURE](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L29) |
| [Map domain trusts and forest roots](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Map%20domain%20trusts%20and%20forest%20roots.yml#L7) | — | — |
| [Members of Allowed RODC Password Replication Group](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Members%20of%20Allowed%20RODC%20Password%20Replication%20Group.yml#L7) | — | — |
| [Nested groups within Tier Zero / High Value](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Nested%20groups%20within%20Tier%20Zero%20High%20Value.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L348), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L348) | [Test: NESTED_GROUPS_WITHIN_TIER_ZERO_HIGH_VALUE](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L182) |
| [Non-Tier Zero principals with access to enabled gMSA passwords](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Non-Tier%20Zero%20principals%20with%20access%20to%20enabled%20gMSA%20passwords.yml#L7) | — | — |
| [Overprivileged Microsoft Entra Connect accounts](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Overprivileged%20Microsoft%20Entra%20Connect%20accounts.yml#L9) | — | — |
| [Owners of Azure Subscriptions](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Owners%20of%20Azure%20Subscriptions.yml#L8) | — | — |
| [PKI hierarchy](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/PKI%20hierarchy.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L222), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L222) | [Test: PKI_HIERARCHY](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L80) |
| [Paths from Domain Users to Tier Zero / High Value targets](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Paths%20from%20Domain%20Users%20to%20Tier%20Zero%20High%20Value%20targets.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L124), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L124) | [Blocked: PATHS_FROM_DOMAIN_USERS_TO_TIER_ZERO_HIGH_VALUE_TARGETS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L41) |
| [Public Key Services container](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Public%20Key%20Services%20container.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L227), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L227) | [Test: PUBLIC_KEY_SERVICES_CONTAINER](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L84) |
| [Shortest Paths from Azure Users to Azure Keyvaults](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20Paths%20from%20Azure%20Users%20to%20Azure%20Keyvaults.yml#L8) | — | — |
| [Shortest Paths from Azure Users to Azure VMs](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20Paths%20from%20Azure%20Users%20to%20Azure%20VMs.yml#L8) | — | — |
| [Shortest Paths from Owned Azure Users to Azure Keyvaults](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20Paths%20from%20Owned%20Azure%20Users%20to%20Azure%20Keyvaults.yml#L8) | — | — |
| [Shortest Paths from Owned Azure Users to Azure VMs](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20Paths%20from%20Owned%20Azure%20Users%20to%20Azure%20VMs.yml#L8) | — | — |
| [Shortest paths from Azure Applications to Tier Zero / High Value targets](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20Azure%20Applications%20to%20Tier%20Zero%20High%20Value%20targets.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L440), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L440) | [Test: SHORTEST_PATHS_FROM_AZURE_APPS_TO_TIER_ZERO_HIGH_VALUE_TARGETS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L247) |
| [Shortest paths from Domain Users to Tier Zero / High Value targets](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20Domain%20Users%20to%20Tier%20Zero%20High%20Value%20targets.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L196), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L196) | [Test: SHORTEST_PATH_FROM_DOMAIN_USERS_TO_TIER_ZERO_HIGH_VALUE_TARGETS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L67) |
| [Shortest paths from Entra Users to Tier Zero / High Value targets](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20Entra%20Users%20to%20Tier%20Zero%20High%20Value%20targets.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L430), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L430) | [Test: SHORTEST_PATHS_FROM_ENTRA_USERS_TO_TIER_ZERO_HIGH_VALUE_TARGETS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L245) |
| [Shortest paths from Owned objects to Tier Zero](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20Owned%20objects%20to%20Tier%20Zero.yml#L7) | — | — |
| [Shortest paths from Owned objects](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20Owned%20objects.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L211), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L211) | [Test: SHORTEST_PATH_FROM_OWNED_OBJECTS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L69) |
| [Shortest paths from non-Tier Zero computers to Tier Zero](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20non-Tier%20Zero%20computers%20to%20Tier%20Zero.yml#L7) | — | — |
| [Shortest paths from non-Tier Zero groups to Tier Zero](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20non-Tier%20Zero%20groups%20to%20Tier%20Zero.yml#L7) | — | — |
| [Shortest paths from non-Tier Zero objects to Tier Zero](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20non-Tier%20Zero%20objects%20to%20Tier%20Zero.yml#L7) | — | — |
| [Shortest paths from non-Tier Zero user accounts to Tier Zero](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20from%20non-Tier%20Zero%20user%20accounts%20to%20Tier%20Zero.yml#L7) | — | — |
| [Shortest paths to Azure Subscriptions](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20to%20Azure%20Subscriptions.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L445), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L445) | [Test: SHORTEST_PATHS_AZURE_SUBSCRIPTIONS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L248) |
| [Shortest paths to Domain Admins from Kerberoastable users](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20to%20Domain%20Admins%20from%20Kerberoastable%20users.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L186), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L186) | [Test: SHORTEST_PATH_TO_DOMAIN_ADMINS_FROM_KERBEROASTABLE_USERS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L65) |
| [Shortest paths to Domain Admins](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20to%20Domain%20Admins.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L201), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L201) | [Test: SHORTEST_PATH_TO_DOMAIN_ADMINS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L68) |
| [Shortest paths to Tier Zero / High Value targets](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20to%20Tier%20Zero%20High%20Value%20targets.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L191), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L191) | [Test: SHORTEST_PATH_TO_TIER_ZERO_HIGH_VALUE_TARGETS](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L66) |
| [Shortest paths to privileged roles](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20to%20privileged%20roles.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L435), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L435) | [Test: SHORTEST_PATHS_TO_PRIVILEGED_ROLES](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L246) |
| [Shortest paths to systems trusted for unconstrained delegation](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Shortest%20paths%20to%20systems%20trusted%20for%20unconstrained%20delegation.yml#L7) | [AGI](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts#L181), [AGT](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts#L181) | [Blocked: SHORTEST_PATH_TO_SYSTEMS_TRUSTED_FOR_UNCONSTRAINED_DELEGATION](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/enums_factory/cypher_queries_enum_utils.py#L64) |
| [Tier Zero accounts not members of Denied RODC Password Replication Group](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Tier%20Zero%20accounts%20not%20members%20of%20Denied%20RODC%20Password%20Replication%20Group.yml#L7) | — | — |
| [Tier Zero accounts that can be delegated](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Tier%20Zero%20accounts%20that%20can%20be%20delegated.yml#L7) | — | — |
| [Tier Zero users not member of Protected Users](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Tier%20Zero%20users%20not%20member%20of%20Protected%20Users.yml#L7) | — | — |
| [Uncommon permission on containers](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Uncommon%20permission%20on%20containers.yml#L7) | — | — |

The same regular-expansion queries can also be exposed to **[DAWGS-032](#dawgs-032)** when a valid trail starts with a self-loop and then uses fresh relationships. That is an additional data-dependent mechanism, not another confirmed failure for every row above. A self-loop detour is generally unnecessary for an ordinary shortest path between distinct endpoints, so shortest-path entries are not automatically classified as [DAWGS-032](#dawgs-032) cases.

### Other query-specific matches and conditional candidates

| Query or consumer | Finding | Assessment and required condition |
| --- | --- | --- |
| [Principals with deprecated or prohibited Entra role assignments](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Principals%20with%20deprecated%20or%20prohibited%20Entra%20role%20assignments.yml#L7) | [DAWGS-007](#dawgs-007) | Its case-insensitive GUID alternatives are not anchored. A larger string containing one of the listed GUIDs can match on PostgreSQL but not under full-string Cypher semantics. If roledefinitionid is always exactly a GUID, this particular difference is latent. The full-query stress probe returned one row on Neo4j and two on PostgreSQL using a malformed embedded-GUID value; the normal GUID control matched both. This does not establish exposure under the valid-GUID invariant. |
| [Uncommon permission on containers](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Uncommon%20permission%20on%20containers.yml#L7) | [DAWGS-007](#dawgs-007), [DAWGS-019](#dawgs-019) | Besides its unbounded Contains expansion, its Exchange pt3 exclusion uses a DN regex without full-string anchors/wildcards at both ends. A substring match can exclude a path that the Cypher predicate retains. The sample-graph validation executed the full query. Separate Exchange and deep-leaf diagnostic variants, each adding one object filter to the original predicates, returned one row on Neo4j and zero on PostgreSQL. The full query's unordered LIMIT makes differing subsets alone inconclusive. |
| [All incoming and local paths for a specific computer](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/All%20incoming%20and%20local%20paths%20for%20a%20specific%20computer.yml#L7) | [DAWGS-033](#dawgs-033) | The path uses an undirected first relationship. If the data contains a qualifying self-loop, the reported undirected-loop translation defect can omit it. This does not establish an effect on ordinary non-loop edges; the full-query validation returned one row on Neo4j and zero on PostgreSQL for a controlled computer AdminTo self-loop. |
| [Cross-forest trusts with abusable configuration](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Cross-forest%20trusts%20with%20abusable%20configuration.yml#L7) | [DAWGS-033](#dawgs-033) | The path uses an undirected first relationship. If the data contains a qualifying self-loop, the reported undirected-loop translation defect can omit it. This does not establish an effect on ordinary non-loop edges; the full-query validation returned one row on Neo4j and zero on PostgreSQL for a deliberately malformed trust self-loop. This is a robustness probe, not a valid AD trust configuration. |
| [getAllShortestPathsInternal](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/cmd/api/src/queries/graph.go#L257) | [DAWGS-019](#dawgs-019) | The UI/API shortest-path builder calls FetchAllShortestPaths without an explicit depth bound. A sole eligible 16-hop route is exposed to the same PostgreSQL default as the literal shortest-path queries. Static call-path mapping; not an end-to-end API replay. |
| [fetchUnrestrictedShortestAttackPaths](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/hunter/relationship_workflows_provider.go#L336) | [DAWGS-019](#dawgs-019) | Hunter's unrestricted semantic attack-path provider also calls FetchAllShortestPaths without a depth bound. Its MaxPaths+1 limit is a row budget. The restricted-user path uses a custom one-hop traversal driver and does not automatically inherit the SQL expansion's 15-hop default ([restricted path](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/hunter/attack_paths_provider.go#L49)). |
| [AbuseTGTDelegation protected-principals help query](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/components/HelpTexts/AbuseTGTDelegation/Abuse.tsx#L47); [CoerceToTGT Linux help](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/components/HelpTexts/CoerceToTGT/LinuxAbuse.tsx#L34); [Windows help](https://github.com/SpecterOps/BloodHound/blob/e1d96ad4402cfd9a3aeb509217f57d847d4c7ae9/packages/javascript/bh-shared-ui/src/components/HelpTexts/CoerceToTGT/WindowsAbuse.tsx#L34) | Documented unsupported recursive predicate; potential [DAWGS-019](#dawgs-019) only after support exists | These examples put MemberOf*.. inside a pattern predicate. The report explicitly excludes recursive pattern predicates as unsupported. Their availability problem should be triaged against that existing limitation, rather than counted as a newly demonstrated [DAWGS-019](#dawgs-019) result failure. |

### Controls, exclusions, and limits on attribution

The following complete-query controls passed in both optimizer modes. These controls checked errors and row counts; complete path contents were not adjudicated. These results prevent a textual match from being presented as a proven application defect; they do not prove correctness for every possible graph.

| Library query | Suspected shape | Fixture result |
| --- | --- | --- |
| [Computers with non-default Primary Group membership](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Computers%20with%20non-default%20Primary%20Group%20membership.yml#L7) | [DAWGS-025](#dawgs-025)/[DAWGS-026](#dawgs-026): reused n/r/g in OPTIONAL MATCH | One Computer with isdc=true, isreadonlydc=false and a primary MemberOf edge to Group D-513, plus one unrelated Owns edge: one result row as expected. Both optional branches fail. No cardinality discrepancy demonstrated. |
| [Map domain trusts and forest roots](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Map%20domain%20trusts%20and%20forest%20roots.yml#L7) | [DAWGS-025](#dawgs-025)/[DAWGS-029](#dawgs-029): previously bound root/config | One string-collected root Domain containing a configuration Container, one string-collected child with SameForestTrust to the root, and one root-to-outside AbuseTGTDelegation boundary: one row as expected. No endpoint-rebinding discrepancy demonstrated. |
| [Domain Admins logons to non-Domain Controllers](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Domain%20Admins%20logons%20to%20non-Domain%20Controllers.yml#L7) | Collected entities, WITH scope, and repeated textual g | One DC member of Group D-516, one User member of Group D-512, and one workstation HasSession to that User: one path as expected. Node-in-node-list membership is not DAWGS-056's cross-entity-type membership trigger. Longer membership paths remain exposed to [DAWGS-019](#dawgs-019). |
| [Computers with local administrator permissions over other computers](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Computers%20with%20local%20administrator%20permissions%20over%20other%20computers.yml#L7) | [DAWGS-028](#dawgs-028): filter spans an expansion root and a later endpoint | One direct Computer→Computer AdminTo path supplies p1. The sole indirect route is Computer c→MemberOf→Group g→AdminTo→c. Both modes returned zero rows, preserving s2<>d2. No dropped-filter result was demonstrated. The unbounded range remains an [DAWGS-019](#dawgs-019) candidate. |
| [Non-Tier Zero principals with access to enabled gMSA passwords](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/queries/Non-Tier%20Zero%20principals%20with%20access%20to%20enabled%20gMSA%20passwords.yml#L7) | [DAWGS-028](#dawgs-028): expansion before a fixed password-read step | A gMSA User m (gmsa/enabled true, Base) is a MemberOf Group g (Base), and g has ReadGMSAPassword to m; another Base also reads m. The only indirect route ends at m itself. Both modes returned zero rows, preserving s2<>d2. The unbounded range remains an [DAWGS-019](#dawgs-019) candidate. |

Additional attribution constraints:

- None of the inspected catalogue queries uses list indexing/slicing, a leading OPTIONAL MATCH, literal-null IS NULL, double NOT, WITH-local ordering/pagination, or a non-first undirected relationship. No concrete catalogue match was established for [DAWGS-001](#dawgs-001)/002/005/010 or [DAWGS-040](#dawgs-040)/07/12. Inbound arrows were not mistaken for undirected edges.
- Property-array ANY/NONE expressions are not [DAWGS-003](#dawgs-003)'s scalar-list quantifier trigger. The library's SPN/encryption filters compiled without that panic. No claim of scalar-quantifier failure is made for those queries.
- “Kerberoastable users with most admin privileges” uses terminal ORDER BY/LIMIT and returns u, rather than the exact RETURN s,c shape required by [DAWGS-030](#dawgs-030). It remains an [DAWGS-019](#dawgs-019) candidate; [DAWGS-040](#dawgs-040)/[DAWGS-030](#dawgs-030) are not established matches.
- The circular-membership queries compare x.objectid with y.objectid; they do not repeat a previously bound node variable as the terminal. They are not direct [DAWGS-029](#dawgs-029) reproductions.
- Regex patterns with suitable start/end anchors or leading/trailing dot-star coverage were not marked affected solely because they use =~. The seven [DAWGS-007](#dawgs-007) entries above have a specific full-match versus substring distinction; two are source-only conditional candidates.
- Float sentinels such as [-1.0,0.0] in password-age queries do not demonstrate [DAWGS-009](#dawgs-009)'s integer-division failure. Likewise, ordinary numeric comparisons, string-valued collect(objectid), and WHERE conjunctions are not enough to establish [DAWGS-016](#dawgs-016)/37 or [DAWGS-006](#dawgs-006).
- No library query projects a property expression as a returned result column. Accordingly, [DAWGS-050](#dawgs-050)/[DAWGS-057](#dawgs-057) are not automatically assigned to this catalogue merely because its WHERE clauses read properties. Large numeric properties in returned entities can still be affected by [DAWGS-051](#dawgs-051) if values exceed exact decoding precision, but no catalogue-specific large-value fixture was run.
- Hunter's raw Cypher tool rejects unbounded/>10-hop ranged patterns, unsafe collection materialization, and missing/>100 terminal row limits ([preflight policy](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/hunter/cypher.go#L277)). Do not assign the unbounded-query list wholesale to that route. Its unrestricted semantic attack-path provider is a different call path, as noted above. Explore-path URL generation emits fixed single-hop steps ([generator](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/lib/go/hunter/explore.go#L330)), so it does not directly trigger [DAWGS-019](#dawgs-019)/004.
- A pure OR of relationship kinds is not by itself an [DAWGS-059](#dawgs-059) failure: hoisting it to a type union can preserve its meaning. Enterprise's trust cleanup is different because kinds are tied to different endpoint predicates inside the OR branches.

The Enterprise test catalogue already contains malformed queries: PRINCIPALS_WITH_FOREIGN_DOMAIN_GROUP_MEMBERSHIP uses `s.domainsidt.domainsid`, PATHS_FROM_DOMAIN_USERS_TO_TIER_ZERO_HIGH_VALUE_TARGETS and SHORTEST_PATH_TO_SYSTEMS_TRUSTED_FOR_UNCONSTRAINED_DELEGATION use unresolved `st`, and AS_REP_ROASTABLE_USERS_DONT_REQ_PRE_AUTH ends in `LIMIT 100,`. These are not newly attributed DAWGS defects. The test class is already marked xfail ([test marker](https://github.com/SpecterOps/bloodhound-enterprise/blob/ecf107871592a31aed70422f8391e36518b8fb64/houndstrike/tests_factory/api_tests/explore_page_tests/test_cypher_queries.py#L15)); an HTTP 200/nonempty assertion cannot establish Cypher result parity. The library's parser validation similarly checks syntax, not database semantics ([syntax test](https://github.com/SpecterOps/BloodHoundQueryLibrary/blob/b1fee8f5715f8fb3358e187ca3f1bddbfd125f93/tests/test_cypher_syntax.py#L32)).

Findings without a concrete source/query mapping here are not proven absent. In particular, dynamic user-provided Cypher, custom graph schemas, uncommon property values, and concurrency schedules require separate fixtures. Query shapes involving unsupported constructs or unresolved API contracts do not establish confirmed application failures.

## Appendix D: Specification and support boundaries

### Documentation boundary

The [BloodHound Supported Cypher Syntax page](https://bloodhound.specterops.io/analyze-data/explore/cypher-supported) describes the public support boundary. It excludes MERGE, CASE, comprehensions, EXISTS subqueries, and recursive pattern predicates. It also describes array inference/comparison limitations, certain bound-node joins, stricter typing, and general null differences. A related limitation does not necessarily describe the specific wrong result in a finding.

The committed [V4 support plan](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/docs/cypher_support_v4.md#L13) identifies list indexing and slicing as planned work. DAWGS-001 and DAWGS-002 therefore concern repository-acknowledged implementation gaps. List subscripts, slicing, and leading OPTIONAL MATCH have no explicit support promise or specific defect entry on the public support page. Their accepted-but-incorrect behavior is documented here without assuming such a promise. DAWGS-049 concerns a mutation related to the page's broad null caveat.

### Specification scope of the findings

Specification references use the openCypher revision listed in the introduction. The baseline is a binary PDF: GitHub cannot give it source-line anchors, so those citations name the printed page and include a PDF page fragment (printed page + four front-matter pages). CIPs and grammar use GitHub line ranges. A grammar citation establishes syntax only unless paired with a semantic rule; an explicit missing-rule or out-of-scope statement is intentional. The Cypher 9 baseline and scope proposal are not interchangeable with every construct in the current grammar. The [CIP directory description](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/README.adoc#L25-L26) identifies the PDF as part of the specification; [Cypher 9's scope](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2017-10-17-Cypher-9.adoc#L19-L37) explains its distributed sources. These references support the expectation stated, without changing each finding's reproduction status.

The entries are **not all openCypher specification deviations**. Evidence level and specification scope are separate: reproducing a library defect does not make it a language-conformance failure, and a source-supported candidate remains unconfirmed even when the expected language behavior is specified.

| Scope | Examples | Interpretation |
| --- | --- | --- |
| openCypher query semantics | DAWGS-001 and DAWGS-002 list operations, DAWGS-019 and DAWGS-020 traversal bounds, DAWGS-021 OPTIONAL MATCH, DAWGS-038 mutation visibility; DAWGS-040 WITH pagination and DAWGS-008 Boolean negation | The observed query behavior conflicts with the corresponding openCypher behavior. The baseline defines omitted bounds as allowing paths of any length ([variable-length pattern semantics, printed p. 11](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/0.baseline/openCypher9.pdf#page=15)). |
| Cypher compatibility with an incomplete openCypher semantic reference | DAWGS-007 regex matching and its downstream query examples; the exact rounding assertion in DAWGS-015 | Full-string matching is the Neo4j Cypher compatibility expectation used by these reproductions. The current [grammar includes the regex operator](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/grammar/openCypher.bnf#L695-L701), but this syntax production does not define full-string versus substring matching. The older [Cypher 9 proposal excludes regex](https://github.com/opencypher/openCypher/blob/677cbafabb8c3c5eed458fd3b1ec0daec8d67d23/cip/1.adopted/CIP2017-10-17-Cypher-9.adoc#L85). |
| DAWGS builder, driver, algorithm, or utility behavior | DAWGS-062 graph selection, DAWGS-073 reachability cache, DAWGS-059 Neo4j builder rewriting, DAWGS-079 pagination helper, import/export and concurrency findings | These concern DAWGS behavior and its callers. DAWGS-059 changes the intended Boolean query during rendering; it does not demonstrate that Neo4j executes the rendered Cypher incorrectly. openCypher does not specify these Go API contracts. |
| API contract questions and related manifestations | Entries marked Contract decision needed or Related | A contract question is not a confirmed language deviation; a related manifestation does not establish a separate root cause. |

Acceptance followed by silent omission of legacy constructs in DAWGS-013 is a parser/implementation concern; acceptance alone does not establish that the constructs belong to the referenced openCypher language. In the downstream impact section, DAWGS-019 maps to language semantics, DAWGS-007 to regex compatibility, and the API/algorithm mappings to library behavior. The section is therefore an impact assessment across these scopes, not a list consisting solely of openCypher violations.

### Differences from documented limitations

The page documents each of these Known Defects as a **translation error**. The findings below produce **silently wrong results** instead.

| Finding | Documented Known Defect | Difference |
| --- | --- | --- |
| DAWGS-025, DAWGS-029 | “Right-Hand Bound Node Lookups” (`match (e) match p = ()-[]->(e)` fails with `invalid reference to FROM-clause entry for table "s0"`) | DAWGS-025 (both endpoints bound, including the `*k..k` lowering and the optimizer-disabled mode) multiplies rows by the node count. DAWGS-029 (terminal bound earlier in the same pattern) rebinds the node. Neither raises an error. |
| DAWGS-026 (sub-case) | Same | `MATCH ()-[r:R]->() WITH r MATCH (x)-[r]->(y)` fails with the documented error text, but for a bound **relationship**. The main DAWGS-026 symptom, multiplication by the edge count, is silent. |
| DAWGS-056 | “Unpacking Arrays of Entities for Comparison” (`not u in disabled` fails with a column-notation error) | DAWGS-056 compares relationship IDs against a collected node list (and vice versa) and returns matches across entity types. |
| DAWGS-042 | “Untyped Array References and Literals” | The triggers listed here use numeric WITH pipelines or a property array. The two documents also disagree: [cypher/Cypher Syntax Support.md](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/Cypher%20Syntax%20Support.md#L1) lists “Unwind Expressions” as unsupported, while the BloodHound page documents UNWIND as supported. |

### Overlap with documented differences

The page's Differences section describes stricter typing (type mismatches that “make it down to the underlying SQL database” and fail) and general `null` differences. These findings sit near those descriptions but behave differently:

| Finding | Related difference | Why it is listed separately |
| --- | --- | --- |
| DAWGS-016 | Stricter Typing | `n.x + n.y` silently concatenates (`"52"`), and `n.x % n.y` returns a boolean. The documented difference describes errors, not silently wrong values. |
| DAWGS-054 | Stricter Typing | `collect(n.x)` silently returns strings for numbers and booleans. |
| DAWGS-018 | Stricter Typing | String predicates and ordered comparisons on non-string values match through the text form, which is looser than Cypher, not stricter. |
| DAWGS-052 | Null Behavior, “Silent Query Failure” | The page's example projects a property through WITH and warns about null ambiguity. It does not describe the raw-jsonb values (`toString(nm)` returns `"\"abc\""`). |
| DAWGS-053 | Null Behavior | `sum()` over zero rows returns `null` (Cypher: `0`), which could be read as a null difference. The precision loss from the `float8` cast is not covered. |
| DAWGS-015 | `toInteger` description | The error case is documented. Rounding `1.9` to `2` instead of truncating is not. |
| DAWGS-112 | [cmd/retriever/README.md](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cmd/retriever/README.md#L1) | The README documents that load refuses non-empty targets and validates every fragment before database writes. It does not describe that a database-side failure leaves a partial graph, which then blocks the retry. |

### Mechanism described in developer documentation

- **DAWGS-032:** [cypher/models/pgsql/translate/README.md](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/cypher/models/pgsql/translate/README.md#L1) shows the primer setting `is_cycle` when `start_id = end_id`, and the recursive step excluding `is_cycle` rows. It does not present the user-visible effect (trails that begin with a self-loop are never extended) as a limitation, and [integration/testdata/self_cycles.md](https://github.com/SpecterOps/DAWGS/blob/c107686a5e8685693a7df660d2ee968c758c17af/integration/testdata/self_cycles.md#L1) does not cover that case.

### Related failure paths

- **DAWGS-017** (a standalone `CALL` parses to a nil query, and formatting that nil query panics) is related to DAWGS-103 (in-query `CALL` is silently omitted), but it is a different code path and symptom.
- **DAWGS-101** (load decodes JSONL numbers as float64) loses precision on import, a separate path from DAWGS-051's result decoding.

### Behavior advertised as supported

The public support page advertises WITH ordering/pagination (DAWGS-040), SET (DAWGS-041), regex matching (DAWGS-007), negation (DAWGS-008), and literal substring/prefix/suffix search without wildcard expansion (DAWGS-014). The query probes contradict those behaviors. Repository optimizer documentation additionally describes semantic preservation, cache identity, and dependency-safe reordering relevant to DAWGS-025, DAWGS-058, and DAWGS-035. DAWGS-056's ID rewrite exists in the fallback translator too; disabling the optimizer does not disable that translator behavior. The retriever documents named-graph dumping, whose isolation is undermined by DAWGS-062.

### Validation limits

A reproduced status applies to the stated trigger. It does not establish every related example, concurrency schedule, proposed cause, or suggested fix. Source-supported candidates need a complete fixture, an expected result or API contract, and captured runtime evidence. Concurrency claims require targeted testing; live Neo4j execution must be distinguished from inspecting generated Cypher.

Result comparisons checked available headers, row counts/order, supported values, and persistent side effects. Exact integer comparisons avoided rounding expected int64 values through floating point. The general query adapter did not distinguish numerically equal integers and integral floats or validate complete path contents, temporal values, mock procedure definitions, and unavailable empty-result metadata. Depth probes counted paths rather than comparing their contents. Exception classes and phases were not comprehensively mapped. Unsupported adapter assertions and resource limits do not establish product defects.

DAWGS-103 is a compilation observation. DAWGS-104 measures generated SQL size and does not demonstrate a backend performance failure. DAWGS-084 concerns test reporting. Downstream sample-graph validation has its own scope and limits in the [downstream impact section](#downstream-impact-and-validation).

# Validated DAWGS variance findings

Reviewed on **2026-10-22** against checkout **`0bf83edb8155d915c55f559bae212a4526e4607f`**. This document rechecks [variance.md](variance.md) against current implementation, regression tests, compiler output, and controlled Go probes. It covers all **113 numbered findings** and **22 additional observations**. The original report revision, `c107686a5e8685693a7df660d2ee968c758c17af`, is absent from the local Git object database; conclusions use the current checkout directly.

## Results and evidence

**90 findings retain all or part of their reported mechanism.** Nine original findings are fixed in current source/output. Four need further validation, and ten need an API or extension contract decision. These are dispositions of report entries, not counts of independent root causes or newly demonstrated database defects.

| Disposition | Count | Meaning |
| --- | ---: | --- |
| Applies | 77 | The reported behavior or implementation mechanism remains. Each entry states whether evidence is a current Go reproduction, generated output, or source inspection. |
| Partially applies | 13 | A specific subcase remains; corrected or unsupported parts are identified. |
| Original finding resolved | 9 | Current source and output address the original trigger; this does not certify every related backend behavior. |
| Needs validation | 4 | Current evidence does not establish the original claimed runtime outcome or magnitude. |
| Contract decision | 10 | Behavior remains observable or source-supported, but the required ownership, ordering, conversion, or compatibility guarantee is unsettled. |

**Current runtime evidence** includes recovered compiler panics, wrong compilation-cache reuse, reachability results, traversal callback/direction errors, utility failures, serialization, and retriever decoding/scrubbing helpers. SQL/Cypher generation checks establish what the current implementation emits. Where a database outcome is inferred from emitted SQL or source, the entry says so.

**Validation boundary:** no live PostgreSQL/Neo4j suite, `make test_all`, or external BloodHound application check was run. No `CONNECTION_STRING` was supplied in the conversation. Original database results, application outcomes, deployment priorities, and sample-graph counts remain historical evidence from the revisions named in variance.md. The external support page and openCypher references were not re-audited. The distinction between language semantics, Neo4j compatibility, DAWGS APIs, and contract questions still applies.

## Changes to the original report's conclusions

- **Fixed original findings:** DAWGS-025, DAWGS-029, DAWGS-032, DAWGS-034, DAWGS-038, DAWGS-044, DAWGS-048, DAWGS-050, and DAWGS-062.
- **Important narrowed findings:** read-only WITH modifiers still disappear (040), while staged mutation boundaries preserve them; property-array UNWIND remains broken (042), while the leading literal pipeline is repaired; multi-target DELETE CTE names are fixed, while DELETE followed by RETURN remains broken (043).
- **Builder/cache distinctions:** the original OR rewrite and branch-correlation failure is fixed (059); a conjunction of relationship kinds still becomes a type union. The cache collision (058) still reproduces through the current cache path despite newer keying/lifetime work.
- **Traversal distinctions:** the original aggregate source-label/repeated-terminal miscounts are fixed, while a pattern-predicate compilation panic remains (030). Generic undirected self-loop omission is fixed, while `(x)-[r]-(x)` remains wrong (033). The 15-hop cap remains (019), but shortest-path documentation now explicitly describes it.
- **Deletion requires renewed database validation:** current queries target graph partitions, while incident-edge deletion uses a statement trigger installed on the parent node table. The original silent-detach result (039) cannot be carried forward from that trigger definition alone. Parent-table deletion with overlapping explicit graph-local node IDs is a separate source-supported investigation described under 062.

The earlier BloodHound priority table needs the following qualifications:

| Original priority/caller mapping | Current disposition |
| --- | --- |
| 007 regex, 019 depth, 073 reachability | Underlying mechanisms remain; 073's Go fixture reproduces. The original application-specific P1 outcomes were not rerun. |
| 098 result budget | A 64 KiB literal still bypasses a 16 KiB budget with a controlled result double. The BloodHound service outcome, path-payload budget, and peak memory remain unmeasured here. |
| 033 computer-path library query | Its generic self-loop omission is fixed. The remaining self-referential-pattern error does not establish that the original library query is still affected. |
| 072 legacy migration | Worker-error blocking reproduces with controlled helper inputs. The external migration function and deployment exposure were not rechecked. |
| 059 Enterprise Neo4j trust cleanup | The original OR/correlation mechanism is repaired. The remaining conjunction case does not establish continued failure of that OR-based caller. |

The original downstream catalogue and sample replay remain historical query-shape/impact records. Their full execution counts, row differences, and source defaults are not current validation results. Appendices A and B remain available as fixture definitions for future backend replay; no claim is made that their old outcomes persist unchanged. The original distinction between feature gaps and silently incorrect execution remains useful; parse-time versus translation-time rejection has changed for some forms, as recorded below.

## Validation performed

The complete unit suite passed with **41 packages reporting success**:

```sh
TZ=Etc/UTC CC=cc CGO_ENABLED=1 GOFLAGS='-ldflags=-linkmode=external' make test
```

A plain `make test` initially stopped before running tests because its `-race` option requires cgo while the Makefile defaults `CGO_ENABLED` to 0. The command above completed with exit 0 and wrote `.coverage/coverage.txt`.

Focused validation also passed: 108 compilation/formatting executions covering 54 query variants for 001–037 across both optimizer modes; 54 compilation executions covering 27 query variants for 038–060 across both modes; existing translator, graph-scope, multipart, formatter, builder, and reachability unit tests; private driver/cache probes through temporary Go test overlays; controlled ops/traversal/cardinality/container probes; and five retriever helper probes for 080, 081, 083, 101, and 102. The helper probes assert the observed current behaviors, including expected panics; a passing probe does not mean the reported defect is fixed.

All temporary test files were removed from the repository. No product code, existing tests, fixtures, README, or variance.md was changed. Passing unit tests do not close the unexecuted database or concurrency checks identified in individual entries.

## Disposition index

Each ID below links to its current finding. Original fixtures and historical outcomes remain under the same ID in [variance.md](variance.md#findings-index).

| ID | Disposition | Finding |
| --- | --- | --- |
| [DAWGS-001](#dawgs-001) | Applies | List indexing returns the index |
| [DAWGS-002](#dawgs-002) | Applies | List slicing returns the upper bound |
| [DAWGS-003](#dawgs-003) | Applies | Scalar-list quantifiers panic |
| [DAWGS-004](#dawgs-004) | Applies | Literal null predicates underflow the expression stack |
| [DAWGS-005](#dawgs-005) | Applies | Required semantic validation is missing |
| [DAWGS-006](#dawgs-006) | Applies | AND outside WHERE becomes a row filter |
| [DAWGS-007](#dawgs-007) | Applies | Regex match is a substring search |
| [DAWGS-008](#dawgs-008) | Applies | Double negation collapses to a single NOT |
| [DAWGS-009](#dawgs-009) | Applies | Whole-number float literals become integers |
| [DAWGS-010](#dawgs-010) | Applies | shortestPath used as an expression returns a node |
| [DAWGS-011](#dawgs-011) | Applies | New panics from valid input |
| [DAWGS-012](#dawgs-012) | Applies | A comment before an arithmetic operator shifts the operator sequence |
| [DAWGS-013](#dawgs-013) | Applies | `CREATE UNIQUE` and `LOAD CSV` are accepted and dropped |
| [DAWGS-014](#dawgs-014) | Applies | STARTS WITH / ENDS WITH / CONTAINS escape `%` and `_` only for a bare property operand |
| [DAWGS-015](#dawgs-015) | Applies | `toInteger` rounds instead of truncating |
| [DAWGS-016](#dawgs-016) | Applies | Arithmetic between two properties is concatenation or trigram similarity |
| [DAWGS-017](#dawgs-017) | Applies | A standalone CALL parses to `(nil, nil)`; formatting that nil query panics |
| [DAWGS-018](#dawgs-018) | Partially applies | String predicates and ordered comparisons on non-string values use the value's text form |
| [DAWGS-019](#dawgs-019) | Applies | Unbounded traversals stop at 15 hops |
| [DAWGS-020](#dawgs-020) | Applies | Fixed-length expansion is not equivalent to an exact range |
| [DAWGS-021](#dawgs-021) | Applies | Leading OPTIONAL MATCH loses the null row |
| [DAWGS-022](#dawgs-022) | Applies | Parallel same-type relationships are rejected |
| [DAWGS-023](#dawgs-023) | Applies | An unknown label raises an error |
| [DAWGS-024](#dawgs-024) | Applies | OPTIONAL MATCH squares repeated input rows |
| [DAWGS-025](#dawgs-025) | Original finding resolved | Bound right nodes cross-join the node table |
| [DAWGS-026](#dawgs-026) | Applies | Reused relationship variables cross-join the edge table |
| [DAWGS-027](#dawgs-027) | Applies | Undirected later hops can reuse the near endpoint |
| [DAWGS-028](#dawgs-028) | Applies | Endpoint predicates on a variable-length step are dropped |
| [DAWGS-029](#dawgs-029) | Original finding resolved | Variable-length steps ignore an earlier binding of their terminal |
| [DAWGS-030](#dawgs-030) | Partially applies | Aggregate traversal count lowering miscounts |
| [DAWGS-031](#dawgs-031) | Applies | Relationship uniqueness is incomplete |
| [DAWGS-032](#dawgs-032) | Original finding resolved | A first-hop self-loop stops variable-length expansion |
| [DAWGS-033](#dawgs-033) | Partially applies | Undirected patterns mishandle self-loops |
| [DAWGS-034](#dawgs-034) | Original finding resolved | `*0..0` also returns one-hop neighbours |
| [DAWGS-035](#dawgs-035) | Applies | Optimizer pattern reordering ignores pattern-predicate dependencies |
| [DAWGS-036](#dawgs-036) | Applies | A pattern predicate in a MATCH followed by another MATCH gives invalid SQL and drops the predicate's type filter |
| [DAWGS-037](#dawgs-037) | Contract decision | Pattern map parameters use jsonb containment |
| [DAWGS-038](#dawgs-038) | Original finding resolved | DELETE before aggregating WITH is omitted |
| [DAWGS-039](#dawgs-039) | Needs validation | Plain DELETE silently detaches connected nodes |
| [DAWGS-040](#dawgs-040) | Partially applies | WITH ordering, skip, and limit are ignored |
| [DAWGS-041](#dawgs-041) | Applies | SET loses updates and drops rows |
| [DAWGS-042](#dawgs-042) | Partially applies | UNWIND generates invalid SQL for common forms |
| [DAWGS-043](#dawgs-043) | Partially applies | DELETE with several targets, or followed by RETURN, generates invalid SQL |
| [DAWGS-044](#dawgs-044) | Original finding resolved | SET, REMOVE, or CREATE before WITH generates invalid SQL |
| [DAWGS-045](#dawgs-045) | Applies | SET and REMOVE of the same property or label apply removals first |
| [DAWGS-046](#dawgs-046) | Applies | WITH → MATCH → WITH builds a self-referencing CTE |
| [DAWGS-047](#dawgs-047) | Applies | ORDER BY alias expressions and internal-name collisions |
| [DAWGS-048](#dawgs-048) | Original finding resolved | WITH n, n AS m generates invalid SQL |
| [DAWGS-049](#dawgs-049) | Applies | CREATE retains a null-valued property |
| [DAWGS-050](#dawgs-050) | Original finding resolved | Unaliased expressions lose their result-column name |
| [DAWGS-051](#dawgs-051) | Applies | Large integer properties lose precision in results |
| [DAWGS-052](#dawgs-052) | Applies | Properties projected through WITH stay raw jsonb |
| [DAWGS-053](#dawgs-053) | Applies | Integer sum loses precision; empty-input sum is null |
| [DAWGS-054](#dawgs-054) | Applies | collect(n.prop) returns strings for numbers and booleans |
| [DAWGS-055](#dawgs-055) | Partially applies | Result aliases are emitted unquoted |
| [DAWGS-056](#dawgs-056) | Applies | Entity membership compares raw ids across entity types |
| [DAWGS-057](#dawgs-057) | Applies | String values that look like JSON are decoded a second time |
| [DAWGS-058](#dawgs-058) | Applies | The translation cache returns another query's SQL |
| [DAWGS-059](#dawgs-059) | Partially applies | Neo4j builder rewrite changes Boolean kind predicates |
| [DAWGS-060](#dawgs-060) | Applies | tx.Query rewrite re-emits a lossy AST |
| [DAWGS-061](#dawgs-061) | Applies | Kind and identity-property names lack escaping |
| [DAWGS-062](#dawgs-062) | Original finding resolved | PostgreSQL reads and deletes ignore the target graph |
| [DAWGS-063](#dawgs-063) | Applies | WithGraph drops existing indexes and constraints |
| [DAWGS-064](#dawgs-064) | Partially applies | Batch de-duplication keys collide |
| [DAWGS-065](#dawgs-065) | Applies | Kind mapping can deadlock the connection pool |
| [DAWGS-066](#dawgs-066) | Applies | Changing the default graph is silently ignored after the first one |
| [DAWGS-067](#dawgs-067) | Applies | AssertSchema never creates the non-default graphs it is given |
| [DAWGS-068](#dawgs-068) | Partially applies | Updating with no changes or nil properties panics |
| [DAWGS-069](#dawgs-069) | Applies | SelectKindID loads the wrong SQL file |
| [DAWGS-070](#dawgs-070) | Applies | Neo4j batch.UpdateNodes ignores property deletions |
| [DAWGS-071](#dawgs-071) | Applies | Dumping an unknown graph name creates that graph |
| [DAWGS-072](#dawgs-072) | Applies | Parallel fetch operations hang when a worker fails |
| [DAWGS-073](#dawgs-073) | Applies | ReachabilityCache returns incomplete reach sets |
| [DAWGS-074](#dawgs-074) | Applies | Traversal patterns changing direction never match |
| [DAWGS-075](#dawgs-075) | Applies | Relationship-cache missing-ID slice |
| [DAWGS-076](#dawgs-076) | Applies | Bitmap mutation during And/AndNot iteration |
| [DAWGS-077](#dawgs-077) | Applies | PackedMap merge loses values and EFSet.Find underflows |
| [DAWGS-078](#dawgs-078) | Applies | BFSTreeFile gzip reader and SerializedSegment reconstruction |
| [DAWGS-079](#dawgs-079) | Applies | Pagination helper panics beyond result bounds |
| [DAWGS-080](#dawgs-080) | Applies | Nested sensitive keys survive preserved outer properties |
| [DAWGS-081](#dawgs-081) | Applies | Case-insensitive domain scrub option unused |
| [DAWGS-082](#dawgs-082) | Applies | Neo4j temporal values serialize as empty objects |
| [DAWGS-083](#dawgs-083) | Applies | Trailing slash unpack directory fails |
| [DAWGS-084](#dawgs-084) | Applies | BDD gate ignores upstream failures |
| [DAWGS-085](#dawgs-085) | Applies | Xor rendering loses builder grouping |
| [DAWGS-086](#dawgs-086) | Applies | Empty v2 KindIn drops relationship predicate |
| [DAWGS-087](#dawgs-087) | Applies | UpdateNodeBy ignores DeletedKinds on PostgreSQL |
| [DAWGS-088](#dawgs-088) | Needs validation | Concurrent kind insert can return no rows |
| [DAWGS-089](#dawgs-089) | Applies | Kind-map getter returns shared map after unlocking |
| [DAWGS-090](#dawgs-090) | Applies | Mixed-case index field causes repeated reconciliation |
| [DAWGS-091](#dawgs-091) | Needs validation | Cursor Close does not join result producer |
| [DAWGS-092](#dawgs-092) | Applies | Neo4j nil-properties dereference |
| [DAWGS-093](#dawgs-093) | Applies | Neo4j execution uses context-free API |
| [DAWGS-094](#dawgs-094) | Partially applies | DatabaseSwitch settings, driver check and switch-state synchronization |
| [DAWGS-095](#dawgs-095) | Applies | Optional following traversal step duplicates callbacks |
| [DAWGS-096](#dawgs-096) | Partially applies | Acyclic helpers replace caller ExpansionFilter |
| [DAWGS-097](#dawgs-097) | Applies | BreadthFirst hides cancellation error |
| [DAWGS-098](#dawgs-098) | Partially applies | FetchByQuery charges shallow result sizes |
| [DAWGS-099](#dawgs-099) | Applies | ThreadSafeDuplex cross-operation deadlock |
| [DAWGS-100](#dawgs-100) | Applies | FetchNodeProperties ignores iteration error |
| [DAWGS-101](#dawgs-101) | Applies | JSON import numbers round through float64 |
| [DAWGS-102](#dawgs-102) | Applies | Directory manifests permit parent-relative fragment paths |
| [DAWGS-103](#dawgs-103) | Applies | In-query CALL omitted from generated SQL |
| [DAWGS-104](#dawgs-104) | Needs validation | Historical CREATE SQL expansion size |
| [DAWGS-105](#dawgs-105) | Contract decision | AsNumeric truncates and wraps |
| [DAWGS-106](#dawgs-106) | Contract decision | Batch flush groups operations by kind |
| [DAWGS-107](#dawgs-107) | Contract decision | Explicit IDs do not adjust node sequence |
| [DAWGS-108](#dawgs-108) | Contract decision | Neo4j relationship upsert sets endpoint lastseen |
| [DAWGS-109](#dawgs-109) | Contract decision | Neo4j write-count transaction rotation |
| [DAWGS-110](#dawgs-110) | Contract decision | AssertSchema owns entire Neo4j schema and rejects composites |
| [DAWGS-111](#dawgs-111) | Contract decision | Kinds.Remove changes shared slice storage |
| [DAWGS-112](#dawgs-112) | Contract decision | Failed multi-batch Load leaves state and prevents ordinary retry |
| [DAWGS-113](#dawgs-113) | Contract decision | ParseDocument permits unknown keys and trailing bytes |

## Findings that still apply in full or in part

Entries below retain current behavior or mechanisms, with their evidence limits. They include source-supported candidates from the original report; an Applies disposition alone does not upgrade a source-only concurrency or backend claim to a runtime reproduction.

<a id="dawgs-001"></a>

### DAWGS-001: List indexing returns the index

**Status: applies.** The parser still loses the list subscript and translation selects its index.

Current trigger(s), run separately:

```cypher
WITH [1,2] AS xs RETURN xs[0] AS x
```

Both modes compile to `with s0 as (select array [1, 2]::int8[] as i0) select 0 as x from s0;`; formatted Cypher also becomes `with [1, 2] as xs return 0 as x`.

Current compiler output checked; the original live result was not rerun.

Evidence: [cypher/frontend/expression.go:394](cypher/frontend/expression.go#L394), [cypher/frontend/context.go:427](cypher/frontend/context.go#L427).

<a id="dawgs-002"></a>

### DAWGS-002: List slicing returns the upper bound

**Status: applies.** List slicing still becomes the final index expression instead of an array slice.

Current trigger(s), run separately:

```cypher
RETURN [1,2,3][0..2] AS xs
```

Both modes emit `select 2 as xs;`.

Current compiler output checked; this independently preserves the reported list-to-scalar error.

Evidence: [cypher/frontend/expression.go:394](cypher/frontend/expression.go#L394), [cypher/frontend/context.go:427](cypher/frontend/context.go#L427).

<a id="dawgs-003"></a>

### DAWGS-003: Scalar-list quantifiers panic

**Status: applies.** Scalar projection quantifiers still dereference a missing current pattern.

Current trigger(s), run separately:

```cypher
RETURN all(x IN [1] WHERE x = 1) AS value
```

The standalone Go probe recovers `runtime error: invalid memory address or nil pointer dereference` in both modes.

This is a current Go runtime reproduction of the compilation panic, without a database.

Evidence: [cypher/models/pgsql/translate/quantifiers.go:43](cypher/models/pgsql/translate/quantifiers.go#L43), [cypher/models/pgsql/translate/quantifiers.go:59](cypher/models/pgsql/translate/quantifiers.go#L59).

<a id="dawgs-004"></a>

### DAWGS-004: Literal null predicates underflow the expression stack

**Status: applies.** The literal-null predicate still underflows the unchecked operand stack.

Current trigger(s), run separately:

```cypher
RETURN null IS NULL AS value
```

The standalone Go probe recovers `runtime error: index out of range [-1]` in both modes.

The broad published null-semantics caveat does not change this current compiler-panic result.

Evidence: [cypher/frontend/expression.go:319](cypher/frontend/expression.go#L319), [cypher/models/pgsql/translate/expression.go:480](cypher/models/pgsql/translate/expression.go#L480).

<a id="dawgs-005"></a>

### DAWGS-005: Required semantic validation is missing

**Status: applies.** Both reported semantic-validation gaps remain: standalone CREATE on an already bound node and duplicate result aliases compile successfully.

Current trigger(s), run separately:

```cypher
MATCH (a) CREATE (a)
RETURN 1 AS a, 2 AS a
```

Run separately: both modes emit a node-match CTE followed by `select 1;` for the first query, and `select 1 as a, 2 as a;` for the second.

Only these concrete examples were rechecked; the old report's counts of upstream negative cases are historical and were not revalidated.

Evidence: [cypher/models/pgsql/translate/node.go:28](cypher/models/pgsql/translate/node.go#L28), [cypher/models/pgsql/translate/projection.go:1751](cypher/models/pgsql/translate/projection.go#L1751).

<a id="dawgs-006"></a>

### DAWGS-006: AND outside WHERE becomes a row filter

**Status: applies.** Top-level AND still extracts an operand as a constraint outside WHERE, including a stored-value expression.

Current trigger(s), run separately:

```cypher
UNWIND [1,2,3] AS x RETURN x, x > 1 AND x < 3 AS mid
MATCH (n:A) SET n.v = n.k > 0 AND n.k < 2
```

Both modes emit `select i0 as x, i0 > 1 as mid ... where (i0 < 3);`. The SET SQL stores only `n.k > 0`; the `< 2` term is absent.

Generated SQL was checked; database row counts and stored properties were not executed.

Evidence: [cypher/models/pgsql/translate/expression.go:1521](cypher/models/pgsql/translate/expression.go#L1521), [cypher/models/pgsql/translate/expression.go:682](cypher/models/pgsql/translate/expression.go#L682).

<a id="dawgs-007"></a>

### DAWGS-007: Regex match is a substring search

**Status: applies.** Regex still maps to unanchored PostgreSQL ~, and literal backslashes are still passed through LIKE escaping.

Current trigger(s), run separately:

```cypher
MATCH (n:A) WHERE n.name =~ 'b' RETURN n.name AS name
MATCH (n:A) WHERE n.name =~ 'x\\.y' RETURN n
MATCH (n:A) WHERE n.name =~ $regex RETURN n
```

Both modes emit `(n0.properties ->> E'name') ~ @...::text` with pattern `b`, without full-match anchoring. A literal whose Cypher value is one backslash plus `.y` is emitted with two backslashes; the parameter value keeps one.

The Neo4j-compatibility baseline and BloodHound downstream impact are inherited context, not newly validated outside this checkout.

Evidence: [cypher/models/pgsql/translate/expression.go:1268](cypher/models/pgsql/translate/expression.go#L1268), [cypher/models/pgsql/translate/expression.go:381](cypher/models/pgsql/translate/expression.go#L381), [cypher/models/pgsql/translate/expression.go:202](cypher/models/pgsql/translate/expression.go#L202).

<a id="dawgs-008"></a>

### DAWGS-008: Double negation collapses to a single NOT

**Status: applies.** Repeated NOT tokens still create one Negation node.

Current trigger(s), run separately:

```cypher
MATCH (n:A) WHERE NOT NOT n.x = 1 RETURN n.x AS x
```

Both modes format `where not n.x = 1` and emit one SQL `not`.

The frontend still uses `len(ctx.AllNOT()) > 0` rather than negation count/parity.

Evidence: [cypher/frontend/expression.go:123](cypher/frontend/expression.go#L123), [cypher/frontend/conjunction.go:63](cypher/frontend/conjunction.go#L63).

<a id="dawgs-009"></a>

### DAWGS-009: Whole-number float literals become integers

**Status: applies.** Integral-valued floats still lose their floating type in both Cypher and SQL rendering.

Current trigger(s), run separately:

```cypher
RETURN 3.0 / 2.0 AS a
```

Both modes emit `select 3 / 2 as a;`; formatted Cypher is `return 3 / 2 as a`.

Current emitter output confirms the integer-division lowering; no database arithmetic result was rerun.

Evidence: [cypher/models/pgsql/format/format.go:212](cypher/models/pgsql/format/format.go#L212), [cypher/models/cypher/format/format.go:435](cypher/models/cypher/format/format.go#L435).

<a id="dawgs-010"></a>

### DAWGS-010: shortestPath used as an expression returns a node

**Status: applies.** shortestPath in expression position still disappears into its final node variable.

Current trigger(s), run separately:

```cypher
MATCH (a:A) MATCH (b:B) RETURN shortestPath((a)-[:R*]->(b)) AS p
```

Both modes format `return b as p` and SQL projects the `b` node composite as p without a path search.

This is distinct from supported shortestPath MATCH patterns; the expression is neither implemented nor rejected.

Evidence: [cypher/frontend/context.go:355](cypher/frontend/context.go#L355), [cypher/frontend/atom.go:157](cypher/frontend/atom.go#L157), [cypher/frontend/atom.go:206](cypher/frontend/atom.go#L206).

<a id="dawgs-011"></a>

### DAWGS-011: New panics from valid input

**Status: applies.** Temporal property lookups still remove their operand and underflow; the translation cache still re-panics rather than returning an error.

Current trigger(s), run separately:

```cypher
RETURN datetime('2020-01-01').epochmillis AS v
MATCH (n:A) RETURN date(n.d).year AS v
```

Each query panics with `runtime error: index out of range [-1]` in both modes in the current Go probe.

The lowercase temporal component spelling was deliberately preserved. Empty builder criteria were source-inspected only, not separately executed; the primary temporal panic is current.

Evidence: [cypher/models/pgsql/translate/expression.go:151](cypher/models/pgsql/translate/expression.go#L151), [cypher/models/pgsql/translate/expression.go:480](cypher/models/pgsql/translate/expression.go#L480), [drivers/pg/translation_cache.go:266](drivers/pg/translation_cache.go#L266).

<a id="dawgs-012"></a>

### DAWGS-012: A comment before an arithmetic operator shifts the operator sequence

**Status: applies.** Arithmetic operator collection still misreads the comment-bearing expression.

Current trigger(s), run separately:

```cypher
MATCH (n:A) RETURN n.x - 1 /*c*/ - 2 + 3 AS v
```

Both modes return `left and right operands for binary expression "" are not compatible: int8 != int8` during translation.

The failure is fully reproduced without node data because it occurs before SQL execution.

Evidence: [cypher/frontend/expression.go:147](cypher/frontend/expression.go#L147), [cypher/frontend/expression.go:188](cypher/frontend/expression.go#L188).

<a id="dawgs-013"></a>

### DAWGS-013: `CREATE UNIQUE` and `LOAD CSV` are accepted and dropped

**Status: applies.** Text-query compilation still drops CREATE UNIQUE and LOAD CSV, although the Cypher formatter now rejects their malformed AST clause shells.

Current trigger(s), run separately:

```cypher
MATCH (a:A),(b:B) CREATE UNIQUE (a)-[:T]->(b)
LOAD CSV FROM 'file:///missing.csv' AS row MATCH (n:A) RETURN count(*) AS c
```

Direct translation in both modes emits plain MATCH CTEs ending in `select 1;` for CREATE UNIQUE and `select count(*)...` for LOAD CSV. No unique creation or CSV access is present. Formatting separately returns `unsupported updating clause type: *cypher.UpdatingClause` / `reading clause has no match or unwind statement`.

The PostgreSQL text compiler parses and translates directly and does not depend on that formatter, so formatter errors do not fix the raw-text entry point. Database side effects were not executed.

Evidence: [cypher/frontend/context.go:279](cypher/frontend/context.go#L279), [cypher/frontend/context.go:291](cypher/frontend/context.go#L291), [drivers/pg/compiler.go:59](drivers/pg/compiler.go#L59), [drivers/pg/compiler.go:85](drivers/pg/compiler.go#L85).

<a id="dawgs-014"></a>

### DAWGS-014: STARTS WITH / ENDS WITH / CONTAINS escape `%` and `_` only for a bare property operand

**Status: applies.** Functional-left string predicates still bypass bare-property wildcard escaping for literal operands.

Current trigger(s), run separately:

```cypher
MATCH (n:A) WHERE toLower(n.name) STARTS WITH 'a_' RETURN n
```

Both modes emit an ILIKE prefilter plus `lower(...name...)::text like @__strlit0::text`, with `__strlit0 = "a_%"`; the underscore is not escaped.

The newer dynamic-string-predicate tests cover helper-function escaping but do not fix this literal functional-left trigger.

Evidence: [cypher/models/pgsql/translate/expression.go:381](cypher/models/pgsql/translate/expression.go#L381), [cypher/models/pgsql/translate/expression.go:1272](cypher/models/pgsql/translate/expression.go#L1272), [cypher/models/pgsql/translate/predicate_test.go:88](cypher/models/pgsql/translate/predicate_test.go#L88).

<a id="dawgs-015"></a>

### DAWGS-015: `toInteger` rounds instead of truncating

**Status: applies.** toInteger still emits a direct PostgreSQL int8 cast rather than an explicit truncation.

Current trigger(s), run separately:

```cypher
RETURN toInteger(1.9) AS a, toInteger(-1.5) AS b
```

Both modes emit `select (1.9)::int8 as a, (- 1.5)::int8 as b;`.

Current lowering/source confirm the mechanism; the original rounding results were not rerun against PostgreSQL. The exact truncation rule remains the original report's compatibility expectation.

Evidence: [cypher/models/pgsql/translate/function.go:978](cypher/models/pgsql/translate/function.go#L978).

<a id="dawgs-016"></a>

### DAWGS-016: Arithmetic between two properties is concatenation or trigram similarity

**Status: applies.** Arithmetic between two unconstrained properties still uses text extraction and string operators.

Current trigger(s), run separately:

```cypher
MATCH (n:A) RETURN n.x + n.y AS plus, n.x % n.y AS mod
```

Both modes emit `(properties ->> 'x') || (properties ->> 'y')` and text `%` text, instead of numeric arithmetic.

The generated SQL establishes concatenation/non-numeric modulo lowering; PostgreSQL results and the availability of the trigram operator were not retested.

Evidence: [cypher/models/pgsql/translate/expression.go:350](cypher/models/pgsql/translate/expression.go#L350), [cypher/models/pgsql/translate/expression.go:1218](cypher/models/pgsql/translate/expression.go#L1218).

<a id="dawgs-017"></a>

### DAWGS-017: A standalone CALL parses to `(nil, nil)`; formatting that nil query panics

**Status: applies.** Standalone CALL still returns a successful nil AST which the formatter dereferences.

Current trigger(s), run separately:

```cypher
CALL db.labels()
```

ParseCypher(NewContext(), ...) returns a nil query with nil error; RegularQuery(nil, false) then panics with `runtime error: invalid memory address or nil pointer dereference`.

The current Go reproduction needs no procedure installation or database. DefaultCypherContext has optional procedure filters, but the public NewContext path and PostgreSQL compiler use the unfiltered parser.

Evidence: [cypher/frontend/parse.go:38](cypher/frontend/parse.go#L38), [cypher/frontend/context.go:305](cypher/frontend/context.go#L305), [cypher/models/cypher/format/format.go:1223](cypher/models/cypher/format/format.go#L1223).

<a id="dawgs-018"></a>

### DAWGS-018: String predicates and ordered comparisons on non-string values use the value's text form

**Status: partial.** String-predicate coercion remains. The ordered-comparison description is stale: numeric comparisons now cast extracted text to int8, which still accepts numeric strings without preserving original JSON type.

Current trigger(s), run separately:

```cypher
MATCH (n:A) WHERE n.x STARTS WITH '1' RETURN n
MATCH (n:A) WHERE n.x < 200 RETURN n
```

Both modes emit `(n0.properties ->> E'x') like ...` for STARTS WITH and `((n0.properties ->> E'x'))::int8 < 200` for ordering. Neither has a JSON type guard.

The first case remains the reported text-form bug. The second still permits string "123" through numeric coercion, but it is no longer a lexicographic text comparison. Current database results were not rerun.

Evidence: [cypher/models/pgsql/translate/expression.go:235](cypher/models/pgsql/translate/expression.go#L235), [cypher/models/pgsql/translate/expression.go:381](cypher/models/pgsql/translate/expression.go#L381), [cypher/models/pgsql/translate/expression.go:405](cypher/models/pgsql/translate/expression.go#L405).

<a id="dawgs-019"></a>

### DAWGS-019: Unbounded traversals stop at 15 hops

**Status: applies.** Unbounded ordinary, shortest, and all-shortest traversals still use an effective maximum of 15.

Current trigger(s), run separately:

```cypher
MATCH (a:AuditStart)-[:AuditR*]->(b:AuditEnd) RETURN b
MATCH p=shortestPath((a:AuditStart)-[:AuditR*]->(b:AuditEnd)) RETURN p
MATCH p=allShortestPaths((a:AuditStart)-[:AuditR*]->(b:AuditEnd)) RETURN p
```

Both modes emit `depth < 15` for ordinary expansion and pass 15 as the maximum to shortest/all-shortest harness calls.

The omission of longer paths remains source/SQL-supported; the 16-hop database fixture and downstream BloodHound service were not run. The original adjective "undocumented" is stale for shortest paths: README now explicitly documents maximum 15 and policy_default provenance.

Evidence: [cypher/models/pgsql/translate/expansion.go:17](cypher/models/pgsql/translate/expansion.go#L17), [cypher/models/pgsql/translate/expansion.go:51](cypher/models/pgsql/translate/expansion.go#L51), [cypher/models/pgsql/translate/expansion.go:3248](cypher/models/pgsql/translate/expansion.go#L3248), [README.md:365](README.md#L365), [cypher/models/pgsql/translate/optimizer_safety_test.go:574](cypher/models/pgsql/translate/optimizer_safety_test.go#L574).

<a id="dawgs-020"></a>

### DAWGS-020: Fixed-length expansion is not equivalent to an exact range

**Status: applies.** A single fixed-length bound still becomes only the minimum, leaving the maximum at 15.

Current trigger(s), run separately:

```cypher
MATCH (a:AuditStart)-[:AuditR*16]->(b:AuditEnd) RETURN b
MATCH (a:AuditStart)-[:AuditR*16..16]->(b:AuditEnd) RETURN b
```

Both modes format *16 as *16.. and emit recursion `depth < 15` with output filter `depth >= 16`; the explicit range emits `depth < 16`.

The contradictory fixed-form bounds are currently visible in generated SQL. Database row counts were not re-executed.

Evidence: [cypher/frontend/pattern.go:100](cypher/frontend/pattern.go#L100), [cypher/frontend/query.go:168](cypher/frontend/query.go#L168), [cypher/models/pgsql/translate/model.go:88](cypher/models/pgsql/translate/model.go#L88), [cypher/models/pgsql/translate/expansion.go:51](cypher/models/pgsql/translate/expansion.go#L51).

<a id="dawgs-021"></a>

### DAWGS-021: Leading OPTIONAL MATCH loses the null row

**Status: applies.** A leading OPTIONAL MATCH is still explicitly treated as plain MATCH.

Current trigger(s), run separately:

```cypher
OPTIONAL MATCH (n) RETURN n
```

Both modes emit only a node-scan CTE and `select s0.n0 as n from s0;`; there is no null-preserving seed/outer join.

The old empty-graph result is source/SQL-supported, not a new database reproduction.

Evidence: [cypher/models/pgsql/translate/match.go:45](cypher/models/pgsql/translate/match.go#L45).

<a id="dawgs-022"></a>

### DAWGS-022: Parallel same-type relationships are rejected

**Status: applies.** Current PostgreSQL schema still forbids distinct same-type relationships with identical endpoints and graph.

Current trigger(s), run separately:

```cypher
CREATE (a), (b) CREATE (a)-[:OCReportParallel]->(b) CREATE (a)-[:OCReportParallel]->(b)
```

The schema defines and reasserts `unique (start_id, kind_id, end_id, graph_id) include (id)`. Relationship identity remains excluded from the unique key.

This is current schema evidence; the old SQLSTATE 23505 was not regenerated against a live database. Removing the constraint alone would require auditing endpoint/type upsert assumptions.

Evidence: [drivers/pg/query/sql/schema_up.sql:248](drivers/pg/query/sql/schema_up.sql#L248), [drivers/pg/query/sql/schema_up.sql:277](drivers/pg/query/sql/schema_up.sql#L277).

<a id="dawgs-023"></a>

### DAWGS-023: An unknown label raises an error

**Status: applies.** Unknown kinds still propagate an error rather than an empty match set.

Current trigger(s), run separately:

```cypher
MATCH (n:OCReportNeverRegisteredLabel) RETURN count(n) AS count
```

With an InMemoryKindMapper lacking the label, enabled translation returns `missing kinds: [OCReportNeverRegisteredLabel]`; disabled translation returns `failed to translate kinds: missing kinds: [OCReportNeverRegisteredLabel]`. SchemaManager.MapKinds separately still returns `unable to map kinds: ...` after a refresh miss.

The compile probe uses the in-memory mapper; the production-manager error wording is source-supported and its database refresh path was not executed.

Evidence: [drivers/pg/manager.go:274](drivers/pg/manager.go#L274), [cypher/models/pgsql/translate/node.go:85](cypher/models/pgsql/translate/node.go#L85).

<a id="dawgs-024"></a>

### DAWGS-024: OPTIONAL MATCH squares repeated input rows

**Status: applies.** OPTIONAL MATCH still joins its result back to repeated origin rows by exported values without a row identity.

Current trigger(s), run separately:

```cypher
UNWIND [1,1,1] AS x MATCH (a:A {name: 'a'}) OPTIONAL MATCH (a)-[:R]->(b:B {name: 'b1'}) RETURN count(*) AS c
```

Both modes emit `from s1 left outer join s2 on (s1.n0 is not distinct from s2.n0) and (s1.i0 is not distinct from s2.i0)`; s2 was itself derived from s1.

Three identical origin rows therefore join to three identical derived matches by SQL semantics; the old count 9 is supported by current SQL, but was not rerun against PostgreSQL.

Evidence: [cypher/models/pgsql/translate/match.go:102](cypher/models/pgsql/translate/match.go#L102), [cypher/models/pgsql/translate/match.go:155](cypher/models/pgsql/translate/match.go#L155).

<a id="dawgs-026"></a>

### DAWGS-026: Reused relationship variables cross-join the edge table

**Status: applies.** A reused relationship binding still leaves a newly joined edge alias unconstrained.

Current trigger(s), run separately:

```cypher
MATCH (a)-[r:R]->(b) MATCH (a)-[r]->(c) RETURN count(*) AS c
```

Both modes join `edge e0` again but the ON clause references only the previous composite: `(s0.n0).id = (s0.e0).start_id`; the node join uses `(s0.e0).end_id`. There is no `e0.id = (s0.e0).id` condition.

This currently emitted uncorrelated edge scan supports the multiplication claim; the old count values were not re-executed.

Evidence: [cypher/models/pgsql/translate/relationship.go:213](cypher/models/pgsql/translate/relationship.go#L213), [cypher/models/pgsql/translate/traversal.go:421](cypher/models/pgsql/translate/traversal.go#L421).

<a id="dawgs-027"></a>

### DAWGS-027: Undirected later hops can reuse the near endpoint

**Status: applies.** Later undirected fixed steps still independently accept either edge endpoint for both near and far nodes.

Current trigger(s), run separately:

```cypher
MATCH (a:A)-[:R]->(b)-[:T]-(c) RETURN c.name AS n
```

Both modes emit the independent OR joins `(previous.b = e1.start_id or previous.b = e1.end_id)` and `(c.id = e1.end_id or c.id = e1.start_id)` without pairing the orientations.

The pairwise correction in directionless root builders does not cover this continuation path. Current SQL still permits c=b on a non-loop edge.

Evidence: [cypher/models/pgsql/translate/constraints.go:76](cypher/models/pgsql/translate/constraints.go#L76), [cypher/models/pgsql/translate/constraints.go:153](cypher/models/pgsql/translate/constraints.go#L153), [cypher/models/pgsql/translate/traversal.go:615](cypher/models/pgsql/translate/traversal.go#L615).

<a id="dawgs-028"></a>

### DAWGS-028: Endpoint predicates on a variable-length step are dropped

**Status: applies.** The original cross-endpoint predicate is still consumed after expansion and then discarded on a following fixed step.

Current trigger(s), run separately:

```cypher
MATCH (s)-[:R*1..2]->(g)-[:T]->(d) WHERE s.v = 99 OR g.v = 99 RETURN count(*) AS c
```

Both modes emit valid-looking expansion/fixed-suffix SQL containing neither 99 nor a v-property lookup. The optimized form additionally pushes an EXISTS suffix test, but still omits the WHERE expression.

This exact original trigger was recompiled. The old live count 5 was not rerun; omission is established by current generated SQL.

Evidence: [cypher/models/pgsql/translate/constraints.go:488](cypher/models/pgsql/translate/constraints.go#L488), [cypher/models/pgsql/translate/traversal.go:1161](cypher/models/pgsql/translate/traversal.go#L1161), [cypher/models/pgsql/translate/traversal.go:1175](cypher/models/pgsql/translate/traversal.go#L1175).

<a id="dawgs-030"></a>

### DAWGS-030: Aggregate traversal count lowering miscounts

**Status: partial.** The source-label and repeated-endpoint miscount shapes now fall back safely, but the reported pattern-predicate panic survives.

Current trigger(s), run separately:

```cypher
MATCH (s:A) WHERE (s)-[:T]->() MATCH (s)-[:R*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100
```

This query currently panics with `runtime error: index out of range [-1]` when optimization is enabled. The original `(s:A) MATCH (s:B)` query now emits both A and B constraints and no candidate_sources lowering; the repeated-terminal variant emits root_id = next_id plus bound-terminal equality.

Focused fallback tests passed for the corrected miscount families. Disabled translation of the residual predicate query does not panic but emits an invalid forward/self CTE reference and drops the T filter, overlapping DAWGS-036. No live aggregate counts were measured.

Evidence: [cypher/models/pgsql/optimize/lowering_plan.go:3244](cypher/models/pgsql/optimize/lowering_plan.go#L3244), [cypher/models/pgsql/translate/optimizer_safety_test.go:2856](cypher/models/pgsql/translate/optimizer_safety_test.go#L2856), [cypher/models/pgsql/translate/aggregate_traversal_count.go:460](cypher/models/pgsql/translate/aggregate_traversal_count.go#L460), [integration/testdata/templates/aggregate_traversal_count_shapes.json:4](integration/testdata/templates/aggregate_traversal_count_shapes.json#L4).

<a id="dawgs-031"></a>

### DAWGS-031: Relationship uniqueness is incomplete

**Status: applies.** Cross-part relationship uniqueness and expansion-versus-expansion uniqueness are still missing, although fixed/expansion continuation checks have improved.

Current trigger(s), run separately:

```cypher
MATCH (a)-[r1:R]->(x), (a)-[r2:R]->(y) RETURN count(*) AS c
MATCH (x)-[:R*1..2]->(y)-[:R*1..2]->(z) RETURN count(*)
```

Both modes omit any edge-identity inequality across the comma-separated parts. For consecutive expansions the second completed-state filter correlates only the shared node; it contains no disjointness/overlap predicate for the two path edge-ID arrays.

Current source now checks fixed relationships against prior expansion paths, so the old broad statement that all expansion interactions are skipped is stale. The two reported residual families above remain source/SQL-supported; database counts were not rerun.

Evidence: [cypher/models/pgsql/translate/traversal.go:955](cypher/models/pgsql/translate/traversal.go#L955), [cypher/models/pgsql/translate/traversal.go:999](cypher/models/pgsql/translate/traversal.go#L999), [cypher/models/pgsql/translate/match.go:17](cypher/models/pgsql/translate/match.go#L17).

<a id="dawgs-033"></a>

### DAWGS-033: Undirected patterns mishandle self-loops

**Status: partial.** Generic and single-bound undirected self-loop omission is corrected, but a self-referential root pattern still matches ordinary incident edges.

Current trigger(s), run separately:

```cypher
MATCH (x)-[r:R]-(x) RETURN x.name AS n
```

Both modes emit `join node n0 on (n0.id = e0.end_id or n0.id = e0.start_id)` and repeat the same OR in WHERE, rather than requiring both endpoints equal n0. The other original triggers `MATCH ()-[r:R]-()` and `MATCH (x:C)-[r:R]-(y)` now use paired endpoint constraints and retain loops.

The old downstream library-query omission should not be retained as current evidence without rerunning it: its generic-loop symptom is fixed. Current self-reference SQL still matches a and b as well as loop node c on the report fixture.

Evidence: [cypher/models/pgsql/translate/traversal_directionless.go:251](cypher/models/pgsql/translate/traversal_directionless.go#L251), [cypher/models/pgsql/translate/traversal_directionless.go:334](cypher/models/pgsql/translate/traversal_directionless.go#L334), [integration/testdata/cases/expand_into.json:65](integration/testdata/cases/expand_into.json#L65).

<a id="dawgs-035"></a>

### DAWGS-035: Optimizer pattern reordering ignores pattern-predicate dependencies

**Status: applies.** The optimized pattern-predicate dependency case still emits an out-of-scope CTE reference after reordering.

Current trigger(s), run separately:

```cypher
MATCH (b:B) MATCH (c {name:'c1'}) WHERE (b)-[:R]->(c) RETURN b, c
```

Enabled mode moves c into s0, then builds s2 whose WHERE subquery reads `s2.n0` and `s2.n1` while s2 is still being defined. Disabled output does not introduce that same reordered self-reference.

Current SQL output checked; PostgreSQL missing-FROM error was not re-executed. Pattern node variables are still absent from the normal walk branches, so the original dependency-analysis explanation remains supported.

Evidence: [cypher/models/pgsql/optimize/analysis.go:492](cypher/models/pgsql/optimize/analysis.go#L492), [cypher/models/walk/walk_cypher.go:674](cypher/models/walk/walk_cypher.go#L674), [cypher/models/walk/walk_cypher.go:688](cypher/models/walk/walk_cypher.go#L688).

<a id="dawgs-036"></a>

### DAWGS-036: A pattern predicate in a MATCH followed by another MATCH gives invalid SQL and drops the predicate's type filter

**Status: applies.** A predicate preceding another MATCH is still rebuilt against a future/current frame and loses its relationship type constraint.

Current trigger(s), run separately:

```cypher
MATCH (s:A) WHERE (s)-[:T]->() MATCH (s)-[:R]->(t) RETURN s,t
```

Both modes produce a CTE s2 containing a predicate subquery that reads `s2.n0` (and exports `s2.n2`) while s2 is being constructed. The inner e0 predicate lacks any T-kind filter; only outer R-kind filtering remains.

The current compilation demonstrates the invalid reference and missing type condition. It does not claim a newly executed PostgreSQL error.

Evidence: [cypher/models/pgsql/translate/predicate.go:140](cypher/models/pgsql/translate/predicate.go#L140), [cypher/models/pgsql/translate/predicate.go:182](cypher/models/pgsql/translate/predicate.go#L182).

<a id="dawgs-040"></a>

### DAWGS-040: WITH ordering, skip, and limit are ignored

**Status: partial.** Read-only multipart boundaries still use buildInlineProjection without applying SortItems/Skip/Limit. The original read-only query emits no ORDER BY or LIMIT in either mode. Staged queries containing nonfinal mutations now apply boundary modifiers, with unit coverage; the blanket original claim should be narrowed.

**Current trigger/output:** MATCH (n:A) WITH n ORDER BY n.k DESC LIMIT 2 RETURN n.k AS k. Current SQL ends SELECT ... FROM s0 with no ordering/pagination.

**Evidence:** [cypher/models/pgsql/translate/projection.go:761](cypher/models/pgsql/translate/projection.go#L761), [cypher/models/pgsql/translate/query.go:66](cypher/models/pgsql/translate/query.go#L66), [cypher/models/pgsql/translate/multipart.go:85](cypher/models/pgsql/translate/multipart.go#L85), [cypher/models/pgsql/translate/multipart_test.go:96](cypher/models/pgsql/translate/multipart_test.go#L96), [cypher/models/pgsql/translate/multipart_test.go:140](cypher/models/pgsql/translate/multipart_test.go#L140).

**Validation level:** Current SQL generation reproduced in both optimizer modes; multipart modifier tests passed; row counts not rerun against a database.

<a id="dawgs-041"></a>

### DAWGS-041: SET loses updates and drops rows

**Status: applies.** Updates still form an UPDATE ... FROM previous_frame ... RETURNING chain. Each next target consumes the previous UPDATE result, preserving the row-collapse mechanism for repeated/null targets. New refreshUpdatedEntityProjection fixes freshness for aliases but does not preserve the original matched row stream.

**Current trigger/output:** MATCH (x:A)-[:R]->(y) SET x.src = true, y.dst = true; emitted second UPDATE reads FROM s1, the first UPDATE RETURNING CTE.

**Evidence:** [cypher/models/pgsql/translate/update.go:150](cypher/models/pgsql/translate/update.go#L150), [cypher/models/pgsql/translate/update.go:376](cypher/models/pgsql/translate/update.go#L376), [cypher/models/pgsql/translate/update_alias_test.go:43](cypher/models/pgsql/translate/update_alias_test.go#L43).

**Validation level:** Current SQL generation reproduced in both optimizer modes; alias-refresh tests passed. Original fan-out write/cardinality database effects are source-supported, not rerun.

<a id="dawgs-042"></a>

### DAWGS-042: UNWIND generates invalid SQL for common forms

**Status: partial.** Leading literal UNWIND followed by WITH is repaired and emits a valid synthetic input frame. Property-array UNWIND still calls unnest on the text extracted with ->>, retaining the second original failure.

**Current trigger/output:** MATCH (n:A) UNWIND n.arr AS x RETURN x emits FROM s0, unnest(((s0.n0).properties ->> E'arr')) AS i0. UNWIND [1,2,3] AS x WITH x WHERE x > 1 RETURN x now emits a proper CTE over unnest(int8[]).

**Evidence:** [cypher/models/pgsql/translate/unwind.go:30](cypher/models/pgsql/translate/unwind.go#L30), [cypher/models/pgsql/translate/unwind.go:65](cypher/models/pgsql/translate/unwind.go#L65), [cypher/models/pgsql/translate/projection.go:787](cypher/models/pgsql/translate/projection.go#L787).

**Validation level:** Both forms currently translated in both optimizer modes; PostgreSQL unnest(text) execution/error was not rerun.

<a id="dawgs-043"></a>

### DAWGS-043: DELETE with several targets, or followed by RETURN, generates invalid SQL

**Status: partial.** Each delete target now receives a separate frame/CTE name, resolving the duplicate-CTE variant. A final DELETE followed by RETURN still builds a non-RETURNING delete CTE and then reads it in the tail projection.

**Current trigger/output:** MATCH (x:A)-[r:R]->(y) DELETE r RETURN count(*) emits s1 AS (DELETE FROM edge_17 ...), followed by SELECT count(*) ... FROM s1; no RETURNING exists. DELETE r, y now emits distinct s1/s2 CTEs.

**Evidence:** [cypher/models/pgsql/translate/delete.go:11](cypher/models/pgsql/translate/delete.go#L11), [cypher/models/pgsql/translate/delete.go:33](cypher/models/pgsql/translate/delete.go#L33), [cypher/models/pgsql/translate/delete.go:96](cypher/models/pgsql/translate/delete.go#L96), [cypher/models/pgsql/translate/query.go:44](cypher/models/pgsql/translate/query.go#L44).

**Validation level:** Current SQL generation reproduced in both optimizer modes; database execution not rerun.

<a id="dawgs-045"></a>

### DAWGS-045: SET and REMOVE of the same property or label apply removals first

**Status: applies.** The folded update expression still subtracts removals first and appends assignments afterward, regardless of clause order. Current property and kind probes produce the same remove-then-add expressions as the report.

**Current trigger/output:** MATCH (n:A) SET n.x = 1 REMOVE n.x emits properties = n1.properties - ARRAY['x'] || jsonb_build_object('x',1); SET n:B REMOVE n:B emits kind_ids = uniq(sort(kind_ids - ARRAY[B] || ARRAY[B])).

**Evidence:** [cypher/models/pgsql/translate/update.go:269](cypher/models/pgsql/translate/update.go#L269), [cypher/models/pgsql/translate/update.go:339](cypher/models/pgsql/translate/update.go#L339).

**Validation level:** Current SQL generation reproduced in both optimizer modes; stored effects not rerun.

<a id="dawgs-046"></a>

### DAWGS-046: WITH → MATCH → WITH builds a self-referencing CTE

**Status: applies.** The read-only multipart path still builds s2 AS (... FROM s2 ...), with references to s0 not present in that inner FROM. Mutation staging does not cover this read-only shape.

**Current trigger/output:** MATCH (a) WITH a MATCH (a)-[:R]->(c) WITH c RETURN c emits s2 AS (WITH s3 AS (SELECT ... FROM s2 JOIN edge_17 e0 ON (s0.n0).id = e0.start_id ...)).

**Evidence:** [cypher/models/pgsql/translate/query.go:77](cypher/models/pgsql/translate/query.go#L77), [cypher/models/pgsql/translate/projection.go:761](cypher/models/pgsql/translate/projection.go#L761).

**Validation level:** Current SQL generation reproduced in both optimizer modes; original PostgreSQL error not rerun.

<a id="dawgs-047"></a>

### DAWGS-047: ORDER BY alias expressions and internal-name collisions

**Status: applies.** rewriteOrderByProjectionAlias only replaces an ORDER BY expression when the whole expression is an Identifier, so an alias nested inside arithmetic retains its internal name. Input/output identifier collisions remain possible.

**Current trigger/output:** MATCH (n:A) RETURN n.k AS x ORDER BY x + 0 emits SELECT ... AS x ... ORDER BY i0 + 0. UNWIND [1,2,3] AS x RETURN 10-x AS i0 ORDER BY x emits SELECT 10-i0 AS i0 ... ORDER BY i0.

**Evidence:** [cypher/models/pgsql/translate/projection.go:1314](cypher/models/pgsql/translate/projection.go#L1314), [cypher/models/pgsql/translate/projection.go:1656](cypher/models/pgsql/translate/projection.go#L1656).

**Validation level:** Current SQL generation reproduced in both optimizer modes; database errors/order not rerun.

<a id="dawgs-049"></a>

### DAWGS-049: CREATE retains a null-valued property

**Status: applies.** buildPropertiesObject still emits every map entry without filtering null. Current translation contains jsonb_build_object(...,12,...,null), so the stored JSON null mechanism persists. Keep the original caveat that exact keys()/property metrics and broader advertised null compatibility need separate interpretation.

**Current trigger/output:** CREATE (n {id:12,name:null}) RETURN n.id AS id,n.name AS p emits JSON construction containing the name:null pair.

**Evidence:** [cypher/models/pgsql/translate/create.go:301](cypher/models/pgsql/translate/create.go#L301), [cypher/models/pgsql/translate/create.go:325](cypher/models/pgsql/translate/create.go#L325).

**Validation level:** Current SQL generation in both optimizer modes; storage snapshot, keys(), and mutation metrics not rerun.

<a id="dawgs-051"></a>

### DAWGS-051: Large integer properties lose precision in results

**Status: applies.** JSON still decodes into any using encoding/json.Unmarshal, yielding float64; no UseNumber path exists in result decoding. A current helper-level probe decodes 4611686018427387905 as float64 4.611686018427388e+18. Emitted property SQL preserves JSONB, so this still reaches the relevant conversion family.

**Current trigger/output:** decodeJSONValue([]byte("4611686018427387905")) returns float64, losing the final unit; original CREATE ... RETURN n.id AS value still emits JSONB property extraction.

**Evidence:** [drivers/pg/result.go:47](drivers/pg/result.go#L47), [drivers/pg/result.go:109](drivers/pg/result.go#L109), [drivers/pg/result_test.go:21](drivers/pg/result_test.go#L21).

**Validation level:** Current Go helper runtime reproduction and current translation; complete live scalar/map/entity round trips not rerun.

<a id="dawgs-052"></a>

### DAWGS-052: Properties projected through WITH stay raw jsonb

**Status: applies.** Property projections intentionally retain the JSONB -> operator. The carried alias is subsequently cast directly to text by toString, retaining JSON quotes around a stored string rather than extracting its string value.

**Current trigger/output:** MATCH (n:A) WITH n.name AS nm RETURN toString(nm) emits inner ((s1.n0).properties -> E'name') AS i0 and outer (i0)::text.

**Evidence:** [cypher/models/pgsql/translate/projection.go:1790](cypher/models/pgsql/translate/projection.go#L1790), [cypher/models/pgsql/translate/with.go:77](cypher/models/pgsql/translate/with.go#L77).

**Validation level:** Current SQL generation reproduced in both optimizer modes; exact returned string and broader comparison failures not rerun.

<a id="dawgs-053"></a>

### DAWGS-053: Integer sum loses precision; empty-input sum is null

**Status: applies.** SUM still casts its input to float8, wraps the result as numeric, and lacks coalesce(...,0). AVG uses the same float8 input cast. Those mechanisms survive; the original exact rounded decimal and empty aggregate value were not remeasured against PostgreSQL.

**Current trigger/output:** MATCH (n:A) RETURN sum(n.big), avg(n.big) emits sum((property_text)::float8)::numeric and avg((property_text)::float8)::numeric with no zero fallback.

**Evidence:** [cypher/models/pgsql/translate/function.go:1017](cypher/models/pgsql/translate/function.go#L1017), [cypher/models/pgsql/translate/function.go:1033](cypher/models/pgsql/translate/function.go#L1033).

**Validation level:** Current SQL generation reproduced in both optimizer modes; precision/empty-input/avg runtime effects remain source-supported.

<a id="dawgs-054"></a>

### DAWGS-054: collect(n.prop) returns strings for numbers and booleans

**Status: applies.** The collected property operand remains text extracted with ->>, and array_agg aggregates that text. The generated numeric and Boolean property path therefore retains the reported type-loss mechanism.

**Current trigger/output:** MATCH (n:A) RETURN collect(n.x) emits array_agg(((s0.n0).properties ->> E'x')) with a text[] empty fallback.

**Evidence:** [cypher/models/pgsql/translate/function.go:594](cypher/models/pgsql/translate/function.go#L594), [cypher/models/pgsql/translate/function.go:1087](cypher/models/pgsql/translate/function.go#L1087).

**Validation level:** Current SQL generation reproduced in both optimizer modes; Go result element types and UNWIND arithmetic not rerun against a database.

<a id="dawgs-055"></a>

### DAWGS-055: Result aliases are emitted unquoted

**Status: partial.** ASCII mixed-case identifiers are still emitted unquoted, so objectId remains subject to PostgreSQL lowercase folding. Unusual aliases are now double-quoted, removing the original syntax-error mechanism, but a backtick alias retains the backticks inside the SQL identifier and therefore has the wrong column spelling.

**Current trigger/output:** RETURN n.k AS objectId emits AS objectId. RETURN n.k AS `object id` emits AS "`object id`" (including literal backticks).

**Evidence:** [cypher/models/pgsql/format/format.go:25](cypher/models/pgsql/format/format.go#L25), [cypher/models/pgsql/format/format.go:438](cypher/models/pgsql/format/format.go#L438), [drivers/pg/result.go:61](drivers/pg/result.go#L61).

**Validation level:** Current SQL generation reproduced in both optimizer modes; actual field-description strings not rerun against PostgreSQL.

<a id="dawgs-056"></a>

### DAWGS-056: Entity membership compares raw ids across entity types

**Status: applies.** The collect-ID lowering is still installed in translateOptimized even when OptimizerDisabled bypasses only the optimizer plan. A node collection is reduced to int8 IDs and relationship IN compares e0.id against those IDs without retaining entity type. The controlled ID-overlap requirement from the old report remains essential.

**Current trigger/output:** MATCH (n) WITH collect(n) AS ns MATCH ()-[r]->() WHERE r IN ns RETURN count(*) AS c emits array_agg((n0).id)::int8[] and e0.id = ANY(s0.i0), in both modes.

**Evidence:** [cypher/models/pgsql/translate/collect_id_membership.go:18](cypher/models/pgsql/translate/collect_id_membership.go#L18), [cypher/models/pgsql/translate/function.go:625](cypher/models/pgsql/translate/function.go#L625), [cypher/models/pgsql/translate/expression.go:771](cypher/models/pgsql/translate/expression.go#L771), [cypher/models/pgsql/translate/translator.go:1663](cypher/models/pgsql/translate/translator.go#L1663), [cypher/models/pgsql/translate/translator.go:1670](cypher/models/pgsql/translate/translator.go#L1670).

**Validation level:** Current SQL generation reproduced in both optimizer modes; no current overlapping-ID database count was executed.

<a id="dawgs-057"></a>

### DAWGS-057: String values that look like JSON are decoded a second time

**Status: applies.** The recent scalar-token guard preserves strings such as 1234/true/null, but strings beginning with [, {, or a quote are still parsed as JSON. Current helper probes reproduce all three original examples.

**Current trigger/output:** decodeJSONValue("[1, 2]") returns []any; decodeJSONValue("\"q\"") returns q; decodeJSONValue("{\"k\":\"v\"}") returns map[string]any.

**Evidence:** [drivers/pg/result.go:95](drivers/pg/result.go#L95), [drivers/pg/result.go:117](drivers/pg/result.go#L117), [drivers/pg/result_test.go:28](drivers/pg/result_test.go#L28).

**Validation level:** Current helper-level runtime reproduction; existing tests explicitly expect structured/quoted string decoding. Full property retrieval/Scan not rerun.

<a id="dawgs-058"></a>

### DAWGS-058: The translation cache returns another query's SQL

**Status: applies.** The current real compileRegularQuery/cache path reproduces the canonicalization collision. Warm KindIn(no kinds) then compile And(): Hits=1/Misses=1/Insertions=1 and the second query receives the empty-kind WHERE SQL. A fresh manager compiling And() emits no WHERE. This is a current wrong cached SQL reproduction, beyond equal formatted text; database result rows were not executed.

**Current trigger/output:** Both ASTs prepare as "match (n) where  return n". Warm And() SQL contains kind_ids && ARRAY[]::int2[]; cold And() SQL omits WHERE.

**Evidence:** [drivers/pg/compiler.go:30](drivers/pg/compiler.go#L30), [drivers/pg/compiler.go:106](drivers/pg/compiler.go#L106), [cypher/models/cypher/format/format.go:704](cypher/models/cypher/format/format.go#L704), [drivers/pg/translation_cache.go:133](drivers/pg/translation_cache.go#L133).

**Validation level:** Current in-memory SchemaManager compilation/cache runtime probe with optimization enabled; no database needed. Optimizer-disabled bypass remains source-supported.

<a id="dawgs-059"></a>

### DAWGS-059: Neo4j builder rewrite changes Boolean kind predicates

**Status: partial.** The primary OR-to-AND defect and branch correlation are repaired by the disjunction/negation ancestry guard and LOGIC01 regression coverage. A conjunction of distinct relationship-kind tests is still hoisted into one R|T pattern, changing AND into OR; retain only that residual claim. External Enterprise consumer outcomes were not revalidated.

**Current trigger/output:** Or(Kind(r,R),Equals(r.w,2)) now renders WHERE (r:R OR r.w=$p0). And(Kind(r,R),Kind(r,T)) still renders MATCH ()-[r:R|T]->() RETURN r.

**Evidence:** [query/neo4j/rewrite.go:64](query/neo4j/rewrite.go#L64), [query/neo4j/rewrite.go:159](query/neo4j/rewrite.go#L159), [query/neo4j/neo4j_test.go:230](query/neo4j/neo4j_test.go#L230).

**Validation level:** Current Prepare/Render runtime probes; LOGIC01 unit test passed. Neo4j execution and external consumer workflows not rerun.

<a id="dawgs-060"></a>

### DAWGS-060: tx.Query rewrite re-emits a lossy AST

**Status: applies.** Current rewriteQuery probes still lose the final namespace separator (`apoc.coll.sum` becomes `apoc.collsum`), convert `WHERE n:A:B` to `(n:A OR n:B)`, and discard the `CYPHER` prefix/index hint when a map parameter forces rewriting. A non-integral `1.5` survives, but integral floats still lose type: `RETURN 3.0 / 2.0 AS value` rewrites as `return 3 / 2 as value`. The original numeric-loss subclaim therefore remains alongside DAWGS-009. Vendor hint/prefix compatibility remains a contract question.

**Current trigger/output:** MATCH (n $props) RETURN apoc.coll.sum([1,2]), props={x:1}, rewrites to MATCH (n {x:$...}) RETURN apoc.collsum([1,2]); WHERE n:A:B becomes a disjunction.

**Evidence:** [drivers/neo4j/query_rewrite.go:51](drivers/neo4j/query_rewrite.go#L51), [cypher/models/cypher/format/format.go:648](cypher/models/cypher/format/format.go#L648), [cypher/models/cypher/format/format.go:704](cypher/models/cypher/format/format.go#L704).

**Validation level:** Current private rewriteQuery runtime probes via test overlays, including the integral-float control; output only, no Neo4j/APOC execution.

<a id="dawgs-061"></a>

### DAWGS-061: Kind and identity-property names lack escaping

**Status: applies.** The relationship upsert emitter still writes kinds and identity-property names verbatim. A current emitted MERGE contains s:User Account and object id:p.s.object id, proving valid names with spaces are not escaped. No injected update was executed or established.

**Current trigger/output:** cypherBuildRelationshipUpdateQueryBatch with kind "Some Kind", endpoint kind "User Account", identity property "object id" emits unquoted names.

**Evidence:** [drivers/neo4j/cypher.go:104](drivers/neo4j/cypher.go#L104), [drivers/neo4j/cypher.go:121](drivers/neo4j/cypher.go#L121), [drivers/neo4j/cypher.go:153](drivers/neo4j/cypher.go#L153).

**Validation level:** Current generated-output runtime probe via test overlay; live Neo4j execution not performed.

<a id="dawgs-063"></a>

### DAWGS-063: WithGraph drops existing indexes and constraints

**Status: applies.** An explicit graph not yet cached is still resolved through AssertGraph with the caller definition. Query.AssertGraph computes removals for existing indexes/constraints absent from that definition, including a name-only WithGraph. Ordinary ReadTransaction still avoids BEGIN unless isolation is configured, so the original implicit schema mutation mechanism persists.

**Current trigger/output:** Fresh manager: tx.WithGraph(graph.Graph{Name:"second"}).Nodes().Count() against a graph carrying property indexes/constraints.

**Evidence:** [drivers/pg/transaction.go:171](drivers/pg/transaction.go#L171), [drivers/pg/manager.go:501](drivers/pg/manager.go#L501), [drivers/pg/manager.go:529](drivers/pg/manager.go#L529), [drivers/pg/query/query.go:350](drivers/pg/query/query.go#L350), [drivers/pg/manager.go:306](drivers/pg/manager.go#L306).

**Validation level:** Current source-supported path; no live DDL effects or UpdateNodeBy failure re-executed.

<a id="dawgs-064"></a>

### DAWGS-064: Batch de-duplication keys collide

**Status: partial.** The primary PostgreSQL 1→23 versus 12→3 relationship-create collision is fixed with a structured endpoint/kind key and a regression test. NodeUpdate/RelationshipUpdate identity concatenation and Properties.HashInto structural-boundary issues remain in source. Neo4j AddKinds(A) and DeleteKinds(A) still produce the identical digest in a current runtime probe, allowing structurally different updates to share a batch.

**Current trigger/output:** nodeToNodeUpdateKey(AddKinds(A)) == nodeToNodeUpdateKey(DeleteKinds(A)) == 1371800463213966980; original PG endpoint pairs are now distinct.

**Evidence:** [drivers/pg/batch.go:777](drivers/pg/batch.go#L777), [drivers/pg/batch.go:803](drivers/pg/batch.go#L803), [drivers/pg/batch_test.go:56](drivers/pg/batch_test.go#L56), [graph/graph.go:167](graph/graph.go#L167), [graph/graph.go:194](graph/graph.go#L194), [drivers/neo4j/cypher.go:17](drivers/neo4j/cypher.go#L17), [drivers/neo4j/cypher.go:251](drivers/neo4j/cypher.go#L251), [graph/properties.go:490](graph/properties.go#L490).

**Validation level:** Current Neo4j helper runtime collision; PG regression test exists. Other identity/hash collisions source-supported unless independently probed elsewhere.

<a id="dawgs-065"></a>

### DAWGS-065: Kind mapping can deadlock the connection pool

**Status: applies.** A write transaction still owns a leased connection when CreateNode calls AssertKinds. Cache misses enter assertKinds under the schema write lock and call WriteTransaction, which acquires another pool connection. MapKind(s)/MapKindIDs misses similarly refresh through a new read transaction. Saturating the effective pool retains the reported wait cycle. Pool limits now come from runtime configuration, so preserve the instruction to verify effective limits instead of assuming a supplied pgx MaxConns.

**Current trigger/output:** N active transactions consume all N effective pool connections, then each registers a fresh kind; inner pool.Acquire cannot proceed until a connection is released/cancelled.

**Evidence:** [drivers/pg/manager.go:194](drivers/pg/manager.go#L194), [drivers/pg/manager.go:210](drivers/pg/manager.go#L210), [drivers/pg/manager.go:414](drivers/pg/manager.go#L414), [drivers/pg/manager.go:168](drivers/pg/manager.go#L168), [drivers/pg/transaction.go:197](drivers/pg/transaction.go#L197), [drivers/pg/pool_runtime.go:168](drivers/pg/pool_runtime.go#L168).

**Validation level:** Source-supported current lock/lease path; no database/pool saturation replay performed.

<a id="dawgs-066"></a>

### DAWGS-066: Changing the default graph is silently ignored after the first one

**Status: applies.** setDefaultGraph still returns immediately when hasDefaultGraph is true; SetDefaultGraph returns success after invoking it. New internal comments call the default immutable, but the public API still says it sets the default namespace and does not report rejection. Keep this as an API/contract discrepancy rather than a new database reproduction.

**Current trigger/output:** SetDefaultGraph(g2) after g1 has been set still reaches the successful no-op guard.

**Evidence:** [drivers/pg/manager.go:451](drivers/pg/manager.go#L451), [drivers/pg/manager.go:467](drivers/pg/manager.go#L467), [drivers/pg/manager.go:480](drivers/pg/manager.go#L480), [graph/graph.go:414](graph/graph.go#L414).

**Validation level:** Current source inspection; selected default/new-node graph IDs not rerun against a database.

<a id="dawgs-067"></a>

### DAWGS-067: AssertSchema never creates the non-default graphs it is given

**Status: applies.** assertSchema iterates Graphs only to define their node/edge kinds; it never asserts their graph definitions. Driver.AssertSchema separately asserts only DefaultGraph. Non-default requested graph partitions/indexes therefore still wait until a later explicit selection/assertion.

**Current trigger/output:** Schema{DefaultGraph:main,Graphs:[main,second]} creates/validates main through AssertDefaultGraph; no AssertGraph(second) is called.

**Evidence:** [drivers/pg/manager.go:554](drivers/pg/manager.go#L554), [drivers/pg/driver.go:274](drivers/pg/driver.go#L274), [drivers/pg/driver.go:285](drivers/pg/driver.go#L285), [graph/graph.go:411](graph/graph.go#L411).

**Validation level:** Current source-supported path; graph catalogue and indexes not rerun against a database.

<a id="dawgs-068"></a>

### DAWGS-068: Updating with no changes or nil properties panics

**Status: partial.** Nil node.Properties still triggers a deterministic nil-pointer panic in UpdateNode. An unchanged relationship still dispatches Exec("") with its ID, but the current pgxmock-backed probe returns nil and does not reproduce a panic for that subcase. Narrow the confirmed panic claim to nil properties; the empty-command path remains an unsupported/no-op behavior concern requiring a real driver check.

**Current trigger/output:** (&transaction{}).UpdateNode(graph.NewNode(1,nil)) panics before database access. UpdateRelationship with graph.NewProperties() and no tracked changes calls Exec("").

**Evidence:** [drivers/pg/transaction.go:222](drivers/pg/transaction.go#L222), [drivers/pg/transaction.go:289](drivers/pg/transaction.go#L289), [drivers/pg/transaction.go:329](drivers/pg/transaction.go#L329), [graph/properties.go:319](graph/properties.go#L319), [graph/properties.go:333](graph/properties.go#L333).

**Validation level:** Current nil-properties runtime panic and pgxmock-confirmed empty SQL dispatch; no real PostgreSQL empty-command behavior asserted.

<a id="dawgs-069"></a>

### DAWGS-069: SelectKindID loads the wrong SQL file

**Status: applies.** sqlSelectKindID still loads select_table_indexes.sql instead of the existing intended select_kind_id.sql. SelectKindID binds name, whereas the loaded statement needs tablename and returns index definitions. The helper remains wired incorrectly; the exact old "-1,no rows" outcome is not guaranteed for the current executor/parameter path.

**Current trigger/output:** query.On(tx).SelectKindID(A) sends SQL over pg_indexes with @tablename while supplying only name=A.

**Evidence:** [drivers/pg/query/sql.go:51](drivers/pg/query/sql.go#L51), [drivers/pg/query/query.go:289](drivers/pg/query/query.go#L289), [drivers/pg/query/sql/select_kind_id.sql:1](drivers/pg/query/sql/select_kind_id.sql#L1), [drivers/pg/query/sql/select_table_indexes.sql:1](drivers/pg/query/sql/select_table_indexes.sql#L1).

**Validation level:** Current source evidence; no database call run, so do not retain "always returns no rows" as a current measured error.

<a id="dawgs-070"></a>

### DAWGS-070: Neo4j batch.UpdateNodes ignores property deletions

**Status: applies.** The emitter still passes only the remaining Properties.Map to SET n += p.properties and never emits Properties.DeletedProperties. The existing integration test still asserts that HeatProperty exists after requesting its deletion; that is coverage of the defect, not a fix.

**Current trigger/output:** A node with tracked Properties.Delete("gone") emits only "unwind $p as p match (n) where id(n) = p.id set n += p.properties;" with an empty property map.

**Evidence:** [drivers/neo4j/cypher.go:282](drivers/neo4j/cypher.go#L282), [drivers/neo4j/cypher.go:319](drivers/neo4j/cypher.go#L319), [drivers/neo4j/batch_integration_test.go:119](drivers/neo4j/batch_integration_test.go#L119), [drivers/neo4j/batch_integration_test.go:137](drivers/neo4j/batch_integration_test.go#L137).

**Validation level:** Current generated-output runtime probe; integration test inspected but not executed.

<a id="dawgs-071"></a>

### DAWGS-071: Dumping an unknown graph name creates that graph

**Status: applies.** Dump snapshot/count and scans still select the supplied graph through WithGraph; uncached names resolve through assertGraph, which creates a graph on ErrNoRows. This remains the same source-supported manifestation of graph selection invoking schema assertion (DAWGS-063). The intended graph-selection contract remains relevant.

**Current trigger/output:** Dump a missing/typo graph name; countGraphEntitySnapshot calls WithGraph before Count and assertGraph chooses CreateGraph.

**Evidence:** [retriever/dump.go:576](retriever/dump.go#L576), [retriever/scan.go:241](retriever/scan.go#L241), [drivers/pg/transaction.go:171](drivers/pg/transaction.go#L171), [drivers/pg/manager.go:501](drivers/pg/manager.go#L501).

**Validation level:** Source-supported, no read-only dump or catalogue mutation run. Do not count as an independently replayed defect family.

<a id="dawgs-072"></a>

### DAWGS-072: Parallel fetch operations hang when a worker fails

**Status: applies.** Both private helpers still report a worker error then let that worker exit without cancelling the producer context. Current in-memory mock probes with max ID 50000 and one worker stay blocked after a controlled query/delegate error and return only after external cancellation. External BloodHound upgrade paths were not checked.

**Current trigger/output:** parallelFetchNodes(...maxID=50000,...workers=1) with Fetch returning controlled worker error, and parallelNodeQuery with controlled delegate error; both blocked at 100ms and released on cancel.

**Evidence:** [ops/ops.go:486](ops/ops.go#L486), [ops/ops.go:539](ops/ops.go#L539), [ops/ops.go:585](ops/ops.go#L585), [ops/parallel.go:283](ops/parallel.go#L283), [ops/parallel.go:334](ops/parallel.go#L334), [ops/parallel.go:368](ops/parallel.go#L368).

**Validation level:** Current Go helper runtime reproduction via overlay, no database; errors returned after cancellation in both probes.

<a id="dawgs-073"></a>

### DAWGS-073: ReachabilityCache returns incomplete reach sets

**Status: applies.** The root reach bitmap remains the global visited set while each intermediate cursor is cached. In the diamond/tail graph, a warmed cache still stores incomplete reach for node 3; the current runtime probe exactly reproduces [3,4] versus fresh [3,4,5]. Existing reach tests cover a linear SCC chain and pass without exercising this diamond. External CanRDP/consumer consequences were not revalidated.

**Current trigger/output:** Edges 1→2,1→3,2→4,3→4,4→5; Reach(1,outbound) then Reach(3,outbound) yields [3 4]; a fresh cache yields [3 4 5].

**Evidence:** [algo/reach.go:208](algo/reach.go#L208), [algo/reach.go:227](algo/reach.go#L227), [algo/reach.go:228](algo/reach.go#L228), [algo/reach_test.go:22](algo/reach_test.go#L22).

**Validation level:** Current Go runtime reproduction; existing TestReachabilityCache passed.

<a id="dawgs-074"></a>

### DAWGS-074: Traversal patterns changing direction never match

**Status: applies.** Driver still computes fetchDirection from currentExpansion and reuses it for nextExpansion. A current in-memory directional-cursor probe at b for a→b←c observes an inbound endpoint fetch for the inbound-T next step, where an outbound endpoint fetch is required; the fetched b is filtered as a cycle and returns zero descendants.

**Current trigger/output:** OutboundWithDepth(1,1,R).InboundWithDepth(1,1,T), with a→b←c: next-step observed FetchDirection=inbound, expected outbound; descendants=0.

**Evidence:** [traversal/traversal.go:199](traversal/traversal.go#L199), [traversal/traversal.go:225](traversal/traversal.go#L225), [traversal/traversal.go:230](traversal/traversal.go#L230).

**Validation level:** Current pattern Driver runtime probe with a fake relationship cursor; complete PostgreSQL BreadthFirst query not rerun.

<a id="dawgs-075"></a>

### DAWGS-075: Relationship-cache missing-ID slice

**Status: applies.** Current Go reproduction with a transaction/cursor double: requesting two absent IDs panics with slice bounds [-2:]. The implementation queries all requested IDs, then slices by the number missing from cache without checking fetched count. The issue is backend-independent at this helper layer; no live PostgreSQL replay was needed.

Evidence: [graphcache/cache.go:167](graphcache/cache.go#L167), [graphcache/cache.go:171](graphcache/cache.go#L171), [graphcache/cache.go:181](graphcache/cache.go#L181).

<a id="dawgs-076"></a>

### DAWGS-076: Bitmap mutation during And/AndNot iteration

**Status: applies.** Current Go reproduction: Bitmap64 {0..9}.And(ThreadSafeDuplex(empty)) leaves [1 3 5 7]; AndNot with a wrapped full set leaves the same values. The non-native Duplex branch removes from the collection being iterated. The 32-bit implementation has the same mechanism; HLL/simplex subclaim was not replayed.

Evidence: [cardinality/roaring64.go:94](cardinality/roaring64.go#L94), [cardinality/roaring64.go:132](cardinality/roaring64.go#L132), [cardinality/roaring32.go:95](cardinality/roaring32.go#L95), [cardinality/roaring32.go:134](cardinality/roaring32.go#L134).

<a id="dawgs-077"></a>

### DAWGS-077: PackedMap merge loses values and EFSet.Find underflows

**Status: applies.** Current Go reproduction: compact (2,102),(3,103), add (1,101), compact again, Get(1) returns 102,true. merge builds a new values slice but assigns a slice of the old s.values instead. Find(5) on EFSet {10,20,30} did not complete within 250ms; source uses unsigned midEntry-1 and highEntry=len-1, preserving the underflow mechanism. The bounded probe establishes noncompletion during that budget, not an exhaustive runtime proof of infinity.

Evidence: [container/pacmap.go:57](container/pacmap.go#L57), [container/pacmap.go:116](container/pacmap.go#L116), [container/ef.go:155](container/ef.go#L155).

<a id="dawgs-078"></a>

### DAWGS-078: BFSTreeFile gzip reader and SerializedSegment reconstruction

**Status: applies.** Current Go reproductions: SerializedSegment{Nodes:[1,2],Edges:[1]}.ToSegment panics at index -1. Reading a valid gzip file containing one serialized segment invokes zero callbacks and returns nil; ReadEach constructs a gzip.Reader but scans the underlying compressed file. This upgrades the original report's gzip subclaim from source-only evidence.

Evidence: [container/bfs.go:28](container/bfs.go#L28), [container/bfs.go:33](container/bfs.go#L33), [container/segment.go:133](container/segment.go#L133), [container/segment.go:141](container/segment.go#L141).

<a id="dawgs-079"></a>

### DAWGS-079: Pagination helper panics beyond result bounds

**Status: applies.** Current Go reproduction: SortAndSliceNodeSet(oneNode,0,10) panics with slice bounds [:10]. Skip and combined skip/limit likewise lack bounds checks. This validates the DAWGS helper only; historical BloodHound endpoint guards and direct caller behavior were not rechecked.

Evidence: [graph/util.go:19](graph/util.go#L19), [graph/util.go:26](graph/util.go#L26).

<a id="dawgs-080"></a>

### DAWGS-080: Nested sensitive keys survive preserved outer properties

**Status: applies.** Current private-helper probe: a map under extra retains nested email and ssn, with action preserve. planProperty classifies the outer value first and actionPreserve returns it unchanged. Keep the stated full-scrub expectation explicit: a deliberate PreserveKeys override may be intentional, but the original report's default unclassified-map behavior still occurs.

Evidence: [retriever/scrubber.go:295](retriever/scrubber.go#L295), [retriever/scrubber.go:310](retriever/scrubber.go#L310), [retriever/scrubber.go:417](retriever/scrubber.go#L417).

<a id="dawgs-081"></a>

### DAWGS-081: Case-insensitive domain scrub option unused

**Status: applies.** Current private-helper probe: CaseInsensitiveDomainNames defaults true, but CORP.LOCAL and corp.local with salt review-salt produce different host pseudonyms. Repository references show this option declared and decoded but not used to normalize pseudonym input. PreserveADSIDDomainPrefixes is also declaration-only; its observable effect was not separately replayed.

Evidence: [retriever/scrubber.go:53](retriever/scrubber.go#L53), [retriever/scrubber.go:61](retriever/scrubber.go#L61), [retriever/scrubber.go:651](retriever/scrubber.go#L651), [retriever/scrubber.go:692](retriever/scrubber.go#L692), [retriever/defaults.toml:14](retriever/defaults.toml#L14).

<a id="dawgs-082"></a>

### DAWGS-082: Neo4j temporal values serialize as empty objects

**Status: applies.** Current Go serialization reproduction using the exported Neo4j value mapper: native Date, LocalDateTime, LocalTime, and OffsetTime properties marshal as {"d":{},"dt":{},"lt":{},"t":{}}. The adapter preserves the driver property map and dump writes that map. A live Neo4j dump was not run.

Evidence: [drivers/neo4j/node.go:56](drivers/neo4j/node.go#L56), [retriever/dump.go:660](retriever/dump.go#L660), [retriever/dump.go:673](retriever/dump.go#L673).

<a id="dawgs-083"></a>

### DAWGS-083: Trailing slash unpack directory fails

**Status: applies.** Current private-helper reproduction with output out/ and force=false: staging/promotion fails with output-directory-not-empty. filepath.Dir(out/) resolves to out, placing staging inside the final output directory; output is trimmed but not cleaned. A full encrypted archive round trip with the trailing path was not run.

Evidence: [retriever/archive_envelope.go:344](retriever/archive_envelope.go#L344), [retriever/archive_envelope.go:353](retriever/archive_envelope.go#L353), [retriever/archive_envelope.go:390](retriever/archive_envelope.go#L390).

<a id="dawgs-084"></a>

### DAWGS-084: BDD gate ignores upstream failures

**Status: applies.** Source-supported current behavior: TestMain ignores m.Run() and fails only on bddDawgsFailed or invalid report, explicitly excluding upstream failures. This preserves the reporting concern. The original eight/twenty scenario counts and historical make test_all exit status are old evidence and were not reproduced; no BDD/integration suite ran.

Evidence: [bdd/steps_test.go:113](bdd/steps_test.go#L113), [bdd/steps_test.go:114](bdd/steps_test.go#L114), [bdd/steps_test.go:116](bdd/steps_test.go#L116).

<a id="dawgs-085"></a>

### DAWGS-085: Xor rendering loses builder grouping

**Status: applies.** Current generated-Cypher reproduction: And(Xor(a,b),c) renders where n.a=$p0 xor n.b=$p1 and n.c=$p2, with no parentheses around XOR. This changes builder grouping where AND binds more tightly. No live Neo4j row selection was tested. The original blanket PostgreSQL-XOR-unsupported exclusion is stale: cypher/models/pgsql/translate/predicate_test.go:82 now tests XOR translation.

Evidence: [query/model.go:186](query/model.go#L186), [query/model.go:192](query/model.go#L192).

<a id="dawgs-086"></a>

### DAWGS-086: Empty v2 KindIn drops relationship predicate

**Status: applies.** Current generated-Cypher reproduction: v2.New().Where(v2.KindIn(v2.Relationship())).Return(...).Build() renders match ()-[r]->() return r; using v2.Node() renders match (n) where  return n. The relationship case silently removes the empty-kind restriction and the node case emits an empty WHERE. Intended empty-kind semantics still merit a contract decision; invalid/omitted generated predicates are directly observed.

Evidence: [query/v2/compat.go:88](query/v2/compat.go#L88), [query/v2/compat.go:95](query/v2/compat.go#L95).

<a id="dawgs-087"></a>

### DAWGS-087: UpdateNodeBy ignores DeletedKinds on PostgreSQL

**Status: applies.** Source-supported current mechanism: identity-based node updates flow through FormatNodeUpsert, whose conflict clause only unions existing and excluded kind_ids and receives no deleted-kind IDs. DeletedKinds handling elsewhere serves update-by-ID, not this path. No live database update was replayed.

Evidence: [drivers/pg/batch.go:195](drivers/pg/batch.go#L195), [drivers/pg/batch.go:436](drivers/pg/batch.go#L436), [drivers/pg/query/format.go:155](drivers/pg/query/format.go#L155).

<a id="dawgs-089"></a>

### DAWGS-089: Kind-map getter returns shared map after unlocking

**Status: applies.** Source-supported current ownership flaw: GetKindIDsByKind returns s.kindIDsByKind directly after dropping its RLock; FetchKinds ranges that map outside the manager lock, while defineKinds can write it. Shared-map exposure is definite from code. A concurrent access/race-detector failure was not executed and remains pending.

Evidence: [drivers/pg/manager.go:203](drivers/pg/manager.go#L203), [drivers/pg/manager.go:206](drivers/pg/manager.go#L206), [drivers/pg/manager.go:224](drivers/pg/manager.go#L224), [drivers/pg/driver.go:307](drivers/pg/driver.go#L307).

<a id="dawgs-090"></a>

### DAWGS-090: Mixed-case index field causes repeated reconciliation

**Status: applies.** Source-supported current mechanism: IndexName retains field case, CREATE INDEX emits the index name unquoted, and reconciliation compares exact string map keys. PostgreSQL folds the emitted name, so requested objectId and observed objectid names can differ repeatedly. OID changes and DDL churn were not measured against a live database.

Evidence: [drivers/pg/model/format.go:27](drivers/pg/model/format.go#L27), [drivers/pg/query/format.go:53](drivers/pg/query/format.go#L53), [drivers/pg/query/query.go:360](drivers/pg/query/query.go#L360), [drivers/pg/query/query.go:372](drivers/pg/query/query.go#L372).

<a id="dawgs-092"></a>

### DAWGS-092: Neo4j nil-properties dereference

**Status: applies.** Source-supported current behavior: Neo4j create/update builders dereference properties.Map (and update property methods) without normalizing nil. In createRelationshipByIDs the dereference occurs while constructing the builder, before running the query. No live Neo4j call was executed; API acceptance of nil versus rejection should be explicit.

Evidence: [drivers/neo4j/transaction.go:234](drivers/neo4j/transaction.go#L234), [drivers/neo4j/transaction.go:240](drivers/neo4j/transaction.go#L240), [drivers/neo4j/transaction.go:265](drivers/neo4j/transaction.go#L265), [drivers/neo4j/batch.go:228](drivers/neo4j/batch.go#L228).

<a id="dawgs-093"></a>

### DAWGS-093: Neo4j execution uses context-free API

**Status: applies.** Source-supported current limitation: caller context governs limiter acquisition, but sessions, BeginTransaction and Run use the legacy context-free Neo4j interfaces. BeginTransaction applies a configured timeout, which is a separate control. Cancellation after query start remains a live-driver test gap; do not equate this source observation with a measured cancellation latency.

Evidence: [drivers/neo4j/driver.go:88](drivers/neo4j/driver.go#L88), [drivers/neo4j/driver.go:120](drivers/neo4j/driver.go#L120), [drivers/neo4j/transaction.go:157](drivers/neo4j/transaction.go#L157), [drivers/neo4j/transaction.go:298](drivers/neo4j/transaction.go#L298).

<a id="dawgs-094"></a>

### DAWGS-094: DatabaseSwitch settings, driver check and switch-state synchronization

**Status: partial.** Current Go recording-driver reproduction: SetWriteFlushSize(7)/SetBatchWriteSize(8) leave the wrapped driver counters at zero; `IsDriver[*driver](rawDriver)` returns false while the wrapped check returns true. inSwitch still has a write outside ctxLock and a read under ctxLock, supporting a race candidate. The unsynchronized race itself was not run under the race detector.

Evidence: [graph/switch.go:13](graph/switch.go#L13), [graph/switch.go:61](graph/switch.go#L61), [graph/switch.go:76](graph/switch.go#L76), [graph/switch.go:80](graph/switch.go#L80), [graph/switch.go:89](graph/switch.go#L89).

<a id="dawgs-095"></a>

### DAWGS-095: Optional following traversal step duplicates callbacks

**Status: applies.** Current Go reproduction with scripted directional cursors representing a-R->b-T->c: OutboundWithDepth(1,1,R).OutboundWithDepth(0,0,T) invokes the delegate for terminal c twice (IDs [3 3], five fetches). The optional preservation branch queues b while the eager next-step fetch already queues c, so T is expanded twice. Historical BloodHound final node-set correctness/deduplication was not rechecked.

Evidence: [traversal/traversal.go:219](traversal/traversal.go#L219), [traversal/traversal.go:229](traversal/traversal.go#L229), [traversal/traversal.go:239](traversal/traversal.go#L239).

<a id="dawgs-096"></a>

### DAWGS-096: Acyclic helpers replace caller ExpansionFilter

**Status: partial.** Source-supported current behavior: both AcyclicTraverseNodes and AcyclicTraverseTerminals unconditionally assign a new visited-bitmap ExpansionFilter, losing the caller filter. This direct replacement still applies. Graph-terminal versus pruned-terminal meanings, skip/limit counting and duplicate terminal behavior remain contract/fixture work and are not promoted to confirmed runtime defects.

Evidence: [ops/traversal.go:226](ops/traversal.go#L226), [ops/traversal.go:235](ops/traversal.go#L235), [ops/traversal.go:258](ops/traversal.go#L258), [ops/traversal.go:266](ops/traversal.go#L266).

<a id="dawgs-097"></a>

### DAWGS-097: BreadthFirst hides cancellation error

**Status: applies.** Current Go reproduction with a recording database and one driver callback: callback cancels caller context and returns context.Canceled; BreadthFirst returns nil while ctx.Err() is context canceled. Workers explicitly exclude cancellation errors and final return is only the error collector. This establishes suppressed cancellation, not counts for every affected caller or a full graph traversal.

Evidence: [traversal/traversal.go:331](traversal/traversal.go#L331), [traversal/traversal.go:356](traversal/traversal.go#L356).

<a id="dawgs-098"></a>

### DAWGS-098: FetchByQuery charges shallow result sizes

**Status: partial.** Current Go reproduction with a raw result double and 16,384-byte transaction limit: FetchByQuery returns a 65,536-byte string with nil error. size.Of(result.Literals) measures the slice descriptor, not contents; path accounting also remains shallow in source. The previous BloodHound/PostgreSQL service reproduction, default-budget impact and peak-memory claims were not revalidated in this checkout.

Evidence: [ops/ops.go:190](ops/ops.go#L190), [ops/ops.go:232](ops/ops.go#L232), [ops/ops.go:235](ops/ops.go#L235), [util/size/size.go:19](util/size/size.go#L19).

<a id="dawgs-099"></a>

### DAWGS-099: ThreadSafeDuplex cross-operation deadlock

**Status: applies.** Current synchronized Go reproduction: two gate providers wrap real Bitmap64 values; both ThreadSafeDuplex.Or calls acquire their own lock before gates release them to the native Or implementation. Each then waits on the opposing wrapper's Each lock and neither completes during 250ms. This deterministically exercises the source lock-order cycle; ungated real scheduling probability was not measured.

Evidence: [cardinality/lock.go:61](cardinality/lock.go#L61), [cardinality/lock.go:89](cardinality/lock.go#L89), [cardinality/roaring64.go:109](cardinality/roaring64.go#L109).

<a id="dawgs-100"></a>

### DAWGS-100: FetchNodeProperties ignores iteration error

**Status: applies.** Current Go result-double reproduction: one property row followed by an iteration failure returns nil, leaving a second node's properties nil. The callback returns nil instead of results.Error(). A database query that completes successfully would not exercise this failure path.

Evidence: [ops/ops.go:30](ops/ops.go#L30), [ops/ops.go:46](ops/ops.go#L46), [ops/ops.go:66](ops/ops.go#L66).

<a id="dawgs-101"></a>

### DAWGS-101: JSON import numbers round through float64

**Status: applies.** Current Go reproductions cover both ParseDocument and actual readNodeFragmentFile JSONL decoding: integer 9007199254740993 becomes float64 9007199254740992. Both decoders omit UseNumber, and arbitrary properties are map[string]any. Full live Load persistence was not tested; the decoding precision loss is backend-independent.

Evidence: [retriever/compression.go:347](retriever/compression.go#L347), [opengraph/load.go:59](opengraph/load.go#L59), [opengraph/load.go:62](opengraph/load.go#L62).

<a id="dawgs-102"></a>

### DAWGS-102: Directory manifests permit parent-relative fragment paths

**Status: applies.** Current private-helper probes: VerifyManifestFiles and verifyCollectionFragments accept ../outside.jsonl with valid checksum, byte counts and record counts. Source joins manifest paths to the input directory without containment checking. This verifies directory reads outside the root, not an end-to-end live database Load or a tar-sanitizer bypass.

Evidence: [retriever/manifest.go:67](retriever/manifest.go#L67), [retriever/manifest.go:70](retriever/manifest.go#L70), [retriever/load.go:758](retriever/load.go#L758), [retriever/load.go:798](retriever/load.go#L798).

<a id="dawgs-103"></a>

### DAWGS-103: In-query CALL omitted from generated SQL

**Status: applies.** Current compile reproduction with frontend.NewContext(): MATCH (n) CALL test.doNothing() RETURN n produces only a node-match CTE and select n, with no call. DefaultCypherContext rejects procedure invocations, so the omission applies to unrestricted parsing/compiler flow and must not be generalized to every entry point. No installed procedure or live procedure semantics were tested.

Evidence: [cypher/frontend/context.go:303](cypher/frontend/context.go#L303), [cypher/frontend/context.go:453](cypher/frontend/context.go#L453), [cypher/frontend/filter.go:22](cypher/frontend/filter.go#L22).

## Findings requiring further runtime validation

These original outcomes or measurements were not established against this checkout. Current source observations are retained to guide a focused check.

<a id="dawgs-039"></a>

### DAWGS-039: Plain DELETE silently detaches connected nodes

**Status: unverified.** translateDelete still ignores the detach flag, so plain and DETACH DELETE remain indistinguishable in generated SQL. However current formatting targets `node_<id>`, whereas delete_node_edges is a statement trigger installed only on the parent node table. That parent trigger is insufficient evidence that a partition-targeted deletion currently detaches edges. The original exact cascading outcome is therefore unverified/changed; missing plain-DELETE validation and DETACH behavior require a database regression check.

**Current trigger/output:** MATCH (n:A) DELETE n on a connected node.

**Evidence:** [cypher/models/pgsql/translate/delete.go:11](cypher/models/pgsql/translate/delete.go#L11), [drivers/pg/query/sql/schema_up.sql:290](drivers/pg/query/sql/schema_up.sql#L290).

**Validation level:** Current SQL generation in both optimizer modes plus source inspection; old report supplies prior database observation only. Do not carry forward silent-detach behavior as currently reproduced.

The trigger-scope assessment follows PostgreSQL's rule that statement triggers attach to the table explicitly named by the modifying statement. Applying that rule to the current partition-targeted SQL is a source/documentation inference, not a live deletion result. [PostgreSQL 18 CREATE TRIGGER documentation](https://www.postgresql.org/docs/18/sql-createtrigger.html#SQL-CREATETRIGGER-NOTES).

<a id="dawgs-088"></a>

### DAWGS-088: Concurrent kind insert can return no rows

**Status: unverified.** Candidate remains source-supported: a statement reads existing rows then INSERT ON CONFLICT DO NOTHING in one snapshot and unions both results. The conflict/snapshot race described in the original report is plausible, but it needs a synchronized two-session PostgreSQL reproduction. Do not count no-rows concurrency failure as a current reproduced defect.

Evidence: [drivers/pg/query/sql/insert_or_get_kind.sql:3](drivers/pg/query/sql/insert_or_get_kind.sql#L3), [drivers/pg/query/sql/insert_or_get_kind.sql:8](drivers/pg/query/sql/insert_or_get_kind.sql#L8), [drivers/pg/manager.go:218](drivers/pg/manager.go#L218).

<a id="dawgs-091"></a>

### DAWGS-091: Cursor Close does not join result producer

**Status: unverified.** The source still calls cancelFunc and underlying result.Close without waiting for the goroutine executing result.Next/marshaller to finish. This supports the shutdown race candidate, but an actual pgx data race or shutdown failure was not reproduced. Keep it pending rather than asserting a measured race.

Evidence: [graph/result.go:82](graph/result.go#L82), [graph/result.go:114](graph/result.go#L114).

<a id="dawgs-104"></a>

### DAWGS-104: Historical CREATE SQL expansion size

**Status: unverified.** The exact historical 77,765,308-byte output cannot be verified: variance.md omits the full 42,200-byte query, and this checkout bundles Create1 but not the referenced Create4 scenario. Translation code has since changed substantially. Retain as an unresolved scalability investigation with historical measurements, not a current measured defect or current confirmed size.

Evidence: [variance.md:5082](variance.md#L5082), [variance.md:5094](variance.md#L5094), [cypher/models/pgsql/translate/create.go:62](cypher/models/pgsql/translate/create.go#L62).

## Behaviors requiring a contract decision

Observable behavior alone does not choose the API promise. The following entries remain questions about the guarantee DAWGS intends to provide.

<a id="dawgs-037"></a>

### DAWGS-037: Pattern map parameters use jsonb containment

**Status: contract.** The jsonb-containment behavior remains, but equality semantics for this whole-map MATCH extension require an explicit support/compatibility contract.

Current trigger(s), run separately:

```cypher
MATCH (n:A $props) RETURN n
// props = {arr: ["x"]}
```

Both modes emit `n0.properties operator (pg_catalog.@>) @pi0::jsonb` with props {arr:["x"]}; this preserves the subset/containment mechanism.

The report itself identifies whole-map MATCH parameters as outside its baseline openCypher rule. Keep this as a current extension-contract question rather than an independently established language-conformance defect; no database match was executed.

Evidence: [cypher/models/pgsql/translate/property.go:106](cypher/models/pgsql/translate/property.go#L106).

<a id="dawgs-105"></a>

### DAWGS-105: AsNumeric truncates and wraps

**Status: contract.** Current Go reproduction: `AsNumeric[int](1.9)` gives 1,nil and `AsNumeric[int8](int64(300))` gives 44,nil. The direct casts still apply; no checked/lossless promise is documented here. Decide checked conversion versus ordinary Go conversion before classifying it as an implementation defect.

Evidence: [graph/mapper.go:32](graph/mapper.go#L32), [graph/mapper.go:54](graph/mapper.go#L54).

<a id="dawgs-106"></a>

### DAWGS-106: Batch flush groups operations by kind

**Status: contract.** Source-supported current behavior: tryFlush still flushes separate update/create/delete buffers in a fixed order, including relationship creates before relationship deletes. The interface expressly says Batch operations differ between databases and does not promise caller order. The original delete/recreate live result was not reexecuted; ordering remains a contract decision.

Evidence: [drivers/pg/batch.go:856](drivers/pg/batch.go#L856), [drivers/pg/batch.go:875](drivers/pg/batch.go#L875), [drivers/pg/statements.go:29](drivers/pg/statements.go#L29), [graph/graph.go:406](graph/graph.go#L406).

<a id="dawgs-107"></a>

### DAWGS-107: Explicit IDs do not adjust node sequence

**Status: contract.** Source-supported current behavior: explicit-ID bulk INSERT writes supplied IDs while automatic creation relies on the normal sequence; the shown explicit-ID statement has no setval/sequence synchronization. No isolated live sequence-collision case ran. Caller versus driver responsibility for explicit-ID import remains undecided.

Evidence: [drivers/pg/statements.go:4](drivers/pg/statements.go#L4), [drivers/pg/statements.go:18](drivers/pg/statements.go#L18).

<a id="dawgs-108"></a>

### DAWGS-108: Neo4j relationship upsert sets endpoint lastseen

**Status: contract.** Current source explicitly appends s.lastseen=datetime({timezone:'UTC'}), e.lastseen=... to generated relationship-update Cypher. This preserves the metadata overwrite concern, but live Neo4j effects were not replayed and whether lastseen is intentional ingestion metadata or caller-owned remains a contract decision.

Evidence: [drivers/neo4j/cypher.go:193](drivers/neo4j/cypher.go#L193).

<a id="dawgs-109"></a>

### DAWGS-109: Neo4j write-count transaction rotation

**Status: contract.** Source-supported current behavior: logWrites commits the current transaction when counted writes reach writeFlushSize and subsequent work opens a new transaction. runAndLog uses this path; arbitrary Raw Cypher statements are not counted equivalently. Atomicity across rotations remains a contract decision. Historical 100,000-write persistence and the suggested small-threshold failure scenario were not rerun.

Evidence: [drivers/neo4j/transaction.go:145](drivers/neo4j/transaction.go#L145), [drivers/neo4j/transaction.go:169](drivers/neo4j/transaction.go#L169), [drivers/neo4j/transaction.go:181](drivers/neo4j/transaction.go#L181), [drivers/neo4j/transaction.go:298](drivers/neo4j/transaction.go#L298).

<a id="dawgs-110"></a>

### DAWGS-110: AssertSchema owns entire Neo4j schema and rejects composites

**Status: contract.** Source still rejects multi-label/multi-property composite indexes, and reconciliation drops present definitions absent from required schema names. This applies as explicit AssertSchema behavior; ownership/support are API choices rather than an independently established corruption defect. No live schema mutation was performed.

Evidence: [drivers/neo4j/index.go:190](drivers/neo4j/index.go#L190), [drivers/neo4j/index.go:222](drivers/neo4j/index.go#L222), [drivers/neo4j/index.go:236](drivers/neo4j/index.go#L236).

<a id="dawgs-111"></a>

### DAWGS-111: Kinds.Remove changes shared slice storage

**Status: contract.** Current Go reproduction: two nodes built from one A,B,C slice share storage; deleting A from one leaves the other with [B C C]. NewNode stores the variadic slice and Remove uses append over the existing backing array. Kinds.Copy exists, so ownership/copy guarantees still need a decision.

Evidence: [graph/kind.go:76](graph/kind.go#L76), [graph/node.go:26](graph/node.go#L26).

<a id="dawgs-112"></a>

### DAWGS-112: Failed multi-batch Load leaves state and prevents ordinary retry

**Status: contract.** Source-supported current behavior: loader checks empty targets and flushes successive batches through separate BatchOperation calls. A later database failure can follow committed earlier batches; ordinary retry then sees a nonempty target. An exact failure/retry fixture was not run. Atomic/resumable load or documented cleanup remains a recovery-contract decision.

Evidence: [retriever/load.go:189](retriever/load.go#L189), [retriever/load.go:622](retriever/load.go#L622), [retriever/load.go:633](retriever/load.go#L633).

<a id="dawgs-113"></a>

### DAWGS-113: ParseDocument permits unknown keys and trailing bytes

**Status: contract.** Current Go reproduction independently tests misspelled grpah and valid graph followed by garbage: both return nil error and an empty graph. Decoder performs one Decode without DisallowUnknownFields/EOF check; Validate explicitly permits empty graphs. Strict whole-document/schema acceptance remains a contract decision.

Evidence: [opengraph/load.go:59](opengraph/load.go#L59), [opengraph/load.go:62](opengraph/load.go#L62), [opengraph/validate.go:24](opengraph/validate.go#L24).

## Original findings resolved in the current implementation

These dispositions address the original reported mechanisms and triggers. Database fixtures were not rerun. Related cases and current limitations are explicitly preserved.

<a id="dawgs-025"></a>

### DAWGS-025: Bound right nodes cross-join the node table

**Status: resolved.** The dual-bound fallback now directly correlates both endpoints to the edge and does not join an unconstrained terminal node.

Current trigger(s), run separately:

```cypher
MATCH (a {name: 'a1'}), (b {name: 'c1'}) MATCH (a)-[:R*1..1]->(b) RETURN count(*) AS c
MATCH (x:A), (y:B) WITH x, y MATCH (x)-[:R]->(y) RETURN count(*)
MATCH (x:A), (y:B) WITH x, y OPTIONAL MATCH (x)-[:R]->(y) RETURN count(*)
```

Both modes now emit endpoint equalities. The optimized exact range and fixed/WITH/optional variants join only edge e0 on `(previous.n0).id = e0.start_id and (previous.n1).id = e0.end_id`; generic expansion has an explicit prior-terminal = next_id predicate.

Focused bound-endpoint unit tests passed. Database fixture counts were not rerun; the separate repeated-input OPTIONAL MATCH defect DAWGS-024 remains.

Evidence: [cypher/models/pgsql/translate/traversal.go:250](cypher/models/pgsql/translate/traversal.go#L250), [cypher/models/pgsql/translate/traversal.go:429](cypher/models/pgsql/translate/traversal.go#L429), [cypher/models/pgsql/translate/traversal.go:619](cypher/models/pgsql/translate/traversal.go#L619), [cypher/models/pgsql/translate/traversal_test.go:13](cypher/models/pgsql/translate/traversal_test.go#L13), [cypher/models/pgsql/translate/optimizer_safety_test.go:2079](cypher/models/pgsql/translate/optimizer_safety_test.go#L2079).

<a id="dawgs-029"></a>

### DAWGS-029: Variable-length steps ignore an earlier binding of their terminal

**Status: resolved.** Expansion projection now explicitly correlates an already bound terminal with next_id.

Current trigger(s), run separately:

```cypher
MATCH (a)-[:R]->(b)-[:R*1..2]->(a) RETURN a.name AS a, b.name AS b
```

Both modes include `(s0.n0).id = s2.next_id` in completed-state projection, as well as the bound-root correlation. The original variable a can no longer be rebound to arbitrary reached nodes by this path.

The focused regression test passed across outbound, inbound, undirected-prefix, zero-depth, path-valued, and WITH-carried variants. Integration fixtures exist but were not run.

Evidence: [cypher/models/pgsql/translate/expansion.go:4225](cypher/models/pgsql/translate/expansion.go#L4225), [cypher/models/pgsql/translate/expansion_binding_test.go:14](cypher/models/pgsql/translate/expansion_binding_test.go#L14), [integration/testdata/templates/bound_expansion_endpoint_shapes.json:4](integration/testdata/templates/bound_expansion_endpoint_shapes.json#L4).

<a id="dawgs-032"></a>

### DAWGS-032: A first-hop self-loop stops variable-length expansion

**Status: resolved.** Ordinary variable-length expansion now initializes is_cycle to false even when its first edge is a loop.

Current trigger(s), run separately:

```cypher
MATCH (x)-[*2..3]->(y) RETURN x.name AS x, y.name AS y
```

Both modes emit primer `select e0.start_id, e0.end_id, 1, false, false, array [e0.id] ...`; recursive expansion rejects reused edge IDs, without marking an initial self-loop as a terminal cycle.

The focused ordinary-expansion self-loop unit test passed. The original live fixture was not run; shortest-path self-endpoint policies are a separate behavior.

Evidence: [cypher/models/pgsql/translate/expansion.go:4592](cypher/models/pgsql/translate/expansion.go#L4592), [cypher/models/pgsql/translate/optimizer_safety_test.go:423](cypher/models/pgsql/translate/optimizer_safety_test.go#L423).

<a id="dawgs-034"></a>

### DAWGS-034: `*0..0` also returns one-hop neighbours

**Status: resolved.** Zero-only expansion now disables its one-edge primer explicitly.

Current trigger(s), run separately:

```cypher
MATCH (x {name:'a'})-[*0..0]->(y) RETURN y
```

Both modes emit the zero-depth seed with root_id=next_id, the primer with `where false`, and recursion guarded by `depth < 0 and ... depth > 0`.

Only the zero-hop origin can survive the current SQL. The focused zero-depth-builder unit test passed; no database neighbor query was executed.

Evidence: [cypher/models/pgsql/translate/expansion.go:3384](cypher/models/pgsql/translate/expansion.go#L3384), [cypher/models/pgsql/translate/expansion_test.go:323](cypher/models/pgsql/translate/expansion_test.go#L323).

<a id="dawgs-038"></a>

### DAWGS-038: DELETE before aggregating WITH is omitted

**Status: resolved.** Nonfinal WITH now builds deletions, restores the input projection frame, and stages writes as sequential statements. Current node and relationship probes both emit DELETE in the first materialization statement; the later aggregate is retained. The original omission is removed.

**Current trigger/output:** MATCH (n:OCReportDelete) WITH n, n.num AS num DELETE n WITH sum(num) AS total RETURN total; relationship equivalent also checked.

**Evidence:** [cypher/models/pgsql/translate/with.go:37](cypher/models/pgsql/translate/with.go#L37), [cypher/models/pgsql/translate/multipart.go:129](cypher/models/pgsql/translate/multipart.go#L129), [drivers/pg/multipart.go:30](drivers/pg/multipart.go#L30), [cypher/models/pgsql/translate/multipart_test.go:14](cypher/models/pgsql/translate/multipart_test.go#L14).

**Validation level:** Current SQL generation reproduced in both optimizer modes; existing multipart unit tests passed. Database side effects were not rerun.

<a id="dawgs-044"></a>

### DAWGS-044: SET, REMOVE, or CREATE before WITH generates invalid SQL

**Status: resolved.** Nonfinal SET, REMOVE, and CREATE now produce staged statement execution. The data-modifying CTEs are top-level within their materialization statements, followed by a separate read stage, removing the original invalid nesting.

**Current trigger/output:** MATCH (n:A) SET n.k = 1 WITH n RETURN n; REMOVE n.k and CREATE (n:A) variants also checked.

**Evidence:** [cypher/models/pgsql/translate/query.go:66](cypher/models/pgsql/translate/query.go#L66), [cypher/models/pgsql/translate/multipart.go:129](cypher/models/pgsql/translate/multipart.go#L129), [drivers/pg/multipart.go:30](drivers/pg/multipart.go#L30), [drivers/pg/multipart_test.go:120](drivers/pg/multipart_test.go#L120), [cypher/models/pgsql/translate/multipart_test.go:14](cypher/models/pgsql/translate/multipart_test.go#L14).

**Validation level:** Current SQL generation reproduced in both optimizer modes; multipart boundary and driver-mock stage tests exist. Focused translator multipart tests passed; no live write transaction rerun.

<a id="dawgs-048"></a>

### DAWGS-048: WITH n, n AS m generates invalid SQL

**Status: resolved.** WITH now creates a distinct binding for the alias while both projections reference the original materialized input frame. The primary original example now emits SELECT s1.n0 AS n0, s1.n0 AS n1 and a valid final s0 projection.

**Current trigger/output:** MATCH (n:A) WITH n, n AS m RETURN n, m.

**Evidence:** [cypher/models/pgsql/translate/with.go:127](cypher/models/pgsql/translate/with.go#L127), [cypher/models/pgsql/translate/with.go:142](cypher/models/pgsql/translate/with.go#L142), [cypher/models/pgsql/translate/with.go:164](cypher/models/pgsql/translate/with.go#L164).

**Validation level:** Current SQL generation checked in both optimizer modes; no missing-FROM reference remains. Database result not rerun.

<a id="dawgs-050"></a>

### DAWGS-050: Unaliased expressions lose their result-column name

**Status: resolved.** translateProjectionItem now synthesizes an implicit alias by formatting the original Cypher expression; expression-shaped aliases are quoted for SQL. The original query now emits AS "p.id" rather than an unnamed SQL expression.

**Current trigger/output:** CREATE (p {id:12}) RETURN p.id emits ... AS "p.id". Other probe output also names sum(n.big), avg(n.big), count(*), toString(nm), collect(n.x).

**Evidence:** [cypher/models/pgsql/translate/projection.go:1768](cypher/models/pgsql/translate/projection.go#L1768), [cypher/models/pgsql/format/format.go:25](cypher/models/pgsql/format/format.go#L25), [drivers/pg/result.go:61](drivers/pg/result.go#L61).

**Validation level:** Current SQL generation checked in both optimizer modes; identifier-format unit test passed; live Result.Keys not rerun.

<a id="dawgs-062"></a>

### DAWGS-062: PostgreSQL reads and deletes ignore the target graph

**Status: resolved.** Graph-scoped queries now resolve the transaction target and render concrete `node_<id>`/`edge_<id>` partitions for reads and writes. Current probes for target 17 emit only node_17/edge_17, including DELETE. The old all-graphs count/delete/dump mechanism is removed. Separate source-only caveat: the parent-node statement trigger deletes from the parent edge table by endpoint ID alone despite graph-scoped node identity. A parent-table deletion with colliding explicit node IDs merits investigation. The trigger is installed on the parent node table, so partition-targeted DELETE may not invoke it; do not attribute that hypothetical cascade to the now-scoped WithGraph query. No current parent-delete runtime check was performed.

**Current trigger/output:** Original WithGraph(gb).Nodes().Delete()/Count() now compile through targetGraphID and WithTargetGraph; current DELETE SQL targets node_17.

**Evidence:** [drivers/pg/transaction.go:185](drivers/pg/transaction.go#L185), [drivers/pg/transaction.go:254](drivers/pg/transaction.go#L254), [drivers/pg/transaction.go:336](drivers/pg/transaction.go#L336), [cypher/models/pgsql/translate/format.go:20](cypher/models/pgsql/translate/format.go#L20), [cypher/models/pgsql/format/format.go:492](cypher/models/pgsql/format/format.go#L492), [cypher/models/pgsql/translate/graph_scope_test.go:14](cypher/models/pgsql/translate/graph_scope_test.go#L14), [drivers/pg/query/sql/schema_up.sql:290](drivers/pg/query/sql/schema_up.sql#L290).

**Validation level:** Current generated SQL and graph-scope unit tests passed; multi-graph live dump/load and trigger edge-ID overlap not rerun.

## Additional observations from the original report

These 22 notes are separate from the 113 numbered dispositions. Feature gaps and unsettled ownership or cancellation contracts remain distinct from demonstrated result defects.

### FetchRelationshipNodes rejects self-loop

**Disposition: Applies.** Source still requires exactly two fetched nodes although a self-loop has one distinct endpoint. The focused probe did not execute this specific helper.

Evidence: [ops/ops.go:425](ops/ops.go#L425), [ops/ops.go:430](ops/ops.go#L430).

### DirectionBoth origin and inconsistent deletion views

**Disposition: Applies.** Current Go reproduction: one 1→2 edge gives DirectionBoth adjacency [1 2] at origin 1; after DeleteEdge, AdjacentNodes is empty but EachAdjacentEdge still emits that edge.

Evidence: [container/triplestore.go:155](container/triplestore.go#L155), [container/triplestore.go:178](container/triplestore.go#L178), [container/triplestore.go:198](container/triplestore.go#L198).

### Cache and adjacency graph report node counts for edges

**Disposition: Applies.** Current Go reproductions: one cached node and zero relationships gives RelationshipsCached=1; a two-node/one-edge adjacency graph gives NumEdges=2.

Evidence: [graphcache/cache.go:38](graphcache/cache.go#L38), [container/adjacencymap.go:98](container/adjacencymap.go#L98).

### PathSegment.Slice allocation/index mismatch

**Disposition: Applies.** Current Go reproduction: Slice on a depth-zero root panics at index -1. Allocation is Depth entries, while iteration includes the root, i.e. Depth+1 entries.

Evidence: [graph/path.go:209](graph/path.go#L209).

### TSStatelessBFS emits maxDepth+1

**Disposition: Applies.** Current Go reproduction: 1→2→3 with maxDepth=1 emits terminal node 3 at distance 2; the depthExceeded test uses maxDepth<Distance, allowing expansion at equality.

Evidence: [container/traversal.go:99](container/traversal.go#L99), [container/traversal.go:117](container/traversal.go#L117).

### AcyclicNodeFilter comment promises global first visit

**Disposition: Contract decision.** Implementation checks per-path IsCycle only and delegates to the supplied filter. The comment promises traversal to a node only once without maintaining a global visited set. Documentation/contract mismatch still applies; no corrupted output is claimed.

Evidence: [traversal/traversal.go:466](traversal/traversal.go#L466).

### Node.Merge on nil receiver Properties

**Disposition: Applies.** Current Go reproduction: NewNode(...,nil).Merge(nodeWithNonemptyProperties) panics with nil pointer dereference. Properties.Merge tolerates nil other but dereferences nil receiver when other is nonempty.

Evidence: [graph/node.go:50](graph/node.go#L50), [graph/node.go:65](graph/node.go#L65), [graph/properties.go:237](graph/properties.go#L237).

### split of empty string

**Disposition: Needs validation.** Translation still directly maps split to PostgreSQL string_to_array; no live PostgreSQL empty-string result was queried. Historical [] result and exact reference semantics were not independently revalidated, so retain as a source-supported candidate.

Evidence: [cypher/models/pgsql/translate/function.go:842](cypher/models/pgsql/translate/function.go#L842).

### Identifier backtick round trips

**Disposition: Partially applies.** The blanket note must be narrowed: property lookup/map keys are now unescaped and covered by explicit tests for embedded backticks and whitespace. SymbolicName visitor still obtains raw token text for other paths, so variable/label round-trip concerns require distinct fixtures; no global current backtick defect is established.

Evidence: [cypher/frontend/literal.go:49](cypher/frontend/literal.go#L49), [cypher/frontend/property_key_test.go:13](cypher/frontend/property_key_test.go#L13), [cypher/frontend/property_key_test.go:60](cypher/frontend/property_key_test.go#L60).

### Formatter omits AND between Where.Expressions

**Disposition: Applies.** Current Go direct-AST reproduction with two Where.Expressions emits adjacent predicate text (n.a = $n.b = $), with no AND. The emitter loops without separators. This is a formatter AST-contract result; normal conjunction-building callers may avoid it.

Evidence: [cypher/models/cypher/format/format.go:306](cypher/models/cypher/format/format.go#L306), [cypher/models/cypher/format/format.go:313](cypher/models/cypher/format/format.go#L313).

### Hex/octal, Unicode escapes and parameterized SKIP/LIMIT

**Disposition: Applies.** Current probes: 0x10 and valid 0o10 reject as invalid integer literals. Unicode string \u0041 parses but translation rejects invalid escape \u; parameterized SKIP parses but translation rejects expected literal skip. These remain clean feature gaps; the old description should distinguish parse-time from translation-time rejection. LIMIT was not separately probed.

Evidence: [cypher/frontend/literal.go:104](cypher/frontend/literal.go#L104), [cypher/models/pgsql/translate/translator_test.go:124](cypher/models/pgsql/translate/translator_test.go#L124), [cypher/models/pgsql/translate/projection.go:1830](cypher/models/pgsql/translate/projection.go#L1830).

### ScanNextResult target arity unchecked

**Disposition: Applies.** Current Go result-double reproduction with one value and zero targets panics at index 0. The source still iterates values and indexes targets without checking length.

Evidence: [graph/query.go:20](graph/query.go#L20), [graph/query.go:27](graph/query.go#L27).

### Non-path map accepted as empty Path

**Disposition: Applies.** Source-supported: FromMap returns nil if both nodes and edges keys are absent, and TryMap therefore succeeds before converting to an empty Path. No focused value-mapper runtime probe was run for this item.

Evidence: [drivers/pg/types.go:397](drivers/pg/types.go#L397), [drivers/pg/types.go:402](drivers/pg/types.go#L402), [drivers/pg/mapper.go:234](drivers/pg/mapper.go#L234).

### MapStringAnyToJSONB modifies caller map

**Disposition: Applies.** Current Go reproduction: caller map x contains nil []string before conversion, but nonnil empty []string afterward, with nil error. Whether mutation is permitted still depends on ownership guarantees.

Evidence: [cypher/models/pgsql/type.go:38](cypher/models/pgsql/type.go#L38), [cypher/models/pgsql/type.go:47](cypher/models/pgsql/type.go#L47).

### SetBatchWriteSize unsynchronized package global

**Disposition: Needs validation.** Source-supported: setter writes package-level batchWriteSize directly. A synchronized concurrent setter/batch test under -race was not executed, so retain the race claim as pending.

Evidence: [drivers/pg/driver.go:158](drivers/pg/driver.go#L158), [drivers/pg/driver.go:160](drivers/pg/driver.go#L160).

### Neo4j errors may surface after initial check

**Disposition: Needs validation.** Initial Error checks precede consumption in several call paths; Close Consume discards its error. This preserves the lazy-error concern, but error attribution to the next statement needs a driver-result test or live failure and was not demonstrated.

Evidence: [drivers/neo4j/result.go:44](drivers/neo4j/result.go#L44), [drivers/neo4j/result.go:52](drivers/neo4j/result.go#L52), [drivers/neo4j/result.go:65](drivers/neo4j/result.go#L65), [drivers/neo4j/transaction.go:181](drivers/neo4j/transaction.go#L181).

### SinglePartQuery order sensitivity and replacing Filter

**Disposition: Applies.** Current Go AST-rendering probes: Limit(1) before Return disappears; two successive Where clauses retain only the second predicate. Skip/Order follow the same Return-present guard in source. Caller-order/filter-composition guarantees should be documented before labeling every such use a defect.

Evidence: [query/model.go:577](query/model.go#L577), [query/model.go:591](query/model.go#L591), [drivers/neo4j/node.go:136](drivers/neo4j/node.go#L136), [drivers/pg/node.go:12](drivers/pg/node.go#L12).

### DatabaseSwitch nested-transaction deadlock

**Disposition: Needs validation.** Source still holds currentDBLock.RLock while executing user delegates, and Switch takes its write lock. Nested read locking can interact with a waiting writer, but no controlled nested-transaction schedule was run. Also account for inSwitch rejecting new contexts when designing the fixture.

Evidence: [graph/switch.go:118](graph/switch.go#L118), [graph/switch.go:131](graph/switch.go#L131), [graph/switch.go:66](graph/switch.go#L66).

### IPv4 pseudonym space only 24 bits

**Disposition: Contract decision.** Source emits `10.<byte>.<byte>.<byte>` from six hex digest characters, i.e. a 24-bit output space. This is a measurable design fact, not proof of a collision or unmet security/uniqueness promise. Collision guarantees require an explicit contract.

Evidence: [retriever/scrubber.go:677](retriever/scrubber.go#L677).

### Node MarshalJSON does not imply round-trip promise

**Disposition: Contract decision.** Node has a custom marshaler; NodeSet has an unmarshaler, but Node lacks one. Absence alone is not a defect without an advertised round-trip contract; preserve this qualification.

Evidence: [graph/node.go:101](graph/node.go#L101), [graph/node.go:271](graph/node.go#L271).

### Low-level TSBFS/TSDFS caller cycle policy

**Disposition: Contract decision.** Low-level traversals accept caller-supplied descent filters and max depth; unlimited cyclic walks without cycle filtering are not by themselves a defect. Trails/simple-path/arbitrary-walk and termination guarantees remain a design question.

Evidence: [container/traversal.go:8](container/traversal.go#L8), [container/traversal.go:53](container/traversal.go#L53).

### Component/reachability cancellation completeness

**Disposition: Contract decision.** The original report gives no concrete fixture and incomplete-return/cancellation guarantees are not established. No such runtime case was reproduced. Constructors return no error slot, and component construction checks context while returning a value; keep this as a contract question only.

Evidence: [algo/scc.go:14](algo/scc.go#L14), [algo/scc.go:289](algo/scc.go#L289), [algo/reach.go:72](algo/reach.go#L72).

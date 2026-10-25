# Open-ended traversal policy — DAWGS-019

Engineering decision: **2026-10-24**, against checkout **`56413e1`**.

**Select complete traversal semantics, with explicit errors when execution cannot finish within its resources.** An omitted upper bound must not become a successful search of only the first 15 levels. Callers can request a semantic depth limit with an explicit Cypher upper bound. A resource budget limits execution and can cause failure; it does not change which paths the query asks for.

This completes the design action in the [priority ledger](../variance_findings_priority.md). Implementation and rollout remain open. Current PostgreSQL execution still applies the depth-15 cap. This decision does not change the Neo4j driver or claim downstream application validation.

## Selected contract

| Query form | Successful execution after repair |
| --- | --- |
| Ordinary `*`, `*m..` | Evaluate every required relationship-unique trail at or above the minimum, with the query's predicates, projections, multiplicity and pagination. Nodes may repeat; a relationship cannot repeat within the same MATCH. |
| `shortestPath` with an open maximum | Return one valid minimum-length trail for each required endpoint pair, or establish that no qualifying trail exists. Existing [tie policy](shortest_path_tie_policy.md) applies. |
| `allShortestPaths` with an open maximum | Return every qualifying relationship-distinct trail at the minimum satisfying depth for each required endpoint pair. Discovery must complete the relevant layer and witness enumeration. |
| Explicit `*m..n`, `*..n`, or exact `*n` | Preserve the requested inclusive bounds. Exhausting the semantic upper bound is ordinary completion. |
| Search budget exhausted before completion | Report an identifiable resource-limit error. An empty result, OPTIONAL MATCH null, incomplete count, or success after partial writes is not a valid substitute. |

Preserve supported zero-hop, direction, bound-endpoint, predicate and MATCH uniqueness semantics. This policy adds no support for shortest-path minimums greater than one; Neo4j rejects those forms in the controlled checks. A relationship-simple search on a finite statement snapshot has finite path space, but that space can grow exponentially. A node visited once is sufficient only for independently qualified shortest-search shapes; it is not a valid ordinary-path deduplication rule.

Streaming rows remain provisional until the result is closed/drained and its final error checked. A late limit or cancellation error must remain observable, and a failed write query must prevent transaction commit even if the delegate ignores its result error. Resource accounting and error handling must cover every stage of a multipart write. These guarantees require driver validation; they are not provided by this documentation change.

Normal query semantics still permit early completion, for example an unordered read with `LIMIT 1` after producing an admissible row. Pagination must not bypass work required by the query's ordering, aggregation, observed all-shortest multiplicity, or mutation target selection. A shortest search can finish after resolving all required pairs and completing the necessary layers; it need not exhaust irrelevant deeper trails.

## Controlled runtime evidence

The fixture is a directed chain of **18 nodes and 17 relationships**: `n0 → n1 → … → n17`. Every node has kind `VarianceOpenNode` and property `name` equal to its fixture ID. Only `n0` also has kind `VarianceOpenStart`. Every relationship has kind `VarianceOpenStep`, with no additional relationships or properties. Create the fixture inside an explicit write transaction and force rollback after each observation. Parameter values are the created database IDs for `n0` and the selected endpoint.

The read queries are:

```cypher
MATCH p = (a:VarianceOpenStart)-[:VarianceOpenStep*]->(b:VarianceOpenNode)
WHERE id(a) = $start_id AND id(b) = $end_id
RETURN length(p)

MATCH p = shortestPath((a:VarianceOpenStart)-[:VarianceOpenStep*]->(b:VarianceOpenNode))
WHERE id(a) = $start_id AND id(b) = $end_id
RETURN length(p)

MATCH p = allShortestPaths((a:VarianceOpenStart)-[:VarianceOpenStep*]->(b:VarianceOpenNode))
WHERE id(a) = $start_id AND id(b) = $end_id
RETURN length(p)
```

For each query, select `n15`, `n16`, then `n17`; also substitute `*1..`, `*0..`, `*1..15`, `*1..16` and `*1..17`. Ordinary traversal additionally checks `*16..` and exact `*16`. Repeat all three families with `RETURN p` for `*` and `*1..17`, checking the complete ordered node sequence and relationship count for every returned path. The shortest families intentionally use minimums zero or one.

Run these two writes separately, without RETURN, on fresh rollback fixtures:

```cypher
MATCH (a:VarianceOpenStart)-[:VarianceOpenStep*]->(b:VarianceOpenNode)
WHERE id(a) = $start_id
SET b.selected = true

MATCH (a:VarianceOpenStart)-[:VarianceOpenStep*]->(b:VarianceOpenNode)
WHERE id(a) = $start_id
CREATE (:VarianceOpenAudit)
```

Inspect `MATCH (n:VarianceOpenNode) WHERE n.selected = true RETURN count(n)` or `MATCH (n:VarianceOpenAudit) RETURN count(n)` before rollback. Register the audit kind before running the CREATE check.

| Observation | PostgreSQL, both compiler modes | Neo4j |
| --- | --- | --- |
| Open maximum, endpoint at depth 15 | One row in each family | One row in each family |
| Open maximum, endpoint at depth 16 or 17 | Zero rows in each family; no error | One row at the requested depth |
| Ordinary `*16..`, endpoint at depth 16 or 17 | Zero rows; no error | One row at the requested depth |
| Explicit `*1..16` / `*1..17` | Reach endpoints within the stated bound | Reach the same endpoints |
| Exact ordinary `*16` | Only `n16` | Only `n16` |
| Full paths with `*1..17` | Correct ordered nodes and 15/16/17 relationships in every family | Same logical paths |
| Open-ended SET without RETURN | 15 selected nodes | 17 selected nodes |
| Open-ended CREATE without RETURN | 15 audit nodes | 17 audit nodes |

The investigation executed **80 variants per PostgreSQL compiler mode and 80 on Neo4j**, using race-enabled Go tests and an investigation-only source overlay. PostgreSQL queries used `TranslateWithOptions`, `TranslatedStatements` and `tx.Raw` to select the compiler mode; Neo4j queries used `tx.Query`. This does not exercise PostgreSQL's default query-cache entry point. All 240 executions completed without database errors. Comparison with Neo4j identifies **28 truncation discrepancies in each PostgreSQL mode**, plus the eight scalar discrepancies below when optimization is enabled. The probe logs observations; its successful exit is not a conformance pass. No core shared expectations or production code were changed. All fixture data and writes rolled back; kind registration is normal schema setup. No service replay or resource/performance qualification is claimed from this small chain.

### Separate observation: optimized all-shortest scalar length

For singleton endpoints, optimized PostgreSQL `allShortestPaths` with minimum one returns `length(p) = 0` even when the nonzero path is present. The eight affected scalar controls are the depth-15 open/default-minimum cases and the successful explicit-bound cases at depths 15–17. Disabled translation and Neo4j report the actual length. `*0..` and `RETURN p` provide correct lengths/path contents in these probes.

A minimal positive control is:

```cypher
MATCH p = allShortestPaths((a:VarianceOpenStart)-[:VarianceOpenStep*1..16]->(b:VarianceOpenNode))
WHERE id(a) = $start_id AND id(b) = $end_id
RETURN length(p)
```

With `end_id` identifying `n16`, optimized translation returns one row containing **0**, disabled translation and Neo4j return **16**, and `RETURN p` returns the correct 16-relationship path in all three executions. This is an independent projection discrepancy, not evidence that explicit traversal stopped early. Root cause and repair remain open. Preserve this control in the implementation's scalar/path matrix instead of using the optimized scalar as a completeness oracle. This new observation does not alter the original triage counts.

## Implementation boundary and cost

The cap has several independent consumers. Updating one constant or replacing 15 with a large integer cannot implement this contract.

| Area | Current source and required follow-on |
| --- | --- |
| Ordinary recursion | [expansion.go](../cypher/models/pgsql/translate/expansion.go), `expansionConstraints`: preserve an absent `ExpansionOptions.MaxDepth`, recurse until the admissible frontier is exhausted, and retain the relationship-trail cycle/uniqueness guards and pending predicates. |
| Shortest planning | [lowering_plan.go](../cypher/models/pgsql/optimize/lowering_plan.go), `defaultShortestPathExpansionDepth`, and [depth provenance](../cypher/models/pgsql/optimize/lowering.go): represent an open maximum explicitly. Diagnostics must distinguish unbounded syntax, explicit bounds and execution budgets. Stop materializing the policy default as an explicit maximum. |
| Harness calls | [expansion.go](../cypher/models/pgsql/translate/expansion.go), `shortestPathsParameters`, `bidirectionalShortestPathsParameters` and `buildCompactBoundShortestPathsRoot`: pass the same depth semantics to every generic, compact and fallback executor. |
| SQL algorithms and schema | [schema_up.sql](../drivers/pg/query/sql/schema_up.sql): version the harness contract to carry a bound-present flag separately from its numeric value. Open searches terminate on frontier exhaustion or completed shortest discovery. Update every forward/bidirectional and compact loop, shallow preflight and fallback. Validate depth arithmetic and representation overflow; existing `int4` loops cannot safely stand in for arbitrary `int64` syntax bounds. |
| Resource accounting | Add an execution-budget policy spanning generated search states, retained frontier/predecessors, path enumeration and output accounting, plus cancellation/deadline handling. Depth alone cannot bound a shallow high-fan-out search. Counter increments must reject overflow before allocation/admission. |
| Driver and staging | [transaction.go](../drivers/pg/transaction.go), [multipart.go](../drivers/pg/multipart.go) and result adapters: bind budgets on every execution, preserve late errors, prevent successful commit after a failed query, and carry consumed budgets through stages and fallback. |
| Cache and policy identity | Version semantic/schema/selector identities and reject incompatible promotion manifests. Any policy value affecting SQL, strategy or admission must either be bound at execution or included in cache identity. A cached translation cannot restore a hidden depth default. |

Use an explicit bound-present flag for the SQL boundary, with a validated numeric value only when that flag is true. Do not overload zero, a large sentinel, or SQL NULL as the complete implementation: several existing harnesses are `STRICT`, and their finite-depth loops require coordinated replacement. Keep existing finite-bound harnesses callable during schema migration; enable new compiler calls only against the matching schema contract.

Existing candidate state/frontier caps generally select an incumbent fallback. They are local strategy guards, not a query-wide resource ceiling. The fallback must retain the original open/finite semantics and the remaining execution budget. A switch to today's depth-15 incumbent cannot count as complete fallback. Until requalified, an executor whose admission relies on a finite maximum, including finite-depth manifest buckets, must decline an open maximum and use a qualified executor or return an explicit unsupported/resource error.

A budget allows successful completion exactly at its limit; detect the next required admission before dropping work. Reaching an execution-depth budget with an exhausted admissible frontier can succeed, while a required deeper continuation must fail explicitly. Absence of a matching terminal within that budget does not prove frontier exhaustion. Apply the same rule to explicitly bounded queries if their required work exceeds an execution budget. Account for duplicate input rows and all traversal steps instead of granting each root or fallback a fresh query budget.

`GraphQueryMemoryLimit` currently guards Go traversal/path containers in `ops` and `traversal`; it does not account for the PostgreSQL recursive CTE or harness workspaces used by `tx.Query`. State/output counters likewise do not establish a bound on total PostgreSQL resident memory or temporary files. Qualify backend statement, workspace and spill behavior separately. Production budget values must be measured on the acceptance corpus; this design does not promote the experimental 100,000-state constants into global defaults or invent an equivalent replacement depth cap.

The selected repair is **L**, covering compiler/optimizer, versioned SQL/schema functions, driver failure handling and the shared regression matrix. Shortest/all-shortest executor qualification and operational resource measurements are additional L work within that delivery program. Ordinary trail enumeration can retain exponentially many valid paths; all-shortest predecessor storage can be compact while witness enumeration still grows exponentially. A strict bound on whole-server memory or redesign of storage/path enumeration would be **XL** and is outside this policy's guarantee. These sizes qualify scope, not elapsed time or an additive estimate.

## Migration and delivery

Ship the changed default as a documented, versioned behavior correction after all three traversal families and failure paths qualify. Until then, documentation must continue to identify the current cap. An implementation landing only rejection/guarding is containment; an ordinary-only fix does not close DAWGS-019.

Existing callers can receive more rows, different counts/order/OPTIONAL MATCH bindings, and additional SET/CREATE/DELETE targets. Audit raw queries, builders and templates using omitted maxima, including negated predicates and multipart writes, before rollout. A caller whose intended query really ends at 15 must write `*..15` or `*m..15`. Raising an explicit upper bound changes the semantic query; raising an execution budget permits more work on the same query. Do not add a permanent silent-truncation compatibility mode.

Implement in this order:

1. Define budget options, accounting units and identifiable error mapping for the driver and standalone translation tools. Qualify concrete defaults against small and adversarial fixtures before enabling them; preserve existing public entry points.
2. Add versioned SQL functions with an explicit open/finite depth contract, safe arithmetic and budget-aware failure. Preserve bounded callers during schema deployment.
3. Wire ordinary and shortest/all-shortest planning, formatting, caches, fallback and multipart execution to that contract. Keep unqualified candidate executors out of open searches.
4. Add the equivalent shared semantic/mutation corpus and driver-scoped resource/error tests below, fix or explicitly contain the separate scalar discrepancy, and measure resource/runtime costs before switching the default.

## Acceptance and rollout gate

Core cases and templates must use identical expectations on both backends. Cover chains at 0, 1, 14, 15, 16, 17 and well above 15 hops; open/minimum-only/finite/exact syntax; empty/disconnected graphs; genuine cycles, self-loops and repeated nodes without repeated relationships; inbound/undirected paths; fixed prefixes/suffixes, comma parts, bound endpoints and path predicates; duplicate inputs, OPTIONAL MATCH, DISTINCT, counts and pagination. Shortest cases must include multiple endpoint pairs, tied shortest witnesses, zero hops and predicate-sensitive paths. Compare complete logical path identities and scalar projections, respecting the singleton tie policy.

For writes, check exact committed node/relationship post-state, untouched decoys and row cardinality with and without RETURN and through WITH boundaries. Force resource/cancellation failure after provisional rows and after earlier multipart writes, including an ignored result error, and prove that the transaction cannot commit partial work. A shallow query on a graph containing an irrelevant long chain must still be able to complete when its required result is established.

Keep PostgreSQL-specific budget exhaustion, context cancellation, fallback-budget accounting, numeric overflow and installed-schema/cache/manifest compatibility tests driver-scoped. Test cap-minus-one, cap and cap-plus-one at discovery, frontier, predecessor, enumeration and output boundaries. Check late errors, explicit upper bounds larger than 15, exhausted frontiers at an execution-depth budget, and fallback after partial candidate work. Do not weaken core cases with backend-specific skips or expected assertions.

Measure cold/warm compilation and runtime, retained/generated states, peak backend workspace/spill behavior, allocations and output volume for long chains, shallow fan-out, converging diamonds, bounded cycles and many tied shortest paths. Include successful completion and forced-budget termination. Establish operational defaults and report remaining regressions before rollout; the 17-edge probe supplies correctness evidence only.

Run parser/analyzer mutation and SQL golden generation through the existing workflow, both PostgreSQL optimizer modes, and the selected-backend full unit/integration/BDD gates. DAWGS-019 remains an open runtime defect until the semantic, error and resource evidence satisfies this gate.

# PostgreSQL Translation

DAWGS translates supported Cypher queries to vanilla PostgreSQL 18 SQL. The implementation lives under
[`cypher/models/pgsql`](../cypher/models/pgsql).

## Package Layout

- `format`: PostgreSQL SQL rendering.
- `translate`: openCypher-to-PostgreSQL translation.
- `optimize`: Cypher query-shape analysis and translator lowering decisions.
- `visualization`: PUML graph formatting for PostgreSQL SQL model trees.
- `test`: translation test cases.

## Optimizer Coverage

The `optimize` package analyzes Cypher query shape before PostgreSQL SQL emission. Rule outputs are exposed as planned
and applied lowerings in translation diagnostics so plan-corpus captures can catch planning/emission gaps.

Current PostgreSQL optimization coverage includes:

- Reproducible plan-corpus capture for PostgreSQL translated SQL, PostgreSQL `EXPLAIN`, Neo4j logical plan operator
  trees, planned/applied lowerings, skipped lowerings, and skipped-lowering reasons.
- Count-store fast paths for simple node and directed-edge count queries, including typed variants where kind filters
  map cleanly.
- Predicate placement accounting for binding-scope predicates pushed into fixed traversal steps, expansion seeds,
  expansion edges, expansion terminal checks, and eligible pattern predicates.
- Shared and late path materialization for path functions such as `nodes(p)`, `relationships(p)`,
  `size(relationships(p))`, `startNode`, `endNode`, and `type`.
- Recursive traversal optimizations for endpoint kind/property predicates, relationship type predicates, bound-node
  filters, traversal direction selection, and limit pushdown where ordering and distinct semantics permit it.
- Expansion suffix pushdown and `ExpandInto` detection for fixed suffixes and shared-endpoint fanout patterns.
- Strict string property equality lowering through `jsonb_typeof(properties -> key) = 'string'` plus
  `properties ->> key = value`, preserving JSON scalar semantics while allowing existing text expression indexes on
  selective fields such as `objectid` and `name`.
- Typed relationship count plans that can use the narrow `kind_id` edge index for filtering.
- Correlated relationship `EXISTS` lowering for typed pattern predicates when relationship types and endpoint
  correlations are sufficient.
- Membership-only `collect(entity)` ID-array lowering with `id = any(...)` membership predicates.
- Shortest-path strategy and terminal-filter planning for selective endpoint predicates and kind-only terminal filters.
- Exact anonymous directed fixed-range expansion lowering for non-shortest-path `*1..1` and `*2..2` patterns. These
  shapes use fixed traversal steps instead of recursive CTEs, preserve path projection semantics, and enforce
  relationship uniqueness across emitted fixed steps. The explicit SQL-size cap is depth 2; broader exact ranges
  continue through the recursive expansion path. Undirected exact ranges are not eligible for this lowering.
- Predicate-only `ANY`/`NONE` over current path `relationships(p)` bindings lowered to `EXISTS` or `NOT EXISTS` over
  path edge IDs, avoiding full `edgecomposite[]` materialization when the final projection does not require it.
- Dependency-safe clause reordering inside non-optional read regions, using existing selectivity heuristics while
  preserving stable tie order and pinning clauses with unresolved external dependencies.

## Indexing Notes

Exact string property equality is emitted with a JSON string type guard and `properties ->>` extraction. This allows
indexes created on expressions such as `properties ->> 'objectid'` and `properties ->> 'name'` to accelerate selective
anchors without matching JSON booleans or numbers.

The baseline edge indexes are:

| Purpose | Key columns | Included columns |
| --- | --- | --- |
| Primary key and edge lookup | `(id, graph_id)` | None |
| Edge uniqueness and outbound traversal | `(start_id, kind_id, end_id, graph_id)` | `(id)` |
| Inbound traversal | `(end_id, kind_id)` | `(id, start_id)` |
| Edge-type counts and deletes | `(kind_id)` | None |

The unique and inbound indexes cover topology-only recursive expansion, including the edge IDs used for path
construction and edge-reuse checks. They also support endpoint lookups without a kind restriction. Index-only scans
can avoid heap access when vacuum has marked the relevant pages all-visible. Edge property predicates still need
property access. The narrow kind index accelerates typed filtering and can cover direct edge-type counts, but Cypher
relationship counts that join both endpoint nodes may need heap reads to obtain endpoint IDs.

`schema_up.sql` replaces both historical edge uniqueness constraints and drops the three superseded covering indexes,
including their attached partition indexes. The replacement retains the same uniqueness columns, so existing
column-based `ON CONFLICT` clauses remain valid. The first upgrade locks the affected tables and rebuilds indexes;
allow a maintenance window for large installations. Reapplying the schema preserves the new indexes rather than
rebuilding them. The PostgreSQL schema index integration tests cover fresh creation, populated upgrades from both
historical constraint orders, repeated application, upserts, and recursive index-only plan eligibility.

Substring and suffix predicates are not promoted to blanket schema indexes. PostgreSQL deployments can request explicit
`TextSearchIndex`/trigram property indexes for fields that need `CONTAINS`, `STARTS WITH`, or `ENDS WITH`. Dynamic
parameter/property forms that lower to helper functions remain outside the hard index-match contract until their
lowering changes.

## Translation Cache

The PostgreSQL driver keeps one bounded, driver-wide compilation cache with a default capacity of 256 rendered SQL
statements. It serves both `Transaction.Query` and the legacy programmatic query builder used by relationship and node
queries. A warm builder hit avoids optimization, lowering-plan construction, PostgreSQL AST construction, and SQL
rendering; it still builds the request AST, deterministically names its runtime parameters, renders the canonical
Cypher cache identity, and assembles its debug comment.

Entries are partitioned by the SHA-256 digest of trimmed Cypher text, target graph ID, sorted parameter names and PostgreSQL type shapes, a
translation-key format/policy identity, and the schema generation. Parameter values are never retained or used as key
material, and the cache key does not retain the source text. The retained data is limited to the SQL string and generated-parameter-to-Cypher-parameter source names;
the cache does not retain caller maps or values, ASTs, contexts, transactions, connections, rows, or connection
strings. Structural literals remain part of the cache identity. Generated parameters that cannot be reconstructed from
request-local named sources, translation errors, disabled/closed cache state, and source text over 64 KiB bypass
retention.

Configure the bounded cache through the PostgreSQL driver constructor:

```go
database := pg.NewDriverWithOptions(0, pool, pg.DriverOptions{
	TranslationCacheEntries: 0, // disables cache lookup and retention
})
```

For a process-wide rollback, disable optimized translation directly and restore the prior state when appropriate:

```go
previous := pg.SetOptimizedTranslation(false)
defer pg.SetOptimizedTranslation(previous)
```

When disabled, newly started PostgreSQL compilations bypass cache lookup and retention and translate the original AST
with no PostgreSQL rewrite rules or lowering decisions. The setting is atomic and applies to every PostgreSQL driver in
the process; each compilation snapshots it at entry, so already-running compilations continue with their selected path.
Cached optimized entries remain dormant and are reusable after re-enabling. A zero cache capacity still runs optimized
translation without retaining entries. The default remains optimized translation with a 256-entry cache.

Concurrent cacheable misses for the same key share one translation. Waiters behind a failed, canceled, or non-cacheable
build continue independently rather than serializing behind repeated failed work. Statistics are available through
`(*pg.Driver).TranslationCacheStats()` and expose aggregate hits, build-leader misses, coalesced requests, bypasses,
unoptimized compilations, insertions, evictions, binding/build failures, live size, capacity, and generation; they never
expose query or value data. Successful schema assertions and kind refreshes advance the schema generation and discard completed entries.
External schema or type changes cannot be detected automatically; recreate the driver/pool after those changes. Driver
close retires the cache before the PostgreSQL pool closes.

## Validation Workflow

Optimizer changes should include focused optimizer/lowering tests, SQL-shape translation tests, and backend-equivalent
integration coverage when behavior affects query semantics. `make test_all` is the default full validation target when
`CONNECTION_STRING` is available.

Run plan-corpus capture for planner, lowering, or SQL-emission changes:

```bash
make plan_corpus
```

The corpus summary should be checked for PostgreSQL cost, `Recursive Union`, `SubPlan`, `Function Scan on unnest`, and
skipped-lowering deltas.

PostgreSQL property index regression coverage is hard-failing under the `manual_integration` tag. The synthetic plan
test translates Cypher to PostgreSQL, disables sequential scans for the `EXPLAIN`, and requires explicit node property
indexes to appear in the JSON plan:

```bash
CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./integration -run TestPostgreSQLPropertyIndexPlans
```

PostgreSQL-only plan-corpus validation should confirm that `ExactRangeExpansion` and `PathRelationshipPredicate` are
planned and applied for their supported cases without skipped entries for either lowering.

## MERGE

The PostgreSQL driver requires version 18 or newer, including callers supplying their own pool. Constructor signatures
remain unchanged; connection hooks and transaction acquisition reject older servers before schema creation or queries.
CI uses PostgreSQL 18.

MERGE snapshots incoming bindings before introducing pattern variables. A materialized input CTE evaluates and validates match
values once on both branches, including cached translations. Fixed maps project one JSONB column per AST key; dynamic
maps retain runtime map validation. Matching uses these columns, including the typed text RHS of indexed string lookups.
Fixed property objects are assembled only in the creation branch. A read branch searches for the **complete** pattern. If
that search fails for an input row, the creation branch allocates identifiers for its unbound nodes and relationships.
Partially matching unbound nodes are not reused. Undirected patterns match either orientation and create relationships
from the left endpoint to the right. Relationship types must be singular; variable-length relationships and already
bound relationship declarations are rejected. Previously declared nodes, including repeated variables within a pattern, must be referenced without redeclaring labels or properties.

Conditional property/label SET clauses and ordinary SET immediately following MERGE use a materialized patch projection
per clause. Each effective RHS is evaluated once against the incoming clause frame. The patch helper splits top-level
null removals from values to set, applying `(properties - removed_keys) || set_values` once per binding/clause.
Nested nulls survive. Final fixed match keys are projected from patches independently of unrelated payloads.
Candidate composites are conservatively assembled at clause boundaries for entity, property, and path consumers before native
`MERGE ... RETURNING` writes. Right-hand sides within one SET clause observe that clause's incoming values. Separate
SET clauses are processed in order. Null assignments remove the selected property; null match properties raise an
error. Changed rows are returned by native MERGE; unchanged matches use `DO NOTHING` and are carried through a read
branch with `UNION ALL`, preserving all existing matches without a dummy UPDATE. Named paths use the returned entity
composites. A singleton materialized guard demands every actual input value through a full aggregate scan and checks candidate
conflicts. Each write source depends on that guard and filters to effective actions. The first native MERGE that can create an entity consumes a private sentinel source row whose DO NOTHING predicate
references the guard. The INSERT action keeps PostgreSQL from pruning that source, even with LIMIT 0 or no actual
entity actions. The sentinel has no target ID and produces no RETURNING row. When all MERGE entities are bound, a
separate native MERGE anchor consumes the guard even without RETURN or with empty write sources. Its unmatched INSERT predicate
is `NOT guard.valid`; the assertion returns true or raises, so no anchor row is inserted and no sequence is consumed.
PostgreSQL prunes a MERGE with only DO NOTHING actions, and can prune an UPDATE-only sentinel source whose target ID
is statically null. Neither form anchors validation. The separate anchor requires INSERT
permission on `node` and invokes INSERT statement triggers; it invokes no row triggers. Database errors roll back the query.
Bound entities with no mutation actions emit no entity write statement; unchanged results use their candidate values.
String property predicates retain the JSON type guard and text equality expression used by property indexes.
Match properties copied from existing entities retain their JSON types, including numbers, booleans, and arrays.
Incoming named paths are materialized from their components before being carried into a subsequent MERGE.

### First iteration limits

This iteration defers visibility of earlier mutations to later clauses. All CTEs share the
statement snapshot. Projected composites can be carried through WITH and RETURN, but a later MATCH cannot discover
newly inserted rows and later separate mutations cannot safely modify a row already written in this statement.
Ordered execution remains the separate extension described in `merge_gaps_plan.md`; this pipeline retains one statement
snapshot and its established rejection contract.

Repeated unchanged matches preserve input cardinality. Before native writes, a materialized candidate guard rejects
repeated target writes, including writes through distinct bindings, with SQLSTATE 22023 and an explicit
`MERGE requires ordered execution` error. Single-node creations accept multiple absent inputs only when neither input
can match the other input's final created properties. Property values are compared exactly, including arrays. Multiple
absent inputs for complete patterns are conservatively rejected. Target conflicts group only graph, entity type, and
target ID across effective actions using UNION ALL. Preserved fixed keys group the complete original JSONB tuple and
count distinct input IDs; changed keys join original tuples to final tuples, excluding the same input. Empty fixed
maps explicitly conflict when more than one input is absent. Dynamic maps use relational per-key comparisons, including
subset maps and exact arrays, and may still require pairwise work. No JSON candidate envelope is built. Narrow key
relations exclude unrelated payload properties, while the mutation/result candidate relation still carries full entity
composites. No input actions are silently discarded. Run rejected inputs as separate database commands until ordered execution is implemented.

The edge uniqueness constraint on `(start_id, kind_id, end_id, graph_id)` is unchanged. A property-qualified merge
requiring another relationship with that tuple raises SQLSTATE 23505 and rolls back; conflicting properties are never
accepted as a match. Concurrent MERGE has PostgreSQL's native conflict behavior, with no automatic lock/recheck or
retry. Concurrent relationship insertions may raise 23505; concurrent node creations without a unique property
constraint may create duplicates. Applications requiring serialization must coordinate transactions externally.
The batch/upsert storage contract remains unchanged; removing relationship uniqueness is a separate schema change.

Shared integration cases and templates run against both backends. PostgreSQL-only tests verify cache hits, runtime
validation, index usage, unchanged row versions, and storage conflicts. Use:

```sh
make format
make test_update
CONNECTION_STRING="$PG_CONNECTION_STRING" make test_all
CONNECTION_STRING="$NEO4J_CONNECTION_STRING" make test_all
CONNECTION_STRING="$PG_CONNECTION_STRING" go test -tags manual_integration ./integration \
  -run '^$' -bench BenchmarkPostgreSQLMerge -benchtime=1s -count=3
```

The semantic execution experiments on PostgreSQL 18.6 showed that a sibling table scan sees zero rows after a MERGE
insert CTE, that DO NOTHING emits no RETURNING row, and that a volatile PL/pgSQL helper can see its preceding insert
and update. The helper was removed from this iteration after the scope change. These observations agree with
[PostgreSQL MERGE](https://www.postgresql.org/docs/18/sql-merge.html) and
[CTE snapshot behavior](https://www.postgresql.org/docs/18/queries-with.html).

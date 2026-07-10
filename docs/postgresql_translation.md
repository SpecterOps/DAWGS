# PostgreSQL Translation

DAWGS translates supported Cypher queries to vanilla PostgreSQL 16 SQL. The implementation lives under
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
- Typed relationship count plans that can use the `kind_id`-first covering edge index.
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

Simple relationship count fast paths depend on the schema's `kind_id`-first edge index for efficient typed counts.

Substring and suffix predicates are not promoted to blanket schema indexes. PostgreSQL deployments can request explicit
`TextSearchIndex`/trigram property indexes for fields that need `CONTAINS`, `STARTS WITH`, or `ENDS WITH`. Dynamic
parameter/property forms that lower to helper functions remain outside the hard index-match contract until their
lowering changes.

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

# Cypher to PostgreSQL Optimization Plan

This plan tracks optimization and rewrite work identified by running the shared integration corpus against Neo4j and PostgreSQL and comparing plan shapes.

## Phase 1: Baseline And Tooling

Status: completed

- Keep a reproducible plan-capture workflow.
  - Capture PostgreSQL translated SQL, PostgreSQL `EXPLAIN`, Neo4j logical plan operator trees, and optimizer planned/applied lowerings.
  - Read `integration/testdata/cases` and `integration/testdata/templates`.
  - Write comparable JSONL output without changing product behavior.
- Add plan-summary reporting.
  - Rank cases by PostgreSQL estimated cost.
  - Count plan operators, recursive CTEs, subplans, path materialization indicators, and optimizer lowerings.
  - Produce markdown and JSON summaries.

## Phase 2: Quick Wins

Status: completed

- Add count-store fast paths for simple count queries:
  - `MATCH (n) RETURN count(n)`
  - `MATCH ()-[r]->() RETURN count(r)`
  - `MATCH (...) RETURN count(*)` for the same exact node and directed-edge shapes.
  - Typed variants where kind filters map cleanly.
  - Implemented as `CountStoreFastPath` lowering for exact node and directed-edge count shapes.
- Audit the planned/applied `PredicatePlacement` gap.
  - Distinguish missing translator consumption from intentional skipped placements.
  - Add explicit skipped-placement reasons when a planned lowering is not applied.
  - Plan-corpus summaries now report skipped lowerings and skipped-lowering reasons.

## Phase 3: Path Materialization

Status: completed

- Share path materialization for repeated path functions.
  - Target `nodes(p)`, `relationships(p)`, `size(relationships(p))`, `startNode`, `endNode`, and `type`.
  - Avoid repeated `SubPlan` and `Function Scan on unnest` work per path binding.
  - Materialize unprojected paths once through a lateral stage when final projections return a path and its components, or repeat node-bearing component expressions.
- Expand late path materialization coverage.
  - Ensure paths are built only when needed for projection, filtering, or mutation semantics.

## Phase 4: Traversal And Recursive CTEs

Status: completed

- Push predicates into recursive traversal anchors and steps where semantics allow.
  - Endpoint kind/property predicates.
  - Relationship type predicates.
  - Bound-node filters.
- Improve traversal direction selection using endpoint selectivity.
  - Bound IDs.
  - Labels/kinds.
  - Equality predicates.
  - Finite relationship type sets.
  - Plan direction flips for right-endpoint binding predicates from `WHERE`, not only inline node constraints.
- Broaden limit pushdown for variable-length path queries when ordering and distinct semantics permit early termination.

## Phase 5: Suffix And Shared Endpoint Rewrites

Status: completed

- Improve expansion suffix pushdown for fixed suffixes after variable-length traversals.
  - Include fixed suffix steps that terminate at already-bound endpoints with inline node constraints.
  - Preserve bound-endpoint constraints in the pushed terminal satisfaction check when present.
- Improve `ExpandInto` and shared endpoint rewrites for ADCS-style fanout patterns.
  - Constrain earlier using bound endpoint semi-joins or correlated expansion lowering where valid.

## Phase 6: Validation

Status: completed

- Add focused regression tests per optimization.
  - Optimizer/lowering selection tests.
  - SQL shape translation tests.
  - Backend-equivalent integration tests.
  - Template corpus setup now clears stale graph data before rollback-only fixture cases, keeping repeated PostgreSQL and Neo4j validation runs deterministic.
- Benchmark after each workstream.
  - Run unit tests.
  - Run backend-specific integration tests.
  - Run plan capture and compare summary deltas.
  - `quality_backend` passes against `postgres://postgres:bhe4eva@localhost/bhe` and `neo4j://neo4j:neo4jj@localhost:7687`.
  - Plan corpus capture records 396 PostgreSQL plans and 396 Neo4j plans; remaining capture errors are expected invalid-query cases surfaced by both systems or Neo4j-specific parameter-map syntax rejection.

## Phase 7: Predicate Placement Accounting

Status: completed

- Record planned binding-scope predicate placements when traversal constraint consumption actually pushes the matching predicate into a fixed traversal step, expansion seed, expansion edge, or expansion terminal constraint.
- Keep skipped-lowering reports focused on predicates that were not consumed by the emitted translation shape, instead of marking already-pushed traversal predicates as skipped.
- Add SQL-shape regression tests for fixed traversal and expansion-root predicate consumption.
- Refreshed plan-corpus capture applies `PredicatePlacement` in 56 of 71 planned PostgreSQL cases, reducing skipped predicate placements from 65 to 15.

## Phase 8: Cross-Clause Predicate Placement Planning

Status: completed

- Stop planning traversal predicate placements for binding predicates owned by a different `MATCH` clause.
- Preserve same-clause binding predicate placement for traversal and suffix pushdown decisions.
- Refreshed plan-corpus capture now plans and applies `PredicatePlacement` in the same 56 PostgreSQL cases, removing all skipped predicate-placement reports.

## Phase 9: Live Dataset Assumption Checks

Status: completed

- Re-vet optimizer assumptions against a large live PostgreSQL graph with `EXPLAIN ANALYZE`.
- Exact string property anchors now lower to `jsonb_typeof(properties -> key) = 'string'` plus `properties ->> key = value`,
  allowing existing `->>` expression indexes on selective fields such as `objectid` and `name` to be used without
  matching JSON booleans or numbers.
- Relationship count fast paths remain endpoint-preserving for correctness, but the PostgreSQL schema now includes a
  `kind_id`-first covering edge index so typed relationship counts have a direct access path instead of relying on
  endpoint-oriented traversal indexes.
- Added PG-scoped manual integration coverage for strict string equality and a read-only live-plan check that asserts
  indexed `objectid` lookups use a PostgreSQL index when the connected database exposes the expected expression index.

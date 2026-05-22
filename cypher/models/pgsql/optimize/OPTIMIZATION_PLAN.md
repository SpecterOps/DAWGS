# Cypher to PostgreSQL Optimization Plan

This plan tracks optimization and rewrite work identified by running the shared integration corpus against Neo4j and PostgreSQL and comparing plan shapes.

## Phase 1: Baseline And Tooling

Status: in progress

- Keep a reproducible plan-capture workflow.
  - Capture PostgreSQL translated SQL, PostgreSQL `EXPLAIN`, Neo4j logical plan operator trees, and optimizer planned/applied lowerings.
  - Read `integration/testdata/cases` and `integration/testdata/templates`.
  - Write comparable JSONL output without changing product behavior.
- Add plan-summary reporting.
  - Rank cases by PostgreSQL estimated cost.
  - Count plan operators, recursive CTEs, subplans, path materialization indicators, and optimizer lowerings.
  - Produce markdown and JSON summaries.

## Phase 2: Quick Wins

Status: pending

- Add count-store fast paths for simple count queries:
  - `MATCH (n) RETURN count(n)`
  - `MATCH ()-[r]->() RETURN count(r)`
  - Typed variants where kind filters map cleanly.
- Audit the planned/applied `PredicatePlacement` gap.
  - Distinguish missing translator consumption from intentional skipped placements.
  - Add explicit skipped-placement reasons when a planned lowering is not applied.

## Phase 3: Path Materialization

Status: pending

- Share path materialization for repeated path functions.
  - Target `nodes(p)`, `relationships(p)`, `size(relationships(p))`, `startNode`, `endNode`, and `type`.
  - Avoid repeated `SubPlan` and `Function Scan on unnest` work per path binding.
- Expand late path materialization coverage.
  - Ensure paths are built only when needed for projection, filtering, or mutation semantics.

## Phase 4: Traversal And Recursive CTEs

Status: pending

- Push predicates into recursive traversal anchors and steps where semantics allow.
  - Endpoint kind/property predicates.
  - Relationship type predicates.
  - Bound-node filters.
- Improve traversal direction selection using endpoint selectivity.
  - Bound IDs.
  - Labels/kinds.
  - Equality predicates.
  - Finite relationship type sets.
- Broaden limit pushdown for variable-length path queries when ordering and distinct semantics permit early termination.

## Phase 5: Suffix And Shared Endpoint Rewrites

Status: pending

- Improve expansion suffix pushdown for fixed suffixes after variable-length traversals.
- Improve `ExpandInto` and shared endpoint rewrites for ADCS-style fanout patterns.
  - Constrain earlier using bound endpoint semi-joins or correlated expansion lowering where valid.

## Phase 6: Validation

Status: pending

- Add focused regression tests per optimization.
  - Optimizer/lowering selection tests.
  - SQL shape translation tests.
  - Backend-equivalent integration tests.
- Benchmark after each workstream.
  - Run unit tests.
  - Run backend-specific integration tests.
  - Run plan capture and compare summary deltas.

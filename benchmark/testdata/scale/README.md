# GraphBench Scale Corpus

This corpus measures graph workload shapes, not general Cypher correctness.
The shared integration corpus remains the source of backend-equivalent semantic
coverage.

Cases declare the values a query observes so benchmark reports can separate
ID-only work from node, relationship, property, and path materialization.
Current execution modes are `postgres_sql`, `local_traversal`, and `neo4j`.
Apache AGE is intentionally not a benchmark mode here; it may appear only in
`reference_design` notes as input for DAWGS design choices.

Each JSON file contains a list of scale cases with:

- `dataset`: the fixture dataset to load from `integration/testdata`.
- `name` and `category`: stable identifiers used in reports.
- `cypher`: the Cypher query under test.
- `params`: named parameter values. A typed temporal parameter uses
  `{"$type":"datetime","value":"2026-01-02T03:04:05Z"}`.
- `node_params`: scalar parameters resolved from fixture node names.
- `node_list_params`: list parameters resolved from fixture node names.
- `expected.row_count`: the expected result cardinality for a read case.
- `observes`: whether the query observes paths, nodes, relationships,
  properties, or only IDs internally.
- `candidate_modes`: the execution modes that should attempt the case.
- `reference_design`: optional design notes, including AGE observations when
  useful.

Mutations are rejected as ordinary read cases. A mutation must add a
`write_scenario` with:

- a selection query and `expected_matched` count;
- an `affected_entity` (`node` or `relationship`) and `expected_affected`
  count;
- one or more `post_state` queries with expected row counts or integer scalar
  values.

The runner drains the mutation result and validates those expectations inside
one rollback transaction. Warm-up, every timed iteration, and PostgreSQL
`EXPLAIN ANALYZE` therefore start from the same committed fixture state.

Use `cmd/graphbench` to run this corpus and produce JSONL, Markdown, and JSON
summaries.

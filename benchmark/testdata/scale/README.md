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

- `source`: the source corpus or workload family.
- `dataset`: the fixture dataset to load from `integration/testdata`.
- `name` and `category`: stable identifiers used in reports.
- `cypher`: the Cypher query under test.
- `parameters`: named parameter values.
- `expected_rows`: the expected result cardinality.
- `observes`: whether the query observes paths, nodes, relationships,
  properties, or only IDs internally.
- `candidate_modes`: the execution modes that should attempt the case.
- `reference_design`: optional design notes, including AGE observations when
  useful.

Use `cmd/graphbench` to run this corpus and produce JSONL, Markdown, and JSON
summaries.

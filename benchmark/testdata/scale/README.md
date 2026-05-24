# GraphBench Scale Corpus

This corpus measures graph workload shapes, not general Cypher correctness.
The shared integration corpus remains the source of backend-equivalent semantic coverage.

Cases declare the values a query observes so benchmark reports can separate ID-only work from node, relationship, property, and path materialization.
Initial execution modes are `postgres_sql`, `local_traversal`, and `neo4j`.
Apache AGE is intentionally not a benchmark mode here; it may appear only in `reference_design` notes as input for DAWGS design choices.


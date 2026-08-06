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
  `{"$type":"datetime","value":"2026-01-02T03:04:05Z"}`. A deterministic
  large string list uses
  `{"$type":"string_list","prefix":"missing","count":1000,"include":["target"]}`.
- `node_params`: scalar parameters resolved from fixture node names.
- `node_list_params`: list parameters resolved from fixture node names.
- `generated_node_list_params`: high-cardinality fixture-ID lists made from
  optional included names plus a prefix/count sequence, for example
  `{"ids":{"prefix":"target","count":2000,"include":["matched-target"]}}`.
- `expected.row_count`: the expected result cardinality for a read case.
- `observes`: whether the query observes paths, nodes, relationships,
  properties, or only IDs internally.
- `candidate_modes`: the execution modes that should attempt the case.
- `unsupported_modes`: explicit backend-to-reason declarations for matrix
  points retained as correctness oracles but not supported by that backend.
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

The `generated_reconciliation`, `generated_trust_pruning`, `generated_hops`,
and `generated_scan_lookups` datasets are constructed by
`testutil.NewReconciliationScaleFixture`,
`testutil.NewTrustPruningScaleFixture`, `testutil.NewHopScaleFixture`, and
`testutil.NewScanLookupScaleFixture`; they are intentionally not large
handwritten OpenGraph JSON files.

The corpus also executes parameterized `generated_shortest_paths_d*_f*` and
`generated_adcs_d*_f*_v*_p*` variants. The normal pairwise subset covers
shortest depth 1/2/4/8/16, fanout 1/16/128, outbound/inbound/directionless,
distance/path/all-shortest output, disconnected and diamond shapes. The ADCS
subset covers depth 0/1/2/4/8/16, fanout 1/10/100/1000, none/sparse/half/all
valid branch suffix density, endpoint/path output, decoys, and a 4 KiB payload.
Each result records the exact configuration name, deterministic graph checksum,
and node/edge cardinality.

Use `cmd/graphbench` to run this corpus and produce JSONL, Markdown, and JSON
summaries. Exact case/dataset/category/tag selectors are intended for targeted
diagnosis and mark their outputs diagnostic-only; they never replace a complete
corpus capture.

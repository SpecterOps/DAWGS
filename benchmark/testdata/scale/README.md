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
`generated_fixed_suffix_expansion_d*_f*_v*_p*` variants. Cases in
`cases/generated_fixed_suffix_expansion.json` use stable `GFSE-*` identifiers.
The normal pairwise subset covers shortest depth 1/2/4/8/16/32/64, fanout
1/16/128/512/1000,
outbound/inbound/directionless, distance/path/all-shortest output, and
disconnected, diamond, cycle, parallel-edge, and self-loop shapes. The
fixed-suffix expansion subset covers depth 0/1/2/4/8/16, fanout
1/10/100/1000, none/sparse/half/all valid branch suffix density, endpoint/path
output, decoys, and a 4 KiB payload.
Each result records the exact configuration name, deterministic graph checksum,
and node/edge cardinality.

Version-two shortest fixtures use
`generated_shortest_paths_v2_d<depth>_o<root-out>_r<root-in>_fo<intermediate-out>_fi<intermediate-in>_l<level>_k<parallel-kinds>_t<parallel-targets>_w<diamond-width>_x<disconnected-width>_p<payload>_c<cycle>_s<self-loop>`.
Names are strict and round-trippable: negative values, partial scans, unknown
suffixes, non-canonical numbers, impossible intermediate levels, and partial
parallel configurations are rejected. The fixture has independent outbound
and physical-inbound paths, so hidden downstream fan-in and its mirrored
fan-out control coexist without changing legacy fixture identities. Every edge
has a stable `logical_key`. Metadata records root and per-level degrees,
physical edges by kind, distinct reachable nodes by level, minimum distance,
path cardinalities, predecessor edges, disconnected state, parallel physical
edges and distinct targets, checksum, and loaded physical cardinality.

`shape.fixture_tier` is one of `normal`, `envelope`, or `stress`; direction,
relationship-kind count, expected state class, and result-cardinality class
are stored alongside it. Stress cases remain exact diagnostics and are not
silently promoted to release p95 evidence.

Version-two fixed-suffix expansion fixtures use
`generated_fixed_suffix_expansion_v2_d<depth>_f<fanout>_r<reachable>_x<disconnected>_i<reverse-fanin>_m<suffix-paths>_z<zero-depth>_p<payload>`.
Unlike the legacy modulus form, every integer is exact: `r0` represents zero
reachable branch suffixes, `x` varies false boundaries independently, `i`
controls reverse fan-in, `m` controls physical suffix multiplicity, and `z` is
either zero or one. Fixture records include declared root rows, forward
expansion states, suffix rows/boundaries, expected reverse states, output
trails, physical cardinality, and checksum. Semantic relationships carry
deterministic `logical_key` properties so relationship-distinct paths can be
compared across backends whose physical IDs differ.

`cases/fixed_suffix_expansion_limits.json` is an optimization-neutral cardinality
holdout suite. It covers 511, 512, 513, and 600 physical suffix rows, productive
endpoint and full-path observations, and exactly 512 physical rows with two
suffix paths per boundary to prove bag multiplicity. These `GFSE-BOUNDARY-*`
cases are not owned by any one optimization design; archived experiment reports
retain their historical case names.
The file-backed `fixed_suffix_expansion_adversarial` fixture adds 17 distinct
root lanes converging on one boundary, a reusable-node cycle, two physical suffix
paths, and noncanonical logical IDs. Its 68-row endpoint bag proves
relationship-trail rejection and multiplicity independently of the generated
limit fixtures.

Use `cmd/graphbench` to run this corpus and produce JSONL, Markdown, and JSON
summaries. Exact case/dataset/category/tag selectors are intended for targeted
diagnosis and mark their outputs diagnostic-only; they never replace a complete
corpus capture.

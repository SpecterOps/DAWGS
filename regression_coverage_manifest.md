# BloodHound Regression Coverage Manifest

Baseline audit for `regression_plan.md`, recorded when the regression harness
was established. This file is
the authoritative gap map for the stable query-form IDs; update a cell when a
case is added, and link the exact test or generated case that changed it.

Status values:

- `E` — existing coverage is equivalent to the complete normalized tuple.
- `P` — a primitive exists, but the production composition, projection,
  cardinality, mutation target, or scale dimension is missing.
- `C` — production-complete coverage added by this regression project.
- `A` — absent.
- `—` — the layer is not required by the plan.

No active production ID was complete when the audit began. The following
references are the existing primitives used by the table; they are linked here
instead of being cloned under BloodHound-specific names:

- `QB-PRED`: [`TestQueryBuilder_Render` predicate, temporal, kind, ID, string,
  null, and mutation subtests](query/neo4j/neo4j_test.go#L209).
- `QB-PROJ`: [`TestQueryBuilder_Render` relationship projection
  subtests](query/neo4j/neo4j_test.go#L740).
- `CY-MUT`: [Cypher create/update/delete parser cases](cypher/test/cases/mutation_tests.json).
- `PG-PRED`: [PostgreSQL node/predicate translation goldens](cypher/models/pgsql/test/translation_cases/nodes.sql).
- `PG-DEL`: [PostgreSQL delete translation goldens](cypher/models/pgsql/test/translation_cases/delete.sql).
- `PG-BIND`: [PostgreSQL binding and rewrite goldens](cypher/models/pgsql/test/translation_cases/pattern_binding.sql).
- `IT-PRED`: [backend-equivalent node predicate cases](integration/testdata/cases/nodes_inline.json).
- `IT-HOP`: [backend-equivalent directed one-hop template cases](integration/testdata/templates/pattern_shapes.json).
- `IT-MUT`: [primitive mutation cases](integration/testdata/cases/delete_inline.json) and
  [the initial exact post-state harness sentinel](integration/testdata/cases/mutation_post_state_inline.json).
- `SC-HOP`: [`one_hop_typed_from_bound_id`](benchmark/testdata/scale/cases/traversal.json).
- `SC-LOOKUP`: [`objectid_exact_string_anchor` and
  `boolean_property_filter`](benchmark/testdata/scale/cases/lookups.json).
- `SC-COUNT`: [`all_node_count`, `typed_node_count`, and
  `typed_edge_count`](benchmark/testdata/scale/cases/counts.json).
- `DR-BATCH`: [`TestBatchTransaction_NodeUpdate`](drivers/neo4j/batch_integration_test.go#L48).
- `PI-IDX`: [`TestPostgreSQLPropertyIndexPlans`](integration/pgsql_property_index_plan_test.go#L58).
- `LOGIC-QB`: [`TestQueryBuilder_LOGIC01PreservesBranchLocalRelationshipKinds`,
  `TestQueryBuilder_LogicalForms`, and
  `TestQueryBuilder_LOGIC05ProjectionOrder`](query/neo4j/neo4j_test.go), plus
  [`TestLegacyBuilderPostgreSQL_LogicalForms` and
  `TestLegacyBuilderPostgreSQL_LOGIC05ProjectionOrder`](cypher/models/pgsql/test/logical_forms_legacy_builder_test.go).
- `LOGIC-CY`: [`LOGIC-04` filtered relationship and node delete parser
  cases](cypher/test/cases/mutation_tests.json).
- `LOGIC-PG`: [`reconciliation.sql`](cypher/models/pgsql/test/translation_cases/reconciliation.sql)
  and [`post_processing.sql`](cypher/models/pgsql/test/translation_cases/post_processing.sql).
- `LOGIC-IT`: [`TestLegacyBuilderLogicalForms`](integration/logical_forms_legacy_builder_test.go)
  and the backend-equivalent [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  and [`post_processing_shapes.json`](integration/testdata/templates/post_processing_shapes.json) corpora.
- `LOGIC-PC`: the `LOGIC-01`, `LOGIC-02`, and `LOGIC-04` families in
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json),
  loaded directly by `cmd/plancorpus` with fixture-ID parameter resolution.
- `REC-QB`: [`TestQueryBuilder_ReconciliationForms`](query/neo4j/neo4j_test.go)
  and [`TestLegacyBuilderPostgreSQL_ReconciliationForms`](cypher/models/pgsql/test/reconciliation_forms_legacy_builder_test.go).
- `REC-CY`: the `REC-01` through `REC-04` and `REC-06` through `REC-08`
  mutation parser cases in [`mutation_tests.json`](cypher/test/cases/mutation_tests.json).
- `REC-PG`: the `REC-01` through `REC-08` PostgreSQL goldens in
  [`reconciliation.sql`](cypher/models/pgsql/test/translation_cases/reconciliation.sql).
- `REC-IT`: the exact reconciliation semantic families in
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  and the [`FetchStartNodes` de-dup contract](integration/delegated_enrollment_legacy_builder_test.go).
- `REC-PC`: the `REC-01` through `REC-08` families loaded from
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  by `cmd/plancorpus`.
- `REC-SC`: the repeatable `REC-01`, `REC-02`, `REC-04`, `REC-06`, and
  `REC-08` write scenarios in
  [`reconciliation.json`](benchmark/testdata/scale/cases/reconciliation.json).
- `TRUST-PRUNE-QB`: [`TestQueryBuilder_TrustAndPruningForms`](query/neo4j/neo4j_test.go)
  and [`TestLegacyBuilderPostgreSQL_TrustAndPruningForms`](cypher/models/pgsql/test/trust_pruning_forms_legacy_builder_test.go).
- `TRUST-PRUNE-PG`: the `TRUST-01` through `TRUST-03` and `PRUNE-01` through
  `PRUNE-04` PostgreSQL goldens in
  [`reconciliation.sql`](cypher/models/pgsql/test/translation_cases/reconciliation.sql)
  and [`post_processing.sql`](cypher/models/pgsql/test/translation_cases/post_processing.sql).
- `TRUST-PRUNE-IT`: the exact truth/null and hydration families in
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  and [`post_processing_shapes.json`](integration/testdata/templates/post_processing_shapes.json),
  plus [`TestLegacyBuilderTrustAndPruningSelectors`](integration/trust_pruning_legacy_builder_test.go).
- `TRUST-PRUNE-PC`: the `TRUST-01` through `TRUST-03` and `PRUNE-01` through
  `PRUNE-04` families loaded from the shared template corpus by `cmd/plancorpus`.
- `TRUST-PRUNE-SC`: the dense trust reads, pruning selectors, and mutation-safe
  batch-delete equivalents in
  [`trust_pruning.json`](benchmark/testdata/scale/cases/trust_pruning.json),
  backed by [`NewTrustPruningScaleFixture`](testutil/reconciliation_fixture.go).
- `PRUNE-DR`: [`TestDirectBatchPruning` and
  `BenchmarkDirectBatchPruning`](integration/trust_pruning_legacy_builder_test.go),
  including IDs absent at delete time and a mixed-direction high-degree cascade.
- `HOP-QB`: [`TestQueryBuilder_StandaloneHopForms`](query/neo4j/neo4j_test.go)
  and [`TestLegacyBuilderPostgreSQL_StandaloneHopForms`](cypher/models/pgsql/test/standalone_hop_forms_legacy_builder_test.go).
- `HOP-PG`: the `HOP-01` through `HOP-10` PostgreSQL goldens in
  [`stepwise_traversal.sql`](cypher/models/pgsql/test/translation_cases/stepwise_traversal.sql).
- `HOP-IT`: the backend-equivalent standalone-hop families in
  [`post_processing_hop_shapes.json`](integration/testdata/templates/post_processing_hop_shapes.json),
  plus [`TestLegacyBuilderStandaloneHops`](integration/standalone_hops_legacy_builder_test.go).
- `HOP-PC`: the `HOP-01` through `HOP-10` families loaded from
  [`post_processing_hop_shapes.json`](integration/testdata/templates/post_processing_hop_shapes.json)
  by `cmd/plancorpus`.
- `HOP-SC`: the repeatable standalone-hop scenarios in
  [`hops.json`](benchmark/testdata/scale/cases/hops.json), backed by
  [`NewHopScaleFixture`](testutil/reconciliation_fixture.go).
- `SCAN-LOOKUP-QB`: [`TestQueryBuilder_RelationshipScans` and
  `TestQueryBuilder_NodeLookups`](query/neo4j/relationship_scans_node_lookups_test.go), plus
  [`TestLegacyBuilderPostgreSQL_RelationshipScans` and
  `TestLegacyBuilderPostgreSQL_NodeLookups`](cypher/models/pgsql/test/relationship_scans_node_lookups_legacy_builder_test.go).
- `SCAN-LOOKUP-PG`: the `SCAN-01` through `SCAN-08` and `LOOKUP-01` through
  `LOOKUP-14`/`LOOKUP-16` PostgreSQL goldens in
  [`relationship_scans_node_lookups.sql`](cypher/models/pgsql/test/translation_cases/relationship_scans_node_lookups.sql).
- `SCAN-LOOKUP-IT`: the backend-equivalent scan, lookup, and count families in
  [`relationship_scan_shapes.json`](integration/testdata/templates/relationship_scan_shapes.json),
  [`basic_lookup_shapes.json`](integration/testdata/templates/basic_lookup_shapes.json),
  [`advanced_lookup_shapes.json`](integration/testdata/templates/advanced_lookup_shapes.json),
  and [`count_shapes.json`](integration/testdata/templates/count_shapes.json),
  plus [`TestLegacyBuilderRelationshipScansAndNodeLookups`](integration/relationship_scans_node_lookups_legacy_builder_test.go).
- `SCAN-LOOKUP-PC`: the `SCAN-*` and applicable `LOOKUP-*` families loaded from
  the shared scan/lookup template corpus by `cmd/plancorpus`.
- `SCAN-LOOKUP-SC`: the required wide-scan, large-list, adjacency, count, and NTLM
  scenarios in [`scans_lookups.json`](benchmark/testdata/scale/cases/scans_lookups.json),
  backed by [`NewScanLookupScaleFixture`](testutil/reconciliation_fixture.go).
- `WRITE-DR`: [`TestDirectWriteDeleteRelationshipBoundariesAndSurvivors` through
  `TestDirectWriteExactKeyMissThenCreateNode`](integration/direct_write_mutations_test.go),
  covering direct batch and transactional APIs on the selected backend with the
  shared [`NewDirectWriteScaleFixture`](testutil/reconciliation_fixture.go), plus
  the PostgreSQL conflict-key/property-index regression in
  [`batch_test.go`](drivers/pg/batch_test.go).
- `WRITE-IT`: the exact-key create/update, full-node update, and exact-key
  miss/create workflows in [`direct_write_mutations_test.go`](integration/direct_write_mutations_test.go),
  with selector and driver-operation assertions kept separate.
- `WRITE-SC`: the reset-per-iteration, post-state-checked
  [`BenchmarkMutationSafeDirectWrites`](integration/direct_write_mutations_test.go)
  at 1,000 items and across the 2,000-item DAWGS flush boundary.
- `SCALE-PI`: [`TestPostgreSQLScalePlanInvariants`](cmd/graphbench/postgresql_plan_invariants_integration_test.go)
  executes every required Cypher scale representative through PostgreSQL with
  `EXPLAIN ANALYZE`, exact read/write cardinality, rollback-isolated mutation
  post-state, mutation-target, binding, and anchor-index assertions. The
  backend-independent [`TestScaleCorpusRequiredRepresentativesDeclareCardinality`](cmd/graphbench/scale_corpus_contract_test.go)
  prevents a required stable ID or its cardinality contract from disappearing.
- `SCALE-BASELINE`: `cmd/graphbench` captures translated SQL, lowering
  metadata, plans, buffer/runtime metrics, and cardinalities for the complete
  scale corpus; `cmd/plancorpus` captures the shared semantic corpus with source
  metadata. Generated captures remain review artifacts under the ignored
  `.coverage/` directory rather than committed machine-specific baselines.
- `DORMANT-GATE`: [`TestDormantFormsStayOutOfPlanCorpus`](cmd/plancorpus/dormant_forms_guard_test.go)
  and [`TestDormantFormsStayOutOfScaleCorpus`](cmd/graphbench/dormant_forms_guard_test.go)
  keep every `FUTURE-*` ID out of active semantic, plan, and scale gates. The
  activation and ongoing source-review procedure is recorded in
  [`regression_source_parity.md`](docs/regression_source_parity.md).
- `COMPLETION-SC`: the `SCAN-01` ID-only, `SCAN-06` shallow IDs/kind,
  `SCAN-02` relationship hydration, and `LOOKUP-09` node hydration scale cases
  are classified and enforced by
  [`TestScaleCorpusDistinguishesProjectionClasses`](cmd/graphbench/scale_corpus_contract_test.go).
- `COMPLETION-GATE`: [`TestRegressionCoverageManifestClosesEveryActiveID`](regression_manifest_test.go)
  requires all 64 stable active IDs to remain present without an `A` or `P`
  layer while preserving `FUTURE-01` as non-production-complete.

## Logical sentinels

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `LOGIC-01` | C (`LOGIC-QB`) | — | C (`LOGIC-PG`) | C (`LOGIC-IT`) | C (`LOGIC-PC`) | C (`SCALE-PI`) | — | — |
| `LOGIC-02` | C (`LOGIC-QB`) | — | C (`LOGIC-PG`) | C (`LOGIC-IT`) | C (`LOGIC-PC`) | C (`SCALE-PI`) | — | — |
| `LOGIC-03` | C (`LOGIC-QB`) | — | C (`LOGIC-PG`) | C (`LOGIC-IT`) | — | — | — | — |
| `LOGIC-04` | — | C (`LOGIC-CY`) | C (`LOGIC-PG`) | C (`LOGIC-IT`) | C (`LOGIC-PC`) | C (`SCALE-PI`) | — | — |
| `LOGIC-05` | C (`LOGIC-QB`) | — | C (`LOGIC-PG`) | C (`LOGIC-IT`) | — | — | — | — |

## Reconciliation

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `REC-01` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | C (`SCALE-PI`) | C (`REC-SC`) | — |
| `REC-02` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | C (`SCALE-PI`) | C (`REC-SC`) | — |
| `REC-03` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | — | — | — |
| `REC-04` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | C (`SCALE-PI`) | C (`REC-SC`) | — |
| `REC-05` | C (`REC-QB`) | — | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | — | — | — |
| `REC-06` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | C (`SCALE-PI`) | C (`REC-SC`) | — |
| `REC-07` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | — | — | — |
| `REC-08` | C (`REC-QB`) | C (`REC-CY`) | C (`REC-PG`) | C (`REC-IT`) | C (`REC-PC`) | C (`SCALE-PI`) | C (`REC-SC`) | — |

## Trust, pruning, and aging

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `TRUST-01` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | C (`TRUST-PRUNE-SC`) | — |
| `TRUST-02` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | C (`TRUST-PRUNE-SC`) | — |
| `TRUST-03` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | — | — |
| `PRUNE-01` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | C (`TRUST-PRUNE-SC`) | — |
| `PRUNE-02` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | C (`TRUST-PRUNE-SC`) | — |
| `PRUNE-03` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | C (`TRUST-PRUNE-SC`) | — |
| `PRUNE-04` | C (`TRUST-PRUNE-QB`) | — | C (`TRUST-PRUNE-PG`) | C (`TRUST-PRUNE-IT`) | C (`TRUST-PRUNE-PC`) | C (`SCALE-PI`) | C (`TRUST-PRUNE-SC`) | — |
| `PRUNE-05` | — | — | — | — | — | — | C (`TRUST-PRUNE-SC`) | C (`PRUNE-DR`) |
| `PRUNE-06` | — | — | — | — | — | — | C (`TRUST-PRUNE-SC`) | C (`PRUNE-DR`) |

## Standalone hops

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `HOP-01` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-02` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-03` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-04` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-05` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-06` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | — | — | — |
| `HOP-07` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-08` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | — | — | — |
| `HOP-09` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | C (`SCALE-PI`) | C (`HOP-SC`) | — |
| `HOP-10` | C (`HOP-QB`) | — | C (`HOP-PG`) | C (`HOP-IT`) | C (`HOP-PC`) | — | — | — |

## Relationship scans and node lookups

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `SCAN-01` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `SCAN-02` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `SCAN-03` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `SCAN-04` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `SCAN-05` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `SCAN-06` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | C (`COMPLETION-SC`) | — |
| `SCAN-07` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `SCAN-08` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-01` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | — | — |
| `LOOKUP-02` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-03` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | — | — | — | — |
| `LOOKUP-04` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-05` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-06` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | — | — |
| `LOOKUP-07` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | — | — | — | — |
| `LOOKUP-08` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | — | — |
| `LOOKUP-09` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-10` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | — | — |
| `LOOKUP-11` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-12` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | — | — |
| `LOOKUP-13` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-14` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | — | — | — |
| `LOOKUP-15` | — | — | — | C (`SCAN-LOOKUP-IT`) | — | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |
| `LOOKUP-16` | C (`SCAN-LOOKUP-QB`) | — | C (`SCAN-LOOKUP-PG`) | C (`SCAN-LOOKUP-IT`) | C (`SCAN-LOOKUP-PC`) | C (`SCALE-PI`) | C (`SCAN-LOOKUP-SC`) | — |

## Direct writes

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `WRITE-01` | — | — | — | — | — | — | C (`WRITE-SC`) | C (`WRITE-DR`) |
| `WRITE-02` | — | — | — | — | — | — | C (`WRITE-SC`) | C (`WRITE-DR`) |
| `WRITE-03` | — | — | — | — | — | — | C (`WRITE-SC`) | C (`WRITE-DR`) |
| `WRITE-04` | — | — | — | — | — | — | C (`WRITE-SC`) | C (`WRITE-DR`) |
| `WRITE-05` | — | — | — | — | — | — | C (`WRITE-SC`) | C (`WRITE-DR`) |
| `WRITE-06` | — | — | — | C (`WRITE-IT`) | — | — | — | C (`WRITE-DR`) |
| `WRITE-07` | — | — | — | C (`WRITE-IT`) | — | — | — | C (`WRITE-DR`) |
| `WRITE-08` | — | — | — | C (`WRITE-IT`) | — | — | — | C (`WRITE-DR`) |

## Dormant coverage

`FUTURE-01` remains intentionally incomplete because its reviewed callers are
disabled. `DORMANT-GATE` protects that classification; it is not query coverage
and therefore does not change the primitive or absent cells below.

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `FUTURE-01` | P (`QB-PRED`) | P (`CY-MUT`) | P (`PG-DEL`) | P (`IT-MUT`) | A | — | A | — |

## Completion audit

The executable manifest gate and the following evidence close the completion
definition in `regression_plan.md`:

1. All 64 active stable IDs are present at their required layers without an
   absent or primitive-only cell (`COMPLETION-GATE`).
2. Shared Cypher mutations and direct writes assert exact targets, survivors,
   properties, and counts with rollback/reset isolation (`IT-MUT`,
   `REC-IT`, `TRUST-PRUNE-IT`, and `WRITE-IT`).
3. The `LOGIC-01` branch-local direction/kind truth table executes through the
   shared integration corpus on PostgreSQL and Neo4j (`LOGIC-IT`).
4. PostgreSQL translation and plan coverage exercises equality-anchored deletes
   in both active endpoint orientations and every production-active list form
   (`REC-PG`, `REC-PC`, and `SCALE-PI`). The only outbound tenant-list
   form is disabled upstream and remains `FUTURE-01` as required.
5. Scale coverage explicitly separates ID-only, shallow IDs/kind, full
   relationship, and full-node projections (`COMPLETION-SC`).
6. Direct-write coverage includes the 1,000-item application batch and the
   1,999/2,000/2,001 DAWGS flush boundary (`WRITE-DR` and `WRITE-SC`).
7. `HOP-*` semantic and scale cases remain standalone one-hop queries; no new
   runner sequences BloodHound traversal behavior (`HOP-IT` and
   `HOP-SC`).
8. Dormant IDs are rejected from active plan and scale corpora until their
   callers are enabled (`DORMANT-GATE`).

## Harness foundation

These prerequisites are intentionally not marked `C` against production IDs:

- Mutation post-state assertions: [standalone sentinel](integration/testdata/cases/mutation_post_state_inline.json)
  and [template rollback/repeat sentinel](integration/testdata/templates/mutation_post_state_shapes.json).
- Reusable deterministic fixture and list/fanout generators:
  [`NewReconciliationFixture`](integration/regression_fixture.go).
- Backend-selected legacy query execution:
  [`WithLegacyNodeQuery` and `WithLegacyRelationshipQuery`](integration/legacy_query_harness.go).
- Mutation-safe scale execution and list-valued fixture IDs:
  [`WriteScenario`](cmd/graphbench/types.go) and
  [`resolveCaseParams`](cmd/graphbench/datasets.go).

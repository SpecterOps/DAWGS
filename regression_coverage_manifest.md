# BloodHound Regression Coverage Manifest

Baseline audit for `regression_plan.md`, recorded during Phase 0. This file is
the authoritative gap map for the stable query-form IDs; update a cell when a
case is added, and link the exact test or generated case that changed it.

Status values:

- `E` — existing coverage is equivalent to the complete normalized tuple.
- `P` — a primitive exists, but the production composition, projection,
  cardinality, mutation target, or scale dimension is missing.
- `C` — production-complete coverage added by this regression project.
- `A` — absent.
- `—` — the layer is not required by the plan.

No active production ID was complete at the start of Phase 0. The following
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
  [Phase 0 exact post-state harness sentinel](integration/testdata/cases/mutation_post_state_inline.json).
- `SC-HOP`: [`one_hop_typed_from_bound_id`](benchmark/testdata/scale/cases/traversal.json).
- `SC-LOOKUP`: [`objectid_exact_string_anchor` and
  `boolean_property_filter`](benchmark/testdata/scale/cases/lookups.json).
- `SC-COUNT`: [`all_node_count`, `typed_node_count`, and
  `typed_edge_count`](benchmark/testdata/scale/cases/counts.json).
- `DR-BATCH`: [`TestBatchTransaction_NodeUpdate`](drivers/neo4j/batch_integration_test.go#L48).
- `PI-IDX`: [`TestPostgreSQLPropertyIndexPlans`](integration/pgsql_property_index_plan_test.go#L58).
- `PHASE1-QB`: [`TestQueryBuilder_LOGIC01PreservesBranchLocalRelationshipKinds`,
  `TestQueryBuilder_Phase1LogicalForms`, and
  `TestQueryBuilder_LOGIC05ProjectionOrder`](query/neo4j/neo4j_test.go), plus
  [`TestLegacyBuilderPostgreSQL_Phase1LogicalForms` and
  `TestLegacyBuilderPostgreSQL_LOGIC05ProjectionOrder`](cypher/models/pgsql/test/phase1_legacy_builder_test.go).
- `PHASE1-CY`: [`LOGIC-04` filtered relationship and node delete parser
  cases](cypher/test/cases/mutation_tests.json).
- `PHASE1-PG`: [`reconciliation.sql`](cypher/models/pgsql/test/translation_cases/reconciliation.sql)
  and [`post_processing.sql`](cypher/models/pgsql/test/translation_cases/post_processing.sql).
- `PHASE1-IT`: [`TestPhase1LegacyBuilderIntegration`](integration/phase1_legacy_builder_test.go)
  and the backend-equivalent [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  and [`post_processing_shapes.json`](integration/testdata/templates/post_processing_shapes.json) corpora.
- `PHASE1-PC`: the `LOGIC-01`, `LOGIC-02`, and `LOGIC-04` families in
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json),
  loaded directly by `cmd/plancorpus` with fixture-ID parameter resolution.
- `PHASE2-QB`: [`TestQueryBuilder_Phase2ReconciliationForms`](query/neo4j/neo4j_test.go)
  and [`TestLegacyBuilderPostgreSQL_Phase2ReconciliationForms`](cypher/models/pgsql/test/phase2_legacy_builder_test.go).
- `PHASE2-CY`: the `REC-01` through `REC-04` and `REC-06` through `REC-08`
  mutation parser cases in [`mutation_tests.json`](cypher/test/cases/mutation_tests.json).
- `PHASE2-PG`: the `REC-01` through `REC-08` PostgreSQL goldens in
  [`reconciliation.sql`](cypher/models/pgsql/test/translation_cases/reconciliation.sql).
- `PHASE2-IT`: the exact reconciliation semantic families in
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  and the [`FetchStartNodes` de-dup contract](integration/phase2_legacy_builder_test.go).
- `PHASE2-PC`: the `REC-01` through `REC-08` families loaded from
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  by `cmd/plancorpus`.
- `PHASE2-SC`: the repeatable `REC-01`, `REC-02`, `REC-04`, `REC-06`, and
  `REC-08` write scenarios in
  [`reconciliation.json`](benchmark/testdata/scale/cases/reconciliation.json).
- `PHASE3-QB`: [`TestQueryBuilder_Phase3TrustAndPruningForms`](query/neo4j/neo4j_test.go)
  and [`TestLegacyBuilderPostgreSQL_Phase3TrustAndPruningForms`](cypher/models/pgsql/test/phase3_legacy_builder_test.go).
- `PHASE3-PG`: the `TRUST-01` through `TRUST-03` and `PRUNE-01` through
  `PRUNE-04` PostgreSQL goldens in
  [`reconciliation.sql`](cypher/models/pgsql/test/translation_cases/reconciliation.sql)
  and [`post_processing.sql`](cypher/models/pgsql/test/translation_cases/post_processing.sql).
- `PHASE3-IT`: the exact truth/null and hydration families in
  [`reconciliation_shapes.json`](integration/testdata/templates/reconciliation_shapes.json)
  and [`post_processing_shapes.json`](integration/testdata/templates/post_processing_shapes.json),
  plus [`TestPhase3LegacyBuilderTrustAndPruningSelectors`](integration/phase3_legacy_builder_test.go).
- `PHASE3-PC`: the `TRUST-01` through `TRUST-03` and `PRUNE-01` through
  `PRUNE-04` families loaded from the shared template corpus by `cmd/plancorpus`.
- `PHASE3-SC`: the dense trust reads, pruning selectors, and mutation-safe
  batch-delete equivalents in
  [`trust_pruning.json`](benchmark/testdata/scale/cases/trust_pruning.json),
  backed by [`NewTrustPruningScaleFixture`](testutil/reconciliation_fixture.go).
- `PHASE3-DR`: [`TestPhase3DirectBatchPruning` and
  `BenchmarkPhase3DirectBatchPruning`](integration/phase3_legacy_builder_test.go),
  including IDs absent at delete time and a mixed-direction high-degree cascade.
- `PHASE4-QB`: [`TestQueryBuilder_Phase4StandaloneHopForms`](query/neo4j/neo4j_test.go)
  and [`TestLegacyBuilderPostgreSQL_Phase4StandaloneHopForms`](cypher/models/pgsql/test/phase4_legacy_builder_test.go).
- `PHASE4-PG`: the `HOP-01` through `HOP-10` PostgreSQL goldens in
  [`stepwise_traversal.sql`](cypher/models/pgsql/test/translation_cases/stepwise_traversal.sql).
- `PHASE4-IT`: the backend-equivalent standalone-hop families in
  [`post_processing_hop_shapes.json`](integration/testdata/templates/post_processing_hop_shapes.json),
  plus [`TestPhase4LegacyBuilderIntegration`](integration/phase4_legacy_builder_test.go).
- `PHASE4-PC`: the `HOP-01` through `HOP-10` families loaded from
  [`post_processing_hop_shapes.json`](integration/testdata/templates/post_processing_hop_shapes.json)
  by `cmd/plancorpus`.
- `PHASE4-SC`: the repeatable standalone-hop scenarios in
  [`hops.json`](benchmark/testdata/scale/cases/hops.json), backed by
  [`NewHopScaleFixture`](testutil/reconciliation_fixture.go).
- `PHASE5-QB`: [`TestQueryBuilder_Phase5RelationshipScans` and
  `TestQueryBuilder_Phase5Lookups`](query/neo4j/phase5_test.go), plus
  [`TestLegacyBuilderPostgreSQL_Phase5RelationshipScans` and
  `TestLegacyBuilderPostgreSQL_Phase5Lookups`](cypher/models/pgsql/test/phase5_legacy_builder_test.go).
- `PHASE5-PG`: the `SCAN-01` through `SCAN-08` and `LOOKUP-01` through
  `LOOKUP-14`/`LOOKUP-16` PostgreSQL goldens in
  [`phase5_scans_lookups.sql`](cypher/models/pgsql/test/translation_cases/phase5_scans_lookups.sql).
- `PHASE5-IT`: the backend-equivalent scan, lookup, and count families in
  [`phase5_relationship_scans.json`](integration/testdata/templates/phase5_relationship_scans.json),
  [`phase5_basic_lookups.json`](integration/testdata/templates/phase5_basic_lookups.json),
  [`phase5_advanced_lookups.json`](integration/testdata/templates/phase5_advanced_lookups.json),
  and [`phase5_counts.json`](integration/testdata/templates/phase5_counts.json),
  plus [`TestPhase5LegacyBuilderIntegration`](integration/phase5_legacy_builder_test.go).
- `PHASE5-PC`: the `SCAN-*` and applicable `LOOKUP-*` families loaded from
  the shared Phase 5 template corpus by `cmd/plancorpus`.
- `PHASE5-SC`: the required wide-scan, large-list, adjacency, count, and NTLM
  scenarios in [`scans_lookups.json`](benchmark/testdata/scale/cases/scans_lookups.json),
  backed by [`NewScanLookupScaleFixture`](testutil/reconciliation_fixture.go).
- `PHASE6-DR`: [`TestPhase6DeleteRelationshipBoundariesAndSurvivors` through
  `TestPhase6ExactKeyMissThenCreateNode`](integration/phase6_direct_write_test.go),
  covering direct batch and transactional APIs on the selected backend with the
  shared [`NewDirectWriteScaleFixture`](testutil/reconciliation_fixture.go), plus
  the PostgreSQL conflict-key/property-index regression in
  [`batch_test.go`](drivers/pg/batch_test.go).
- `PHASE6-IT`: the exact-key create/update, full-node update, and exact-key
  miss/create workflows in [`phase6_direct_write_test.go`](integration/phase6_direct_write_test.go),
  with selector and driver-operation assertions kept separate.
- `PHASE6-SC`: the reset-per-iteration, post-state-checked
  [`BenchmarkPhase6MutationSafeDirectWrites`](integration/phase6_direct_write_test.go)
  at 1,000 items and across the 2,000-item DAWGS flush boundary.
- `PHASE7-PI`: [`TestPostgreSQLPhase7PlanInvariants`](cmd/graphbench/phase7_plan_integration_test.go)
  executes every required Cypher scale representative through PostgreSQL with
  `EXPLAIN ANALYZE`, exact read/write cardinality, rollback-isolated mutation
  post-state, mutation-target, binding, and anchor-index assertions. The
  backend-independent [`TestPhase7RequiredScaleRepresentativesDeclareCardinality`](cmd/graphbench/phase7_test.go)
  prevents a required stable ID or its cardinality contract from disappearing.
- `PHASE7-BASELINE`: `cmd/graphbench` captures translated SQL, lowering
  metadata, plans, buffer/runtime metrics, and cardinalities for the complete
  scale corpus; `cmd/plancorpus` captures the shared semantic corpus with source
  metadata. Generated captures remain review artifacts under the ignored
  `.coverage/` directory rather than committed machine-specific baselines.
- `PHASE8-GATE`: [`TestPhase8DormantFormsStayOutOfPlanCorpus`](cmd/plancorpus/phase8_test.go)
  and [`TestPhase8DormantFormsStayOutOfScaleCorpus`](cmd/graphbench/phase8_test.go)
  keep every `FUTURE-*` ID out of active semantic, plan, and scale gates. The
  activation and ongoing source-review procedure is recorded in
  [`regression_source_parity.md`](docs/regression_source_parity.md).
- `COMPLETION-SC`: the `SCAN-01` ID-only, `SCAN-06` shallow IDs/kind,
  `SCAN-02` relationship hydration, and `LOOKUP-09` node hydration scale cases
  are classified and enforced by
  [`TestScaleCorpusDistinguishesProjectionClasses`](cmd/graphbench/phase7_test.go).
- `COMPLETION-GATE`: [`TestRegressionCoverageManifestClosesEveryActiveID`](regression_manifest_test.go)
  requires all 64 stable active IDs to remain present without an `A` or `P`
  layer while preserving `FUTURE-01` as non-production-complete.

## Phase 1 sentinels

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `LOGIC-01` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | C (`PHASE1-PC`) | C (`PHASE7-PI`) | — | — |
| `LOGIC-02` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | C (`PHASE1-PC`) | C (`PHASE7-PI`) | — | — |
| `LOGIC-03` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | — | — | — | — |
| `LOGIC-04` | — | C (`PHASE1-CY`) | C (`PHASE1-PG`) | C (`PHASE1-IT`) | C (`PHASE1-PC`) | C (`PHASE7-PI`) | — | — |
| `LOGIC-05` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | — | — | — | — |

## Phase 2 reconciliation

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `REC-01` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | C (`PHASE7-PI`) | C (`PHASE2-SC`) | — |
| `REC-02` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | C (`PHASE7-PI`) | C (`PHASE2-SC`) | — |
| `REC-03` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | — | — |
| `REC-04` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | C (`PHASE7-PI`) | C (`PHASE2-SC`) | — |
| `REC-05` | C (`PHASE2-QB`) | — | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | — | — |
| `REC-06` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | C (`PHASE7-PI`) | C (`PHASE2-SC`) | — |
| `REC-07` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | — | — |
| `REC-08` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | C (`PHASE7-PI`) | C (`PHASE2-SC`) | — |

## Phase 3 trust, pruning, and aging

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `TRUST-01` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | C (`PHASE3-SC`) | — |
| `TRUST-02` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | C (`PHASE3-SC`) | — |
| `TRUST-03` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | — | — |
| `PRUNE-01` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | C (`PHASE3-SC`) | — |
| `PRUNE-02` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | C (`PHASE3-SC`) | — |
| `PRUNE-03` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | C (`PHASE3-SC`) | — |
| `PRUNE-04` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | C (`PHASE7-PI`) | C (`PHASE3-SC`) | — |
| `PRUNE-05` | — | — | — | — | — | — | C (`PHASE3-SC`) | C (`PHASE3-DR`) |
| `PRUNE-06` | — | — | — | — | — | — | C (`PHASE3-SC`) | C (`PHASE3-DR`) |

## Phase 4 standalone hops

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `HOP-01` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-02` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-03` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-04` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-05` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-06` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | — | — | — |
| `HOP-07` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-08` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | — | — | — |
| `HOP-09` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | C (`PHASE7-PI`) | C (`PHASE4-SC`) | — |
| `HOP-10` | C (`PHASE4-QB`) | — | C (`PHASE4-PG`) | C (`PHASE4-IT`) | C (`PHASE4-PC`) | — | — | — |

## Phase 5 scans and lookups

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `SCAN-01` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `SCAN-02` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `SCAN-03` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `SCAN-04` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `SCAN-05` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `SCAN-06` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | C (`COMPLETION-SC`) | — |
| `SCAN-07` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `SCAN-08` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-01` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | — | — |
| `LOOKUP-02` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-03` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | — | — | — | — |
| `LOOKUP-04` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-05` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-06` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | — | — |
| `LOOKUP-07` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | — | — | — | — |
| `LOOKUP-08` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | — | — |
| `LOOKUP-09` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-10` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | — | — |
| `LOOKUP-11` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-12` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | — | — |
| `LOOKUP-13` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-14` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | — | — | — |
| `LOOKUP-15` | — | — | — | C (`PHASE5-IT`) | — | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |
| `LOOKUP-16` | C (`PHASE5-QB`) | — | C (`PHASE5-PG`) | C (`PHASE5-IT`) | C (`PHASE5-PC`) | C (`PHASE7-PI`) | C (`PHASE5-SC`) | — |

## Phase 6 direct writes

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `WRITE-01` | — | — | — | — | — | — | C (`PHASE6-SC`) | C (`PHASE6-DR`) |
| `WRITE-02` | — | — | — | — | — | — | C (`PHASE6-SC`) | C (`PHASE6-DR`) |
| `WRITE-03` | — | — | — | — | — | — | C (`PHASE6-SC`) | C (`PHASE6-DR`) |
| `WRITE-04` | — | — | — | — | — | — | C (`PHASE6-SC`) | C (`PHASE6-DR`) |
| `WRITE-05` | — | — | — | — | — | — | C (`PHASE6-SC`) | C (`PHASE6-DR`) |
| `WRITE-06` | — | — | — | C (`PHASE6-IT`) | — | — | — | C (`PHASE6-DR`) |
| `WRITE-07` | — | — | — | C (`PHASE6-IT`) | — | — | — | C (`PHASE6-DR`) |
| `WRITE-08` | — | — | — | C (`PHASE6-IT`) | — | — | — | C (`PHASE6-DR`) |

## Phase 8 dormant coverage

`FUTURE-01` remains intentionally incomplete because its reviewed callers are
disabled. `PHASE8-GATE` protects that classification; it is not query coverage
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
   `PHASE2-IT`, `PHASE3-IT`, and `PHASE6-IT`).
3. The `LOGIC-01` branch-local direction/kind truth table executes through the
   shared integration corpus on PostgreSQL and Neo4j (`PHASE1-IT`).
4. PostgreSQL translation and plan coverage exercises equality-anchored deletes
   in both active endpoint orientations and every production-active list form
   (`PHASE2-PG`, `PHASE2-PC`, and `PHASE7-PI`). The only outbound tenant-list
   form is disabled upstream and remains `FUTURE-01` as required.
5. Scale coverage explicitly separates ID-only, shallow IDs/kind, full
   relationship, and full-node projections (`COMPLETION-SC`).
6. Direct-write coverage includes the 1,000-item application batch and the
   1,999/2,000/2,001 DAWGS flush boundary (`PHASE6-DR` and `PHASE6-SC`).
7. `HOP-*` semantic and scale cases remain standalone one-hop queries; no new
   runner sequences BloodHound traversal behavior (`PHASE4-IT` and
   `PHASE4-SC`).
8. Dormant IDs are rejected from active plan and scale corpora until their
   callers are enabled (`PHASE8-GATE`).

## Phase 0 harness state

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

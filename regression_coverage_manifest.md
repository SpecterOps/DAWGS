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

## Phase 1 sentinels

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `LOGIC-01` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | C (`PHASE1-PC`) | A | — | — |
| `LOGIC-02` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | C (`PHASE1-PC`) | A | — | — |
| `LOGIC-03` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | — | — | — | — |
| `LOGIC-04` | — | C (`PHASE1-CY`) | C (`PHASE1-PG`) | C (`PHASE1-IT`) | C (`PHASE1-PC`) | A | — | — |
| `LOGIC-05` | C (`PHASE1-QB`) | — | C (`PHASE1-PG`) | C (`PHASE1-IT`) | — | — | — | — |

## Phase 2 reconciliation

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `REC-01` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | C (`PHASE2-SC`) | — |
| `REC-02` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | C (`PHASE2-SC`) | — |
| `REC-03` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | — | — |
| `REC-04` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | C (`PHASE2-SC`) | — |
| `REC-05` | C (`PHASE2-QB`) | — | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | — | — |
| `REC-06` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | C (`PHASE2-SC`) | — |
| `REC-07` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | — | — |
| `REC-08` | C (`PHASE2-QB`) | C (`PHASE2-CY`) | C (`PHASE2-PG`) | C (`PHASE2-IT`) | C (`PHASE2-PC`) | — | C (`PHASE2-SC`) | — |

## Phase 3 trust, pruning, and aging

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `TRUST-01` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | C (`PHASE3-SC`) | — |
| `TRUST-02` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | C (`PHASE3-SC`) | — |
| `TRUST-03` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | — | — |
| `PRUNE-01` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | C (`PHASE3-SC`) | — |
| `PRUNE-02` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | C (`PHASE3-SC`) | — |
| `PRUNE-03` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | C (`PHASE3-SC`) | — |
| `PRUNE-04` | C (`PHASE3-QB`) | — | C (`PHASE3-PG`) | C (`PHASE3-IT`) | C (`PHASE3-PC`) | A | C (`PHASE3-SC`) | — |
| `PRUNE-05` | — | — | — | — | — | — | C (`PHASE3-SC`) | C (`PHASE3-DR`) |
| `PRUNE-06` | — | — | — | — | — | — | C (`PHASE3-SC`) | C (`PHASE3-DR`) |

## Phase 4 standalone hops

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `HOP-01` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | P (`SC-HOP`) | — |
| `HOP-02` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `HOP-03` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `HOP-04` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `HOP-05` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `HOP-06` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `HOP-07` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | A | — |
| `HOP-08` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `HOP-09` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `HOP-10` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | — | — | — |

## Phase 5 scans and lookups

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `SCAN-01` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `SCAN-02` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `SCAN-03` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | A | — |
| `SCAN-04` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `SCAN-05` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `SCAN-06` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | — | — | — |
| `SCAN-07` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `SCAN-08` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `LOOKUP-01` | P (`QB-PROJ`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `LOOKUP-02` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | P (`SC-LOOKUP`) | — |
| `LOOKUP-03` | P (`QB-PROJ`) | — | P (`PG-PRED`) | P (`IT-PRED`) | — | — | — | — |
| `LOOKUP-04` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | A | — |
| `LOOKUP-05` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | A | — |
| `LOOKUP-06` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `LOOKUP-07` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | — | — | — | — |
| `LOOKUP-08` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `LOOKUP-09` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | A | — |
| `LOOKUP-10` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `LOOKUP-11` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `LOOKUP-12` | P (`QB-PRED`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | — | — | — |
| `LOOKUP-13` | P (`QB-PROJ`) | — | P (`PG-BIND`) | P (`IT-HOP`) | A | A | A | — |
| `LOOKUP-14` | P (`QB-PROJ`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | — | — | — |
| `LOOKUP-15` | — | — | — | P (`IT-PRED`) | — | A | P (`SC-COUNT`) | — |
| `LOOKUP-16` | P (`QB-PRED`) | — | P (`PG-PRED`) | P (`IT-PRED`) | A | A | A | — |

## Phase 6 direct writes

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `WRITE-01` | — | — | — | — | — | — | A | P (`DR-BATCH`) |
| `WRITE-02` | — | — | — | — | — | — | A | P (`DR-BATCH`) |
| `WRITE-03` | — | — | — | — | — | — | A | P (`DR-BATCH`) |
| `WRITE-04` | — | — | — | — | — | — | A | P (`DR-BATCH`) |
| `WRITE-05` | — | — | — | — | — | — | A | P (`DR-BATCH`) |
| `WRITE-06` | — | — | — | P (`IT-HOP`) | — | — | — | P (`DR-BATCH`) |
| `WRITE-07` | — | — | — | P (`IT-PRED`) | — | — | — | P (`DR-BATCH`) |
| `WRITE-08` | — | — | — | P (`IT-PRED`) | — | — | — | P (`DR-BATCH`) |

## Phase 8 dormant coverage

| ID | QB | CY | PG | IT | PC | PI | SC | DR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `FUTURE-01` | P (`QB-PRED`) | P (`CY-MUT`) | P (`PG-DEL`) | P (`IT-MUT`) | A | — | A | — |

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

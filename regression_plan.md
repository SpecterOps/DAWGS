# BloodHound Reconciliation and Post-Processing Regression Plan

## Goal

Build a DAWGS regression corpus that represents every distinct query form used by the reviewed BloodHound Enterprise (BHE) reconciliation paths and the active BloodHound Community Edition (BHCE) post-processing and changelog paths. The corpus must catch semantic, rendering, translation, plan-shape, and scale regressions without importing BloodHound business logic into DAWGS.

The source snapshots reviewed for this plan are:

- BHE commit `c9f61530f45b`.
- BHCE commit `74dd3daa58a8` under `bhe/bhce`.
- Both applications pin DAWGS `v0.6.0`; the DAWGS review baseline was `v0.6.0-13-g6638cc2`.

## Scope and guardrails

This plan is query-form focused.

In scope:

- Legacy `query` builder ASTs used by the reviewed BHE and BHCE code.
- Raw Cypher parsing and PostgreSQL translation for equivalent forms.
- Cross-backend semantics for active forms.
- PostgreSQL plan and scale coverage for forms likely to be cardinality- or join-sensitive.
- Direct driver and batch operations that are part of reconciliation or post-processing.
- Dormant forms kept in a clearly separated future-coverage tier.

Out of scope:

- Reimplementing, repairing, or porting BHE/BHCE stepwise traversal algorithms.
- Adding a BloodHound-aware traversal executor to DAWGS.
- Reproducing complete ADCS, NTLM, Azure role, or trust path composition in a new test runner.
- Treating every relationship name as a distinct form when only the name changes.

Stepwise traversal code is evidence for standalone one-hop cases only. Each such case must test one hop's generated criteria and projection: the current endpoint ID anchor, relationship kind constraint, endpoint kind/property predicates, and returned values. The tests must not sequence those hops or assert higher-level BloodHound path results.

## Normalization rule

Normalize every discovered query into this tuple:

```text
query target
+ direction
+ start/end ID anchor
+ start/end kind constraints
+ relationship kind constraints
+ node/relationship property predicates
+ logical grouping
+ projection
+ terminal operation
```

Two call sites may share a regression case only when this entire tuple is equivalent. Add a new case whenever a call site introduces a new operator, grouping, direction, anchor location, projection, or mutation target. Relationship names may share a case, but kind-list cardinality is a test dimension because one kind and a 30-kind disjunction can produce materially different translations and plans.

Do not duplicate already-covered primitive predicates merely to rename them after BloodHound schema elements. Audit the primitive first, then add the production composition, builder path, parameter cardinality, projection, or scale dimension that is actually absent.

The canonical forms below are schematic normalization labels, not copy-paste Cypher. Implement each case with syntax accepted by the relevant frontend while preserving the stated tuple and truth table.

## Coverage layers

The query-form tables use these layer identifiers:

| ID | Layer | Purpose |
|---|---|---|
| `QB` | Legacy query-builder pipeline tests | Preserve the AST and both backend forms actually constructed from BHE/BHCE criteria. Required for rewrite-sensitive forms; raw Cypher alone is insufficient. |
| `CY` | Cypher parser/mutation cases | Preserve accepted syntax, formatting, and mutation parsing. |
| `PG` | PostgreSQL translation goldens | Preserve SQL, parameters, binding correlation, projection, and mutation target. |
| `IT` | Shared integration cases | Prove backend-equivalent results and exact mutation effects. |
| `PC` | Plan corpus | Capture plain PostgreSQL `EXPLAIN`, translated SQL, and lowering metadata for later comparison. |
| `PI` | PostgreSQL plan-invariant test | Assert index use, binding orientation, filter placement, affected rows, or another stable optimizer invariant on a seeded PostgreSQL fixture. |
| `SC` | Scale/runtime corpus | Exercise representative cardinality and selectivity with repeatable baselines. |
| `DR` | Driver integration/benchmark | Exercise direct driver and batch APIs that do not pass through Cypher translation. |

Coverage rules:

1. Every active form with a Cypher equivalent gets `PG` and `IT` coverage.
2. Every form produced through the legacy builder gets `QB`; rewrite-sensitive forms must also get `IT` coverage through the builder API rather than only through an equivalent raw string.
3. Every Cypher mutation gets `CY`, `PG`, and an `IT` post-state assertion. Direct driver mutations get `DR` semantic post-state coverage instead.
4. Every high-cardinality or join-sensitive read/delete gets `PC`; the representative forms identified in Phase 7 also get `SC`, and the listed plan-sensitive forms get `PI`.
5. Every direct driver mutation gets `DR` semantic coverage; batched operations also get flush-boundary coverage.
6. Shared integration cases must remain backend-equivalent. PostgreSQL-only plan and runtime assertions belong in PostgreSQL-scoped tests or the scale corpus.

## Planned artifacts

Keep the stable case IDs from this plan in test names and generated case descriptions so failures map back to production evidence. Prefer these repository homes, splitting a file only when it becomes unwieldy:

| Coverage | Planned home |
|---|---|
| Legacy builder construction and backend lowering | `query/builder_test.go`, `query/neo4j/neo4j_test.go`, and a focused legacy-builder-to-PostgreSQL pipeline test |
| Cypher mutation parsing | `cypher/test/cases/mutation_tests.json` |
| PostgreSQL translation goldens | `cypher/models/pgsql/test/translation_cases/reconciliation.sql` and `post_processing.sql` |
| Backend-equivalent semantics | `integration/testdata/templates/reconciliation_shapes.json` and `post_processing_shapes.json`; add focused files under `integration/testdata/cases/` only for non-template cases |
| Plain plan-corpus capture | The shared integration cases above, consumed directly by `cmd/plancorpus` |
| PostgreSQL plan assertions | `integration/pgsql_reconciliation_plan_test.go` and `integration/pgsql_post_processing_plan_test.go` |
| Direct driver and batch contracts | driver-scoped integration tests, following `drivers/neo4j/batch_integration_test.go`, plus an equivalent PostgreSQL-scoped home |
| Repeatable Cypher scale cases | `benchmark/testdata/scale/cases/reconciliation.json` and `post_processing.json` |
| Direct-driver mutation performance | Go driver benchmarks with explicit fixture reset/rollback, not a `ScaleCase` JSON file |

Phase 0 should establish any missing harness support before these files are populated; do not encode backend-specific expectations in the shared generated semantic cases.

## Delivery sequence

| Phase | Outcome | Depends on |
|---|---|---|
| 0 | Mutation assertions, reusable fixtures, and safe scale execution exist. | None |
| 1 | Logical grouping and legacy-builder rewrite hazards are locked down. | Phase 0 for mutation effects |
| 2 | BHE ingestion reconciliation delete forms are covered. | Phases 0-1 |
| 3 | Trust reconciliation, pruning, and aging forms are covered. | Phases 0-1 |
| 4 | Standalone one-hop forms derived from stepwise post-processing are covered. | Phase 1 |
| 5 | Wide scans, lookup predicates, and projection variants are covered. | Phase 1 |
| 6 | Direct driver delete/create/update forms are covered. | Phase 0 |
| 7 | Production-like plan and scale baselines are recorded. | Phases 2-6 |
| 8 | Dormant forms and ongoing source-parity checks are recorded. | Phases 2-7 |

## Phase 0: Test prerequisites

Complete these prerequisites before adding mutation cases in bulk.

- [x] Create a coverage manifest keyed by the IDs in this document and classify each required layer as existing, primitive-only, production-complete, or absent. Link existing test names rather than cloning equivalent primitives.
- [x] Extend both integration schemas and runners so a case can execute a mutation and then run one or more state assertions inside the same rollback transaction: `testCase` in `integration/cypher_test.go` and `cypherTemplateVariant` in `integration/cypher_template_test.go`.
- [x] Always drain and check the mutation result before inspecting state.
- [x] Support assertions for exact surviving node fixture IDs, exact surviving relationship triples, properties, and counts.
- [x] Add a backend-equivalent integration helper for executing legacy `NodeQuery` and `RelationshipQuery` criteria directly. An equivalent raw Cypher case does not exercise legacy AST construction or Neo4j preparation.
- [x] Require every mutation fixture to contain positive matches and decoys for direction, kind, property, ID, null/missing property, and relationship property where applicable.
- [x] Add a reusable reconciliation/post-processing fixture with typed endpoints, multi-kind nodes, duplicate edge kinds, missing properties, timestamps, and high-degree nodes.
- [x] Add deterministic fixture generators for list sizes and fanout; do not commit enormous handwritten JSON fixtures.
- [x] Append the synthetic 9- and 30-kind golden-test kinds to `translationTestKinds()` in `cypher/models/pgsql/test/translation_test.go`; never insert them before existing kinds and renumber established goldens.
- [x] Add list-valued fixture-ID parameter resolution to the scale corpus so `StartID`/`EndID` list forms do not require hard-coded database IDs.
- [x] Add typed temporal parameter support. Legacy-builder tests must pass `time.Time`; raw Cypher cases must use typed decoding or an explicit form such as `datetime($threshold)` so the test cannot pass through lexical string comparison.
- [x] Before putting Cypher mutations in a `ScaleCase` file, add an explicit write-scenario mode with expected matched/affected/post-state fields and rollback/reset semantics so warm-up and earlier iterations cannot change later measurements. Until then, keep only the selection-equivalent reads in JSON and measure actual direct mutations in Go `DR` benchmarks.
- [x] Record source commit and DAWGS version metadata with generated plan/scale baselines.

Exit criteria:

- A deliberately over-broad delete fails because a decoy disappears.
- A deliberately under-broad delete fails because a target survives.
- Re-running a delete case against its original fixture produces the same assertion result.
- Benchmark mutation iterations begin from identical graph state.

## Phase 1: Logical and builder correctness sentinels

These cases protect logical structure before expanding the corpus.

| ID | Canonical form | Required variants and assertions | Layers | Source |
|---|---|---|---|---|
| `LOGIC-01` | `(forward IDs AND r:KindA) OR (reverse IDs AND r:KindB)` | Include both valid combinations and both invalid kind/direction combinations. Verify branch-local kind predicates remain branch-local after rendering. | `QB`, `PG`, `IT`, `PC` | [BHE trust follow-up](bhe/lib/go/analysis/ad/post.go#L134), [Neo4j rewrite](query/neo4j/rewrite.go#L130) |
| `LOGIC-02` | `r.lastseen < s.lastcollected OR r.lastseen < e.lastcollected` | Older than start only, older than end only, older than both, equal, newer, and missing/null on each binding. | `QB`, `PG`, `IT`, `PC` | [BHE stale trust](bhe/lib/go/analysis/ad/post.go#L100) |
| `LOGIC-03` | `NOT KindIn(...) AND (NOT exists(p) OR p < $value)` | Prove negation applies only to its intended matcher and that missing, null, and present properties retain backend parity. | `QB`, `PG`, `IT` | [BHE pruning](bhe/lib/go/analysis/pruning/pruning.go#L147) |
| `LOGIC-04` | Filtered `DELETE r` and `DETACH DELETE n` | Preserve the selected mutation binding through optimization. Include another bound node/relationship that must survive. | `CY`, `PG`, `IT`, `PC` | [BHE reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L173) |
| `LOGIC-05` | Custom directional projections | Cover full opposite node plus relationship, opposite ID/kinds plus relationship ID/kind, start/relationship/end triple, relationship ID only, and full relationship. Assert column order and types. | `QB`, `PG`, `IT` | [ops directional fetch](ops/ops.go#L310), [active kinds projection](bhe/bhce/packages/go/analysis/ad/post.go#L286) |

Do not proceed with the nested-`OR` reconciliation case until `LOGIC-01` proves that the legacy Neo4j rewrite preserves the intended truth table.

## Phase 2: BHE ingestion reconciliation forms

### Relationship reads and deletes

| ID | Canonical form | Required variants | Layers | Source |
|---|---|---|---|---|
| `REC-01` | `MATCH (s)-[r:K1\|...\|Kn]->(e:EntityKind) WHERE e.objectid = $id DELETE r` | `n = 1, 2, 9, 30`; zero, one, and many matching edges; multi-kind endpoint; wrong endpoint kind/property and wrong edge-kind decoys. | `QB`, `CY`, `PG`, `IT`, `PC`, `SC` | [Inbound structure reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L181) |
| `REC-02` | `MATCH (s:EntityKind)-[r:K1\|...\|Kn]->(e) WHERE s.objectid = $id DELETE r` | Mirror every `REC-01` variant to protect start/end join orientation. | `QB`, `CY`, `PG`, `IT`, `PC`, `SC` | [Outbound structure reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L192) |
| `REC-03` | Endpoint-anchored `MemberOf` delete plus `r.isprimarygroup = $flag` | Inbound/`false` and outbound/`true`; missing property; opposite boolean; non-`MemberOf` decoy. | `QB`, `CY`, `PG`, `IT`, `PC` | [Primary-group reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L202) |
| `REC-04` | `MATCH ()-[r:K]->(e:Entity) WHERE e.objectid IN $object_ids DELETE r` | Empty, singleton, duplicate, small, 1,000-item, and large lists; no-match and high-match selectivity; AD and Azure base kinds. | `QB`, `CY`, `PG`, `IT`, `PC`, `SC` | [Azure reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L80), [computer reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L371) |
| `REC-05` | `MATCH (s:CertTemplate)-[r:PublishedTo]->(e) WHERE e.objectid IN $ca_ids RETURN r, s` | Empty/single/large CA list; duplicate paths to the same template; full directional hydration. Raw results must retain relationship rows, while the `FetchStartNodes` helper contract must de-duplicate its returned node set. | `QB`, `PG`, `IT`, `PC` | [Delegated enrollment discovery](bhe/lib/go/daemons/datapipe/ingest.go#L45) |
| `REC-06` | `MATCH ()-[r:DelegatedEnrollmentAgent]->(e:CertTemplate) WHERE id(e) IN $template_ids DELETE r` | Empty/single/large ID list and decoys for end kind, direction, and relationship kind. | `QB`, `CY`, `PG`, `IT`, `PC`, `SC` | [Delegated enrollment delete](bhe/lib/go/daemons/datapipe/ingest.go#L70) |
| `REC-07` | `MATCH ()-[r:HostsCAService]->(e:EnterpriseCA) WHERE e.objectid = $id DELETE r` | Exact hit, no hit, wrong CA kind, wrong object ID, and duplicate matching edges. | `QB`, `CY`, `PG`, `IT`, `PC` | [HostsCAService reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L240) |

### Node deletion

| ID | Canonical form | Required variants | Layers | Source |
|---|---|---|---|---|
| `REC-08` | `MATCH (n:ADEntity) WHERE n.objectid IN $object_ids DETACH DELETE n` | Empty/single/large list; wrong kind and wrong property decoys; isolated, low-degree, and high-degree targets; inbound, outbound, and self-incident edges. | `QB`, `CY`, `PG`, `IT`, `PC`, `SC` | [Removal ingestion](bhe/lib/go/daemons/datapipe/ingest.go#L427) |

Phase 2 exit criteria:

- Every delete verifies exact targets and exact survivors, not merely successful execution.
- Equality and list forms exist in both endpoint orientations where production has both.
- PostgreSQL goldens show that filtering occurs before mutation and that the delete targets the intended binding.
- The plan corpus contains all active BHE reconciliation forms.

## Phase 3: Trust reconciliation, pruning, and aging

| ID | Canonical form | Required variants | Layers | Source |
|---|---|---|---|---|
| `TRUST-01` | Typed Domain endpoints, `SameForestTrust`, temporal cross-binding `OR`, return relationship IDs | Truth-table and null variants from `LOGIC-02`; sparse and dense trust edges. | `QB`, `PG`, `IT`, `PC`, `SC` | [Same-forest reconciliation](bhe/lib/go/analysis/ad/post.go#L100) |
| `TRUST-02` | Same temporal form for `CrossForestTrust`, return full relationships | Same result IDs as the ID-only form while also validating full hydration/properties. | `QB`, `PG`, `IT`, `PC`, `SC` | [Cross-forest reconciliation](bhe/lib/go/analysis/ad/post.go#L118) |
| `TRUST-03` | Directional/type disjunction from `LOGIC-01`, return IDs | Execute once with each orientation as the driving stale trust edge; include invalid cross-combinations. | `QB`, `PG`, `IT`, `PC` | [Derived trust-edge lookup](bhe/lib/go/analysis/ad/post.go#L134) |
| `PRUNE-01` | `NOT r:<non-prunable kinds> AND r.lastseen < $threshold`, return IDs | One and several excluded kinds; older/equal/newer/missing/null timestamps; low/high selectivity. | `QB`, `PG`, `IT`, `PC`, `SC` | [General relationship TTL](bhe/lib/go/analysis/pruning/pruning.go#L160) |
| `PRUNE-02` | `r:HasSession AND (NOT exists(r.lastseen) OR r.lastseen < $threshold)`, return IDs | Missing/null/older/equal/newer; wrong relationship-kind decoys. | `QB`, `PG`, `IT`, `PC`, `SC` | [HasSession TTL](bhe/lib/go/analysis/pruning/pruning.go#L176) |
| `PRUNE-03` | `NOT n:<non-prunable kinds> AND (NOT exists(n.lastseen) OR n.lastseen < $threshold)`, return IDs | Multi-kind protected nodes, missing/null/present values, and low/high selectivity. | `QB`, `PG`, `IT`, `PC`, `SC` | [Node TTL](bhe/lib/go/analysis/pruning/pruning.go#L193) |
| `PRUNE-04` | `NOT n:<non-prunable kinds> AND NOT exists(n.name) AND n.objectid STARTS WITH $sid_prefix`, return IDs | Missing versus null/empty name, matching/nonmatching prefix, and protected multi-kind nodes. | `QB`, `PG`, `IT`, `PC`, `SC` | [Orphan pruning](bhe/lib/go/analysis/pruning/pruning.go#L115) |
| `PRUNE-05` | ID-selection result followed by batched relationship deletion | Empty/single/many result sets and an ID that is absent by delete time. | `DR`, `SC` | [PruneRelationships](bhe/lib/go/analysis/pruning/pruning.go#L75) |
| `PRUNE-06` | ID-selection result followed by batched node deletion/cascade | Empty/single/many; high-degree nodes; mixed inbound/outbound edges; survivor verification. | `DR`, `SC` | [PruneNodes](bhe/lib/go/analysis/pruning/pruning.go#L35) |

## Phase 4: Standalone one-hop forms derived from post-processing

Each case in this phase is an independent one-hop query. No case should call a BloodHound pattern builder, loop over prior results, or reconstruct an end-to-end path.

Active hop cases use the full directional output family:

```cypher
RETURN r, e
```

Reverse the endpoint projection for inbound cases. Do not require the `LightweightDriver` shallow projection for every hop: it is not the projection used by the reviewed active BHCE patterns. Its active component shapes are covered by `SCAN-06` and `SCAN-07`; any shallow `HOP-*` variant is optional DAWGS support coverage, not BHE/BHCE parity coverage.

| ID | One-hop form | Required variants | Layers | Evidence |
|---|---|---|---|---|
| `HOP-01` | Bound start ID plus one relationship kind | Exact ID and one-element `IN`; zero/one/high fanout; full relationship plus end-node projection. | `QB`, `PG`, `IT`, `PC`, `SC` | [Traversal anchor construction](traversal/traversal.go#L51), [ops traversal](ops/traversal.go#L73) |
| `HOP-02` | Bound end ID plus one relationship kind | Inbound mirror of `HOP-01`. | `QB`, `PG`, `IT`, `PC`, `SC` | [Traversal anchor construction](traversal/traversal.go#L62) |
| `HOP-03` | Bound endpoint plus `r:K1\|...\|Kn` | `n = 2, 5, 9, 30`; outbound and inbound; one allowed and many disallowed kinds at the anchor. | `QB`, `PG`, `IT`, `PC`, `SC` | [Azure role kinds](bhe/bhce/packages/go/analysis/azure/filters.go#L28), [AD consolidated rights](bhe/bhce/packages/go/analysis/ad/queries.go#L1842) |
| `HOP-04` | Bound endpoint plus relationship kinds plus opposite endpoint `Kind`/`KindIn` | Single/multiple endpoint kinds and multi-kind nodes; wrong-kind decoys. | `QB`, `PG`, `IT`, `PC`, `SC` | [ADCS hop examples](bhe/bhce/packages/go/analysis/ad/esc1.go#L92), [Azure tenant adjacency](bhe/bhce/packages/go/analysis/azure/tenant.go#L66) |
| `HOP-05` | Bound endpoint plus endpoint ID equality or `IN` in addition to kind constraints | Empty/single/large ID sets; endpoint ID predicate matching and contradicting the traversal anchor; exercise active builder spellings that pass `StartID`/`EndID` and `Start`/`End` to `InIDs`. | `QB`, `PG`, `IT`, `PC`, `SC` | [ADCS ID-constrained hops](bhe/bhce/packages/go/analysis/ad/esc3.go#L816) |
| `HOP-06` | Bound endpoint plus simple opposite-end property predicate | Boolean `true/false`, numeric equality, string equality, missing/null property, and the production string value `"true"` for role-assignable groups. | `QB`, `PG`, `IT`, `PC` | [Azure role-assignable hop](bhe/bhce/packages/go/analysis/azure/filters.go#L83), [NTLM endpoint property](bhe/bhce/packages/go/analysis/ad/ntlm.go#L330) |
| `HOP-07` | Bound endpoint plus nested `AND`/`OR` over opposite-end properties | Preserve the production-style schema-version branches, `>`, boolean equality, and numeric equality. Include one decoy failing each leaf and decoys satisfying only cross-branch combinations. | `QB`, `PG`, `IT`, `PC`, `SC` | [ESC1 certificate-template hop](bhe/bhce/packages/go/analysis/ad/esc1.go#L97), [ESC3 variant](bhe/bhce/packages/go/analysis/ad/esc3.go#L782) |
| `HOP-08` | Bound endpoint plus collection-property predicates | `size(e.values) = 0`, `$value IN e.values`, empty/nonempty/missing/null arrays, and nested `OR` with scalar predicates. | `QB`, `PG`, `IT`, `PC` | [ESC10 template criteria](bhe/bhce/packages/go/analysis/ad/esc10.go#L217) |
| `HOP-09` | Two-sided ID lists plus relationship kind | Empty/single/large list on each side, overlapping/nonoverlapping sets, duplicate IDs, and dense edges between both sets. | `QB`, `PG`, `IT`, `PC`, `SC` | [Special-group membership](bhe/bhce/packages/go/analysis/ad/esc_shared.go#L337) |
| `HOP-10` | Opposite endpoint kind plus property plus bound endpoint | Both start-filtered and end-filtered orientations; return start/end node, start/end ID, and relationship as separate projection variants. | `QB`, `PG`, `IT`, `PC` | [Azure post adjacency](bhe/bhce/packages/go/analysis/azure/post.go#L145), [AD local-group lookup](bhe/bhce/packages/go/analysis/ad/post.go#L454) |

Phase 4 exit criteria:

- Every criteria operator used by a reviewed hop appears in at least one standalone one-hop case.
- Both ID-anchor orientations and the active full directional projection are covered; shallow projection support is tracked separately.
- Complex predicates are tested as a single hop and are not embedded in a variable-length or client-side traversal test.

## Phase 5: Wide scans, lookup predicates, and projections

### Relationship scans

| ID | Canonical form | Required variants | Layers | Source |
|---|---|---|---|---|
| `SCAN-01` | Start/end base kinds plus one post-processed relationship kind, return IDs | AD/Azure base-kind alternatives; exact relationship kind; sparse/dense matches; ID-only projection. | `QB`, `PG`, `IT`, `PC`, `SC` | [DeleteTransitEdges](bhe/bhce/packages/go/analysis/post/post.go#L32) |
| `SCAN-02` | `NOT` Meta start/end kinds plus relationship `KindIn`, return full relationships | One/many relationship kinds; Meta only on start, end, and both; multi-kind Meta nodes; property hydration. | `QB`, `PG`, `IT`, `PC`, `SC` | [Delta tracker](bhe/bhce/packages/go/analysis/post/tracker.go#L295) |
| `SCAN-03` | `NOT` Meta endpoints plus exact relationship kind plus `exists(r.lastseen)`, return IDs | Present/null/missing `lastseen`; one kind per scan; Meta decoys. | `QB`, `PG`, `IT`, `PC`, `SC` | [DCA migration](bhe/bhce/packages/go/analysis/post/migration.go#L32) |
| `SCAN-04` | Raw relationship kind plus `start:Entity`, return full relationships | `OwnsRaw` and `WriteOwnerRaw` representatives; wrong start kind; high-cardinality targets. | `QB`, `PG`, `IT`, `PC`, `SC` | [Owns/WriteOwner](bhe/bhce/packages/go/analysis/ad/owns.go#L93) |
| `SCAN-05` | `start:Entity`, nine relationship kinds, bound end ID, return relationship plus start node | One versus nine kinds; zero/one/high inbound degree; full hydration and partition-by-kind correctness. | `QB`, `PG`, `IT`, `PC`, `SC` | [Consolidated ADCS inbound scan](bhe/bhce/packages/go/analysis/ad/queries.go#L1842) |
| `SCAN-06` | Relationship kind plus typed end, return `id(s), id(r), type(r), id(e)` | Assert the exact `FetchKinds` column order/types and avoid accidental full-property or node-kind projection in `PG`. | `QB`, `PG`, `IT`, `PC` | [LocalToComputer kind scan](bhe/bhce/packages/go/analysis/ad/post.go#L271) |
| `SCAN-07` | Relationship kind only, return start/end IDs | One/many edge kinds, zero/sparse/dense matches, and duplicate endpoints. Keep the database form as one directed `id(s), id(e)` scan; inbound/outbound interpretation by an in-memory consumer is not another query form. | `QB`, `PG`, `IT`, `PC`, `SC` | [Directed graph loaders](bhe/bhce/packages/go/analysis/ad/post.go#L733), [ID-pair projection](container/fetch.go#L12) |
| `SCAN-08` | Start `KindIn`, end ID `IN`, relationship `KindIn`, optional end `KindIn`, return start IDs | ESC9 scenario A: three start kinds, large victim-ID list, six relationship kinds, no end-kind restriction. Scenario B: the same anchors, end `Computer`, and five relationship kinds. Cross empty/single/large victim lists with sparse/dense matches and wrong start/end/edge-kind decoys. | `QB`, `PG`, `IT`, `PC`, `SC` | [ESC9/ESC10 attacker scan](bhe/bhce/packages/go/analysis/ad/queries.go#L1866) |

### Node and relationship lookups

| ID | Canonical form | Required variants | Layers | Source |
|---|---|---|---|---|
| `LOOKUP-01` | Node `Kind`/`KindIn`, return IDs or full nodes | One/many kinds, multi-kind nodes, ID-only versus full hydration. | `QB`, `PG`, `IT`, `PC` | [AD post scans](bhe/bhce/packages/go/analysis/ad/post.go#L242), [Azure tenants](bhe/bhce/packages/go/analysis/azure/tenant.go#L81) |
| `LOOKUP-02` | Node kind plus one or two property equalities, optionally `LIMIT 1`/`First` | Indexed object ID, no-kind object ID lookup, boolean property, two strings, hit/no-hit/multiple-hit. | `QB`, `PG`, `IT`, `PC`, `SC` | [Trust account](bhe/bhce/packages/go/analysis/ad/post.go#L347), [well-known node](bhe/bhce/packages/go/analysis/ad/ad.go#L440) |
| `LOOKUP-03` | Node kind plus boolean property, return node ID and that property | `true`, `false`, null, and missing; preserve two-column projection order/type. | `QB`, `PG`, `IT` | [URA lookup](bhe/bhce/packages/go/analysis/ad/post.go#L621) |
| `LOOKUP-04` | Property `STARTS WITH`/`ENDS WITH` plus kind/equality predicates | Case-sensitive prefix/suffix, OR of two suffixes, matching/nonmatching kind, and combined domain equality. | `QB`, `PG`, `IT`, `PC`, `SC` | [AdminSDHolder lookup](bhe/bhce/packages/go/analysis/ad/post.go#L425), [admin group suffixes](bhe/bhce/packages/go/analysis/ad/owns.go#L308) |
| `LOOKUP-05` | Case-insensitive `STARTS WITH` or `CONTAINS` | Exact-case and mixed-case values; literal `%` and `_` input; substring false positives retained for application-side exact checking; repeated lookup scale. | `QB`, `PG`, `IT`, `PC`, `SC` | [Local group name](bhe/bhce/packages/go/analysis/ad/post.go#L545), [Azure approver lookup](bhe/bhce/packages/go/analysis/azure/role_approver.go#L196) |
| `LOOKUP-06` | Required and negated kind groups combined with suffix/equality predicates | Cover `(Group OR User) AND Entity AND objectid ENDS WITH $suffix AND domainsid = $domain`, plus `Entity AND NOT (Group OR LocalGroup) AND objectid ENDS WITH $suffix`. Include nodes having both included and excluded kinds. | `QB`, `PG`, `IT`, `PC` | [Well-known selection](bhe/bhce/packages/go/analysis/ad/ad.go#L59), [type repair](bhe/bhce/packages/go/analysis/ad/ad.go#L105) |
| `LOOKUP-07` | `NOT exists(n.name)` | Missing, explicit null, empty string, and populated property. | `QB`, `PG`, `IT` | [Domain association](bhe/bhce/packages/go/analysis/ad/ad.go#L153) |
| `LOOKUP-08` | Kind and booleans plus `propertyA IS NOT NULL OR propertyB IS NOT NULL` | Neither, either, and both present; null versus missing; wrong tenant and approval flag decoys. | `QB`, `PG`, `IT`, `PC` | [Azure role approvers](bhe/bhce/packages/go/analysis/azure/role_approver.go#L67) |
| `LOOKUP-09` | `id(n) IN $ids`, return full nodes | Empty/single/duplicate/1,000/large lists; sparse and dense matches. | `QB`, `PG`, `IT`, `PC`, `SC` | [Owns target hydration](bhe/bhce/packages/go/analysis/ad/owns.go#L104) |
| `LOOKUP-10` | Kind plus nested negated property-presence/value pairs plus `id(n) IN $ids` | `NOT (exists(gmsa) AND gmsa=true)` and the MSA mirror; all missing/null/boolean combinations. | `QB`, `PG`, `IT`, `PC` | [ADCS user filtering](bhe/bhce/packages/go/analysis/ad/esc_shared.go#L388) |
| `LOOKUP-11` | Bound tenant start plus `Contains`, endpoint kinds, optional endpoint property `IN`/equality | End-kind list sizes, role-template ID lists, boolean/string endpoint property, empty/single/large lists. | `QB`, `PG`, `IT`, `PC`, `SC` | [Azure tenant adjacency](bhe/bhce/packages/go/analysis/azure/tenant.go#L99), [Azure post reads](bhe/bhce/packages/go/analysis/azure/post.go#L145) |
| `LOOKUP-12` | Exact start ID, end ID, and relationship kind followed by `First` | Hit/no-hit, reverse-direction decoy, wrong-kind decoy, duplicate prevention. | `QB`, `PG`, `IT`, `PC` | [Well-known edge upsert lookup](bhe/bhce/packages/go/analysis/ad/ad.go#L481) |
| `LOOKUP-13` | Endpoint property suffix plus relationship kind and bound opposite endpoint | Return full start node and start ID as separate cases; wrong suffix/kind/end decoys. | `QB`, `PG`, `IT`, `PC`, `SC` | [Local group by SID suffix](bhe/bhce/packages/go/analysis/ad/post.go#L454) |
| `LOOKUP-14` | Kind scan ordered by a node property descending | Missing/equal/distinct sort properties, multi-kind nodes, and deterministic tie handling only when a secondary key is specified. | `QB`, `PG`, `IT`, `PC` | [Ordered Domain scan](bhe/bhce/packages/go/analysis/ad/queries.go#L101) |
| `LOOKUP-15` | Sequential unfiltered node and relationship counts | Audit the existing count corpus and direct `Nodes().Count()`/`Relationships().Count()` contract against empty, node-only, edge-bearing, and dense graphs. Keep concurrency with writers out of this query-form case. | `IT`, `SC` | [BHCE changelog sizing](bhe/bhce/cmd/api/src/daemons/changelog/flag.go#L159) |
| `LOOKUP-16` | Four node-property equalities, optionally with a node kind | Typed `Computer` and untyped forms; domain string, `isdc = true`, availability `= true`, and signing/EPA `= false`; LDAP and LDAPS property sets; ID-only and full-node projections; one decoy failing each leaf. | `QB`, `PG`, `IT`, `PC`, `SC` | [Typed NTLM lookup](bhe/bhce/packages/go/analysis/ad/ntlm.go#L624), [untyped NTLM cache lookup](bhe/bhce/packages/go/analysis/ad/ntlm.go#L882) |

## Phase 6: Direct driver mutation forms

These cases exercise DAWGS driver APIs rather than raw Cypher. Keep their semantic fixtures aligned across drivers where the API contract is shared, while retaining PostgreSQL-specific plan/runtime checks separately.

| ID | Operation form | Required variants | Layers | Source |
|---|---|---|---|---|
| `WRITE-01` | `DeleteRelationship(id)` buffered into `DELETE ... WHERE id = ANY($1)` | Empty, 1, 1,000, 1,999, 2,000, 2,001, 4,001, and larger batches; duplicate and missing IDs; exact survivor set. | `DR`, `SC` | [Post sink deletion](bhe/bhce/packages/go/analysis/post/sink.go#L126), [PG statement](drivers/pg/statements.go#L28) |
| `WRITE-02` | `DeleteNode(id)` buffered into `DELETE ... WHERE id = ANY($1)` | Same size boundaries; duplicate and missing IDs; isolated and self-connected targets; low/high incident-edge degree; mixed directions; cascade survivor checks. | `DR`, `SC` | [BHE pruning](bhe/lib/go/analysis/pruning/pruning.go#L35), [PG statement](drivers/pg/statements.go#L19) |
| `WRITE-03` | Batched `CreateRelationshipByIDs` with conflict update/property merge | Unique edges; the same edge submitted repeatedly; duplicates within one buffer and across flushes; reversed endpoints and different relationship kinds as non-conflicts; empty/mixed properties; `firstseen`/`lastseen` plus custom properties; assert the documented winner/merge result for conflicting keys. | `DR`, `SC` | [Post writer](bhe/bhce/packages/go/analysis/post/operation.go#L57), [PG conflict statement](drivers/pg/statements.go#L23) |
| `WRITE-04` | `UpdateNodeBy` keyed by `objectid` | Insert versus update; duplicate object IDs in one batch and across retry/flush boundaries; last-seen replacement; 1,000-item changelog batch and DAWGS flush boundaries. | `DR`, `SC` | [BHCE node changelog](bhe/bhce/cmd/api/src/daemons/changelog/model.go#L86) |
| `WRITE-05` | `UpdateRelationshipBy` keyed by start/end `objectid` and relationship kind | Missing/existing endpoints; insert/update; duplicate updates within and across retries; reversed endpoints and mixed relationship kinds as distinct keys; property merge; 1,000-item batch and flush boundaries. | `DR`, `SC` | [BHCE edge changelog](bhe/bhce/cmd/api/src/daemons/changelog/model.go#L149) |
| `WRITE-06` | Read-by-exact-key followed by create or `UpdateRelationship` | Existing and absent edge, timestamp/property update, idempotent repeat, reverse edge decoy. | `DR`, `IT` | [Well-known edge maintenance](bhe/bhce/packages/go/analysis/ad/ad.go#L481) |
| `WRITE-07` | Full-node `UpdateNode` after suffix, missing-property, or kind query | Update properties only, kinds only, and both; verify unrelated kinds/properties survive. | `DR`, `IT` | [Well-known/domain fixes](bhe/bhce/packages/go/analysis/ad/ad.go#L105), [management-group naming](bhe/bhce/packages/go/analysis/azure/post.go#L994) |
| `WRITE-08` | Direct `CreateNode` with properties and multiple kinds after exact-key miss | Create with generic `Entity` and `Group` kinds plus the complete property bag; exact object-ID miss creates once, while a hit returns/updates the existing node rather than creating a duplicate. Keep selector and driver-operation assertions separable. | `DR`, `IT` | [Well-known node creation](bhe/bhce/packages/go/analysis/ad/ad.go#L437) |

Execution variants, not new query forms:

- Run `WRITE-01` through `WRITE-05` at DAWGS' flush boundary and at BHCE's 1,000-item changelog batch size.

## Phase 7: Plan and scale baselines

### Required scale representatives

Add scale cases for at least these IDs:

- `REC-01`, `REC-02`, `REC-04`, `REC-06`, and `REC-08`.
- `TRUST-01`, `TRUST-02`, and `PRUNE-01` through `PRUNE-04`.
- `HOP-01` through `HOP-05`, `HOP-07`, and `HOP-09` as standalone one-hop queries.
- `SCAN-01` through `SCAN-05`, `SCAN-07`, and `SCAN-08`.
- `LOOKUP-02`, `LOOKUP-04`, `LOOKUP-05`, `LOOKUP-09`, `LOOKUP-11`, `LOOKUP-13`, `LOOKUP-15`, and `LOOKUP-16`.
- `WRITE-01` through `WRITE-05` in a mutation-safe driver benchmark.

### Required plan-invariant representatives

`PC` capture is observational and uses plain `EXPLAIN`; it does not prove index use, join orientation, or scaled runtime behavior. Add PostgreSQL-scoped `PI` assertions for `LOGIC-01`, `LOGIC-02`, `LOGIC-04`, and every Cypher query listed under required scale representatives. For mutation cases, assert the selection/target plan and affected rows inside rollback rather than depending only on captured plan text.

### Cardinality matrix

Use the smallest matrix that exposes plan changes while retaining the production extremes:

| Dimension | Required points |
|---|---|
| Relationship-kind list | 1, 2, 9, 30 |
| ID/property list | 0, 1, 32, 1,000, 1,999, 2,000, 2,001, and a larger stress value |
| Anchor selectivity | no match, one match, many matches, and most rows |
| One-hop fanout | 0, 1, moderate, and dense |
| Endpoint degree for node delete | isolated, low, and high in both directions |
| Property state | missing, null, false/zero/empty, matching, and nonmatching |
| Equality conjunction width | 1, 2, and 4 predicates, plus separately grouped nested logic |
| Projection | ID-only, IDs/kinds, full relationship, full endpoint, and relationship plus endpoint |
| Duplicate write input | none, repeated within a batch, and repeated across flushes |

### Baseline procedure

- [x] Capture PostgreSQL translated SQL, plan text/operators, lowering metadata, row counts, and runtime statistics on the same fixture.
- [x] Capture a `v0.6.0` reference and current-main result for the same query-form IDs when investigating the reported regression.
- [x] Run that comparison from one external/versioned harness, or apply the same test-only corpus commit to temporary worktrees for `v0.6.0` and the target revision. Do not assume the new harness exists when checking out the old tag.
- [x] Use `EXPLAIN (ANALYZE, BUFFERS)` for read-only scale cases.
- [x] Use rollback/reset isolation for mutation runtime measurements.
- [x] If the shared scale runner cannot safely execute a mutating form, benchmark its selection-equivalent read in `SC` and measure the actual mutation through the isolated `DR` benchmark; do not silently omit the mutation workload.
- [x] Compare ID-only and full-hydration projections separately; do not infer one from the other.
- [x] Flag new unbounded scans, unexpected materialization, join-order inversions, row-estimate explosions, and loss of endpoint/property index use.
- [x] Keep correctness gates deterministic. Store performance baselines and tolerances in the benchmark workflow rather than asserting a universal wall-clock threshold in unit tests.

Phase 7 exit criteria:

- Every high-priority active form has a captured plan.
- Every scale representative declares expected result or mutation cardinality.
- Reports identify query-form IDs so regressions can be mapped back to semantic fixtures and source call sites.
- Single-hop results are reported as single-hop cases; no benchmark result is labeled as a complete BloodHound traversal.

## Phase 8: Dormant forms and source-parity maintenance

### Dormant/future form

Keep this outside the active regression gate until BHE enables its caller:

| ID | Canonical form | Coverage when activated | Source |
|---|---|---|---|
| `FUTURE-01` | `MATCH (s:AZEntity)-[r:K]->() WHERE s.tenantid IN $tenant_ids DELETE r` | Add the same empty/single/large list, decoy, `PG`, `IT`, `PC`, and `SC` coverage as `REC-04`, but in the outbound orientation. | [Disabled tenant-wide reconciliation](bhe/lib/go/daemons/datapipe/ingest.go#L80) |

### Ongoing parity checklist

For each BHE/BHCE update:

- [ ] Search active reconciliation/post entry points for new `Filter`, `Filterf`, `Query`, `First`, `Count`, `Fetch*`, `Create*`, `Delete*`, `Update*`, and `BatchOperation` calls.
- [ ] Trace helpers to an active entry point; label helper-only or commented-out forms rather than presenting them as production-active.
- [ ] Normalize each active call using the tuple in this document.
- [ ] Map it to an existing query-form ID or add a new ID and source link.
- [ ] If a stepwise traversal criterion changes, update or add only the corresponding standalone `HOP-*` case.
- [ ] Recheck projection choice independently of predicate choice.
- [ ] Recheck kind-list and ID-list cardinality whenever schema relationship sets change.
- [ ] Record the BHE, BHCE, and DAWGS commits used for the audit.

## Implementation-slice validation order

For each phase or coherent case family:

1. Add or update harness coverage first.
2. Add legacy builder/render tests.
3. Add frontend source cases and PostgreSQL translation cases.
4. Run `make test_update` for analyzer/translation goldens and review the generated diff. Integration templates and case files are loaded directly; do not generate one from the other.
5. Add shared semantic cases with decoys and, for mutations, post-state verification.
6. Add PostgreSQL plan/runtime and scale representatives.
7. Add direct driver and batch contract cases where required.
8. Run `make format` and `make test`.
9. With an explicit `CONNECTION_STRING`, run `make test_all` for the selected backend. Repeat with the other supported backend when both connection strings are available.
10. Capture plan and scale baselines against the fixed source versions recorded at the top of this document.

## Completion definition

This plan is complete when:

1. All active `REC-*`, `TRUST-*`, `PRUNE-*`, `HOP-*`, `SCAN-*`, `LOOKUP-*`, and `WRITE-*` cases are implemented at their required layers.
2. Mutation cases prove exact post-state with positive and negative fixtures.
3. The branch-local relationship-kind `OR` truth table passes on every supported backend.
4. PostgreSQL translation and plan coverage includes equality and list-anchored delete forms in both directions.
5. Scale coverage distinguishes ID-only, shallow IDs/kinds, and full-hydration projections.
6. Batch coverage crosses the 2,000-item DAWGS flush boundary and the 1,000-item BHCE changelog size.
7. No new test runner or production change attempts to reproduce or alter BloodHound stepwise traversal behavior.
8. `FUTURE-*` cases remain visibly separate from active production coverage until their callers are enabled.

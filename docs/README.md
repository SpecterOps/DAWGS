# Documentation

[Project overview](../README.md)

## Guides

- [PostgreSQL driver](postgresql-driver.md): pools, caches, kind metadata, schema upgrades, and storage.
- [Query semantics and compatibility](query-semantics.md): supported forms, result types, matching, and mutations.
- [Development workflow](development.md): formatting, metrics, and contributor commands.
- [Testing](testing.md): integration prerequisites, fixture regeneration, and BDD reporting.
- [Benchmarking and traversal qualification](benchmarking.md): captures, experimental policies, and qualification boundaries.
- [Library utilities and tools](library-and-tools.md): traversal helpers, collections, and import/export.

## Reference guides

- [Cypher library](../cypher/README.md): parser generation and Cypher package overview.
- [Cypher walker semantics](cypher_walker_semantics.md): semantic and structural traversal contracts.
- [PostgreSQL translation](postgresql_translation.md): PostgreSQL translator behavior, optimizer lowerings, indexing notes, and validation expectations.
- [CySQL traversal performance priorities](cysql_traversal_priorities.md): source-grounded roadmap for orientation, SP/ASP, probes, statistics, telemetry, and qualification.
- [Traversal implementation snapshot](experiments/traversal_priority_implementation_status_v1.md): dated component identities and fail-closed gates; later dispositions are in the delivery record below.
- [Remaining traversal outlier delivery](experiments/remaining_outlier_delivery_v1.md): SP-I2, fixed-suffix, qualification, promotion-closure, and rollback handoff.
- [Production-wide SQL selection](experiments/production_wide_sql_selection_v1.md): staged structural and topology-aware PostgreSQL routing contract.
- [Topology fixed-suffix v4 status](experiments/topology_fixed_suffix_v4_status_v1.md): implemented default-off routing boundary and promotion non-activation record.
- [Topology fixed-suffix v4 capture procedure](experiments/topology_fixed_suffix_v4_capture_v1.md): clean-source evidence sequence required before a v4 policy may be installed.
- [Topology fixed-suffix first-use routing](experiments/topology_fixed_suffix_first_use_v1.md): separately versioned v5 selector and its promotion contract.
- [Composite fixed-suffix lowering](experiments/fixed_suffix_composite_lowering_v1.md): query-atomic multi-region retry substrate.
- [ASP A1 inline hydration disposition](experiments/asp_a1_inline_hydration_v1.md): parameterized attribution, A1 hydration result, and B2 negative disposition.
- [Ordinary path materialization batching](experiments/path_batch_materialization_v1.md): complete-value regression results, bulk/first-row and memory measurements, and the bounded-hydration action queue.
- [ASP N1 negative-exhaustion disposition](experiments/asp_n1_negative_exhaustion_v1.md): bounded no-path proof with exact A1 fallback and its production-selection boundary.
- [Plan corpus capture](../cmd/plancorpus/README.md): shared integration corpus plan diagnostics.
- [Graph benchmark capture](../cmd/graphbench/README.md): runtime diagnostics for scale scenarios.
- [Integration corpus](../integration/testdata/README.md): fixture, mutation post-state, and typed-parameter schema.
- [BloodHound regression coverage manifest](regression_coverage_manifest.md): per-query-form layer status and existing primitive links.
- [BloodHound source-parity workflow](regression_source_parity.md): dormant-form activation rules and repeatable BHE/BHCE source audits.
- [Cypher syntax support](../cypher/Cypher%20Syntax%20Support.md): supported Cypher behavior and semantic notes.

## Reviews and proposals

- [Review and validation records](reviews/README.md): active variance backlog, completed repairs, dated audits, and the retained PostgreSQL V2 PR draft.
- [Cypher V4 proposal](plans/cypher-support-v4.md): unimplemented translation ideas; use the query semantics and syntax references above for current support.
- [Traversal experiment records](experiments/README.md): implementation contracts, qualification procedures, and historical dispositions.

## Repository Map

- `cypher/`: parser, Cypher AST, walkers, and backend translation models.
- `drivers/`: database driver implementations.
- `integration/`: backend-equivalent integration suites and fixtures.
- `cmd/`: command-line tools for capture, export, and diagnostics.
- `tools/`: developer tools such as `dawgrun` and metrics reporting.

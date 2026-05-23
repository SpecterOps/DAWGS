# Optimizer Continuation Status: Aggregate Traversal Shape

## Current State

The aggregate traversal optimization plan has been implemented through the latest widening and selectivity work. The
work is split into focused commits:

- `9c5232c` added the initial `AggregateTraversalCount` lowering and live-query optimization work.
- `94b7d78` added a guarded PostgreSQL live-plan assertion for the aggregate traversal shape.
- `51fe99c` records skipped traversal-direction diagnostics for kind-only terminal estimates.
- `61f039d` widens aggregate traversal matching to equivalent `COUNT(*)` row-count forms.
- `7f62b68` treats uniquely constrained bound sources, such as `objectid = ...`, as selective traversal anchors.
- `97c3ffa` expands aggregate traversal baseline coverage for explicit depth bounds, inbound source-left traversal, and
  unsafe non-lowering shapes.
- `835a26f` widens final aggregate projections to preserve both the source node and the aggregate count with aliases.
- `0068c3e` carries source selectivity through multipart `WITH` projections, `LIMIT`, and top-N operations.
- `b9f7b4b` folds terminal-local filters into the aggregate traversal terminal-node materialization.

The lowering now recognizes the kerberoastable aggregate family and emits an ID-only, source-anchored recursive CTE. The
emitted SQL:

- builds source candidates as IDs;
- traverses with `root_id`, `next_id`, `depth`, and edge-ID `path`;
- uses source-anchored edge index access;
- materializes terminal node IDs once, including terminal-local predicates when present;
- groups by `root_id`;
- applies top-N before rejoining source node composites;
- can return the source node alone or the source node plus the aggregate count.

The optimizer still keeps unsafe aggregate variants out of this lowering:

- `COUNT(DISTINCT terminal)`;
- `OPTIONAL MATCH`;
- observed terminal projection or reuse beyond the aggregate count;
- path projection or path functions;
- relationship projection, relationship predicates, or relationship reuse;
- correlated terminal filters, such as `terminal.name = source.name`;
- post-aggregation predicates that depend on the count.

## Latest Validation Evidence

The current implementation has passing unit and PostgreSQL integration coverage:

```bash
go test ./cypher/models/pgsql/... -count=1
make test
CONNECTION_STRING='postgres://...' make test_integration
```

The full PostgreSQL integration run completed successfully. The `integration` package took about `351.7s`.

`make format` still fails in this environment because `goimports` is unavailable or not executable:

```text
xargs: goimports: Permission denied
```

Touched Go files were formatted with `gofmt`.

## Latest Plan Comparison

After the integration suite, the PostgreSQL database no longer looked like the restored large live aggregate dataset to
the guarded aggregate assertion:

```text
candidateUsers:0 computers:0 adminEdges:0
```

The guarded assertion therefore skipped rather than producing a large-live-dataset plan verdict. The comparison runner
still completed successfully against the current PostgreSQL and Neo4j databases and refreshed
`.coverage/live-plan-comparison.md/json`.

Current comparison-run timings:

- `group_objectid_exact_string_equality`: PostgreSQL `0.2 ms`; Neo4j oracle shape `node-label-scan + top-or-limit`.
- `domain_admins_reverse_membership_source_disjunction`: PostgreSQL `101.8 ms`; Neo4j oracle shape
  `directed-expand + node-label-scan + top-or-limit + var-length-expand`.
- `dangerous_domain_users_privileges_exclude_memberof`: PostgreSQL `99.7 ms`; Neo4j oracle shape
  `directed-expand + top-or-limit`.
- `domain_admin_logons_exclude_domain_controllers`: PostgreSQL `141.6 ms`; Neo4j oracle shape
  `directed-expand + top-or-limit + var-length-expand`.
- `kerberoastable_users_by_admin_privilege_count`: PostgreSQL `95.5 ms`; Neo4j oracle shape
  `aggregation + directed-expand + node-label-scan + top-or-limit + var-length-expand`.
- `kerberoastable_users_left_label_guard`: PostgreSQL `104.6 ms`; Neo4j oracle shape
  `aggregation + directed-expand + node-label-scan + top-or-limit + var-length-expand`.
- `shortest_path_domain_users_to_tier_zero`: PostgreSQL `201.0 ms`; Neo4j oracle shape
  `node-label-scan + top-or-limit + var-length-expand`.
- `cross_forest_trusts_require_connected_abuse_edge`: PostgreSQL `0.1 ms`; Neo4j oracle shape
  `anti-semi-apply + top-or-limit`.
- `azure_high_privileged_role_bounded_membership`: PostgreSQL `1.6 ms`; Neo4j oracle shape
  `directed-expand + node-label-scan + top-or-limit + var-length-expand`.

These numbers are smoke evidence for the current implementation. They should not replace the earlier large-live-dataset
baseline because the guarded assertion reported that the large aggregate dataset was not present.

The earlier restored-live-dataset comparison remains the best large-data aggregate baseline:

- `kerberoastable_users_by_admin_privilege_count`: `12025.1 ms` execution, `12030.6 ms` wall duration;
- `kerberoastable_users_left_label_guard`: `12376.4 ms` execution, `12379.5 ms` wall duration;
- source-anchored recursive traversal from filtered `User` candidates;
- `Hash Join` from traversal rows to materialized `terminal_nodes`;
- `HashAggregate` with `Group Key: traversal.root_id`;
- final source node primary-key lookups after the top-N stage.

The original live cardinalities that motivated this work were:

- candidate users after the property filter: about `222`;
- `Computer` nodes: about `139k`;
- `MemberOf|AdminTo` edges: about `2.09M`.

## Neo4j Oracle

The Neo4j oracle still commonly prefers label scans followed by `VarLengthExpand(All)`, eager aggregation, and top-N for
the aggregate shapes. That remains useful as an operator-order comparison, but PostgreSQL should not mirror this shape
on the observed large data. The PostgreSQL-specific win comes from filtered source candidates, ID-only traversal, root-ID
aggregation, and deferred source materialization.

The comparison runner should stay in place as the base for further oracle testing.

## Completed Plan Items

### 1. Aggregate Widening Baseline

Completed in `97c3ffa`.

Coverage now includes:

- explicit variable-length depth bounds;
- inbound source-left traversal;
- unsafe non-lowering candidates for distinct counts, optional matches, path bindings, relationship bindings, and
  post-aggregation filters.

### 2. Final Projection Widening

Completed in `835a26f`.

The aggregate traversal shape now preserves:

- source return alias;
- optional count return alias;
- final return forms that include only the source node or both source node and aggregate count.

### 3. Broader Selectivity Heuristics

Completed in `0068c3e`.

The lowering planner now carries selectivity through multipart query parts with a graded model:

- no useful selectivity;
- kind-only selectivity;
- property predicate selectivity;
- unique equality selectivity;
- limited source sets;
- top-N source sets.

This prevents a prior limited or top-N source set from being flipped toward a broad terminal unless the terminal side is
also selective enough to justify the direction change.

### 4. Terminal-Local Filter Folding

Completed in `b9f7b4b`.

Terminal-local `WHERE` predicates can now be folded into `terminal_nodes` when every referenced symbol belongs to the
terminal node. Correlated terminal filters remain excluded.

### 5. Validation and Documentation

Completed in this document update.

Validation performed:

- PostgreSQL model tests;
- unit test suite;
- PostgreSQL integration suite;
- guarded aggregate live-plan assertion, which skipped because the current PostgreSQL corpus did not match the restored
  large live dataset;
- PostgreSQL/Neo4j comparison runner, which completed and refreshed ignored comparison artifacts.

## Remaining Work

### 1. Re-run Large-Live-Dataset Vetting After Reload

The next live reload should rerun:

```bash
CONNECTION_STRING='postgres://...' go test -v -tags manual_integration ./integration \
  -run TestPostgreSQLLiveAggregateTraversalCountPlanShape \
  -count=1 -parallel=1 -timeout=90s

PG_CONNECTION_STRING='postgres://...' NEO4J_CONNECTION_STRING='neo4j://...' \
  LIVE_VET_TIMEOUT='30s' go run .coverage/live_plan_compare.go
```

Acceptance criteria:

- the guarded assertion does not skip;
- the aggregate query completes under the statement timeout;
- the recursive CTE is source-anchored;
- grouping is by `root_id`, not node composites;
- source node materialization occurs after top-N limiting;
- terminal-local filters remain inside terminal-node materialization when the query shape allows it.

### 2. Add Evidence Before Further Aggregate Widening

The next widening candidates should require specific query-corpus examples and tests before implementation:

- post-aggregation count predicates that can be safely converted to `HAVING`;
- multiple aggregate return aliases if a real query needs them;
- source-side filters that can be pushed into source candidate materialization after multipart rewrites.

### 3. Keep Broader Selectivity Conservative

The new selectivity model is intentionally coarse. Future broadening should be backed by live plan mismatches for:

- known unique equality predicates beyond `objectid`;
- source candidates reduced by prior aggregation;
- property predicates with observed high selectivity;
- label/kind combinations whose cardinality is small enough to anchor traversal.

### 4. Keep Schema Index Work Deferred

Do not add default indexes yet. The measured large-live bottleneck was traversal and aggregation shape, not candidate-user
lookup. Revisit targeted expression or partial indexes only if refreshed live plans show source candidate filtering has
become the next dominant cost.

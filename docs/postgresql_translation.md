# PostgreSQL Translation

DAWGS translates supported Cypher queries to vanilla PostgreSQL 16 SQL. The implementation lives under
[`cypher/models/pgsql`](../cypher/models/pgsql).

## Package Layout

- `format`: PostgreSQL SQL rendering.
- `translate`: openCypher-to-PostgreSQL translation.
- `optimize`: Cypher query-shape analysis and translator lowering decisions.
- `visualization`: PUML graph formatting for PostgreSQL SQL model trees.
- `test`: translation test cases.

## Optimizer Coverage

The `optimize` package analyzes Cypher query shape before PostgreSQL SQL emission. Rule outputs are exposed as planned
and applied lowerings in translation diagnostics so plan-corpus captures can catch planning/emission gaps.

Current PostgreSQL optimization coverage includes:

- Reproducible plan-corpus capture for PostgreSQL translated SQL, PostgreSQL `EXPLAIN`, Neo4j logical plan operator
  trees, planned/applied lowerings, skipped lowerings, and skipped-lowering reasons.
- Count-store fast paths for simple node and directed-edge count queries, including typed variants where kind filters
  map cleanly.
- Predicate placement accounting for binding-scope predicates pushed into fixed traversal steps, expansion seeds,
  expansion edges, expansion terminal checks, and eligible pattern predicates.
- Shared and late path materialization for path functions such as `nodes(p)`, `relationships(p)`,
  `size(relationships(p))`, `startNode`, `endNode`, and `type`.
- Recursive traversal optimizations for endpoint kind/property predicates, relationship type predicates, bound-node
  filters, traversal direction selection, and limit pushdown where ordering and distinct semantics permit it.
- Static shortest-path executor selection for one read-only, uncorrelated, directed traversal with one ID equality per
  endpoint and no observed relationship/path predicate. Distance observations use scalar `SP-S3-U-D` state, with deep
  physical-inbound searches sent to `SP-S4-C-D`. Bounded directed single-kind one-path witnesses use
  `SP-S3-U-E+MAT-M0`; deep inbound and multi-kind or untyped witnesses use
  `SP-S4-C-WE+MAT-M0`. Both S4 executors canonicalize
  expansion, keep recursive state ID-only, enforce a bounded state
  ceiling, and fall back to an exact relationship-trail query in the same statement and snapshot before returning a
  row. Singleton ties return one valid minimal trail; physical edge-ID order is not public. See
  `docs/shortest_path_tie_policy.md`.
- Default-off compact bidirectional SP candidates preserve that singleton
  endpoint and observation envelope. `SP-B1-C-ALT-NODE-D` and
  `SP-B1-C-ALT-NODE-WE+MAT-M0` alternate one accepted node per side;
  `SP-B2-C-MIN-LEVEL-D` and `SP-B2-C-MIN-LEVEL-WE+MAT-M0` expand the smaller
  complete current level. Both use ID-only invocation-local state, a
  lower-bound stop condition, late witness hydration, independent
  seen/frontier/predecessor caps, and exact S4 fallback before output. They are
  reference and explicit-tool arms; the production driver rejects them.
  Legacy `SP-I1-C-D` remains tool-only. Its guarded successor `SP-I2-C-D`
  performs reverse-physical ID-only distance discovery behind independent
  state/frontier gates, exposes same-statement runtime receipts, and invokes
  exact `SP-S4-C-D` on overflow. Production authorization is restricted to
  exact inbound typed single-kind distance buckets under selector
  `sp-static-v8-hidden-fanin` and the preregistered production-form
  `state_limit=100000`/`frontier_limit=100000` cap contract. These are
  immutable protocol inputs, not qualified values: the dirty-tree rehearsal
  stopped before a discovery report or freeze, the cycle-control point
  estimates missed the frozen bounds, and no protected holdout was opened.
  Tool-forced diagnostic translations may still use smaller caps to exercise
  overflow and fallback behavior. Eligible
  canaries require SHA-256-allowlisted queries under repeatable-read or
  serializable isolation and a schema-v2 promotion manifest whose reports
  repeat its complete authorization identity. The ordinary production path
  remains unchanged.
- Omitted shortest-path upper bounds retain Cypher's repository-defined
  effective maximum of 15. The lowering decision records
  `maximum_depth_source=policy_default` and uses selector
  `sp-static-v7-contained`; explicit bounds retain `explicit` provenance.
- Static `allShortestPaths` selection through `asp-static-v1` for a single directed, read-only endpoint pair with
  minimum depth one. `ASP-A1-DAG` has exact one- and two-hop arms, discovers minimum node-depth layers, retains every
  relationship-distinct predecessor at those layers, and enumerates the predecessor DAG. Open maximum ranges use the
  documented depth cap of 15. Unsupported or ambiguous forms retain exact `SP-S0` with a machine-readable reason.
- Default-off `ASP-B1-DAG-ALT-NODE` and `ASP-B2-DAG-MIN-LEVEL` reuse compact
  two-sided search while retaining every same-minimum-depth predecessor. They
  enumerate at one canonical completed meeting cut and apply separate
  discovery, predecessor, saturating path-count, staged-output, and byte gates.
  Overflow clears candidate state and invokes exact `ASP-A1-DAG` before output.
  Production remains on A1 until independent training, frozen-holdout,
  resource, and reference-closure reports pass; the allowlisted canary seam
  uses the same explicit stable-snapshot requirement as SP.
- Expansion suffix pushdown and `ExpandInto` detection for fixed suffixes and shared-endpoint fanout patterns.
- Typed compound expansion-search planning for directed bounded expansions followed by fixed suffixes. The decision
  records its fixed-suffix expansion family, planned candidates, exact eligibility facts, observation mode, suffix
  bounds,
  selected/fallback strategy, selector version/mode, and stable fallback code separately from the legacy
  boolean suffix prefilter. Correlated suffix bindings and predicates spanning the expansion/suffix boundary have
  distinct conservative fallback codes. Candidate factored-forward and backward-viability SQL remains
  reference-only. `EXPANSION-SUFFIX-SEEDED-REVERSE` has a repository-native,
  qualification-only emitter. Explicit tool options select it and fail closed
  unless translation records the matching target as applied. Production deliberately
  retains the `EXPANSION-STEPWISE-FORWARD` translator and reports
  `tournament_unqualified` for otherwise eligible three-hop forms because no
  hard suffix-density or reverse-state bound is available before translation.
  Full-path reverse rows additionally carry ordered node IDs and hydrate node
  and edge composites with graph-partition-scoped aggregate lookups in the
  translated statement. Endpoint-only rows omit that state. Guarded candidate
  and incumbent branches still expose one identical path-composite column; the
  incumbent retains `ordered_edge_ids_to_path` as its exact fallback boundary.
- The default-off `orientation-probe-v1` guarded and shadow statements measure
  bounded duplicate-preserving roots, suffix rows/distinct boundaries, and
  typed first-hop work from both sides. Every relation has a cap+1 sentinel;
  reverse must beat forward by the versioned strict 3/4 hysteresis rule.
  Guarded execution also caps reverse state and marker-gates candidate and
  incumbent output chains independently. Probe and state overflow select the
  exact forward fallback and produce a truthful runtime receipt. Shadow
  execution always runs the incumbent, records only `would_select`, and emits
  its marker-first receipt even for an empty result. Plan telemetry attributes
  work from exact CTE materialization subplans so repeated consumer scans cannot
  inflate probe or branch loops. A versioned query-allowlisted
  driver canary can emit the guarded form only when it also binds a verified
  promotion-manifest SHA-256, while the zero policy and every non-allowlisted
  query remain forward.
- The independent `suffix-reverse-guard-v1` statement remains tool-only. It
  enrolls complete-path fixed-suffix queries, applies separate 512-row suffix
  and reverse-state caps, and marker-gates exact suffix-seeded reverse against
  exact stepwise forward in one Repeatable Read statement. Its
  chronology-valid training capture failed the immutable guard-overhead gate,
  so the generation is terminally stopped: it has no protected-holdout,
  manifest, driver-policy, automatic-selector, or rollback path. The exact
  reverse executor and ordered-ID hydration remain reusable by a newly
  preregistered architecture.
- Guarded endpoint-seeded expansion selection covers a separate
  `fixed_prefix_terminal_expansion` family: exactly one directed fixed prefix followed by one terminal, directed,
  single-kind variable expansion with minimum depth one and a local selective terminal predicate. Production emits
  `EXPANSION-ENDPOINT-SEEDED-REVERSE` with at most 32 terminal seeds and 4096 reverse states. Sentinel rows select an
  exact stepwise-forward fallback inside the same statement and snapshot before candidate rows are exposed. Both arms
  preserve ordered relationship IDs and enforce relationship uniqueness across the fixed prefix and expansion.
- Strict string property equality lowering through `jsonb_typeof(properties -> key) = 'string'` plus
  `properties ->> key = value`, preserving JSON scalar semantics while allowing existing text expression indexes on
  selective fields such as `objectid` and `name`.
- Typed relationship count plans that can use the `kind_id`-first covering edge index.
- Correlated relationship `EXISTS` lowering for typed pattern predicates when relationship types and endpoint
  correlations are sufficient.
- Membership-only `collect(entity)` ID-array lowering with `id = any(...)` membership predicates.
- Shortest-path strategy and terminal-filter planning for selective endpoint predicates and kind-only terminal filters.
- Analysis-only endpoint resolution metadata classifies ID equality, bounded
  nonunique property equality, literal or parameterized small sets, and
  correlated pairs with explicit 1/2/32/33 contracts. Property syntax is not a
  uniqueness proof. Analysis-only traversal predicate metadata distinguishes
  step-local and universal node/relationship forms from whole-path and
  unsupported forms. Neither diagnostic broadens execution until the compact
  candidates and that semantic class independently qualify.
- The fixed one-hop, bound-pair `ExpandInto` study exposes exact direct-pair,
  lower-degree adjacency, and statement-local pair-reuse reference arms. It
  covers outbound, inbound, directionless, wildcard/multi-kind, duplicate,
  missing, and self-loop behavior but does not select a production policy.
  Fixed-hop correctness does not depend on the study marker: dual-bound steps
  always retain an exact pair-join fallback, including endpoints carried across
  `WITH` or introduced by node-valued `UNWIND`. Directionless fixed hops use
  paired endpoint orientations so self-loops are emitted once for unbound,
  single-bound, and dual-bound forms.
- Exact anonymous directed fixed-range expansion lowering for non-shortest-path `*1..1` and `*2..2` patterns. These
  shapes use fixed traversal steps instead of recursive CTEs, preserve path projection semantics, and enforce
  relationship uniqueness across emitted fixed steps. The explicit SQL-size cap is depth 2; broader exact ranges
  continue through the recursive expansion path. Undirected exact ranges are not eligible for this lowering.
- Predicate-only `ANY`/`NONE` over current path `relationships(p)` bindings lowered to `EXISTS` or `NOT EXISTS` over
  path edge IDs, avoiding full `edgecomposite[]` materialization when the final projection does not require it.
- Dependency-safe clause reordering inside non-optional read regions, using existing selectivity heuristics while
  preserving stable tie order and pinning clauses with unresolved external dependencies.
- Field-sensitive continuation lowering carries node IDs as scalar columns between eligible fixed or recursive
  traversal steps. Property, full-entity, path, cross-pattern, and mutation consumers retain composite bindings;
  ID-only expansion endpoints still join the graph-scoped node partition so orphan filtering and multiplicity remain
  unchanged.

## Repeated-query compilation

Each PostgreSQL driver keeps bounded least-recently-used caches of 256 successfully parsed Cypher ASTs and 256 safe SQL
translations. Parse-cache keys are
the trimmed query text; invalid input is not retained, and queries larger than 64 KiB bypass the cache. Concurrent misses
for the same text are coalesced. Cached ASTs remain immutable: the optimizer copies an AST before applying rules, so
parallel executions cannot mutate shared parser output.

The cache deliberately retains complete trimmed query text, including literals, until LRU eviction or driver close.
That lifetime is bounded to 256 entries per driver; closing the driver clears all retained keys and AST references and
prevents in-flight misses from repopulating the cache. Queries whose source text exceeds 64 KiB bypass retention. Cache
diagnostics expose aggregate hit, miss, bypass, eviction, coalesced-miss, entry, and pending counts only—never query
text, literals, parameters, or credentials.

The translation cache is keyed by trimmed query text, graph ID, parameter names, the PostgreSQL data type negotiated
for each parameter, and the exact effective traversal-policy identity. Values are rebound on every hit. This deliberately separates empty untyped lists from typed lists
and separates different graph partitions. A translation containing generated/static fragment parameters is not cached,
because those values cannot be reconstructed safely from caller parameters. Concurrent cacheable misses are coalesced;
waiters rebuild uncacheable translations rather than inheriting the first caller's values. Driver close clears both
caches. `ParseCacheStats` and `TranslationCacheStats` expose aggregate, query-text-free counters.

`pg.TraversalPolicy` is default-off and admits one candidate family per
nonzero generation. It requires a nonempty allowlist built with
`pg.TraversalPolicyQuerySHA256`. Operations must first verify the complete
evidence closure with GraphBench and then install those exact manifest bytes
and their digest. The driver revalidates the manifest JSON structure, digest,
candidate, selector, execution boundary, immutable caps, training/holdout
buckets, exact query cohort, exact evidence-role set, and evidence-reference
digests before accepting
the policy. It does not open evidence paths or reproduce the role-specific
reports; acceptance by the driver is therefore not a substitute for the
GraphBench final verifier. Generation and policy contents partition the
translation cache. Setting the zero policy makes older candidate entries
immediately unreachable. B1/B2 candidates are not production-canary eligible.
`DisableEndpointSeededReverse` is an emergency rollback control and
intentionally requires no promotion artifact. Policy forcing never broadens a
lowering's structural correctness envelope.

If a manifest-backed candidate carries an emergency switch, it may carry
exactly one and it must be the switch dedicated to that candidate: orientation
with `DisableExpansionOrientation`, ASP-I1 with `DisableInlineASPDAG`, canonical
SP-I1 witness with `DisableInlineSPWitness`, or SP-I2 distance with
`DisableInlineSPDistance`. Any unrelated or second switch is rejected.
`DisableEndpointSeededReverse` is standalone-only. Every standalone rollback
policy must disable all manifest candidates and leave the manifest digest,
manifest JSON, and query allowlist empty (`promotion_manifest_sha256`,
`promotion_manifest_json`, and `query_sha256_allowlist`). A matching rollback
retains the installed manifest bytes and candidate anchor unchanged, but derives
an incumbent-only effective policy, clears the effective SQL-anchor comparison,
and uses its new generation as a distinct cache identity.

Final activation treats the manifest collections as exact sets: it requires
only the six defined evidence roles, one globally unique query digest, unique
bucket names and query entries, and exactly one canonical training/holdout
split declaration per bucket. Duplicate JSON keys, duplicate policy-allowlist
digests, absolute or escaping evidence paths, and extra roles fail closed. The
single operational SQL digest is checked against rendered candidate SQL before
execution; an unanchored GraphBench manifest is permitted only for the
non-promotional preflight that discovers that digest.

GraphBench final verification also requires each promotion reference case to
match exactly one native PostgreSQL A/A workload by dataset, name, and workload
digest. For every promotion case, the resource report must cover the exact
performance round count and its flattened candidate receipt-chain set must equal
performance's complete set. Reference receipts remain an independently captured
raw-pgx/comparator stream rather than sharing invocation IDs with that set.
Confirmation and performance do not embed raw samples, so their typed decisions
can be recomputed but their bootstrap draws cannot yet be independently replayed.
The operational validator consumes and recomputes an assembled 32-record native
input; the repository does not yet provide a standalone producer for that input.

The same policy boundary now admits `ASP-I1-U-DAG+MAT-M0` as a default-off,
exact-query canary under Repeatable Read or Serializable isolation. Its
manifest must authorize the query SHA and exact direction/observation/depth/
relationship-kind bucket, declare positive immutable state, predecessor,
enumeration, and output-byte caps, name `ASP-A1-DAG` as fallback, and use the
`guarded_dual_arm` boundary. Exact one- and two-hop targets bypass recursive
discovery. The inline statement materializes cap+1 preflight, distance,
predecessor, and enumeration relations before opening either output arm. A
version-2 runtime receipt retains the complete ordered event chain and
identifies `inline_predecessor_dag`, `inline_no_path`, or `exact_a1_fallback`;
the unselected arm emits no rows. Read Committed and
queries outside the exact allowlist retain A1. `DisableInlineASPDAG` is the
evidence-free emergency rollback control.

The default-off fixed-suffix orientation runtime seam recognizes v1 and v2
selector identities. Either shape must name `EXPANSION-STEPWISE-FORWARD` as
fallback, use `guarded_dual_arm`, and bind the immutable
`root_row_limit=512`, `reverse_seed_row_limit=512`,
`directional_degree_row_limit=16384`, and `state_limit=4096` caps. This is only
structural runtime admission: the final schema-v2 verifier rejects the legacy
`orientation-probe-v1` evidence schema because it cannot bind source, corpus,
and cohort identity. It separately terminally rejects the frozen
`orientation-probe-v2` generation because its immutable training overhead gate
failed. Neither generation is release-authorized; v2 must not be recaptured or
advanced to its unopened holdouts. The guarded statement exposes the production
boundary in traversal telemetry; shadow and forced single-arm statements report
`inline_statement` and cannot stand in for production-boundary evidence.

Runtime receipt workspaces must exist on the exact PostgreSQL session before
an explicit read-only transaction begins. GraphBench satisfies this by pinning
and preparing one session. Driver callers that intentionally arm receipts from
inside a graph transaction can pass
`pg.OptionInitializeTraversalRuntimeAttestation()`; the driver then prepares
the acquired session immediately before `BEGIN READ ONLY`.

The driver automatically prepares the production S4 and A1 session-local
workspaces before every explicit Repeatable Read or Serializable read-only
graph transaction. The underlying PostgreSQL transaction uses `READ WRITE`
access because workspace reset mutates session-local temporary tables; graph
data remains non-mutating. This keeps incumbent execution and a guarded
candidate's exact fallback valid on a fresh pooled connection.

`SP-I1-C-WE+MAT-M0` uses the same guarded production boundary for singleton
one-path observations, with `SP-S4-C-WE+MAT-M0` as its declared fallback. The
manifest must authorize an exact `one_path` bucket and the same four positive
caps. It is admitted only at Repeatable Read or Serializable isolation;
`DisableInlineSPWitness` immediately restores the statically selected S3/S4
incumbent and changes the cache identity without requiring evidence.

The shortest-path functions use session-local `ON COMMIT PRESERVE ROWS`
workspace-v2 tables with invocation versions. Calls reset seen, candidate, and
predecessor state once, then derive each frontier from depth-tagged seen rows;
they do not create, drop, swap, or truncate frontier tables at every level. The functions set a
local `recursive_worktable_factor`, declare explicit `COST`/`ROWS` estimates, and carry graph/node/edge IDs until one
outer hydration boundary. Temporary-workspace buffers are expected for S4/ASP; executor temp-file spill and WAL remain
resource-gate failures.

Raw PostgreSQL graph-composite values are driver implementation details. Use the result value mapper or
`graph.ScanNextResult` for nodes, relationships, paths, and their arrays instead of depending on pgx's historical
`map[string]any` composite representation.

`Result.Keys()` returns metadata cached once for the result set; callers must treat that slice and its strings as
immutable for the result lifetime. `Result.Values()` remains row-scoped raw driver data. Public graph values produced
through the mapper are owned independently of later row advancement and pooled connection reuse.

## Indexing Notes

Exact string property equality is emitted with a JSON string type guard and `properties ->>` extraction. This allows
indexes created on expressions such as `properties ->> 'objectid'` and `properties ->> 'name'` to accelerate selective
anchors without matching JSON booleans or numbers.

Simple relationship count fast paths depend on the schema's `kind_id`-first edge index for efficient typed counts.

Substring and suffix predicates are not promoted to blanket schema indexes. PostgreSQL deployments can request explicit
`TextSearchIndex`/trigram property indexes for fields that need `CONTAINS`, `STARTS WITH`, or `ENDS WITH`. Dynamic
parameter/property forms that lower to helper functions remain outside the hard index-match contract until their
lowering changes.

## Validation Workflow

Optimizer changes should include focused optimizer/lowering tests, SQL-shape translation tests, and backend-equivalent
integration coverage when behavior affects query semantics. `make test_all` is the default full validation target when
`CONNECTION_STRING` is available.

Run plan-corpus capture for planner, lowering, or SQL-emission changes:

```bash
make plan_corpus
```

The corpus summary should be checked for PostgreSQL cost, `Recursive Union`, `SubPlan`, `Function Scan on unnest`, and
skipped-lowering deltas.

PostgreSQL property index regression coverage is hard-failing under the `manual_integration` tag. The synthetic plan
test translates Cypher to PostgreSQL, disables sequential scans for the `EXPLAIN`, and requires explicit node property
indexes to appear in the JSON plan:

```bash
CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./integration -run TestPostgreSQLPropertyIndexPlans
```

PostgreSQL-only plan-corpus validation should confirm that `ExactRangeExpansion` and `PathRelationshipPredicate` are
planned and applied for their supported cases without skipped entries for either lowering.

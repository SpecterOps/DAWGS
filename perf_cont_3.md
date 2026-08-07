# CySQL Performance Continuation Plan 3

## Purpose

This document follows `perf_cont_2.md` from the clean live PostgreSQL versus
Neo4j capture completed on 2026-08-06. It turns the newly isolated large-ADCS
hotspot into a bounded implementation, qualification, and rollout sequence.

The immediate objective is to reduce the PostgreSQL burden for this shape:

```cypher
MATCH (n:Group)
WHERE n.objectid = $objectid
MATCH p = (n)-[:MemberOf*0..16]->()
             -[:Enroll]->(ca:EnterpriseCA)
             -[:TrustedForNTAuth]->(:NTAuthStore)
             -[:NTAuthStoreFor]->(d:Domain)
RETURN p
```

The endpoint-only variant returns `id(ca), id(d)` instead of `p`.

The live evidence shows that PostgreSQL expands the complete forward
`MemberOf` trail space before applying a highly selective fixed suffix. It
then performs root lookup, expansion-end lookup, and `Enroll` lookup once per
recursive row. Neo4j chooses the opposite physical order: fixed suffix first,
then reverse `MemberOf` expansion, then the root predicate.

This plan therefore optimizes in this order:

1. keep recursive and suffix state scalar and hydrate only surviving rows;
2. factor the fixed suffix into one exact, multiplicity-preserving relation;
3. compare exact forward, reverse, and backward-viability-assisted search at
   identical result boundaries;
4. ship the selected suffix-driven strategy only inside a proven bounded
   eligibility and fallback envelope;
5. change frontier mechanics only if a material residual remains after search
   direction and cardinality are fixed.

This plan narrowly replaces the ADCS deferral and evidence assumptions in
Phase C6 of `perf_cont_2.md`. It does not replace that document's singleton
shortest-path, generic traversal, decoding, caching, statistical, artifact,
concurrency, rollback, or soak requirements. The correctness, graph-scoping,
backend-equivalence, mutation/template coverage, and operational safeguards in
`perf_rework_plan.md`, `perf_cont_1.md`, and `perf_cont_2.md` remain in force
unless this document makes a narrower rule stricter.

Neo4j remains an exact-result and implementation-shape oracle. Its latency is
reported because it motivated this investigation, but it is not a CySQL
acceptance gate. Production decisions compare CySQL with its immediate
PostgreSQL predecessor and the best correct PostgreSQL reference.

## State entering this continuation

### Authoritative live capture

The historical evidence bundle for this continuation is:

```text
.coverage/live-cross-current-20260806/
```

It records source commit
`7bb291c57fd9a4621360bde7223a99e826b4cc6c`, dirty-tree fingerprint
`9cea3efb986de9b8ee367baf840e95b7d820e13c402cc818f3899e1f46db14b2`, and
GraphBench binary fingerprint
`147c9235368269c62fd03bf14a2afdef31952e0c92ac5b67713ce25596f8bacf`.

| Artifact | SHA-256 |
|---|---|
| `REPORT.md` | `aff81ff38eb46a902d44fb6f251aa454a0cf8cfd7f4ac60e941549bb44aeee2c` |
| `round-1.jsonl` | `23e432dbf11bd9003fe2395f93de85832c18776fdeec025fd33de962264f22cc` |
| `round-2.jsonl` | `9ef42c6de5327a096a049bb962c58fa454d22bbd57a778903788482ba28eb019` |
| `round-3.jsonl` | `8b1a477baf638ec83bb00cdd400a4d57ce572ab467745f7baecc958b2ba5aeae` |
| `round-4.jsonl` | `b4f4d2f1ad7bd32b906370764bc526c34e2c238e3cab7ac5dd137c3849e99250` |
| `round-5.jsonl` | `0b30220df5774480b4089961056981f7f1e345e6becb79575f0c3b2ffc60bc5f` |

The capture used:

- five independently reloaded rounds;
- alternating backend order;
- ten untimed warmups and thirty measured warm observations per case,
  backend, and round;
- pool size one;
- exact result validation for both backends;
- PostgreSQL physical row-count validation before timing;
- `VACUUM (ANALYZE)` after fixture loading;
- 60 records, zero errors, and all 30 PostgreSQL records physically
  validated.

The complete live integration suites passed for PostgreSQL and Neo4j. The
PostgreSQL suite used the IPv4 loopback equivalent of the supplied URI because
`localhost` resolved to an unavailable IPv6 listener in the test environment.

`.coverage` is staging rather than durable publication. Phase R0 below must
copy the accepted baseline, source patch, binary, raw plans, and manifests into
a reviewed reconstructible artifact bundle before a production change is
accepted.

### Current cross-backend result

| Observation | Endpoint IDs | Full path |
|---|---:|---:|
| PostgreSQL median | 55.734 ms | 65.631 ms |
| PostgreSQL p95 | 58.030 ms | 68.941 ms |
| Neo4j median, diagnostic only | 1.086 ms | 1.173 ms |
| Neo4j median advantage | 52.29x | 55.96x |
| Neo4j p95 advantage | 35.14x | 34.96x |
| PostgreSQL median `EXPLAIN` planning | 3.290 ms | 3.150 ms |
| PostgreSQL median `EXPLAIN` execution | 59.479 ms | 69.032 ms |
| PostgreSQL shared hits | 126,215 | 158,403 |
| PostgreSQL shared reads | 0 | 0 |
| PostgreSQL temp reads/writes | 0 / 0 | 0 / 0 |
| Result rows | 2 | 2 |

The five-round planning/execution ranges are 3.072-5.098 ms and
53.569-59.941 ms for endpoint IDs, and 2.934-3.162 ms and 65.103-70.030 ms
for the full path.

The D16/F1000 fixture contains 16,006 nodes and 16,008 relationships. Its
active PostgreSQL child partitions occupy 2,326,528 node bytes and 4,759,552
edge bytes.

### PostgreSQL plan attribution

The forward recursive CTE emits exactly 16,001 states:

```text
depth 0 root                         1
depth 1 first-hop states         1,000
depths 2 through 16             15,000
total                            16,001
```

PostgreSQL estimates 12 recursive rows rather than 16,001, a 1,333x
underestimate. The seed edge access estimates one row but returns 1,000. The
recursive worktable estimates approximately one row while processing about
938 rows per generation across 16 generations.

The hot work is stable across all five rounds:

| Work | Endpoint shared hits | Path shared hits | Observation |
|---|---:|---:|---|
| `MemberOf` recursive edge probes | 30,001 | 30,001 | 15,000 recursive covering-index probes |
| invariant root lookup | 32,003 | 48,003 | repeated for all 16,001 states |
| expansion-end lookup/hydration | 32,003 | 48,003 | repeated for all 16,001 states |
| `Enroll` lookup | 32,003 | 32,003 | repeated for all 16,001 states |
| all other plan work | 205 | 393 | fixed suffix tail and output |
| total | 126,215 | 158,403 | all cached shared hits |

The four cardinality-proportional operations account for 99.84% of endpoint
hits and 99.75% of full-path hits. The recursive CTE reports 30,175 inclusive
hits because its seed/root work adds 174 hits already represented elsewhere in
the plan; 30,001 is the non-overlapping recursive edge-probe bucket used in the
table. The `Enroll` lookup produces only three candidates; two survive the
complete suffix and output semantics. Thus 15,998 of 16,001 `Enroll` probes
fail.

The current full-path plan's recursive union costs approximately 23 ms and
30,175 hits. Premature root and expansion-end hydration adds 96,006 hits, and
the per-state `Enroll` lookup adds 32,003 hits. Endpoint-only output uses the
same 16,001-state search and remains approximately 56 ms, proving that ordinary
path materialization is not the primary cause. Full-path observation adds
approximately 32,000 hits and 9.5-15 ms, so late hydration is material but
cannot close the search gap by itself.

There is no executor spill, local-buffer workspace, or shared read I/O in the
ADCS plans. `work_mem` is already 512 MiB in the diagnostic environment. The
primary cost is cached executor work and repeated B-tree probes, not storage
latency or insufficient memory.

### Search-order evidence

The PostgreSQL lowering records `ExpansionSuffixPushdown` as planned but not
applied for both large ADCS cases. Its decision says:

```text
immediate observed continuation produces suffix rows
```

That decision correctly avoids using a correlated boolean `EXISTS` as a
cardinality-losing replacement for real suffix rows. It does not create a
consumed result-producing suffix relation, and it does not allow the search to
start at the suffix.

The translated PostgreSQL shape is:

```text
root predicate
  -> forward MemberOf*0..16: 16,001 states
  -> root lookup: 16,001 loops
  -> expansion-end lookup: 16,001 loops
  -> Enroll lookup: 16,001 loops
  -> fixed suffix tail
  -> two results
```

The captured Neo4j plan is:

```text
NTAuthStoreFor relationship-type scan
  -> TrustedForNTAuth backward
  -> Enroll backward
  -> MemberOf*0..16 backward
  -> Group/objectid root filter
  -> two results
```

The deterministic fixture contains approximately three exact suffix boundary
sources: the root, one reachable branch terminal, and one disconnected source.
An exact suffix-first reverse traversal is therefore expected to emit three
depth-zero seeds plus sixteen states along the one productive chain, or about
19 reverse states. This is an operation-count hypothesis, not yet a PostgreSQL
benchmark result. If measured, it would be an approximately 842x state-count
reduction from the current 16,001 states.

PostgreSQL already has the required graph-partitioned covering indexes:

```text
(start_id, kind_id) INCLUDE (id, end_id)
(end_id, kind_id)   INCLUDE (id, start_id)
(kind_id)           INCLUDE (id, start_id, end_id)
```

The initial comparator and production work therefore requires no schema or
index migration.

### Evidence gaps that block implementation selection

The current evidence diagnoses the incumbent but does not yet qualify a
replacement:

- generated ADCS cases currently register no PostgreSQL reference arms;
- `referenceSpecs` recognizes only the legacy `adcs_p1_*` names, not the
  `generated_adcs` category;
- the current ADCS reference hard-codes `max_depth = 15`, so it cannot exactly
  represent D16;
- the hand-written ADCS reference repeats the same forward-first architecture
  and is not a performance floor;
- one `EXPLAIN ANALYZE` per round attributes work but is not a sampled
  server-time distribution;
- some generated endpoint cases declare only row count rather than the exact
  duplicate ID multiset;
- the generator couples reachable suffix density, root zero-depth validity,
  disconnected suffix candidates, suffix multiplicity, and output
  cardinality;
- `ValidSuffixEvery` cannot express zero reachable branch suffixes because
  branch zero always satisfies the modulus rule;
- the current artifact does not record boundary candidates, forward/reverse
  state counts, examined edges, hydration row counts, or retained state bytes
  as first-class metrics.

Phase R0 repairs these gaps before any production lowering is selected.

## Decisions fixed by the evidence

The following decisions are predeclared for this continuation:

1. Treat 55.96x as a search-order and stage-boundary defect, not a generic
   PostgreSQL recursive-CTE ceiling.
2. Preserve the current stepwise forward translator as the semantic fallback
   until every replacement gate passes.
3. Keep hydration, suffix production, search direction, adaptive selection,
   and frontier mechanics in separately measured and independently reversible
   increments.
4. Do not use a global visited-node BFS, shortest-path harness, or deduplicated
   reachability relation to emit ADCS results. This query returns all
   relationship-unique trails, including duplicate endpoint pairs.
5. Do not justify an ADCS rewrite from the existing slow hand-written
   reference. Build exact competitive forward and reverse references first.
6. Do not begin with `work_mem`, JIT, parallelism, pool, parser/template cache,
   or client codec changes. The current plans neither spill nor read from
   storage, and planning is about 4-5% of plan-plus-execution time.
7. Do not add a new edge index for the initial experiment. Forward, reverse,
   and kind-first covering indexes already exist.
8. Do not add a transitive-closure table or adjacency cache in this
   continuation. Their write amplification, invalidation, and path-multiplicity
   costs require a separate workload-specific ADR.
9. Report the PostgreSQL/Neo4j ratio after every accepted increment, but use
   the matched PostgreSQL predecessor and best correct PostgreSQL reference for
   acceptance.
10. An optimization that wins only on sparse suffixes must have an explicit
    dense/overflow fallback. An always-reverse heuristic is not acceptable.
11. A structural optimization may be retained inside a later compound arm
    even if it does not independently clear the latency gate, but it may not be
    claimed as an independently shipped performance win.
12. Any semantic, graph-scope, cancellation, memory-ceiling, or session-reuse
    failure rejects the candidate regardless of latency.

## Correctness model

### This is all-trail enumeration, not shortest path

For an eligible directed pattern, the logical result is a bag join:

```text
R(root source rows)
  JOIN T(root_id, boundary_id, ordered_member_edge_ids)
  JOIN S(boundary_id, fixed suffix bindings, ordered_suffix_edge_ids)
```

`R`, `T`, and `S` are bags, not sets. Two different `MemberOf` trails that
reach the same boundary and fixed suffix produce two result rows. Two physical
fixed suffix trails with the same boundary, CA, and Domain also produce two
result rows. Endpoint-only output does not make those duplicates disposable.

Every directed relationship trail from a root to a boundary has a one-to-one
reverse trail from that boundary to the root. Reverse physical execution is
therefore valid only when it restores original path order and preserves every
trail and suffix row.

### Invariants every arm must preserve

- one row per relationship-unique complete trail;
- root-source duplicate multiplicity;
- fixed-suffix path multiplicity;
- Cartesian multiplication of root rows, variable trails, and suffix rows;
- relationship uniqueness within the variable expansion;
- pairwise relationship uniqueness within the fixed suffix;
- relationship uniqueness across the variable and fixed segments;
- repeated nodes, node cycles, and self-loops where relationship uniqueness
  permits them;
- same-endpoint relationship-distinct trails permitted by the storage model,
  including distinct allowed kinds;
- minimum and maximum expansion depth;
- zero-depth behavior for `*0..N`;
- original outbound or inbound logical direction;
- exact ordered node and relationship identity for path output;
- endpoint ID, kind, property, existence, null, and contradiction semantics;
- graph scope, including colliding node and edge IDs in different graphs;
- aliases, `WITH`, aggregation, path functions, and downstream bindings;
- optional-match, mutation, directionless, correlated, and unsupported forms
  through an explicit conservative fallback;
- one PostgreSQL statement snapshot and transaction semantics;
- cancellation, rollback, error, and physical-session reuse safety.

The PostgreSQL edge schema intentionally has no endpoint foreign keys. Moving
node hydration later must preserve the current behavior that dangling
relationship endpoints do not become matched nodes. Unless the supported write
path is first proven to guarantee endpoint existence, every node implied by a
final candidate trail must be validated set-wise before output; checking only
the root and final boundary is insufficient. PostgreSQL-scoped cases must
separate missing root, missing intermediate expansion node, missing boundary,
and missing fixed-suffix node behavior. Public Cypher semantics remain
backend-equivalent.

### Permitted deduplication

Search may deduplicate only relations that are not used to produce result
multiplicity:

- root IDs before root-independent search, followed by a join back to the
  original root-source bag;
- boundary IDs before boundary-independent reverse search, followed by a join
  back to the exact suffix bag;
- backward viability `(node_id, reverse_distance)` states used only as a
  permissive pruning filter.

It must never deduplicate exact variable trails or exact suffix rows.

## Target relational architecture

### Scalar root and expansion state

The ordinary forward candidate state should be no wider than:

```text
(root_id, boundary_id, depth, member_edge_ids)
```

Relationship IDs remain required even in endpoint mode because they enforce
relationship-trail uniqueness and preserve duplicate rows from distinct
trails. Node and relationship composites do not belong in recursive state.

When the root was already validated and materialized by a preceding frame:

- reuse that root composite if a later observation needs it;
- otherwise carry only its ID;
- never look the same root up once per recursive row merely to prove it still
  exists.

Delay boundary-node existence and constraints until a row has qualified
against the suffix, unless the exact suffix relation validates the boundary
node itself. Endpoint-ID mode projects suffix IDs and performs no path
hydration. Full-path mode joins or reuses the root only for final rows, appends
ordered member and suffix edge IDs, and invokes the selected linear
materializer once per result.

The current unconditional root and expansion-end lookups are emitted at the
expansion projection boundary in `cypher/models/pgsql/translate/expansion.go`.
Root reuse and late boundary hydration are useful generic improvements, but
they must remain independently attributable from the compound suffix rewrite.

Every factored, viability, reverse, or adaptive form begins with a one-time
`root_presence` gate. If the source bag contains no valid root, suffix
production and recursion must have zero actual loops; a missing-root query may
not turn into graph-wide suffix work. The exact source bag is restored only
after root-independent work when a valid root exists.

### Exact factored suffix bag

Build the immediate fixed continuation once as a bag relation:

```text
suffix_rows(
    suffix_key,
    boundary_id,
    ordered_suffix_edge_ids,
    required_fixed_node_ids,
    required_fixed_relationship_ids
)
```

For the current P1 pattern this is:

```text
boundary -[Enroll]-> EnterpriseCA
         -[TrustedForNTAuth]-> NTAuthStore
         -[NTAuthStoreFor]-> Domain
```

The suffix key may be the ordered physical suffix edge-ID tuple. An internal
ordinal is acceptable only if it is assigned without collapsing duplicates
and its cost is measured.

Requirements:

- one row per physical suffix trail;
- no `DISTINCT` on `suffix_rows`;
- suffix edge IDs retained internally even for endpoint output;
- suffix-local edge and node predicates applied while building the relation;
- pairwise suffix relationship inequality enforced;
- boundary-node existence validated where required by current semantics;
- only IDs retained after the last predicate that needs kinds or properties;
- non-local and path-dependent predicates deferred to the exact candidate
  join;
- graph-scoped access to every node and edge relation;
- exact suffix bindings restored from this relation rather than retraversed.

A separate `boundary_ids` set may select distinct `boundary_id` values as a
search seed. Search results must join back to `suffix_rows` to restore every
suffix trail and output binding.

Compare explicit `AS MATERIALIZED` with an inline relation. Materialization can
prevent PostgreSQL from re-correlating suffix work into one lookup per frontier
row, but it can add fixed work or spill for dense suffixes. Record rows, bytes,
temporary I/O, and concurrency behavior under supported deployment memory,
not only the 512 MiB diagnostic setting.

The existing `ExpansionSuffixPushdownDecision` represents a supplemental
correlated predicate. It is not a relation-producing lowering. Preserve its
legacy meaning for compatibility and add a separate compound-region search
decision.

### A1a/A1b: Root reuse and forward search with late hydration

A1a changes only root staging: it reuses the already validated root binding,
preserves the source bag, and removes invariant root lookups from the recursive
row path. A1b includes A1a and preserves current exact forward enumeration
while:

- carrying a scalar expansion state;
- testing the suffix before boundary hydration;
- hydrating node and path values only for suffix-qualified rows.

The separate A1a and A1b controls make root reuse and late hydration
independently attributable and reversible. A1b removes measured repeated node
work while retaining the 16,001-state forward search and per-state suffix
probe. Later arms include A1b unless explicitly stated otherwise.

### A2: Factored suffix plus exact forward search

A2 evaluates `suffix_rows` once and joins it to exact forward trails:

```text
forward_member_trails
  JOIN suffix_rows
    ON suffix_rows.boundary_id = forward_member_trails.boundary_id
```

Apply prefix/suffix relationship disjointness at this join. A2 removes the
16,001 correlated `Enroll` lookups but still generates all 16,001 forward
states. Its expected structural floor is therefore the approximately
30,175-hit recursive component plus fixed suffix and final output work.

A2 is both a production fallback candidate and a control that separates
suffix evaluation from search direction.

### A3: Exact suffix-seeded reverse all-trail search

A3 seeds from distinct suffix boundary IDs and walks incoming expansion
relationships:

```text
reverse_trails(suffix_seed, current_id, depth, member_edge_ids)
```

Conceptually:

```sql
SELECT boundary_id, boundary_id, 0, ARRAY[]::int8[]
FROM boundary_ids

UNION ALL

SELECT
    reverse_trails.boundary_id,
    edge.start_id,
    reverse_trails.depth + 1,
    edge.id || reverse_trails.member_edge_ids
FROM reverse_trails
JOIN edge
  ON edge.end_id = reverse_trails.current_id
WHERE reverse_trails.depth < max_depth
  AND edge satisfies expansion-local predicates
  AND edge.id <> ALL(reverse_trails.member_edge_ids)
```

The production AST must use the correct logical predecessor/end columns for
the original direction rather than assuming outbound patterns universally.

Important rules:

- use `UNION ALL`; exact trails must not be deduplicated;
- prepend each reverse edge ID so the array remains in original
  root-to-boundary order;
- retain depth-zero seeds and apply the original minimum depth when accepting
  roots;
- do not stop recursion merely because a valid root is reached; a longer
  relationship-unique trail may pass through a valid root before ending at a
  valid root;
- apply root ID/kind/property/existence predicates to candidate reverse states
  or join distinct valid roots, then restore the original root-source bag;
- reject any member relationship that occurs in the suffix relationship tuple;
- join matching search trails back to the exact suffix bag;
- construct the observed path from
  `member_edge_ids || ordered_suffix_edge_ids`;
- hydrate only after every root, depth, suffix, and uniqueness constraint has
  passed.

No base schema migration is required. Plans must prove use of the active graph
partition's `(end_id, kind_id)` covering index for reverse `MemberOf` access.

### A4: Backward viability plus exact forward enumeration

A4 builds a permissive depth-aware relation:

```text
viable(node_id, reverse_distance)
```

from distinct suffix boundaries, then permits a forward state only when a
viability row proves that some suffix can be reached inside the remaining
depth budget.

`viable` may use `UNION` on `(node_id, reverse_distance)` because it is only a
filter. It may ignore relationship uniqueness and non-local predicates when
that creates false positives but never false negatives. It must not emit
results or determine multiplicity.

The final forward CTE remains an exact `UNION ALL` relationship-trail
enumerator. It applies minimum depth, prefix uniqueness, cross-segment
uniqueness, root-source multiplicity, and suffix multiplicity normally.

On the D16/F1000 sparse fixture, A4 should still inspect the root's 1,000
first-hop relationships but can prevent traversal down the 999 irrelevant
chains. It is a useful middle regime when reverse exact enumeration has too
many boundary seeds or reverse fan-in.

### S1: Bounded adaptive hybrid

A suffix-source cap alone does not protect against one boundary with enormous
reverse fan-in. Broad reverse enablement requires bounded work and a complete
fallback.

Compare these portable SQL designs before considering a helper:

1. a bounded suffix probe returning at most `suffix_limit + 1` rows;
2. a demand-limited reverse CTE consumed through
   `LIMIT state_limit + 1`;
3. mutually exclusive reverse and late-hydrated forward result branches;
4. a backward-viability branch for measured intermediate density.

If the suffix probe is complete and below its limit, it may supply the exact
suffix bag. If it overflows, discard its truncated rows and execute the exact
forward fallback. If reverse state overflows, discard every partial reverse
result and restart the exact fallback in the same statement and snapshot.

This design is acceptable only if `EXPLAIN ANALYZE` and adversarial tests prove:

- recursive production actually stops at the cap rather than computing the
  full relation behind an outer `LIMIT`;
- only one result-producing branch executes;
- no truncated suffix or reverse result can escape;
- fallback preserves exact multiplicity and order semantics;
- probe overhead is bounded on dense and missing-root cases;
- cancellation interrupts probes and both branches;
- no state survives rollback or session reuse.

If portable SQL cannot provide reliable bounded restart behavior, evaluate a
typed PL/pgSQL helper in Phase R6. Do not ship an unbounded always-reverse
heuristic.

### Observation modes

The compound lowering must distinguish at least:

```text
endpoint_ids
ordered_path_ids
full_path
```

Endpoint mode still carries relationship IDs for trail uniqueness but hydrates
no path. Ordered-ID mode is the common search/reference boundary. Full-path
mode uses the selected M0/M1-style linear materializer only after exact result
selection.

If a downstream expression observes node/relationship properties, a path
function, or the path composite itself, field-requirement tracking must retain
or hydrate the minimum required values at the last responsible stage. Unknown
or unsupported observations fall back rather than receiving a partially
hydrated value.

### Frontier mechanics are conditional

The current recursive edge lookup uses a correlated lateral subquery with
`OFFSET 0` to prevent PostgreSQL from flattening it into a merge over the full
edge index. It performs 15,000 point probes in the current forward plan.

Only after A1a/A1b and A2-A4 establish the winning search topology, and S1
proves its safety contract, should Phase R6 compare:

- the current fenced indexed lookup for small frontiers;
- an unfenced set-oriented worktable-to-edge join;
- level-synchronous frontier batching;
- parent-linked trace state instead of repeated array copying;
- a typed helper with bounded state and exact fallback.

Removing `OFFSET 0` is not inherently an improvement. A flattened plan can
scan a large relationship-kind range once per generation. Reverse search is
expected to make the target frontier tiny, in which case frontier work may
close as a measured no-op.

## Strategy decision and fallback contract

### New typed decision

Do not overload the boolean `ApplySupplemental` field on
`ExpansionSuffixPushdownDecision`. Add a distinct typed decision, such as:

```text
ExpansionSearchStrategyDecision
```

with at least:

```text
target
selected_strategy
structurally_eligible
eligibility_facts
suffix_start_step
suffix_end_step
suffix_length
observation_mode
logical_direction
minimum_depth
maximum_depth
selection_mode
suffix_probe_limit
reverse_state_limit
fallback_strategy
fallback_reason
```

Initial strategy identifiers are:

```text
stepwise_forward
late_hydrated_forward
factored_suffix_forward
suffix_seeded_reverse
backward_viability_forward
bounded_reverse_forward
```

Translation diagnostics must distinguish planned, applied, and skipped
outcomes. GraphBench must additionally attribute the branch that actually ran
and any fallback from JSON-plan `Actual Loops` plus benchmark-only diagnostic
counters; compile-time diagnostics alone must not be labeled as runtime facts.
Production query results gain no side-effecting counters. SQL and strategy
fingerprints must remain stable across parameter values inside a declared
template class.

### Initial structural eligibility

The first production compound lowering requires all of the following:

- an ordinary non-optional, read-only inner `MATCH`;
- one directed variable expansion;
- a finite supported maximum depth, initially no greater than the envelope
  qualified in R2 and never silently above 64;
- exactly the qualified three-hop, directed, fixed suffix used by the initial
  ADCS production class; suffix lengths one, two, four, and beyond remain on
  the incumbent until a separate length/topology sweep clears the same gates;
- no second variable expansion inside the consumed suffix region;
- expansion-local relationship kinds and predicates that can be applied in
  the physical direction selected;
- suffix-local predicates that can be evaluated while building
  `suffix_rows`;
- no unresolved cross-region or outer-row predicate requiring composite state
  during recursion;
- no path-dependent predicate that changes which partial trails are valid;
- no optional or mutation-returning dependency;
- no unsafe interaction with limit pushdown or another path call;
- a root-source bag whose duplicates can be restored, or a proven singleton
  root source;
- a supported endpoint-ID, ordered-ID, or full-path observation;
- graph-scoped node and edge access throughout.

Directionless, mixed-direction, unbounded, optional, correlated, multiple
expansion, unsupported observation, and mutation shapes retain the existing
stepwise forward translation until independently qualified.

### Stable fallback codes

Use stable codes, including at least:

```text
no_fixed_suffix
suffix_too_short
optional_match
shortest_path
all_shortest_paths
directionless_expansion
directionless_suffix
unbounded_depth
unsupported_depth
multiple_variable_expansions
correlated_suffix
cross_region_predicate
path_dependent_predicate
relationship_variable
relationship_predicate
multiple_path_calls
limit_pushdown_conflict
unsupported_observation
mutation
tournament_unqualified
runtime_suffix_density
runtime_candidate_limit
runtime_state_limit
```

Runtime overflow is a control-flow result, not an empty result or transaction
error. It must select a complete exact fallback. A candidate without a safe
same-snapshot restart remains statically restricted rather than returning
partial data.

### Density selection inputs

The client-side optimizer has no live graph-cardinality catalog. It must not
infer suffix density from relationship names, labels, or suffix length alone.

Compare two selection regimes:

1. a conservative static envelope whose worst-case work is bounded by
   structural constraints independent of current data, with its performance
   hypothesis learned from R2 and confirmed on holdouts;
2. a bounded query-local probe using inputs available without performing the
   recursive search.

A bounded selector may inspect only capped values such as:

- matching root rows up to `root_limit + 1`;
- root first-hop degree up to `fanout_limit + 1`;
- exact suffix rows or distinct boundaries up to `suffix_limit + 1`;
- declared minimum/maximum depth;
- observation mode and logical direction.

Candidate-source count alone does not bound reverse fan-in. Broad reverse
selection therefore also needs a proven static depth/fan-in envelope or the
tested reverse state cap described above.

Equivalent analyzed fixtures must make the same decision. The planned
strategy and fallback contract must be visible without adding side effects to
the timed query; actual branch/fallback attribution follows the separate
GraphBench plan/diagnostic mechanism below.

## Sequenced delivery plan

| Phase | Outcome | Depends on | Ship decision |
|---|---|---|---|
| R0 | Freeze evidence and repair generated-ADCS references | Current clean artifacts | No production change |
| R1 | Build orthogonal semantic, density, and resource corpus | R0 reference schema | No production change |
| R2 | Run the A0-E2E/A0-SQL, A1a/A1b, and A2-A4/S1 benchmark-only tournament; A5 only if triggered | R0/R1 | Selects architecture and envelope |
| R3 | Ship root reuse, then late hydration | Proven A1a/A1b | Two independent increments if material |
| R4 | Qualify and conditionally ship the exact factored suffix relation | Proven A2 and R3 boundary | Ships only when its full structural envelope is safe |
| R5 | Implement and qualify the selected reverse/viability lowering behind the incumbent | Proven A3/A4 and R4 | No density-dependent production activation |
| R6 | Prove bounded selection/overflow fallback, enable the qualified branch in the candidate build, and conditionally tune frontiers | R5 evidence | Blocks candidate-build activation; frontier work optional |
| R7 | Full semantic, integration, concurrency, cancellation, and soak qualification | Accepted R3-R6 increments | Blocks workstream completion |
| R8 | Rerun live cross-backend corpus and reprioritize residual | R7 | Next plan or stop |

R0 and R1 may proceed in parallel after the reference-result schema is fixed.
A1a/A1b and A2-A4 may be prototyped in parallel as benchmark-only SQL, but no
candidate dispatcher branch is added before R2 selects an architecture. R3
and R4 stay separate even if the final accepted binary contains both, so their
effects and rollback boundaries remain attributable.

“Ship” in R3/R4 means accept the increment into the release-candidate build,
not deploy it. R7 is the release gate for every accumulated change; no user or
production rollout starts before R7 passes.

## Phase R0: Freeze evidence and repair references

### Durable entering baseline

Preserve the five clean live rounds in a reconstructible bundle containing:

- source commit and tracked-source patch;
- manifest and checksums for untracked source;
- reproducible `-trimpath` build command;
- retained GraphBench binary and checksum;
- sanitized invocation;
- corpus declaration and checksum;
- raw JSONL, Markdown report, plan JSON, and exact observations;
- PostgreSQL/Neo4j versions and relevant settings;
- fixture configuration, physical cardinality, relation sizes, and checksum;
- host/kernel/CPU/cgroup identity;
- connection/session identifiers without credentials;
- start/end timestamps and run-series ID.

Freeze a new contemporaneous incumbent control if the historical binary cannot
be reconstructed or if the fresh incumbent differs beyond same-binary block/reload A/A
resolution. Historical evidence remains published even if it is not used for
causal acceptance.

### Extend generated ADCS reference coverage

Update GraphBench so every supported `generated_adcs` case can request exact
PostgreSQL references. Specifically:

- route the `generated_adcs` category through `adcsReferenceSpecs`;
- derive minimum and maximum depth from `ScaleCase.Shape` rather than
  hard-coding 15;
- handle endpoint-ID and path-observed generated names by declared observation
  metadata rather than legacy string names;
- retain graph, relationship-kind, direction, label, property, and uniqueness
  constraints identical to the public query;
- validate exact endpoint multisets and complete path identity outside timed
  intervals;
- treat an empty search result as a valid exact result: reference setup must
  not require precomputed hydration IDs, complete comparators must return a
  typed empty result, and a hydration-only arm must either emit its typed empty
  result or record `not_applicable_empty_input` instead of failing setup;
- declare architecture, implementation ID, state shape, observation boundary,
  and semantic-validation level on every arm;
- retain legacy base-fixture reference names for historical readers without
  grouping unlike implementations.

The direct reference ladder must include:

1. prepared round trip;
2. root predicate/validation;
3. fixed suffix rows and distinct boundary IDs;
4. root first-hop adjacency;
5. current forward ordered-ID search;
6. forward search with factored suffix;
7. exact suffix-seeded reverse ordered-ID search;
8. backward viability plus exact forward ordered-ID search;
9. hydration from precomputed ordered IDs;
10. complete endpoint or path result for each search arm;
11. translated CySQL.

Component references may return a different row count when their boundary is
explicitly diagnostic. Every complete comparator must return the exact public
observation.

Add an exact reference-arm selector such as `-postgres-reference-arms`. Reject
unknown and duplicate arm names. A targeted run must not pay for every
tournament arm unless requested.

### Structured plan attribution

Extend the JSON plan visitor and result schema to record the fields PostgreSQL
actually exposes:

- root rows;
- exact suffix rows and distinct boundaries;
- recursive node rows, loops, total rows, row width, and timing;
- forward and reverse edge probes;
- root, expansion-end, suffix-node, and final hydration loops;
- shared/local/temp reads, hits, dirtied, and written blocks;
- temp files/bytes where available;
- planning and execution time;
- SQL bytes and fingerprint;
- planned strategy and fallback contract.

Plan metrics must be extracted structurally from JSON rather than inferred only
from total buffer counts or brittle text-plan lines. PostgreSQL plan JSON does
not expose per-depth recursive counts, semantic rejection reasons, retained
trail bytes, or actual fallback identity. Collect those through separate
untimed instrumented reference queries, fixture-declared counts, or
benchmark-only helper diagnostics, and label every value `measured`,
`fixture_derived`, or `estimated`. Never sum nested inclusive plan times as if
they were exclusive; use non-overlapping plan regions or controlled component
deltas for time attribution.

### R0 exit criteria

- Every generated ADCS target has exact PostgreSQL reference coverage.
- D16 uses a true maximum depth of 16.
- Endpoint references preserve duplicate ID rows.
- Full-path references validate ordered node/relationship identity,
  properties, direction, and multiplicity.
- A direct reverse comparator emits exact results on the current fixture.
- At least 90% of incumbent shared-hit work **and** 90% of incumbent execution
  time are attributed through non-overlapping regions or controlled deltas.
- Plan plus instrumented attribution records distinguish boundary generation,
  recursion, suffix join, and hydration with explicit provenance.
- Zero-result generated cases complete without reference-setup errors.
- The entering incumbent bundle is durable and reconstructible.
- No production SQL changes in this phase.

## Phase R1: Build an orthogonal ADCS corpus

### Fixture controls

Replace or supplement modulus-only `ValidSuffixEvery` with exact independent
controls for:

- root has or lacks a valid zero-depth suffix;
- exact reachable suffix source count;
- exact reachable suffix depths;
- exact disconnected suffix source count;
- invalid-kind source count;
- invalid-direction source count;
- invalid-endpoint-kind source count;
- suffix paths per boundary source;
- fixed-suffix branching and convergence;
- same-endpoint relationship-distinct expansion and suffix trails using
  distinct allowed kinds within PostgreSQL's uniqueness constraint;
- expansion cycles and self-loops;
- root match count and duplicate source-row count;
- property payload size.

Keep deterministic fixture IDs and checksums. Add logical relationship keys to
semantic fixtures so storage-permitted relationship-distinct trails, including
same endpoints with distinct allowed kinds, can be distinguished exactly.
Same-endpoint/same-kind parallelism cannot be loaded under the current unique
constraint and is explicitly outside this continuation unless a separate
schema-capability proposal selects that migration.

Fixture metadata must declare expected:

- root-source rows and distinct roots;
- forward member states for the generated acyclic shapes;
- suffix rows and distinct boundary sources;
- reachable and disconnected boundaries;
- expected reverse states for deterministic acyclic shapes;
- complete output trail count;
- node/edge counts and checksum.

### Predeclared scale slices

Do not run an unnecessarily large full Cartesian product. Use orthogonal
slices plus adversarial interactions.

| Slice | Fixed values | Sweep |
|---|---|---|
| Depth | fanout 16, one reachable suffix | 0, 1, 2, 4, 8, 16, 32, 64 |
| Fanout | depth 8, one reachable suffix | 1, 16, 128, 512, 1000 |
| Large sparse | current topology | D16/F1000, one branch suffix, one disconnected suffix |
| Large false boundary | D16/F1000, one reachable suffix | disconnected boundaries 0, 1, 1000, 10,000 |
| Reverse fan-in | one suffix boundary | inbound fan-in 1, 16, 128, 512, 1000 |
| Suffix length | qualified directed topology | 1, 2, 3, 4 fixed hops; only 3 is initially production-eligible |

Use exact reachable branch-source counts rather than rounded percentages:

```text
D8/F512:    0, 1, 5, 51, 256, 512
D16/F1000:  0, 1, 10, 100, 500, 1000
```

For every positive reachable count `r` in the two discovery sweeps, use exact
disconnected counts `0, r, 10*r, 100*r`. A ratio is undefined
when `r = 0`, so zero-reachable controls instead use absolute disconnected
counts `0, 1, fanout, 10*fanout`. Store the exact integer counts—not percentage
labels—in each fixture manifest and checksum.

Keep holdout configurations out of threshold selection, including D6/F64,
D12/F256, and D24/F768. Their respective reachable-count sweeps are
`0,1,6,32,64`, `0,1,3,26,128,256`, and `0,1,8,77,384,768`; disconnected
counts follow the rule above. They are used only to validate selector regret.

### Output and hydration slices

Cover output cardinalities:

```text
0, 1, 2, 32, 128, 1000
```

and property payloads:

```text
0 bytes, normal fixture payload, 4 KiB
```

Measure endpoint IDs, raw ordered IDs, and full paths. Pair ordered-ID and
full-path samples on the same physical connection and round so materialization
tax is a direct paired delta.

### Semantic adapter

The exact adapter includes:

- zero-length and positive-minimum paths;
- exact lower/upper bounds and open-upper-bound fallback;
- direct, linear, branching, convergent, cyclic, repeated-node, self-loop,
  dead-end, and disconnected shapes;
- same-endpoint relationship-distinct trails using distinct allowed kinds;
- multiple suffix paths from one boundary;
- multiple boundaries producing the same CA/domain IDs;
- root, middle, and suffix relationship reuse rejection;
- overlapping relationship-kind sets across variable and fixed segments;
- outbound, inbound, wrong-direction, wrong-kind, and directionless fallback;
- missing, null, contradictory, non-unique, and graph-colliding roots;
- endpoint kind/property/existence rejection;
- missing root, missing intermediate expansion node, missing boundary, and
  missing fixed-suffix node rejection;
- duplicate source rows and correlated/multi-root fallback;
- path aliases, `WITH`, path functions, aggregation, optional match, and
  mutation fallback;
- two compound path calls in one statement;
- cancellation, rollback, error, and physical-session reuse.

Shared public semantics belong in backend-equivalent integration cases and
templates. PostgreSQL-only orphan, plan, buffer, and helper behavior belongs in
driver-scoped tests selected only by a PostgreSQL connection string.

### R1 exit criteria

- Density, false-boundary population, output count, and payload can vary
  independently.
- Zero reachable suffixes are representable.
- Exact expected forward/reverse state and result counts are fixture metadata.
- Storage-permitted relationship-distinct and duplicate-output semantics are
  independently validated; same-endpoint/same-kind parallelism remains a
  separately justified schema-capability workstream.
- The normal, crossover, dense, and adversarial cases are predeclared before
  the tournament.
- Existing generated-case checksums remain stable or receive an explicit
  versioned migration in the corpus declaration.

## Phase R2: Run the benchmark-only tournament

### Comparator arms

GraphBench exposes both client boundaries explicitly. Only raw-pgx arms enter
architecture ratios; the production boundary enters predecessor and rollout
gates.

| ID | Architecture | Purpose |
|---|---|---|
| A0-E2E | current production CySQL query end to end | production predecessor control only |
| A0-SQL | A0-E2E's emitted SQL through raw pgx | raw topology and client-attribution control |
| A1a | current forward exact trails with bound-root reuse only | isolate invariant root work |
| A1b | A1a plus scalar state and late hydration | isolate repeated hydration cost |
| A2 | factored exact suffix bag plus forward exact trails | remove per-state suffix lookup |
| A3 | exact suffix-seeded reverse trails | highest sparse-case upside |
| A4 | backward viability plus exact forward trails | intermediate-density alternative |
| A5 | exact meet-in-the-middle trails | conditional only if A3/A4 leave a material gap |
| S1 | bounded density/state selector | production strategy candidate |
| O0-p50/O0-p95 | per-fixture fastest correct PostgreSQL arm for each metric | offline selector-regret oracles |
| N0 | public Neo4j query | exactness and plan-order oracle only |

Classic bidirectional BFS is not A5. Any meet-in-the-middle arm must use one
canonical split depth derived from the total accepted path length, preserve
both half-trail identities, reject relationship overlap across halves and
suffix, restore exact multiplicity, and prove that every ordered complete
edge sequence is emitted exactly once even with repeated nodes. Do not build
A5 unless neither A3 nor A4 meets the reference-gap rule.

Every raw topology arm must use identical:

- graph scope and fixture snapshot;
- root and suffix semantics;
- parameters and transaction boundary;
- binary result formats and client drain path;
- endpoint-ID, ordered-ID, or complete-path observation boundary;
- untimed exact validation.

A0-E2E intentionally differs only at the CySQL translation/client boundary. It
is never divided by a raw arm for an architecture acceptance ratio. A0-SQL is
the denominator for A1a/A1b and A2-A5 raw topology comparisons; A0-E2E is the
denominator for a candidate production CySQL build measured end to end.

Records declare architecture/version, direction, state shape, observation
shape, exactness level, boundary count, forward/reverse states, examined edges,
hydrated rows, retained bytes, and selected/fallback reason.

### Tournament protocol

Predeclare and retain the exact arm schedule. For five simultaneously timed
arms, use a ten-sequence Williams/balanced carryover design; if the active arm
count changes, generate the appropriate carryover-balanced design or split
arms into independently balanced blocks with a shared A0-SQL control.
Reversing a long list on even rounds is insufficient because middle arms remain
systematically in the middle.

The initial blocks are fixed as:

| Slot | Block B1 | Block B2 | Conditional B3 |
|---|---|---|---|
| T1 | A0-E2E | A0-SQL | A0-SQL |
| T2 | A0-SQL | A2 | A2 |
| T3 | A1a | A3 | A3 |
| T4 | A1b | A4 | A4 |
| T5 | A2 | S1 | A5 |

B3 is opened only by the A5 trigger. Within each block, rounds use this exact
slot order, where each row is one independently reloaded round:

```text
T1 T2 T5 T3 T4
T2 T3 T1 T4 T5
T3 T4 T2 T5 T1
T4 T5 T3 T1 T2
T5 T1 T4 T2 T3
T4 T3 T5 T2 T1
T5 T4 T1 T3 T2
T1 T5 T2 T4 T3
T2 T1 T3 T5 T4
T3 T2 T4 T1 T5
```

This gives every directed carryover pair twice. Preserve the schedule and its
arm mapping in the artifact bundle; O0-p50/O0-p95 are computed offline and N0
runs in a separate untimed-oracle block.

Discovery uses:

- ten independently reloaded rounds for each five-arm balanced block;
- twenty untimed warmups;
- thirty measured warm observations;
- pool size one and a pinned physical connection;
- fresh fixture truncate/reload, cardinality/checksum verification, and
  `VACUUM (ANALYZE)`;
- PostgreSQL-only timing, with Neo4j exact-oracle blocks separate from primary
  timing;
- cold preparation recorded separately;
- raw samples and plan JSON for every arm and round.

Discovery data selects candidate architectures and thresholds. It is not
reused for final acceptance after arm or threshold selection.

### R2 selection rules

- Reject an arm immediately on exactness, graph-scope, cancellation, memory,
  or cleanup failure.
- Close any arm Pareto-dominated across latency, p95, buffers, retained state,
  cold cost, and concurrency.
- Keep A1b inside later arms even if A1a or A1b is not independently
  shippable.
- Keep A2 as the exact forward fallback unless A1b or A0-SQL dominates
  it throughout the density matrix.
- Select A3 only over a sparse envelope where its state and resource slopes are
  bounded.
- Select A4 only if it materially reduces selector regret in an intermediate
  density region.
- Do not build A5 unless A3/A4 both miss the correct PostgreSQL reference by
  more than 10% and 0.50 ms.
- Do not begin frontier/helper work while direction still accounts for a
  material gap.

### R2 exit criteria

- Every complete arm is exact across the R1 adapter.
- Forward, reverse, suffix, and hydration work are independently measured.
- A3's measured state count on D16/F1000 is close to the declared fixture
  expectation rather than 16,001.
- The fastest correct arm and crossover region are stable across reloads.
- A production strategy hypothesis is predeclared against holdout fixtures.
- Every rejected arm and reason remains in the durable tournament report.
- No production dispatcher branch exists yet.

## Phase R3: Ship root reuse, then late hydration

R3 changes staging, not search direction.

### Root reuse

When an expansion's left node is already present in the preceding frame:

- project that existing scalar or composite binding into the candidate stage;
- constrain recursive `root_id` to the preceding binding without another
  node-table lookup;
- preserve duplicate preceding rows by rejoining the exact source bag;
- rehydrate at most once if a later stage upgrades an ID-only root to a full
  entity.

Do not assume a root is unique merely because the fixture's object ID is
unique. The general lowering must either preserve the original bag or remain
inside a proven singleton envelope.

### Boundary and path hydration

Keep recursive output scalar through suffix qualification. Then:

- validate boundary-node existence after a suffix match, or inside the exact
  suffix bag;
- hydrate the boundary node only when a downstream observation needs it;
- hydrate fixed suffix nodes/relationships only after their local predicates
  have passed;
- project CA/Domain IDs directly in endpoint mode;
- invoke the path materializer only for final complete path rows;
- retain ordered relationship IDs and exact multiplicity throughout.

### R3 plan invariants

On the D16/F1000 control:

- invariant root lookup loops do not scale with 16,001 recursive rows;
- full boundary-composite lookup loops are bounded by suffix-qualified rows;
- endpoint mode contains no path materializer;
- path hydration loops are bounded by the two final rows;
- no required endpoint-existence check disappears;
- graph partition pruning remains concrete.

### R3 shipment rule

Ship A1a and A1b as separate measured changes. A1a-SQL must clear its raw
topology gate against A0-SQL, then the A1a production CySQL build must clear its
end-to-end predecessor gate against A0-E2E before activation. A1b-SQL is
measured against accepted A1a-SQL, followed by the same end-to-end candidate
versus immediate-predecessor check. If either structural reduction is correct
but its standalone latency does not clear materiality, keep it only as an
attributable dependency of a later qualified change without claiming an
independent win.

### R3 exit criteria

- Root and boundary lookups no longer run once per recursive state.
- Endpoint and full-path observations remain exact.
- PostgreSQL orphan behavior is unchanged.
- Field-requirement and last-use tests prove composites are not retained past
  their last required stage.
- Translation goldens, templates, mutation fallbacks, and shared integration
  cases pass.
- Matched A1a-SQL/A0-SQL and A1b-SQL/A1a-SQL evidence, plus separate
  candidate/predecessor CySQL end-to-end evidence, supports independent
  shipment or an explicit combine-with-R4 disposition.

## Phase R4: Qualify and conditionally ship the exact factored suffix relation

R4 adds a compound-region builder that consumes the expansion plus its planned
fixed suffix and emits one result-producing relation.

### Compound builder contract

Intercept the planned region before the ordinary per-step CTE builder. Internally
emit:

```text
source/root rows
distinct roots when safe
suffix_rows
distinct boundary_ids when useful
forward search
exact candidate join
late hydration
```

The builder consumes the entire expansion-plus-suffix region and publishes the
suffix-end frame contract expected by later query stages. Mark consumed steps
so the normal traversal renderer does not emit them again.

The current stepwise builder remains unchanged as fallback.

### Exact suffix consumption

Both suffix qualification and final suffix bindings must come from the same
`suffix_rows` relation. Do not:

- use a boolean `EXISTS` as a result-producing substitute;
- prove suffix existence and traverse the suffix again;
- deduplicate suffix rows by boundary or endpoint;
- omit suffix relationship IDs needed for cross-segment uniqueness;
- materialize node/JSONB fields after their last predicate use.

Compare materialized and inline physical forms. Select one only over the
measured density/memory envelope and assert that PostgreSQL does not recreate a
16,001-loop `Enroll` probe.

R4 may activate before R6 only if its chosen physical form is non-inferior
across the entire structurally eligible suffix-density, missing-root, and
concurrency envelope. If materialization choice depends on live density, keep
the compound branch behind the incumbent and qualify it with S1 in R6. Query
shape alone is not evidence that suffix materialization is sparse.

### R4 plan invariants

- the fixed suffix is evaluated once per statement/outer eligible source, not
  once per recursive row;
- no suffix-producing edge scan has 16,001 loops on the sparse fixture;
- exact suffix multiplicity is retained;
- endpoint/full-path hydration occurs after the candidate join;
- prefix and suffix relationship IDs are disjoint;
- graph-specific child relations and indexes are used;
- suffix production and forward recursion have zero actual loops when
  `root_presence` is empty;
- normal-tier materialization performs no temp I/O.

### R4 exit criteria

- A2 is exact across sparse, dense, duplicate-suffix, and false-boundary cases.
- The plan has no cardinality-proportional suffix probe.
- A2 clears its viability gate or is retained only as the tested forward
  fallback.
- Fallback stepwise translation remains exact for every ineligible form.
- The change is independently reversible from search-direction selection.

## Phase R5: Implement and qualify the selected suffix-driven search

### Candidate lowering behind the incumbent

Implement only the R2 winner:

- exact suffix-seeded reverse enumeration for its proven sparse envelope;
- backward viability plus exact forward enumeration for a proven crossover
  envelope;
- or the factored forward query if no reverse candidate qualifies.

Add `ExpansionSearchStrategyDecision` with the production selector still
choosing the existing or already safe factored-forward path. Translation
diagnostics, benchmark-only routing, plan invariants, and tests must stabilize
before R6 may activate a density-dependent branch.

Qualify one observation boundary at a time:

1. endpoint-ID observation;
2. ordered-ID internal boundary;
3. full-path observation through the selected materializer.

Do not combine activation with a new helper, index, cache, or client decoder.

### Reverse search requirements

The reverse candidate branch must:

- seed every distinct eligible boundary;
- preserve and restore every suffix row;
- prepend expansion edge IDs;
- apply the original min/max depth;
- enforce variable and cross-segment relationship uniqueness;
- preserve repeated nodes and every storage-permitted relationship-distinct
  trail;
- validate roots at candidate states or rejoin exact valid roots;
- restore root-source multiplicity;
- hydrate only final rows;
- record strategy and fallback;
- use stable typed parameters and graph-specific relations.

It must also be guarded by `root_presence`: reverse seeds and recursive work
have zero actual loops when no valid root exists. Every node implied by an
accepted trail is existence-validated unless the supported write path has
first been proven to guarantee it.

### Viability search requirements

The viability branch must:

- keep reverse distance in the deduplication key;
- remain a permissive filter only;
- never use viability row count as result multiplicity;
- retain an exact `UNION ALL` forward trail enumerator;
- allow false-positive viability states but no false negatives;
- preserve all final suffix and source multiplicity.

### R5 exit criteria

- Sparse D16/F1000 work is no longer proportional to all 16,001 forward
  states in the benchmark-only candidate.
- The selected arm clears the sparse structural and reference-closure gates.
- Dense, false-boundary, high-fan-in, and missing-root cases remain exact and
  bounded in qualification.
- Every ineligible shape records a stable fallback code.
- Endpoint and path observations pass the complete semantic adapter.
- Production selection still chooses the incumbent/safe forward path; no
  density-dependent reverse or viability plan is active yet.
- No new schema migration is required unless separately selected in R6.

## Phase R6: Bound density/overflow behavior and tune residual frontiers

### Adaptive selection

Run the predeclared selector on discovery-independent holdout fixtures. Compare
its chosen p50 and p95 with O0-p50 and O0-p95 respectively and report
selection regret.

If a bounded probe/hybrid is used:

- execute it in the same statement and snapshot as both branches;
- record probe inputs, chosen strategy, and overflow reason;
- prove the unchosen recursive branch has zero actual loops;
- bound probe rows and bytes;
- discard every partial result on overflow;
- fall back exactly rather than raising a resource error;
- test parameter changes under prepared `auto`, forced custom, and forced
  generic plans.

If no selector clears the regret gate, restrict reverse search to a static
envelope only when structural constraints prove bounded behavior across every
possible data distribution in that envelope; otherwise retain the forward
strategy. Do not infer sparsity from query shape or widen eligibility by
intuition.

### Candidate-build activation

Only after the selector or genuinely static envelope clears every holdout,
overflow, missing-root, plan-cache, resource, and concurrency gate may R6
enable the branch in the release-candidate build. Enable endpoint-ID, then
ordered-ID, then full-path observation as separate measured changes. In every
case, the unchosen result-producing branch must show zero actual loops, and
overflow must return the complete incumbent result in the same statement
snapshot. This is not a
production rollout; R7 qualification still blocks release.

### Conditional frontier tournament

Open frontier work only when post-activation R6 attribution shows a portable
SQL gap larger than both 10% and 0.50 ms. Compare at an identical ordered-ID
boundary:

```text
F0 fenced LATERAL/OFFSET 0 point probes
F1 unfenced recursive worktable-to-edge join
F2 level-synchronous set-oriented frontier
F3 parent-linked trace representation
F4 typed PL/pgSQL bounded helper
```

Measure small and wide frontiers separately. Preserve exact trail state; global
node visited/dedup remains invalid.

### Helper boundary, only if selected

A helper must:

- accept fully typed graph, kind, direction, depth, root/boundary, and limit
  inputs;
- return fully typed IDs, depth, found/overflow, and counters;
- avoid runtime SQL strings;
- be graph-scoped in every query;
- use a hard state and memory limit;
- transparently select a correct fallback on overflow;
- expose no partial results;
- leave no session-global mutable result state;
- pass fresh-install/full-teardown/up, versioned upgrade/compensating rollback,
  cancellation, concurrency, and physical-session reuse tests;
- declare realistic row estimates only where PostgreSQL uses them correctly.

If exact same-statement restart cannot be proven, reject the helper or narrow
its static envelope.

### Supporting statistics and indexes

Only after R5/R6 re-attribution, consider:

- an expression B-tree property index for the root predicate when root
  validation is at least 10% and 0.50 ms of remaining time;
- higher per-partition statistics targets or multicolumn statistics when they
  materially improve a factored-suffix plan decision;
- partial relationship-kind indexes only when their measured read benefit
  exceeds index size and write amplification.

Do not change global PostgreSQL planner settings for this workload. Better
cardinality estimates may support the chosen topology but cannot by themselves
remove 16,001 exact states.

### R6 exit criteria

- Selector regret clears the holdout gate or reverse eligibility remains
  statically bounded by constraints that do not depend on current data.
- Overflow returns exact fallback results in the same snapshot.
- Decision overhead is below its gate.
- Candidate-build activation occurs only after the bounded
  selector/static-envelope proof; otherwise the candidate continues to select
  the safe forward path. Production rollout remains blocked on R7.
- Any frontier/helper change closes a measured residual rather than masking a
  direction defect.
- Any schema change has complete migration and operational evidence.

## Phase R7: Full qualification

### Test workflow for every behavior increment

1. Add optimizer decision, eligibility, and fallback unit tests.
2. Add translator planned/applied/skipped and selector-contract tests; derive
   actual runtime branch assertions from plan loops or explicit
   benchmark-diagnostic counters.
3. Add backend-equivalent integration cases/templates for public Cypher
   semantics.
4. Add PostgreSQL-scoped orphan, plan, buffer, state-limit, and helper tests.
5. Update translation source cases, run `make test_update`, and inspect every
   copied/generated golden diff before accepting it.
6. Run `make format` after code and generated-artifact changes.
7. Run `make test`.
8. Run PostgreSQL `make test_all` with the approved PostgreSQL connection
   string.
9. Run Neo4j `make test_all` with the approved Neo4j connection string.
10. Run `go test -race ./cmd/graphbench` and focused race tests for shared
    optimizer/cache state.
11. Run targeted GraphBench A/A and matched candidate blocks.
12. Run the complete performance corpus and exact Neo4j oracle manifest.
13. Run `git diff --check`.

Do not add driver-specific expected public results or skips to the shared
integration corpus. Connection strings remain approved environment input and
must be redacted from artifacts and documentation.

### Planning and partition dimensions

Run representative sparse, crossover, dense, missing-root, and false-boundary
points with:

- `plan_cache_mode = auto`;
- forced custom plans;
- forced generic plans;
- cold and warm prepared state;
- one and multiple graph partitions;
- colliding explicit IDs in a decoy graph.

Assert active-child pruning and graph-scoped access in every branch and
fallback.

### Concurrency and cancellation

Run:

- pool size one;
- half supported pool;
- full supported pool;
- twice-pool request concurrency;
- cold whole-pool initialization;
- mixed ADCS, shortest, lookup, and mutation traffic;
- cancellation at shallow, deep, dense, and disconnected points;
- success -> error/rollback -> success on the same physical connection.

Record QPS, pool wait, p50/p95, backend identity, shared/local/temp buffers,
temp files/bytes, memory high-water, cancellation latency, cleanup, and state
visible after connection reuse.

For each load level, run at least ten independently initialized matched blocks,
alternating A0-E2E/candidate-E2E order by block. Reload and analyze the fixture
before each block pair, use the same request trace and connection count for both arms,
and bootstrap paired block-level QPS and p95 differences. Apply the QPS lower
confidence bound and p95 ratio upper confidence bound in the concurrency gate,
using predeclared one-sided 95% intervals; individual request samples are not
independent block replicates.

Freeze acceptance ceilings at 64 MiB additional high-water per backend session
and 512 MiB additional high-water for an eight-connection pool before capture.
A later product-budget change requires a new predeclaration and fresh capture;
it may not retroactively rescue a failed run.

Use named mechanisms: capture backend PID, sample `/proc/<pid>/status` high-water
where available, sample PostgreSQL memory-context totals on the same physical
connection before/after untimed diagnostic runs, and sample the isolated test
cgroup/process-tree high-water for the pool. Attribute temp work from JSON-plan
temp blocks plus isolated `pg_stat_database` temp-file/temp-byte deltas. Measure
trail-state bytes in untimed diagnostic SQL with `pg_column_size` over the exact
state rows. If platform access prevents one mechanism, mark the metric missing
and fail its gate rather than substituting an unlabelled estimate.

Run at least 10,000 mixed calls before closing the workstream. p99 remains
diagnostic until each gated series has at least 10,000 observations and its
A/A-derived sample requirement is satisfied.

### R7 exit criteria

- Shared PostgreSQL and Neo4j integration semantics pass.
- Every strategy and fallback passes graph-scope, multiplicity, path-order,
  orphan, cancellation, rollback, and session-reuse tests.
- Complete-corpus p50/p95 gates pass.
- Pool memory and per-session ceilings pass.
- Twice-pool load produces bounded pool wait rather than extra backend state
  or memory growth.
- Normal-tier queries have no unexpected temp or local-buffer I/O.
- Soak finds no unbounded memory, prepared statement, workspace, or retained
  result growth.
- Only after every R7 exit criterion passes may the accepted candidate enter
  the rollout sequence; any failure leaves production on the incumbent.

## Phase R8: Live rerun and residual decision

After R7, repeat the clean cross-backend protocol on independently reloaded
fixtures. Publish current and predecessor:

- endpoint/path p50 and p95;
- PostgreSQL/Neo4j ratios as diagnostics;
- PostgreSQL reference gaps;
- forward/reverse states and examined edges;
- shared/local/temp buffers;
- planning, execution, transfer, decode, and end-to-end time;
- selector decisions and regret;
- concurrency and memory results.

Rank remaining work by:

```text
addressable_cost = max(candidate - best_correct_pg_reference, 0)

weighted_cost = addressable_cost
              * documented_workload_frequency
              * confidence
              * concurrency_or_resource_amplifier
```

Do not rank by the Neo4j ratio. If selected portable SQL at the raw-pgx
boundary is within 1.10 of its correct PostgreSQL reference at that same
boundary, or the absolute gap is below measurement resolution, and production
CySQL clears its predecessor gate, close this workstream even if Neo4j remains
faster.

Open a native-extension or closure/storage ADR only if two plausible portable
alternatives fail, the remaining absolute gap is material, profiling attributes
it to unavoidable PostgreSQL executor bookkeeping, and the deployment model
accepts the operational cost.

## Metrics and plan invariants

### Primary performance metrics

- client-visible p50 and p95;
- matched absolute and relative changes;
- raw-pgx and translated-CySQL boundaries;
- PostgreSQL planning and execution time;
- throughput and pool wait under concurrency.

### Search metrics

- exact suffix rows and distinct boundaries;
- forward and reverse seed/state rows by depth from instrumented untimed arms,
  with aggregate recursive rows/loops from plan JSON;
- recursive generations;
- relationship index probes and edges examined;
- states rejected by root, depth, suffix, and uniqueness constraints;
- retained trail/state bytes from `pg_column_size` diagnostics and frontier
  high-water from explicit instrumented counters;
- time, buffers, and bytes per retained state.

### Hydration and client metrics

- root, boundary, suffix-node, and edge hydration rows;
- full paths materialized;
- raw ordered-ID to full-path paired tax;
- first-row, all-row transfer, decode, drain, and allocation bytes;
- result ownership and retained memory.

### Resource metrics

- shared/local/temp hits, reads, dirtied, and written blocks;
- temp files and bytes;
- WAL, which must remain zero for read-only arms;
- backend and whole-pool memory high-water;
- cancellation and cleanup latency.

Every resource metric records its mechanism, scope, and provenance. JSON-plan
buffers are query-local; `pg_stat_database` and process/cgroup deltas are valid
only in the isolated benchmark interval; fixture-derived or estimated values
are never accepted as measured resource-gate evidence.

### Required sparse reverse plan invariants

- reverse `MemberOf` expansion uses the active child partition's
  `(end_id, kind_id) INCLUDE (id, start_id)` index;
- the fixed suffix is produced once;
- no validated root is scanned or hydrated once per recursive row;
- boundary and full-entity hydration is bounded by qualified candidates;
- path hydration occurs after root reachability;
- graph partition pruning is concrete;
- no 16,001-loop `Enroll` access remains;
- exact reverse states are proportional to suffix-seeded reverse trails rather
  than the full forward closure;
- no normal-tier temp or local I/O appears.

Capture plan JSON for every arm and round. Assertions should target semantic
operators, loop/state counts, and access direction rather than brittle complete
plan text.

## Statistical protocol

### Discovery and confirmation are separate

The R2 discovery tournament selects architectures and thresholds. Its samples
must not also serve as final confirmation after that selection.

Final confirmation uses:

- saved, checksummed incumbent and candidate binaries;
- ten independently reloaded matched rounds initially;
- twenty untimed warmups and fifty measured warm samples per primary case;
- incumbent then candidate in odd rounds and candidate then incumbent in even
  rounds;
- predeclared five-round extensions, to at most twenty rounds, only when
  confidence remains insufficient;
- five rounds and thirty samples for the broader scale/control matrix after
  the primary confirmation;
- a same-binary block/reload A/A in addition to within-session alternating
  A/A;
- the worse applicable relative and absolute A/A resolution.

Bootstrap matched round medians and stratified p95 with a recorded seed and
confidence level. Use fresh confirmation samples and either Holm-adjust the
endpoint/path primary comparisons or use 97.5% intervals for the two primary
hypotheses.

Abort a block on a source, binary, SQL, fixture, schema, index-size, settings,
result, physical-connection, maintenance, intended plan-class, or
predeclared-host-saturation mismatch.

Keep p99 diagnostic until the A/A-derived requirement and at least 10,000
observations per gated series are both satisfied.

### General materiality and non-inferiority

For a production behavior increment, require both relative and absolute
evidence. A ratio movement below measurement resolution is not a win, and a
large absolute regression cannot hide behind a percentage.

Unless a phase sets a stricter target:

```text
improvement ratio UCB <= 0.90
median saving LCB      >= max(case A/A absolute resolution, 0.10 ms)
```

For affected-family controls:

```text
p50 and p95 ratio UCB <= 1.05
```

or:

```text
absolute increase UCB <= max(0.10 ms, case-specific A/A absolute resolution)
```

The complete-corpus 20% threshold remains an
emergency ceiling, not permission for an unexplained 5-19% regression.

Every declared PostgreSQL record and Neo4j oracle record must be present and
exact. Do not compare only the intersection of successful records.

## Architecture-specific acceptance gates

### Correctness gate

Any mismatch is an immediate failure in:

- exact result multiset;
- duplicate multiplicity;
- ordered path node/relationship identity, properties, direction, or
  uniqueness;
- graph scope;
- zero-length/minimum/maximum depth behavior;
- null, missing, contradiction, error, and optional behavior;
- cancellation, rollback, or physical-session reuse.

No timing or resource win can waive this gate.

### A1a root-reuse and A1b late-hydration gates

A1a must eliminate per-recursive-row invariant root work and clear the general
affected-family non-inferiority gate. Its raw topology comparator is A0-SQL;
the shippable production build is compared separately with A0-E2E. Claim it as
an independent performance win only when its median saving also exceeds A/A
absolute resolution at both applicable boundaries.

A1b may ship independently only when both D16/F1000 endpoint and path forms
have:

- median-ratio upper confidence bound at most `0.85` versus A0-SQL;
- median-saving lower bound at least `5 ms`;
- p95-ratio upper confidence bound at most `0.90`;
- shared-hit ratio at most `0.60`;
- zero per-recursive-row invariant-root hydration;
- no affected-family regression beyond the 5% non-inferiority budget.

Measure A1b-SQL against accepted A1a-SQL and report the cumulative raw ratio
against A0-SQL, then apply the production end-to-end predecessor gate.
If it misses timing but satisfies correctness and structural requirements,
retain A1b inside A2-A4 and mark independent shipment as not material.

### A2 factored-forward gate

Continue A2 as a production candidate only when the large sparse case has:

- median-ratio upper bound at most `0.70` versus A0-SQL;
- shared-hit ratio at most `0.40`;
- no suffix-producing lookup whose loop count scales with recursive rows;
- exact suffix and output multiplicity;
- no normal-tier temp I/O.

An A2 arm Pareto-dominated by A1b or A3 at every density point remains only as
historical evidence. A correct non-dominated A2 may remain the dense or
overflow fallback even if it is not the sparse winner.

### Sparse search-direction gate

A3, A4, or a later exact structural arm must meet all of these on both
D16/F1000 endpoint and path forms before a direction-aware production lowering
is justified:

- median-ratio upper bound at most `0.25` versus A0-SQL;
- p95-ratio upper bound at most `0.40`;
- median-saving lower bound at least `30 ms`;
- shared-hit ratio at most `0.10`;
- recursive/search-state ratio at most `0.02`;
- no temp or local I/O;
- exact two-row result.

Relative to the entering medians, the ratio gate corresponds to approximately
14 ms endpoint and 16.5 ms path. The program objective is an absolute warm
median below 5 ms on this fixture. That objective is reported against the
correct PostgreSQL reference; it is not enforced through a Neo4j ratio.

If exact reverse search does not reduce the expected 16,001 states by at least
90%, stop and correct the relational architecture before tuning indexes,
arrays, or frontier mechanics.

### PostgreSQL reference-closure gate

Use identical raw-pgx execution, binary decoding, result validation, and drain
boundaries for the portable-SQL closure comparison. For every selected target:

```text
candidate_sql_raw_pgx / best_correct_reference_sql_raw_pgx UCB <= 1.10
```

Alternatively, the absolute remaining gap upper bound may be below:

```text
max(case-specific A/A absolute resolution, 0.10 ms)
```

Separately compare production CySQL end to end with its immediate production
CySQL predecessor under the phase's materiality and non-inferiority gates.
Translated CySQL versus raw-pgx latency is an attribution measurement, not the
reference-closure ratio; client/translation overhead may not be hidden inside
one side of the `1.10` comparison.

Do not open a typed helper or native-extension phase unless the winning
direction has passed correctness and the remaining portable SQL gap exceeds
both 10% and 0.50 ms.

### Selector gate

On every discovery-independent holdout fixture/observation pair, run the
selector and all correct oracle arms in matched rounds. Define `O0-p50`
separately as the arm with the lowest p50 and `O0-p95` as the arm with the
lowest p95; they need not be the same arm. In each paired bootstrap resample,
reselect the corresponding oracle minimum, compute selector/oracle regret, and
then take the maximum across all predeclared holdouts. Use a simultaneous
max-statistic bootstrap or Holm-adjusted one-sided 95% intervals so oracle
selection and the number of holdouts are both reflected in the bounds.

The simultaneous holdout gates are:

- maximum p50 selector-regret upper bound at most `1.15` versus `O0-p50`;
- maximum p95 selector-regret upper bound at most `1.25` versus `O0-p95`;
- decision overhead at most `max(0.10 ms, 5% of selected-arm latency)`;
- identical decision for equivalent analyzed fixtures;
- explicit exact fallback when estimates, bounds, or probes are unavailable;
- zero loops in every unselected recursive result branch.

If no selector passes, restrict reverse search to a static envelope only when
structural constraints bound every allowed data distribution, or retain the
forward strategy. Do not ship always-reverse.

### Path materialization gate

With search fixed, require:

- paired ordered-ID-to-full-path tax upper bound at most `1.0 ms` for the
  D16/F1000 two-path case;
- no entity hydration before final root-reachable candidates;
- endpoint-only arms perform no path hydration;
- execution and retained bytes from path length 32 to 64 grow by at most
  `2.2`;
- no normal-tier spill;
- exact duplicate and path order.

Materialization may reuse the M0/M1 work from `perf_cont_2.md`; it must not
re-run search or rediscover connectivity already represented by ordered IDs.

### Resource and slope gate

- No normal-tier temp files or local-buffer workspace.
- No WAL for read-only arms.
- No unexplained adjacent-tier increase above `1.25` in time per examined edge
  or bytes per retained state.
- D64/F1000 and the high-disconnected tier finish the complete operation within
  a predeclared two-second normal timeout, including every probe, overflow
  detection, restart, and exact fallback execution; merely choosing fallback
  does not satisfy the gate.
- A cancelled 100 ms search returns control within 250 ms.
- The same physical session succeeds on an exact query immediately after
  cancellation or rollback.
- Per-session and whole-pool memory remain below declared ceilings.

### Concurrency gate

At half-pool, full-pool, and twice-pool load:

- zero incorrect rows, transaction-abort leaks, and unexpected errors;
- candidate-E2E p95 upper ratio versus A0-E2E at most `0.75` on the primary
  sparse workload;
- candidate-E2E QPS lower bound at least `1.5` times A0-E2E at full pool;
- on dense, false-boundary, overflow/fallback, and mixed-traffic controls,
  candidate-E2E p95 ratio UCB at most `1.05` and QPS ratio LCB at least `0.95`
  versus A0-E2E, using the same matched-block protocol;
- whole-pool memory below the declared ceiling;
- oversubscription expressed as bounded pool wait rather than extra backend
  state or memory;
- no state visible after success, error, rollback, cancellation, or physical
  connection reuse.

## Implementation seams

### GraphBench and fixtures

- `cmd/graphbench/references.go`: generated ADCS routing, depth-aware
  parameters, A1a/A1b, A2-A5, and S1 reference SQL, exact boundaries.
- `cmd/graphbench/references_test.go`: architecture identity, SQL invariants,
  exact comparator behavior.
- `cmd/graphbench/postgres.go`: selected reference arms and measurement.
- `cmd/graphbench/results.go` and `types.go`: strategy/state/plan counters.
- `cmd/graphbench/summary.go`: component, selector-regret, and reference-gap
  reporting.
- `cmd/graphbench/postgresql_plan_invariants_integration_test.go`: live plan
  properties.
- `testutil/perf_fixtures.go`: independent suffix/density/fan-in controls.
- `cmd/graphbench/datasets.go`: versioned generated dataset names.
- `benchmark/testdata/scale/cases/generated_adcs.json`: target and holdout cases.
- `benchmark/testdata/scale/README.md`: deterministic configuration contract.

### Optimizer and translator

- `cypher/models/pgsql/optimize/lowering.go`: new typed strategy decision,
  enums, facts, and fallback codes.
- `cypher/models/pgsql/optimize/lowering_plan.go`: whole-pattern region
  recognition and observation/eligibility analysis.
- `cypher/models/pgsql/optimize/selectivity.go`: only bounded static facts;
  never pretend to have live graph statistics.
- `cypher/models/pgsql/translate/translator.go`: index planned decisions and
  report applied/skipped outcomes.
- `cypher/models/pgsql/translate/pattern.go`: intercept an eligible compound
  region before per-step CTE emission.
- `cypher/models/pgsql/translate/traversal.go`: field requirements, consumed
  steps, fallback, and final frame contract.
- `cypher/models/pgsql/translate/expansion.go`: compound suffix/search builder,
  root reuse, scalar candidate state, forward/reverse ASTs.
- `cypher/models/pgsql/translate/model.go`: explicit scalar/search bindings.
- `cypher/models/pgsql/translate/projection.go`: endpoint versus ordered-ID
  versus full-path observation.
- `cypher/models/pgsql/translate/renamer.go`: safe aliases for nested compound
  regions.

Do not implement reverse search by globally mutating logical traversal steps
with `FlipNodes()`. Use a physical compound-region builder while preserving the
logical frame and path direction.

### Schema and materialization

The initial work uses existing indexes and `ordered_edge_ids_to_path`, so it
has no schema migration.

If R6 independently selects a helper or index:

- first publish an R6 ADR naming the owning upgrade/migration mechanism and
  deployment order; this repository currently exposes fresh-install
  `schema_up.sql` and full-teardown `schema_down.sql`, not a stepwise rollback
  system;
- update `schema_up.sql` and full-teardown `schema_down.sql`, but do not treat
  an up/down/up test as proof of an in-place rollback;
- supply and exercise versioned existing-installation upgrade and compensating
  rollback migrations through the mechanism selected by the ADR; if no such
  mechanism is adopted, do not ship the schema-dependent candidate;
- version helper signatures rather than changing behavior in place;
- test fresh install, upgrade, downgrade, and up/down/up;
- measure index size, write amplification, and lock duration;
- document whether online index creation is required;
- keep old binaries functional through the declared rollback window.

## Observability contract

Expose compile-time translator facts without adding query side effects:

- planned and applied strategy;
- structural eligibility facts;
- configured selector/fallback strategy and static fallback reason;
- probe/state limits embedded in the generated strategy.

GraphBench attributes runtime behavior separately:

- infer the branch that actually executed from JSON-plan `Actual Loops` on the
  mutually exclusive result branches;
- collect overflow, generation, rejection, and frontier counters only from
  benchmark-only instrumented SQL/helper diagnostics;
- label plan-derived, directly measured, fixture-derived, and estimated fields;
- never alter public result rows, perform DML, or add session-global counters
  to obtain telemetry.

The complete diagnostic capture also includes:

- actual selected branch and fallback/overflow reason when directly
  observable, otherwise `unknown` rather than a compile-time guess;
- suffix rows and distinct boundaries in diagnostic captures;
- recursive rows, generations, frontier high-water, and examined edges;
- final hydration row count;
- shared/local/temp buffers;
- planning, execution, client, and materialization times;
- strategy, SQL, plan, fixture, source, and binary fingerprints.

Update `docs/postgresql_translation.md` whenever behavior ships. Update
`cmd/graphbench/README.md`, the scale-corpus README, and the root `README.md`
when commands, artifacts, configuration, or user-visible workflows change.

## Rollout and rollback

Each behavior is independently reversible:

1. bound-root reuse;
2. late boundary/path hydration;
3. exact suffix materialization;
4. reverse or backward-viability search selection;
5. adaptive density/state fallback;
6. optional frontier helper or schema change.

Do not retain a dormant permanent feature flag after qualification. The
generic stepwise translator remains the semantic fallback. Rollback is a
forward source change that returns eligible queries to the previous strategy;
the versioned compensating migration selected by the R6 ADR removes any
separately justified helper. Full-teardown `schema_down.sql` is not an
existing-installation rollback mechanism.
Never rewrite repository history or use `git revert` as the agent workflow.

## Risk register

| Risk | Mitigation |
|---|---|
| Exact trails or suffix paths are deduplicated | `UNION ALL` and bag joins for result relations; deduplicate only seed/viability filters, then rejoin exact bags |
| Reversed relationship IDs are misordered | Prepend IDs and validate complete ordered path identities |
| Expansion reuses a suffix relationship | Retain all suffix IDs and perform explicit cross-array exclusion |
| Zero-depth results disappear | Emit boundary seeds at depth zero and apply original minimum at acceptance |
| Reaching a root stops a longer valid trail | Continue recursion through root states until the maximum depth |
| Late hydration exposes dangling endpoints | Preserve final existence joins and add PostgreSQL orphan tests |
| Missing root triggers graph-wide factored work | Require `root_presence` and zero actual suffix/recursive loops on empty roots |
| Scalar recursion crosses a missing intermediate node | Validate every implied final-trail node set-wise unless supported writes prove endpoint integrity |
| Dense suffix or reverse fan-in explodes | Holdout density matrix, capped probes/state, exact forward fallback |
| Outer `LIMIT` fails to bound recursive work | Require plan/runtime proof; reject portable hybrid if demand limiting is unreliable |
| Materialized suffix spills under concurrency | Rows/bytes/temp metrics, supported-memory matrix, forward fallback |
| Planner inlines or re-correlates suffix work | Explicit materialization where selected and plan loop-count invariants |
| Generic-plan behavior differs from custom plan | Test `auto`, forced custom, and forced generic modes |
| Selector overfits fixture thresholds | Discovery-independent holdouts and regret gate |
| SQL size/planning offsets execution gain | Gate SQL bytes and planning separately |
| New index harms writes | No index by default; require separate read/write evidence and migration plan |
| Helper state leaks across sessions | Typed bounded state, cleanup, cancellation, rollback, reuse, and soak tests |
| PostgreSQL-version plan drift | Test supported versions and assert semantic plan properties rather than full text |

## Durable artifact layout

Publish a bundle similar to:

```text
artifacts/perf/adcs-search-<series>/
  predeclaration.json
  manifest.json
  source.patch
  source-untracked-manifest.json
  bin/
    incumbent-graphbench
    candidate-graphbench
    checksums.sha256
  corpus-declaration.json
  fixture-matrix.json
  semantic-results.json
  discovery/
  confirmation/
  baseline/
  candidates/<arm>/
  plans/<arm>/
  state-counters/
  references/
  within-run-aa.json
  block-reload-aa.json
  selector-regret.json
  concurrency/
  cancellation/
  gate.json
  report.md
  checksums.sha256
```

Record source, binary, SQL, schema, fixture, settings, plan, raw sample, arm
order, and exact-observation identities. Connection credentials must not appear
in any artifact.

## Change sequence

Keep behavior changes independently attributable. The recommended sequence is:

1. Generated ADCS reference routing and depth-bound repair.
2. Exact endpoint/path comparator validation.
3. Structured ADCS plan/state attribution.
4. Orthogonal suffix-density, false-boundary, fan-in, payload, and multiplicity
   fixtures.
5. Benchmark-only A1a root-reuse and A1b late-hydration arms.
6. Benchmark-only A2 factored-suffix forward arm.
7. Benchmark-only A3 exact reverse arm.
8. Benchmark-only A4 backward-viability arm.
9. Discovery tournament and architecture/threshold report.
10. New optimizer strategy decision and explicit fallback model, still
    selecting the incumbent stepwise strategy.
11. Candidate-build bound-root reuse.
12. Candidate-build late hydration.
13. Candidate-build exact suffix relation only if non-inferior across its complete
    structural envelope; otherwise keep it behind the incumbent.
14. Reverse or viability implementation and qualification behind the
    production incumbent.
15. Bounded density/state selector and exact overflow fallback, followed by
    staged candidate-build activation after R6 gates and production rollout
    only after R7 passes.
16. Conditional frontier/helper experiment only if residual gates trigger.
17. Conditional schema migration only if independently selected.
18. Full semantic, complete-corpus, concurrency, cancellation, and soak
    qualification.
19. Durable artifact publication and clean live PostgreSQL/Neo4j rerun.
20. Residual cost report and next-plan/stop decision.

Tests accompany every behavior change; they are not deferred to a final
test-only change. Do not combine search direction, materialization, helper,
schema, cache, and client decoding into one performance increment.

## Immediate next actions

Execute in this order:

1. Extend generated ADCS PostgreSQL reference coverage and remove the hard-coded
   depth-15 limit.
2. Add exact endpoint multiset and full ordered-path validation.
3. Add suffix rows, distinct boundaries, recursive states, edge probes, and
   hydration loops to the plan/result schema.
4. Version the ADCS fixture configuration so reachable suffixes, disconnected
   boundaries, fan-in, multiplicity, and output count vary independently.
5. Implement benchmark-only A1a, A1b, and A2 at the ordered-ID and
   complete-result boundaries.
6. Implement benchmark-only A3 with `UNION ALL`, prepended member edge IDs,
   cross-segment uniqueness, zero-depth seeds, and exact suffix rejoin.
7. Implement A4 only as a permissive viability filter plus exact forward
   enumeration.
8. Run the balanced discovery tournament and freeze the candidate/selector
   predeclaration.
9. Add the typed optimizer strategy decision while still selecting the
   incumbent.
10. Accept A1a/A1b independently into the candidate when material; enable R4
    there only if its whole eligible envelope is safe, and otherwise retain it
    as a benchmark candidate.
11. Qualify R5 behind the incumbent, then add and prove R6 bounded
    selection/fallback before candidate activation; require R7 before rollout.
12. Reprofile before opening frontier, statistics, index, helper, cache, or
    native work.

Do not begin with `work_mem`, JIT, global planner settings, a new edge index,
translation caching, classic BFS, or a closure table.

## Definition of done

This continuation is complete when:

- the clean entering artifact and accepted candidate are durable and
  reconstructible;
- every generated ADCS target has a correct competitive PostgreSQL reference;
- at least 90% of incumbent ADCS execution time **and** shared-hit work is
  attributed using non-overlapping regions or controlled deltas;
- sparse D16/F1000 no longer performs work proportional to all 16,001 forward
  states unless an explicit tested fallback selects that path;
- endpoint and full-path forms are exact across the semantic matrix;
- one row is preserved per variable trail, suffix trail, and root-source bag
  combination;
- relationship uniqueness and ordered path identity are exact across forward,
  reverse, and fallback strategies;
- missing root, intermediate, boundary, and fixed-suffix nodes never become
  matched through dangling relationships;
- root, boundary, and full-path hydration occur only after their last required
  qualification stage;
- missing-root queries execute neither suffix production nor recursion;
- compile-time planned/applied/skipped decisions and runtime plan-derived
  selected/fallback outcomes are separately observable with stable provenance;
- dense, false-boundary, and overflow regimes are bounded and complete;
- no normal-tier temp spill, local workspace, memory-ceiling failure, or
  session-state leak occurs;
- PostgreSQL and Neo4j integration suites, translation/template/mutation
  coverage, race tests, cancellation, rollback, concurrency, and complete
  performance gates pass;
- selected candidate SQL at the raw-pgx boundary is within `1.10` of the best
  correct reference SQL at that same boundary, or its remaining gap is below
  absolute resolution, while production CySQL also clears its predecessor
  end-to-end gate;
- the live PostgreSQL/Neo4j comparison is rerun and reported without using the
  Neo4j ratio as the acceptance rule;
- rejected production experiments are removed and their evidence retained;
- remaining work is ranked by absolute addressable cost and either opened as a
  new bounded continuation or explicitly closed.

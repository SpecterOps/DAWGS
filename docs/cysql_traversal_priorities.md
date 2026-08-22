# CySQL traversal performance priorities

Date: 2026-08-12

Status: implementation complete; promotion evidence pending

The code and current activation disposition are recorded in
[`experiments/traversal_priority_implementation_status_v1.md`](experiments/traversal_priority_implementation_status_v1.md).
New production promotion remains evidence-gated as specified below.

This plan turns the fresh CySQL/PostgreSQL versus Cypher/Neo4j benchmark and
source review into an implementation and qualification program. It focuses on
ordinary variable-length traversal orientation, bound `shortestPath` (SP),
`allShortestPaths` (ASP), and fixed one-hop `ExpandInto` behavior.

The principal decision is to build one exact, observable traversal-selection
framework rather than add another isolated lowering. The first production
targets are a topology-aware forward/reverse orientation tournament and compact
bidirectional SP candidates. Bidirectional ASP follows after the shared search
kernel and telemetry are qualified. Fixed one-hop `ExpandInto` is a narrow,
measure-first opportunity. A persistent topology synopsis is deferred until
runtime probes prove that its maintenance and cache complexity are warranted.

## Executive priority order

Engineering effort should proceed in this order:

| Priority | Work | Reason |
| --- | --- | --- |
| P0 | Shared telemetry, matched plan deltas, and frozen qualification corpus | Current PostgreSQL function scans hide traversal work, while Neo4j 4.4 SP/ASP profiles do not count internal relationship traversal. Selector work is not explainable or safely promotable without independent counters. |
| P1 | General ordinary-expansion orientation tournament | The measured fixed-suffix crossover is the largest ordinary-traversal opportunity: reverse is dramatically better on sparse terminal topology and materially worse under high reverse fan-in. |
| P2 | Compact SP architecture and scheduler tournament | Current S4 witness and deep/inbound execution is the main SP loss, while exact inline references show that the gap is not inherent to PostgreSQL storage. |
| P3 | Compact bidirectional ASP predecessor DAG | Recursive ASP is materially slower than Neo4j and currently lacks independent predecessor/output gates. It should reuse the proven SP search and telemetry foundation. |
| P4 | Bounded endpoint resolution and step-local predicate support | The current singleton-ID envelope excludes unique property seeks, small endpoint sets, and safe universal predicates that Neo4j can prepare before traversal. |
| P5 | Fixed one-hop `ExpandInto` endpoint choice and pair reuse | Neo4j's lower-degree scan and pair cache are useful hypotheses, but PostgreSQL may already choose an efficient plan for the current bound-pair join, including a parameterized index lookup or `Memoize`; this must be measured before adding probes. |
| P6 | Optional versioned topology synopsis | Persistent estimates may reduce probe cost, but they are advisory, mutation-sensitive, and absent from the current translation-cache identity. Runtime evidence comes first. |

This is the engineering-priority order, not necessarily the automatic-promotion
order. A semantically narrow fixed-hop candidate may graduate before a recursive
candidate if it independently passes every gate. Orientation and SP reference
work can proceed in parallel after P0. ASP depends on the common bidirectional
state model and its counters.

## Outcomes and success measures

The program should deliver:

1. Exact runtime selection between forward and reverse ordinary expansions for
   qualified shapes, with a same-statement forward incumbent on uncertainty or
   overflow.
2. Exact SP comparison among the current single-ended compact executor,
   Neo4j-4.4-style strict per-node alternation, and current-Neo4j-style
   smaller-current-level expansion.
3. Exact ASP comparison among the current single-ended predecessor DAG and two
   bidirectional predecessor-DAG schedulers, with independent discovery,
   predecessor, and output-enumeration gates.
4. Executor-reported work metrics that explain a choice in terms of seeds,
   directional degree, frontier growth, edge scans, reconvergence,
   predecessor multiplicity, meeting width, fallback, and hydration.
5. Matched PostgreSQL/Neo4j plan-delta reports that identify starting side,
   physical direction, predicate placement, estimate error, and traversal
   setup without treating unlike backend operator counters as equivalent.
6. Versioned selectors, reference identities, negative-result records, and a
   reversible rollout path.

Promotion is not defined as "beat Neo4j everywhere." Neo4j is an exact-result
and descriptive latency oracle. A CySQL candidate is promoted only when it is
exact, beats or contains its PostgreSQL incumbent on predeclared topology
buckets, and stays within resource and operational limits.

For tied singleton SP, "exact" means the same minimum distance and one valid
minimum relationship-unique witness, not the same arbitrary witness as Neo4j
or another CySQL executor. ASP and bag-valued ordinary traversals require their
complete logical result multisets.

## Scope and explicit non-goals

The initial scope is read-only, directed, bounded traversal with a single
variable region or one statically proven endpoint pair. It includes the current
endpoint-seeded and three-hop fixed-suffix envelopes, singleton bound SP/ASP,
and fixed one-hop `ExpandInto`.

The first program does not:

- implement a general IDP query-graph solver or reorder arbitrary Cypher
  components;
- infer correctness from planner estimates or make mutable statistics a
  translation-time dependency;
- change trail, bag, tie, optional-match, mutation, or predicate semantics;
- make legacy full-trail bidirectional harnesses production candidates;
- revive the retired suffix keyset-continuation design;
- force one SP/ASP scheduler across every topology or observation mode;
- use Neo4j latency or opaque 4.4 `ShortestPath` DB hits as a CySQL release
  threshold.

## Baseline evidence to freeze

The 2026-08-12 discovery capture used PostgreSQL 17.10 and Neo4j 4.4.44. It
contained two backend-order-balanced rounds, ten warmups, and thirty measured
samples per round. These results motivate the work, but they are not a release
gate and must be recaptured as milestone M0.

| Shape | Discovery result | Planning implication |
| --- | --- | --- |
| Bounded outbound SP distance | CySQL S3 was about 6-30x faster | Preserve S3 as a real tournament arm; do not replace it globally. |
| SP witness and deep physical-inbound search | Neo4j was about 5-16x faster | Tournament both execution boundary and bidirectional scheduler. |
| Recursive ASP at depths 3 and 16 | Neo4j was about 5.7-13.1x faster | Shallow two-hop fixtures are insufficient; exercise the predecessor workspace. |
| Sparse fixed suffix | Neo4j was about 51x faster than production CySQL | General orientation selection has high expected value. |
| Forced CySQL suffix reverse on that sparse case | About 460x faster than forward endpoint-ID output | The reverse implementation is viable when topology is favorable. |
| High reverse fan-in | CySQL forward was about 3.7-4.4x faster than Neo4j; forced reverse was about 3.4x slower than forward | Static "always reverse" is unsafe as a performance policy. |
| Exact inline PostgreSQL references | About 2.5-45.7x faster than corresponding compact production functions on selected cases | Function/workspace overhead and algorithm must be separated in the tournament. |

The source capture, raw benchmark records, and local review currently live
under `.coverage/fresh-plan-delta-20260812`. M0 must create a checksummed capture
bundle and commit only compact, credential-free decision records; raw
environment-specific artifacts remain ignored.

## Neo4j lessons to use deliberately

The primary source target is the measured Neo4j 4.4.44 tag at commit
[`17d7609`](https://github.com/neo4j/neo4j/tree/17d7609361109bd9b08ea149a5ed5966f1115324).
Current upstream behavior is pinned separately to the reviewed 2026.06 commit
[`eccd584`](https://github.com/neo4j/neo4j/tree/eccd584a64d468af3daeab421478fe78567c518f).
Current behavior must not be projected backward onto the measured server.

The source review establishes these design inputs:

- Ordinary relationship planning creates candidates from both endpoints and
  lets bounded IDP retain the cheapest orientation. The suffix-first benchmark
  plan is a general enumeration result, not a special suffix rule. See
  [`SingleComponentPlanner`](https://github.com/neo4j/neo4j/blob/17d7609361109bd9b08ea149a5ed5966f1115324/community/cypher/cypher-planner/src/main/scala/org/neo4j/cypher/internal/compiler/planner/logical/idp/SingleComponentPlanner.scala#L215-L244).
- Neo4j 4.4 statistics contain global node, label, relationship-step, and index
  selectivity values, but no endpoint-local degree, frontier survival,
  reconvergence, meeting-cut width, or predecessor/output multiplicity. See
  [`GraphStatistics`](https://github.com/neo4j/neo4j/blob/17d7609361109bd9b08ea149a5ed5966f1115324/community/cypher/planner-spi/src/main/scala/org/neo4j/cypher/internal/planner/spi/GraphStatistics.scala#L27-L66).
- Generic `VarLengthExpand(All/Into)` is a single-ended stack-based DFS in its
  planned orientation. `Into` checks the bound target when emitting; it does
  not become target-directed or bidirectional. See
  [`VarLengthExpandPipe`](https://github.com/neo4j/neo4j/blob/17d7609361109bd9b08ea149a5ed5966f1115324/community/cypher/interpreted-runtime/src/main/scala/org/neo4j/cypher/internal/runtime/interpreted/pipes/VarLengthExpandPipe.scala#L50-L135).
- Fixed one-hop `ExpandInto` is different: Neo4j can scan the lower-degree
  endpoint and cache a node-pair result. See
  [`CachingExpandInto`](https://github.com/neo4j/neo4j/blob/17d7609361109bd9b08ea149a5ed5966f1115324/community/cypher/runtime-util/src/main/java/org/neo4j/internal/kernel/api/helpers/CachingExpandInto.java#L139-L207).
- Bound SP/ASP is attached only after both endpoints are available. Neo4j
  4.4's specialized bidirectional BFS alternates one newly discovered node per
  side and retains same-depth predecessor relationships. See
  [`ShortestPath`](https://github.com/neo4j/neo4j/blob/17d7609361109bd9b08ea149a5ed5966f1115324/community/graph-algo/src/main/java/org/neo4j/graphalgo/impl/path/ShortestPath.java#L207-L343).
- Current Neo4j expands a complete level from the side with the smaller current
  level, a materially different scheduler. See
  [`BiDirectionalBFSImpl`](https://github.com/neo4j/neo4j/blob/eccd584a64d468af3daeab421478fe78567c518f/community/cypher/runtime-util/src/main/java/org/neo4j/internal/kernel/api/helpers/traversal/BiDirectionalBFSImpl.java#L167-L195).
- Neo4j 4.4's Cypher profiler does not expose internal SP relationship reads.
  Raw `ShortestPath` DB-hit counts must be marked opaque, not compared to
  PostgreSQL recursive rows or edge probes.

The plan adopts orientation enumeration, endpoint binding, bidirectional BFS,
frontier-aware scheduling, and two-sided predecessor reconstruction as
candidate ideas. It does not adopt Neo4j's global-average cost blindness,
opaque SP/ASP telemetry, or generic DFS behavior as CySQL requirements.

## Architecture and decision boundaries

The target decision flow is:

```text
Cypher shape analysis
        |
        v
exact candidate envelope + observation classification
        |
        +---------------- compile-time diagnostics ----------------+
        |                                                          |
        v                                                          v
same-statement capped probes or executor frontier state      plan-delta record
        |
        v
versioned runtime policy
        |
        +---------+-----------+------------------+
        |         |           |                  |
        v         v           v                  v
forward/reverse   SP arm      ASP arm       fixed-hop arm
        |         |           |                  |
        +---------+-----------+------------------+
                          |
                          v
             exact gated output or incumbent fallback
                          |
                          v
              late hydration + runtime telemetry
```

Compile-time facts and runtime facts must remain distinct:

- The optimizer records the correctness envelope, candidates, observation
  mode, selector version, caps, and fallback policy.
- The emitted SQL or executor records probes performed, scheduler decisions,
  runtime arm, work, overflow, and fallback actually executed.
- GraphBench must not claim that a compile-time candidate ran merely because it
  was planned or emitted.
- Tool forcing may choose among structurally eligible candidates; it may never
  broaden their correctness envelope.

The current translation cache is keyed by normalized query text, graph ID, and
parameter-name/type shape. Mutable parameter values or graph statistics must
therefore be consulted inside the generated statement. If a future selector
embeds a synopsis value at translation time, a statistics generation and
invalidation contract must first be added to the cache key.

Mutable rollout policy is subject to the same rule. Feature-gate state,
selector version, and caps are not in the current cache key. A policy that can
change during a driver's lifetime must be supplied at execution time, add an
explicit cache generation, or invalidate affected translations. Otherwise a
rollback can leave cached tournament SQL active. Immutable caps may be SQL
literals; planner-created SQL parameters without `ParameterSources` currently
make a translation non-cacheable and need explicit rebinding/cache support if
that behavior is not desired.

## Non-negotiable semantic contract

Every candidate, probe, and fallback must preserve:

- graph partition and resolved relationship-kind filtering;
- logical direction and the correct physical adjacency index;
- inclusive minimum and maximum depth, including qualified zero-length paths;
- relationship-trail uniqueness while permitting repeated nodes where Cypher
  permits them;
- ordered relationship and node IDs in logical source-to-target order;
- prefix/suffix relationship non-reuse across stitched path regions;
- duplicate root rows, endpoint rows, suffix rows, and output bag
  multiplicity;
- SP's one arbitrary valid minimum trail and ASP's complete set of
  relationship-distinct minimum trails;
- predicate null behavior, locality, determinism, and evaluation count;
- optional-match and mutation visibility rules;
- one top-level SQL statement for probes, candidate, and fallback, plus an
  explicit snapshot contract. SQL-only CTE arms share a statement snapshot;
  `VOLATILE` PL/pgSQL internal statements under `READ COMMITTED` must not be
  assumed to do so. Function-backed candidates require a deliberate mechanism
  such as repeatable-read execution, or an independently proven equivalent,
  before claiming snapshot-stable fallback;
- no candidate row exposure until every fallback-triggering gate has passed;
- prompt cancellation, rollback recovery, and clean reuse of a pooled session.

The singleton SP tie policy remains the contract in
[`shortest_path_tie_policy.md`](shortest_path_tie_policy.md). Physical edge ID
or insertion order is not public. ASP may not use the singleton tie policy to
discard equal-depth predecessors.

The PostgreSQL schema currently has a unique
`(start_id, end_id, kind_id, graph_id)` relationship constraint. Same-kind
parallel physical relationships cannot be represented in the current backend.
Cross-kind parallel relationships must be covered now; same-kind parallel-edge
parity remains an explicit storage boundary, not a silently skipped test.

## Workstream 0: observability and matched plan deltas

This is the prerequisite for every selector change.

### 0.1 PostgreSQL executor telemetry

Add a versioned `TraversalExecutionTelemetry` schema to GraphBench records and
PostgreSQL full-comparator records. Preserve `PostgresPlanMetrics` for measured
plan facts, but do not infer hidden PL/pgSQL work from a `Function Scan` loop.

Use two telemetry levels:

- A lightweight summary: requested/planned/emitted/runtime/applied identity,
  selector and scheduler version, caps, runtime branch, overflow, and fallback.
- A tool-only diagnostic replay on the same connection: per-level and
  per-stage executor counters. It runs outside the timed sample block so
  detailed instrumentation does not contaminate latency evidence.

Replay counters describe that untimed invocation, not a particular timed
sample. Store them in a separate diagnostic boundary and do not combine their
resource values with the production timing record.

Missing required telemetry is a qualification failure, not a zero value. Every
derived field carries provenance naming the function, CTE, or executor metric
that produced it.

Record at minimum:

| Family | Required runtime counters |
| --- | --- |
| Ordinary DFS/recursive CTE | roots, edge candidates, admitted states, relationship-repeat rejects, recursive rows, peak state, emitted trails, hydration rows |
| Orientation policy | forward/reverse seeds, duplicate seeds, suffix rows, distinct boundaries, typed directional degree samples, shallow survival, probe rows/time/buffers, scores, selected side, sentinel overflow, branch loops |
| SP | scheduler actions, per-side depth/frontier, candidate edges, distinct new nodes, seen/frontier/queue peaks, meeting candidates, frozen distance, witness rows, fallback |
| ASP | SP counters plus same-depth predecessor additions, predecessor peak, meeting nodes, cut depth, saturating path-count estimate, enumerated candidates, duplicate rejects, output paths/edge cells/bytes |
| Hydration | path count, node/edge lookups, loops, rows, time, and bytes separately from discovery |

Candidate workspace metrics must be invocation-keyed and session-local so
concurrent pooled sessions cannot collide. Cancellation and SQL errors
propagate; they are not converted into performance fallbacks.

### 0.2 Neo4j read profiling

Extend GraphBench to run a read-only `PROFILE` pass after the timed block while
retaining `EXPLAIN` for writes. Persist:

- planner and runtime version;
- ordered operator tree and child order;
- estimated and actual rows, loops, DB hits, page-cache hits/misses, and
  operator time where the server exposes them;
- leaf variables, access predicates, expansion direction, and starting side;
- an explicit `internal_traversal_work=opaque` marker for 4.4 SP/ASP.

Normalize the current doubled `@neo4j` operator suffix and verify endpoint-child
fidelity. Neo4j profile data remains descriptive and must not become a CySQL
release gate.

### 0.3 Paired PlanCorpus record

Add a versioned PostgreSQL/Neo4j plan-delta record keyed by dataset, case,
workload hash, source revision, and backend plan fingerprints. It should
compare semantic stages rather than raw operator names:

- starting and terminal access;
- logical and physical traversal direction;
- predicate placement and endpoint binding;
- ordinary expand versus SP/ASP operator family;
- estimated seeds, traversal multiplier/frontier, output, and Q-error;
- PostgreSQL planned/emitted/runtime/fallback identities;
- whether Neo4j reordered the pattern and whether the chosen side did less
  observed work.

Rank opposite-side choices, largest estimate disagreements, predicate moves,
fallback/cap cases, and hydration deltas. Incomplete pairs must be explicit;
they must not disappear through intersection-only reporting. PlanCorpus remains
the plan inventory and GraphBench remains the runtime authority.

## Workstream 1: ordinary traversal orientation tournament

The strategy should be general in framework and deliberately narrow at first
activation.

### 1.1 Candidate model

Introduce runtime policy identity `orientation-probe-v1`. Keep executed arm
identities separate:

- `EXPANSION-STEPWISE-FORWARD` is the permanent exact incumbent.
- `EXPANSION-SUFFIX-SEEDED-REVERSE` is the exact fixed-suffix reverse arm.
- `EXPANSION-ENDPOINT-SEEDED-REVERSE` remains the exact terminal-seeded arm.
- factored-forward and backward-viability arms remain references until they
  independently qualify.

Do not overload compile-time `SelectedStrategy` to imply a runtime choice. Add
emitted-policy, probe-cap, admission, and candidate fields to the typed
`ExpansionSearchStrategyDecision` and translation outcome. Record the actual
arm, probe results, overflow, and fallback only in execution/GraphBench
telemetry; translation cannot know them, and a translation-cache hit does not
reconstruct a fresh runtime outcome.

Initial eligibility remains conservative:

- one read-only, non-optional ordinary pattern region;
- one directed, bounded variable expansion with maximum depth at most 64;
- a bound/safely materializable seed region on each considered side;
- no relationship variable or relationship/path-dependent predicate;
- no cross-region correlation or limit-pushdown conflict;
- endpoint-ID, ordered-ID, or full-path observation with proven projection
  alignment.

The first suffix activation must reproduce the current envelope exactly: a
bound root; one outbound, single-kind variable expansion; exactly three
outbound, single-kind fixed suffix hops; exactly one right-node kind on every
suffix hop; and the existing dependency, observation, and no-function-call
restrictions. Endpoint-seeded migration likewise preserves its current
identity-function exception and all other restrictions. "Deterministic" is not
enough to broaden expression eligibility because repeated probing can change
evaluation count and exception behavior. Other predicates or contiguous fixed
regions wait for the predicate-class workstream and their own decision record.

### 1.2 Same-statement probe and branch design

Emit one statement containing:

1. A capped forward-root materialization.
2. A capped reverse seed materialization: terminal endpoints or exact suffix
   rows plus distinct boundary nodes.
3. Capped typed directional-degree probes using the existing covering
   `(start_id, kind_id)` and `(end_id, kind_id)` indexes.
4. An optional, statically enabled one-level survival probe with an explicit
   row/edge cap; its cost envelope is qualified offline.
5. A versioned score and hysteresis decision CTE.
6. A reverse-state admission relation capped at `state_limit + 1`.
7. Strictly disjoint reverse and forward-incumbent branches.

Every cap uses a `cap + 1` sentinel. Probe relations must actually contain an
explicit bound. The existing unused `buildFixedSuffixProbeCTE` helper is not
currently limited despite its comment; bounding or replacing it is a
prerequisite, not evidence that suffix probing is already safe.

Capped relations are evidence, not automatically exact query inputs. Keep an
uncapped exact source for the incumbent. A candidate may consume a capped root,
endpoint, or suffix relation only after its sentinel proves that the relation
is complete; overflow must not feed truncated rows to either arm. If a complete
probe relation is reused to avoid duplicate work, tests must prove that it
retains the exact duplicate and suffix-bag multiplicity required by that arm.

Record:

- distinct and duplicate roots;
- reverse seed rows and distinct seed nodes;
- suffix row multiplicity and distinct boundary count;
- first-hop typed adjacency rows, maximum sampled degree, and a high percentile
  when the seed set is small;
- one-level admitted-next-node ratio;
- reverse states consumed before admission;
- total probe latency and buffers.

Latency and buffers are post-execution telemetry used to qualify the policy;
plain CTE SQL cannot observe them in time to choose a branch within that same
statement.

The initial policy is dominance-based, not a fragile learned formula:

- choose reverse only when required probes are complete below their caps and
  its versioned score beats forward by a qualified hysteresis margin;
- choose forward on overflow, missing evidence, ties, or ambiguous
  correlation;
- if reverse-state admission crosses its sentinel, discard all candidate state
  and run the exact forward incumbent before returning a row.

Thresholds are derived from predeclared GraphBench training buckets and frozen
before the holdout is opened. Parameter values and topology stay runtime inputs,
so cached SQL remains safe.

### 1.3 Implementation sequence

1. Refactor fixed-prefix and fixed-suffix analysis in
   `cypher/models/pgsql/optimize/lowering_plan.go` into a common contiguous
   orientation-candidate analyzer while retaining specific fallback reasons.
2. Extend typed decisions in `cypher/models/pgsql/optimize/lowering.go` and
   outcomes in `cypher/models/pgsql/translate/translator.go`.
3. Add `cypher/models/pgsql/translate/expansion_orientation.go` and extract
   reusable seed, reverse recursion, projection alignment, overflow, and
   incumbent-gating helpers from `expansion_endpoint_seeded.go` and
   `expansion_suffix_seeded.go`.
4. Emit the incumbent first, then wrap it with probes and disjoint gates in
   `pattern.go`. Distinguish tournament emission from runtime arm execution.
5. Migrate endpoint-seeded reverse to the common framework without changing
   its current 32-endpoint/4096-state behavior.
6. Add guarded suffix reverse; keep the existing force seams as independent
   A/B controls.
7. Run shadow selection before changing production. The shadow can compute
   `would_select` while executing the incumbent; regret comes from separate
   matched GraphBench runs that execute the exact forced arms.

The retired keyset-continuation experiment is not a candidate. Its confirmed
negative result remains authoritative unless a materially different design is
given a new identity and hypothesis.

## Workstream 2: compact SP scheduler tournament

SP must tournament algorithm, scheduler, and execution boundary. Current
production winners remain controls:

- `SP-S3-U-D` for qualified outbound distance and shallow physical-inbound
  distance;
- `SP-S4-C-D` for qualified deep physical-inbound distance;
- `SP-S4-C-WE+MAT-M0` for qualified one-path witnesses;
- `SP-S0` as the exact broad-envelope incumbent.

The specialized SP envelope requires an explicit bounded maximum depth at most
64. The current ASP envelope differs: an omitted maximum is admitted as depth
15, while minimum depth must be one for `ASP-A1-DAG`. Preserve those distinctions
in candidate eligibility, comparator choice, and serialized decisions.

Reserve stable candidate identities before capture:

| Candidate | Scheduler ID | Observation | Reference arm |
| --- | --- | --- | --- |
| `SP-B1-C-ALT-NODE-D` | `strict_alternating_node` | distance | `sp_b1_strict_alternating_distance` |
| `SP-B1-C-ALT-NODE-WE+MAT-M0` | `strict_alternating_node` | one witness | `sp_b1_strict_alternating_witness_m0` |
| `SP-B2-C-MIN-LEVEL-D` | `smaller_current_level` | distance | `sp_b2_smaller_frontier_distance` |
| `SP-B2-C-MIN-LEVEL-WE+MAT-M0` | `smaller_current_level` | one witness | `sp_b2_smaller_frontier_witness_m0` |

Add a typed scheduler field to `ShortestPathExecutorDecision`; scheduler
behavior must not be inferred from a display name. Freeze
`single_ended_level` for S3/S4/A1 as well as the two candidate scheduler values
before the first artifact.

### 2.1 Shared compact kernel

Prototype a typed, graph-scoped bound-pair kernel with distinct forward and
backward structures:

- node/depth frontier and next-front state;
- minimum-depth seen state per side;
- one deterministic predecessor/successor per accepted node for SP witness;
- per-node FIFO queue state for strict alternation;
- invocation telemetry and independently versioned limits.

Keep relationship and node IDs only until one late hydration boundary. Preserve
logical source-to-target relationship order even when physical search begins at
the target. Outbound logical search uses `start_id -> end_id` forward and
`end_id -> start_id` backward; inbound search reverses those accesses.

The legacy `bidirectional_sp_harness` already contains smaller-frontier control
logic, but it retains full path arrays, executes generated SQL text, and uses
generic pathspace tables. Reuse its control-flow lessons only. Do not promote or
rename it as a compact candidate.

Strict alternation must dequeue one accepted node from each side in turn;
alternating whole SQL levels is a different scheduler. Smaller-frontier must
expand a complete level and use a deterministic tie break. Both schedulers need
a documented lower-bound termination proof: do not stop merely at the first
intersection, and complete enough depth on both sides to prove that no shorter
path remains.

Retain exact zero-, one-, and two-hop arms before workspace allocation. Their
latency is a setup control, not evidence that distinguishes recursive
schedulers.

### 2.2 Architecture boundary tournament

The discovery references show that inline recursive SQL can be much faster than
the current session-workspace functions. Therefore:

- retain exact inline S3/S4/ASP full comparators;
- implement compact bidirectional references with explicit internal counters;
- compare a typed function/workspace boundary to the smallest viable inline or
  SQL-visible boundary where the scheduler permits it;
- attribute search, workspace reset, predecessor reconstruction, and hydration
  separately.

Do not select a scheduler based on a comparison that also changes hydration or
public observation. Each pair must share the same output boundary.

### 2.3 Gates and fallback

SP admission gates are separate counters:

- total distinct seen nodes across both sides;
- current/next frontier or queue rows;
- retained witness-predecessor rows;
- optionally bounded meeting candidates.

No recursive result is emitted until all gates pass. Overflow invokes
the production incumbent for the candidate's bucket in the same top-level
statement: S3 for S3 distance buckets, and S4 for deep-inbound distance or
witness buckets. Alternatively, restrict the first B1/B2 production activation
to S4 buckets. Candidate workspace names must be distinct from the current
`spd_*` workspace so nested fallback cannot corrupt state. Record the complete
fallback chain when S4 invokes its relationship-trail fallback, and establish
the function snapshot contract described above before calling the chain
snapshot-stable.

After confirmation, a new `sp-static-v5` may select candidates only for the
topology and observation buckets that pass. A global scheduler winner is not
required: S3 or S4 may remain best for shallow or selective shapes.

Before shadow or production use, define a versioned mapping from facts available
to the real query—query shape, observation, physical direction, depth, bounded
endpoint/degree probes, or executor frontier state—to each selectable topology
bucket. Fixture metadata and post-run telemetry label evaluation strata; they
cannot drive production selection. If a bucket cannot be recognized from
runtime inputs, it remains a diagnostic classification.

## Workstream 3: bidirectional ASP predecessor DAG

ASP begins only after the shared bidirectional search kernel, termination proof,
and SP telemetry pass qualification.

Reserve:

| Candidate | Scheduler ID | Reference arm |
| --- | --- | --- |
| `ASP-B1-DAG-ALT-NODE` | `strict_alternating_node` | `asp_b1_bidirectional_dag_strict_m0` |
| `ASP-B2-DAG-MIN-LEVEL` | `smaller_current_level` | `asp_b2_bidirectional_dag_smaller_frontier_m0` |

The current `ASP-A1-DAG` remains the single-ended exact production control.
The legacy `bidirectional_asp_harness` carries complete trails and is not the
new candidate.

### 3.1 State and reconstruction

Each side retains:

- minimum reached depth per node;
- every relationship-distinct predecessor or successor that reaches that node
  at the same minimum depth;
- frontier state and scheduler order independently from predecessor state.

When minimum distance `L` is proven, select one deterministic completed meeting
cut `k`. Enumerate source predecessor paths to nodes at depth `k`, target
successor paths from the same nodes at depth `L-k`, and stitch ordered edge ID
arrays. Using one cut ensures that a complete path is not emitted once per
overlap level. For the initial singleton pair, uniquely stage ordered
`edge_ids` and assert relationship uniqueness before public output. Endpoint
broadening must key uniqueness by input-pair identity plus `edge_ids`, then
reapply duplicate input-pair multiplicity; otherwise repeated endpoint rows
would be collapsed.

Within the initial distinct-endpoint, minimum-depth-one envelope, an unweighted
minimum path cannot repeat a node because removing the intervening cycle would
make it shorter. This justifies minimum-node-depth discovery for this envelope
only. It does not justify directionless traversal, positive-minimum self cycles,
whole-path predicates, or broader trail semantics.

### 3.2 Independent resource gates

ASP has three different explosion modes and therefore three limits:

1. Discovery: distinct seen/frontier nodes.
2. Predecessors: same-minimum-depth relationship-distinct predecessor rows.
3. Enumeration: distinct ordered edge arrays and materialized bytes.

Before enumeration, calculate a saturating path-count bound over the predecessor
DAG. Stage output under `limit + 1` sentinels. Any overflow clears candidate
state and invokes `all_shortest_paths_dag` before exposing a row. This fallback
uses the same top-level statement, but still requires the deliberate function
snapshot contract before it can be described as one-snapshot execution.

These are candidate-admission guards, not public result limits. ASP may never
silently truncate a required path set. If the exact incumbent itself cannot
complete within an external statement/resource policy, propagate that error;
do not relabel truncation as fallback success.

After independent confirmation, `asp-static-v2` may select a qualified
bidirectional arm. If enumeration dominates total latency or no candidate
contains predecessor/output risk, retain A1 and record the new arm as a frozen
negative result.

## Workstream 4: endpoints and predicate classes

The first SP/ASP candidates retain the current one-literal-ID-per-endpoint
envelope. Broaden only after their core algorithms are stable.

### 4.1 Bounded endpoint resolution

Materialize endpoint resolution once with explicit 1/2/32/33 sentinels and exact
fallback. Qualify independently:

- ID equality;
- unique indexed property equality;
- nonunique property equality that returns a small bounded set;
- explicitly supplied small endpoint sets;
- endpoint pairs whose correlation must be preserved rather than treated as a
  Cartesian product.

Record input rows, distinct endpoint IDs, duplicate multiplicity, pair count,
resolution plan/index, and overflow. Endpoint cardinality is runtime evidence;
predicate syntax alone is not selectivity proof.

Keep the compact bidirectional ASP kernel singleton-only until a wrapper assigns
stable input-pair identities, deduplicates paths within each pair, and reapplies
duplicate pair-row multiplicity. Endpoint broadening must not make global
`edge_ids` uniqueness collapse the Cypher result bag.

### 4.2 Predicate classification

Add an explicit classifier for:

- step-local node predicates;
- step-local relationship predicates;
- universal `ALL`/`NONE` predicates over path nodes or relationships that can
  be evaluated on each expansion step;
- whole-path predicates requiring a complete materialized candidate.

Only step-local or proven universal predicates may enter the compact expander.
Whole-path predicates retain an exact fallback-capable exhaustive plan. Each
predicate class needs mutation and translation fixtures because placement can
change evaluation and output semantics.

## Workstream 5: fixed one-hop `ExpandInto`

This work applies only when both endpoints of a fixed, one-hop relationship are
bound. It must not be generalized to variable-length `Into`.

Start with a three-way plan study:

1. Current bound-endpoint edge join, recording the plan PostgreSQL actually
   chooses (for example, parameterized index lookup, hash join, or another
   shape).
2. Typed lower-degree endpoint probe followed by adjacency scan and opposite
   endpoint check.
3. The bound-pair join plus PostgreSQL `Memoize` or an explicit
   statement-local distinct-pair cache for repeated input pairs.

Measure wildcard and multi-kind cases separately. An actual parameterized pair
index plan may make lower-degree probing redundant for singleton typed pairs,
while pair reuse may matter only with duplicate outer rows. Add policy metadata
to the currently marker-only `ExpandIntoDecision` only if a candidate
demonstrates a real crossover.

Pair caching stores or reproduces all matching relationship rows, not only a
connectivity boolean. It must preserve relationship IDs/properties, one-per-kind
multiplicity, wildcard/multi-kind and directionless behavior, self-loops, and
duplicate outer-row multiplicity even when it deduplicates lookup work. Qualify
cache hit/miss, missing endpoints, cross-kind parallel relationships,
cancellation, and generic/custom plans.

## Workstream 6: statistics and probe roadmap

Runtime capped probes are the first authority because they use the current
parameters and graph contents in the executing statement. Function-backed
search and fallback remain subject to the explicit snapshot contract above.
The useful evidence is:

| Evidence | Primary use |
| --- | --- |
| Root/terminal endpoint rows and distinct IDs | Bound pair count and seed cost |
| Typed directional degree at each endpoint | First-step orientation and frontier risk |
| Suffix rows, distinct boundaries, and path multiplicity | Reverse seed and reconstruction cost |
| One-level survival and distinct-next ratio | Predicate selectivity and reconvergence hint |
| Per-level frontier and candidate edges | Adaptive SP/ASP scheduler choice |
| Seen-to-frontier and candidate-to-new-node ratios | Cycle/reconvergence cost |
| Same-depth predecessor additions | ASP predecessor memory risk |
| Meeting-node count and cut width | Bidirectional reconstruction cost |
| Saturating returned-path count and edge cells | ASP output/hydration risk |

An optional synopsis is a later optimization, never a correctness proof. A
versioned synopsis may contain:

- node counts by graph and kind;
- relationship counts by graph, direction, kind, and endpoint kind;
- distinct start/end counts and most-common endpoints;
- directional degree quantiles and heavy hitters;
- observed frontier survival/reconvergence buckets by depth;
- predecessor and output multiplicity buckets for qualified generated shapes.

Node multi-kind membership makes endpoint-kind estimates overlapping rather
than additive. Sampling, refresh cadence, mutation overhead, stale-data
behavior, and graph drop/reload handling require an explicit design record. The
runtime guard remains authoritative. Prefer reading a synopsis at execution
time; embedding it in translated SQL requires a synopsis epoch in
`cypherTranslationCacheKey` and mutation-safe invalidation.

## Qualification corpus

Preserve the scale corpus's `normal`, `envelope`, and `stress` tiers. Gate normal
and envelope; use stress for exact fallback and failure-mode diagnosis. Expand
the existing deterministic generators before adding a new generator family.

| Area | Required axes |
| --- | --- |
| Orientation | root and terminal seeds `0/1/2/32/33/128/512/513`; independent forward/reverse typed degree `0/1/4/32/128/1000/16000`; productive fraction `0/sparse/half/all`; mirrored fan-out/fan-in; hidden spike at first/middle/final depth |
| Common traversal | depth `0/1/2/4/8/16/32/64`; outbound/inbound/directionless; one/multiple kinds; fixed prefix/suffix `0/1/3`; disconnected decoys; cycles; self-loops; convergence; payload |
| SP | direct and two-hop controls; highly asymmetric endpoints; alternating-frontier crossovers; shallow target plus huge continuation; disconnected exhaustion; intermediate skew; one/equal witnesses; distance and path observations |
| ASP | depths `3/8/16`; diamond width and path count `1/2/16/128+`; same node count with different predecessor density; multiple meeting nodes; merge-then-split DAG; modest state with explosive output; large predecessor state with modest output |
| `ExpandInto` | asymmetric degrees; typed/wildcard/multi-kind; missing endpoints; self-loop; repeated pair hit/miss; duplicate outer rows |
| Endpoints/predicates | ID, unique property, nonunique property, small sets; local node/edge universal and whole-path predicates |
| Limits | every probe/state/predecessor/output cap at `N-1/N/N+1`, including current `32/33` and `4096/4097` boundaries |
| Output | scalar/count, endpoint IDs, ordered witness, full path/hydration, `LIMIT` absent/one/small |

Freeze a topology holdout before selector thresholds are tuned. Include textually
permuted multi-`MATCH` and multi-pattern forms to compare Neo4j reorder
invariance with CySQL clause ordering. Record unsupported same-kind parallel
edges as a storage boundary while covering cross-kind multiplicity.

## Tests required for every behavior change

### Unit, translation, and mutation coverage

- Optimizer table tests for candidate lists, exact eligibility facts, physical
  direction, policy/scheduler versions, caps, and stable fallback reasons.
- SQL-shape tests for materialized probes, explicit `LIMIT cap+1`, disjoint
  branch dependencies, ID-only state, edge-index orientation, and late
  hydration.
- Fail-closed forcing tests for wrong observation, predicates, mutation,
  correlation, optional match, directionless traversal, multiple calls, and
  unsupported depth.
- Reverse path-order, relationship-overlap, suffix bag multiplicity, duplicate
  roots, parameter rebinding, and generic/custom-plan tests.
- Source translation-case updates plus generated artifacts and mutation tests
  for parsing, lowering, rendering, and predicate placement changes.

### Semantic integration

- Shared backend-equivalent cases validate logical stable observations; no
  driver-specific expected values or skips belong in the shared corpus.
- PostgreSQL-scoped tests validate candidate branch loops, exact fallback,
  workspace state, edge indexes, caps, buffers, and function invocation.
- Cover missing/null/equal endpoints, zero depth, maximum-depth miss, both
  directions, cycles, repeated nodes without repeated relationships, suffix
  multiplicity, empty/disconnected sides, and every accepted/rejected predicate
  class.
- For singleton SP ties, compare distance and validate that each returned trail
  is minimum and relationship-unique; use unique-witness cases when an exact
  ordered-ID reference comparator is required.
- For ASP, compare the full stable path multiset and predecessor/output cap
  boundaries, not only row count.

### Operational integration

- Pool sizes `1/2/8` and concurrency `1/8/16`.
- Prompt cancellation followed by successful rollback and reuse of the same
  PostgreSQL backend PID.
- A concurrent-writer semantic test proving the selected snapshot mechanism or
  rejecting function-backed fallback under the default isolation behavior.
- Low `work_mem`, forced generic plan, forced custom plan, and normal `auto`
  plan modes.
- No cross-invocation workspace or telemetry contamination.
- Schema-up/schema-down symmetry and upgrade coverage for every new helper or
  temporary workspace.

## Performance and resource gates

Use the existing balanced GraphBench protocols:

- Discovery: at least five independently reloaded rounds, five warmups, and ten
  samples per arm.
- Confirmation: 10-20 independently reloaded rounds, at least 20 warmups and
  50 samples per arm, seeded 97.5% intervals, and balanced arm order.
- Before accepting three-arm SP or ASP evidence, add and freeze a balanced
  three-arm Latin/Williams schedule. The current non-five-arm forward/reverse
  ordering leaves the middle arm in the middle and is not carryover-balanced.
- A/A calibration: derive per-host p50/p95 absolute and ratio resolution before
  applying materiality.
- Complete declarations only: filtered or adaptive artifacts are diagnostic and
  cannot pass a release gate.

Initial promotion thresholds are policy inputs and must be versioned:

- target p50 candidate/incumbent ratio upper bound at most `0.95`, or absolute
  saving lower bound at least `100us`;
- no p95 regression beyond the greater of host A/A noise and 5%, using the
  greater of A/A absolute noise and `100us` for very fast cases;
- no confirmed normal/envelope regression outside that same noise band;
- selector regret versus the fastest exact arm: ratio upper bound at most
  `1.10` or within the A/A absolute floor;
- probe overhead versus the forced selected arm: at most 10% or `100us`;
- production/reference closure: retain the existing `1.10` ratio/A/A floor;
- Neo4j latency and PROFILE remain descriptive.

Extend the resource gate to enforce numeric envelopes, not only spill classes:

- probe rows at or below `cap + 1`;
- frontier, queue, seen, predecessor, output, and bytes at declared ceilings;
- no executor temp-file read/write or WAL for non-mutating candidates;
- local workspace only for explicitly workspace-qualified architectures;
- measured per-session and pool memory ceilings;
- no unexpected fallback in admitted normal/envelope buckets;
- exactly attributed fallback in stress buckets.

The identities must form a valid chain: the translation-applied policy matches
the planned candidate set, the runtime arm belongs to that emitted policy, and
any runtime fallback matches the declared incumbent chain. Probes execute at
most once, unselected arms show zero work, and fallback executes once before any
output. Any missing or contradictory attribution fails the gate.

## Milestones and exit criteria

| Milestone | Deliverables | Exit criterion |
| --- | --- | --- |
| M0: freeze baseline | Clean-source capture bundle; PostgreSQL/Neo4j environment fingerprints; current plans; A/A calibration; stable candidate IDs; topology holdout split | Checksummed artifacts reproduce exact observations and the discovery findings without credentials. |
| M1: observability | `TraversalExecutionTelemetry`; PostgreSQL diagnostic counters; Neo4j read `PROFILE`; paired PlanCorpus deltas; numeric resource schema | No result change; measured telemetry overhead is within A/A noise or disabled outside diagnostic replay; missing counters fail qualification. |
| M2: orientation framework | Common candidate analyzer; bounded seed/degree probes; endpoint-reverse migration; guarded suffix reverse; forced and shadow modes | Exact parity across semantic/cap cases; disjoint branches; selector-regret and probe-overhead reports exist. Production still uses the incumbent except the already qualified endpoint family. |
| M3: SP references | Strict-alternating and smaller-level compact reference arms; typed scheduler metadata; balanced three-arm schedule; formal termination invariant; inline/function boundary comparison | Exact distance/witness results, bounded state, cancellation/reuse, and discovery report across asymmetric topology buckets. |
| M4: SP production qualification | Incumbent-specific same-statement fallback; snapshot contract; complete confirmation/holdout/resource/reference-closure reports; `sp-static-v5` policy | Only runtime-recognizable, passing topology/observation buckets select a new arm; all other shapes preserve S3/S4/S0 with precise reasons. |
| M5: ASP references and qualification | Two-sided predecessor state; canonical meeting cut; three independent gates; full multiset comparator; ASP stress corpus | Exact ASP output, no truncation, bounded candidate state, confirmation and holdout pass; otherwise freeze a negative result and retain A1. |
| M6: envelope broadening | Bounded property/small-set endpoints; step-local/universal predicates; fixed one-hop `ExpandInto` study and any qualified policy | Each class has its own eligibility, exact fallback, corpus, and decision record. No broadening by tool forcing. |
| M7: optional synopsis | Synopsis ADR, schema/refresh/cache design, shadow comparison against runtime probes | Implement only if it materially reduces probe/selector regret and its mutation/cache cost passes independent gates. |

M2 and M3 may proceed in parallel after M1. M5 begins after the shared SP
kernel and telemetry stabilize. M6's `ExpandInto` plan study may run earlier,
but automatic behavior still requires its own evidence.

## Repository implementation map

| Concern | Primary files |
| --- | --- |
| Typed decisions and selectors | `cypher/models/pgsql/optimize/lowering.go`, `lowering_plan.go`, `optimizer_test.go` |
| Ordinary orientation emission | `cypher/models/pgsql/translate/expansion_orientation.go` (new), `expansion_endpoint_seeded.go`, `expansion_suffix_seeded.go`, `pattern.go`, `traversal.go`, `translator.go` |
| SP/ASP builders and dispatch | `cypher/models/pgsql/translate/expansion.go`, `pattern.go`, `optimizer_safety_test.go`, `cypher/models/pgsql/functions.go` |
| Compact workspaces/functions | `drivers/pg/query/sql/schema_up.sql`, `schema_down.sql`, `drivers/pg/query/sql_workspace_test.go`, schema-upgrade integration tests |
| Translation cache contract | `drivers/pg/translation_cache.go` and tests; change if mutable rollout policy is translated rather than supplied at execution, or if a synopsis is embedded |
| GraphBench telemetry/references | `cmd/graphbench/results.go`, `postgres_plan.go`, `neo4j.go`, `references.go`, `datasets.go`, `main.go` and tests |
| Gates and reports | `cmd/graphbench/resource_gate.go`, `perf_gate.go`, reference-pair/closure reports, backend-delta report |
| Matched plan deltas | `cmd/plancorpus/types.go`, `report.go`, capture/report tests |
| Deterministic topology generators | `testutil/perf_shortest_v2.go`, `perf_endpoint_seeded.go`, `perf_fixtures.go` |
| Scale declarations | `benchmark/testdata/scale/cases/generated_shortest_paths_v2.json`, `generated_endpoint_seeded_expansion_v1.json`, `generated_fixed_suffix_expansion.json` |
| Semantic fixtures | `integration/testdata/cases`, `integration/testdata/templates`, PostgreSQL-scoped plan-invariant tests |
| Documentation and evidence | this plan, `recursive_descent_cost_controls.md`, `postgresql_translation.md`, GraphBench/scale READMEs, and versioned `docs/experiments` records |

Changes should be sliced so telemetry, candidate implementation, selector
activation, and envelope broadening are separately reviewable. Do not combine a
new algorithm, new semantic support, and automatic selection in one change.

## Rollout and rollback

Every candidate follows the same stages:

1. Telemetry only; no selection change.
2. Exact benchmark reference arm with a frozen implementation ID.
3. Tool-forced production emitter, failing closed outside its envelope.
4. Shadow selection that records `would_select` while executing the incumbent;
   matched diagnostic arms calculate regret.
5. Explicit opt-in with same-statement exact fallback and an established
   snapshot contract for function-backed arms.
6. Narrow automatic selection for named, passing topology buckets.
7. One-bucket-at-a-time expansion after new holdout confirmation.

Keep the incumbent selector and previous function/schema identity available for
at least one release after automatic activation. A feature gate must be able to
return all traffic to the incumbent without a data migration, and changing it
must invalidate cached translated SQL or be an execution-time policy input.

Immediately disable automatic selection on:

- any correctness or ASP multiplicity mismatch;
- planned/emitted/runtime attribution disagreement;
- cap breach, partial candidate output, unexpected spill, or read-query WAL;
- cancellation poisoning or workspace/telemetry cross-talk;
- unstable SQL/plan fingerprint outside a declared change;
- abnormal fallback frequency in a qualified bucket;
- a confirmed p95 regression outside the A/A/materiality envelope.

Do not retune a failed identity post hoc. Preserve the failed arm and compact
evidence in `docs/experiments`, assign a new ID to a materially changed design,
and reopen discovery with a new hypothesis.

## Risk register

| Risk | Mitigation |
| --- | --- |
| Probe overhead erases the orientation win | Cap every probe, materialize once, measure probe-only cost, use hysteresis, and keep forward on ambiguous small gains. |
| Reverse admission plus fallback doubles expensive work | Gate before output, measure fallback regret explicitly, lower admission caps, and qualify overflow buckets separately. |
| Mutable topology or rollout policy invalidates cached SQL | Keep topology values inside same-statement probes; make mutable policy an execution input or cache generation; require a synopsis epoch before embedding statistics. |
| Bidirectional search stops at a nonminimal first meeting | Require a documented lower-bound termination proof and adversarial asymmetric/reconvergent tests. |
| ASP predecessor or output explosion is hidden by node-state counts | Enforce separate discovery, predecessor, path-count, output-row, and byte gates. |
| Session workspaces consume excessive pool memory or collide | Use invocation/session isolation, explicit per-session/pool ceilings, concurrency tests, and prompt cleanup on error/cancel. |
| Detailed telemetry changes the measured algorithm | Keep detailed counters in untimed diagnostic replay; separately measure lightweight summary overhead. |
| Fixed `ExpandInto` copies a Neo4j optimization that PostgreSQL does not need | Compare direct pair index lookup, lower-degree scan, and `Memoize`/pair cache before implementation. |
| Predicate pushdown changes evaluation semantics | Classify locality/universality, retain exact fallback, and require mutation plus cross-backend semantic fixtures. |
| Aggregate benchmark wins hide topology regressions | Gate by predeclared buckets, worst-case containment, and a frozen holdout rather than aggregate median alone. |
| Neo4j version differences corrupt interpretation | Pin source commits and server version in every artifact; keep 4.4 strict alternation and current smaller-level scheduling as separate arms. |

## Validation and evidence workflow

After code changes, run formatting and unit validation:

```bash
make format
make test
make lint
```

Run backend-specific full validation separately, using only disposable targets
and the repository's destructive-integration guards:

```bash
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="$PG_DISPOSABLE_TARGET" \
  CONNECTION_STRING="$PG_CONNECTION_STRING" make test_all

DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="$NEO4J_DISPOSABLE_TARGET" \
  CONNECTION_STRING="$NEO4J_CONNECTION_STRING" make test_all
```

Then run both-backend PlanCorpus and the staged GraphBench workflow:

1. plan corpus and matched delta capture;
2. discovery plus exact reference comparisons;
3. A/A calibration;
4. 10-20-round confirmation;
5. numeric resource gate;
6. production/reference closure;
7. concurrency, cancellation, and session-reuse cases;
8. topology holdout;
9. descriptive backend delta;
10. complete performance gate and capture bundle checksum.

Never place connection strings, endpoint IDs from sensitive graphs, query
parameters, or credentials in durable artifacts. Existing-graph confirmation
uses the current redacted anchor-manifest workflow and cannot substitute for the
deterministic correctness corpus.

For every accepted or rejected candidate, add
`docs/experiments/<candidate>_vN.md` containing:

- immutable implementation and selector IDs;
- source and artifact SHA-256 values;
- backend versions and relevant settings;
- corpus declaration and holdout identity;
- rounds, warmups, samples, order balancing, and confidence policy;
- correctness, performance, resource, fallback, concurrency, and cancellation
  results;
- the promotion/rejection decision and unchanged incumbent behavior.

Raw captures remain under `.coverage`; compact canonical reports may be
committed when they contain no secrets or unstable physical identifiers.

## Definition of done

This priority plan is complete when:

- traversal decisions and runtime execution are separately observable and
  matched across plan records;
- Neo4j read plans include actual evidence with SP/ASP opacity represented
  honestly;
- ordinary orientation, SP, and ASP each have exact incumbent and candidate
  arms with stable identities;
- every candidate has bounded probes/state, disjoint output/fallback behavior,
  and precise machine-readable fallback reasons;
- semantic, cap-boundary, operational, resource, performance, and holdout gates
  run reproducibly;
- production selectors enable only independently passing topology/observation
  buckets and remain quickly reversible;
- nonwinning candidates are retired with durable negative evidence rather than
  left as ambiguous code paths;
- documentation describes current production behavior separately from future
  candidates and their qualification status.

Success may legitimately conclude that S3/S4/A1 or direct PostgreSQL pair
lookup remains best for some or all buckets. The required outcome is a measured,
exact, explainable selector program—not a predetermined Neo4j-shaped executor.

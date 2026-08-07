# CySQL Performance Continuation Plan 5

Date: 2026-08-07

Status: proposed implementation and qualification plan. This document does not
claim that the work below has been implemented.

## Purpose

This continuation converts the expanded real-world PostgreSQL findings into a
bounded production program. `perf_cont_4.md` is complete; its accepted
horizontal work, shortest-path emitters, diagnostics, benchmark contracts, and
exact incumbent fallbacks remain the entering implementation. This plan does
not reopen those results merely because a larger dataset is harder.

The new evidence does change the production disposition of part of the
`sp-static-v2` envelope. The selected singleton executors remain semantically
exact, but query shape alone did not bound their work on two real graph
structures:

- a low-degree physical-inbound root followed by extreme intermediate reverse
  fan-in; and
- a high-cardinality, multi-kind one-path search whose edge-distinct recursive
  trails spilled before one winning path was materialized.

The immediate objective is therefore containment, followed by a new shortest
search and state-management tournament. The wider objective is to turn the
remaining real-data findings—`allShortestPaths`, large exact counts, hydration
tails, missing ADCS suffix coverage, and absent same-data Neo4j evidence—into
independently gated workstreams rather than one undifferentiated optimization.

## Executive decision

Implement this continuation in the following order:

1. Freeze the expanded live evidence and publish the revised qualified
   envelope.
2. Introduce `sp-static-v3` as a safety containment selector:
   preserve the current candidate for the proven physical-outbound envelope;
   retain it for physical-inbound depth zero/one only; and route deep
   physical-inbound searches plus multi-kind full-path searches to `SP-S0`.
3. Add generated hidden-fan-in and high-cardinality parallel-kind fixtures,
   and move the ad hoc live-data procedure into a safe, resumable GraphBench
   mode.
4. Tournament canonical source-oriented, bounded bidirectional, and guarded
   search candidates against both `SP-S3` and `SP-S0`.
5. Replace edge-trail proliferation for singleton `shortestPath` with an exact
   one-witness-per-node architecture if it preserves the accepted tie and path
   contract.
6. Add a runtime state guard only if it can prove a hard work bound, discard
   partial work, and execute exact fallback in the same statement snapshot.
7. Keep `allShortestPaths` in a separate `ASP` family and evaluate a
   shortest-depth predecessor-DAG design.
8. Pursue exact maintained counts only after an explicit count-latency/write-
   cost objective is accepted.
9. Re-run hydration attribution, load an identity-equivalent sanitized graph
   into Neo4j, and revisit ADCS only on data with a complete trust suffix.
10. Activate each accepted increment independently, then publish a cumulative
    PostgreSQL/Neo4j report and the next residual ranking.

No release may restore the broad `sp-static-v2` envelope merely because a new
candidate wins a few live anchors. Every restored shape must pass generated
threshold, holdout, state, spill, concurrency, cancellation, and exact-fallback
gates.

## Entering state and evidence boundary

### Authoritative source state

The plan was prepared against:

| Item | Value |
|---|---|
| Git commit | `b50764e921baf2e1004abfb7fa27e54b1fce420e` |
| Branch | `cysql-bench-optimizer` |
| `perf_cont_4.md` SHA-256 | `ca96785af09d494c4aff5569a005e5a351e5ccd172771a3b461cf20679c4b4f3` |
| Completion report SHA-256 | `b761cbe344dbf69b3610fc39481207eaf63bd3013be02ea25705a70c32c1bde8` |
| Expanded live report SHA-256 | `f69f771aac51a667632f5b1a39118c802c4aed73cf9722e04478b3531e25ce54` |

The expanded report is
`artifacts/perf/real-world-live-v2/REPORT.md`. Its raw artifact hashes are the
authoritative evidence for the figures summarized here. Credentials, raw
sensitive properties, and connection strings are not durable evidence and
must not be copied into new artifacts.

### Production behavior entering this plan

The PostgreSQL optimizer currently records a `ShortestPathExecutorDecision`
and selects:

- `SP-S3-U-D` for qualified distance observations;
- `SP-S3-U-E+MAT-M0` for qualified one-path observations; and
- `SP-S0` for structurally ineligible singleton cases and all other generic
  shortest-path forms.

The current `sp-static-v2` facts require one non-optional directed traversal,
bounded depth zero/one through 64, no relationship variable or relationship
predicate, one static ID equality per endpoint, no path predicate, one
uncorrelated endpoint pair, one statement-wide shortest call, a known
observation mode, and a read-only statement. The selector records only whether
the traversal is directed; it does not record which physical edge endpoint is
expanded or the number of relationship kinds.

`SP-S3-U-D` emits a recursive scalar state containing current node ID and
depth, using `UNION` to deduplicate equal node/depth rows. It does not carry a
path. `SP-S3-U-E+MAT-M0` emits `UNION ALL` state containing current node ID,
depth, and the complete ordered edge-ID trail, then hydrates the selected
trail. Distinct edge trails reaching the same node remain distinct states.

`SP-S0` remains the exact incumbent. It uses reusable session-local
`pg_temp.bsp_*` workspace and therefore must retain its temporary-write,
cleanup, cancellation, rollback, and physical-session-reuse contract.

The count fast path is already active, but relationship counts intentionally
join both endpoint nodes. The edge table has graph ownership and uniqueness
constraints but no endpoint foreign keys. A statement-level node-delete
trigger normally removes incident edges; raw SQL and external bulk paths are
still part of the invariant question. Dropping endpoint joins is therefore not
a harmless rendering cleanup.

ADCS continues to select `ADCS-INCUMBENT-STEPWISE`. Tool-only ADCS alternatives
remain closed for automatic selection by the prior plan's reverse-fan-in and
fallback gates.

### Expanded real-data result

The sanitized PostgreSQL graph contains exactly 1,845,833 nodes and 44,133,029
relationships, including 8,742,373 `MemberOf` and 5,732,248 `AZMemberOf`
relationships. Post-run counts were unchanged.

The important observed deltas are:

| Shape | Candidate | `SP-S0` | Candidate / incumbent | Result |
|---|---:|---:|---:|---|
| Outbound F987 D16 distance | 1.492 ms | 10.492 ms | 0.142 | retain candidate |
| Outbound F987 D16 path | 1.754 ms | 10.548 ms | 0.166 | retain candidate |
| Inbound true-depth D3 distance | 117.998 ms | 6.027 ms | 19.58 | contain |
| Inbound true-depth D3 path | 154.445 ms | 8.413 ms | 18.36 | contain |
| Inbound true-depth D64 distance | 596.545 ms | 7.983 ms | 74.73 | contain |
| Inbound true-depth D64 path | 646.992 ms | 8.248 ms | 78.44 | contain |
| Parallel K1/D1 distance | 236.017 ms | 4,126.454 ms | 0.057 | candidate wins, expensive shape |
| Parallel K1/D1 path | 220.175 ms | 3,887.278 ms | 0.057 | candidate wins, expensive shape |
| Parallel K7/D2 distance | 2,387.204 ms | 13,302.470 ms | 0.179 | candidate wins, stress tier |
| Parallel K7/D2 path | 8,070.438 ms | 12,249.235 ms | 0.659 | candidate wins but fails resource gate |

The inbound D64 candidate retained 348,667 recursive rows and touched
1,306,199 shared-hit plus 90,493 shared-read blocks. Its physical-inbound root
had only two matching edges; a later node had 170,593 matching incoming edges.
A root-degree probe cannot detect this topology.

The seven-kind path retained 9,527,404 recursive states, performed 2,810,044
edge loops, and read/wrote 48,380/114,253 temporary blocks. The corresponding
root had 2,810,036 matching physical outgoing edges but only 657,349 distinct
next nodes. The current full-path state prices every edge trail before choosing
one result.

Other residuals are real but lower priority:

- a ten-path all-shortest diamond took 462 ms median;
- seven parallel one-hop all-shortest paths took 8.15 seconds median;
- exact `MemberOf` count took 1.88 seconds and exact all-edge count took 3.06
  seconds;
- 1,000 indexed node IDs took 1.23 ms while full hydration took 6.74 ms;
- 1,000 typed user IDs had a 1.95 ms median but a 79.84 ms maximum;
- the dataset has no `TrustedForNTAuth`, so it cannot qualify ADCS; and
- no identity-equivalent copy of this graph was run on Neo4j.

### Evidence interpretation

The real-data run does not invalidate the exactness of the accepted emitters.
It invalidates the claim that their previous static shape envelope bounds
performance across production topologies.

The report also does not prove that `SP-S0` is a universally better executor.
It is dramatically better for the hidden reverse-fan-in chain and dramatically
worse for the high-cardinality parallel-kind root. Containment and replacement
must therefore be treated as separate decisions:

- containment chooses a conservative known exact executor while the envelope
  is unqualified;
- replacement chooses among exact architectures using a wider topology and
  resource matrix; and
- adaptive selection ships only if complete candidate-plus-fallback regret is
  bounded.

## Scope

### Required work

This plan requires:

- a revised shortest-path production selector;
- generated fixtures that reproduce the two missing topology classes;
- durable read-only live-data benchmark support;
- a shortest search-direction and one-path state tournament;
- an exact bounded-overflow feasibility decision;
- a separate all-shortest architecture disposition;
- a count-product decision and, if triggered, a count architecture
  disposition;
- hydration-tail attribution;
- an identity-equivalent Neo4j qualification; and
- ADCS qualification only after a complete suffix dataset exists.

### Explicit non-goals

The following are not substitutes for the required work:

- raising `work_mem` until the seven-kind path stops spilling;
- increasing statement timeouts and calling a completed query qualified;
- selecting by root degree alone;
- using PostgreSQL planner estimates as a correctness or hard-resource bound;
- adding a universal edge index without an attributed candidate plan;
- treating approximate catalog statistics as exact Cypher `count()`;
- merging `shortestPath` and `allShortestPaths` because both use the word
  “shortest”;
- publishing synthetic Neo4j numbers as real-data backend deltas;
- tuning thresholds on the same anchors used for final confirmation; or
- reopening accepted parser, codec, row-ownership, or scalar-continuation work
  without a newly reproduced residual.

## Fixed decisions

1. `SP-S0` remains the exact fallback until a replacement passes all gates.
2. Physical expansion direction, not textual variable naming, is part of the
   selector and diagnostic identity.
3. A low root degree is not proof of bounded downstream work.
4. The immediate selector narrows before new search code activates.
5. Outbound shapes whose SQL and live behavior remain qualified are not rolled
   back with unrelated inbound shapes.
6. Multi-kind one-path state is removed from the normal production candidate
   envelope until a one-witness or hard-bounded implementation qualifies.
7. Distance and one-path observation modes continue to activate separately.
8. A singleton shortest path needs one valid minimal trail, not every minimal
   trail. Any tie-policy change must nevertheless be deliberate, documented,
   and tested against the existing PostgreSQL compatibility contract.
9. `allShortestPaths` must preserve every relationship-distinct shortest
   result and remains a separate executor family.
10. Runtime overflow may not leak partial rows or restart under a different
    snapshot.
11. An unprovable state cap is only a diagnostic limit, not a production
    safety mechanism.
12. Exact relationship counts retain endpoint existence semantics unless a
    database-enforced invariant makes those joins redundant.
13. Approximate counts require a separate public API; they never replace exact
    Cypher aggregation silently.
14. Adaptive timeout and sample reduction are discovery tools. They cannot
    manufacture release-grade p95 evidence.
15. Neo4j remains a semantic oracle and contextual backend comparison, not the
    pass/fail comparator for PostgreSQL implementation choices.
16. Existing stricter correctness, reference-closure, selector-regret,
    resource, cancellation, and concurrency gates from `perf_cont_4.md`
    remain in force unless this plan states a stronger gate.

## Candidate and selector namespace

Architecture names must describe state and execution, not an experiment file
or SQL alias.

| Identity | Meaning |
|---|---|
| `SP-S0` | Existing exact workspace incumbent |
| `SP-S3-U-D` | Current unidirectional node/depth distance executor |
| `SP-S3-U-E+MAT-M0` | Current unidirectional edge-trail one-path executor plus M0 hydration |
| `SP-S4-C-D` | Candidate that canonicalizes a directed pattern to relationship-source-oriented distance search |
| `SP-S4-C-WE+MAT-M0` | Canonical source-oriented candidate retaining one deterministic witness trail per accepted node state |
| `SP-S4-BI-D` | Bounded native bidirectional distance candidate |
| `SP-S4-BI-WE+MAT-M0` | Bounded bidirectional one-witness path candidate |
| `SP-G1` | Runtime state-budget and exact-overflow policy wrapped around an already qualified executor |
| `ASP-A0` | Existing exact all-shortest workspace implementation |
| `ASP-A1-DAG` | Candidate shortest-depth search plus relationship-distinct predecessor DAG enumeration |
| `COUNT-C0` | Current endpoint-preserving exact scan |
| `COUNT-C1` | Exact edge-only count enabled by a database-enforced endpoint invariant |
| `COUNT-C2` | Transactionally maintained exact graph/kind summary |

The initial containment selector is `sp-static-v3`. A later static selector
that activates an accepted S4 executor is `sp-static-v4`; it must not mutate
the meaning of v3 in place. A runtime policy, if accepted, is
`sp-bounded-v1` and records `SP-G1` independently from the wrapped executor.

Each arm records architecture, implementation ID, state shape, observation
shape, search origin, physical expansion column, relationship-kind count,
timing boundary, SQL fingerprint, semantic-validation mode, selected plan
mode, source/binary/fixture hashes, and planned/selected/applied/runtime-
fallback identities. Different architectures with the same SQL fingerprint
are rejected unless one is declared as an A/A alias.

## Correctness and safety contract

### Public path semantics

Every shortest candidate must preserve:

- endpoint existence and graph partition scope;
- relationship direction and allowed-kind filtering;
- minimum and maximum depth, including zero depth;
- the current same-endpoint error behavior for minimum depth one;
- relationship-unique trail semantics;
- complete ordered node and relationship hydration when a path is observed;
- relationship and node properties, kinds, and null behavior;
- missing-root, missing-endpoint, disconnected, cycle, self-loop, and graph-ID
  collision behavior;
- aliases, `WITH`, optional, correlated, multipart, mutation, multiple-path,
  path-predicate, relationship-variable, and directionless fallback behavior;
  and
- cancellation, rollback, transaction cleanup, and physical-session reuse.

For a singleton `shortestPath` tie, validation must prove that the returned
trail is valid and minimal. Before an S4 witness implementation ships, record
whether DAWGS promises the current PostgreSQL physical-edge-ID tie order. If
that order is retained, witness selection uses the same deterministic order.
If it is not a public promise, shared PostgreSQL/Neo4j cases compare logical
validity and logical relationship keys rather than backend physical IDs. This
decision must be documented; it may not emerge accidentally from a faster
query.

For `allShortestPaths`, exact result multiset and relationship-distinct
multiplicity are mandatory. A result cap, timeout, or cancellation may stop
the query with an error, but no executor may silently truncate the set.

### Runtime fallback

`SP-G1` is acceptable only if all of the following are true:

- the state budget limits actual recursive work, not merely emitted rows;
- overflow is explicit and distinguishable from “no path”;
- no candidate row is visible before the overflow decision;
- candidate and fallback observe one statement snapshot;
- exactly one result branch executes and returns rows;
- unselected search and materializer descendants have zero loops;
- the fallback is byte-for-byte/publicly equivalent to `SP-S0`;
- missing endpoints execute no recursive candidate or fallback work beyond
  endpoint validation;
- cancellation during probe, candidate, overflow, fallback, and hydration
  leaves the connection reusable; and
- complete probe plus discarded work plus fallback meets the declared regret
  and resource ceiling.

The preferred design is a single SQL statement with mutually exclusive
branches. If PostgreSQL cannot enforce a hard cap in that form, runtime
selection closes and the static envelope remains restricted. A driver retry,
new transaction, wall-clock kill, or planner row estimate does not satisfy the
contract.

### Read-only and data safety

Generated benchmark writes run only in the existing rollback-isolated fixture
workflow. Live sanitized-data qualification is read-only:

- no Cypher write case is accepted;
- graph cardinalities are captured before and after;
- only documented `pg_temp` incumbent workspace writes are allowed;
- no persistent helper or index is created by the live runner;
- sensitive properties and connection credentials are redacted; and
- interrupted runs are resumable without modifying graph state.

Schema or maintained-count experiments use a disposable clone. They never
migrate the sole live sanitized database in place.

## Target shortest-path architecture

### Containment selector: `sp-static-v3`

Add explicit analyzed facts to `ShortestPathExecutorDecision`:

- `direction` using the graph direction enum;
- `physical_expansion` as `start_id` or `end_id`;
- `relationship_kind_count` plus an explicit untyped/wildcard indicator;
- `topology_classification` with static values only; and
- the existing minimum/maximum depth and observation mode.

The recommended v3 production rules are:

| Observation | Physical expansion | Other condition | Selected executor |
|---|---|---|---|
| Distance | `start_id` | Existing v2 facts pass | `SP-S3-U-D` |
| One path | `start_id` | Existing v2 facts pass and exactly one named relationship kind | `SP-S3-U-E+MAT-M0` |
| Distance or one path | `end_id` | Maximum depth is zero or one; one-path case also has exactly one kind | Existing S3 executor |
| Distance or one path | `end_id` | Maximum depth exceeds one | `SP-S0` |
| One path | Either | Untyped/wildcard or more than one relationship kind | `SP-S0` |
| Any | Either | Any existing eligibility fact fails | `SP-S0` with the existing more-specific reason |

The new stable fallback reasons are:

- `deep_inbound_unqualified`; and
- `non_single_kind_path_state_unqualified`.

Existing structural reasons retain precedence. For example, a directionless
query remains `directionless`, and a mutation remains `mutation`; the new
performance reason must not mask a semantic ineligibility.

This static rule is deliberately conservative. A direct one-hop result written
with a cap of 16 is indistinguishable at compile time from the observed
three-hop hidden-fan-in case, so it falls back until S4 or `SP-G1` qualifies.
The containment evidence must report that direct-inbound regret rather than
hiding it.

### Canonical source-oriented candidates

The current unidirectional builder starts from the left pattern endpoint. For
an inbound pattern, this means probing `edge.end_id` even when both endpoints
are bound and the relationship source is the right endpoint.

`SP-S4-C-D` and `SP-S4-C-WE+MAT-M0` test a canonical directed orientation:

- seed from the relationship source endpoint;
- expand through `start_id -> end_id` regardless of left/right pattern syntax;
- return endpoint projections in the original pattern binding order;
- reverse or normalize the edge trail before hydration when search order and
  path order differ; and
- preserve the exact graph, depth, same-endpoint, and missing-endpoint
  contracts.

This is the first implementation candidate for the observed inbound chain,
but it is not assumed universally superior. Mirror fixtures must put extreme
fan-out on the canonical source side and low degree on the destination side.
If canonical orientation merely moves the pathological side, it may qualify
only behind a bounded selector or close.

### Native bidirectional candidates

`SP-S4-BI-D` and `SP-S4-BI-WE+MAT-M0` are genuinely bounded-endpoint native
SQL candidates, not aliases for the current workspace harness. They should:

- seed both validated singleton endpoints;
- keep forward and reverse frontier identities separate;
- expand only a provably selected bounded frontier;
- stop at the first minimal meeting depth without exploring greater depth;
- reconstruct one relationship-unique witness for singleton path output;
- avoid persistent or session-local mutable workspace in the candidate arm;
  and
- expose forward/reverse states and meeting rows in plan diagnostics.

A SQL implementation that evaluates both full unidirectional searches before
choosing a result is not bidirectional and fails architecture identity. A
frontier choice based only on initial degree is diagnostic unless downstream
work is also bounded.

### One-witness state for singleton path output

The one-path candidate must stop retaining every equal-depth edge trail to a
node merely to return one result. The primary architecture is:

1. discover minimum-depth node state;
2. retain one deterministic predecessor relationship for each accepted
   node/depth state, or an equivalent compact witness structure;
3. stop accepting deeper states once the target minimum is fixed;
4. reconstruct one ordered edge-ID trail; and
5. hydrate only that trail through the accepted materializer boundary.

The design must prove that deduplication cannot remove the only valid shortest
trail under the current static envelope. Relationship predicates, path
predicates, relationship variables, multiple endpoint pairs, and other shapes
whose validity can depend on the full trail remain on `SP-S0`.

Candidate SQL must not use a trailing `DISTINCT` over millions of full trail
arrays and call that compact state. Plan invariants should show state scaling
with accepted node/depth witnesses, not physical parallel-edge trails. For the
seven-kind live shape, the target state order is bounded by distinct reached
nodes plus predecessor metadata rather than the observed 9.53 million trail
rows.

Distance and one-path implementations remain distinct. A path optimization
must not add predecessor or materialization columns to `SP-S4-C-D` or
`SP-S4-BI-D`.

### Runtime state guard

After static S4 candidates are qualified, prototype `SP-G1` with separate
budgets for distance and one-path rows because their retained widths differ.
The guard key includes:

- executor architecture;
- observation mode;
- physical direction;
- maximum depth;
- relationship-kind count;
- state budget; and
- selector version.

Test at budget minus one, budget, and budget plus one for anchor, recursive,
meeting, witness, and hydration boundaries. The state limit already present in
the decision model remains zero for static selection and becomes nonzero only
when an actual bounded runtime policy is emitted.

Do not choose a production threshold from the 170,593 or 2,810,036 live values.
Use discovery fixtures to define candidate ranges, freeze the threshold, then
run unseen generated and real holdouts. If no threshold passes complete
selector regret, retain `sp-static-v3`/`sp-static-v4` without runtime
selection.

## Generated fixture and benchmark design

### Shortest fixture v2

Keep legacy `generated_shortest_paths_d*_f*` datasets immutable so prior
artifacts remain reconstructible. Add a v2 configuration rather than changing
their meaning.

Recommended exact configuration fields are:

- `Depth`;
- `ForwardRootFanOut`;
- `ReverseRootFanIn`;
- `IntermediateFanOut`;
- `IntermediateReverseFanIn`;
- `FanInLevel`;
- `ParallelKindCount`;
- `ParallelTargetCount`;
- `DiamondWidth`;
- `DisconnectedWidth`;
- `PropertyPayloadSize`; and
- explicit true/false controls for cycle and self-loop additions where the
  default shape would obscure state accounting.

Use an exact, round-trippable dataset identity such as:

```text
generated_shortest_paths_v2_d<depth>_o<root-out>_r<root-in>_
fo<intermediate-out>_fi<intermediate-in>_l<fanin-level>_
k<parallel-kinds>_t<parallel-targets>_w<diamond-width>_
x<disconnected-width>_p<payload>
```

The parser must reject partial scans, negative values, impossible fan-in
levels, unknown suffixes, and non-canonical spellings. Fixture generation is
deterministic and every semantic relationship receives a stable
`logical_key` for cross-backend path comparison.

`FixtureMetadata` gains a shortest-specific expectation block containing at
least:

- root forward and reverse degrees;
- maximum intermediate forward and reverse degree by level;
- physical traversable edge count by kind;
- distinct reachable node count by level;
- expected minimum distance;
- expected one-path and all-shortest cardinality;
- expected relationship-distinct predecessor edges;
- disconnected state cardinality; and
- complete graph checksum and physical loaded cardinality.

### Required generated topology matrix

The normal and envelope matrix must include:

| Dimension | Required values |
|---|---|
| True depth | 0, 1, 2, 3, 4, 8, 16, 32, 64 |
| Query cap | exact depth, depth + 1, 16, 64 where legal |
| Direction | physical outbound, physical inbound, mirrored syntax |
| Hidden intermediate fan-in | 0, 16, 128, 1,024, 16,384; real holdout near 170k |
| Fan-in level | 1, 2, penultimate |
| Root degree | 0, 1, 2, 16, 1,024 |
| Parallel kinds | 1, 2, 7, 16, 30 |
| Parallel targets | 1, 16, 1,024, 16,384; real holdout near 657k |
| Result | reachable, disconnected, missing root, missing endpoint |
| Observation | distance, one path, all shortest |
| Shape | linear, diamond, cycle, self-loop, parallel tie |

Large points are benchmark fixtures, not ordinary integration fixtures. A
small representative of every semantic shape belongs in shared backend-
equivalent integration coverage; envelope and stress points run through
GraphBench on both declared backends.

At least one holdout must reproduce the defining blind spot: physical-inbound
root degree two, a true path of depth three, and a large reverse fan-in at the
second intermediate. At least one mirrored holdout must put the same explosion
on the other physical direction so a canonical-source candidate cannot overfit
the first graph.

At least one parallel fixture must have many physical relationships but far
fewer distinct next nodes. PostgreSQL's uniqueness constraint permits one edge
per `(start, end, kind, graph)`, so physical multiplicity is generated through
distinct relationship kinds and/or destinations, not invalid duplicate rows.

### Durable live-data mode

Promote the expanded live harness behavior into `cmd/graphbench` instead of
maintaining another copied program. Add an explicit existing-graph/read-only
mode with these properties:

- never clears or loads a graph;
- rejects every `write_scenario` and mutation keyword before execution;
- resolves anchors from a versioned logical manifest;
- supports indexed anchor validation and bounded sampling discovery;
- captures redacted dataset cardinality, relation size, PostgreSQL version,
  schema/index fingerprints, and logical content identity;
- captures counts before and after the run;
- emits progress before each case, arm, sample, plan, and concurrency block;
- writes a checkpoint after each complete record and resumes by stable case
  identity;
- records timeout escalation and sample reduction in each result;
- keeps initial timeouts as retained diagnostics rather than overwriting them;
  and
- refuses to call a filtered/adaptive run a complete release corpus.

Anchor manifests store logical keys or one-way hashes, not raw identifying
properties. Parameter rendering remains redacted in durable artifacts.

### Discovery and confirmation effort

Discovery may adapt effort to obtain useful evidence:

- begin with a short per-case timeout;
- on timeout, record progress and retry only through predeclared timeout
  classes;
- reduce warm samples after the first stable latency class;
- run plans once a case completes;
- stop an architecture arm after a deterministic semantic mismatch or
  resource-ceiling breach; and
- preserve every timeout and stopped arm in the artifact.

Confirmation does not adapt silently. It uses frozen cases, timeouts, arm
order, samples, thresholds, and binaries. Fast normal-tier targets retain the
prior ten-round, 20-warmup, 50-measurement protocol. Formal p95 claims require
at least the existing 150 warm samples. Heavy stress cases may use fewer
samples, but then report median, range, plan, state, and resource evidence as
stress diagnostics rather than a release p95.

## Sequenced delivery plan

```text
N0 evidence freeze
  -> N1 static containment
  -> N2 fixtures + durable benchmark platform
       -> N3 source-oriented/bidirectional shortest tournament
       -> N4 singleton witness-state tournament
       -> N6 all-shortest program
       -> N7 count and hydration residual decisions
       -> N8 same-data Neo4j and complete-suffix ADCS qualification
  N3 + N4 -> N5 bounded runtime selector feasibility
  accepted N1..N8 dispositions -> N9 cumulative release qualification
```

N3 and N4 may prototype in parallel after N2, but production activation of a
combined one-path stack requires both the chosen search and materializer/state
boundary to pass independently. N6, N7, and N8 do not block a safe N1 release.

## Phase N0: Freeze evidence and revise the declared envelope

### Work

1. Copy the expanded live report and all referenced JSON/JSONL files into a
   checksum-bound continuation baseline bundle.
2. Record source commit, dirty-tree manifest, schema/index fingerprints,
   PostgreSQL version/settings, graph cardinalities, and benchmark harness
   hash.
3. Add a concise production advisory to the performance completion document:
   Plan 4 is complete, but live-v2 evidence narrows the deep-inbound and
   multi-kind one-path performance qualification.
4. Mark the current live-v2 run as discovery/qualification evidence with no
   same-data Neo4j claim.
5. Freeze the proposed `sp-static-v3` rules and fallback reason strings before
   measuring their release candidate.
6. Define normal, envelope, and stress tiers for the new fixture matrix.

### Exit criteria

- Every entering claim resolves to a retained artifact and SHA-256.
- The data remained unchanged and no credential appears in the bundle.
- The old broad selector envelope is no longer described as uniformly
  real-data-qualified.
- V3 rules and gate thresholds are versioned before implementation timing.

## Phase N1: Ship static containment

### Optimizer changes

1. Extend shortest decisions with direction, physical expansion, and
   relationship-kind cardinality.
2. Add eligibility facts for the v3 physical-direction/depth and one-path-kind
   boundaries.
3. Add stable fallback constants for deep inbound and multi-kind path state.
4. Preserve existing structural-reason precedence.
5. Select v3 only after statement-wide read-only, call-count, and observation
   finalization.
6. Leave forced tool selection available for qualification; do not add a
   runtime environment flag.

### Translator changes

No new search SQL is required in N1. Translation applies the selected existing
S3 or `SP-S0` executor and reports selected/applied/fallback identities.
Outbound single-kind SQL fingerprints must remain unchanged from v2.

### Tests

Add optimizer and translator cases for:

- outbound distance and path at depths 0/1/2/16/64;
- inbound distance and path at maximum depths 0, 1, 2, and 64;
- one versus two relationship kinds in distance and path observations;
- inbound multi-kind reason precedence;
- directionless, relationship-variable, relationship-predicate, optional,
  mutation, multiple-call, correlated, and unknown-observation controls;
- planned/selected/applied/skipped diagnostics;
- forced S3 and forced `SP-S0` SQL; and
- statement-wide behavior across `WITH`.

Update source translation cases and generated SQL artifacts through the
existing workflow. Shared integration semantics remain identical on
PostgreSQL and Neo4j; selector and SQL-plan assertions are PostgreSQL-scoped.

### Live qualification

Run forced candidate and forced incumbent controls before enabling v3:

- observed inbound true-depth D3/D64 distance and path;
- direct inbound one-hop anchors written with caps 1, 2, 16, and 64;
- outbound F1/F128/F987 distance and path;
- one-, two-, and seven-kind distance/path controls;
- disconnected and missing endpoints; and
- one/full/twice-pool concurrency plus cancellation/reuse.

V3 is containment, not a claimed speed optimization. It passes when:

- exact observations match;
- deep inbound and multi-kind one-path cases actually emit `SP-S0`;
- current qualified outbound cases retain identical SQL and performance within
  affected-family non-inferiority;
- no newly ineligible candidate subtree executes;
- direct-inbound fallback regret is fully reported;
- fallback temp state is cleaned after success, error, cancellation, and
  rollback; and
- no unqualified shape is re-enabled to hide a fallback regression.

### Exit criteria

- `sp-static-v3` is the production default.
- Every fallback has the expected stable reason.
- The two live-v2 failure classes are outside the candidate envelope.
- Existing generic fallback and tool-force paths remain tested.
- The release note identifies both the narrowed envelope and likely latency
  tradeoff for direct inbound queries whose declared cap exceeds one.

## Phase N2: Build topology-complete fixtures and benchmark support

### Fixture implementation

1. Add a v2 shortest configuration and deterministic builder in
   `testutil/perf_fixtures.go` or a focused adjacent file.
2. Add canonical name parsing in `cmd/graphbench/datasets.go`.
3. Add exact shortest fixture metadata and physical-cardinality validation.
4. Register small semantic cases and the normal/envelope/stress scale matrix.
5. Add logical relationship keys and backend-independent expected paths.
6. Keep legacy fixture names and checksums unchanged.

### GraphBench implementation

1. Extend `WorkloadShape` with direction, kind count, fixture tier, expected
   state class, and result-cardinality class.
2. Add candidate/reference arms for v3, forced S3, S4 prototypes, and `SP-S0`
   without conflating raw-pgx and E2E boundaries.
3. Extend plan metrics with architecture-labeled recursive rows, frontier
   rows, witness rows, meeting rows, and hydration rows where PostgreSQL plans
   expose them.
4. Retain temp read/write blocks, shared/local buffers, WAL, planning time,
   execution time, SQL fingerprint, and plan mode.
5. Add existing-graph read-only, progress, checkpoint/resume, timeout-class,
   and adaptive-discovery support.
6. Add a state/resource gate report separate from the latency-only performance
   gate.
7. Add a descriptive cross-backend delta report that never marks Neo4j as the
   PostgreSQL pass/fail baseline.

### Harness tests

Cover:

- canonical v2 name round trips and invalid names;
- deterministic graph checksums;
- exact fixture metadata formulas;
- physical cardinality checks after PostgreSQL and Neo4j load;
- case/backend declaration completeness;
- mutation rejection in existing-graph mode;
- before/after count verification;
- redaction of parameters and properties;
- progress and checkpoint atomicity;
- resume without duplicate samples;
- timeout escalation and sample-reduction recording;
- filtered/adaptive artifact refusal by the complete-corpus gate;
- plan-metric provenance; and
- reference architecture/fingerprint identity.

### Exit criteria

- The synthetic corpus reproduces hidden intermediate fan-in and parallel-kind
  state growth by orders of magnitude, not only by labels.
- PostgreSQL and Neo4j small semantic cases agree.
- Existing-graph mode is read-only by construction and survives interruption.
- Every result is attributable to an exact fixture, source, binary, SQL, and
  environment identity.
- Discovery and confirmation artifacts cannot be confused.

## Phase N3: Qualify search origin and direction

### Tournament arms

For distance and one-path observation boundaries separately, compare:

- `SP-S0`;
- `SP-S3-U-D` or `SP-S3-U-E+MAT-M0`;
- `SP-S4-C-D` or `SP-S4-C-WE+MAT-M0`; and
- a genuine `SP-S4-BI-*` prototype or a documented feasibility closure.

Every arm must be a full exact comparator at the same raw and E2E boundary.
Do not compare a distance-only reference against full path hydration or a
precomputed trail materializer against a complete search.

### Required regimes

- outbound and inbound linear paths;
- hidden fan-in at first, second, and penultimate levels;
- mirrored hidden fan-out;
- reachable target before, at, and after the explosive level;
- disconnected endpoint with full depth exhaustion;
- root degrees below and above downstream degrees;
- caps 2/3/8/16/64;
- one and many relationship kinds;
- auto/custom/generic PostgreSQL plans;
- cold diagnostic and warm confirmation; and
- one/half/full/twice-pool concurrency.

### Candidate invariants

- Canonical source orientation expands the declared physical index direction.
- Original pattern endpoint order is restored in public projections.
- Distance state has no edge trail, predecessor array, node composite, or path
  materializer.
- One-path search emits ordered edge IDs exactly once for hydration.
- A bidirectional arm reports both frontier state counts and a minimal meeting
  depth.
- No arm explores beyond the first accepted shortest depth.
- Missing endpoints execute no recursive search.
- Graph partition pruning remains visible in all plan modes.

### Selection rule

Prefer one static S4 executor only if it is non-dominated across mirrored
normal and envelope regimes. If canonical source orientation fixes the live
inbound chain but regresses the mirrored topology, it remains a runtime-policy
candidate rather than a static default. If the native bidirectional design
cannot meet SQL, planning, or state bounds, close it with a concrete feasibility
record; do not keep a name-only alternative open.

### Exit criteria

- Every architecture is exact and truthfully identified.
- The hidden-fan-in holdout is no worse than `SP-S0` under affected-family
  p50/p95 gates or remains statically on `SP-S0`.
- Existing outbound controls are non-inferior to S3.
- Direction and endpoint-order path semantics pass.
- At least one non-S3 architecture is implemented and measured or closed by
  the prior plan's alternative-closure rule.
- Accepted static shapes are ready for `sp-static-v4`; data-dependent shapes
  remain on v3 pending N5.

## Phase N4: Replace singleton edge-trail proliferation

### Semantic decision first

Before changing SQL, add an accepted tie-policy decision and tests for:

- two parallel kinds connecting the same endpoint pair;
- equal-length diamond paths;
- cycles and self-loops;
- physical IDs inserted in different orders;
- logical keys shared across PostgreSQL and Neo4j; and
- repeated execution under custom and generic plans.

The test oracle must distinguish “one valid shortest trail” from “all shortest
trails” and must not accidentally require Neo4j to select PostgreSQL's physical
edge ID.

### Candidate implementation

Implement `SP-S4-*-WE+MAT-M0` so recursive state retains one deterministic
witness per accepted node/depth state. Candidate techniques may include a
frontier relation with one predecessor row, a shortest-depth relation followed
by constrained witness reconstruction, or another architecture with the same
bounded state identity.

Reject an implementation that:

- builds all full edge arrays and deduplicates after recursion;
- hydrates every tied path before `LIMIT 1`;
- uses `allShortestPaths` workspace under a new name;
- loses relationship uniqueness or direction; or
- relies on increased `work_mem` to pass.

### Factorial comparison

Measure search and hydration separately:

| Search | Observation | Materializer |
|---|---|---|
| S3 edge trails | ordered IDs | existing `MAT-M0` |
| S4 witness | ordered IDs | existing `MAT-M0` |
| S4 witness | full path | existing `MAT-M0` |
| Selected S4 search | full path | any proposed new materializer, if residual triggers it |

No materializer arm may receive precomputed inputs while the comparator pays
search unless it is labeled materializer-only and excluded from full-query
claims.

### State and spill gate

For one-path normal tiers:

- zero temp reads/writes and zero local workspace;
- zero read-only WAL;
- recursive/witness state bounded by the declared distinct node/depth formula
  plus a small constant endpoint overhead;
- no state multiplication proportional to parallel relationship-kind count
  after one witness for a node is accepted;
- no unexplained adjacent-tier time-per-edge or bytes-per-state slope above
  1.25; and
- full path hydration occurs only for the selected trail.

The seven-kind live anchor must complete without temp spill and materially
improve the 8.07-second S3 path while remaining reference-closed. If unavoidable
edge scanning keeps it in a stress tier, report that classification explicitly;
removing spill alone does not imply a normal-tier latency pass.

### Exit criteria

- Path correctness and tie policy are explicit.
- The selected witness architecture is non-dominated against S3 and `SP-S0`.
- Parallel physical edges no longer create full-trail recursive-state growth
  for singleton output.
- Distance SQL is unchanged by the path-state work.
- Multi-kind one-path activation remains off until N5 or a static S4 envelope
  independently proves a hard resource bound.

## Phase N5: Decide bounded runtime selection

### Feasibility ladder

Evaluate in this order:

1. a hard-bounded recursive-state candidate that returns an explicit overflow
   status without public rows;
2. a same-statement mutually exclusive candidate/fallback query;
3. exact fallback branch-loop and snapshot proof;
4. selector thresholds frozen from generated discovery data; and
5. unseen generated and real holdout regret.

Stop if any layer fails. Do not optimize threshold prediction before proving
overflow semantics.

### Required threshold matrix

For each distance/path budget and each selected executor, test:

- limit minus one, limit, and limit plus one;
- overflow in anchor, first recursive level, intermediate level, meeting state,
  witness reconstruction, and hydration;
- zero result and missing endpoints;
- hidden fan-in beyond a low-degree root;
- high initial degree followed by a tiny path;
- disconnected exhaustion;
- cancellation before and after overflow;
- prepared statement custom/generic reuse; and
- repeated success/overflow/fallback on one physical connection.

### Numeric gates

Retain the prior selector gates:

```text
maximum p50 selector-regret UCB <= 1.15
maximum p95 selector-regret UCB <= 1.25
decision overhead <= max(0.10 ms, 5% of selected-arm latency)
fallback-control p50/p95 UCB <= 1.05
```

Also require:

- discarded candidate work is bounded by the recorded state limit;
- complete overflow plus fallback stays within the case timeout and resource
  ceiling;
- no partial result or duplicate result branch;
- no temp spill in a selected normal-tier S4 arm;
- fallback workspace cleanup and zero persistent mutation; and
- selector decisions and reasons match actual branch loops.

### Exit criteria

One of two explicit dispositions is recorded:

- `sp-bounded-v1` passes and reopens only its proven inbound and/or multi-kind
  envelope; or
- runtime selection is closed, `StateLimit` remains unused in production, and
  static v3/v4 fallback remains the final safe disposition.

Failure to invent a runtime selector is an acceptable completion. Shipping an
unbounded probe is not.

## Phase N6: Separate all-shortest program

### Architecture

Retain `ASP-A0` as the exact incumbent and prototype `ASP-A1-DAG`:

1. discover the minimum target depth with compact node state;
2. retain every relationship-distinct predecessor edge that participates in a
   minimum-depth route;
3. stop search beyond the minimum depth;
4. enumerate complete paths only through the resulting predecessor DAG; and
5. hydrate each emitted path once.

Unlike singleton witness state, all equal-depth predecessor edges may be
semantically required. The plan must distinguish unavoidable output
cardinality from avoidable search/workspace overhead.

### Matrix

Run:

- diamonds of width 1/2/10/100;
- parallel kinds 1/2/7/16/30;
- depth 1/2/4/8/16;
- products that yield 0/1/10/100/1,000+ shortest paths;
- disconnected and cyclic controls;
- `RETURN p`, `nodes(p)`, `relationships(p)`, and count forms where supported;
- limit pushdown forms only when semantics permit; and
- cancellation while searching and while draining large output.

Record minimum-depth states, predecessor-DAG rows, enumerated paths, result
bytes, first-row time, drain time, hydration time, temp I/O, and cleanup.

### Gates

- Exact relationship-distinct result multiset matches both incumbent and
  backend-equivalent logical oracle.
- Search does not continue beyond minimum depth.
- The seven-parallel-one-hop and ten-diamond controls materially improve or
  receive an explicit architecture closure.
- Normal output tiers have no unexplained temp spill or session-state leak.
- Large-output stress is cancellable and reports output-proportional cost; it
  is not required to meet a singleton latency SLA.
- Singleton selector code and identities are untouched.

### Exit criteria

`ASP-A1-DAG` is independently accepted and activated for a bounded exact
envelope, or it is rejected with durable evidence and `ASP-A0` remains the
documented implementation. No unfinished all-shortest work blocks completion
of singleton shortest safety.

## Phase N7: Count and hydration residual decisions

### Exact count decision

First obtain an explicit product objective for exact counts, including:

- required query shapes: all nodes, one node kind, all relationships, one
  relationship kind, or more complex label combinations;
- freshness and transaction-snapshot requirements;
- target p50/p95 latency;
- acceptable write amplification and contention; and
- bulk-import/migration constraints.

If no objective is accepted, retain `COUNT-C0`, document the measured 1.88-3.06
second large-edge cost, and close count work for this continuation.

### Count architecture tournament

If triggered, compare:

#### `COUNT-C1`: invariant-backed edge-only count

This candidate is eligible only if the database enforces that every edge
endpoint exists in the same graph for every driver, bulk load, raw import,
update, delete, rollback, and migration path. Evaluate composite endpoint
foreign keys, validated constraint triggers, or another database-enforced
mechanism. The existing delete trigger alone is not sufficient proof.

Migration planning must include a full orphan audit, lock duration, validation
strategy, rollback, write overhead, and partition behavior on supported
PostgreSQL versions. Only after the invariant is enforced may the translator
remove endpoint joins for the exact simple edge-count envelope.

#### `COUNT-C2`: transactionally maintained summary

Use a graph/kind keyed exact summary only if C1 cannot meet the count objective.
Define transactional updates for node creation/deletion/kind changes, edge
creation/deletion/kind changes, graph deletion, bulk import, rollback, and
concurrent writers. Multi-kind node counts require one counter per label or a
deliberately narrower query envelope.

Measure row-lock contention and write amplification. Sharded counters that
require summing shards may be valid if the read remains exact in the statement
snapshot. Eventually consistent or estimated summaries are out of scope for
Cypher `count()`.

### Count correctness and performance gates

- Exact values match endpoint-preserving `COUNT-C0` under generated mutations,
  rollback, concurrent writes, node deletion, edge deletion, kind changes,
  graph deletion, bulk load, and graph-ID collisions.
- Orphan attempts are rejected or represented according to the explicit
  invariant; they never make C1 silently disagree with C0.
- Count read latency meets the accepted product SLA.
- Write p50/p95, throughput, lock wait, WAL, and storage overhead stay within
  the predeclared budget.
- Migration is resumable or safely restartable and has a forward rollback.
- Unsupported count shapes retain C0 with a specific diagnostic reason.

### Hydration-tail attribution

Repeat the real-data horizontal cases with release-grade sampling before
opening code work:

- 10/100/1,000 ID lookups and full-node hydration;
- typed scan IDs and full nodes;
- outbound/inbound one-hop ID and full-object rows;
- path ID search versus M0 hydration; and
- cold/warm, raw-pgx, decode, first-row, drain, allocation, retained-byte, and
  result-byte boundaries.

The 79.84 ms maximum on the 1,000-user ID scan is a trigger only if it
reproduces in p95/plan/host evidence. Attribute cache misses, server execution,
pool wait, transfer, decode, GC, and consumer drain before proposing code.
Open relationship-ID continuation, decode batching, or another horizontal
candidate only from a stable residual and confirm it independently.

### Exit criteria

- Count work is accepted, rejected, or explicitly not triggered by product
  objectives.
- Any accepted count candidate preserves exactness and write budgets.
- Hydration tails have a stable attribution or are closed as noise/unavoidable
  payload cost.
- No speculative horizontal change enters the cumulative binary.

## Phase N8: Same-data Neo4j and complete-suffix ADCS qualification

### Identity-equivalent Neo4j dataset

Load a clone of the sanitized logical graph into Neo4j using a migration
manifest that records:

- a stable logical node key independent of backend physical IDs;
- node kinds and canonical property hash;
- edge logical key, start/end logical keys, kind, and canonical property hash;
- total and per-kind cardinalities;
- duplicate/missing-key checks; and
- a backend-independent Merkle or sorted-stream content digest.

Do not put raw identifying properties in the artifact. Validate all counts and
digests after load. Create only the indexes/constraints required by the
declared production-equivalent Neo4j setup and record their definitions,
database version, memory/page-cache settings, storage size, and host context.

Run the same logical anchor matrix, query parameters, observation contract,
timeout classes, warm/cold classification, and concurrency levels. Physical
IDs and plans are backend-specific; logical observations must match.

Publish:

- PostgreSQL and Neo4j p50/p95/throughput deltas with environment caveats;
- first-row and drain deltas;
- result-size and materialization deltas;
- backend plan/operator summaries; and
- unsupported-mode declarations, including PostgreSQL directionless
  variable-length traversal.

These ratios are descriptive. PostgreSQL release gates continue to compare
against the immediate PostgreSQL predecessor and best correct PostgreSQL
reference.

### ADCS qualification

The current sanitized graph has zero `TrustedForNTAuth` relationships and
cannot exercise a complete ADCS suffix. Do not infer A3 viability from its
10-15 ms missing-suffix controls.

ADCS reopens only when either:

- an identity-safe real dataset contains complete `Enroll ->
  TrustedForNTAuth -> NTAuthStoreFor` paths; or
- the existing exact ADCS v2 fixture is scaled to a separately declared
  real-like topology and used as synthetic qualification, with no real-data
  claim.

The matrix retains zero/sparse/dense reachable suffixes, false boundaries,
disconnected suffixes, high reverse fan-in, multiplicity, depths through 64,
payload, endpoint/path observations, and auto/custom/generic planning. The
strict A3 thresholds from `perf_cont_4.md` remain unchanged. If no qualifying
real dataset appears, ADCS stays on the incumbent and this phase closes by
explicit data-coverage disposition.

### Exit criteria

- The same logical graph is proven on PostgreSQL and Neo4j before real-data
  backend deltas are published.
- Every compared query has matching logical observations.
- Environment differences and unsupported shapes are explicit.
- ADCS either passes on complete-suffix evidence or remains closed without
  weakening its gates.

## Phase N9: Cumulative release qualification and activation

### Activation order

Use independently reversible steps:

```text
current production
  -> sp-static-v3 containment
  -> accepted sp-static-v4 S4 distance envelope
  -> accepted S4 one-path witness envelope
  -> sp-bounded-v1 only if N5 passes
  -> accepted ASP envelope, independently
  -> accepted count envelope, independently
  -> any independently triggered horizontal increment
```

ADCS remains an independent branch. Same-data Neo4j reporting does not alter
PostgreSQL selection.

### Full validation workflow

For every relevant code increment:

1. run focused unit, optimizer, translator, renderer, fixture, and GraphBench
   tests;
2. update source translation/template/mutation cases and generated artifacts;
3. run `make format`;
4. run `make test`;
5. run `make test_all` once with the supplied PostgreSQL connection selected;
6. run `make test_all` once with the supplied Neo4j connection selected;
7. run PostgreSQL-scoped plan/resource integration tests;
8. run focused race tests for shared benchmark/runtime state;
9. run cancellation, rollback, temporary-workspace cleanup, and physical-
   session-reuse tests;
10. run complete generated corpus and exact backend observation validation;
11. run matched predecessor/candidate confirmation with saved binaries; and
12. run the cumulative concurrency and soak matrix.

The backend selected by `CONNECTION_STRING` is the only integration backend
run in that invocation. Shared integration expectations stay backend-
equivalent; PostgreSQL-only SQL and plan assertions remain driver-scoped.

### Concurrency, cancellation, and soak

Run one connection, half pool, full pool, and twice-pool offered load for:

- retained outbound S3;
- deep-inbound fallback;
- accepted S4 inbound;
- single- and multi-kind one-path;
- runtime overflow plus fallback, if present;
- all-shortest small and bounded-large output;
- count reads mixed with writes, if C1/C2 is present;
- ID and full-object hydration; and
- a mixed production-weighted workload.

Require correct results, bounded whole-pool memory, no state or temporary-table
leak, and oversubscription expressed through pool wait rather than unbounded
backend state. A cancelled 100 ms search must return control within the
existing 250 ms bound, and an exact query must succeed on the same physical
session afterward.

Run at least the inherited 10,000-operation shortest soak for each newly
activated S4 observation boundary, including prepared-plan reuse and
connection churn. Runtime fallback, if present, receives a mixed
success/overflow soak rather than success-only traffic.

### Exit criteria

- All relevant tests and both backend integration invocations pass.
- Every declared corpus record is present with expected status.
- Exact observations, plans, resources, selectors, and branch loops match.
- Each activated increment passes immediate-predecessor non-inferiority and
  same-boundary PostgreSQL reference closure.
- No normal-tier selected portable candidate spills, uses local workspace, or
  emits read-only WAL.
- Cancellation and session reuse pass after every outcome.
- Each activation has a tested forward rollback to the previous selector or
  executor.
- A clean cumulative PostgreSQL/Neo4j report and residual ranking are
  published.

## Qualification matrices

### Shortest semantic matrix

| Dimension | Required coverage |
|---|---|
| Endpoint | present, missing root, missing terminal, same endpoint |
| Graph | default graph, alternate graph with colliding IDs |
| Direction | outbound, inbound, directionless fallback |
| Depth | 0/0, 0/1, 1/1, 1/2, 1/3, 1/8, 1/16, 1/32, 1/64, unsupported open bound |
| Topology | linear, hidden fan-in, mirrored fan-out, diamond, cycle, self-loop, disconnected |
| Kinds | one, two, seven, many; allowed and wrong-kind decoys |
| Observation | length, path, nodes, relationships, endpoint projection |
| Context | alias, `WITH`, optional, correlated, multipart, mutation, two shortest calls |
| Predicate | endpoint IDs, labels, relationship variable/property, path predicate |
| Outcome | candidate, static fallback, overflow fallback, cancellation, expected error |

### Performance and resource matrix

Every primary shortest point captures:

- E2E p50/p95/max and raw-pgx server/client boundaries;
- compile/optimize/translate/render time and allocations;
- planning and execution time;
- first-row and drain time;
- recursive, frontier, predecessor, meeting, and hydration rows;
- examined edge loops by physical direction;
- shared/local/temp buffers, temp bytes where available, and WAL;
- result rows and bytes;
- process/backend memory where reproducibly observable;
- SQL and plan fingerprints;
- custom/generic plan identity;
- selected/applied/runtime/fallback diagnostics; and
- concurrency QPS, pool wait, and p95.

### Count mutation matrix

If count work triggers, test within rollback-isolated generated fixtures:

- create/delete node;
- add/remove one node kind and multiple node kinds;
- create/delete/update relationship and kind;
- delete a node with inbound/outbound/self-loop relationships;
- attempted orphan and cross-graph endpoint;
- transaction rollback and savepoint rollback;
- concurrent writers touching the same and different kinds;
- bulk load, failed bulk load, and graph deletion; and
- migration from preexisting clean and intentionally orphaned clones.

These are graph mutations only in disposable or rollback-isolated databases.
They never run against the read-only sanitized live graph.

## Statistical and gate contract

### General inherited gates

Unless a stronger phase gate applies:

```text
target improvement median-ratio UCB <= 0.90
median-saving LCB >= max(case A/A resolution, 0.10 ms)
affected-family p50 and p95 ratio UCB <= 1.05
raw production / best correct reference UCB <= 1.10
```

Use ten independently reloaded matched rounds, alternating arm order, 20
untimed warmups, 50 warm measurements for normal-tier primary cases, bootstrap
matched round medians, stratified p95, recorded random seed, and 97.5%
intervals or the prior Holm adjustment. Extension is predeclared and never
selected after reading the desired direction.

The complete-corpus 20% gate remains an emergency ceiling, not permission for
an unexplained smaller regression.

### Topology-specific gates

Deep-inbound accepted candidate:

- p50/p95 UCB no greater than 1.05 versus `SP-S0` on hidden-fan-in controls;
- material improvement versus the contained production predecessor where the
  predecessor is `SP-S0` E2E;
- no regression beyond affected-family bounds on direct inbound and mirrored
  outbound controls;
- no search beyond minimum depth; and
- zero normal-tier spill, local workspace, and read-only WAL.

Singleton witness accepted candidate:

- exact one-path validity and accepted tie behavior;
- recursive state follows distinct node/depth witnesses rather than physical
  edge-trail multiplicity;
- zero normal-tier spill;
- material improvement on K7 path stress and non-inferiority on K1/small ties;
- hydration only after winner selection; and
- distance mode remains SQL-fingerprint-identical to its accepted predecessor.

Runtime policy retains the stricter selector-regret gates stated in N5.

### Absolute tiers

Freeze tier membership before confirmation:

- **normal:** expected routine production shape; must gather formal p95 and
  pass all no-spill/resource gates;
- **envelope:** largest shape eligible for automatic selection; must finish
  within the inherited two-second timeout unless a stricter family gate
  applies; and
- **stress:** diagnostic topology or unavoidable output volume; may use longer
  timeout and fewer samples, but must remain exact, cancellable, and bounded by
  its declared resource ceiling.

A case cannot be moved from normal/envelope to stress after it fails. Such a
change requires a new versioned product-envelope decision and fresh holdouts.

### A/A and host validity

Abort or invalidate a block on mismatched source, binary, fixture, schema,
relation sizes, settings, result, connection, maintenance activity, plan class,
or host saturation. Run same-binary within-session A/A and block/reload A/A for
new heavy fixtures. Capture cache state and do not mix cold diagnostics into
warm confirmation.

## Implementation seams

### Optimizer and diagnostics

Primary files:

- `cypher/models/pgsql/optimize/lowering.go`
- `cypher/models/pgsql/optimize/lowering_plan.go`
- `cypher/models/pgsql/optimize/optimizer_test.go`

Expected changes include new executor/fallback identities, physical direction
and kind-count facts, selector versions, stable reason precedence, and
statement-wide finalization tests. `StateLimit` becomes meaningful only with an
accepted N5 policy.

### PostgreSQL translation

Primary files:

- `cypher/models/pgsql/translate/pattern.go`
- `cypher/models/pgsql/translate/expansion.go`
- `cypher/models/pgsql/translate/model.go`
- `cypher/models/pgsql/translate/translator.go`
- `cypher/models/pgsql/translate/optimizer_safety_test.go`
- `cypher/models/pgsql/translate/expansion_test.go`
- translation source cases and generated SQL under
  `cypher/models/pgsql/test/`

Keep separate builders for S3, canonical S4, bidirectional S4, singleton
witness, and ASP DAG state. Shared helpers may be factored only when SQL
fingerprints and architecture identities remain truthful.

### Fixtures and GraphBench

Primary files:

- `testutil/perf_fixtures.go` or focused adjacent fixture files;
- `benchmark/testdata/scale/cases/generated_shortest_paths.json`;
- `benchmark/testdata/scale/README.md`;
- `cmd/graphbench/datasets.go`;
- `cmd/graphbench/types.go`;
- `cmd/graphbench/results.go`;
- `cmd/graphbench/postgres.go` and `postgres_plan.go`;
- `cmd/graphbench/neo4j.go`;
- `cmd/graphbench/references.go`;
- `cmd/graphbench/perf_gate.go` and reference reports;
- `cmd/graphbench/concurrency.go`;
- `cmd/graphbench/selection.go` and run-lock/checkpoint support; and
- `cmd/graphbench/README.md`.

Add focused tests beside each component. Corpus declarations, generated
fixture expectations, and backend modes change together.

### Counts and schema

Primary files if N7 count work triggers:

- `cypher/models/pgsql/translate/count_fast_path.go`;
- count optimizer/translator tests;
- PostgreSQL schema and migration SQL under `drivers/pg/query/sql/`;
- PostgreSQL graph write/delete/bulk-load paths;
- integration mutation cases; and
- count benchmark declarations and plan invariants.

Any endpoint constraint or maintained-summary schema has a forward migration,
compatibility/version handling, and compensating rollback. Do not edit the
schema merely to make the benchmark query shorter.

### Documentation

Update, as behavior lands:

- `README.md` for benchmark/test workflow changes;
- `cmd/graphbench/README.md` for live read-only and adaptive protocols;
- `benchmark/testdata/scale/README.md` for v2 fixtures and tiers;
- `docs/performance_plan_completion.md` for the revised production envelope;
- release notes for selector versions and fallback reasons; and
- a final continuation-5 report with every accepted/rejected disposition.

## Observability contract

Per shortest target, expose without high-cardinality labels:

- selector version;
- planned candidates;
- selected and applied executor;
- observation mode;
- direction and physical expansion;
- minimum/maximum depth;
- relationship-kind count;
- static eligibility facts;
- state limit when nonzero;
- runtime selected/fallback executor;
- overflow indicator and stable reason; and
- materializer identity.

Diagnostic output must distinguish compile-time fallback from runtime
overflow. It must not expose endpoint IDs, relationship IDs, query text,
properties, or logical anchor keys in metrics labels.

GraphBench artifacts may contain redacted case-local parameters necessary for
reproduction, but production metrics use bounded enumerations only. Plan/state
counters remain benchmark diagnostics unless a low-overhead production source
is proven.

For count candidates, record selected architecture and fallback reason, but do
not emit graph/kind combinations as unbounded production metric labels.

## Rollout and rollback

### Rollout

1. Land N0 evidence/docs with no behavior change.
2. Land v3 diagnostics and tests, proving incumbent SQL unchanged.
3. Activate `sp-static-v3` as a narrow forward source change.
4. Land N2 benchmark/fixture support with no production selector expansion.
5. Land each S4/ASP/count candidate behind deterministic tool forcing only.
6. Qualify and activate one observation/envelope at a time with a new selector
   version.
7. Add runtime policy only after complete N5 evidence.
8. Run cumulative release qualification and publish the clean rerun.

Do not leave public environment toggles that silently select experimental SQL.
A build-tagged or tool-only force seam may remain for deterministic regression
and benchmark coverage.

### Rollback

- V3 rolls back through a forward source change selecting the previous
  executor policy; do not revert history.
- Each S4 activation rolls back to the immediately preceding selector version
  without removing semantic fallback tests.
- Runtime selection rolls back to the accepted static v3/v4 envelope.
- ASP rolls back independently to `ASP-A0`.
- Count SQL rolls back to `COUNT-C0`; schema rollback preserves data and is
  rehearsed before activation.
- Benchmark fixtures and rejected reference arms remain as evidence even when
  production code is removed.

Rollback verification includes SQL/plan identity, exact output, cancellation,
temporary workspace cleanup, and session reuse. A rollback that restores
latency but leaves a count trigger, helper, or summary write path active is
incomplete.

## Risk register

| Risk | Consequence | Mitigation |
|---|---|---|
| Selector overfits one inbound chain | Pathology moves to mirrored topology | Mirrored generated holdouts and no root-degree-only promotion |
| Conservative v3 hurts direct inbound queries | Known latency regression | Measure every cap; publish regret; replace only with qualified S4/guard |
| Witness dedup changes tie selection | Compatibility break | Decide tie contract first; deterministic logical/physical tests |
| Recursive SQL “limit” does not cap executor work | False safety | Plan-derived threshold tests and reject unprovable guard |
| Candidate overflows then pays full fallback | Worse tail and resource use | Gate total regret; retain static fallback if it fails |
| Increased `work_mem` hides state growth | Host-level instability returns | Fixed production-equivalent settings and state/slope gates |
| Large fixtures make CI unusable | Coverage is skipped or unstable | Small shared semantic tier; explicit benchmark normal/envelope/stress tiers |
| Adaptive samples bias conclusions | False performance claim | Discovery-only label; fixed independent confirmation |
| Existing-graph runner mutates data | Sanitized dataset damage | Read-only mode, write rejection, before/after counts, clone for schema work |
| Count endpoint joins are removed without invariant | Incorrect orphan counts | C1 requires database-enforced endpoint existence and mutation proof |
| Maintained counters serialize writers | Write throughput collapse | Predeclared write/concurrency gates and C0 fallback |
| Count migration locks 44M-edge graph | Operational outage | Clone rehearsal, staged validation, lock budget, forward rollback |
| Backend physical IDs are compared | False semantic mismatch | Logical keys and backend-independent content/path digest |
| Neo4j environment differs | Misleading speed winner | Descriptive deltas with environment manifest, no PG pass/fail use |
| ADCS missing suffix is treated as a win | Invalid activation | Require complete suffix and preserve prior strict gates |
| `allShortestPaths` output explosion is hidden | Unbounded memory/drain | Separate ASP metrics, output tiers, exact cancellation, no truncation |
| Plan cache changes architecture behavior | Prepared-query regression | Auto/custom/generic and first-use/reuse qualification |
| Temp workspace survives cancellation | Pool contamination | Cleanup and same-connection exact-query tests after every outcome |

## Durable artifact layout

Use a checksum-bound tree such as:

```text
artifacts/perf/continuation-5/
  manifest.json
  baseline/
  containment-v3/
  fixtures-v2/
  live-runner/
  shortest-direction/
  shortest-witness/
  shortest-selector/
  all-shortest/
  counts/
  hydration/
  neo4j-same-data/
  adcs-complete-suffix/
  release/
  REPORT.md
```

Every experiment directory contains, where applicable:

- source and dirty-tree manifests;
- executable SHA-256 and build metadata;
- corpus declaration and selection identity;
- fixture configuration, logical checksum, and physical cardinality;
- environment, schema, index, relation-size, and settings manifests;
- raw JSONL samples and progress/checkpoint record;
- compiled SQL and normalized fingerprint;
- PostgreSQL JSON plans and parsed metrics;
- Neo4j plans/operators;
- A/A and reference-pair reports;
- performance, resource, and selector gate reports;
- exact observation report;
- concurrency/cancellation/soak output; and
- a concise disposition with rollback identity.

No artifact contains credentials, unredacted connection strings, or raw
sensitive properties. Existing live-v2 files are copied or referenced by hash;
they are not rewritten to make later results look uniform.

## Reviewable implementation sequence

Keep changes independently attributable. The intended review sequence is:

1. Freeze N0 evidence and update the declared envelope documentation.
2. Add shortest direction/kind diagnostics with zero SQL change.
3. Add v3 fallback facts, reasons, optimizer tests, and translation artifacts.
4. Activate and qualify `sp-static-v3`.
5. Add shortest fixture v2 generation, parser, metadata, and small semantics.
6. Add normal/envelope/stress v2 corpus declarations.
7. Add GraphBench existing-graph progress/checkpoint/adaptive discovery mode.
8. Add state/resource and descriptive backend-delta reports.
9. Implement and force `SP-S4-C-D`; run direction tournament.
10. Implement and force `SP-S4-C-WE+MAT-M0`; settle tie policy.
11. Implement native bidirectional candidates or publish feasibility closure.
12. Select and qualify the static S4 envelope; activate `sp-static-v4` if it
    passes.
13. Prototype `SP-G1` overflow signaling and same-statement fallback.
14. Run threshold/holdout regret; activate `sp-bounded-v1` or close it.
15. Implement and tournament `ASP-A1-DAG` independently.
16. Make the exact-count product decision; implement C1/C2 only if triggered.
17. Complete hydration-tail attribution and open only reproduced residuals.
18. Load and validate the identity-equivalent Neo4j dataset; publish deltas.
19. Run complete-suffix ADCS qualification or retain the closed disposition.
20. Build cumulative binaries, run all validation/concurrency/soak, activate
    accepted increments, and publish the final report.

Tests and documentation land with each behavior. Do not defer correctness,
generated artifacts, or rollback work to a final cleanup change.

## Immediate next actions

Execute these first:

1. Retain and checksum the expanded live-v2 report and raw artifact set.
2. Add direction, physical-expansion, and relationship-kind fields to shortest
   decisions without changing emitted SQL.
3. Add optimizer tests proving how inbound syntax maps to `start_id` versus
   `end_id` expansion.
4. Implement `sp-static-v3` and the two stable fallback reasons.
5. Confirm deep inbound, direct inbound, retained outbound, and multi-kind
   fallback arms on the sanitized PostgreSQL graph.
6. Add the hidden-intermediate-fan-in v2 fixture before writing a new search
   candidate.
7. Add the high-cardinality parallel-kind fixture and exact state metadata.
8. Move progress, timeout escalation, sample reduction, and checkpoint/resume
   into GraphBench existing-graph mode.
9. Prototype canonical source-oriented distance search and verify original
   endpoint/path orientation.
10. Freeze the singleton tie-policy decision before implementing witness
    deduplication.

Do not begin with global memory tuning, an endpoint-join removal, a maintained
counter, another ADCS selector, or a Neo4j speed claim.

## Definition of done

This continuation is complete when:

- the expanded real-data evidence and revised production boundary are durable
  and checksum-bound;
- `sp-static-v3` contains deep physical-inbound and multi-kind one-path shapes
  with truthful diagnostics and exact `SP-S0` fallback;
- retained outbound S3 SQL and performance remain non-inferior;
- direct-inbound containment regret is measured and published;
- generated fixtures reproduce hidden downstream reverse fan-in, mirrored
  fan-out, and high-cardinality parallel-kind state;
- fixture names, metadata, checksums, physical cardinalities, and logical path
  keys are deterministic and tested;
- GraphBench can safely, progressively, and resumably qualify an existing
  graph without mutation or credential leakage;
- canonical source-oriented and genuine bidirectional shortest alternatives
  are implemented and measured or explicitly closed;
- any accepted S4 distance/path architecture is exact, reference-closed,
  resource-bounded, and non-dominated in its declared envelope;
- singleton one-path state no longer proliferates complete trails by physical
  parallel-edge multiplicity, or that architecture is rejected with durable
  evidence and the shape remains on fallback;
- the singleton tie contract is explicit and backend-independent where
  required;
- runtime state selection either passes hard-cap, same-snapshot, regret,
  cancellation, and branch-loop gates or is explicitly closed;
- no partial overflow result can escape and `StateLimit` is not cosmetic;
- all-shortest has an independent exact accepted/rejected disposition and does
  not share singleton activation;
- exact count work has a product-triggered accepted/rejected/not-triggered
  disposition, with endpoint semantics and write costs preserved;
- hydration tails are reproduced and attributed before any new horizontal
  implementation is accepted;
- PostgreSQL and Neo4j are compared only after logical dataset identity and
  exact observations are proven;
- ADCS is evaluated only with a complete suffix or remains explicitly closed;
- focused, unit, template/mutation, PostgreSQL integration, Neo4j integration,
  race, plan/resource, cancellation, rollback, session-reuse, concurrency, and
  soak validation pass for every accepted increment;
- every activated selector/executor has a tested forward rollback;
- the final artifact bundle reconstructs every causal and comparative claim;
  and
- remaining work is ranked by measured addressable cost and production
  frequency, then accepted, rejected, not triggered, or opened as a new
  bounded continuation.

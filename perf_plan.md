# DAWGS performance context and next-work plan

Date: 2026-08-13 UTC

Status: the current traversal implementation is test-green; several new
executors are available through guarded production canaries, but promotion
closure and broader default selection remain evidence-gated.

Revision note: the required repository-wide `make format` target now passes via
an explicit wrapper-managed `goimports` path. Unit tests and the complete
PostgreSQL and Neo4j `make test_all` runs passed for the current revision.
"Test-green" is not a release-ready clean-source claim.

## Purpose

This document is the handoff context for the next performance iteration. It
records the source state, local test connections, current production/default
selection, fresh benchmark results, known limitations, and the most likely
next optimization work in recommended order.

The governing design and implementation records are:

- [CySQL traversal priorities](docs/cysql_traversal_priorities.md)
- [Traversal implementation status](docs/experiments/traversal_priority_implementation_status_v1.md)
- [Recursive-descent cost controls](docs/recursive_descent_cost_controls.md)
- [PostgreSQL translation](docs/postgresql_translation.md)
- [Inline ASP experiment](docs/experiments/asp_i1_inline_v1.md)

## Source and author context

The latest broad cross-backend capture was built from:

| Field | Value |
| --- | --- |
| Base commit | `94f6dd570768d8686841b4d4e31841e9c9178d80` |
| Benchmark dirty-diff SHA-256 | `9dead8b2e76d331f1c7fbb00ad27a854f0691b2aeae8ccce6e2723f2569ccca4` |
| Benchmark binary SHA-256 | `181b0352c2032d929683167f83f35116bf4f1ebbf12491fb28a565917a4cc403` |
| Corpus SHA-256 | `771ee99e7197f8948d6137997b1f00cab3f8c5f5be4ca637e429b53a4ebdb291` |
| Go | `go1.26.5-X:nodwarf5`, linux/amd64 |
| Host | 20 logical CPUs, Intel i9-12900HK, performance governor |
| PostgreSQL | 17.10; `plan_cache_mode=auto`; `work_mem=512MB` |
| Neo4j | 4.4.44, interpreted runtime and COST/IDP planner |

The corpus digest in this table is for the full automatic-production capture.
Focused studies use selection-specific corpus digests because each resolves a
smaller declaration cohort; they share the source and binary identities above.

The traversal implementation has since been preserved in clean commits. The
newest clean ASP timing study was captured from:

| Field | Value |
| --- | --- |
| Source commit | `84f38758b2ffaa48e3404310c5dba9c44061b8db` |
| Source archive SHA-256 | `4c8c846dbe54088409a1e60524a6df7bb60e3a3347f17c5c421306840fe6aa37` |
| Benchmark binary SHA-256 | `0fce1470f6fc29966b5cebc3beeba087d81f6f7f79cc504d36ac481d3cfe28b5` |
| Full corpus SHA-256 | `ff889d180965ee3fd9d6f9c0e9c49c145f37184a4008bfb834fede88a26d20ed` |
| Dirty-diff SHA-256 | empty-input SHA-256 (`e3b0c442...b855`) |

The table above identifies the newest ASP A/A and causal artifacts. It does
not replace the older source identity for the broad PostgreSQL/Neo4j capture.

The `.coverage` captures are local ignored artifacts. The historical broad
captures are diagnostics; the `qualification-84f3875` timing artifacts have a
clean source identity but remain a failed qualification, not release evidence.
None may be represented as rollout authorization.

The current tree and every raw artifact cited by this document were preserved
in `.coverage/perf-plan-diagnostic-20260813.tar.gz` before revision work began;
its SHA-256 is
`c973858ac8afc1e86fec5f4f4456010f5b9c4a15d1754227b31cf97fa82e3e64`.
Recomputing the working-tree fingerprint while excluding this document exactly
reproduced the captured dirty-diff digest above. The original benchmark
executable was no longer present and is therefore not included; the archive is
a diagnostic preservation artifact, not a portable promotion bundle.

## Local database connections

These are disposable local test targets supplied for this work:

```bash
export PG_CONNECTION_STRING="postgres://postgres:bhe4eva@127.0.0.1/bhe"
export NEO4J_CONNECTION_STRING="neo4j://neo4j:neo4jj@127.0.0.1:7687"

export DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1
export DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://127.0.0.1:5432/bhe,neo4j://127.0.0.1:7687/<default>"
```

The allowlist is deliberately credential-free and uses the normalized target
identity. Never add a live benchmark or production database to
`DAWGS_INTEGRATION_DISPOSABLE_TARGETS`. GraphBench clears and reloads fixtures.
Run only one destructive GraphBench process per target at a time.

For backend-complete validation, run each scheme separately:

```bash
CONNECTION_STRING="$PG_CONNECTION_STRING" make test_all
CONNECTION_STRING="$NEO4J_CONNECTION_STRING" make test_all
```

The captured implementation passed both commands. During the current revision,
`make format` was made configurable through `GOIMPORTS_CMD`, fixed to exclude
the ignored `.coverage` artifact tree, and passed using the wrapper-managed
`goimports` binary. The current revision then passed both backend-specific
`make test_all` commands independently.

## Current production/default execution map

### Singleton shortest path

The automatic selector is split by observation and physical envelope:

| Shape | Current default |
| --- | --- |
| Directed qualified distance | `SP-S3-U-D` |
| Deep physical-inbound distance | `SP-S4-C-D` |
| Bounded, directed, typed single-kind witness outside the deep-inbound envelope | `SP-S3-U-E+MAT-M0` |
| Deep physical-inbound witness | `SP-S4-C-WE+MAT-M0` |
| Multi-kind or untyped witness | `SP-S4-C-WE+MAT-M0` |
| Unsupported/correlated/unbounded shapes | exact legacy/incumbent path |

The witness selector is `sp-static-v5-contained`. S4 and A1 use shared
session-local workspace v2. S4 has one/two-hop preflights, bounded state, late
M0 hydration, and exact relationship-trail fallback before output.

`SP-I1-C-WE+MAT-M0` is implemented but default-off. It is a guarded inline
canonical-predecessor witness with four cap+1 gates and exact S4 fallback. Its
production path requires:

- an exact normalized-query SHA allowlist;
- a verified promotion manifest with a matching `one_path` bucket;
- positive state, predecessor, enumeration, and output-byte caps;
- Repeatable Read or Serializable isolation;
- `SP-S4-C-WE+MAT-M0` as its declared fallback.

`DisableInlineSPWitness` immediately restores the static S3/S4 selection and
changes the translation-cache identity without requiring promotion evidence.

### All shortest paths

`asp-static-v1` automatically selects `ASP-A1-DAG` for the qualified directed,
read-only singleton endpoint pair with minimum depth one. An open maximum uses
the existing depth-15 policy. Unsupported zero-depth, self-endpoint,
directionless, correlated, predicate, optional, mutation, or ambiguous shapes
retain the exact incumbent.

`ASP-I1-U-DAG+MAT-M0` is implemented but default-off. It has exact one/two-hop
preflights, cap+1 discovery/predecessor/enumeration/output-byte gates, inline
M0 hydration, an exact A1 fallback, and runtime-receipt schema v2. Like
canonical SP I1, it requires an exact query/manifest bucket and stable
transaction isolation. `DisableInlineASPDAG` is the evidence-free rollback.

### Ordinary expansion and `ExpandInto`

- Ordinary production traversal remains the stepwise forward incumbent except
  for the already-qualified endpoint-seeded reverse envelope.
- `EXPANSION-ENDPOINT-SEEDED-REVERSE` uses endpoint and state sentinels and an
  exact same-statement forward fallback.
- General `EXPANSION-SUFFIX-SEEDED-REVERSE` remains tool-only because sparse
  suffix and high reverse-fan-in topologies cross over sharply.
- `orientation-probe-v1` is available as a default-off, exact-query guarded or
  shadow policy. It is not a broad automatic default.
- Fixed one-hop `ExpandInto` translation is exact and performs strongly in the
  current corpus; no additional production selector is justified yet.

### Policy, cache, and observability

`drivers/pg.TraversalPolicy` is generation-keyed and compiled once at policy
installation. It validates the manifest digest, candidate, selector, boundary,
caps, fallback, training/holdout buckets, exact query cohort, and evidence
digests. The compiled identity partitions the translation cache, making zero
policy and kill-switch rollback immediate.

Runtime receipt schema v2 retains the complete ordered branch chain. A
canonical witness overflow may therefore report:

```text
SP-I1-C-WE+MAT-M0
  -> SP-S4-C-WE+MAT-M0
  -> SP-S3-U-E+MAT-M0
```

B1/B2 bidirectional SP and ASP schedulers remain reference/tooling arms. They
are not production-canary eligible.

## Known implementation gaps to close before promotion

The guarded executors are substantially more complete than the promotion
closure around them. The following are current code gaps, not merely missing
benchmark runs:

1. **Evidence cross-binding was incomplete in the captured source.**
   `cmd/graphbench/promotion_manifest.go` verifies each referenced report's
   SHA-256 and a role-specific pass/eligibility flag, but it does not verify
   that every report repeats and matches the manifest candidate, selector,
   source/binary/corpus digests, caps, buckets, and exact query cohort. The
   driver-side manifest model also omits source/binary/corpus fields. The
   current revision introduces promotion-manifest schema v2, a
   manifest-derived report identity, a report-binding command, exact identity
   comparison during verification, and source/binary/corpus fields in the
   driver model. Table-driven adversarial tests now reject mismatched
   candidate, selector, boundary, fallback, source, binary, corpus, cap,
   bucket, split, and query identities. Clean-source qualification remains due.
2. **Full receipt chains were not retained in timed samples.**
   GraphBench validates receipt schema v2 and its contiguous event chain, then
   reduced it to terminal identity/branch/fallback fields. The current revision
   persists the ordered events in timed samples, JSON summaries, confirmation,
   performance, resource, and reference-closure reports and validates the
   terminal event against the reduced outcome.
3. **Only one candidate family can be enabled per policy generation.**
   ASP I1, canonical SP I1, and orientation cannot currently coexist as
   canaries. A policy v2 needs independent rules/manifests/buckets and kill
   switches under one deterministic cache identity before simultaneous
   rollout.
4. **`SP-I1-C-D` remains under-guarded and is now tool-only.**
   The current revision removed production-canary eligibility while preserving
   explicit tool forcing. Reintroduce eligibility only after implementing a
   capped `I1 distance -> S4` dual arm with receipts and rollback.
5. **Canonical SP operational coverage is incomplete.**
   Direct production translation and nested fallback execution are covered,
   but live driver-policy selection plus kill-switch behavior, concurrent
   writers, low `work_mem`, plan-cache modes, cancellation, and pooled-session
   reuse need candidate-specific coverage comparable to ASP I1.
6. **Canonical SP fallback selection is not yet incumbent-relative.**
   The guarded canonical witness currently declares S4 as its fallback. If a
   future bucket admits canonical I1 over an S3 incumbent, the policy decision
   must retain and emit that exact incumbent rather than hard-code S4. Test
   nested overflow and kill-switch rollback for both incumbent families.
7. **The stale canary status table was corrected in the current revision.**
   Only canonical predecessor SP and ASP I1 are described as inline production
   canaries; legacy witness and distance I1 remain tool-only.

### Gap closure matrix

| Gap | Work item | Acceptance test |
| --- | --- | --- |
| Evidence identity | P0 shared evidence contract | Each role rejects a mismatched candidate, selector, boundary, fallback, source, binary, corpus, cap, bucket, split, or query digest. |
| Receipt reduction | P0 receipt persistence | Direct and nested fallback chains survive raw samples, summaries, and all gate reports with contiguous ordinals and a matching terminal outcome. |
| Single-family policy | Rollout policy v2 | Two independently qualified families coexist with independent kill switches and one deterministic cache identity; v1 authorization fails closed. |
| Distance containment | P4 | Production rejects `SP-I1-C-D` until four caps, exact fallback, receipts, and rollback tests pass. |
| Canonical SP operations | P3 | Live policy, kill switch, concurrent writer, low `work_mem`, generic/custom/auto plan, cancellation, and pooled-reuse tests pass. |
| Incumbent-relative SP fallback | P3 | Both `I1 -> S3` and `I1 -> S4` overflow and rollback chains are exact and fully attributed. |
| Documentation drift | P0 documentation closure | Status tables and production eligibility tests name the same executors. |

## Fresh benchmark position

### Global automatic-production capture

The latest full run used 10 warmups and 30 measured samples per case against
both local backends:

- 265/265 backend records passed;
- PostgreSQL: 132/132 passed;
- Neo4j: 133/133 passed;
- the extra Neo4j-only declaration is the directionless
  `GSP-D08-F128_path_directionless` case;
- no row mismatches or execution errors;
- PostgreSQL was faster in 90 of 132 matched cases;
- Neo4j was faster in 42 of 132 matched cases;
- median per-case PostgreSQL/Neo4j ratio was `0.305`.

The median ratio means PostgreSQL latency was about 69.5% lower for the median
case. It is an equal-weight descriptive summary across heterogeneous workloads,
not a release threshold.

The broad category picture is:

| Category | Median PG/Neo ratio | Interpretation |
| --- | ---: | --- |
| Fixed one-hop `ExpandInto` | `0.036` | PostgreSQL strongly ahead |
| Counts | `0.074` | PostgreSQL strongly ahead |
| Lookups | `0.125` | PostgreSQL strongly ahead |
| Generated ordinary SP | `0.132` | PostgreSQL usually ahead |
| Relationship scans | `0.347` | PostgreSQL ahead |
| Endpoint-seeded expansion | `0.390` | PostgreSQL ahead overall |
| Generated all-shortest control | `1.798` | PostgreSQL behind |
| Generated fixed-suffix expansion | `2.094` | PostgreSQL behind |
| Generated SP v2 topology cases | `2.976` | Mixed; hidden fan-in/inbound dominate losses |
| Base unbounded shortest cases | `7.019` | PostgreSQL materially behind |

Largest remaining database gaps include:

| Workload | PostgreSQL/Neo4j |
| --- | ---: |
| Hidden-fan-in distance stress | `60.34x` slower |
| Sparse fixed-suffix path | `57.79x` slower |
| Sparse fixed-suffix endpoint IDs | about `45.9-47.0x` slower |
| Base unbounded one-path shortest | `7.74x` slower |
| Base unbounded shortest distance | `6.30x` slower |
| ASP depth-16 stress | `5.40x` slower |
| Inbound ASP depth 8 | `5.08x` slower |
| Outbound ASP depth 8 | `4.84x` slower |

### Focused SP witness tournament

Six forced PostgreSQL arms were captured with 10 warmups and 30 samples. The
median per-case comparison was:

| Comparison | Median delta |
| --- | ---: |
| S4 versus S3 | S4 `+857.0%` slower |
| Canonical I1 versus S3 | I1 `+214.8%` slower |
| Canonical I1 versus S4 | I1 `-69.8%` faster |

Canonical I1 improved the expensive S4 cases substantially:

| Case | I1 versus S4 |
| --- | ---: |
| D16/F16 witness | `-87.0%` |
| D4/F128 witness | `-83.4%` |
| Depth-8 inbound witness | `-79.4%` |
| Hidden-fan-in witness | `-60.1%` |

However, S3 was still fastest in five of six cases. The exception was the
parallel-kind case: S4 beat S3 by 36.4% and I1 beat S3 by 18.6%. Therefore:

- do not make canonical I1 the general witness default;
- investigate a contained S3 expansion for deep inbound single-kind work;
- retain S4 for multi-kind/untyped work unless broader evidence says otherwise;
- use canonical I1 as the safer candidate replacement where S3 resource growth
  cannot be contained.

### Focused ASP tournament

ASP I1 versus A1 had a median per-case improvement of 57.2%:

| Case | I1 versus A1 |
| --- | ---: |
| Outbound depth 3 | `-56.2%` |
| Outbound depth 8 | `-63.3%` |
| Inbound depth 8 | `-58.1%` |
| Disconnected depth 8 | `-91.6%` |
| Diamond depth 2 | `+8.3%` |
| Parallel-kind depth 2 | `+9.9%` |

The likely initial I1 qualification envelope is directed, read-only,
singleton endpoints, minimum depth one, explicit maximum 3 through 64, one
typed relationship kind, complete path observation, and no path/relationship
predicate. This is a hypothesis for clean qualification, not an authorization.
A1 should remain the default for maximum depth two and shallow
multiplicity-heavy/multi-kind cases.

### Go microbenchmarks

All 40 repository benchmark functions passed with three 500 ms repetitions.
There is no matched prior artifact in this run, so these are absolute hotspot
measurements rather than regression deltas.

| Benchmark | Current result |
| --- | --- |
| Cached Cypher parse | about `218ns/op`, 0 allocations |
| Uncached Cypher parse | about `38.6us/op`, 28KB, 395 allocations |
| Owned node composite decode | about `1.29us/op`, 912B, 25 allocations |
| Owned node-array decode | about `156us/op`, 108KB, 2,820 allocations |
| Owned path decode | about `70us/op`, 51KB, 1,239 allocations |
| Fragment path loading | about `75.8ms/op`, 63.8MB, 970K allocations |
| Registry-free scrub | about `6.6-6.7s/op`, 1.53GB allocated |
| Read-only properties retained heap | about 694MB |
| Edge scrub | about `4.07us/op`, 934B, 21 allocations |

Owned composite decoding remains materially better than the map-based form.
The retriever fragment loader, scrub pass, and retained-property footprint are
the clearest non-database optimization targets.

## Evidence caveats

The broad cross-backend results remain decision-quality diagnostics: that
capture used one round from the older source identity and is suitable for
ranking work, not promotion. The newest ASP timing evidence is stronger: it
uses a clean committed source, one immutable binary, balanced host A/A, a
guarded production candidate boundary, exact observations, and complete timed
receipts. It still does not authorize rollout because the broad ASP cohort
failed p95 qualification and no resource, reference-closure, cancellation,
concurrency, or operational report set was closed into a verified manifest.

## Recommended next work and dependencies

The work is organized as a dependency graph rather than a single serial queue:

```text
diagnostic preservation
  -> safety/evidence closure
  -> format + two-backend validation
  -> authorized clean source + immutable binary
  -> A/A and incumbent baseline
  -> exact-query canaries
  -> automatic selector changes

retriever profiling -------------------------------> matched Go benchmark gate
distance and inbound-witness discovery ------------> contained canary design
unbounded SP design / unbounded ASP design --------> separate later programs
```

ASP I1, fixed-suffix orientation, inbound witness, hidden-fan-in distance, and
retriever discovery may proceed in parallel after the shared safety/evidence
contract is stable. No lane may change an automatic selector before the common
clean-source baseline and its lane-specific exact-query canary close.

### P0: Freeze a promotion-grade baseline

Goal: turn the current opportunity signals into comparable evidence.

Implementation status: steps 1-9 are complete for the ASP timing lane through
commit `84f3875`, including a clean immutable binary and a valid 12-case host
A/A report. Step 10 is complete only for A/A and causal confirmation; the
remaining resource and operational reports are deliberately deferred because
the candidate failed the step-11 p95 gate.

1. Preserve the captured diagnostic tree and raw artifacts before editing.
   Record unavailable inputs explicitly rather than reconstructing them.
2. Remove production eligibility from any executor that lacks its declared
   caps, exact fallback, receipt, and evidence-free rollback contract.
3. Make evidence reports self-identifying and cross-bind every report to the
   manifest's candidate, selector, source, binary, corpus, caps, bucket, and
   query cohort. Fail closed on absent or contradictory fields.
4. Persist the full ordered runtime receipt event chain in timed samples,
   summaries, confirmation reports, resource reports, and promotion checks.
5. Add negative tests proving that a passing report from another binary,
   selector, cap set, or query cohort cannot authorize a policy.
6. Restore `goimports`, run `make format`, and pass unit plus both backend
   `make test_all` commands.
7. With explicit commit authorization, preserve the implementation in a clean
   source state. Do not obtain cleanliness by discarding the dirty worktree.
8. Build one immutable GraphBench binary with `go build -trimpath`; record
   source, binary, corpus, and
   database identities.
9. Run the governing confirmation protocol: 10-20 independently reloaded,
   carryover-balanced rounds, at least 20 untimed warmups, and at least 50
   measured samples per arm per round. Use the predeclared Williams schedule
   appropriate to the exact arm count.
10. Produce host A/A, confirmation, performance, resource, reference-closure,
   cancellation/concurrency, and operational reports.
11. Require seeded 97.5% intervals, the existing relative-or-absolute
   5%/100us materiality rule, p95
   containment, exact result parity, complete runtime attribution, and zero
   inactive-arm work.

Multi-family policy v2 is a rollout-infrastructure dependency, not a baseline
dependency. Implement it immediately before two independently qualified
candidate families must coexist in one generation; preserve fail-closed
manifest-v1 decoding and reject it for new promotion authorization.

This is required before changing a default selector. It does not block local
implementation and diagnostic work on P1-P6.

### P1: Qualify ASP I1 for recursive typed single-kind buckets

Why first: the executor and rollback path already exist, and it improved four
of six focused cases by 56-92%.

Implementation status: corpus step 1 now includes normal-tier training cases
for early targets at depths 1/2/3 under maximums 16/64, inbound mirrors,
cyclic dead tails, reconvergence, and disconnected maximum misses. These cases
passed both backends and the forced ASP I1 exact-result/runtime-receipt check.
Cap-threshold branch behavior remains in the live guarded-statement integration
matrix because ordinary corpus declarations cannot override immutable
production caps. Statistical and operational qualification in steps 2-6 is
still due. GraphBench now accepts a provisional version-2 manifest for exact
guarded-production-boundary capture, executes its authorized queries under
Repeatable Read, and records per-sample receipts; tool-forced I1 output is no
longer the only measurable candidate boundary. Matched incumbent capture has
an explicit Repeatable Read mode so both arms satisfy the same admission
contract; Read Committed/autocommit baselines are diagnostic only.

The first clean governing capture rejected the proposed broad envelope. Commit
`494bb9b8fcf65ff90dc6e71b2c0f3cd32bba1004` used one immutable binary and a
valid 12-case A/A calibration with 10 carryover-balanced rounds, 20 warmups,
50 samples per arm per round, and 97.5% seeded intervals. All 240 A/A records
and all 240 causal-arm records completed, and every timed candidate sample had
an exact `ASP-I1-U-DAG+MAT-M0@inline_predecessor_dag` receipt. All three
holdouts and five of nine training cases cleared p95 non-inferiority. The
remaining four training cases were inconclusive: outbound and inbound one-hop
targets, the two-hop target, and reconvergence. Inbound one-hop estimated a
1.100 p50 ratio and 1.238 p95 ratio. Therefore `minimum_depth = 1` with
`3 <= maximum_depth <= 64` is not promotion-eligible and must not be recovered
by post-hoc removal of failed cases.

Query hashes and the current manifest buckets cannot enforce runtime endpoint
distance, so the passing deep cases cannot define a safe post-hoc production
bucket. At that point, the only valid next attempts were to reduce the guarded
statement's one-/two-hop overhead or introduce a parameter-independent,
fail-closed eligibility dimension before a newly predeclared cohort was
captured.
Reconvergence remains a separate topology stress bucket. No exact query hash
from this rejected envelope may be activated merely because its benchmark
parameters happened to resolve at depth three or greater.

Two clean shallow-overhead iterations followed. Commit `f6290e8` materialized
and reused the direct preflight; in a matched old/new diagnostic it improved
outbound depth-one p50/p95 by 3.75%/0.51% and inbound depth-one by
2.41%/10.71%. Commit `84f3875` then reused the materialized admission result
inside runtime attestation, removing four duplicate cap probes and one
duplicate output-byte aggregation. Both backend `make test_all` runs passed.

The final fixed 20-round confirmation at `84f3875` used 20 warmups and 50
samples per arm per round against the valid 12-case A/A report. All 480 causal
records succeeded. All 12,000 timed candidate samples had exact, contiguous
non-fallback receipts: 10,000 `inline_predecessor_dag` and 2,000
`inline_no_path`. Every holdout passed. Seven of nine training cases passed;
outbound depth one and depth two remained p95-inconclusive:

| Case | I1 versus A1 p50 | I1 versus A1 p95 | Result |
| --- | ---: | ---: | --- |
| Outbound depth 1 / max 16 | `+2.9%` | `+13.8%` | inconclusive |
| Outbound depth 2 / max 64 | `-3.4%` | `+21.9%` | inconclusive |
| Inbound depth 1 / max 16 | `+3.9%` | `+19.0%` | cleared by A/A floors |
| Reconvergence / max 16 | `-4.1%` | `-4.0%` | cleared |
| Outbound depth 3 / max 16 | `-52.1%` | `-43.2%` | cleared |
| Inbound depth 3 / max 64 | `-51.4%` | `-43.6%` | cleared |
| Disconnected / max 64 | `-95.2%` | `-90.7%` | cleared |

This satisfies the P1 stop condition: two isolated overhead iterations did not
clear the full predeclared shallow envelope. Keep A1 as the production default,
keep I1 default-off as a diagnostic tool, do not activate passing hashes from
the rejected cohort, and move primary optimization effort to P2. A future ASP
attempt requires a new parameter-independent eligibility design and a newly
predeclared cohort, not another post-hoc subset of these results.

Implementation sequence:

1. Extend the ASP corpus with early targets at depths 1/2/3 under maximums
   16/64, cyclic dead tails, reconvergence, disconnected searches, inbound
   mirrors, and cap-boundary cases.
2. Confirm complete relationship-ID path multisets, not only counts.
3. Re-run A1 versus I1 under generic/custom/auto plans, low `work_mem`, pools
   1/2/8, concurrency, cancellation, and policy-generation rollback.
4. Treat the rejected broad-envelope captures as discovery. The two planned
   shallow-overhead iterations are complete; pause this lane until there is a
   new parameter-independent eligibility rule and a newly predeclared cohort.
5. First activate exact query hashes through `TraversalPolicy` under Repeatable
   Read or Serializable isolation.
6. Keep Read Committed, reconvergence unless separately qualified, and
   multi-kind/untyped cases on A1. Do not select from observed endpoint depth
   unless that choice is enforced inside the exact guarded statement.
7. Only after canary closure consider an automatic `asp-static-v2` selector.

Primary files:

- `cypher/models/pgsql/optimize/lowering_plan.go`
- `cypher/models/pgsql/translate/expansion_all_shortest_inline.go`
- `cypher/models/pgsql/translate/translator.go`
- `drivers/pg/traversal_policy.go`
- `integration/pgsql_inline_asp_test.go`
- `benchmark/testdata/scale/cases/generated_shortest_paths_v2.json`

### P2: Put guarded fixed-suffix orientation into selected production paths

Why: sparse fixed-suffix cases remain 46-58x slower than Neo4j, while previous
forced reverse evidence showed a very large win. High reverse fan-in remains a
known crossover where reverse loses. The emitter, probes, fallback, report
generator, and default-off policy already exist, making this the most mature
non-ASP production-path opportunity.

2026-08-13 continuation checkpoint:

- A five-case discovery block confirmed that forced suffix-reverse is roughly
  31-452x faster than the forward incumbent on the two sparse cases, about 4.3x
  faster on zero-reachable, and about 4.1x faster on the cyclic bag case; it is
  about 2.6x slower on high reverse fan-in. These artifacts are diagnostic only:
  the runner had not applied requested Repeatable Read to attested tool arms.
- `orientation-probe-v1` selected reverse for the sparse pair and forward for
  zero-reachable, high-fan-in, and cyclic. The latter two reverse wins show that
  v1 cannot qualify this cohort without a new immutable selector identity.
- Shadow and guarded evidence is now marker-first for zero-row output, reports
  probe overflow, evaluates the state sentinel once, truthfully attests probe
  and state fallback, attributes exact CTE materialization bodies instead of
  repeated scans, and rejects any inactive-arm traversal loops.
- Shadow overhead exceeded the `10%`/`100us` gate on zero-reachable,
  high-fan-in, and cyclic discovery cases. Reduce probe overhead before freezing
  a new selector and opening fresh blind holdouts.
- A follow-up attempt derived completeness from the existing cap+1 probe counts
  to remove four `EXISTS/OFFSET` scans. It removed eight PostgreSQL plan nodes,
  but a 12-block, order-balanced, Repeatable Read comparison with 360 timed
  samples per arm/case did not solve the overhead gate. Pooled median deltas
  ranged from `-3.15%` to `+0.57%`; the sparse endpoint case was slower in 10 of
  12 block medians. The rewrite was rejected. Optimize the evidence probes
  themselves rather than their in-memory completeness checks.
- Shadow-only suffix evidence now projects only the boundary ID, and degree
  probes project one boolean evidence row per typed adjacency. This preserves
  every join, constraint, row multiplicity, cap, and guarded-candidate input
  while reducing suffix tuple width from `64`/`160` bytes to `8` and degree
  tuples from `16` bytes to `1`. A separate 12-block, order-balanced,
  Repeatable Read confirmation (360 timed samples per arm/case) found paired
  block-median deltas from `-7.09%` to `+0.68%`; the zero-reachable probe-plan
  median fell from `10.428ms` to `9.740ms` with identical rows and buffer hits.
  No case showed a stable total-latency regression, so retain this structural
  reduction. It does not by itself close the shadow-overhead gate.
- Production orientation manifests now bind the exact v1 caps, forward
  fallback, and `guarded_dual_arm` execution boundary. GraphBench production
  options enable expansion orientation directly, and traversal telemetry
  distinguishes guarded production from inline shadow/forced statements.
- The staged `orientation-probe-v2` identity freezes
  `F2 = root_rows + maximum_depth * forward_degree_rows` and
  `R2 = suffix_rows + boundary_rows + reverse_degree_rows`, choosing reverse
  only for complete probes with `4 * R2 < 3 * F2`. It retains the v1 caps and
  exact forward fallback on every probe or reverse-state overflow; v1 SQL,
  reporting, manifests, and current production behavior are unchanged.
- A checksum-bound v3 corpus now declares eight training cases spanning every
  encoded dimension and four holdouts at previously unused depths 7, 11, 13,
  and 15. The cases independently exercise suffix density, matching-root
  multiplicity, reverse fan-in, reachable fraction, path observation,
  relationship-distinct productive cycles and self-loops, payload, zero depth,
  and suffix multiplicity.
- The v2 reporter requires four exact matched artifacts labeled `shadow`,
  `incumbent`, `reverse`, and `guarded`. Each round must use distinct positions
  1-4 in a position-balanced rotation with one block and run UUID. Every arm is
  measured under Repeatable Read with traversal telemetry and a size-one pool;
  shadow and guarded timings additionally require per-invocation receipt chains.
  Discovery must run from a clean tree on exactly the eight canonical training
  cases and request both `-orientation-v2-output` and
  `-orientation-v2-freeze-output`. The freeze binds the policy, formula, caps,
  source commit, clean dirty-diff, binary, canonical cohort declaration, and
  discovery-report SHA-256. Confirmation requires that exact manifest through
  `-orientation-v2-freeze`, its bound training report through
  `-orientation-v2-discovery-report`, and exactly the canonical eight training
  plus four holdout cases.
- Per-case A/A evidence now binds the exact PostgreSQL timing environment,
  including transaction isolation and normalized ANALYZE state, and the exact
  validated fixture. V2 rejects any mismatch between that evidence and the
  incumbent timing artifact. The four-arm report also freezes corpus, host,
  workload, SQL, and exact public observations across arms.
- GraphBench now accepts repeated `-aa-artifact` inputs so the two explicit A/A
  labels remain separate append-safe run series and are combined by a
  checksum-bound native reporter. The capture protocol uses one clean prebuilt
  binary and one stable series UUID across every arm and appended round.
- The first clean training capture failed closed before report creation because
  the four path-observed v3 cases declared only row counts. Their exact stable
  node, relationship-kind, and logical-key path multisets are now part of the
  checked-in training/holdout declaration; the corpus contract rejects any v2
  path case without that independent oracle. Stable observation reconstructs
  repeated cycle/self-loop node positions from the ordered relationship walk,
  eliminating a Neo4j/PostgreSQL path-adapter representation difference. No
  holdout timing was opened.
- The replacement clean discovery at source commit `b4e896b` completed all
  five balanced rounds for shadow, exact forward, exact reverse, guarded, and
  both A/A arms. It failed qualification on all eight training cases, so the
  four v3 holdouts remain unopened. V2 chose the faster exact orientation in
  seven cases; its only miss cost about `9us`. By contrast, the guarded
  statement added `187-376us` over its selected exact arm. Exact reverse saved
  only `10-177us` in this cohort, so even a training-perfect threshold cannot
  amortize the same-statement selector. Shadow and guarded plans contain
  `93-95` and `120-121` nodes versus `33-35` for exact arms, locating the main
  cost in the common probe/dispatch scaffold rather than inactive-arm work.
  A direct 1,000-iteration session measurement put the armed runtime-receipt
  record call at about `9.5us` versus `1.1us` unarmed; receipt optimization
  cannot close the observed gap. Treat this as the P2 stop condition: preserve
  v2 and its freeze as failed training evidence, do not tune a v3 threshold on
  these cases, and do not open the holdouts.
- V2 gates forward-selected cases on shadow/forward overhead and every case on
  guarded/selected overhead plus guarded/fastest regret. Reverse-selected
  shadow overhead remains diagnostic rather than an automatic pass. No v2
  four-arm discovery or confirmation qualification benchmark has passed yet;
  the new identity, corpus, capture flags, and report schema only stage that
  experiment.

Implementation sequence:

1. Preserve `orientation-probe-v1` and its three-arm shadow report as immutable
   diagnostic identities; the observed zero-reachable and cyclic choices rule
   out promotion without a new selector version.
2. Predeclare a checksum-bound v2 training corpus that independently varies
   suffix density, root multiplicity, reverse fan-in, reachable fraction, path
   observation, duplicates, and cycles, plus fresh unseen holdouts. This is now
   staged as the v3 eight-training/four-holdout declaration; do not inspect
   holdout timing before the selector is frozen.
3. Add a new `orientation-probe-v2` policy identity and report schema. Require
   four matched arms: shadow, exact forward, forced reverse, and the actual
   guarded statement. The tooling and immutable report schema are staged, but
   have not produced qualifying evidence.
4. Gate every case on guarded/selected overhead and guarded/fastest regret.
   Keep shadow/forward qualification-applicable only when the selector chooses
   forward; reverse-selected shadow overhead remains diagnostic, never an
   automatic pass.
5. The clean eight-case discovery and freeze are complete and failed every
   training case on guarded overhead or regret. Preserve the artifacts as
   negative evidence; do not run confirmation or inspect holdout timing.
6. Pause P2 until a new architecture removes the common same-statement probe
   cost or exposes a parameter-independent applicability dimension with enough
   work to amortize it. Any restart requires a new identity and predeclared
   corpus rather than a threshold fitted to this failed cohort.

Primary files:

- `cypher/models/pgsql/optimize/expansion_orientation.go`
- `cypher/models/pgsql/translate/expansion_orientation.go`
- `cypher/models/pgsql/translate/expansion_suffix_seeded.go`
- `cmd/graphbench/orientation_selector_report.go`
- `cmd/graphbench/orientation_selector_report_v2.go`
- `benchmark/testdata/scale/cases/generated_fixed_suffix_expansion.json`

### P3: Replace S4 where deep inbound witnesses do not need it

Why next: forced S3 beat S4 by roughly 9.57x at the median, while canonical I1
beat S4 by roughly 3.3x. Current production still sends all deep inbound
witnesses to S4, but this change needs more resource-safety work than P2.

2026-08-13 evidence checkpoint:

- Canonical `SP-I1-C-WE+MAT-M0` evidence now uses the distinct emitted policy
  identity `sp-i1-canonical-guarded-v1`; it no longer relies on the ASP
  `asp-i1-guarded-v1` identity to expose the shared inline-predecessor SQL
  shape. The target outcome records canonical I1 as the exact candidate and
  `SP-S4-C-WE+MAT-M0` as its exact fallback.
- Traversal telemetry schema v2 serializes canonical bounded-relation and branch
  counters under `inline_shortest_path`, separately from ASP I1's `inline_asp`
  family. Named candidate/fallback markers must attribute exactly one arm, and
  the unselected output branch must remain at zero rows. Parent-linked plan
  evidence also requires the selected branch's direct inner executor to run
  and the unselected executor to remain at zero loops. Candidate execution is
  reported as `inline_canonical_witness` or `inline_canonical_no_path`; fallback
  execution is reported as `exact_s4_fallback` with the S4 runtime identity.
- This closes an evidence-attribution gap only. Canonical I1 remains a
  default-off exact-query canary, and the automatic production selector remains
  `sp-static-v5-contained` with its current S3/S4 choices.
- A non-holdout live PostgreSQL smoke on
  `GSPV2-NORMAL-hidden-fanin-path` passed resource-gate v4 with complete
  schema-v2 telemetry: candidate marker/branch/executor `1/1/1`, fallback
  marker/branch/executor `0/0/0`, 133 bounded-relation states, 132 predecessor
  entries, 4 enumerated rows, and 961 hydrated output bytes. The observed
  limits were respectively 100,000, 100,000, 100,000, and 64 MiB.

Implementation sequence:

1. Build a dedicated inbound witness tournament across depths 2/4/8/16/32/64,
   low/high fan-in, early targets, disconnected graphs, cycles, self-loops,
   reconvergence, and relationship-kind multiplicity.
2. Compare S3, S4, canonical I1, B1, and B2 at their real execution and
   hydration boundaries with branch receipts and resource counters.
3. Determine whether S3's relationship-trail state stays bounded for a narrow
   typed single-kind inbound envelope. Measure worst-case state rather than
   inferring safety from latency.
4. If S3 passes resource and p95 gates, introduce a contained `sp-static-v6`
   candidate bucket for those exact shapes, but retain the incumbent automatic
   selector.
5. If S3 cannot be safely contained, qualify canonical I1 as the replacement
   for the passing S4 buckets. Make its fallback incumbent-relative, retain
   all cap+1 gates, and test both `I1 -> S3` and `I1 -> S4` receipt chains.
6. Exercise the winning candidate through an exact-query canary with complete
   rollback and operational closure before changing `sp-static-v6` defaults.
7. Keep S4 for multi-kind/untyped witness work until independently disproven.

Do not globally select S3 merely because it won the six-case diagnostic set;
its unbounded relationship-trail growth is the reason the deep-inbound envelope
was previously contained.

### P4: Fix hidden-fan-in distance search

Why: the stress case is the largest shortest-path deficit at 60.34x Neo4j,
and normal hidden-fan-in distance is also materially behind. This follows P3
because the currently eligible I1 distance arm still lacks equivalent caps,
fallback validation, and a dedicated rollback switch.

Implementation sequence:

1. Run `SP-S3-U-D`, `SP-S4-C-D`, `SP-I1-C-D`, B1 distance, and B2 distance on
   normal, holdout, disconnected, cyclic, early-target, and stress fan-in
   cases.
2. Attribute time to workspace reset, frontier construction, edge probes,
   duplicate rejection, target detection, and outer materialization.
3. Test terminal-seeded/reversed physical search and smaller-frontier
   scheduling. Keep logical path direction independent from physical search
   direction.
4. Prefer an inline, ID-only distance executor if it removes workspace cost
   without exposing unbounded state.
5. Give I1 distance the same cap+1 containment, exact incumbent fallback,
   runtime receipt, manifest binding, and kill-switch contract as witness and
   ASP I1 before retaining production-canary eligibility.
6. Promote only a runtime-recognizable or exact-query bucket; no static
   "always reverse inbound" rule is justified by current evidence.

Primary files and identities:

- `cypher/models/pgsql/translate/expansion.go`
- `drivers/pg/query/sql/schema_up.sql`
- `SP-I1-C-D`
- `SP-B1-C-ALT-NODE-D`
- `SP-B2-C-MIN-LEVEL-D`

### P5: Design exact unbounded singleton SP

Why: the two base unbounded shortest cases are 6.3-7.7x slower than Neo4j and
currently report `unsupported_depth`.

Implementation sequence:

1. Define an exact terminating BFS contract for a bound singleton pair without
   inventing a semantic maximum depth.
2. Stop at the first complete target layer and return one valid minimum SP
   witness without introducing a semantic maximum.
3. Bound local candidate work with cap+1 gates. On overflow, invoke the existing
   exact unbounded incumbent before returning any row.
4. Cover equal endpoints, zero-length semantics, cycles, self-loops,
   disconnected graphs, graph/kind filters, and cancellation.
5. Give the bounded and unbounded architectures distinct identities and
   evidence; never describe a depth-15 policy as exact unbounded SP.

`unsupported_depth` in the captured cases is selector telemetry: execution
still succeeds through the exact legacy incumbent. It is not a query error.

### P5b: Design exact unbounded singleton ASP separately

Do not infer ASP support from the SP design. ASP must retain every equal-depth
relationship-distinct predecessor, define independent state/enumeration/output
containment, and fall back to the exact unbounded ASP incumbent before emitting
rows. The existing open-maximum depth-15 policy is a bounded implementation
policy and must not be described as exact unbounded ASP.

### P6: Reduce retriever allocation and retained-heap hotspots

This can proceed independently of traversal qualification.

1. Profile `BenchmarkLoadFragmentPath` by allocation site; target repeated
   composite/map creation and intermediate slice growth first.
2. Investigate batching or arena-like ownership for fragment loading while
   preserving value ownership after row advancement.
3. Profile registry-free scrub's 1.53GB allocation and approximately 37M
   allocations; replace whole-graph temporary representations with bounded
   batches where semantics allow.
4. Audit why read-only properties retain about 694MB and whether immutable
   shared storage can be safely introduced.
5. Preserve the owned composite decoder; it is already 29-34% faster and uses
   materially less memory than map decoding.
6. Add matched Go benchmark baselines before claiming improvements.

## Correctness and rollout invariants

Every optimization above must preserve:

- graph partition and relationship-kind filtering;
- logical direction and correct physical adjacency indexes;
- inclusive minimum/maximum depth and qualified zero-length behavior;
- relationship-trail uniqueness while permitting repeated nodes;
- ordered logical node and relationship IDs;
- duplicate/bag multiplicity;
- one valid minimum SP witness, without depending on physical edge-ID tie
  order;
- the complete ASP relationship-distinct minimum-path multiset;
- predicate null behavior, locality, determinism, and evaluation count;
- optional-match and mutation visibility;
- no candidate output before every fallback-triggering guard is known;
- one exact declared fallback, with the full branch chain recorded;
- prompt cancellation, rollback, and clean same-session reuse.

Function-backed fallbacks require an explicit stable-snapshot contract. SQL
CTE arms share one statement snapshot; separate volatile PL/pgSQL statements
under Read Committed must not be assumed equivalent.

## Reproduction commands

### Full automatic production corpus

Diagnostic iteration may use `go run`, but promotion capture must build and
execute one preserved binary:

```bash
go build -trimpath -o .coverage/bin/graphbench-promotion ./cmd/graphbench
GRAPHBENCH_BINARY=".coverage/bin/graphbench-promotion"
sha256sum "$GRAPHBENCH_BINARY"

"$GRAPHBENCH_BINARY" \
  -destructive-lock .coverage/graphbench-perf-plan.lock \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -warmup-iterations 10 \
  -iterations 30 \
  -pool-size 1 \
  -round 1 \
  -postgres-traversal-telemetry summary \
  -jsonl-output .coverage/production-global-rerun.jsonl \
  -summary .coverage/production-global-rerun.md \
  -summary-json .coverage/production-global-rerun.json
```

Use a distinct round number, run UUID, output set, and reversed backend/arm
order for confirmation. Do not append incomparable source, binary, corpus, or
selection identities into one artifact.

### Focused forcing identities

Use exact `-cases` declarations and unique output files for each arm:

| Study | Identities |
| --- | --- |
| SP witness | `SP-S3-U-E+MAT-M0`, `SP-S4-C-WE+MAT-M0`, `SP-I1-C-WE+MAT-M0`, B1/B2 witness |
| SP distance | `SP-S3-U-D`, `SP-S4-C-D`, `SP-I1-C-D`, B1/B2 distance |
| ASP | `ASP-A1-DAG`, `ASP-I1-U-DAG+MAT-M0`, B1/B2 DAG |

Example:

```bash
SP_CASES="GSP-D02-F016_path,GSP-D04-F128_path,GSP-D16-F016_path,GSPV2-NORMAL-hidden-fanin-path,GSPV2-NORMAL-parallel-kind-path,GSPV2-HOLDOUT-depth8-inbound-path"

"$GRAPHBENCH_BINARY" \
  -destructive-lock .coverage/graphbench-perf-plan.lock \
  -modes postgres_sql \
  -pg-connection "$PG_CONNECTION_STRING" \
  -cases "$SP_CASES" \
  -postgres-force-shortest-executor SP-I1-C-WE+MAT-M0 \
  -warmup-iterations 10 \
  -iterations 30 \
  -pool-size 1 \
  -round 1 \
  -arm sp-i1-canonical \
  -arm-order 2 \
  -postgres-traversal-telemetry diagnostic \
  -jsonl-output .coverage/sp-i1-canonical.jsonl \
  -summary .coverage/sp-i1-canonical.md \
  -summary-json .coverage/sp-i1-canonical.json
```

### Go benchmarks

```bash
make test_bench BENCH_COUNT=3 BENCH_TIME=500ms \
  > .coverage/go-benchmarks.txt
```

For regression claims, capture an immutable baseline and candidate under the
same host/toolchain conditions and compare them with `benchstat` or the
repository benchmark-diff workflow. A single absolute run is only a hotspot
inventory.

## Current artifacts

- Diagnostic preservation archive:
  `.coverage/perf-plan-diagnostic-20260813.tar.gz` (SHA-256 above).
- Every GraphBench report below has a matching raw `.jsonl` capture and JSON
  summary with the same stem. These ignored local files must be copied into a
  verified portable bundle before handoff or promotion use.
- [Global automatic-production report](.coverage/production-global-rerun-20260813.md)
- [S3 witness report](.coverage/sp-s3-rerun-20260813.md)
- [S4 witness report](.coverage/sp-s4-rerun-20260813.md)
- [Canonical I1 witness report](.coverage/sp-i1-canonical-rerun-20260813.md)
- [A1 report](.coverage/asp-a1-rerun-20260813.md)
- [ASP I1 report](.coverage/asp-i1-rerun-20260813.md)
- [Go microbenchmark output](.coverage/go-benchmarks-rerun-20260813.txt)
- Clean ASP A/A report:
  `.coverage/qualification-84f3875/aa/asp-incumbent-aa-resolution.json`
- Clean 20-round ASP confirmation:
  `.coverage/qualification-84f3875/confirmation20/asp-i1-confirmation.json`

The near-term recommendation is therefore: pause broad ASP I1 and qualify
guarded fixed-suffix orientation for exact sparse query cohorts. After that
canary matures, decide whether deep inbound single-kind witnesses can safely
return to S3 or should move from S4 to canonical I1, then harden and evaluate
the hidden-fan-in distance arm.

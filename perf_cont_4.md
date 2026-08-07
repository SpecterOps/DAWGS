# CySQL Performance Continuation Plan 4

## Purpose

This document follows `perf_cont_3.md` after the wider review of every
performance-relevant change between `upstream/main` and the complete local
worktree on 2026-08-06. It replaces an A3-centered productionization sequence
with a portfolio that preserves the strongest independently measured work and
qualifies the remaining benchmark-only architectures before selecting them.

The immediate objectives are:

1. land the horizontal driver and scalar-continuation improvements as
   independently attributable production changes;
2. close the material gap between translated ADCS forward SQL and the exact
   hand-written ADCS-A0 forward reference before assuming a change of search
   direction is required everywhere;
3. qualify singleton shortest-path SP-S3-U in parallel with ADCS work because
   its measured gap is large, belongs to a different query family, and must not
   be serialized behind A3;
4. compare complete search-plus-materialization architectures rather than
   selecting MAT-M1 from full-query arms that both carry node-ID recursive
   state and therefore do not price MAT-M0's leanest architecture;
5. retain ADCS-A3 and ADCS-A4 as co-candidates until sparse, dense,
   zero-result, disconnected-boundary, and reverse-fan-in evidence supports a
   bounded selector;
6. make planned, applied, runtime-selected, and fallback decisions truthful
   before any experimental executor becomes production behavior; and
7. activate shortest distance, shortest full path, ADCS endpoint, and ADCS
   full-path behavior independently, each with a correct bounded fallback.

In this document, **lift** means that a benchmark or staged optimization is
implemented through a normal production seam, passes its phase gates, and is
accepted into a release candidate. It does not mean deployment, and it does
not waive rollout, rollback, or final complete-corpus qualification.

This continuation narrows and corrects parts of `perf_cont_2.md` and
`perf_cont_3.md`; it does not discard their correctness, statistical,
artifact, graph-scoping, backend-equivalence, concurrency, cancellation,
rollback, or soak requirements. The following prior rules remain in force
unless this document makes them stricter:

- discovery and confirmation samples are separate;
- exact semantics are a hard gate and cannot be traded for timing;
- raw PostgreSQL reference closure and production end-to-end predecessor
  improvement are separate comparisons;
- tests accompany every behavior increment;
- rejected experiments are removed from production code but retained as
  durable evidence;
- Neo4j is an exact-result and implementation-shape oracle, while its latency
  is diagnostic and never the CySQL acceptance gate; and
- no schema, helper, cache, search, decoding, or materialization changes are
  bundled into one causal claim.

The central correction is simple: ADCS-A3 is the current sparse ADCS search
leader, not the center of the whole optimization program and not yet a
universal production strategy.

### Prior-plan disposition

| Prior work | Disposition in this continuation |
|---|---|
| `perf_cont_2.md` C0R/C1R evidence and attribution rules | inherited; L0 republishes and repairs identity |
| C2Q/C3S singleton tournament/shipment | continued by L0, L2S, L4, and L6/L7 with family-qualified identities |
| C4M materialization | continued and corrected by L0/L3M; edge-only MAT-M0 is now mandatory |
| C3G generic/all-shortest | still inherited and outside the initial singleton lift; singleton success does not close it |
| C5A/C5B variable-state/decode/list work | L1C/L1D cover the proven decode/node-ID subset; broader traversal and list-cardinality work remains inherited or conditional |
| C6 and `perf_cont_3.md` R0-R8 ADCS sequence | superseded by L0, L2F, L3A, L4, and L6/L7 |
| C7 planning/cache work | PostgreSQL planning moves into L3A; translation caching remains conditional L5 |
| C8/C9 concurrency, soak, and complete-corpus loop | inherited and consolidated in L6/L7 |

Generic variable traversal, correlated/multi-pair shortest, directionless
shortest, and `allShortestPaths` remain separate inherited workstreams. They do
not block a narrowly qualified singleton lift, but no singleton result may be
presented as generic-family completion.

## State entering this continuation

### Comparison boundary and worktree inventory

The review used the complete `upstream/main`-to-worktree boundary, then split
that boundary into layers so already-active production changes were not
confused with benchmark-only work.

| Boundary | Identity or size |
|---|---|
| Upstream mainline | `6638cc2e12160a7be184817af2b5ed41a7dad3da` |
| Local `HEAD` | `7bb291c57fd9a4621360bde7223a99e826b4cc6c` |
| Commit relationship | local `HEAD` is 13 commits ahead and 0 behind |
| Tracked mainline-to-worktree delta | 177 files, 27,838 insertions, 787 deletions |
| Index relative to `HEAD` | 45 files, 2,787 insertions, 227 deletions |
| Unstaged layer relative to index | 23 files, 1,075 insertions, 111 deletions |
| Untracked files before this document | `cmd/graphbench/postgres_plan.go`, its test, and `perf_cont_3.md` |

These counts were recorded before this document was added. They describe the
audited tracked boundary, not a promise that the dirty tree will remain
byte-identical. The L0 manifest must include untracked files explicitly because
normal `git diff` statistics do not.

The useful attribution layers are:

- `upstream/main..HEAD`: the broader optimizer, translator, schema, benchmark,
  and regression foundation already accumulated on the local branch;
- `HEAD` to index: production-active parse, decode, ownership, and scalar-state
  increments plus their tests; and
- index-to-worktree: the ADCS comparator tournament, planner attribution,
  typed conservative search decision, and related fixture/harness work.

No performance result may be attributed to one of these layers merely because
the relevant file is located there. Causal attribution requires an isolated
predecessor/candidate binary or a genuinely isolated microbenchmark.

### Reviewed but not reopened as new lift candidates

The broader mainline-to-HEAD delta also contains production lowerings and
correctness work that remain part of the accepted predecessor/control surface:

- count-store and typed relationship-count fast paths;
- predicate placement, correlated `EXISTS`, and clause reordering;
- traversal-direction, limit, suffix-pushdown, and `ExpandInto` decisions;
- shared/late path materialization and collect-ID membership;
- index-friendly strict string equality;
- exact directed `*1..1`/`*2..2` expansion;
- path relationship `ANY`/`NONE` predicate lowering;
- incumbent shortest strategy/filter/workspace improvements;
- graph-scoped materializer/schema corrections; and
- Neo4j logical-scope, JSON/null, and cross-backend regression corrections.

These changes are not omitted from validation. They remain plan-corpus,
complete-corpus, semantic, and rollback controls. They are not promoted as new
benchmark-to-production candidates here because they are already active in the
local production path, lack a new isolated causal result in the entering
bundle, or are correctness changes rather than performance candidates. Any one
may reopen only from new isolated evidence and a bounded plan.

### Authoritative entering artifacts

The entering evidence is staged under `.coverage` and must be copied into a
durable reconstructible artifact directory during Phase L0.

| Artifact | Purpose | SHA-256 |
|---|---|---|
| `.coverage/live-priorities-20260806/REPORT.md` | five-round production-active increment and materializer summary | `5a742469c66a6aed8607fbaf5a572b6aff83c993fc9d19ed0da9d04a3f74924c` |
| `.coverage/live-next-20260805/REPORT.md` | singleton shortest SP-S3-U reference gap | `66f378b2e8d75c583b03202a57f5c8d359326a23280a3e72e703ed2224fc5b28` |
| `.coverage/live-bench-rerun-20260805/REPORT.md` | incumbent singleton/workspace predecessor gain | `1ef0e7d31e21955dd6d5eb7d92612f7419fc9315e1d2f9a72219a76d726a2356` |
| `.coverage/perf-cont-3-validation/round-{1..10}.jsonl` | ten-round ADCS-A0/A1b/A2/A3/A4 validation | checksums listed below |
| `perf_cont_2.md` | inherited shortest/materializer plan | `9504ab3563580ac61b672396cfa123f03949793b7ea4a942cba9807d55e97cd7` |
| `perf_cont_3.md` | inherited ADCS plan and gates | `d43cfa84f4174c41b118a9f524332424c2a8dda46f8289f541e4b22195be6340` |

The immutable ADCS validation members are:

| Member | SHA-256 |
|---|---|
| `graphbench` | `4b497850381f16a3bf2d4591a2251db1aea70df361a32e4002222c1c55a1e1e2` |
| `round-1.jsonl` | `126c88de68f42f75b4169d50c4af5eeabbe3c97b065ce9730a4b10dc5d748c74` |
| `round-2.jsonl` | `296172659cc548d98878b52214947474256d4f9cf513d73b6894d6b14fdb07eb` |
| `round-3.jsonl` | `3aa8ee068e2935e730ec1039c1129018975333b6b8802a10b502df77477b0f20` |
| `round-4.jsonl` | `1f01e4e44b661f71aee6f445d2c04ec6d6d74bf1a32d489219cb246a945c8baa` |
| `round-5.jsonl` | `e5d50b059b45952f27354145cee3ff42c652758cee8faad2be686c24797f39db` |
| `round-6.jsonl` | `8ecf559f2215cb3d1919146f5bdf7df5496da370349895674d6a6e6b7441ef25` |
| `round-7.jsonl` | `a241c8ac0cec0cd31a7ba923526bb26074cd48eb1470c5d62b3a5a110e1931b4` |
| `round-8.jsonl` | `daa18a42a99fac86e1d6029c98440673c9791abc0a4c6b4fd4047e15fe2ebd38` |
| `round-9.jsonl` | `d384a42af649eb89b164ccd1850d42a2b2743518db26325070f303cd9e7cca08` |
| `round-10.jsonl` | `6035b58da3d65dee1c286eeeb4ae93c5ba8a4275bc7ba2ea8aa7b77afa3c2fec` |

The ten ADCS rounds used exact public-observation validation and retained raw
PostgreSQL plans. The earlier horizontal comparison used five independently
reloaded rounds, alternating predecessor/candidate order, ten untimed warmups,
thirty warm observations, pool size one, exact observations, and validated
physical relation sizes. The complete PostgreSQL and Neo4j integration suites
passed after the changes, as did focused race and live composite-decoding
coverage.

These are strong entering observations, not final production confirmation.
The horizontal end-to-end candidate contained several increments together,
and the ADCS tournament exercised only the two primary sparse v2 cases.

### Horizontal production-active results

The worktree already routes four generally useful increments through normal
production code. They are candidates to split, qualify, and land; they are not
benchmark-only executor designs.

| Increment | Entering live evidence | Isolated evidence | Current interpretation |
|---|---:|---:|---|
| Bounded parsed-AST cache | repeated lookup `0.851 -> 0.356 ms`, 49.9% faster | cache hit `42-44 us`, about 28 KiB and 395 allocations to `214-235 ns`, 0 B and 0 allocations | high-value independent production increment |
| Typed PostgreSQL composite decoding | 1,000-node hydration `3.377 -> 2.566 ms`, 31.9% faster | one node about 2.0 to 1.4 us; 128-node array about 286 to 192 us | strong decode increment with compatibility review required |
| Result key and value-ownership reuse | dense raw hydration `0.801 -> 0.686 ms`, 21.8% faster | field keys about 27 to 0.94 ns; value ownership about 32 to 4.74 ns, both removing one allocation | small-surface hot-row candidate pending compatibility/lifetime review and isolated E2E capture |
| Scalar node-ID continuation | D4 endpoint `0.893 -> 0.710 ms`, 20.5% faster; D16/F1000 endpoint `55.043 -> 49.382 ms`, 9.4% faster | translated endpoint SQL shrinks while full-path SQL is intentionally unchanged | useful production lowering and state primitive |

The D4 full-path control improved 15.0% because of client decode/cache work
while its SQL stayed unchanged. The D16/F1000 full-path control was effectively
flat at 0.6% faster. This is the intended evidence that scalar continuation is
restricted when a full path must remain observable.

The reported horizontal percentages are medians of paired per-round candidate/
predecessor ratios, so they intentionally need not equal quotients of the
displayed aggregate medians.

The production-lift program must not report the table's end-to-end labels as
perfectly isolated causal deltas. The microbenchmarks isolate the client hot
paths; each production increment still requires a matched immediate-predecessor
confirmation binary.

### Singleton shortest search and materialization result

Historical reports use SP-S3-U as an umbrella name for exact benchmark-only
unidirectional recursive CTEs. This document canonicalizes distance as
SP-S3-U-D and node-plus-edge path state as SP-S3-U-NE; future reports may not
reuse one implementation ID for both state shapes. Neither is a qualified
production executor. Exactness is established for the retained captured cases,
not yet the complete semantic adapter. The addressable gap is too large to
defer behind ADCS.

Earlier search-only/full-reference evidence recorded:

| Case | Incumbent E2E | SP-S3-U-D | Incumbent / SP-S3-U-D |
|---|---:|---:|---:|
| D1/F1 distance | 4.240 ms | 0.177 ms | 23.9x |
| D2/F16 distance | 6.087 ms | 0.223 ms | 27.2x |
| D4/F128 distance | 9.675 ms | 0.362 ms | 26.7x |
| D8/F1 inbound distance | 13.837 ms | 0.277 ms | 50.0x |
| D16/F16 distance | 25.329 ms | 0.228 ms | 111.1x |

The refreshed exact path materializer capture then showed the size of the
complete search-plus-hydration opportunity:

| Case | Public production path | SP-S3-U-NE + MAT-M1 | Potential ratio |
|---|---:|---:|---:|
| D1/F1 | 4.038 ms | 0.328 ms | 12.3x |
| D2/F16 | 5.682 ms | 0.354 ms | 16.1x |
| D4/F128 | 8.950 ms | 0.475 ms | 18.8x |
| D16/F16 | 26.352 ms | 0.469 ms | 56.2x |

These are exact same-run direct-reference comparisons, not production
candidate measurements. They justify implementation and qualification; they
do not justify immediate dispatch.

The displayed shortest values are medians of per-round summaries, not pooled
sample percentiles.

The existing production singleton/workspace changes are also a real gain: an
earlier capture improved `10.915 -> 5.278 ms` (51.6%). That change missed its
then-declared ratio and PostgreSQL/Neo4j gates, and the later SP-S3-U+MAT
references dominate it by another order of magnitude. Retain the workspace
work as a proven generic fallback/control rather than making further workspace
tuning the primary singleton track.

At D4, with one shared SP-S3-U-NE search definition:

| Materializer | Hydration only | Full SP-S3-U-NE plus hydration |
|---|---:|---:|
| Incumbent | 0.766 ms | 1.130 ms |
| MAT-M0 | 0.259 ms | 0.535 ms |
| MAT-M1 | 0.222 ms | 0.475 ms |

Using paired order-balanced per-round ratios, MAT-M1 is 13.5% faster than
MAT-M0 for hydration and 10.4% faster end to end at D4; those percentages need
not equal quotients of the displayed aggregate medians. It wins the measured
hydration comparison at every measured depth. That does **not** yet select
MAT-M1 as the best complete architecture: both full arms reuse a search that
carries ordered node and edge arrays. MAT-M0 was not allowed to realize its
potential advantage of edge-only recursive state.

The missing whole-architecture comparison is:

```text
SP-S3-U-E: edge-only recursive search
  + direction-aware MAT-M0 node derivation

versus

SP-S3-U-NE: node-and-edge recursive search
  + MAT-M1 independent ordinal hydration
```

The current trail-carrying SP-S3-B is deprioritized by the earlier exact
same-materializer evidence, which found no stable advantage over SP-S3-U. A
later mixed comparison against SP-S3-U+MAT-M0/M1 does not by itself prove
search-architecture domination, and refreshed roughly 0.02 ms crossovers near
the measurement floor do not select SP-S3-B. It is not the compact
trace-relation SP-S2 promised by `perf_cont_2.md`; preserve it as a rejected
control. True SP-S1 and SP-S2 remain unimplemented and therefore unmeasured,
not disproven.

### ADCS forward, reverse, and viability result

The ten-round primary sparse result is:

| Boundary | Production CySQL | ADCS-A0 SQL | ADCS-A3 | ADCS-A4 | Neo4j diagnostic |
|---|---:|---:|---:|---:|---:|
| Endpoint IDs median | 54.806 ms | 36.879 ms | 15.609 ms | 16.390 ms | 1.267 ms |
| Full path median | 64.906 ms | 40.827 ms | 17.009 ms | 17.709 ms | 1.427 ms |
| Endpoint p95 | 56.430 ms | 38.538 ms | 15.880 ms | 17.257 ms | 1.941 ms |
| Full-path p95 | 67.514 ms | 44.264 ms | 18.112 ms | 18.360 ms | 2.252 ms |

These are medians of ten per-round medians or per-round p95s, not pooled
sample p95s.

This exposes two distinct gaps at different boundaries:

1. ADCS-A0 raw-pgx is 32.7% below production E2E for endpoints and 37.1% below
   it for paths, showing forward-shape headroom without proving an attributable
   production win; and
2. sparse reverse/viability search offers a further roughly 58% median
   improvement over ADCS-A0 at the direct SQL boundary.

ADCS-A3 reduces the sparse D16/F1000 search from 16,001 forward states to 19
reverse states. It reduces shared hits from roughly 45,000 in ADCS-A0 to about
575 for endpoints and 775 for paths. ADCS-A4 uses 36 states and is only about
4-5% slower than ADCS-A3 at the observed sparse point.

ADCS-A3 nevertheless fails the predeclared `perf_cont_3.md` sparse activation
gates against ADCS-A0:

| Gate | Endpoint result | Path result | Requirement |
|---|---:|---:|---:|
| Median ratio | 0.423 | 0.417 | upper bound at most 0.25 |
| p95 ratio | 0.412 | 0.409 | upper bound at most 0.40 |
| Median saving | about 21.3 ms | about 23.8 ms | lower bound at least 30 ms |

These are descriptive quotients/differences of aggregate per-round summaries,
not formal bootstrapped UCBs/LCBs; the retained validation did not produce
formal gate intervals. Because the point estimates already miss the required
side of each timing threshold, they cannot establish qualification. The
structural state and shared-hit point estimates are strong. The timing gate
remains closed, and this document does not weaken it after observing the
result.

### ADCS residual attribution: planning, not client hydration

The retained plans show that PostgreSQL planning, rather than A3 server
execution, is the dominant measured residual.

| Arm | Boundary | Median planning | Median execution | Boundary median |
|---|---|---:|---:|---:|
| ADCS-A0 | endpoint | 4.863 ms | 33.072 ms | 36.879 ms |
| ADCS-A3 | endpoint | 13.933 ms | 1.394 ms | 15.609 ms |
| ADCS-A4 | endpoint | 13.881 ms | 1.977 ms | 16.390 ms |
| ADCS-A0 | path | 5.261 ms | 37.474 ms | 40.827 ms |
| ADCS-A3 | path | 14.230 ms | 2.333 ms | 17.009 ms |
| ADCS-A4 | path | 14.445 ms | 3.021 ms | 17.709 ms |

Planning and execution are one `EXPLAIN` observation per round. The displayed
values are separately sampled medians and are not additive components of the
boundary median. ADCS-A3 server execution is below five milliseconds, but the
`perf_cont_3.md` objective applies to the total warm median and remains unmet.
PostgreSQL planning dominates the measured residual. The parsed-Cypher AST
cache cannot fix this because it operates before optimization, translation,
SQL rendering, PostgreSQL parse analysis, and PostgreSQL planning.

The ADCS production track must therefore include a stable-SQL/planning-policy
tournament before frontier, index, helper, or native work. It must compare the
same semantics under:

- `plan_cache_mode=auto`;
- `force_custom_plan`;
- `force_generic_plan`;
- parent-table SQL versus graph-partition-specific stable SQL;
- ordinary prepared statements versus a narrowly typed stable helper boundary
  only if portable SQL cannot retain the required plan; and
- one graph versus representative partition counts.

No experiment may change a global server setting as the production solution.
Generic-plan wins must retain graph pruning and may not hide execution
regressions behind reduced planning.

### Comparator and evidence defects that must be repaired

#### ADCS-A1a is an A/A arm

`a1a_root_reuse_*` and ADCS-A0 both use the same `legacyForward` SQL. No root
reuse experiment occurred. GraphBench must reject two advertised
architectures with the same normalized SQL fingerprint unless a comparator is
explicitly declared as an A/A control.

#### ADCS-A1b and ADCS-A2 do not isolate their advertised ideas

ADCS-A0 already carries scalar node and relationship ID arrays. The current
ADCS-A1b/ADCS-A2 implementations remove the cheap recursive node-existence
join, then preserve orphan safety by running a correlated
`allMemberNodesExist` `unnest`/anti-join over every retained trail.

The consequence is implementation-specific explosion:

| Arm | Approximate endpoint shared hits | Plan-derived node-relation loops |
|---|---:|---:|
| ADCS-A0 | 45,493 | 10 |
| ADCS-A1b | 957,675 | 456,007 |
| ADCS-A2 | 349,439 | 152,011 |

Their roughly 322 ms and 141 ms medians reject that final correlated rescan.
They do not reject scalar continuation, late hydration, or suffix factoring.
Corrected comparators must retain a cheap graph-scoped ID-only node-existence
check during recursion and avoid full-composite projection until required.

#### MAT-M0 versus MAT-M1 does not price distinct search-state shapes

The current full comparison shares node-and-edge SP-S3-U-NE state. A production
choice requires total search state, planning, execution, transfer, decode,
allocations, and memory. Hydration-only evidence remains useful but cannot
select the final state shape.

#### ADCS-A3 versus ADCS-A4 covers one regime

The ten-round tournament ran only the sparse endpoint and sparse path cases.
The declared v2 fixtures already contain zero-reachable and high-reverse-fan-in
cases, but they were not in that capture. Dense suffixes, suffix multiplicity,
payload, false boundaries, and discovery-independent holdouts are also still
required.

#### Lowering diagnostics are not yet trustworthy enough for activation

The optimizer emits `ShortestPathExecutorDecision` and
`ExpansionSearchStrategyDecision`, but the translator does not index and
consume either decision. Shortest translation currently records
`ShortestPathExecutorDecision` as applied whenever a shortest pattern exists,
even though the selected executor is still the incumbent. The skipped-count
inventory omits `FieldRequirements` and `ShortestPathExecutorDecision`.

Before candidate activation:

- a planned decision means only that analysis emitted a decision;
- an applied decision means the selected decision changed emitted SQL;
- runtime selected/fallback is reported separately from compile-time applied;
- a fallback reason identifies the actual rejected eligibility or runtime
  bound; and
- plan-corpus and GraphBench records agree with the emitted SQL fingerprint
  and observed runtime branch.

### Current production-candidate ranking

This is an engineering/evidence-readiness order, not a global ROI ordering.
Production shape frequency is not available in the entering artifacts; final
global priority uses absolute addressable cost multiplied by observed workload
frequency when that data exists.

| Priority | Candidate | Status entering L0 | Required next action |
|---:|---|---|---|
| 1 | Horizontal AST/decode/ownership/scalar increments | production-active in worktree | isolate, qualify, and land separately |
| 2 | SP-S3-U-D distance | benchmark-only, large exact gap | qualify bounded distance executor in parallel with ADCS |
| 3 | Production ADCS-A0 parity | exact handwritten reference only | converge translated forward SQL incrementally |
| 4 | SP-S3-U-E + MAT-M0 versus SP-S3-U-NE + MAT-M1 | missing whole-architecture tournament | implement and select complete path architecture |
| 5 | ADCS-A3/ADCS-A4 bounded selector | benchmark-only sparse evidence | run full regime matrix and planning tournament |
| 6 | ADCS-A3/A4 plus MAT-M0/M1 | unmeasured compounds | add after search and materializer identities are fixed |
| 7 | Relationship-ID scalar continuation | analysis metadata only | benchmark after node scalar continuation lands |
| 8 | SP-S1/SP-S2, MAT-M2, translation cache, helpers | conditional/unimplemented | trigger or explicitly close from residual evidence |

## Candidate namespace and measurement boundaries

### Family-qualified candidate names

Earlier plans reuse labels such as `S1` for unrelated shortest and ADCS
architectures. All new artifacts, diagnostics, reports, and code comments must
use family-qualified names.

| Prefix | Family | Required identities |
|---|---|---|
| `H-` | horizontal production increments | `H-AST`, `H-CODEC`, `H-ROWS`, `H-NODE-ID` |
| `SP-` | singleton shortest search | `SP-S0`, `SP-S1`, `SP-S2`, `SP-S3-U-D`, `SP-S3-U-E`, `SP-S3-U-NE`, `SP-S3-B` |
| `ADCS-` | compound expansion search | `ADCS-A0`, corrected `ADCS-A1a`, corrected `ADCS-A1b`, corrected `ADCS-A2`, `ADCS-A3`, `ADCS-A4`, conditional `ADCS-A5` |
| `MAT-` | final path materialization | `MAT-M0`, `MAT-M1`, conditional `MAT-M2` |

Historical artifact aliases remain readable, but every new manifest records
both the historical name and the canonical family-qualified identity.

### Observation and timing boundaries

Every comparison declares two independent dimensions: one result/observation
shape and one timing boundary.

| Observation shape | Result contract |
|---|---|
| `distance_scalar` | exact shortest depth only |
| `endpoint_ids` | exact endpoint ID rows/multiset |
| `ordered_ids` | exact ordered node and/or edge IDs required by the architecture |
| `hydrated_result` | complete PostgreSQL public-value shape |
| `public_observation` | fully mapped public CySQL result |

| Timing boundary | Includes | Excludes |
|---|---|---|
| `client_parse` | Cypher text normalization and parse/cache lookup | optimize, translate, render, server |
| `client_compile` | parse, optimize, translate, kind mapping, render | server protocol and execution |
| `server_plan` | PostgreSQL planning | execution, transfer, client decode |
| `server_search` | execution to the declared distance/ID observation | final hydration and client decode |
| `raw_pgx` | prepared protocol, planning/execution, transfer, pgx decode, drain | Cypher compilation |
| `production_e2e` | public CySQL API from query text to drained mapped result | nothing in the request path |

Thus an SP-S3-U-E arm can declare `observation_shape=ordered_ids` and
`timing_boundary=raw_pgx`; these are not mutually exclusive labels.

`ADCS-A0-SQL`, `SP-S3-U-D-REF`, and `SP-S3-U-NE-REF` denote direct raw-pgx
references. `ADCS-A0-E2E` and `SP-S0-E2E` denote production predecessors. A
raw reference may not be compared with production E2E and presented as one
closure ratio.

### Architecture identity contract

Every non-control arm records:

- architecture, implementation ID, and state shape;
- observation shape and timing boundary;
- normalized SQL fingerprint;
- full-comparator status;
- exact semantic-validation mode;
- source, dirty-tree, binary, fixture, schema, and environment fingerprints;
- PostgreSQL plan fingerprint and selected plan-cache mode; and
- compile-time planned/applied plus runtime selected/fallback identities.

Two distinct architecture IDs with the same normalized SQL fingerprint fail
the run unless the manifest predeclares one as an A/A alias. Two identical
architecture IDs with different state or observation shapes also fail.

## Decisions fixed by the evidence

1. **Do not organize the program around ADCS-A3 alone.** Run horizontal,
   shortest, and ADCS tracks with separate attribution.
2. **Land horizontal increments independently.** Parser cache, codec, result
   ownership, scalar continuation, search, and materialization never share a
   production-candidate binary for causal confirmation.
3. **Pursue SP-S3-U-D and production ADCS-A0 parity in parallel.** They
   address different query families and both have material evidence.
4. **Treat ADCS-A3 as a sparse specialist.** ADCS-A4 remains a co-candidate
   until high reverse fan-in and other crossover holdouts run.
5. **Do not reject late hydration or suffix factoring from current A1b/A2.**
   Reject their correlated final trail revalidation and rebuild the intended
   architectures.
6. **Do not select MAT-M1 from evidence whose full-query arms share
   node-and-edge recursive state.** Compare edge-only SP-S3-U-E+MAT-M0 with
   node-and-edge SP-S3-U-NE+MAT-M1.
7. **Ship shortest distance before shortest path when it qualifies.** Distance
   state carries no ordered trail or materializer cost.
8. **Keep SP-S3-B rejected.** It is neither the measured leader nor true
   compact SP-S2 evidence.
9. **Prototype or explicitly close true SP-S1/SP-S2.** A declared but
   unimplemented alternative is not evidence that the SP-S3-U family is
   globally optimal.
10. **Keep the incumbent workspace and stepwise forward lowerings as semantic
    fallbacks.** Do not continue optimizing the workspace as the final
    singleton specialist unless new evidence reverses the SP-S3-U family's gap.
11. **Resolve PostgreSQL planning before frontier mechanics.** ADCS-A3/A4
    planning is now the dominant measured cost.
12. **Do not lower prior gates to fit observed A3 results.** Improve the
    candidate/planning boundary, restrict it to a proven envelope, or retain
    fallback.
13. **No production dispatcher precedes truthful diagnostics.** Planned,
    applied, runtime selected, and fallback must be independently testable.
14. **Relationship-ID continuation, MAT-M2, translation caching, typed
    helpers, indexes, and native code are conditional residual work.** Open
    them only when a stable lower layer leaves a measured addressable cost.
15. **Neo4j latency remains contextual.** Exact Neo4j results must pass, but
    PostgreSQL production choices compare against the immediate CySQL
    predecessor and best correct PostgreSQL reference.

## Correctness, attribution, and acceptance model

### Exact semantic contract

Every horizontal or executor increment must preserve the public behavior of
the predecessor. For search and materialization this includes:

- exact result multiset and duplicate multiplicity;
- exact ordered node and relationship identities for every observed path;
- relationship direction and relationship-unique trail semantics;
- node and relationship kinds, properties, nulls, and errors;
- zero-length, minimum-depth, maximum-depth, and same-endpoint behavior;
- valid one-path tie selection and exact all-shortest fallback behavior;
- graph partition scope, including colliding IDs in another graph;
- missing-root, missing-endpoint, dangling-node, and contradictory predicate
  behavior;
- optional, correlated, multi-part, multi-source, mutation, and multiple-path
  fallback semantics;
- cancellation, rollback, transaction reuse, and physical-session reuse; and
- backend-equivalent public observations in the shared integration corpus.

Any mismatch closes the candidate regardless of its speed. Row-count equality
alone is insufficient for path or duplicate-bearing results.

### One behavior increment per confirmation

Each production confirmation compares an immediate predecessor binary with a
candidate binary that changes one behavior group:

- H-AST only;
- H-CODEC only;
- H-ROWS only;
- H-NODE-ID only;
- one shortest search state only;
- one materializer only with search fixed;
- one ADCS search strategy only with observation/materialization fixed; or
- one selector/fallback policy only with candidate emitters fixed.

Source and binary manifests must prove the intended difference. Incidental
formatting or test-only changes are allowed, but a search result may not be
attributed to a binary that also changes parsing, codecs, schema, indexes, or
pool behavior.

### Two independent performance comparisons

Every shippable search change must clear both comparisons:

1. **Reference closure:** at an identical raw-pgx boundary, the production SQL
   candidate is within `1.10` of the best correct PostgreSQL reference, or its
   absolute remaining gap is below the case's A/A resolution.
2. **Production improvement:** at the public E2E boundary, the candidate
   materially improves its immediate CySQL predecessor and is non-inferior on
   affected-family controls.

The reference may explain addressable server work but may not absorb Cypher
compilation on only one side. Conversely, a client cache win may not be
presented as a search-architecture win.

### Fallback is part of the candidate

A candidate's correctness, latency, resource, and timeout measurements include
all eligibility probes, state-limit detection, discarded partial work, and
fallback execution. Selecting fallback is not itself a pass.

Fallback must:

- observe the same statement snapshot;
- return exactly the incumbent result;
- discard all partial candidate rows after overflow;
- avoid DML, session-global mutable state, and externally visible side
  effects;
- return rows from only one result branch, with zero loops in every unselected
  recursive search/materializer descendant; and
- remain cancellable and safe for connection reuse.

If a same-statement exact restart cannot be proven, restrict the candidate to a
static envelope whose bound cannot overflow.

### Backend-equivalent and driver-scoped coverage

Public semantics belong in shared integration cases and must stay equivalent
for PostgreSQL and Neo4j. PostgreSQL-specific plan, buffer, helper, codec, and
fallback-state assertions belong in clearly PostgreSQL-scoped tests that skip
unless `CONNECTION_STRING` selects PostgreSQL. No shared case gains a
driver-specific expected result or skip.

Changes affecting parsing, Cypher optimization, translation, SQL rendering,
or semantics require the mutation/template coverage specified by
`AGENTS.md`. Changes to raw composite representation require direct driver
compatibility tests in addition to mapper-level public tests.

## Target production architecture

### Horizontal path

#### H-AST: bounded immutable parse reuse

Keep the current per-driver LRU architecture:

- at most 256 successful entries;
- trimmed query text as the cache key;
- queries larger than 64 KiB bypass the cache;
- invalid parses are never cached;
- concurrent misses for the same text coalesce;
- cached ASTs remain immutable; and
- optimization copies the AST before applying rules.

Only parsing is cached. Graph selection, schema/kind generation, optimization,
translation, parameter binding, and SQL rendering remain per call. Any later
translation cache is a separate conditional phase with explicit dependency
keys and invalidation.

Cache keys and ASTs retain the complete trimmed query, including literal
values, until eviction or cache/driver teardown. L1B must make an explicit
privacy/lifecycle decision for that bounded in-memory retention, document the
driver lifetime, and prove eviction plus driver close/teardown release
references. If that retention is unacceptable for a query class, restrict or
bypass caching for that class rather than implying that absence of telemetry
eliminates in-memory retention.

#### H-CODEC: typed owned composites

Register typed node, edge, path, and array codecs while retaining a safe
fallback for NULL internal composite fields and NULL array elements. Validate
field names, order, OIDs, and ownership at registration or through an equally
strong versioned contract.

The public mapper contract must remain stable. Before shipment, make an
explicit compatibility decision for callers that inspect raw `Result.Values()`
and may have depended on pgx's historical `map[string]any` representation.

#### H-ROWS: result metadata and value ownership

Cache field names once per result set and reuse the otherwise-unexposed
`Rows.Values()` slice when replacing JSON values. Specify that returned keys
are immutable for the result lifetime. Prove that no nested ownership or row
lifetime escapes into later rows, cancellation, pool reuse, or concurrent
consumers.

#### H-NODE-ID: field-sensitive scalar continuation

Carry node IDs rather than node composites only when field requirements prove
that every intermediate consumer is ID-only. Continue to join the graph-scoped
node partition so dangling endpoints do not become matches and multiplicity is
unchanged.

Property, kind, full-entity, path, cross-pattern, optional, mutation, and
unknown-function consumers keep composite state unless separately proven.
Relationship-ID continuation is not silently included in H-NODE-ID; it is a
new conditional candidate.

### Singleton shortest path

#### Distance state

The first production candidate is SP-S3-U-D distance mode for the existing
singleton eligibility envelope. Distance mode carries only the state required
to find the shortest depth. It must contain no:

- ordered edge array;
- ordered node array;
- predecessor chain;
- hydrated entity; or
- path materializer call.

Aliases and `WITH` propagation remain eligible only when every downstream use
is distance-only. Direct path output, `nodes()`, `relationships()`, collection,
path predicates, or an unknown consumer requires path mode or fallback.

#### One-path state

Path mode selects between complete architectures, not isolated materializers:

- **SP-S3-U-E + MAT-M0:** recursive state carries ordered edge IDs; directed
  materialization hydrates edges once and derives ordered nodes from root and
  endpoints; or
- **SP-S3-U-NE + MAT-M1:** recursive state carries ordered node and edge IDs;
  materialization hydrates both streams independently and restores order by
  ordinality.

Outbound and inbound variants must be measured. Directionless and mixed
direction remain incumbent fallback unless a separate exact architecture
qualifies. Neither architecture may re-run search or rediscover connectivity
already represented by its state.

#### Alternative obligation

The SP-S3-U family may become the production winner only after at least one
genuinely different SP-S1/SP-S2 architecture is prototyped and measured, or a
predeclared feasibility closure shows that its required correctness/state
model cannot meet the resource envelope. Implementation effort alone is not a
closure rule.

### ADCS compound search

#### Production ADCS-A0 parity

Before direction selection, converge the generic translator toward the exact
forward ADCS-A0 reference through separately measurable steps:

- scalar root seed reuse;
- removal of redundant invariant root hydration/rejoins;
- graph-scoped ID-only intermediate node existence;
- compact scalar recursive projection;
- direct fixed-suffix joins without per-recursive-row invariant work where
  semantics allow; and
- final boundary hydration only after trail acceptance.

This phase does not paste reference SQL into production. It extends typed
optimizer decisions and PostgreSQL AST builders while preserving generic
scope/frame contracts. Each step must demonstrate which plan loops/hits it
removes.

#### Corrected forward comparators

Implement or relabel:

- corrected ADCS-A1a as actual bound scalar root reuse, with a distinct SQL
  fingerprint from ADCS-A0;
- corrected ADCS-A1b as late composite hydration while retaining cheap ID-only
  node existence during recursion; and
- corrected ADCS-A2 as exact factored-suffix forward enumeration without the
  correlated final trail rescan.

ADCS-A2 remains a candidate only if it is non-dominated on a dense or overflow
tier. Its sparse regression does not make it the default fallback.

#### Sparse reverse and viability candidates

ADCS-A3 remains exact suffix-seeded reverse all-trail search. It must:

- build an exact multiplicity-preserving suffix bag;
- deduplicate only a filter/seeding boundary, never result trails;
- prepend relationship/node IDs while walking backward;
- preserve zero-depth and minimum/maximum depth semantics;
- continue through root states when longer valid trails remain possible;
- exclude relationship reuse within the variable segment and across the fixed
  suffix; and
- rejoin the exact suffix bag to restore multiplicity.

ADCS-A4 builds a permissive deduplicated backward viability relation, then
performs exact forward trail enumeration. Viability may discard impossible
states but may never manufacture or deduplicate output trails.

The intended selector portfolio is:

- ADCS-A3 for bounded sparse suffixes and bounded reverse fan-in;
- ADCS-A4 when viability collapses reverse fan-in before exact forward work;
- corrected ADCS-A2 or production ADCS-A0 for dense, high-state, unavailable
  estimate, or overflow cases; and
- incumbent stepwise translation for structurally ineligible forms.

This is a hypothesis to qualify, not a hard-coded policy.

#### ADCS full-path materialization

Search selection and path hydration are orthogonal. After raw search identity
is fixed, run:

- ADCS-A3 + direction-aware MAT-M0;
- ADCS-A3 + MAT-M1;
- ADCS-A4 + direction-aware MAT-M0; and
- ADCS-A4 + MAT-M1.

Endpoint-only forms carry no state solely for materialization. Full-path arms
must price the recursive cost of node IDs rather than reusing a shared larger
search state for convenience.

### PostgreSQL planning boundary

The production AST emitter must produce stable SQL for equivalent query
shapes. Runtime values do not change the SQL fingerprint. Planner work compares
portable SQL first and may introduce a typed helper only when:

- the search architecture is already correct and selected;
- the portable SQL reference-closure gap is greater than both 10% and 0.50 ms;
- the gap is demonstrably PostgreSQL planning/dispatch rather than execution;
- generic/custom plan experiments cannot close it safely; and
- helper schema, upgrade/downgrade, cancellation, graph-scope, and rollback
  costs are included.

No dynamic SQL text or rewritten fragment is passed to a helper. No new index,
statistics target, JIT setting, `work_mem`, or global planner setting is part
of the initial solution.

Forced plan-cache modes are diagnostic in L3A. The initial shippable surface
may change emitted SQL or normal preparation behavior, not set
`plan_cache_mode`. If a session/local plan-cache policy is later proposed, it
is a separate driver increment with protocol-cost attribution, transaction
scoping/reset, error/cancellation cleanup, pool/session reuse, and isolated
confirmation.

## Strategy decision and diagnostics contract

### Compile-time decisions

`ShortestPathExecutorDecision` and `ExpansionSearchStrategyDecision` must be
indexed by traversal target in `Translator.SetOptimizationPlan`. The translator
must consume the exact target decision rather than infer activation from the
presence of a shortest or variable-length pattern.

Each decision records at least:

```text
target
family and observation mode
planned candidates
selected strategy/executor
fallback strategy/executor
eligibility facts
compile-time ineligibility/selection reason, empty when not applicable
minimum and maximum depth
suffix bounds, when applicable
state/probe limits, when applicable
selector version and selection mode
```

Fallback identity, compile-time reason, and runtime overflow reason are
different fields. A selected candidate may name the executor available on
runtime overflow without claiming that a compile-time fallback occurred.

Observation mode is finalized by an explicit statement-wide lineage pass, not
merely from whether a path symbol is referenced. That pass must:

- trace aliases and `WITH` projections backward across query parts;
- use external `FieldRequirementUse` entries rather than internal
  representation requirements;
- apply shortest and expansion observation modes after field requirements are
  complete, including an `applyExpansionSearchObservationModes`-style pass;
- retain `(query_part, symbol)` identity for field requirements while mapping
  their consumers to traversal targets; and
- classify unknown expressions/functions as full-path observation or
  unsupported fallback.

At minimum distinguish:

- distance;
- endpoint IDs;
- ordered path IDs;
- full path/entity observation; and
- unsupported/unknown observation.

### Statement-wide safety finalization

Expansion decisions need statement-wide finalization analogous to shortest
decisions. It must reject or conservatively classify:

- multiple variable expansions across clauses or `WITH` boundaries;
- correlated suffixes or correlated endpoint sources;
- cross-region and path-dependent predicates;
- relationship variables/properties not supported by the candidate;
- optional matches;
- all-shortest and shortest constructs in the compound region;
- later mutations or multiple path calls;
- limit-pushdown conflicts;
- unsupported direction or depth; and
- ordered-ID/full-path observations unsupported by the selected state.

Every static fallback code must be reachable in a focused optimizer test.
Different targets in one statement retain independent decisions and reasons.

Static compile-time codes include the existing family-qualified forms of:

| Family | Static reason codes |
|---|---|
| Shortest | `all_shortest_paths`, `correlated_endpoints`, `multiple_endpoint_pairs`, `non_singleton_id`, `multiple_id_equalities`, `path_predicate`, `relationship_predicate`, `relationship_variable`, `directionless`, `optional_match`, `unsupported_depth`, `mutation`, `multiple_path_calls`, `tournament_unqualified` |
| ADCS expansion | `no_fixed_suffix`, `suffix_too_short`, `optional_match`, `shortest_path`, `all_shortest_paths`, `directionless_expansion`, `directionless_suffix`, `unbounded_depth`, `unsupported_depth`, `multiple_variable_expansions`, `correlated_suffix`, `cross_region_predicate`, `path_dependent_predicate`, `relationship_variable`, `relationship_predicate`, `multiple_path_calls`, `limit_pushdown_conflict`, `unsupported_observation`, `mutation`, `tournament_unqualified` |

Runtime codes include shortest `state_limit` and ADCS
`runtime_suffix_density`, `runtime_candidate_limit`, and
`runtime_state_limit`. Static codes require focused optimizer tests. Runtime
codes require exact live branch/threshold tests. Remove unused codes rather
than retaining unreachable vocabulary.

### Lowering precedence and supersession

Executor/search decisions are outer dispatchers for the target region.

- A selected shortest candidate bypasses legacy shortest strategy/filter,
  limit-harness, generic expansion, and workspace construction for consumed
  steps.
- A selected ADCS compound candidate bypasses generic per-step traversal
  direction, suffix pushdown, projection/late-materialization mutations, and
  generic expansion emission for every consumed suffix step.
- Incumbent selection delegates to those legacy lowerings unchanged.
- Target outcomes mark non-consumed legacy decisions with
  `superseded_by_<family_decision>` rather than claiming both applied.

Preflight and precedence must prevent candidate and legacy emitters from
mutating the same frames or bindings.

### Planned, applied, skipped, and runtime outcomes

Use these definitions:

- **planned:** optimizer analysis emitted a target decision;
- **selected:** the compile-time decision chose a named emitter;
- **applied:** that emitter changed the emitted SQL for the target;
- **skipped:** a planned decision did not change SQL, with a target-specific
  reason; and
- **runtime outcome:** GraphBench or execution diagnostics observed the
  mutually exclusive selected or fallback branch.

Selecting `incumbent_workspace` or `stepwise_forward` is not an applied
experimental lowering. Compile-time output reports runtime outcome as unknown.
GraphBench may infer actual branches only from structured plan evidence or an
equally exact side-effect-free signal. An unselected one-time-filter node may
show `Actual Loops=1`; the invariant is zero output rows from that branch and
zero loops in its recursive search/materializer descendants. Gate the recursive
anchor or equivalent subplan so an outer `UNION ALL` filter cannot leave an
eagerly materialized unselected CTE running.

Add a target-aware outcome record containing target kind/coordinates,
selected identity, applied identity, and skip/supersession reason. Traversal
decisions use traversal targets; field requirements retain their natural
`(query_part, symbol)` target. Derive existing aggregate name/count summaries
from these outcome records for compatibility.

`plannedLoweringCounts`/derived aggregates must include field requirements and
shortest executor decisions, and planned/applied/skipped totals must reconcile
per target. A statement-wide “first fallback reason” is insufficient when
several targets exist.

### Forced candidate seam

Before automatic selection, GraphBench and focused tests may force a qualified
emitter through a concrete build-tagged tool API or a narrow deterministic
test/tool options API. GraphBench is a separate package and may not depend on
an inaccessible unexported translator hook. This seam:

- is unavailable through the public query API;
- cannot bypass structural correctness eligibility;
- records forced selection distinctly from adaptive/static selection;
- may remain for deterministic matched regression tests; and
- exposes no public/runtime production configurability or dormant feature
  flag.

Candidate builders preflight the complete region before modifying scope,
frames, aliases, or CTEs. Failed preflight emits byte-identical incumbent SQL
and no partial candidate fragments.

## Sequenced delivery plan

The horizontal lane and the two search lanes may proceed in parallel after L0.
They must use separate branches of evidence and separate candidate binaries.

| Phase | Depends on | Production behavior | Outcome |
|---|---|---|---|
| L0 | entering artifacts | no production query SQL/request-semantic change; benchmark SQL and diagnostics may change | freeze evidence; repair identities and diagnostics |
| L1A-L1D | L0 attribution manifest | one horizontal increment at a time | independently accepted H-AST/H-CODEC/H-ROWS/H-NODE-ID |
| L2F | L0; final confirmation waits for the L1D disposition when H-NODE-ID is reused | forced candidate first | production ADCS-A0 parity increments |
| L2S | L0 | forced candidate first | qualified SP-S3-U-D builder |
| L3M | benchmark tournament after L0; forced builder depends on L2S decision/emitter semantics | benchmark-only, then forced path candidate | select total shortest search/materializer architecture |
| L3A | discovery after L0; final E2E/fallback needs frozen L2F, and path completion needs L3M | forced candidates only | qualify ADCS-A3/A4 and diagnostic planning policy |
| L4 | qualified emitters from L2/L3 | candidate-build selector; incumbent remains the production default | exact static/runtime selectors and fallback |
| L5 | stable residual report; parallel and nonblocking | conditional | close or open relationship IDs, MAT-M2, cache/helper work |
| L6 | accepted L1-L4 portfolio plus any triggered L5 candidate joining this release | release candidate | full semantic/resource/concurrency/soak qualification |
| L7 | L6 | accepted defaults | clean live rerun, durable publication, residual decision |

L2F and L2S are intentionally parallel. L3M and L3A may also run in parallel.
No shared capture is used to claim both lanes' causality.

## Phase L0: Freeze evidence and repair the promotion foundation

L0 changes benchmark identity/reference SQL, structured diagnostics, and tests
only. It must not select an experimental production executor, change incumbent
production query SQL, or change request semantics.

### Durable entering baseline

Publish the current evidence with:

- source commit `7bb291c57fd9a4621360bde7223a99e826b4cc6c`;
- the recorded dirty-diff and binary fingerprints from every raw artifact;
- all ten ADCS round files and checksums;
- all five rounds for each horizontal predecessor/candidate family and every
  balanced materializer reference round;
- fixture declarations, checksums, physical row counts, relation sizes, and
  analyze state;
- PostgreSQL version, partition count, plan-cache mode, settings, and plans;
- Neo4j version and exact logical/public observations;
- source patches and an untracked-file manifest;
- saved benchmark binaries and checksums; and
- the commands, arm order, warmups, sample counts, seeds, and report generator.

The retained ADCS series must record that it contains 40 successful top-level
records, 20 PostgreSQL and 20 Neo4j records, plus 100 successful PostgreSQL
reference observations validated as `exact_public_observation`. It used 20
untimed warmups and 30 measured samples per round with the balanced reference
schedule. It used only `plan_cache_mode=auto`; custom/generic evidence is new
work, not entering evidence.

### Repair architecture identity

Add harness tests that:

- reject distinct non-control architecture IDs with equal normalized SQL
  fingerprints;
- permit a named A/A alias only when the manifest declares it;
- verify advertised state shape from the reference definition;
- verify observation shape and full-comparator status;
- require identical parameter shape and exact validation between compared
  arms; and
- fail if a requested reference silently disappears from a round.

Relabel the current ADCS-A1a duplicate as an A/A control immediately; a later
true ADCS-A1a uses a new implementation ID. Mark the historical ADCS-A1b/A2
implementations invalid for concept-level inference and freeze their corrected
definitions. Rebuild them before L2F/L3A uses another architectural report.
Historical broken arms and results remain in the durable artifact with explicit
rejection reasons.

### Repair materializer factorial identity

Add separate search definitions for SP-S3-U-E and SP-S3-U-NE. The former must
not carry node arrays merely because a shared helper already does. The latter
must expose the incremental bytes, allocations, and planning/execution cost of
node IDs.

Cross these fixed searches with valid materializers and require the report to
show both:

- hydration-only delta under identical ordered IDs; and
- whole-query delta under each architecture's minimal state.

Add outbound and inbound exact cases. If MAT-M0 is direction-specific, encode
direction in its implementation ID and keep directionless fallback explicit.

### Repair lowering telemetry

With incumbent selection unchanged:

1. index shortest-executor and expansion-search decisions by target;
2. add expansion statement-wide finalization;
3. derive observation mode from field requirements;
4. add missing planned-lowering counts;
5. stop recording shortest experimental application merely because a shortest
   pattern exists;
6. report target-specific skipped reasons; and
7. assert that emitted incumbent SQL fingerprints do not change.

Plan-corpus captures must show planned conservative decisions, zero applied
experimental executors, and stable `tournament_unqualified` or structural
fallback reasons.

### Extend the fixture declaration

Predeclare a bounded orthogonal slice, without inspecting candidate timings.
It is not the full Cartesian product: every named case records exact controls,
cardinality, checksum, tier, and the interaction it isolates. The slice covers:

- ADCS zero reachable with many disconnected suffix boundaries;
- ADCS high reverse fan-in;
- no suffix, sparse, half, and all suffix density;
- suffix multiplicity 1, 2, 8, and a high-cardinality tier;
- depth 0/1/2/4/8/16/32/64;
- fanout 1/16/128/512/1000;
- empty, normal, and 4 KiB payloads;
- missing root and graph-colliding IDs;
- shortest linear, recursively branching, diamond, cycle, parallel edge,
  self-loop, dead-end, and dense-disconnected shapes; and
- shortest outbound, inbound, and explicit directionless fallback controls.

Separate discovery fixtures from selector holdouts using fixed checksums.

### L0 exit criteria

- Entering artifacts are reconstructible outside `.coverage`.
- Every architecture identity and SQL fingerprint is explicit.
- Historical ADCS-A1a is honestly labeled A/A, and a true implementation has a
  distinct reserved identity.
- Historical A1b/A2 are explicitly invalid for concept-level inference;
  corrected definitions are frozen and block L2F/L3A tournament use until they
  remove the correlated final all-node trail rescan.
- SP-S3-U-E and SP-S3-U-NE are distinct state shapes.
- Planned/applied/skipped totals reconcile per target.
- Incumbent SQL and public behavior are unchanged.
- Every static fallback reason is reachable in a focused optimizer test, and
  every runtime density/state-limit reason has a declared exact live branch
  test.
- The discovery and holdout matrices are frozen before new tournament timing.

## Phase L1: Independently qualify and lift horizontal increments

Each L1 subphase uses its own immediate predecessor and candidate. Subphases
may be developed in parallel but are confirmed and accepted separately.

### L1A: H-ROWS result metadata and ownership reuse

Run direct unit, race, and live driver coverage for:

- zero, one, and many rows;
- JSON/JSONB and non-JSON fields;
- multiple columns and repeated calls to `Keys()`/`Values()`;
- callers retaining mapped values after advancing rows;
- cancellation and error while decoding;
- result close and physical connection reuse; and
- pool-sized concurrent independent results.

Confirm the dense raw hydration target against an otherwise identical
predecessor binary. Require zero semantic/lifetime mismatch, zero added
allocation on the hot ownership/key paths, general materiality on the affected
case, and affected-family non-inferiority.

### L1B: H-AST bounded parse cache

Test:

- hit, miss, eviction, duplicate text after trimming, invalid query, and
  greater-than-64-KiB bypass;
- concurrent same-key miss coalescing;
- concurrent different-key contention;
- optimizer copy isolation and race behavior;
- bounded retained bytes under 256 varied entries;
- eviction and driver close/teardown release query-key and AST references;
- cache isolation between driver instances; and
- repeated schema, graph, kind generation, and parameter changes proving later
  compilation still executes.

Add cache hit/miss/bypass/eviction/coalesced-miss counters to diagnostic
benchmarks without logging query text. Compare the repeated exact lookup with
an isolated H-AST predecessor/candidate pair and include a high-concurrency
contention block.

### L1C: H-CODEC typed composite decoding

Test binary and text formats for:

- node, edge, path, node array, edge array, and path array;
- empty arrays and zero-length paths;
- NULL internal fields and NULL array elements through the generic fallback;
- copied-buffer ownership after the source buffer is reused;
- unknown or changed field/OID layout;
- direct mapper use and public result scanning;
- large 1,000-entity hydration and 4 KiB payloads;
- cancellation, error, session reuse, and concurrent results; and
- the explicit raw `Result.Values()` compatibility contract.

Confirm node, array, and path allocation/time microbenchmarks plus an isolated
1,000-node live predecessor/candidate run. A mapper-compatible win cannot waive
an unreviewed raw representation break.

### L1D: H-NODE-ID scalar continuation

Cover ID-only fixed and recursive continuation plus negative cases for:

- node properties, kinds, full entity, and downstream path observation;
- aliases and `WITH`;
- optional and correlated clauses;
- following expansions and shared symbols;
- exact fixed ranges and variable ranges;
- mutation/delete/update consumers;
- missing intermediate nodes and dangling edges;
- graph-colliding IDs; and
- endpoint-only versus full-path ADCS output.

Require SQL-shape goldens, optimizer-decision tests, template/mutation coverage,
shared integration semantics, PostgreSQL plan assertions, and isolated D4 and
D16/F1000 E2E confirmation. The full-path control must remain SQL-identical
unless a later separately qualified materializer changes it.

### L1 shipment rule

Each subphase must clear:

```text
affected improvement ratio UCB <= 0.90
median saving LCB >= max(case A/A resolution, 0.10 ms)
affected-family p50 and p95 ratio UCB <= 1.05
```

For nanosecond microbenchmarks, allocation and retained-byte improvement may
establish mechanism, but the production change still requires an E2E or
representative decode boundary above measurement resolution.

### L1 exit criteria

- Each accepted horizontal increment has its own predecessor/candidate
  artifact and rollback boundary.
- Cache bounds, codec compatibility, result lifetime, and scalar semantics are
  documented and tested.
- PostgreSQL and Neo4j complete integration suites pass after each relevant
  public behavior increment.
- Race, cancellation, and session-reuse coverage pass.
- No horizontal result is claimed as evidence for a search architecture.

## Phase L2F: Converge production ADCS forward lowering toward ADCS-A0

L2F is the lower-risk ADCS production track. It uses forward search and lands
only independently qualified transformations.

### Attribution ladder

Start from the accepted production predecessor and build a forced-candidate
ladder:

```text
F0: production incumbent after accepted H-NODE-ID
F1: scalar bound root seed and root reuse
F2: remove redundant invariant root rehydration/lateral rejoins
F3: compact graph-scoped ID-only recursive node existence
F4: late suffix-boundary hydration and direct fixed suffix
F5: complete production ADCS-A0-parity AST
```

Every adjacent pair has a distinct fingerprint and isolated plan delta. Stop
landing steps when the next step is not material or fails controls; a later
compound win may continue in forced-candidate evidence only with a predeclared
factorial/ablation report. If only the compound is material, treat it as one
atomic rollout with its own predecessor confirmation and make no independent
substep performance claim. Never ship a bundle merely by adding individually
non-material point estimates.

### Required plan attribution

Record for every rung:

- SQL bytes and PostgreSQL planning time;
- recursive states and generations;
- root lookup/rejoin loops;
- intermediate node-existence loops;
- fixed suffix edge/node loops;
- path materializer loops;
- shared/local/temp buffers;
- server execution, raw-pgx, and production E2E; and
- exact endpoint/path observations.

The target is to explain the production-to-ADCS-A0 gap, not merely reproduce a
textually similar query.

### L2F acceptance gate

For the final parity candidate:

```text
production_candidate_raw_pgx / ADCS-A0-SQL UCB <= 1.10
```

or the absolute remaining gap upper bound is below A/A resolution. Separately,
the production E2E candidate clears the general materiality gate against F0.
Endpoint and path controls must be non-inferior, and no rung may weaken orphan
filtering, graph scope, trail uniqueness, or duplicate multiplicity.

### L2F exit criteria

- A real root-reuse comparator exists.
- The production AST builder reaches reference closure or records the exact
  remaining planner/emitter gap.
- Every accepted rung has focused optimizer, golden, mutation/template, and
  integration tests.
- Ineligible/non-ADCS shapes keep incumbent SQL.
- The accepted forward candidate becomes the new ADCS predecessor/fallback for
  L3A; direct handwritten SQL never becomes the production implementation.

## Phase L2S: Qualify SP-S3-U-D distance-only production lowering

L2S proceeds independently from L2F.

### Eligibility envelope

Initial eligibility remains the conservative singleton envelope from
`perf_cont_2.md`:

- `shortestPath`, not `allShortestPaths`;
- exactly one bounded variable-length traversal;
- one literal/parameter integer-ID equality per endpoint;
- one endpoint pair and no correlated or multi-row source;
- read-only, non-optional statement;
- supported outbound or inbound direction;
- supported relationship kind predicates;
- qualified minimum/maximum depth;
- no relationship variable/property or path-dependent predicate;
- no conflicting second path call or later mutation;
- distance-only observation proven through aliases/`WITH`; and
- graph-scoped endpoint validation before search.

Missing/invalid endpoints invoke no search. Same-endpoint zero-length and
minimum-one behavior is resolved before recursive state is allocated.

### Production emitter

Implement SP-S3-U-D through repository-native PostgreSQL AST nodes. Do
not inject the benchmark SQL string. Preflight eligibility before altering the
translation frame. Keep SP-S0 incumbent workspace byte-identical for fallback.

The forced candidate must emit stable SQL for different endpoint values and
record a genuinely applied SP-S3-U-D decision only when that SQL is emitted.

### Qualification matrix

Cover depths 0/1/2/4/8/16/32/64, fanout 1/16/128/512/1000, outbound/inbound,
linear/branching/diamond/cycle/parallel/self-loop/dead-end/disconnected, kind
filters, missing/contradictory endpoints, graph collisions, cold/warm sessions,
and pool-sized concurrency.

Record examined edges, recursive states, retained bytes, shared/local/temp
buffers, planning/execution, raw pgx, E2E, cancellation latency, and session
reuse.

### Alternative closure

Run SP-S0, exact SP-S3-U-D, and at least one genuine SP-S1/SP-S2
prototype at identical boundaries, or apply a predeclared feasibility closure.
SP-S3-B stays a historical control but does not satisfy the SP-S2 obligation.

### L2S exit criteria

- Distance state contains no trail/predecessor/materializer representation.
- Exact semantics pass the complete singleton adapter.
- Normal tiers have no temp/local workspace or WAL from the candidate.
- Candidate/reference raw-pgx UCB is at most 1.10 or the gap is below
  resolution.
- Production E2E clears general materiality and controls are non-inferior.
- D32/D64 and dense-disconnected tiers meet time and memory ceilings.
- Cancellation returns within the inherited bound and the session is reusable.
- A genuine alternative is measured or explicitly closed.
- Automatic dispatch remains off through L4/L6; the forced builder is ready
  for L7 activation only after those gates pass.

## Phase L3M: Select the shortest path state/materializer architecture

### Correct whole-architecture tournament

Compare at minimum:

| Search state | Materializer | Purpose |
|---|---|---|
| SP-S3-U-E | MAT-M0 outbound | lean edge-only architecture |
| SP-S3-U-E | MAT-M0 inbound | direction-aware inbound architecture |
| SP-S3-U-NE | MAT-M1 | direction-independent ordinal hydration given node IDs |
| SP-S0 | incumbent materializer | production control |

Hydration-only comparisons reuse identical ordered IDs. Whole-query
comparisons use each architecture's minimal search state. Reports show both
and never substitute one for the other.

### Path semantics and scale

Cover singleton E2E lengths 0/1/2/4/8/16/32/64, outbound/inbound, valid
equal-length ties, parallel edges, self-loops, cycles, repeated nodes without
relationship reuse, disconnected results, and empty, normal, and 4 KiB entity
payloads. Because the eligible bound-pair `shortestPath` returns at most one
row, output cardinalities 4/32/128/1000 are materializer-only batched controls
or later MAT-M2/generic/ADCS cases, not singleton E2E cases.

Measure recursive bytes, transfer bytes, materializer server execution,
allocations, decoded retained bytes, planning, spill, and full E2E.

### L3M selection rule

Select an architecture only when it is not Pareto-dominated on p50, p95,
planning, execution, retained state, transfer, allocations, spill, cold cost,
or concurrency. If one architecture dominates within confidence/resource
budgets, select it. If several are non-dominated but win stable predeclared
directions/tiers, retain a static portfolio only after its decision rule clears
the selector-regret gate. If the tradeoff has no stable partition or frozen
workload-weighted rule, do not declare one winner; keep the incumbent
production path and the candidates benchmark-only. A direction-specific split
is allowed when its static eligibility is exact and observable.

Retain the `perf_cont_2.md` path-tax and linearity gates. In addition, the
whole selected stack must reach the best correct same-boundary reference
within 1.10 or absolute resolution.

### L3M exit criteria

- Search and hydration costs are independently measurable.
- MAT-M0 is priced with edge-only search state.
- MAT-M1 is priced with the incremental node-ID state it requires.
- Exact node/edge order, direction, duplicates, properties, and graph scope
  pass.
- Endpoint/distance modes perform zero materialization.
- The selected architecture has an explicit direction/resource envelope.
- A forced production path builder passes optimizer, golden, integration,
  cancellation, and concurrency tests.
- Automatic path dispatch remains off through L4/L6 and is eligible for L7
  activation only after those gates pass.

## Phase L3A: Qualify ADCS-A3/A4 and PostgreSQL planning

### Corrected tournament arms

Run at least:

- accepted production forward predecessor from L2F;
- ADCS-A0-SQL reference;
- corrected ADCS-A2 when it is non-dominated on a discovery tier;
- ADCS-A3 endpoint and ordered-ID forms;
- ADCS-A4 endpoint and ordered-ID forms;
- ADCS-A3/A4 crossed with the selected applicable MAT-M0/M1 forms; and
- the incumbent production full-result boundary.

ADCS-A1b remains only if its corrected implementation is a genuine independent
candidate. Do not pad the tournament with invalid historical arms.

### Search regime matrix

The discovery matrix varies independently:

- forward fanout and depth;
- reachable suffix boundary count;
- disconnected/false suffix boundary count;
- reverse fan-in;
- suffix multiplicity;
- output trail cardinality;
- zero-depth root suffix;
- endpoint versus full path; and
- entity payload.

The zero-reachable and high-reverse-fan-in v2 cases are mandatory primary
crossover diagnostics. Separate checksummed fixtures remain unseen selector
holdouts until thresholds are frozen.

### Planner-policy tournament

For identical candidate semantics and stable SQL fingerprints, capture:

- `auto`, `force_custom_plan`, and `force_generic_plan`;
- first execution and prepared reuse;
- parent-table and partition-targeted forms where both are production-safe;
- representative graph/partition counts;
- planning and execution separately; and
- parameter values spanning sparse/dense regimes without changing SQL.

Report the two causal dimensions factorially: compare each emitter under the
same diagnostic plan mode, then compare auto/custom/generic for a fixed
emitter. Never attribute a plan-mode movement to A3/A4 search architecture.
Forced modes remain diagnostic unless separately promoted through the driver
increment defined above.

Reject a lower-planning policy that loses required graph pruning, changes
results, or regresses execution enough to fail total E2E gates. The production
solution may not require a global PostgreSQL setting.

### L3A acceptance rule

The original `perf_cont_3.md` sparse search-direction gates remain unchanged.
Current ADCS-A3 point estimates fail them, so no current artifact authorizes
activation. New confirmation occurs only after architecture/planning changes
and uses fresh samples.

ADCS-A4 remains a selector candidate only if it wins or materially reduces
resource/tail risk on a predeclared crossover tier. Corrected ADCS-A2 remains
only if non-dominated on dense/overflow tiers.

### L3A exit criteria

- A3/A4 forced AST builders are exact and stable, with no injected SQL text.
- Sparse gates pass or the sparse production candidate remains closed.
- Zero-result, reverse-fan-in, dense, multiplicity, and payload results are
  complete.
- Planning is separately attributed under all required plan-cache modes.
- Search and materialization winners are selected independently.
- Candidate limits and initial selector hypotheses are frozen before holdouts.
- Incumbent/accepted forward SQL remains the production default until L7.

## Phase L4: Prove bounded selection and exact fallback

### Start with static selection

Prefer a static structural envelope when it bounds all allowed data
distributions. Static shortest selection may use observation, direction,
depth, and predicate facts. Static ADCS selection may use only facts whose
bounds are known without running the search.

Runtime probes are added only when holdouts show that data-dependent suffix
density or reverse state materially changes the winner.

### Runtime selector contract

When required, probes must be bounded and side-effect-free. Record:

- suffix rows and distinct boundaries up to a cap;
- reverse states up to a cap;
- whether the cap was exceeded;
- selected strategy and selector version; and
- exact fallback/overflow reason.

The query uses mutually exclusive result branches in one statement/snapshot.
Overflow discards partial candidate state and executes the exact accepted
forward fallback. Missing roots execute no suffix work and no recursion.

### Threshold tests

For every limit, test:

- threshold minus one;
- threshold;
- threshold plus one;
- unknown/unavailable estimate;
- cap overflow after partial work;
- zero result;
- false boundaries;
- cancellation during probe, candidate, and fallback; and
- session reuse after each outcome.

### Selector gates

Use discovery-independent holdouts and the simultaneous regret method from
`perf_cont_3.md`:

```text
maximum p50 selector-regret UCB <= 1.15
maximum p95 selector-regret UCB <= 1.25
decision overhead <= max(0.10 ms, 5% of selected-arm latency)
fallback-control p50/p95 UCB <= 1.05
```

Probe plus overflow plus complete fallback must meet the declared case timeout
and resource ceiling. Only the selected branch may return rows, and every
unselected recursive search/materializer descendant has zero loops; one-time
filter nodes are not the branch invariant. If no selector passes, restrict to a
static envelope or retain forward search.

### L4 exit criteria

- Every automatically selected executor already passed its raw and forced E2E
  phase gates.
- Static eligibility and runtime bounds are versioned and observable.
- Threshold and overflow semantics are exact.
- Same-snapshot fallback is proven or the candidate is statically restricted.
- Selector regret, overhead, resource, cancellation, and concurrency gates
  pass.
- Automatic activation is still separated by observation boundary for L6/L7
  confirmation.

## Phase L5: Conditional residual work

L5 opens only from a stable residual report after the lower layers are fixed.
It is not a parking lot that must all be implemented.

### Relationship-ID scalar continuation

Trigger a discovery candidate only when an accepted query family still spends
material time carrying or hydrating relationship composites for ID-only
consumers. Reuse `FieldRequirementRelationshipIDs`, but keep the implementation
separate from H-NODE-ID.

The semantic matrix must cover relationship kind/property/full-entity
consumers, relationship variables, path construction, deletes/updates,
direction, parallel edges, aliases/`WITH`, collection membership, and unknown
functions. Carrying an ID may not discard information required to distinguish
parallel relationships or construct a path later.

Close the candidate if an isolated affected family cannot clear general
materiality without a control regression.

### MAT-M2 high-cardinality batching

Open MAT-M2 only if accepted MAT-M0/M1 still leaves material hydration work at
output cardinalities 128/1000. Batch across rows using a stable row ordinal,
hydrate distinct entities set-wise, and reconstruct every row with exact order
and multiplicity.

MAT-M2 must clear the high-cardinality gate and keep the common one-path case
within the 5% non-inferiority budget. Otherwise close it.

### Translation or rendered-SQL caching

The H-AST cache does not imply translation caching. Open a later compilation
cache only when stable production SQL leaves at least 10% and 0.10 ms of
isolated repeated-query client compilation cost after H-AST.

Any cache key must account for graph, schema, kind-generation, optimizer
version, parameter shape/type, query text, and every dependency shown to alter
SQL or parameters. Invalidation, bounded memory, concurrent miss coalescing,
and mutation isolation are mandatory. If these keys cannot be made complete,
close the cache.

### Typed helper, index, statistics, or native extension

Open one of these only after portable SQL architecture and planning policy are
stable and the remaining measured gap exceeds the inherited trigger. Each is a
separate ADR, schema/migration plan, read/write experiment, rollback path, and
production increment.

Do not use a helper to disguise unstable dynamic SQL, an index to compensate
for wrong search order, or a native extension to skip the portable reference
tournament.

### ADCS-A5 and frontier mechanics

Do not build ADCS-A5 meet-in-the-middle search unless both ADCS-A3 and ADCS-A4,
after accepted planning and materialization work, remain more than 10% and
more than 0.50 ms slower than the best correct same-boundary PostgreSQL
reference. Otherwise close A5.

Likewise, reopen frontier mechanics only when the selected search still leaves
a material execution/search residual after planning is separated. Planning
latency alone cannot trigger frontier tables, helpers, indexes, or native code.
Any triggered A5/frontier experiment is benchmark-only until it independently
passes the same correctness, reference-closure, resource, and production E2E
gates.

### L5 exit criteria

- L5 is parallel and nonblocking: only a triggered, ready L5 candidate joins a
  later L6 release confirmation.
- Every deferred conditional item receives an explicit triggered/deferred/
  closed disposition before this continuation closes.
- Triggered items have independent comparator, correctness, resource, and
  rollback plans.
- Rejected code is absent from production paths.
- L5 changes do not delay already-qualified independent activations.

## Phase L6: Full release-candidate qualification

L6 uses accepted implementations and frozen selectors. It does not tune
thresholds from its confirmation samples.

### Cumulative release-candidate chain

Save and qualify cumulative binaries in the same partial order intended for
activation, so every L7 “immediate predecessor” has already received semantic,
corpus, resource, concurrency, and soak coverage rather than timing alone:

```text
each accepted H increment: actual chosen predecessor -> predecessor + H

shortest: accepted horizontal base -> SP-S3-U-D -> selected SP path/MAT

ADCS: accepted horizontal base
      -> H-NODE-ID when reused
      -> production ADCS-A0 parity
      -> A3/A4 endpoint envelope
      -> A3/A4 full-path envelope
      -> adaptive selector, if selected
```

Independent horizontal and search-family edges may be qualified in parallel,
but a combined portfolio binary does not replace the saved edge-by-edge
evidence.

### Validation workflow for every production increment

For relevant code changes:

1. run focused unit/optimizer/translator/driver tests;
2. update translation source cases and generated artifacts through the
   repository workflow;
3. run `make format`;
4. run `make test`;
5. run PostgreSQL `make test_all` with the supplied PostgreSQL
   `CONNECTION_STRING`;
6. run Neo4j `make test_all` with the supplied Neo4j `CONNECTION_STRING`;
7. run focused race tests for caches, codecs, results, and shared analysis;
8. run PostgreSQL-scoped plan/resource integration tests;
9. run cancellation, rollback, and physical-session reuse tests; and
10. run matched performance confirmation with saved binaries.

The integration suite runs only the backend selected by the connection-string
scheme. Shared integration cases remain backend-equivalent.

### Cross-phase correctness matrix

| Dimension | Horizontal | Shortest | ADCS | Selector/fallback |
|---|---|---|---|---|
| Empty/missing/null | cache invalid/miss and NULL codec fallback | missing endpoints, same endpoint | missing root/suffix/intermediate | no candidate work or exact fallback |
| Graph scope | per-driver graph compilation remains fresh | colliding endpoint/edge IDs | colliding root/boundary IDs | probes and both branches scoped |
| Direction | codec preserves endpoints | outbound/inbound; directionless fallback | qualified directed compound only | direction part of eligibility |
| Depth | unchanged parse/translation semantics | 0/1/2/4/8/16/32/64 | 0/1/2/4/8/16/32/64 | both sides of bounds |
| Duplicates | decoded arrays and keys unchanged | valid tie and parallel edges | trail/suffix/root bag multiplicity | partial candidate rows discarded |
| Observation | raw/mapped values stable | distance versus one path | endpoint versus full path | selected state supports observation |
| Predicates | optimizer still runs per call | endpoint/kind and unsupported path predicates | root/suffix/path/cross-region | ineligible predicates fall back |
| Statement | transaction/error/reuse | aliases, `WITH`, two path calls, mutation | multipart, optional, mutation | one target does not mask another |
| Concurrency | cache/codec/result race and bounds | pool search state | pool suffix/reverse state | simultaneous branch/resource bounds |
| Cancellation | decode/cache miss cleanup | search cancellation | planning/search/hydration cancellation | probe/candidate/fallback cancellation |

### Planning and partition dimensions

Run supported PostgreSQL versions and representative graph partition counts.
For affected SQL fingerprints capture `auto`, forced custom, and forced generic
plans, first-use and prepared reuse, graph pruning, planning time, execution
time, shared/local/temp buffers, and plan invariants.

Assertions target semantic operators, access direction, branch loops, state
counts, and pruning rather than brittle complete plan text.

### Resource and slope envelope

Record:

- examined edges and recursive/search states;
- retained bytes per state and total process/session memory;
- materialized suffix/viability rows and bytes;
- transfer and decoded retained bytes;
- shared reads/hits, local buffers, temp files/bytes, and WAL;
- p50, p95, diagnostic p99, throughput, and pool wait;
- planning, execution, raw-pgx, compile, and E2E intervals; and
- cleanup/reuse after success, error, cancellation, and rollback.

Normal tiers require no temp spill, no local workspace for the new portable
candidate, and no WAL for read-only queries. No unexplained adjacent-tier
increase above 1.25 in time per examined edge or bytes per retained state is
allowed.

### Concurrency, cancellation, and soak

Run one connection, half pool, full pool, and twice-pool offered load. Include
shortest-only, ADCS-only, horizontal lookup/hydration, dense fallback, and mixed
traffic.

Require bounded whole-pool memory, correct results, no state leaks, no
transaction-abort leak, and oversubscription expressed through pool wait rather
than unbounded backend state. Preserve the stricter ADCS concurrency gates from
`perf_cont_3.md`.

Cancel searches during endpoint validation, planning/execution where
observable, search, materialization, runtime probe, and fallback. A cancelled
100 ms search returns control within 250 ms, and an exact query succeeds on the
same physical session afterward.

Run a duration/operation-count soak predeclared in the artifact manifest. It
must include connection churn and prepared-plan reuse.

### L6 exit criteria

- All focused, unit, integration, race, plan, cancellation, rollback, and
  session-reuse tests pass.
- Every declared PostgreSQL-supported record succeeds; every public-query
  target/control has its declared exact Neo4j oracle record; PostgreSQL-only
  raw reference, plan, codec, and materializer arms carry explicit backend
  declarations rather than impossible Neo4j requirements.
- Complete-corpus performance and affected-family non-inferiority pass.
- Plan-cache modes and partition dimensions have no correctness or pruning
  failure.
- Resource, slope, concurrency, memory, cancellation, and soak gates pass.
- Accepted SQL closes its correct PostgreSQL reference at the same boundary.
- Every selected/fallback diagnostic matches actual SQL and branch loops.
- No threshold is modified using L6 confirmation samples.

## Phase L7: Activate narrow defaults and publish the result

### Activation partial order

Each accepted H-ROWS, H-AST, H-CODEC, and H-NODE-ID increment activates
independently from its saved predecessor; their relative order follows actual
readiness rather than one synthetic bundle.

The search-family dependencies are:

```text
shortest: accepted base
          -> SP-S3-U-D
          -> singleton path + selected MAT-M0/M1

ADCS: accepted base
      -> H-NODE-ID, only when reused by the ADCS builder
      -> production ADCS-A0 parity
      -> endpoint-only static A3/A4 envelope
      -> full-path envelope + selected materializer
      -> adaptive density/state selector, only when L4 proves it necessary
```

Cross-family edges do not block one another; accepted ADCS forward parity need
not wait for shortest path. Each activation uses the matching L6 cumulative
binary, has a fresh immediate-predecessor confirmation, and can be rolled back
through a forward source change that selects the preceding executor.

### Clean live rerun

After release-candidate acceptance:

- rebuild from the accepted source state;
- reload and validate physical fixtures;
- rerun PostgreSQL and Neo4j exact integration;
- run the primary matched PostgreSQL predecessor/candidate confirmation;
- run a fresh current PostgreSQL versus Neo4j contextual report;
- run the complete performance corpus and plan corpus;
- publish raw samples, plans, manifests, A/A, statistics, and checksums; and
- issue a residual cost report ranked by absolute cost times observed workload
  frequency where production frequency data is available.

Neo4j ratios appear in context but do not decide pass/fail.

### L7 exit criteria

- Accepted defaults are narrow, observable, and independently reversible.
- Public/runtime force configurability and dormant flags are removed;
  deterministic build-tagged/test-tool seams may remain in source.
- Generic incumbent paths remain tested semantic fallbacks.
- The durable bundle reconstructs every causal claim.
- Remaining candidates are ranked, triggered, or explicitly closed.
- A new continuation opens only for a measured residual that clears its
  trigger.

## Metrics and plan invariants

### Primary timing and allocation metrics

Capture as applicable:

- parse/cache lookup time, hit/miss classification, allocations, and retained
  cache bytes;
- optimize, translate-including-optimize, render, and total client compilation
  without summing overlapping intervals;
- PostgreSQL planning and execution;
- prepared first-use and reuse;
- transfer, pgx decode, mapping, drain, and public E2E;
- allocations and allocated/retained bytes per row and per result; and
- p50, p95, diagnostic p99, max, QPS, pool wait, and cold cost.

### Search and hydration metrics

- seed rows, recursive generations, and states;
- examined edge rows and node-existence probes;
- suffix rows, distinct boundaries, false boundaries, and multiplicity;
- reverse/viability states and cap/overflow state;
- accepted/output trails;
- ordered node/edge IDs and bytes;
- hydration rows/loops and materializer execution;
- shared/local/temp buffers and spill bytes; and
- result-branch actual loops.

### Horizontal invariants

- H-AST hits execute no parse and allocate zero cache-hit bytes in the focused
  benchmark.
- H-AST misses do not skip optimization/translation and the cache never exceeds
  its declared bound.
- H-CODEC typed decoding never aliases a reusable pgx buffer and the generic
  NULL fallback remains exact.
- H-ROWS builds keys once per result set and never exposes one row's mutable
  values as another row.
- H-NODE-ID retains a graph-scoped node-existence join and never scalarizes a
  composite-observed symbol.

### Shortest invariants

- SP-S3-U-D contains no trail/predecessor/materializer state.
- Missing endpoints execute zero search loops.
- Only the selected executor returns rows; unselected recursive search/
  materializer descendants have zero loops.
- SP-S3-U-E contains no ordered node array.
- MAT-M0 hydrates ordered edges once and derives nodes linearly.
- SP-S3-U-NE+MAT-M1 prices node-ID recursive state and hydrates both streams by
  ordinality.
- No materializer re-runs search.

### ADCS invariants

- Missing roots execute zero suffix and recursive loops.
- Production ADCS-A0 parity removes identified invariant root/suffix loops
  without changing forward states.
- ADCS-A3 sparse reverse states remain within the declared fixture/bound and
  do not scale with all forward dead ends inside its envelope.
- ADCS-A4 viability is a permissive filter; exact forward enumeration restores
  trails/multiplicity.
- A3/A4 preserve cross-segment relationship uniqueness and ordered IDs.
- Endpoint-only output invokes no path materializer.
- Unselected candidate/fallback recursive search/materializer descendants have
  zero loops and their result branches return zero rows.

## Statistical protocol

### Discovery and confirmation remain separate

Use discovery samples to select architecture, state, thresholds, and planner
policy. Final confirmation uses new saved binaries and fixtures after the
selection is frozen.

Unless a stricter inherited phase applies, final primary confirmation uses:

- ten independently reloaded matched rounds;
- twenty untimed warmups and fifty measured warm samples;
- predecessor/candidate order reversed on alternating rounds;
- same-binary within-session and block/reload A/A;
- predeclared extension by five rounds, to at most twenty, only when confidence
  remains insufficient;
- bootstrap matched round medians and stratified p95 with recorded seed; and
- 97.5% intervals or Holm adjustment for paired endpoint/path primary
  hypotheses.

Abort a block on source, binary, SQL, fixture, schema, relation size, settings,
result, connection, maintenance, either arm's predeclared plan-class invariant,
or host-saturation mismatch. Predecessor and candidate plans may intentionally
differ; each must match its own declared invariant.
Every expected-supported record must be `ok`; every expected-unsupported record
must match its declared status/reason; no record may be omitted or become an
unexpected error. Do not compare only the successful intersection.

Keep p99 diagnostic until both an A/A-derived requirement and at least 10,000
observations per gated series exist.

### General materiality and non-inferiority

Unless a stricter gate below applies:

```text
improvement ratio UCB <= 0.90
median saving LCB >= max(case A/A absolute resolution, 0.10 ms)
```

For affected-family controls:

```text
p50 and p95 ratio UCB <= 1.05
```

or the absolute increase UCB is no more than
`max(0.10 ms, case-specific A/A resolution)`.

The complete-corpus 20% threshold remains an emergency ceiling, not permission
for an unexplained smaller regression.

### Reference closure

At identical raw-pgx boundaries:

```text
production_candidate / best_correct_reference UCB <= 1.10
```

or the absolute remaining-gap UCB is below
`max(case A/A resolution, 0.10 ms)`. Report production E2E predecessor
improvement separately.

### Existing stricter gates remain fixed

Retain without post-hoc weakening:

- the `perf_cont_3.md` ADCS sparse search-direction gates;
- its selector regret, path materialization, resource/slope, and concurrency
  gates;
- the `perf_cont_2.md` singleton semantic/resource envelope and materializer
  path-tax gates; and
- every test/rollback requirement in the repository instructions.

Current ADCS-A3 evidence is structurally strong but timing-unqualified. Current
SP-S3-U/MAT evidence is reference evidence but not a production-candidate
confirmation.

### Alternative closure rule

An alternative architecture closes only when:

- a correct prototype is Pareto-dominated across its declared envelope;
- a predeclared feasibility analysis proves it cannot meet correctness/state
  bounds.

“Likely slower” and implementation effort are not closure evidence.
For the required singleton tournament, SP-S0, the SP-S3-U family, and at least
one genuinely different exact architecture or feasibility closure remain
mandatory; reference closure alone does not waive that obligation.

## Architecture-specific acceptance gates

### H-AST gate

- Cache-hit parse work is zero allocations and reproduces a matched material
  ratio improvement beyond A/A resolution; the entering 214-235 ns range is
  context, not a portable absolute threshold.
- Invalid and greater-than-64-KiB input never enters the cache.
- Entry count and retained bytes remain bounded under churn.
- Query-text/literal retention has an explicit accepted lifecycle; eviction and
  driver teardown release key/AST references.
- Same-key misses coalesce without deadlock; different-key contention clears
  the concurrency non-inferiority gate.
- Optimizer copy/race tests prove cached AST immutability.
- Repeated-query E2E clears general materiality against the isolated
  predecessor.

### H-CODEC gate

- Every typed/generic binary/text/NULL case maps to the exact public graph
  value.
- Raw `Result.Values()` compatibility is documented and deliberately accepted;
  an accidental representation break fails.
- No decoded composite aliases a reusable source buffer.
- Node, array, and path microbenchmarks reproduce the typed-decoding allocation
  mechanism beyond A/A noise.
- Isolated 1,000-node/path E2E clears general materiality and codec controls
  remain non-inferior.
- Race, cancellation, close, and pool reuse pass.

### H-ROWS gate

- Field keys are built once per result set and remain immutable for the result
  lifetime.
- In-place JSON replacement never exposes one row's mutable values as another
  row or after an invalid lifetime.
- Field-key and value-ownership microbenchmarks reproduce zero-allocation hot
  paths beyond A/A noise.
- The isolated dense raw hydration target clears general materiality and result
  controls remain non-inferior.
- Retained-row, close, error, race, cancellation, and physical-session reuse
  tests pass.

### H-NODE-ID gate

- Field requirements prove ID-only use through aliases and `WITH`.
- The graph-scoped node-existence join remains in SQL.
- Endpoint D4 and D16/F1000 clear general materiality in isolated binaries.
- Full-path and composite-observed controls preserve predecessor SQL unless a
  separately accepted later phase changes it.
- Orphan, optional, multipart, mutation, and graph-collision semantics pass.

### Production ADCS-A0 parity gate

- Final production raw-pgx/reference UCB is at most 1.10 or the absolute gap is
  below resolution.
- Production E2E clears general materiality versus its immediate forward
  predecessor.
- Root, node-existence, and fixed-suffix loop reductions are attributed.
- Planning/SQL-size movement does not offset execution gains.
- Endpoint/full-path exactness and affected-family controls pass.

### SP-S3-U-D distance gate

- Exact singleton semantics pass at depths through 64, outbound/inbound, and
  disconnected/branching/cycle/parallel/self-loop shapes.
- Distance state has no path representation.
- Normal tiers have no temp/local workspace, spill, WAL, or unbounded retained
  state.
- Adjacent-tier slope, cancellation, concurrency, and session reuse pass.
- Raw reference closure and production materiality pass.
- A genuine SP-S1/SP-S2 alternative is measured or closed by rule.

### Shortest path state/materializer gate

- The comparison prices total minimal search state and hydration.
- The selected stack is not Pareto-dominated on p50, p95, planning, execution,
  memory, transfer, allocations, spill, cold cost, or concurrency.
- Exact path order, direction, duplicates, properties, and graph scope pass.
- Length 32-to-64 execution and retained bytes grow by at most the inherited
  `2.2` bound.
- Paired path-tax UCB is at most 0.25 ms on the small generic fixture and
  0.35 ms on the inherited ADCS P1 boundary; the D16/F1000 two-path ADCS
  ordered-ID-to-full-path tax is at most 1.0 ms.
- Distance/endpoint modes perform zero materialization.
- Same-boundary reference closure and inherited path-tax gates pass.

### ADCS-A3/A4 gate

On both sparse D16/F1000 endpoint and path forms, preserve these
`perf_cont_3.md` thresholds against ADCS-A0-SQL:

```text
median-ratio UCB <= 0.25
p95-ratio UCB <= 0.40
median-saving LCB >= 30 ms
shared-hit ratio <= 0.10
search-state ratio <= 0.02
```

Also require exact two-row observations and no temp/local I/O.

- Run zero-result, high reverse fan-in, density, multiplicity, depth, payload,
  and discovery-independent holdouts.
- Planning and execution are reported separately under auto/custom/generic
  plan modes.
- No global planner setting is required.
- A4 remains only when it wins or bounds a crossover tier; corrected A2 remains
  only when non-dominated on dense/overflow.
- Full-path search/materializer compounds clear exactness and reference
  closure separately from endpoint search.

### Selector and fallback gate

- Static and runtime decisions are deterministic for equivalent analyzed
  shapes.
- Selector regret and overhead clear the L4 numeric gates.
- Threshold-1/threshold/threshold+1, unknown, overflow, missing-root, false
  boundary, cancellation, and reuse cases pass.
- Partial candidate results never escape.
- Only one result branch returns rows; unselected recursive search/materializer
  descendants have zero loops.
- Complete probe+candidate/fallback time and resources fit the declared bound.

### Resource and concurrency gate

- No normal-tier temp file, local workspace, or read-only WAL for portable
  candidates.
- No unexplained adjacent-tier time-per-edge or bytes-per-state increase above
  1.25.
- D64/F1000 and dense-disconnected operations, including fallback, complete
  within the inherited two-second normal timeout.
- Half/full/twice-pool traffic has correct rows, bounded memory, no state leak,
  and no unexpected error.
- Accepted ADCS sparse traffic requires candidate/predecessor p95-ratio UCB at
  most 0.75 and full-pool QPS-ratio LCB at least 1.5. Dense/fallback/mixed
  controls require p95-ratio UCB at most 1.05 and QPS-ratio LCB at least 0.95.
- Cancellation and rollback preserve physical-session reuse.

## Implementation seams

### GraphBench and fixtures

Primary files include:

- `cmd/graphbench/references.go` for canonical architecture/state/materializer
  definitions and exact reference validation;
- `cmd/graphbench/references_test.go` for fingerprint and identity contracts;
- `cmd/graphbench/datasets.go` and `datasets_test.go` for fixture declaration
  and physical-cardinality proofs;
- `cmd/graphbench/measure.go`, `results.go`, and `types.go` for timing boundaries,
  decisions, state, and planner metrics;
- `cmd/graphbench/postgres.go` and `postgres_plan.go` for raw-pgx execution and
  structured plan attribution;
- `cmd/graphbench/confirm_report.go`, `perf_gate.go`, and report tests for
  matched statistics and gates;
- `benchmark/testdata/scale/cases/generated_shortest_paths.json`;
- `benchmark/testdata/scale/cases/generated_adcs.json`; and
- `benchmark/testdata/scale/README.md` and `cmd/graphbench/README.md` for
  reproducible workflow changes.

Do not embed production selection logic in GraphBench. It may force internal
emitters and run references, but production eligibility remains in the
optimizer/lowering model.

### Optimizer and translator

Primary seams include:

- `cypher/models/pgsql/optimize/lowering.go` for typed decisions, candidate
  identities, observation modes, facts, limits, and stable reasons;
- `cypher/models/pgsql/optimize/lowering_plan.go` for target analysis and
  statement-wide finalization;
- `cypher/models/pgsql/optimize/source_references.go` for field requirements
  and alias/use analysis;
- optimizer tests for every eligibility fact, reason, observation, and target;
- `cypher/models/pgsql/translate/translator.go` for target indexes and truthful
  planned/applied/skipped accounting;
- `cypher/models/pgsql/translate/pattern.go`, `traversal.go`, and
  `expansion.go` for shortest pattern assembly and forced/selected emitters;
- `cypher/models/pgsql/translate/model.go`, `function.go`, and `projection.go`
  so `length(p)` can consume SP-S3-U-D scalar depth without manufacturing a
  `PathComposite`;
- a whole-region interception in `translateTraversalPatternPart`/
  `buildTraversalPatternPart` for ADCS, with explicit consumed suffix steps and
  final frame/binding construction;
- `cypher/models/pgsql/translate/expansion.go` for scalar continuation and the
  typed compound ADCS region emitter; and
- translation cases/goldens plus optimizer safety, graph-scope, template, and
  mutation tests.

Build candidate SQL with the PostgreSQL model/AST. Do not insert benchmark SQL
strings into the translator. Preflight an entire region before mutating scope,
frames, aliases, or emitted CTEs.

ADCS-A3 physically traverses edges in reverse while preserving the logical
path direction and final binding contract. It must not call global
`FlipNodes()` or mutate logical path direction as an implementation shortcut.

### Driver and client runtime

- `drivers/pg/query_cache.go` and tests own H-AST.
- `drivers/pg/composite_codec.go`, `types.go`, `manager.go`, `mapper.go`, and
  their tests own H-CODEC.
- `drivers/pg/result.go` and tests own H-ROWS.
- `drivers/pg/transaction.go` wires parse reuse without caching later
  compilation stages.

Keep diagnostics aggregate and privacy-safe; never expose query text or
credentials in artifacts.

### Schema and materialization

Portable inline SQL is preferred. Existing `ordered_edge_ids_to_path` remains
the generic fallback while MAT-M0/M1 are qualified. Any selected helper change
requires:

- typed graph-scoped inputs/outputs;
- schema up/down/up and repeated assertion tests;
- existing-installation forward migration and compensating rollback plan;
- cancellation, error, and concurrent-session coverage;
- realistic volatility/parallel/row declarations; and
- independent evidence that the helper boundary beats stable portable SQL.

Do not use full teardown `schema_down.sql` as an installed-release rollback
mechanism.

### Integration and documentation

Public semantic cases stay in `integration/testdata/cases` and templates with
backend-equivalent expectations. PostgreSQL plan/resource behavior belongs in
scoped integration tests.

Update `README.md`, `docs/postgresql_translation.md`, GraphBench documentation,
fixture documentation, and migration instructions whenever production
behavior, commands, environment variables, diagnostics, or driver contracts
change.

## Observability contract

Every translated candidate target exposes enough structured information to
answer:

```text
what was recognized?
what observation was required?
what candidates were eligible?
what was selected at compile time?
what SQL emitter actually applied?
what static fallback reason applied?
what limits and selector version were used?
which branch actually ran?
did runtime overflow/fallback occur?
```

Required fields include target coordinates, family-qualified identity,
observation mode, eligibility facts, selected/fallback identities and reason,
limits, selection mode/version, applied identity, SQL fingerprint, plan-cache
mode, and runtime outcome when measured.

Runtime branch inference uses exact plan loop evidence or another
side-effect-free signal. It never writes telemetry tables inside a read query.
Compile-time diagnostics never claim a runtime outcome.

Ordinary production query execution in this repository does not expose exact
branch/fallback execution. Exact outcomes are available through GraphBench or
canary `EXPLAIN (ANALYZE)` sampling. If rollout requires actual fleet selection
rates, host-application telemetry is an explicitly owned dependency with its
own privacy/performance review; absent that dependency, rollout monitoring is
limited to compile-time planned rates plus canary plan sampling.

Aggregate production telemetry, when added through the host application, must
avoid endpoint IDs, properties, query text, credentials, or result data. It
should count selected/fallback reasons, state-limit events, and coarse latency
and resource classes sufficient for rollback decisions.

## Rollout and rollback

### Rollout

Use narrow release-candidate activations in the L7 order. For each activation:

- freeze its structural/resource envelope;
- compare with its immediate production predecessor;
- retain exact fallback tests;
- monitor compile-time selection rates and canary actual fallback outcomes;
  require the host-telemetry dependency above before claiming fleet runtime
  rates;
- start with the narrowest observation mode and direction; and
- expand only after new confirmation of the added envelope.

No candidate remains indefinitely behind a dormant public feature flag.
Public/runtime force overrides are removed after qualification; the
deterministic build-tagged/test-tool regression seam may remain.

### Rollback

Rollback is a forward source change that selects the previous qualified
executor/lowering. The generic translator remains the semantic fallback. Any
schema helper has a versioned compensating migration; driver-only increments
have independent source rollback boundaries.

Never rewrite repository history, discard unrelated user work, or use
`git revert` as the agent workflow.

## Risk register

| Risk | Mitigation |
|---|---|
| Bundled live candidate creates false causal attribution | one behavior group per predecessor/candidate binary and micro mechanism evidence |
| A1a duplicate arm appears as architecture evidence | fingerprint identity gate and explicit A/A alias |
| A1b/A2 final trail rescan rejects the wrong concept | rebuild with cheap graph-scoped ID existence; preserve historical rejection wording |
| MAT-M1 wins because MAT-M0 pays unused node-ID state | whole-architecture SP-S3-U-E/M0 versus SP-S3-U-NE/M1 comparison |
| A3 is activated from one sparse point | retain A4/A0/A2 portfolio and discovery-independent crossover holdouts |
| Reverse fan-in or suffix density explodes A3 | bounded probes/static envelope and exact forward fallback |
| Viability deduplicates real trails | use viability only as permissive filter; exact forward enumeration restores results |
| Suffix/root/path multiplicity is lost | exact bags, restricted seed/filter deduplication, multiset/path validation |
| Planning erases A3 execution win | separate planning/execution; stable SQL and auto/custom/generic tournament |
| AST cache is mistaken for server-plan cache | boundary-specific metrics and explicit documentation |
| Generic plan loses graph pruning | representative partition tests and reject total-E2E regression |
| Dynamic parameter values change SQL | stable fingerprint assertions across values |
| Lowering telemetry reports incumbent as experimental application | target-indexed decision consumption and reconciled planned/applied/skipped counts |
| Runtime overflow leaks partial rows | mutually exclusive same-statement branches and threshold tests |
| Fallback doubles work beyond timeout | measure probe+discard+fallback as one operation and restrict envelope |
| Scalar continuation matches dangling endpoints | preserve graph-scoped ID-only node-existence join |
| Typed codec changes raw caller contract | explicit compatibility decision and fallback/layout validation |
| In-place result value reuse aliases rows | ownership/lifetime/cancellation/session-reuse tests |
| Parse cache retains query text/literals for driver lifetime | explicit privacy/lifecycle decision, bounded entries, class bypass if required, eviction/teardown release, no query-text telemetry |
| SP-S3-U trail state spills at high depth/fanout | distance-only first, explicit state bytes, D32/D64/F512/F1000 envelope |
| Existing workspace tuning distracts from stronger shortest architecture | keep proven workspace improvement as generic fallback/control |
| Selector overfits discovery fixtures | frozen unseen holdouts and simultaneous regret gate |
| New helper/index harms operations or writes | conditional ADR, independent read/write evidence, migration/rollback |
| Concurrent candidates multiply per-session memory | half/full/twice-pool memory and pool-wait gates |
| Cancellation leaves transaction/session state | cancel each stage and execute exact query on same physical session |
| PostgreSQL plan drift breaks brittle tests | assert semantic plan/state/pruning invariants, not complete text |
| Neo4j ratio becomes a shipment target | exact/shape oracle only; predecessor/reference PostgreSQL gates decide |

## Durable artifact layout

Publish a versioned bundle similar to:

```text
artifacts/perf/production-lift-<series>/
  manifest.json
  comparison-boundary.json
  source.patch
  source-untracked-manifest.json
  checksums.sha256
  bin/
    predecessor-graphbench
    candidate-graphbench
  baselines/
    horizontal/
    shortest/
    adcs/
  corpus/
    declaration.json
    fixtures.json
    checksums.sha256
  architecture/
    identities.json
    sql-fingerprints.json
    closure.json
  discovery/
    shortest/
    materializer/
    adcs/
    planning/
  confirmation/
    horizontal/
    shortest-distance/
    shortest-path/
    adcs-endpoint/
    adcs-path/
    selector/
  plans/
  state-counters/
  references/
  aa/
  concurrency/
  cancellation/
  soak/
  plan-corpus/
  gate.json
  report.md
```

Record source, binary, SQL, schema, fixture, settings, plan, raw samples,
warmup/sample counts, arm/order, physical connection, exact observations,
statistics, decisions, and checksums. Redact connection credentials and never
publish private data.

## Pull-request and experiment sequence

Keep changes reviewable and independently attributable. The intended sequence
is:

1. Publish the entering worktree/evidence manifest.
2. Add architecture/fingerprint identity validation and relabel historical
   aliases.
3. Fix planned/applied/skipped decision accounting with zero incumbent SQL
   change.
4. Add the full orthogonal ADCS/shortest fixture and holdout declaration.
5. Isolate and confirm H-ROWS.
6. Isolate and confirm H-AST.
7. Isolate and confirm H-CODEC.
8. Isolate and confirm H-NODE-ID.
9. Implement real ADCS-A1a and corrected A1b/A2 references.
10. Build the production ADCS-A0 parity attribution ladder.
11. Repair SP-S3-U-E/SP-S3-U-NE and MAT-M0/M1 factorial references.
12. Complete the SP-S3-U-D reference tournament and alternative closure.
13. Add and confirm the forced SP-S3-U-D production AST builder.
14. Select and add the forced shortest path state/materializer builder.
15. Run ADCS-A3/A4 regime and planner-policy tournaments.
16. Add forced ADCS-A3/A4 AST builders for raw-qualified candidates.
17. Cross selected ADCS search with MAT-M0/M1 for full paths.
18. Prove static selection, then runtime selector/fallback only if triggered.
19. Run full semantic, plan, corpus, concurrency, cancellation, and soak
   qualification.
20. Activate observation boundaries one at a time with fresh confirmation.
21. Publish the clean PostgreSQL/Neo4j live rerun and residual report.
22. Open or close conditional L5 work from quantified residuals.

Tests and documentation accompany the behavior they cover. Do not postpone
them into one final cleanup change.

## Immediate next actions

Execute in this order:

1. Copy and checksum the current horizontal, shortest, and ADCS evidence into a
   durable L0 bundle.
2. Add the architecture identity/fingerprint contract so A1a-like aliases
   cannot recur silently.
3. Correct lowering diagnostics while proving incumbent SQL unchanged.
4. Implement genuine ADCS-A1a and corrected A1b/A2 benchmark references.
5. Implement the edge-only SP-S3-U-E search reference and inbound MAT-M0/M1
   exact cases.
6. Add ADCS-A3/A4+MAT-M0/M1 compound reference arms.
7. Freeze discovery and unseen holdout fixture checksums, including zero-result
   and high reverse fan-in.
8. Produce isolated predecessor/candidate binaries for H-ROWS, H-AST,
   H-CODEC, and H-NODE-ID.
9. Begin L2F ADCS forward-parity and L2S shortest-distance work in parallel.
10. Run the auto/custom/generic planner-policy matrix before any A3 frontier,
    index, or helper work.
11. Keep every production selector on the incumbent until its forced emitter,
    resource envelope, and fallback gates pass.
12. Reprofile after each accepted layer and update the residual ranking by
    absolute cost and workload frequency.

Do not begin with unconditional ADCS-A3, a universal MAT-M1 choice, further
workspace tuning as the primary shortest strategy, `work_mem`, JIT, a new edge
index, translation caching, MAT-M2, a typed helper, or native code.

## Definition of done

This continuation is complete when:

- the full upstream-main-to-accepted-worktree boundary and every causal
  predecessor/candidate are durable and reconstructible;
- horizontal parse, codec, result, and scalar increments are independently
  accepted or rejected with exact rollback boundaries;
- cache bounds, codec/raw compatibility, row ownership, and scalar orphan
  semantics are explicit and tested;
- benchmark architecture IDs, state shapes, observation boundaries, and SQL
  fingerprints are truthful;
- ADCS-A1a is no longer a disguised A/A arm;
- corrected A1b/A2 evidence distinguishes concepts from the rejected correlated
  revalidation implementation;
- production forward ADCS SQL closes ADCS-A0-SQL or the remaining exact gap is
  quantified and dispositioned;
- SP-S3-U-D is exact, bounded, trail-free, reference-closed, and either
  accepted or rejected from fresh production confirmation;
- true SP-S1/SP-S2 alternatives are measured or closed by the declared rule;
- edge-only SP-S3-U-E+MAT-M0 is fairly compared with
  SP-S3-U-NE+MAT-M1;
- the selected shortest path stack is exact and non-dominated across its
  declared direction/resource envelope;
- ADCS-A3 and A4 are evaluated across sparse, dense, zero-result,
  disconnected-boundary, reverse-fan-in, multiplicity, depth, and payload
  regimes;
- current A3 gate failures remain visible and no threshold was weakened after
  observing them;
- PostgreSQL planning is attributed under auto/custom/generic modes and no
  unsafe global setting is required;
- ADCS endpoint and full-path search/materializer choices are independently
  qualified;
- planned, selected, applied, skipped, runtime selected, and runtime fallback
  diagnostics match emitted SQL and actual branch loops per target;
- bounded selectors pass unseen-holdout regret, overhead, threshold, overflow,
  same-snapshot fallback, timeout, and resource gates;
- missing roots/endpoints execute no candidate work and partial overflow rows
  never escape;
- exact multiset, multiplicity, ordered path, uniqueness, graph scope, null,
  error, optional, correlated, multipart, and mutation semantics pass;
- PostgreSQL and Neo4j complete integration suites, translation/template/
  mutation coverage, race, plan, cancellation, rollback, and session-reuse
  tests pass;
- D32/D64, F512/F1000, dense-disconnected, payload, cold/warm, concurrency,
  memory, spill, and soak envelopes pass;
- each accepted search/materialization SQL candidate closes the best correct
  PostgreSQL reference at an identical raw boundary and materially improves
  its immediate CySQL predecessor E2E;
- each accepted horizontal increment clears its isolated mechanism and
  immediate-predecessor gates without requiring an inapplicable SQL reference;
- no selected normal-tier portable candidate creates temp/local workspace or
  read-only WAL;
- public/runtime force overrides and dormant feature flags are removed while a
  deterministic build-tagged/test-tool regression seam may remain;
- generic incumbent paths remain tested semantic fallbacks;
- accepted behavior can be rolled back through forward source changes and any
  helper has a compensating migration;
- a clean PostgreSQL/Neo4j live rerun and complete performance/plan corpus are
  published with raw samples and checksums;
- rejected prototypes are absent from production code and retained as durable
  evidence; and
- every remaining optimization is ranked by addressable cost and workload
  frequency, then triggered, explicitly closed, or opened as a new bounded
  continuation.

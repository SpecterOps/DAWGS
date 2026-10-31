# Remaining SP and traversal outlier delivery

This delivery turns the remaining PostgreSQL-versus-Neo4j outlier plan into
independently reversible slices. A code-complete candidate is not a promoted
candidate: an eligible, non-terminal candidate still requires clean-source
training and holdout evidence plus every schema-v2 manifest role. A terminally
rejected generation cannot be revived by recapture or rebinding.

## Delivered sequence

1. Backend-delta reports now aggregate matched successful rounds, retain SQL
   and runtime identities, and rank only repeated PostgreSQL regressions.
2. The driver/runtime seam can stage `orientation-probe-v2` only when the
   manifest candidate and selector are both v2, preserving its diagnostic and
   guarded statement tooling. Final authorization terminally rejects that
   immutable generation because its training overhead gate failed; further work
   requires a new policy generation. The same verifier rejects v1 because its
   legacy evidence schema cannot bind source, corpus, and frozen-cohort
   identity. The v1 and v2 formulas remain immutable and independently
   reversible for diagnostics.
3. Syntax-open singleton shortest paths use the repository's effective depth
   15 in contained S3/S4 selection. Diagnostics distinguish `policy_default`
   from `explicit` depth through `maximum_depth_source` and selector
   `sp-static-v7-contained`.
4. `SP-I2-C-D` addresses hidden fan-in with reverse-physical ID-only distance
   discovery. Total state and each breadth-first level are capped before any
   candidate row is visible; overflow invokes exact S4 in the same statement.
   Runtime markers, plan-replay counters, production manifests, stable-snapshot
   enforcement, query-SHA buckets, and `DisableInlineSPDistance` are wired.
5. Existing ASP-I1 qualification and promotion tooling remains a separate
   later evidence lane; it is not a substitute for the next clean SP-I2
   discovery decision. A1 stays automatic because the current focused evidence
   has known shallow regressions and does not authorize broad I1 selection.
6. B2 remains conditional. Its kernels and tournament arms stay available,
   but no production policy accepts B2 until a frozen cohort proves a stable
   win over both incumbent and B1 with resource and closure gates passing.

## SP-I2 diagnostic checkpoint

The 2026-08-13 two-round diagnostic run for
`GSPV2-STRESS-hidden-fanin-distance` exposed and corrected a fail-closed
admission defect: the SQL formatter emitted `GROUP BY` but omitted the
frontier sentinel's `HAVING count(*) > cap`, so every non-empty candidate
incorrectly selected S4 fallback. Formatter and translation regressions now
require the `HAVING` predicate.

After correction, both rounds matched Neo4j observations, executed
`SP-I2-C-D` with 17 state rows, and recorded zero fallback loops. PostgreSQL
median latency was 0.330-0.357 ms versus Neo4j's 2.191-3.528 ms, or a
6.14-10.68x descriptive advantage. This validates the candidate's intended
hidden-fan-in search shape, but it is not promotion evidence by itself: the
full training/holdout, cap-overflow, resource, closure, and operational matrix
below remains mandatory.

A subsequent two-round expansion covered shallow hidden fan-in, the depth-16
stress case, and disconnected exhaustion. All six matched backend comparisons
were semantically equal. PostgreSQL was 2.62-4.77x faster on the shallow case,
6.32-10.53x faster on stress, and 1.89-2.60x faster on the disconnected
control. Candidate receipts reported four or 17 states for reachable searches
and the expected no-path branch for exhaustion, with no fallback loops.

Formal qualification is now implemented as the independent
`sp-i2-distance-v1` staged protocol. It seals six training declarations and
four unopened holdouts behind exact tags/case identities, declaration and
resolved-selection digests, clean source/archive/binary identity, and
recomputed training evidence before any holdout database setup. Discovery
requires 5-20 alternating `SP-S4-C-D`/`SP-I2-C-D` rounds with at least five
warmups and ten samples per arm; confirmation requires 10-20 rounds with at
least 20 warmups and 50 samples. Every normal case must preserve exact scalar
observations and timed runtime receipts, execute the guarded distance branch
without fallback, pass the state/frontier resource contract, meet median ratio
upper `<= 0.95` or saving lower `>= 100us`, and keep p95 ratio upper `<= 1.05`.
The preregistered cycle control instead uses the bounded-overhead gate: median
ratio upper `<= 1.10` or absolute overhead upper `<= 100us`, with the same p95
limit.
The preregistered production-form state and frontier limits are both 100,000.
They are immutable protocol inputs, not qualified caps; qualification requires
the passing clean-source discovery freeze that does not yet exist.
The complete commands and artifact sequence are documented in GraphBench's
`Frozen SP-I2 distance qualification` section. No protected timing is
authorized until a clean committed tree produces a passing discovery freeze.

The implementation workflow was then exercised with the minimum five
alternating training rounds, six cases, and 50 timed samples per arm/case.
That live rehearsal caught two capture-recipe defects before they could enter
promotion evidence: `block` must equal `round`, and even rounds must physically
execute I2 before S4 rather than merely swapping the arm-order labels. The
documented capture loop and a chronology regression test now enforce both
rules. A fresh 60-record capture passed corpus, schedule, runtime-receipt, and
resource validation and then stopped at the intended clean-source/archive
barrier before either a report or freeze was written. No holdout was selected
or timed.

The first dirty-tree rehearsal exposed a cycle-control regression: after
reaching the requested root at depth one, the recursive candidate expanded
back out of that completed target until the maximum depth. Target-terminal
pruning now prevents those strictly longer descendants while retaining all
unrelated branches, cap admission, and same-statement exact fallback. The
cycle plan consequently fell from 65 recursive states to two in every
diagnostic replay.

A fresh five-round, order-balanced training-only capture on 2026-08-14 retained
zero fallback across all six cases. Pooled candidate median ratios were
`0.033-0.759` for the five target cases. The cycle control measured `1.015`
with about `4.7us` overhead, and its pooled p95 ratio was `0.894`; these point
estimates are inside the preregistered control bounds. The clean-source check
still stopped report and freeze creation, as required. These dirty-tree
results solve the observed control mechanism but do not authorize holdout
access or production activation; an authoritative confidence-bound decision
requires a clean committed recapture.

The production-manifest seam was also exercised independently. An inbound,
typed, distance-only disconnected case executed the no-path candidate branch
with nine states and no fallback; PostgreSQL's 0.260 ms median compared with
Neo4j's 1.184 ms. A deliberately reduced state/frontier cap of ten admitted
exactly the cap+1 sentinel, exposed zero candidate rows, and recorded the full
`SP-I2-C-D -> SP-S4-C-D -> SP-S3-U-E+MAT-M0` exact fallback receipt chain.

## Required evidence order

For SP-I2, ASP-I1, or a newly preregistered candidate generation, capture in
this order: order-balanced A/A, training discovery and freeze, unopened holdout
confirmation, matched performance, resource gate, reference closure, and
operational cancellation/concurrency/session-reuse evidence. This also records
the historical orientation-v2 protocol order, but that frozen generation is
terminally rejected and must not be recaptured, retuned, or promoted. Bind each
report to one promotion identity, then verify the final manifest before
installing a driver policy. Any missing runtime receipt, inactive-arm proof,
cap counter, or exact query bucket fails closed.

Promotion binding embeds the exact native producer bytes for A/A, resource, and
reference-closure reports. SP confirmation names the exact native resource
digest; confirmation, performance, and resource bind the same candidate
artifact. For every promotion case, resource evidence must contain exactly the
performance round count, and its flattened candidate receipt-chain set must
equal performance's complete set. Reference closure deliberately uses its own
raw-pgx/comparator capture, while matching candidate/source/binary/corpus
identity, exact query/dataset/name/split cohort, thresholds, and independently
valid production receipt chains. Each reference workload must also match exactly
one native PostgreSQL A/A case by dataset, name, and workload digest; its
independent invocation IDs are not equated with the performance/resource set.
Confirmation and performance do not yet embed raw benchmark samples, so a
future producer schema is required for independent bootstrap replay.

Formal operational capture is two-pass. A non-promotional preflight may omit
`operational_candidate_sql_sha256` only long enough to derive the exact SQL
digest emitted by the provisional production policy. Freeze that digest in the
manifest, discard the preflight records, then recapture formal evidence. The
operational requirements and every non-overflow record must equal the manifest
anchor; the runner, final verifier, and driver reject a populated anchor that
is not canonical, and the runner additionally rejects a canonical anchor that
does not match generated SQL. An anchored schema-v2 manifest therefore admits
exactly one unique query digest; cohort variation remains in bound parameters
and fixtures.

The operational command validates an already assembled 32-record source
document; no standalone producer currently generates it. Release engineering
must preserve the complete native GraphBench worker/iteration, cancellation,
snapshot, isolation, overflow, optimization, plan-replay, and fixture evidence
when assembling `OperationalGateInput`. A summarized or hand-authored pass claim
cannot satisfy the validator.

## Rollback boundaries

- orientation: `DisableExpansionOrientation` or a zero policy;
- endpoint-seeded reverse: `DisableEndpointSeededReverse`;
- canonical witness: `DisableInlineSPWitness`;
- guarded distance: `DisableInlineSPDistance`;
- ASP-I1: `DisableInlineASPDAG`.

If a manifest-backed candidate carries a rollback switch, it may carry exactly
one and it must be dedicated to that candidate: orientation with
`DisableExpansionOrientation`, ASP-I1 with
`DisableInlineASPDAG`, canonical SP-I1 witness with `DisableInlineSPWitness`,
or SP-I2 distance with `DisableInlineSPDistance`. An unrelated or second switch
is rejected. `DisableEndpointSeededReverse` is standalone-only. Every standalone
rollback policy must have no manifest candidate and must leave its manifest
digest, manifest JSON, and query allowlist empty
(`promotion_manifest_sha256`, `promotion_manifest_json`, and
`query_sha256_allowlist`).

Changing a policy generation changes the translation-cache identity
immediately. For a manifest-backed emergency rollback, install the disable
switch under a new nonzero generation. The effective rollback copy clears the
candidate SQL-anchor comparison so incumbent SQL can execute, while the stored
manifest and its candidate anchor remain immutable. A zero policy returns every
query directly to its incumbent identity. B1/B2 have no production activation
boundary and therefore need no production rollback switch yet.

## Fixed-suffix checkpoint

Earlier orientation-v2 training diagnostics show that its topology choice is
mostly correct, but the guarded statement adds a roughly fixed 187-376
microseconds of probe and dispatch work. That overhead dominates the shallow
training cases, so v2 is terminally rejected; its immutable formula must not be
retuned or recaptured in place.

A fresh exact-reverse comparison isolates the next boundary. On
`GFSE-V2-D16-F1000-R1-X1-M1-sparse_endpoint_ids`, PostgreSQL returned the same
stable observations as Neo4j in 0.274 ms versus 1.401 ms, a 5.11x advantage.
The complete-path form was also semantically equal, but took 3.290 ms versus
Neo4j's 1.616 ms. PostgreSQL plan execution rose from 0.236 ms for endpoint IDs
to 2.975 ms for paths while search state stayed at 19 rows. The remaining
sparse-path deficit is therefore path hydration, not reverse discovery.

The V2 sparse-path fixture now declares both deterministic path rows instead
of row count alone, making future backend reports fail closed on real path
semantic differences. The next implementation slice should preserve the
proven reverse ID-only arm and batch or specialize directed path hydration;
only after that should a new selector generation attempt to amortize or avoid
v2's fixed guarded-probe cost.

That hydration slice is now implemented. A component run measured generic
`ordered_edge_ids_to_path` hydration at 1.979 ms median and 350 shared-buffer
hits for one long path, while direct hydration from precomputed ordered node
and edge IDs measured 0.143 ms and 21 hits. Reverse traversal carries ordered
node IDs only when a complete path is observed and hydrates both arrays in the
translated statement. On the sparse path case, exact reverse fell from 3.290
ms to 0.278 ms median versus Neo4j at 1.117 ms, with the exact two-path oracle
matching. Endpoint-only output retains the narrower reverse state.

The guarded orientation-v2 candidate uses the same ordered-ID hydration while
the exact forward fallback retains the established generic materializer. Its
initial end-to-end medians were 0.930 ms for endpoint IDs and 1.021 ms for full
paths, versus Neo4j at 1.177 ms and 0.988 ms respectively. Plan decomposition
then showed that the degree probes materialized one boolean tuple per adjacency
and the metrics CTE rescanned those tuples. The probes now aggregate the same
cap+1-limited streams to scalar counts; the selector formula, overflow boundary,
duplicate-root contribution, and exact fallback behavior are unchanged.

In the subsequent 30-sample diagnostic, guarded PostgreSQL medians fell to
0.468 ms for endpoint IDs and 0.671 ms for full paths, versus Neo4j at 1.00 ms
and 1.0 ms. PostgreSQL plan execution for the path case fell from 0.926 ms to
0.635 ms with the same 275 inclusive shared-buffer hits. Telemetry still
reported 1,000 forward and one reverse degree samples, selected exact reverse,
and recorded no overflow or fallback. Targeted live integration also retained
the probe-overflow and reverse-state-overflow fail-closed receipts. These are
diagnostic results from a dirty development tree, not qualification evidence;
they cannot revive orientation-v2. The final verifier terminally rejects this
generation, and its protected holdouts remain unopened.

## Orientation-v2 qualification checkpoint

The scalar-probe implementation was then exercised through the exact discovery
protocol: five position-balanced rounds, ten measured samples per arm and
round, eight canonical training cases, separate `shadow`, `incumbent`,
`reverse`, and `guarded` artifacts, and a matching two-arm A/A report. All 160
four-arm records and 80 A/A records were successful, used one binary identity,
matched exact observations, and carried the required Repeatable Read receipts.
The dirty-tree discovery report was emitted, while freeze creation correctly
failed closed because the implementation is not committed.

The performance decision is nevertheless conclusive before a clean recapture:
all eight training cases failed the immutable selected-arm overhead gate.
Guarded median overhead over the selected exact arm ranged from 156 to 396
microseconds in the scalar-probe capture, above both the 1.10 ratio and 100
microsecond absolute limits. A follow-up prototype removed a redundant reverse
gate and state-probe lateral boundary, but a fresh five-round capture still
reported 205-377 microseconds of selected-arm overhead across the cohort. The
prototype was reverted because the matched evidence did not confirm a useful
improvement and the original boundary gives stronger inactive-arm proof.

Orientation v2 is therefore rejected as a production candidate on its frozen
cohort, not merely blocked on source cleanliness. Its formula, thresholds, and
holdouts must remain unchanged, and the final manifest verifier enforces that
terminal decision. Further fixed-suffix work requires a new policy generation
that avoids paying topology probes plus dual-arm dispatch on every invocation;
the existing exact reverse executor and scalar-probe evidence remain valid
components for that study.

## Static suffix-reverse guard implementation

That independent generation is now implemented as the default-off,
tool-only `suffix-reverse-guard-v1` policy. It deliberately removes every
orientation-v2 topology and degree probe. Static enrollment is limited to
complete-path fixed-suffix queries; a 512-row suffix cap and independent
512-row reverse-state cap select either the existing suffix-seeded reverse
executor or the unchanged stepwise-forward statement. Both arms are
marker-gated in one Repeatable Read statement, and the runtime receipt records
the precise reverse, suffix-overflow, or state-overflow branch.

The diagnostic surface has a separate `suffix_guard` counter family, stable
named CTE attribution, cap+1 observations, complementary marker rows, and
direct executor-loop proof. A real PostgreSQL training-case plan measured
three suffix rows, seven reverse-state rows, three output rows, one candidate
marker/executor loop, and zero fallback marker/executor loops. This verifies
the production-shaped plan boundary, not performance qualification.

The predeclared feasibility gate is intentionally early and training-only:
the two already-open V3 training path cases are bound by an exact schema-v2
selection declaration; each uses all six doubled-Williams orders exactly once,
five warmups, and exactly ten samples per arm for exact forward, exact reverse,
and guarded execution, plus matching order-balanced A/A evidence. It rejects
substitute training workloads, diagnostic or protected holdout timing, and
requires invocation-bound timed receipts plus a separate complete plan replay
that proves the inactive executor performed zero work. Guard overhead must be
within `1.10` or `100us` of exact reverse;
regret must be within `1.10` or the A/A floor of the fastest exact arm; and the
guard must materially improve forward p50 (`<=0.95` ratio or `>=100us`
saving) with p95 `<=1.05`. Only a passing stop gate warrants a new sealed
qualification corpus and production-manifest generation. Until then there is
no suffix-guard driver policy, rollback switch, or automatic selection.

The schedule is physical evidence, not an arm-label convention. Every capture
uses `block == round`; the exact two-case records for each arm/round must share
one nonzero GraphBench process interval. Within a round, those intervals must
be non-overlapping and follow the declared arm positions, and each new round
must start at or after the prior round completes. The gate also requires one
run UUID across all six rounds. Its chronology tamper tests reject relabeled
execution, overlapping arms or rounds, mixed cohort invocations, and missing
timestamps.

The matching incumbent A/A evidence uses the same fail-closed rule. Its two
processes must physically alternate first position across contiguous rounds,
use `block == round`, share one A/A run UUID, and execute the exact cohort
without arm or round overlap. A/A report schema v4 records artifact-bound
`physical_chronology` provenance, and the suffix gate refuses earlier reports
that lack it. Consequently the original label-balanced `aa.json` cannot be
reused for a compliant recapture.

The first six-round live training capture could not establish that stop decision.
Both cases matched exact observations, supplied 60 timed samples per arm, and
executed the reverse branch without overflow or fallback, but the recorded
process intervals reveal incumbent, reverse, then guarded execution in every
round. The labels alone claimed the six Williams orders. The hardened gate now
rejects round 2 for contradictory arm chronology before reading its timing into
a decision.

The legacy `.coverage/suffix-reverse-guard-v1/feasibility.json` remains useful
only as a dirty-tree diagnostic: its estimated guard/reverse ratios (`1.692`
and `1.511`) and overhead intervals are not valid preregistered feasibility
evidence.

A chronology-valid recapture then reached the stop decision without changing
the cohort, sampling, caps, or thresholds. Both cases again matched exact
observations, supplied 60 timed samples per arm, executed reverse without
overflow or fallback, and materially improved exact forward. Estimated
guard/reverse median ratios were `1.346` and `1.206`; their one-sided upper
bounds were `1.593` and `2.002`, and absolute-overhead upper bounds were
`201us` and `445us`. Both failed the primary `1.10`/`100us` overhead gate,
while regret and forward-improvement gates passed. The authoritative report is
`.coverage/suffix-reverse-guard-v1-chronology/feasibility.json` with
`passed=false`.

This valid failure does not authorize holdout, manifest, driver-policy, or
automatic-selector work. `suffix-reverse-guard-v1` is terminally stopped; its
thresholds must not be weakened or retuned. The exact reverse executor and its
ordered-ID hydration remain reusable components for a newly preregistered
architecture that removes same-statement guarded-dispatch cost.

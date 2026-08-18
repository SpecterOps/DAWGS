# All-shortest P4 open baseline v1

Status: A1 diagnostic prerequisite implemented; fresh clean-source recapture
pending. This is a training-only baseline and telemetry inventory, not an ASP
candidate comparison, power study, qualification, or selector change.

## Why this is a fresh baseline

The retained clean P0 corpus has one all-shortest PostgreSQL/Neo4j ratio, for
the protected shallow diamond. Its 1.70x descriptive ratio neither establishes
the 4.8-5.4x P4 opportunity nor may it select a target. Earlier ASP-I1 archive
captures also mixed protected holdouts, so they are historical diagnostics and
cannot serve as P4 evidence.

This generation therefore measures only the nine declared
`generated_shortest_paths_v2` all-shortest training cases. They cover shallow
outbound, early target, inbound hidden-fan-in, cycle/dead-tail, reconvergent
multi-path, and disconnected behavior. All generated holdout, diagnostic, and
stress declarations remain unopened.

## Frozen schedule

After P3 commit `7f5d0f9dcc7bd1a86dd2846e7180e06b7795c13c`, capture from the
clean commit that contains this roster. Capture provenance records that exact
source commit and binary digest. Run four
independent rounds with one warm-up and five timed samples per case/backend.
Each round runs the PostgreSQL `ASP-A1-DAG` incumbent under Repeatable Read,
pool size one, and diagnostic traversal telemetry, alongside the ordinary
Neo4j reference. GraphBench alternates requested backend order on even rounds.
The capture consequently contains 72 exact case/backend/round records and
360 timed samples. PostgreSQL exact path-multiset, all-shortest, hydration,
workspace, plan-resource, runtime identity, and fallback evidence must all be
present; Neo4j records must be exact and present.

The machine-readable roster is
[`benchmark/testdata/scale/protocols/asp_p4_open_baseline_v1.json`](../../benchmark/testdata/scale/protocols/asp_p4_open_baseline_v1.json).
The forced A1 identity is an attribution control: `asp-static-v1` already
selects A1 for these shapes, and this run does not exercise an automatic
candidate policy.

## Stop and next gate

Any non-exact record, missing telemetry, fallback, incomplete counters, or
unexpected PostgreSQL runtime identity stops the baseline and permits no
candidate timing. A completed baseline only authorizes a separately frozen
A1-versus-one-candidate P4 preflight if an open training case is materially
behind Neo4j. It does not authorize ASP-I1/B1/B2 timing, a power study,
protected cases, a manifest, or automatic selection.

## V1 stop result

The first clean combined-backend round ran from
`3a74d14be83f2c99b1694109d54840501ebbc3f5` with GraphBench binary SHA-256
`4e47611c0e06d508e81011cc35d348a167c4c9a1d863095e3b72add821780c91`.
Its artifact `.coverage/p4-open-baseline-3a74d14/round-1.jsonl` hashes to
`b6c09fbd9dd1e46bd262af224469c12a9a69367f5ad7e504e853da5247fb53f6`.
All 18 records (nine PostgreSQL and nine Neo4j) had exact public observations;
PostgreSQL reported `ASP-A1-DAG`, the selected branch, and no fallback.

The diagnostic replay failed the frozen telemetry condition for every A1
record. The outer `Function Scan` exposes neither invocation-local search nor
predecessor, enumeration, hydration, and workspace counters, and therefore
reported `hidden_counters_unavailable`. The remaining three rounds were not
run. The first round is retained only as a stop artifact and cannot select a
P4 target. [`docs/experiments/asp_a1_diagnostic_prerequisite_v1.md`](asp_a1_diagnostic_prerequisite_v1.md)
defines the distinct prerequisite required before a clean recapture.

## Clean-recapture boundary

The separate A1 invocation-local diagnostic is now implemented and validated
for shallow, recursive, reconvergent, inbound, no-path, session-isolation, and
cancellation/rollback paths. The next capture must use a clean commit that
contains that diagnostic and must restart at round one. It may not merge,
pool, or compare the stopped V1 round with the new capture.

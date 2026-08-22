# All-shortest P4 open baseline v1

Status: completed training-only baseline; one open target selected for a
separately frozen candidate preflight. This is not an ASP candidate comparison,
power study, qualification, or selector change.

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
cancellation/rollback paths. The clean capture below used a commit that
contains that diagnostic and restarted at round one. It does not merge, pool,
or compare the stopped V1 round with the new capture.

## Clean recapture result

The fresh capture ran from `bf055b3aaeda1f887e652a399b065290db560236` with
GraphBench SHA-256
`8a8cdd0998ef9391776ee4a6de3689f31f0a2133675bc96d9cffd95d6caceb31`.
The appended four-round artifact
`.coverage/p4-open-baseline-bf055b3/round-1.jsonl` hashes to
`69c29c0a79e2ca566bbd54e533c896138a7770b245d030dc347ebc9413e6b6fe`.
It contains 72 exact records (36 PostgreSQL and 36 Neo4j), 360 timed samples,
and 72 excluded warm-up samples. The source diff hash is empty in every record.

Every PostgreSQL record has runtime/applied identity `ASP-A1-DAG`, no fallback,
and complete all-shortest, hydration, and `spd_*` workspace receipts. Every
Neo4j record is exact and present. The capture host reported the `powersave`
CPU governor, so the deltas below are target-selection diagnostics only—not
performance qualification evidence.

| Open case | Matched median PostgreSQL/Neo4j | Matched p95 PostgreSQL/Neo4j | Branch |
| --- | ---: | ---: | --- |
| `GSPV2-TRAINING-disconnected-all-shortest-max64` | 5.825× | 5.263× | `search_no_path` |
| `GSPV2-NORMAL-outbound-all-shortest-depth3` | 3.801× | 2.942× | `single_ended_search` |
| `GSPV2-TRAINING-early-depth3-all-shortest-max16` | 3.076× | 3.766× | `single_ended_search` |
| `GSPV2-TRAINING-inbound-early-depth3-all-shortest-max64` | 2.852× | 2.703× | `single_ended_search` |
| shallow/reconvergent controls | 1.125–1.206× | 1.018–1.242× | preflight |

The cycle/dead-tail case is exact on each backend but its serialized public
observations do not match across backends, so it is visible in the artifact but
excluded from the matched backend-delta ranking. The descriptive delta report
is `.coverage/p4-open-baseline-bf055b3/backend-delta.json` (SHA-256
`e127d5d09bf68bd809c41f1c849044f2d61f5dc6c0d363c6a8d33eab28765fa0`).

## Selected target and next gate

`GSPV2-TRAINING-disconnected-all-shortest-max64` is the sole selected open
target: all four matched median ratios exceed 5.22×, its four-round median is
5.825×, and its p95 ratio is 5.263×. Its A1 receipt consistently reports the
`search_no_path` branch with 32 candidate edges, seen peak 33, predecessor peak
32, zero output paths, and 229,376 bytes of `spd_*` workspace.

This selection authorizes only a new, immutable A1-versus-one-candidate P4
telemetry preflight roster. It does not authorize candidate timing beyond that
separately frozen preflight, a power study, holdouts, a selector change, or
reuse of the stopped V1 round.

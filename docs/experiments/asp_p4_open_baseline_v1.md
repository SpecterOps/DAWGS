# All-shortest P4 open baseline v1

Status: scheduled. This is a training-only baseline and telemetry inventory,
not an ASP candidate comparison, power study, qualification, or selector
change.

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

# All-shortest P4 I1 disconnected preflight v1

Status: frozen pending clean PostgreSQL capture. This is a small,
training-only telemetry preflight, not a power study, performance
qualification, holdout opening, selector change, or promotion decision.

## Scope and hypothesis

The clean P4 A1 baseline selected
`GSPV2-TRAINING-disconnected-all-shortest-max64`: its four matched
PostgreSQL/Neo4j median ratios were 7.041×, 5.825×, 10.560×, and 5.228×, and
the invocation-local A1 receipt consistently used `search_no_path`. The case
has no public paths, but A1 still records 32 candidate edges, a seen peak of
33, and a predecessor peak of 32.

`ASP-I1-U-DAG+MAT-M0` is the only candidate in this roster. It is a distinct,
default-off inline predecessor-DAG executor with a typed `inline_no_path`
receipt, complete guarded-branch evidence, and exact A1 fallback. A second
bidirectional search is not a justified first response to the selected
single-ended no-path target. This preflight tests whether I1 can execute its
own inline no-path path exactly and observably before any broader candidate
work is considered.

## Frozen roster and schedule

The target is `GSPV2-TRAINING-disconnected-all-shortest-max64`. The adverse
controls are `GSPV2-TRAINING-early-depth1-all-shortest-max16`,
`GSPV2-TRAINING-early-depth2-all-shortest-max64`, and
`GSPV2-TRAINING-reconvergent-all-shortest-max16`. They keep shallow early
returns and relationship-distinct multipath behavior in the preflight rather
than allowing a no-path result to obscure a shallow regression.

Capture only PostgreSQL, with pool size one, Repeatable Read, GraphBench
diagnostic traversal telemetry, one warm-up, and five timed samples. Every
case receives forced `ASP-A1-DAG` and forced `ASP-I1-U-DAG+MAT-M0` in four
carryover-balanced orders: A1/I1, I1/A1, A1/I1, I1/A1. The capture therefore
contains 32 exact case/arm/round records, 160 timed samples, and 32 excluded
warm-up samples. No cap override, reference arm, concurrency exercise, or
other candidate is permitted.

The complete machine-readable contract is
[`benchmark/testdata/scale/protocols/asp_p4_i1_disconnected_preflight_v1.json`](../../benchmark/testdata/scale/protocols/asp_p4_i1_disconnected_preflight_v1.json).
The GraphBench binary must be built from this clean roster commit; results are
not pooled with the P4 A1/Neo4j baseline or any historical I1 archive.

## Acceptance and stop gate

Every record must preserve the exact public all-shortest path multiset and
complete hydration/workspace evidence. A1 records must identify `ASP-A1-DAG`
and supply complete invocation-local A1 telemetry. I1 records must identify
`ASP-I1-U-DAG+MAT-M0`, expose complete `asp-i1-guarded-v1` typed telemetry,
and execute no fallback. The disconnected target must report `inline_no_path`;
each control must report `inline_predecessor_dag`.

Any inexact result, missing/hidden/contradictory counter, wrong identity or
branch, fallback, undeclared cap behavior, or adverse-control failure stops
the preflight. A complete capture still authorizes neither formal power,
additional candidate timing, protected cases, nor a selector/manifest change:
a separately frozen next-stage decision is required.

# All-shortest P4 I1 disconnected preflight v1

Status: current I1 arm terminally rejected. The capture was a small,
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

## Capture result and stop

The capture ran from clean source
`307e62f4d0e102384752e031f9c2850d6a73dbfe` with GraphBench SHA-256
`ce101efdc55d173176aaa221b3ca3a18b4d40e3fb3a970fa346fdc98c125557c`.
The ignored artifact directory `.coverage/p4-i1-disconnected-307e62f`
contains the separate arm streams `a1.jsonl` (SHA-256
`03114fd4b2cfdc50959697eebbfe08e4932ab307c38845b88415d8a287a3778f`)
and `i1.jsonl` (SHA-256
`f147df180b8b351bf14ed83977105beaf48b96a822a143a33229afbb979083b6`).
Its capture ledger hashes to
`9baa5f3d25f47d5efd1a496ac266fee013edd419d9a696eb3f953e65c40a29f5`.

It contains the frozen 32 records, 160 timed samples, and 32 excluded warm-up
samples. All source-diff hashes are empty. Every record is exact, and A1/I1
public path multisets match for every case/round pair. A1 has complete
invocation-local telemetry. Every I1 record has complete `asp-i1-guarded-v1`
telemetry, the expected I1 runtime identity, and zero fallback; the target
uses `inline_no_path` and all controls use `inline_predecessor_dag`.

The first I1 command tried to append its rows to the existing A1 JSONL.
GraphBench correctly rejected the different arm at serialization, so those
rows were not accepted as an artifact. The identical frozen I1 round was
immediately rerun into its own stream before validation; the rejected output
is excluded from all counts and comparisons. A later typo in the disposable
target allowlist was rejected before database work. The host reported a
`powersave` governor, so this remains diagnostic-only evidence.

| Case | I1/A1 pooled median | I1/A1 pooled p95 | Result |
| --- | ---: | ---: | --- |
| `GSPV2-TRAINING-disconnected-all-shortest-max64` | 0.192× | 0.246× | target improves |
| `GSPV2-TRAINING-early-depth1-all-shortest-max16` | 1.365× | 1.516× | fails control |
| `GSPV2-TRAINING-early-depth2-all-shortest-max64` | 1.410× | 1.070× | fails control |
| `GSPV2-TRAINING-reconvergent-all-shortest-max16` | 1.200× | 0.896× | fails control |

The no-path target win cannot average away three adverse-control median
regressions. Under P4's frozen stop gate, `ASP-I1-U-DAG+MAT-M0` is terminally
rejected for this generation before any power study, broader I1 timing,
holdout, selector, or manifest work. Reopening P4 requires a distinct executor
and a separately frozen clean-source roster; this artifact cannot be retuned,
pooled, or promoted.

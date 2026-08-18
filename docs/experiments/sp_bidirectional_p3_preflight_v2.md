# Compact bidirectional shortest-path P3 preflight V2

Status: current B1/B2 function-workspace arms terminally rejected. This was a
telemetry and component-boundary readiness preflight, not a performance
qualification. It supersedes the incomplete V1 schedule and does not authorize
a selector, formal performance tournament, protected holdout, or production
activation.

## V2 correction and fixed scope

V1's primary S3/S4/B1/B2 roster is retained only as a diagnostic reference;
its direct-floor lane named a nonexistent `SP-S4-C-DIRECT` identity and never
completed. V2 is a distinct generation and requires a full clean recapture.
Its only schedule change is to run the direct floor separately by observation:
distance compares `SP-S4-C-D` with `SP-S0-DIRECT`, and one-path compares
`SP-S4-C-WE+MAT-M0` with `SP-S0-DIRECT`.

The baseline is the same clean P0 source `57be1681140a2642639df0c06f7167bc17203e9b`
with retained round hashes `5cd14dc4b13008f5e307d44a16c56ff608eb79596b2ecaddf59d1eb70c31c6a1`
and `3bb71d1951b66559677abd4bba5441d844567269e6c1b1694cc95b67b4bc1f4d`.
Its descriptive target selection remains unchanged: inbound depth-8 one-path
at 3.52x PostgreSQL/Neo4j, inbound depth-8 distance at 2.63x, and long
outbound one-path at 1.69x.

## Frozen roster and schedule

The primary targets are `GSP-D08-F001_distance_inbound`,
`GSP-D08-F001_path_inbound`, and `GSP-D64-F1000_path`. The controls are
`GSP-D16-F016_distance`, `GSP-D16-F016_path`,
`GSP-D04-F128_disconnected`, `GSP-D04-F128_path_disconnected`,
`GSP-D02-F016_distance_cycle`, `GSP-D02-F016_path_cycle`,
`GSP-D02-F016_distance_self_loop`, `GSP-D02-F016_path_self_loop`,
`GSP-D01-F016_distance_parallel`, `GSP-D01-F016_path_parallel`,
`shortest_distance_bound_pair`, and `one_shortest_path_bound_pair`.
The old protected V2, SP-I1, and SP-I2 declarations remain excluded.

Every primary observation receives S4, S3, B1, and B2 with pool size one,
Repeatable Read, diagnostic telemetry, one warm-up, and five timed samples.
The four carryover-balanced orders are `S4,B1,S3,B2`, `B1,B2,S4,S3`,
`B2,S3,B1,S4`, and `S3,S4,B2,B1`.

The depth-one distance case `GSP-D01-F001_distance` and one-path case
`GSP-D01-F001_path` each receive two separate counterbalanced comparisons:
`S4,S0` followed by `S0,S4`. They must use the observation-specific S4 identity
above; no synthetic direct S4 identity exists.

## Stop gate

Every B1/B2 record must have an exact observation, correct runtime identity and
scheduler, one invocation-local search call, complete per-level and aggregate
search counters, measured workspace high water, and attributed plan
buffers/temp/WAL. One-path records additionally require complete hydration and
decode attribution. Any fallback, hidden counter, missing component boundary,
or unexplained inactive work fails closed. The full machine-readable contract
is `benchmark/testdata/scale/protocols/sp_bidirectional_p3_preflight_v2.json`.

After—and only after—a complete V2 capture, a new separately named power study
may be calibrated. No V2 result can itself authorize formal timing, a holdout,
or a production selector.

## Clean V2 result

The complete V2 capture ran from clean source
`d77409674d6da4b00c3e379356955a5678dccbae` with GraphBench binary SHA-256
`e08dd6d7f95b83421d91e3af19e7462b35d56b3a704b26d11d2e200d44d57ae4`.
The ignored artifact directory `.coverage/p3-preflight-d774096` contains 32
primary and eight direct-floor JSONL artifacts: 248 exact records and 1,240
timed observations. The sorted full capture ledger hashes to
`edd64b293ea0bb8a19fac41ee8e58d7042a511090b8043d5f85736fa4a2b567a`.

All 120 B1/B2 case-round records were exact, had matching candidate runtime
identity, emitted no fallback, and supplied complete invocation-local search,
workspace, hydration, and plan-resource telemetry. The preflight therefore
resolved the earlier validator defects rather than hiding them. It was still
diagnostic-only—its host recorded a `powersave` CPU governor—so it does not
constitute a powered qualification result.

That limitation cannot rescue either existing B arm: pooling the 20 warm samples
per target/arm gives the following B-to-S4 ratios.

| Target | B1 median / p95 | B2 median / p95 |
| --- | ---: | ---: |
| `GSP-D08-F001_distance_inbound` | 4.59x / 3.90x | 3.27x / 2.79x |
| `GSP-D08-F001_path_inbound` | 4.65x / 3.92x | 3.74x / 3.18x |
| `GSP-D64-F1000_path` | 8.13x / 8.98x | 4.19x / 4.25x |

B2 is the faster compact arm but fails the incumbent on every frozen target by
more than threefold. The direct floor is observation-sensitive as expected:
S0 reduces the distance median from 536us to 371us, but raises the one-path
median from 724us to 1,145us. It cannot justify a broad direct policy.

This stops the existing B1/B2 stored-function/workspace identities before any
component implementation, power simulation, formal performance tournament,
holdout, or selector work. Keep S4 (and the markedly faster S3 references) on
the tested shapes. A future P3 successor requires a distinct executor,
workspace boundary, roster, telemetry contract, and prospective power study;
these V2 observations cannot be repurposed as its qualification evidence.

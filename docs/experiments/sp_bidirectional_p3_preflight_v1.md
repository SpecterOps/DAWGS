# Compact bidirectional shortest-path P3 preflight V1

Status: superseded before direct-floor capture. This is a telemetry and
component-boundary readiness preflight, not a performance qualification. It
does not authorize a selector, a formal tournament, a protected holdout, or
production activation.

## Disposition

The primary S3/S4/B1/B2 captures were diagnostic-only and cannot qualify this
generation. The declared direct-floor comparison used the nonexistent
`SP-S4-C-DIRECT` identity, so the frozen schedule could not complete. V1 is
superseded rather than amended. Its raw observations may explain the correction
but must not support a P3 decision. V2 preserves the primary roster and uses
the real `SP-S4-C-D` and `SP-S4-C-WE+MAT-M0` arms in distinct direct-floor
distance and one-path comparisons.

## Purpose and separation

P2 is terminal: the `sp-i2-distance-v1`, `sp-i2-distance-v2`, and
`sp-i2-distance-v3-power-study` identities must not be retuned or reused.
P3 evaluates only the existing, tool-forceable compact bidirectional SP
references, with a distinct identity `sp-bidirectional-p3-preflight-v1`.
The candidates remain default-off and reference-only:

| Arm | Distance identity | One-path identity |
| --- | --- | --- |
| Incumbent | `SP-S4-C-D` | `SP-S4-C-WE+MAT-M0` |
| Single-ended reference | `SP-S3-U-D` | `SP-S3-U-E+MAT-M0` |
| Strict alternating node | `SP-B1-C-ALT-NODE-D` | `SP-B1-C-ALT-NODE-WE+MAT-M0` |
| Smaller current level | `SP-B2-C-MIN-LEVEL-D` | `SP-B2-C-MIN-LEVEL-WE+MAT-M0` |

`SP-S0-DIRECT` is only a direct one-hop floor; it is not part of the primary
unbounded four-arm comparison.

## Clean baseline and frozen opportunity selection

The only selection input is the clean two-round P0 capture from source
`57be1681140a2642639df0c06f7167bc17203e9b`. Its retained artifacts are
`.coverage/p0-clean-57be168-round1.jsonl`
(`5cd14dc4b13008f5e307d44a16c56ff608eb79596b2ecaddf59d1eb70c31c6a1`) and
`.coverage/p0-clean-57be168-round2.jsonl`
(`3bb71d1951b66559677abd4bba5441d844567269e6c1b1694cc95b67b4bc1f4d`).
Each used one PostgreSQL and one Neo4j pool session, one warm-up, and three
timed observations per case per round. The small sample is descriptive only;
it is deliberately insufficient for a P3 performance conclusion.

Pooling the two round medians identified the following open targets:

| Case | PostgreSQL median | Neo4j median | PostgreSQL / Neo4j |
| --- | ---: | ---: | ---: |
| `GSP-D08-F001_path_inbound` | 4.219ms | 1.200ms | 3.52x |
| `GSP-D08-F001_distance_inbound` | 3.874ms | 1.472ms | 2.63x |
| `GSP-D64-F1000_path` | 1.939ms | 1.148ms | 1.69x |

The depth-8 inbound pair is the only material open P3 target. The long
outbound path remains a declared weaker target to prevent an inbound-only
claim. All other selected cases are controls: the same P0 capture already
shows PostgreSQL at or ahead of Neo4j for many of them, so a compact workspace
cannot claim success by moving broad costs into those shapes.

The frozen primary roster is:

- Targets: `GSP-D08-F001_distance_inbound`,
  `GSP-D08-F001_path_inbound`, and `GSP-D64-F1000_path`.
- Typed controls: `GSP-D16-F016_distance`, `GSP-D16-F016_path`,
  `GSP-D04-F128_disconnected`, `GSP-D04-F128_path_disconnected`,
  `GSP-D02-F016_distance_cycle`, `GSP-D02-F016_path_cycle`,
  `GSP-D02-F016_distance_self_loop`, `GSP-D02-F016_path_self_loop`,
  `GSP-D01-F016_distance_parallel`, and `GSP-D01-F016_path_parallel`.
- Untyped controls: `shortest_distance_bound_pair` and
  `one_shortest_path_bound_pair`.
- Separate direct-floor probes: `GSP-D01-F001_distance` and
  `GSP-D01-F001_path`.

The roster covers typed and untyped, one and multiple relationship kinds,
inbound and outbound expansion, path and distance observation, shallow and
deep depth bounds, cycle/self-loop, parallel-kind, disconnected, and direct
floor behavior. It deliberately excludes every `generated_shortest_paths_v2`,
SP-I1, and SP-I2 declaration because their existing training/holdout partitions
do not belong to P3.

## Frozen capture schedule

Every primary case receives the four corresponding observation arms in four
rounds, using a one-session PostgreSQL pool, Repeatable Read, diagnostic
telemetry, one warm-up, and five timed samples. The arm order is the balanced
four-arm carryover sequence:

| Round | Order |
| --- | --- |
| 1 | `S4`, `B1`, `S3`, `B2` |
| 2 | `B1`, `B2`, `S4`, `S3` |
| 3 | `B2`, `S3`, `B1`, `S4` |
| 4 | `S3`, `S4`, `B2`, `B1` |

The direct-floor probes use two counterbalanced S4/S0 comparisons:
`S4,S0` then `S0,S4`. An arm is one separately invoked GraphBench command and
must set matching `round`, `block`, `arm`, and `arm-order` fields. Cap
overrides, reference mode, concurrency measurements, P2 generation options,
and protected corpus tags are forbidden.

The complete machine-readable contract is
`benchmark/testdata/scale/protocols/sp_bidirectional_p3_preflight_v1.json`.
Changing the roster, arms, order, warm-up count, timed count, or component
requirements creates a different preflight generation.

## Telemetry and component stop gate

P3’s first gate is observability, not speed. For every B1/B2 replay, the
invocation-local diagnostic must prove exactly one search call; its scheduler;
the selected runtime branch; per-level side, action, depth, frontier, seen,
queue, predecessor, and meeting counts; aggregate peaks; frozen distance;
witness rows; and workspace high-water bytes. PostgreSQL plan evidence must
attribute shared/local/temp buffers, temporary files and bytes, and WAL records
and bytes.

For a one-path result, witness recovery, hydration, and decoding must also be
complete and separately attributable. A nested exact S4 fallback currently
marks its hidden traversal work unavailable, and a missing hydration counter
does the same. Those conditions are intentional fail-closed outcomes: the
affected B1/B2 record cannot qualify and must not be compared as a faster
candidate. The preflight also rejects a missing runtime receipt, scheduler
mismatch, non-exact public result, absent workspace measurement, or hidden
inactive-arm work.

The required component boundaries are workspace reset, temporary-table access,
search, witness recovery, hydration, and result decoding. Existing GraphBench
boundary timings and invocation-local diagnostic replay identify the aggregate
work, but they do not yet separately attribute all six boundaries. Therefore
the preflight cannot become a formal performance tournament without a dedicated
component-boundary implementation and its tests.

## Next authorization

No P3 performance threshold or sample count is implied by this preflight. Once
all records are exact and complete, the resulting open trace may calibrate a
new, separately named power simulation. Only a passing simulation may freeze a
formal target/control performance schedule, including its sample counts,
confidence intervals, arm-order strata, median/p95/resource gates, and
component boundaries. A simulation pass still does not authorize a protected
holdout or a production selector.

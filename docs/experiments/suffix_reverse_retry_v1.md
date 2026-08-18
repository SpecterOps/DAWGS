# Suffix reverse transaction retry v1

Date: 2026-08-17

Status: open training roster frozen; no production selector or protected holdout is authorized

## Purpose

`suffix-reverse-retry-v1` tests whether the already-correct suffix-seeded
reverse component retains its sparse-topology advantage when the successful
path contains neither topology probes nor an inactive forward body. It is a
new generation and does not reuse the evidence identity of
`orientation-probe-v1`, `orientation-probe-v2`, or
`suffix-reverse-guard-v1`.

## Frozen development identity

- policy: `suffix-reverse-retry-v1`;
- candidate executor: `EXPANSION-SUFFIX-SEEDED-REVERSE`;
- incumbent and retry executor: `EXPANSION-STEPWISE-FORWARD`;
- execution boundary: `transaction_retry`;
- suffix rows: 512 complete rows, with a cap+1 sentinel;
- reverse states: 512 complete rows, with a cap+1 sentinel;
- buffered output rows: 4,096, with a cap+1 sentinel in candidate SQL;
- buffered encoded output: 16 MiB;
- isolation: PostgreSQL Repeatable Read;
- public observation: complete hydrated paths only;
- selection: tool-only exact query/corpus selection; no production policy.

Changing an identity, cap, observation, or execution boundary creates a new
development generation. Command-line cap overrides are diagnostic and cannot
qualify this frozen identity.

## P0 descriptive entry snapshot

On 2026-08-18, two independently reloaded broad GraphBench rounds ran the
complete scale corpus with a fresh binary, `-pool-size 1`, and PostgreSQL
diagnostic telemetry. Both rounds had exact observations for every recorded
row: 173 PostgreSQL and 174 Neo4j records per round. This is descriptive
opportunity accounting only because the source worktree was dirty; it cannot
freeze a corpus, authorize a holdout, or satisfy this generation's stop gate.

The combined two-round per-case medians confirm the P1 premise. The worst
single loss was hidden-fan-in distance at `63.92x` PostgreSQL/Neo4j, but sparse
fixed-suffix forms occupied the next four positions at `44.17x`, `41.86x`,
`36.43x`, and `31.80x`. Across all 35 generated fixed-suffix cases, the
geometric-mean ratio was `2.74x`; their long-pole concentration makes this the
largest multi-case opportunity. The P1-admissible path targets
`GFSE-D16-F1000-sparse_path` and
`GFSE-V2-D16-F1000-R1-X1-M1-sparse_path` shared incumbent SQL fingerprint
`dc8aab1f84de2cae582bc9252d4b7996653113ed1df785abcd8e4e17b4c32961`.
Their structured plans are retained in the raw captures.

The ignored raw captures are
`.coverage/p0-20260818-round1.jsonl`
(`248282767bdd041f4b48d4d8c850727b5f27357589ada7790216a8c84832acee`)
and `.coverage/p0-20260818-round2.jsonl`
(`348ae3757f09bd6339a2eef6fcbc073b1621cc22ecc882d986dde60055d5345c`).
Before any qualifying P1 timing, repeat P0 from a clean committed source and
freeze the open training selection.

## Clean P0 baseline

On 2026-08-18, P0 was repeated from committed source
`57be1681140a2642639df0c06f7167bc17203e9b` with GraphBench binary SHA-256
`5f9c5c3b7dcfbb7ffd69554b04b75b899cc6a6f1772e1e952d90a1abd0814c8c`.
Each of two independently reloaded rounds used pool size one, one warm-up, and
three timed iterations in both PostgreSQL and Neo4j modes. Every record was
exact: 176 PostgreSQL and 176 Neo4j records in each round. The raw captures
are `.coverage/p0-clean-57be168-round1.jsonl`
(`5cd14dc4b13008f5e307d44a16c56ff608eb79596b2ecaddf59d1eb70c31c6a1`)
and `.coverage/p0-clean-57be168-round2.jsonl`
(`3bb71d1951b66559677abd4bba5441d844567269e6c1b1694cc95b67b4bc1f4d`).

The protected hidden-fan-in stress case remains the largest cross-backend
loss, at about `82.21x` PostgreSQL/Neo4j by the two-round case-median ratio.
Among open P1 targets, `GFSE-D16-F1000-sparse_path` and
`GFSE-V2-D16-F1000-R1-X1-M1-sparse_path` remain the leading sparse full-path
opportunities, at about `50.03x` and `39.02x`, respectively. This baseline
authorizes P1 open timing only; it does not authorize a protected holdout.

## Open transaction smoke

The initial open transaction smoke ran on 2026-08-18 against the P0 sparse
path target `GFSE-V2-D16-F1000-R1-X1-M1-sparse_path`. It returned the exact
two paths on every observation. With frozen default caps, all three timed warm
samples completed on `EXPANSION-SUFFIX-SEEDED-REVERSE` with
`reverse_complete` receipts (median `6.79ms`). This is directionally faster
than the P0 incumbent descriptive median (`57.52ms`), but it is not a paired
tournament and does not evaluate the stop gate.

A separate state-limit-one diagnostic forced transaction-local retry. Every
timed sample preserved exact rows and reported the ordered receipt chain
`forward_retry_state_overflow` on the reverse executor followed by
`exact_forward_retry_complete` on `EXPANSION-STEPWISE-FORWARD`; its median was
`62.68ms`. This validates the savepoint rollback, deferred completion receipt,
and exact-incumbent fallback boundary, but cap overrides and dirty source make
it non-qualifying.

The ignored smoke artifacts are
`.coverage/p1-retry-smoke-20260818.jsonl`
(`9dce1d49f8a8c08e14ddf20d693f61fecb108ceaf99f9a7fd94315cc7c13d5bb`)
and `.coverage/p1-retry-forced-state-20260818.jsonl`
(`cbc9ac50d72688a5d706a4b971a9888b1e1169eaf57f61f7136ad66fc5e117cf`).

One-iteration comparator smokes on the same target also verified the three
execution surfaces: ordinary exact forward (`58.73ms`), forced exact reverse
(`5.82ms`), and retry (`6.79ms`). They establish neither an overhead bound nor
a performance result. The forward and reverse artifacts are
`.coverage/p1-comparator-forward-smoke-20260818.jsonl`
(`9b1d989b13d6bfdb0f0acf8685fb09cefae325a38dacbf3ced5525c824da2d69`)
and `.coverage/p1-comparator-reverse-smoke-20260818.jsonl`
(`0c17a895ef93496830cfc6ef80d5b2e329a1f284284e071b2e85d8920580f7c9`).

## Frozen open training roster

The following exact non-holdout full-path declarations are frozen for P1 open
development. Each is captured independently because retry translation admits
one target per invocation:

| Role | Cases |
| --- | --- |
| Sparse targets | `GFSE-V2-D16-F1000-R1-X1-M1-sparse_path`; `GFSE-D16-F1000-sparse_path` |
| Shallow and zero-depth controls | `GFSE-D00-F001-none_path`; `GFSE-D01-F010-sparse_path` |
| Suffix density/payload controls | `GFSE-D04-F010-half_payload_path`; `GFSE-D08-F001-all_path` |
| Root multiplicity, cycle, and self-loop control | `GFSE-V3-TRAIN-Q4-C1-S1-productive_cycle_self_loop_path` |
| Multi-root/disconnected control | `GFSE-V3-TRAIN-D05-F008-R4-X3-I0-M1-Q3-path` |
| Candidate suffix-cap retry | `GFSE-BOUNDARY-S513-productive-path` |
| P1-only reverse-fan-in | `GFSE-P1-TRAIN-D09-F017-R0-X2-I1024-M1-Q1-high_reverse_fanin_path` |
| P1-only no-path exhaustion | `GFSE-P1-TRAIN-D09-F513-R0-X512-no_path_exhaustion` |
| P1-only output-byte retry | `GFSE-P1-TRAIN-D00-F001-R0-X0-M4-P2100000-output_byte_retry_path` |

The three P1-only declarations use fresh V2 fixture identities and the
`suffix-reverse-retry-v1-training` tag; they do not alter the frozen V3
orientation cohorts or reuse a legacy holdout. The byte case returns four
complete paths whose hydrated root and heads carry 2,100,000-byte payloads,
intentionally exceeding the frozen 16 MiB candidate buffer while remaining an
exact forward-retry control. It is PostgreSQL-only in GraphBench because it
tests that driver's retry buffer; Neo4j does not contribute a comparable
execution boundary and did not complete this hydrated 25 MiB observation
within a 90-second diagnostic deadline. Do not reuse a legacy holdout as a
convenience control. Adding, deleting, or replacing any roster identity
creates a new P1 generation.

One-iteration PostgreSQL smokes on 2026-08-18 confirmed exact observations for
all three controls. The high-fan-in path naturally produced
`forward_retry_state_overflow` followed by `exact_forward_retry_complete`; the
no-path exhaustion control completed reverse-only; and the payload control
naturally produced `forward_retry_output_bytes` followed by
`exact_forward_retry_complete`. These dirty-source artifacts are diagnostic
only: `.coverage/p1-high-fanin-fixture-smoke-20260818.jsonl`
(`eb47d849fc165c924000169dbdf1b5c75c8eb9d73f7f798f2956afc2deed0cb1`),
`.coverage/p1-no-path-fixture-smoke-20260818.jsonl`
(`b6af2bdf940fc9881082791ab8ae913548e16677ce3f6436d6f9d17418d3cc40`),
and `.coverage/p1-output-byte-fixture-smoke-20260818.jsonl`
(`c33fef4ffecb8d30d06906e3a0c3850531112428ac2362ea604b9acd386cba2a`).

Before this freeze, one individual PostgreSQL retry capture per roster member
returned its declared exact result. Nine members, including all sparse,
shallow, density, and V3 controls, completed reverse-only. The three required
retry controls emitted the expected ordered chains: suffix overflow for
`GFSE-BOUNDARY-S513-productive-path`, state overflow for the P1 high-fan-in
case, and output-byte overflow for the P1 payload case, each followed by
`exact_forward_retry_complete`.

## Execution contract

1. Start an explicit Repeatable Read transaction without initializing unrelated
   shortest-path workspaces.
2. Establish a savepoint and execute the reverse-only statement. The statement
   computes bounded suffix and reverse-state probes, records one transaction-
   local status, and contains no forward CTE or forward executor.
3. Drain and buffer candidate rows. No row is exposed until the SQL status,
   row cap, byte cap, and result decoding are complete.
4. On `reverse_complete`, release the savepoint and publish the buffer.
5. On a declared suffix, state, output-row, output-byte, or encoding overflow,
   discard the complete candidate buffer, roll back to the savepoint, release
   it, and execute the ordinary exact forward translation in the same
   transaction.
6. Unknown or missing status, candidate error, cancellation, receipt failure,
   or savepoint failure returns an error. It never becomes a performance
   fallback.

Timed invocation receipts distinguish the candidate observation from the
actual forward retry. `exact_forward_retry_complete` is recorded only after
the fallback result drains and validates without error; an error or early close
does not create that completion receipt. The fallback remains the ordinary
independently translated incumbent; the candidate statement cannot initialize
it.

## Open development command

Use only open training/control cases:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql \
  -postgres-expansion-suffix-reverse-retry \
  -postgres-repeatable-read \
  -postgres-traversal-telemetry diagnostic \
  -pool-size 1 \
  -cases <one-p1-admissible-full-path-case> \
  -jsonl-output <artifact>
```

The run must use pool size one. Reference and concurrency side measurements
are separate development captures until the retry-aware versions of those
measurement paths are implemented.

The current retry tool deliberately translates exactly one statically eligible
full-path target per invocation. The retired `orientation-v2-training` cohort
contains multiple endpoint-only and path-observed declarations, so it is not a
valid P1 selector. Build the fresh P1 training/control roster from explicit
single-case captures (or add a dedicated, frozen P1 cohort) before beginning a
multi-case tournament.

## Frozen P1 capture schedule

Each frozen roster member receives three separately invoked PostgreSQL arms in
each of six rounds: ordinary exact forward (`F`), forced exact suffix-seeded
reverse (`R`), and transaction retry (`T`). The round orders are the six
permutations `FRT`, `FTR`, `RFT`, `RTF`, `TFR`, and `TRF`, in that order. Each
arm uses the committed binary, pool size one, Repeatable Read, diagnostic
telemetry, one warm-up, and five timed iterations. No cap override, reference,
or concurrency option is permitted.

An arm is one exact case invocation and writes its own JSONL artifact. Its
GraphBench `round`, `block`, `arm`, and `arm-order` fields must match this
schedule. A retry arm must retain exact rows and timed receipt chains; a
reverse-only arm must never contain a forward retry receipt. The schedule is
prospective: changing cases, binary, arm definitions, counts, warm-ups, or
orders creates a new generation. Only after all open captures are exact and
the early-stop gate passes may a separately authorized holdout step begin.

## Early stop gate

The generation stops before a protected holdout unless every open case has
exact observations and complete receipts and satisfies all of:

- successful retry fast-path overhead versus exact reverse is at most `1.10`
  or `100us` at the median, with p95 ratio upper at most `1.05`;
- each target improves over exact forward with median-ratio upper at most
  `0.95` or saving lower at least `100us`, with p95 upper at most `1.05`;
- controls and real retries stay within `1.10` or `100us` at the median and
  p95 upper at most `1.05`;
- `reverse_complete` performs zero forward work;
- retry exposes zero candidate rows;
- memory, temporary bytes, WAL, cancellation, session reuse, and receipt
  attribution pass their declared bounds.

Only a clean-source open result that passes this gate may create a separately
committed formal corpus, prospective power study, and untouched holdout. A
failure is terminal for this identity.

## Hidden-fan-in sequencing

SP-I2 V1 and V2 remain terminal. A hidden-fan-in successor receives a new
executor, selector, corpus, rollback, and evidence identity only after this P1
generation reaches a passing or terminal disposition. Its first artifact is a
prospectively frozen power study based on archived open V1/V2 traces; candidate
timing and implementation changes are forbidden until that study passes.

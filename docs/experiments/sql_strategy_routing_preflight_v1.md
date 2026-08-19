# SQL strategy-routing preflight v1

Status: frozen preimplementation feasibility. This is a default-off,
PostgreSQL/driver-only experiment. It is not a retry of
`suffix-reverse-retry-v1`, `suffix-reverse-guard-v1`, or either terminal
orientation-probe identity; it cannot change production routing, cached
translations, schema, or query results.

## Basis

The clean two-round P0 recapture at `a4b29f2` recorded 704 successful backend
records. The largest comparable repeated loss was the full-path sparse suffix
case `GFSE-V2-D16-F1000-R1-X1-M1-sparse_path`: PostgreSQL was 91.06 times the
Neo4j median. Its endpoint-only companion was 34.22 times the Neo4j median.
Both execute `EXPANSION-STEPWISE-FORWARD` because the production policy is
`fixed-suffix-static-v1` with `compile_time_fallback`.

Earlier component evidence establishes that exact suffix-seeded reverse search
and ordered-ID hydration can be fast. It also establishes two hard boundaries:
the same-statement guard paid fixed probe/dispatch cost, and the transaction
retry generation added at least 2.02 ms to its exact-reverse fast path. Neither
execution boundary may be reused here.

## Question

Can a separately selected, single exact SQL arm retain the direct reverse
component's benefit without a same-statement probe, an inactive forward body,
or per-query transaction setup?

The preflight evaluates this question only in an explicit diagnostic mode. A
future automatic policy may be considered only after this preflight passes and
after it separately proves decision-cache staleness, mutation, snapshot, and
rollback behavior.

## Frozen scope

The preflight uses the reusable exact executor
`EXPANSION-SUFFIX-SEEDED-REVERSE` with ordered node/edge-ID hydration. The
incumbent is `EXPANSION-STEPWISE-FORWARD`. The temporary diagnostic label is
`suffix-route-component-v1`; it is not a production policy identity.

The fresh, open training roster is fixed in the accompanying protocol and uses
11 new v3 fixture identities (never a relabelled P0/P1/orientation fixture):

- sparse endpoint-ID and complete-path fixed suffixes;
- high reverse fan-in, dense suffix, no-path, cap-boundary, cycle, self-loop,
  relationship-distinct, and multi-path controls;
- no generated shortest-path, all-shortest, endpoint-seeded, P1 terminal, or
  previously protected declaration is eligible for candidate timing.

The two targets are `GFSE-SRC-V1-TARGET-D16-F1024-sparse_endpoint_ids` and
`GFSE-SRC-V1-TARGET-D17-F1025-sparse_path`. The nine controls cover high
reverse fan-in, dense suffixes, no-path exhaustion, 511/512/513 disconnected
suffix rows, a productive cycle, a productive self-loop, and relationship-
distinct multiple suffix paths. Each declaration carries an exact ID-row or
complete path oracle. The P0 case names remain discovery inputs only and may
not be relabelled as training or holdout evidence.

## Four-round component comparison

The comparison is PostgreSQL-only and remains non-promotional. In each round,
capture the complete `suffix-route-component-v1` roster twice: once as the
ordinary `EXPANSION-STEPWISE-FORWARD` incumbent and once with
`-postgres-expansion-suffix-route-component`. Use a fresh JSONL artifact for
each arm/round, one warm-up, five timed iterations, pool size one, caller-owned
Repeatable Read, diagnostic telemetry, and one shared nonempty run UUID. The
four counterbalanced orders are incumbent/component, component/incumbent,
incumbent/component, and component/incumbent. This yields 55 timed samples per
arm/round and 440 timed samples over the full comparison.

For example, the component half of round one is:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql -tags suffix-route-component-v1 \
  -warmup-iterations 1 -iterations 5 -pool-size 1 \
  -round 1 -block 1 -run-uuid "$RUN_UUID" -arm reverse_component -arm-order 2 \
  -require-clean-source \
  -postgres-repeatable-read -postgres-traversal-telemetry diagnostic \
  -postgres-expansion-suffix-route-component \
  -jsonl-output .coverage/sql-routing-preflight-v1/round-1-reverse-component.jsonl
```

The incumbent command uses the same flags and roster, omits the component
flag, and sets `-arm incumbent -arm-order 1`. Subsequent rounds rotate the
declared order. Do not append a second attempt to an artifact: an incomplete or
non-exact arm stops this generation.

## Clean-source recapture requirements

Every arm of a replacement capture must use `-require-clean-source`. GraphBench
checks the tracked diff and untracked source fingerprint before target
validation, fixture loading, or acquisition of the destructive-run lock. The
binary must therefore be built from a clean committed tree, with capture output
kept in an ignored directory or outside the repository.

Each component record must report diagnostic counter status `complete` with
exact suffix, boundary, reverse-state, one-row receipt, ordered node/edge
hydration loop and row counters, exact public output rows, and the untimed
PostgreSQL planning/execution timings. A missing or ambiguous named CTE,
ordered-hydration alias, receipt, or timing fails closed.

Before starting the four arms, run the PostgreSQL manual operational test for
`TestPostgreSQLSuffixRouteComponentCancellationReusesPoolSession`. It proves a
statement-timeout cancellation (`57014`), rollback, release/reacquisition of
the size-one pool's same backend PID, and a cardinality-preserving direct
component replay. Store its test output beside the four-round artifact; it is
operational evidence only and must not be mixed into timed samples.

## First four-round capture (diagnostic only)

The first capture completed on 2026-08-19 with 88 successful records and 440
timed samples: five samples for each of 11 cases in both arms across four
counterbalanced rounds. All public row/path observations matched their
declared oracle. Each of the 220 component samples emitted one
`suffix_route_component` receipt with
`EXPANSION-SUFFIX-SEEDED-REVERSE`, no fallback, and no active
`EXPANSION-STEPWISE-FORWARD` SQL body. The ordinary arm remained the
compile-time-forward incumbent.

The two targets were materially faster as direct components: endpoint IDs had
a four-round median ratio of `0.087x` (about `42.05ms` saved) and the complete
path target had a ratio of `0.050x` (about `74.73ms` saved), both component /
incumbent. The high-fan-in, dense, and 511/512/513 suffix controls were slower
under reverse (`1.33-1.84x`); the no-path and relationship-distinct controls
improved. These are descriptive component results, not a selection rule.

The raw artifacts are ignored under `.coverage/sql-routing-preflight-v1`.
Their ordered SHA-256 ledger is:

```text
0bdb199f72751f5a5586f99c599a9fa0c81059da0c244e1f097caa01cc9aa55f  round-1-reverse-component.jsonl
6ff9ac81a525f9fcf83891f9d28a50be8424c43b3ef1ede4b3cb58a00f1b329c  round-1-incumbent.jsonl
7cc6b084593273ada82245e117597119259774a996f08928ab3c6119bb6b4229  round-2-reverse-component.jsonl
ae943d9616a70e68446f8c29039f150f2371a64f0b8e6b67404e16796ab7d736  round-2-incumbent.jsonl
4051d3908348b069bd246fc656c2d7271d3b8f6f3f61f8c9cdc5e8045e0ecc6d  round-3-incumbent.jsonl
eceb58b830c4951001185750facfba45e8489dd4f154435c35231ef4b7381368  round-3-reverse-component.jsonl
4eeb99c02bfd3d9dbde2392eed6e925b98386098d25edcd8791cd802aa6ad5fb  round-4-incumbent.jsonl
b9531a518ac4369248fda3ec813a65208375c2828cb312b07e00f2cd9a3d74e1  round-4-reverse-component.jsonl
```

This capture has source commit `a4b29f22b81c2191316b54b8383283fc40a1900d`,
dirty diff `ad94dc9497eff73211eef6b6cb519e41286a7bde105745348fc1c0ef6448010e`,
binary `4cdb5d00a5aa8fdb5f4ea8d93537a443312d520cc2864adf50f5534f91af588e`,
and corpus `6255a9495172e0749f5e330b4648631d8d8a8b10cbdfeb88a4f7f2eed5157d60`.
Diagnostic telemetry is present but reports `plan_derived_partial`, so it does
not yet provide all preregistered component counters. The dirty source and
partial telemetry prevent cache work, automatic routing, protected access, or
promotion. A clean-source recapture with complete component telemetry and the
remaining cancellation/pool-reuse evidence is required before the separate
cache-feasibility decision.

## Clean-source four-round recapture

The replacement capture completed on 2026-08-19 from committed source
`aaecb745c328128115273b4da7fa71a8de3351b7`, with the clean-tree SHA-256
(`e3b0...b855`), binary SHA-256
`4c65ff1f7d642a47bcc4e96b9aee19f57fe8a30c9fe2218cd91514a0bdc71860`,
and corpus SHA-256
`2aa00d2df9a32e7fbca6e9682058ba30e82a4b2968f87b123c7fa161924cac18`.
All 88 records were exact and successful; all 44 component records carried
complete suffix, boundary, reverse-state, receipt, ordered-hydration, and
planning/execution telemetry. The 220 timed component samples each had one
direct `suffix_route_component` receipt with no fallback or forward SQL body.

The replacement ledger is:

```text
2fcd84240a3466facff45e8074b7630491a639d86bd7e532dd42ffd45271389a  round-1-incumbent.jsonl
92bee28dc8363ee3165bf4cf3ba9863e1b5254204f8273a15b7b20030ca063ab  round-1-reverse-component.jsonl
14b49b15364c6ecf073ab52bb89a6e56710c881eab487e808287ceecf69294b8  round-2-reverse-component.jsonl
e485a599c08863571ebeec847412a8f70628dfbf43c665f074f9872e407b95f3  round-2-incumbent.jsonl
45c853856e30a193649ad097ceb0000464c7e1e5939bee47d5217a5dc6696a5d  round-3-reverse-component.jsonl
f95e9fb9c0d489e0f9f5012140b0a9b419f5da221b62454498c323700f5f5c9c  round-3-incumbent.jsonl
38bd271656362fb7a850e5137360588a20fa90de5e2a8321801fa809906134fe  round-4-reverse-component.jsonl
da5e39fd4e2a4292ef9af5bfac9650787130d6396d19202194f6da7828eaadfe  round-4-incumbent.jsonl
86241c76a01bd23c80d21437126f0e55f6b792bbd7d6b9cbb710189c25c4164c  cancellation-pool-reuse.log
```

The four-round median-of-round-medians component/incumbent ratios were
`0.081x` for sparse endpoint IDs (about `44.86ms` saved) and `0.048x` for the
sparse complete path (about `81.87ms` saved). The high-fan-in, dense-suffix,
and 511/512/513 controls regressed (`1.42-1.87x`); no-path and the three
relationship-distinct controls improved. A timeout cancellation returned
`57014` in `1.159ms`, rolled back successfully, and the size-one pool
reacquired the same backend before an exact replay. This remains descriptive
component evidence only: it does not authorize routing, cache work, protected
access, or promotion.

## Boundary and workspace closure

The clean recapture closes exactness, typed component counters, plan-visible
buffers/temp/WAL, cancellation, and pool reuse. It does not yet decompose the
client/raw-PGX boundary into prepared-statement states, nor does it bind the
component telemetry to measured temporary-workspace high water. Those are
separate required observations; the existing `planning_ms` and `execution_ms`
fields must not be treated as a substitute for bind, first-row, decode, drain,
or session-reuse timings.

[`sql_strategy_routing_component_closure_v1.json`](../../benchmark/testdata/scale/protocols/sql_strategy_routing_component_closure_v1.json)
freezes the only permitted closure. It reuses the same eleven open fixtures,
four counterbalanced incumbent/component rounds, caller-owned Repeatable Read
contract, and size-one PostgreSQL pool. It adds no selector, retry, cache,
schema state, reference arm, or concurrency mode.

For each arm/case, the closure records one newly opened-session prepared miss,
five same-session prepared hits, one miss on a separate newly opened size-one
raw-PGX pool, and five hits after release/reacquisition of that same pooled
backend. Every raw execution must
match the public row/path observation. The raw-PGX samples separately retain
transaction setup, bind/prepare, first row, complete decode, drain/close, and
total timing. Each sample also records a SHA-256 of its sorted normalized
public rows; the runner rejects disagreement with the primary CySQL observation
or any other prepared-state stratum. Workspace observation runs only after result drain and is
excluded from those timing intervals. The exact client parse/optimize/translate/render
waterfall is retained beside it.

The command must supply the frozen one-MiB session and pool workspace ceilings
even though direct reverse is expected to allocate no component workspace. The
measurement sums non-diagnostic temporary relations visible in the query
transaction, excluding the runtime-attestation and telemetry scaffolding. The
size-one pool makes pooled-session and pool peaks directly comparable. Direct
component telemetry must then declare both `suffix_component` and `workspace`
families with complete provenance.

For example, the reverse arm of round one is:

```bash
graphbench \
  -modes postgres_sql -tags suffix-route-component-v1 \
  -warmup-iterations 1 -iterations 5 -pool-size 1 \
  -round 1 -block 1 -run-uuid "$RUN_UUID" -arm reverse_component -arm-order 2 \
  -require-clean-source \
  -postgres-repeatable-read -postgres-traversal-telemetry diagnostic \
  -postgres-expansion-suffix-route-component \
  -postgres-suffix-route-component-closure \
  -session-memory-ceiling-bytes 1048576 \
  -pool-memory-ceiling-bytes 1048576 \
  -jsonl-output .coverage/sql-routing-component-closure-v1/round-1-reverse-component.jsonl
```

The incumbent uses the same closure and ceiling flags but omits
`-postgres-expansion-suffix-route-component`; it remains the exact ordinary
forward statement. A failed row count, absent stage, changed pooled backend,
missing workspace observation, ceiling breach, incomplete component telemetry,
or target performance reversal stops this generation. No closure result is a
cache hit or automatic-selection result.

### Current closure disposition

The complete four-round artifact from source `94fe902` is retained as
diagnostic pre-enforcement evidence: it has all 88 successful main records,
the required prepared-state strata, zero measured temporary workspace, and
both sparse targets materially faster. It predates the per-sample normalized
observation SHA-256 requirement, however, and therefore records only raw row
counts for its direct pgx executions. It must not be used to assert that this
closure has passed.

The next capture must use a clean committed tree containing the observation
hash enforcement, a fresh nonempty run UUID, a new ignored artifact directory,
and the same four counterbalanced rounds. Run the cancellation/pool-reuse
operational test before timed arms. Do not append to or replace the retained
`94fe902` artifact.

## Required implementation slice

The first slice is diagnostic only:

1. Add a GraphBench arm that emits one forced reverse statement with the new
   diagnostic identity and records search, ordered-ID hydration, planning,
   execution, decode, and first-session timing separately.
2. Add new fixture declarations for the frozen classes and exact path/row
   oracles. Do not modify existing terminal-generation fixtures.
3. Capture the incumbent and direct-reverse component in counterbalanced
   PostgreSQL-only rounds under the same externally owned Repeatable Read
   transaction. No retry, selector, cache hit, or fallback is permitted.
4. Stop before automatic routing unless all exactness, resource, cancellation,
   pool-reuse, and direct-component overhead requirements pass.

The preflight does not measure a cache hit as a candidate result. A later,
separately named cache feasibility protocol must show that its key is scoped to
the graph and transaction/snapshot, that misses preserve the incumbent, and
that stale or absent metadata cannot alter correctness.

## Stop conditions

Stop this generation before cache implementation, automatic dispatch, or
protected holdout access if any of the following occur:

- the direct component is not exact for every frozen target/control;
- component telemetry is absent, contradictory, or attributes work to an
  inactive forward arm;
- direct reverse fails its declared resource, cancellation, or pool-reuse
  limits;
- direct reverse fails to materially improve both sparse full-path targets;
- a proposed external selection boundary adds more than the predeclared host
  A/A floor to an already-fast direct reverse execution;
- the design requires a persistent synopsis, graph epoch, or schema change.

After the boundary/workspace closure passes, the next decision is a separate
non-native architecture feasibility protocol for a transaction-scoped routing
cache. It must declare cache keys, invalidation, stale-data behavior,
transaction ownership, write/WAL budget, and rollback/removal before code is
added.

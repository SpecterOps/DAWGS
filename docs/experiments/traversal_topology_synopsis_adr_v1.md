# Traversal topology synopsis ADR v1

Status: **deferred; no synopsis is read by production translation or execution**.

Decision ID: `traversal-topology-synopsis-v1`. This record defines the design
and qualification boundary requested by M7 of
[`cysql_traversal_priorities.md`](../cysql_traversal_priorities.md). It does not
authorize a schema migration or selector change. Same-statement capped probes
and executor frontier state remain authoritative until a synopsis demonstrates
lower selector regret or lower probe overhead on the frozen holdout and also
passes the mutation, cache, and resource gates below.

## Decision

Do not add persistent topology tables yet. First capture the M1 diagnostic
counters and complete the M2--M6 candidate studies. Those artifacts provide the
runtime labels needed to test whether a synopsis predicts anything useful. A
synopsis implementation may proceed only as a separately versioned experiment;
it may influence a candidate score, but it may never prove correctness, bypass
an admission sentinel, or suppress exact fallback.

If the experiment proceeds, prefer reading the current synopsis at execution
time. Embedding a synopsis value in translated SQL is forbidden until its epoch
is part of `cypherTranslationCacheKey` and an epoch change either invalidates or
misses every affected cached translation. Mutable rollout policy is likewise an
execution input or an explicit cache generation, never unkeyed translator
state.

## Proposed storage contract

The candidate schema is graph-scoped and generation-scoped. All rows for a new
generation become visible atomically by advancing one graph metadata row after
the generation is complete.

| Relation | Key | Candidate values |
| --- | --- | --- |
| `traversal_synopsis_generation` | `(graph_id)` | `epoch`, schema/estimator version, source mutation epoch, build start/end, sampled/full mode, status |
| `traversal_synopsis_node_count` | `(graph_id, epoch, kind_id)` | exact or sampled count and error bound |
| `traversal_synopsis_edge_count` | `(graph_id, epoch, direction, kind_id, endpoint_kind_id)` | count, distinct starts/ends, error bound |
| `traversal_synopsis_degree` | `(graph_id, epoch, direction, kind_id, bucket)` | quantiles, heavy-hitter threshold, sample size |
| `traversal_synopsis_frontier` | `(graph_id, epoch, shape_bucket, depth_bucket)` | survival and reconvergence distributions, sample size |
| `traversal_synopsis_risk` | `(graph_id, epoch, shape_bucket)` | predecessor/output multiplicity buckets and saturation rate |

Multi-kind node membership is represented by separate overlapping strata; the
reader must not sum them as disjoint populations. Every estimate carries sample
size, method, error bound, build timestamp, source mutation epoch, and estimator
version. Missing, stale, building, failed, or incompatible generations produce
`synopsis_unavailable` and leave the runtime-probe policy unchanged.

## Refresh and mutation contract

- Graph creation starts with no usable generation. Graph drop removes or makes
  unreachable all generations for that graph.
- Bulk load or fixture replacement builds a fresh generation after the load and
  publishes it atomically. Readers never mix epochs.
- Incremental node/edge mutations advance a graph mutation epoch. A published
  synopsis whose source epoch differs is stale and advisory-only; the initial
  experiment treats it as unavailable rather than estimating staleness.
- Refresh work runs outside query latency measurements, has bounded memory and
  temporary storage, and records its own WAL, CPU, elapsed time, and table size.
- Failed or cancelled refresh leaves the previous generation intact but stale.
  Cleanup is idempotent and cannot delete the currently published generation.
- The first implementation must include schema-up/schema-down symmetry,
  concurrent reader/refresh tests, graph reload/drop tests, and an upgrade test.

## Shadow comparison

Shadow mode records a synopsis prediction beside the same-statement runtime
probe decision while executing the incumbent. It must not alter emitted arms.
Each record binds the workload, fixture and holdout identity, source revision,
graph mutation epoch, synopsis epoch/version, runtime policy version, probe
caps, predicted arm/score, observed probe values, actual selected exact arm,
fallback, and measured probe overhead.

Evaluate normal and envelope tiers on the frozen holdout. Stress remains a
fallback/staleness diagnostic. Report at least:

- prediction coverage and stale/unavailable frequency;
- selector regret against every exact arm;
- disagreement with capped runtime probes and executor frontier decisions;
- probe latency and buffer work saved after charging synopsis lookup cost;
- refresh latency, WAL, persistent bytes, and mutation write amplification;
- cache hit/miss and invalidation behavior across epoch changes;
- correctness, fallback, cancellation, pool-reuse, and concurrent-writer results.

## Admission gate

The synopsis experiment is rejected or remains deferred unless all of these are
shown with checksummed discovery and confirmation artifacts under the standard
97.5% protocol:

1. The synopsis materially lowers selector regret or probe overhead on both the
   declared corpus and frozen holdout after lookup cost.
2. No normal/envelope bucket regresses beyond the host A/A timing floor, and
   resource limits pass without unexpected WAL or spill in read execution.
3. Stale, absent, incompatible, or partially refreshed data always selects the
   unchanged runtime-probe/incumbent chain with a precise reason.
4. Mutation and refresh overhead passes an independently declared budget; it is
   not hidden inside query measurements.
5. Translation-cache tests prove that no SQL can retain an unkeyed embedded
   epoch or rollout policy.

Until those gates pass, `traversal-topology-synopsis-v1` has no database schema,
no cache-key effect, no production feature gate, and no automatic selector
bucket. This is the reversible outcome required by the priority plan: runtime
evidence remains the authority, and lack of a synopsis is normal operation.

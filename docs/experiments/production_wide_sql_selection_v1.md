# Production-wide SQL selection v1

Status: implementation contract. This work expands verified SQL selection from
one exact-query canary to structurally eligible PostgreSQL traversal queries.
It does not make an unqualified candidate automatic, weaken an existing
promotion gate, revive terminally rejected selector identities, or permit a
stale topology decision to change query results.

## Outcome

For a verified policy generation, every structurally eligible query is matched
to one of these outcomes:

1. a graph-independent qualified candidate;
2. a snapshot-bound topology-selected qualified candidate; or
3. the incumbent.

The incumbent is required for every unknown, stale, unsupported, disabled,
unqualified, malformed, cancelled, or resource-limited selection. Candidates
retain their existing admission sentinels, exact fallback, and dedicated
rollback switch.

## Frozen safety contract

Selection has two distinct stages:

```text
Cypher -> structural shape -> verified policy bucket -> StrategySelection
                                               |              |
                                               |              +-> selected arm
                                               v
                                      transaction synopsis state
```

`StrategySelection` is immutable metadata. It includes the policy generation,
selector version, bucket, candidate, fallback, immutable caps, and selection
reason. Topology-sensitive selections additionally carry the observed graph
mutation epoch and synopsis generation. It must never retain rows, graph
values, a snapshot identifier, a transaction, a connection, a result, or a
caller-owned parameter map.

Translation cache entries are partitioned by the effective policy identity and
selected arm. A routing decision is not a translation-cache entry. Until a
separate evidence-backed admission proves otherwise, topology decisions are
owned by one read-only Repeatable Read or Serializable transaction and are
discarded before that transaction closes.

## Candidate scope

The implementation order is deliberately narrow:

| Family | Initial selection type | Current boundary |
| --- | --- | --- |
| ASP-I1 predecessor DAG | static structural | exact-query canary becomes a verified structural bucket after clean evidence |
| Canonical SP-I1 | static structural | qualified shape bucket only |
| Endpoint-seeded reverse | existing static envelope | preserves current rollback semantics |
| Fixed-suffix reverse | topology-sensitive | new identity only; rejected orientation and suffix-guard identities remain terminal |

SP-I2, B1, B2, adjacency materialization, result caching, cross-transaction
routing caches, and any persistent route cache are outside this version.

## Admission SLOs

Every production candidate requires exact semantic observations, complete
resource and execution receipts, and the normal GraphBench evidence closure.
The selection layer additionally requires all of the following on its declared
training and frozen holdout cohorts:

- p50 materially improves by at least 5% or 100 microseconds;
- p95 is at most 1.05 times the incumbent after selector cost;
- selector overhead is at most 1.10 times or 100 microseconds of the selected
  exact arm;
- no selected candidate exposes output before its declared fallback gates pass;
- stale, absent, malformed, partial, or incompatible selector state selects
  the incumbent and emits a precise reason;
- cancellation, rollback, pool reuse, concurrent mutation, and schema reset
  preserve exact observations and leave no reusable decision state.

Stress cases prove correctness, caps, and fallback only. They cannot tune a
selector or promote a bucket.

## Phased delivery

1. Add a versioned structural traversal identity and typed shadow selection.
2. Extend policy manifests and GraphBench evidence to bind structural buckets.
3. Promote graph-independent qualified buckets through the existing policy
   generation and rollback path.
4. Add graph mutation epochs, then a versioned topology synopsis in
   shadow-only mode.
5. Admit one snapshot-bound selected SQL arm only after synopsis evidence
   passes.
6. Qualify a new fixed-suffix selector identity and roll it out by policy
   generation and structural bucket.

Each phase must be independently reversible. No later phase may silently make
an earlier diagnostic, shadow, or tool-only candidate automatic.

## Rollout

Policy generations are the rollout unit. Deployment begins with shadow-only
selection, then moves through explicit graph/bucket cohorts. A matching
candidate rollback switch or a zero policy immediately produces a distinct
incumbent cache identity. Automatic rollback decisions are deliberately out of
scope; the first release uses operator-controlled generation changes and
query-text-free telemetry.

## Definition of done

Production-wide SQL selection is complete only when all qualified structural
buckets route without exact-query enumeration, topology-dependent selection is
snapshot-bound and stale-safe, each selected statement has one primary arm and
an exact fallback, and the PostgreSQL and Neo4j validation matrix plus clean
GraphBench evidence pass for every activated bucket.

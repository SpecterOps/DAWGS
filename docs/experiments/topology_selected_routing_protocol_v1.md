# Topology-selected routing protocol v1

Status: frozen implementation protocol. This supersedes the deferred
pre-schema decision in `traversal_topology_synopsis_adr_v1.md`. It does not
enable a production candidate by itself.

## Version boundaries

- Promotion manifest v2 authorizes one exact Cypher query and its rendered SQL.
- Promotion manifest v3 authorizes graph-independent shortest-path structural
  buckets.
- Promotion manifest v4 is reserved for topology-dependent fixed-suffix
  routing. It must bind the structural shape, estimator version, immutable
  thresholds, candidate and fallback template identities, and synopsis schema
  compatibility.

No version reinterprets a previous version. Tool-only and terminal identities
remain ineligible for production activation.

## Selection and snapshot contract

Topology selection is permitted only in a caller-owned, read-only Repeatable
Read or Serializable PostgreSQL transaction. The synopsis is read through the
same transaction and snapshot as the graph query. A missing, building, failed,
incompatible, stale, ambiguous, or resource-limited synopsis selects the
incumbent with a query-text-free reason.

The selector reads no graph values from a synopsis and cannot establish query
correctness. It estimates only candidate cost. Candidate admission caps and an
exact incumbent fallback remain authoritative.

`topology-fixed-suffix-counts-v1` admits the reverse candidate only when the
synopsis reports `edge_count * 1000 <= node_count *
maximum_edge_to_node_ratio_per_mille`. Version v1 freezes that threshold at
`1000`; it is manifest-bound and therefore part of the route-cache policy
identity. A new estimator or threshold requires a new selector version.

## Route-decision cache

A route decision is transaction-owned application memory, never a translation
cache entry. It is keyed by a transaction-owner token, graph ID, structural
shape, canonical parameter fingerprint, policy identity, synopsis generation,
mutation epoch, and local invalidation generation. It is bounded to 64 entries,
64 KiB total, and 4 KiB per entry with no eviction.

A miss executes the incumbent only. A hit may execute the qualified reverse
candidate only in the same active transaction and snapshot. Writes, savepoints,
rollback, cancellation, retry, pool release, and transaction completion discard
or disable all decisions.

## Execution contract

The fixed-suffix candidate is a single arm: it never embeds an inactive
forward arm. Candidate rows are fully buffered within fixed row and byte caps
before they become public. A cap status or candidate incompleteness discards
candidate output and executes the exact forward incumbent in the same snapshot.

The production selector receives its own identity and emergency rollback
switch. The zero policy and the rollback switch produce an incumbent-specific
translation identity immediately.

## Evidence and rollout

GraphBench must independently recompute the structural and SQL-template
identities; report selector coverage, regret, lookup cost, refresh cost, WAL,
storage, mutation amplification, cache state, candidate/fallback receipts, and
all transaction-boundary states. Training is frozen before holdout execution.

Activation requires exact observations, complete receipts, p50 improvement of
at least 5% or 100 microseconds, p95 no worse than 1.05 times the incumbent,
and selector overhead no worse than 1.10 times or 100 microseconds. A failed
gate produces a terminal rejection record and leaves production selection off.

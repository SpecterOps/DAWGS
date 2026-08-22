# Suffix-route cache feasibility v1

Status: frozen pre-implementation feasibility contract. The default remains
`EXPANSION-STEPWISE-FORWARD`; this document does not enable a selector, cache,
retry, schema object, or production behavior.

## Admission

The direct-component closure from clean commit `c490f3c` passed with 88 exact
records at `.coverage/sql-routing-component-closure-v1-c490f3c`. Every record
has twelve agreeing raw-PGX observation digests, stable pooled backend identity,
and zero measured temporary workspace. Its paired component/incumbent median
ratios are `0.0839` for sparse endpoint IDs and `0.0445` for sparse complete
paths.

That evidence permits this protocol only. It does not establish that a routing
decision can outlive a transaction or that a cache hit is safe.

## Cache boundary

The candidate is an application-memory cache owned by exactly one active,
caller-owned, read-only `REPEATABLE READ` PostgreSQL transaction. It stores an
immutable routing decision, never public results, graph values, translated SQL,
plans, or graph metadata. A cache miss executes the exact ordinary forward
incumbent. A hit may use the already-qualified direct reverse statement only
inside the same owner transaction and snapshot.

Every key includes an opaque transaction-owner token minted after `BEGIN`, the
graph ID, normalized Cypher shape, canonical parameter names/types/values,
frozen policy identity, and a transaction-local invalidation generation. The
owner token is not a reusable connection, backend PID, pool slot, or process
identity. Missing or unverifiable key data is an incumbent-only bypass.

There is deliberately no cross-transaction, cross-snapshot, cross-connection,
or retry reuse. The repository has no graph mutation epoch, so permitting any
such reuse would be unsafe.

## Lifetime and invalidation

The cache is allocated after `BEGIN` and discarded before the transaction is
returned to its caller. Commit, rollback, cancellation, connection release,
and retry discard every entry. The feasibility scope permits cache use only in
transactions with no graph mutation and no savepoint lifecycle; either boundary
invalidates all entries, increments the local generation, and disables cache
use for the rest of that transaction.

This restriction intentionally makes rollback removal application-memory-only.
It avoids a cache-maintenance statement whose write, WAL, or cleanup behavior
could contaminate a read-path measurement.

## Resource and observability contract

The cache is capped at 64 entries, 64 KiB total, and 4 KiB per entry. It has no
eviction: capacity exhaustion bypasses caching and publishes nothing. Entries
are immutable and must not retain caller-owned mutable buffers.

The cache may not create database objects or issue data-modifying cache SQL.
It must produce zero cache-attributable WAL and no temp relation, durable
write, catalog change, translation-cache key change, or persistent metadata.
Feasibility evidence must retain redacted key/owner/generation receipts,
bounded-memory high-water, cache state, backend/snapshot provenance, exact
public observations, and cancellation/rollback replay evidence.

## Required study and stop rules

Only the eleven open `suffix-route-component-v1` training declarations may be
used. Any separately authorized feasibility implementation must exercise disabled,
miss, hit, capacity-exhausted, invalidated, cancelled, and rolled-back states.
Every state must retain exact public rows/paths. A miss must execute only the
ordinary incumbent; failures, cancellation, timeout, malformed entries, and
capacity exhaustion publish no decision.

Stop immediately on any cache reuse across an ownership boundary; a candidate
execution on a miss; stale-state divergence; cache-attributable WAL or durable
state; unbounded allocation; incomplete cleanup; protected-fixture access; or
an automatic-routing claim.

The machine-readable frozen contract is
[`suffix_route_cache_feasibility_v1.json`](../../benchmark/testdata/scale/protocols/suffix_route_cache_feasibility_v1.json).

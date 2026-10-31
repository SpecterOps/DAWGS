# Topology fixed-suffix v4 status

Status: implementation complete; promotion unactivated.

The PostgreSQL V2 driver now implements the manifest-v4 execution boundary
defined by [Topology-selected routing protocol v1](topology_selected_routing_protocol_v1.md).
This record deliberately does not grant a promotion manifest or activate the
candidate.

## Implemented boundary

- The driver validates the v4 candidate identity, exact immutable caps,
  estimator, synopsis schema, route-cache protocol, fixed-suffix structural
  fingerprint, SQL-template fingerprint, and the frozen 1000-per-mille
  edge-to-node-density threshold.
- A current v2 synopsis and a read-only Repeatable Read or Serializable
  transaction are mandatory.
- Decisions are transaction-owned and parameter-sensitive. A cache miss runs
  the incumbent; a cache hit in the identical snapshot may run the one
  reverse-only candidate.
- Candidate output is buffered within the v4 output limits. Incompleteness,
  overflow, or an unrecognized status discards the candidate and executes the
  exact forward fallback in the same transaction.
- The emergency v4 rollback switch and a zero policy immediately restore
  incumbent-only SQL.

## Non-activation decision

No v4 promotion manifest is committed or installed. A successful implementation
or smoke execution is not performance qualification: the required frozen
training/holdout evidence has not demonstrated selector coverage, regret,
refresh and mutation cost, receipt closure, and the protocol's p50/p95/overhead
gates. The driver therefore remains default-off and any missing, stale,
incompatible, first-seen, mutable, cancelled, or resource-limited route stays
on the incumbent.

The next promotion action is to capture the frozen v4 cohort with GraphBench,
verify all six evidence roles, and either install its digest-bound manifest or
write a terminal rejection record for that evidence generation.

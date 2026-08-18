# P5 adjacency materialization feasibility v2

Status: implemented and PostgreSQL-integration-validated; pending a clean
feasibility capture. This is a prospective storage-and-mutation feasibility
study only. It does not authorize a Cypher candidate, query routing, a
production schema migration, protected-corpus timing, or a promotion claim.

V2 preserves the shadow-only architecture and the frozen base/shadow roster
from V1. V1's completed artifact is rejected by
[`p5_adjacency_materialization_feasibility_v1_rejection.json`](../../benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v1_rejection.json): PostgreSQL's
`EXPLAIN (ANALYZE, WAL)` plan-node counter omitted the WAL emitted by the
shadow's row triggers. It therefore could not attribute the complete physical
write cost.

## Measurement correction

For each committed calibration V2 executes the ordinary mutation once with a
unique SQL comment tag, then reads that tagged top-level statement's one-call
delta from `pg_stat_statements`. PostgreSQL attributes the mutation's trigger
maintenance to that statement, so the artifact separately records WAL records,
full-page images, and bytes. The report also preserves quiescent LSN deltas as
diagnostics, but does not use them as the attributed mutation result.

The capture-only runner may install `pg_stat_statements` in its disposable
database when the extension is absent, and removes it again only if it created
it. PostgreSQL must already preload the module; an unavailable preload setting
is a clear capture precondition failure. This measurement dependency is not a
driver or production-schema dependency.

## Frozen boundary and roster

The shadow remains `public.p5_adjacency_v1`, containing two graph-scoped rows
per base edge, with no Cypher translator, executor, policy, cache key, or
production selector allowed to read it. The runner fixes four counterbalanced
base/shadow blocks at sizes 1, 1,000, and 2,000; one warm-up and five timed
rollback-only samples cover relationship create, conflict/upsert,
property-only update, relationship delete, node-delete cascade, graph reload,
and graph drop. Raw parameterized adjacency reads collect physical probes only.

Exact mapping, property-update identity, rollback, cancellation/pool reuse,
graph cleanup, autovacuum quiescence, and clean-source checks remain mandatory.
A passed artifact still authorizes only a separate, frozen resource-budget
decision; it cannot authorize a candidate, selector, production schema, or
performance claim.

The complete V2 contract is
[`p5_adjacency_materialization_feasibility_v2.json`](../../benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v2.json).

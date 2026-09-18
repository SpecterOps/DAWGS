# P5 adjacency materialization feasibility v2

Status: captured and terminally not advanced. The valid physical evidence does
not authorize a Cypher candidate, query routing, a production schema migration,
protected-corpus timing, or a promotion claim.

V2 preserves the shadow-only architecture and the frozen base/shadow roster
from V1. V1's completed artifact is rejected by
[`p5_adjacency_materialization_feasibility_v1_rejection.json`](../../benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v1_rejection.json): PostgreSQL's
`EXPLAIN (ANALYZE, WAL)` plan-node counter omitted the WAL emitted by the
shadow's row triggers. It therefore could not attribute the complete physical
write cost.

## Measurement correction

For each committed calibration V2 executes the ordinary mutation once with an
operation-specific no-op CTE marker, then reads that statement's before/after
one-call delta from `pg_stat_statements`. PostgreSQL attributes the mutation's
trigger maintenance to that statement, so the artifact separately records WAL
records, full-page images, and bytes. The report also preserves quiescent LSN
deltas as diagnostics, but does not use them as the attributed mutation result.

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

## Captured result and disposition

The clean-source capture completed on PostgreSQL 17.10 at commit `d257cd9`
with all 24 timed conditions, 42 committed calibrations, exact-state oracles,
cancellation/pool-reuse proof, quiescent LSN checks, and final cleanup passing.
The immutable ignored-workspace artifact has SHA-256
`c8c4d7f88d0904bd00d1b85355f2f5806dcb74cf84a41d80237bf081a7222d04`.
Its statement WAL fields are one-call `pg_stat_statements` deltas, including
trigger maintenance; this is distinct from V1's invalid plan-node counter.

The following values are upper medians across the four per-block medians. The
storage rows compare base edge storage with base plus the additional shadow
relation; the latency rows compare shadow with base.

| Measurement | 1,000 targets | 2,000 targets |
| --- | ---: | ---: |
| Combined storage / base | 1.92x | 1.95x |
| Attributed WAL: batch create | 2.22x | 2.22x |
| Attributed WAL: relationship delete | 3.00x | 3.00x |
| Relationship-delete latency | 605x | 1,324x |
| Node-delete-cascade latency | 182x | 524x |
| Graph-clear/reload latency | 245x | 640x |
| Graph-drop latency | 224x | 559x |

The raw shadow lookup probe was faster than the base physical probe (0.66x at
1,000 and 0.53x at 2,000), but it is intentionally not a Cypher result and
does not offset the storage, WAL, or mutation cost. No separate write-budget
decision is created and no candidate experiment is authorized. The complete
non-promotional record is
[`p5_adjacency_materialization_feasibility_v2_disposition.json`](../../benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v2_disposition.json).

No P5 successor is currently selected. The native-extension feasibility path
was withdrawn; it does not revive, retune, or reuse this materialization as a
candidate.

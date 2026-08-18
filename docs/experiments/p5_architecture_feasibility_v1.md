# P5 architecture feasibility v1

Status: deferred before candidate implementation. This is a read-only
repository and platform inventory, not a performance study, schema migration,
or production-design decision.

## Why P5 was assessed

P1's static suffix-retry generation and P3's B1/B2 shortest-path generation
are terminally rejected. The sole current P4 I1 preflight is also terminally
rejected because its no-path target win regressed every shallow/reconvergent
control. That meets the performance plan's condition to assess an
architectural lane, but it does not itself justify a persistent copy of graph
data or native server code.

## Inventory

The PostgreSQL layout is already graph-partitioned. Every edge partition has
covering B-tree indexes for both physical adjacency directions:
`(start_id, kind_id) INCLUDE (id, end_id)` and `(end_id, kind_id) INCLUDE
(id, start_id)`, plus a kind-first covering index. A denormalized adjacency
relation would therefore duplicate data already available to the recursive
emitters and would have to prove a benefit after its read, write, WAL, and
storage costs.

The mutation path performs direct batched node/edge inserts, upserts, updates,
and deletes in PostgreSQL transactions. Neither the schema nor the driver
maintains a graph mutation epoch. The translation-cache key contains normalized
query text, graph ID, parameter-type shape, and policy identity—but no graph
data generation. Thus a mutable adjacency copy or synopsis cannot safely be
embedded in generated SQL, and a maintained lookup needs an explicit epoch,
publication, staleness, and cache contract first.

There is also no native PostgreSQL extension package in this repository: no C
sources, control file, PGXS build, versioned SQL install scripts, or deployment
matrix. The local build environment supplies PostgreSQL 18.4 PGXS, while the
live PostgreSQL target used for this work is 17.10. Building a server extension
locally would not produce a testable binary for that target. The live server
has only `intarray`, `pg_trgm`, and `plpgsql` installed; `pg_stat_statements`
is preloaded but is not an installed execution extension.

The credential-free inventory is
`.coverage/p5-feasibility-4db030c/inventory.json`. It records the source
commit, server/build versions, existing indexes, installed extensions, and the
cache/mutation findings; its SHA-256 is
`6913d46209601b5bd8bd955b28738f1040dc6c14a514d95aeae081d2977ffb92`.
It is a platform observation rather than portable performance evidence.

## Disposition

Do not add a topology synopsis, duplicate adjacency table, application-side
traversal service, or native extension under the current P5 scope:

- The versioned topology synopsis remains deferred under
  [`traversal_topology_synopsis_adr_v1.md`](traversal_topology_synopsis_adr_v1.md).
  Its required graph mutation epoch, atomic refresh publication, stale-read
  behavior, mutation/WAL budget, and cache-key proof do not yet exist.
- A duplicate adjacency relation has no predeclared exact target or measured
  read advantage over the existing direction-specific covering indexes. Adding
  it first would expose write and storage costs without an admission case.
- A native extension requires a separately owned PostgreSQL 17/18 build,
  installation, upgrade, rollback, and CI matrix before it can be evaluated.
  No such deployment contract is currently present.
- An application-side service cannot be considered until it can preserve the
  driver's Repeatable Read snapshot and exact fallback contract.

This is a defer, not a terminal claim about all future native work. A future
P5 successor must choose exactly one architecture and first freeze a
non-candidate feasibility protocol that specifies the server-version delivery
matrix, mutation fixture and write/WAL/storage budget, graph-generation and
cache behavior, rollback/removal path, and a read-only exact baseline. Only
after that prerequisite passes may it create a candidate roster. P4 artifacts
and the current platform inventory cannot tune or promote such a successor.

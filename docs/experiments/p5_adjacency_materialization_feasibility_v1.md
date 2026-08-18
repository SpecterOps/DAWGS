# P5 adjacency materialization feasibility v1

Status: implemented and PostgreSQL-integration-validated; pending a clean
feasibility capture. This is a prospective
storage-and-mutation feasibility study only. It does not authorize a Cypher
candidate, query routing, a production schema migration, protected-corpus
timing, or a promotion claim.

## Chosen architecture

The feasibility audit ruled out starting with a native extension: this
repository has no extension package and the local PostgreSQL 18 PGXS build
interface cannot produce a module testable against the live PostgreSQL 17
server. The topology synopsis remains separately deferred because it lacks a
graph mutation epoch and cache contract. The remaining locally testable option
is a shadow, graph-scoped directed adjacency materialization.

`public.p5_adjacency_v1` will contain exactly two rows for every base edge:
one outbound `(start_id, end_id)` row and one inbound `(end_id, start_id)` row,
both retaining graph, kind, and edge identity. Its only proposed lookup index
is `(graph_id, direction, anchor_id, kind_id, edge_id) INCLUDE (neighbor_id)`.
It is deliberately compared with the existing base-edge covering indexes, not
with an artificial sequential-scan floor.

The table is shadow-only. No Cypher translation, shortest-path executor,
runtime policy, translation-cache key, or production selector may read it.
This boundary lets the work establish maintenance and storage feasibility
before it can change a public read path.

## Frozen measurement roster

The roster uses the existing direct-write fixture at sizes 1, 1,000, and 2,000
for relationship create, conflict/upsert, property-only update, relationship
delete, node-delete cascade, graph reload, and graph drop. Four
counterbalanced blocks use one warm-up and five timed samples. Each mutation
validates its post-state then rolls back; committed calibration runs separately
measure WAL LSN deltas and relation/index bytes.

Every base edge must have exactly one outbound and one inbound shadow row, and
every shadow row must map back to one matching base edge. Property-only updates
must retain row identity. Cancellation, rollback, pool reuse, reload, and
graph drop must leave no stale committed row. Raw parameterized adjacency
lookups may collect plan/buffer/cardinality measurements only after these
oracles pass; their rows never become a Cypher result.

The complete contract is
[`benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v1.json`](../../benchmark/testdata/scale/protocols/p5_adjacency_materialization_feasibility_v1.json).
It freezes what to measure, but intentionally freezes no pass/fail latency or
write budget: the first report must expose the physical trade-off before a
separate budget decision can authorize any candidate experiment.

## Stop boundary

Any shadow/base mismatch, stale committed row, non-transactional maintenance,
unattributed write/WAL/storage metric, or read-path access by a Cypher
candidate stops the study. Even a complete report authorizes only a separately
frozen budget decision. It cannot authorize a production schema, automatic
selector, cache-key change, protected corpus, or performance claim.

## Implemented shadow boundary

The shadow is an explicit `query.On(tx).InstallP5AdjacencyShadow` action;
ordinary `schema_up.sql` and normal driver startup neither install nor read it.
The paired remove action leaves core graph storage intact. Installation creates
graph partitions for existing graphs, backfills each edge into one outbound and
one inbound row, and then enables same-transaction triggers for edge insert,
endpoint/kind update, and delete. A property-only edge update does not touch
the shadow rows. The base edge foreign key and node-delete edge cascade remove
shadow rows on relationship/node/graph deletion.

The unit SQL boundary test and targeted PostgreSQL lifecycle test pass. The
lifecycle test covers backfill, insert, endpoint update, property-only
non-rewrite, node-delete cascade, rollback, canceled statement recovery through
a replacement pooled connection, graph deletion, and shadow removal. It does
not provide any P5 latency, WAL, storage, or query-performance result; those
still require a clean source capture under this roster.

## Capture runner

`graphbench -p5-adjacency-feasibility-output <report.json>` is the sole capture
entry point for this roster. It requires the disposable PostgreSQL guard, a
clean source tree, and a one-connection pool. It fixes the four counterbalanced
blocks, one warm-up, five timed rollback-only samples, fixture sizes, and
mutation roster internally; it rejects Cypher corpus selectors and normal
GraphBench result outputs.

The runner first captures base-only blocks with the shadow relation removed,
then alternates shadow and base conditions by dropping or reinstalling the
explicit shadow schema at each block boundary. It creates a fresh graph-scoped
fixture per condition, verifies exact base/shadow mapping around every write,
and records raw base and shadow adjacency probes separately. Its committed
calibration graph runs report setup and per-mutation WAL deltas; they are never
timed Cypher observations. The graph-clear/reload rollback sample clears the
fixture within its transaction and uses the required rollback to restore the
same fixture before the next sample.

For example, build the artifact from a clean commit and write the report to an
ignored workspace location:

```bash
go build -trimpath -o .coverage/graphbench-p5 ./cmd/graphbench
CONNECTION_STRING='postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs' \
  ./.coverage/graphbench-p5 \
  -p5-adjacency-feasibility-output .coverage/p5-adjacency-feasibility.json
```

The JSON report contains the protocol checksum, clean source and binary
identity, per-operation p50/p95 values, committed WAL deltas, relation bytes,
raw parameterized lookup plans with buffers, and cancellation/pool-reuse
evidence. A successful report remains a feasibility record only: it cannot set
a write budget or authorize a Cypher read-path experiment.

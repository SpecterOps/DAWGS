# PostgreSQL Hash-Filtered Ingest

`(*pg.Driver).Ingest` is a new PostgreSQL-only path for additive graph ingestion. It does not replace the
backend-independent `graph.Database` interface, and the existing transaction APIs and `BatchOperation` remain
supported. The hash-filtered path is intended for a graph managed exclusively through this API; do not mix writers on
that graph.

## API

The target declaration must contain exactly one unique B-tree node constraint on the exact string property `objectid`;
the driver validates that declaration before doing any ingest work. `Driver.Ingest` resolves the target through
`AssertGraph`: a missing graph is created from the declaration, while an existing graph is asserted to match it. The
following example compiles as written and supplies nodes before edges through `iter.Seq2`:

```go
package example

import (
	"context"
	"iter"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

func ingestExample(ctx context.Context, driver *pg.Driver) (pg.IngestStats, error) {
	target := graph.Graph{
		Name: "managed_graph",
		NodeConstraints: []graph.Constraint{{
			Field: "objectid",
			Type:  graph.BTreeIndex,
		}},
	}

	nodes := iter.Seq2[*pg.IngestNode, error](func(yield func(*pg.IngestNode, error) bool) {
		if !yield(&pg.IngestNode{
			ObjectID: "user:alice",
			Kinds:    graph.Kinds{graph.StringKind("User")},
			Properties: graph.AsProperties(map[string]any{
				"objectid": "user:alice",
				"name":     "Alice",
			}),
		}, nil) {
			return
		}
		yield(&pg.IngestNode{
			ObjectID: "group:engineering",
			Kinds:    graph.Kinds{graph.StringKind("Group")},
			Properties: graph.AsProperties(map[string]any{
				"objectid": "group:engineering",
				"name":     "Engineering",
			}),
		}, nil)
	})

	edges := iter.Seq2[*pg.IngestEdge, error](func(yield func(*pg.IngestEdge, error) bool) {
		yield(&pg.IngestEdge{
			StartObjectID: "user:alice",
			EndObjectID:   "group:engineering",
			Kind:          graph.StringKind("MemberOf"),
			Properties: graph.AsProperties(map[string]any{
				"source": "directory",
			}),
		}, nil)
	})

	return driver.Ingest(ctx, target, pg.IngestInput{
		Nodes: nodes,
		Edges: edges,
	}, pg.IngestOptions{
		BucketCount:        4_096,
		TempDir:            "",
		ClusterAfterIngest: false,
	})
}
```

An iterator may yield its own error; ingestion stops and returns it with phase context. A nil node or edge sequence is
an empty phase. All node buckets finish before the edge iterator is consumed, so an edge may refer to a node supplied in
the same call or already stored in the target graph.

## Identity and stored metadata

A node identity is the exact, case-sensitive UTF-8 string in `objectid`. There is no trimming, case folding, or Unicode
normalization. `IngestNode.ObjectID` must be non-empty and must equal an incoming `objectid` property when that property
is present; the driver adds the property to a cloned map when it is absent. The target's single B-tree constraint is a
unique expression index over `properties ->> 'objectid'`, not a second identity column.

An edge identity is the directed tuple `(start objectid, kind, end objectid)`. Direction and exact string bytes matter.
Ingested edge rows persist `start_object_id` and `end_object_id` alongside database endpoint IDs. These source strings
allow narrow hash reads and detect legacy or inconsistent rows without joining both endpoint tables. Edges written by
older APIs leave the new source and hash columns nullable and cannot safely be mixed into a managed ingest graph.

Identity bucketing uses a domain-separated, length-framed XXH32 value. PostgreSQL stores the same 32 bits in a signed
`integer`; negative values are normal. Hash collisions never establish identity: the driver always compares the exact
node string or complete directed edge tuple.

## Additive merge and hash contract

Ingest never deletes an entity. Node kinds are unioned, and incoming property values win when maps are merged. Only
`graph.Properties.Map` participates; `Modified` and `Deleted` tracking metadata do not remove values. Duplicate input
identities within one bucket coalesce in arrival order with the same union/last-value-wins rules.

Each stored `content_hash` is the first 16 bytes of SHA-256 over a versioned DAWGS canonical encoding of the complete
merged database state. A node hash includes sorted exact kind names and properties except `objectid`; an edge hash
includes all properties. Canonical values distinguish null, booleans, strings, normalized decimal numbers, ordered
arrays, and objects whose UTF-8 keys are sorted and length-framed. Numerically equivalent JSON values such as `1`,
`1.0`, and `1e0` hash alike. NaN, infinity, invalid UTF-8, unsupported Go values, and values PostgreSQL JSONB cannot
represent are rejected.

The client-side exact identity/content-hash comparison is the only no-op filter. Every mismatch is staged, and the
PostgreSQL upsert is unconditional. In particular, a partial mutation can omit stored fields, hash differently, and be
staged even when its additive merge leaves the logical row unchanged. There is deliberately no database-side no-op
`WHERE` guard in this proof of concept.

## Buckets, spooling, and retries

`BucketCount` must be a power of two from 1 through 2^32 (and representable by the running Go platform). It divides the
32-bit hash space into contiguous signed integer ranges used by ordinary B-tree predicates. PostgreSQL stores no bucket
configuration, so the count is runtime-only and may change between calls. More buckets reduce read amplification for
sparse input but increase local-file, query, and transaction overhead; fewer buckets favor dense loads. Start with a
count that leaves a useful number of records per populated bucket, then measure representative dense and sparse runs.

Nodes and edges spool separately. `TempDir` selects only the parent; the driver creates and owns a private child
directory with mode `0700` and lazy bucket files with mode `0600`. Spools contain graph identities and properties, so
the parent must be trusted, local storage should be encrypted where required, and disk capacity must cover the current
phase plus filesystem overhead. The driver removes only its private child on success, iterator or database failure,
context cancellation, and clustering failure. Abrupt process or host termination can still require operator cleanup.

Each populated bucket has its own transaction. A failed bucket rolls back and stops later work, but earlier buckets
remain committed. A node-phase failure prevents edge consumption; an edge-phase failure leaves committed nodes and
earlier edge buckets. Returned statistics describe completed work. There is no checkpoint file: retry the complete
input after correcting the error. Full retry is idempotent only under the single-writer rule.

## Writer ownership and rebuilds

Only this hash-maintaining path may write a managed graph, and only one ingest may run against that graph at a time.
Concurrent ingest calls, `BatchOperation`, Cypher updates, ordinary write transactions, direct SQL, and other tools can
leave stored hashes or edge source strings stale. A stale hash can make a later client comparison discard a mutation
incorrectly. `BatchOperation` remains a supported DAWGS API and the benchmark uses it as a baseline, but it must target
separate graphs unless a graph is being managed without hash-filtered ingest.

Existing graphs require a rebuild into a fresh schema before adopting this path; there is no online backfill. Hash
domains, canonical encoding, and ignored-property rules are versioned. Changing any of them requires a new version and
a full rebuild. This proof of concept does not automatically migrate or upgrade stored hashes.

## Optional clustering

`ClusterAfterIngest` defaults to `false`. When enabled, it runs after both ingest phases commit and clusters only the
target graph's node partition and edge partition on each partition's child `id_hash` index. It never clusters parent
tables or another graph.

PostgreSQL `CLUSTER` rewrites each table, takes an exclusive lock, and can need temporary disk comparable to the table
and indexes. It is an offline locality experiment, not routine maintenance, and PostgreSQL does not preserve the order
after later writes. If node clustering succeeds and edge clustering fails, all ingest data remains committed and the
returned statistics include the clustering time accrued before the error. Plan lock windows and free disk explicitly;
do not enable this option by default in a serving workload.

## Manual PostgreSQL benchmark

`BenchmarkPostgresHashFilteredIngest` requires a live PostgreSQL database and the `manual_integration` build tag. It
creates a unique constrained graph for every measured iteration and drops its graph-local partitions and indexes after
the iteration. Dataset configuration, graph reset, path-native seeding, optional pre-clustering, partition maintenance,
garbage collection, correctness checks, relation-size queries, WAL snapshots, `EXPLAIN`, and cleanup are outside the
timed section. Node and edge records are generated lazily while the selected `BatchOperation` or `Driver.Ingest` call
consumes its iterators, so that record construction is part of the timed API call. The benchmark does not write reports
or plan artifacts.

Seeded `BatchOperation` cases write their seed through `BatchOperation`; seeded `Driver.Ingest` cases write it through
`Driver.Ingest`. A benchmark graph never mixes the two writers. After every seed, the benchmark runs
`VACUUM (ANALYZE)` on exactly that graph's node and edge child partitions. This supplies planner statistics and marks
eligible heap pages all-visible so stable reads and covering-index variants can exercise index-only plans. After a
measured write, the same target-only `VACUUM (ANALYZE)` runs immediately before representative `EXPLAIN` capture, so
plans reflect the post-write state and visibility map. Maintenance failures fail the benchmark; none of this
maintenance is included in elapsed time or allocation measurements.

The default workload is intentionally large:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
go test -tags manual_integration ./drivers/pg \
  -run '^$' \
  -bench '^BenchmarkPostgresHashFilteredIngest$' \
  -benchmem \
  -count=1
```

Use a smaller smoke run before a full comparison:

```bash
DAWGS_INGEST_BENCH_NODES=1000 \
DAWGS_INGEST_BENCH_EDGES=2000 \
DAWGS_INGEST_BENCH_BUCKETS=16,256 \
DAWGS_INGEST_BENCH_CLUSTER=false,true \
go test -tags manual_integration ./drivers/pg \
  -run '^$' \
  -bench BenchmarkPostgresHashFilteredIngest \
  -benchmem \
  -count=1
```

Configuration is parsed before any benchmark graph is created. Blank, malformed, duplicate, out-of-range, and
non-power-of-two values fail with the variable name in the error.

| Variable | Default | Meaning |
| --- | ---: | --- |
| `DAWGS_INGEST_BENCH_NODES` | `100000` | Positive deterministic node count. |
| `DAWGS_INGEST_BENCH_EDGES` | `200000` | Positive deterministic directed-edge count; it may not exceed nodes squared. |
| `DAWGS_INGEST_BENCH_CHANGE_PERCENT` | `1` | Integer percentage from 1 through 100; selection is evenly distributed and rounds down with a minimum of one record. |
| `DAWGS_INGEST_BENCH_BUCKETS` | `256,4096,65536` | Comma-separated unique power-of-two bucket counts. |
| `DAWGS_INGEST_BENCH_CLUSTER` | `false,true` | Comma-separated unique clustering modes. |

Every path receives the same generated logical records. The current `BatchOperation` path uses a batch size of 2,000 as
the baseline. `Driver.Ingest` runs every requested bucket count with natural indexes, optional
`ClusterAfterIngest`, and additive benchmark-only covering indexes. The covering indexes are created transactionally on
only the ephemeral child partitions and disappear when those partitions are dropped; they are not a production schema
recommendation.

Before accepting a seed or measured result, the benchmark streams every logical node and edge in deterministic order
and compares a length-framed canonical checksum plus each exact record. Validation includes every node `objectid`, the
complete node kind set, every directed `(start objectid, kind, end objectid)` tuple, and all properties including nested
values. It deliberately excludes database IDs and ingest-only hash/source columns, so a graph written exclusively by
`BatchOperation` remains a valid baseline even though those metadata columns are null.

The five scenarios are:

- `fresh_insert`: write the complete dataset into an empty graph.
- `dense_full_replay`: seed the complete dataset, then replay every full record unchanged.
- `dense_one_percent_change`: seed, then send every full record with the configured percentage changed.
- `partial_merge_noop`: seed, then send strict partial records whose additive merge changes no logical value; these are
  intentional database updates after client hash mismatches.
- `sparse_change`: seed, then send only the deterministically selected changed records.

Go reports elapsed time, `entities/s`, `allocs/op`, `B/op`, and sampled `peak-heap-B/op`. All paths report final
`table-B/op` and `index-B/op`. POC cases additionally report narrow `identity-rows/op`, `hash-matches/op`,
`staged-mutations/op`, `committed-mutations/op`, `spool-B/op`, and `cluster-ns/op`. When the connected role and server
permit it, `pg_current_wal_lsn` with `pg_wal_lsn_diff` supplies `wal-B/op`; unavailable WAL inspection is logged and does
not fail the benchmark. The two WAL snapshots sit immediately outside the measured call, after heap-sampler setup and
before validation. PostgreSQL's WAL position is cluster-global, so concurrent activity on the same cluster is included
in the delta; isolate the benchmark cluster for a clean comparison. Each POC case logs representative narrow node and
edge plans from `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` to the test output without creating files.

Without `CONNECTION_STRING`, ordinary tests and a manual-tag compile still work; the live benchmark is skipped. A
non-PostgreSQL `CONNECTION_STRING` is skipped in the same way.

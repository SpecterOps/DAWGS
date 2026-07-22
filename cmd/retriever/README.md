# retriever

`retriever` dumps and loads live Dawgs graph databases as manifest-based
collections of compact JSONL fragments.

The v1 collection format is intended for idle databases. Dumps are deterministic
by entity ID order and cap each graph scan at the entity counts observed when
counting starts, but they do not provide a transactional cross-fragment snapshot.

The collection manifest format is `retriever-jsonl-collection-v1`. Node and
relationship fragments contain one JSON object per line and use paths such as
`graphs/default/nodes-000001.jsonl.zst` and
`graphs/default/edges-000001.jsonl.zst`. The manifest identifies each file's
phase, record count, compressed and uncompressed sizes, and SHA-256 checksum.
The writer always terminates records with a newline; the loader also accepts a
final record without one. The loader starts with a 64 KiB scan buffer, grows it
only when needed, and rejects JSONL lines larger than 10 MiB.
New dumps default to zstd level 3. `-compression` accepts `zstd`, `gzip`, or
`none`; higher zstd levels remain available through `-zstd-level` for operators
who have measured an artifact-size benefit worth the additional workspace.

## Dump

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub none \
  -compression zstd \
  -zstd-level 3 \
  -shard-size 100000
```

Use repeated `-graph` flags to dump multiple named graphs. For PostgreSQL,
`-all-graphs` discovers graph names from Dawgs' `graph` metadata table and
validates that expected node and edge partitions exist. For Neo4j, `-all-graphs`
means the selected Neo4j database only.

Existing non-empty output directories are refused unless `-force` is supplied.
The manifest is written last as `manifest.json`; if a dump fails before that
point, the directory is intentionally left for inspection without a success
manifest.

Every database read is an ascending keyset query with a server-side
`-batch-size` limit. Retriever also rejects record B+1 at the cursor boundary,
checks every ID for strict increase, and processes records directly from the
cursor instead of retaining a result-sized slice. Dump, verify, and bench use
this same bounded scan path.

Interrupted dumps can resume from the last atomically committed fragment by
repeating the original command with `-resume`:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub none \
  -compression zstd \
  -zstd-level 3 \
  -shard-size 100000 \
  -batch-size 10000 \
  -resume
```

The hidden checkpoint binds the driver, ordered graph targets, batch and shard
sizes, compression settings, scrub rules/configuration, and a SHA-256 identity
of the salt without storing the salt. Resume verifies every committed fragment,
rejects unexpected files, recounts the source, reconstructs compact metrics
state, and continues after the last committed source ID. `-resume` and `-force`
are mutually exclusive. The source must remain quiescent for the entire original
and resumed dump: count changes are detected, but a same-count property or
topology replacement cannot be identified without a database snapshot token.

New dumps include a `retriever-metrics-v1` manifest section with graph metrics
computed from the same node and relationship streams written to the fragments.
The metrics include entity counts, kind histograms, degree histograms, endpoint
kind-shape histograms, and a canonical SHA-256 fingerprint. They intentionally
exclude IDs, property keys, property values, source identifiers, examples, and
sampled paths.

Rows inserted after the initial graph count are ignored once the counted entity
total has been scanned. Deletes or other concurrent source mutations can still
make the final dumped counts differ from the initial counts, in which case dump
fails rather than writing a misleading manifest.

Dump progress is emitted with `log/slog` on stderr. Notices mark output
directory preparation, graph counting, node and relationship phase boundaries,
periodic entity progress, checkpoint publication, manifest writing, and
completion. Phase and periodic progress includes `heap_alloc_bytes`,
`heap_inuse_bytes`, `sys_bytes`, `gc_count`, and `rss_bytes`; routine telemetry
does not force a garbage collection. Library `ProgressEvent` callbacks receive
the same memory fields, plus fragment pass and compressed/decompressed byte
counters where applicable.

`GOMEMLIMIT` is useful defense in depth after choosing a measured batch size;
it is not a substitute for the server and client cursor bounds.

## Runtime Profiling

The long-running `dump`, `load`, `verify`, and `bench` commands can expose Go's
standard pprof HTTP endpoints with `-pprof-listen`. Profiling is disabled when
the flag is omitted. For example:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub full \
  -salt "$RETRIEVER_SCRUB_SALT" \
  -pprof-listen 127.0.0.1:6060
```

The command writes the active endpoint to stderr after the listener starts. A
heap profile can then be captured and inspected while the command is running:

```bash
curl -o retriever-heap.pb.gz \
  'http://127.0.0.1:6060/debug/pprof/heap?gc=1'
go tool pprof -http=127.0.0.1:0 retriever-heap.pb.gz
```

The `gc=1` query runs a garbage collection before capturing the heap, making the
profile useful for distinguishing live retained state from allocation churn.
Use `/debug/pprof/allocs` to inspect cumulative allocations and
`/debug/pprof/goroutine?debug=1` for a text goroutine dump. Captures from two
phases can be compared with `go tool pprof -base earlier.pb.gz later.pb.gz`.

The pprof server has no authentication and accepts loopback listen addresses
only. Its dedicated handler deliberately does not register the command-line
endpoint because arguments may contain connection strings, salts, or key paths.
The listener shuts down when the Retriever command exits, and failure to bind
the requested address fails the command before database work starts. Treat all
captured profiles as sensitive operational artifacts.

## Encrypted Archives

Generate a recipient key pair before creating encrypted archives:

```bash
retriever keygen \
  -private retriever-private.key \
  -public retriever-public.key
```

The private key is written as an unencrypted JSON key envelope with restrictive
file permissions. Store it separately from archives and treat it as sensitive.
Key generation refuses to replace existing key files.

Pass `-archive-out` and `-recipient` to create a single encrypted TAR archive
after a dump succeeds:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub full \
  -salt "$RETRIEVER_SCRUB_SALT" \
  -archive-out ./dump.tar.pq \
  -recipient ./retriever-public.key
```

The archive layer uses an uncompressed TAR stream encrypted with HPKE using
ML-KEM-1024, HKDF-SHA512, and AES-256-GCM. Fragment compression inside the dump
directory remains controlled by `-compression`; the archive itself is not
compressed. If archive creation fails, the command fails and removes partial
archive output while leaving the completed dump directory for inspection.

Unpack an encrypted archive with the private key:

```bash
retriever unpack \
  -archive ./dump.tar.pq \
  -identity ./retriever-private.key \
  -out ./dumpdir
```

Existing non-empty unpack output directories are refused unless `-force` is
supplied. Unpacked collections retain the normal `manifest.json` and fragment
layout and can be loaded with `retriever load -in ./dumpdir`. Before promoting
the staged output directory, unpack validates the manifest, expected paths,
compressed sizes and checksums, and the absence of unexpected files. Unpack does
not reopen every extracted fragment for a standalone checksum pass: it hashes
the decrypted TAR payload while writing it. Unpack does not decompress
fragments or validate JSONL records, source IDs, or record counts; that semantic
validation occurs when the collection is passed to `retriever load`.

## Scrubbed Dumps

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./scrubbed-dump \
  -graph default \
  -scrub full \
  -salt "$RETRIEVER_SCRUB_SALT"
```

`-scrub full` fails closed unless a salt is supplied by `-salt` or
`RETRIEVER_SCRUB_SALT`. The legacy `RETRIEVR_SCRUB_SALT` name is accepted as a
fallback for existing scripts. Scrubbing preserves topology and source database
IDs while deterministically transforming sensitive property values. Action
counts are recorded per file, per graph, and globally in the manifest.
Pseudonyms are derived directly from the stable salt, so full scrub does not
retain a graph-wide identifier registry or perform a node observation pre-pass.
Scrub state is isolated per selected graph.

Classifier and graph-identifier settings can be overridden with `-config`; see
`../../retriever/defaults.toml` for the supported TOML shape.

## Load

```bash
retriever load \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir
```

Encrypted archives produced by `dump -archive-out` can be loaded directly:

```bash
retriever load \
  -connection "$CONNECTION_STRING" \
  -archive ./dump.tar.pq \
  -identity ./retriever-private.key
```

Load reads and validates `manifest.json`, then verifies every fragment checksum,
JSONL record, and record count before performing database writes. It asserts
destination graph schemas from the manifest metadata and loads all nodes before
relationships. Graph names from the dump are preserved; load does not support
overriding graph names. Direct archive loads decrypt and perform integrity-only
unpack validation in a temporary collection directory, then run the same
semantic preflight as directory loads before schema assertion or database
writes.

`-batch-size` bounds both node and relationship writes. PostgreSQL uses a
correlated bulk insert and Neo4j uses grouped `UNWIND` creates; generated node
IDs are returned in stable input-ordinal order before relationship endpoints
are resolved. Retriever-produced decimal source IDs use a compact numeric map,
while arbitrary valid archive IDs retain a compatibility fallback.

The preflight combines compressed byte counting and SHA-256 calculation with
JSONL decoding in one read of each fragment. Load then reopens and decodes the
fragments while writing to the database. This intentional second decode ensures
that every fragment is semantically valid before any database mutation. Measure
the complete preflight-plus-decode fragment path with:

```bash
go test ./retriever -run '^$' -bench '^BenchmarkLoadFragmentPath$' -benchmem
```

Load refuses to write into target graphs that already contain nodes or
relationships; clear the destination graph before restoring a collection.

Load progress is emitted with `log/slog` on stderr. Notices mark manifest
reading, checksum verification, schema assertion, graph boundaries, node and
relationship phase boundaries, periodic entity progress, and completion.

Pass `-verify-metrics` to scan the loaded destination graph after load and
compare it against the metrics stored in the manifest:

```bash
retriever load \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir \
  -verify-metrics
```

Metrics verification adds a full post-load node and relationship scan.

## Verify

```bash
retriever verify \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir
```

Verification reads expected metrics from `manifest.json`, computes actual
metrics from the destination graph database, and compares them strictly. A
successful run prints the verified graph, node, and relationship counts. A
mismatch exits non-zero and prints deterministic differences, for example:

```text
retriever: graph metrics mismatch:
  graph "default" node_count: expected 884868, actual 884867
```

Use standalone verification when the load already happened or when you want the
proof step to be a separate operational checkpoint. Older dumps without a
metrics section cannot be verified with this command.

## Bench

```bash
retriever bench \
  -connection "$CONNECTION_STRING" \
  -graph default \
  -workers 1 \
  -batch-size 10000 \
  -sample-size 1000000
```

Benchmark mode performs read-only scans and reports node and relationship
throughput. By default, each phase scans at most 1,000,000 nodes or
relationships so large graphs do not require a full read to produce a throughput
estimate. Pass `-sample-size 0` to scan the full graph. Use `-all-graphs` to
benchmark every graph discoverable by the selected driver, mirroring the dump
command's discovery behavior. Use `-workers` to compare concurrent batch
processing counts; database keyset scans stay sequential, while JSON
encode/compression work runs across the requested workers. The benchmark keeps
database read timing separate from optional JSON encode/compression timing:

```bash
retriever bench -connection "$CONNECTION_STRING" -graph default -compression zstd -json
```

Benchmark progress is emitted with `log/slog` on stderr. Notices mark benchmark
start, graph counting, each worker run, node and edge phase boundaries, and
completion. Text or JSON benchmark reports remain on stdout.

## Testing Policy

Unit tests focus on collection format validation, compression/checksum behavior,
scrub transformations, schema planning, edge resolution, CLI flag validation,
and other pure helpers. Full database dump/load behavior is covered by
integration tests for the backend selected by `CONNECTION_STRING`. The round-trip
case crosses scan-batch and shard boundaries and verifies restored properties and
topology. `BenchmarkLoadFragmentPath` measures preflight plus the second fragment
decode used during database loading.

### Retained-heap baseline and harness

The incident baseline from the 1,845,833-node/44-million-edge run was 5.26 GB
forced-GC `HeapAlloc` in nodes and 16.70 GB early in edges, with 4.41 GB and
14.88 GB respectively retained by graph-wide result materialization. The last
controlled sample reached 27.31 GB `HeapAlloc` and 29.68 GB MaxRSS. The compact
metrics baseline was 401.7 MiB, the scrub registry baseline was 294.6 MiB, and
a level-11 zstd writer retained about 51 MiB versus 18.25 MiB at level 3.

Run the retained-heap harness with one measured construction per case:

```bash
go test ./retriever -run '^$' \
  -bench '^BenchmarkRetainedHeap' \
  -benchmem -benchtime=1x -count=1

go test ./retriever -run '^$' \
  -bench '^(BenchmarkEdgeMetricsAllocation|BenchmarkEdgeScrubAllocation)$' \
  -benchmem -benchtime=100000x -count=1
```

The 2026-07-21 local post-change sample reported 112,591,760 retained bytes for
metrics at 1,845,833 nodes (about 73% below the 401.7 MiB baseline), 178,152
retained bytes after pseudonymizing the same number of unique scrub identifiers,
and 0 B/op with 0 allocs/op on the warmed steady-state edge metrics path. The
hydration cases at B, 10B, and 100B remain in the harness to make raw result
materialization growth visible; `BenchmarkScanEntityBatchesFixedBatch` verifies
the production scan while holding B constant. Numbers are machine-specific, so
save benchmark output with the backend, batch, scrub, metrics, and compression
settings when evaluating the 2 GiB HeapAlloc/4 GiB RSS scale gate.

Database validation requires separate runs for both schemes:

```bash
CONNECTION_STRING='postgresql://...' make test_all
CONNECTION_STRING='neo4j://...' make test_all
```

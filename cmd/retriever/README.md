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
final record without one. The loader rejects JSONL lines larger than 10 MiB.

## Dump

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub none \
  -compression zstd \
  -zstd-level 11 \
  -parquet \
  -shard-size 100000
```

Use repeated `-graph` flags to dump multiple named graphs. For PostgreSQL,
`-all-graphs` discovers graph names from Dawgs' `graph` metadata table and
validates that expected node and edge partitions exist. For Neo4j, `-all-graphs`
means the selected Neo4j database only.

Existing non-empty output directories are refused unless `-force` is supplied.
For JSONL-only dumps, the manifest is written last as `manifest.json`. With a
Parquet sidecar, its metadata precedes `manifest.json` and `parquet/_SUCCESS`
is written last. Failed dumps intentionally leave completed fragments for
inspection without the final applicable success boundary.

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
directory preparation, graph counting, scrub pre-pass work, node and relationship
phase boundaries, periodic entity progress, manifest writing, and completion.

### Parquet sidecar

Pass `-parquet` to write a strict Parquet sidecar in addition to the normal
compressed JSONL collection. JSONL remains mandatory and keeps its existing
paths, bytes, manifest, load behavior, and checksums.

Parquet fragments use matching logical shard boundaries under
`parquet/graphs/<graph>/`. Node rows contain `id`, `kinds`, and JSON-annotated
`properties`. Relationship rows contain `id`, `start_id`, `end_id`, `kind`, and
JSON-annotated `properties`; unlike JSONL v1, the Parquet sidecar retains the
source relationship ID. Columns use Zstandard compression.

The sidecar metadata is `parquet/manifest.json` with format
`retriever-parquet-export-v1`. Publication writes that metadata first, the
existing JSONL `manifest.json` second, and `parquet/_SUCCESS` last. A requested
Parquet failure fails the dump, and `_SUCCESS` is absent unless both outputs
and their metadata completed.

Load and verify continue to consume the JSONL collection. Encrypted archive
creation also retains its existing behavior and packages the JSONL manifest
and files; it does not include the optional Parquet sidecar.

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
not decompress fragments or validate JSONL records, source IDs, or record
counts; that semantic validation occurs when the collection is passed to
`retriever load`.

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

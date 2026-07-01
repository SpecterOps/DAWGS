# retriever

`retriever` dumps and loads live Dawgs graph databases as manifest-based
collections of compressed OpenGraph-derived JSON Lines files.

The v1 JSONL collection format is intended for idle databases. Dumps are
deterministic by entity ID order and cap each graph scan at the entity counts
observed when counting starts, but they do not provide a transactional
cross-phase snapshot.

## Dump

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub none \
  -compression zstd \
  -zstd-level 11
```

Use repeated `-graph` flags to dump multiple named graphs. For PostgreSQL,
`-all-graphs` discovers graph names from Dawgs' `graph` metadata table and
validates that expected node and edge partitions exist. For Neo4j, `-all-graphs`
means the selected Neo4j database only.

Existing non-empty output directories are refused unless `-force` is supplied.
The manifest is written last as `manifest.json`; if a dump fails before that
point, the directory is intentionally left for inspection without a success
manifest.

Each graph is written under its own directory with one node file and one edge
file:

```text
manifest.json
graphs/default/nodes.jsonl.zst
graphs/default/edges.jsonl.zst
```

Each JSONL row is shaped like the current retriever entity item. Node rows have
`id`, `kinds`, and `properties`; edge rows have `start_id`, `end_id`, `kind`,
and `properties`. Property values remain inside `properties`, including
`objectid`, so downstream analytics jobs can decorate Parquet projections
without changing the retriever dump contract.

Pass `-parquet` to derive analytics-friendly Parquet files after the JSONL dump
succeeds:

```bash
retriever dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -compression zstd \
  -parquet
```

Parquet conversion uses the embedded DuckDB Go client, so builds need a
CGO-capable toolchain. The derived files are written next to the JSONL files:

```text
graphs/default/nodes.parquet
graphs/default/edges.parquet
```

`nodes.parquet` contains `id`, `objectid`, `kinds`, and `properties`.
`edges.parquet` contains `id`, `start_id`, `end_id`, `start_objectid`,
`end_objectid`, `kind`, and `properties`. Edge IDs are generated as
`start_objectid || '_' || kind || '_' || end_objectid`; endpoint object IDs are
derived by joining edges back to the node projection. `retriever load`
continues to load from the manifest-listed JSONL files.

New dumps include a `retriever-metrics-v1` manifest section with graph metrics
computed from the same node and relationship streams written to the JSONL files.
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
ML-KEM-1024, HKDF-SHA512, and AES-256-GCM. JSONL compression inside the dump
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
supplied. Unpacked collections retain the normal `manifest.json` and JSONL
layout and can be loaded with `retriever load -in ./dumpdir`.

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
`defaults.toml` for the supported TOML shape.

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

Load reads and validates `manifest.json`, verifies every JSONL file checksum and
record count in a separate pass, asserts destination graph schemas from the
manifest metadata, and then loads all nodes before relationships. Graph names
from the dump are preserved; load does not support overriding graph names.
Archive loads decrypt and validate into a temporary collection directory before
opening the database. Manifests from the previous compact-fragment format are
recognized as legacy inputs.

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
estimate. Pass `-sample-size 0` to scan the full graph. Parallel worker counts
above `1` are currently rejected until the benchmark has a safe partitioned read
strategy. The benchmark keeps database read timing separate from optional JSON
encode/compression timing:

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
integration tests rather than heavy mocks of Dawgs database interfaces. This
keeps tests focused on durable behavior instead of line coverage for
orchestration code.

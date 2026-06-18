# retrievr

`retrievr` dumps and loads live Dawgs graph databases as manifest-based
collections of compact OpenGraph-derived JSON fragments.

The v1 collection format is intended for idle databases. Dumps are deterministic
by entity ID order and bound each graph scan to the highest entity IDs observed
when counting starts, but they do not provide a transactional cross-fragment
snapshot.

## Dump

```bash
retrievr dump \
  -connection "$CONNECTION_STRING" \
  -out ./dumpdir \
  -graph default \
  -scrub none \
  -compression zstd \
  -zstd-level 11 \
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

Rows inserted after the initial graph count are ignored by the bounded scan.
Deletes or other concurrent source mutations can still make the final dumped
counts differ from the initial counts, in which case dump fails rather than
writing a misleading manifest.

Dump progress is emitted with `log/slog` on stderr. Notices mark output
directory preparation, graph counting, scrub pre-pass work, node and relationship
phase boundaries, periodic entity progress, manifest writing, and completion.

## Scrubbed Dumps

```bash
retrievr dump \
  -connection "$CONNECTION_STRING" \
  -out ./scrubbed-dump \
  -graph default \
  -scrub full \
  -salt "$RETRIEVR_SCRUB_SALT"
```

`-scrub full` fails closed unless a salt is supplied by `-salt` or
`RETRIEVR_SCRUB_SALT`. Scrubbing preserves topology and source database IDs while
deterministically transforming sensitive property values. Action counts are
recorded per file, per graph, and globally in the manifest.

Classifier and graph-identifier settings can be overridden with `-config`; see
`defaults.toml` for the supported TOML shape.

## Load

```bash
retrievr load \
  -connection "$CONNECTION_STRING" \
  -in ./dumpdir
```

Load reads and validates `manifest.json`, verifies every fragment checksum in a
separate pass, asserts destination graph schemas from the manifest metadata, and
then loads all nodes before relationships. Graph names from the dump are
preserved; load does not support overriding graph names.

Load progress is emitted with `log/slog` on stderr. Notices mark manifest
reading, checksum verification, schema assertion, graph boundaries, node and
relationship phase boundaries, periodic entity progress, and completion.

## Bench

```bash
retrievr bench \
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
retrievr bench -connection "$CONNECTION_STRING" -graph default -compression zstd -json
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

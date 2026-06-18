# retrievr

`retrievr` dumps and loads live Dawgs graph databases as manifest-based
collections of compact OpenGraph-derived JSON fragments.

The v1 collection format is intended for idle databases. Dumps are deterministic
by entity ID order, but they do not provide a transactional cross-fragment
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

## Bench

```bash
retrievr bench \
  -connection "$CONNECTION_STRING" \
  -graph default \
  -workers 1,2,4,8 \
  -batch-size 10000
```

Benchmark mode performs read-only scans and reports node and relationship
throughput. It keeps database read timing separate from optional JSON
encode/compression timing:

```bash
retrievr bench -connection "$CONNECTION_STRING" -graph default -compression zstd -json
```

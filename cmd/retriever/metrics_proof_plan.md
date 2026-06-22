# Retriever Graph Metrics Proof Plan

## Goal

Provide a way to prove that the sanitized dataset produced by `retriever dump`
and restored by `retriever load` preserves the same graph metrics without
including attribution.

The proof should compare metrics from the actual dumped artifact against
metrics computed from the loaded destination graph.

## Non-Attribution Requirements

The proof artifact must not include:

- Original node IDs.
- Original relationship IDs.
- Property keys.
- Property values.
- Source identifiers.
- Example paths, example entities, or other sampled graph data.

The metrics should describe the graph shape and schema-level counts only.

## Metrics To Capture

The initial metrics set should include:

- Node count.
- Relationship count.
- Node kind-set histogram.
- Relationship kind histogram.
- In-degree histogram.
- Out-degree histogram.
- Total-degree histogram.
- Optional endpoint-shape histogram:
  `start node kind-set + relationship kind + end node kind-set`.

These metrics are stable across sanitization and node ID remapping.

## Manifest Schema

Add a metrics section to the retriever manifest.

Example:

```json
{
  "metrics": {
    "version": "retriever-metrics-v1",
    "graphs": [
      {
        "name": "default",
        "node_count": 884868,
        "edge_count": 18622890,
        "node_kind_histogram": {},
        "edge_kind_histogram": {},
        "in_degree_histogram": {},
        "out_degree_histogram": {},
        "total_degree_histogram": {},
        "endpoint_kind_histogram": {},
        "fingerprint": "sha256:..."
      }
    ]
  }
}
```

The fingerprint should be SHA-256 over a canonical, sorted representation of
the non-attribution metrics. Timestamps and other unstable metadata must not be
included in the fingerprint.

## Dump Metrics

Dump-side metrics should be computed from the same entity stream that is written
to the dump artifact.

This avoids proving a separate live database scan that may not match the data
actually written if the source graph changes during the dump.

During node dump:

- Increment the dumped node count.
- Record the node kind-set histogram.
- Keep an in-memory `nodeID -> kind-set` map for topology metrics.

During relationship dump:

- Increment the dumped relationship count.
- Record the relationship kind histogram.
- Increment in-degree and out-degree maps.
- Optionally record endpoint-shape counts using the node kind-set map.

The manifest should then describe the sanitized dataset that was actually
written.

## Loaded Metrics

After load, compute the same metrics by scanning the destination graph.

This destination scan should not rely on original IDs because load may remap
node IDs. The comparison should be based only on counts, histograms, and the
canonical fingerprint.

## Verification Command

Add a standalone verification command:

```bash
retriever verify -connection "$CONNECTION_STRING" -in ./dump
```

This command should:

- Read expected metrics from the dump manifest.
- Compute actual metrics from the loaded destination graph.
- Compare each graph strictly.
- Return a non-zero exit status on mismatch.
- Print a compact human-readable diff.

Example failure:

```text
retriever: graph metrics mismatch for "default"
  node_count: expected 884868, actual 884867
  out_degree_histogram[0]: expected 1203, actual 1204
  endpoint_kind_histogram["User|MEMBER_OF|Group"]: expected 812, actual 811
```

## Optional Load-Time Verification

After the standalone command exists, add an explicit load flag:

```bash
retriever load -connection "$CONNECTION_STRING" -in ./dump -verify-metrics
```

Keep verification explicit at first because it adds a full post-load database
scan and can be expensive on large graphs.

## Progress Logging

Add `slog` notices around metrics collection and verification:

- Metrics collection started.
- Node metrics phase started.
- Node metrics phase completed.
- Relationship metrics phase started.
- Relationship metrics phase completed.
- Fingerprint computed.
- Verification started.
- Verification passed or failed.

Include graph name, processed counts, elapsed time, and entity throughput where
useful.

## Testing Plan

Focus tests on durable behavior instead of mocked orchestration-heavy flows.

Unit tests should cover:

- Metrics builder counts nodes, relationships, kinds, and degrees correctly.
- Zero-degree nodes are represented in degree histograms.
- Empty node kind sets and empty relationship kinds are normalized
  consistently.
- Fingerprints are stable regardless of input ordering.
- Properties do not affect metrics.
- Serialized metrics contain no IDs or property data.
- Comparator output includes useful, deterministic diffs.

Integration tests should cover:

- Dump writes metrics to the manifest.
- Load followed by verify succeeds for an unchanged round trip.
- Verify fails with a clear diff when the destination graph differs.

Integration tests should run only when `CONNECTION_STRING` is available.

## Implementation Sequence

1. Add pure metrics types, canonical serialization, fingerprinting, comparison,
   and unit tests.
2. Add a `metricsBuilder` that observes dumped nodes and relationships without
   retaining attribution in the finalized metrics.
3. Add optional manifest metrics validation so old manifests still load, while
   new manifests can carry `retriever-metrics-v1` data.
4. Wire the builder into the dump node and relationship streams so the manifest
   proves the artifact that was actually written.
5. Add a destination database metrics scanner using the existing ordered batch
   read helpers.
6. Add `retriever verify` to compare manifest metrics with destination graph
   metrics and return deterministic diffs.
7. Add explicit `retriever load -verify-metrics` support after standalone
   verification works.
8. Add `slog` progress notices around metrics collection, fingerprinting, and
   verification.
9. Update `cmd/retriever/README.md` with the proof workflow and performance
   cost.
10. Validate with `go test ./cmd/retriever` and a direct command build.

## Kind Name Decision

Node kind names and relationship kind names are included in the proof artifact
by default.

They are treated as schema-level metrics and make the proof operationally
useful.

If kind names are considered sensitive in some environments, add a future
option such as:

```bash
retriever dump -metrics-kind-mode hash
```

That option would hash kind names before writing metrics while preserving exact
comparison behavior.

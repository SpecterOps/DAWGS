# Library utilities and tools

Reachability, collection helpers, traversal patterns, and graph import/export.

[Documentation index](README.md) · [Project overview](../README.md)

## Reachability and containers

`algo.ReachabilityCache` retains complete reach sets for requested components in separate bounded inbound and outbound
caches. A traversal reuses complete cached results and visits each remaining component at most once, using one local
reach bitmap. Warming an ancestor does not prewarm intermediate components; querying a new component can require another
traversal. Cache capacity limits entries per direction (minimum one), rather than bitmap bytes. Reach results include
the origin and its strongly connected component; `DirectionBoth` computes weak connectivity without caching that result.

`cardinality.ThreadSafeDuplex` and `cardinality.ThreadSafeSimplex` synchronize every operation of their wrapped provider.
Binary operations (`Or`, `And`, `Xor`, `AndNot`) snapshot another thread-safe wrapper before locking the receiver, so
concurrent cross-wrapper operations, self-operations, and merges between wrapped HyperLogLog sketches complete without
deadlock or silent drops. A snapshot reflects the other wrapper only at the moment it is taken; each operation is atomic
for its receiver alone. Delegates passed to `Each` must not call back into the same wrapper.

Bitmap `And` and `AndNot` with a non-native Duplex operand (a thread-safe wrapper or a custom implementation) copy the
operand's values into a native bitmap and apply the native roaring operation. The receiver is never modified while it is
being iterated, so intersections and differences with such operands are exact on both bitmap widths; native bitmap
operands keep their direct native paths.

`container.PackedMap` keeps every key/value association across repeated compactions: merging pending entries with a
compacted bucket stores the merged values with the merged key set. `container.EFSet.Find` terminates without a match for
targets below the stored minimum and for empty sets instead of underflowing its unsigned binary-search bound; a miss
returns `(0, false)`.

## Traversal patterns

`traversal.NewPattern` chained expansions fetch each step's endpoint using that step's own direction, so patterns whose
steps change direction (for example `OutboundWithDepth(1, 1).InboundWithDepth(1, 1)` over `a→b←c`) match. Each
expansion's criteria still select the relationships entering or leaving the current node; only the fetched endpoint
follows the expansion being performed.

An optional following expansion (minimum depth 0) preserves the boundary segment instead of eager-fetching the step
twice: the preserved segment performs the optional expansion when the traversal visits it, so every optional-step match
reaches the terminal delegate exactly once, and the unmatched boundary segment is delegated once when the optional step
has no matches. The shared `TestTraversalPatternDirectionChange` and `TestTraversalPatternOptionalStep` integration
regressions run complete BreadthFirst pattern traversals on both backends, and the `traversal` package regressions pin
the observed fetch directions and exact callback cardinality.

## Import and export

`go run ./cmd/retriever` dumps and loads live Dawgs graph databases as
manifest-based collections of compressed JSONL fragments. It supports
PostgreSQL and Neo4j, uncompressed, gzip, and zstd fragments, bounded keyset
scans, resumable dump checkpoints, checksum validation before load, optional
deterministic property scrubbing, optional write-only Parquet sidecars for
analytical readers, and a read-throughput benchmark mode. It can also package
dumps as single HPKE/ML-KEM encrypted TAR archives. See
[cmd/retriever/README.md](../cmd/retriever/README.md) for dump, Parquet sidecar,
encrypted archive, load, scrubbed dump, metrics verification, and benchmark
examples.
Full scrub recursively classifies nested map properties, including maps inside arrays. Explicit
`preserve_keys` overrides preserve whole values. The `retriever-scrub-v3` policy records recursive container
plans as `scrub_nested` in top-level action counts and rejects resume checkpoints from earlier scrub rules.
Retriever JSONL loads and OpenGraph document imports preserve integer tokens as exact signed
64-bit values, including inside arrays and nested maps. Decimal and exponent tokens use
`float64` with ordinary binary floating-point rounding. Out-of-range integer tokens and
floating-point overflow are rejected before import writes; backend property-shape limits still apply.
`opengraph.ParseDocument` now returns `int64` for integer tokens instead of `float64`.
Collection fragment paths must be clean relative paths with slash separators. Checksum,
preflight and load reads resolve files beneath the collection root; escaping paths and
symlinks are rejected, while contained relative symlinks are supported for directory loads.
The same import/export functionality is available to library consumers from
`github.com/specterops/dawgs/retriever`; callers provide an already-open
`graph.Database`, and archive helpers support both path-based and stream-based
APIs. The package exposes CLI-matching default option constructors, structured
progress callbacks, manifest/metrics helpers, HPKE key envelope reader/writer
helpers, and typed errors for validation, compatibility, checksum, metrics, and
count mismatches.

## Local module development

For local development against a checkout, use a Go module replacement in the consuming project:

```go
replace github.com/specterops/dawgs => /path/to/dawgs
```

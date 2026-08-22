# Benchmark

Runs query scenarios against a real database and outputs markdown, JSON, or benchfmt timing data. Markdown reports include warm-up row counts, path-heavy scenarios can report distinct and duplicate returned path rows, and PostgreSQL explain capture includes translated SQL, plan text, and optimizer rule/lowering metadata in JSON output.

## Usage

```bash
# Default datasets (base, fixed_suffix_expansion_fanout, and traversal_shapes)
go run ./cmd/benchmark -connection "postgresql://dawgs:dawgs@localhost:5432/dawgs"

# Traversal shape dataset only
go run ./cmd/benchmark -connection "..." -dataset traversal_shapes

# Fixed-suffix expansion fanout dataset with PostgreSQL EXPLAIN diagnostics
go run ./cmd/benchmark -connection "..." -dataset fixed_suffix_expansion_fanout -json-output report.json -explain

# Local dataset (not committed to repo)
go run ./cmd/benchmark -connection "..." -dataset local/phantom

# Default + local dataset
go run ./cmd/benchmark -connection "..." -local-dataset local/phantom

# Neo4j
go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:password@localhost:7687"

# Explicit PostgreSQL v2 connection-local translation cache
go run ./cmd/benchmark -driver pg-v2 -connection "..." -iterations 10

# Cold and warm concurrent cache measurements (4 workers × 20 samples)
go run ./cmd/benchmark -driver pg-v2 -connection "..." -dataset traversal_shapes -pg-v2-min-conns 0 -pg-v2-max-conns 4 -workers 4 -warmup 0 -iterations 20
go run ./cmd/benchmark -driver pg-v2 -connection "..." -dataset traversal_shapes -pg-v2-min-conns 0 -pg-v2-max-conns 4 -workers 4 -warmup 2 -iterations 20

# Benchmark the guarded inline predecessor-DAG executor without enabling it in production
go run ./cmd/benchmark -driver pg-v2 -connection "..." -dataset traversal_shapes -pg-v2-shortest-path-executor 'ASP-I1-U-DAG+MAT-M0' -pg-plan-cache-mode auto -iterations 20

# Exercise one verified manifest-authorized query through the real V2 policy path
go run ./cmd/benchmark -driver pg-v2 -connection "..." -dataset traversal_shapes -pg-v2-traversal-policy-manifest .coverage/promotion.json -pg-v2-traversal-policy-generation 7 -pg-plan-cache-mode auto -iterations 20

# Derive a non-promotional exact SQL anchor for one provisional traversal-policy bucket
go run ./cmd/benchmark -driver pg-v2 -connection "..." -dataset traversal_shapes -pg-v2-traversal-policy-preflight-manifest .coverage/provisional.json -pg-v2-traversal-policy-preflight-output .coverage/policy-preflight.json

# Save to file
go run ./cmd/benchmark -connection "..." -output report.md

# Save markdown and JSON for quality baseline comparison
go run ./cmd/benchmark -connection "..." -output report.md -json-output report.json

# Emit benchfmt for benchstat
go run ./cmd/benchmark -connection "..." -format benchfmt -output report.bench
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-driver` | `pg` | Database driver (`pg`, `pg-v2`, `neo4j`) |
| `-connection` | | Connection string (or `CONNECTION_STRING` env) |
| `-iterations` | `10` | Timed iterations per scenario |
| `-warmup` | `1` | Untimed iterations per worker; use `0` to include cold-query cost |
| `-workers` | `1` | Concurrent workers per scenario; each contributes `-iterations` samples |
| `-pg-v2-cache-entries` | `64` | V2 translations retained per physical PostgreSQL connection |
| `-pg-v2-shared-shortest-path-template-entries` | `128` | Immutable shortest-path SQL templates shared across V2 physical connections; zero disables the L2 tier |
| `-pg-v2-shortest-path-executor` | | Benchmark-only qualified executor identity; production routing remains manifest-controlled |
| `-pg-v2-traversal-policy-manifest` | | A GraphBench-verified promotion manifest to install through `pg-v2`'s real `SetTraversalPolicy` path |
| `-pg-v2-traversal-policy-generation` | `1` | Nonzero traversal-policy generation used for cache identity in manifest policy mode |
| `-pg-v2-traversal-policy-preflight-manifest` | | Provisional one-query manifest used only to render the V2 candidate SQL anchor |
| `-pg-v2-traversal-policy-preflight-output` | | JSON destination for the non-promotional preflight record; required with the preflight manifest |
| `-pg-plan-cache-mode` | `auto` | Plan mode for forced or manifest-policy PostgreSQL shortest-path runs (`auto`, `force_custom_plan`, `force_generic_plan`) |
| `-pg-jit` | `true` | Enable PostgreSQL JIT transaction-locally during forced or manifest-policy shortest-path runs |
| `-pg-v2-min-conns` | `5` | V2 minimum physical PostgreSQL connections |
| `-pg-v2-max-conns` | `50` | V2 maximum physical PostgreSQL connections |
| `-explain` | `false` | Capture PostgreSQL JSON `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)` and translated SQL for Cypher scenarios |
| `-dataset` | | Run only this dataset |
| `-local-dataset` | | Add a local dataset to the default set |
| `-dataset-dir` | `integration/testdata` | Path to testdata directory |
| `-format` | `markdown` | Output format (`markdown`, `json`, `benchfmt`) |
| `-output` | stdout | Output file for selected format |
| `-json-output` | | JSON output file for baseline comparison |

Use `-format benchfmt` when comparing scenario timings with `benchstat`. Each timed scenario iteration is emitted as a separate `ns/op` sample so two benchmark runs can be compared directly.

`pg-v2` is benchmark-only opt-in selection for `drivers/pg/v2`; it does not register a connection-string driver scheme.
It constructs a matching v2 pool and driver directly, uses the default 64-entry cache per physical PostgreSQL connection,
and supports the same PostgreSQL EXPLAIN capture as `pg`.
Its JSON and Markdown reports also include query-text-free translation-cache, traversal-workspace, and prepared-statement
counters, structured PostgreSQL planning/execution timings, and configured pool limits. For shortest paths it also reports
query-text-free parse, cache/bind, translation, formatting, and dispatch totals plus shared-template L2 activity. Use a cold (`-warmup 0`) and warm (`-warmup 2`) run with the same worker count and
V2 pool configuration to measure cache effectiveness; do not compare their latency distributions without accounting for the
intentionally different warm-up state.

`-pg-v2-shortest-path-executor` is deliberately a tool-only comparison mode: it
retranslates at the benchmark boundary and bypasses V2's production policy
selection. `-pg-v2-traversal-policy-manifest` instead installs the supplied
document on the newly opened V2 driver, runs at Repeatable Read, and leaves
translation, SQL-anchor verification, cache lookup, and fallback selection to
the driver. The report identifies this as `production_policy` and records only
the policy generation and manifest SHA-256, never its raw contents.

Before using manifest policy mode, verify the complete evidence closure with
`go run ./cmd/graphbench -promotion-manifest <path>`. The driver fails closed
on an invalid candidate, manifest, query set, snapshot, or SQL anchor, but the
benchmark command does not treat a digest-shaped document as promotion proof.
The current policy contract authorizes exactly one query digest, so policy mode
requires `-dataset` and runs exactly one matching parameterized Cypher
scenario. `-explain` is intentionally unavailable in this mode because the
standalone explainer would otherwise bypass the live policy gate and report a
different statement.

When a formal manifest needs its candidate SQL anchor, use
`-pg-v2-traversal-policy-preflight-manifest` with the same selected benchmark
dataset. Its provisional manifest supplies the candidate, selector, caps, and
single query bucket; the command loads the graph and renders that exact V2
translation, then writes only the query and SQL SHA-256 values plus translation
metadata. It does not install a traversal policy, execute the candidate SQL,
or create verification evidence. Copy the SQL hash into the provisional
manifest and recapture the complete GraphBench evidence closure before using
the resulting document with policy mode. The output path must be new: preflight
refuses to overwrite either the provisional manifest or an earlier record.

The committed default datasets are `base`, `fixed_suffix_expansion_fanout`, and
`traversal_shapes`. `traversal_shapes` covers chain, fanout, bounded cycle,
disconnected, edge-kind-selective, and multi-path shortest-path traversal
shapes. Scenarios with declared expected row counts fail before reporting
timings if a query returns the wrong result shape.

## Example: Neo4j on local/phantom

```
$ go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:testpassword@localhost:7687" -dataset local/phantom
```

| Query | Dataset | Rows | Distinct Rows | Duplicate Rows | Median | P95 | Max | Explain |
|-------|---------|-----:|--------------:|---------------:|-------:|----:|----:|:--------|
| Match Nodes | local/phantom | 1000 | - | - | 1.4ms | 2.3ms | 2.3ms | - |
| Match Edges | local/phantom | 2000 | - | - | 1.6ms | 1.9ms | 1.9ms | - |

## Example: PG on local/phantom

```
$ export CONNECTION_STRING="postgresql://dawgs:dawgs@localhost:5432/dawgs"
$ go run ./cmd/benchmark -dataset local/phantom
```

| Query | Dataset | Rows | Distinct Rows | Duplicate Rows | Median | P95 | Max | Explain |
|-------|---------|-----:|--------------:|---------------:|-------:|----:|----:|:--------|
| Match Nodes | local/phantom | 1000 | - | - | 2.0ms | 6.5ms | 6.5ms | - |
| Match Edges | local/phantom | 2000 | - | - | 464ms | 604ms | 604ms | - |

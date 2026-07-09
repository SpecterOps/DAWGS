# Benchmark

Runs query scenarios against a real database and outputs markdown, JSON, or benchfmt timing data. Markdown reports include warm-up row counts, path-heavy scenarios can report distinct and duplicate returned path rows, and PostgreSQL explain capture includes translated SQL, plan text, and optimizer rule/lowering metadata in JSON output.

## Usage

```bash
# Default datasets (base, adcs_fanout, and traversal_shapes)
go run ./cmd/benchmark -connection "postgresql://dawgs:dawgs@localhost:5432/dawgs"

# Traversal shape dataset only
go run ./cmd/benchmark -connection "..." -dataset traversal_shapes

# ADCS fanout dataset with PostgreSQL EXPLAIN diagnostics
go run ./cmd/benchmark -connection "..." -dataset adcs_fanout -json-output report.json -explain

# Local dataset (not committed to repo)
go run ./cmd/benchmark -connection "..." -dataset local/phantom

# Default + local dataset
go run ./cmd/benchmark -connection "..." -local-dataset local/phantom

# Neo4j
go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:password@localhost:7687"

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
| `-driver` | `pg` | Database driver (`pg`, `neo4j`) |
| `-connection` | | Connection string (or `CONNECTION_STRING` env) |
| `-iterations` | `10` | Timed iterations per scenario |
| `-explain` | `false` | Capture PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` and translated SQL for Cypher scenarios in JSON output |
| `-dataset` | | Run only this dataset |
| `-local-dataset` | | Add a local dataset to the default set |
| `-dataset-dir` | `integration/testdata` | Path to testdata directory |
| `-format` | `markdown` | Output format (`markdown`, `json`, `benchfmt`) |
| `-output` | stdout | Output file for selected format |
| `-json-output` | | JSON output file for baseline comparison |

Use `-format benchfmt` when comparing scenario timings with `benchstat`. Each timed scenario iteration is emitted as a separate `ns/op` sample so two benchmark runs can be compared directly.

The committed default datasets are `base`, `adcs_fanout`, and `traversal_shapes`. `traversal_shapes` covers chain,
fanout, bounded cycle, disconnected, edge-kind-selective, and multi-path shortest-path traversal shapes. Scenarios with
declared expected row counts fail before reporting timings if a query returns the wrong result shape.

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

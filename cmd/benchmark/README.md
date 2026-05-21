# Benchmark

Runs query scenarios against a real database and outputs a markdown timing table with warm-up row counts.

## Usage

```bash
# Default datasets (base and adcs_fanout)
go run ./cmd/benchmark -connection "postgresql://dawgs:dawgs@localhost:5432/dawgs"

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
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-driver` | `pg` | Database driver (`pg`, `neo4j`) |
| `-connection` | | Connection string (or `CONNECTION_STRING` env) |
| `-iterations` | `10` | Timed iterations per scenario |
| `-dataset` | | Run only this dataset |
| `-local-dataset` | | Add a local dataset to the default set |
| `-dataset-dir` | `integration/testdata` | Path to testdata directory |
| `-output` | stdout | Markdown output file |
| `-json-output` | | JSON output file for baseline comparison |

## Example: Neo4j on local/phantom

```
$ go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:testpassword@localhost:7687" -dataset local/phantom
```

| Query | Dataset | Rows | Median | P95 | Max |
|-------|---------|-----:|-------:|----:|----:|
| Match Nodes | local/phantom | 1000 | 1.4ms | 2.3ms | 2.3ms |
| Match Edges | local/phantom | 2000 | 1.6ms | 1.9ms | 1.9ms |

## Example: PG on local/phantom

```
$ export CONNECTION_STRING="postgresql://dawgs:dawgs@localhost:5432/dawgs"
$ go run ./cmd/benchmark -dataset local/phantom
```

| Query | Dataset | Rows | Median | P95 | Max |
|-------|---------|-----:|-------:|----:|----:|
| Match Nodes | local/phantom | 1000 | 2.0ms | 6.5ms | 6.5ms |
| Match Edges | local/phantom | 2000 | 464ms | 604ms | 604ms |

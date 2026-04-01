# Benchmark

Runs query scenarios against a real database and outputs a markdown timing table.

## Usage

```bash
# Default dataset (base)
go run ./cmd/benchmark -connection "postgresql://dawgs:dawgs@localhost:5432/dawgs"

# Local dataset (not committed to repo)
go run ./cmd/benchmark -connection "..." -dataset local/phantom

# Default + local dataset
go run ./cmd/benchmark -connection "..." -local-dataset local/phantom

# Neo4j
go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:password@localhost:7687"

# Save to file
go run ./cmd/benchmark -connection "..." -output report.md
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-driver` | `pg` | Database driver (`pg`, `neo4j`) |
| `-connection` | | Connection string (or `PG_CONNECTION_STRING` env) |
| `-iterations` | `10` | Timed iterations per scenario |
| `-dataset` | | Run only this dataset |
| `-local-dataset` | | Add a local dataset to the default set |
| `-dataset-dir` | `integration/testdata` | Path to testdata directory |
| `-output` | stdout | Markdown output file |

## Example: Neo4j on local/phantom

```
$ go run ./cmd/benchmark -driver neo4j -connection "neo4j://neo4j:testpassword@localhost:7687" -dataset local/phantom
```

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | local/phantom | 1.4ms | 2.3ms | 2.3ms |
| Match Edges | local/phantom | 1.6ms | 1.9ms | 1.9ms |
| Filter By Kind / User | local/phantom | 2.0ms | 2.6ms | 2.6ms |
| Filter By Kind / Group | local/phantom | 2.1ms | 2.3ms | 2.3ms |
| Filter By Kind / Computer | local/phantom | 1.6ms | 2.0ms | 2.0ms |
| Traversal Depth / depth 1 | local/phantom | 1.4ms | 2.1ms | 2.1ms |
| Traversal Depth / depth 2 | local/phantom | 1.6ms | 1.9ms | 1.9ms |
| Traversal Depth / depth 3 | local/phantom | 2.5ms | 3.3ms | 3.3ms |
| Edge Kind Traversal / MemberOf | local/phantom | 1.2ms | 1.4ms | 1.4ms |
| Edge Kind Traversal / GenericAll | local/phantom | 1.1ms | 1.5ms | 1.5ms |
| Edge Kind Traversal / HasSession | local/phantom | 1.1ms | 1.4ms | 1.4ms |
| Shortest Paths / 41 -> 587 | local/phantom | 1.5ms | 1.9ms | 1.9ms |

## Example: PG on local/phantom

```
$ export PG_CONNECTION_STRING="postgresql://dawgs:dawgs@localhost:5432/dawgs"
$ go run ./cmd/benchmark -dataset local/phantom
```

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | local/phantom | 2.0ms | 6.5ms | 6.5ms |
| Match Edges | local/phantom | 464ms | 604ms | 604ms |
| Filter By Kind / User | local/phantom | 4.5ms | 18.3ms | 18.3ms |
| Filter By Kind / Group | local/phantom | 6.2ms | 28.8ms | 28.8ms |
| Filter By Kind / Computer | local/phantom | 1.1ms | 5.5ms | 5.5ms |
| Traversal Depth / depth 1 | local/phantom | 596ms | 636ms | 636ms |
| Traversal Depth / depth 2 | local/phantom | 639ms | 660ms | 660ms |
| Traversal Depth / depth 3 | local/phantom | 726ms | 745ms | 745ms |
| Edge Kind Traversal / MemberOf | local/phantom | 602ms | 627ms | 627ms |
| Edge Kind Traversal / GenericAll | local/phantom | 676ms | 791ms | 791ms |
| Edge Kind Traversal / HasSession | local/phantom | 682ms | 778ms | 778ms |
| Shortest Paths / 41 -> 587 | local/phantom | 708ms | 731ms | 731ms |

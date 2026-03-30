# Benchmark

Runs query scenarios against a real database and outputs a markdown timing table.

## Usage

```bash
# All small datasets
go run ./cmd/benchmark -connection "postgresql://dawgs:dawgs@localhost:5432/dawgs"

# Single dataset
go run ./cmd/benchmark -connection "..." -dataset diamond

# Local dataset (not committed to repo)
go run ./cmd/benchmark -connection "..." -dataset local/phantom

# All small datasets + local dataset
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
| Match Nodes | local/phantom | 1.2ms | 1.3ms | 1.3ms |
| Match Edges | local/phantom | 1.3ms | 2.2ms | 2.2ms |
| Filter By Kind / User | local/phantom | 2.7ms | 4.5ms | 4.5ms |
| Filter By Kind / Group | local/phantom | 2.7ms | 3.1ms | 3.1ms |
| Filter By Kind / Computer | local/phantom | 1.6ms | 1.8ms | 1.8ms |
| Traversal Depth / depth 1 | local/phantom | 1.3ms | 2.0ms | 2.0ms |
| Traversal Depth / depth 2 | local/phantom | 1.4ms | 2.0ms | 2.0ms |
| Traversal Depth / depth 3 | local/phantom | 2.5ms | 4.0ms | 4.0ms |
| Edge Kind Traversal / MemberOf | local/phantom | 1.3ms | 1.3ms | 1.3ms |
| Edge Kind Traversal / GenericAll | local/phantom | 1.2ms | 1.4ms | 1.4ms |
| Edge Kind Traversal / HasSession | local/phantom | 1.1ms | 1.2ms | 1.2ms |
| Shortest Paths / 41 -> 587 | local/phantom | 1.6ms | 1.9ms | 1.9ms |

## Example: PG on local/phantom

```
$ export PG_CONNECTION_STRING="postgresql://dawgs:dawgs@localhost:5432/dawgs"
$ go run ./cmd/benchmark -dataset local/phantom
```

| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | local/phantom | 2.0ms | 4.4ms | 4.4ms |
| Match Edges | local/phantom | 411ms | 457ms | 457ms |
| Filter By Kind / User | local/phantom | 2.2ms | 3.3ms | 3.3ms |
| Filter By Kind / Group | local/phantom | 2.9ms | 3.3ms | 3.3ms |
| Filter By Kind / Computer | local/phantom | 1.4ms | 2.0ms | 2.0ms |
| Traversal Depth / depth 1 | local/phantom | 585ms | 631ms | 631ms |
| Traversal Depth / depth 2 | local/phantom | 661ms | 696ms | 696ms |
| Traversal Depth / depth 3 | local/phantom | 743ms | 779ms | 779ms |
| Edge Kind Traversal / MemberOf | local/phantom | 617ms | 670ms | 670ms |
| Edge Kind Traversal / GenericAll | local/phantom | 702ms | 755ms | 755ms |
| Edge Kind Traversal / HasSession | local/phantom | 680ms | 729ms | 729ms |
| Shortest Paths / 41 -> 587 | local/phantom | 703ms | 765ms | 765ms |

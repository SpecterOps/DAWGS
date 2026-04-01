# Export

Exports a graph from a PostgreSQL database to an OpenGraph JSON file. This is how local test datasets (like `phantom.json`) are captured from a running BloodHound instance.

## Usage

```bash
# Default connection (bloodhound local dev) and output (graph_export.json)
go run ./cmd/export

# Custom output file
go run ./cmd/export my_graph.json

# Custom connection
PGCONN="postgresql://dawgs:dawgs@localhost:5432/dawgs" go run ./cmd/export
```

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `PGCONN` | `postgresql://bloodhound:bloodhoundcommunityedition@localhost:5432/bloodhound` | PostgreSQL connection string |

## Output

Writes an OpenGraph JSON document with all nodes and edges from the default graph:

```json
{
  "graph": {
    "nodes": [{"id": "1", "kinds": ["User", "Base"], "properties": {...}}, ...],
    "edges": [{"start_id": "1", "end_id": "2", "kind": "MemberOf"}, ...]
  }
}
```

The output can be placed in `integration/testdata/local/` for use with the benchmark tool and integration tests.

# Plan Corpus Capture

`plancorpus` captures query-plan diagnostics for the shared integration corpus.

It reads `integration/testdata/cases` and `integration/testdata/templates`, loads the same datasets and inline fixtures used by the integration tests, and writes backend-specific JSONL plan records plus markdown and JSON summaries.

## Usage

```bash
PG_CONNECTION_STRING="postgres://postgres:password@localhost/db" \
NEO4J_CONNECTION_STRING="neo4j://neo4j:password@localhost:7687" \
go run ./cmd/plancorpus
```

Useful flags:

| Flag | Default | Description |
| --- | --- | --- |
| `-dataset-dir` | `integration/testdata` | Integration corpus root |
| `-output-dir` | `.coverage` | Output directory |
| `-connection` | `CONNECTION_STRING` | Capture one backend selected by URL scheme |
| `-pg-connection` | `PG_CONNECTION_STRING` | PostgreSQL backend |
| `-neo4j-connection` | `NEO4J_CONNECTION_STRING` | Neo4j backend |
| `-summary` | `.coverage/plan-corpus-summary.md` | Markdown summary |
| `-summary-json` | `.coverage/plan-corpus-summary.json` | JSON summary |
| `-top` | `25` | Number of expensive PostgreSQL plans to include in summaries |

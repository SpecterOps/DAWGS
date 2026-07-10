# Plan Corpus Capture

`plancorpus` captures query-plan diagnostics for the shared integration corpus.

It reads `integration/testdata/cases` and `integration/testdata/templates`, loads the same datasets and inline fixtures used by the integration tests, and writes backend-specific JSONL plan records plus markdown and JSON summaries.

Use this command to baseline PostgreSQL translator and optimizer changes. PostgreSQL captures include translated SQL,
`EXPLAIN` output, plan operator counts, estimated plan cost, recursive CTE indicators, path materialization indicators,
planned lowerings, applied lowerings, skipped lowerings, and skipped-lowering reasons. Neo4j captures include logical
plan operator trees for cross-backend plan-shape comparison.

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

## Reviewing Captures

The markdown summary is intended for human review. It ranks the highest-cost PostgreSQL plans, reports feature counts
such as `Recursive Union`, `SubPlan`, and `Function Scan on unnest`, and summarizes planned/applied/skipped lowerings.

The JSON summary is intended for automation and baseline comparison. For optimizer work, check that intentional SQL
shape changes are explained and that skipped-lowering accounting remains actionable. A planned lowering without a
matching applied lowering should either have a specific skipped reason or indicate a translator consumption bug.

Expected capture errors should be limited to invalid-query cases surfaced by the integration corpus or backend-specific
syntax differences. Unexpected capture errors should be treated as validation failures for planner or translator work.

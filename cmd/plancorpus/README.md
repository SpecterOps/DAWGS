# Plan Corpus Capture

`plancorpus` captures query-plan diagnostics for the shared integration corpus.

It reads `integration/testdata/cases` and `integration/testdata/templates`, loads the same datasets and inline fixtures used by the integration tests, and writes backend-specific JSONL plan records plus markdown and JSON summaries.
Fixture-backed `node_params` and `node_list_params` are resolved after each
fixture load, preserving ID-anchored production query shapes in captured plans.

Use this command to baseline PostgreSQL translator and optimizer changes. PostgreSQL captures include translated SQL,
`EXPLAIN` output, plan operator counts, estimated plan cost, recursive CTE indicators, path materialization indicators,
planned lowerings, applied lowerings, skipped lowerings, and skipped-lowering reasons. Neo4j read captures use `PROFILE`
after execution and retain ordered operators, estimated and actual rows, DB and page-cache hits, loops, and operator
time when the server exposes them. Writes remain `EXPLAIN`-only.

Every run also writes a semantic PostgreSQL/Neo4j delta over the union of captured workloads. The delta is keyed by
workload hash and source revision, fingerprints each backend plan, compares access side, physical direction, predicate
placement, endpoint binding, traversal family, estimates, and PostgreSQL planned/emitted/fallback identities, and ranks
the largest disagreements. A missing or failed backend remains an explicit incomplete pair; it is never discarded by an
intersection-only comparison. Runtime-arm attribution remains GraphBench's responsibility.

## Usage

```bash
PG_CONNECTION_STRING="postgres://postgres:password@localhost/db" \
  NEO4J_CONNECTION_STRING="neo4j://neo4j:password@localhost:7687" \
  go run ./cmd/plancorpus
```

Plan capture reloads fixtures and refuses to open a selected backend unless the destructive acknowledgement is set and
its exact credential-free target is allowlisted. PostgreSQL aliases and omitted default ports are canonicalized;
multi-host PostgreSQL URLs are accepted only when every fallback resolves to the same target.

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
| `-plan-delta-json` | `.coverage/plan-corpus-delta.json` | Versioned paired semantic delta, including incomplete backend pairs |
| `-top` | `25` | Number of expensive PostgreSQL plans to include in summaries |
| `-dawgs-version` | auto-detected | DAWGS source version recorded in output |

## Reviewing Captures

The markdown summary is intended for human review. It ranks the highest-cost PostgreSQL plans, reports feature counts
such as `Recursive Union`, `SubPlan`, and `Function Scan on unnest`, and summarizes planned/applied/skipped lowerings.

The JSON summary and paired delta are intended for automation and baseline comparison. For optimizer work, check that intentional SQL
shape changes are explained and that skipped-lowering accounting remains actionable. A planned lowering without a
matching applied lowering should either have a specific skipped reason or indicate a translator consumption bug.
Both per-query JSONL records and summaries include the DAWGS source version
needed to compare captures made from different worktrees.

Expected capture errors should be limited to invalid-query cases surfaced by the integration corpus or backend-specific
syntax differences. Unexpected capture errors should be treated as validation failures for planner or translator work.

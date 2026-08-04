# GraphBench

`graphbench` runs the scale benchmark corpus under `benchmark/testdata/scale`.
It is meant for runtime gap accounting: query duration, returned row counts,
PostgreSQL plan details, Neo4j plan operators, fallback reasons, and comparison
summaries.

The current execution modes are:

- `postgres_sql`: runs DAWGS' PostgreSQL SQL translation against a PostgreSQL database.
- `local_traversal`: records explicit `not_implemented` placeholders until the local traversal executor lands.
- `neo4j`: runs the same corpus against Neo4j through the DAWGS Neo4j backend.

Apache AGE is not an execution mode in this harness yet. AGE behavior can be
captured in corpus `reference_design` notes so DAWGS can use it as design input
without treating it as a direct benchmark comparison.

## Inputs

The command loads cases from `benchmark/testdata/scale` by default and imports
the fixture datasets from `integration/testdata`.

Corpus parameters support fixture IDs through `node_params` and
`node_list_params`. Tagged datetime values are decoded to `time.Time`, avoiding
lexical string comparisons in temporal cases. Mutating cases require an
explicit `write_scenario`; the runner checks matched and affected counts plus
post-state queries and rolls back warm-up, timed iterations, and PostgreSQL
plan capture.

Connection strings can be supplied as flags or environment variables:

- PostgreSQL: `-pg-connection`, `PG_CONNECTION_STRING`, `-connection`, or `CONNECTION_STRING`.
- Neo4j: `-neo4j-connection`, `NEO4J_CONNECTION_STRING`, `-connection`, or `CONNECTION_STRING`.

Every output record includes BHE, BHCE, and DAWGS source metadata. Use
`-bhe-commit`, `-bhce-commit`, and `-dawgs-version` to override the recorded
defaults.

## Examples

Run only PostgreSQL SQL translation:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench-postgres.jsonl \
  -summary .coverage/graphbench-postgres.md \
  -summary-json .coverage/graphbench-postgres.json
```

Capture PostgreSQL, local traversal placeholders, and Neo4j in one report:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql,local_traversal,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench.jsonl \
  -summary .coverage/graphbench.md \
  -summary-json .coverage/graphbench.json
```

Compare a run against a previous JSONL capture:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -baseline .coverage/graphbench-baseline.jsonl \
  -jsonl-output .coverage/graphbench.jsonl \
  -summary .coverage/graphbench.md
```

## Outputs

JSONL output contains one `CaseResult` record per case and execution mode.
Markdown and JSON summaries aggregate mode status counts, per-case timings, row
counts, fallback reasons, and baseline regressions or improvements when a
baseline capture is supplied.

Write records additionally report matched and affected counts and each
post-state observation. The recorded duration covers the mutation query; setup,
verification, and rollback are outside that duration.

PostgreSQL records include translated SQL and `EXPLAIN (ANALYZE, BUFFERS,
TIMING OFF)` metrics. Neo4j records include plan operator names
when an `EXPLAIN` plan can be captured.

## Phase 7 correctness gate

The PostgreSQL-only `TestPostgreSQLPhase7PlanInvariants` test loads the same
scale corpus and fixture as the command. It executes all Phase 7 Cypher
representatives, requires their declared cardinalities and mutation post-state,
and verifies that the captured plan came from `EXPLAIN ANALYZE`. Stable
assertions cover relationship/node mutation targets, branch-local logical
structure, temporal filtering, and anchored edge-index orientation. The test
uses rollback isolation for writes and runs automatically under
`make test_all` when `CONNECTION_STRING` selects PostgreSQL.

Run only the Phase 7 gate with:

```bash
CONNECTION_STRING="$PG_CONNECTION_STRING" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLPhase7PlanInvariants|Phase7RequiredScaleRepresentativesDeclareCardinality)' \
  -count=1
```

The non-integration cardinality test also guarantees that every required stable
query-form ID remains represented in the scale corpus and declares an expected
read or write cardinality.

# Development Workflow

This document covers repository workflows for contributors. The top-level [README](../README.md) keeps the main
quick-start commands and links to detailed documentation.

## Build And Test

The [Makefile](../Makefile) drives build, test, formatting, and reporting commands.

```bash
make build
make test
```

`make test` runs unit tests with race detection and writes coverage artifacts under `.coverage/`:

- `.coverage/unit.out`
- `.coverage/coverage.txt`

Run the integration suite when a backend is available:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
make test_integration
```

`CONNECTION_STRING` selects the active backend by URL scheme. Neo4j connection strings may use `neo4j://`,
`neo4j+s://`, or `neo4j+ssc://`; a single path segment selects the Neo4j database name.

Benign local examples:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
export CONNECTION_STRING="neo4j://neo4j:weneedbetterpasswords@localhost:7687"
```

Use backend-specific targets when needed:

```bash
make test_pg
make test_neo4j
```

`make test_all` runs unit tests and integration tests:

```bash
make test_all
```

The shared integration cases under `integration/testdata/cases` and `integration/testdata/templates` must stay
semantically equivalent across supported backends. Avoid driver-specific skips or expected results in those files; add
driver-scoped integration coverage when a backend-only capability needs coverage.

## Formatting

Run:

```bash
make format
```

The target uses `goimports`; install it locally if it is missing from your environment.

## Quality And Metrics

Cyclomatic complexity, CRAP, and quality signal reports are available through dedicated metric targets:

```bash
make complexity
make crap
make quality
make metrics
```

Outputs are written under `.coverage/`:

- `.coverage/cyclomatic.txt`
- `.coverage/crap.txt`
- `.coverage/crap.json`
- `.coverage/quality.txt`
- `.coverage/quality.json`
- `.coverage/metrics.html`

Generated parser files, tests, vendor code, and testdata are excluded from these reports. The HTML report embeds its CSS
and JavaScript directly in the document, so it can be opened without network access.

Optional quality inputs can be supplied through Make variables:

```bash
make quality BACKEND_RESULT_ARGS="-backend-result pg=.coverage/integration-pg.json -backend-result neo4j=.coverage/integration-neo4j.json"
make quality BENCHMARK_REPORT=.coverage/benchmark.json BENCHMARK_BASELINE=.coverage/benchmark-baseline.json
make quality FUZZ_REPORT=.coverage/fuzz.json MUTATION_REPORT=.coverage/mutation.json
```

`make quality_backend` captures PostgreSQL and Neo4j integration results for backend equivalence comparison. It
requires `PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING`. `make quality_bench` writes benchmark markdown and JSON
captures for later baseline comparison.

Thresholds are report-only by default. To enforce configured thresholds, run:

```bash
make metrics_check
```

The defaults can be adjusted with `CYCLO_TOP`, `CYCLO_OVER`, `CRAP_TOP`, `CRAP_OVER`, and `BENCHMARK_REGRESSION`.

## Plan Corpus

`make plan_corpus` captures plan diagnostics for the shared Cypher integration corpus. It accepts either
`CONNECTION_STRING` for one backend or `PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING` for both backends, then
writes JSONL captures and markdown/JSON summaries under `.coverage/`.

Run it when changing PostgreSQL Cypher planning, lowering, or SQL emission. The summaries rank expensive PostgreSQL
plans and report recursive CTEs, `SubPlan`, `Function Scan on unnest`, planned/applied optimizer lowerings, and
skipped-lowering reasons.

See [Plan Corpus Capture](../cmd/plancorpus/README.md) for flags and review guidance.

## Graph Benchmarks

`go run ./cmd/graphbench` captures runtime diagnostics for the scale corpus under `benchmark/testdata/scale`.

Current modes are:

- `postgres_sql`
- `local_traversal`
- `neo4j`

AGE is reference-design input only and is not a direct comparison mode. The command can emit JSONL records plus
Markdown and JSON summaries, and can compare current timings against a previous JSONL baseline.

See [Graph Benchmark Capture](../cmd/graphbench/README.md) for command examples.

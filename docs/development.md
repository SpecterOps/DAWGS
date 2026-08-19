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
export DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1
export DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs"
make test_integration
```

`CONNECTION_STRING` selects the active backend by URL scheme. Neo4j connection strings may use `neo4j://`,
`neo4j+s://`, or `neo4j+ssc://`; a single path segment selects the Neo4j database name.

Benign local examples:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
export CONNECTION_STRING="neo4j://neo4j:weneedbetterpasswords@localhost:7687"
```

`DAWGS_INTEGRATION_DISPOSABLE_TARGETS` is a comma-separated list of exact,
credential-free targets in `<scheme>://<host>/<database>` form. Use
`/<default>` when the connection URL selects the driver's default database.
`make test_all`, `make test_pg`, `make test_neo4j`, and fixture-loading
GraphBench runs refuse targets absent from the list. GraphBench
`-existing-graph` mode remains exempt because it rejects writes and validates
before/after cardinalities. Its PostgreSQL sessions remain read-write so
temporary traversal workspaces retain production behavior.

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

The target uses `goimports`; install it locally if it is missing from your
environment. Sandboxed or nonstandard installations can supply its explicit
path without changing `PATH`:

```bash
make format GOIMPORTS_CMD=/absolute/path/to/goimports
```

`make lint` runs the standard Go vet analyzers across the repository. The unreachable-code analyzer is rerun only for
handwritten packages because ANTLR emits intentional terminal branches in `cypher/parser`; generated parser code still
receives every other vet analyzer.

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
writes JSONL captures and markdown/JSON summaries under `.coverage/`. Fixture loading requires the same destructive
acknowledgement and exact credential-free allowlist entries as integration testing.

Run it when changing PostgreSQL Cypher planning, lowering, or SQL emission. The summaries rank expensive PostgreSQL
plans and report recursive CTEs, `SubPlan`, `Function Scan on unnest`, planned/applied optimizer lowerings, and
skipped-lowering reasons.

See [Plan Corpus Capture](../cmd/plancorpus/README.md) for flags and review guidance.

## Graph Benchmarks

`go run ./cmd/graphbench` captures runtime diagnostics for the scale corpus under `benchmark/testdata/scale`.

Implemented modes are:

- `postgres_sql`
- `neo4j`

`local_traversal` emits non-gating `not_implemented` diagnostics only; it is not an implemented executor.

AGE is reference-design input only and is not a direct comparison mode. The command can emit JSONL records plus
Markdown and JSON summaries, and can compare current timings against a previous JSONL baseline.

The tool-only `-postgres-expansion-suffix-reverse-retry` mode measures the
reverse-only fixed-suffix P1 candidate with exact forward retry inside one
Repeatable Read transaction. It requires diagnostic traversal telemetry and a
pool size of one; see the
[frozen development protocol](experiments/suffix_reverse_retry_v1.md) before
selecting cases or overriding caps.

The tool-only `-postgres-expansion-suffix-route-component` mode measures one
exact suffix-seeded reverse statement for the default-off SQL-routing
preflight. It requires Repeatable Read, diagnostic traversal telemetry, and a
pool size of one. It forbids probes, retries, cap overrides, cache behavior,
and production manifests; see the
[preimplementation contract](experiments/sql_strategy_routing_preflight_v1.md).

The PostgreSQL scale-plan correctness gate shares the scale runner. It checks the
required stable query-form IDs, declared read/write cardinalities, rollback-safe
mutation post-state, `EXPLAIN ANALYZE` capture, and stable plan invariants. It
runs under `make test_all` for PostgreSQL or can be selected directly:

```bash
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs" \
  CONNECTION_STRING="$PG_CONNECTION_STRING" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLScalePlanInvariants|ScaleCorpusRequiredRepresentativesDeclareCardinality)' \
  -count=1
```

Store graphbench and plan-corpus captures under `.coverage/`; they are
environment-specific review artifacts, not committed correctness goldens.

See [Graph Benchmark Capture](../cmd/graphbench/README.md) for command examples.

## BloodHound Source-Parity Audits

When the reviewed BHE or BHCE snapshots change, repeat the call-site inventory,
active-entry-point trace, normalized query-form mapping, and commit recording in
[BloodHound Regression Source Parity](regression_source_parity.md).

Dormant `FUTURE-*` forms stay manifest-only until a reviewed caller is enabled.
The unit suites reject dormant IDs from both shared plan inputs and scale cases;
activating a form requires updating those gates together with its required
semantic, plan, and scale coverage.

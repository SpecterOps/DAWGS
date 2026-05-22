# DAWGS

Database Abstraction Wrapper for Graph Schemas

![A Corgi Treat](logo_small.png)

## Purpose

DAWGS is a collection of tools and query language helpers to enable running property graphs on vanilla PostgreSQL
without the need for additional plugins.

At the core of the library is an abstraction layer that allows users to swap out existing database backends (currently
Neo4j and PostgreSQL) or build their own with no change to query implementation. The query interface is built around
openCypher with translation implementations for backends that do not natively support the query language.

## Development Setup

For users making changes to `dawgs` and its packages, the [go mod replace](https://go.dev/ref/mod#go-mod-file-replace)
directive can be utilized. This allows changes made in the checked out `dawgs` repo to be immediately visible to
consuming projects.

**Example**

```
replace github.com/specterops/dawgs => /home/zinic/work/dawgs
```

### Building and Testing

The [Makefile](Makefile) drives build and test automation. The default `make` target should suffice for normal
development processes.

```bash
make
```

For validation before handing off a change, run the full test target:

```bash
make test_all
```

`make test_all` runs unit tests and the integration suites. Integration tests use the `CONNECTION_STRING` environment
variable and run against the backend selected by that connection string's scheme.

The shared integration cases under `integration/testdata/cases` and `integration/testdata/templates` are expected to be
semantically equivalent across supported backends. Avoid driver-specific skips or expected results in those files; add
driver-scoped integration coverage instead when a backend-only capability needs to be tested.

Benign local examples:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
export CONNECTION_STRING="neo4j://neo4j:weneedbetterpasswords@localhost:7687"
```

Use `make test` for unit tests only and `make test_integration` for integration tests only.

### Test Metrics

`make test` writes unit test coverage artifacts under `.coverage/`:

```bash
make test
```

The stable coverage profile is `.coverage/unit.out`, and the function coverage summary is `.coverage/coverage.txt`.

Cyclomatic complexity, CRAP, and quality signal reports are available through dedicated metric targets:

```bash
make complexity
make crap
make quality
make metrics
```

`make complexity` writes `.coverage/cyclomatic.txt`. `make crap` reruns unit tests for a fresh coverage profile, then
writes `.coverage/crap.txt`, `.coverage/crap.json`, `.coverage/quality.txt`, `.coverage/quality.json`, and a standalone
HTML report at `.coverage/metrics.html`. The quality section summarizes semantic drift, backend equivalence,
integration/template invariants, fuzz health, mutation score, and benchmark drift. Signals that need external captures are
reported as pending unless their input files are provided.
Generated parser files, tests, vendor code, and testdata are excluded from these reports. The HTML report embeds its CSS
and JavaScript directly in the document, so it can be opened without network access.

Optional quality inputs can be supplied through Make variables:

```bash
make quality BACKEND_RESULT_ARGS="-backend-result pg=.coverage/integration-pg.json -backend-result neo4j=.coverage/integration-neo4j.json"
make quality BENCHMARK_REPORT=.coverage/benchmark.json BENCHMARK_BASELINE=.coverage/benchmark-baseline.json
make quality FUZZ_REPORT=.coverage/fuzz.json MUTATION_REPORT=.coverage/mutation.json
```

`make quality_backend` captures PostgreSQL and Neo4j integration results for backend equivalence comparison. It requires
`PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING`. `make quality_bench` writes benchmark markdown and JSON captures
for later baseline comparison.

`make plan_corpus` captures plan diagnostics for the shared Cypher integration corpus. It accepts either
`CONNECTION_STRING` for one backend or `PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING` for both backends, then
writes JSONL captures and markdown/JSON summaries under `.coverage/`.

Thresholds are report-only by default. To enforce the configured thresholds, run:

```bash
make metrics_check
```

The defaults can be adjusted with `CYCLO_TOP`, `CYCLO_OVER`, `CRAP_TOP`, `CRAP_OVER`, and `BENCHMARK_REGRESSION`.

# DAWGS

Database Abstraction Wrapper for Graph Schemas

![A Corgi Treat](logo_small.png)

DAWGS provides tools and query helpers for running property graphs on vanilla PostgreSQL without extra database
plugins. It exposes a backend abstraction for graph queries, with current backend support for PostgreSQL and Neo4j.
The query interface is built around openCypher, including a PostgreSQL SQL translator for environments that do not
support Cypher natively.

## Quick Start

Build the repository:

```bash
make build
```

Run unit tests:

```bash
make test
```

Run integration tests when a backend is available:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
make test_integration
```

Use this module from another Go project:

```bash
go get github.com/specterops/dawgs
```

`make quality_backend` captures PostgreSQL and Neo4j integration results for backend equivalence comparison. It requires
`PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING`. `make quality_bench` writes benchmark markdown and JSON captures
for later baseline comparison. Benchmark drift comparison can be performed by `make quality` through `tools/metrics` when
`BENCHMARK_REPORT` and `BENCHMARK_BASELINE` are provided.

Run the package benchmark suite with:

```bash
make test_bench
```

Use `cmd/benchdiff` to compare benchmarks between two committed refs without changing the active worktree:

```bash
go run ./cmd/benchdiff -base main -target HEAD -kind unit
```

For integration benchmark comparisons, provide the same `CONNECTION_STRING` used by integration tests:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
go run ./cmd/benchdiff -base main -target HEAD -kind all -driver pg -fail-regression 10%
```

The harness writes raw outputs and a Markdown report under `.bench/runs/` by default. The report begins with comparison
findings, includes the raw `benchstat` output for each benchmark suite, and ends with a table of all captured benchmark
numbers.

The integration benchmark runner includes committed `base`, `adcs_fanout`, and `traversal_shapes` datasets by default.
The traversal shape suite checks expected result counts for chain, fanout, bounded cycle, disconnected,
edge-kind-selective, and multi-path shortest-path scenarios before recording timings.

`make plan_corpus` captures plan diagnostics for the shared Cypher integration corpus. It accepts either
`CONNECTION_STRING` for one backend or `PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING` for both backends, then
writes JSONL captures and markdown/JSON summaries under `.coverage/`.

`go run ./cmd/graphbench` captures runtime diagnostics for the scale corpus under `benchmark/testdata/scale`. The
current modes are `postgres_sql`, `local_traversal`, and `neo4j`; AGE is reference-design input only and is not a direct
comparison mode yet. The command can emit JSONL records plus Markdown and JSON summaries, and can compare current timings
against a previous JSONL baseline.

`go run ./cmd/retriever` dumps and loads live Dawgs graph databases as
manifest-based collections of compressed JSONL fragments. It supports
PostgreSQL and Neo4j, gzip and zstd compression, checksum validation before
load, optional deterministic property scrubbing, and a read-throughput benchmark
mode. It can also package dumps as single HPKE/ML-KEM encrypted TAR archives.
See [cmd/retriever/README.md](cmd/retriever/README.md) for dump, encrypted
archive, load, scrubbed dump, metrics verification, and benchmark examples.
The same import/export functionality is available to library consumers from
`github.com/specterops/dawgs/retriever`; callers provide an already-open
`graph.Database`, and archive helpers support both path-based and stream-based
APIs. The package exposes CLI-matching default option constructors, structured
progress callbacks, manifest/metrics helpers, HPKE key envelope reader/writer
helpers, and typed errors for validation, compatibility, checksum, metrics, and
count mismatches.

PostgreSQL translates exact string property equality with a JSON string type guard and `properties ->>` extraction, so
indexes created on expressions such as `properties ->> 'objectid'` and `properties ->> 'name'` can be used for selective
anchors without matching JSON booleans or numbers. Simple relationship count fast paths depend on the schema's
`kind_id`-first edge index for efficient typed counts.

PostgreSQL property index regression coverage is hard-failing under the `manual_integration` tag. The synthetic plan
test translates Cypher to PgSQL, disables sequential scans for the `EXPLAIN`, and requires explicit node property
indexes to appear in the JSON plan:

For local development against a checkout, use a Go module replacement in the consuming project:

```go
replace github.com/specterops/dawgs => /path/to/dawgs
```

## Documentation

- [Development workflow](docs/development.md): build, test, integration, metrics, quality, and corpus-capture commands.
- [Cypher library](cypher/README.md): parser generation and Cypher package overview.
- [PostgreSQL translation](docs/postgresql_translation.md): PostgreSQL translator behavior, optimizer lowerings, indexing notes, and validation expectations.
- [Plan corpus capture](cmd/plancorpus/README.md): shared integration corpus plan diagnostics.
- [Graph benchmark capture](cmd/graphbench/README.md): runtime diagnostics for scale scenarios.
- [Cypher syntax support](cypher/Cypher%20Syntax%20Support.md): supported Cypher behavior and semantic notes.

## Repository Map

- `cypher/`: parser, Cypher AST, walkers, and backend translation models.
- `drivers/`: database driver implementations.
- `integration/`: backend-equivalent integration suites and fixtures.
- `cmd/`: command-line tools for capture, export, and diagnostics.
- `tools/`: developer tools such as `dawgrun` and metrics reporting.

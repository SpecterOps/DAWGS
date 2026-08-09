# DAWGS

Database Abstraction Wrapper for Graph Schemas

![A Corgi Treat](logo_small.png)

DAWGS provides tools and query helpers for running property graphs on vanilla PostgreSQL without extra database
plugins. It exposes a backend abstraction for graph queries, with current backend support for PostgreSQL and Neo4j.
The query interface is built around openCypher, including a PostgreSQL SQL translator for environments that do not
support Cypher natively.

The PostgreSQL driver bounds repeated parser work with an immutable 256-entry Cypher AST cache; optimization and SQL
translation still run per execution so graph, schema, kind, and parameter changes remain visible. Cached query text is
released by LRU eviction or driver close, and diagnostics expose aggregate counters without query text.

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
export DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1
export DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs"
make test_integration
```

Integration suites and fixture-loading GraphBench runs delete graph data. The
acknowledgement and credential-free target allowlist above are both required;
an absent or mismatched target is rejected before testing. Existing-graph
GraphBench runs reject mutating cases and do not require destructive
acknowledgement. PostgreSQL sessions remain read-write so temporary traversal
workspaces use the same reset strategy as production.

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

The direct-write regression benchmark is integration-scoped because it
measures real driver batch APIs. It reloads or clears its fixture outside the
timed region and validates post-state after every iteration:

```bash
CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./integration -run '^$' \
  -bench BenchmarkMutationSafeDirectWrites -benchtime=1x
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
writes JSONL captures and markdown/JSON summaries under `.coverage/`. Captures record the DAWGS source version, which
can be overridden with a command flag when needed.

`go run ./cmd/graphbench` captures runtime diagnostics for the scale corpus under `benchmark/testdata/scale`. The
current modes are `postgres_sql`, `local_traversal`, and `neo4j`; AGE is reference-design input only and is not a direct
comparison mode yet. The command can emit JSONL records plus Markdown and JSON summaries, and can compare current timings
against a previous JSONL baseline. Mutating scale cases must declare a `write_scenario`; each warm-up and timed iteration
runs in a rollback transaction and verifies matched, affected, and post-state cardinality.
Read timings retain every raw warm sample and are bracketed by untimed exact-row
multiset checks. PostgreSQL datasets are vacuumed and analyzed after loading and
before measured reads. Fixture reloads truncate the active relationship and node
partitions together, and
PostgreSQL captures fail before timing unless the active partitions' physical
node and edge counts exactly match the declared fixture; active child-partition
sizes are retained with each fixture. Node-ID expectations and recorded paths use stable
fixture identities rather than backend-assigned IDs, while preserving duplicate
rows and path order.
For sanitized production-like data, `-existing-graph` uses a versioned
logical-key anchor manifest and bypasses every schema/load/clear/vacuum path.
It rejects mutation cases, verifies before/after cardinalities, redacts anchor
values, and supports atomic checkpoints, resume, progress JSONL, and explicitly
labeled adaptive discovery. See `cmd/graphbench/README.md` for the fixed
confirmation and timeout-class workflows.
The executable gate uses the complete corpus/backend declaration instead of the
intersection of successful records, treats Neo4j only as an exact-result and
informational latency oracle, and supports predeclared materiality thresholds.
`make perf_aa` derives p50/p95 measurement resolution from repeated A/A
captures. Exact case/dataset/category/tag selectors create diagnostic-only
artifacts that the complete gate refuses; configured warmups and matched
arm/block/run metadata support isolated confirmation. `make perf_confirm`
reports paired absolute and relative p50/p95 changes with optional block/reload
A/A floors. Capture bundles can retain the source patch, untracked sources,
module state, binary, manifest, raw records, and checksums. Opt-in pool
concurrency blocks and PostgreSQL component/full-query
references are documented in `cmd/graphbench/README.md`. Path-observed
singleton captures include exact benchmark-only M0/M1 materializer arms with a
shared search boundary; they do not enable an experimental production executor.
Generated ADCS captures also provide selectable exact A1a/A1b/A2/A3/A4
forward, factored-suffix, reverse, and viability arms plus versioned fixtures
with independent suffix-density and reverse-fan-in controls. The optimizer
reports a typed expansion-search decision, but keeps production on its exact
stepwise fallback until the predeclared live qualification gates pass.

PostgreSQL recursive shortest-path execution also includes bounded S4
singleton executors and an all-shortest predecessor-DAG executor, with exact
same-statement fallback, reusable session-local workspaces, late hydration, and
a parameter-shape-aware translation cache. The implementation and its
qualification boundaries are documented in
[Recursive-descent cost controls](docs/recursive_descent_cost_controls.md).

The PostgreSQL scale-plan gate runs as part of `make test_all` when
`CONNECTION_STRING` selects PostgreSQL. It executes every required Cypher scale
representative with `EXPLAIN ANALYZE`, enforces declared result or mutation
cardinality, and checks stable mutation-target and anchored edge-index
invariants. Run it directly with:

```bash
CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLScalePlanInvariants|ScaleCorpusRequiredRepresentativesDeclareCardinality)' \
  -count=1
```

Runtime and plan captures are intentionally generated under the ignored
`.coverage/` directory. Keep them as reviewed environment-specific artifacts;
use the stable `REC-*`, `TRUST-*`, `PRUNE-*`, `HOP-*`, `SCAN-*`, and `LOOKUP-*`
IDs to compare captures with their semantic fixtures and manifest entries.

`go run ./cmd/retriever` dumps and loads live Dawgs graph databases as
manifest-based collections of compressed JSONL fragments. It supports
PostgreSQL and Neo4j, uncompressed, gzip, and zstd fragments, bounded keyset
scans, resumable dump checkpoints, checksum validation before load, optional
deterministic property scrubbing, and a read-throughput benchmark mode. It can
also package dumps as single HPKE/ML-KEM encrypted TAR archives.
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
- [Integration corpus](integration/testdata/README.md): fixture, mutation post-state, and typed-parameter schema.
- [BloodHound regression coverage manifest](regression_coverage_manifest.md): per-query-form layer status and existing primitive links.
- [BloodHound source-parity workflow](docs/regression_source_parity.md): dormant-form activation rules and repeatable BHE/BHCE source audits.
- [Cypher syntax support](cypher/Cypher%20Syntax%20Support.md): supported Cypher behavior and semantic notes.

## Repository Map

- `cypher/`: parser, Cypher AST, walkers, and backend translation models.
- `drivers/`: database driver implementations.
- `integration/`: backend-equivalent integration suites and fixtures.
- `cmd/`: command-line tools for capture, export, and diagnostics.
- `tools/`: developer tools such as `dawgrun` and metrics reporting.

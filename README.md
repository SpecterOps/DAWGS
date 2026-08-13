# DAWGS

Database Abstraction Wrapper for Graph Schemas

![A Corgi Treat](logo_small.png)

DAWGS provides tools and query helpers for running property graphs on vanilla PostgreSQL without extra database
plugins. It exposes a backend abstraction for graph queries, with current backend support for PostgreSQL and Neo4j.
The query interface is built around openCypher, including a PostgreSQL SQL translator for environments that do not
support Cypher natively.

The PostgreSQL driver bounds repeated work with immutable 256-entry Cypher AST and SQL translation caches. Translation
entries are keyed by normalized query text, graph ID, a collision-safe parameter-name/type shape, and the effective
versioned traversal-policy identity; they retain SQL
and parameter-source mappings, never request values or defaults, and fail closed when a required source value is absent.
Cached query text is released by LRU eviction or driver close, and diagnostics expose aggregate counters without query
text.

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
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs" \
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

The integration benchmark runner includes committed `base`,
`fixed_suffix_expansion_fanout`, and `traversal_shapes` datasets by default.
The traversal shape suite checks expected result counts for chain, fanout, bounded cycle, disconnected,
edge-kind-selective, and multi-path shortest-path scenarios before recording timings.

`make plan_corpus` captures plan diagnostics for the shared Cypher integration corpus. It accepts either
`CONNECTION_STRING` for one backend or `PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING` for both backends, then
writes JSONL captures and markdown/JSON summaries under `.coverage/`. Captures record the DAWGS source version, which
can be overridden with a command flag when needed. Because it reloads fixtures, it also requires
`DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1` and every selected credential-free target in
`DAWGS_INTEGRATION_DISPOSABLE_TARGETS`.

`go run ./cmd/graphbench` captures runtime diagnostics for the scale corpus under `benchmark/testdata/scale`. The
implemented execution modes are `postgres_sql` and `neo4j`; `local_traversal` is an explicit, non-gating
`not_implemented` diagnostic placeholder, and AGE is reference-design input only. The command can emit JSONL records
plus Markdown and JSON summaries, and can compare current timings against a previous JSONL baseline. Mutating scale
cases must declare a `write_scenario`; each warm-up and timed iteration runs in a rollback transaction and verifies
matched, affected, and post-state cardinality.
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
`make perf_aa` derives host-fingerprinted p50/p95 measurement resolution from
order-balanced repeated A/A captures. Complete normal/envelope performance
gates require that checksummed per-case evidence and use minimum 5%/100us
floors; stress timing remains diagnostic. Exact case/dataset/category/tag selectors create diagnostic-only
artifacts that the complete gate refuses; configured warmups and matched
arm/block/run metadata support isolated confirmation. The GraphBench CLI accepts
repeated `-aa-artifact` inputs so two immutable append-series arms can be
validated without an external merge. `make perf_confirm`
reports paired absolute and relative p50/p95 changes with optional block/reload
A/A floors. Capture bundles can retain the source patch, untracked sources,
module state, binary, manifest, raw records, and checksums. Opt-in pool
concurrency blocks and PostgreSQL component/full-query
references are documented in `cmd/graphbench/README.md`. Path-observed
singleton captures include exact benchmark-only M0/M1 materializer arms with a
shared search boundary; they do not enable an experimental production executor.
Generated fixed-suffix expansion captures provide selectable exact root-reuse,
late-hydration, factored-suffix forward, suffix-seeded reverse, and
backward-viability forward arms plus versioned fixtures with independent
suffix-density and reverse-fan-in controls. V3 fixtures additionally encode
matching-root multiplicity and independent relationship-distinct cycle and
self-loop controls at the productive boundary. The optimizer reports a typed
expansion-search decision. Repository-native
`EXPANSION-SUFFIX-SEEDED-REVERSE` is an exact qualification-only implementation.
Production selection remains on the stepwise incumbent because query shape and
available metadata do not provide hard suffix-density or reverse-state bounds.
The staged, tool-only `orientation-probe-v2` experiment uses
`F2 = root_rows + maximum_depth * forward_degree_rows` and
`R2 = suffix_rows + boundary_rows + reverse_degree_rows`, selecting reverse
only when every bounded probe is complete and `4 * R2 < 3 * F2`. Its frozen v3
corpus contains eight selector-training cases and four evaluation holdouts whose
timings remain unopened. Qualification requires matched `shadow`, `incumbent`,
`reverse`, and `guarded` artifacts captured under Repeatable Read with traversal
telemetry. On a clean tree, discovery must emit both its report and freeze
manifest from the exact eight training cases; confirmation must consume those
checksum-bound files and the exact eight-training/four-holdout cohort. Per-case
A/A evidence also binds the PostgreSQL timing environment, including transaction
isolation, and the exact validated fixture. See
[GraphBench](cmd/graphbench/README.md) for the exact capture and report protocol.
No v2 qualification benchmark has passed. The existing
`orientation-probe-v1` report, exact-query production seam, and default
production behavior are unchanged by this staging work.
For the distinct one-fixed-prefix plus selective-terminal-expansion shape,
production uses guarded `EXPANSION-ENDPOINT-SEEDED-REVERSE`: 32 endpoint and
4096 reverse-state caps select either the reverse candidate or an exact
same-statement forward fallback without exposing partial candidate rows.

PostgreSQL recursive shortest-path execution includes contained S3/S4
singleton selection, a guarded canonical inline witness canary, and an
all-shortest predecessor-DAG executor, with exact same-statement fallback,
reusable session-local workspace-v2 state, late hydration, event-chain runtime
receipts, and
a parameter-shape-aware translation cache. The implementation and its
qualification boundaries are documented in
[Recursive-descent cost controls](docs/recursive_descent_cost_controls.md).

New inline SP and ordinary-orientation lowerings remain default-off. Canonical
SP-I1 authorization now uses selector `sp-static-v6` and accepts only the
qualified inbound, typed, single-kind, one-path `min=1`/`max=64` bucket; the
automatic `sp-static-v5-contained` S3/S4 choices are unchanged. The
PostgreSQL driver's `SetTraversalPolicy` API can expose one eligible candidate
to an explicit normalized-query SHA-256 allowlist under a nonzero generation.
Activation requires the exact promotion manifest, including its measured
execution boundary and evidence digests. Manifest schema v2 also requires every
evidence report to repeat the exact candidate, selector, source, binary,
corpus, cap, bucket, and query-cohort identity; a digest-shaped string alone is
not authorization. B1/B2 and `SP-I1-C-D` remain tooling-only. Endpoint-seeded reverse,
inline ASP, and inline canonical SP each have an evidence-free emergency
disable switch. Resetting the policy to its
zero value immediately returns all queries to incumbent cache identities. This
is a reversible canary seam, not evidence that a candidate is qualified for
broad production use.

The PostgreSQL scale-plan gate runs as part of `make test_all` when
`CONNECTION_STRING` selects PostgreSQL. It executes every required Cypher scale
representative with `EXPLAIN ANALYZE`, enforces declared result or mutation
cardinality, and checks stable mutation-target and anchored edge-index
invariants. Run it directly with:

```bash
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs" \
  CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLScalePlanInvariants|ScaleCorpusRequiredRepresentativesDeclareCardinality)' \
  -count=1
```

Runtime and plan captures are intentionally generated under the ignored
`.coverage/` directory. Keep them as reviewed environment-specific artifacts;
use the stable `GFSE-*`, `REC-*`, `TRUST-*`, `PRUNE-*`, `HOP-*`, `SCAN-*`, and
`LOOKUP-*` IDs to compare captures with their semantic fixtures and manifest
entries.

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
- [CySQL traversal performance priorities](docs/cysql_traversal_priorities.md): source-grounded roadmap for orientation, SP/ASP, probes, statistics, telemetry, and qualification.
- [Traversal priority implementation status](docs/experiments/traversal_priority_implementation_status_v1.md): implemented candidate identities, fail-closed gates, and current no-promotion disposition.
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

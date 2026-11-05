# Testing

Build, fixture regeneration, integration prerequisites, and BDD reporting. Use a disposable database for integration suites.

[Documentation index](README.md) · [Project overview](../README.md)

Build the repository:

```bash
make build
```

Run unit tests (no database required, even when `CONNECTION_STRING` is set):

```bash
make test
```

After editing Cypher JSON cases or PostgreSQL translation fixtures, run
`make test_update` and include the regenerated files. CI repeats this command
and requires no diff, including fixture formatting.

Run integration tests when a backend is available:

Export the `CONNECTION_STRING` environment variable for the PostgreSQL or Neo4j driver.
`make test_integration` enables the `manual_integration` and `integration` build
tags, including PostgreSQL composite/JSON codec and graph-wipe tests. Tests scoped
to another backend skip automatically. `make test_all` runs the unit, integration,
and BDD suites for the selected backend; run it separately for each backend.

The unit suite validates record-assertion fields across the integration corpus.
It also checks the active query-form coverage contract in the
[BloodHound regression manifest](regression_coverage_manifest.md); the
[source-parity workflow](regression_source_parity.md) describes how to maintain it.
Templates can declare `node_kinds` and `edge_kinds` for query-only schema kinds;
see [the corpus format](../integration/testdata/README.md). These declarations also
apply to plan capture and do not insert fixture entities.
Tagged corpus datetimes normalize to a UTC fixed offset for equivalent instant
comparisons on both backends.

PostgreSQL integration tests require `pg_stat_statements` in `shared_preload_libraries`
before the server starts. The CI PostgreSQL integration service configures this through
`POSTGRES_INITDB_ARGS="--set=shared_preload_libraries=pg_stat_statements"` when initializing
its fresh PostgreSQL 18 cluster. The tests attempt to create the extension as needed. If the test role lacks extension
installation privileges, an administrator must first run `CREATE EXTENSION IF NOT EXISTS pg_stat_statements` in the
selected test database.

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
make test_integration
```

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
make test_bdd_integration
```

The BDD run writes Cucumber results to `.coverage/opencypher-tck-results.json` and an HTML summary to `.coverage/opencypher-tck-report.html`.

All untagged scenarios are enforced, including DAWGS features and upstream CREATE scenarios 1–2. Upstream scenarios 3–20 carry `@report-only` while their harness assertions or query semantics remain unsupported. They still execute and retain their failures in both reports, with the HTML identifying enforced and report-only scenarios separately. Remove the tag as each scenario becomes supported; new scenarios are enforced by default.

Enforced failures, empty or invalid reports, runner errors, and ordinary Go-test failures fail the command. Report-only failures do not establish conformance or fail the gate. Run backend BDD checks sequentially because they share report paths.

Integration suites and fixture-loading GraphBench runs delete graph data.
Existing-graph GraphBench runs reject mutating cases and do not require
fixture loading.
PostgreSQL sessions remain read-write so temporary traversal
workspaces use the same reset strategy as production.

## Multi-relationship expansion

With `CONNECTION_STRING` set to a disposable test database, run these checks
separately for PostgreSQL and Neo4j:

```bash
go test ./cypher/models/pgsql/translate -run '^TestMultiRelationshipExpansion' -count=1
go test -tags 'manual_integration integration' ./integration -run '^(TestMultiRelationshipExpansion.*|TestPostgreSQLMultiRelationshipExpansion.*)$' -count=1 -v
go test -tags 'manual_integration integration' ./integration -run '^TestCypherTemplates/multi_relationship_expansion_shapes/' -count=1 -v
```

`[:A|B*1..2]` permits all six type sequences `A`, `B`, `AA`, `AB`, `BA`,
and `BB`; two single-type expansions omit `AB` and `BA`. Bounds through three
hops permit fourteen sequences. The equivalence tests compare complete path
multisets against every fixed sequence, preserving convergent paths and duplicate
inputs. Shared templates cover Okta and AD types, bounds, directions, endpoint
filters, cycles, self-loops, and exact mutation state with and without RETURN.
PostgreSQL executes those templates in both compiler modes.

The fan-out diagnostic measures both counts and fully consumed paths, rotates
three query arms across rounds, and reports medians after warmup. It compares the
combined pattern, exhaustive fixed components, and the smaller workload from two
single-type queries. Timings include query processing and client consumption; separate
component queries also include additional round trips. Timings are observations,
not pass/fail thresholds or a guarantee for production graphs. PostgreSQL plan
checks assert exact recursive frontier sizes with and without disconnected
decoys; these row counts do not prove that physical scans never inspect rejected
edges. Run `make test_all` separately for each backend for full validation.

## Path materialization experiment

The [canonical study and action queue](experiments/path_batch_materialization_v1.md)
record the 2026-11-05 results, coverage exclusions, evidence provenance, and
implementation follow-ups. A [durable measurement summary](experiments/path_batch_materialization_v1_results.json)
preserves the observations independently of ignored local captures.

The test-only batching candidate compares complete ordered node and relationship
values with the current PostgreSQL materializer. Comparisons retain duplicate
paths, null paths, exact property types, labels, relationship types and endpoints.
The corpus runner inventories all ordinary cases, template variants and
metamorphic queries in both compiler modes, and reports unsupported shapes
explicitly. Unsupported cases never count as successful candidate comparisons.
The candidate does not change production translation or schema functions.

Use a fresh report directory for each study; the commands below use
`path-batch-rerun` to preserve the recorded local evidence. Run correctness with
`PATH_BATCH_STUDY=0` so performance timing remains separate from race checks.

```bash
PATH_BATCH_STUDY=0 PATH_BATCH_REPORT_DIR="$PWD/.coverage/path-batch-rerun" go test -race -tags manual_integration ./integration -run '^(TestPathBatchCompleteValueCanonicalization|TestPostgreSQLPathBatch.*)$' -count=1 -v
PATH_BATCH_STUDY=1 PATH_BATCH_REPORT_DIR="$PWD/.coverage/path-batch-rerun" go test -tags manual_integration ./integration -run '^TestPostgreSQLPathBatchPerformance$' -count=1 -v
```

Run the reference measurement separately with `CONNECTION_STRING` selecting
Neo4j:

```bash
PATH_BATCH_STUDY=1 PATH_BATCH_REPORT_DIR="$PWD/.coverage/path-batch-rerun" go test -tags manual_integration ./integration -run '^TestNeo4jPathBatchReference$' -count=1 -v
```

The performance matrix matches 512 three-hop paths on a shared-prefix/convergent
graph and on independent chains sharing only their root. Each topology has
narrow properties and distinct 2,048-byte payloads on every node and edge. A
4,096-path shared graph checks scaling. Every row is decoded and mapped to a
`graph.Path`. Latencies are five-round medians after six warmup cycles, with query
order rotated. Warmup samples are retained to expose prepared-plan transitions.
PostgreSQL uses an isolated graph: its empty partitions are vacuumed before each
scenario to remove rolled-back tuples, then analyzed after fixture loading.
Both maintenance steps are outside timing. Heap sizes are recorded with plans.
Time to first decoded row starts before query submission, including
the first-row wait inside the driver. Fixture setup, complete-value assertions,
memory sampling and EXPLAIN are outside latency measurements.

`LIMIT 1` has three PostgreSQL arms: the current query, batching followed by an
outer limit, and limiting the compact path input before batching. The latter is
safe for this simple final `RETURN p` projection; it is not a general rewrite
for ordering, distinctness, aggregates, filters on hydrated values or mutations.
Without requested ordering, a limited row must belong to the complete result
multiset; its identity need not match another plan's first row. Plans assert
how many compact paths enter the batch.

Client memory reports allocation traffic (`TotalAlloc` delta) and extra live Go
heap sampled every millisecond during a separate streaming/discard replay. The
sample is process-wide and may miss a peak. PostgreSQL reports planner memory,
the largest visible executor-node memory field, hash batches and temporary
reads/writes at default `work_mem` and at 64kB. Root temporary block counters are
inclusive and are not summed across children. These measurements are not a
complete backend memory peak: helper internals are partly hidden from EXPLAIN,
and the supplied role cannot read backend memory contexts. Raw samples, plans,
SQL and environment metadata are written only when `PATH_BATCH_REPORT_DIR` is
set. Timings are diagnostics, with no hardware-dependent pass/fail thresholds.
See the [study disposition](experiments/path_batch_materialization_v1.md#finding-and-disposition)
for the measured throughput/first-row trade-off and qualification work still open.

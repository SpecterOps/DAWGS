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

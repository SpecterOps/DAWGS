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

Cyclomatic complexity and CRAP reports are available through dedicated metric targets:

```bash
make complexity
make crap
make metrics
```

`make complexity` writes `.coverage/cyclomatic.txt`. `make crap` reruns unit tests for a fresh coverage profile, then
writes `.coverage/crap.txt`, `.coverage/crap.json`, and a standalone HTML report at `.coverage/metrics.html`.
Generated parser files, tests, vendor code, and testdata are excluded from these reports. The HTML report embeds its CSS
and JavaScript directly in the document, so it can be opened without network access.

Thresholds are report-only by default. To enforce the configured thresholds, run:

```bash
make metrics_check
```

The defaults can be adjusted with `CYCLO_TOP`, `CYCLO_OVER`, `CRAP_TOP`, and `CRAP_OVER`.
## PostgreSQL Extensions

The PostgreSQL driver's schema bootstrap (`drivers/pg/query/sql/schema_up.sql`) installs the following extensions:

| Extension     | Required | Purpose                                                                                                  |
|---------------|----------|----------------------------------------------------------------------------------------------------------|
| `pg_trgm`     | yes      | GIN trigram indexes for `contains` / `starts with` / `ends with` lookups on graph entity properties.     |
| `intarray`    | yes      | Extended integer array operations used when maintaining node kind arrays.                                |
| `pgstattuple` | no       | Measures btree leaf density and fragmentation for the driver-managed index optimization.                 |

`pgstattuple` is treated as best-effort. Installing it typically requires a superuser role and on some managed
Postgres deployments the contrib package is not exposed at all. Bootstrap wraps the install in an exception
handler that downgrades any failure to a `WARNING`, and the driver's `Optimize` implementation re-checks for the
extension at runtime and logs a warning when it is missing rather than failing the caller. To enable index
optimization in such environments, have an administrator install the extension out-of-band:

```sql
create extension if not exists pgstattuple;
```

### Index Optimization

When `pgstattuple` is available the driver's `Optimize` method (exposed through the optional `graph.Optimizer`
interface) performs the following on each invocation against btree indexes on partitions of the `node` and `edge`
tables:

1. Drops any `INVALID` indexes whose name matches the `_ccnew[N]` pattern, which Postgres leaves behind when a
   prior `REINDEX CONCURRENTLY` was aborted. Cleanup failures are logged at `WARN` and never fatal; an orphan
   wastes disk but does not block productive rebuilds.
2. Measures every candidate index with `pgstatindex` and flags those whose average leaf density falls below
   `60%` or whose leaf-page fragmentation reaches `40%`. Thresholds are calibrated against production samples
   and a freshly rebuilt baseline (~73.8% density).
3. Rebuilds each flagged index with `REINDEX INDEX CONCURRENTLY`, smallest first so that an early cancellation
   still produces the maximum number of completed rebuilds. Per-index failures are logged at `WARN` and the
   loop continues with the next candidate. Context cancellation aborts before the next candidate; an in-flight
   `REINDEX CONCURRENTLY` runs to completion (interrupting it would leave a `_ccnew` artifact for the next pass
   to reap).

The pass is not bounded by wall-clock time or candidate count. Callers should serialize `Optimize` against
their own scheduling loop.

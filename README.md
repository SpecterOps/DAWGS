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

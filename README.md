# DAWGS

**Database Abstraction Wrapper for Graph Schemas**

![A Corgi Treat](logo_small.png)

DAWGS is a Go library for working with property graphs through a shared database
interface. Run graph queries on vanilla PostgreSQL without database plugins, or
use Neo4j's native graph engine. DAWGS provides openCypher parsing, a PostgreSQL
SQL translator, and Go APIs for graph queries, traversal, and batch updates.

## Supported backends

| Backend | Query execution | Requirements |
| --- | --- | --- |
| PostgreSQL | Translates supported openCypher queries to SQL. | PostgreSQL 16 or newer; 18 recommended. No graph extension required. |
| Neo4j | Executes Cypher through the native driver. | Neo4j using the Go v5 driver. |

Cypher support varies by backend. See [syntax support](cypher/Cypher%20Syntax%20Support.md)
and [query compatibility](docs/query-semantics.md) for supported forms and limitations.

## Get started

Install from your Go project:

```bash
go get github.com/specterops/dawgs
```

Use the Go toolchain specified in [go.mod](go.mod). The driver packages are
[`drivers/pg`](drivers/pg) and [`drivers/neo4j`](drivers/neo4j).
See the [PostgreSQL driver guide](docs/postgresql-driver.md) for pool setup and runtime configuration.

To build and run unit tests from a checkout:

```bash
make build
make test
```

Integration tests use a disposable PostgreSQL or Neo4j database selected by
`CONNECTION_STRING`. See [testing](docs/testing.md) for setup and commands.

## Guides

- [PostgreSQL driver](docs/postgresql-driver.md): connections, caches, schema upgrades, and storage.
- [Query semantics](docs/query-semantics.md): backend behavior and compatibility.
- [Import and export](cmd/retriever/README.md): dump, load, scrub, and archive graphs.
- [Development](docs/development.md): formatting, tests, and contributor workflows.
- [Benchmarking](docs/benchmarking.md): performance captures and traversal qualification.
- [All documentation](docs/README.md): library utilities, architecture, and detailed references.

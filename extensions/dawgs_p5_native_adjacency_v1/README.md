# P5 native adjacency feasibility extension

This is an opt-in, capture-only PostgreSQL extension for the frozen
`P5-NATIVE-ADJACENCY-SCAN-V1` feasibility protocol. It is not part of normal
DAWGS schema installation, query translation, or driver startup. It does not
implement Cypher traversal or selection.

Build the package only with PostgreSQL 17 or 18's matching `pg_config`:

```bash
make p5_native_extension_build P5_NATIVE_PG_CONFIG=/path/to/pg_config
```

Stage a loadable package into an empty directory without touching a PostgreSQL
installation. `P5_NATIVE_IMAGE_ID` is a required immutable identifier for the
matched-major build image or host:

```bash
P5_NATIVE_IMAGE_ID='postgres:18.4-linux-amd64' \
  make p5_native_extension_stage \
  P5_NATIVE_PG_CONFIG=/path/to/pg_config \
  P5_NATIVE_EXTENSION_STAGE="$PWD/.coverage/p5-native-pg18"
```

Staging records the PostgreSQL version, image identity, compiler, headers,
source checksums, unstripped binary, and stripped installed binary. It enforces
the frozen 1 MiB stripped-library cap. The resulting files must be loaded only
by the matching PostgreSQL major. Creating, dropping, reinstalling, and
exactness-testing the extension remain separate Gate 0 and Gate 1 work.

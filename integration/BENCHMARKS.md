# Integration Benchmarks

|                |            |
| -------------- | ---------- |
| **Driver**     | pg         |
| **Git Ref**    | f6372ea    |
| **Date**       | 2026-03-30 |
| **Iterations** | 100        |

## Match Nodes

| Dataset         | Nodes | Median |    P95 |    Max |
| --------------- | ----: | -----: | -----: | -----: |
| diamond         |     4 | 0.14ms | 0.22ms | 0.31ms |
| linear          |     3 | 0.13ms | 0.20ms | 0.28ms |
| wide_diamond    |     5 | 0.15ms | 0.23ms | 0.34ms |
| disconnected    |     2 | 0.12ms | 0.19ms | 0.25ms |
| dead_end        |     4 | 0.14ms | 0.21ms | 0.30ms |
| direct_shortcut |     4 | 0.14ms | 0.22ms | 0.29ms |
| local/phantom   |     - |      - |      - |      - |

## Match Edges

| Dataset         | Edges | Median |    P95 |    Max |
| --------------- | ----: | -----: | -----: | -----: |
| diamond         |     4 | 0.15ms | 0.24ms | 0.33ms |
| linear          |     2 | 0.13ms | 0.21ms | 0.27ms |
| wide_diamond    |     6 | 0.16ms | 0.25ms | 0.36ms |
| disconnected    |     0 | 0.11ms | 0.18ms | 0.22ms |
| dead_end        |     3 | 0.14ms | 0.22ms | 0.30ms |
| direct_shortcut |     4 | 0.15ms | 0.23ms | 0.32ms |
| local/phantom   |     - |      - |      - |      - |

## Shortest Paths

### PostgreSQL V2 inline predecessor-DAG qualification

The default-off `ASP-I1-U-DAG+MAT-M0` executor resolves singleton endpoints
before recursive work, discovers distance and predecessors using identifier-only
state, and hydrates emitted paths through an inline M0 lateral operator. The
incumbent PostgreSQL SQL remains unchanged; production use still requires an
exact traversal-policy manifest and stable-snapshot transaction.

Matched live `traversal_shapes` runs use 20 timed iterations, two warm-up
iterations, one worker, PostgreSQL `plan_cache_mode=auto`, and JIT enabled:

| Scenario | PG V2 incumbent p50/p95 | PG V2 candidate p50/p95 | Neo4j p50/p95 |
|---|---:|---:|---:|
| Diamond, three shortest paths | 31.6ms / 216ms | 1.8ms / 4.5ms | 1.9ms / 2.7ms |
| Disconnected endpoints | 2.5ms / 42.3ms | 1.5ms / 2.2ms | 1.5ms / 2.8ms |

The stored-workspace `ASP-B2-DAG-MIN-LEVEL` candidate did not qualify on this
fixture because workspace execution dominated the small search. Forced custom
planning also regressed both shortest-path shapes; `auto` remains the selected
plan policy. The B2 executor remains available only for diagnostic/tool runs.

### V2 production-policy path

The forced-executor measurements above establish a candidate SQL comparison,
but do not exercise V2's manifest selection or connection-local translation
cache. `cmd/benchmark` now has a separate `production_policy` mode that loads a
GraphBench-verified manifest into `Driver.SetTraversalPolicy`, requires
Repeatable Read, and runs exactly its single allowlisted parameterized Cypher
scenario. A live PostgreSQL manual integration test renders the candidate SQL,
binds its SHA-256 into a schema-v2 manifest, installs that policy on `pg-v2`,
and executes the route successfully.

No forced-mode latency is relabeled as a production-policy result here: a
comparable publication requires a clean-source, GraphBench-verified manifest
whose SQL anchor and exact query digest match the current benchmark schema.
The V2 policy route was nevertheless executed live against PostgreSQL on
2026-08-20: `TestPostgresV2BenchmarkPolicyPath` rendered and anchor-validated
the candidate statement, installed it through `SetTraversalPolicy`, and
returned the expected path. The full PostgreSQL `make test_all` suite also
passed. These are execution-validation results, not promotion-performance
evidence.

When that evidence is available, run:

```bash
go run ./cmd/graphbench -promotion-manifest .coverage/promotion.json
go run ./cmd/benchmark \
  -driver pg-v2 \
  -connection "postgresql://user:password@localhost/database" \
  -dataset traversal_shapes \
  -pg-v2-traversal-policy-manifest .coverage/promotion.json \
  -pg-v2-traversal-policy-generation 7 \
  -pg-plan-cache-mode auto \
  -iterations 20 -warmup 2 -workers 1
```

```bash
go run ./cmd/benchmark \
  -driver pg-v2 \
  -connection "postgresql://user:password@localhost/database" \
  -dataset traversal_shapes \
  -iterations 20 -warmup 2 -workers 1 \
  -pg-v2-min-conns 0 -pg-v2-max-conns 1 \
  -pg-v2-shortest-path-executor 'ASP-I1-U-DAG+MAT-M0' \
  -pg-plan-cache-mode auto
```

| Dataset         | Start | End | Paths | Median |    P95 |    Max |
| --------------- | ----- | --- | ----: | -----: | -----: | -----: |
| diamond         | a     | d   |     2 | 0.42ms | 0.68ms | 0.91ms |
| direct_shortcut | a     | d   |     1 | 0.31ms | 0.50ms | 0.72ms |
| linear          | a     | c   |     1 | 0.33ms | 0.54ms | 0.74ms |
| dead_end        | a     | c   |     1 | 0.34ms | 0.55ms | 0.76ms |
| disconnected    | a     | b   |     0 | 0.18ms | 0.29ms | 0.40ms |
| wide_diamond    | a     | e   |     3 | 0.51ms | 0.82ms | 1.12ms |
| local/phantom   | -     | -   |     - |      - |      - |      - |

## Variable-Length Traversal

| Dataset       | Start | Reachable | Median |    P95 |    Max |
| ------------- | ----- | --------: | -----: | -----: | -----: |
| linear        | a     |         2 | 0.28ms | 0.45ms | 0.62ms |
| diamond       | a     |         3 | 0.35ms | 0.56ms | 0.78ms |
| wide_diamond  | a     |         4 | 0.41ms | 0.66ms | 0.90ms |
| dead_end      | a     |         3 | 0.34ms | 0.55ms | 0.75ms |
| disconnected  | a     |         0 | 0.15ms | 0.24ms | 0.33ms |
| local/phantom | -     |         - |      - |      - |      - |

## Match Return Nodes

| Dataset       | Start | Returned | Median |    P95 |    Max |
| ------------- | ----- | -------: | -----: | -----: | -----: |
| diamond       | a     |        2 | 0.19ms | 0.30ms | 0.42ms |
| linear        | a     |        1 | 0.17ms | 0.27ms | 0.38ms |
| wide_diamond  | a     |        3 | 0.21ms | 0.34ms | 0.47ms |
| local/phantom | -     |        - |      - |      - |      - |

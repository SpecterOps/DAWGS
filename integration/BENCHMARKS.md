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


### PostgreSQL MERGE matrix and plan captures

`BenchmarkPostgreSQLMerge` separates explicit same-value SET from alternating updates. Its selected matrix varies
batch size (1, 8, 64, 512, 4096), unrelated payload (0, 256, 4096, 65536 bytes), and independent assignments (0, 1, 8, 32).
Creation, unchanged matches, updates, mixed composite keys, dynamic maps, and target conflicts have separate cases.
Dynamic matching uses one map input against a batch-sized fixture; Cypher parameter maps are not specialized from
runtime keys. Seeds and parameters are prepared outside timing. Matrix transactions roll back, preserving the
match/create mix across iterations; result drain and transaction rollback are included. The small legacy scenarios
commit. Compilation is warmed before timing. Sequence values consumed by rolled-back creations are not reset.

With a PostgreSQL CONNECTION_STRING supplied:

```sh
go test -tags manual_integration ./integration -run '^$' \
  -bench BenchmarkPostgreSQLMerge -benchtime=1s -count=5 -benchmem
MERGE_PLAN_DIR="$PWD/.coverage/merge-plans" go test -tags manual_integration ./integration \
  -run '^TestPostgreSQLMergePlans$' -count=1
```

Plan capture writes SQL and JSON with runtime parameters for fixed keys, wide payloads, changed keys, dynamic maps,
bound endpoints, and repeated-target rejection. Successful plans use EXPLAIN (ANALYZE, BUFFERS, WAL, VERBOSE, FORMAT JSON)
in rollback transactions. PostgreSQL 18.6 can emit malformed execution JSON for partitioned MERGE (tuple counters
appear inside the Target Tables array). The harness preserves that raw output and captures valid non-executing JSON
and EXPLAIN ANALYZE text in separate rollback transactions. It does not rewrite the server output. Rejected workloads use non-executing EXPLAIN and assert the runtime error separately.
Run captures and database suites serially: integration setup can reset shared database tables. Do not run EXPLAIN ANALYZE
on production fixtures. For before/after comparisons use the same harness and server settings in a separate baseline
checkout. Record work_mem, durability, fixture/index setup, cache warmth, and timing boundaries with results.

See [MERGE implementation evidence](../docs/merge_implementation.md) for measurements and remaining costs.

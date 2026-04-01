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

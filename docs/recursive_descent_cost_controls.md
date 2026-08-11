# Recursive-descent cost controls

Date: 2026-08-09

This implementation addresses the six recursive-descent findings from the PostgreSQL/Neo4j delta review. It changes
the PostgreSQL execution architecture; it does not claim that the cross-backend latency gap is closed until the same
corpus is recaptured against both supplied backends.

| Finding | Implemented control |
|---|---|
| 1. `allShortestPaths` retained too much trail state | `ASP-A1-DAG` performs minimum-layer discovery, stores all relationship-distinct predecessors only for minimum layers, then enumerates the predecessor DAG. |
| 2. Small depths paid recursive setup cost | Both production functions have exact one-hop and two-hop SQL arms before workspace allocation. |
| 3. Breadth-first levels churned temporary catalog objects | Session-local workspaces are created once per connection and reset/versioned per invocation; legacy swaps now copy/truncate without table renames. |
| 4. Singleton shortest paths needed a bounded compact search | `SP-S4-C-D` and `SP-S4-C-WE+MAT-M0` use canonical ID-only BFS state, a 100,000-state default ceiling, and exact same-statement fallback. |
| 5. Recursive rows hydrated entities too early | New executors carry node/relationship IDs and perform one ordered path hydration after search. |
| 6. Repeated compilation and unstable recursive estimates added overhead | Functions declare `COST`/`ROWS` and set `recursive_worktable_factor`; the driver has a bounded, coalescing, parameter-shape-aware translation cache. |

Terminal-selective ordinary expansions also have a guarded reverse lowering. The optimizer only selects it for one
fixed directed prefix hop followed by a terminal directed expansion (`*1..64`) with one relationship kind and a local
terminal ID/property search. The statement probes 33 endpoints and 4097 reverse states: up to 32/4096 uses the reverse
candidate, while either sentinel activates the exact forward incumbent in the same snapshot. Candidate output is
gated until both probes finish, so overflow and cancellation cannot leak partial results.

## Selection boundaries

`asp-static-v1` selects `ASP-A1-DAG` only for one read-only, non-optional, directed `allShortestPaths` traversal with one
static ID equality per endpoint, minimum depth one, no path/relationship predicate, and no observed relationship value.
An open maximum uses depth 15. Minimum-depth-zero, self-endpoint, directionless, correlated, mutation, and predicate
shapes retain the incumbent exact executor.

`sp-static-v4` retains `SP-S3-U-D` for qualified distance work, with `SP-S4-C-D` for deep physical-inbound distance
searches, and selects `SP-S4-C-WE+MAT-M0` for every qualified one-path witness. The older S3 edge-trail materializer
remains tool-forceable for qualification only. The compact S4 function checks its state ceiling before emitting any row;
overflow invokes the exact relationship-trail fallback inside the same SQL statement and snapshot.

`EXPANSION-SUFFIX-SEEDED-REVERSE` remains tool-only. Existing evidence showed a
fixed-suffix expansion topology crossover that query shape alone does not safely
bound, so this work does not activate the strategy in production. The rejected
bounded-fallback and continuation experiments are retained only as historical
decision records under `docs/experiments`.

## Qualification contract

GraphBench recognizes `ASP-A1-DAG`, `SP-S4-C-D`, and `SP-S4-C-WE+MAT-M0` as applied architectures. Their resource gate
allows the declared local workspace but rejects executor temporary-file reads/writes and WAL for non-mutating queries.
Use the generated depth/fanout corpus, exact path observations, planner modes, concurrency, cancellation/session reuse,
and matched PostgreSQL/Neo4j delta report before treating the implementation as performance-qualified.

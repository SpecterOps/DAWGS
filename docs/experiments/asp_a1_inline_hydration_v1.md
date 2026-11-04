# ASP A1 inline hydration disposition v1

The `allShortestPaths` diamond regression was attributed with parameterized
`EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)`. The previous benchmark
explainer translated without scenario parameters and therefore captured an
empty endpoint plan; this disposition uses the corrected exact-parameter
capture.

The A1 predecessor-DAG function was not the dominant cost. The generated SQL
materialized each returned edge-ID path through `ordered_edge_ids_to_path`, a
separately planned generic helper. `ASP-A1-DAG` now uses the existing inline
M0 hydration relation used by compact shortest-path executors: it hydrates the
ordered edges and terminal nodes once at the outer statement boundary.

On the local `traversal_shapes` corpus with one V2 connection, two warmups,
and ten timed samples:

| Arm | Diamond median | Disconnected median |
| --- | ---: | ---: |
| Previous A1 generic hydration | 24.22 ms | 1.46 ms |
| A1 inline M0 hydration | 0.65 ms | 1.60 ms |
| B1 bidirectional component | 1.50 ms | 9.30 ms |
| B2 bidirectional component | 1.40 ms | 6.60 ms |
| Neo4j reference | 1.23 ms | 1.35 ms |

The A1 diamond improvement is approximately 37x and places PostgreSQL V2
ahead of the observed Neo4j median. Neither B1 nor B2 clears the required 20%
win over A1 and both are frozen as tool-only negative results for this workload.

No new ASP production selector is installed. The existing automatic A1 route
remains the conservative production control, while the corrected benchmark
stage capture remains available for future deeper or wider topology studies.

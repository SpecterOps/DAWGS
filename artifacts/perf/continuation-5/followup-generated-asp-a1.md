# GraphBench Summary

Generated: 2026-08-07T19:50:12Z

DAWGS version: `(devel)`

## Modes

| Mode | Total | OK | Row Mismatch | Error | Not Implemented |
| --- | ---: | ---: | ---: | ---: | ---: |
| postgres_sql | 1 | 1 | 0 | 0 | 0 |

## Cases

| Case | Dataset | Category | postgres_sql | local_traversal | neo4j |
| --- | --- | --- | --- | --- | --- |
| GSPV2-NORMAL-diamond-all-shortest | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 10.7ms; rows=2; all_shortest_paths | - | - |

## Raw PostgreSQL Cost Models

### generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 / GSPV2-NORMAL-diamond-all-shortest

Boundary attribution: 97.9% of 8.3ms.

| Component | Interval | Median | p95 | Share of E2E | Confidence |
| --- | --- | ---: | ---: | ---: | --- |
| Pool acquisition | exclusive | 0.00ms | 0.01ms | 0.1% | raw-pgx observed boundary |
| Transaction setup | exclusive | 0.09ms | 0.26ms | 1.1% | raw-pgx observed boundary |
| Bind/prepare | exclusive | 7.4ms | 9.0ms | 89.1% | raw-pgx observed boundary |
| First-row transfer/decode | exclusive | 0.03ms | 0.05ms | 0.4% | raw-pgx observed boundary |
| Remaining transfer/decode | exclusive | 0.01ms | 0.02ms | 0.1% | raw-pgx observed boundary |
| Drain/close | exclusive | 0.60ms | 0.72ms | 7.2% | raw-pgx observed boundary |
| Unexplained residual | derived | 0.17ms | 0.00ms | 2.1% | derived |
| Server execution | inclusive/overlapping | 8.0ms | 0.00ms | 96.5% | single EXPLAIN diagnostic |

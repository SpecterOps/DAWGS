# GraphBench Summary

Generated: 2026-08-07T19:50:07Z

DAWGS version: `(devel)`

## Modes

| Mode | Total | OK | Row Mismatch | Error | Not Implemented |
| --- | ---: | ---: | ---: | ---: | ---: |
| postgres_sql | 1 | 1 | 0 | 0 | 0 |

## Cases

| Case | Dataset | Category | postgres_sql | local_traversal | neo4j |
| --- | --- | --- | --- | --- | --- |
| GSPV2-NORMAL-hidden-fanin-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 2.1ms; rows=1; deep_inbound_unqualified,shortest_path | - | - |

## Raw PostgreSQL Cost Models

### generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 / GSPV2-NORMAL-hidden-fanin-path

Boundary attribution: 96.9% of 1.9ms.

| Component | Interval | Median | p95 | Share of E2E | Confidence |
| --- | --- | ---: | ---: | ---: | --- |
| Pool acquisition | exclusive | 0.00ms | 0.00ms | 0.1% | raw-pgx observed boundary |
| Transaction setup | exclusive | 0.02ms | 0.09ms | 1.1% | raw-pgx observed boundary |
| Bind/prepare | exclusive | 1.7ms | 2.0ms | 91.1% | raw-pgx observed boundary |
| First-row transfer/decode | exclusive | 0.02ms | 0.04ms | 1.1% | raw-pgx observed boundary |
| Remaining transfer/decode | exclusive | 0.00ms | 0.00ms | 0.0% | raw-pgx observed boundary |
| Drain/close | exclusive | 0.07ms | 0.08ms | 3.5% | raw-pgx observed boundary |
| Unexplained residual | derived | 0.06ms | 0.00ms | 3.1% | derived |
| Server execution | inclusive/overlapping | 1.6ms | 0.00ms | 83.1% | single EXPLAIN diagnostic |

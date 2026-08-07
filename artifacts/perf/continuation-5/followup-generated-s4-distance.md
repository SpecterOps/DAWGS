# GraphBench Summary

Generated: 2026-08-07T19:50:01Z

DAWGS version: `(devel)`

## Modes

| Mode | Total | OK | Row Mismatch | Error | Not Implemented |
| --- | ---: | ---: | ---: | ---: | ---: |
| postgres_sql | 1 | 1 | 0 | 0 | 0 |

## Cases

| Case | Dataset | Category | postgres_sql | local_traversal | neo4j |
| --- | --- | --- | --- | --- | --- |
| GSPV2-NORMAL-hidden-fanin-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 1.3ms; rows=1; deep_inbound_unqualified,shortest_path | - | - |

## Raw PostgreSQL Cost Models

### generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 / GSPV2-NORMAL-hidden-fanin-distance

Boundary attribution: 99.3% of 1.2ms.

| Component | Interval | Median | p95 | Share of E2E | Confidence |
| --- | --- | ---: | ---: | ---: | --- |
| Pool acquisition | exclusive | 0.00ms | 0.01ms | 0.1% | raw-pgx observed boundary |
| Transaction setup | exclusive | 0.02ms | 0.03ms | 1.6% | raw-pgx observed boundary |
| Bind/prepare | exclusive | 1.1ms | 1.6ms | 93.9% | raw-pgx observed boundary |
| First-row transfer/decode | exclusive | 0.00ms | 0.01ms | 0.1% | raw-pgx observed boundary |
| Remaining transfer/decode | exclusive | 0.00ms | 0.00ms | 0.1% | raw-pgx observed boundary |
| Drain/close | exclusive | 0.04ms | 0.06ms | 3.5% | raw-pgx observed boundary |
| Unexplained residual | derived | 0.01ms | 0.00ms | 0.7% | derived |
| Server execution | inclusive/overlapping | 1.2ms | 0.00ms | 106.0% | single EXPLAIN diagnostic |

# GraphBench Summary

Generated: 2026-08-07T19:48:47Z

DAWGS version: `(devel)`

## Modes

| Mode | Total | OK | Row Mismatch | Error | Not Implemented |
| --- | ---: | ---: | ---: | ---: | ---: |
| postgres_sql | 4 | 4 | 0 | 0 | 0 |

## Cases

| Case | Dataset | Category | postgres_sql | local_traversal | neo4j |
| --- | --- | --- | --- | --- | --- |
| GSPV2-NORMAL-hidden-fanin-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 1.3ms; rows=1; 1.02x; shortest_path | - | - |
| GSPV2-NORMAL-hidden-fanin-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 1.8ms; rows=1; 0.95x; shortest_path | - | - |
| GSPV2-NORMAL-parallel-kind-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 0.07ms; rows=1; 0.07x; shortest_path | - | - |
| GSPV2-NORMAL-parallel-kind-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 0.70ms; rows=1; 0.48x; shortest_path | - | - |

## Baseline Regressions

| Case | Dataset | Mode | Baseline | Current | Ratio |
| --- | --- | --- | ---: | ---: | ---: |
| GSPV2-NORMAL-hidden-fanin-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | postgres_sql | 1.3ms | 1.3ms | 1.02x |

## Baseline Improvements

| Case | Dataset | Mode | Baseline | Current | Ratio |
| --- | --- | --- | ---: | ---: | ---: |
| GSPV2-NORMAL-parallel-kind-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | postgres_sql | 0.96ms | 0.07ms | 0.07x |
| GSPV2-NORMAL-parallel-kind-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | postgres_sql | 1.5ms | 0.70ms | 0.48x |
| GSPV2-NORMAL-hidden-fanin-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | postgres_sql | 1.9ms | 1.8ms | 0.95x |

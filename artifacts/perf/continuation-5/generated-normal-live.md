# GraphBench Summary

Generated: 2026-08-07T17:51:41Z

DAWGS version: `(devel)`

## Modes

| Mode | Total | OK | Row Mismatch | Error | Not Implemented |
| --- | ---: | ---: | ---: | ---: | ---: |
| neo4j | 43 | 43 | 0 | 0 | 0 |
| postgres_sql | 42 | 42 | 0 | 0 | 0 |

## Cases

| Case | Dataset | Category | postgres_sql | local_traversal | neo4j |
| --- | --- | --- | --- | --- | --- |
| GADCS-D00-F001-none_endpoint_ids | generated_adcs_d0_f1_v1_p0 | generated_adcs | 2.2ms; rows=1; tournament_unqualified | - | 1.1ms; rows=1 |
| GADCS-D00-F001-none_path | generated_adcs_d0_f1_v1_p0 | generated_adcs | 4.3ms; rows=1; tournament_unqualified | - | 1.2ms; rows=1 |
| GADCS-D16-F1000-sparse_endpoint_ids | generated_adcs_d16_f1000_v1000_p0 | generated_adcs | 52.9ms; rows=2; tournament_unqualified | - | 0.95ms; rows=2 |
| GADCS-D16-F1000-sparse_path | generated_adcs_d16_f1000_v1000_p0 | generated_adcs | 63.6ms; rows=2; tournament_unqualified | - | 0.97ms; rows=2 |
| GADCS-D01-F010-sparse_endpoint_ids | generated_adcs_d1_f10_v10_p0 | generated_adcs | 2.9ms; rows=2; tournament_unqualified | - | 1.5ms; rows=2 |
| GADCS-D01-F010-sparse_path | generated_adcs_d1_f10_v10_p0 | generated_adcs | 3.7ms; rows=2; tournament_unqualified | - | 1.00ms; rows=2 |
| GADCS-D02-F100-sparse_endpoint_ids | generated_adcs_d2_f100_v10_p0 | generated_adcs | 2.9ms; rows=11; tournament_unqualified | - | 0.85ms; rows=11 |
| GADCS-D02-F100-sparse_path | generated_adcs_d2_f100_v10_p0 | generated_adcs | 4.3ms; rows=11; tournament_unqualified | - | 1.3ms; rows=11 |
| GADCS-D04-F010-half_payload_endpoint_ids | generated_adcs_d4_f10_v2_p4096 | generated_adcs | 2.9ms; rows=6; tournament_unqualified | - | 0.94ms; rows=6 |
| GADCS-D04-F010-half_payload_path | generated_adcs_d4_f10_v2_p4096 | generated_adcs | 5.3ms; rows=6; tournament_unqualified | - | 1.6ms; rows=6 |
| GADCS-D08-F001-all_endpoint_ids | generated_adcs_d8_f1_v1_p0 | generated_adcs | 2.7ms; rows=2; tournament_unqualified | - | 1.3ms; rows=2 |
| GADCS-D08-F001-all_path | generated_adcs_d8_f1_v1_p0 | generated_adcs | 4.1ms; rows=2; tournament_unqualified | - | 1.2ms; rows=2 |
| GADCS2-D16-F1000-R1-X1-M1-sparse_endpoint_ids | generated_adcs_v2_d16_f1000_r1_x1_i0_m1_z1_p0 | generated_adcs | 53.4ms; rows=2; tournament_unqualified | - | 1.7ms; rows=2 |
| GADCS2-D16-F1000-R1-X1-M1-sparse_path | generated_adcs_v2_d16_f1000_r1_x1_i0_m1_z1_p0 | generated_adcs | 62.8ms; rows=2; tournament_unqualified | - | 0.95ms; rows=2 |
| GADCS2-D08-F016-R1-I1000-high_reverse_fanin | generated_adcs_v2_d8_f16_r1_x0_i1000_m1_z0_p0 | generated_adcs | 2.8ms; rows=1; tournament_unqualified | - | 2.1ms; rows=1 |
| GADCS2-D08-F512-R0-X512-zero_reachable | generated_adcs_v2_d8_f512_r0_x512_i0_m1_z0_p0 | generated_adcs | 15.1ms; tournament_unqualified | - | 3.3ms |
| GSP-D16-F016_distance | generated_shortest_paths_d16_f16 | generated_shortest_path | 0.49ms; rows=1; shortest_path | - | 0.86ms; rows=1 |
| GSP-D16-F016_path | generated_shortest_paths_d16_f16 | generated_shortest_path | 0.80ms; rows=1; shortest_path | - | 0.96ms; rows=1 |
| GSP-D00-F001_path_zero | generated_shortest_paths_d1_f1 | generated_shortest_path | 0.80ms; rows=1; shortest_path | - | 0.97ms; rows=1 |
| GSP-D01-F001_distance | generated_shortest_paths_d1_f1 | generated_shortest_path | 0.35ms; rows=1; shortest_path | - | 1.0ms; rows=1 |
| GSP-D01-F001_path | generated_shortest_paths_d1_f1 | generated_shortest_path | 0.71ms; rows=1; shortest_path | - | 0.87ms; rows=1 |
| GSP-D01-F016_distance_parallel | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.66ms; rows=1; shortest_path | - | 1.1ms; rows=1 |
| GSP-D01-F016_path_parallel | generated_shortest_paths_d2_f16 | generated_shortest_path | 6.2ms; rows=1; non_single_kind_path_state_unqualified,shortest_path | - | 1.1ms; rows=1 |
| GSP-D02-F016_distance | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.52ms; rows=1; shortest_path | - | 1.1ms; rows=1 |
| GSP-D02-F016_distance_cycle | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.72ms; rows=1; shortest_path | - | 1.2ms; rows=1 |
| GSP-D02-F016_distance_self_loop | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.61ms; rows=1; shortest_path | - | 1.0ms; rows=1 |
| GSP-D02-F016_path | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.98ms; rows=1; shortest_path | - | 0.82ms; rows=1 |
| GSP-D02-F016_path_cycle | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.95ms; rows=1; shortest_path | - | 1.9ms; rows=1 |
| GSP-D02-F016_path_self_loop | generated_shortest_paths_d2_f16 | generated_shortest_path | 0.75ms; rows=1; shortest_path | - | 1.8ms; rows=1 |
| GSP-D04-F128_all_shortest_diamond | generated_shortest_paths_d4_f128 | generated_all_shortest_paths | 14.3ms; rows=2; all_shortest_paths | - | 1.0ms; rows=2 |
| GSP-D04-F128_disconnected | generated_shortest_paths_d4_f128 | generated_shortest_path | 0.61ms; shortest_path | - | 0.91ms |
| GSP-D04-F128_distance | generated_shortest_paths_d4_f128 | generated_shortest_path | 0.52ms; rows=1; shortest_path | - | 1.2ms; rows=1 |
| GSP-D04-F128_path | generated_shortest_paths_d4_f128 | generated_shortest_path | 0.89ms; rows=1; shortest_path | - | 0.89ms; rows=1 |
| GSP-D04-F128_path_disconnected | generated_shortest_paths_d4_f128 | generated_shortest_path | 0.77ms; shortest_path | - | 1.1ms |
| GSP-D08-F001_distance_inbound | generated_shortest_paths_d8_f1 | generated_shortest_path | 12.8ms; rows=1; deep_inbound_unqualified,shortest_path | - | 1.1ms; rows=1 |
| GSP-D08-F001_path_inbound | generated_shortest_paths_d8_f1 | generated_shortest_path | 13.9ms; rows=1; deep_inbound_unqualified,shortest_path | - | 1.2ms; rows=1 |
| GSP-D08-F128_path_directionless | generated_shortest_paths_d8_f128 | generated_shortest_path | - | - | 1.8ms; rows=1 |
| GSPV2-NORMAL-diamond-all-shortest | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 13.2ms; rows=2; all_shortest_paths | - | 1.1ms; rows=2 |
| GSPV2-NORMAL-hidden-fanin-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 7.6ms; rows=1; deep_inbound_unqualified,shortest_path | - | 1.0ms; rows=1 |
| GSPV2-NORMAL-hidden-fanin-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 11.8ms; rows=1; deep_inbound_unqualified,shortest_path | - | 1.2ms; rows=1 |
| GSPV2-NORMAL-outbound-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 0.52ms; rows=1; shortest_path | - | 1.2ms; rows=1 |
| GSPV2-NORMAL-parallel-kind-distance | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 0.76ms; rows=1; shortest_path | - | 1.5ms; rows=1 |
| GSPV2-NORMAL-parallel-kind-path | generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1 | generated_shortest_path_v2 | 6.0ms; rows=1; non_single_kind_path_state_unqualified,shortest_path | - | 1.1ms; rows=1 |

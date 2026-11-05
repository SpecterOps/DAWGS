# Traversal experiment records

[Documentation index](../README.md) · [Benchmarking guide](../benchmarking.md)

These records preserve versioned routing contracts and experiment dispositions.
An implemented or qualified component does not activate a production selector.
Use each candidate's promotion contract and the maintained benchmarking guide
for qualification requirements.

## Implementation and qualification references

- [Production-wide SQL selection](production_wide_sql_selection_v1.md)
- [Topology-selected routing protocol](topology_selected_routing_protocol_v1.md)
- [Topology fixed-suffix v4 status](topology_fixed_suffix_v4_status_v1.md)
- [Topology fixed-suffix v4 capture procedure](topology_fixed_suffix_v4_capture_v1.md)
- [Fixed-suffix first-use routing](topology_fixed_suffix_first_use_v1.md)
- [Composite fixed-suffix lowering](fixed_suffix_composite_lowering_v1.md)
- [Inline all-shortest-path predecessor DAG](asp_i1_inline_v1.md)
- [A1 diagnostic prerequisite](asp_a1_diagnostic_prerequisite_v1.md)
- [A1 inline hydration disposition](asp_a1_inline_hydration_v1.md)
- [N1 negative-exhaustion disposition](asp_n1_negative_exhaustion_v1.md)
- [Remaining outlier delivery](remaining_outlier_delivery_v1.md)
- [Traversal implementation snapshot](traversal_priority_implementation_status_v1.md)

## Feasibility and historical dispositions

| Record | Disposition |
| --- | --- |
| [Ordinary path materialization batching](path_batch_materialization_v1.md) | Diagnostic only; test-only batching improves selected bulk reads, with first-row and spill costs. Bounded hydration and production qualification remain open. |
| [Guarded suffix keyset continuation](guarded_suffix_keyset_continuation_v1.md) | Rejected and retired. |
| [Suffix reverse retry](suffix_reverse_retry_v1.md) | Terminally rejected by the clean P1 capture. |
| [SP-I2 successor power study](sp_i2_successor_power_study_v3.md) | Rejected before implementation. |
| [Bidirectional P3 preflight V1](sp_bidirectional_p3_preflight_v1.md) | Superseded by V2. |
| [Bidirectional P3 preflight V2](sp_bidirectional_p3_preflight_v2.md) | B1/B2 arms terminally rejected. |
| [All-shortest P4 baseline](asp_p4_open_baseline_v1.md) | Completed training-only baseline. |
| [All-shortest P4 I1 preflight](asp_p4_i1_disconnected_preflight_v1.md) | I1 arm terminally rejected. |
| [P5 architecture feasibility](p5_architecture_feasibility_v1.md) | Deferred before candidate implementation. |
| [P5 adjacency feasibility V1](p5_adjacency_materialization_feasibility_v1.md) | Rejected evidence; superseded by V2. |
| [P5 adjacency feasibility V2](p5_adjacency_materialization_feasibility_v2.md) | Captured and not advanced. |
| [Topology synopsis ADR](traversal_topology_synopsis_adr_v1.md) | Superseded by the topology-selected routing protocol. |
| [Fixed-suffix cardinality audit](fixed_suffix_cardinality_metadata_audit.md) | No hard pre-translation bound available. |
| [SQL strategy-routing preflight](sql_strategy_routing_preflight_v1.md) | Frozen feasibility protocol. |
| [Suffix-route cache feasibility](suffix_route_cache_feasibility_v1.md) | Frozen feasibility contract. |

Superseded and rejected records remain because later contracts and tracked
benchmark evidence refer to their immutable identities and negative results.

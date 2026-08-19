-- Copyright 2026 Specter Ops, Inc.
-- SPDX-License-Identifier: Apache-2.0

create function p5_native_adjacency_scan_v1(
    graph_id int4,
    anchor_id int8,
    edge_kind_ids int2[],
    inbound bool
)
returns table(
    edge_ids int8[],
    next_node_ids int8[],
    kind_ids int2[],
    scanned_index_tuples int8,
    heap_fetches int8,
    returned_rows int4,
    overflow bool,
    complete bool
)
as 'MODULE_PATHNAME', 'p5_native_adjacency_scan_v1'
language c
stable
strict
security invoker
parallel restricted;

revoke all on function p5_native_adjacency_scan_v1(int4, int8, int2[], bool) from public;

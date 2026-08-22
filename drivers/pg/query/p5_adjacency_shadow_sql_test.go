package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestP5AdjacencyShadowIsOptIn verifies the experimental materialization has
// its own install/remove boundary and cannot alter normal schema creation.
func TestP5AdjacencyShadowIsOptIn(t *testing.T) {
	require.NotContains(t, sqlSchemaUp, "p5_adjacency_v1")
	require.NotContains(t, sqlSchemaDown, "p5_adjacency_v1")
	require.Contains(t, sqlP5AdjacencyShadowUp, "create table if not exists public.p5_adjacency_v1")
	require.Contains(t, sqlP5AdjacencyShadowUp, "partition by list (graph_id)")
	require.Contains(t, sqlP5AdjacencyShadowUp, "primary key (graph_id, direction, edge_id)")
	require.Contains(t, sqlP5AdjacencyShadowUp, "foreign key (edge_id, graph_id) references edge (id, graph_id) on delete cascade")
	require.Contains(t, sqlP5AdjacencyShadowUp, "p5_adjacency_v1_lookup_index")
	require.Contains(t, sqlP5AdjacencyShadowUp, "on conflict (graph_id, direction, edge_id) do update")
	require.Contains(t, sqlP5AdjacencyShadowUp, "after update of graph_id, start_id, end_id, kind_id")
	require.Contains(t, sqlP5AdjacencyShadowUp, "after insert")
	require.Contains(t, sqlP5AdjacencyShadowUp, "after delete")
	require.Contains(t, sqlP5AdjacencyShadowDown, "drop trigger if exists p5_adjacency_v1_after_insert on edge")
	require.Contains(t, sqlP5AdjacencyShadowDown, "drop table if exists public.p5_adjacency_v1")
}

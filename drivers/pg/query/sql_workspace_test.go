package query

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBidirectionalShortestPathWorkspaceIsReusable(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public._bidirectional_sp_harness")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.bidirectional_sp_harness")
	require.NotEqual(t, -1, end)
	harness := sqlSchemaUp[start : start+end]

	require.Contains(t, sqlSchemaUp, "create or replace function public.ensure_bsp_core_workspace()")
	require.Contains(t, sqlSchemaUp, "on commit preserve rows")
	require.Contains(t, harness, "perform public.reset_bsp_workspace(not use_array_parameters)")
	require.Contains(t, harness, "pg_temp.bsp_forward_front")
	require.Contains(t, harness, "pg_temp.bsp_backward_front")
	require.Contains(t, harness, "pg_temp.bsp_next_front")
	require.NotContains(t, harness, "create temporary table")
	require.NotContains(t, harness, "create index")
}

func TestBidirectionalShortestPathArrayModeSkipsGenericWorkspace(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "if not use_array_parameters then\nperform public.load_bsp_filter_tables")
	require.Contains(t, sqlSchemaUp, "perform public.reset_bsp_workspace(not use_array_parameters)")
}

func TestBidirectionalShortestPathFragmentsRewriteLegacyFilterTables(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.bsp_workspace_fragment")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.reset_bsp_workspace")
	require.NotEqual(t, -1, end)
	rewriter := sqlSchemaUp[start : start+end]

	require.Contains(t, rewriter, "'traversal_root_filter', 'pg_temp.bsp_root_filter'")
	require.Contains(t, rewriter, "'traversal_terminal_filter', 'pg_temp.bsp_terminal_filter'")
	require.Contains(t, rewriter, "'traversal_pair_filter', 'pg_temp.bsp_pair_filter'")
}

func TestLinearPathMaterializerScopesPersistentLookups(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.ordered_edge_ids_to_path")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.create_unidirectional_pathspace_tables")
	require.NotEqual(t, -1, end)
	materializer := sqlSchemaUp[start : start+end]

	require.Contains(t, materializer, "e.graph_id = target_graph_id")
	require.Contains(t, materializer, "n.graph_id = target_graph_id")
	require.Contains(t, materializer, "next_edge.ordinality = path_walk.idx + 1")
	require.NotContains(t, materializer, "order by case when")
}

func TestLegacyPathMaterializersRequireTargetGraph(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "nodes_to_path(target_graph_id int4")
	require.Contains(t, sqlSchemaUp, "edges_to_path(target_graph_id int4")
	require.Contains(t, sqlSchemaUp, "ordered_edges_to_path(target_graph_id int4")
	require.Contains(t, sqlSchemaUp, "n.graph_id = target_graph_id")
	require.Contains(t, sqlSchemaUp, "r.graph_id = target_graph_id")
}

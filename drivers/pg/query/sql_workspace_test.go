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
	require.Contains(t, sqlSchemaUp, "if present_version is not null and present_version is distinct from expected_version then")
	require.Contains(t, sqlSchemaUp, "on commit preserve rows")
	require.Contains(t, harness, "perform public.reset_bsp_workspace(not use_array_parameters)")
	require.Contains(t, harness, "pg_temp.bsp_forward_front")
	require.Contains(t, harness, "pg_temp.bsp_backward_front")
	require.Contains(t, harness, "pg_temp.bsp_next_front")
	require.NotContains(t, harness, "create temporary table")
	require.NotContains(t, harness, "create index")
	require.Contains(t, harness, "truncate table pg_temp.bsp_forward_front")
	require.Contains(t, harness, "truncate table pg_temp.bsp_backward_front")
	require.Contains(t, harness, "truncate table pg_temp.bsp_next_front")
}

func TestBidirectionalShortestPathWarmWorkspaceUsesTruncate(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.reset_bsp_workspace")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.load_bsp_filter_tables")
	require.NotEqual(t, -1, end)
	reset := sqlSchemaUp[start : start+end]

	require.Contains(t, reset, "truncate table pg_temp.bsp_forward_front")
	require.Contains(t, reset, "pg_temp.bsp_resolved_pairs")
	require.NotContains(t, reset, "delete from pg_temp.bsp_")
	require.NotContains(t, sqlSchemaUp, "current_setting('transaction_read_only')")
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

func TestGraphBenchS1DistancePrototypeIsBoundedAndGraphScoped(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.graphbench_s1_distance_bfs")
	require.NotEqual(t, -1, start)
	prototype := sqlSchemaUp[start:]

	require.Contains(t, prototype, "edge.graph_id = target_graph_id")
	require.Contains(t, prototype, "cardinality(visited) + cardinality(next_frontier) > state_limit")
	require.Contains(t, prototype, "overflow := true")
	require.NotContains(t, prototype, "create temporary table")
	require.NotContains(t, prototype, "insert into")
	require.Contains(t, sqlSchemaDown, "drop function if exists graphbench_s1_distance_bfs")
}

func TestCompactShortestExecutorsUseReusableTypedWorkspace(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "create or replace function public.ensure_shortest_dag_workspace()")
	require.Contains(t, sqlSchemaUp, "create or replace function public.reset_shortest_dag_workspace()")
	require.Contains(t, sqlSchemaUp, "on commit preserve rows")
	require.Contains(t, sqlSchemaUp, "create or replace function public.all_shortest_paths_dag(")
	require.Contains(t, sqlSchemaUp, "create or replace function public.shortest_path_compact(")
	require.Contains(t, sqlSchemaUp, "rows 100")
	require.Contains(t, sqlSchemaUp, "rows 1")
	require.Contains(t, sqlSchemaDown, "drop function if exists all_shortest_paths_dag")
	require.Contains(t, sqlSchemaDown, "drop function if exists shortest_path_compact")
}

func TestAllShortestDAGHasExactSmallDepthArmsAndLateEnumeration(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.all_shortest_paths_dag")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.shortest_path_compact")
	require.NotEqual(t, -1, end)
	executor := sqlSchemaUp[start : start+end]

	require.Contains(t, executor, "array[e.id]::int8[]")
	require.Contains(t, executor, "array[e1.id, e2.id]::int8[]")
	require.Contains(t, executor, "e1.id <> e2.id")
	require.Contains(t, executor, "perform public.reset_shortest_dag_workspace()")
	require.Contains(t, executor, "insert into pg_temp.spd_predecessor")
	require.Contains(t, executor, "with recursive shortest_paths")
	require.Contains(t, executor, "if exists (select 1 from pg_temp.spd_next where node_id = target_id) then")
	require.NotContains(t, executor, "execute ")
}

func TestCompactSingletonOverflowFallsBackBeforeReturning(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.shortest_path_compact")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.bsp_workspace_fragment")
	require.NotEqual(t, -1, end)
	executor := sqlSchemaUp[start : start+end]

	require.Contains(t, executor, "retained_state > state_limit")
	require.Contains(t, executor, "if overflowed then")
	require.Contains(t, executor, "with recursive trails")
	require.Contains(t, executor, "not e.id = any(trails.edge_ids)")
	require.NotContains(t, executor, "execute ")
}

func TestLegacyASPFallbackReusesWorkspaceWithoutCatalogSwaps(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.create_unidirectional_pathspace_tables")
	require.NotEqual(t, -1, start)
	legacyWorkspace := sqlSchemaUp[start:]

	require.Contains(t, legacyWorkspace, "create temporary table if not exists forward_front")
	require.Contains(t, legacyWorkspace, "create temporary table if not exists backward_front")
	require.Contains(t, legacyWorkspace, "on commit preserve rows")
	require.Contains(t, legacyWorkspace, "truncate table forward_front, next_front")
	require.Contains(t, legacyWorkspace, "insert into forward_front select * from next_front")
	require.Contains(t, legacyWorkspace, "insert into backward_front select * from next_front")
	require.NotContains(t, legacyWorkspace, "alter table forward_front")
	require.NotContains(t, legacyWorkspace, "alter table backward_front")
}

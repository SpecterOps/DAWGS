package query

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBidirectionalShortestPathWorkspaceIsReusable verifies shortest-path SQL creates reusable session-scoped workspace tables.
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

// TestBidirectionalShortestPathWarmWorkspaceUsesTruncate verifies repeated shortest-path execution clears existing workspace instead of recreating it.
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

// TestBidirectionalShortestPathArrayModeSkipsGenericWorkspace verifies array-backed execution does not initialize table-backed workspace.
func TestBidirectionalShortestPathArrayModeSkipsGenericWorkspace(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "if not use_array_parameters then\nperform public.load_bsp_filter_tables")
	require.Contains(t, sqlSchemaUp, "perform public.reset_bsp_workspace(not use_array_parameters)")
}

// TestBidirectionalShortestPathFragmentsRewriteLegacyFilterTables verifies generated fragments target the current workspace filter tables.
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

// TestLinearPathMaterializerScopesPersistentLookups verifies persistent node and edge lookups include the selected graph ID.
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

// TestLegacyPathMaterializersRequireTargetGraph verifies legacy materializer signatures cannot bypass graph scoping.
func TestLegacyPathMaterializersRequireTargetGraph(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "drop function if exists public.nodes_to_path(int8[])")
	require.Contains(t, sqlSchemaUp, "drop function if exists public.edges_to_path(int8[])")
	require.Contains(t, sqlSchemaUp, "drop function if exists public.ordered_edges_to_path(nodeComposite, edgeComposite[], nodeComposite[])")
	require.Contains(t, sqlSchemaUp, "nodes_to_path(target_graph_id int4")
	require.Contains(t, sqlSchemaUp, "edges_to_path(target_graph_id int4")
	require.Contains(t, sqlSchemaUp, "ordered_edges_to_path(target_graph_id int4")
	require.Contains(t, sqlSchemaUp, "n.graph_id = target_graph_id")
	require.Contains(t, sqlSchemaUp, "r.graph_id = target_graph_id")
	require.Contains(t, sqlSchemaDown, "drop function if exists nodes_to_path(int4, int8[])")
	require.Contains(t, sqlSchemaDown, "drop function if exists nodes_to_path(int8[])")
	require.Contains(t, sqlSchemaDown, "drop function if exists edges_to_path(int4, int8[])")
	require.Contains(t, sqlSchemaDown, "drop function if exists edges_to_path(int8[])")
	require.NotContains(t, sqlSchemaDown, "drop function if exists nodes_to_path;")
	require.NotContains(t, sqlSchemaDown, "drop function if exists edges_to_path;")
}

// TestGraphBenchS1DistancePrototypeIsBoundedAndGraphScoped verifies the benchmark prototype constrains depth and graph identity.
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

// TestCompactShortestExecutorsUseReusableTypedWorkspace verifies compact executors use typed, reusable workspace structures.
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

// TestAllShortestDAGHasExactSmallDepthArmsAndLateEnumeration verifies shallow-depth specializations precede deferred path enumeration.
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
	require.Contains(t, executor, "if exists (select 1 from pg_temp.spd_candidate where depth = search_depth and node_id = target_id) then")
	require.NotContains(t, executor, "execute ")
}

// TestCompactSingletonOverflowFallsBackBeforeReturning verifies compact overflow takes the safe fallback before emitting a result.
func TestCompactSingletonOverflowFallsBackBeforeReturning(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.shortest_path_compact")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.ensure_bidirectional_shortest_path_workspace")
	require.NotEqual(t, -1, end)
	executor := sqlSchemaUp[start : start+end]

	require.Contains(t, executor, "retained_state > state_limit")
	require.Contains(t, executor, "if overflowed then")
	require.Contains(t, executor, "with recursive trails")
	require.Contains(t, executor, "not e.id = any(trails.edge_ids)")
	require.NotContains(t, executor, "execute ")
}

// TestCompactBidirectionalWorkspaceIsVersionedAndDisjoint verifies candidate
// state can coexist with the S4 fallback workspace on a pooled session.
func TestCompactBidirectionalWorkspaceIsVersionedAndDisjoint(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.ensure_bidirectional_shortest_path_workspace")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.shortest_path_bidirectional_compact_v1")
	require.NotEqual(t, -1, end)
	workspace := sqlSchemaUp[start : start+end]

	require.Contains(t, workspace, "expected_version constant int4 := 1")
	require.Contains(t, workspace, "pg_temp.spb_workspace_version")
	require.Contains(t, workspace, "create temporary table spb_front")
	require.Contains(t, workspace, "create temporary table spb_seen")
	require.Contains(t, workspace, "create temporary table spb_active")
	require.Contains(t, workspace, "create temporary table spb_candidate")
	require.Contains(t, workspace, "create temporary table spb_predecessor")
	require.Contains(t, workspace, "queue_order int8 not null")
	require.Contains(t, workspace, "truncate table pg_temp.spb_front")
	require.NotContains(t, workspace, "spd_front")
	require.NotContains(t, workspace, "path int8[]")
}

// TestTraversalRuntimeAttestationIsSessionLocalAndSymmetric verifies the
// timed-invocation receipt cannot persist data or survive schema teardown.
func TestTraversalRuntimeAttestationIsSessionLocalAndSymmetric(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "create temporary table traversal_runtime_attestation_v1")
	require.Contains(t, sqlSchemaUp, "on commit preserve rows")
	require.Contains(t, sqlSchemaUp, "current_setting('dawgs.traversal_runtime_invocation_id', true)")
	require.Contains(t, sqlSchemaUp, "record_count = receipt.record_count + 1")
	require.Contains(t, sqlSchemaUp, "events = receipt.events || jsonb_build_array")
	require.Contains(t, sqlSchemaUp, "'schema_version', 2")
	require.Contains(t, sqlSchemaUp, "if not exists (\nselect 1\nfrom pg_attribute")
	require.Contains(t, sqlSchemaUp, "create or replace function public.read_traversal_runtime_attestation_v1")
	require.Contains(t, sqlSchemaUp, "create or replace function public.clear_traversal_runtime_attestation_v1")
	for _, function := range []string{
		"clear_traversal_runtime_attestation_v1(text)",
		"read_traversal_runtime_attestation_v1(text)",
		"record_requested_traversal_runtime_attestation_v1(text, bool, text)",
		"record_traversal_runtime_attestation_v1(text, text, bool)",
		"begin_traversal_runtime_attestation_v1(text, text)",
		"ensure_traversal_runtime_attestation_workspace_v1()",
	} {
		require.Contains(t, sqlSchemaDown, "drop function if exists "+function)
	}
}

// TestCompactBidirectionalKernelHasExactPreflightBoundsAndFallback verifies all
// candidate gates run before output and overflow delegates to exact S4 state.
func TestCompactBidirectionalKernelHasExactPreflightBoundsAndFallback(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.shortest_path_bidirectional_compact_v1")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.shortest_path_b1_strict_alternating")
	require.NotEqual(t, -1, end)
	kernel := sqlSchemaUp[start : start+end]

	zeroHop := strings.Index(kernel, "if source_id = target_id then")
	oneHop := strings.Index(kernel, "if min_depth <= 1 and max_depth >= 1 then")
	twoHop := strings.Index(kernel, "if min_depth <= 2 and max_depth >= 2 then")
	workspaceReset := strings.Index(kernel, "reset_bidirectional_shortest_path_workspace")
	require.Greater(t, zeroHop, -1)
	require.Greater(t, oneHop, zeroHop)
	require.Greater(t, twoHop, oneHop)
	require.Greater(t, workspaceReset, twoHop)

	require.Contains(t, kernel, "forward_depth + backward_depth >= best_distance")
	require.Contains(t, kernel, "current_setting('transaction_isolation') <> 'repeatable read'")
	require.Contains(t, kernel, "current_setting('transaction_isolation') <> 'serializable'")
	require.Contains(t, kernel, "limit admission_limit + 1")
	require.Contains(t, kernel, "seen_rows + candidate_rows > state_limit")
	require.Contains(t, kernel, "active_rows + frontier_rows + candidate_rows > frontier_limit")
	require.Contains(t, kernel, "predecessor_rows + candidate_rows > predecessor_limit")
	require.Contains(t, kernel, "from public.shortest_path_compact(")
	require.Contains(t, kernel, "with recursive\nforward_witness")
	require.Less(t, strings.Index(kernel, "from public.shortest_path_compact("), strings.Index(kernel, "with recursive\nforward_witness"))
	require.NotContains(t, kernel, "nodeComposite")
	require.NotContains(t, kernel, "edgeComposite")
}

// TestCompactBidirectionalWrappersFreezeSchedulersAndDownMigration verifies the
// two scheduler identities have typed wrappers and symmetric teardown.
func TestCompactBidirectionalWrappersFreezeSchedulersAndDownMigration(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "create or replace function public.shortest_path_b1_strict_alternating(")
	require.Contains(t, sqlSchemaUp, "'strict_alternating_node'")
	require.Contains(t, sqlSchemaUp, "create or replace function public.shortest_path_b2_smaller_current_level(")
	require.Contains(t, sqlSchemaUp, "'smaller_current_level'")
	require.Contains(t, sqlSchemaDown, "drop function if exists shortest_path_b1_strict_alternating(int4, int8, int8, int4, int4, int2[], bool, int8, int8, int8)")
	require.Contains(t, sqlSchemaDown, "drop function if exists shortest_path_b2_smaller_current_level(int4, int8, int8, int4, int4, int2[], bool, int8, int8, int8)")
	require.Contains(t, sqlSchemaDown, "drop function if exists shortest_path_bidirectional_compact_v1(int4, int8, int8, int4, int4, int2[], bool, int8, int8, int8, text)")
	require.Contains(t, sqlSchemaDown, "drop function if exists reset_bidirectional_shortest_path_workspace()")
	require.Contains(t, sqlSchemaDown, "drop function if exists ensure_bidirectional_shortest_path_workspace()")
}

// TestCompactBidirectionalDiagnosticTelemetryIsInvocationScoped verifies the
// untimed replay API records explicit internal counters in a distinct,
// session-local workspace and has symmetric teardown.
func TestCompactBidirectionalDiagnosticTelemetryIsInvocationScoped(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.ensure_bidirectional_shortest_path_telemetry_workspace")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.shortest_path_bidirectional_compact_v1")
	require.NotEqual(t, -1, end)
	telemetry := sqlSchemaUp[start : start+end]

	require.Contains(t, telemetry, "expected_version constant int4 := 1")
	require.Contains(t, telemetry, "create temporary table spb_telemetry_invocation")
	require.Contains(t, telemetry, "create temporary table spb_telemetry_call")
	require.Contains(t, telemetry, "create temporary table spb_telemetry_level")
	require.Contains(t, telemetry, "on commit preserve rows")
	require.Contains(t, telemetry, "set_config('dawgs.spb_diagnostic_invocation_id', invocation_id, true)")
	require.Contains(t, telemetry, "where invocation.invocation_id = target_invocation_id")
	require.Contains(t, telemetry, "'scheduler_actions'")
	require.Contains(t, telemetry, "'candidate_edges'")
	require.Contains(t, telemetry, "'seen_peak'")
	require.Contains(t, telemetry, "'frontier_peak'")
	require.Contains(t, telemetry, "'queue_peak'")
	require.Contains(t, telemetry, "'predecessor_peak'")
	require.Contains(t, telemetry, "'meeting_candidates'")
	require.Contains(t, telemetry, "'fallback_executed'")
	require.NotContains(t, telemetry, "create unlogged table")
	require.NotContains(t, telemetry, "create table public.spb_telemetry")

	kernelStart := strings.Index(sqlSchemaUp, "create or replace function public.shortest_path_bidirectional_compact_v1")
	require.NotEqual(t, -1, kernelStart)
	wrapperStart := strings.Index(sqlSchemaUp[kernelStart:], "create or replace function public.shortest_path_b1_strict_alternating")
	require.NotEqual(t, -1, wrapperStart)
	kernel := sqlSchemaUp[kernelStart : kernelStart+wrapperStart]
	require.Contains(t, kernel, "_start_bidirectional_shortest_path_diagnostic_call_v1")
	require.Contains(t, kernel, "_record_bidirectional_shortest_path_diagnostic_level_v1")
	require.Contains(t, kernel, "_finish_bidirectional_shortest_path_diagnostic_call_v1")
	require.Contains(t, kernel, "if telemetry_search_id is not null then")
	require.Contains(t, kernel, "select count(*) into telemetry_action_candidate_edges")
	require.Contains(t, kernel, "'exact_s4_fallback'")
	require.Contains(t, kernel, "'preflight_zero_hop'")
	require.Contains(t, kernel, "'preflight_one_hop'")
	require.Contains(t, kernel, "'preflight_two_hop'")

	for _, function := range []string{
		"_finish_bidirectional_shortest_path_diagnostic_call_v1",
		"_record_bidirectional_shortest_path_diagnostic_level_v1",
		"_start_bidirectional_shortest_path_diagnostic_call_v1",
		"clear_bidirectional_shortest_path_diagnostic_v1",
		"read_bidirectional_shortest_path_diagnostic_v1",
		"begin_bidirectional_shortest_path_diagnostic_v1",
		"ensure_bidirectional_shortest_path_telemetry_workspace",
	} {
		require.Contains(t, sqlSchemaDown, "drop function if exists "+function)
	}
}

// TestBidirectionalAllShortestWorkspaceSeparatesDiscoveryPredecessorAndOutput
// verifies reusable candidate state is ID-only until the staged output boundary
// and remains disjoint from the exact ASP-A1 fallback workspace.
func TestBidirectionalAllShortestWorkspaceSeparatesDiscoveryPredecessorAndOutput(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.ensure_bidirectional_all_shortest_path_workspace")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.all_shortest_paths_bidirectional_compact_v1")
	require.NotEqual(t, -1, end)
	workspace := sqlSchemaUp[start : start+end]

	require.Contains(t, workspace, "expected_version constant int4 := 1")
	for _, table := range []string{
		"asb_front", "asb_seen", "asb_active", "asb_candidate_node",
		"asb_candidate_predecessor", "asb_predecessor", "asb_path_count", "asb_output",
	} {
		require.Contains(t, workspace, "temporary table "+table)
		require.Contains(t, workspace, "pg_temp."+table)
	}
	require.Contains(t, workspace, "primary key (side, node_id, depth, adjacent_id, edge_id)")
	require.Contains(t, workspace, "edge_ids int8[] not null primary key")
	require.Contains(t, workspace, "on commit preserve rows")
	require.NotContains(t, workspace, "spd_")
	require.NotContains(t, workspace, "spb_")
	// Discovery/frontier tables carry scalar IDs only; arrays are confined to
	// asb_output after path-count admission.
	discoveryEnd := strings.Index(workspace, "create temporary table asb_output")
	require.Greater(t, discoveryEnd, -1)
	require.NotContains(t, workspace[:discoveryEnd], "int8[]")
}

// TestBidirectionalAllShortestKernelProvesOneCutAndGatesBeforeOutput verifies
// scheduler termination, complete equal-depth predecessor retention, and all
// independent cap+1/fallback boundaries are explicit in the SQL kernel.
func TestBidirectionalAllShortestKernelProvesOneCutAndGatesBeforeOutput(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.all_shortest_paths_bidirectional_compact_v1")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.all_shortest_paths_b1_strict_alternating")
	require.NotEqual(t, -1, end)
	kernel := sqlSchemaUp[start : start+end]

	require.Contains(t, kernel, "if min_depth <> 1 then")
	require.Contains(t, kernel, "if max_depth > 64 then")
	require.Contains(t, kernel, "current_setting('transaction_isolation') <> 'repeatable read'")
	require.Contains(t, kernel, "current_setting('transaction_isolation') <> 'serializable'")
	require.Contains(t, kernel, "forward_depth + backward_depth >= best_distance")
	require.Contains(t, kernel, "cut_depth = best_distance / 2")
	require.Contains(t, kernel, "forward_ready_depth >= cut_depth")
	require.Contains(t, kernel, "backward_ready_depth >= best_distance - cut_depth")
	require.Contains(t, kernel, "scheduler = 'strict_alternating_node'")
	require.Contains(t, kernel, "scheduler <> 'smaller_current_level'")
	require.Contains(t, kernel, "seen.depth = active.depth + 1")
	require.Contains(t, kernel, "limit discovery_admission_limit + 1")
	require.Contains(t, kernel, "limit predecessor_admission_limit + 1")
	require.Contains(t, kernel, "path_count_sentinel = path_count_limit + 1")
	require.Contains(t, kernel, "least(path_count_sentinel::numeric")
	require.Contains(t, kernel, "limit enumeration_limit + 1")
	require.Contains(t, kernel, "output_bytes > output_bytes_limit")
	require.Contains(t, kernel, "select distinct stitched.edge_ids")
	require.Contains(t, kernel, "count(distinct path_edge.edge_id)")
	require.Contains(t, kernel, "join backward_paths using (meeting_id)")

	firstFallback := strings.Index(kernel, "perform public.clear_bidirectional_all_shortest_path_workspace();")
	firstPublicOutput := strings.LastIndex(kernel, "from pg_temp.asb_output output")
	require.Greater(t, firstFallback, -1)
	require.Greater(t, firstPublicOutput, firstFallback)
	require.Contains(t, kernel, "from public.all_shortest_paths_dag(")
	require.NotContains(t, kernel, "nodeComposite")
	require.NotContains(t, kernel, "edgeComposite")
}

// TestBidirectionalAllShortestWrappersAndDownMigrationAreSymmetric verifies
// both frozen scheduler identities and every new helper have exact teardown.
func TestBidirectionalAllShortestWrappersAndDownMigrationAreSymmetric(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "create or replace function public.all_shortest_paths_b1_strict_alternating(")
	require.Contains(t, sqlSchemaUp, "create or replace function public.all_shortest_paths_b2_smaller_current_level(")
	require.Contains(t, sqlSchemaUp, "enumeration_limit, output_bytes_limit, 'strict_alternating_node'")
	require.Contains(t, sqlSchemaUp, "enumeration_limit, output_bytes_limit, 'smaller_current_level'")
	for _, signature := range []string{
		"all_shortest_paths_b1_strict_alternating(int4, int8, int8, int4, int4, int2[], bool, int8, int8, int8, int8, int8)",
		"all_shortest_paths_b2_smaller_current_level(int4, int8, int8, int4, int4, int2[], bool, int8, int8, int8, int8, int8)",
		"all_shortest_paths_bidirectional_compact_v1(int4, int8, int8, int4, int4, int2[], bool, int8, int8, int8, int8, int8, text)",
		"clear_bidirectional_all_shortest_path_workspace()",
		"reset_bidirectional_all_shortest_path_workspace()",
		"ensure_bidirectional_all_shortest_path_workspace()",
	} {
		require.Contains(t, sqlSchemaDown, "drop function if exists "+signature)
	}
}

// TestBidirectionalAllShortestDiagnosticTelemetryIsInvocationScoped verifies
// the ASP replay API carries every required search, predecessor, cut, count,
// and output counter in session-local keyed state with symmetric teardown.
func TestBidirectionalAllShortestDiagnosticTelemetryIsInvocationScoped(t *testing.T) {
	start := strings.Index(sqlSchemaUp, "create or replace function public.ensure_bidirectional_all_shortest_path_telemetry_workspace")
	require.NotEqual(t, -1, start)
	end := strings.Index(sqlSchemaUp[start:], "create or replace function public.all_shortest_paths_bidirectional_compact_v1")
	require.NotEqual(t, -1, end)
	telemetry := sqlSchemaUp[start : start+end]

	require.Contains(t, telemetry, "expected_version constant int4 := 1")
	for _, table := range []string{"asb_telemetry_invocation", "asb_telemetry_call", "asb_telemetry_level"} {
		require.Contains(t, telemetry, "create temporary table "+table)
	}
	require.Contains(t, telemetry, "on commit preserve rows")
	require.Contains(t, telemetry, "set_config('dawgs.asb_diagnostic_invocation_id', invocation_id, true)")
	require.Contains(t, telemetry, "where invocation.invocation_id = target_invocation_id")
	for _, counter := range []string{
		"scheduler_actions", "candidate_edges", "distinct_new_nodes", "seen_peak",
		"frontier_peak", "queue_peak", "predecessor_peak", "meeting_candidates",
		"frozen_distance", "witness_rows", "same_depth_predecessor_additions",
		"meeting_nodes", "cut_depth", "path_count_estimate", "path_count_saturated",
		"enumerated_candidates", "duplicate_rejects", "output_paths",
		"output_edge_cells", "output_bytes",
	} {
		require.Contains(t, telemetry, "'"+counter+"'")
	}
	require.NotContains(t, telemetry, "create table public.asb_telemetry")

	kernelStart := strings.Index(sqlSchemaUp, "create or replace function public.all_shortest_paths_bidirectional_compact_v1")
	wrapperStart := strings.Index(sqlSchemaUp[kernelStart:], "create or replace function public.all_shortest_paths_b1_strict_alternating")
	require.NotEqual(t, kernelStart, -1)
	require.NotEqual(t, wrapperStart, -1)
	kernel := sqlSchemaUp[kernelStart : kernelStart+wrapperStart]
	require.Contains(t, kernel, "_start_bidirectional_all_shortest_path_diagnostic_call_v1")
	require.Contains(t, kernel, "_record_bidirectional_all_shortest_path_diagnostic_level_v1")
	require.Contains(t, kernel, "_finish_bidirectional_all_shortest_path_diagnostic_call_v1")
	require.Contains(t, kernel, "'exact_a1_fallback'")
	require.Contains(t, kernel, "'preflight_one_hop'")
	require.Contains(t, kernel, "'preflight_two_hop'")
	require.Contains(t, kernel, "perform public.clear_bidirectional_all_shortest_path_workspace();")

	for _, function := range []string{
		"_finish_bidirectional_all_shortest_path_diagnostic_call_v1",
		"_record_bidirectional_all_shortest_path_diagnostic_level_v1",
		"_start_bidirectional_all_shortest_path_diagnostic_call_v1",
		"clear_bidirectional_all_shortest_path_diagnostic_v1",
		"read_bidirectional_all_shortest_path_diagnostic_v1",
		"begin_bidirectional_all_shortest_path_diagnostic_v1",
		"ensure_bidirectional_all_shortest_path_telemetry_workspace",
	} {
		require.Contains(t, sqlSchemaDown, "drop function if exists "+function)
	}
}

// TestLegacyASPFallbackReusesWorkspaceWithoutCatalogSwaps verifies legacy all-shortest fallback reuses workspace without replacing catalog objects.
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

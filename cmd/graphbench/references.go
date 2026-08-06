// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

const postgresReferenceSchemaVersion = 2

type postgresReferenceSpec struct {
	name               string
	legacyName         string
	architecture       string
	implementationID   string
	stateShape         string
	observationShape   string
	semanticValidation string
	boundary           string
	fullComparator     bool
	sql                string
	parameters         map[string]any
}

func (s *postgresSQLRunner) measureReferences(ctx context.Context, testCase ScaleCase, params map[string]any, idMap opengraph.IDMap, publicObservation []string, warmupIterations, iterations int) ([]PostgresReferenceResult, error) {
	specs, err := s.referenceSpecs(ctx, testCase, params)
	if err != nil {
		return nil, err
	}
	results := make([]PostgresReferenceResult, 0, len(specs))
	for _, spec := range specs {
		spec = normalizedReferenceSpec(spec)
		rowCount, stats, err := measureRawPostgres(ctx, s.db, spec.sql, spec.parameters, warmupIterations, iterations)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spec.name, err)
		}
		var observedRows []string
		if spec.fullComparator {
			var observedCount int64
			err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var err error
				observedCount, observedRows, err = observeRawRows(tx, spec.sql, spec.parameters, idMap, resultContainsNodeIDs(testCase.Expected), resultContainsPaths(testCase.Expected))
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("%s exact observation: %w", spec.name, err)
			}
			if observedCount != rowCount {
				return nil, fmt.Errorf("%s exact observation row count changed from %d to %d", spec.name, rowCount, observedCount)
			}
			if testCase.Expected.RowCount != nil && rowCount != *testCase.Expected.RowCount {
				return nil, fmt.Errorf("%s returned %d rows, expected %d", spec.name, rowCount, *testCase.Expected.RowCount)
			}
			if err := validateExpectedObservations(testCase.Expected, observedRows); err != nil {
				return nil, fmt.Errorf("%s semantic validation: %w", spec.name, err)
			}
			if publicObservation != nil && !slices.Equal(publicObservation, observedRows) {
				return nil, fmt.Errorf("%s exact public observation differs: public=%v reference=%v", spec.name, publicObservation, observedRows)
			}
		}
		for idx := range stats.Samples {
			stats.Samples[idx].Backend = ModePostgresSQL
			stats.Samples[idx].Dataset = testCase.Dataset
			stats.Samples[idx].Case = testCase.Name + "/reference/" + spec.name
		}
		plan, metrics, err := explainRawPostgres(ctx, s.db, spec.sql, spec.parameters)
		if err != nil {
			return nil, fmt.Errorf("%s explain: %w", spec.name, err)
		}
		results = append(results, PostgresReferenceResult{
			SchemaVersion: postgresReferenceSchemaVersion, Name: spec.name, LegacyName: spec.legacyName,
			Architecture: spec.architecture, ImplementationID: spec.implementationID, StateShape: spec.stateShape,
			ObservationShape: spec.observationShape, SemanticValidation: spec.semanticValidation,
			Boundary: spec.boundary, FullComparator: spec.fullComparator,
			SQL: spec.sql, SQLFingerprint: sqlFingerprint(spec.sql), RowCount: rowCount, ObservedRows: observedRows, Stats: stats,
			PostgresPlan: plan, PostgresMetrics: &metrics,
		})
	}
	return results, nil
}

func explainRawPostgres(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any) ([]string, PostgresPlanMetrics, error) {
	var plan []string
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF) "+sqlQuery, params)
		defer result.Close()
		for result.Next() {
			if values := result.Values(); len(values) > 0 {
				plan = append(plan, fmt.Sprint(values[0]))
			}
		}
		return result.Error()
	})
	if err != nil {
		return nil, PostgresPlanMetrics{}, err
	}
	return plan, parsePostgresPlanMetrics(plan), nil
}

func normalizedReferenceSpec(spec postgresReferenceSpec) postgresReferenceSpec {
	if spec.architecture == "" {
		spec.architecture = "component_probe"
	}
	if spec.implementationID == "" {
		spec.implementationID = spec.name + "_v1"
	}
	if spec.stateShape == "" {
		spec.stateShape = "implementation_defined"
	}
	if spec.observationShape == "" {
		spec.observationShape = spec.boundary
	}
	if spec.semanticValidation == "" {
		spec.semanticValidation = "row_count_stability"
		if spec.fullComparator {
			spec.semanticValidation = "exact_public_observation"
		}
	}
	return spec
}

func (s *postgresSQLRunner) referenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	if testCase.Category == "generated_shortest_path" {
		return s.shortestReferenceSpecs(ctx, testCase, params)
	}
	switch testCase.Name {
	case "shortest_distance_bound_pair", "one_shortest_path_bound_pair":
		return s.shortestReferenceSpecs(ctx, testCase, params)
	case "adcs_p1_endpoint_ids", "adcs_p1_path_observed":
		return s.adcsReferenceSpecs(ctx, testCase, params)
	default:
		return nil, nil
	}
}

func (s *postgresSQLRunner) shortestReferenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	probeParams := copyReferenceParams(params)
	probeParams["graph_id"] = s.graphID
	probeParams["max_depth"] = int32(15)
	if testCase.Shape.MaxDepth != nil {
		probeParams["max_depth"] = int32(*testCase.Shape.MaxDepth)
	}
	edgeKinds := make(graph.Kinds, 0, len(testCase.Shape.EdgeKinds))
	for _, name := range testCase.Shape.EdgeKinds {
		edgeKinds = append(edgeKinds, graph.StringKind(name))
	}
	edgeKindIDs, err := s.pgDriver.KindMapper().MapKinds(ctx, edgeKinds)
	if err != nil {
		return nil, fmt.Errorf("map shortest reference edge kinds: %w", err)
	}
	probeParams["edge_kind_ids"] = edgeKindIDs
	search := shortestReferenceSearch()
	values, err := readReferenceRow(ctx, s.db, search+` select depth, node_ids, edge_ids from shortest`, probeParams)
	if err != nil {
		return nil, fmt.Errorf("precompute shortest hydration IDs: %w", err)
	}
	if len(values) != 0 && len(values) != 3 {
		return nil, fmt.Errorf("precompute shortest hydration IDs returned %d columns, expected 3", len(values))
	}
	var edgeIDs []int64
	if len(values) == 3 {
		edgeIDs, err = referenceInt64Slice(values[2])
		if err != nil {
			return nil, fmt.Errorf("decode shortest hydration edge IDs: %w", err)
		}
	}
	return buildShortestReferenceSpecs(testCase, probeParams, edgeIDs), nil
}

func shortestReferenceSearch() string {
	return `with recursive search(node_id, depth, node_ids, edge_ids) as (
  select @start_id::int8, 0, array[@start_id::int8]::int8[], array[]::int8[]
  union all
  select e.end_id, search.depth + 1, search.node_ids || e.end_id, search.edge_ids || e.id
  from search
  join edge e on e.graph_id = @graph_id and e.start_id = search.node_id
  where search.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(search.edge_ids)
), shortest as materialized (
  select depth, node_ids, edge_ids from search
  where node_id = @end_id and depth >= 1
  order by depth limit 1
)`
}

func shortestDistanceReferenceSearch() string {
	return `with recursive search(node_id, depth) as (
  select @start_id::int8, 0
  union
  select e.end_id, search.depth + 1
  from search
  join edge e on e.graph_id = @graph_id and e.start_id = search.node_id
  where search.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
), shortest as materialized (
  select depth from search
  where node_id = @end_id and depth >= 1
  order by depth limit 1
)`
}

func buildShortestReferenceSpecs(testCase ScaleCase, probeParams map[string]any, edgeIDs []int64) []postgresReferenceSpec {
	search := shortestReferenceSearch()
	fullSQL := shortestDistanceReferenceSearch() + ` select depth from shortest`
	boundary := "distance scalar"
	if testCase.Name == "one_shortest_path_bound_pair" {
		fullSQL = search + `
select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  shortest.edge_ids,
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from shortest join node root on root.graph_id = @graph_id and root.id = @start_id`
		boundary = "complete path composite"
	}
	if testCase.Expected.ResultKind == "path_set" {
		fullSQL = search + `
select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  shortest.edge_ids,
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from shortest join node root on root.graph_id = @graph_id and root.id = @start_id`
		boundary = "complete path composite"
	}
	hydrationParams := copyReferenceParams(probeParams)
	hydrationParams["edge_ids"] = edgeIDs
	hydrationSQL := `select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  @edge_ids::int8[],
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from node root where root.graph_id = @graph_id and root.id = @start_id`
	specs := []postgresReferenceSpec{
		{name: "round_trip", boundary: "prepared protocol and transaction", sql: `select 1`, parameters: nil},
		{name: "endpoint_validation", boundary: "validated endpoint IDs", sql: `select id from node where graph_id = @graph_id and id = any(array[@start_id::int8, @end_id::int8]) order by id`, parameters: probeParams},
		{name: "minimum_graph_access", boundary: "root adjacency edge IDs", sql: `select e.id from edge e where e.graph_id = @graph_id and e.start_id = @start_id and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[])) order by e.id`, parameters: probeParams},
		{name: "search_ordered_ids", boundary: "depth plus ordered node/edge IDs", sql: search + ` select depth, node_ids, edge_ids from shortest`, parameters: probeParams},
	}
	if edgeIDs != nil {
		specs = append(specs, postgresReferenceSpec{name: "hydration_only", boundary: "complete path composite from precomputed ordered edge IDs", sql: hydrationSQL, parameters: hydrationParams})
	}
	specs = append(specs,
		postgresReferenceSpec{name: "s3_unidirectional_trail_cte", legacyName: "complete_reference_s1_array_cte", architecture: "S3-U", implementationID: "inline_recursive_cte_unidirectional_v2", stateShape: shortestS3UStateShape(testCase), observationShape: boundary, semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: fullSQL, parameters: probeParams},
		postgresReferenceSpec{name: "s3_bidirectional_trail_cte", legacyName: "candidate_s2_bidirectional_cte", architecture: "S3-B", implementationID: "inline_recursive_cte_bidirectional_trails_v1", stateShape: "paired per-row relationship trail arrays", observationShape: boundary, semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: shortestBidirectionalReferenceSQL(testCase), parameters: probeParams},
	)
	return specs
}

func shortestS3UStateShape(testCase ScaleCase) string {
	if testCase.Expected.ResultKind == "path_set" || testCase.Name == "one_shortest_path_bound_pair" {
		return "per-row node and relationship trail arrays"
	}
	return "distance frontier node and depth only; no path or predecessor state"
}

func shortestBidirectionalReferenceSQL(testCase ScaleCase) string {
	search := `with recursive
forward(node_id, depth, edge_ids) as (
  select @start_id::int8, 0, array[]::int8[]
  union all
  select e.end_id, forward.depth + 1, forward.edge_ids || e.id
  from forward join edge e on e.graph_id = @graph_id and e.start_id = forward.node_id
  where forward.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(forward.edge_ids)
), backward(node_id, depth, edge_ids) as (
  select @end_id::int8, 0, array[]::int8[]
  union all
  select e.start_id, backward.depth + 1, e.id || backward.edge_ids
  from backward join edge e on e.graph_id = @graph_id and e.end_id = backward.node_id
  where backward.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(backward.edge_ids)
), shortest as materialized (
  select forward.depth + backward.depth as depth, forward.edge_ids || backward.edge_ids as edge_ids
  from forward join backward using (node_id)
  where forward.depth + backward.depth between 1 and @max_depth
    and not exists (select 1 from unnest(forward.edge_ids) edge_id where edge_id = any(backward.edge_ids))
  order by depth limit 1
)`
	if testCase.Expected.ResultKind != "path_set" {
		return search + ` select depth from shortest`
	}
	return search + `
select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  shortest.edge_ids,
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from shortest join node root on root.graph_id = @graph_id and root.id = @start_id`
}

func (s *postgresSQLRunner) adcsReferenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	kindNames := []string{"Group", "EnterpriseCA", "NTAuthStore", "Domain", "MemberOf", "Enroll", "TrustedForNTAuth", "NTAuthStoreFor"}
	probeParams := copyReferenceParams(params)
	probeParams["graph_id"] = s.graphID
	for _, name := range kindNames {
		kindID, err := s.pgDriver.KindMapper().MapKind(ctx, graph.StringKind(name))
		if err != nil {
			return nil, fmt.Errorf("map reference kind %s: %w", name, err)
		}
		probeParams[name+"_kind"] = kindID
	}
	probeParams["max_depth"] = int32(15)
	specs := buildADCSReferenceSpecs(testCase, probeParams)
	searchIdx := referenceSpecIndex(specs, "search_ordered_ids")
	values, err := readReferenceRow(ctx, s.db, specs[searchIdx].sql, specs[searchIdx].parameters)
	if err != nil {
		return nil, fmt.Errorf("precompute ADCS hydration IDs: %w", err)
	}
	if len(values) != 2 {
		return nil, fmt.Errorf("precompute ADCS hydration IDs returned %d columns, expected 2", len(values))
	}
	boundaryNodeIDs, err := referenceInt64Slice(values[0])
	if err != nil || len(boundaryNodeIDs) == 0 {
		return nil, fmt.Errorf("decode ADCS hydration boundary node IDs: %w", err)
	}
	edgeIDs, err := referenceInt64Slice(values[1])
	if err != nil {
		return nil, fmt.Errorf("decode ADCS hydration edge IDs: %w", err)
	}
	hydrationParams := copyReferenceParams(probeParams)
	hydrationParams["root_id"] = boundaryNodeIDs[0]
	hydrationParams["edge_ids"] = edgeIDs
	hydration := postgresReferenceSpec{
		name: "hydration_only", boundary: "one complete path composite from precomputed ordered edge IDs",
		sql: `select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  @edge_ids::int8[],
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from node root where root.graph_id = @graph_id and root.id = @root_id`,
		parameters: hydrationParams,
	}
	completeIdx := referenceSpecIndex(specs, "complete_reference")
	specs = append(specs, postgresReferenceSpec{})
	copy(specs[completeIdx+1:], specs[completeIdx:])
	specs[completeIdx] = hydration
	return specs, nil
}

func buildADCSReferenceSpecs(testCase ScaleCase, probeParams map[string]any) []postgresReferenceSpec {
	search := `with recursive roots(root_id) as materialized (
  select n.id from node n
  where n.graph_id = @graph_id
    and @Group_kind::int2 = any(n.kind_ids)
    and n.properties ->> 'objectid' = @objectid
), members(root_id, node_id, edge_ids, depth) as (
  select root_id, root_id, array[]::int8[], 0 from roots
  union all
  select members.root_id, e.end_id, members.edge_ids || e.id, members.depth + 1
  from members join edge e
    on e.graph_id = @graph_id and e.start_id = members.node_id and e.kind_id = @MemberOf_kind
  where members.depth < @max_depth and e.id != all(members.edge_ids)
), paths as materialized (
  select members.root_id,
         members.edge_ids || enroll.id || trusted.id || store_for.id as edge_ids,
         array[members.root_id, members.node_id, ca.id, store.id, domain_node.id]::int8[] as boundary_node_ids
  from members
  join edge enroll on enroll.graph_id = @graph_id and enroll.start_id = members.node_id and enroll.kind_id = @Enroll_kind and enroll.id != all(members.edge_ids)
  join node ca on ca.graph_id = @graph_id and ca.id = enroll.end_id and @EnterpriseCA_kind::int2 = any(ca.kind_ids)
  join edge trusted on trusted.graph_id = @graph_id and trusted.start_id = ca.id and trusted.kind_id = @TrustedForNTAuth_kind
    and trusted.id != enroll.id and trusted.id != all(members.edge_ids)
  join node store on store.graph_id = @graph_id and store.id = trusted.end_id and @NTAuthStore_kind::int2 = any(store.kind_ids)
  join edge store_for on store_for.graph_id = @graph_id and store_for.start_id = store.id and store_for.kind_id = @NTAuthStoreFor_kind
    and store_for.id != enroll.id and store_for.id != trusted.id and store_for.id != all(members.edge_ids)
  join node domain_node on domain_node.graph_id = @graph_id and domain_node.id = store_for.end_id and @Domain_kind::int2 = any(domain_node.kind_ids)
)`
	fullSQL := search + ` select boundary_node_ids[3], boundary_node_ids[5] from paths`
	boundary := "endpoint ID pairs"
	if testCase.Name == "adcs_p1_path_observed" {
		fullSQL = search + `
select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  paths.edge_ids,
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from paths join node root on root.graph_id = @graph_id and root.id = paths.root_id`
		boundary = "complete path composite"
	}
	return []postgresReferenceSpec{
		{name: "round_trip", boundary: "prepared protocol and transaction", sql: `select 1`},
		{name: "endpoint_validation", boundary: "validated root ID", sql: `select n.id from node n where n.graph_id = @graph_id and @Group_kind::int2 = any(n.kind_ids) and n.properties ->> 'objectid' = @objectid`, parameters: probeParams},
		{name: "minimum_graph_access", boundary: "root adjacency edge IDs", sql: search[:strings.Index(search, "), members")] + `) select e.id from roots join edge e on e.graph_id = @graph_id and e.start_id = roots.root_id and e.kind_id = @MemberOf_kind order by e.id`, parameters: probeParams},
		{name: "search_ordered_ids", boundary: "ordered node/edge IDs without hydration", sql: search + ` select boundary_node_ids, edge_ids from paths`, parameters: probeParams},
		{name: "complete_reference", boundary: boundary, fullComparator: true, sql: fullSQL, parameters: probeParams},
	}
}

func referenceSpecIndex(specs []postgresReferenceSpec, name string) int {
	for idx, spec := range specs {
		if spec.name == name {
			return idx
		}
	}
	panic("missing PostgreSQL reference spec " + name)
}

func readReferenceRow(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any) ([]any, error) {
	var values []any
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(sqlQuery, params)
		defer result.Close()
		if !result.Next() {
			if err := result.Error(); err != nil {
				return err
			}
			return nil
		}
		values = append(values, result.Values()...)
		return result.Error()
	})
	return values, err
}

func referenceInt64Slice(value any) ([]int64, error) {
	switch typed := value.(type) {
	case []int64:
		return append([]int64(nil), typed...), nil
	case []int32:
		result := make([]int64, len(typed))
		for idx, item := range typed {
			result[idx] = int64(item)
		}
		return result, nil
	case []any:
		result := make([]int64, len(typed))
		for idx, item := range typed {
			switch integer := item.(type) {
			case int64:
				result[idx] = integer
			case int32:
				result[idx] = int64(integer)
			default:
				return nil, fmt.Errorf("array item %d has type %T", idx, item)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected integer array, got %T", value)
	}
}

func copyReferenceParams(params map[string]any) map[string]any {
	copy := make(map[string]any, len(params)+10)
	for name, value := range params {
		copy[name] = value
	}
	return copy
}

func measureRawPostgres(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any, warmupIterations, iterations int) (int64, DurationStats, error) {
	run := func() (int64, error) {
		var count int64
		err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result := tx.Raw(sqlQuery, params)
			defer result.Close()
			for result.Next() {
				count++
				_ = result.Values()
			}
			return result.Error()
		})
		return count, err
	}
	coldStart := time.Now()
	rowCount, err := run()
	if err != nil {
		return 0, DurationStats{}, err
	}
	coldDuration := time.Since(coldStart)
	for range warmupIterations {
		nextCount, err := run()
		if err != nil {
			return 0, DurationStats{}, err
		}
		if nextCount != rowCount {
			return 0, DurationStats{}, fmt.Errorf("reference row count changed from %d to %d", rowCount, nextCount)
		}
	}
	durations := make([]time.Duration, iterations)
	for idx := range iterations {
		start := time.Now()
		nextCount, err := run()
		if err != nil {
			return 0, DurationStats{}, err
		}
		if nextCount != rowCount {
			return 0, DurationStats{}, fmt.Errorf("reference row count changed from %d to %d", rowCount, nextCount)
		}
		durations[idx] = time.Since(start)
	}
	stats, err := computeDurationStats(durations)
	if err != nil {
		return 0, DurationStats{}, err
	}
	stats.WarmupIterations = warmupIterations
	stats.Samples = append([]LatencySample{{Iteration: 0, Classification: "cold", Duration: coldDuration}}, stats.Samples...)
	return rowCount, stats, nil
}

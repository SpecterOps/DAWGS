// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

const postgresReferenceSchemaVersion = 3

var postgresReferenceArms = []string{
	"round_trip",
	"endpoint_validation",
	"fixed_suffix_rows",
	"minimum_graph_access",
	"search_ordered_ids",
	"current_forward_ordered_ids",
	"a1a_root_reuse_ordered_ids",
	"a1b_late_hydration_ordered_ids",
	"a2_factored_suffix_forward_ordered_ids",
	"a3_suffix_seeded_reverse_ordered_ids",
	"a4_viability_forward_ordered_ids",
	"hydration_only",
	"complete_reference",
	"a1a_root_reuse_complete",
	"a1b_late_hydration_complete",
	"a2_factored_suffix_forward_complete",
	"a3_suffix_seeded_reverse_complete",
	"a4_viability_forward_complete",
	"m0_directed_hydration_only",
	"m1_ordered_ids_hydration_only",
	"s3_unidirectional_trail_cte",
	"s3_unidirectional_cte_m0_directed",
	"s3_unidirectional_cte_m1_ordered_ids",
	"s3_bidirectional_trail_cte",
	"s1_array_bfs_distance",
}

func validPostgresReferenceArm(name string) bool {
	return slices.Contains(postgresReferenceArms, name)
}

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
	aaAliasOf          string
	timingBoundary     string
	sql                string
	parameters         map[string]any
	validationSQL      string
	validationParams   map[string]any
}

func (s *postgresSQLRunner) measureReferences(ctx context.Context, testCase ScaleCase, params map[string]any, idMap opengraph.IDMap, publicObservation []string, warmupIterations, iterations int) ([]PostgresReferenceResult, error) {
	specs, err := s.referenceSpecs(ctx, testCase, params)
	if err != nil {
		return nil, err
	}
	for idx := range specs {
		specs[idx] = normalizedReferenceSpec(specs[idx])
	}
	if err := validateReferenceSpecs(specs); err != nil {
		return nil, fmt.Errorf("validate PostgreSQL reference identities: %w", err)
	}
	if len(s.referenceArms) > 0 {
		specs, err = selectReferenceSpecs(specs, s.referenceArms)
		if err != nil {
			return nil, fmt.Errorf("%w for %s/%s", err, testCase.Dataset, testCase.Name)
		}
	}
	specs = referenceSpecsForRound(specs, s.round)
	results := make([]PostgresReferenceResult, 0, len(specs))
	for _, spec := range specs {
		rowCount, stats, err := measureRawPostgres(ctx, s.db, spec.sql, spec.parameters, warmupIterations, iterations)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spec.name, err)
		}
		var observedRows []string
		if spec.fullComparator || spec.validationSQL != "" {
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
			if spec.validationSQL != "" {
				var validationCount int64
				var validationRows []string
				err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
					var err error
					validationCount, validationRows, err = observeRawRows(tx, spec.validationSQL, spec.validationParams, idMap, resultContainsNodeIDs(testCase.Expected), resultContainsPaths(testCase.Expected))
					return err
				})
				if err != nil {
					return nil, fmt.Errorf("%s validation reference observation: %w", spec.name, err)
				}
				if validationCount != observedCount || !slices.Equal(validationRows, observedRows) {
					return nil, fmt.Errorf("%s materialized observation differs from validation reference: candidate=%v reference=%v", spec.name, observedRows, validationRows)
				}
			}
			if testCase.Expected.RowCount != nil && rowCount != *testCase.Expected.RowCount {
				return nil, fmt.Errorf("%s returned %d rows, expected %d", spec.name, rowCount, *testCase.Expected.RowCount)
			}
			if spec.semanticValidation != "exact_ordered_ids" {
				if err := validateExpectedObservations(testCase.Expected, observedRows); err != nil {
					return nil, fmt.Errorf("%s semantic validation: %w", spec.name, err)
				}
				if publicObservation != nil && !slices.Equal(publicObservation, observedRows) && !validAlternativeShortestPathObservation(testCase, publicObservation, observedRows) {
					return nil, fmt.Errorf("%s exact public observation differs: public=%v reference=%v", spec.name, publicObservation, observedRows)
				}
			}
		}
		for idx := range stats.Samples {
			stats.Samples[idx].Backend = ModePostgresSQL
			stats.Samples[idx].Dataset = testCase.Dataset
			stats.Samples[idx].Case = testCase.Name + "/reference/" + spec.name
		}
		plan, planJSON, metrics, err := explainRawPostgres(ctx, s.db, spec.sql, spec.parameters)
		if err != nil {
			return nil, fmt.Errorf("%s explain: %w", spec.name, err)
		}
		results = append(results, PostgresReferenceResult{
			SchemaVersion: postgresReferenceSchemaVersion, Name: spec.name, LegacyName: spec.legacyName,
			Architecture: spec.architecture, ImplementationID: spec.implementationID, StateShape: spec.stateShape,
			ObservationShape: spec.observationShape, SemanticValidation: spec.semanticValidation,
			Boundary: spec.boundary, TimingBoundary: spec.timingBoundary, FullComparator: spec.fullComparator, AAAliasOf: spec.aaAliasOf,
			SQL: spec.sql, SQLFingerprint: normalizedSQLFingerprint(spec.sql), RowCount: rowCount, ObservedRows: observedRows, Stats: stats,
			PostgresPlan: plan, PostgresPlanJSON: planJSON, PostgresMetrics: &metrics,
		})
	}
	return results, nil
}

func selectReferenceSpecs(specs []postgresReferenceSpec, names []string) ([]postgresReferenceSpec, error) {
	selected := make([]postgresReferenceSpec, 0, len(names))
	for _, name := range names {
		idx := referenceSpecIndexOrMissing(specs, name)
		if idx < 0 {
			return nil, fmt.Errorf("requested PostgreSQL reference arm %q is unavailable", name)
		}
		selected = append(selected, specs[idx])
	}
	return selected, nil
}

func explainRawPostgres(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any) ([]string, json.RawMessage, PostgresPlanMetrics, error) {
	var plan []string
	var planJSON json.RawMessage
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF) "+sqlQuery, params)
		defer result.Close()
		for result.Next() {
			if values := result.Values(); len(values) > 0 {
				plan = append(plan, fmt.Sprint(values[0]))
			}
		}
		if err := result.Error(); err != nil {
			return err
		}
		jsonResult := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF, FORMAT JSON) "+sqlQuery, params)
		defer jsonResult.Close()
		if jsonResult.Next() && len(jsonResult.Values()) > 0 {
			var err error
			planJSON, err = encodePostgresPlanJSON(jsonResult.Values()[0])
			if err != nil {
				return err
			}
		}
		return jsonResult.Error()
	})
	if err != nil {
		return nil, nil, PostgresPlanMetrics{}, err
	}
	metrics, err := parsePostgresPlanJSONMetrics(planJSON)
	if err != nil {
		return nil, nil, PostgresPlanMetrics{}, err
	}
	return plan, planJSON, metrics, nil
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
		spec.observationShape = "component_observation"
	}
	if spec.timingBoundary == "" {
		spec.timingBoundary = "raw_pgx"
	}
	if spec.semanticValidation == "" {
		spec.semanticValidation = "row_count_stability"
		if spec.fullComparator {
			spec.semanticValidation = "exact_public_observation"
		}
	}
	return spec
}

func normalizedSQLFingerprint(sql string) string {
	return sqlFingerprint(strings.Join(strings.Fields(sql), " "))
}

func validateReferenceSpecs(specs []postgresReferenceSpec) error {
	byName := make(map[string]postgresReferenceSpec, len(specs))
	byImplementation := make(map[string]postgresReferenceSpec, len(specs))
	byFingerprint := make(map[string]postgresReferenceSpec, len(specs))
	for _, spec := range specs {
		if spec.name == "" || spec.architecture == "" || spec.implementationID == "" || spec.stateShape == "" || spec.observationShape == "" || spec.timingBoundary == "" || spec.semanticValidation == "" {
			return fmt.Errorf("reference %q has an incomplete architecture identity", spec.name)
		}
		if _, found := byName[spec.name]; found {
			return fmt.Errorf("duplicate reference name %q", spec.name)
		}
		fingerprint := normalizedSQLFingerprint(spec.sql)
		if previous, found := byImplementation[spec.implementationID]; found && (previous.stateShape != spec.stateShape || previous.observationShape != spec.observationShape || normalizedSQLFingerprint(previous.sql) != fingerprint) {
			return fmt.Errorf("implementation %q changes state, observation, or SQL identity between %q and %q", spec.implementationID, previous.name, spec.name)
		}
		if previous, found := byFingerprint[fingerprint]; found {
			previousCanonical := previous.name
			if previous.aaAliasOf != "" {
				previousCanonical = previous.aaAliasOf
			}
			specCanonical := spec.name
			if spec.aaAliasOf != "" {
				specCanonical = spec.aaAliasOf
			}
			if specCanonical != previousCanonical {
				return fmt.Errorf("references %q and %q have identical normalized SQL without a declared A/A alias", previous.name, spec.name)
			}
			canonical, alias := byName[previousCanonical], spec
			if canonical.name == "" {
				canonical = previous
			}
			if parameterShape(canonical.parameters) != parameterShape(alias.parameters) || canonical.observationShape != alias.observationShape || canonical.timingBoundary != alias.timingBoundary || canonical.fullComparator != alias.fullComparator || canonical.semanticValidation != alias.semanticValidation {
				return fmt.Errorf("A/A alias %q does not match canonical arm %q at an identical comparison boundary", alias.name, canonical.name)
			}
		}
		byName[spec.name] = spec
		byImplementation[spec.implementationID] = spec
		byFingerprint[fingerprint] = spec
	}
	for _, spec := range specs {
		if spec.aaAliasOf == "" {
			continue
		}
		canonical, found := byName[spec.aaAliasOf]
		if !found {
			return fmt.Errorf("A/A alias %q names missing canonical arm %q", spec.name, spec.aaAliasOf)
		}
		if normalizedSQLFingerprint(spec.sql) != normalizedSQLFingerprint(canonical.sql) {
			return fmt.Errorf("A/A alias %q SQL differs from canonical arm %q", spec.name, canonical.name)
		}
	}
	return nil
}

func parameterShape(parameters map[string]any) string {
	names := make([]string, 0, len(parameters))
	for name := range parameters {
		names = append(names, name)
	}
	sort.Strings(names)
	var shape strings.Builder
	for _, name := range names {
		shape.WriteString(name)
		shape.WriteByte('=')
		if parameters[name] == nil {
			shape.WriteString("<nil>")
		} else {
			shape.WriteString(reflect.TypeOf(parameters[name]).String())
		}
		shape.WriteByte(';')
	}
	return shape.String()
}

func validAlternativeShortestPathObservation(testCase ScaleCase, publicRows, referenceRows []string) bool {
	if testCase.Expected.ResultKind != "path_set" || strings.Contains(strings.ToLower(testCase.Cypher), "allshortestpaths") {
		return false
	}
	provablyOutbound, err := shortestReferenceIsProvablyOutbound(testCase.Cypher)
	if err != nil || !provablyOutbound {
		return false
	}

	publicPath, publicOK := singleStablePathObservation(publicRows)
	referencePath, referenceOK := singleStablePathObservation(referenceRows)
	if !publicOK || !referenceOK || !validOutboundStablePath(publicPath, testCase.Shape.EdgeKinds) || !validOutboundStablePath(referencePath, testCase.Shape.EdgeKinds) {
		return false
	}
	if len(publicPath.Relationships) != len(referencePath.Relationships) {
		return false
	}

	publicStart, publicEnd := publicPath.Nodes[0].Identity, publicPath.Nodes[len(publicPath.Nodes)-1].Identity
	referenceStart, referenceEnd := referencePath.Nodes[0].Identity, referencePath.Nodes[len(referencePath.Nodes)-1].Identity
	return publicStart == referenceStart && publicEnd == referenceEnd
}

func singleStablePathObservation(rows []string) (stablePathObservation, bool) {
	if len(rows) != 1 {
		return stablePathObservation{}, false
	}

	var columns []json.RawMessage
	if err := json.Unmarshal([]byte(rows[0]), &columns); err != nil || len(columns) != 1 {
		return stablePathObservation{}, false
	}

	var path stablePathObservation
	if err := json.Unmarshal(columns[0], &path); err != nil {
		return stablePathObservation{}, false
	}
	return path, true
}

func validOutboundStablePath(path stablePathObservation, allowedKinds []string) bool {
	if len(path.Nodes) == 0 || len(path.Nodes) != len(path.Relationships)+1 {
		return false
	}
	for _, node := range path.Nodes {
		if strings.HasPrefix(node.Identity, "unmapped-node:") {
			return false
		}
	}
	for idx, relationship := range path.Relationships {
		if relationship.Start != path.Nodes[idx].Identity || relationship.End != path.Nodes[idx+1].Identity {
			return false
		}
		if len(allowedKinds) != 0 && !slices.Contains(allowedKinds, relationship.Kind) {
			return false
		}
	}
	return true
}

func referenceSpecsForRound(specs []postgresReferenceSpec, round int) []postgresReferenceSpec {
	if len(specs) == 5 && round > 0 {
		// Ten-sequence Williams/carryover-balanced schedule predeclared by the
		// ADCS tournament. Slots are the caller-selected arms, so B1/B2/B3 can
		// share this schedule without hard-coding architecture names here.
		schedule := [10][5]int{
			{0, 1, 4, 2, 3}, {1, 2, 0, 3, 4}, {2, 3, 1, 4, 0}, {3, 4, 2, 0, 1}, {4, 0, 3, 1, 2},
			{3, 2, 4, 1, 0}, {4, 3, 0, 2, 1}, {0, 4, 1, 3, 2}, {1, 0, 2, 4, 3}, {2, 1, 3, 0, 4},
		}
		row := schedule[(round-1)%len(schedule)]
		ordered := make([]postgresReferenceSpec, len(specs))
		for idx, slot := range row {
			ordered[idx] = specs[slot]
		}
		return ordered
	}
	ordered := append([]postgresReferenceSpec(nil), specs...)
	if round > 0 && round%2 == 0 {
		slices.Reverse(ordered)
	}
	return ordered
}

func (s *postgresSQLRunner) referenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	if testCase.Category == "generated_adcs" {
		return s.adcsReferenceSpecs(ctx, testCase, params)
	}
	if testCase.Category == "generated_shortest_path" {
		// The singleton references return one shortest path. They are not an
		// all-shortest predecessor-DAG implementation and therefore cannot serve
		// as an exact comparator for allShortestPaths.
		if strings.Contains(strings.ToLower(testCase.Cypher), "allshortestpaths") {
			return nil, nil
		}
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
	probeParams["min_depth"] = int32(1)
	if testCase.Shape.MinDepth != nil {
		probeParams["min_depth"] = int32(*testCase.Shape.MinDepth)
	}
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
	direction, err := shortestReferenceDirection(testCase.Cypher)
	if err != nil {
		return nil, fmt.Errorf("classify shortest reference direction: %w", err)
	}
	if direction == graph.DirectionBoth {
		return nil, nil
	}
	rootParameter, terminalParameter, err := shortestReferenceEndpointParameters(testCase.Cypher)
	if err != nil {
		return nil, fmt.Errorf("resolve shortest reference endpoint parameters: %w", err)
	}
	searchParams := copyReferenceParams(probeParams)
	searchParams["start_id"] = probeParams[rootParameter]
	searchParams["end_id"] = probeParams[terminalParameter]
	search := shortestReferenceSearchForDirection(direction)
	values, err := readReferenceRow(ctx, s.db, search+` select depth, node_ids, edge_ids from shortest`, searchParams)
	if err != nil {
		return nil, fmt.Errorf("precompute shortest hydration IDs: %w", err)
	}
	if len(values) != 0 && len(values) != 3 {
		return nil, fmt.Errorf("precompute shortest hydration IDs returned %d columns, expected 3", len(values))
	}
	var nodeIDs, edgeIDs []int64
	if len(values) == 3 {
		nodeIDs, err = referenceInt64Slice(values[1])
		if err != nil {
			return nil, fmt.Errorf("decode shortest hydration node IDs: %w", err)
		}
		edgeIDs, err = referenceInt64Slice(values[2])
		if err != nil {
			return nil, fmt.Errorf("decode shortest hydration edge IDs: %w", err)
		}
	}
	return buildShortestReferenceSpecs(testCase, searchParams, nodeIDs, edgeIDs, direction), nil
}

func shortestReferenceEndpointParameters(query string) (string, string, error) {
	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return "", "", err
	}
	if parsed == nil || parsed.SingleQuery == nil || parsed.SingleQuery.SinglePartQuery == nil || parsed.SingleQuery.MultiPartQuery != nil {
		return "", "", fmt.Errorf("expected a single-part shortest query")
	}
	for _, readingClause := range parsed.SingleQuery.SinglePartQuery.ReadingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		bindings := map[string]string{}
		if readingClause.Match.Where != nil {
			for _, expression := range readingClause.Match.Where.Expressions {
				collectIdentityParameterBindings(expression, bindings)
			}
		}
		for _, patternPart := range readingClause.Match.Pattern {
			if patternPart == nil || (!patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern) || len(patternPart.PatternElements) < 3 {
				continue
			}
			root, rootOK := patternPart.PatternElements[0].AsNodePattern()
			terminal, terminalOK := patternPart.PatternElements[len(patternPart.PatternElements)-1].AsNodePattern()
			if !rootOK || !terminalOK || root.Variable == nil || terminal.Variable == nil {
				return "", "", fmt.Errorf("shortest reference endpoints must have variables")
			}
			rootParameter, rootBound := bindings[root.Variable.Symbol]
			terminalParameter, terminalBound := bindings[terminal.Variable.Symbol]
			if !rootBound || !terminalBound {
				return "", "", fmt.Errorf("shortest reference endpoints must have parameter ID equalities")
			}
			return rootParameter, terminalParameter, nil
		}
	}
	return "", "", fmt.Errorf("shortest pattern not found")
}

func collectIdentityParameterBindings(expression cypher.Expression, bindings map[string]string) {
	switch typed := expression.(type) {
	case *cypher.Conjunction:
		for _, child := range typed.Expressions {
			collectIdentityParameterBindings(child, bindings)
		}
	case *cypher.Parenthetical:
		collectIdentityParameterBindings(typed.Expression, bindings)
	case *cypher.Comparison:
		if typed == nil || len(typed.Partials) != 1 || typed.Partials[0].Operator != cypher.OperatorEquals {
			return
		}
		if symbol, ok := identityReferenceSymbol(typed.Left); ok {
			if parameter, ok := typed.Partials[0].Right.(*cypher.Parameter); ok {
				bindings[symbol] = parameter.Symbol
			}
		}
		if symbol, ok := identityReferenceSymbol(typed.Partials[0].Right); ok {
			if parameter, ok := typed.Left.(*cypher.Parameter); ok {
				bindings[symbol] = parameter.Symbol
			}
		}
	}
}

func identityReferenceSymbol(expression cypher.Expression) (string, bool) {
	function, ok := expression.(*cypher.FunctionInvocation)
	if !ok || function == nil || !strings.EqualFold(function.Name, cypher.IdentityFunction) || len(function.Arguments) != 1 {
		return "", false
	}
	variable, ok := function.Arguments[0].(*cypher.Variable)
	if !ok || variable == nil || variable.Symbol == "" {
		return "", false
	}
	return variable.Symbol, true
}

func shortestReferenceIsProvablyOutbound(query string) (bool, error) {
	direction, err := shortestReferenceDirection(query)
	return direction == graph.DirectionOutbound, err
}

func shortestReferenceDirection(query string) (graph.Direction, error) {
	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return graph.DirectionBoth, err
	}
	if parsed == nil || parsed.SingleQuery == nil || parsed.SingleQuery.SinglePartQuery == nil || parsed.SingleQuery.MultiPartQuery != nil {
		return graph.DirectionBoth, nil
	}

	var (
		shortestParts int
		relationships int
		direction     graph.Direction
	)
	for _, readingClause := range parsed.SingleQuery.SinglePartQuery.ReadingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}
		for _, patternPart := range readingClause.Match.Pattern {
			if patternPart == nil || !patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern {
				continue
			}
			shortestParts++
			for _, patternElement := range patternPart.PatternElements {
				if relationship, isRelationship := patternElement.AsRelationshipPattern(); isRelationship {
					relationships++
					direction = relationship.Direction
				}
			}
		}
	}

	if shortestParts != 1 || relationships != 1 {
		return graph.DirectionBoth, nil
	}
	return direction, nil
}

func shortestReferenceSearch() string {
	return shortestReferenceSearchForDirection(graph.DirectionOutbound)
}

func shortestReferenceSearchForDirection(direction graph.Direction) string {
	edgeJoin, nextNode := "e.start_id = search.node_id", "e.end_id"
	if direction == graph.DirectionInbound {
		edgeJoin, nextNode = "e.end_id = search.node_id", "e.start_id"
	}
	return `with recursive search(node_id, depth, node_ids, edge_ids) as (
  select @start_id::int8, 0, array[@start_id::int8]::int8[], array[]::int8[]
  union all
  select ` + nextNode + `, search.depth + 1, search.node_ids || ` + nextNode + `, search.edge_ids || e.id
  from search
  join edge e on e.graph_id = @graph_id and ` + edgeJoin + `
  where search.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(search.edge_ids)
), shortest as materialized (
  select depth, node_ids, edge_ids from search
  where node_id = @end_id and depth >= @min_depth
  order by depth, edge_ids limit 1
)`
}

func shortestEdgeReferenceSearch(direction graph.Direction) string {
	edgeJoin, nextNode := "e.start_id = search.node_id", "e.end_id"
	if direction == graph.DirectionInbound {
		edgeJoin, nextNode = "e.end_id = search.node_id", "e.start_id"
	}
	return `with recursive search(node_id, depth, edge_ids) as (
  select @start_id::int8, 0, array[]::int8[]
  union all
  select ` + nextNode + `, search.depth + 1, search.edge_ids || e.id
  from search
  join edge e on e.graph_id = @graph_id and ` + edgeJoin + `
  where search.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(search.edge_ids)
), shortest as materialized (
  select depth, edge_ids from search
  where node_id = @end_id and depth >= @min_depth
  order by depth, edge_ids limit 1
)`
}

func shortestDistanceReferenceSearch() string {
	return shortestDistanceReferenceSearchForDirection(graph.DirectionOutbound)
}

func shortestDistanceReferenceSearchForDirection(direction graph.Direction) string {
	edgeJoin, nextNode := "e.start_id = search.node_id", "e.end_id"
	if direction == graph.DirectionInbound {
		edgeJoin, nextNode = "e.end_id = search.node_id", "e.start_id"
	}
	return `with recursive search(node_id, depth) as (
  select @start_id::int8, 0
  union
  select ` + nextNode + `, search.depth + 1
  from search
  join edge e on e.graph_id = @graph_id and ` + edgeJoin + `
  where search.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
), shortest as materialized (
  select depth from search
  where node_id = @end_id and depth >= @min_depth
  order by depth limit 1
)`
}

func buildShortestReferenceSpecs(testCase ScaleCase, probeParams map[string]any, nodeIDs, edgeIDs []int64, direction graph.Direction) []postgresReferenceSpec {
	searchNE := shortestReferenceSearchForDirection(direction)
	searchE := shortestEdgeReferenceSearch(direction)
	fullSQL := shortestDistanceReferenceSearchForDirection(direction) + ` select depth from shortest`
	boundary := "distance scalar"
	pathObserved := testCase.Name == "one_shortest_path_bound_pair" || testCase.Expected.ResultKind == "path_set"
	if pathObserved {
		fullSQL = searchNE + `
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
	hydrationParams["node_ids"] = nodeIDs
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
		{name: "search_ordered_ids", architecture: "SP-S3-U-NE", observationShape: "ordered_ids", stateShape: "ordered node and edge ID arrays", boundary: "depth plus ordered node/edge IDs", sql: searchNE + ` select depth, node_ids, edge_ids from shortest`, parameters: probeParams},
	}
	if edgeIDs != nil {
		specs = append(specs, postgresReferenceSpec{name: "hydration_only", boundary: "complete path composite from precomputed ordered edge IDs", sql: hydrationSQL, parameters: hydrationParams})
		if pathObserved && direction != graph.DirectionBoth {
			specs = append(specs,
				postgresReferenceSpec{
					name: "m0_directed_hydration_only", architecture: "MAT-M0", implementationID: "directed_set_hydration_" + strings.ToLower(direction.String()) + "_v1",
					stateShape:       "precomputed ordered edge IDs; node order derived from directed edge endpoints",
					observationShape: "complete path composite", semanticValidation: "precomputed_exact_path_inputs",
					boundary: "directed complete path composite from precomputed ordered edge IDs", sql: shortestM0HydrationSQL(direction), parameters: hydrationParams,
					validationSQL: hydrationSQL, validationParams: hydrationParams,
				},
				postgresReferenceSpec{
					name: "m1_ordered_ids_hydration_only", architecture: "MAT-M1", implementationID: "ordered_ids_set_hydration_v1",
					stateShape:       "precomputed ordered node and edge IDs",
					observationShape: "complete path composite", semanticValidation: "precomputed_exact_path_inputs",
					boundary: "complete path composite from precomputed ordered node and edge IDs", sql: shortestM1HydrationSQL(), parameters: hydrationParams,
					validationSQL: hydrationSQL, validationParams: hydrationParams,
				},
			)
		}
	}
	specs = append(specs, postgresReferenceSpec{name: "s3_unidirectional_trail_cte", legacyName: "complete_reference_s1_array_cte", architecture: shortestArchitectureForCase(testCase), implementationID: "inline_recursive_cte_unidirectional_v3", stateShape: shortestS3UStateShape(testCase), observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: fullSQL, parameters: probeParams})
	if shortestS1DistanceEligible(testCase, probeParams, direction, pathObserved) {
		s1Params := copyReferenceParams(probeParams)
		s1Params["state_limit"] = int32(100_000)
		specs = append(specs, postgresReferenceSpec{
			name: "s1_array_bfs_distance", architecture: "SP-S1", implementationID: "typed_plpgsql_array_bfs_distance_v1",
			stateShape:       "array-resident frontier and visited node IDs with explicit state ceiling; no path or predecessor state",
			observationShape: "distance scalar", semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true,
			sql: shortestS1DistanceSQL(fullSQL, direction), parameters: s1Params,
		})
	}
	if pathObserved && direction != graph.DirectionBoth {
		specs = append(specs,
			postgresReferenceSpec{
				name: "s3_unidirectional_cte_m0_directed", architecture: "SP-S3-U-E+MAT-M0", implementationID: "s3_u_edge_search_directed_set_materializer_" + strings.ToLower(direction.String()) + "_v1",
				stateShape:       "edge-only recursive trail; materializer derives node order from directed edge endpoints",
				observationShape: "public_observation", semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true,
				sql: shortestM0FullSQL(searchE, direction), parameters: probeParams,
			},
			postgresReferenceSpec{
				name: "s3_unidirectional_cte_m1_ordered_ids", architecture: "SP-S3-U-NE+MAT-M1", implementationID: "s3_u_node_edge_search_ordered_ids_set_materializer_v1",
				stateShape:       "ordered node-and-edge recursive trails; materializer hydrates both streams by ordinal",
				observationShape: "public_observation", semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true,
				sql: shortestM1FullSQL(searchNE), parameters: probeParams,
			},
		)
	}
	specs = append(specs, postgresReferenceSpec{name: "s3_bidirectional_trail_cte", legacyName: "candidate_s2_bidirectional_cte", architecture: "SP-S3-B", implementationID: "inline_recursive_cte_bidirectional_trails_v2", stateShape: "paired per-row relationship trail arrays", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: shortestBidirectionalReferenceSQL(testCase, direction), parameters: probeParams})
	return specs
}

func shortestS1DistanceEligible(testCase ScaleCase, parameters map[string]any, direction graph.Direction, pathObserved bool) bool {
	if pathObserved || direction == graph.DirectionBoth {
		return false
	}
	minDepth := 1
	if testCase.Shape.MinDepth != nil {
		minDepth = *testCase.Shape.MinDepth
	}
	return minDepth <= 1 && !reflect.DeepEqual(parameters["start_id"], parameters["end_id"])
}

func shortestS1DistanceSQL(fallbackSQL string, direction graph.Direction) string {
	inbound := "false"
	if direction == graph.DirectionInbound {
		inbound = "true"
	}
	return `with s1 as materialized (
  select * from graphbench_s1_distance_bfs(
    @graph_id, @start_id, @end_id, @min_depth, @max_depth,
    @edge_kind_ids, ` + inbound + `, @state_limit
  )
)
select depth from s1 where matched
union all
select fallback.depth from (` + fallbackSQL + `) fallback
where (select overflow from s1)
limit 1`
}

func shortestArchitectureForCase(testCase ScaleCase) string {
	if testCase.Expected.ResultKind == "path_set" || testCase.Name == "one_shortest_path_bound_pair" {
		return "SP-S3-U-NE"
	}
	return "SP-S3-U-D"
}

func shortestM0HydrationSQL(direction graph.Direction) string {
	return `with shortest(edge_ids) as (select @edge_ids::int8[])` + shortestM0MaterializationSelect(direction)
}

func shortestM0FullSQL(search string, direction graph.Direction) string {
	return search + shortestM0MaterializationSelect(direction)
}

// shortestM0MaterializationSelect is intentionally outbound-only. The S3-U
// reference search emits an ordered, graph-scoped outbound edge stream, so M0
// can derive each next node directly from edge.end_id without recursively
// rediscovering connectivity.
func shortestM0MaterializationSelect(direction graph.Direction) string {
	nextNode := "edge.end_id"
	if direction == graph.DirectionInbound {
		nextNode = "edge.start_id"
	}
	return `
select row(
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[] ||
    coalesce(hydrated.nodes, array[]::nodeComposite[]),
  coalesce(hydrated.edges, array[]::edgeComposite[])
)::pathComposite
from shortest
join node root on root.graph_id = @graph_id and root.id = @start_id
cross join lateral (
  select
    array_agg((terminal.id, terminal.kind_ids, terminal.properties)::nodeComposite order by path_edge.ordinality)::nodeComposite[] as nodes,
    array_agg((edge.id, edge.start_id, edge.end_id, edge.kind_id, edge.properties)::edgeComposite order by path_edge.ordinality)::edgeComposite[] as edges,
    count(*) as hydrated_count
  from unnest(shortest.edge_ids) with ordinality as path_edge(id, ordinality)
  join edge on edge.graph_id = @graph_id and edge.id = path_edge.id
  join node terminal on terminal.graph_id = @graph_id and terminal.id = ` + nextNode + `
) hydrated
where hydrated.hydrated_count = cardinality(shortest.edge_ids)`
}

func shortestM1HydrationSQL() string {
	return `with shortest(node_ids, edge_ids) as (select @node_ids::int8[], @edge_ids::int8[])` + shortestM1MaterializationSelect()
}

func shortestM1FullSQL(search string) string {
	return search + shortestM1MaterializationSelect()
}

// shortestM1MaterializationSelect hydrates the ordered node and edge streams
// independently and restores public path order with ordinality. M0 and M1 use
// the same S3-U search in full-comparator measurements so their delta isolates
// materialization rather than search state generation.
func shortestM1MaterializationSelect() string {
	return `
select row(
  coalesce(hydrated_nodes.nodes, array[]::nodeComposite[]),
  coalesce(hydrated_edges.edges, array[]::edgeComposite[])
)::pathComposite
from shortest
cross join lateral (
  select
    array_agg((node.id, node.kind_ids, node.properties)::nodeComposite order by path_node.ordinality)::nodeComposite[] as nodes,
    count(*) as hydrated_count
  from unnest(shortest.node_ids) with ordinality as path_node(id, ordinality)
  join node on node.graph_id = @graph_id and node.id = path_node.id
) hydrated_nodes
cross join lateral (
  select
    array_agg((edge.id, edge.start_id, edge.end_id, edge.kind_id, edge.properties)::edgeComposite order by path_edge.ordinality)::edgeComposite[] as edges,
    count(*) as hydrated_count
  from unnest(shortest.edge_ids) with ordinality as path_edge(id, ordinality)
  join edge on edge.graph_id = @graph_id and edge.id = path_edge.id
) hydrated_edges
where cardinality(shortest.node_ids) = cardinality(shortest.edge_ids) + 1
  and hydrated_nodes.hydrated_count = cardinality(shortest.node_ids)
  and hydrated_edges.hydrated_count = cardinality(shortest.edge_ids)`
}

func shortestS3UStateShape(testCase ScaleCase) string {
	if testCase.Expected.ResultKind == "path_set" || testCase.Name == "one_shortest_path_bound_pair" {
		return "per-row node and relationship trail arrays"
	}
	return "distance frontier node and depth only; no path or predecessor state"
}

func shortestBidirectionalReferenceSQL(testCase ScaleCase, direction graph.Direction) string {
	forwardJoin, forwardNext := "e.start_id = forward.node_id", "e.end_id"
	backwardJoin, backwardNext := "e.end_id = backward.node_id", "e.start_id"
	if direction == graph.DirectionInbound {
		forwardJoin, forwardNext = "e.end_id = forward.node_id", "e.start_id"
		backwardJoin, backwardNext = "e.start_id = backward.node_id", "e.end_id"
	}
	search := `with recursive
forward(node_id, depth, edge_ids) as (
  select @start_id::int8, 0, array[]::int8[]
  union all
  select ` + forwardNext + `, forward.depth + 1, forward.edge_ids || e.id
  from forward join edge e on e.graph_id = @graph_id and ` + forwardJoin + `
  where forward.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(forward.edge_ids)
), backward(node_id, depth, edge_ids) as (
  select @end_id::int8, 0, array[]::int8[]
  union all
  select ` + backwardNext + `, backward.depth + 1, e.id || backward.edge_ids
  from backward join edge e on e.graph_id = @graph_id and ` + backwardJoin + `
  where backward.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    and e.id != all(backward.edge_ids)
), shortest as materialized (
  select forward.depth + backward.depth as depth, forward.edge_ids || backward.edge_ids as edge_ids
  from forward join backward using (node_id)
  where forward.depth + backward.depth between @min_depth and @max_depth
    and not exists (select 1 from unnest(forward.edge_ids) edge_id where edge_id = any(backward.edge_ids))
  order by depth, edge_ids limit 1
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
	probeParams["min_depth"] = int32(0)
	if testCase.Shape.MinDepth != nil {
		probeParams["min_depth"] = int32(*testCase.Shape.MinDepth)
	}
	probeParams["max_depth"] = int32(15)
	if testCase.Shape.MaxDepth != nil {
		probeParams["max_depth"] = int32(*testCase.Shape.MaxDepth)
	}
	specs := buildADCSReferenceSpecs(testCase, probeParams)
	searchIdx := referenceSpecIndex(specs, "a3_suffix_seeded_reverse_ordered_ids")
	values, err := readReferenceRow(ctx, s.db, specs[searchIdx].sql, specs[searchIdx].parameters)
	if err != nil {
		return nil, fmt.Errorf("precompute ADCS hydration IDs: %w", err)
	}
	if len(values) == 0 {
		completeIdx := referenceSpecIndex(specs, "complete_reference")
		specs = slices.Insert(specs, completeIdx, postgresReferenceSpec{
			name: "hydration_only", architecture: "hydration", implementationID: "typed_empty_v1",
			stateShape: "empty ordered ID input", observationShape: "typed empty path result",
			semanticValidation: "not_applicable_empty_input", boundary: "typed empty path result",
			sql: `select null::pathComposite where false`, parameters: probeParams,
		})
		return specs, nil
	}
	if len(values) != 3 {
		return nil, fmt.Errorf("precompute ADCS hydration IDs returned %d columns, expected 3", len(values))
	}
	nodeIDs, err := referenceInt64Slice(values[0])
	if err != nil || len(nodeIDs) == 0 {
		return nil, fmt.Errorf("decode ADCS hydration node IDs: %w", err)
	}
	edgeIDs, err := referenceInt64Slice(values[2])
	if err != nil {
		return nil, fmt.Errorf("decode ADCS hydration edge IDs: %w", err)
	}
	hydrationParams := copyReferenceParams(probeParams)
	hydrationParams["root_id"] = nodeIDs[0]
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
	specs = slices.Insert(specs, completeIdx, hydration)
	return specs, nil
}

func buildADCSReferenceSpecs(testCase ScaleCase, probeParams map[string]any) []postgresReferenceSpec {
	roots := `roots(root_id) as materialized (
  select n.id from node n
  where n.graph_id = @graph_id
    and @Group_kind::int2 = any(n.kind_ids)
    and n.properties ->> 'objectid' = @objectid
)`
	suffix := `suffix_rows(boundary_id, ca_id, domain_id, suffix_edge_ids, suffix_node_ids) as materialized (
  select boundary.id, ca.id, domain_node.id,
         array[enroll.id, trusted.id, store_for.id]::int8[],
         array[boundary.id, ca.id, store.id, domain_node.id]::int8[]
  from (select 1 from roots limit 1) root_presence
  cross join edge enroll
  join node boundary on boundary.graph_id = @graph_id and boundary.id = enroll.start_id
  join node ca on ca.graph_id = @graph_id and ca.id = enroll.end_id and @EnterpriseCA_kind::int2 = any(ca.kind_ids)
  join edge trusted on trusted.graph_id = @graph_id and trusted.start_id = ca.id and trusted.kind_id = @TrustedForNTAuth_kind
  join node store on store.graph_id = @graph_id and store.id = trusted.end_id and @NTAuthStore_kind::int2 = any(store.kind_ids)
  join edge store_for on store_for.graph_id = @graph_id and store_for.start_id = store.id and store_for.kind_id = @NTAuthStoreFor_kind
  join node domain_node on domain_node.graph_id = @graph_id and domain_node.id = store_for.end_id and @Domain_kind::int2 = any(domain_node.kind_ids)
  where enroll.graph_id = @graph_id and enroll.kind_id = @Enroll_kind
    and trusted.id <> enroll.id
    and store_for.id <> enroll.id and store_for.id <> trusted.id
)`
	forwardMembers := `members(root_id, node_id, node_ids, edge_ids, depth) as (
  select root_id, root_id, array[root_id]::int8[], array[]::int8[], 0 from roots
  union all
  select members.root_id, e.end_id, members.node_ids || e.end_id, members.edge_ids || e.id, members.depth + 1
  from members join edge e
    on e.graph_id = @graph_id and e.start_id = members.node_id and e.kind_id = @MemberOf_kind
  join node next_node on next_node.graph_id = @graph_id and next_node.id = e.end_id
  where members.depth < @max_depth and e.id != all(members.edge_ids)
)`
	scalarForwardMembers := strings.Replace(forwardMembers, "\n  join node next_node on next_node.graph_id = @graph_id and next_node.id = e.end_id", "", 1)
	allMemberNodesExist := `not exists (
	      select 1 from unnest(members.node_ids) as member_node_id(id)
	      left join node member_node on member_node.graph_id = @graph_id and member_node.id = member_node_id.id
      where member_node.id is null
    )`
	legacyForward := `with recursive ` + roots + `, ` + forwardMembers + `, paths as materialized (
  select members.node_ids || array[ca.id, store.id, domain_node.id]::int8[] as node_ids,
         ca.id as ca_id, domain_node.id as domain_id,
         members.edge_ids || enroll.id || trusted.id || store_for.id as edge_ids
  from members
  join edge enroll on enroll.graph_id = @graph_id and enroll.start_id = members.node_id and enroll.kind_id = @Enroll_kind and enroll.id != all(members.edge_ids)
  join node ca on ca.graph_id = @graph_id and ca.id = enroll.end_id and @EnterpriseCA_kind::int2 = any(ca.kind_ids)
  join edge trusted on trusted.graph_id = @graph_id and trusted.start_id = ca.id and trusted.kind_id = @TrustedForNTAuth_kind
    and trusted.id != enroll.id and trusted.id != all(members.edge_ids)
  join node store on store.graph_id = @graph_id and store.id = trusted.end_id and @NTAuthStore_kind::int2 = any(store.kind_ids)
  join edge store_for on store_for.graph_id = @graph_id and store_for.start_id = store.id and store_for.kind_id = @NTAuthStoreFor_kind
    and store_for.id != enroll.id and store_for.id != trusted.id and store_for.id != all(members.edge_ids)
  join node domain_node on domain_node.graph_id = @graph_id and domain_node.id = store_for.end_id and @Domain_kind::int2 = any(domain_node.kind_ids)
  where members.depth >= @min_depth
)`
	lateHydratedForward := `with recursive ` + roots + `, ` + scalarForwardMembers + `, paths as materialized (
  select members.node_ids || array[ca.id, store.id, domain_node.id]::int8[] as node_ids,
         ca.id as ca_id, domain_node.id as domain_id,
         members.edge_ids || enroll.id || trusted.id || store_for.id as edge_ids
  from members
  join edge enroll on enroll.graph_id = @graph_id and enroll.start_id = members.node_id and enroll.kind_id = @Enroll_kind and enroll.id != all(members.edge_ids)
  join node ca on ca.graph_id = @graph_id and ca.id = enroll.end_id and @EnterpriseCA_kind::int2 = any(ca.kind_ids)
  join edge trusted on trusted.graph_id = @graph_id and trusted.start_id = ca.id and trusted.kind_id = @TrustedForNTAuth_kind
    and trusted.id != enroll.id and trusted.id != all(members.edge_ids)
  join node store on store.graph_id = @graph_id and store.id = trusted.end_id and @NTAuthStore_kind::int2 = any(store.kind_ids)
  join edge store_for on store_for.graph_id = @graph_id and store_for.start_id = store.id and store_for.kind_id = @NTAuthStoreFor_kind
    and store_for.id != enroll.id and store_for.id != trusted.id and store_for.id != all(members.edge_ids)
  join node domain_node on domain_node.graph_id = @graph_id and domain_node.id = store_for.end_id and @Domain_kind::int2 = any(domain_node.kind_ids)
  where members.depth >= @min_depth and ` + allMemberNodesExist + `
)`
	factoredForward := `with recursive ` + roots + `, ` + suffix + `, ` + scalarForwardMembers + `, paths as materialized (
  select members.node_ids || suffix_rows.suffix_node_ids[2:4] as node_ids,
         suffix_rows.ca_id, suffix_rows.domain_id,
         members.edge_ids || suffix_rows.suffix_edge_ids as edge_ids
  from members join suffix_rows on suffix_rows.boundary_id = members.node_id
  where members.depth >= @min_depth
	    and not exists (select 1 from unnest(members.edge_ids) as member_edge(id) where member_edge.id = any(suffix_rows.suffix_edge_ids))
    and ` + allMemberNodesExist + `
)`
	reverse := `with recursive ` + roots + `, ` + suffix + `, boundary_ids(boundary_id) as materialized (
  select distinct boundary_id from suffix_rows
), reverse_trails(boundary_id, node_id, node_ids, edge_ids, depth) as (
  select boundary_id, boundary_id, array[boundary_id]::int8[], array[]::int8[], 0 from boundary_ids
  union all
	  select reverse_trails.boundary_id, e.start_id, array_prepend(e.start_id, reverse_trails.node_ids),
	         array_prepend(e.id, reverse_trails.edge_ids), reverse_trails.depth + 1
  from reverse_trails join edge e
    on e.graph_id = @graph_id and e.end_id = reverse_trails.node_id and e.kind_id = @MemberOf_kind
  where reverse_trails.depth < @max_depth and e.id != all(reverse_trails.edge_ids)
), paths as materialized (
  select reverse_trails.node_ids || suffix_rows.suffix_node_ids[2:4] as node_ids,
         suffix_rows.ca_id, suffix_rows.domain_id,
         reverse_trails.edge_ids || suffix_rows.suffix_edge_ids as edge_ids
  from reverse_trails
  join roots on roots.root_id = reverse_trails.node_id
  join suffix_rows on suffix_rows.boundary_id = reverse_trails.boundary_id
  where reverse_trails.depth >= @min_depth
	    and not exists (select 1 from unnest(reverse_trails.edge_ids) as member_edge(id) where member_edge.id = any(suffix_rows.suffix_edge_ids))
	    and not exists (
	      select 1 from unnest(reverse_trails.node_ids) as member_node_id(id)
	      left join node member_node on member_node.graph_id = @graph_id and member_node.id = member_node_id.id
      where member_node.id is null
    )
)`
	viability := `with recursive ` + roots + `, ` + suffix + `, boundary_ids(boundary_id) as materialized (
  select distinct boundary_id from suffix_rows
), viable(node_id, reverse_distance) as (
  select boundary_id, 0 from boundary_ids
  union
  select e.start_id, viable.reverse_distance + 1
  from viable join edge e
    on e.graph_id = @graph_id and e.end_id = viable.node_id and e.kind_id = @MemberOf_kind
  where viable.reverse_distance < @max_depth
), members(root_id, node_id, node_ids, edge_ids, depth) as (
  select root_id, root_id, array[root_id]::int8[], array[]::int8[], 0 from roots
  where exists (select 1 from viable where viable.node_id = roots.root_id and viable.reverse_distance <= @max_depth)
  union all
  select members.root_id, e.end_id, members.node_ids || e.end_id, members.edge_ids || e.id, members.depth + 1
  from members join edge e
    on e.graph_id = @graph_id and e.start_id = members.node_id and e.kind_id = @MemberOf_kind
  where members.depth < @max_depth and e.id != all(members.edge_ids)
    and exists (select 1 from viable where viable.node_id = e.end_id and viable.reverse_distance <= @max_depth - members.depth - 1)
), paths as materialized (
  select members.node_ids || suffix_rows.suffix_node_ids[2:4] as node_ids,
         suffix_rows.ca_id, suffix_rows.domain_id,
         members.edge_ids || suffix_rows.suffix_edge_ids as edge_ids
  from members join suffix_rows on suffix_rows.boundary_id = members.node_id
  where members.depth >= @min_depth
	    and not exists (select 1 from unnest(members.edge_ids) as member_edge(id) where member_edge.id = any(suffix_rows.suffix_edge_ids))
    and ` + allMemberNodesExist + `
)`

	fullSQL := legacyForward + ` select ca_id, domain_id from paths`
	boundary := "endpoint ID pairs"
	pathObserved := testCase.Observes.Paths || testCase.Expected.ResultKind == "path_set"
	complete := func(search string) string {
		if !pathObserved {
			return search + ` select ca_id, domain_id from paths`
		}
		return search + `
select ordered_edge_ids_to_path(
  @graph_id,
  (root.id, root.kind_ids, root.properties)::nodeComposite,
  paths.edge_ids,
  array[(root.id, root.kind_ids, root.properties)::nodeComposite]::nodeComposite[]
)::pathComposite
from paths join node root on root.graph_id = @graph_id and root.id = paths.node_ids[1]`
	}
	if pathObserved {
		fullSQL = complete(legacyForward)
		boundary = "complete path composite"
	}
	orderedLegacy := legacyForward + ` select node_ids, ca_id, edge_ids from paths`
	orderedReference := func(spec postgresReferenceSpec) postgresReferenceSpec {
		spec.semanticValidation = "exact_ordered_ids"
		spec.validationSQL = orderedLegacy
		spec.validationParams = probeParams
		return spec
	}
	return []postgresReferenceSpec{
		{name: "round_trip", architecture: "protocol", stateShape: "none", boundary: "prepared protocol and transaction", sql: `select 1`},
		{name: "endpoint_validation", architecture: "root_validation", stateShape: "root ID bag", boundary: "validated root ID", sql: `select n.id from node n where n.graph_id = @graph_id and @Group_kind::int2 = any(n.kind_ids) and n.properties ->> 'objectid' = @objectid`, parameters: probeParams},
		{name: "fixed_suffix_rows", architecture: "factored_suffix", stateShape: "boundary and ordered suffix IDs", boundary: "exact suffix rows and distinct boundary IDs", sql: `with ` + roots + `, ` + suffix + ` select boundary_id, ca_id, domain_id, suffix_edge_ids from suffix_rows`, parameters: probeParams},
		{name: "minimum_graph_access", architecture: "root_adjacency", stateShape: "edge IDs", boundary: "root adjacency edge IDs", sql: `with ` + roots + ` select e.id from roots join edge e on e.graph_id = @graph_id and e.start_id = roots.root_id and e.kind_id = @MemberOf_kind order by e.id`, parameters: probeParams},
		orderedReference(postgresReferenceSpec{name: "search_ordered_ids", legacyName: "current_forward_ordered_ids", architecture: "ADCS-A0-SQL", observationShape: "ordered_ids", stateShape: "root/boundary IDs and ordered relationship trail", boundary: "ordered node/edge IDs without hydration", sql: orderedLegacy, parameters: probeParams}),
		orderedReference(postgresReferenceSpec{name: "current_forward_ordered_ids", architecture: "ADCS-A0-AA", aaAliasOf: "search_ordered_ids", observationShape: "ordered_ids", stateShape: "root/boundary IDs and ordered relationship trail", boundary: "ordered node/edge IDs", sql: orderedLegacy, parameters: probeParams}),
		orderedReference(postgresReferenceSpec{name: "a1a_root_reuse_ordered_ids", architecture: "ADCS-A0-AA", aaAliasOf: "search_ordered_ids", observationShape: "ordered_ids", stateShape: "root/boundary IDs and ordered relationship trail", boundary: "ordered node/edge IDs", sql: orderedLegacy, parameters: probeParams}),
		orderedReference(postgresReferenceSpec{name: "a1b_late_hydration_ordered_ids", architecture: "ADCS-A1b", observationShape: "ordered_ids", stateShape: "scalar expansion state and ordered relationship trail", boundary: "ordered node/edge IDs", sql: lateHydratedForward + ` select node_ids, ca_id, edge_ids from paths`, parameters: probeParams}),
		orderedReference(postgresReferenceSpec{name: "a2_factored_suffix_forward_ordered_ids", architecture: "ADCS-A2", observationShape: "ordered_ids", stateShape: "scalar forward trails joined to exact suffix bag", boundary: "ordered node/edge IDs", sql: factoredForward + ` select node_ids, ca_id, edge_ids from paths`, parameters: probeParams}),
		orderedReference(postgresReferenceSpec{name: "a3_suffix_seeded_reverse_ordered_ids", architecture: "ADCS-A3", observationShape: "ordered_ids", stateShape: "scalar reverse trails with prepended relationship IDs", boundary: "ordered node/edge IDs", sql: reverse + ` select node_ids, ca_id, edge_ids from paths`, parameters: probeParams}),
		orderedReference(postgresReferenceSpec{name: "a4_viability_forward_ordered_ids", architecture: "ADCS-A4", observationShape: "ordered_ids", stateShape: "depth-aware viability filter plus exact forward trails", boundary: "ordered node/edge IDs", sql: viability + ` select node_ids, ca_id, edge_ids from paths`, parameters: probeParams}),
		{name: "complete_reference", architecture: "ADCS-A0-SQL", stateShape: "forward relationship trails", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: fullSQL, parameters: probeParams},
		{name: "a1a_root_reuse_complete", architecture: "ADCS-A0-AA", aaAliasOf: "complete_reference", stateShape: "forward relationship trails", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: complete(legacyForward), parameters: probeParams},
		{name: "a1b_late_hydration_complete", architecture: "ADCS-A1b", stateShape: "scalar expansion state with final-only hydration", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: complete(lateHydratedForward), parameters: probeParams},
		{name: "a2_factored_suffix_forward_complete", architecture: "ADCS-A2", stateShape: "exact forward trails joined to suffix bag", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: complete(factoredForward), parameters: probeParams},
		{name: "a3_suffix_seeded_reverse_complete", architecture: "ADCS-A3", stateShape: "exact reverse trails joined back to suffix bag", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: complete(reverse), parameters: probeParams},
		{name: "a4_viability_forward_complete", architecture: "ADCS-A4", stateShape: "permissive viability plus exact forward trails", observationShape: observationShapeForCase(testCase), semanticValidation: "exact_public_observation", boundary: boundary, fullComparator: true, sql: complete(viability), parameters: probeParams},
	}
}

func observationShapeForCase(testCase ScaleCase) string {
	if testCase.Observes.Paths || testCase.Expected.ResultKind == "path_set" {
		return "public_observation"
	}
	return "endpoint_ids"
}

func referenceSpecIndex(specs []postgresReferenceSpec, name string) int {
	for idx, spec := range specs {
		if spec.name == name {
			return idx
		}
	}
	panic("missing PostgreSQL reference spec " + name)
}

func referenceSpecIndexOrMissing(specs []postgresReferenceSpec, name string) int {
	for idx, spec := range specs {
		if spec.name == name {
			return idx
		}
	}
	return -1
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
		result := make([]int64, len(typed))
		copy(result, typed)
		return result, nil
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

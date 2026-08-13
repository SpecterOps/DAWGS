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

// postgresReferenceSchemaVersion identifies the serialized schema revision for PostgreSQL reference schema.
const postgresReferenceSchemaVersion = 1

// postgresReferenceArms lists the independently implemented PostgreSQL comparison arms.
var postgresReferenceArms = []string{
	"round_trip",
	"endpoint_validation",
	"fixed_suffix_rows",
	"minimum_graph_access",
	"search_ordered_ids",
	"stepwise_forward_aa_ordered_ids",
	"root_reuse_ordered_ids",
	"late_hydration_ordered_ids",
	"factored_suffix_forward_ordered_ids",
	"suffix_seeded_reverse_ordered_ids",
	"backward_viability_forward_ordered_ids",
	"hydration_only",
	"complete_reference",
	"root_reuse_complete",
	"late_hydration_complete",
	"factored_suffix_forward_complete",
	"suffix_seeded_reverse_complete",
	"backward_viability_forward_complete",
	"m0_directed_hydration_only",
	"m1_ordered_ids_hydration_only",
	"s3_unidirectional_trail_cte",
	"s3_unidirectional_cte_m0_directed",
	"s3_unidirectional_cte_m1_ordered_ids",
	"s3_bidirectional_trail_cte",
	"s1_array_bfs_distance",
	"s4_canonical_source_distance",
	"s4_canonical_source_witness_m0",
	"sp_b1_strict_alternating_distance",
	"sp_b1_strict_alternating_witness_m0",
	"sp_b2_smaller_frontier_distance",
	"sp_b2_smaller_frontier_witness_m0",
	"asp_a1_stored_helper_m0",
	"asp_i1_inline_predecessor_dag_m0",
	"asp_b1_bidirectional_dag_strict_m0",
	"asp_b2_bidirectional_dag_smaller_frontier_m0",
	"expand_into_pair_join",
	"expand_into_lower_degree_scan",
	"expand_into_pair_cache",
}

// validPostgresReferenceArm reports whether a reference-arm selector is declared.
func validPostgresReferenceArm(name string) bool {
	return slices.Contains(postgresReferenceArms, name)
}

// postgresReferenceSpec defines one independent PostgreSQL reference implementation and its observation contract.
type postgresReferenceSpec struct {
	// name is the canonical selector and serialized identity for the reference arm.
	name string
	// legacyName retains the compatibility alias accepted for a reference arm.
	legacyName string
	// architecture retains the executor architecture that must remain stable across rounds.
	architecture string
	// implementationID provides a versioned identity for the reference algorithm and materialization strategy.
	implementationID string
	// stateShape describes recursive state retained by the reference implementation.
	stateShape string
	// observationShape describes the normalized values returned by the reference boundary.
	observationShape string
	// semanticValidation describes the exact observation contract enforced for the reference.
	semanticValidation string
	// boundary identifies the timed boundary exposed by the reference arm.
	boundary string
	// fullComparator reports whether the reference produces the complete public observation.
	fullComparator bool
	// aaAliasOf identifies the reference arm reused as an explicit A/A alias.
	aaAliasOf string
	// timingBoundary describes which portion of reference execution contributes to latency samples.
	timingBoundary string
	// sql contains the executable SQL for an independent reference arm.
	sql string
	// parameters supplies resolved parameters to the reference SQL query.
	parameters map[string]any
	// validationSQL contains SQL used to validate affected entity counts after a write.
	validationSQL string
	// validationParams supplies parameters used to validate precomputed reference inputs.
	validationParams map[string]any
}

// measureReferences executes references and records its timing observations.
func (s *postgresSQLRunner) measureReferences(ctx context.Context, testCase ScaleCase, params map[string]any, idMap opengraph.IDMap, publicObservation []string, warmupIterations, iterations int) ([]PostgresReferenceResult, error) {
	readOptions := s.readTransactionOptions()
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
		rowCount, stats, err := measureRawPostgres(ctx, s.db, spec.sql, spec.parameters, warmupIterations, iterations, readOptions...)
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
			}, readOptions...)
			if err != nil {
				return nil, fmt.Errorf("%s exact observation: %w", spec.name, err)
			}
			if observedCount != rowCount {
				return nil, fmt.Errorf("%s exact observation row count changed from %d to %d", spec.name, rowCount, observedCount)
			}
			if spec.validationSQL != "" {
				var (
					validationCount int64
					validationRows  []string
				)

				err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
					var err error
					validationCount, validationRows, err = observeRawRows(tx, spec.validationSQL, spec.validationParams, idMap, resultContainsNodeIDs(testCase.Expected), resultContainsPaths(testCase.Expected))
					return err
				}, readOptions...)
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
			stats.Samples[idx].ConnectionID = s.backendPID
		}
		plan, planJSON, metrics, err := explainRawPostgres(ctx, s.db, spec.sql, spec.parameters, readOptions...)
		if err != nil {
			return nil, fmt.Errorf("%s explain: %w", spec.name, err)
		}
		results = append(results, PostgresReferenceResult{
			SchemaVersion:                postgresReferenceSchemaVersion,
			Name:                         spec.name,
			LegacyName:                   spec.legacyName,
			Architecture:                 spec.architecture,
			ImplementationID:             spec.implementationID,
			StateShape:                   spec.stateShape,
			ObservationShape:             spec.observationShape,
			SemanticValidation:           spec.semanticValidation,
			Boundary:                     spec.boundary,
			TimingBoundary:               spec.timingBoundary,
			FullComparator:               spec.fullComparator,
			AAAliasOf:                    spec.aaAliasOf,
			SQL:                          spec.sql,
			SQLFingerprint:               normalizedSQLFingerprint(spec.sql),
			RowCount:                     rowCount,
			ObservedRows:                 observedRows,
			Stats:                        stats,
			PostgresPlan:                 plan,
			PostgresPlanJSON:             planJSON,
			PostgresMetrics:              &metrics,
			traversalTelemetryParameters: copyReferenceParams(spec.parameters),
		})
	}
	return results, nil
}

// selectReferenceSpecs restricts reference arms to explicit selectors and rejects missing requested arms.
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

// explainRawPostgres runs raw PostgreSQL EXPLAIN and returns normalized plan text, JSON, and metrics.
func explainRawPostgres(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any, transactionOptions ...graph.TransactionOption) ([]string, json.RawMessage, PostgresPlanMetrics, error) {
	var (
		plan     []string
		planJSON json.RawMessage
	)

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
		jsonResult := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING ON, FORMAT JSON) "+sqlQuery, params)
		defer jsonResult.Close()
		if jsonResult.Next() && len(jsonResult.Values()) > 0 {
			var err error
			planJSON, err = encodePostgresPlanJSON(jsonResult.Values()[0])
			if err != nil {
				return err
			}
		}
		return jsonResult.Error()
	}, transactionOptions...)
	if err != nil {
		return nil, nil, PostgresPlanMetrics{}, err
	}
	metrics, err := parsePostgresPlanJSONMetrics(planJSON)
	if err != nil {
		return nil, nil, PostgresPlanMetrics{}, err
	}
	return plan, planJSON, metrics, nil
}

// normalizedReferenceSpec fills legacy reference metadata defaults used for stable identity comparisons.
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

// normalizedSQLFingerprint hashes SQL after collapsing insignificant whitespace.
func normalizedSQLFingerprint(sql string) string {
	return sqlFingerprint(strings.Join(strings.Fields(sql), " "))
}

// validateReferenceSpecs rejects duplicate, incomplete, or semantically inconsistent reference specifications.
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

// parameterShape returns a type-only description of query parameters for reference identity checks.
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

// validAlternativeShortestPathObservation reports whether two observations are both valid shortest-path witnesses.
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

// singleStablePathObservation returns the sole normalized path when the result contains exactly one valid path.
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

// validOutboundStablePath reports whether a stable path follows every relationship in outbound order.
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

// referenceSpecsForRound returns reference specifications in the predeclared balanced order for a round.
func referenceSpecsForRound(specs []postgresReferenceSpec, round int) []postgresReferenceSpec {
	if len(specs) == 3 && round > 0 {
		// Odd-sized treatment sets need a doubled Williams design. Across these
		// six rows every arm occupies every position twice, and every directed
		// first-order carryover pair occurs twice.
		schedule := [6][3]int{
			{0, 1, 2},
			{1, 2, 0},
			{2, 0, 1},
			{2, 1, 0},
			{0, 2, 1},
			{1, 0, 2},
		}
		row := schedule[(round-1)%len(schedule)]
		ordered := make([]postgresReferenceSpec, len(specs))
		for idx, slot := range row {
			ordered[idx] = specs[slot]
		}
		return ordered
	}
	if len(specs) == 5 && round > 0 {
		// Ten-sequence Williams/carryover-balanced schedule predeclared by the
		// fixed-suffix expansion tournament. Slots are the caller-selected arms, so B1/B2/B3 can
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

// referenceSpecs constructs the independent PostgreSQL reference implementations for a scale case.
func (s *postgresSQLRunner) referenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	if testCase.Category == "expand_into_one_hop" {
		return s.expandIntoReferenceSpecs(ctx, testCase, params)
	}
	if testCase.Category == "generated_fixed_suffix_expansion" {
		return s.fixedSuffixExpansionReferenceSpecs(ctx, testCase, params)
	}
	if testCase.Category == "generated_shortest_path" || testCase.Category == "generated_shortest_path_v2" {
		// Singleton and all-shortest architectures are kept as distinct arms;
		// allShortestPaths uses its relationship-distinct predecessor DAG only.
		if strings.Contains(strings.ToLower(testCase.Cypher), "allshortestpaths") {
			return s.allShortestReferenceSpecs(ctx, testCase, params)
		}
		return s.shortestReferenceSpecs(ctx, testCase, params)
	}
	switch testCase.Name {
	case "shortest_distance_bound_pair", "one_shortest_path_bound_pair":
		return s.shortestReferenceSpecs(ctx, testCase, params)
	case "fixed_suffix_expansion_endpoint_ids", "fixed_suffix_expansion_path_observed":
		return s.fixedSuffixExpansionReferenceSpecs(ctx, testCase, params)
	default:
		return nil, nil
	}
}

// allShortestDAGSearch returns the predecessor-DAG SQL search for all shortest paths in one direction.
func allShortestDAGSearch(direction graph.Direction) string {
	distanceJoin, distanceNext := "e.start_id = distance.node_id", "e.end_id"
	predecessorJoin := "e.start_id = prior.node_id and e.end_id = paths.node_id"
	if direction == graph.DirectionInbound {
		distanceJoin, distanceNext = "e.end_id = distance.node_id", "e.start_id"
		predecessorJoin = "e.end_id = prior.node_id and e.start_id = paths.node_id"
	}
	return `with recursive validated(start_id, end_id) as materialized (
  select start_node.id, end_node.id
  from node start_node, node end_node
  where start_node.graph_id = @graph_id and start_node.id = @start_id
    and end_node.graph_id = @graph_id and end_node.id = @end_id
), distance(node_id, depth) as (
  select validated.start_id, 0 from validated
  union
  select ` + distanceNext + `, distance.depth + 1
  from distance
  join edge e on e.graph_id = @graph_id and ` + distanceJoin + `
  where distance.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
), target as materialized (
  select depth from distance
  where node_id = @end_id and depth >= @min_depth
  order by depth limit 1
), predecessor(node_id, depth, predecessor_id, edge_id) as materialized (
  select paths.node_id, paths.depth, prior.node_id, e.id
  from distance paths
  join target on paths.depth > 0 and paths.depth <= target.depth
  join distance prior on prior.depth = paths.depth - 1
  join edge e on e.graph_id = @graph_id and ` + predecessorJoin + `
  where (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
), paths(node_id, depth, edge_ids) as (
  select @end_id::int8, target.depth, array[]::int8[] from target
  union all
  select predecessor.predecessor_id, paths.depth - 1, array[predecessor.edge_id]::int8[] || paths.edge_ids
  from paths join predecessor on predecessor.node_id = paths.node_id and predecessor.depth = paths.depth
), shortest(depth, edge_ids) as materialized (
  select target.depth, paths.edge_ids
  from paths join target on true where paths.node_id = @start_id and paths.depth = 0
)`
}

// allShortestA1ReferenceSQL supports benchmark evidence processing for all shortest a1 reference sql.
func allShortestA1ReferenceSQL(direction graph.Direction) string {
	inbound := "false"
	if direction == graph.DirectionInbound {
		inbound = "true"
	}
	search := `with shortest as materialized (
  select depth, path as edge_ids
  from all_shortest_paths_dag(
    @graph_id, @start_id, @end_id, @min_depth, @max_depth,
    @edge_kind_ids, ` + inbound + `
  )
)`
	return shortestM0FullSQL(search, direction)
}

// allShortestBidirectionalReferenceSQL exposes a forced two-sided
// predecessor-DAG kernel at the same complete M0 path boundary as ASP-A1.
func allShortestBidirectionalReferenceSQL(functionName string, direction graph.Direction) string {
	inbound := "false"
	if direction == graph.DirectionInbound {
		inbound = "true"
	}
	search := `with shortest as materialized (
  select depth, path as edge_ids
  from ` + functionName + `(
    @graph_id, @start_id, @end_id, @min_depth, @max_depth,
    @edge_kind_ids, ` + inbound + `, @state_limit, @frontier_limit,
    @predecessor_limit, @enumeration_limit, @output_bytes_limit
  )
)`
	return shortestM0FullSQL(search, direction)
}

// allShortestReferenceSpecs builds the predecessor-DAG reference for an all-shortest-path workload.
func (s *postgresSQLRunner) allShortestReferenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
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
	var edgeKindIDs []int16
	if len(edgeKinds) > 0 {
		if s.pgDriver == nil {
			return nil, fmt.Errorf("map all-shortest reference edge kinds: PostgreSQL driver is unavailable")
		}
		var err error
		edgeKindIDs, err = s.pgDriver.KindMapper().MapKinds(ctx, edgeKinds)
		if err != nil {
			return nil, fmt.Errorf("map all-shortest reference edge kinds: %w", err)
		}
	}
	probeParams["edge_kind_ids"] = edgeKindIDs
	direction, err := shortestReferenceDirection(testCase.Cypher)
	if err != nil || direction == graph.DirectionBoth {
		return nil, err
	}
	rootParameter, terminalParameter, err := shortestReferenceEndpointParameters(testCase.Cypher)
	if err != nil {
		return nil, err
	}
	probeParams["start_id"] = probeParams[rootParameter]
	probeParams["end_id"] = probeParams[terminalParameter]
	specs := []postgresReferenceSpec{{
		name:               "asp_a1_stored_helper_m0",
		architecture:       "ASP-A1-DAG",
		implementationID:   "all_shortest_paths_dag_stored_helper_m0_v1",
		stateShape:         "minimum-depth helper workspace with relationship-distinct predecessors",
		observationShape:   "complete all-shortest path multiset",
		semanticValidation: "exact_public_observation",
		boundary:           "complete path composites",
		fullComparator:     true,
		sql:                allShortestA1ReferenceSQL(direction),
		parameters:         probeParams,
	}}

	// I1 is valid only inside the same distinct-endpoint, min-one bounded
	// contract enforced by the production emitter. A1 remains available as the
	// exact control outside that envelope.
	startID, startOK := probeParams["start_id"].(int64)
	endID, endOK := probeParams["end_id"].(int64)
	maximumDepth, maximumOK := probeParams["max_depth"].(int32)
	if probeParams["min_depth"] != int32(1) || !maximumOK || maximumDepth < 1 || maximumDepth > 64 || !startOK || !endOK || startID == endID {
		return specs, nil
	}
	search := allShortestDAGSearch(direction)
	specs = append(specs, postgresReferenceSpec{
		name:               "asp_i1_inline_predecessor_dag_m0",
		architecture:       "ASP-I1-U-DAG+MAT-M0",
		implementationID:   "inline_shortest_depth_predecessor_dag_m0_v1",
		stateShape:         "node/depth discovery plus every relationship-distinct shortest-depth predecessor edge",
		observationShape:   "complete all-shortest path multiset",
		semanticValidation: "exact_public_observation",
		boundary:           "complete path composites",
		fullComparator:     true,
		sql:                shortestM0FullSQL(search, direction),
		parameters:         probeParams,
	})

	// B1/B2 are intentionally tool/reference-only. Keep automatic production
	// selection on ASP-A1 until independent confirmation passes, and do not
	// expose candidate arms outside their distinct-endpoint minimum-one envelope.
	candidateParams := copyReferenceParams(probeParams)
	candidateParams["state_limit"] = int64(100_000)
	candidateParams["frontier_limit"] = int64(100_000)
	candidateParams["predecessor_limit"] = int64(100_000)
	candidateParams["enumeration_limit"] = int64(100_000)
	candidateParams["output_bytes_limit"] = int64(64 * 1024 * 1024)
	for _, candidate := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// architecture retains the architecture while anonymous record is assembled or evaluated.
		architecture string
		// implementationID identifies the implementation id.
		implementationID string
		// functionName identifies the function name.
		functionName string
	}{
		{
			name:             "asp_b1_bidirectional_dag_strict_m0",
			architecture:     "ASP-B1-DAG-ALT-NODE",
			implementationID: "typed_two_sided_predecessor_dag_strict_alternating_v1",
			functionName:     "all_shortest_paths_b1_strict_alternating",
		},
		{
			name:             "asp_b2_bidirectional_dag_smaller_frontier_m0",
			architecture:     "ASP-B2-DAG-MIN-LEVEL",
			implementationID: "typed_two_sided_predecessor_dag_smaller_current_level_v1",
			functionName:     "all_shortest_paths_b2_smaller_current_level",
		},
	} {
		specs = append(specs, postgresReferenceSpec{
			name:               candidate.name,
			architecture:       candidate.architecture,
			implementationID:   candidate.implementationID,
			stateShape:         "two-sided minimum-node-depth discovery plus every relationship-distinct equal-depth predecessor/successor at one canonical cut",
			observationShape:   "complete all-shortest path multiset",
			semanticValidation: "exact_public_observation",
			boundary:           "complete path composites",
			fullComparator:     true,
			sql:                allShortestBidirectionalReferenceSQL(candidate.functionName, direction),
			parameters:         copyReferenceParams(candidateParams),
		})
	}
	return specs, nil
}

// shortestReferenceSpecs builds eligible shortest-path reference implementations and measurement boundaries.
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
	values, err := readReferenceRow(ctx, s.db, search+` select depth, node_ids, edge_ids from shortest`, searchParams, s.readTransactionOptions()...)
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

// shortestReferenceEndpointParameters maps public start and end parameters to physical search endpoints for the parsed direction.
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

// collectIdentityParameterBindings extracts equality-bound ID parameters for the two variables in a shortest-path pattern.
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

// identityReferenceSymbol returns the variable whose ID is projected directly by a reference query.
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

// shortestReferenceIsProvablyOutbound reports whether a supported shortest-path query has outbound direction.
func shortestReferenceIsProvablyOutbound(query string) (bool, error) {
	direction, err := shortestReferenceDirection(query)
	if err != nil {
		return false, err
	}

	return direction == graph.DirectionOutbound, nil
}

// shortestReferenceDirection parses a shortest-path query and returns its single relationship direction.
func shortestReferenceDirection(query string) (graph.Direction, error) {
	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return 0, err
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
			if patternPart == nil || (!patternPart.ShortestPathPattern && !patternPart.AllShortestPathsPattern) {
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

// shortestReferenceSearch returns the compact recursive shortest-path search SQL for a projection mode.
func shortestReferenceSearch() string {
	return shortestReferenceSearchForDirection(graph.DirectionOutbound)
}

// shortestReferenceSearchForDirection returns direction-specific shortest-path search SQL and endpoint columns.
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

// shortestEdgeReferenceSearch returns the edge-only shortest-path search SQL for a direction.
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

// shortestDistanceReferenceSearch returns the minimal-state shortest-distance search SQL for a direction.
func shortestDistanceReferenceSearch() string {
	return shortestDistanceReferenceSearchForDirection(graph.DirectionOutbound)
}

// shortestDistanceReferenceSearchForDirection returns direction-specific shortest-distance SQL and endpoint columns.
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

// shortestCanonicalWitnessSearch returns SQL that reconstructs one deterministic witness from compact predecessor state.
func shortestCanonicalWitnessSearch(reverseForPublicPath bool) string {
	edgeIDs := "witness.edge_ids"
	if reverseForPublicPath {
		edgeIDs = `(select coalesce(array_agg(reversed.edge_id order by reversed.ordinal desc), array[]::int8[])
    from unnest(witness.edge_ids) with ordinality reversed(edge_id, ordinal))`
	}
	return `with recursive distance(node_id, depth) as (
  select @search_start_id::int8, 0
  union
  select e.end_id, distance.depth + 1
  from distance
  join edge e on e.graph_id = @graph_id and e.start_id = distance.node_id
  where distance.depth < @max_depth
    and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
), target as materialized (
  select depth from distance
  where node_id = @search_end_id and depth >= @min_depth
  order by depth limit 1
), witness(node_id, depth, edge_ids) as (
  select @search_end_id::int8, target.depth, array[]::int8[] from target
  union all
  select predecessor.node_id, witness.depth - 1, array[predecessor.edge_id]::int8[] || witness.edge_ids
  from witness
  join lateral (
    select prior.node_id, e.id as edge_id
    from distance prior
    join edge e on e.graph_id = @graph_id and e.start_id = prior.node_id and e.end_id = witness.node_id
    where prior.depth = witness.depth - 1
      and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[]))
    order by e.id, prior.node_id limit 1
  ) predecessor on witness.depth > 0
), shortest as materialized (
  select target.depth, ` + edgeIDs + ` as edge_ids
  from witness join target on true where witness.depth = 0
)`
}

// shortestBidirectionalCompactReferenceSQL exposes one forced compact kernel at
// the same distance or M0 hydration boundary as its production control.
func shortestBidirectionalCompactReferenceSQL(functionName string, direction graph.Direction, pathObserved bool) string {
	inbound := "false"
	if direction == graph.DirectionInbound {
		inbound = "true"
	}
	search := `with shortest as materialized (
  select depth, path as edge_ids
  from ` + functionName + `(
    @graph_id, @start_id, @end_id, @min_depth, @max_depth,
    @edge_kind_ids, ` + inbound + `, @state_limit, @frontier_limit, @predecessor_limit
  )
)`
	if !pathObserved {
		return search + ` select depth from shortest`
	}
	return search + shortestM0MaterializationSelect(direction)
}

// buildShortestReferenceSpecs assembles exact shortest-path comparators supported by the workload shape.
func buildShortestReferenceSpecs(testCase ScaleCase, probeParams map[string]any, nodeIDs, edgeIDs []int64, direction graph.Direction) []postgresReferenceSpec {
	searchNE := shortestReferenceSearchForDirection(direction)
	searchE := shortestEdgeReferenceSearch(direction)
	fullSQL := shortestDistanceReferenceSearchForDirection(direction) + ` select depth from shortest`
	boundary := "distance scalar"
	pathObserved := testCase.Name == "one_shortest_path_bound_pair" || testCase.Expected.ResultKind == "path_set"
	compactBidirectionalParams := copyReferenceParams(probeParams)
	compactBidirectionalParams["state_limit"] = int64(100_000)
	compactBidirectionalParams["frontier_limit"] = int64(100_000)
	compactBidirectionalParams["predecessor_limit"] = int64(100_000)
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
		{
			name:       "round_trip",
			boundary:   "prepared protocol and transaction",
			sql:        `select 1`,
			parameters: nil,
		},
		{
			name:       "endpoint_validation",
			boundary:   "validated endpoint IDs",
			sql:        `select id from node where graph_id = @graph_id and id = any(array[@start_id::int8, @end_id::int8]) order by id`,
			parameters: probeParams,
		},
		{
			name:       "minimum_graph_access",
			boundary:   "root adjacency edge IDs",
			sql:        `select e.id from edge e where e.graph_id = @graph_id and e.start_id = @start_id and (cardinality(@edge_kind_ids::int2[]) = 0 or e.kind_id = any(@edge_kind_ids::int2[])) order by e.id`,
			parameters: probeParams,
		},
		{
			name:             "search_ordered_ids",
			architecture:     "SP-S3-U-NE",
			observationShape: "ordered_ids",
			stateShape:       "ordered node and edge ID arrays",
			boundary:         "depth plus ordered node/edge IDs",
			sql:              searchNE + ` select depth, node_ids, edge_ids from shortest`,
			parameters:       probeParams,
		},
	}
	if edgeIDs != nil {
		specs = append(specs, postgresReferenceSpec{
			name:       "hydration_only",
			boundary:   "complete path composite from precomputed ordered edge IDs",
			sql:        hydrationSQL,
			parameters: hydrationParams,
		})
		if pathObserved && direction != graph.DirectionBoth {
			specs = append(specs,
				postgresReferenceSpec{
					name:               "m0_directed_hydration_only",
					architecture:       "MAT-M0",
					implementationID:   "directed_set_hydration_" + strings.ToLower(direction.String()) + "_v1",
					stateShape:         "precomputed ordered edge IDs; node order derived from directed edge endpoints",
					observationShape:   "complete path composite",
					semanticValidation: "precomputed_exact_path_inputs",
					boundary:           "directed complete path composite from precomputed ordered edge IDs",
					sql:                shortestM0HydrationSQL(direction),
					parameters:         hydrationParams,
					validationSQL:      hydrationSQL,
					validationParams:   hydrationParams,
				},
				postgresReferenceSpec{
					name:               "m1_ordered_ids_hydration_only",
					architecture:       "MAT-M1",
					implementationID:   "ordered_ids_set_hydration_v1",
					stateShape:         "precomputed ordered node and edge IDs",
					observationShape:   "complete path composite",
					semanticValidation: "precomputed_exact_path_inputs",
					boundary:           "complete path composite from precomputed ordered node and edge IDs",
					sql:                shortestM1HydrationSQL(),
					parameters:         hydrationParams,
					validationSQL:      hydrationSQL,
					validationParams:   hydrationParams,
				},
			)
		}
	}
	specs = append(specs, postgresReferenceSpec{
		name:               "s3_unidirectional_trail_cte",
		legacyName:         "complete_reference_s1_array_cte",
		architecture:       shortestArchitectureForCase(testCase),
		implementationID:   "inline_recursive_cte_unidirectional_v3",
		stateShape:         shortestS3UStateShape(testCase),
		observationShape:   observationShapeForCase(testCase),
		semanticValidation: "exact_public_observation",
		boundary:           boundary,
		fullComparator:     true,
		sql:                fullSQL,
		parameters:         probeParams,
	})
	if !pathObserved && direction == graph.DirectionInbound {
		canonicalParams := copyReferenceParams(probeParams)
		canonicalParams["start_id"], canonicalParams["end_id"] = probeParams["end_id"], probeParams["start_id"]
		specs = append(specs, postgresReferenceSpec{
			name:               "s4_canonical_source_distance",
			architecture:       "SP-I1-C-D",
			implementationID:   "canonical_relationship_source_distance_v1",
			stateShape:         "relationship-source-oriented node and depth set state",
			observationShape:   "distance scalar",
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                shortestDistanceReferenceSearchForDirection(graph.DirectionOutbound) + ` select depth from shortest`,
			parameters:         canonicalParams,
		})
	}
	if shortestS1DistanceEligible(testCase, probeParams, direction, pathObserved) {
		s1Params := copyReferenceParams(probeParams)
		s1Params["state_limit"] = int32(100_000)
		specs = append(specs, postgresReferenceSpec{
			name:               "s1_array_bfs_distance",
			architecture:       "SP-S1",
			implementationID:   "typed_plpgsql_array_bfs_distance_v1",
			stateShape:         "array-resident frontier and visited node IDs with explicit state ceiling; no path or predecessor state",
			observationShape:   "distance scalar",
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                shortestS1DistanceSQL(fullSQL, direction),
			parameters:         s1Params,
		})
	}
	if pathObserved && direction != graph.DirectionBoth {
		specs = append(specs,
			postgresReferenceSpec{
				name:               "s3_unidirectional_cte_m0_directed",
				architecture:       "SP-S3-U-E+MAT-M0",
				implementationID:   "s3_u_edge_search_directed_set_materializer_" + strings.ToLower(direction.String()) + "_v1",
				stateShape:         "edge-only recursive trail; materializer derives node order from directed edge endpoints",
				observationShape:   "public_observation",
				semanticValidation: "exact_public_observation",
				boundary:           boundary,
				fullComparator:     true,
				sql:                shortestM0FullSQL(searchE, direction),
				parameters:         probeParams,
			},
			postgresReferenceSpec{
				name:               "s3_unidirectional_cte_m1_ordered_ids",
				architecture:       "SP-S3-U-NE+MAT-M1",
				implementationID:   "s3_u_node_edge_search_ordered_ids_set_materializer_v1",
				stateShape:         "ordered node-and-edge recursive trails; materializer hydrates both streams by ordinal",
				observationShape:   "public_observation",
				semanticValidation: "exact_public_observation",
				boundary:           boundary,
				fullComparator:     true,
				sql:                shortestM1FullSQL(searchNE),
				parameters:         probeParams,
			},
		)

		witnessParams := copyReferenceParams(probeParams)
		witnessParams["search_start_id"], witnessParams["search_end_id"] = probeParams["start_id"], probeParams["end_id"]
		reverseForPublicPath := false
		if direction == graph.DirectionInbound {
			witnessParams["search_start_id"], witnessParams["search_end_id"] = probeParams["end_id"], probeParams["start_id"]
			reverseForPublicPath = true
		}
		witnessSearch := shortestCanonicalWitnessSearch(reverseForPublicPath)
		specs = append(specs, postgresReferenceSpec{
			name:               "s4_canonical_source_witness_m0",
			architecture:       "SP-I1-C-WE+MAT-M0",
			implementationID:   "canonical_source_compact_witness_m0_v1",
			stateShape:         "node/depth discovery plus one deterministic predecessor per witness depth; no recursive full trails",
			observationShape:   "public_observation",
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                shortestM0FullSQL(witnessSearch, direction),
			parameters:         witnessParams,
		})
	}
	if direction != graph.DirectionBoth {
		if pathObserved {
			specs = append(specs,
				postgresReferenceSpec{
					name:               "sp_b1_strict_alternating_witness_m0",
					architecture:       "SP-B1-C-ALT-NODE-WE+MAT-M0",
					implementationID:   "typed_bidirectional_strict_alternating_node_witness_m0_v1",
					stateShape:         "ID-only per-side FIFO, minimum-depth seen state, and one deterministic predecessor per accepted node",
					observationShape:   "public_observation",
					semanticValidation: "exact_public_observation",
					boundary:           boundary,
					fullComparator:     true,
					sql:                shortestBidirectionalCompactReferenceSQL("shortest_path_b1_strict_alternating", direction, true),
					parameters:         compactBidirectionalParams,
				},
				postgresReferenceSpec{
					name:               "sp_b2_smaller_frontier_witness_m0",
					architecture:       "SP-B2-C-MIN-LEVEL-WE+MAT-M0",
					implementationID:   "typed_bidirectional_smaller_current_level_witness_m0_v1",
					stateShape:         "ID-only per-side complete levels, minimum-depth seen state, and one deterministic predecessor per accepted node",
					observationShape:   "public_observation",
					semanticValidation: "exact_public_observation",
					boundary:           boundary,
					fullComparator:     true,
					sql:                shortestBidirectionalCompactReferenceSQL("shortest_path_b2_smaller_current_level", direction, true),
					parameters:         compactBidirectionalParams,
				},
			)
		} else {
			specs = append(specs,
				postgresReferenceSpec{
					name:               "sp_b1_strict_alternating_distance",
					architecture:       "SP-B1-C-ALT-NODE-D",
					implementationID:   "typed_bidirectional_strict_alternating_node_distance_v1",
					stateShape:         "ID-only per-side FIFO and minimum-depth seen state; witness predecessor retained outside the observation boundary",
					observationShape:   "distance scalar",
					semanticValidation: "exact_public_observation",
					boundary:           boundary,
					fullComparator:     true,
					sql:                shortestBidirectionalCompactReferenceSQL("shortest_path_b1_strict_alternating", direction, false),
					parameters:         compactBidirectionalParams,
				},
				postgresReferenceSpec{
					name:               "sp_b2_smaller_frontier_distance",
					architecture:       "SP-B2-C-MIN-LEVEL-D",
					implementationID:   "typed_bidirectional_smaller_current_level_distance_v1",
					stateShape:         "ID-only per-side complete levels and minimum-depth seen state; witness predecessor retained outside the observation boundary",
					observationShape:   "distance scalar",
					semanticValidation: "exact_public_observation",
					boundary:           boundary,
					fullComparator:     true,
					sql:                shortestBidirectionalCompactReferenceSQL("shortest_path_b2_smaller_current_level", direction, false),
					parameters:         compactBidirectionalParams,
				},
			)
		}
	}
	specs = append(specs, postgresReferenceSpec{
		name:               "s3_bidirectional_trail_cte",
		legacyName:         "candidate_s2_bidirectional_cte",
		architecture:       "SP-S3-B",
		implementationID:   "inline_recursive_cte_bidirectional_trails_v2",
		stateShape:         "paired per-row relationship trail arrays",
		observationShape:   observationShapeForCase(testCase),
		semanticValidation: "exact_public_observation",
		boundary:           boundary,
		fullComparator:     true,
		sql:                shortestBidirectionalReferenceSQL(testCase, direction),
		parameters:         probeParams,
	})
	return specs
}

// shortestS1DistanceEligible reports whether a case can use the bounded single-direction distance prototype.
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

// shortestS1DistanceSQL wraps a shortest-path query with the bounded S1 distance prototype.
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

// shortestArchitectureForCase chooses the witness-producing or distance-only S3 reference architecture from the case's observable result contract.
func shortestArchitectureForCase(testCase ScaleCase) string {
	if testCase.Expected.ResultKind == "path_set" || testCase.Name == "one_shortest_path_bound_pair" {
		return "SP-S3-U-NE"
	}
	return "SP-S3-U-D"
}

// shortestM0HydrationSQL returns SQL that hydrates paths from ordered relationship IDs.
func shortestM0HydrationSQL(direction graph.Direction) string {
	return `with shortest(edge_ids) as (select @edge_ids::int8[])` + shortestM0MaterializationSelect(direction)
}

// shortestM0FullSQL combines edge-only search with M0 path hydration.
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

// shortestM1HydrationSQL returns SQL that hydrates paths from ordered node and relationship IDs.
func shortestM1HydrationSQL() string {
	return `with shortest(node_ids, edge_ids) as (select @node_ids::int8[], @edge_ids::int8[])` + shortestM1MaterializationSelect()
}

// shortestM1FullSQL combines node-and-edge search with M1 path hydration.
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

// shortestS3UStateShape describes recursive state retained by the selected unidirectional search projection.
func shortestS3UStateShape(testCase ScaleCase) string {
	if testCase.Expected.ResultKind == "path_set" || testCase.Name == "one_shortest_path_bound_pair" {
		return "per-row node and relationship trail arrays"
	}
	return "distance frontier node and depth only; no path or predecessor state"
}

// shortestBidirectionalReferenceSQL returns the bidirectional shortest-path reference query for the requested result shape.
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

// fixedSuffixExpansionReferenceSpecs builds exact reference implementations for fixed-suffix expansion cases.
func (s *postgresSQLRunner) fixedSuffixExpansionReferenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	kindNames := []string{"ExpansionRoot", "SuffixHead", "SuffixMiddle", "SuffixTerminal", "Expand", "EnterSuffix", "ContinueSuffix", "CompleteSuffix"}
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
	specs := buildFixedSuffixExpansionReferenceSpecs(testCase, probeParams)
	if !referenceHydrationRequested(s.referenceArms) {
		return specs, nil
	}
	searchIdx := referenceSpecIndex(specs, "suffix_seeded_reverse_ordered_ids")
	values, err := readReferenceRow(ctx, s.db, specs[searchIdx].sql, specs[searchIdx].parameters, s.readTransactionOptions()...)
	if err != nil {
		return nil, fmt.Errorf("precompute fixed-suffix expansion hydration IDs: %w", err)
	}
	if len(values) == 0 {
		completeIdx := referenceSpecIndex(specs, "complete_reference")
		specs = slices.Insert(specs, completeIdx, postgresReferenceSpec{
			name:               "hydration_only",
			architecture:       "hydration",
			implementationID:   "typed_empty_v1",
			stateShape:         "empty ordered ID input",
			observationShape:   "typed empty path result",
			semanticValidation: "not_applicable_empty_input",
			boundary:           "typed empty path result",
			sql:                `select null::pathComposite where false`,
			parameters:         probeParams,
		})
		return specs, nil
	}
	if len(values) != 3 {
		return nil, fmt.Errorf("precompute fixed-suffix expansion hydration IDs returned %d columns, expected 3", len(values))
	}
	nodeIDs, err := referenceInt64Slice(values[0])
	if err != nil || len(nodeIDs) == 0 {
		return nil, fmt.Errorf("decode fixed-suffix expansion hydration node IDs: %w", err)
	}
	edgeIDs, err := referenceInt64Slice(values[2])
	if err != nil {
		return nil, fmt.Errorf("decode fixed-suffix expansion hydration edge IDs: %w", err)
	}
	hydrationParams := copyReferenceParams(probeParams)
	hydrationParams["root_id"] = nodeIDs[0]
	hydrationParams["edge_ids"] = edgeIDs
	hydration := postgresReferenceSpec{
		name:     "hydration_only",
		boundary: "one complete path composite from precomputed ordered edge IDs",
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

// referenceHydrationRequested reports whether the selected arm requires precomputed hydration inputs.
func referenceHydrationRequested(referenceArms []string) bool {
	return len(referenceArms) == 0 || slices.Contains(referenceArms, "hydration_only")
}

// buildFixedSuffixExpansionReferenceSpecs assembles fixed-suffix search and hydration references for one case.
func buildFixedSuffixExpansionReferenceSpecs(testCase ScaleCase, probeParams map[string]any) []postgresReferenceSpec {
	roots := `roots(root_id) as materialized (
  select n.id from node n
  where n.graph_id = @graph_id
    and @ExpansionRoot_kind::int2 = any(n.kind_ids)
    and n.properties ->> 'root_key' = @root_key
)`
	suffix := `suffix_rows(boundary_id, head_id, terminal_id, suffix_edge_ids, suffix_node_ids) as materialized (
  select boundary.id, suffix_head.id, suffix_terminal.id,
         array[enter_suffix.id, continue_suffix.id, complete_suffix.id]::int8[],
         array[boundary.id, suffix_head.id, suffix_middle.id, suffix_terminal.id]::int8[]
  from (select 1 from roots limit 1) root_presence
  cross join edge enter_suffix
  join node boundary on boundary.graph_id = @graph_id and boundary.id = enter_suffix.start_id
  join node suffix_head on suffix_head.graph_id = @graph_id and suffix_head.id = enter_suffix.end_id and @SuffixHead_kind::int2 = any(suffix_head.kind_ids)
  join edge continue_suffix on continue_suffix.graph_id = @graph_id and continue_suffix.start_id = suffix_head.id and continue_suffix.kind_id = @ContinueSuffix_kind
  join node suffix_middle on suffix_middle.graph_id = @graph_id and suffix_middle.id = continue_suffix.end_id and @SuffixMiddle_kind::int2 = any(suffix_middle.kind_ids)
  join edge complete_suffix on complete_suffix.graph_id = @graph_id and complete_suffix.start_id = suffix_middle.id and complete_suffix.kind_id = @CompleteSuffix_kind
  join node suffix_terminal on suffix_terminal.graph_id = @graph_id and suffix_terminal.id = complete_suffix.end_id and @SuffixTerminal_kind::int2 = any(suffix_terminal.kind_ids)
  where enter_suffix.graph_id = @graph_id and enter_suffix.kind_id = @EnterSuffix_kind
    and continue_suffix.id <> enter_suffix.id
    and complete_suffix.id <> enter_suffix.id and complete_suffix.id <> continue_suffix.id
)`
	forwardExpansion := `expansion_paths(root_id, node_id, node_ids, edge_ids, depth) as (
  select root_id, root_id, array[root_id]::int8[], array[]::int8[], 0 from roots
  union all
  select expansion_paths.root_id, e.end_id, expansion_paths.node_ids || e.end_id, expansion_paths.edge_ids || e.id, expansion_paths.depth + 1
  from expansion_paths join edge e
    on e.graph_id = @graph_id and e.start_id = expansion_paths.node_id and e.kind_id = @Expand_kind
  join node next_node on next_node.graph_id = @graph_id and next_node.id = e.end_id
  where expansion_paths.depth < @max_depth and e.id != all(expansion_paths.edge_ids)
)`
	scalarForwardExpansion := strings.Replace(forwardExpansion, "\n  join node next_node on next_node.graph_id = @graph_id and next_node.id = e.end_id", "", 1)
	allExpansionNodesExist := `not exists (
	      select 1 from unnest(expansion_paths.node_ids) as expansion_node_id(id)
	      left join node expansion_node on expansion_node.graph_id = @graph_id and expansion_node.id = expansion_node_id.id
      where expansion_node.id is null
    )`
	legacyForward := `with recursive ` + roots + `, ` + forwardExpansion + `, paths as materialized (
  select expansion_paths.node_ids || array[suffix_head.id, suffix_middle.id, suffix_terminal.id]::int8[] as node_ids,
         suffix_head.id as head_id, suffix_terminal.id as terminal_id,
         expansion_paths.edge_ids || enter_suffix.id || continue_suffix.id || complete_suffix.id as edge_ids
  from expansion_paths
  join edge enter_suffix on enter_suffix.graph_id = @graph_id and enter_suffix.start_id = expansion_paths.node_id and enter_suffix.kind_id = @EnterSuffix_kind and enter_suffix.id != all(expansion_paths.edge_ids)
  join node suffix_head on suffix_head.graph_id = @graph_id and suffix_head.id = enter_suffix.end_id and @SuffixHead_kind::int2 = any(suffix_head.kind_ids)
  join edge continue_suffix on continue_suffix.graph_id = @graph_id and continue_suffix.start_id = suffix_head.id and continue_suffix.kind_id = @ContinueSuffix_kind
    and continue_suffix.id != enter_suffix.id and continue_suffix.id != all(expansion_paths.edge_ids)
  join node suffix_middle on suffix_middle.graph_id = @graph_id and suffix_middle.id = continue_suffix.end_id and @SuffixMiddle_kind::int2 = any(suffix_middle.kind_ids)
  join edge complete_suffix on complete_suffix.graph_id = @graph_id and complete_suffix.start_id = suffix_middle.id and complete_suffix.kind_id = @CompleteSuffix_kind
    and complete_suffix.id != enter_suffix.id and complete_suffix.id != continue_suffix.id and complete_suffix.id != all(expansion_paths.edge_ids)
  join node suffix_terminal on suffix_terminal.graph_id = @graph_id and suffix_terminal.id = complete_suffix.end_id and @SuffixTerminal_kind::int2 = any(suffix_terminal.kind_ids)
  where expansion_paths.depth >= @min_depth
)`
	lateHydratedForward := `with recursive ` + roots + `, ` + scalarForwardExpansion + `, paths as materialized (
  select expansion_paths.node_ids || array[suffix_head.id, suffix_middle.id, suffix_terminal.id]::int8[] as node_ids,
         suffix_head.id as head_id, suffix_terminal.id as terminal_id,
         expansion_paths.edge_ids || enter_suffix.id || continue_suffix.id || complete_suffix.id as edge_ids
  from expansion_paths
  join edge enter_suffix on enter_suffix.graph_id = @graph_id and enter_suffix.start_id = expansion_paths.node_id and enter_suffix.kind_id = @EnterSuffix_kind and enter_suffix.id != all(expansion_paths.edge_ids)
  join node suffix_head on suffix_head.graph_id = @graph_id and suffix_head.id = enter_suffix.end_id and @SuffixHead_kind::int2 = any(suffix_head.kind_ids)
  join edge continue_suffix on continue_suffix.graph_id = @graph_id and continue_suffix.start_id = suffix_head.id and continue_suffix.kind_id = @ContinueSuffix_kind
    and continue_suffix.id != enter_suffix.id and continue_suffix.id != all(expansion_paths.edge_ids)
  join node suffix_middle on suffix_middle.graph_id = @graph_id and suffix_middle.id = continue_suffix.end_id and @SuffixMiddle_kind::int2 = any(suffix_middle.kind_ids)
  join edge complete_suffix on complete_suffix.graph_id = @graph_id and complete_suffix.start_id = suffix_middle.id and complete_suffix.kind_id = @CompleteSuffix_kind
    and complete_suffix.id != enter_suffix.id and complete_suffix.id != continue_suffix.id and complete_suffix.id != all(expansion_paths.edge_ids)
  join node suffix_terminal on suffix_terminal.graph_id = @graph_id and suffix_terminal.id = complete_suffix.end_id and @SuffixTerminal_kind::int2 = any(suffix_terminal.kind_ids)
  where expansion_paths.depth >= @min_depth and ` + allExpansionNodesExist + `
)`
	factoredForward := `with recursive ` + roots + `, ` + suffix + `, ` + scalarForwardExpansion + `, paths as materialized (
  select expansion_paths.node_ids || suffix_rows.suffix_node_ids[2:4] as node_ids,
         suffix_rows.head_id, suffix_rows.terminal_id,
         expansion_paths.edge_ids || suffix_rows.suffix_edge_ids as edge_ids
  from expansion_paths join suffix_rows on suffix_rows.boundary_id = expansion_paths.node_id
  where expansion_paths.depth >= @min_depth
	    and not exists (select 1 from unnest(expansion_paths.edge_ids) as expansion_edge(id) where expansion_edge.id = any(suffix_rows.suffix_edge_ids))
    and ` + allExpansionNodesExist + `
)`
	reverse := `with recursive ` + roots + `, ` + suffix + `, boundary_ids(boundary_id) as materialized (
  select distinct boundary_id from suffix_rows
), reverse_trails(boundary_id, node_id, node_ids, edge_ids, depth) as (
  select boundary_id, boundary_id, array[boundary_id]::int8[], array[]::int8[], 0 from boundary_ids
  union all
	  select reverse_trails.boundary_id, e.start_id, array_prepend(e.start_id, reverse_trails.node_ids),
	         array_prepend(e.id, reverse_trails.edge_ids), reverse_trails.depth + 1
  from reverse_trails join edge e
    on e.graph_id = @graph_id and e.end_id = reverse_trails.node_id and e.kind_id = @Expand_kind
  where reverse_trails.depth < @max_depth and e.id != all(reverse_trails.edge_ids)
), paths as materialized (
  select reverse_trails.node_ids || suffix_rows.suffix_node_ids[2:4] as node_ids,
         suffix_rows.head_id, suffix_rows.terminal_id,
         reverse_trails.edge_ids || suffix_rows.suffix_edge_ids as edge_ids
  from reverse_trails
  join roots on roots.root_id = reverse_trails.node_id
  join suffix_rows on suffix_rows.boundary_id = reverse_trails.boundary_id
  where reverse_trails.depth >= @min_depth
	    and not exists (select 1 from unnest(reverse_trails.edge_ids) as expansion_edge(id) where expansion_edge.id = any(suffix_rows.suffix_edge_ids))
	    and not exists (
	      select 1 from unnest(reverse_trails.node_ids) as expansion_node_id(id)
	      left join node expansion_node on expansion_node.graph_id = @graph_id and expansion_node.id = expansion_node_id.id
      where expansion_node.id is null
    )
)`
	viability := `with recursive ` + roots + `, ` + suffix + `, boundary_ids(boundary_id) as materialized (
  select distinct boundary_id from suffix_rows
), viable(node_id, reverse_distance) as (
  select boundary_id, 0 from boundary_ids
  union
  select e.start_id, viable.reverse_distance + 1
  from viable join edge e
    on e.graph_id = @graph_id and e.end_id = viable.node_id and e.kind_id = @Expand_kind
  where viable.reverse_distance < @max_depth
), expansion_paths(root_id, node_id, node_ids, edge_ids, depth) as (
  select root_id, root_id, array[root_id]::int8[], array[]::int8[], 0 from roots
  where exists (select 1 from viable where viable.node_id = roots.root_id and viable.reverse_distance <= @max_depth)
  union all
  select expansion_paths.root_id, e.end_id, expansion_paths.node_ids || e.end_id, expansion_paths.edge_ids || e.id, expansion_paths.depth + 1
  from expansion_paths join edge e
    on e.graph_id = @graph_id and e.start_id = expansion_paths.node_id and e.kind_id = @Expand_kind
  where expansion_paths.depth < @max_depth and e.id != all(expansion_paths.edge_ids)
    and exists (select 1 from viable where viable.node_id = e.end_id and viable.reverse_distance <= @max_depth - expansion_paths.depth - 1)
), paths as materialized (
  select expansion_paths.node_ids || suffix_rows.suffix_node_ids[2:4] as node_ids,
         suffix_rows.head_id, suffix_rows.terminal_id,
         expansion_paths.edge_ids || suffix_rows.suffix_edge_ids as edge_ids
  from expansion_paths join suffix_rows on suffix_rows.boundary_id = expansion_paths.node_id
  where expansion_paths.depth >= @min_depth
	    and not exists (select 1 from unnest(expansion_paths.edge_ids) as expansion_edge(id) where expansion_edge.id = any(suffix_rows.suffix_edge_ids))
    and ` + allExpansionNodesExist + `
)`
	fullSQL := legacyForward + ` select head_id, terminal_id from paths`
	boundary := "endpoint ID pairs"
	pathObserved := testCase.Observes.Paths || testCase.Expected.ResultKind == "path_set"
	complete := func(search string) string {
		if !pathObserved {
			return search + ` select head_id, terminal_id from paths`
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
	orderedLegacy := legacyForward + ` select node_ids, head_id, edge_ids from paths`
	orderedReference := func(spec postgresReferenceSpec) postgresReferenceSpec {
		spec.semanticValidation = "exact_ordered_ids"
		spec.validationSQL = orderedLegacy
		spec.validationParams = probeParams
		return spec
	}
	return []postgresReferenceSpec{
		{
			name:         "round_trip",
			architecture: "protocol",
			stateShape:   "none",
			boundary:     "prepared protocol and transaction",
			sql:          `select 1`,
		},
		{
			name:         "endpoint_validation",
			architecture: "root_validation",
			stateShape:   "root ID bag",
			boundary:     "validated root ID",
			sql:          `select n.id from node n where n.graph_id = @graph_id and @ExpansionRoot_kind::int2 = any(n.kind_ids) and n.properties ->> 'root_key' = @root_key`,
			parameters:   probeParams,
		},
		{
			name:         "fixed_suffix_rows",
			architecture: "factored_suffix",
			stateShape:   "boundary and ordered suffix IDs",
			boundary:     "exact suffix rows and distinct boundary IDs",
			sql:          `with ` + roots + `, ` + suffix + ` select boundary_id, head_id, terminal_id, suffix_edge_ids from suffix_rows`,
			parameters:   probeParams,
		},
		{
			name:         "minimum_graph_access",
			architecture: "root_adjacency",
			stateShape:   "edge IDs",
			boundary:     "root adjacency edge IDs",
			sql:          `with ` + roots + ` select e.id from roots join edge e on e.graph_id = @graph_id and e.start_id = roots.root_id and e.kind_id = @Expand_kind order by e.id`,
			parameters:   probeParams,
		},
		orderedReference(postgresReferenceSpec{
			name:             "search_ordered_ids",
			architecture:     "EXPANSION-STEPWISE-FORWARD-SQL",
			observationShape: "ordered_ids",
			stateShape:       "root/boundary IDs and ordered relationship trail",
			boundary:         "ordered node/edge IDs without hydration",
			sql:              orderedLegacy,
			parameters:       probeParams,
		}),
		orderedReference(postgresReferenceSpec{
			name:             "stepwise_forward_aa_ordered_ids",
			architecture:     "EXPANSION-STEPWISE-FORWARD-AA",
			aaAliasOf:        "search_ordered_ids",
			observationShape: "ordered_ids",
			stateShape:       "root/boundary IDs and ordered relationship trail",
			boundary:         "ordered node/edge IDs",
			sql:              orderedLegacy,
			parameters:       probeParams,
		}),
		orderedReference(postgresReferenceSpec{
			name:             "root_reuse_ordered_ids",
			architecture:     "EXPANSION-STEPWISE-FORWARD-AA",
			aaAliasOf:        "search_ordered_ids",
			observationShape: "ordered_ids",
			stateShape:       "root/boundary IDs and ordered relationship trail",
			boundary:         "ordered node/edge IDs",
			sql:              orderedLegacy,
			parameters:       probeParams,
		}),
		orderedReference(postgresReferenceSpec{
			name:             "late_hydration_ordered_ids",
			architecture:     "EXPANSION-LATE-HYDRATED-FORWARD",
			observationShape: "ordered_ids",
			stateShape:       "scalar expansion state and ordered relationship trail",
			boundary:         "ordered node/edge IDs",
			sql:              lateHydratedForward + ` select node_ids, head_id, edge_ids from paths`,
			parameters:       probeParams,
		}),
		orderedReference(postgresReferenceSpec{
			name:             "factored_suffix_forward_ordered_ids",
			architecture:     "EXPANSION-FACTORED-SUFFIX-FORWARD",
			observationShape: "ordered_ids",
			stateShape:       "scalar forward trails joined to exact suffix bag",
			boundary:         "ordered node/edge IDs",
			sql:              factoredForward + ` select node_ids, head_id, edge_ids from paths`,
			parameters:       probeParams,
		}),
		orderedReference(postgresReferenceSpec{
			name:             "suffix_seeded_reverse_ordered_ids",
			architecture:     "EXPANSION-SUFFIX-SEEDED-REVERSE",
			observationShape: "ordered_ids",
			stateShape:       "scalar reverse trails with prepended relationship IDs",
			boundary:         "ordered node/edge IDs",
			sql:              reverse + ` select node_ids, head_id, edge_ids from paths`,
			parameters:       probeParams,
		}),
		orderedReference(postgresReferenceSpec{
			name:             "backward_viability_forward_ordered_ids",
			architecture:     "EXPANSION-BACKWARD-VIABILITY-FORWARD",
			observationShape: "ordered_ids",
			stateShape:       "depth-aware viability filter plus exact forward trails",
			boundary:         "ordered node/edge IDs",
			sql:              viability + ` select node_ids, head_id, edge_ids from paths`,
			parameters:       probeParams,
		}),
		{
			name:               "complete_reference",
			architecture:       "EXPANSION-STEPWISE-FORWARD-SQL",
			stateShape:         "forward relationship trails",
			observationShape:   observationShapeForCase(testCase),
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                fullSQL,
			parameters:         probeParams,
		},
		{
			name:               "root_reuse_complete",
			architecture:       "EXPANSION-STEPWISE-FORWARD-AA",
			aaAliasOf:          "complete_reference",
			stateShape:         "forward relationship trails",
			observationShape:   observationShapeForCase(testCase),
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                complete(legacyForward),
			parameters:         probeParams,
		},
		{
			name:               "late_hydration_complete",
			architecture:       "EXPANSION-LATE-HYDRATED-FORWARD",
			stateShape:         "scalar expansion state with final-only hydration",
			observationShape:   observationShapeForCase(testCase),
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                complete(lateHydratedForward),
			parameters:         probeParams,
		},
		{
			name:               "factored_suffix_forward_complete",
			architecture:       "EXPANSION-FACTORED-SUFFIX-FORWARD",
			stateShape:         "exact forward trails joined to suffix bag",
			observationShape:   observationShapeForCase(testCase),
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                complete(factoredForward),
			parameters:         probeParams,
		},
		{
			name:               "suffix_seeded_reverse_complete",
			architecture:       "EXPANSION-SUFFIX-SEEDED-REVERSE",
			stateShape:         "exact reverse trails joined back to suffix bag",
			observationShape:   observationShapeForCase(testCase),
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                complete(reverse),
			parameters:         probeParams,
		},
		{
			name:               "backward_viability_forward_complete",
			architecture:       "EXPANSION-BACKWARD-VIABILITY-FORWARD",
			stateShape:         "permissive viability plus exact forward trails",
			observationShape:   observationShapeForCase(testCase),
			semanticValidation: "exact_public_observation",
			boundary:           boundary,
			fullComparator:     true,
			sql:                complete(viability),
			parameters:         probeParams,
		},
	}
}

// observationShapeForCase selects full public path observations when the case exposes paths and endpoint IDs otherwise.
func observationShapeForCase(testCase ScaleCase) string {
	if testCase.Observes.Paths || testCase.Expected.ResultKind == "path_set" {
		return "public_observation"
	}
	return "endpoint_ids"
}

// referenceSpecIndex returns a reference arm's index and panics when the arm is absent.
func referenceSpecIndex(specs []postgresReferenceSpec, name string) int {
	for idx, spec := range specs {
		if spec.name == name {
			return idx
		}
	}
	panic("missing PostgreSQL reference spec " + name)
}

// referenceSpecIndexOrMissing returns a reference arm's index or -1 when absent.
func referenceSpecIndexOrMissing(specs []postgresReferenceSpec, name string) int {
	for idx, spec := range specs {
		if spec.name == name {
			return idx
		}
	}
	return -1
}

// readReferenceRow reads reference row and propagates I/O or decoding failures.
func readReferenceRow(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any, transactionOptions ...graph.TransactionOption) ([]any, error) {
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
	}, transactionOptions...)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// referenceInt64Slice normalizes supported driver array representations to []int64.
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

// copyReferenceParams duplicates reference params without aliasing mutable state.
func copyReferenceParams(params map[string]any) map[string]any {
	copy := make(map[string]any, len(params)+10)
	for name, value := range params {
		copy[name] = value
	}
	return copy
}

// measureRawPostgres executes raw PostgreSQL and records its timing observations.
func measureRawPostgres(ctx context.Context, db graph.Database, sqlQuery string, params map[string]any, warmupIterations, iterations int, transactionOptions ...graph.TransactionOption) (int64, DurationStats, error) {
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
		}, transactionOptions...)
		if err != nil {
			return 0, err
		}

		return count, nil
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
	stats.Samples = append([]LatencySample{{
		Iteration:      0,
		Classification: "cold",
		Duration:       coldDuration,
	}}, stats.Samples...)
	return rowCount, stats, nil
}

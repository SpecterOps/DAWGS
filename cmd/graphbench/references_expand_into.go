// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

const expandIntoInputPairs = `input_pairs(pair_ordinal, start_id, end_id) as materialized (
  select pair_ordinal, start_id, @end_id::int8
  from unnest(@start_ids::int8[]) with ordinality input(start_id, pair_ordinal)
)`

// expandIntoReferenceSpecs builds three exact one-hop bound-pair arms sharing the same public relationship boundary.
func (s *postgresSQLRunner) expandIntoReferenceSpecs(ctx context.Context, testCase ScaleCase, params map[string]any) ([]postgresReferenceSpec, error) {
	probeParams := copyReferenceParams(params)
	probeParams["graph_id"] = s.graphID
	edgeKinds := make(graph.Kinds, 0, len(testCase.Shape.EdgeKinds))
	for _, name := range testCase.Shape.EdgeKinds {
		edgeKinds = append(edgeKinds, graph.StringKind(name))
	}
	var edgeKindIDs []int16
	if len(edgeKinds) > 0 {
		if s.pgDriver == nil {
			return nil, fmt.Errorf("map ExpandInto reference edge kinds: PostgreSQL driver is unavailable")
		}
		var err error
		edgeKindIDs, err = s.pgDriver.KindMapper().MapKinds(ctx, edgeKinds)
		if err != nil {
			return nil, fmt.Errorf("map ExpandInto reference edge kinds: %w", err)
		}
	}
	probeParams["edge_kind_ids"] = edgeKindIDs
	return buildExpandIntoReferenceSpecs(probeParams, testCase.Shape.Direction), nil
}

// buildExpandIntoReferenceSpecs constructs the exact SQL arms after graph/kind parameters are resolved.
func buildExpandIntoReferenceSpecs(probeParams map[string]any, direction string) []postgresReferenceSpec {
	pairJoinPredicate := expandIntoPairPredicate(direction, "matched", "input_pairs")
	startDegreePredicate := expandIntoEndpointPredicate(direction, true, "start_adj", "input_pairs")
	endDegreePredicate := expandIntoEndpointPredicate(direction, false, "end_adj", "input_pairs")
	startScanPredicate := expandIntoEndpointPredicate(direction, true, "outbound", "input_pairs")
	endScanPredicate := expandIntoEndpointPredicate(direction, false, "inbound", "input_pairs")
	startPairPredicate := expandIntoPairPredicate(direction, "outbound", "input_pairs")
	endPairPredicate := expandIntoPairPredicate(direction, "inbound", "input_pairs")
	cachePairPredicate := expandIntoPairPredicate(direction, "matched", "distinct_pairs")

	pairJoin := `with ` + expandIntoInputPairs + `
select (matched.id, matched.start_id, matched.end_id, matched.kind_id, matched.properties)::edgeComposite
from input_pairs
join edge matched on matched.graph_id = @graph_id
	and ` + pairJoinPredicate + `
  and (cardinality(@edge_kind_ids::int2[]) = 0 or matched.kind_id = any(@edge_kind_ids::int2[]))`

	lowerDegree := `with ` + expandIntoInputPairs + `
select (matched.id, matched.start_id, matched.end_id, matched.kind_id, matched.properties)::edgeComposite
from input_pairs
join lateral (
  with degrees as materialized (
    select
		(select count(*) from edge start_adj
		 where start_adj.graph_id = @graph_id and ` + startDegreePredicate + `
		   and (cardinality(@edge_kind_ids::int2[]) = 0 or start_adj.kind_id = any(@edge_kind_ids::int2[]))) as start_degree,
		(select count(*) from edge end_adj
		 where end_adj.graph_id = @graph_id and ` + endDegreePredicate + `
		   and (cardinality(@edge_kind_ids::int2[]) = 0 or end_adj.kind_id = any(@edge_kind_ids::int2[]))) as end_degree
  )
  select candidate.*
  from degrees
  join lateral (
    select outbound.* from edge outbound
    where degrees.start_degree <= degrees.end_degree
		and outbound.graph_id = @graph_id and ` + startScanPredicate + `
		and ` + startPairPredicate + `
      and (cardinality(@edge_kind_ids::int2[]) = 0 or outbound.kind_id = any(@edge_kind_ids::int2[]))
    union all
    select inbound.* from edge inbound
    where degrees.end_degree < degrees.start_degree
		and inbound.graph_id = @graph_id and ` + endScanPredicate + `
		and ` + endPairPredicate + `
      and (cardinality(@edge_kind_ids::int2[]) = 0 or inbound.kind_id = any(@edge_kind_ids::int2[]))
  ) candidate on true
) matched on true`

	pairCache := `with ` + expandIntoInputPairs + `,
distinct_pairs(start_id, end_id) as materialized (
  select distinct start_id, end_id from input_pairs
), pair_matches(start_id, end_id, id, edge_start_id, edge_end_id, kind_id, properties) as materialized (
  select distinct_pairs.start_id, distinct_pairs.end_id,
         matched.id, matched.start_id, matched.end_id, matched.kind_id, matched.properties
  from distinct_pairs
  join edge matched on matched.graph_id = @graph_id
		and ` + cachePairPredicate + `
    and (cardinality(@edge_kind_ids::int2[]) = 0 or matched.kind_id = any(@edge_kind_ids::int2[]))
)
select (pair_matches.id, pair_matches.edge_start_id, pair_matches.edge_end_id, pair_matches.kind_id, pair_matches.properties)::edgeComposite
from input_pairs
join pair_matches on pair_matches.start_id = input_pairs.start_id and pair_matches.end_id = input_pairs.end_id`

	return []postgresReferenceSpec{
		{
			name: "expand_into_pair_join", architecture: "EXPAND-INTO-PAIR-JOIN",
			implementationID:   "expand_into_parameterized_pair_join_v2",
			stateShape:         "outer pair rows joined directly to matching relationships",
			observationShape:   "complete relationship composites",
			semanticValidation: "exact_public_observation", boundary: "complete matching relationships",
			fullComparator: true, sql: pairJoin, parameters: probeParams,
		},
		{
			name: "expand_into_lower_degree_scan", architecture: "EXPAND-INTO-LOWER-DEGREE",
			implementationID:   "expand_into_typed_lower_degree_scan_v2",
			stateShape:         "per-pair typed directional degrees plus one disjoint adjacency scan",
			observationShape:   "complete relationship composites",
			semanticValidation: "exact_public_observation", boundary: "complete matching relationships",
			fullComparator: true, sql: lowerDegree, parameters: probeParams,
		},
		{
			name: "expand_into_pair_cache", architecture: "EXPAND-INTO-PAIR-CACHE",
			implementationID:   "expand_into_distinct_pair_match_cache_v2",
			stateShape:         "statement-local distinct pair keys and every matching relationship row",
			observationShape:   "complete relationship composites with duplicate outer-row multiplicity reapplied",
			semanticValidation: "exact_public_observation", boundary: "complete matching relationships",
			fullComparator: true, sql: pairCache, parameters: probeParams,
		},
	}
}

// expandIntoPairPredicate returns the complete physical edge predicate for one
// logical bound pair. The directionless form uses one OR predicate rather than
// UNION ALL so a self-loop is emitted once, matching Cypher relationship
// multiplicity.
func expandIntoPairPredicate(direction, edgeAlias, pairAlias string) string {
	outbound := fmt.Sprintf("%s.start_id = %s.start_id and %s.end_id = %s.end_id", edgeAlias, pairAlias, edgeAlias, pairAlias)
	inbound := fmt.Sprintf("%s.end_id = %s.start_id and %s.start_id = %s.end_id", edgeAlias, pairAlias, edgeAlias, pairAlias)
	switch direction {
	case "inbound":
		return inbound
	case "directionless":
		return "((" + outbound + ") or (" + inbound + "))"
	default:
		return outbound
	}
}

// expandIntoEndpointPredicate returns the physical adjacency predicate for the
// logical start or end endpoint used by the lower-degree reference arm.
func expandIntoEndpointPredicate(direction string, logicalStart bool, edgeAlias, pairAlias string) string {
	pairColumn := "end_id"
	if logicalStart {
		pairColumn = "start_id"
	}
	physicalColumn := pairColumn
	if direction == "inbound" {
		if physicalColumn == "start_id" {
			physicalColumn = "end_id"
		} else {
			physicalColumn = "start_id"
		}
	}
	if direction == "directionless" {
		return fmt.Sprintf("(%s.start_id = %s.%s or %s.end_id = %s.%s)", edgeAlias, pairAlias, pairColumn, edgeAlias, pairAlias, pairColumn)
	}
	return fmt.Sprintf("%s.%s = %s.%s", edgeAlias, physicalColumn, pairAlias, pairColumn)
}

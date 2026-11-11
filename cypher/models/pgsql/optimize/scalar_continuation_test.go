// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
//
// SPDX-License-Identifier: Apache-2.0

package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// fieldRequirementForSymbol optimizes cypherQuery and returns the field-requirement decision for symbol.
func fieldRequirementForSymbol(t *testing.T, cypherQuery, symbol string) FieldRequirementDecision {
	t.Helper()

	query, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	require.NoError(t, err)

	plan, err := Optimize(query)
	require.NoError(t, err)

	for _, decision := range plan.LoweringPlan.FieldRequirements {
		if decision.Symbol == symbol {
			return decision
		}
	}

	require.FailNow(t, "field requirement decision not found", symbol)
	return FieldRequirementDecision{}
}

// TestScalarContinuationFieldRequirementAllowsIDOnlyObservation verifies that an ID consumer permits scalar continuation state.
func TestScalarContinuationFieldRequirementAllowsIDOnlyObservation(t *testing.T) {
	t.Parallel()

	decision := fieldRequirementForSymbol(t,
		`MATCH (s)-[*1..]->(mid)-[]->(e) RETURN id(mid), id(e)`,
		"mid",
	)

	require.Contains(t, decision.Fields, FieldRequirementEntityID)
	require.NotContains(t, decision.Fields, FieldRequirementFullEntity)
}

// TestScalarContinuationFieldRequirementRetainsFullEntityForMutation verifies that mutation prevents scalar-only continuation state.
func TestScalarContinuationFieldRequirementRetainsFullEntityForMutation(t *testing.T) {
	t.Parallel()

	decision := fieldRequirementForSymbol(t,
		`MATCH (s)-[*1..]->(mid)-[]->(e) DELETE mid`,
		"mid",
	)

	require.Contains(t, decision.Fields, FieldRequirementFullEntity)
}

func TestInlineMapFieldRequirements(t *testing.T) {
	for _, query := range []string{
		`MATCH (s)-[]->(m)-[]->(e {x: m.x}) RETURN id(e)`,
		`MATCH (s)-[]->(m)-[{x: m.x}]->(e) RETURN id(e)`,
		`MATCH (s)-[]->(m)-[]->(e {x: id(m)}) RETURN id(e)`,
	} {
		t.Run(query, func(t *testing.T) {
			decision := fieldRequirementForSymbol(t, query, "m")
			require.Contains(t, decision.Fields, FieldRequirementEntityID)
			if query != `MATCH (s)-[]->(m)-[]->(e {x: id(m)}) RETURN id(e)` {
				require.Contains(t, decision.Fields, FieldRequirementProperties)
			} else {
				require.NotContains(t, decision.Fields, FieldRequirementFullEntity)
			}
		})
	}
}

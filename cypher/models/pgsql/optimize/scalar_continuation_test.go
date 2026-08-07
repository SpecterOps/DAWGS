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

func TestScalarContinuationFieldRequirementAllowsIDOnlyObservation(t *testing.T) {
	t.Parallel()

	decision := fieldRequirementForSymbol(t,
		`MATCH (s)-[*1..]->(mid)-[]->(e) RETURN id(mid), id(e)`,
		"mid",
	)

	require.Contains(t, decision.Fields, FieldRequirementEntityID)
	require.NotContains(t, decision.Fields, FieldRequirementFullEntity)
}

func TestScalarContinuationFieldRequirementRetainsFullEntityForMutation(t *testing.T) {
	t.Parallel()

	decision := fieldRequirementForSymbol(t,
		`MATCH (s)-[*1..]->(mid)-[]->(e) DELETE mid`,
		"mid",
	)

	require.Contains(t, decision.Fields, FieldRequirementFullEntity)
}

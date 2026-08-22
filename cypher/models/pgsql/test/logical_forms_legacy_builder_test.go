// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// translateLegacyQuery builds legacy criteria, translates the resulting Cypher, and returns formatted SQL and metadata.
func translateLegacyQuery(t *testing.T, criteria ...graph.Criteria) (string, translate.Result) {
	t.Helper()

	builder := query.NewBuilderWithCriteria(criteria...)
	regularQuery, err := builder.Build(false)
	require.NoError(t, err)

	translation, err := translate.Translate(context.Background(), regularQuery, newKindMapper(), nil, translate.DefaultGraphID)
	require.NoError(t, err)

	formatted, err := translate.Translated(translation)
	require.NoError(t, err)
	return formatted, translation
}

// TestLegacyBuilderPostgreSQL_LogicalForms verifies boolean grouping, typed thresholds, and binding-local predicates in migrated builder queries.
func TestLegacyBuilderPostgreSQL_LogicalForms(t *testing.T) {
	t.Run("LOGIC-01 branch-local relationship kinds", func(t *testing.T) {
		formatted, _ := translateLegacyQuery(t,
			query.Where(query.Or(
				query.And(
					query.Equals(query.StartID(), graph.ID(101)),
					query.Equals(query.EndID(), graph.ID(202)),
					query.KindIn(query.Relationship(), graph.StringKind("RegressionKind01")),
				),
				query.And(
					query.Equals(query.StartID(), graph.ID(202)),
					query.Equals(query.EndID(), graph.ID(101)),
					query.KindIn(query.Relationship(), graph.StringKind("RegressionKind02")),
				),
			)),
			query.Returning(query.RelationshipID()),
		)

		require.Contains(t, formatted, " or ")
		require.Contains(t, formatted, "n0.id = @pi0")
		require.Contains(t, formatted, "n1.id = @pi1")
		require.Contains(t, formatted, "n0.id = @pi2")
		require.Contains(t, formatted, "n1.id = @pi3")
		require.Contains(t, formatted, "e0.kind_id = any")
	})

	t.Run("LOGIC-02 cross-binding temporal disjunction", func(t *testing.T) {
		formatted, _ := translateLegacyQuery(t,
			query.Where(query.Or(
				query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
				query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
			)),
			query.Returning(query.RelationshipID()),
		)

		require.Contains(t, formatted, "e0.properties -> 'lastseen'")
		require.Contains(t, formatted, "n0.properties -> 'lastcollected'")
		require.Contains(t, formatted, "n1.properties -> 'lastcollected'")
		require.Contains(t, formatted, " or ")
	})

	t.Run("LOGIC-03 typed threshold and scoped negation", func(t *testing.T) {
		threshold := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
		formatted, translation := translateLegacyQuery(t,
			query.Where(query.And(
				query.Not(query.KindIn(query.Node(), graph.StringKind("RegressionKind03"))),
				query.Or(
					query.Not(query.Exists(query.NodeProperty("lastseen"))),
					query.Before(query.NodeProperty("lastseen"), threshold),
				),
			)),
			query.Returning(query.NodeID()),
		)

		require.Contains(t, formatted, "not")
		require.Contains(t, formatted, " or ")
		require.Contains(t, formatted, "n0.properties -> 'lastseen'")
		require.Contains(t, formatted, "@pi0")
		require.Equal(t, map[string]any{"pi0": threshold}, translation.Parameters)
	})
}

// TestLegacyBuilderPostgreSQL_LOGIC05ProjectionOrder verifies that migrated projections preserve caller-specified column order.
func TestLegacyBuilderPostgreSQL_LOGIC05ProjectionOrder(t *testing.T) {
	testCases := map[string]struct {
		// projection supplies the legacy graph criteria for the case.
		projection *graphProjection
		// columns lists the SQL fragments in their required projection order.
		columns []string
	}{
		"full opposite node plus relationship": {
			projection: projectionOf(query.Relationship(), query.End()),
			columns:    []string{"select s0.e0 as r", "s0.n1 as e"},
		},
		"opposite ID and kinds plus relationship ID and kind": {
			projection: projectionOf(query.EndID(), query.KindsOf(query.End()), query.RelationshipID(), query.KindsOf(query.Relationship())),
			columns:    []string{"select (s0.n1).id", "(s0.n1).kind_ids", "(s0.e0).id", "kind_name((s0.e0).kind_id)"},
		},
		"start relationship end triple": {
			projection: projectionOf(query.Start(), query.Relationship(), query.End()),
			columns:    []string{"select s0.n0 as s", "s0.e0 as r", "s0.n1 as e"},
		},
		"relationship ID only": {
			projection: projectionOf(query.RelationshipID()),
			columns:    []string{"select (s0.e0).id"},
		},
		"full relationship": {
			projection: projectionOf(query.Relationship()),
			columns:    []string{"select s0.e0 as r"},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			formatted, _ := translateLegacyQuery(t, testCase.projection.criteria)
			cursor := 0
			for _, column := range testCase.columns {
				next := strings.Index(formatted[cursor:], column)
				require.NotEqualf(t, -1, next, "missing projection column %q in %s", column, formatted)
				cursor += next + len(column)
			}
		})
	}
}

// graphProjection keeps the table-driven projection cases strongly typed
// without obscuring that they are legacy query criteria.
type graphProjection struct {
	// criteria is the legacy returning criterion represented by this projection.
	criteria graph.Criteria
}

// projectionOf wraps returning criteria in the strongly typed projection used by table-driven cases.
func projectionOf(criteria ...graph.Criteria) *graphProjection {
	return &graphProjection{criteria: query.Returning(criteria...)}
}

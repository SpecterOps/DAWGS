package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func translatedUpdateProjection(t *testing.T, queryText string, mode OptimizerMode) (pgsql.Update, pgsql.Select) {
	t.Helper()
	query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
	require.NoError(t, err)
	translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
	require.NoError(t, err)
	var updates []pgsql.Update
	var projections []pgsql.Select
	writes := map[pgsql.Identifier]struct{}{}
	var collect func(pgsql.Query)
	collect = func(query pgsql.Query) {
		if query.CommonTableExpressions != nil {
			for _, expression := range query.CommonTableExpressions.Expressions {
				collect(expression.Query)
				if _, ok := expression.Query.Body.(pgsql.Update); ok {
					writes[expression.Alias.Name] = struct{}{}
				}
			}
		}
		if update, ok := query.Body.(pgsql.Update); ok {
			updates = append(updates, update)
		}
		if projection, ok := query.Body.(pgsql.Select); ok {
			for _, from := range projection.From {
				for _, join := range from.Joins {
					identifier, ok := join.Table.(pgsql.Identifier)
					if _, write := writes[identifier]; ok && write {
						projections = append(projections, projection)
					}
				}
			}
		}
	}
	if len(translation.Stages) == 0 {
		collect(translation.Statement.(pgsql.Query))
	} else {
		for _, stage := range translation.Stages {
			collect(stage.Query)
		}
	}
	require.Len(t, updates, 1)
	require.Len(t, projections, 1)
	_, err = TranslatedStatements(translation, "alias_test_")
	require.NoError(t, err)
	return updates[0], projections[0]
}

func TestUpdateProjectionRefreshesCarriedEntityAliases(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, testCase := range []struct {
			name   string
			query  string
			family pgsql.DataType
		}{
			{"node_alias", `MATCH (n) WITH n, n AS alias SET n.x = 1 RETURN alias`, pgsql.NodeComposite},
			{"independent_node_binding", `MATCH (n) MATCH (m) WHERE id(m) = id(n) SET n.x = 1 RETURN m`, pgsql.NodeComposite},
			{"edge_alias", `MATCH ()-[r:MemberOf]->() WITH r, r AS alias SET r.x = 1 RETURN alias`, pgsql.EdgeComposite},
			{"independent_edge_binding", `MATCH ()-[r:MemberOf]->() MATCH ()-[other:MemberOf]->() WHERE id(other) = id(r) SET r.x = 1 RETURN other`, pgsql.EdgeComposite},
			{"property_remove", `MATCH (n) WITH n, n AS alias REMOVE n.x RETURN alias`, pgsql.NodeComposite},
			{"kind_set", `MATCH (n) WITH n, n AS alias SET n:Group RETURN alias`, pgsql.NodeComposite},
			{"kind_remove", `MATCH (n:User) WITH n, n AS alias REMOVE n:User RETURN alias`, pgsql.NodeComposite},
			{"kind_set_and_remove", `MATCH (n:User) WITH n, n AS alias SET n:Group REMOVE n:User RETURN alias`, pgsql.NodeComposite},
			{"property_set_and_remove", `MATCH (n) WITH n, n AS alias SET n.x = 1 REMOVE n.y RETURN alias`, pgsql.NodeComposite},
			{"created_node_alias", `CREATE (n:User {x: 0}) WITH n, n AS alias SET n.x = 1 WITH alias RETURN alias.x`, pgsql.NodeComposite},
		} {
			t.Run(string(mode)+"/"+testCase.name, func(t *testing.T) {
				update, carried := translatedUpdateProjection(t, testCase.query, mode)
				var refreshed int
				for _, item := range carried.Projection {
					projection, ok := item.(*pgsql.AliasedExpression)
					require.True(t, ok)
					conditional, ok := projection.Expression.(pgsql.Case)
					if !ok {
						continue
					}
					refreshed++
					require.Len(t, conditional.Conditions, 1)
					require.Len(t, conditional.Then, 1)
					condition, ok := conditional.Conditions[0].(*pgsql.BinaryExpression)
					require.True(t, ok)
					require.Equal(t, pgsql.OperatorEquals, condition.Operator)
					require.Equal(t, pgsql.RowColumnReference{Identifier: conditional.Else, Column: pgsql.ColumnID}, condition.LOperand)
					fresh := pgsql.CompoundIdentifier{carried.From[0].Joins[0].Table.(pgsql.Identifier), update.Table.Binding.Value}
					require.Equal(t, pgsql.RowColumnReference{Identifier: fresh, Column: pgsql.ColumnID}, condition.ROperand)
					require.Equal(t, fresh, conditional.Then[0])
					returned := update.Returning[0].(*pgsql.AliasedExpression).Expression.(pgsql.CompositeValue)
					require.Equal(t, testCase.family, returned.DataType)
				}
				require.GreaterOrEqual(t, refreshed, 2, "the target and its carried entity aliases must observe the updated row")
			})
		}
	}
}

func TestUpdateProjectionPreservesCapturedScalarProperties(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		_, carried := translatedUpdateProjection(t, `MATCH (n) WITH n, n.x AS captured SET n.x = 1 RETURN captured`, mode)
		requireOnlyTargetRefreshed(t, carried.Projection)
	}
}

func TestUpdateProjectionPreservesIdentityValues(t *testing.T) {
	projection := &pgsql.AliasedExpression{
		Expression: pgsql.CompoundIdentifier{"input", "identity"},
		Alias:      pgsql.AsOptionalIdentifier("identity"),
	}
	identity := &BoundIdentifier{Identifier: "identity", DataType: pgsql.NodeComposite, IDOnly: true}
	updated := &BoundIdentifier{Identifier: "updated", DataType: pgsql.NodeComposite}
	actual := refreshUpdatedEntityProjection(projection, identity, updated, pgsql.CompoundIdentifier{"writes", "updated"})
	require.Same(t, projection, actual, "a scalar identity must never be replaced by an entity composite")
}

func TestUpdateRejectsUnmappedKindChanges(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, clause := range []string{"SET n:UnmappedUpdateKind", "REMOVE n:UnmappedUpdateKind"} {
			t.Run(string(mode)+"/"+clause, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (n) "+clause+" RETURN n")
				require.NoError(t, err)
				_, err = TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.ErrorContains(t, err, "failed to translate kinds")
			})
		}
	}
}

func TestUpdateProjectionPreservesCarriedRelationshipLists(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		_, carried := translatedUpdateProjection(t, `MATCH ()-[r:MemberOf]->() MATCH (a)-[rs:MemberOf*1..2]->(b) WITH r, rs SET r.x = 1 RETURN rs`, mode)
		requireOnlyTargetRefreshed(t, carried.Projection)
	}
}

func TestUpdateProjectionPreservesCapturedPathsAndNodeLists(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MATCH p = (n)-[:MemberOf]->(m) WITH n, p SET n.x = 1 RETURN p`,
			`MATCH (n) WITH n, collect(n) AS captured SET n.x = 1 RETURN captured`,
		} {
			_, carried := translatedUpdateProjection(t, query, mode)
			requireOnlyTargetRefreshed(t, carried.Projection)
		}
	}
}

func requireOnlyTargetRefreshed(t *testing.T, items pgsql.Projection) {
	t.Helper()
	refreshed := 0
	for _, item := range items {
		projection := item.(*pgsql.AliasedExpression)
		if _, ok := projection.Expression.(pgsql.Case); ok {
			refreshed++
		}
	}
	require.Equal(t, 1, refreshed, "only the target changes; captured scalars, lists and paths retain their values")
}

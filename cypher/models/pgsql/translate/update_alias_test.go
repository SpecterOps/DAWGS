package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func translatedUpdates(t *testing.T, queryText string, mode OptimizerMode) []pgsql.Update {
	t.Helper()
	query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
	require.NoError(t, err)
	translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
	require.NoError(t, err)
	var updates []pgsql.Update
	var collect func(pgsql.Query)
	collect = func(query pgsql.Query) {
		if query.CommonTableExpressions != nil {
			for _, expression := range query.CommonTableExpressions.Expressions {
				collect(expression.Query)
			}
		}
		if update, ok := query.Body.(pgsql.Update); ok {
			updates = append(updates, update)
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
	_, err = TranslatedStatements(translation, "alias_test_")
	require.NoError(t, err)
	return updates
}

func TestUpdateReturningRefreshesCarriedEntityAliases(t *testing.T) {
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
			{"created_node_alias", `CREATE (n:User {x: 0}) WITH n, n AS alias SET n.x = 1 WITH alias RETURN alias.x`, pgsql.NodeComposite},
		} {
			t.Run(string(mode)+"/"+testCase.name, func(t *testing.T) {
				update := translatedUpdates(t, testCase.query, mode)[0]
				var refreshed int
				for _, item := range update.Returning {
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
					require.Equal(t, pgsql.CompoundIdentifier{update.Table.Binding.Value, pgsql.ColumnID}, condition.ROperand)
					fresh, ok := conditional.Then[0].(pgsql.CompositeValue)
					require.True(t, ok)
					require.Equal(t, testCase.family, fresh.DataType)
					columns := pgsql.NodeTableColumns
					if testCase.family == pgsql.EdgeComposite {
						columns = pgsql.EdgeTableColumns
					}
					require.Len(t, fresh.Values, len(columns))
					for index, column := range columns {
						require.Equal(t, pgsql.CompoundIdentifier{update.Table.Binding.Value, column}, fresh.Values[index])
					}
				}
				require.Equal(t, 1, refreshed, "carried entity aliases must observe the updated row")
			})
		}
	}
}

func TestUpdateReturningPreservesCapturedScalarProperties(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		update := translatedUpdates(t, `MATCH (n) WITH n, n.x AS captured SET n.x = 1 RETURN captured`, mode)[0]
		for _, item := range update.Returning {
			projection := item.(*pgsql.AliasedExpression)
			_, refreshed := projection.Expression.(pgsql.Case)
			require.False(t, refreshed, "a captured scalar remains the pre-update value")
		}
	}
}

func TestUpdateReturningPreservesCarriedRelationshipLists(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		update := translatedUpdates(t, `MATCH ()-[r:MemberOf]->() MATCH (a)-[rs:MemberOf*1..2]->(b) WITH r, rs SET r.x = 1 RETURN rs`, mode)[0]
		for _, item := range update.Returning {
			projection := item.(*pgsql.AliasedExpression)
			_, refreshed := projection.Expression.(pgsql.Case)
			require.False(t, refreshed, "a relationship list must retain its array representation")
		}
	}
}

func TestUpdateReturningPreservesCapturedPathsAndNodeLists(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MATCH p = (n)-[:MemberOf]->(m) WITH n, p SET n.x = 1 RETURN p`,
			`MATCH (n) WITH n, collect(n) AS captured SET n.x = 1 RETURN captured`,
		} {
			update := translatedUpdates(t, query, mode)[0]
			for _, item := range update.Returning {
				projection := item.(*pgsql.AliasedExpression)
				_, refreshed := projection.Expression.(pgsql.Case)
				require.False(t, refreshed, "captured paths and lists retain their values")
			}
		}
	}
}

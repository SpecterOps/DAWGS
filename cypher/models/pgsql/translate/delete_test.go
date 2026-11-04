package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestDeleteConnectedNodeTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			query string
			guard bool
		}{
			{"MATCH (n) DELETE n", true},
			{"MATCH (n) DETACH DELETE n", false},
			{"MATCH ()-[r]->() DELETE r", false},
			{"MATCH (n)-[r]->(m) DELETE n, r", true},
			{"MATCH (n)-[r]->(m) DELETE n DETACH DELETE m", true},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, 42, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(result)
				require.NoError(t, err)
				if tc.guard {
					require.Contains(t, formatted.Statement, "cypher_assert_node_deletable(42,")
				} else {
					require.NotContains(t, formatted.Statement, "cypher_assert_node_deletable")
				}
			})
		}
	}
}

func TestDeleteContinuationTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			"MATCH (n) DELETE n RETURN count(*) AS c",
			"MATCH (n) DETACH DELETE n RETURN 1 AS x",
			"MATCH ()-[r]->() DELETE r RETURN count(*) AS c",
			"MATCH (n)-[r]->(m) DELETE r, n RETURN count(*) AS c",
			"MATCH (n) WITH n, n.name AS name DETACH DELETE n RETURN name",
			"OPTIONAL MATCH (n) DELETE n RETURN count(*) AS c",
		} {
			t.Run(string(mode)+"/"+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				require.Empty(t, result.Stages)
				statement := result.Statement.(pgsql.Query)
				deletions := map[pgsql.Identifier]bool{}
				for _, cte := range statement.CommonTableExpressions.Expressions {
					if _, ok := cte.Query.Body.(pgsql.Delete); ok {
						deletions[cte.Alias.Name] = true
					}
				}
				require.NotEmpty(t, deletions)
				for _, from := range statement.Body.(pgsql.Select).From {
					if name, ok := from.Source.(pgsql.Identifier); ok {
						require.False(t, deletions[name], "DELETE does not export a row relation")
					}
				}
				_, err = Translated(result)
				require.NoError(t, err)
			})
		}
	}
}

func TestDeleteAfterWritesUsesStatementBoundary(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			"MATCH (n) SET n.x = 1 DELETE n RETURN count(*) AS c",
			"MATCH ()-[r]->() SET r.x = 1 DELETE r",
			"CREATE (n:NodeKind1) DELETE n RETURN count(*) AS c",
			"MATCH (n) SET n.x = 1 DELETE n WITH 1 AS x RETURN x",
		} {
			t.Run(string(mode)+"/"+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				require.Nil(t, result.Statement)
				require.GreaterOrEqual(t, len(result.Stages), 2)
				statements, err := TranslatedStatements(result, "delete_test_")
				require.NoError(t, err)
				require.Contains(t, statements[0].Statement, "create temporary table")
				require.NotContains(t, statements[0].Statement, "delete from")
				require.Contains(t, statements[1].Statement, "delete from")
			})
		}
	}
}

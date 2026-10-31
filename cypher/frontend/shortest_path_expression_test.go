package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestShortestPathExpressionsRejected(t *testing.T) {
	for _, function := range []string{"shortestPath", "allShortestPaths"} {
		for _, pattern := range []string{"(a)-[:R*]->(b)", "(a)<-[:R*1..3]-(b)", "(a)-[:R*0..2]-(b)"} {
			for _, template := range []string{
				"MATCH (a), (b) RETURN %s AS p",
				"MATCH (a), (b) RETURN length(%s) AS size",
				"MATCH (a), (b) WITH %s AS p RETURN p",
				"MATCH (a), (b) WHERE %s IS NOT NULL RETURN a",
				"MATCH (a), (b) SET a.value = length(%s) RETURN a",
				"MATCH (a), (b) SET a.value = length(%s)",
				"MATCH (a), (b) CREATE (:Created {value: length(%s)})",
				"MATCH (a), (b) WHERE %s IS NOT NULL DELETE a",
			} {
				source := fmt.Sprintf(template, function+"("+pattern+")")
				t.Run(source, func(t *testing.T) {
					for _, ctx := range []*frontend.Context{frontend.NewContext(), frontend.DefaultCypherContext()} {
						_, err := frontend.ParseCypher(ctx, source)
						require.ErrorContains(t, err, "shortest-path expressions are not supported")
					}
					formatted, err := test.CypherToCypher(frontend.NewContext(), source)
					require.ErrorContains(t, err, "shortest-path expressions are not supported")
					require.Empty(t, formatted)
				})
			}
		}
	}
}

func TestShortestPathExpressionErrorLocation(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (a), (b)\nRETURN shortestPath((a)-[:R*]->(b))")
	require.EqualError(t, err, "line 2:7 shortest-path expressions are not supported")
}

func TestShortestPathMatchPatternsPreserved(t *testing.T) {
	for _, function := range []string{"shortestPath", "allShortestPaths"} {
		for _, template := range []string{"MATCH p = %s((a)-[:R*1..3]->(b)) RETURN p", "MATCH %s((a)-[:R*1..3]->(b)) RETURN b", "OPTIONAL MATCH p = %s((a)-[:R*1..3]->(b)) RETURN p"} {
			source := fmt.Sprintf(template, function)
			t.Run(source, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), source)
				require.NoError(t, err)
				require.NotNil(t, query)
				formatted, err := test.CypherToCypher(frontend.NewContext(), source)
				require.NoError(t, err)
				require.Contains(t, formatted, function+"(")
			})
		}
	}
}

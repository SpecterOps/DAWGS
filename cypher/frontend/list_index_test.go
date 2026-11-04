package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestListIndexingRejected(t *testing.T) {
	for _, expression := range []string{
		"xs[0]", "xs[-1]", "xs[99]", "xs[null]", "xs[$idx]",
		"xs[1 + 1]", "xs[xs[0]]", "xs[0][1]", "xs[0].name",
		"[1, 2][0]", "[][0]", "null[0]", "$values[0]", "n.values[0]",
		"xs['..']", "xs[size([1, 2][0..1])]",
	} {
		for _, template := range []string{
			"WITH [1, 2] AS xs MATCH (n) RETURN %s AS value",
			"WITH [1, 2] AS xs MATCH (n) WHERE %s = 0 RETURN n",
			"WITH [1, 2] AS xs MATCH (n) SET n.value = %s RETURN n",
			"WITH [1, 2] AS xs MATCH (n) WHERE %s = 0 DELETE n",
			"WITH [1, 2] AS xs CREATE (n {value: %s})",
		} {
			source := fmt.Sprintf(template, expression)
			t.Run(source, func(t *testing.T) {
				for _, ctx := range []*frontend.Context{frontend.NewContext(), frontend.DefaultCypherContext()} {
					_, err := frontend.ParseCypher(ctx, source)
					require.ErrorContains(t, err, "list indexing is not supported")
				}
				formatted, err := test.CypherToCypher(frontend.NewContext(), source)
				require.ErrorContains(t, err, "list indexing is not supported")
				require.Empty(t, formatted)
			})
		}
	}
}

func TestListIndexingErrorLocation(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.NewContext(), "WITH [1,2] AS xs\nRETURN xs[0] AS x")
	require.EqualError(t, err, "line 2:9 list indexing is not supported")
}

func TestListIndexingPreservesSupportedListSyntax(t *testing.T) {
	for _, source := range []string{
		"return [1, 2] as xs",
		"with [1, 2] as xs return xs",
		"unwind [1, 2] as x return x",
		"match (n) where n.value in [1, 2] return n",
		"match (n) set n.values = [1, 2] return n",
		"match (n) return n.values",
	} {
		t.Run(source, func(t *testing.T) {
			formatted, err := test.CypherToCypher(frontend.NewContext(), source)
			require.NoError(t, err)
			require.Equal(t, source, formatted)
		})
	}
}

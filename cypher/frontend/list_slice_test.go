package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestListSlicingRejected(t *testing.T) {
	for _, expression := range []string{
		"xs[0..2]", "xs[..2]", "xs[1..]", "xs[..]", "xs[-2..-1]",
		"xs[0..99]", "xs[null..2]", "xs[$lo..$hi]", "xs[1 + 1..size(xs)]",
		"xs[0..2][..1]", "xs[0..2][0]", "xs[0..2].name",
		"[1, 2, 3][0..2]", "[][..]", "null[..]", "$values[..2]", "n.values[1..]",
		"xs[size(xs[..1])..2]",
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
					require.ErrorContains(t, err, "list slicing is not supported")
				}
				formatted, err := test.CypherToCypher(frontend.NewContext(), source)
				require.ErrorContains(t, err, "list slicing is not supported")
				require.Empty(t, formatted)
			})
		}
	}
}

func TestListSlicingErrorLocation(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.NewContext(), "WITH [1,2] AS xs\nRETURN xs[0..2] AS x")
	require.EqualError(t, err, "line 2:9 list slicing is not supported")
}

func TestListSlicingPreservesSupportedListSyntax(t *testing.T) {
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

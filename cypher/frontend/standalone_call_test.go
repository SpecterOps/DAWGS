package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestStandaloneCallRejected(t *testing.T) {
	for _, invocation := range []string{"db.labels()", "db.labels", "db.labels() YIELD *", "db.labels() YIELD label", "db.labels() YIELD label AS name WHERE name = 'x'", "example.write($value)", "example.write({value: 1})"} {
		for _, template := range []string{"CALL %s", "call %s;", "/* before */\n  CALL %s"} {
			source := fmt.Sprintf(template, invocation)
			t.Run(source, func(t *testing.T) {
				for _, ctx := range []*frontend.Context{frontend.NewContext(), frontend.DefaultCypherContext()} {
					query, err := frontend.ParseCypher(ctx, source)
					require.ErrorContains(t, err, "standalone CALL is not supported")
					require.Nil(t, query)
				}
				formatted, err := test.CypherToCypher(frontend.NewContext(), source)
				require.ErrorContains(t, err, "standalone CALL is not supported")
				require.Empty(t, formatted)
			})
		}
	}
}

func TestStandaloneCallErrorLocation(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.NewContext(), "// comment\n  CALL db.labels()")
	require.EqualError(t, err, "line 2:2 standalone CALL is not supported")
}

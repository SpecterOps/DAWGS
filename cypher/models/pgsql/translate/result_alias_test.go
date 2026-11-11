package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// TestResultAliasTranslation checks that translated projections preserve caller-visible result aliases.
func TestResultAliasTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				// token is the Cypher spelling, including any backtick quoting.
				token string
				// sql is the SQL statement or identifier spelling exercised by the case.
				sql string
			}{
				{
					token: "objectId",
					sql:   `"objectId"`,
				},
				{
					token: "`object id`",
					sql:   `"object id"`,
				},
				{
					token: "`object``id`",
					sql:   "\"object`id\"",
				},
				{
					token: "`object\"id`",
					sql:   `"object""id"`,
				},
				{
					token: "`select`",
					sql:   `"select"`,
				},
			} {
				for _, query := range []string{
					"RETURN 7 AS " + tc.token,
					"MATCH (n:NodeKind1) RETURN n.name AS " + tc.token + " ORDER BY " + tc.token,
					"MATCH (n:NodeKind1) SET n.name = 'changed' RETURN n.name AS " + tc.token,
					"MATCH (n:NodeKind1) WITH n.name AS " + tc.token + " CREATE (c:NodeKind1 {name: " + tc.token + "}) RETURN c.name AS " + tc.token,
				} {
					t.Run(query, func(t *testing.T) {
						parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
						require.NoError(t, err)
						result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{
							OptimizerMode: mode,
						})
						require.NoError(t, err)
						formatted, err := Translated(result)
						require.NoError(t, err)
						require.Contains(t, formatted.Statement, " as "+tc.sql)
					})
				}
			}
		})
	}
}

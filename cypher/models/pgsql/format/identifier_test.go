package format_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestIdentifierSpelling(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"objectId", `"objectId"`},
		{"object_id", "object_id"},
		{"`a` + `b`", "\"`a` + `b`\""},
		{"`object id`", `"object id"`},
		{"`object``id`", "\"object`id\""},
		{"`object\"id`", `"object""id"`},
		{"`select`", `"select"`},
		{"`*`", `"*"`},
		{"*", "*"},
		{"`étiquette`", `"étiquette"`},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := format.Expression(pgsql.Identifier(tc.input), format.NewOutputBuilder())
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Statement)
			aliased, err := format.Expression(pgsql.AliasedExpression{Expression: pgsql.NewLiteral(1, pgsql.Int), Alias: pgsql.AsOptionalIdentifier(pgsql.Identifier(tc.input))}, format.NewOutputBuilder())
			require.NoError(t, err)
			require.Equal(t, "1 as "+tc.want, aliased.Statement)
		})
	}
}

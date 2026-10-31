package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

func TestEmptyListProjectionHasConcreteType(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, queryText := range []string{
			`WITH [] AS xs RETURN xs`,
			`WITH [] AS xs UNWIND xs AS x RETURN x`,
			`WITH ([]) AS xs UNWIND xs AS x RETURN x`,
			`WITH [] AS xs WITH xs AS ys UNWIND ys AS y RETURN y`,
			`WITH [] AS xs UNWIND xs AS x CREATE (n {x: x}) RETURN n`,
		} {
			t.Run(string(mode)+"/"+queryText, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), query, pgutil.NewInMemoryKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				require.NotContains(t, formatted.Statement, "array []::anyarray")
				require.Contains(t, formatted.Statement, "array []::int8[]")
			})
		}
	}
}

func TestProjectionPreservesConcreteListTypes(t *testing.T) {
	for _, tc := range []struct {
		query, cast string
	}{
		{`WITH [1, 2] AS xs UNWIND xs AS x RETURN x`, "::int8[]"},
		{`WITH [1.5, 2.5] AS xs UNWIND xs AS x RETURN x`, "::float8[]"},
		{`WITH ['a', 'b'] AS xs UNWIND xs AS x RETURN x`, "::text[]"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)
			translation, err := Translate(context.Background(), query, pgutil.NewInMemoryKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)
			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Contains(t, formatted.Statement, tc.cast)
		})
	}
	for _, dataType := range []pgsql.DataType{pgsql.Int8Array, pgsql.TextArray, pgsql.Float8Array} {
		literal := pgsql.ArrayLiteral{CastType: dataType}
		require.Equal(t, literal, concreteEmptyArray(literal), "an already typed empty array retains its type")
	}
}

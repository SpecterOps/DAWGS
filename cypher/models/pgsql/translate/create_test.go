package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestConsecutiveCreateClausesAreBuiltOnce(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("NodeKind1"))
	kindMapper.Put(graph.StringKind("NodeKind2"))

	query, err := frontend.ParseCypher(frontend.NewContext(), `CREATE (a:NodeKind1 {name: 'a'}) CREATE (b:NodeKind2 {name: 'b'}) RETURN a, b`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)

	require.Equal(t, 2, strings.Count(formatted.Statement, "insert into node"))
	require.Equal(t, 2, strings.Count(formatted.Statement, "nextval(pg_get_serial_sequence(@__strlit0::text, @__strlit1::text))"))
}

func TestLeadingMultipartClauseSources(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, queryText := range []string{
			`UNWIND [1, 2] AS x WITH x RETURN x`,
			`CREATE (n) WITH n RETURN n`,
		} {
			t.Run(string(mode)+"/"+queryText, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), query, pgutil.NewInMemoryKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				if strings.HasPrefix(queryText, "UNWIND") {
					require.Equal(t, 1, strings.Count(formatted.Statement, "from s0"), formatted.Statement)
				} else {
					require.NotContains(t, formatted.Statement, "as (with", formatted.Statement)
				}
			})
		}
	}
}

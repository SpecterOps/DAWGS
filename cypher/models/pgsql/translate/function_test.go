package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

func TestPathComponentFunctionsResolvePathAliases(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = (a)-[r]->(b) WITH p AS q RETURN nodes(q), relationships(q)`)
	require.NoError(t, err)

	translation, err := Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted, ".nodes")
	require.Contains(t, formatted, ".edges")
}

func TestPrepareCollectExpressionMissingBindingErrorNamesArgument(t *testing.T) {
	t.Parallel()

	_, _, err := prepareCollectExpression(NewScope(), pgsql.Identifier("missing"), cypher.CollectFunction)

	require.EqualError(t, err, "binding not found for collect function argument missing")
}

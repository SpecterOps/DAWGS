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

	require.Equal(t, 2, strings.Count(formatted, "insert into node"))
	require.Equal(t, 2, strings.Count(formatted, "nextval(pg_get_serial_sequence('node', 'id'))"))
}

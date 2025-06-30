package visualization

import (
	"bytes"
	"context"
	"testing"

	"github.com/specterops/dawgs/database/pg/pgutil"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func TestGraphToPUMLDigraph(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), "match (s), (e) where s.name = s.other + 1 / s.last return s")
	require.Nil(t, err)

	translation, err := translate.Translate(context.Background(), regularQuery, kindMapper, nil)
	require.Nil(t, err)

	graph, err := SQLToDigraph(translation.Statement)
	require.Nil(t, err)

	require.Nil(t, err)
	require.Nil(t, GraphToPUMLDigraph(graph, &bytes.Buffer{}))
}

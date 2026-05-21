package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestTranslatorRejectsNeo4jAuthoritativeInvalidShapes(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()
	kindMapper.Put(graph.StringKind("NodeKind1"))

	testCases := []string{
		"WITH [1, 2, 3] AS x UNWIND x AS x RETURN x",
		"match (n)-[r*..]->(e:NodeKind1) where n.name = 'n1' and r.prop = 'a' return e",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), testCase)
			require.NoError(t, err)

			_, err = Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
			require.Error(t, err)
		})
	}
}

func TestTranslatorRejectsUnsupportedPropertyLookupSourcesDirectly(t *testing.T) {
	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(frontend.NewContext(), `RETURN [1, 2, 3].prop`)
	require.NoError(t, err)

	_, err = Translate(context.Background(), query, kindMapper, nil, DefaultGraphID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported property lookup prop on expression type int8[]")
}

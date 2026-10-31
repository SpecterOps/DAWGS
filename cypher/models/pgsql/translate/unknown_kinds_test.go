package translate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type failingMatchKindMapper struct {
	*pgutil.InMemoryKindMapper
	err error
}

func (s failingMatchKindMapper) MapKinds(context.Context, graph.Kinds) ([]int16, error) {
	return nil, s.err
}

func TestUnknownMatchKinds(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			"MATCH (n:Missing) RETURN count(n)",
			"MATCH (n:Known:Missing) RETURN n",
			"MATCH ()-[r:Missing|Known]->() RETURN r",
			"MATCH (n:Known) WHERE NOT n:Missing RETURN n",
			"MATCH (n:Known)-[:Missing*1..2]->(t) RETURN count(t)",
			"MATCH (n:Known) WHERE n:Missing RETURN count(n)",
			"MATCH ()-[r:Known]->() WHERE type(r) = 'Missing' RETURN r",
		} {
			t.Run(string(mode)+"/"+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				mapper := pgutil.NewInMemoryKindMapper()
				mapper.Put(graph.StringKind("Known"))
				result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				require.True(t, result.HasMissingKinds)
				require.Len(t, mapper.KindToID, 1, "matching must not register unknown kinds")
				_, err = Translated(result)
				require.NoError(t, err)
				for _, failure := range []error{context.Canceled, context.DeadlineExceeded, errors.New("database unavailable")} {
					_, err = TranslateWithOptions(context.Background(), parsed, failingMatchKindMapper{mapper, fmt.Errorf("lookup: %w", failure)}, nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.ErrorIs(t, err, failure)
				}
			})
		}
	}
}

func TestMatchKindsKeepsStrictMapperContract(t *testing.T) {
	mapper := pgutil.NewInMemoryKindMapper()
	known := graph.StringKind("Known")
	missing := graph.StringKind("Missing")
	id := mapper.Put(known)
	ids, err := mapper.MapKinds(context.Background(), graph.Kinds{known, missing})
	require.Nil(t, ids)
	var miss *pgsql.MissingKindsError
	require.ErrorAs(t, err, &miss)
	require.Equal(t, graph.Kinds{missing}, miss.Kinds)
	require.Equal(t, []int16{id}, miss.KindIDs)
	wrapped := newContextAwareKindMapper(context.Background(), mapper, nil)
	ids, hasMissing, err := wrapped.MapMatchKinds(graph.Kinds{missing, known})
	require.NoError(t, err)
	require.True(t, hasMissing)
	require.Equal(t, []int16{id}, ids)
	_, err = wrapped.MapKinds(graph.Kinds{missing})
	require.ErrorAs(t, err, &miss)
	ids, err = wrapped.AssertKinds(graph.Kinds{missing})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Len(t, mapper.KindToID, 2, "CREATE must still register kinds")
}

func TestCreateRegistersUnknownKinds(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		mapper := pgutil.NewInMemoryKindMapper()
		parsed, err := frontend.ParseCypher(frontend.NewContext(), "CREATE (a:NewNode)-[:NewEdge]->(b:NewNode) RETURN a")
		require.NoError(t, err)
		result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
		require.NoError(t, err)
		require.False(t, result.HasMissingKinds)
		require.Len(t, mapper.KindToID, 2)
	}
}

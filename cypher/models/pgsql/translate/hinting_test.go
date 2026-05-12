package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type emptyKindMapper struct{}

func (s emptyKindMapper) MapKinds(context.Context, graph.Kinds) ([]int16, error) {
	return nil, nil
}

func (s emptyKindMapper) AssertKinds(context.Context, graph.Kinds) ([]int16, error) {
	return nil, nil
}

func TestLiteralKindIDEmptyMappedKindsIsNotConverted(t *testing.T) {
	t.Parallel()

	literal, converted, err := literalKindID(
		newContextAwareKindMapper(context.Background(), emptyKindMapper{}),
		pgsql.NewLiteral("MissingKind", pgsql.Text),
	)

	require.NoError(t, err)
	require.False(t, converted)
	require.Equal(t, pgsql.Literal{}, literal)
}

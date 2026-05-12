package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestMapKinds(t *testing.T) {
	ctx := context.Background()

	t.Run("int16 slice maps through kind mapper", func(t *testing.T) {
		mapper := pgutil.NewInMemoryKindMapper()
		userKind := graph.StringKind("User")
		groupKind := graph.StringKind("Group")

		userKindID := mapper.Put(userKind)
		groupKindID := mapper.Put(groupKind)

		mappedKinds, ok := mapKinds(ctx, mapper, []int16{userKindID, groupKindID})

		require.True(t, ok)
		require.Equal(t, graph.Kinds{userKind, groupKind}, mappedKinds)
	})

	t.Run("string slice maps directly to kinds", func(t *testing.T) {
		mappedKinds, ok := mapKinds(ctx, nil, []string{"User", "Group"})

		require.True(t, ok)
		require.Equal(t, graph.StringsToKinds([]string{"User", "Group"}), mappedKinds)
	})

	t.Run("any string slice maps directly to kinds", func(t *testing.T) {
		mappedKinds, ok := mapKinds(ctx, nil, []any{"User", "Group"})

		require.True(t, ok)
		require.Equal(t, graph.StringsToKinds([]string{"User", "Group"}), mappedKinds)
	})

	t.Run("any integer slice maps through kind mapper", func(t *testing.T) {
		mapper := pgutil.NewInMemoryKindMapper()
		userKind := graph.StringKind("User")
		groupKind := graph.StringKind("Group")

		userKindID := mapper.Put(userKind)
		groupKindID := mapper.Put(groupKind)

		mappedKinds, ok := mapKinds(ctx, mapper, []any{userKindID, groupKindID})

		require.True(t, ok)
		require.Equal(t, graph.Kinds{userKind, groupKind}, mappedKinds)
	})

	t.Run("mixed any slice is not mapped", func(t *testing.T) {
		mapper := pgutil.NewInMemoryKindMapper()
		userKindID := mapper.Put(graph.StringKind("User"))

		mappedKinds, ok := mapKinds(ctx, mapper, []any{"User", userKindID})

		require.False(t, ok)
		require.Nil(t, mappedKinds)
	})

	t.Run("unsupported any slice is not mapped", func(t *testing.T) {
		mappedKinds, ok := mapKinds(ctx, nil, []any{true})

		require.False(t, ok)
		require.Nil(t, mappedKinds)
	})
}

func TestValueMapperMapsStringArraysByTargetType(t *testing.T) {
	valueMapper := NewValueMapper(context.Background(), nil)

	var kindTarget graph.Kinds
	require.True(t, valueMapper.Map([]any{"User", "Group"}, &kindTarget))
	require.Equal(t, graph.StringsToKinds([]string{"User", "Group"}), kindTarget)

	var stringTarget []string
	require.True(t, valueMapper.Map([]any{"Alice", "Bob"}, &stringTarget))
	require.Equal(t, []string{"Alice", "Bob"}, stringTarget)
}

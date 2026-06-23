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

func TestValueMapperMapsCompositeArrays(t *testing.T) {
	ctx := context.Background()
	mapper := pgutil.NewInMemoryKindMapper()
	userKindID := mapper.Put(graph.StringKind("User"))
	memberOfKindID := mapper.Put(graph.StringKind("MemberOf"))
	valueMapper := NewValueMapper(ctx, mapper)

	t.Run("node array preserves order", func(t *testing.T) {
		rawNodes := []any{
			map[string]any{
				"id":         int64(1),
				"kind_ids":   []any{userKindID},
				"properties": map[string]any{"name": "Alice"},
			},
			map[string]any{
				"id":         int64(2),
				"kind_ids":   []any{userKindID},
				"properties": map[string]any{"name": "Bob"},
			},
		}

		var nodes []*graph.Node
		require.True(t, valueMapper.Map(rawNodes, &nodes))
		require.Len(t, nodes, 2)
		require.Equal(t, graph.ID(1), nodes[0].ID)
		require.Equal(t, graph.ID(2), nodes[1].ID)
		require.Equal(t, graph.StringKind("User"), nodes[0].Kinds[0])
		require.Equal(t, "Alice", nodes[0].Properties.Get("name").Any())
	})

	t.Run("relationship array preserves order", func(t *testing.T) {
		rawRelationships := []any{
			map[string]any{
				"id":         int64(10),
				"start_id":   int64(1),
				"end_id":     int64(2),
				"kind_id":    memberOfKindID,
				"properties": map[string]any{"ordinal": int64(1)},
			},
			map[string]any{
				"id":         int64(11),
				"start_id":   int64(2),
				"end_id":     int64(3),
				"kind_id":    memberOfKindID,
				"properties": map[string]any{"ordinal": int64(2)},
			},
		}

		var relationships []*graph.Relationship
		require.True(t, valueMapper.Map(rawRelationships, &relationships))
		require.Len(t, relationships, 2)
		require.Equal(t, graph.ID(10), relationships[0].ID)
		require.Equal(t, graph.ID(11), relationships[1].ID)
		require.Equal(t, graph.StringKind("MemberOf"), relationships[0].Kind)
	})
}

func TestAsKindID(t *testing.T) {
	testsCases := []struct {
		value           any
		expectedValue   int16
		expectedBoolean bool
	}{
		{
			value:           float64(1),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           int(32767),
			expectedValue:   32767,
			expectedBoolean: true,
		},
		{
			value:           int(32768),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           int8(1),
			expectedValue:   1,
			expectedBoolean: true,
		},
		{
			value:           int16(1),
			expectedValue:   1,
			expectedBoolean: true,
		},
		{
			value:           int32(-32769),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           int32(32768),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           int32(32767),
			expectedValue:   32767,
			expectedBoolean: true,
		},
		{
			value:           int64(-32769),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           int64(32768),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           int64(32767),
			expectedValue:   32767,
			expectedBoolean: true,
		},
		{
			value:           uint32(32768),
			expectedValue:   0,
			expectedBoolean: false,
		},
		{
			value:           uint32(32767),
			expectedValue:   32767,
			expectedBoolean: true,
		},
		{
			value:           uint64(32767),
			expectedValue:   32767,
			expectedBoolean: true,
		},
		{
			value:           uint64(32768),
			expectedValue:   0,
			expectedBoolean: false,
		},
	}

	for _, testCase := range testsCases {
		actualValue, actualBoolean := asKindID(testCase.value)
		require.Equal(t, int16(testCase.expectedValue), actualValue)
		require.Equal(t, testCase.expectedBoolean, actualBoolean)
	}
}

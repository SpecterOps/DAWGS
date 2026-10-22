package pg

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestBuilderIdentityIncludesTypedStructure(t *testing.T) {
	identity := func(expression any) builderCacheIdentity {
		builder := query.NewBuilder(nil)
		builder.Apply(query.Returning(expression))
		return compilePreparedBuilderQuery(t, builder).identity
	}
	for _, pair := range [][2]any{
		{query.Literal(1), query.Literal(1.0)},
		{query.Literal(0.0), query.Literal(math.Copysign(0, -1))},
		{query.KindIn(query.Node()), query.And()},
		{query.And(query.Literal(true), query.Or(query.Literal(false), query.Literal(true))), query.Or(query.And(query.Literal(true), query.Literal(false)), query.Literal(true))},
		{cypher.NewKindMatcher(query.Node(), graph.Kinds{graph.StringKind("A")}, false), cypher.NewKindMatcher(query.Node(), graph.Kinds{graph.StringKind("A")}, true)},
	} {
		first, second := identity(pair[0]), identity(pair[1])
		require.True(t, first.valid)
		require.True(t, second.valid)
		require.NotEqual(t, first.digest, second.digest)
	}
}

func TestBuilderIdentityExcludesRuntimeParameters(t *testing.T) {
	first := buildPreparedNodeLookup(t, graph.ID(1))
	second := buildPreparedNodeLookup(t, graph.ID(2))
	require.True(t, first.identity.valid)
	require.Equal(t, first.identity, second.identity)

	encoder := builderIdentityEncoder{}
	require.True(t, encoder.encode(reflect.ValueOf(cypher.NewParameter("private-name", "private-value")), 0))
	other := builderIdentityEncoder{}
	require.True(t, other.encode(reflect.ValueOf(cypher.NewParameter("another-name", "another-value")), 0))
	require.Equal(t, encoder.sum(), other.sum())
}

func TestBuilderIdentityFramesAndSortsMapEntries(t *testing.T) {
	encode := func(value any) [32]byte {
		encoder := builderIdentityEncoder{}
		require.True(t, encoder.encode(reflect.ValueOf(value), 0))
		return encoder.sum()
	}
	first := cypher.MapLiteral{}
	first["a"] = query.Literal(int64(1))
	first["b"] = query.Literal(true)
	second := cypher.MapLiteral{}
	second["b"] = query.Literal(true)
	second["a"] = query.Literal(int64(1))
	require.Equal(t, encode(first), encode(second))
	require.NotEqual(t, encode(map[string]string{"a": "bc"}), encode(map[string]string{"ab": "c"}))
	require.NotEqual(t, encode([]any{nil}), encode([]any{}))
	require.NotEqual(t, encode([]any(nil)), encode([]any{}))
	require.NotEqual(t, encode(int32(1)), encode(int64(1)))
	require.NotEqual(t, encode(uint32(1)), encode(uint64(1)))
	require.NotEqual(t, encode(float32(1)), encode(float64(1)))
}

func TestBuilderIdentityBypassesUnsupportedAndBoundedWork(t *testing.T) {
	cycle := cypher.NewParenthetical(nil)
	cycle.Expression = cycle
	for _, value := range []any{
		func() {},
		struct{ Hidden string }{"external state"},
		map[int]string{1: "unsupported key"},
		cycle,
		strings.Repeat("x", maxCachedCypherBytes),
		make([]bool, maxCachedCypherBytes+1),
	} {
		encoder := builderIdentityEncoder{}
		require.False(t, encoder.encode(reflect.ValueOf(value), 0), "%T", value)
		require.LessOrEqual(t, encoder.total, maxCachedCypherBytes)
	}
}

func BenchmarkPrepareRegularQueryIdentity(b *testing.B) {
	builder := query.NewBuilder(nil)
	builder.Apply(query.Where(query.Equals(query.NodeProperty("objectid"), graph.ID(1))), query.Returning(query.Node()))
	regularQuery, err := builder.Build(false)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := prepareRegularQuery(regularQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}

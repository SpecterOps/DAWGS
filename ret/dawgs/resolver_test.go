package dawgs_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/stretchr/testify/require"
)

func TestResolverKeepsNonCanonicalNumericIDsDistinct(t *testing.T) {
	resolver := dawgs.NewResolver(2)
	require.True(t, resolver.Put("1", graph.ID(10)))
	require.True(t, resolver.Put("01", graph.ID(11)))
	require.Equal(t, graph.ID(10), mustResolve(t, resolver, "1"))
	require.Equal(t, graph.ID(11), mustResolve(t, resolver, "01"))
}

func TestResolverRejectsDuplicateSourceIDsAndLeavesMissingIDsUnresolved(t *testing.T) {
	resolver := dawgs.NewResolver(3)
	require.True(t, resolver.Put("42", graph.ID(100)))
	require.True(t, resolver.Put("node-a", graph.ID(101)))
	require.False(t, resolver.Put("42", graph.ID(999)))
	require.False(t, resolver.Put("node-a", graph.ID(999)))

	_, found := resolver.Resolve("missing")
	require.False(t, found)
}

func mustResolve(t *testing.T, resolver *dawgs.Resolver, sourceID string) graph.ID {
	t.Helper()
	value, found := resolver.Resolve(sourceID)
	require.Truef(t, found, "resolve %q", sourceID)
	return value
}

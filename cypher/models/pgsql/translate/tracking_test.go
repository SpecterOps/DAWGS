package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestScope(t *testing.T) {
	var (
		scope = NewScope()
	)

	grandparent, err := scope.PushFrame()
	require.Nil(t, err)

	parent, err := scope.PushFrame()
	require.Nil(t, err)

	child, err := scope.PushFrame()
	require.Nil(t, err)

	require.Equal(t, 0, grandparent.id)
	require.Equal(t, 1, parent.id)
	require.Equal(t, 2, child.id)

	require.Nil(t, scope.UnwindToFrame(parent))
	require.Equal(t, parent.id, scope.CurrentFrame().id)
}

func TestScopeLookupDataTypeResolvesAliases(t *testing.T) {
	var (
		scope   = NewScope()
		binding = scope.Define(pgsql.Identifier("n0"), pgsql.NodeComposite)
	)

	scope.Alias(pgsql.Identifier("n"), binding)

	dataType, found := scope.LookupDataType(pgsql.Identifier("n"))

	require.True(t, found)
	require.Equal(t, pgsql.NodeComposite, dataType)
}

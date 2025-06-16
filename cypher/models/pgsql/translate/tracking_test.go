package translate

import (
	"testing"

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

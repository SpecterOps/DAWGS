package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKindHash(t *testing.T) {
	t.Run("hash returns same bytes for out-of-order kinds", func(t *testing.T) {
		kindsOne := Kinds{StringKind("A"), StringKind("B"), StringKind("C")}
		kindsTwo := Kinds{StringKind("C"), StringKind("B"), StringKind("A")}

		hash1, err := kindsOne.Hash()
		require.NoError(t, err)
		hash2, err := kindsTwo.Hash()
		require.NoError(t, err)

		require.Equal(t, hash1, hash2)
	})

	t.Run("hash returns different bytes when kinds have ambiguous boundaries e.g. [a, bc] vs [ab, c]", func(t *testing.T) {
		kindsOne := Kinds{StringKind("A"), StringKind("BC")}
		kindsTwo := Kinds{StringKind("AB"), StringKind("C")}

		hash1, err := kindsOne.Hash()
		require.NoError(t, err)
		hash2, err := kindsTwo.Hash()
		require.NoError(t, err)

		require.NotEqual(t, hash1, hash2)
	})

	t.Run("hash returns different hash for different kinds", func(t *testing.T) {
		kindsOne := Kinds{StringKind("A"), StringKind("B")}
		kindsTwo := Kinds{StringKind("C"), StringKind("B")}

		hash1, err := kindsOne.Hash()
		require.NoError(t, err)
		hash2, err := kindsTwo.Hash()
		require.NoError(t, err)

		require.NotEqual(t, hash1, hash2)
	})
}

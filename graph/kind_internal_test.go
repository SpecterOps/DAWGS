package graph

import (
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestKindHash(t *testing.T) {
	t.Run("hash returns same bytes for out-of-order kinds", func(t *testing.T) {
		kindsOne := Kinds{StringKind("A"), StringKind("B"), StringKind("C")}
		kindsTwo := Kinds{StringKind("C"), StringKind("B"), StringKind("A")}

		h := xxhash.New()

		h.Reset()
		require.NoError(t, kindsOne.HashInto(h))
		sum1 := h.Sum64()

		h.Reset()
		require.NoError(t, kindsTwo.HashInto(h))
		sum2 := h.Sum64()

		require.Equal(t, sum1, sum2)
	})

	t.Run("hash returns different bytes when kinds have ambiguous boundaries e.g. [a, bc] vs [ab, c]", func(t *testing.T) {
		kindsOne := Kinds{StringKind("A"), StringKind("BC")}
		kindsTwo := Kinds{StringKind("AB"), StringKind("C")}

		h := xxhash.New()

		h.Reset()
		require.NoError(t, kindsOne.HashInto(h))
		sum1 := h.Sum64()

		h.Reset()
		require.NoError(t, kindsTwo.HashInto(h))
		sum2 := h.Sum64()

		require.NotEqual(t, sum1, sum2)
	})

	t.Run("hash returns different hash for different kinds", func(t *testing.T) {
		kindsOne := Kinds{StringKind("A"), StringKind("B")}
		kindsTwo := Kinds{StringKind("C"), StringKind("B")}

		h := xxhash.New()

		h.Reset()
		require.NoError(t, kindsOne.HashInto(h))
		sum1 := h.Sum64()

		h.Reset()
		require.NoError(t, kindsTwo.HashInto(h))
		sum2 := h.Sum64()

		require.NotEqual(t, sum1, sum2)
	})
}

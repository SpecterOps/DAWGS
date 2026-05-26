package v2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdentifierSetOrAndRemoveNilSafe(t *testing.T) {
	var nilSet *identifierSet

	nilSet.Or(newIdentifierSet("ignored"))
	nilSet.Remove(newIdentifierSet("ignored"))

	set := newIdentifierSet("kept")
	set.Or(nil)
	set.Remove(nil)

	require.True(t, set.Contains("kept"))
}

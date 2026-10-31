package v2

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
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

func TestValidateConstraintKindPredicatesPropagatesWalkErrors(t *testing.T) {
	// Unsupported constraint roots fail the shared model walk before Build reaches
	// kind-predicate validation; the validator still propagates the walk error.
	err := validateConstraintKindPredicates([]cypher.SyntaxNode{struct{ Unsupported bool }{}})
	require.ErrorContains(t, err, "unable to negotiate cypher model type")
}

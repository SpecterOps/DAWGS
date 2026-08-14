package entity_test

import (
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/stretchr/testify/require"
)

func TestCloneKindsPreservesOrderAndDuplicates(t *testing.T) {
	input := []string{"User", "Admin", "User"}

	got := entity.CloneKinds(input)

	require.Equal(t, input, got)
	got[0] = "Changed"
	require.Equal(t, "User", input[0])
}

func TestClonePropertiesIsShallow(t *testing.T) {
	nested := map[string]any{"secret": "shared"}
	input := map[string]any{"name": "Ada", "nested": nested}

	got := entity.CloneProperties(input)

	got["name"] = "Grace"
	got["nested"].(map[string]any)["secret"] = "mutated"
	require.Equal(t, "Ada", input["name"])
	require.Equal(t, "mutated", nested["secret"])
}

func TestNodeValidateRequiresSourceID(t *testing.T) {
	require.Error(t, (entity.Node{}).Validate())
	require.NoError(t, (entity.Node{SourceID: "node-1"}).Validate())
}

func TestRelationshipValidateRequiresEndpointsAndKindButNotSourceID(t *testing.T) {
	valid := entity.Relationship{StartID: "node-1", EndID: "node-2", Kind: "MEMBER_OF"}

	require.NoError(t, valid.Validate())
	require.Error(t, (entity.Relationship{EndID: valid.EndID, Kind: valid.Kind}).Validate())
	require.Error(t, (entity.Relationship{StartID: valid.StartID, Kind: valid.Kind}).Validate())
	require.Error(t, (entity.Relationship{StartID: valid.StartID, EndID: valid.EndID}).Validate())
}

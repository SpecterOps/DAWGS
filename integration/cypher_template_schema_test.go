//go:build manual_integration

package integration

import (
	"encoding/json"
	"testing"

	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

func TestCypherTemplateDeclaredKinds(t *testing.T) {
	var file cypherTemplateFile
	require.NoError(t, json.Unmarshal([]byte(`{"node_kinds":["QueryOnly","Input"],"edge_kinds":["QueryLink"]}`), &file))
	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{{ID: "a", Kinds: []string{"Input"}}},
		Edges: []opengraph.Edge{{StartID: "a", EndID: "a", Kind: "FixtureLink"}},
	}
	file.Families = []cypherTemplateFamily{{Fixture: fixture}, {}}
	file.Metamorphic = []cypherMetamorphicFamily{{Fixture: fixture}, {}}
	nodeKinds, edgeKinds := cypherTemplateKinds([]cypherTemplateFile{file})
	require.ElementsMatch(t, []string{"QueryOnly", "Input"}, nodeKinds.Strings())
	require.ElementsMatch(t, []string{"QueryLink", "FixtureLink"}, edgeKinds.Strings())
	require.Len(t, fixture.Nodes, 1, "schema declarations must not add fixture entities")
}

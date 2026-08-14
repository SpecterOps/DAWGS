package ret

import (
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
)

func readJSONLNodesForTest(root string, artifact collection.JSONLArtifact) ([]entity.Node, error) {
	var values []entity.Node
	err := collection.ReadJSONLNodes(root, artifact, func(value entity.Node) error {
		values = append(values, value)
		return nil
	})
	return values, err
}

func readJSONLRelationshipsForTest(root string, artifact collection.JSONLArtifact) ([]entity.Relationship, error) {
	var values []entity.Relationship
	err := collection.ReadJSONLRelationships(root, artifact, func(value entity.Relationship) error {
		values = append(values, value)
		return nil
	})
	return values, err
}

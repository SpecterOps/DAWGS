package neo4j

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestRelationshipUpsertQuotesIdentifiers(t *testing.T) {
	for _, name := range []string{"User Account", "object`id", "MATCH", "étiquette", "a\"b", "x:Other) SET x.p = 1 //"} {
		t.Run(name, func(t *testing.T) {
			kind := graph.StringKind(name)
			update := graph.RelationshipUpdate{
				Start:             graph.PrepareNode(graph.NewProperties().Set(name, "start"), kind),
				End:               graph.PrepareNode(graph.NewProperties().Set(name, "end"), kind),
				Relationship:      graph.PrepareRelationship(graph.NewProperties().Set(name, "edge"), kind),
				StartIdentityKind: kind, EndIdentityKind: kind,
				StartIdentityProperties: []string{name}, EndIdentityProperties: []string{name}, IdentityProperties: []string{name},
			}
			statements, params := cypherBuildRelationshipUpdateQueryBatch([]graph.RelationshipUpdate{update})
			require.Len(t, statements, 1)
			quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
			for _, fragment := range []string{"s:" + quoted, "e:" + quoted, "r:" + quoted, quoted + ":p.s." + quoted, quoted + ":p.e." + quoted, quoted + ":p.r." + quoted} {
				require.Contains(t, statements[0], fragment)
			}
			require.Equal(t, "edge", params[0][0]["r"].(map[string]any)[name])
		})
	}
}

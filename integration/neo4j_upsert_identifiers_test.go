//go:build manual_integration

package integration

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNeo4jRelationshipUpsertEscapedIdentifiers(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("UpsertFixture")}})
	if _, isPostgreSQL := session.DB.(*pg.Driver); isPostgreSQL {
		t.Skip("Neo4j hand-built relationship upsert emitter")
	}
	ClearGraph(t, session.DB, session.Ctx)
	for i, name := range []string{"User Account", "object`id", "MATCH", "étiquette", "a\"b"} {
		t.Run(name, func(t *testing.T) {
			startKind, endKind, edgeKind := graph.StringKind("Start "+name), graph.StringKind("End "+name), graph.StringKind("Step "+name)
			extra := graph.StringKind("Extra " + name)
			value := fmt.Sprintf("identity-%d", i)
			update := graph.RelationshipUpdate{
				Start:             graph.PrepareNode(graph.NewProperties().Set(name, value+"-s"), startKind, extra),
				End:               graph.PrepareNode(graph.NewProperties().Set(name, value+"-e"), endKind, extra),
				Relationship:      graph.PrepareRelationship(graph.NewProperties().Set(name, value), edgeKind),
				StartIdentityKind: startKind, EndIdentityKind: endKind,
				StartIdentityProperties: []string{name}, EndIdentityProperties: []string{name}, IdentityProperties: []string{name},
			}
			for pass := 0; pass < 2; pass++ {
				update.Relationship.Properties.Set("revision", pass)
				require.NoError(t, session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error { return batch.UpdateRelationshipBy(update) }))
			}
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("MATCH (s)-[r]->(e) WHERE type(r) = $kind RETURN s, r, e", map[string]any{"kind": edgeKind.String()})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				var start, end graph.Node
				var rel graph.Relationship
				require.NoError(t, result.Scan(&start, &rel, &end))
				require.Equal(t, value+"-s", start.Properties.Map[name])
				require.Equal(t, value+"-e", end.Properties.Map[name])
				require.ElementsMatch(t, graph.Kinds{startKind, extra}, start.Kinds)
				require.ElementsMatch(t, graph.Kinds{endKind, extra}, end.Kinds)
				require.EqualValues(t, 1, rel.Properties.Map["revision"])
				require.Equal(t, value, rel.Properties.Map[name])
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}

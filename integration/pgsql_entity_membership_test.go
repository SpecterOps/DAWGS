//go:build manual_integration

package integration

import (
	"errors"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLEntityMembershipCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "entity_membership_shapes")
}

// Independent node/edge sequences need not overlap in an ordinary fixture.
// Pin that prerequisite explicitly, without changing either sequence.
func TestPostgreSQLEntityMembershipOverlappingIDs(t *testing.T) {
	nodeKind, edgeKind := graph.StringKind("MembershipOverlapNode"), graph.StringKind("MembershipOverlapEdge")
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true,
		CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{nodeKind}, ExtraEdgeKinds: graph.Kinds{edgeKind}})
	ClearGraph(t, session.DB, session.Ctx)
	driver := session.DB.(*pg.Driver)
	defaultGraph, ok := driver.DefaultGraph()
	require.True(t, ok)
	var nodeID graph.ID
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(map[string]any{"tag": "a"}), nodeKind)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.AsProperties(map[string]any{"tag": "b"}), nodeKind)
		if err != nil {
			return err
		}
		r, err := tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.AsProperties(map[string]any{"tag": "r"}))
		if err != nil {
			return err
		}
		nodeID = a.ID
		result := tx.Raw("UPDATE edge SET id = @overlap WHERE graph_id = @graph AND id = @edge", map[string]any{
			"overlap": a.ID, "graph": defaultGraph.ID, "edge": r.ID})
		defer result.Close()
		for result.Next() {
		}
		return result.Error()
	}))
	const (
		nodes     = "MATCH (n:MembershipOverlapNode {tag:'a'}) WITH collect(n) AS xs "
		edges     = "MATCH ()-[r:MembershipOverlapEdge]->() WITH collect(r) AS xs "
		matchNode = "MATCH (m:MembershipOverlapNode {tag:'a'}) "
		matchEdge = "MATCH ()-[s:MembershipOverlapEdge]->() "
	)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query string
				want        any
			}{
				{"overlap prerequisite", "MATCH (n:MembershipOverlapNode {tag:'a'}) MATCH ()-[r:MembershipOverlapEdge]->() RETURN id(n) = id(r)", true},
				{"edge in nodes", nodes + matchEdge + "WHERE s IN xs RETURN count(*)", int64(0)},
				{"node in edges", edges + matchNode + "WHERE m IN xs RETURN count(*)", int64(0)},
				{"negated edge", nodes + matchEdge + "WHERE NOT s IN xs RETURN count(*)", int64(1)},
				{"negated node", edges + matchNode + "WHERE NOT m IN xs RETURN count(*)", int64(1)},
				{"same node", nodes + matchNode + "WHERE m IN xs RETURN count(*)", int64(1)},
				{"same edge", edges + matchEdge + "WHERE s IN xs RETURN count(*)", int64(1)},
				{"full node collection", nodes + matchEdge + "RETURN s IN xs, size(xs)", false},
				{"full edge collection", edges + matchNode + "RETURN m IN xs, size(xs)", false},
				{"numeric identity", nodes + matchNode + "RETURN id(m) IN xs", false},
				{"edge SET", nodes + matchEdge + "WHERE s IN xs SET s.hit = true", nil},
				{"node SET", edges + matchNode + "WHERE m IN xs SET m.hit = true", nil},
				{"CREATE", nodes + matchEdge + "WHERE s IN xs CREATE (:MembershipOverlapNode {tag: 'created'})", nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
						parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
						require.NoError(t, err)
						translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), nil, defaultGraph.ID, translate.Options{OptimizerMode: mode})
						require.NoError(t, err)
						result := executeTemplateTranslation(t, tx, translation)
						if tc.want != nil {
							require.True(t, result.Next(), "%v", result.Error())
							require.Equal(t, tc.want, result.Values()[0])
						}
						for result.Next() {
						}
						require.NoError(t, result.Error())
						result.Close()
						// Check all stored entities, even for writes without RETURN.
						result = tx.Query("MATCH (n:MembershipOverlapNode) RETURN n ORDER BY n.tag", nil)
						for _, tag := range []string{"a", "b"} {
							require.True(t, result.Next(), "%v", result.Error())
							var n graph.Node
							require.NoError(t, result.Scan(&n))
							require.Equal(t, map[string]any{"tag": tag}, n.Properties.Map)
						}
						require.False(t, result.Next())
						require.NoError(t, result.Error())
						result.Close()
						result = tx.Query("MATCH ()-[r:MembershipOverlapEdge]->() RETURN r", nil)
						require.True(t, result.Next(), "%v", result.Error())
						var r graph.Relationship
						require.NoError(t, result.Scan(&r))
						require.Equal(t, nodeID, r.ID)
						require.Equal(t, map[string]any{"tag": "r"}, r.Properties.Map)
						require.False(t, result.Next())
						require.NoError(t, result.Error())
						result.Close()
						return errFixtureRollback
					})
					require.True(t, errors.Is(err, errFixtureRollback), "%v", err)
				})
			}
		})
	}
}

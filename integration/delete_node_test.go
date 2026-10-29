//go:build manual_integration

package integration

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Commit-time failures and physical edge counts need committed fixtures; the
// rollback-only corpus cannot detect either a failed commit or stored orphans.
func TestDeleteNodeCommitAndRollback(t *testing.T) {
	nodeKind, edgeKind := graph.StringKind("DeleteCommitNode"), graph.StringKind("DeleteCommitEdge")
	session := Open(t, Options{SkipIfNoConnection: true, ExtraNodeKinds: graph.Kinds{nodeKind}, ExtraEdgeKinds: graph.Kinds{edgeKind}})
	modes := []translate.OptimizerMode{translate.OptimizerEnabled}
	if session.Driver == pg.DriverName {
		modes = append(modes, translate.OptimizerDisabled)
	}
	for _, mode := range modes {
		for _, tc := range []struct {
			name, query  string
			reject       bool
			nodes, edges int64
		}{
			{"plain connected", "MATCH (n:DeleteCommitNode {name:'target'}) DELETE n", true, 4, 5},
			{"plain self loop", "MATCH (n:DeleteCommitNode {name:'target'})-[r:DeleteCommitEdge]->(n) DELETE n", true, 4, 5},
			{"plain several connected", "MATCH (n:DeleteCommitNode) WHERE n.name <> 'isolated' DELETE n", true, 4, 5},
			{"plain null relationship", "MATCH (n:DeleteCommitNode {name:'target'}) OPTIONAL MATCH (n)-[r:DeleteCommitEdge {name:'absent'}]->() DELETE n, r", true, 4, 5},
			{"plain write failure", "MATCH (n:DeleteCommitNode {name:'target'}) DELETE n", true, 4, 5},
			{"explicit subset leaves edges", "MATCH (n:DeleteCommitNode {name:'target'})-[r:DeleteCommitEdge {name:'out'}]->() DELETE n, r", true, 4, 5},
			{"detach connected", "MATCH (n:DeleteCommitNode {name:'target'}) DETACH DELETE n", false, 3, 1},
			{"detach incoming endpoint", "MATCH (n:DeleteCommitNode {name:'source'}) DETACH DELETE n", false, 3, 3},
			{"plain isolated", "MATCH (n:DeleteCommitNode {name:'isolated'}) DELETE n", false, 3, 5},
			{"explicit all incident edges", "MATCH (n:DeleteCommitNode {name:'target'})-[r]-() DELETE r, n", false, 3, 1},
			{"plain and detach", "MATCH (n:DeleteCommitNode {name:'source'}), (m:DeleteCommitNode {name:'target'}), (sink:DeleteCommitNode {name:'sink'}) DELETE n DETACH DELETE m, sink", false, 1, 0},
			{"missing node", "MATCH (n:DeleteCommitNode {name:'absent'}) DELETE n", false, 4, 5},
		} {
			for _, rollback := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/rollback=%t", mode, tc.name, rollback), func(t *testing.T) {
					session.ClearGraph(t)
					require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
						ids := map[string]graph.ID{}
						for _, name := range []string{"target", "source", "sink", "isolated"} {
							node, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), nodeKind)
							if err != nil {
								return err
							}
							ids[name] = node.ID
						}
						for _, edge := range [][3]string{{"target", "sink", "out"}, {"source", "target", "in"}, {"sink", "target", "reverse"}, {"target", "target", "loop"}, {"source", "sink", "survivor"}} {
							if _, err := tx.CreateRelationshipByIDs(ids[edge[0]], ids[edge[1]], edgeKind, graph.AsProperties(map[string]any{"name": edge[2]})); err != nil {
								return err
							}
						}
						return nil
					}))
					err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
						if tc.name == "plain write failure" {
							// Check rollback of an earlier successful statement. Same-part
							// SET/DELETE frame continuation is tracked separately under 043.
							prior := tx.Query("MATCH (n:DeleteCommitNode {name:'target'}) SET n.touched = true", nil)
							for prior.Next() {
							}
							prior.Close()
							if err := prior.Error(); err != nil {
								return err
							}
						}
						var result graph.Result
						if session.Driver == pg.DriverName {
							driver := session.DB.(*pg.Driver)
							target, ok := driver.DefaultGraph()
							require.True(t, ok)
							parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
							if err != nil {
								return err
							}
							translated, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), nil, target.ID, translate.Options{OptimizerMode: mode})
							if err != nil {
								return err
							}
							result = executeTemplateTranslation(t, tx, translated)
						} else {
							result = tx.Query(tc.query, nil)
						}
						for result.Next() {
						}
						result.Close()
						if err := result.Error(); err != nil {
							return err
						}
						if rollback {
							return errFixtureRollback
						}
						return nil
					})
					if rollback {
						require.Error(t, err)
						if !tc.reject {
							require.ErrorIs(t, err, errFixtureRollback)
						}
					} else if tc.reject {
						require.Error(t, err)
						require.Contains(t, err.Error(), "relationship")
					} else {
						require.NoError(t, err)
					}
					wantNodes, wantEdges := tc.nodes, tc.edges
					if rollback || tc.reject {
						wantNodes, wantEdges = 4, 5
					}
					require.Equal(t, wantNodes, countByCypher(t, session.Ctx, session.DB, "MATCH (n:DeleteCommitNode) RETURN count(n)"))
					if session.Driver == pg.DriverName {
						require.Equal(t, wantEdges, countEdgesByKindSQL(t, session.Ctx, session.DB, edgeKind))
					} else {
						require.Equal(t, wantEdges, countByCypher(t, session.Ctx, session.DB, "MATCH ()-[r:DeleteCommitEdge]->() RETURN count(r)"))
					}
					// A failed/rolled-back deletion must preserve the original properties too.
					if rollback || tc.reject {
						require.Equal(t, int64(1), countByCypher(t, session.Ctx, session.DB, "MATCH (n:DeleteCommitNode {name:'target'}) RETURN count(n)"))
						require.Zero(t, countByCypher(t, session.Ctx, session.DB, "MATCH (n:DeleteCommitNode {touched:true}) RETURN count(n)"))
					}
				})
			}
		}
	}
}

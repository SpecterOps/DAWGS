//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLDeleteNodeCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "delete_node_shapes")
}

// Error-producing deletion guards must never run on unmatched connected nodes,
// even when PostgreSQL scans the delete target using a hash join.
func TestPostgreSQLPlainDeletePlanIndependence(t *testing.T) {
	nodeKind, edgeKind := graph.StringKind("DeletePlanNode"), graph.StringKind("DeletePlanEdge")
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, ExtraNodeKinds: graph.Kinds{nodeKind}, ExtraEdgeKinds: graph.Kinds{edgeKind}})
	session.ClearGraph(t)
	driver := session.DB.(*pg.Driver)
	target, ok := driver.DefaultGraph()
	require.True(t, ok)
	parsed, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (n:DeletePlanNode {name:'isolated'}) DELETE n")
	require.NoError(t, err)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			require.NoError(t, session.WithRollback(t, func(tx graph.Transaction) error {
				isolated, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "isolated"}), nodeKind)
				if err != nil {
					return err
				}
				a, err := tx.CreateNode(graph.NewProperties(), nodeKind)
				if err != nil {
					return err
				}
				b, err := tx.CreateNode(graph.NewProperties(), nodeKind)
				if err != nil {
					return err
				}
				if _, err := tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties()); err != nil {
					return err
				}
				for _, setting := range []string{"set local enable_nestloop = off", "set local enable_mergejoin = off"} {
					result := tx.Raw(setting, nil)
					result.Close()
					if err := result.Error(); err != nil {
						return err
					}
				}
				translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), nil, target.ID, translate.Options{OptimizerMode: mode})
				if err != nil {
					return err
				}
				result := executeTemplateTranslation(t, tx, translation)
				for result.Next() {
				}
				result.Close()
				if err := result.Error(); err != nil {
					return err
				}
				count, err := tx.Nodes().Count()
				if err != nil {
					return err
				}
				require.Equal(t, int64(2), count)
				result = tx.Raw("select count(*) from node where graph_id=@graph and id=@id", map[string]any{"graph": target.ID, "id": int64(isolated.ID)})
				defer result.Close()
				require.True(t, result.Next())
				require.NoError(t, result.Scan(&count))
				require.Zero(t, count)
				return result.Error()
			}))
		})
	}
}

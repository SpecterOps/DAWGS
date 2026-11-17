//go:build manual_integration

package integration

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

const (
	// multiExpansionStatisticsGraph isolates planner-statistics preparation from other integration fixtures.
	multiExpansionStatisticsGraph = "integration_multi_expansion_statistics"

	// multiExpansionFixtureStatisticsSQL reads immediate relation estimates for the selected graph's two partitions.
	multiExpansionFixtureStatisticsSQL = `select nodes.reltuples::bigint, edges.reltuples::bigint
from pg_catalog.pg_class nodes, pg_catalog.pg_class edges
where nodes.oid = @node_table::regclass and edges.oid = @edge_table::regclass`
)

// TestPostgreSQLMultiExpansionFixtureStatistics checks fixture-local statistics, complete fixed-length paths, and rollback visibility.
func TestPostgreSQLMultiExpansionFixtureStatistics(t *testing.T) {
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		GraphName:            multiExpansionStatisticsGraph,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{graph.StringKind("MultiExpansionNode")},
		ExtraEdgeKinds:       graph.StringsToKinds([]string{multiExpansionDomains[0][0], multiExpansionDomains[0][1], "MultiExpansionOther"}),
	})
	session.ClearGraph(t)
	selected, found := session.DB.(*pg.Driver).DefaultGraph()
	require.True(t, found)
	var (
		nodeTable = pgx.Identifier{"public", selected.Partitions.Node.Name}.Sanitize()
		edgeTable = pgx.Identifier{"public", selected.Partitions.Edge.Name}.Sanitize()
	)

	// Start with known empty estimates independently of the preparation under
	// test. A missing preparation call must not inherit useful earlier stats.
	_, err := session.PGPool.Exec(session.Ctx, fmt.Sprintf(multiExpansionAnalyzeStatement, nodeTable, edgeTable))
	require.NoError(t, err)

	for _, width := range []int{2, 3} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			fixture := multiExpansionFixture(multiExpansionDomains[0], width, 3)
			require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
				if err := prepareMultiExpansionFixtureStatistics(session, tx); err != nil {
					return err
				}

				statistics := tx.Raw(multiExpansionFixtureStatisticsSQL, map[string]any{
					"node_table": nodeTable,
					"edge_table": edgeTable,
				})
				require.True(t, statistics.Next(), "%v", statistics.Error())
				var nodeEstimate, edgeEstimate int64
				require.NoError(t, statistics.Scan(&nodeEstimate, &edgeEstimate))
				require.False(t, statistics.Next())
				statistics.Close()
				require.NoError(t, statistics.Error())
				require.EqualValues(t, len(fixture.Nodes), nodeEstimate)
				require.EqualValues(t, len(fixture.Edges), edgeEstimate)

				var (
					sequence = []string{multiExpansionDomains[0][0], multiExpansionDomains[0][1], multiExpansionDomains[0][0]}
					params   = map[string]any{"root": ids["root"].Int64()}
					expected = make(map[string]int, width*width*width)
					actual   = make(map[string]int, width*width*width)
				)
				for first := range width {
					for second := range width {
						for third := range width {
							expected[fmt.Sprint([]graph.ID{
								ids["root"],
								ids[fmt.Sprintf("layer-1-%d", first)],
								ids[fmt.Sprintf("layer-2-%d", second)],
								ids[fmt.Sprintf("layer-3-%d", third)],
							})] = 1
						}
					}
				}
				require.EqualValues(t, width*width*width, multiExpansionScalar(t, tx.Query(multiExpansionQuery(sequence, false, "count(*)"), params)))

				paths := tx.Query(multiExpansionQuery(sequence, false, "p"), params)
				defer paths.Close()
				for paths.Next() {
					var path graph.Path
					require.NoError(t, paths.Scan(&path))
					require.Len(t, path.Edges, len(sequence))
					require.Len(t, path.Nodes, len(sequence)+1)
					nodeIDs := make([]graph.ID, len(path.Nodes))
					for index, node := range path.Nodes {
						nodeIDs[index] = node.ID
					}
					for index, edge := range path.Edges {
						require.Equal(t, sequence[index], edge.Kind.String())
						require.Equal(t, nodeIDs[index], edge.StartID)
						require.Equal(t, nodeIDs[index+1], edge.EndID)
					}
					actual[fmt.Sprint(nodeIDs)]++
				}
				require.NoError(t, paths.Error())
				require.Equal(t, expected, actual)
				return nil
			}))

			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				if nodes, err := tx.Nodes().Count(); err != nil {
					return err
				} else if edges, err := tx.Relationships().Count(); err != nil {
					return err
				} else {
					require.Zero(t, nodes)
					require.Zero(t, edges)
					return nil
				}
			}))
		})
	}
}

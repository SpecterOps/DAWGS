//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestKindCacheStableSnapshotUsesCatalogAtFirstStatement(t *testing.T) {
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		for _, change := range []string{"rename", "reassign_id"} {
			for _, firstQuery := range []string{"cypher", "fluent", "raw"} {
				t.Run(fmt.Sprintf("%s/%s/%s", isolation, change, firstQuery), func(t *testing.T) {
					driver := newV2IntegrationDriver(t, 2, 8, nil)
					setUpV2IntegrationGraph(t, driver)
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					name := fmt.Sprintf("FirstSnapshot%d", time.Now().UnixNano())
					original := graph.StringKind(name)
					replacement := original
					if change == "rename" {
						replacement = graph.StringKind(name + "Replacement")
					}
					t.Cleanup(func() {
						_, err := driver.pool.Exec(context.Background(), "delete from kind where name=any($1)", []string{original.String(), replacement.String()})
						require.NoError(t, err)
					})
					var node *graph.Node
					require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
						var err error
						node, err = tx.CreateNode(graph.NewProperties(), original)
						return err
					}))
					require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
						// BEGIN has run, but this transaction has not established a
						// PostgreSQL data snapshot when the external change commits.
						if change == "rename" {
							_, err := driver.pool.Exec(ctx, "update kind set name=$1 where name=$2", replacement.String(), original.String())
							if err != nil {
								return err
							}
							if err := driver.RefreshKinds(ctx); err != nil {
								return err
							}
						} else {
							replaceMutationKind(t, ctx, driver, original)
						}
						originalCount := int64(1)
						if change == "rename" {
							originalCount = 0
						}
						switch firstQuery {
						case "cypher":
							requireSnapshotCypherKindCount(t, tx, original, originalCount)
						case "fluent":
							count, err := tx.Nodes().Filter(query.Kind(query.Node(), original)).Count()
							require.NoError(t, err)
							require.Equal(t, originalCount, count)
						case "raw":
							requireSnapshotRawNodeKind(t, tx, node.ID, replacement)
						}
						requireAdoptionKindCount(t, tx, original, originalCount)
						requireAdoptionKindCount(t, tx, replacement, 1)
						requireSnapshotRawNodeKind(t, tx, node.ID, replacement)
						return nil
					}, OptionSetTransactionIsolation(isolation)))
				})
			}
		}
	}
}

func requireSnapshotCypherKindCount(t *testing.T, tx graph.Transaction, kind graph.Kind, want int64) {
	t.Helper()
	result := tx.Query(fmt.Sprintf("MATCH (n:%s) RETURN count(n)", kind), nil)
	defer result.Close()
	require.True(t, result.Next(), "%v", result.Error())
	var count int64
	require.NoError(t, graph.ScanNextResult(result, &count))
	require.NoError(t, result.Error())
	require.Equal(t, want, count)
}

func requireSnapshotRawNodeKind(t *testing.T, tx graph.Transaction, id graph.ID, kind graph.Kind) {
	t.Helper()
	result := tx.Raw("select (id,kind_ids,properties)::nodecomposite from node where id=@id", map[string]any{"id": id})
	defer result.Close()
	require.True(t, result.Next(), "%v", result.Error())
	var node graph.Node
	require.NoError(t, graph.ScanNextResult(result, &node))
	require.NoError(t, result.Error())
	require.Equal(t, graph.Kinds{kind}, node.Kinds)
}

func TestKindCacheStableSnapshotFirstMutationsUseRefreshedKindID(t *testing.T) {
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		for _, operation := range []string{"node", "relationship"} {
			t.Run(fmt.Sprintf("%s/%s", isolation, operation), func(t *testing.T) {
				driver := newV2IntegrationDriver(t, 2, 8, nil)
				fixture := setUpV2IntegrationGraph(t, driver)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				kind := graph.StringKind(fmt.Sprintf("FirstMutationSnapshot%d", time.Now().UnixNano()))
				_, err := driver.AssertKinds(ctx, graph.Kinds{kind})
				require.NoError(t, err)
				t.Cleanup(func() {
					_, err := driver.pool.Exec(context.Background(), "delete from kind where name=$1", kind.String())
					require.NoError(t, err)
				})
				require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					newID := replaceMutationKind(t, ctx, driver, kind)
					if operation == "node" {
						node, err := tx.CreateNode(graph.NewProperties(), kind)
						if err != nil {
							return err
						}
						require.Equal(t, graph.Kinds{kind}, node.Kinds)
						var storedIDs []int16
						require.NoError(t, tx.(*transaction).tx.QueryRow(ctx, "select kind_ids from node where id=$1", node.ID).Scan(&storedIDs))
						require.Equal(t, []int16{newID}, storedIDs)
					} else {
						edge, err := tx.CreateRelationshipByIDs(fixture.start, fixture.end, kind, graph.NewProperties())
						if err != nil {
							return err
						}
						require.Equal(t, kind, edge.Kind)
						var storedID int16
						require.NoError(t, tx.(*transaction).tx.QueryRow(ctx, "select kind_id from edge where id=$1", edge.ID).Scan(&storedID))
						require.Equal(t, newID, storedID)
					}
					return nil
				}, OptionSetTransactionIsolation(isolation)))
			})
		}
	}
}

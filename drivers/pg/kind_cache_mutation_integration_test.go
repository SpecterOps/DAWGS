//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// replaceMutationKind models a committed external catalog reset and updates
// only rows using this test's unique kind, before invoking the public refresh.
func replaceMutationKind(t *testing.T, ctx context.Context, driver *Driver, kind graph.Kind) int16 {
	t.Helper()
	oldID := driver.kindCache.current.Load().kinds[kind]
	tx, err := driver.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "delete from kind where name=$1", kind.String())
	require.NoError(t, err)
	var newID int16
	require.NoError(t, tx.QueryRow(ctx, "insert into kind(name) values ($1) returning id", kind.String()).Scan(&newID))
	_, err = tx.Exec(ctx, "update node set kind_ids=array_replace(kind_ids,$1::int2,$2::int2) where $1=any(kind_ids)", oldID, newID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "update edge set kind_id=$2 where kind_id=$1", oldID, newID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, driver.RefreshKinds(ctx))
	require.NotEqual(t, oldID, newID)
	return newID
}

func TestKindCacheDirectMutationsUseRefreshedKinds(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		name := "node"
		if relationship {
			name = "relationship"
		}
		t.Run(name, func(t *testing.T) {
			driver := newV2IntegrationDriver(t, 2, 8, nil)
			fixture := setUpV2IntegrationGraph(t, driver)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			kind := graph.StringKind(fmt.Sprintf("MutationDirect%d", time.Now().UnixNano()))
			_, err := driver.AssertKinds(ctx, graph.Kinds{kind})
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := driver.pool.Exec(context.Background(), "delete from kind where name=$1", kind.String())
				require.NoError(t, err)
			})
			require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				newID := replaceMutationKind(t, ctx, driver, kind)
				if relationship {
					edge, err := tx.CreateRelationshipByIDs(fixture.start, fixture.end, kind, graph.NewProperties())
					if err != nil {
						return err
					}
					require.Equal(t, kind, edge.Kind)
					var storedID int16
					require.NoError(t, tx.(*transaction).tx.QueryRow(ctx, "select kind_id from edge where id=$1", edge.ID).Scan(&storedID))
					require.Equal(t, newID, storedID)
				} else {
					node, err := tx.CreateNode(graph.NewProperties(), kind)
					if err != nil {
						return err
					}
					require.Equal(t, graph.Kinds{kind}, node.Kinds)
					var storedIDs []int16
					require.NoError(t, tx.(*transaction).tx.QueryRow(ctx, "select kind_ids from node where id=$1", node.ID).Scan(&storedIDs))
					require.Equal(t, []int16{newID}, storedIDs)
				}
				return nil
			}))
		})
	}
}

func TestKindCacheBatchMutationsUseRefreshedKinds(t *testing.T) {
	for _, operation := range []string{"create", "preset_create", "bulk_create", "update", "large_update", "upsert", "relationship_create", "relationship_upsert"} {
		t.Run(operation, func(t *testing.T) {
			driver := newV2IntegrationDriver(t, 2, 8, nil)
			fixture := setUpV2IntegrationGraph(t, driver)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if operation == "upsert" || operation == "relationship_upsert" {
				schema := v2IntegrationSchema.Graphs[0]
				field := "cache_identity"
				if operation == "relationship_upsert" {
					field = "name"
				}
				schema.NodeConstraints = []graph.Constraint{{Field: field, Type: graph.BTreeIndex}}
				require.NoError(t, driver.AssertSchema(ctx, graph.Schema{Graphs: []graph.Graph{schema}, DefaultGraph: schema}))
			}
			kind := graph.StringKind(fmt.Sprintf("MutationBatch%d", time.Now().UnixNano()))
			_, err := driver.AssertKinds(ctx, graph.Kinds{kind})
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := driver.pool.Exec(context.Background(), "delete from kind where name=$1", kind.String())
				require.NoError(t, err)
			})
			var newID int16
			require.NoError(t, driver.BatchOperation(ctx, func(generic graph.Batch) error {
				b := generic.(*batch)
				for round := 0; round < 2; round++ {
					newID = replaceMutationKind(t, ctx, driver, kind)
					nodes := make([]*graph.Node, 2)
					for i := range nodes {
						nodes[i] = graph.PrepareNode(graph.AsProperties(map[string]any{"cache_key": kind.String(), "cache_identity": fmt.Sprintf("%s_%d", kind, i), "ordinal": i}), kind)
					}
					switch operation {
					case "bulk_create":
						if _, err := b.CreateNodes(nodes); err != nil {
							return err
						}
					case "update", "large_update":
						nodes[0].ID, nodes[1].ID = fixture.start, fixture.end
						if operation == "large_update" {
							if err := b.largeUpdate(nodes); err != nil {
								return err
							}
						} else if err := b.UpdateNodes(nodes); err != nil {
							return err
						}
					default:
						for i, node := range nodes {
							switch operation {
							case "create", "preset_create":
								if operation == "preset_create" {
									require.NoError(t, driver.pool.QueryRow(ctx, "select nextval(pg_get_serial_sequence('node','id'))").Scan(&node.ID))
								}
								if err := b.CreateNode(node); err != nil {
									return err
								}
							case "upsert":
								if err := b.UpdateNodeBy(graph.NodeUpdate{Node: node, IdentityProperties: []string{"cache_identity"}}); err != nil {
									return err
								}
							case "relationship_create", "relationship_upsert":
								start, end := fixture.start, fixture.end
								startName, endName := "start", "end"
								if i == 1 {
									start, end, startName, endName = end, start, endName, startName
								}
								if operation == "relationship_create" {
									if err := b.CreateRelationshipByIDs(start, end, kind, node.Properties); err != nil {
										return err
									}
								} else if err := b.UpdateRelationshipBy(graph.RelationshipUpdate{
									Relationship: &graph.Relationship{Kind: kind, Properties: node.Properties},
									Start:        graph.PrepareNode(graph.NewProperties().Set("name", startName), v2IntegrationNodeKind), StartIdentityProperties: []string{"name"},
									End: graph.PrepareNode(graph.NewProperties().Set("name", endName), v2IntegrationNodeKind), EndIdentityProperties: []string{"name"},
								}); err != nil {
									return err
								}
							}
						}
					}
					if err := b.Commit(); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(1)))
			var total, valid int64
			want := int64(2)
			if operation == "create" || operation == "preset_create" || operation == "bulk_create" {
				want = 4
			}
			if operation == "relationship_create" || operation == "relationship_upsert" {
				err = driver.pool.QueryRow(ctx, "select count(*),count(*) filter (where kind_id=$2) from edge where properties->>'cache_key'=$1", kind.String(), newID).Scan(&total, &valid)
			} else {
				err = driver.pool.QueryRow(ctx, `select count(*),count(*) filter (where $2=any(kind_ids)
					and not exists(select 1 from unnest(kind_ids) item(id) where not exists(select 1 from kind where kind.id=item.id)))
					from node where properties->>'cache_key'=$1`, kind.String(), newID).Scan(&total, &valid)
			}
			require.NoError(t, err)
			require.Equal(t, want, total)
			require.Equal(t, total, valid, "every committed row must use the refreshed catalog ID")
		})
	}
}

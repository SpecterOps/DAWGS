//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestOpenTraversalResourceFailureRollsBackWrites(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kind := graph.StringKind("DAWGS065ResourceNode")
	step := graph.StringKind("DAWGS065ResourceStep")
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var prior *graph.Node
		for i := 0; i < 8; i++ {
			node, err := tx.CreateNode(graph.NewProperties().Set("i", i), kind)
			if err != nil {
				return err
			}
			if prior != nil {
				if _, err := tx.CreateRelationshipByIDs(prior.ID, node.ID, step, graph.NewProperties()); err != nil {
					return err
				}
			}
			prior = node
		}
		return nil
	}))
	for _, budget := range []struct{ name, setting, value string }{
		{"states", "dawgs.traversal_state_limit", "2"},
		{"bytes", "dawgs.traversal_byte_limit", "150"},
	} {
		for _, query := range []string{
			"MATCH (a:DAWGS065ResourceNode {i:0})-[:DAWGS065ResourceStep*]->(b) SET b.changed = true RETURN b",
			"MATCH (a:DAWGS065ResourceNode {i:0}) SET a.changed = true WITH a MATCH (a)-[:DAWGS065ResourceStep*]->(b) SET b.changed = true RETURN b",
			"MATCH p=allShortestPaths((a:DAWGS065ResourceNode {i:0})-[:DAWGS065ResourceStep*]->(b:DAWGS065ResourceNode {i:7})) SET b.changed=true RETURN p",
		} {
			t.Run(budget.name+query, func(t *testing.T) {
				var executionError error
				err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					result := tx.Raw("select set_config(@setting, @value, true)", map[string]any{"setting": budget.setting, "value": budget.value})
					result.Close()
					if err := result.Error(); err != nil {
						return err
					}
					result = tx.Query(query, nil)
					for result.Next() {
					}
					result.Close()
					executionError = result.Error()
					return nil // Consuming the result error must never allow writes to commit.
				})
				require.Error(t, err)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, executionError, &pgErr)
				require.Equal(t, "54000", pgErr.Code)
				require.Contains(t, pgErr.Message, "traversal resource limit")
				require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
					result := tx.Query("MATCH (n:DAWGS065ResourceNode) WHERE n.changed = true RETURN count(n)", nil)
					defer result.Close()
					require.True(t, result.Next(), "%v", result.Error())
					require.EqualValues(t, 0, result.Values()[0])
					return result.Error()
				}))
			})
		}
	}
	// Independent executions receive fresh budgets; short successful searches do
	// not consume the next query's admission allowance on a reused connection.
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < 2; i++ {
			result := tx.Query("MATCH (a:DAWGS065ResourceNode {i:0})-[:DAWGS065ResourceStep*]->(b) RETURN count(b)", nil)
			require.True(t, result.Next(), "%v", result.Error())
			require.EqualValues(t, 7, result.Values()[0])
			result.Close()
			if err := result.Error(); err != nil {
				return err
			}
		}
		return nil
	}))
}

// A shallow high-fan-out search must charge every joined candidate, not just
// its parent row. LIMIT may finish early, but does not grant free admission.
func TestOpenTraversalFanoutAdmission(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		root, err := tx.CreateNode(graph.NewProperties().Set("root", true), dawgs065BaseNodeKind)
		if err != nil {
			return err
		}
		middle, err := tx.CreateNode(graph.NewProperties(), dawgs065BaseNodeKind)
		if err != nil {
			return err
		}
		step := graph.StringKind("DAWGS065FanoutStep")
		if _, err = tx.CreateRelationshipByIDs(root.ID, middle.ID, step, graph.NewProperties()); err != nil {
			return err
		}
		for i := 0; i < 32; i++ {
			end, err := tx.CreateNode(graph.NewProperties(), dawgs065BaseNodeKind)
			if err != nil {
				return err
			}
			if _, err = tx.CreateRelationshipByIDs(middle.ID, end.ID, step, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}))
	err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		setting := tx.Raw("select set_config('dawgs.traversal_state_limit','5',true)", nil)
		setting.Close()
		if err := setting.Error(); err != nil {
			return err
		}
		result := tx.Query("MATCH (s:DAWGS065WedgeNode {root:true})-[:DAWGS065FanoutStep*]->(e) RETURN id(e) LIMIT 6", nil)
		for result.Next() {
		}
		result.Close()
		return result.Error()
	})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "54000", pgErr.Code)
}

// Diagnostic qualification of complete search on longer chains. This is a
// bounded fixture, not a production performance or peak-memory assertion.
func TestOpenTraversalLongChainQualification(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rollback := fmt.Errorf("rollback chain qualification")
	require.ErrorIs(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		kind, step := graph.StringKind("DAWGS065LongNode"), graph.StringKind("DAWGS065LongStep")
		nodes := make([]*graph.Node, 129)
		for i := range nodes {
			node, err := tx.CreateNode(graph.NewProperties(), kind)
			if err != nil {
				return err
			}
			nodes[i] = node
			if i > 0 {
				if _, err := tx.CreateRelationshipByIDs(nodes[i-1].ID, node.ID, step, graph.NewProperties()); err != nil {
					return err
				}
			}
		}
		for _, depth := range []int{17, 64, 128} {
			for _, mode := range []string{"ordinary", "shortestPath", "allShortestPaths"} {
				bounds := []string{"*", fmt.Sprintf("*1..%d", depth)}
				for _, bound := range bounds {
					pattern := "(s:DAWGS065LongNode)-[:DAWGS065LongStep" + bound + "]->(e:DAWGS065LongNode)"
					if mode != "ordinary" {
						pattern = mode + "(" + pattern + ")"
					}
					query := "MATCH p=" + pattern + " WHERE id(s)=$start_id AND id(e)=$end_id RETURN length(p)"
					started := time.Now()
					result := tx.Query(query, map[string]any{"start_id": nodes[0].ID, "end_id": nodes[depth].ID})
					require.True(t, result.Next(), "%v", result.Error())
					require.EqualValues(t, depth, result.Values()[0])
					require.False(t, result.Next())
					result.Close()
					if err := result.Error(); err != nil {
						return err
					}
					t.Logf("mode=%s depth=%d bound=%s elapsed=%s", mode, depth, bound, time.Since(started))
				}
			}
		}
		return rollback
	}), rollback)
}

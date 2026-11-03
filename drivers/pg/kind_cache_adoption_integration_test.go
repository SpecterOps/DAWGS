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

func requireAdoptionKindCount(t *testing.T, tx graph.Transaction, kind graph.Kind, want int64) {
	t.Helper()
	result := tx.Query(fmt.Sprintf("MATCH (n:%s) RETURN count(n)", kind), nil)
	require.True(t, result.Next(), "%v", result.Error())
	var count int64
	require.NoError(t, graph.ScanNextResult(result, &count))
	result.Close()
	require.NoError(t, result.Error())
	require.Equal(t, want, count)
	count, err := tx.Nodes().Filter(query.Kind(query.Node(), kind)).Count()
	require.NoError(t, err)
	require.Equal(t, want, count)
}

func TestKindCacheReadCommittedAdoptsRefreshWithPrivateOverlays(t *testing.T) {
	driver := newV2IntegrationDriver(t, 2, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	name := fmt.Sprintf("AdoptionRC%d", time.Now().UnixNano())
	known, learned, private, existing := graph.StringKind(name), graph.StringKind(name+"Learned"), graph.StringKind(name+"Private"), graph.StringKind(name+"Existing")
	t.Cleanup(func() {
		_, err := driver.pool.Exec(context.Background(), "delete from kind where name=any($1)", []string{known.String(), learned.String(), private.String(), existing.String()})
		require.NoError(t, err)
	})
	var node *graph.Node
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		node, err = tx.CreateNode(graph.NewProperties(), known)
		return err
	}))
	_, err := driver.pool.Exec(ctx, "insert into kind(name) values ($1)", learned.String())
	require.NoError(t, err)
	var existingID int16
	require.NoError(t, driver.pool.QueryRow(ctx, "insert into kind(name) values ($1) returning id", existing.String()).Scan(&existingID))
	target, _ := driver.DefaultGraph()
	var existingNodeID int64
	require.NoError(t, driver.pool.QueryRow(ctx, "insert into node(graph_id,kind_ids,properties) values ($1,array[$2]::int2[],'{}') returning id", target.ID, existingID).Scan(&existingNodeID))
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		requireAdoptionKindCount(t, tx, learned, 0)
		mapper := tx.(*transaction).kindMapper
		require.Contains(t, mapper.kinds, learned)
		privateIDs, err := mapper.AssertKinds(ctx, graph.Kinds{private})
		if err != nil {
			return err
		}
		existingIDs, err := mapper.AssertKinds(ctx, graph.Kinds{existing})
		if err != nil {
			return err
		}
		require.Equal(t, []int16{existingID}, existingIDs, "InsertOrGetKind also retains existing committed rows")
		before := mapper.snapshot
		replacement, err := driver.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer replacement.Rollback(ctx)
		if _, err := replacement.Exec(ctx, "delete from kind where name=$1", known.String()); err != nil {
			return err
		}
		var newID int16
		if err := replacement.QueryRow(ctx, "insert into kind(name) values ($1) returning id", known.String()).Scan(&newID); err != nil {
			return err
		}
		if _, err := replacement.Exec(ctx, "update node set kind_ids=array[$1]::int2[] where id=$2", newID, node.ID); err != nil {
			return err
		}
		if _, err := replacement.Exec(ctx, "delete from kind where name=$1", existing.String()); err != nil {
			return err
		}
		var newExistingID int16
		if err := replacement.QueryRow(ctx, "insert into kind(name) values ($1) returning id", existing.String()).Scan(&newExistingID); err != nil {
			return err
		}
		if _, err := replacement.Exec(ctx, "update node set kind_ids=array[$1]::int2[] where id=$2", newExistingID, existingNodeID); err != nil {
			return err
		}
		if err := replacement.Commit(ctx); err != nil {
			return err
		}
		if err := driver.RefreshKinds(ctx); err != nil {
			return err
		}
		require.Greater(t, driver.kindCache.current.Load().epoch, before.epoch)
		requireAdoptionKindCount(t, tx, known, 1)
		requireAdoptionKindCount(t, tx, existing, 1)
		require.Same(t, driver.kindCache.current.Load(), mapper.snapshot)
		id, err := mapper.MapKind(ctx, private)
		require.NoError(t, err)
		require.Equal(t, privateIDs[0], id, "refresh cannot lose this transaction's uncommitted registration")
		kind, err := mapper.MapKindID(ctx, id)
		require.NoError(t, err)
		require.Equal(t, private, kind)
		return nil
	}, OptionSetTransactionIsolation(pgx.ReadCommitted)))
	require.NotContains(t, driver.kindCache.current.Load().kinds, private, "old registrations cannot publish across the refreshed epoch")
	require.True(t, driver.kindCache.pending())
	require.NoError(t, driver.RefreshKinds(ctx))
	require.Contains(t, driver.kindCache.current.Load().kinds, private)
}

func TestKindCacheStableSnapshotFencesReassignedKindName(t *testing.T) {
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(isolation), func(t *testing.T) {
			driver := newV2IntegrationDriver(t, 2, 8, nil)
			setUpV2IntegrationGraph(t, driver)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			name := fmt.Sprintf("AdoptionStable%d", time.Now().UnixNano())
			original, replacement := graph.StringKind(name), graph.StringKind(name+"Replacement")
			t.Cleanup(func() {
				_, err := driver.pool.Exec(context.Background(), "delete from kind where name=any($1)", []string{original.String(), replacement.String()})
				require.NoError(t, err)
			})
			require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				_, err := tx.CreateNode(graph.NewProperties(), original)
				return err
			}))
			require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				requireAdoptionKindCount(t, tx, original, 1)
				_, err := driver.pool.Exec(ctx, "update kind set name=$1 where name=$2", replacement.String(), original.String())
				if err != nil {
					return err
				}
				if err := driver.RefreshKinds(ctx); err != nil {
					return err
				}
				requireAdoptionKindCount(t, tx, replacement, 0)
				requireAdoptionKindCount(t, tx, original, 1)
				return nil
			}, OptionSetTransactionIsolation(isolation)))
		})
	}
}

//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestUnknownKindsRemainUnregisteredAndRecompile(t *testing.T) {
	driver := newV2IntegrationDriver(t, 3, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kind := graph.StringKind(fmt.Sprintf("DAWGS023Unregistered%d", time.Now().UnixNano()))
	query := fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS c", kind)
	assertCount := func(want int64) {
		t.Helper()
		require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result := tx.Query(query, nil)
			defer result.Close()
			require.True(t, result.Next())
			var got int64
			require.NoError(t, result.Scan(&got))
			require.Equal(t, want, got)
			require.False(t, result.Next())
			return result.Error()
		}))
	}
	// Both compilation entry points must return empty matches without creating metadata.
	graphTarget, ok := driver.DefaultGraph()
	require.True(t, ok)
	for i := 0; i < 2; i++ {
		assertCount(0)
		_, _, err := driver.SchemaManager.compileText(ctx, query, nil, graphTarget.ID)
		require.NoError(t, err)
	}
	var count int64
	require.NoError(t, driver.pool.QueryRow(ctx, "select count(*) from kind where name = $1", kind.String()).Scan(&count))
	require.Zero(t, count)
	_, err := driver.MapKinds(ctx, graph.Kinds{kind})
	var missing *pgsql.MissingKindsError
	require.ErrorAs(t, err, &missing)
	require.Empty(t, missing.KindIDs)
	// Registration must affect the exact previously compiled query.
	ids, err := driver.AssertKinds(ctx, graph.Kinds{kind})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := driver.pool.Exec(context.Background(), "delete from kind where id = $1", ids[0])
		require.NoError(t, err)
	})
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties(), kind)
		return err
	}))
	t.Cleanup(func() {
		require.NoError(t, driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			result := tx.Query(fmt.Sprintf("MATCH (n:%s) DELETE n", kind), nil)
			defer result.Close()
			for result.Next() {
			}
			return result.Error()
		}))
	})
	assertCount(1)
	sql, params, err := driver.SchemaManager.compileText(ctx, query, nil, graphTarget.ID)
	require.NoError(t, err)
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(sql, params)
		defer result.Close()
		require.True(t, result.Next())
		var got int64
		require.NoError(t, result.Scan(&got))
		require.Equal(t, int64(1), got)
		return result.Error()
	}))
}

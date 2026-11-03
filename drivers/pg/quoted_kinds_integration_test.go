//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestQuotedKindsCatalogAndCaches(t *testing.T) {
	driver := newV2IntegrationDriver(t, 3, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw := fmt.Sprintf("QuotedKind%d", time.Now().UnixNano())
	kinds := graph.Kinds{graph.StringKind(raw), graph.StringKind("`" + raw + "`")}
	queries := []string{
		"MATCH (n:" + raw + ") RETURN count(n)",
		"MATCH (n:" + cypher.EscapeKindName(raw) + ") RETURN count(n)",
		"MATCH (n:`" + raw + "`) RETURN count(n)",
		"MATCH (n:" + cypher.EscapeKindName(kinds[1].String()) + ") RETURN count(n)",
	}
	target, ok := driver.DefaultGraph()
	require.True(t, ok)
	check := func(query string, want int64) {
		t.Helper()
		for _, compileText := range []bool{false, true} {
			require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var result graph.Result
				if compileText {
					sql, params, err := driver.SchemaManager.compileText(ctx, query, nil, target.ID)
					if err != nil {
						return err
					}
					result = tx.Raw(sql, params)
				} else {
					result = tx.Query(query, nil)
				}
				defer result.Close()
				require.True(t, result.Next())
				var got int64
				require.NoError(t, result.Scan(&got))
				require.Equal(t, want, got, query)
				require.False(t, result.Next())
				return result.Error()
			}))
		}
	}
	for i := 0; i < 2; i++ {
		for _, query := range queries {
			check(query, 0)
		}
	}
	var count int64
	require.NoError(t, driver.pool.QueryRow(ctx, "SELECT count(*) FROM kind WHERE name = ANY($1::text[])", kinds.Strings()).Scan(&count))
	require.Zero(t, count, "reads must not register kinds")
	ids, err := driver.AssertKinds(ctx, kinds)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := driver.pool.Exec(context.Background(), "DELETE FROM kind WHERE id = ANY($1::int2[])", ids)
		require.NoError(t, err)
	})
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i, kind := range kinds {
			for j := 0; j <= i; j++ {
				if _, err := tx.CreateNode(graph.NewProperties(), kind); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	for i := 0; i < 3; i++ {
		for idx, query := range queries {
			want := int64(1)
			if idx == 3 {
				want = 2
			}
			check(query, want)
		}
	}
	var names []string
	rows, err := driver.pool.Query(ctx, "SELECT name FROM kind WHERE id = ANY($1::int2[]) ORDER BY id", ids)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, kinds.Strings(), names)
}

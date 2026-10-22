//go:build manual_integration

package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func requireNoMultipartTables(t *testing.T, tx graph.Transaction) {
	t.Helper()
	result := tx.Raw(`select count(*) from pg_class where relnamespace = pg_my_temp_schema() and starts_with(relname, '__dawgs_multipart_')`, nil)
	defer result.Close()
	require.True(t, result.Next(), "%v", result.Error())
	var count int64
	require.NoError(t, result.Scan(&count))
	require.Zero(t, count, "multipart boundary tables must be released")
	require.False(t, result.Next())
	require.NoError(t, result.Error())
}

func multipartPersistedNames(t *testing.T, database graph.Database, marker string) []string {
	t.Helper()
	var names []string
	require.NoError(t, database.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`MATCH (n:PGV2IntegrationNode) WHERE n.review_case = $marker RETURN n.name ORDER BY n.name`, map[string]any{"marker": marker})
		defer result.Close()
		for result.Next() {
			var name string
			require.NoError(t, result.Scan(&name))
			names = append(names, name)
		}
		return result.Error()
	}))
	return names
}

func TestPostgreSQLMultipartStagesCommitReadAfterWriteAndDistinctBindings(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	const marker = "multipart-distinct-bindings"
	require.NoError(t, driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`CREATE (a:PGV2IntegrationNode {name: 'first', review_case: $marker})
WITH a
CREATE (b:PGV2IntegrationNode {name: 'second', review_case: $marker})
WITH a, b
MATCH (fresh:PGV2IntegrationNode)
WHERE fresh.review_case = $marker AND fresh.name = 'first'
RETURN a.name, b.name, fresh.name`, map[string]any{"marker": marker})
		defer result.Close()
		require.IsType(t, &stagedQueryResult{}, result)
		require.True(t, result.Next(), "%v", result.Error())
		var first, second, observed string
		require.NoError(t, result.Scan(&first, &second, &observed))
		require.Equal(t, []string{"first", "second", "first"}, []string{first, second, observed})
		require.False(t, result.Next())
		require.NoError(t, result.Error())
		requireNoMultipartTables(t, tx)
		return result.Error()
	}))
	require.Equal(t, []string{"first", "second"}, multipartPersistedNames(t, driver, marker))
}

func TestPostgreSQLMultipartStagesCleanUpRepeatedResults(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	const marker = "multipart-result-cleanup"
	require.NoError(t, driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		for iteration := range 3 {
			result := tx.Query(`CREATE (n:PGV2IntegrationNode {name: 'created', review_case: $marker}) WITH n UNWIND [1, 2] AS x RETURN x`, map[string]any{"marker": marker})
			require.IsType(t, &stagedQueryResult{}, result)
			require.True(t, result.Next(), "%v", result.Error())
			if iteration%2 == 0 {
				result.Close()
			} else {
				require.True(t, result.Next())
				require.False(t, result.Next())
			}
			require.NoError(t, result.Error())
			result.Close()
			result.Close()
			require.False(t, result.Next())
			requireNoMultipartTables(t, tx)
		}
		return nil
	}))
	require.Equal(t, []string{"created", "created", "created"}, multipartPersistedNames(t, driver, marker))
}

func TestPostgreSQLMultipartStagesPreserveWritesAcrossEmptyBoundaries(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	const marker = "multipart-empty-boundary"
	require.NoError(t, driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`CREATE (a:PGV2IntegrationNode {name: 'first', review_case: $marker})
WITH a WHERE false
CREATE (b:PGV2IntegrationNode {name: 'second', review_case: $marker})
WITH a, b RETURN a`, map[string]any{"marker": marker})
		defer result.Close()
		require.IsType(t, &stagedQueryResult{}, result)
		require.False(t, result.Next())
		require.NoError(t, result.Error())
		requireNoMultipartTables(t, tx)
		return nil
	}))
	require.Equal(t, []string{"first"}, multipartPersistedNames(t, driver, marker))
}

func TestPostgreSQLMultipartStagesCaughtFailuresStillRollBack(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	for _, failure := range []string{"server", "scan"} {
		t.Run(failure, func(t *testing.T) {
			marker := "multipart-failure-" + failure
			var resultErr error
			err := driver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
				query := `CREATE (a:PGV2IntegrationNode {name: 'first', review_case: $marker}) WITH a CREATE (b:PGV2IntegrationNode {name: 'second', review_case: $marker}) WITH a, b RETURN a.name`
				if failure == "server" {
					query = `CREATE (a:PGV2IntegrationNode {name: 'first', review_case: $marker}) WITH a CREATE (b:PGV2IntegrationNode {name: 'second', review_case: $marker}) WITH a, b RETURN 1 / $zero`
				}
				result := tx.Query(query, map[string]any{"marker": marker, "zero": int64(0)})
				if failure == "scan" {
					require.True(t, result.Next(), "%v", result.Error())
					var unsupported struct{}
					resultErr = result.Scan(&unsupported)
					require.Error(t, resultErr)
				} else {
					for result.Next() {
					}
				}
				result.Close()
				if resultErr == nil {
					resultErr = result.Error()
				}
				require.Error(t, resultErr)
				// Even if a caller catches the result failure, Commit must not
				// publish mutations from the earlier successful stages.
				return nil
			})
			require.Error(t, err)
			require.ErrorIs(t, err, resultErr)
			require.Empty(t, multipartPersistedNames(t, driver, marker))
			require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
				requireNoMultipartTables(t, tx)
				return nil
			}))
		})
	}
}

func TestPostgreSQLMultipartStagesCancellationRollsBackAndPoolRecovers(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	const marker = "multipart-canceled"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(`CREATE (n:PGV2IntegrationNode {name: 'canceled', review_case: $marker}) WITH n RETURN n.name`, map[string]any{"marker": marker})
		require.True(t, result.Next(), "%v", result.Error())
		cancel()
		result.Close()
		return nil
	})
	require.Error(t, err)
	require.Empty(t, multipartPersistedNames(t, driver, marker))
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		requireNoMultipartTables(t, tx)
		result := tx.Query("RETURN 1", nil)
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		return result.Error()
	}))
}

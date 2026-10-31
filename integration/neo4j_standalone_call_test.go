//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Native standalone procedures bypass unsupported shared-parser rewrites.
func TestNeo4jNativeStandaloneCall(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true})
	for _, source := range []string{
		"CALL dbms.components() YIELD name",
		"CALL dbms.components YIELD name",
	} {
		t.Run(source, func(t *testing.T) {
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query(source, map[string]any{"name": "Neo4j Kernel"})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{"Neo4j Kernel"}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		result := tx.Query("CALL db.awaitIndexes($timeout)", map[string]any{"timeout": int64(1)})
		defer result.Close()
		require.False(t, result.Next())
		return result.Error()
	}))

}

//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Native in-query procedures retain their arguments and YIELD output.
func TestNeo4jNativeInQueryCall(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true})
	for _, source := range []string{
		"WITH $value AS x CALL dbms.components() YIELD name RETURN x, name",
		"WITH $value AS x CALL dbms.components() YIELD name AS component WHERE component = 'Neo4j Kernel' RETURN x, component",
	} {
		t.Run(source, func(t *testing.T) {
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query(source, map[string]any{"value": int64(7)})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{int64(7), "Neo4j Kernel"}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}

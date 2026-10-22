//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLMultipartCreatePersists(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("F7Created")}})
	var created graph.Node
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		result := tx.Query(`CREATE (n:F7Created {name: 'multipart-persisted'}) WITH n RETURN n`, nil)
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		require.NoError(t, result.Scan(&created))
		require.False(t, result.Next())
		return result.Error()
	}))
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		result := tx.Query(`MATCH (n:F7Created) WHERE n.name = 'multipart-persisted' RETURN n`, nil)
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		var stored graph.Node
		require.NoError(t, result.Scan(&stored))
		require.Equal(t, created.ID, stored.ID)
		require.Equal(t, created.Properties.Map, stored.Properties.Map)
		require.False(t, result.Next())
		return result.Error()
	}))
}

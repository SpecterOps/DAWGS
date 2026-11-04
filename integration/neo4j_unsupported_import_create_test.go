//go:build manual_integration

package integration

import (
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNeo4jNativeUnsupportedImportAndCreate(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true})
	for _, tc := range []struct{ source, code string }{
		{"CREATE UNIQUE (:Created {value: $value})", "Neo.ClientError.Statement.SyntaxError"},
		{"LOAD CSV FROM $url AS row RETURN count(*)", "Neo.ClientError.Statement.ExternalResourceFailed"},
		{"LOAD CSV WITH HEADERS FROM $url AS row CREATE (:Created {value: row.value})", "Neo.ClientError.Statement.ExternalResourceFailed"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query(tc.source, map[string]any{"value": int64(1), "url": "file:///dawgs013-file-that-does-not-exist.csv"})
				defer result.Close()
				for result.Next() {
				}
				return result.Error()
			})
			// The native server diagnoses unsupported legacy syntax or the missing
			// input file; the shared-parser rejection must not replace its query.
			var nativeError *neo4jdriver.Neo4jError
			require.ErrorAs(t, err, &nativeError)
			require.Equal(t, tc.code, nativeError.Code)
		})
	}
}

//go:build manual_integration

package integration

import (
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Exercise actual Go parameters independently of the corpus datetime decoder.
func TestNeo4jTemporalParameterOffsets(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true,
		SkipIfDriverMismatch: true, CleanupMode: CloseOnly})
	for _, input := range []string{"2026-01-02T04:04:05.123456789+01:00", "2026-01-01T21:34:05.123456789-05:30"} {
		t.Run(input, func(t *testing.T) {
			timestamp, err := time.Parse(time.RFC3339Nano, input)
			require.NoError(t, err)
			for _, query := range []string{"RETURN $value", "RETURN $nested[0].value", "UNWIND $typed AS value RETURN value"} {
				for _, raw := range []bool{false, true} {
					require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
						params := map[string]any{"value": timestamp, "nested": []any{map[string]any{"value": timestamp}}, "typed": []time.Time{timestamp}}
						var result graph.Result
						if raw {
							result = tx.Raw(query, params)
						} else {
							result = tx.Query(query, params)
						}
						defer result.Close()
						require.True(t, result.Next(), "%s raw=%v: %v", query, raw, result.Error())
						var actual time.Time
						require.NoError(t, result.Scan(&actual))
						require.True(t, actual.Equal(timestamp))
						_, expectedOffset := timestamp.Zone()
						_, actualOffset := actual.Zone()
						require.Equal(t, expectedOffset, actualOffset)
						require.Equal(t, timestamp.Nanosecond(), actual.Nanosecond())
						require.Equal(t, timestamp, params["value"])
						require.False(t, result.Next())
						return result.Error()
					}))
				}
			}
			require.NoError(t, session.WithRollback(t, func(tx graph.Transaction) error {
				node, err := tx.CreateNode(graph.AsProperties(map[string]any{"created_at": timestamp}))
				if err != nil {
					return err
				}
				created, err := node.Properties.Get("created_at").Time()
				require.NoError(t, err)
				require.True(t, created.Equal(timestamp))
				_, expectedOffset := timestamp.Zone()
				_, createdOffset := created.Zone()
				require.Equal(t, expectedOffset, createdOffset)

				updated := timestamp.Add(time.Hour)
				result := tx.Query("MATCH (n) WHERE id(n)=$id SET n.created_at=$value RETURN n.created_at", map[string]any{
					"id": int64(node.ID), "value": updated,
				})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				var actual time.Time
				require.NoError(t, result.Scan(&actual))
				require.True(t, actual.Equal(updated))
				_, actualOffset := actual.Zone()
				require.Equal(t, expectedOffset, actualOffset)
				require.Equal(t, updated.Nanosecond(), actual.Nanosecond())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}

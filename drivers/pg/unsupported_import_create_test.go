package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func unsupportedImportCreateCases() []struct{ source, message string } {
	return []struct{ source, message string }{
		{"MATCH (a), (b) CREATE UNIQUE (a)-[:RejectedEdge]->(b)", "CREATE UNIQUE is not supported"},
		{"CREATE UNIQUE (:PGV2IntegrationNode {name: 'rejected'})", "CREATE UNIQUE is not supported"},
		{"MATCH (a), (b) SET a.name = 'changed' CREATE UNIQUE (a)-[:RejectedEdge]->(b) RETURN a", "CREATE UNIQUE is not supported"},
		{"CREATE (:PGV2IntegrationNode {name: 'before'}) WITH 1 AS x CREATE UNIQUE (:PGV2IntegrationNode {name: 'after'})", "CREATE UNIQUE is not supported"},
		{"MATCH (a), (b) CREATE UNIQUE (a)-[:RejectedEdge]->(b) WITH a RETURN a", "CREATE UNIQUE is not supported"},
		{"LOAD CSV FROM 'file:///missing.csv' AS row MATCH (n) RETURN count(*) AS c", "LOAD CSV is not supported"},
		{"LOAD CSV WITH HEADERS FROM 'file:///missing.csv' AS row FIELDTERMINATOR ';' RETURN row", "LOAD CSV is not supported"},
		{"LOAD CSV FROM $url AS row WITH row RETURN row", "LOAD CSV is not supported"},
		{"LOAD CSV FROM $url AS row CREATE (:PGV2IntegrationNode {name: 'rejected'})", "LOAD CSV is not supported"},
		{"LOAD CSV FROM $url AS row MATCH (n) SET n.name = 'changed'", "LOAD CSV is not supported"},
		{"LOAD CSV FROM $url AS row MATCH (n) DETACH DELETE n", "LOAD CSV is not supported"},
		{"CREATE (:PGV2IntegrationNode {name: 'before'}) WITH 1 AS x LOAD CSV FROM $url AS row RETURN x", "LOAD CSV is not supported"},
	}
}

func TestCompileTextRejectsUnsupportedImportAndCreate(t *testing.T) {
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 4})
			for _, tc := range unsupportedImportCreateCases() {
				t.Run(tc.source, func(t *testing.T) {
					for range 2 {
						sql, _, err := manager.compileText(context.Background(), tc.source, map[string]any{"url": "file:///missing.csv"}, 7)
						require.ErrorContains(t, err, tc.message)
						require.NotContains(t, err.Error(), "panic")
						require.Empty(t, sql)
					}
				})
			}
		})
	}
}

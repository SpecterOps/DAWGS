//go:build manual_integration

package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Captures repeatable plans in rollback transactions. Sequences are deliberately
// not reset: nextval effects survive rollback, but fixtures and match keys do not.
func TestPostgreSQLMergePlans(t *testing.T) {
	db := setupIndexedPostgresDB(t, "integration_merge_plans", []graph.Index{{Field: "name", Type: graph.BTreeIndex}})
	require.NoError(t, db.db.WriteTransaction(db.ctx, func(tx graph.Transaction) error {
		for idx := 0; idx < 128; idx++ {
			if _, err := tx.CreateNode(graph.NewProperties().Set("name", fmt.Sprintf("seed-%d", idx)).Set("domain", 1).Set("payload", strings.Repeat("x", 4096)), db.propertyKind); err != nil {
				return err
			}
		}
		return nil
	}))
	analyzeIndexedNodePartition(t, db)
	conn, err := pgx.Connect(db.ctx, os.Getenv("CONNECTION_STRING"))
	require.NoError(t, err)
	defer conn.Close(db.ctx)
	names := make([]string, 64)
	for idx := range names {
		names[idx] = fmt.Sprintf("created-%d", idx)
	}
	for _, scenario := range []struct {
		name, query string
		params      map[string]any
		failing     bool
	}{
		{"fixed_created", `UNWIND $names AS name MERGE (n:IndexedNode {name:name}) RETURN n`, map[string]any{"names": names}, false},
		{"fixed_wide", `UNWIND $names AS name MERGE (n:IndexedNode {name:name}) ON CREATE SET n.payload=$payload RETURN n`, map[string]any{"names": names, "payload": strings.Repeat("x", 4096)}, false},
		{"fixed_changed_keys", `UNWIND $names AS name MERGE (n:IndexedNode {name:name}) ON CREATE SET n.name='away' RETURN n`, map[string]any{"names": names}, false},
		{"dynamic_matched", `MERGE (n:IndexedNode $props) RETURN n`, map[string]any{"props": map[string]any{"name": "seed-0"}}, false},
		{"bound_endpoints", `MATCH (a:IndexedNode {name:'seed-0'}),(b:IndexedNode {name:'seed-1'}) MERGE (a)-[r:EdgeKind1]->(b) RETURN r`, nil, false},
		{"repeated_targets", `UNWIND [1,2] AS value MERGE (n:IndexedNode {name:'seed-0'}) SET n.score=value RETURN n`, nil, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			sql, params := translateIndexedCypher(t, db, scenario.query, scenario.params)
			var plan any
			command := "explain (analyze, buffers, wal, verbose, format json) "
			if scenario.failing {
				command = "explain (verbose, format json) "
			}
			tx, err := conn.Begin(db.ctx)
			require.NoError(t, err)
			var raw string
			err = tx.QueryRow(db.ctx, command+sql, pgx.NamedArgs(params)).Scan(&raw)
			require.NoError(t, tx.Rollback(db.ctx))
			require.NoError(t, err)
			var executionText string
			if parseErr := json.Unmarshal([]byte(raw), &plan); parseErr != nil {
				// PostgreSQL 18.6 places MERGE tuple instrumentation inside the
				// Target Tables array, producing invalid JSON for partitions.
				// Keep the raw output and capture independent valid representations.
				require.True(t, regexp.MustCompile(`(?s)"Target Tables": \[.*?\},\s*"Tuples Inserted":`).MatchString(raw), "unexpected invalid server plan: %v", parseErr)
				tx, err = conn.Begin(db.ctx)
				require.NoError(t, err)
				var planned string
				err = tx.QueryRow(db.ctx, "explain (verbose, format json) "+sql, pgx.NamedArgs(params)).Scan(&planned)
				require.NoError(t, tx.Rollback(db.ctx))
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal([]byte(planned), &plan))
				tx, err = conn.Begin(db.ctx)
				require.NoError(t, err)
				rows, err := tx.Query(db.ctx, "explain (analyze, buffers, wal, verbose, format text) "+sql, pgx.NamedArgs(params))
				require.NoError(t, err)
				var lines []string
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
					lines = append(lines, line)
				}
				require.NoError(t, rows.Err())
				rows.Close()
				require.NoError(t, tx.Rollback(db.ctx))
				executionText = strings.Join(lines, "\n")
			}
			require.NotNil(t, plan)
			if scenario.failing {
				err = db.db.WriteTransaction(db.ctx, func(tx graph.Transaction) error { return drainMerge(tx, scenario.query, scenario.params) })
				require.ErrorContains(t, err, "repeated writes to the same target")
			}
			if directory := os.Getenv("MERGE_PLAN_DIR"); directory != "" {
				require.NoError(t, os.MkdirAll(directory, 0755))
				require.NoError(t, os.WriteFile(filepath.Join(directory, scenario.name+".sql"), []byte(sql), 0644))
				encoded, err := json.MarshalIndent(map[string]any{"cypher": scenario.query, "parameters": params, "plan": plan, "raw_execution_json": raw, "execution_text": executionText}, "", "  ")
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(directory, scenario.name+".json"), encoded, 0644))
			}
		})
	}
}

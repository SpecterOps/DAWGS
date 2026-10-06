//go:build manual_integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

func BenchmarkPostgreSQLMerge(b *testing.B) {
	connection := os.Getenv(ConnectionStringEnv)
	backend, err := DriverFromConnectionString(connection)
	if err != nil || backend != pg.DriverName {
		b.Skip("PostgreSQL CONNECTION_STRING is required")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(connection)
	require.NoError(b, err)
	pool, err := pg.NewPool(cfg)
	require.NoError(b, err)
	db := pg.NewDriver(size.Size(0), pool)
	kind := graph.StringKind("MergeBenchmarkNode")
	require.NoError(b, db.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: "merge_benchmark", Nodes: graph.Kinds{kind}, NodeIndexes: []graph.Index{{Field: "name", Type: graph.BTreeIndex}}}}))
	b.Cleanup(func() {
		_ = db.WriteTransaction(ctx, func(tx graph.Transaction) error { return tx.Nodes().Delete() })
		_ = db.Close(ctx)
	})
	require.NoError(b, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Nodes().Delete(); err != nil {
			return err
		}
		for _, name := range []string{"target", "other"} {
			if _, err := tx.CreateNode(graph.NewProperties().Set("name", name), kind); err != nil {
				return err
			}
		}
		return nil
	}))
	for _, scenario := range []struct{ name, query string }{
		{"indexed unchanged", `MERGE (n:MergeBenchmarkNode {name:$name}) RETURN n`},
		{"repeated unchanged inputs", `UNWIND [1,2,3,4,5,6,7,8] AS input MERGE (n:MergeBenchmarkNode {name:$name}) RETURN n`},
		{"indexed on match update", `MERGE (n:MergeBenchmarkNode {name:$name}) ON MATCH SET n.score=1 RETURN n`},
		{"alternating on match update", `MERGE (n:MergeBenchmarkNode {name:$name}) ON MATCH SET n.score=$score RETURN n`},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			parameters := map[string]any{"name": "target", "score": 1}
			require.NoError(b, db.WriteTransaction(ctx, func(tx graph.Transaction) error { return drainMerge(tx, scenario.query, parameters) }))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				parameters["score"] = 1 + i%2
				require.NoError(b, db.WriteTransaction(ctx, func(tx graph.Transaction) error { return drainMerge(tx, scenario.query, parameters) }))
			}
		})
	}
	benchmarkMergeMatrix(b, ctx, db, kind)
}

// Each timed transaction rolls back, keeping creation/mixed workloads stable.
// Parameters and seeds are prepared outside timing. Result drain and rollback
// are included; compilation is warmed before ResetTimer.
func benchmarkMergeMatrix(b *testing.B, ctx context.Context, db graph.Database, kind graph.Kind) {
	rollback := errors.New("benchmark rollback")
	for _, scenario := range []struct {
		batch, payload, assignments int
		mix, shape                  string
	}{
		{1, 0, 0, "created", "fixed"}, {8, 0, 0, "created", "fixed"}, {64, 0, 0, "created", "fixed"}, {512, 0, 0, "created", "fixed"}, {4096, 0, 0, "created", "fixed"},
		{64, 256, 0, "created", "fixed"}, {64, 4096, 0, "created", "fixed"}, {64, 65536, 0, "created", "fixed"},
		{64, 4096, 1, "matched", "fixed"}, {64, 4096, 8, "matched", "fixed"}, {64, 4096, 32, "matched", "fixed"},
		{64, 0, 0, "matched", "fixed"}, {64, 4096, 8, "mixed", "composite"},
		{64, 4096, 0, "matched", "dynamic"}, {64, 0, 1, "conflict", "fixed"},
	} {
		name := fmt.Sprintf("matrix/%s/%s/batch%d/payload%d/set%d", scenario.mix, scenario.shape, scenario.batch, scenario.payload, scenario.assignments)
		b.Run(name, func(b *testing.B) {
			values := make([]string, scenario.batch)
			payload := strings.Repeat("x", scenario.payload)
			require.NoError(b, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
				if err := tx.Nodes().Delete(); err != nil {
					return err
				}
				for idx := range values {
					values[idx] = fmt.Sprintf("matrix-%d", idx)
					seeded := scenario.mix == "matched" || scenario.mix == "conflict" || (scenario.mix == "mixed" && idx%2 == 0)
					if seeded {
						if _, err := tx.CreateNode(graph.NewProperties().Set("name", values[idx]).Set("domain", 1).Set("payload", payload), kind); err != nil {
							return err
						}
					}
				}
				return nil
			}))
			match := "{name:name}"
			if scenario.shape == "composite" {
				match = "{name:name,domain:1}"
			}
			if scenario.shape == "dynamic" {
				match = "$props"
			}
			if scenario.mix == "conflict" {
				match = "{name:'matrix-0'}"
			}
			query := "UNWIND $names AS name MERGE (n:MergeBenchmarkNode " + match + ")"
			assignments := []string{}
			if scenario.mix == "created" || scenario.mix == "mixed" {
				assignments = append(assignments, "n.payload=$payload")
			}
			for idx := 0; idx < scenario.assignments; idx++ {
				assignments = append(assignments, fmt.Sprintf("n.field%d=$score", idx))
			}
			if len(assignments) > 0 {
				query += " SET " + strings.Join(assignments, ",")
			}
			query += " RETURN n"
			params := map[string]any{"names": values, "payload": payload, "score": 1, "props": map[string]any{"domain": 1}}
			// Dynamic workloads use a single match input to preserve the contract; the
			// fixture size remains an independent lookup cardinality dimension.
			if scenario.shape == "dynamic" {
				params["names"] = []string{"input"}
			}
			execute := func() error {
				return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
					if err := drainMerge(tx, query, params); err != nil {
						return err
					}
					return rollback
				})
			}
			check := func(err error) {
				if scenario.mix == "conflict" {
					require.ErrorContains(b, err, "repeated writes to the same target")
				} else {
					require.ErrorIs(b, err, rollback)
				}
			}
			check(execute())
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				params["score"] = 1 + idx%2
				check(execute())
			}
			b.StopTimer()
		})
	}
}

// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestPostgreSQLOptimizeStorageDropsInvalidReindexArtifacts builds a real INVALID btree index named like an
// interrupted REINDEX CONCURRENTLY artifact on a node partition and verifies that OptimizeStorage drops it while
// leaving a valid lookalike and an invalid index outside the artifact naming pattern untouched.
func TestPostgreSQLOptimizeStorageDropsInvalidReindexArtifacts(t *testing.T) {
	nodeKind := graph.StringKind("OptimizeStorageNode")

	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		ExtraNodeKinds:       graph.Kinds{nodeKind},
	})

	var (
		ctx    = session.Ctx
		pool   = session.PGPool
		nodeID graph.ID
	)

	require.NoError(t, session.DB.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for range 2 {
			node, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			nodeID = node.ID
		}
		return nil
	}))

	var schema, partition string
	require.NoError(t, pool.QueryRow(ctx, `select n.nspname, c.relname
from node x
         join pg_class c on c.oid = x.tableoid
         join pg_namespace n on n.oid = c.relnamespace
where x.id = $1`, nodeID).Scan(&schema, &partition))

	var (
		table          = pgx.Identifier{schema, partition}.Sanitize()
		invalidCCNew   = partition + "_optimize_test_ccnew"
		validCCNew     = partition + "_optimize_test_valid_ccnew"
		invalidUnnamed = partition + "_optimize_test_invalid"
	)

	t.Cleanup(func() {
		for _, name := range []string{invalidCCNew, validCCNew, invalidUnnamed} {
			execSimple(t, context.Background(), pool, "DROP INDEX CONCURRENTLY IF EXISTS "+pgx.Identifier{schema, name}.Sanitize())
		}
	})

	// A unique index over graph_id fails on the two fixture rows and leaves the index behind as INVALID.
	for _, name := range []string{invalidCCNew, invalidUnnamed} {
		_, err := pool.Exec(ctx, "CREATE UNIQUE INDEX CONCURRENTLY "+pgx.Identifier{name}.Sanitize()+" ON "+table+" (graph_id)", pgx.QueryExecModeSimpleProtocol)
		require.Error(t, err)
	}
	execSimple(t, ctx, pool, "CREATE INDEX CONCURRENTLY "+pgx.Identifier{validCCNew}.Sanitize()+" ON "+table+" (graph_id)")

	require.Equal(t, map[string]bool{invalidCCNew: false, validCCNew: true, invalidUnnamed: false},
		indexValidity(t, ctx, pool, schema, invalidCCNew, validCCNew, invalidUnnamed))

	require.NoError(t, session.DB.OptimizeStorage(ctx))

	require.Equal(t, map[string]bool{validCCNew: true, invalidUnnamed: false},
		indexValidity(t, ctx, pool, schema, invalidCCNew, validCCNew, invalidUnnamed))
}

// TestPostgreSQLOptimizeStorageRebuildsBtreeIndexes verifies that OptimizeStorage rebuilds every valid btree index on
// node and edge partitions, observed as a new relation file node, and leaves GIN indexes untouched.
func TestPostgreSQLOptimizeStorageRebuildsBtreeIndexes(t *testing.T) {
	var (
		nodeKind = graph.StringKind("OptimizeStorageReindexNode")
		edgeKind = graph.StringKind("OptimizeStorageReindexEdge")
	)

	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		ExtraNodeKinds:       graph.Kinds{nodeKind},
		ExtraEdgeKinds:       graph.Kinds{edgeKind},
	})

	ctx := session.Ctx

	require.NoError(t, session.DB.WriteTransaction(ctx, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}

		end, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}

		_, err = tx.CreateRelationshipByIDs(start.ID, end.ID, edgeKind, graph.NewProperties())
		return err
	}))

	before := graphPartitionIndexFileNodes(t, ctx, session.PGPool)
	require.NoError(t, session.DB.OptimizeStorage(ctx))
	after := graphPartitionIndexFileNodes(t, ctx, session.PGPool)

	var btreeCount, ginCount int
	for name, previous := range before {
		current, exists := after[name]
		require.True(t, exists, "index %s missing after OptimizeStorage", name)
		require.Equal(t, previous.method, current.method)

		switch previous.method {
		case "btree":
			btreeCount++
			require.NotEqual(t, previous.fileNode, current.fileNode, "btree index %s was not rebuilt", name)
		case "gin":
			ginCount++
			require.Equal(t, previous.fileNode, current.fileNode, "gin index %s was rebuilt", name)
		}
	}

	require.Positive(t, btreeCount)
	require.Positive(t, ginCount)
}

type indexFileNode struct {
	method   string
	fileNode uint32
}

// graphPartitionIndexFileNodes returns the access method and relation file node of every valid index on node and edge
// partitions, keyed by schema-qualified index name.
func graphPartitionIndexFileNodes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]indexFileNode {
	t.Helper()

	rows, err := pool.Query(ctx, `select n.nspname || '.' || i.relname, a.amname, pg_relation_filenode(i.oid)
from pg_inherits inh
         join pg_index x on x.indrelid = inh.inhrelid
         join pg_class i on i.oid = x.indexrelid
         join pg_namespace n on n.oid = i.relnamespace
         join pg_am a on a.oid = i.relam
where inh.inhparent in ('node'::regclass, 'edge'::regclass)
  and x.indisvalid`)
	require.NoError(t, err)
	defer rows.Close()

	fileNodes := map[string]indexFileNode{}
	for rows.Next() {
		var (
			name  string
			entry indexFileNode
		)
		require.NoError(t, rows.Scan(&name, &entry.method, &entry.fileNode))
		fileNodes[name] = entry
	}
	require.NoError(t, rows.Err())

	return fileNodes
}

func execSimple(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stmt string) {
	t.Helper()

	_, err := pool.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol)
	require.NoError(t, err)
}

// indexValidity returns indisvalid for each named index that exists in schema.
func indexValidity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string, names ...string) map[string]bool {
	t.Helper()

	rows, err := pool.Query(ctx, `select i.relname, x.indisvalid
from pg_index x
         join pg_class i on i.oid = x.indexrelid
         join pg_namespace n on n.oid = i.relnamespace
where n.nspname = $1
  and i.relname = any($2)`, schema, names)
	require.NoError(t, err)
	defer rows.Close()

	validity := map[string]bool{}
	for rows.Next() {
		var (
			name  string
			valid bool
		)
		require.NoError(t, rows.Scan(&name, &valid))
		validity[name] = valid
	}
	require.NoError(t, rows.Err())

	return validity
}

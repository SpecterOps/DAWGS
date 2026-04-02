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
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

func TestLocalDataset(t *testing.T) {
	if *localDatasetFlag == "" {
		t.Skip("no -local-dataset flag provided")
	}

	dataset := *localDatasetFlag
	loadStart := time.Now()

	db, ctx := SetupDB(t, dataset)
	idMap := LoadDataset(t, db, ctx, dataset)

	t.Logf("load: %d nodes mapped in %s", len(idMap), time.Since(loadStart))

	t.Run("CountNodes", func(t *testing.T) {
		start := time.Now()

		var count int64
		err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			var countErr error
			count, countErr = tx.Nodes().Count()
			return countErr
		})
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if int(count) != len(idMap) {
			t.Fatalf("node count: got %d, want %d", count, len(idMap))
		}

		t.Logf("count(*) nodes = %d [%s]", count, time.Since(start))
	})

	t.Run("CountEdges", func(t *testing.T) {
		start := time.Now()

		var count int64
		err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			var countErr error
			count, countErr = tx.Relationships().Count()
			return countErr
		})
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		t.Logf("count(*) edges = %d [%s]", count, time.Since(start))
	})

	t.Run("FilterNodesByKind", func(t *testing.T) {
		for _, kind := range []string{"User", "Group", "Computer"} {
			t.Run(kind, func(t *testing.T) {
				start := time.Now()

				cypher := fmt.Sprintf("MATCH (n:%s) RETURN n", kind)
				got := QueryNodeIDs(t, ctx, db, cypher, idMap)

				t.Logf("MATCH (n:%s) = %d nodes [%s]", kind, len(got), time.Since(start))
			})
		}
	})

	t.Run("TraversalDepth", func(t *testing.T) {
		startID := pickNodeByKind(t, ctx, db, idMap, "User")
		if startID == "" {
			t.Skip("no User node found")
		}

		for _, depth := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
				start := time.Now()

				cypher := fmt.Sprintf(
					"MATCH (s)-[*1..%d]->(e) WHERE id(s) = %d RETURN e",
					depth, idMap[startID],
				)

				got := QueryNodeIDs(t, ctx, db, cypher, idMap)
				t.Logf("node %s depth %d: %d reachable [%s]", startID, depth, len(got), time.Since(start))
			})
		}
	})

	t.Run("ShortestPath", func(t *testing.T) {
		startID := pickNodeByKind(t, ctx, db, idMap, "User")
		endID := pickNodeByKind(t, ctx, db, idMap, "Domain")
		if startID == "" || endID == "" {
			t.Skip("could not find User and Domain nodes")
		}

		start := time.Now()

		cypher := fmt.Sprintf(
			"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
			idMap[startID], idMap[endID],
		)

		paths := QueryPaths(t, ctx, db, cypher)
		t.Logf("shortest paths %s -> %s: %d paths [%s]", startID, endID, len(paths), time.Since(start))
	})

	t.Run("EdgeTraversalByKind", func(t *testing.T) {
		startID := pickNodeByKind(t, ctx, db, idMap, "User")
		if startID == "" {
			t.Skip("no User node found")
		}

		for _, edgeKind := range []string{"MemberOf", "GenericAll", "HasSession"} {
			t.Run(edgeKind, func(t *testing.T) {
				start := time.Now()

				cypher := fmt.Sprintf(
					"MATCH (s)-[:%s*1..]->(e) WHERE id(s) = %d RETURN e",
					edgeKind, idMap[startID],
				)

				got := QueryNodeIDs(t, ctx, db, cypher, idMap)
				t.Logf("node %s via %s: %d reachable [%s]", startID, edgeKind, len(got), time.Since(start))
			})
		}
	})
}

// pickNodeByKind finds the first node in the database with the given kind and returns its fixture ID.
func pickNodeByKind(t *testing.T, ctx context.Context, db graph.Database, idMap opengraph.IDMap, kind string) string {
	t.Helper()

	rev := make(map[graph.ID]string, len(idMap))
	for fid, dbID := range idMap {
		rev[dbID] = fid
	}

	cypher := fmt.Sprintf("MATCH (n:%s) RETURN n LIMIT 1", kind)

	var nodeID string
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()

		if result.Next() {
			var n graph.Node
			if err := result.Scan(&n); err != nil {
				return err
			}

			nodeID = rev[n.ID]
		}

		return result.Error()
	})
	if err != nil {
		t.Fatalf("pickNodeByKind query failed: %v", err)
	}

	return nodeID
}

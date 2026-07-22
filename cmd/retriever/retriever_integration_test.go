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

//go:build manual_integration || integration

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/retriever"
)

func TestDumpLoadRoundTrip(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING not set")
	}
	driverName, err := driverFromConnectionString(connection)
	if err != nil {
		t.Fatalf("infer driver: %v", err)
	}
	ctx := context.Background()
	db, _, err := openDatabase(ctx, databaseConfig{
		Connection: connection,
	})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close(ctx)

	graphName := fmt.Sprintf("retriever_it_%d", time.Now().UTC().UnixNano())
	userKind := graph.StringKind("RetrieverUser")
	systemKind := graph.StringKind("RetrieverSystem")
	adminKind := graph.StringKind("RetrieverAdminTo")
	graphSchema := graph.Graph{
		Name:  graphName,
		Nodes: graph.Kinds{userKind, systemKind},
		Edges: graph.Kinds{adminKind},
	}
	if err := db.AssertSchema(ctx, graph.Schema{
		Graphs:       []graph.Graph{graphSchema},
		DefaultGraph: graphSchema,
	}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	clearGraph := func() error {
		return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.WithGraph(graph.Graph{
				Name: graphName,
			}).Nodes().Delete()
		})
	}
	if err := clearGraph(); err != nil {
		t.Fatalf("clear graph before seed: %v", err)
	}
	t.Cleanup(func() {
		_ = clearGraph()
	})

	const (
		nodeCount = 7
		edgeCount = 5
		batchSize = 2
		shardSize = 3
	)
	var seededNodeIDs, seededEdgeIDs []graph.ID
	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{
			Name: graphName,
		})

		nodes := make([]*graph.Node, 0, nodeCount)
		for index := range nodeCount {
			kind := userKind
			if index%2 == 1 {
				kind = systemKind
			}

			node, err := tx.CreateNode(graph.AsProperties(map[string]any{
				"name": fmt.Sprintf("node-%d", index),
				"role": fmt.Sprintf("role-%d", index%3),
			}), kind)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			seededNodeIDs = append(seededNodeIDs, node.ID)
		}

		for index := range edgeCount {
			relationship, err := tx.CreateRelationshipByIDs(nodes[index].ID, nodes[index+1].ID, adminKind, graph.AsProperties(map[string]any{
				"route": fmt.Sprintf("route-%d", index),
			}))
			if err != nil {
				return err
			}
			seededEdgeIDs = append(seededEdgeIDs, relationship.ID)
		}

		return nil
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, graph.Graph{
		Name: graphName,
	})
	if err != nil {
		t.Fatalf("count graph entities: %v", err)
	}
	if entitySnapshot.NodeCount != nodeCount || entitySnapshot.EdgeCount != edgeCount {
		t.Fatalf("unexpected seeded graph counts: nodes=%d edges=%d", entitySnapshot.NodeCount, entitySnapshot.EdgeCount)
	}

	assertBoundedRetrieverScans(t, ctx, db, graph.Graph{Name: graphName}, entitySnapshot, batchSize)
	assertSkipAndLimit(t, ctx, db, graph.Graph{Name: graphName}, seededNodeIDs, seededEdgeIDs)

	dumpDir := t.TempDir()
	dumpResult, err := retriever.Dump(ctx, db, driverName, []retriever.GraphTarget{{
		Name: graphName,
	}}, retriever.DumpOptions{
		OutputDir:   dumpDir,
		Scrub:       retriever.ScrubNone,
		Compression: retriever.CompressionGzip,
		ZstdLevel:   retriever.DefaultZstdLevel,
		ShardSize:   shardSize,
		BatchSize:   batchSize,
	})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	if dumpResult.NodeCount != nodeCount || dumpResult.EdgeCount != edgeCount {
		t.Fatalf("unexpected dump counts: nodes=%d edges=%d", dumpResult.NodeCount, dumpResult.EdgeCount)
	}
	if got := len(dumpResult.Manifest.Graphs); got != 1 {
		t.Fatalf("dump manifest graph count = %d", got)
	}
	graphEntry := dumpResult.Manifest.Graphs[0]
	if graphEntry.NodeCount != nodeCount || graphEntry.EdgeCount != edgeCount {
		t.Fatalf("unexpected manifest graph counts: nodes=%d edges=%d", graphEntry.NodeCount, graphEntry.EdgeCount)
	}
	var nodeFiles, edgeFiles int
	for _, fileEntry := range graphEntry.Files {
		switch fileEntry.Phase {
		case retriever.PhaseNodes:
			nodeFiles++
		case retriever.PhaseEdges:
			edgeFiles++
		}
		if fileEntry.Count > shardSize {
			t.Fatalf("fragment %s count %d exceeds shard size %d", fileEntry.Path, fileEntry.Count, shardSize)
		}
	}
	if nodeFiles != 3 || edgeFiles != 2 {
		t.Fatalf("unexpected manifest shards: node files=%d edge files=%d", nodeFiles, edgeFiles)
	}
	if dumpResult.Manifest.Metrics == nil {
		t.Fatalf("dump manifest is missing metrics")
	}
	if got := len(dumpResult.Manifest.Metrics.Graphs); got != 1 {
		t.Fatalf("dump metrics graph count = %d", got)
	}
	if dumpResult.Manifest.Metrics.Graphs[0].NodeCount != nodeCount || dumpResult.Manifest.Metrics.Graphs[0].EdgeCount != edgeCount {
		t.Fatalf("unexpected dump metrics counts: %+v", dumpResult.Manifest.Metrics.Graphs[0])
	}

	if _, err := retriever.Load(ctx, db, driverName, retriever.LoadOptions{
		InputDir:      dumpDir,
		BatchSize:     batchSize,
		VerifyMetrics: true,
	}); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Fatalf("expected non-empty target load error, got %v", err)
	}

	if err := clearGraph(); err != nil {
		t.Fatalf("clear graph before load: %v", err)
	}

	loadResult, err := retriever.Load(ctx, db, driverName, retriever.LoadOptions{
		InputDir:      dumpDir,
		BatchSize:     batchSize,
		VerifyMetrics: true,
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loadResult.NodeCount != nodeCount || loadResult.EdgeCount != edgeCount {
		t.Fatalf("unexpected load counts: nodes=%d edges=%d", loadResult.NodeCount, loadResult.EdgeCount)
	}

	var loadedNodes []*graph.Node
	var loadedEdges []*graph.Relationship
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{Name: graphName})
		var err error
		if loadedNodes, err = ops.FetchNodes(tx.Nodes()); err != nil {
			return err
		}
		loadedEdges, err = ops.FetchRelationships(tx.Relationships())
		return err
	}); err != nil {
		t.Fatalf("read restored topology: %v", err)
	}
	if len(loadedNodes) != nodeCount || len(loadedEdges) != edgeCount {
		t.Fatalf("unexpected restored topology counts: nodes=%d edges=%d", len(loadedNodes), len(loadedEdges))
	}

	nodeNames := make(map[graph.ID]string, len(loadedNodes))
	expectedRoles := make(map[string]string, nodeCount)
	expectedKinds := make(map[string]string, nodeCount)
	for index := range nodeCount {
		name := fmt.Sprintf("node-%d", index)
		expectedRoles[name] = fmt.Sprintf("role-%d", index%3)
		expectedKinds[name] = userKind.String()
		if index%2 == 1 {
			expectedKinds[name] = systemKind.String()
		}
	}
	for _, node := range loadedNodes {
		if node.Properties == nil {
			t.Fatalf("restored node %d is missing properties", node.ID)
		}
		name := fmt.Sprint(node.Properties.Get("name").Any())
		role := fmt.Sprint(node.Properties.Get("role").Any())
		nodeNames[node.ID] = name
		if expectedRole, ok := expectedRoles[name]; !ok || role != expectedRole {
			t.Fatalf("unexpected restored node properties: %+v", node.Properties.MapOrEmpty())
		}
		if len(node.Kinds) != 1 || node.Kinds[0].String() != expectedKinds[name] {
			t.Fatalf("unexpected restored node kinds for %s: %v", name, node.Kinds.Strings())
		}
	}

	restoredRoutes := map[string]string{}
	for _, relationship := range loadedEdges {
		startName, startOK := nodeNames[relationship.StartID]
		endName, endOK := nodeNames[relationship.EndID]
		if !startOK || !endOK {
			t.Fatalf("restored relationship has unresolved endpoint IDs: %+v", relationship)
		}
		if relationship.Kind == nil || relationship.Kind.String() != adminKind.String() {
			t.Fatalf("unexpected restored relationship kind: %+v", relationship.Kind)
		}
		if relationship.Properties == nil {
			t.Fatalf("restored relationship is missing properties: %+v", relationship)
		}
		restoredRoutes[startName+"->"+endName] = fmt.Sprint(relationship.Properties.Get("route").Any())
	}
	for index := range edgeCount {
		path := fmt.Sprintf("node-%d->node-%d", index, index+1)
		if route := restoredRoutes[path]; route != fmt.Sprintf("route-%d", index) {
			t.Fatalf("restored route %s = %q", path, route)
		}
	}

	verifyResult, err := retriever.Verify(ctx, db, driverName, retriever.VerifyOptions{
		InputDir:  dumpDir,
		BatchSize: batchSize,
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verifyResult.NodeCount != nodeCount || verifyResult.EdgeCount != edgeCount {
		t.Fatalf("unexpected verify counts: nodes=%d edges=%d", verifyResult.NodeCount, verifyResult.EdgeCount)
	}

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{
			Name: graphName,
		})
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "extra"}), userKind)
		return err
	}); err != nil {
		t.Fatalf("mutate loaded graph: %v", err)
	}

	_, err = retriever.Verify(ctx, db, driverName, retriever.VerifyOptions{
		InputDir:  dumpDir,
		BatchSize: batchSize,
	})
	var mismatch retriever.MetricsMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected metrics mismatch error, got %v", err)
	}
	if !strings.Contains(err.Error(), "node_count") {
		t.Fatalf("expected mismatch to include node_count, got %v", err)
	}
}

func assertBoundedRetrieverScans(t *testing.T, ctx context.Context, db graph.Database, targetGraph graph.Graph, snapshot graphEntitySnapshot, batchSize int) {
	t.Helper()

	var nodeIDs []graph.ID
	processed, err := retriever.ScanDatabaseNodes(ctx, db, targetGraph, snapshot.NodeCount, batchSize, func(node *graph.Node) error {
		nodeIDs = append(nodeIDs, node.ID)
		return nil
	}, func(event retriever.ScanBatchEvent) error {
		if event.Count > batchSize {
			return fmt.Errorf("node cursor callback count %d exceeds batch size %d", event.Count, batchSize)
		}
		return nil
	})
	if err != nil || processed != snapshot.NodeCount {
		t.Fatalf("bounded node scan processed=%d err=%v", processed, err)
	}
	assertStrictIDs(t, "node", nodeIDs)

	var edgeIDs []graph.ID
	processed, err = retriever.ScanDatabaseRelationships(ctx, db, targetGraph, snapshot.EdgeCount, batchSize, func(relationship *graph.Relationship) error {
		edgeIDs = append(edgeIDs, relationship.ID)
		return nil
	}, func(event retriever.ScanBatchEvent) error {
		if event.Count > batchSize {
			return fmt.Errorf("relationship cursor callback count %d exceeds batch size %d", event.Count, batchSize)
		}
		return nil
	})
	if err != nil || processed != snapshot.EdgeCount {
		t.Fatalf("bounded relationship scan processed=%d err=%v", processed, err)
	}
	assertStrictIDs(t, "relationship", edgeIDs)
}

func assertStrictIDs(t *testing.T, entityName string, ids []graph.ID) {
	t.Helper()
	for index := 1; index < len(ids); index++ {
		if ids[index] <= ids[index-1] {
			t.Fatalf("%s IDs are not strictly increasing: %v", entityName, ids)
		}
	}
}

func assertSkipAndLimit(t *testing.T, ctx context.Context, db graph.Database, targetGraph graph.Graph, nodeIDs, edgeIDs []graph.ID) {
	t.Helper()

	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		var actualNodeIDs []graph.ID
		if err := tx.Nodes().OrderBy(query.NodeID()).Offset(1).Limit(2).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				actualNodeIDs = append(actualNodeIDs, id)
			}
			return cursor.Error()
		}); err != nil {
			return err
		}
		if !reflect.DeepEqual(actualNodeIDs, nodeIDs[1:3]) {
			return fmt.Errorf("node skip/limit IDs = %v, want %v", actualNodeIDs, nodeIDs[1:3])
		}

		var actualEdgeIDs []graph.ID
		if err := tx.Relationships().OrderBy(query.RelationshipID()).Offset(1).Limit(2).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				actualEdgeIDs = append(actualEdgeIDs, id)
			}
			return cursor.Error()
		}); err != nil {
			return err
		}
		if !reflect.DeepEqual(actualEdgeIDs, edgeIDs[1:3]) {
			return fmt.Errorf("relationship skip/limit IDs = %v, want %v", actualEdgeIDs, edgeIDs[1:3])
		}

		return nil
	}); err != nil {
		t.Fatalf("assert skip and limit: %v", err)
	}
}

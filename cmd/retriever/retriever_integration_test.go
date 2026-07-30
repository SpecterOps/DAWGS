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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/ret"
	"github.com/specterops/dawgs/ret/archive"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

func TestRetFacadeCollectionMatrix(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		jsonl    jsonl.Config
		parquet  parquet.Config
		loadable bool
	}{
		{
			name:     "jsonl",
			jsonl:    jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd},
			loadable: true,
		},
		{
			name:     "parquet",
			parquet:  parquet.Config{Enabled: true},
			loadable: false,
		},
		{
			name:     "dual",
			jsonl:    jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd},
			parquet:  parquet.Config{Enabled: true},
			loadable: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newRetIntegrationHarness(t)
			fixture := harness.seedStandardGraph(t)
			config := harness.dumpConfig(fixture.name, testCase.jsonl, testCase.parquet)

			dumpResult, err := ret.Dump(harness.ctx, harness.database, config)
			if err != nil {
				t.Fatalf("dump: %v", err)
			}
			assertOperationCounts(t, dumpResult.GraphCount, dumpResult.NodeCount, dumpResult.RelationshipCount, 1, 4, 3)

			verifyResult, err := ret.VerifyCollection(harness.ctx, ret.VerifyCollectionConfig{Directory: config.Directory})
			if err != nil {
				t.Fatalf("verify collection: %v", err)
			}
			assertOperationCounts(t, verifyResult.GraphCount, verifyResult.NodeCount, verifyResult.RelationshipCount, 1, 4, 3)
			assertConcreteOutputs(t, config.Directory, testCase.jsonl.Enabled, testCase.parquet.Enabled)
			assertArchiveRoundTrip(t, config.Directory)
			if testCase.jsonl.Enabled && testCase.parquet.Enabled {
				damageFirstParquetArtifact(t, config.Directory)
			}

			before := harness.snapshot(t, fixture.name)
			loadResult, err := ret.Load(harness.ctx, harness.database, ret.LoadConfig{
				Directory: config.Directory,
				BatchSize: 2,
			})
			if !testCase.loadable {
				if !errors.Is(err, ret.ErrCollectionNotLoadable) {
					t.Fatalf("load parquet-only collection error = %v, want %v", err, ret.ErrCollectionNotLoadable)
				}
				if after := harness.snapshot(t, fixture.name); after != before {
					t.Fatalf("parquet-only load mutated target: before=%+v after=%+v", before, after)
				}
				return
			}
			if !errors.Is(err, ret.ErrNonEmptyTarget) {
				t.Fatalf("load nonempty target error = %v, want %v", err, ret.ErrNonEmptyTarget)
			}
			if after := harness.snapshot(t, fixture.name); after != before {
				t.Fatalf("nonempty-target rejection mutated target: before=%+v after=%+v", before, after)
			}
			harness.assertStandardGraph(t, fixture)

			harness.clearGraph(t, fixture.name)
			loadResult, err = ret.Load(harness.ctx, harness.database, ret.LoadConfig{
				Directory: config.Directory,
				BatchSize: 2,
			})
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			assertOperationCounts(t, loadResult.GraphCount, loadResult.NodeCount, loadResult.RelationshipCount, 1, 4, 3)

			databaseResult, err := ret.VerifyDatabase(harness.ctx, harness.database, ret.VerifyDatabaseConfig{
				Directory: config.Directory,
				BatchSize: 2,
			})
			if err != nil {
				t.Fatalf("verify database: %v", err)
			}
			assertOperationCounts(t, databaseResult.GraphCount, databaseResult.NodeCount, databaseResult.RelationshipCount, 1, 4, 3)
			harness.assertStandardGraph(t, fixture)
		})
	}
}

func TestRetFacadeDumpResume(t *testing.T) {
	harness := newRetIntegrationHarness(t)
	fixture := harness.seedStandardGraph(t)
	config := harness.dumpConfig(
		fixture.name,
		jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd},
		parquet.Config{Enabled: true},
	)
	config.ShardSize = 2

	interruptedConfig, err := interruptDumpAfterFirstNodeShard(harness.ctx, harness.database, config)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted dump error = %v, want %v", err, context.Canceled)
	}

	interruptedConfig.Resume = true
	result, err := ret.Dump(harness.ctx, harness.database, interruptedConfig)
	if err != nil {
		t.Fatalf("resume dump: %v", err)
	}
	assertOperationCounts(t, result.GraphCount, result.NodeCount, result.RelationshipCount, 1, 4, 3)
	if _, err := ret.VerifyCollection(harness.ctx, ret.VerifyCollectionConfig{Directory: config.Directory}); err != nil {
		t.Fatalf("verify resumed collection: %v", err)
	}
}

func TestRetFacadeResumeRejectsChangedCounts(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, *retIntegrationHarness, seededGraph)
	}{
		{name: "node total", mutate: mutateNodeTotal},
		{name: "relationship total", mutate: mutateRelationshipTotal},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newRetIntegrationHarness(t)
			fixture := harness.seedStandardGraph(t)
			config := harness.dumpConfig(
				fixture.name,
				jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd},
				parquet.Config{},
			)
			config.ShardSize = 2

			interruptedConfig, err := interruptDumpAfterFirstNodeShard(harness.ctx, harness.database, config)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("interrupted dump error = %v, want %v", err, context.Canceled)
			}
			testCase.mutate(t, harness, fixture)

			interruptedConfig.Resume = true
			if _, err := ret.Dump(harness.ctx, harness.database, interruptedConfig); !errors.Is(err, ret.ErrSourceCountChanged) {
				t.Fatalf("resume after %s change error = %v, want %v", testCase.name, err, ret.ErrSourceCountChanged)
			}
		})
	}
}

func TestRetFacadeScrubbedDualOutput(t *testing.T) {
	harness := newRetIntegrationHarness(t)
	fixture := harness.seedScrubGraph(t)
	scrubConfig := scrub.DefaultConfig()
	scrubConfig.Salt = "ret-integration-salt"
	config := harness.dumpConfig(
		fixture.name,
		jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd},
		parquet.Config{Enabled: true},
	)
	config.Scrub = &scrubConfig

	if _, err := ret.Dump(harness.ctx, harness.database, config); err != nil {
		t.Fatalf("dump scrubbed collection: %v", err)
	}
	if _, err := ret.VerifyCollection(harness.ctx, ret.VerifyCollectionConfig{Directory: config.Directory}); err != nil {
		t.Fatalf("verify scrubbed collection: %v", err)
	}

	artifacts := readConcreteArtifacts(t, config.Directory)
	if !reflect.DeepEqual(normalizeNodes(t, artifacts.jsonlNodes), normalizeNodes(t, artifacts.parquetNodes)) {
		t.Fatalf("JSONL and Parquet node values differ:\nJSONL: %#v\nParquet: %#v", artifacts.jsonlNodes, artifacts.parquetNodes)
	}
	if !reflect.DeepEqual(
		normalizeRelationships(t, artifacts.jsonlRelationships),
		normalizeRelationships(t, artifacts.parquetRelationships),
	) {
		t.Fatalf("JSONL and Parquet relationship values differ:\nJSONL: %#v\nParquet: %#v", artifacts.jsonlRelationships, artifacts.parquetRelationships)
	}
	assertScrubFixture(t, artifacts, fixture.kinds)

	harness.clearGraph(t, fixture.name)
	if _, err := ret.Load(harness.ctx, harness.database, ret.LoadConfig{
		Directory: config.Directory,
		BatchSize: 2,
	}); err != nil {
		t.Fatalf("load scrubbed JSONL: %v", err)
	}
	if _, err := ret.VerifyDatabase(harness.ctx, harness.database, ret.VerifyDatabaseConfig{
		Directory: config.Directory,
		BatchSize: 2,
	}); err != nil {
		t.Fatalf("verify scrubbed database: %v", err)
	}
	harness.assertScrubGraph(t, fixture, artifacts)
}

func assertArchiveRoundTrip(t *testing.T, collectionDirectory string) {
	t.Helper()
	t.Run("archive", func(t *testing.T) {
		if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
			t.Skip("ret archive publication is supported only on Linux and Darwin")
		}

		archivePath, unpackedDirectory, recipient, identity := newArchiveFixture(t)
		if err := ret.Pack(context.Background(), ret.PackConfig{
			CollectionDirectory: collectionDirectory,
			ArchivePath:         archivePath,
			Recipient:           recipient,
		}); err != nil {
			t.Fatalf("pack collection: %v", err)
		}
		if err := ret.Unpack(context.Background(), ret.UnpackConfig{
			ArchivePath:     archivePath,
			OutputDirectory: unpackedDirectory,
			Identity:        identity,
		}); err != nil {
			t.Fatalf("unpack collection: %v", err)
		}
		if _, err := ret.VerifyCollection(context.Background(), ret.VerifyCollectionConfig{Directory: unpackedDirectory}); err != nil {
			t.Fatalf("verify unpacked collection: %v", err)
		}
	})
}

var retIntegrationSequence atomic.Uint64

type retIntegrationHarness struct {
	ctx      context.Context
	database graph.Database
	root     string
}

type seededGraph struct {
	name             string
	nodeIDs          []graph.ID
	nodeKinds        map[string][]string
	nodeRoles        map[string]string
	relationshipKind string
	kinds            []string
}

type concreteArtifacts struct {
	jsonlNodes             []entity.Node
	parquetNodes           []entity.Node
	jsonlRelationships     []entity.Relationship
	parquetRelationships   []entity.Relationship
	parquetRelationshipIDs []string
	scrubCounts            scrub.ActionCounts
	scrubMetadata          collection.ScrubMetadata
}

func newRetIntegrationHarness(t *testing.T) *retIntegrationHarness {
	t.Helper()
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING not set")
	}

	ctx := context.Background()
	database, _, err := openDatabase(ctx, databaseConfig{Connection: connection})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := database.Close(closeCtx); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	return &retIntegrationHarness{
		ctx:      ctx,
		database: database,
		root:     t.TempDir(),
	}
}

func (s *retIntegrationHarness) graphName() string {
	return fmt.Sprintf("ret_it_%d_%d", time.Now().UTC().UnixNano(), retIntegrationSequence.Add(1))
}

func (s *retIntegrationHarness) dumpConfig(graphName string, jsonlConfig jsonl.Config, parquetConfig parquet.Config) ret.DumpConfig {
	return ret.DumpConfig{
		Directory:       filepath.Join(s.root, fmt.Sprintf("collection-%d", retIntegrationSequence.Add(1))),
		Graphs:          []string{graphName},
		EntityBatchSize: 2,
		ShardSize:       2,
		JSONL:           jsonlConfig,
		Parquet:         parquetConfig,
	}
}

func (s *retIntegrationHarness) seedStandardGraph(t *testing.T) seededGraph {
	t.Helper()
	graphName := s.graphName()
	userKind := graph.StringKind("RetIntegrationUser")
	systemKind := graph.StringKind("RetIntegrationSystem")
	relationshipKind := graph.StringKind("RetIntegrationLink")
	s.assertSchema(t, graphName, graph.Kinds{userKind, systemKind}, graph.Kinds{relationshipKind})
	s.clearGraph(t, graphName)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.clearGraphError(cleanupCtx, graphName); err != nil {
			t.Errorf("clean up graph %q: %v", graphName, err)
		}
	})

	fixture := seededGraph{
		name:             graphName,
		nodeKinds:        make(map[string][]string, 4),
		nodeRoles:        make(map[string]string, 4),
		relationshipKind: relationshipKind.String(),
	}
	if err := s.database.WriteTransaction(s.ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{Name: graphName})
		nodes := make([]*graph.Node, 0, 4)
		for index := range 4 {
			kinds := graph.Kinds{userKind}
			if index%2 == 1 {
				kinds = graph.Kinds{systemKind}
			}
			node, err := tx.CreateNode(graph.AsProperties(map[string]any{
				"name": fmt.Sprintf("node-%d", index),
				"role": fmt.Sprintf("role-%d", index%2),
			}), kinds...)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			fixture.nodeIDs = append(fixture.nodeIDs, node.ID)
			name := fmt.Sprintf("node-%d", index)
			fixture.nodeKinds[name] = kinds.Strings()
			fixture.nodeRoles[name] = fmt.Sprintf("role-%d", index%2)
		}
		for index := range 3 {
			if _, err := tx.CreateRelationshipByIDs(
				nodes[index].ID,
				nodes[index+1].ID,
				relationshipKind,
				graph.AsProperties(map[string]any{"route": fmt.Sprintf("route-%d", index)}),
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed standard graph: %v", err)
	}
	return fixture
}

func (s *retIntegrationHarness) seedScrubGraph(t *testing.T) seededGraph {
	t.Helper()
	graphName := s.graphName()
	firstKind := graph.StringKind("RetIntegrationFirst")
	secondKind := graph.StringKind("RetIntegrationSecond")
	relationshipKind := graph.StringKind("RetIntegrationScrubLink")
	s.assertSchema(t, graphName, graph.Kinds{firstKind, secondKind}, graph.Kinds{relationshipKind})
	s.clearGraph(t, graphName)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.clearGraphError(cleanupCtx, graphName); err != nil {
			t.Errorf("clean up graph %q: %v", graphName, err)
		}
	})

	orderedDuplicateKinds := graph.Kinds{secondKind, firstKind, secondKind}
	graphNodes := []*graph.Node{
		graph.NewNode(0, graph.AsProperties(map[string]any{
			"enabled":         true,
			"preserved_count": int64(42),
			"email":           "alice@example.com",
			"password":        "super-secret",
			"created_at":      "2026-01-01T00:00:00Z",
		}), orderedDuplicateKinds...),
		graph.NewNode(0, graph.AsProperties(map[string]any{}), firstKind),
	}
	var nodeIDs []graph.ID
	if err := s.database.BatchOperation(s.ctx, func(batch graph.Batch) error {
		creator, ok := batch.WithGraph(graph.Graph{Name: graphName}).(graph.NodeBatchCreator)
		if !ok {
			return errors.New("database batch does not support correlated node creation")
		}
		var err error
		nodeIDs, err = creator.CreateNodes(graphNodes)
		return err
	}, graph.WithBatchSize(2)); err != nil {
		t.Fatalf("seed scrub nodes: %v", err)
	}
	if len(nodeIDs) != 2 {
		t.Fatalf("seed scrub node IDs = %d, want 2", len(nodeIDs))
	}
	if err := s.database.WriteTransaction(s.ctx, func(tx graph.Transaction) error {
		_, err := tx.WithGraph(graph.Graph{Name: graphName}).CreateRelationshipByIDs(
			nodeIDs[0],
			nodeIDs[1],
			relationshipKind,
			graph.AsProperties(map[string]any{}),
		)
		return err
	}); err != nil {
		t.Fatalf("seed scrub relationship: %v", err)
	}

	nodes, _ := s.fetchGraph(t, graphName)
	var observedKinds []string
	for _, node := range nodes {
		if node.Properties != nil && node.Properties.MapOrEmpty()["enabled"] == true {
			observedKinds = node.Kinds.Strings()
			break
		}
	}
	if observedKinds == nil {
		t.Fatal("seeded scrub node was not returned by the database")
	}
	return seededGraph{name: graphName, nodeIDs: nodeIDs, kinds: observedKinds}
}

func (s *retIntegrationHarness) assertSchema(t *testing.T, graphName string, nodeKinds, relationshipKinds graph.Kinds) {
	t.Helper()
	target := graph.Graph{Name: graphName, Nodes: nodeKinds, Edges: relationshipKinds}
	if err := s.database.AssertSchema(s.ctx, graph.Schema{
		Graphs:       []graph.Graph{target},
		DefaultGraph: target,
	}); err != nil {
		t.Fatalf("assert graph schema: %v", err)
	}
}

func (s *retIntegrationHarness) clearGraph(t *testing.T, graphName string) {
	t.Helper()
	if err := s.clearGraphError(s.ctx, graphName); err != nil {
		t.Fatalf("clear graph %q: %v", graphName, err)
	}
}

func (s *retIntegrationHarness) clearGraphError(ctx context.Context, graphName string) error {
	return s.database.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.WithGraph(graph.Graph{Name: graphName}).Nodes().Delete()
	})
}

func (s *retIntegrationHarness) snapshot(t *testing.T, graphName string) dawgs.Snapshot {
	t.Helper()
	source, err := dawgs.NewSource(s.database, graphName, 2)
	if err != nil {
		t.Fatalf("create graph source: %v", err)
	}
	snapshot, err := source.Snapshot(s.ctx)
	if err != nil {
		t.Fatalf("snapshot graph: %v", err)
	}
	return snapshot
}

func (s *retIntegrationHarness) fetchGraph(t *testing.T, graphName string) ([]*graph.Node, []*graph.Relationship) {
	t.Helper()
	var nodes []*graph.Node
	var relationships []*graph.Relationship
	if err := s.database.ReadTransaction(s.ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{Name: graphName})
		var err error
		if nodes, err = ops.FetchNodes(tx.Nodes()); err != nil {
			return err
		}
		relationships, err = ops.FetchRelationships(tx.Relationships())
		return err
	}); err != nil {
		t.Fatalf("fetch graph %q: %v", graphName, err)
	}
	return nodes, relationships
}

func (s *retIntegrationHarness) assertStandardGraph(t *testing.T, fixture seededGraph) {
	t.Helper()
	nodes, relationships := s.fetchGraph(t, fixture.name)
	if len(nodes) != 4 || len(relationships) != 3 {
		t.Fatalf("loaded graph counts: nodes=%d relationships=%d, want 4 and 3", len(nodes), len(relationships))
	}

	namesByID := make(map[graph.ID]string, len(nodes))
	for _, node := range nodes {
		properties := node.Properties.MapOrEmpty()
		name := fmt.Sprint(properties["name"])
		namesByID[node.ID] = name
		if got, want := node.Kinds.Strings(), fixture.nodeKinds[name]; !reflect.DeepEqual(got, want) {
			t.Fatalf("node %q kinds = %v, want %v", name, got, want)
		}
		wantRole := fixture.nodeRoles[name]
		if got := fmt.Sprint(properties["role"]); got != wantRole {
			t.Fatalf("node %q role = %q, want %q", name, got, wantRole)
		}
	}

	routes := make(map[string]string, len(relationships))
	for _, relationship := range relationships {
		if relationship.Kind == nil || relationship.Kind.String() != fixture.relationshipKind {
			t.Fatalf("relationship kind = %v, want %q", relationship.Kind, fixture.relationshipKind)
		}
		key := namesByID[relationship.StartID] + "->" + namesByID[relationship.EndID]
		routes[key] = fmt.Sprint(relationship.Properties.MapOrEmpty()["route"])
	}
	for index := range 3 {
		key := fmt.Sprintf("node-%d->node-%d", index, index+1)
		if got, want := routes[key], fmt.Sprintf("route-%d", index); got != want {
			t.Fatalf("relationship %q route = %q, want %q", key, got, want)
		}
	}
}

func (s *retIntegrationHarness) assertScrubGraph(t *testing.T, fixture seededGraph, artifacts concreteArtifacts) {
	t.Helper()
	nodes, relationships := s.fetchGraph(t, fixture.name)
	if len(nodes) != 2 || len(relationships) != 1 {
		t.Fatalf("loaded scrub graph counts: nodes=%d relationships=%d, want 2 and 1", len(nodes), len(relationships))
	}
	var loaded *graph.Node
	for _, node := range nodes {
		if node.Properties != nil && node.Properties.MapOrEmpty()["enabled"] == true {
			loaded = node
			break
		}
	}
	if loaded == nil {
		t.Fatal("loaded scrub graph is missing the primary node")
	}
	if got := loaded.Kinds.Strings(); !reflect.DeepEqual(got, fixture.kinds) {
		t.Fatalf("loaded ordered kinds = %v, want %v", got, fixture.kinds)
	}
	artifactNode := primaryArtifactNode(t, artifacts.jsonlNodes)
	if got, want := normalizeProperties(t, loaded.Properties.MapOrEmpty()), normalizeProperties(t, artifactNode.Properties); !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded scrub properties = %#v, want %#v", got, want)
	}
}

func interruptDumpAfterFirstNodeShard(
	parent context.Context,
	database graph.Database,
	config ret.DumpConfig,
) (ret.DumpConfig, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	config.Observer = &cancelOnNodeShardObserver{cancel: cancel}
	_, err := ret.Dump(ctx, database, config)
	config.Observer = nil
	return config, err
}

type cancelOnNodeShardObserver struct {
	cancel context.CancelFunc
	once   sync.Once
}

func (s *cancelOnNodeShardObserver) Observe(_ context.Context, event observe.Event) {
	if committed, ok := event.(observe.ShardCommitted); ok && committed.EntityType == "node" {
		s.once.Do(s.cancel)
	}
}

func mutateNodeTotal(t *testing.T, harness *retIntegrationHarness, fixture seededGraph) {
	t.Helper()
	if err := harness.database.WriteTransaction(harness.ctx, func(tx graph.Transaction) error {
		_, err := tx.WithGraph(graph.Graph{Name: fixture.name}).CreateNode(
			graph.AsProperties(map[string]any{"name": "count-change"}),
			graph.StringKind("RetIntegrationUser"),
		)
		return err
	}); err != nil {
		t.Fatalf("mutate node total: %v", err)
	}
}

func mutateRelationshipTotal(t *testing.T, harness *retIntegrationHarness, fixture seededGraph) {
	t.Helper()
	if err := harness.database.WriteTransaction(harness.ctx, func(tx graph.Transaction) error {
		_, err := tx.WithGraph(graph.Graph{Name: fixture.name}).CreateRelationshipByIDs(
			fixture.nodeIDs[0],
			fixture.nodeIDs[len(fixture.nodeIDs)-1],
			graph.StringKind("RetIntegrationLink"),
			graph.AsProperties(map[string]any{"route": "count-change"}),
		)
		return err
	}); err != nil {
		t.Fatalf("mutate relationship total: %v", err)
	}
}

func assertOperationCounts(
	t *testing.T,
	graphs int,
	nodes, relationships int64,
	wantGraphs int,
	wantNodes, wantRelationships int64,
) {
	t.Helper()
	if graphs != wantGraphs || nodes != wantNodes || relationships != wantRelationships {
		t.Fatalf(
			"operation counts: graphs=%d nodes=%d relationships=%d, want %d %d %d",
			graphs,
			nodes,
			relationships,
			wantGraphs,
			wantNodes,
			wantRelationships,
		)
	}
}

func assertConcreteOutputs(t *testing.T, root string, wantJSONL, wantParquet bool) {
	t.Helper()
	manifest, err := collection.Read(root)
	if err != nil {
		t.Fatalf("read collection manifest: %v", err)
	}
	if got := manifest.Outputs.JSONL != nil; got != wantJSONL {
		t.Fatalf("manifest JSONL enabled = %t, want %t", got, wantJSONL)
	}
	if got := manifest.Outputs.Parquet != nil; got != wantParquet {
		t.Fatalf("manifest Parquet enabled = %t, want %t", got, wantParquet)
	}
	for _, graphEntry := range manifest.Graphs {
		for _, shard := range graphEntry.NodeShards {
			if (shard.JSONL != nil) != wantJSONL || (shard.Parquet != nil) != wantParquet {
				t.Fatalf("node shard %d concrete outputs do not match collection capabilities", shard.Index)
			}
		}
		for _, shard := range graphEntry.RelationshipShards {
			if (shard.JSONL != nil) != wantJSONL || (shard.Parquet != nil) != wantParquet {
				t.Fatalf("relationship shard %d concrete outputs do not match collection capabilities", shard.Index)
			}
		}
	}
}

func damageFirstParquetArtifact(t *testing.T, root string) {
	t.Helper()
	manifest, err := collection.Read(root)
	if err != nil {
		t.Fatalf("read dual-output manifest before damaging Parquet: %v", err)
	}
	for _, graphEntry := range manifest.Graphs {
		for _, shard := range graphEntry.NodeShards {
			if shard.Parquet != nil {
				path := filepath.Join(root, filepath.FromSlash(shard.Parquet.Path))
				if err := os.WriteFile(path, []byte("intentionally damaged Parquet"), 0o600); err != nil {
					t.Fatalf("damage Parquet artifact: %v", err)
				}
				return
			}
		}
	}
	t.Fatal("dual-output collection has no Parquet artifact to damage")
}

func newArchiveFixture(t *testing.T) (string, string, archive.PublicKey, archive.PrivateKey) {
	t.Helper()
	root := t.TempDir()
	privatePath := filepath.Join(root, "identity.key")
	publicPath := filepath.Join(root, "recipient.key")
	if err := ret.Keygen(ret.KeygenConfig{
		PrivateKeyPath: privatePath,
		PublicKeyPath:  publicPath,
	}); err != nil {
		t.Fatalf("generate archive keys: %v", err)
	}
	recipient, err := archive.ReadPublicKey(publicPath)
	if err != nil {
		t.Fatalf("read archive recipient: %v", err)
	}
	identity, err := archive.ReadPrivateKey(privatePath)
	if err != nil {
		t.Fatalf("read archive identity: %v", err)
	}
	return filepath.Join(root, "collection.ret.enc"), filepath.Join(root, "unpacked"), recipient, identity
}

func readConcreteArtifacts(t *testing.T, root string) concreteArtifacts {
	t.Helper()
	manifest, err := collection.Read(root)
	if err != nil {
		t.Fatalf("read scrub manifest: %v", err)
	}
	if len(manifest.Graphs) != 1 {
		t.Fatalf("scrub manifest graph count = %d, want 1", len(manifest.Graphs))
	}
	graphEntry := manifest.Graphs[0]
	result := concreteArtifacts{
		scrubCounts:   scrub.ActionCounts{},
		scrubMetadata: manifest.Scrub,
	}
	for _, shard := range graphEntry.NodeShards {
		result.scrubCounts.Add(shard.ScrubCounts)
		if shard.JSONL == nil || shard.Parquet == nil {
			t.Fatalf("dual node shard %d is missing a concrete artifact", shard.Index)
		}
		if err := jsonl.ReadNodes(root, *shard.JSONL, func(node entity.Node) error {
			result.jsonlNodes = append(result.jsonlNodes, node)
			return nil
		}); err != nil {
			t.Fatalf("read JSONL node shard %d: %v", shard.Index, err)
		}
		if err := parquet.ReadNodes(root, *shard.Parquet, func(node entity.Node) error {
			result.parquetNodes = append(result.parquetNodes, node)
			return nil
		}); err != nil {
			t.Fatalf("read Parquet node shard %d: %v", shard.Index, err)
		}
	}
	for _, shard := range graphEntry.RelationshipShards {
		result.scrubCounts.Add(shard.ScrubCounts)
		if shard.JSONL == nil || shard.Parquet == nil {
			t.Fatalf("dual relationship shard %d is missing a concrete artifact", shard.Index)
		}
		if err := jsonl.ReadRelationships(root, *shard.JSONL, func(relationship entity.Relationship) error {
			result.jsonlRelationships = append(result.jsonlRelationships, relationship)
			return nil
		}); err != nil {
			t.Fatalf("read JSONL relationship shard %d: %v", shard.Index, err)
		}
		if err := parquet.ReadRelationships(root, *shard.Parquet, func(relationship entity.Relationship) error {
			result.parquetRelationshipIDs = append(result.parquetRelationshipIDs, relationship.SourceID)
			relationship.SourceID = ""
			result.parquetRelationships = append(result.parquetRelationships, relationship)
			return nil
		}); err != nil {
			t.Fatalf("read Parquet relationship shard %d: %v", shard.Index, err)
		}
	}
	return result
}

func assertScrubFixture(t *testing.T, artifacts concreteArtifacts, wantKinds []string) {
	t.Helper()
	if !artifacts.scrubMetadata.Enabled {
		t.Fatal("scrub metadata is not enabled")
	}
	wantCounts := scrub.ActionCounts{
		Preserve:       2,
		Pseudonymize:   1,
		Redact:         1,
		ShiftTimestamp: 1,
	}
	if !reflect.DeepEqual(artifacts.scrubCounts, wantCounts) {
		t.Fatalf("scrub action counts = %#v, want %#v", artifacts.scrubCounts, wantCounts)
	}
	for index, relationship := range artifacts.jsonlRelationships {
		if relationship.SourceID != "" {
			t.Fatalf("JSONL relationship %d retained source ID %q", index, relationship.SourceID)
		}
	}
	for index, sourceID := range artifacts.parquetRelationshipIDs {
		if sourceID == "" {
			t.Fatalf("Parquet relationship %d omitted its source ID", index)
		}
	}

	node := primaryArtifactNode(t, artifacts.jsonlNodes)
	if !reflect.DeepEqual(node.Kinds, wantKinds) {
		t.Fatalf("artifact ordered kinds = %v, want database-observed %v", node.Kinds, wantKinds)
	}
	if node.Properties["enabled"] != true {
		t.Fatalf("preserved property = %#v, want true", node.Properties["enabled"])
	}
	normalized := normalizeProperties(t, node.Properties)
	if got, ok := normalized["preserved_count"].(json.Number); !ok || got.String() != "42" {
		t.Fatalf(
			"normalized preserved count = %#v (%T), want json.Number(%q)",
			normalized["preserved_count"],
			normalized["preserved_count"],
			"42",
		)
	}
	if node.Properties["password"] != "[REDACTED]" {
		t.Fatalf("redacted property = %#v, want [REDACTED]", node.Properties["password"])
	}
	if node.Properties["created_at"] != "2026-01-18T00:00:00Z" {
		t.Fatalf("shifted timestamp = %#v, want 2026-01-18T00:00:00Z", node.Properties["created_at"])
	}
	if got := fmt.Sprint(node.Properties["email"]); got != "user-d47583d80c3e@example.invalid" {
		t.Fatalf("pseudonymized email = %q, want user-d47583d80c3e@example.invalid", got)
	}
}

func primaryArtifactNode(t *testing.T, nodes []entity.Node) entity.Node {
	t.Helper()
	for _, node := range nodes {
		if node.Properties["enabled"] == true {
			return node
		}
	}
	t.Fatal("artifact is missing the primary scrub node")
	return entity.Node{}
}

func normalizeNodes(t *testing.T, nodes []entity.Node) []entity.Node {
	t.Helper()
	normalized := append([]entity.Node(nil), nodes...)
	for index := range normalized {
		normalized[index].Properties = normalizeProperties(t, normalized[index].Properties)
	}
	return normalized
}

func normalizeRelationships(t *testing.T, relationships []entity.Relationship) []entity.Relationship {
	t.Helper()
	normalized := append([]entity.Relationship(nil), relationships...)
	for index := range normalized {
		normalized[index].Properties = normalizeProperties(t, normalized[index].Properties)
	}
	return normalized
}

func normalizeProperties(t *testing.T, properties map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(properties)
	if err != nil {
		t.Fatalf("encode properties for logical normalization: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized map[string]any
	if err := decoder.Decode(&normalized); err != nil {
		t.Fatalf("decode properties for logical normalization: %v", err)
	}
	return normalized
}

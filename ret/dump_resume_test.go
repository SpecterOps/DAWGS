package ret

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/checkpoint"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func TestDumpResumeReconstructsMetricsAndContinuesAfterCommittedShard(t *testing.T) {
	// Break caught: resuming from the cursor without replaying committed nodes,
	// which loses metrics endpoint state, kinds, or the first shard.
	config, database := interruptedDumpAfterFirstNodeShard(t, true, true)
	config.Resume = true
	var starts []observe.PhaseStarted
	config.Observer = observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if value, ok := event.(observe.PhaseStarted); ok {
			starts = append(starts, value)
		}
	})

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.EqualValues(t, manifest.Graphs[0].NodeCount, manifest.Graphs[0].Metrics.NodeCount)
	require.EqualValues(t, manifest.Graphs[0].RelationshipCount, manifest.Graphs[0].Metrics.RelationshipCount)
	require.Equal(t, []int{1, 2, 3}, dumpNodeShardIndices(manifest.Graphs[0]))
	require.NoFileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
	require.Len(t, starts, 2)
	require.EqualValues(t, 1, starts[0].Completed)
	require.EqualValues(t, 0, starts[1].Completed)
}

func TestDumpResumeReconstructsParquetOnlyCheckpoint(t *testing.T) {
	// Break caught: making resume depend on loadable JSONL even though Parquet
	// contains enough canonical entities to rebuild dump state.
	config, database := interruptedDumpAfterFirstNodeShard(t, false, true)
	config.Resume = true

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Equal(t, []int{1, 2, 3}, dumpNodeShardIndices(manifest.Graphs[0]))
	require.Nil(t, manifest.Graphs[0].NodeShards[0].JSONL)
	require.NotNil(t, manifest.Graphs[0].NodeShards[0].Parquet)
	require.EqualValues(t, 3, manifest.Graphs[0].Metrics.NodeCount)
	require.EqualValues(t, 1, manifest.Graphs[0].Metrics.RelationshipCount)
}

func TestDumpResumeContinuesRelationshipPhaseAfterCommittedShard(t *testing.T) {
	// Break caught: treating every checkpoint as a nodes-phase checkpoint and
	// either rescanning nodes or omitting committed relationship metrics.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes: dumpTestNodes(1, 2, 3),
			relationships: []*graph.Relationship{
				graph.NewRelationship(10, 1, 2, graph.NewProperties(), graph.StringKind("FIRST")),
				graph.NewRelationship(11, 2, 3, graph.NewProperties(), graph.StringKind("SECOND")),
			},
		},
	})
	config := validRootDumpConfig(t)
	config.EntityBatchSize = 3
	config.ShardSize = 1
	originalWrite := writeJSONLRelationships
	calls := 0
	injected := errors.New("injected second relationship shard failure")
	writeJSONLRelationships = func(
		tempPath, relativePath string,
		output jsonl.Config,
		relationships []entity.Relationship,
	) (jsonl.RelationshipArtifact, error) {
		calls++
		if calls == 2 {
			return jsonl.RelationshipArtifact{}, injected
		}
		return originalWrite(tempPath, relativePath, output, relationships)
	}
	t.Cleanup(func() { writeJSONLRelationships = originalWrite })

	_, err := Dump(context.Background(), database, config)
	writeJSONLRelationships = originalWrite
	require.ErrorIs(t, err, injected)
	state, exists, err := (checkpoint.Store{Root: config.Directory}).Load()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, checkpoint.PhaseRelationships, state.Graphs[0].Phase)
	require.Len(t, state.Graphs[0].RelationshipShards, 1)

	config.Resume = true
	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Equal(t, []int{1, 2}, dumpRelationshipShardIndices(manifest.Graphs[0]))
	require.EqualValues(t, 3, manifest.Graphs[0].Metrics.NodeCount)
	require.EqualValues(t, 2, manifest.Graphs[0].Metrics.RelationshipCount)
	require.Equal(t, []string{"Entity", "FIRST", "SECOND"}, manifest.Graphs[0].KindCatalog)
	require.NoFileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
}

func TestDumpResumePublishesFromCompleteCheckpoint(t *testing.T) {
	// Break caught: requiring an active scan phase on resume instead of
	// reconstructing a complete checkpoint and retrying final publication.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	})
	config := validRootDumpConfig(t)
	injected := errors.New("injected manifest publication failure")
	originalWrite := writeCollection
	writeCollection = func(string, collection.Manifest) error { return injected }
	t.Cleanup(func() { writeCollection = originalWrite })

	_, err := Dump(context.Background(), database, config)
	writeCollection = originalWrite
	require.ErrorIs(t, err, injected)
	state, exists, err := (checkpoint.Store{Root: config.Directory}).Load()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, checkpoint.PhaseComplete, state.Graphs[0].Phase)
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))

	config.Resume = true
	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.EqualValues(t, 2, manifest.Graphs[0].Metrics.NodeCount)
	require.EqualValues(t, 1, manifest.Graphs[0].Metrics.RelationshipCount)
	require.NoFileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
}

func TestDumpResumePreservesScrubbedArtifactsAndActionCounts(t *testing.T) {
	// Break caught: losing committed scrub metadata on resume or applying
	// scrubbing only to the pre-interruption portion of the source.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.AsProperties(map[string]any{"password": "one"}), graph.StringKind("Entity")),
				graph.NewNode(2, graph.AsProperties(map[string]any{"password": "two"}), graph.StringKind("Entity")),
				graph.NewNode(3, graph.AsProperties(map[string]any{"password": "three"}), graph.StringKind("Entity")),
			},
		},
	})
	config := validRootDumpConfig(t)
	config.EntityBatchSize = 3
	config.ShardSize = 1
	config.Parquet.Enabled = false
	originalWrite := writeJSONLNodes
	calls := 0
	injected := errors.New("injected scrub resume writer failure")
	writeJSONLNodes = func(
		tempPath, relativePath string,
		output jsonl.Config,
		nodes []entity.Node,
	) (jsonl.NodeArtifact, error) {
		calls++
		if calls == 2 {
			return jsonl.NodeArtifact{}, injected
		}
		return originalWrite(tempPath, relativePath, output, nodes)
	}
	t.Cleanup(func() { writeJSONLNodes = originalWrite })

	_, err := Dump(context.Background(), database, config)
	writeJSONLNodes = originalWrite
	require.ErrorIs(t, err, injected)
	state, exists, err := (checkpoint.Store{Root: config.Directory}).Load()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, scrub.ActionCounts{Redact: 1}, state.Graphs[0].NodeShards[0].ScrubCounts)

	config.Resume = true
	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Len(t, manifest.Graphs[0].NodeShards, 3)
	for _, shard := range manifest.Graphs[0].NodeShards {
		require.Equal(t, scrub.ActionCounts{Redact: 1}, shard.ScrubCounts)
		nodes, err := jsonl.ReadNodes(config.Directory, *shard.JSONL)
		require.NoError(t, err)
		for _, node := range nodes {
			require.Equal(t, "[REDACTED]", node.Properties["password"])
		}
	}
}

func TestDumpResumeReplayCancellationIsNotArtifactIntegrityFailure(t *testing.T) {
	// Break caught: wrapping an active context cancellation from a replay visitor
	// with ErrArtifactIntegrity and falsely classifying durable bytes as corrupt.
	for _, test := range []struct {
		name         string
		setup        func(*testing.T) (DumpConfig, *dumpTestDatabase)
		cancelOnCall int
		errorContext string
	}{
		{
			name: "node replay",
			setup: func(t *testing.T) (DumpConfig, *dumpTestDatabase) {
				return interruptedDumpAfterFirstNodeShard(t, true, false)
			},
			cancelOnCall: 3,
			errorContext: `reconstruct graph "asset" node shard 1`,
		},
		{
			name: "relationship replay",
			setup: func(t *testing.T) (DumpConfig, *dumpTestDatabase) {
				database := newDumpTestDatabase(map[string]*dumpTestGraph{
					"asset": {
						nodes:         dumpTestNodes(1, 2),
						relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
					},
				})
				config := validRootDumpConfig(t)
				injected := errors.New("injected complete checkpoint manifest failure")
				originalWrite := writeCollection
				writeCollection = func(string, collection.Manifest) error { return injected }
				t.Cleanup(func() { writeCollection = originalWrite })
				_, err := Dump(context.Background(), database, config)
				writeCollection = originalWrite
				require.ErrorIs(t, err, injected)
				return config, database
			},
			cancelOnCall: 5,
			errorContext: `reconstruct graph "asset" relationship shard 1`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, database := test.setup(t)
			checkpointBefore := mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName))
			ctx := newStagedCancelContext()
			armed := false
			database.onRelationshipCount = func(context.Context, string) {
				if !armed {
					armed = true
					ctx.arm(test.cancelOnCall)
				}
			}
			config.Resume = true

			_, err := Dump(ctx, database, config)

			require.ErrorIs(t, err, context.Canceled)
			require.False(t, errors.Is(err, ErrArtifactIntegrity))
			require.ErrorContains(t, err, test.errorContext)
			require.Equal(t, checkpointBefore, mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName)))
			require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
		})
	}
}

func TestDumpResumeRejectsIdentityChanges(t *testing.T) {
	// Break caught: continuing with configuration that changes cursor behavior,
	// logical artifacts, concrete encoding, or deterministic scrubbing.
	for _, test := range []struct {
		name   string
		mutate func(*DumpConfig)
		match  string
	}{
		{
			name: "graph order",
			mutate: func(config *DumpConfig) {
				config.Graphs = []string{"other", "asset"}
			},
			match: "ordered graph names",
		},
		{
			name:   "batch size",
			mutate: func(config *DumpConfig) { config.EntityBatchSize++ },
			match:  "entity batch size",
		},
		{
			name:   "shard size",
			mutate: func(config *DumpConfig) { config.ShardSize++ },
			match:  "shard size",
		},
		{
			name: "JSONL codec",
			mutate: func(config *DumpConfig) {
				config.JSONL.Codec = jsonl.CodecZstd
			},
			match: "JSONL codec",
		},
		{
			name:   "JSONL level",
			mutate: func(config *DumpConfig) { config.JSONL.Level++ },
			match:  "JSONL level",
		},
		{
			name:   "enabled output",
			mutate: func(config *DumpConfig) { config.Parquet.Enabled = false },
			match:  "Parquet enabled",
		},
		{
			name:   "scrub enabled",
			mutate: func(config *DumpConfig) { config.Scrub = nil },
			match:  "scrub enabled",
		},
		{
			name: "scrub rules",
			mutate: func(config *DumpConfig) {
				config.Scrub.Rules.FakeDomain = "changed.example"
			},
			match: "scrub rules fingerprint",
		},
		{
			name:   "scrub salt",
			mutate: func(config *DumpConfig) { config.Scrub.Salt = "changed-salt" },
			match:  "scrub salt fingerprint",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newDumpTestDatabase(map[string]*dumpTestGraph{
				"asset": {
					nodes:         dumpTestNodesWithKinds([]string{"User"}, 1, 2, 3),
					relationships: dumpTestRelationships(10, 1, 3, "MEMBER_OF"),
				},
				"other": {},
			})
			config := validRootDumpConfig(t)
			config.Graphs = []string{"asset", "other"}
			config.EntityBatchSize = 3
			config.ShardSize = 1
			config.JSONL.Codec = jsonl.CodecGzip
			config.JSONL.Level = 1
			config, database = interruptedDumpFromConfig(t, config, database)
			config.Resume = true
			test.mutate(&config)

			_, err := Dump(context.Background(), database, config)

			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, test.match)
			require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
			require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
		})
	}
}

func TestDumpResumeRemovesRecognizedUncheckpointedArtifact(t *testing.T) {
	// Break caught: either retaining the known next-shard crash-window artifact
	// or deleting it only after a writer collides with its deterministic path.
	config, database := interruptedDumpAfterFirstNodeShard(t, true, false)
	orphan, err := writeNodeShard(
		config.Directory,
		"asset",
		2,
		99,
		scrub.ActionCounts{},
		[]entity.Node{{SourceID: "99", Kinds: []string{"Orphan"}}},
		config.JSONL,
		config.Parquet,
	)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(config.Directory, filepath.FromSlash(orphan.JSONL.Path)))

	originalWrite := writeJSONLNodes
	writeJSONLNodes = func(string, string, jsonl.Config, []entity.Node) (jsonl.NodeArtifact, error) {
		return jsonl.NodeArtifact{}, errors.New("stop after resume cleanup")
	}
	t.Cleanup(func() { writeJSONLNodes = originalWrite })
	config.Resume = true

	_, err = Dump(context.Background(), database, config)

	writeJSONLNodes = originalWrite
	require.ErrorContains(t, err, "stop after resume cleanup")
	require.NoFileExists(t, filepath.Join(config.Directory, filepath.FromSlash(orphan.JSONL.Path)))
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
}

func TestDumpResumeRejectsUnknownFileWithoutDeletingIt(t *testing.T) {
	// Break caught: broad cleanup that deletes caller data merely because it is
	// present in a resumable collection directory.
	config, database := interruptedDumpAfterFirstNodeShard(t, true, false)
	unknown := filepath.Join(config.Directory, "caller-owned.txt")
	require.NoError(t, os.WriteFile(unknown, []byte("keep"), 0o600))
	config.Resume = true

	_, err := Dump(context.Background(), database, config)

	require.ErrorIs(t, err, ErrInvalidCollection)
	require.FileExists(t, unknown)
	require.Equal(t, "keep", string(mustReadFile(t, unknown)))
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
}

func TestDumpResumeRejectsSourceCountChangeBeforeScanning(t *testing.T) {
	// Break caught: resuming a cursor against a source whose entity totals no
	// longer match the checkpoint snapshot.
	config, database := interruptedDumpAfterFirstNodeShard(t, true, true)
	database.graphs["asset"].snapshots = append(database.graphs["asset"].snapshots, dawgs.Snapshot{
		NodeCount:         4,
		RelationshipCount: 1,
	})
	config.Resume = true

	_, err := Dump(context.Background(), database, config)

	require.ErrorIs(t, err, ErrSourceCountChanged)
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
}

func TestDumpResumeCancellationDuringCountSnapshotStopsBeforeOrphanCleanup(t *testing.T) {
	// Break caught: continuing from a successful resume count result after the
	// count callback canceled, including deleting the recognized crash artifact.
	config, database := interruptedDumpAfterFirstNodeShard(t, true, false)
	orphan, err := writeNodeShard(
		config.Directory,
		"asset",
		2,
		99,
		scrub.ActionCounts{},
		[]entity.Node{{SourceID: "99", Kinds: []string{"Orphan"}}},
		config.JSONL,
		config.Parquet,
	)
	require.NoError(t, err)
	orphanPath := filepath.Join(config.Directory, filepath.FromSlash(orphan.JSONL.Path))
	require.FileExists(t, orphanPath)
	checkpointBefore := mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName))

	ctx, cancel := context.WithCancel(context.Background())
	database.onRelationshipCount = func(context.Context, string) {
		cancel()
	}
	config.Resume = true

	_, err = Dump(ctx, database, config)

	require.ErrorIs(t, err, context.Canceled)
	require.FileExists(t, orphanPath)
	require.Equal(t, checkpointBefore, mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName)))
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
}

func TestDumpRecountsEveryGraphBeforePublishingManifest(t *testing.T) {
	// Break caught: publishing after successful scans without detecting a later
	// total change in one of the source graphs.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes:     dumpTestNodes(1, 2, 3),
			snapshots: []dawgs.Snapshot{{NodeCount: 3}, {NodeCount: 4}},
		},
	})
	config := validRootDumpConfig(t)

	_, err := Dump(context.Background(), database, config)

	require.ErrorIs(t, err, ErrSourceCountChanged)
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
}

func TestDumpClassifiesPhaseScanTotalMismatchBeforePartialShardPublication(t *testing.T) {
	// Break caught: attempting to checkpoint a partial shard against a larger
	// snapshot and returning a checkpoint-validation error instead of the public
	// source-count change classification.
	for _, test := range []struct {
		name     string
		value    *dumpTestGraph
		artifact string
	}{
		{
			name: "nodes",
			value: &dumpTestGraph{
				nodes:     dumpTestNodes(1),
				snapshots: []dawgs.Snapshot{{NodeCount: 3}},
			},
			artifact: collection.NodeJSONLPath("asset", 1, jsonl.CodecNone),
		},
		{
			name: "relationships",
			value: &dumpTestGraph{
				nodes:         dumpTestNodes(1, 2),
				relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
				snapshots:     []dawgs.Snapshot{{NodeCount: 2, RelationshipCount: 2}},
			},
			artifact: collection.RelationshipJSONLPath("asset", 1, jsonl.CodecNone),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": test.value})
			config := validRootDumpConfig(t)
			config.ShardSize = 2

			_, err := Dump(context.Background(), database, config)

			require.ErrorIs(t, err, ErrSourceCountChanged)
			require.NoFileExists(t, filepath.Join(config.Directory, filepath.FromSlash(test.artifact)))
			require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
			require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
		})
	}
}

func TestDumpDoesNotClaimToDetectSameCountSourceMutation(t *testing.T) {
	// Break caught: accidentally promising content-level source consistency when
	// the publication contract intentionally checks totals only.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1, 2)},
	})
	fetches := 0
	database.onNodeFetch = func(graphName string) {
		fetches++
		if fetches == 2 {
			database.graphs[graphName].nodes[0] = graph.NewNode(
				1,
				graph.NewProperties(),
				graph.StringKind("MutatedAfterScan"),
			)
		}
	}
	config := validRootDumpConfig(t)
	config.EntityBatchSize = 2

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Equal(t, []string{"Entity"}, manifest.Graphs[0].KindCatalog)
	require.Equal(t, "MutatedAfterScan", database.graphs["asset"].nodes[0].Kinds[0].String())
}

func TestDumpWriterFailurePreservesCheckpointedShardsAndNoManifest(t *testing.T) {
	// Break caught: deleting durable progress or publishing a manifest after the
	// next concrete shard writer fails.
	config, _ := interruptedDumpAfterFirstNodeShard(t, true, true)
	state, exists, err := (checkpoint.Store{Root: config.Directory}).Load()

	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, state.Graphs[0].NodeShards, 1)
	require.FileExists(t, filepath.Join(config.Directory, filepath.FromSlash(state.Graphs[0].NodeShards[0].JSONL.Path)))
	require.FileExists(t, filepath.Join(config.Directory, filepath.FromSlash(state.Graphs[0].NodeShards[0].Parquet.Path)))
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
}

func TestDumpResumeRequiresCheckpointAndRejectsPublishedManifest(t *testing.T) {
	// Break caught: treating resume as a fresh dump when no durable checkpoint
	// exists, or overwriting a completed collection.
	missing := validRootDumpConfig(t)
	missing.Resume = true
	_, err := Dump(context.Background(), newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}}), missing)
	require.ErrorIs(t, err, ErrCheckpointMissing)

	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}})
	published := validRootDumpConfig(t)
	result, err := Dump(context.Background(), database, published)
	require.NoError(t, err)
	require.FileExists(t, result.ManifestPath)
	published.Resume = true

	_, err = Dump(context.Background(), database, published)

	require.ErrorIs(t, err, ErrDestinationExists)
	require.FileExists(t, result.ManifestPath)
}

func TestDumpValidatesBeforeCreatingDestination(t *testing.T) {
	// Break caught: leaving an empty destination behind for an invalid request.
	config := validRootDumpConfig(t)
	config.ShardSize = 0

	_, err := Dump(context.Background(), newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}}), config)

	require.ErrorIs(t, err, ErrInvalidConfig)
	require.NoDirExists(t, config.Directory)
}

func interruptedDumpAfterFirstNodeShard(t *testing.T, jsonlEnabled, parquetEnabled bool) (DumpConfig, *dumpTestDatabase) {
	t.Helper()
	return interruptedDumpWithGraphs(t, jsonlEnabled, parquetEnabled, []string{"asset"})
}

func interruptedDumpWithGraphs(t *testing.T, jsonlEnabled, parquetEnabled bool, graphs []string) (DumpConfig, *dumpTestDatabase) {
	t.Helper()
	values := map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodesWithKinds([]string{"User"}, 1, 2, 3),
			relationships: dumpTestRelationships(10, 1, 3, "MEMBER_OF"),
		},
		"other": {},
	}
	database := newDumpTestDatabase(values)
	config := validRootDumpConfig(t)
	config.Graphs = append([]string(nil), graphs...)
	config.EntityBatchSize = 3
	config.ShardSize = 1
	config.JSONL.Enabled = jsonlEnabled
	config.Parquet.Enabled = parquetEnabled
	return interruptedDumpFromConfig(t, config, database)
}

func interruptedDumpFromConfig(t *testing.T, config DumpConfig, database *dumpTestDatabase) (DumpConfig, *dumpTestDatabase) {
	t.Helper()
	config.Directory = filepath.Join(t.TempDir(), "collection")
	injected := errors.New("injected second node shard failure")
	if config.JSONL.Enabled {
		originalWrite := writeJSONLNodes
		calls := 0
		writeJSONLNodes = func(tempPath, relativePath string, output jsonl.Config, nodes []entity.Node) (jsonl.NodeArtifact, error) {
			calls++
			if calls == 2 {
				return jsonl.NodeArtifact{}, injected
			}
			return originalWrite(tempPath, relativePath, output, nodes)
		}
		t.Cleanup(func() { writeJSONLNodes = originalWrite })
		_, err := Dump(context.Background(), database, config)
		writeJSONLNodes = originalWrite
		require.ErrorIs(t, err, injected)
	} else {
		originalWrite := writeParquetNodes
		calls := 0
		writeParquetNodes = func(tempPath, relativePath string, output parquet.Config, nodes []entity.Node) (parquet.NodeArtifact, error) {
			calls++
			if calls == 2 {
				return parquet.NodeArtifact{}, injected
			}
			return originalWrite(tempPath, relativePath, output, nodes)
		}
		t.Cleanup(func() { writeParquetNodes = originalWrite })
		_, err := Dump(context.Background(), database, config)
		writeParquetNodes = originalWrite
		require.ErrorIs(t, err, injected)
	}
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
	return config, database
}

func dumpNodeShardIndices(graph collection.Graph) []int {
	indices := make([]int, len(graph.NodeShards))
	for index, shard := range graph.NodeShards {
		indices[index] = shard.Index
	}
	return indices
}

func dumpRelationshipShardIndices(graph collection.Graph) []int {
	indices := make([]int, len(graph.RelationshipShards))
	for index, shard := range graph.RelationshipShards {
		indices[index] = shard.Index
	}
	return indices
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return contents
}

type stagedCancelContext struct {
	context.Context

	cancel    context.CancelFunc
	mu        sync.Mutex
	remaining int
	armed     bool
}

func newStagedCancelContext() *stagedCancelContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &stagedCancelContext{Context: ctx, cancel: cancel}
}

func (s *stagedCancelContext) arm(calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
	s.remaining = calls
}

func (s *stagedCancelContext) Err() error {
	s.mu.Lock()
	if s.armed && s.Context.Err() == nil {
		s.remaining--
		if s.remaining == 0 {
			s.cancel()
		}
	}
	s.mu.Unlock()
	return s.Context.Err()
}

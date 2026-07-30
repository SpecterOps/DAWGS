package ret

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/ret/checkpoint"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func TestWriteNodeShardPublishesBothConcreteArtifacts(t *testing.T) {
	// Break caught: publishing only one enabled artifact or returning metadata before both files exist.
	root := t.TempDir()
	shard, err := writeNodeShard(
		root, "asset", 1, 42, scrub.ActionCounts{Redact: 1},
		[]entity.Node{{SourceID: "42", Kinds: []string{"User"}, Properties: map[string]any{"name": "Ada"}}},
		jsonl.Config{Enabled: true, Codec: jsonl.CodecZstd, Level: 3},
		parquet.Config{Enabled: true},
	)
	require.NoError(t, err)
	require.NotNil(t, shard.JSONL)
	require.NotNil(t, shard.Parquet)
	require.EqualValues(t, 1, shard.Count)
	require.Equal(t, 42, int(shard.LastSourceID))
	require.FileExists(t, filepath.Join(root, "graphs", "asset", "nodes", "000001.jsonl.zst"))
	require.FileExists(t, filepath.Join(root, "graphs", "asset", "nodes", "000001.parquet"))
}

func TestWriteNodeShardPublishesExactlyTheEnabledConcreteOutput(t *testing.T) {
	// Break caught: emitting a disabled format or omitting the one enabled format.
	for _, test := range []struct {
		name        string
		jsonl       jsonl.Config
		parquet     parquet.Config
		wantJSONL   bool
		wantParquet bool
	}{
		{name: "jsonl only", jsonl: jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, wantJSONL: true},
		{name: "parquet only", parquet: parquet.Config{Enabled: true}, wantParquet: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			shard, err := writeNodeShard(root, "asset", 1, 42, scrub.ActionCounts{}, []entity.Node{{SourceID: "42"}}, test.jsonl, test.parquet)

			require.NoError(t, err)
			require.Equal(t, test.wantJSONL, shard.JSONL != nil)
			require.Equal(t, test.wantParquet, shard.Parquet != nil)
			require.EqualValues(t, 1, shard.Count)
			require.Len(t, regularFiles(t, root), 1)
		})
	}
}

func TestWriteRelationshipShardPublishesBothConcreteArtifacts(t *testing.T) {
	// Break caught: applying the logical dual-output commit only to node shards.
	root := t.TempDir()
	shard, err := writeRelationshipShard(
		root, "asset", 1, 99, scrub.ActionCounts{Redact: 1},
		[]entity.Relationship{{SourceID: "99", StartID: "1", EndID: "2", Kind: "MemberOf"}},
		jsonl.Config{Enabled: true, Codec: jsonl.CodecNone},
		parquet.Config{Enabled: true},
	)
	require.NoError(t, err)
	require.NotNil(t, shard.JSONL)
	require.NotNil(t, shard.Parquet)
	require.EqualValues(t, 1, shard.Count)
	require.FileExists(t, filepath.Join(root, "graphs", "asset", "relationships", "000001.jsonl"))
	require.FileExists(t, filepath.Join(root, "graphs", "asset", "relationships", "000001.parquet"))
}

func TestWriteNodeShardCleansAllArtifactsWhenSecondWriterFails(t *testing.T) {
	// Break caught: leaving the first concrete artifact behind when the second writer fails.
	originalWriteParquetNodes := writeParquetNodes
	writeParquetNodes = func(string, string, parquet.Config, []entity.Node) (parquet.NodeArtifact, error) {
		return parquet.NodeArtifact{}, errors.New("injected Parquet failure")
	}
	t.Cleanup(func() { writeParquetNodes = originalWriteParquetNodes })

	root := t.TempDir()
	_, err := writeNodeShard(root, "asset", 1, 42, scrub.ActionCounts{}, []entity.Node{{SourceID: "42"}}, jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, parquet.Config{Enabled: true})
	require.ErrorContains(t, err, "injected Parquet failure")
	require.Empty(t, regularFiles(t, root))
}

func TestWriteNodeShardRejectsMismatchedWriterCount(t *testing.T) {
	// Break caught: publishing metadata whose artifact count disagrees with the logical shard count.
	originalWriteJSONLNodes := writeJSONLNodes
	writeJSONLNodes = func(tempPath, finalRelativePath string, config jsonl.Config, nodes []entity.Node) (jsonl.NodeArtifact, error) {
		artifact, err := originalWriteJSONLNodes(tempPath, finalRelativePath, config, nodes)
		artifact.Count++
		return artifact, err
	}
	t.Cleanup(func() { writeJSONLNodes = originalWriteJSONLNodes })

	root := t.TempDir()
	_, err := writeNodeShard(root, "asset", 1, 42, scrub.ActionCounts{}, []entity.Node{{SourceID: "42"}}, jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, parquet.Config{})
	require.ErrorIs(t, err, ErrArtifactIntegrity)
	require.Empty(t, regularFiles(t, root))
}

func TestWriteNodeShardCleansPublishedArtifactWhenSecondRenameFails(t *testing.T) {
	// Break caught: leaving the first published artifact behind when the second publication fails.
	originalRename := shardRename
	renames := 0
	shardRename = func(oldPath, newPath string) error {
		renames++
		if renames == 2 {
			return errors.New("injected second rename failure")
		}
		return originalRename(oldPath, newPath)
	}
	t.Cleanup(func() { shardRename = originalRename })

	root := t.TempDir()
	_, err := writeNodeShard(root, "asset", 1, 42, scrub.ActionCounts{}, []entity.Node{{SourceID: "42"}}, jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, parquet.Config{Enabled: true})
	require.ErrorContains(t, err, "injected second rename failure")
	require.Empty(t, regularFiles(t, root))
}

func TestShardStageIsRemovedByCheckpointOrphanCleanup(t *testing.T) {
	// Break caught: a crash-leftover shard stage uses a name the checkpoint's strict cleanup inventory rejects.
	root := t.TempDir()
	state := checkpoint.State{
		Format: checkpoint.Format,
		Identity: checkpoint.Identity{
			Graphs:               []string{"asset"},
			EntityBatchSize:      1,
			ShardSize:            1,
			JSONLEnabled:         true,
			JSONLCodec:           string(jsonl.CodecNone),
			JSONLSchemaVersion:   jsonl.SchemaVersion,
			ParquetSchemaVersion: parquet.SchemaVersion,
		},
		Graphs: []checkpoint.GraphState{{
			Name:     "asset",
			Snapshot: dawgs.Snapshot{NodeCount: 1},
			Phase:    checkpoint.PhaseNodes,
		}},
	}
	store := checkpoint.Store{Root: root}
	require.NoError(t, store.Save(state))

	finalRelativePath := "graphs/asset/nodes/000001.jsonl"
	temporary, _, err := shardPaths(root, finalRelativePath)
	require.NoError(t, err)
	require.Len(t, temporary, 1)
	_, err = writeJSONLNodes(temporary[0], finalRelativePath, jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, []entity.Node{{SourceID: "1"}})
	require.NoError(t, err)

	stageRelativePath, err := filepath.Rel(root, temporary[0])
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(filepath.ToSlash(stageRelativePath), finalRelativePath+".tmp-"))

	require.NoError(t, store.CleanupOrphans(state))
	require.NoFileExists(t, temporary[0])
}

func regularFiles(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			paths = append(paths, path)
		}
		return nil
	}))
	return paths
}

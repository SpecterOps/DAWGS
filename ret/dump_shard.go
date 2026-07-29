package ret

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

var (
	writeJSONLNodes           = jsonl.WriteNodes
	writeParquetNodes         = parquet.WriteNodes
	writeJSONLRelationships   = jsonl.WriteRelationships
	writeParquetRelationships = parquet.WriteRelationships
	shardRename               = os.Rename
	shardRemove               = os.Remove
	shardNonce                uint64
)

func writeNodeShard(
	root, graphName string,
	index int,
	lastSourceID uint64,
	counts scrub.ActionCounts,
	nodes []entity.Node,
	jsonlConfig jsonl.Config,
	parquetConfig parquet.Config,
) (collection.NodeShard, error) {
	jsonlPath := ""
	if jsonlConfig.Enabled {
		jsonlPath = collection.NodeJSONLPath(graphName, index, jsonlConfig.Codec)
	}
	parquetPath := ""
	if parquetConfig.Enabled {
		parquetPath = collection.NodeParquetPath(graphName, index)
	}

	temporary, finals, err := shardPaths(root, jsonlPath, parquetPath)
	if err != nil {
		return collection.NodeShard{}, fmt.Errorf("%w: prepare node shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err)
	}
	cleanup := newShardCleanup(temporary)
	if err := cleanup.ensureDestinationsUnused(finals); err != nil {
		return collection.NodeShard{}, fmt.Errorf("%w: node shard %d for graph %q: %w", ErrDestinationExists, index, graphName, err)
	}

	var jsonlArtifact *jsonl.NodeArtifact
	if jsonlConfig.Enabled {
		artifact, err := writeJSONLNodes(temporary[0], jsonlPath, jsonlConfig, nodes)
		if err != nil {
			return collection.NodeShard{}, cleanup.fail(fmt.Errorf("%w: write JSONL node shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err))
		}
		if artifact.Count != int64(len(nodes)) {
			return collection.NodeShard{}, cleanup.fail(fmt.Errorf("%w: JSONL node shard %d for graph %q count %d does not match %d", ErrArtifactIntegrity, index, graphName, artifact.Count, len(nodes)))
		}
		jsonlArtifact = &artifact
	}

	var parquetArtifact *parquet.NodeArtifact
	if parquetConfig.Enabled {
		temporaryIndex := len(temporary) - 1
		artifact, err := writeParquetNodes(temporary[temporaryIndex], parquetPath, parquetConfig, nodes)
		if err != nil {
			return collection.NodeShard{}, cleanup.fail(fmt.Errorf("%w: write Parquet node shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err))
		}
		if artifact.Count != int64(len(nodes)) {
			return collection.NodeShard{}, cleanup.fail(fmt.Errorf("%w: Parquet node shard %d for graph %q count %d does not match %d", ErrArtifactIntegrity, index, graphName, artifact.Count, len(nodes)))
		}
		parquetArtifact = &artifact
	}

	if err := cleanup.publish(finals); err != nil {
		return collection.NodeShard{}, cleanup.fail(fmt.Errorf("%w: publish node shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err))
	}

	return collection.NodeShard{Index: index, Count: int64(len(nodes)), LastSourceID: lastSourceID, ScrubCounts: counts, JSONL: jsonlArtifact, Parquet: parquetArtifact}, nil
}

func writeRelationshipShard(
	root, graphName string,
	index int,
	lastSourceID uint64,
	counts scrub.ActionCounts,
	relationships []entity.Relationship,
	jsonlConfig jsonl.Config,
	parquetConfig parquet.Config,
) (collection.RelationshipShard, error) {
	jsonlPath := ""
	if jsonlConfig.Enabled {
		jsonlPath = collection.RelationshipJSONLPath(graphName, index, jsonlConfig.Codec)
	}
	parquetPath := ""
	if parquetConfig.Enabled {
		parquetPath = collection.RelationshipParquetPath(graphName, index)
	}

	temporary, finals, err := shardPaths(root, jsonlPath, parquetPath)
	if err != nil {
		return collection.RelationshipShard{}, fmt.Errorf("%w: prepare relationship shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err)
	}
	cleanup := newShardCleanup(temporary)
	if err := cleanup.ensureDestinationsUnused(finals); err != nil {
		return collection.RelationshipShard{}, fmt.Errorf("%w: relationship shard %d for graph %q: %w", ErrDestinationExists, index, graphName, err)
	}

	var jsonlArtifact *jsonl.RelationshipArtifact
	if jsonlConfig.Enabled {
		artifact, err := writeJSONLRelationships(temporary[0], jsonlPath, jsonlConfig, relationships)
		if err != nil {
			return collection.RelationshipShard{}, cleanup.fail(fmt.Errorf("%w: write JSONL relationship shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err))
		}
		if artifact.Count != int64(len(relationships)) {
			return collection.RelationshipShard{}, cleanup.fail(fmt.Errorf("%w: JSONL relationship shard %d for graph %q count %d does not match %d", ErrArtifactIntegrity, index, graphName, artifact.Count, len(relationships)))
		}
		jsonlArtifact = &artifact
	}

	var parquetArtifact *parquet.RelationshipArtifact
	if parquetConfig.Enabled {
		temporaryIndex := len(temporary) - 1
		artifact, err := writeParquetRelationships(temporary[temporaryIndex], parquetPath, parquetConfig, relationships)
		if err != nil {
			return collection.RelationshipShard{}, cleanup.fail(fmt.Errorf("%w: write Parquet relationship shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err))
		}
		if artifact.Count != int64(len(relationships)) {
			return collection.RelationshipShard{}, cleanup.fail(fmt.Errorf("%w: Parquet relationship shard %d for graph %q count %d does not match %d", ErrArtifactIntegrity, index, graphName, artifact.Count, len(relationships)))
		}
		parquetArtifact = &artifact
	}

	if err := cleanup.publish(finals); err != nil {
		return collection.RelationshipShard{}, cleanup.fail(fmt.Errorf("%w: publish relationship shard %d for graph %q: %w", ErrArtifactIntegrity, index, graphName, err))
	}

	return collection.RelationshipShard{Index: index, Count: int64(len(relationships)), LastSourceID: lastSourceID, ScrubCounts: counts, JSONL: jsonlArtifact, Parquet: parquetArtifact}, nil
}

type shardCleanup struct {
	temporary []string
	published []string
}

func newShardCleanup(temporary []string) *shardCleanup {
	return &shardCleanup{temporary: temporary}
}

func (s *shardCleanup) ensureDestinationsUnused(finals []string) error {
	for _, final := range finals {
		if _, err := os.Lstat(final); err == nil {
			return fmt.Errorf("artifact already exists: %s", final)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect destination %s: %w", final, err)
		}
	}
	return nil
}

func (s *shardCleanup) publish(finals []string) error {
	for index, temporary := range s.temporary {
		if err := shardRename(temporary, finals[index]); err != nil {
			return err
		}
		s.published = append(s.published, finals[index])
	}
	return nil
}

func (s *shardCleanup) fail(primary error) error {
	errorsToJoin := []error{primary}
	for _, filename := range append(append([]string(nil), s.temporary...), s.published...) {
		if err := shardRemove(filename); err != nil && !errors.Is(err, os.ErrNotExist) {
			errorsToJoin = append(errorsToJoin, fmt.Errorf("cleanup shard artifact %s: %w", filename, err))
		}
	}
	return errors.Join(errorsToJoin...)
}

func shardPaths(root string, relativePaths ...string) ([]string, []string, error) {
	finals := make([]string, 0, len(relativePaths))
	temporary := make([]string, 0, len(relativePaths))
	nonce := atomic.AddUint64(&shardNonce, 1)
	for _, relativePath := range relativePaths {
		if relativePath == "" {
			continue
		}
		final, err := collection.SafeJoin(root, relativePath)
		if err != nil {
			return nil, nil, err
		}
		if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
			return nil, nil, fmt.Errorf("create artifact directory: %w", err)
		}
		finals = append(finals, final)
		temporary = append(temporary, fmt.Sprintf("%s.tmp-%d", final, nonce))
	}
	if len(finals) == 0 {
		return nil, nil, fmt.Errorf("no shard output is enabled")
	}
	return temporary, finals, nil
}

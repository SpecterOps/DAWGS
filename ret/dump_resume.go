package ret

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/specterops/dawgs/ret/checkpoint"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/scrub"
)

func (s *dumpRunner) runResume() (DumpResult, error) {
	if err := s.prepareResume(); err != nil {
		return DumpResult{}, err
	}

	for index, graphName := range s.config.Graphs {
		if err := s.ctx.Err(); err != nil {
			return DumpResult{}, fmt.Errorf("resume dump graph %q: %w", graphName, err)
		}

		var runtime *dumpGraphRuntime
		var err error
		if index < len(s.state.Graphs) {
			runtime, err = s.reconstructGraph(index)
		} else {
			runtime, err = s.startFreshGraph(graphName)
		}
		if err != nil {
			return DumpResult{}, err
		}
		if err := s.processGraph(index, runtime); err != nil {
			return DumpResult{}, err
		}
	}

	return s.finalize()
}

func (s *dumpRunner) prepareResume() error {
	rootInfo, err := os.Lstat(s.config.Directory)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %s", ErrCheckpointMissing, s.config.Directory)
	}
	if err != nil {
		return fmt.Errorf("inspect resume destination: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: resume destination is not a non-symlink directory", ErrInvalidCollection)
	}

	manifestPath := filepath.Join(s.config.Directory, collection.ManifestName)
	if _, err := os.Lstat(manifestPath); err == nil {
		return fmt.Errorf("%w: resume destination already contains %s", ErrDestinationExists, collection.ManifestName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect resume manifest: %w", err)
	}

	state, exists, err := s.store.Load()
	if err != nil {
		return fmt.Errorf("%w: load resume checkpoint: %w", ErrInvalidCollection, err)
	}
	if !exists {
		return fmt.Errorf("%w: %s", ErrCheckpointMissing, filepath.Join(s.config.Directory, checkpoint.FileName))
	}
	if err := checkpoint.ValidateIdentity(s.identity, state.Identity); err != nil {
		return fmt.Errorf("%w: resume checkpoint identity: %w", ErrInvalidConfig, err)
	}
	if err := s.validateResumeCounts(state); err != nil {
		return err
	}
	if err := s.store.CleanupOrphans(state); err != nil {
		return fmt.Errorf("%w: clean resume crash artifacts: %w", ErrInvalidCollection, err)
	}
	s.state = state
	return nil
}

func (s *dumpRunner) validateResumeCounts(state checkpoint.State) error {
	for _, graphState := range state.Graphs {
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("validate resume graph %q counts: %w", graphState.Name, err)
		}
		source, err := dawgs.NewSource(s.database, graphState.Name, s.config.EntityBatchSize)
		if err != nil {
			return fmt.Errorf("prepare resume count source for graph %q: %w", graphState.Name, err)
		}
		current, err := source.Snapshot(s.ctx)
		if err != nil {
			return fmt.Errorf("validate resume graph %q counts: %w", graphState.Name, err)
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("validate resume graph %q counts: %w", graphState.Name, err)
		}
		if current != graphState.Snapshot {
			return fmt.Errorf(
				"%w: graph %q got nodes=%d relationships=%d want nodes=%d relationships=%d",
				ErrSourceCountChanged,
				graphState.Name,
				current.NodeCount,
				current.RelationshipCount,
				graphState.Snapshot.NodeCount,
				graphState.Snapshot.RelationshipCount,
			)
		}
	}
	return nil
}

func (s *dumpRunner) reconstructGraph(index int) (*dumpGraphRuntime, error) {
	graphState := &s.state.Graphs[index]
	source, err := dawgs.NewSource(s.database, graphState.Name, s.config.EntityBatchSize)
	if err != nil {
		return nil, fmt.Errorf("prepare resumed dump source for graph %q: %w", graphState.Name, err)
	}
	source.SetNodeCursor(graphState.NodeCursor)
	source.SetRelationshipCursor(graphState.RelationshipCursor)

	runtime := &dumpGraphRuntime{
		source:           source,
		builder:          metrics.NewBuilder(),
		catalog:          newDumpKindCatalog(),
		totalScrubCounts: scrub.ActionCounts{},
	}
	for _, shard := range graphState.NodeShards {
		if err := s.reconstructNodeShard(graphState.Name, shard, runtime); err != nil {
			return nil, err
		}
		runtime.totalScrubCounts.Add(shard.ScrubCounts)
	}
	for _, shard := range graphState.RelationshipShards {
		if err := s.reconstructRelationshipShard(graphState.Name, shard, runtime); err != nil {
			return nil, err
		}
		runtime.totalScrubCounts.Add(shard.ScrubCounts)
	}
	return runtime, nil
}

func (s *dumpRunner) reconstructNodeShard(
	graphName string,
	shard collection.NodeShard,
	runtime *dumpGraphRuntime,
) error {
	visit := func(node entity.Node) error {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if err := runtime.builder.ObserveNode(node); err != nil {
			return err
		}
		runtime.catalog.observeNode(node)
		runtime.nodeCount++
		return nil
	}

	var err error
	switch {
	case shard.JSONL != nil:
		err = collection.ReadJSONLNodes(s.config.Directory, *shard.JSONL, visit)
	case shard.Parquet != nil:
		err = collection.ReadParquetNodes(s.config.Directory, *shard.Parquet, visit)
	default:
		err = errors.New("committed node shard has no concrete artifact")
	}
	if err != nil {
		if contextErr := s.ctx.Err(); contextErr != nil {
			return fmt.Errorf("reconstruct graph %q node shard %d: %w", graphName, shard.Index, contextErr)
		}
		return fmt.Errorf("%w: reconstruct graph %q node shard %d: %w", ErrArtifactIntegrity, graphName, shard.Index, err)
	}
	return nil
}

func (s *dumpRunner) reconstructRelationshipShard(
	graphName string,
	shard collection.RelationshipShard,
	runtime *dumpGraphRuntime,
) error {
	visit := func(relationship entity.Relationship) error {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if err := runtime.builder.ObserveRelationship(relationship); err != nil {
			return err
		}
		runtime.catalog.observeRelationship(relationship)
		runtime.relationshipCount++
		return nil
	}

	var err error
	switch {
	case shard.JSONL != nil:
		err = collection.ReadJSONLRelationships(s.config.Directory, *shard.JSONL, visit)
	case shard.Parquet != nil:
		err = collection.ReadParquetRelationships(s.config.Directory, *shard.Parquet, visit)
	default:
		err = errors.New("committed relationship shard has no concrete artifact")
	}
	if err != nil {
		if contextErr := s.ctx.Err(); contextErr != nil {
			return fmt.Errorf("reconstruct graph %q relationship shard %d: %w", graphName, shard.Index, contextErr)
		}
		return fmt.Errorf("%w: reconstruct graph %q relationship shard %d: %w", ErrArtifactIntegrity, graphName, shard.Index, err)
	}
	return nil
}

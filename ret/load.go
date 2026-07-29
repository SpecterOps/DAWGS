package ret

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/observe"
)

const (
	loadOperationName      = "load"
	loadNodesPhase         = "nodes"
	loadRelationshipsPhase = "relationships"
)

type replayGraphFunc func(
	context.Context,
	string,
	collection.Graph,
	func(entity.Node) error,
	func(entity.Relationship) error,
) error

// Load validates and replays every JSONL graph in a collection into empty
// target graphs.
func Load(ctx context.Context, database graph.Database, config LoadConfig) (LoadResult, error) {
	return loadWithReplay(ctx, database, config, collection.ReplayGraph)
}

func loadWithReplay(
	ctx context.Context,
	database graph.Database,
	config LoadConfig,
	replay replayGraphFunc,
) (result LoadResult, resultErr error) {
	started := time.Now()
	observe.Emit(ctx, config.Observer, observe.OperationStarted{Operation: loadOperationName})
	defer func() {
		observe.Emit(ctx, config.Observer, observe.OperationCompleted{
			Operation: loadOperationName,
			Duration:  time.Since(started),
			Err:       resultErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return LoadResult{}, fmt.Errorf("load: %w", err)
	}
	if err := config.Validate(); err != nil {
		return LoadResult{}, err
	}

	verification, err := collection.VerifyJSONLForLoad(ctx, config.Directory, config.Observer)
	if err != nil {
		return LoadResult{}, fmt.Errorf("%w: %w", ErrCollectionNotLoadable, err)
	}
	if err := ctx.Err(); err != nil {
		return LoadResult{}, fmt.Errorf("load collection preflight: %w", err)
	}

	targets := make([]*dawgs.Target, len(verification.Manifest.Graphs))
	for index, graphEntry := range verification.Manifest.Graphs {
		target, err := dawgs.NewTarget(database, graphEntry.Name, config.BatchSize)
		if err != nil {
			return LoadResult{}, fmt.Errorf("prepare load target for graph %q: %w", graphEntry.Name, err)
		}
		targets[index] = target
	}
	for index, target := range targets {
		graphName := verification.Manifest.Graphs[index].Name
		if err := target.RequireEmpty(ctx); err != nil {
			if errors.Is(err, dawgs.ErrTargetNotEmpty) {
				return LoadResult{}, fmt.Errorf("%w: graph %q: %w", ErrNonEmptyTarget, graphName, err)
			}
			return LoadResult{}, fmt.Errorf("load graph %q emptiness check: %w", graphName, err)
		}
		if err := ctx.Err(); err != nil {
			return LoadResult{}, fmt.Errorf("load graph %q emptiness check: %w", graphName, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return LoadResult{}, fmt.Errorf("load target emptiness checks: %w", err)
	}
	for index, target := range targets {
		graphEntry := verification.Manifest.Graphs[index]
		if err := ctx.Err(); err != nil {
			return LoadResult{}, fmt.Errorf("load graph %q schema phase: %w", graphEntry.Name, err)
		}
		if err := target.AssertSchema(ctx, graphEntry.KindCatalog); err != nil {
			return LoadResult{}, fmt.Errorf("load graph %q schema phase: %w", graphEntry.Name, err)
		}
		if err := ctx.Err(); err != nil {
			return LoadResult{}, fmt.Errorf("load graph %q schema phase: %w", graphEntry.Name, err)
		}
	}

	result.GraphCount = len(verification.Manifest.Graphs)
	for index, graphEntry := range verification.Manifest.Graphs {
		nodes, relationships, err := loadGraph(ctx, config, graphEntry, targets[index], replay)
		if err != nil {
			return LoadResult{}, err
		}
		result.NodeCount += nodes
		result.RelationshipCount += relationships
	}
	if err := ctx.Err(); err != nil {
		return LoadResult{}, fmt.Errorf("load completion: %w", err)
	}
	return result, nil
}

func loadGraph(
	ctx context.Context,
	config LoadConfig,
	graphEntry collection.Graph,
	target *dawgs.Target,
	replay replayGraphFunc,
) (nodeCount int64, relationshipCount int64, resultErr error) {
	graphStarted := time.Now()
	observe.Emit(ctx, config.Observer, observe.GraphStarted{
		Operation: loadOperationName,
		Graph:     graphEntry.Name,
	})
	if err := ctx.Err(); err != nil {
		return 0, 0, partialLoadError(graphEntry.Name, loadNodesPhase, err)
	}

	resolver := dawgs.NewResolver(graphEntry.NodeCount)
	pendingNodes := make([]entity.Node, 0, config.BatchSize)
	pendingRelationships := make([]entity.Relationship, 0, config.BatchSize)
	nodesCompleted := false
	relationshipsStarted := false

	observe.Emit(ctx, config.Observer, observe.PhaseStarted{
		Operation: loadOperationName,
		Graph:     graphEntry.Name,
		Phase:     loadNodesPhase,
		Completed: 0,
		Total:     graphEntry.NodeCount,
	})
	if err := ctx.Err(); err != nil {
		return 0, 0, partialLoadError(graphEntry.Name, loadNodesPhase, err)
	}

	flushNodes := func() error {
		if len(pendingNodes) == 0 {
			return nil
		}
		if err := target.CreateNodes(ctx, pendingNodes, resolver); err != nil {
			return err
		}
		nodeCount += int64(len(pendingNodes))
		pendingNodes = pendingNodes[:0]
		observe.Emit(ctx, config.Observer, observe.PhaseProgress{
			Operation: loadOperationName,
			Graph:     graphEntry.Name,
			Phase:     loadNodesPhase,
			Completed: nodeCount,
			Total:     graphEntry.NodeCount,
		})
		return ctx.Err()
	}
	finishNodes := func() error {
		if nodesCompleted {
			return nil
		}
		if err := flushNodes(); err != nil {
			return err
		}
		if nodeCount != graphEntry.NodeCount {
			return fmt.Errorf("count mismatch: got %d want %d", nodeCount, graphEntry.NodeCount)
		}
		nodesCompleted = true
		observe.Emit(ctx, config.Observer, observe.PhaseCompleted{
			Operation: loadOperationName,
			Graph:     graphEntry.Name,
			Phase:     loadNodesPhase,
			Completed: nodeCount,
			Duration:  time.Since(graphStarted),
		})
		if err := ctx.Err(); err != nil {
			return err
		}
		relationshipsStarted = true
		observe.Emit(ctx, config.Observer, observe.PhaseStarted{
			Operation: loadOperationName,
			Graph:     graphEntry.Name,
			Phase:     loadRelationshipsPhase,
			Completed: 0,
			Total:     graphEntry.RelationshipCount,
		})
		return ctx.Err()
	}
	flushRelationships := func() error {
		if len(pendingRelationships) == 0 {
			return nil
		}
		if err := target.CreateRelationships(ctx, pendingRelationships, resolver); err != nil {
			return err
		}
		relationshipCount += int64(len(pendingRelationships))
		pendingRelationships = pendingRelationships[:0]
		observe.Emit(ctx, config.Observer, observe.PhaseProgress{
			Operation: loadOperationName,
			Graph:     graphEntry.Name,
			Phase:     loadRelationshipsPhase,
			Completed: relationshipCount,
			Total:     graphEntry.RelationshipCount,
		})
		return ctx.Err()
	}

	err := replay(
		ctx,
		config.Directory,
		graphEntry,
		func(node entity.Node) error {
			pendingNodes = append(pendingNodes, node)
			if len(pendingNodes) == config.BatchSize {
				return flushNodes()
			}
			return nil
		},
		func(relationship entity.Relationship) error {
			if err := finishNodes(); err != nil {
				return err
			}
			pendingRelationships = append(pendingRelationships, relationship)
			if len(pendingRelationships) == config.BatchSize {
				return flushRelationships()
			}
			return nil
		},
	)
	if err != nil {
		phase := loadNodesPhase
		if relationshipsStarted {
			phase = loadRelationshipsPhase
		}
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, phase, err)
	}
	if err := ctx.Err(); err != nil {
		phase := loadNodesPhase
		if relationshipsStarted {
			phase = loadRelationshipsPhase
		}
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, phase, err)
	}
	if err := finishNodes(); err != nil {
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, loadNodesPhase, err)
	}
	if err := flushRelationships(); err != nil {
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, loadRelationshipsPhase, err)
	}
	if relationshipCount != graphEntry.RelationshipCount {
		return nodeCount, relationshipCount, partialLoadError(
			graphEntry.Name,
			loadRelationshipsPhase,
			fmt.Errorf("count mismatch: got %d want %d", relationshipCount, graphEntry.RelationshipCount),
		)
	}
	if err := ctx.Err(); err != nil {
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, loadRelationshipsPhase, err)
	}

	observe.Emit(ctx, config.Observer, observe.PhaseCompleted{
		Operation: loadOperationName,
		Graph:     graphEntry.Name,
		Phase:     loadRelationshipsPhase,
		Completed: relationshipCount,
		Duration:  time.Since(graphStarted),
	})
	if err := ctx.Err(); err != nil {
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, loadRelationshipsPhase, err)
	}
	observe.Emit(ctx, config.Observer, observe.GraphCompleted{
		Operation:     loadOperationName,
		Graph:         graphEntry.Name,
		Nodes:         nodeCount,
		Relationships: relationshipCount,
		Duration:      time.Since(graphStarted),
	})
	if err := ctx.Err(); err != nil {
		return nodeCount, relationshipCount, partialLoadError(graphEntry.Name, loadRelationshipsPhase, err)
	}
	return nodeCount, relationshipCount, nil
}

func partialLoadError(graphName, phase string, err error) error {
	return fmt.Errorf(
		"load graph %q %s phase failed; partial graph must be cleared before retry: %w",
		graphName,
		phase,
		err,
	)
}

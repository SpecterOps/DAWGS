package ret

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/observe"
)

const (
	verifyDatabaseOperationName      = "verify_database"
	verifyDatabaseNodesPhase         = "nodes"
	verifyDatabaseRelationshipsPhase = "relationships"
)

// VerifyDatabase compares every graph in a collection manifest with its
// current database contents. It reads and validates only manifest.json; the
// collection's JSONL and Parquet artifacts are not opened.
func VerifyDatabase(
	ctx context.Context,
	database graph.Database,
	config VerifyDatabaseConfig,
) (result VerifyDatabaseResult, resultErr error) {
	started := time.Now()
	observe.Emit(ctx, config.Observer, observe.OperationStarted{Operation: verifyDatabaseOperationName})
	defer func() {
		observe.Emit(ctx, config.Observer, observe.OperationCompleted{
			Operation: verifyDatabaseOperationName,
			Duration:  time.Since(started),
			Err:       resultErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return VerifyDatabaseResult{}, fmt.Errorf("verify database: %w", err)
	}
	if err := config.Validate(); err != nil {
		return VerifyDatabaseResult{}, err
	}
	manifest, err := collection.Read(config.Directory)
	if err != nil {
		return VerifyDatabaseResult{}, fmt.Errorf("%w: %w", ErrInvalidCollection, err)
	}
	if err := ctx.Err(); err != nil {
		return VerifyDatabaseResult{}, fmt.Errorf("verify database collection preflight: %w", err)
	}

	differences := make([]string, 0)
	for _, graphEntry := range manifest.Graphs {
		graphResult, graphDifferences, err := verifyDatabaseGraph(ctx, database, config, graphEntry)
		if err != nil {
			return VerifyDatabaseResult{}, err
		}
		result.GraphCount++
		result.NodeCount += graphResult.nodes
		result.RelationshipCount += graphResult.relationships
		differences = append(differences, graphDifferences...)
	}
	if err := ctx.Err(); err != nil {
		return VerifyDatabaseResult{}, fmt.Errorf("verify database completion: %w", err)
	}
	if len(differences) != 0 {
		return VerifyDatabaseResult{}, fmt.Errorf("%w: %s", ErrMetricsMismatch, strings.Join(differences, "; "))
	}

	return result, nil
}

type verifyDatabaseGraphResult struct {
	nodes         int64
	relationships int64
}

func verifyDatabaseGraph(
	ctx context.Context,
	database graph.Database,
	config VerifyDatabaseConfig,
	graphEntry collection.Graph,
) (result verifyDatabaseGraphResult, differences []string, resultErr error) {
	graphStarted := time.Now()
	observe.Emit(ctx, config.Observer, observe.GraphStarted{
		Operation: verifyDatabaseOperationName,
		Graph:     graphEntry.Name,
	})
	if err := ctx.Err(); err != nil {
		return result, nil, verifyDatabaseGraphError(graphEntry.Name, err)
	}

	source, err := dawgs.NewSource(database, graphEntry.Name, config.BatchSize)
	if err != nil {
		return result, nil, fmt.Errorf("prepare database verification source for graph %q: %w", graphEntry.Name, err)
	}
	builder := metrics.NewBuilder()
	catalog := newVerifyDatabaseKindCatalog()

	if result.nodes, err = verifyDatabaseNodes(ctx, source, config.Observer, graphEntry, builder, catalog); err != nil {
		return result, nil, err
	}
	if result.relationships, err = verifyDatabaseRelationships(ctx, source, config.Observer, graphEntry, builder, catalog); err != nil {
		return result, nil, err
	}
	if err := ctx.Err(); err != nil {
		return result, nil, verifyDatabaseGraphError(graphEntry.Name, err)
	}

	if result.nodes != graphEntry.NodeCount {
		differences = append(differences, fmt.Sprintf(
			"graph %q node count differs: expected %d, actual %d",
			graphEntry.Name, graphEntry.NodeCount, result.nodes,
		))
	}
	if result.relationships != graphEntry.RelationshipCount {
		differences = append(differences, fmt.Sprintf(
			"graph %q relationship count differs: expected %d, actual %d",
			graphEntry.Name, graphEntry.RelationshipCount, result.relationships,
		))
	}
	if !slices.Equal(graphEntry.KindCatalog, catalog.values) {
		differences = append(differences, fmt.Sprintf("graph %q kind catalog differs", graphEntry.Name))
	}
	if err := metrics.Compare(graphEntry.Metrics, builder.Finalize()); err != nil {
		differences = append(differences, fmt.Sprintf("graph %q: %v", graphEntry.Name, err))
	}

	observe.Emit(ctx, config.Observer, observe.GraphCompleted{
		Operation:     verifyDatabaseOperationName,
		Graph:         graphEntry.Name,
		Nodes:         result.nodes,
		Relationships: result.relationships,
		Duration:      time.Since(graphStarted),
	})
	if err := ctx.Err(); err != nil {
		return result, nil, verifyDatabaseGraphError(graphEntry.Name, err)
	}
	return result, differences, nil
}

func verifyDatabaseNodes(
	ctx context.Context,
	source *dawgs.Source,
	observer observe.Observer,
	graphEntry collection.Graph,
	builder *metrics.Builder,
	catalog *verifyDatabaseKindCatalog,
) (int64, error) {
	phaseStarted := time.Now()
	observe.Emit(ctx, observer, observe.PhaseStarted{
		Operation: verifyDatabaseOperationName,
		Graph:     graphEntry.Name,
		Phase:     verifyDatabaseNodesPhase,
		Completed: 0,
		Total:     graphEntry.NodeCount,
	})
	if err := ctx.Err(); err != nil {
		return 0, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
	}

	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
		}
		batch, err := source.NextNodes(ctx)
		if err != nil {
			return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
		}
		if err := ctx.Err(); err != nil {
			return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
		}
		if len(batch.Entities) == 0 {
			break
		}
		for _, node := range batch.Entities {
			if err := observeVerifyDatabaseNode(ctx, observer, graphEntry, builder, catalog, node, &count); err != nil {
				return count, err
			}
		}
	}

	observe.Emit(ctx, observer, observe.PhaseCompleted{
		Operation: verifyDatabaseOperationName,
		Graph:     graphEntry.Name,
		Phase:     verifyDatabaseNodesPhase,
		Completed: count,
		Duration:  time.Since(phaseStarted),
	})
	if err := ctx.Err(); err != nil {
		return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
	}
	return count, nil
}

func observeVerifyDatabaseNode(
	ctx context.Context,
	observer observe.Observer,
	graphEntry collection.Graph,
	builder *metrics.Builder,
	catalog *verifyDatabaseKindCatalog,
	node entity.Node,
	count *int64,
) error {
	if err := ctx.Err(); err != nil {
		return verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
	}
	if err := builder.ObserveNode(node); err != nil {
		return fmt.Errorf("verify database graph %q node metrics: %w", graphEntry.Name, err)
	}
	catalog.observeNode(node)
	*count += 1
	observe.Emit(ctx, observer, observe.PhaseProgress{
		Operation: verifyDatabaseOperationName,
		Graph:     graphEntry.Name,
		Phase:     verifyDatabaseNodesPhase,
		Completed: *count,
		Total:     graphEntry.NodeCount,
	})
	if err := ctx.Err(); err != nil {
		return verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseNodesPhase, err)
	}
	return nil
}

func verifyDatabaseRelationships(
	ctx context.Context,
	source *dawgs.Source,
	observer observe.Observer,
	graphEntry collection.Graph,
	builder *metrics.Builder,
	catalog *verifyDatabaseKindCatalog,
) (int64, error) {
	phaseStarted := time.Now()
	observe.Emit(ctx, observer, observe.PhaseStarted{
		Operation: verifyDatabaseOperationName,
		Graph:     graphEntry.Name,
		Phase:     verifyDatabaseRelationshipsPhase,
		Completed: 0,
		Total:     graphEntry.RelationshipCount,
	})
	if err := ctx.Err(); err != nil {
		return 0, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
	}

	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
		}
		batch, err := source.NextRelationships(ctx)
		if err != nil {
			return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
		}
		if err := ctx.Err(); err != nil {
			return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
		}
		if len(batch.Entities) == 0 {
			break
		}
		for _, relationship := range batch.Entities {
			if err := ctx.Err(); err != nil {
				return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
			}
			if err := builder.ObserveRelationship(relationship); err != nil {
				return count, fmt.Errorf("verify database graph %q relationship metrics: %w", graphEntry.Name, err)
			}
			catalog.observeRelationship(relationship)
			count++
			observe.Emit(ctx, observer, observe.PhaseProgress{
				Operation: verifyDatabaseOperationName,
				Graph:     graphEntry.Name,
				Phase:     verifyDatabaseRelationshipsPhase,
				Completed: count,
				Total:     graphEntry.RelationshipCount,
			})
			if err := ctx.Err(); err != nil {
				return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
			}
		}
	}

	observe.Emit(ctx, observer, observe.PhaseCompleted{
		Operation: verifyDatabaseOperationName,
		Graph:     graphEntry.Name,
		Phase:     verifyDatabaseRelationshipsPhase,
		Completed: count,
		Duration:  time.Since(phaseStarted),
	})
	if err := ctx.Err(); err != nil {
		return count, verifyDatabasePhaseError(graphEntry.Name, verifyDatabaseRelationshipsPhase, err)
	}
	return count, nil
}

func verifyDatabaseGraphError(graphName string, err error) error {
	return fmt.Errorf("verify database graph %q: %w", graphName, err)
}

func verifyDatabasePhaseError(graphName, phase string, err error) error {
	return fmt.Errorf("verify database graph %q %s phase: %w", graphName, phase, err)
}

type verifyDatabaseKindCatalog struct {
	seen   map[string]struct{}
	values []string
}

func newVerifyDatabaseKindCatalog() *verifyDatabaseKindCatalog {
	return &verifyDatabaseKindCatalog{seen: make(map[string]struct{})}
}

func (s *verifyDatabaseKindCatalog) observeNode(node entity.Node) {
	for _, kind := range node.Kinds {
		s.add(kind)
	}
}

func (s *verifyDatabaseKindCatalog) observeRelationship(relationship entity.Relationship) {
	s.add(relationship.Kind)
}

func (s *verifyDatabaseKindCatalog) add(kind string) {
	if _, found := s.seen[kind]; found {
		return
	}
	s.seen[kind] = struct{}{}
	s.values = append(s.values, kind)
}

package ret

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/checkpoint"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

const dumpOperationName = "dump"

var writeCollection = collection.Write

// Dump exports the configured graphs into a validated collection.
func Dump(ctx context.Context, database graph.Database, config DumpConfig) (result DumpResult, resultErr error) {
	started := time.Now()
	observe.Emit(ctx, config.Observer, observe.OperationStarted{Operation: dumpOperationName})
	defer func() {
		observe.Emit(ctx, config.Observer, observe.OperationCompleted{
			Operation: dumpOperationName,
			Duration:  time.Since(started),
			Err:       resultErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return DumpResult{}, fmt.Errorf("dump: %w", err)
	}
	if err := config.Validate(); err != nil {
		return DumpResult{}, err
	}
	compiledScrubber, err := scrub.New(config.Scrub)
	if err != nil {
		return DumpResult{}, fmt.Errorf("%w: compile scrub configuration: %w", ErrInvalidConfig, err)
	}

	runner := dumpRunner{
		ctx:      ctx,
		database: database,
		config:   config,
		scrubber: compiledScrubber,
		store:    checkpoint.Store{Root: config.Directory},
		identity: dumpCheckpointIdentity(config, compiledScrubber),
	}
	if config.Resume {
		return runner.runResume()
	}
	return runner.runFresh()
}

type dumpRunner struct {
	ctx      context.Context
	database graph.Database
	config   DumpConfig
	scrubber *scrub.Scrubber
	store    checkpoint.Store
	identity checkpoint.Identity
	state    checkpoint.State
	graphs   []collection.Graph
}

type dumpGraphRuntime struct {
	source            *dawgs.Source
	builder           *metrics.Builder
	catalog           *dumpKindCatalog
	totalScrubCounts  scrub.ActionCounts
	nodeCount         int64
	relationshipCount int64
}

func (s *dumpRunner) runFresh() (DumpResult, error) {
	if err := s.prepareFreshDestination(); err != nil {
		return DumpResult{}, err
	}
	s.state = checkpoint.State{Format: checkpoint.Format, Identity: s.identity}

	for index, graphName := range s.config.Graphs {
		if err := s.ctx.Err(); err != nil {
			return DumpResult{}, fmt.Errorf("dump graph %q: %w", graphName, err)
		}
		runtime, err := s.startFreshGraph(graphName)
		if err != nil {
			return DumpResult{}, err
		}
		if err := s.processGraph(index, runtime); err != nil {
			return DumpResult{}, err
		}
	}

	return s.finalize()
}

func (s *dumpRunner) prepareFreshDestination() error {
	if _, err := os.Lstat(s.config.Directory); err == nil {
		return fmt.Errorf("%w: %s", ErrDestinationExists, s.config.Directory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect dump destination: %w", err)
	}
	if err := os.Mkdir(s.config.Directory, 0o755); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %s", ErrDestinationExists, s.config.Directory)
		}
		return fmt.Errorf("create dump destination: %w", err)
	}
	return nil
}

func (s *dumpRunner) startFreshGraph(graphName string) (*dumpGraphRuntime, error) {
	source, err := dawgs.NewSource(s.database, graphName, s.config.EntityBatchSize)
	if err != nil {
		return nil, fmt.Errorf("prepare dump source for graph %q: %w", graphName, err)
	}
	snapshot, err := source.Snapshot(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot dump graph %q: %w", graphName, err)
	}
	if err := s.ctx.Err(); err != nil {
		return nil, fmt.Errorf("snapshot dump graph %q: %w", graphName, err)
	}
	s.state.Graphs = append(s.state.Graphs, checkpoint.GraphState{
		Name:     graphName,
		Snapshot: snapshot,
		Phase:    checkpoint.PhaseNodes,
	})
	if err := s.store.Save(s.state); err != nil {
		return nil, fmt.Errorf("save initial checkpoint for graph %q: %w", graphName, err)
	}
	return &dumpGraphRuntime{
		source:           source,
		builder:          metrics.NewBuilder(),
		catalog:          newDumpKindCatalog(),
		totalScrubCounts: scrub.ActionCounts{},
	}, nil
}

func (s *dumpRunner) processGraph(index int, runtime *dumpGraphRuntime) error {
	graphState := &s.state.Graphs[index]
	graphStarted := time.Now()
	observe.Emit(s.ctx, s.config.Observer, observe.GraphStarted{
		Operation: dumpOperationName,
		Graph:     graphState.Name,
	})
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("dump graph %q: %w", graphState.Name, err)
	}

	if graphState.Phase == checkpoint.PhaseNodes {
		if err := s.processNodes(graphState, runtime); err != nil {
			return err
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q: %w", graphState.Name, err)
		}
	}
	if graphState.Phase == checkpoint.PhaseRelationships {
		if err := s.processRelationships(graphState, runtime); err != nil {
			return err
		}
	}
	if graphState.Phase != checkpoint.PhaseComplete {
		return fmt.Errorf("dump graph %q has unsupported checkpoint phase %q", graphState.Name, graphState.Phase)
	}
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("dump graph %q: %w", graphState.Name, err)
	}

	graph := collection.Graph{
		Name:               graphState.Name,
		NodeCount:          runtime.nodeCount,
		RelationshipCount:  runtime.relationshipCount,
		KindCatalog:        append([]string(nil), runtime.catalog.values...),
		NodeShards:         append([]collection.NodeShard(nil), graphState.NodeShards...),
		RelationshipShards: append([]collection.RelationshipShard(nil), graphState.RelationshipShards...),
		Metrics:            runtime.builder.Finalize(),
	}
	s.graphs = append(s.graphs, graph)
	observe.Emit(s.ctx, s.config.Observer, observe.GraphCompleted{
		Operation:     dumpOperationName,
		Graph:         graphState.Name,
		Nodes:         runtime.nodeCount,
		Relationships: runtime.relationshipCount,
		Duration:      time.Since(graphStarted),
	})
	return nil
}

func (s *dumpRunner) finalize() (DumpResult, error) {
	if err := s.recountEveryGraph(); err != nil {
		return DumpResult{}, err
	}
	manifest := s.manifest()
	if err := s.ctx.Err(); err != nil {
		return DumpResult{}, fmt.Errorf("publish dump manifest: %w", err)
	}
	if err := writeCollection(s.config.Directory, manifest); err != nil {
		return DumpResult{}, fmt.Errorf("publish dump manifest: %w", err)
	}
	if err := s.store.Remove(); err != nil {
		return DumpResult{}, fmt.Errorf("remove completed dump checkpoint: %w", err)
	}

	result := DumpResult{
		ManifestPath: filepath.Join(s.config.Directory, collection.ManifestName),
		GraphCount:   len(s.graphs),
	}
	for _, graph := range s.graphs {
		result.NodeCount += graph.NodeCount
		result.RelationshipCount += graph.RelationshipCount
	}
	return result, nil
}

func (s *dumpRunner) processNodes(graphState *checkpoint.GraphState, runtime *dumpGraphRuntime) error {
	phaseStarted := time.Now()
	observe.Emit(s.ctx, s.config.Observer, observe.PhaseStarted{
		Operation: dumpOperationName,
		Graph:     graphState.Name,
		Phase:     string(checkpoint.PhaseNodes),
		Completed: runtime.nodeCount,
		Total:     graphState.Snapshot.NodeCount,
	})

	var active []entity.Node
	activeCounts := scrub.ActionCounts{}
	var activeLastID uint64
	flush := func() error {
		if len(active) == 0 {
			return nil
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q nodes: %w", graphState.Name, err)
		}
		shard, err := writeNodeShard(
			s.config.Directory,
			graphState.Name,
			len(graphState.NodeShards)+1,
			activeLastID,
			activeCounts,
			active,
			s.config.JSONL,
			s.config.Parquet,
		)
		if err != nil {
			return err
		}
		graphState.NodeShards = append(graphState.NodeShards, shard)
		graphState.NodeCursor = shard.LastSourceID
		if err := s.store.Save(s.state); err != nil {
			return fmt.Errorf("checkpoint graph %q node shard %d: %w", graphState.Name, shard.Index, err)
		}
		emitNodeShardCommitted(s.ctx, s.config.Observer, graphState.Name, shard)
		active = nil
		activeCounts = scrub.ActionCounts{}
		activeLastID = 0
		return nil
	}

	for {
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q nodes: %w", graphState.Name, err)
		}
		batch, err := runtime.source.NextNodes(s.ctx)
		if err != nil {
			return fmt.Errorf("dump graph %q nodes: %w", graphState.Name, err)
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q nodes: %w", graphState.Name, err)
		}
		if len(batch.Entities) == 0 {
			break
		}
		for _, node := range batch.Entities {
			if err := s.ctx.Err(); err != nil {
				return fmt.Errorf("dump graph %q nodes: %w", graphState.Name, err)
			}
			sourceID, err := parseDumpSourceID(node.SourceID)
			if err != nil {
				return fmt.Errorf("dump graph %q node source ID: %w", graphState.Name, err)
			}
			counts := s.scrubber.Scrub(node.Properties)
			mergeDumpActionCounts(activeCounts, counts)
			mergeDumpActionCounts(runtime.totalScrubCounts, counts)
			if err := runtime.builder.ObserveNode(node); err != nil {
				return fmt.Errorf("dump graph %q node metrics: %w", graphState.Name, err)
			}
			runtime.catalog.observeNode(node)
			active = append(active, node)
			activeLastID = sourceID
			runtime.nodeCount++
			if runtime.nodeCount > graphState.Snapshot.NodeCount {
				return fmt.Errorf(
					"%w: graph %q got at least nodes=%d want nodes=%d",
					ErrSourceCountChanged,
					graphState.Name,
					runtime.nodeCount,
					graphState.Snapshot.NodeCount,
				)
			}
			observe.Emit(s.ctx, s.config.Observer, observe.PhaseProgress{
				Operation: dumpOperationName,
				Graph:     graphState.Name,
				Phase:     string(checkpoint.PhaseNodes),
				Completed: runtime.nodeCount,
				Total:     graphState.Snapshot.NodeCount,
			})
			if len(active) == s.config.ShardSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if runtime.nodeCount != graphState.Snapshot.NodeCount {
		return fmt.Errorf(
			"%w: graph %q got nodes=%d want nodes=%d",
			ErrSourceCountChanged,
			graphState.Name,
			runtime.nodeCount,
			graphState.Snapshot.NodeCount,
		)
	}
	if err := flush(); err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("dump graph %q nodes: %w", graphState.Name, err)
	}

	graphState.Phase = checkpoint.PhaseRelationships
	if err := s.store.Save(s.state); err != nil {
		return fmt.Errorf("checkpoint graph %q node phase completion: %w", graphState.Name, err)
	}
	observe.Emit(s.ctx, s.config.Observer, observe.PhaseCompleted{
		Operation: dumpOperationName,
		Graph:     graphState.Name,
		Phase:     string(checkpoint.PhaseNodes),
		Completed: runtime.nodeCount,
		Duration:  time.Since(phaseStarted),
	})
	return nil
}

func (s *dumpRunner) processRelationships(graphState *checkpoint.GraphState, runtime *dumpGraphRuntime) error {
	phaseStarted := time.Now()
	observe.Emit(s.ctx, s.config.Observer, observe.PhaseStarted{
		Operation: dumpOperationName,
		Graph:     graphState.Name,
		Phase:     string(checkpoint.PhaseRelationships),
		Completed: runtime.relationshipCount,
		Total:     graphState.Snapshot.RelationshipCount,
	})

	var active []entity.Relationship
	activeCounts := scrub.ActionCounts{}
	var activeLastID uint64
	flush := func() error {
		if len(active) == 0 {
			return nil
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q relationships: %w", graphState.Name, err)
		}
		shard, err := writeRelationshipShard(
			s.config.Directory,
			graphState.Name,
			len(graphState.RelationshipShards)+1,
			activeLastID,
			activeCounts,
			active,
			s.config.JSONL,
			s.config.Parquet,
		)
		if err != nil {
			return err
		}
		graphState.RelationshipShards = append(graphState.RelationshipShards, shard)
		graphState.RelationshipCursor = shard.LastSourceID
		if err := s.store.Save(s.state); err != nil {
			return fmt.Errorf("checkpoint graph %q relationship shard %d: %w", graphState.Name, shard.Index, err)
		}
		emitRelationshipShardCommitted(s.ctx, s.config.Observer, graphState.Name, shard)
		active = nil
		activeCounts = scrub.ActionCounts{}
		activeLastID = 0
		return nil
	}

	for {
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q relationships: %w", graphState.Name, err)
		}
		batch, err := runtime.source.NextRelationships(s.ctx)
		if err != nil {
			return fmt.Errorf("dump graph %q relationships: %w", graphState.Name, err)
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("dump graph %q relationships: %w", graphState.Name, err)
		}
		if len(batch.Entities) == 0 {
			break
		}
		for _, relationship := range batch.Entities {
			if err := s.ctx.Err(); err != nil {
				return fmt.Errorf("dump graph %q relationships: %w", graphState.Name, err)
			}
			sourceID, err := parseDumpSourceID(relationship.SourceID)
			if err != nil {
				return fmt.Errorf("dump graph %q relationship source ID: %w", graphState.Name, err)
			}
			counts := s.scrubber.Scrub(relationship.Properties)
			mergeDumpActionCounts(activeCounts, counts)
			mergeDumpActionCounts(runtime.totalScrubCounts, counts)
			if err := runtime.builder.ObserveRelationship(relationship); err != nil {
				return fmt.Errorf("dump graph %q relationship metrics: %w", graphState.Name, err)
			}
			runtime.catalog.observeRelationship(relationship)
			active = append(active, relationship)
			activeLastID = sourceID
			runtime.relationshipCount++
			if runtime.relationshipCount > graphState.Snapshot.RelationshipCount {
				return fmt.Errorf(
					"%w: graph %q got at least relationships=%d want relationships=%d",
					ErrSourceCountChanged,
					graphState.Name,
					runtime.relationshipCount,
					graphState.Snapshot.RelationshipCount,
				)
			}
			observe.Emit(s.ctx, s.config.Observer, observe.PhaseProgress{
				Operation: dumpOperationName,
				Graph:     graphState.Name,
				Phase:     string(checkpoint.PhaseRelationships),
				Completed: runtime.relationshipCount,
				Total:     graphState.Snapshot.RelationshipCount,
			})
			if len(active) == s.config.ShardSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if runtime.relationshipCount != graphState.Snapshot.RelationshipCount {
		return fmt.Errorf(
			"%w: graph %q got relationships=%d want relationships=%d",
			ErrSourceCountChanged,
			graphState.Name,
			runtime.relationshipCount,
			graphState.Snapshot.RelationshipCount,
		)
	}
	if err := flush(); err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("dump graph %q relationships: %w", graphState.Name, err)
	}

	graphState.Phase = checkpoint.PhaseComplete
	if err := s.store.Save(s.state); err != nil {
		return fmt.Errorf("checkpoint graph %q relationship phase completion: %w", graphState.Name, err)
	}
	observe.Emit(s.ctx, s.config.Observer, observe.PhaseCompleted{
		Operation: dumpOperationName,
		Graph:     graphState.Name,
		Phase:     string(checkpoint.PhaseRelationships),
		Completed: runtime.relationshipCount,
		Duration:  time.Since(phaseStarted),
	})
	return nil
}

func (s *dumpRunner) recountEveryGraph() error {
	for index, graphName := range s.config.Graphs {
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("recount dump graph %q: %w", graphName, err)
		}
		source, err := dawgs.NewSource(s.database, graphName, s.config.EntityBatchSize)
		if err != nil {
			return fmt.Errorf("prepare recount source for graph %q: %w", graphName, err)
		}
		current, err := source.Snapshot(s.ctx)
		if err != nil {
			return fmt.Errorf("recount dump graph %q: %w", graphName, err)
		}
		if err := s.ctx.Err(); err != nil {
			return fmt.Errorf("recount dump graph %q: %w", graphName, err)
		}
		snapshot := s.state.Graphs[index].Snapshot
		if current != snapshot {
			return fmt.Errorf(
				"%w: graph %q got nodes=%d relationships=%d want nodes=%d relationships=%d",
				ErrSourceCountChanged,
				graphName,
				current.NodeCount,
				current.RelationshipCount,
				snapshot.NodeCount,
				snapshot.RelationshipCount,
			)
		}
	}
	return nil
}

func (s *dumpRunner) manifest() collection.Manifest {
	outputs := collection.OutputConfig{}
	if s.config.JSONL.Enabled {
		outputs.JSONL = &collection.JSONLOutput{
			SchemaVersion: jsonl.SchemaVersion,
			Codec:         string(s.config.JSONL.Codec),
			Level:         s.config.JSONL.Level,
		}
	}
	if s.config.Parquet.Enabled {
		outputs.Parquet = &collection.ParquetOutput{SchemaVersion: parquet.SchemaVersion}
	}
	scrubMetadata := collection.ScrubMetadata{Enabled: s.config.Scrub.Enabled}
	if s.config.Scrub.Enabled {
		scrubMetadata.RulesFingerprint = s.scrubber.RulesFingerprint()
		scrubMetadata.SaltFingerprint = s.scrubber.SaltFingerprint()
	}
	return collection.Manifest{
		Format:    collection.Format,
		CreatedAt: time.Now().UTC(),
		Outputs:   outputs,
		Scrub:     scrubMetadata,
		Graphs:    append([]collection.Graph(nil), s.graphs...),
	}
}

func dumpCheckpointIdentity(config DumpConfig, compiled *scrub.Scrubber) checkpoint.Identity {
	identity := checkpoint.Identity{
		Graphs:               append([]string(nil), config.Graphs...),
		EntityBatchSize:      config.EntityBatchSize,
		ShardSize:            config.ShardSize,
		JSONLEnabled:         config.JSONL.Enabled,
		JSONLCodec:           string(config.JSONL.Codec),
		JSONLLevel:           config.JSONL.Level,
		ParquetEnabled:       config.Parquet.Enabled,
		JSONLSchemaVersion:   jsonl.SchemaVersion,
		ParquetSchemaVersion: parquet.SchemaVersion,
		ScrubEnabled:         config.Scrub.Enabled,
	}
	if config.Scrub.Enabled {
		identity.ScrubRulesFingerprint = compiled.RulesFingerprint()
		identity.ScrubSaltFingerprint = compiled.SaltFingerprint()
	}
	return identity
}

func parseDumpSourceID(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != value {
		return 0, fmt.Errorf("expected canonical positive Dawgs ID, got %q", value)
	}
	return parsed, nil
}

func mergeDumpActionCounts(target, source scrub.ActionCounts) {
	for action, count := range source {
		target[action] += count
	}
}

type dumpKindCatalog struct {
	seen   map[string]struct{}
	values []string
}

func newDumpKindCatalog() *dumpKindCatalog {
	return &dumpKindCatalog{seen: make(map[string]struct{})}
}

func (s *dumpKindCatalog) observeNode(node entity.Node) {
	for _, kind := range node.Kinds {
		s.add(kind)
	}
}

func (s *dumpKindCatalog) observeRelationship(relationship entity.Relationship) {
	s.add(relationship.Kind)
}

func (s *dumpKindCatalog) add(kind string) {
	if _, found := s.seen[kind]; found {
		return
	}
	s.seen[kind] = struct{}{}
	s.values = append(s.values, kind)
}

func emitNodeShardCommitted(ctx context.Context, observer observe.Observer, graphName string, shard collection.NodeShard) {
	event := observe.ShardCommitted{
		Graph:      graphName,
		EntityType: "node",
		Index:      shard.Index,
		Count:      shard.Count,
	}
	if shard.JSONL != nil {
		event.JSONLPath = shard.JSONL.Path
		event.JSONLBytes = shard.JSONL.StoredBytes
	}
	if shard.Parquet != nil {
		event.ParquetPath = shard.Parquet.Path
		event.ParquetBytes = shard.Parquet.StoredBytes
	}
	observe.Emit(ctx, observer, event)
}

func emitRelationshipShardCommitted(ctx context.Context, observer observe.Observer, graphName string, shard collection.RelationshipShard) {
	event := observe.ShardCommitted{
		Graph:      graphName,
		EntityType: "relationship",
		Index:      shard.Index,
		Count:      shard.Count,
	}
	if shard.JSONL != nil {
		event.JSONLPath = shard.JSONL.Path
		event.JSONLBytes = shard.JSONL.StoredBytes
	}
	if shard.Parquet != nil {
		event.ParquetPath = shard.Parquet.Path
		event.ParquetBytes = shard.Parquet.StoredBytes
	}
	observe.Emit(ctx, observer, event)
}

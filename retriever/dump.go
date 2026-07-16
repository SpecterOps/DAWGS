package retriever

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/specterops/dawgs/graph"
)

type DumpResult struct {
	Manifest     Manifest
	ManifestPath string
	NodeCount    int64
	EdgeCount    int64
}

type dumpOverrides struct {
	workspace collectionWorkspace
	publisher collectionPublisher
	nodeSink  fragmentSink[normalizedNode, FileManifest]
	edgeSink  fragmentSink[normalizedEdge, FileManifest]
}

func Dump(ctx context.Context, db graph.Database, driverName string, targets []GraphTarget, options DumpOptions) (DumpResult, error) {
	return runDump(ctx, newDatabaseGraphSource(db), driverName, targets, options, dumpOverrides{})
}

func runDump(ctx context.Context, source graphSource, driverName string, targets []GraphTarget, options DumpOptions, overrides dumpOverrides) (DumpResult, error) {
	if err := options.validate(); err != nil {
		return DumpResult{}, err
	}

	if len(targets) == 0 {
		return DumpResult{}, fmt.Errorf("at least one graph target is required")
	}

	startedAt := time.Now()
	slog.Info("retriever dump started",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.String("output_dir", options.OutputDir),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("shard_size", options.ShardSize),
		slog.String("compression", string(options.Compression)),
		slog.String("scrub", string(options.Scrub)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:   OperationDump,
		Message:     "retriever dump started",
		Driver:      driverName,
		GraphCount:  len(targets),
		OutputDir:   options.OutputDir,
		BatchSize:   options.BatchSize,
		ShardSize:   options.ShardSize,
		Compression: options.Compression,
		Scrub:       options.Scrub,
	})

	transform, err := newTransformSession(options)
	if err != nil {
		return DumpResult{}, err
	}

	slog.Info("retriever dump preparing output directory",
		slog.String("output_dir", options.OutputDir),
		slog.Bool("force", options.Force),
	)
	workspace := overrides.workspace
	if workspace == nil {
		workspace = newLocalCollectionWorkspace(options.OutputDir, options.Force)
	}
	if err := workspace.Prepare(ctx); err != nil {
		return DumpResult{}, err
	}

	slog.Info("retriever dump output directory ready",
		slog.String("output_dir", options.OutputDir),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationDump,
		Message:   "retriever dump output directory ready",
		OutputDir: options.OutputDir,
	})

	publisher := overrides.publisher
	if publisher == nil {
		publisher = newJSONLCollectionPublisher(workspace, driverName, options, transform.Metadata(), len(targets))
	}
	nodeSink := overrides.nodeSink
	if nodeSink == nil {
		nodeSink = newJSONLNodeSinkInWorkspace(options, workspace)
	}
	edgeSink := overrides.edgeSink
	if edgeSink == nil {
		edgeSink = newJSONLEdgeSinkInWorkspace(options, workspace)
	}

	var totalNodes, totalEdges int64

	for targetIndex, target := range targets {
		graphStartedAt := time.Now()
		slog.Info("retriever dump graph started",
			slog.String("graph", target.Name),
			slog.Int("graph_index", targetIndex+1),
			slog.Int("graph_count", len(targets)),
		)
		options.Progress.emit(ProgressEvent{
			Operation:  OperationDump,
			Message:    "retriever dump graph started",
			Graph:      target.Name,
			GraphIndex: targetIndex + 1,
			GraphCount: len(targets),
			OutputDir:  options.OutputDir,
		})

		graphEntry, schemaEntry, metricsEntry, err := dumpGraph(ctx, source, target, options, transform, nodeSink, edgeSink)
		if err != nil {
			return DumpResult{}, err
		}

		publisher.AddGraph(graphEntry, schemaEntry, metricsEntry)

		totalNodes += graphEntry.NodeCount
		totalEdges += graphEntry.EdgeCount

		slog.Info("retriever dump graph completed",
			slog.String("graph", target.Name),
			slog.Int64("node_count", graphEntry.NodeCount),
			slog.Int64("edge_count", graphEntry.EdgeCount),
			slog.Int("file_count", len(graphEntry.Files)),
			slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
		)
		options.Progress.emit(ProgressEvent{
			Operation:  OperationDump,
			Message:    "retriever dump graph completed",
			Graph:      target.Name,
			GraphIndex: targetIndex + 1,
			GraphCount: len(targets),
			OutputDir:  options.OutputDir,
			FileCount:  len(graphEntry.Files),
			NodeCount:  graphEntry.NodeCount,
			EdgeCount:  graphEntry.EdgeCount,
			Elapsed:    time.Since(graphStartedAt),
		})
	}

	slog.Info("retriever dump writing manifest",
		slog.String("output_dir", options.OutputDir),
		slog.Int64("node_count", totalNodes),
		slog.Int64("edge_count", totalEdges),
	)
	publication, err := publisher.Publish(ctx)
	if err != nil {
		return DumpResult{}, err
	}

	slog.Info("retriever dump completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.String("manifest", publication.Path),
		slog.Int64("node_count", totalNodes),
		slog.Int64("edge_count", totalEdges),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationDump,
		Message:    "retriever dump completed",
		Driver:     driverName,
		GraphCount: len(targets),
		OutputDir:  options.OutputDir,
		NodeCount:  totalNodes,
		EdgeCount:  totalEdges,
		Elapsed:    time.Since(startedAt),
	})

	return DumpResult{
		Manifest:     publication.Manifest,
		ManifestPath: publication.Path,
		NodeCount:    totalNodes,
		EdgeCount:    totalEdges,
	}, nil
}

func dumpGraph(ctx context.Context, source graphSource, target GraphTarget, options DumpOptions, transform transformSession, nodeSink fragmentSink[normalizedNode, FileManifest], edgeSink fragmentSink[normalizedEdge, FileManifest]) (GraphManifest, GraphSchemaMetadata, GraphMetrics, error) {
	targetGraph := graph.Graph{
		Name: target.Name,
	}

	countStartedAt := time.Now()
	slog.Info("retriever dump counting graph entities",
		slog.String("graph", target.Name),
	)
	entitySnapshot, err := source.Inventory(ctx, targetGraph)
	if err != nil {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
	}

	slog.Info("retriever dump graph counts ready",
		slog.String("graph", target.Name),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(countStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationDump,
		Message:   "retriever dump graph counts ready",
		Graph:     target.Name,
		NodeCount: entitySnapshot.NodeCount,
		EdgeCount: entitySnapshot.EdgeCount,
		Elapsed:   time.Since(countStartedAt),
	})

	if transform.NeedsPreparation() {
		scrubStartedAt := time.Now()
		slog.Info("retriever dump scrub pre-pass started",
			slog.String("graph", target.Name),
			slog.Int64("node_count", entitySnapshot.NodeCount),
			slog.Int("batch_size", options.BatchSize),
		)
		options.Progress.emit(ProgressEvent{
			Operation: OperationDump,
			Message:   "retriever dump scrub pre-pass started",
			Graph:     target.Name,
			Phase:     PhaseNodes,
			Planned:   entitySnapshot.NodeCount,
			BatchSize: options.BatchSize,
		})

		observedNodes, err := prepareTransformSession(ctx, source, targetGraph, options.BatchSize, transform, entitySnapshot, options.Progress, options.ProgressInterval)
		if err != nil {
			return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
		}

		slog.Info("retriever dump scrub pre-pass completed",
			slog.String("graph", target.Name),
			slog.Int64("processed", observedNodes),
			slog.Duration("wall_elapsed", time.Since(scrubStartedAt)),
		)
		options.Progress.emit(ProgressEvent{
			Operation:         OperationDump,
			Message:           "retriever dump scrub pre-pass completed",
			Graph:             target.Name,
			Phase:             PhaseNodes,
			Processed:         observedNodes,
			Planned:           entitySnapshot.NodeCount,
			Elapsed:           time.Since(scrubStartedAt),
			EntitiesPerSecond: perSecond(observedNodes, time.Since(scrubStartedAt)),
		})
	}

	graphEntry := GraphManifest{
		Name:      target.Name,
		NodeCount: entitySnapshot.NodeCount,
		EdgeCount: entitySnapshot.EdgeCount,
	}
	observer := newGraphObserver(target.Name, entitySnapshot.NodeCount)

	nodeStartedAt := time.Now()
	slog.Info("retriever dump node phase started",
		slog.String("graph", target.Name),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("shard_size", options.ShardSize),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationDump,
		Message:   "retriever dump node phase started",
		Graph:     target.Name,
		Phase:     PhaseNodes,
		Planned:   entitySnapshot.NodeCount,
		BatchSize: options.BatchSize,
		ShardSize: options.ShardSize,
	})

	nodeFiles, err := dumpNodePhase(ctx, source, targetGraph, options, transform, observer, entitySnapshot, nodeSink)
	if err != nil {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
	}

	graphEntry.Files = append(graphEntry.Files, nodeFiles...)

	slog.Info("retriever dump node phase completed",
		slog.String("graph", target.Name),
		slog.Int64("processed", fileTotal(nodeFiles)),
		slog.Int("file_count", len(nodeFiles)),
		slog.Duration("wall_elapsed", time.Since(nodeStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:         OperationDump,
		Message:           "retriever dump node phase completed",
		Graph:             target.Name,
		Phase:             PhaseNodes,
		Processed:         fileTotal(nodeFiles),
		Planned:           entitySnapshot.NodeCount,
		FileCount:         len(nodeFiles),
		Elapsed:           time.Since(nodeStartedAt),
		EntitiesPerSecond: perSecond(fileTotal(nodeFiles), time.Since(nodeStartedAt)),
	})

	edgeStartedAt := time.Now()
	slog.Info("retriever dump edge phase started",
		slog.String("graph", target.Name),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("shard_size", options.ShardSize),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationDump,
		Message:   "retriever dump edge phase started",
		Graph:     target.Name,
		Phase:     PhaseEdges,
		Planned:   entitySnapshot.EdgeCount,
		BatchSize: options.BatchSize,
		ShardSize: options.ShardSize,
	})

	edgeFiles, err := dumpEdgePhase(ctx, source, targetGraph, options, transform, observer, entitySnapshot, edgeSink)
	if err != nil {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
	}

	graphEntry.Files = append(graphEntry.Files, edgeFiles...)

	slog.Info("retriever dump edge phase completed",
		slog.String("graph", target.Name),
		slog.Int64("processed", fileTotal(edgeFiles)),
		slog.Int("file_count", len(edgeFiles)),
		slog.Duration("wall_elapsed", time.Since(edgeStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:         OperationDump,
		Message:           "retriever dump edge phase completed",
		Graph:             target.Name,
		Phase:             PhaseEdges,
		Processed:         fileTotal(edgeFiles),
		Planned:           entitySnapshot.EdgeCount,
		FileCount:         len(edgeFiles),
		Elapsed:           time.Since(edgeStartedAt),
		EntitiesPerSecond: perSecond(fileTotal(edgeFiles), time.Since(edgeStartedAt)),
	})

	if observer.nodeCount != entitySnapshot.NodeCount {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, EntityCountMismatchError{
			Operation: OperationDump,
			Graph:     target.Name,
			Phase:     PhaseNodes,
			Expected:  entitySnapshot.NodeCount,
			Actual:    observer.nodeCount,
			Message:   fmt.Sprintf("dumped %d nodes for graph %q but counted %d at scan start; source graph changed during dump or the ID scan was inconsistent", observer.nodeCount, target.Name, entitySnapshot.NodeCount),
		}
	}

	if observer.edgeCount != entitySnapshot.EdgeCount {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, EntityCountMismatchError{
			Operation: OperationDump,
			Graph:     target.Name,
			Phase:     PhaseEdges,
			Expected:  entitySnapshot.EdgeCount,
			Actual:    observer.edgeCount,
			Message:   fmt.Sprintf("dumped %d relationships for graph %q but counted %d at scan start; source graph changed during dump or the ID scan was inconsistent", observer.edgeCount, target.Name, entitySnapshot.EdgeCount),
		}
	}

	graphEntry.NodeActionCounts = observer.nodeActionCounts
	graphEntry.EdgeActionCounts = observer.edgeActionCounts
	metricsEntry := observer.Metrics()

	slog.Info("retriever dump metrics fingerprint computed",
		slog.String("graph", target.Name),
		slog.String("fingerprint", metricsEntry.Fingerprint),
		slog.Int64("node_count", metricsEntry.NodeCount),
		slog.Int64("edge_count", metricsEntry.EdgeCount),
	)

	schemaEntry := observer.Schema()

	return graphEntry, schemaEntry, metricsEntry, nil
}

func prepareTransformSession(ctx context.Context, source graphSource, targetGraph graph.Graph, batchSize int, transform transformSession, entitySnapshot graphEntitySnapshot, progress ProgressFunc, progressInterval int64) (int64, error) {
	processed, err := runFaucetWithProgress(ctx, source.Nodes(targetGraph, entitySnapshot.NodeCount, batchSize), entitySnapshot.NodeCount, progressInterval, func(nodes []*graph.Node) error {
		for _, node := range nodes {
			transform.PrepareNode(node)
		}
		return nil
	}, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump scrub pre-pass progress", targetGraph.Name, PhaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt, progress, progressInterval)
	})
	if err != nil {
		return processed, fmt.Errorf("scrub pre-pass: %w", err)
	}

	return processed, nil
}

// bufferedShardReceiver preserves the whole-shard dump path until the
// single-output coordinator replaces it.
type bufferedShardReceiver[T any] struct {
	id      shardID
	records []T
	emit    func(shardSummary, []T) error
	active  bool
}

func (s *bufferedShardReceiver[T]) BeginShard(id shardID) error {
	if s.active {
		return fmt.Errorf("begin shard while shard %d is active", s.id.Number)
	}
	s.id = id
	s.records = nil
	s.active = true
	return nil
}

func (s *bufferedShardReceiver[T]) WriteBatch(records []T) error {
	if !s.active {
		return fmt.Errorf("write shard batch without an active shard")
	}
	s.records = append(s.records, records...)
	return nil
}

func (s *bufferedShardReceiver[T]) FinishShard(summary shardSummary) error {
	if !s.active {
		return fmt.Errorf("finish shard without an active shard")
	}
	if summary.ID != s.id {
		return fmt.Errorf("finish shard %d while shard %d is active", summary.ID.Number, s.id.Number)
	}
	if summary.Rows != len(s.records) {
		return fmt.Errorf("finish shard %d with %d rows after receiving %d", summary.ID.Number, summary.Rows, len(s.records))
	}
	if err := s.emit(summary, s.records); err != nil {
		return err
	}

	s.active = false
	s.records = nil
	return nil
}

func dumpNodePhase(ctx context.Context, source graphSource, targetGraph graph.Graph, options DumpOptions, transform transformSession, observer *graphObserver, entitySnapshot graphEntitySnapshot, sink fragmentSink[normalizedNode, FileManifest]) ([]FileManifest, error) {
	if entitySnapshot.NodeCount == 0 {
		return nil, nil
	}

	sharder, err := newLogicalSharder[normalizedNode](targetGraph.Name, PhaseNodes, options.ShardSize)
	if err != nil {
		return nil, err
	}

	var files []FileManifest
	emit := func(summary shardSummary, records []normalizedNode) error {
		fileEntry, err := writeFragment(ctx, sink, summary, records)
		if err != nil {
			return err
		}
		fileEntry.ActionCounts = cloneActionCounts(summary.ActionCounts)
		files = append(files, fileEntry)
		return nil
	}
	receiver := &bufferedShardReceiver[normalizedNode]{emit: emit}

	if _, err := runFaucetWithProgress(ctx, source.Nodes(targetGraph, entitySnapshot.NodeCount, options.BatchSize), entitySnapshot.NodeCount, options.ProgressInterval, func(nodes []*graph.Node) error {
		batch := transform.TransformNodes(nodes)
		for index, record := range batch.Records {
			if err := observer.ObserveNode(record, batch.ActionCounts[index]); err != nil {
				return err
			}
		}

		return sharder.Add(batch, receiver)
	}, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump node phase progress", targetGraph.Name, PhaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt, options.Progress, options.ProgressInterval)
	}); err != nil {
		return nil, err
	}

	if err := sharder.Flush(receiver); err != nil {
		return nil, err
	}

	return files, nil
}

func dumpEdgePhase(ctx context.Context, source graphSource, targetGraph graph.Graph, options DumpOptions, transform transformSession, observer *graphObserver, entitySnapshot graphEntitySnapshot, sink fragmentSink[normalizedEdge, FileManifest]) ([]FileManifest, error) {
	if entitySnapshot.EdgeCount == 0 {
		return nil, nil
	}

	sharder, err := newLogicalSharder[normalizedEdge](targetGraph.Name, PhaseEdges, options.ShardSize)
	if err != nil {
		return nil, err
	}

	var files []FileManifest
	emit := func(summary shardSummary, records []normalizedEdge) error {
		fileEntry, err := writeFragment(ctx, sink, summary, records)
		if err != nil {
			return err
		}
		fileEntry.ActionCounts = cloneActionCounts(summary.ActionCounts)
		files = append(files, fileEntry)
		return nil
	}
	receiver := &bufferedShardReceiver[normalizedEdge]{emit: emit}

	if _, err := runFaucetWithProgress(ctx, source.Edges(targetGraph, entitySnapshot.EdgeCount, options.BatchSize), entitySnapshot.EdgeCount, options.ProgressInterval, func(relationships []*graph.Relationship) error {
		batch := transform.TransformEdges(relationships)
		for index, record := range batch.Records {
			if err := observer.ObserveEdge(record, batch.ActionCounts[index]); err != nil {
				return err
			}
		}

		return sharder.Add(batch, receiver)
	}, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump edge phase progress", targetGraph.Name, PhaseEdges, processed, entitySnapshot.EdgeCount, startedAt, nextProgressAt, options.Progress, options.ProgressInterval)
	}); err != nil {
		return nil, err
	}

	if err := sharder.Flush(receiver); err != nil {
		return nil, err
	}

	return files, nil
}

func fileTotal(files []FileManifest) int64 {
	var total int64
	for _, fileEntry := range files {
		total += int64(fileEntry.Count)
	}

	return total
}

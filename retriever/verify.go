package retriever

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
)

type VerifyResult struct {
	GraphCount int
	NodeCount  int64
	EdgeCount  int64
}

type MetricsMismatchError struct {
	Differences []string
}

func (s MetricsMismatchError) Error() string {
	if len(s.Differences) == 0 {
		return "graph metrics mismatch"
	}
	return "graph metrics mismatch:\n  " + strings.Join(s.Differences, "\n  ")
}

func Verify(ctx context.Context, db graph.Database, driverName string, options VerifyOptions) (VerifyResult, error) {
	if err := options.validate(); err != nil {
		return VerifyResult{}, err
	}

	startedAt := time.Now()
	slog.Info("retriever verify started",
		slog.String("driver", driverName),
		slog.String("input_dir", options.InputDir),
		slog.Int("batch_size", options.BatchSize),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationVerify,
		Message:   "retriever verify started",
		Driver:    driverName,
		InputDir:  options.InputDir,
		BatchSize: options.BatchSize,
	})

	readManifestStartedAt := time.Now()
	slog.Info("retriever verify reading manifest",
		slog.String("input_dir", options.InputDir),
	)
	nextManifest, err := readManifest(options.InputDir)
	if err != nil {
		return VerifyResult{}, err
	}

	if nextManifest.Metrics == nil {
		return VerifyResult{}, MissingMetricsError{Operation: OperationVerify}
	}

	if driverName == neo4j.DriverName && len(nextManifest.Graphs) > 1 {
		return VerifyResult{}, IncompatibleDriverError{
			Operation: OperationVerify,
			Driver:    driverName,
			Reason:    "cannot verify a multi-graph collection against neo4j because Dawgs graph names are no-ops for that driver",
		}
	}

	slog.Info("retriever verify manifest ready",
		slog.String("input_dir", options.InputDir),
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Duration("wall_elapsed", time.Since(readManifestStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationVerify,
		Message:    "retriever verify manifest ready",
		InputDir:   options.InputDir,
		GraphCount: len(nextManifest.Graphs),
		FileCount:  manifestFileCount(nextManifest),
		Elapsed:    time.Since(readManifestStartedAt),
	})

	actualMetrics, result, err := collectDatabaseMetrics(ctx, db, nextManifest.Graphs, options.BatchSize, options.Progress, options.ProgressInterval)
	if err != nil {
		return VerifyResult{}, err
	}

	differences := compareMetricsManifest(*nextManifest.Metrics, actualMetrics)
	if len(differences) > 0 {
		slog.Info("retriever verify failed",
			slog.Int("graph_count", result.GraphCount),
			slog.Int("difference_count", len(differences)),
			slog.Duration("wall_elapsed", time.Since(startedAt)),
		)
		return result, MetricsMismatchError{
			Differences: differences,
		}
	}

	slog.Info("retriever verify passed",
		slog.Int("graph_count", result.GraphCount),
		slog.Int64("node_count", result.NodeCount),
		slog.Int64("edge_count", result.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationVerify,
		Message:    "retriever verify passed",
		Driver:     driverName,
		InputDir:   options.InputDir,
		GraphCount: result.GraphCount,
		NodeCount:  result.NodeCount,
		EdgeCount:  result.EdgeCount,
		Elapsed:    time.Since(startedAt),
	})

	return result, nil
}

func collectDatabaseMetrics(ctx context.Context, db graph.Database, graphEntries []GraphManifest, batchSize int, progress ProgressFunc, progressInterval int64) (MetricsManifest, VerifyResult, error) {
	result := VerifyResult{
		GraphCount: len(graphEntries),
	}
	nextMetrics := newMetricsManifest(len(graphEntries))

	slog.Info("retriever metrics collection started",
		slog.Int("graph_count", len(graphEntries)),
		slog.Int("batch_size", batchSize),
	)

	for graphIndex, graphEntry := range graphEntries {
		graphStartedAt := time.Now()

		slog.Info("retriever metrics graph started",
			slog.String("graph", graphEntry.Name),
			slog.Int("graph_index", graphIndex+1),
			slog.Int("graph_count", len(graphEntries)),
		)
		progress.emit(ProgressEvent{
			Operation:  OperationVerify,
			Message:    "retriever metrics graph started",
			Graph:      graphEntry.Name,
			GraphIndex: graphIndex + 1,
			GraphCount: len(graphEntries),
		})

		metricsEntry, err := collectDatabaseGraphMetrics(ctx, db, graphEntry.Name, batchSize, progress, progressInterval)
		if err != nil {
			return MetricsManifest{}, VerifyResult{}, err
		}

		nextMetrics.Graphs = append(nextMetrics.Graphs, metricsEntry)
		result.NodeCount += metricsEntry.NodeCount
		result.EdgeCount += metricsEntry.EdgeCount

		slog.Info("retriever metrics graph completed",
			slog.String("graph", graphEntry.Name),
			slog.String("fingerprint", metricsEntry.Fingerprint),
			slog.Int64("node_count", metricsEntry.NodeCount),
			slog.Int64("edge_count", metricsEntry.EdgeCount),
			slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
		)
		progress.emit(ProgressEvent{
			Operation:  OperationVerify,
			Message:    "retriever metrics graph completed",
			Graph:      graphEntry.Name,
			GraphIndex: graphIndex + 1,
			GraphCount: len(graphEntries),
			NodeCount:  metricsEntry.NodeCount,
			EdgeCount:  metricsEntry.EdgeCount,
			Elapsed:    time.Since(graphStartedAt),
		})
	}

	return nextMetrics, result, nil
}

func collectDatabaseGraphMetrics(ctx context.Context, db graph.Database, graphName string, batchSize int, progress ProgressFunc, progressInterval int64) (GraphMetrics, error) {
	targetGraph := graph.Graph{
		Name: graphName,
	}

	countStartedAt := time.Now()
	slog.Info("retriever metrics counting graph entities",
		slog.String("graph", graphName),
	)
	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return GraphMetrics{}, err
	}

	slog.Info("retriever metrics graph counts ready",
		slog.String("graph", graphName),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(countStartedAt)),
	)
	progress.emit(ProgressEvent{
		Operation: OperationVerify,
		Message:   "retriever metrics graph counts ready",
		Graph:     graphName,
		NodeCount: entitySnapshot.NodeCount,
		EdgeCount: entitySnapshot.EdgeCount,
		Elapsed:   time.Since(countStartedAt),
	})

	metricsBuilder := newMetricsBuilder(graphName, entitySnapshot.NodeCount)
	if err := collectDatabaseNodeMetrics(ctx, db, targetGraph, batchSize, entitySnapshot, metricsBuilder, progress, progressInterval); err != nil {
		return GraphMetrics{}, err
	}

	if err := collectDatabaseRelationshipMetrics(ctx, db, targetGraph, batchSize, entitySnapshot, metricsBuilder, progress, progressInterval); err != nil {
		return GraphMetrics{}, err
	}

	metricsEntry := metricsBuilder.finalize()

	slog.Info("retriever metrics fingerprint computed",
		slog.String("graph", graphName),
		slog.String("fingerprint", metricsEntry.Fingerprint),
		slog.Int64("node_count", metricsEntry.NodeCount),
		slog.Int64("edge_count", metricsEntry.EdgeCount),
	)

	return metricsEntry, nil
}

func collectDatabaseNodeMetrics(ctx context.Context, db graph.Database, targetGraph graph.Graph, batchSize int, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder, progress ProgressFunc, progressInterval int64) error {
	slog.Info("retriever metrics node phase started",
		slog.String("graph", targetGraph.Name),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int("batch_size", batchSize),
	)
	progress.emit(ProgressEvent{
		Operation: OperationVerify,
		Message:   "retriever metrics node phase started",
		Graph:     targetGraph.Name,
		Phase:     PhaseNodes,
		Planned:   entitySnapshot.NodeCount,
		BatchSize: batchSize,
	})

	startedAt := time.Now()
	processed, err := scanDatabaseNodesWithProgressInterval(ctx, db, targetGraph, entitySnapshot.NodeCount, batchSize, progressInterval, func(nodes []*graph.Node) error {
		for _, node := range nodes {
			if err := metricsBuilder.observeNode(node.ID.String(), node.Kinds.Strings()); err != nil {
				return err
			}
		}

		return nil
	}, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever metrics node phase progress", targetGraph.Name, PhaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt, progress, progressInterval)
	})
	if err != nil {
		return err
	}

	if processed != entitySnapshot.NodeCount {
		return EntityCountMismatchError{
			Operation: OperationVerify,
			Graph:     targetGraph.Name,
			Phase:     PhaseNodes,
			Expected:  entitySnapshot.NodeCount,
			Actual:    processed,
			Message:   fmt.Sprintf("collected %d nodes for graph %q but counted %d at scan start; destination graph changed during metrics collection or the ID scan was inconsistent", processed, targetGraph.Name, entitySnapshot.NodeCount),
		}
	}

	slog.Info("retriever metrics node phase completed",
		slog.String("graph", targetGraph.Name),
		slog.Int64("processed", processed),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
		slog.Float64("entities_per_second", perSecond(processed, time.Since(startedAt))),
	)
	progress.emit(ProgressEvent{
		Operation:         OperationVerify,
		Message:           "retriever metrics node phase completed",
		Graph:             targetGraph.Name,
		Phase:             PhaseNodes,
		Processed:         processed,
		Planned:           entitySnapshot.NodeCount,
		Elapsed:           time.Since(startedAt),
		EntitiesPerSecond: perSecond(processed, time.Since(startedAt)),
	})

	return nil
}

func collectDatabaseRelationshipMetrics(ctx context.Context, db graph.Database, targetGraph graph.Graph, batchSize int, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder, progress ProgressFunc, progressInterval int64) error {
	slog.Info("retriever metrics relationship phase started",
		slog.String("graph", targetGraph.Name),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Int("batch_size", batchSize),
	)
	progress.emit(ProgressEvent{
		Operation: OperationVerify,
		Message:   "retriever metrics relationship phase started",
		Graph:     targetGraph.Name,
		Phase:     PhaseEdges,
		Planned:   entitySnapshot.EdgeCount,
		BatchSize: batchSize,
	})

	startedAt := time.Now()
	processed, err := scanDatabaseRelationshipsWithProgressInterval(ctx, db, targetGraph, entitySnapshot.EdgeCount, batchSize, progressInterval, func(relationships []*graph.Relationship) error {
		for _, relationship := range relationships {
			kind := ""
			if relationship.Kind != nil {
				kind = relationship.Kind.String()
			}

			if err := metricsBuilder.observeRelationship(relationship.StartID.String(), relationship.EndID.String(), kind); err != nil {
				return err
			}
		}

		return nil
	}, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever metrics relationship phase progress", targetGraph.Name, PhaseEdges, processed, entitySnapshot.EdgeCount, startedAt, nextProgressAt, progress, progressInterval)
	})
	if err != nil {
		return err
	}

	if processed != entitySnapshot.EdgeCount {
		return EntityCountMismatchError{
			Operation: OperationVerify,
			Graph:     targetGraph.Name,
			Phase:     PhaseEdges,
			Expected:  entitySnapshot.EdgeCount,
			Actual:    processed,
			Message:   fmt.Sprintf("collected %d relationships for graph %q but counted %d at scan start; destination graph changed during metrics collection or the ID scan was inconsistent", processed, targetGraph.Name, entitySnapshot.EdgeCount),
		}
	}

	slog.Info("retriever metrics relationship phase completed",
		slog.String("graph", targetGraph.Name),
		slog.Int64("processed", processed),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
		slog.Float64("entities_per_second", perSecond(processed, time.Since(startedAt))),
	)
	progress.emit(ProgressEvent{
		Operation:         OperationVerify,
		Message:           "retriever metrics relationship phase completed",
		Graph:             targetGraph.Name,
		Phase:             PhaseEdges,
		Processed:         processed,
		Planned:           entitySnapshot.EdgeCount,
		Elapsed:           time.Since(startedAt),
		EntitiesPerSecond: perSecond(processed, time.Since(startedAt)),
	})

	return nil
}

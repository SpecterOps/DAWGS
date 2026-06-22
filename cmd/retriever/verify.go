package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
)

type verifyResult struct {
	GraphCount int
	NodeCount  int64
	EdgeCount  int64
}

type metricsMismatchError struct {
	Differences []string
}

func (s metricsMismatchError) Error() string {
	if len(s.Differences) == 0 {
		return "graph metrics mismatch"
	}
	return "graph metrics mismatch:\n  " + strings.Join(s.Differences, "\n  ")
}

func Verify(ctx context.Context, db graph.Database, driverName string, options verifyOptions) (verifyResult, error) {
	if err := options.validate(); err != nil {
		return verifyResult{}, err
	}

	startedAt := time.Now()
	slog.Info("retriever verify started",
		slog.String("driver", driverName),
		slog.String("input_dir", options.InputDir),
		slog.Int("batch_size", options.BatchSize),
	)

	readManifestStartedAt := time.Now()
	slog.Info("retriever verify reading manifest",
		slog.String("input_dir", options.InputDir),
	)
	nextManifest, err := readManifest(options.InputDir)
	if err != nil {
		return verifyResult{}, err
	}
	if nextManifest.Metrics == nil {
		return verifyResult{}, fmt.Errorf("manifest does not contain graph metrics; create a new dump with metrics support before verifying")
	}
	if driverName == neo4j.DriverName && len(nextManifest.Graphs) > 1 {
		return verifyResult{}, fmt.Errorf("cannot verify a multi-graph collection against neo4j because Dawgs graph names are no-ops for that driver")
	}
	slog.Info("retriever verify manifest ready",
		slog.String("input_dir", options.InputDir),
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Duration("wall_elapsed", time.Since(readManifestStartedAt)),
	)

	actualMetrics, result, err := collectDatabaseMetrics(ctx, db, nextManifest.Graphs, options.BatchSize)
	if err != nil {
		return verifyResult{}, err
	}
	differences := compareMetricsManifest(*nextManifest.Metrics, actualMetrics)
	if len(differences) > 0 {
		slog.Info("retriever verify failed",
			slog.Int("graph_count", result.GraphCount),
			slog.Int("difference_count", len(differences)),
			slog.Duration("wall_elapsed", time.Since(startedAt)),
		)
		return result, metricsMismatchError{
			Differences: differences,
		}
	}

	slog.Info("retriever verify passed",
		slog.Int("graph_count", result.GraphCount),
		slog.Int64("node_count", result.NodeCount),
		slog.Int64("edge_count", result.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)
	return result, nil
}

func collectDatabaseMetrics(ctx context.Context, db graph.Database, graphEntries []graphManifest, batchSize int) (metricsManifest, verifyResult, error) {
	result := verifyResult{
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

		metricsEntry, err := collectDatabaseGraphMetrics(ctx, db, graphEntry.Name, batchSize)
		if err != nil {
			return metricsManifest{}, verifyResult{}, err
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
	}

	return nextMetrics, result, nil
}

func collectDatabaseGraphMetrics(ctx context.Context, db graph.Database, graphName string, batchSize int) (graphMetrics, error) {
	targetGraph := graph.Graph{
		Name: graphName,
	}

	countStartedAt := time.Now()
	slog.Info("retriever metrics counting graph entities",
		slog.String("graph", graphName),
	)
	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return graphMetrics{}, err
	}
	slog.Info("retriever metrics graph counts ready",
		slog.String("graph", graphName),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(countStartedAt)),
	)

	metricsBuilder := newMetricsBuilder(graphName, entitySnapshot.NodeCount)
	if err := collectDatabaseNodeMetrics(ctx, db, targetGraph, batchSize, entitySnapshot, metricsBuilder); err != nil {
		return graphMetrics{}, err
	}
	if err := collectDatabaseRelationshipMetrics(ctx, db, targetGraph, batchSize, entitySnapshot, metricsBuilder); err != nil {
		return graphMetrics{}, err
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

func collectDatabaseNodeMetrics(ctx context.Context, db graph.Database, targetGraph graph.Graph, batchSize int, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder) error {
	if entitySnapshot.NodeCount == 0 {
		return nil
	}

	var (
		lastID         graph.ID
		hasLastID      bool
		processed      int64
		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAt(entitySnapshot.NodeCount)
	)
	slog.Info("retriever metrics node phase started",
		slog.String("graph", targetGraph.Name),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int("batch_size", batchSize),
	)
	for processed < entitySnapshot.NodeCount {
		remaining := entitySnapshot.NodeCount - processed
		nodes, err := readDatabaseNodes(ctx, db, targetGraph, lastID, hasLastID, retrieverBatchLimit(remaining, batchSize))
		if err != nil {
			return err
		}
		if int64(len(nodes)) > remaining {
			nodes = nodes[:int(remaining)]
		}
		if len(nodes) == 0 {
			break
		}
		for _, node := range nodes {
			if err := metricsBuilder.observeNode(node.ID.String(), node.Kinds.Strings()); err != nil {
				return err
			}
		}
		processed += int64(len(nodes))
		nextProgressAt = logRetrieverEntityProgress("retriever metrics node phase progress", targetGraph.Name, phaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt)
		lastID = nodes[len(nodes)-1].ID
		hasLastID = true
	}
	if processed != entitySnapshot.NodeCount {
		return fmt.Errorf("collected %d nodes for graph %q but counted %d at scan start; destination graph changed during metrics collection or the ID scan was inconsistent", processed, targetGraph.Name, entitySnapshot.NodeCount)
	}
	slog.Info("retriever metrics node phase completed",
		slog.String("graph", targetGraph.Name),
		slog.Int64("processed", processed),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
		slog.Float64("entities_per_second", perSecond(processed, time.Since(startedAt))),
	)
	return nil
}

func collectDatabaseRelationshipMetrics(ctx context.Context, db graph.Database, targetGraph graph.Graph, batchSize int, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder) error {
	if entitySnapshot.EdgeCount == 0 {
		return nil
	}

	var (
		lastID         graph.ID
		hasLastID      bool
		processed      int64
		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAt(entitySnapshot.EdgeCount)
	)
	slog.Info("retriever metrics relationship phase started",
		slog.String("graph", targetGraph.Name),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Int("batch_size", batchSize),
	)
	for processed < entitySnapshot.EdgeCount {
		remaining := entitySnapshot.EdgeCount - processed
		relationships, err := readDatabaseRelationships(ctx, db, targetGraph, lastID, hasLastID, retrieverBatchLimit(remaining, batchSize))
		if err != nil {
			return err
		}
		if int64(len(relationships)) > remaining {
			relationships = relationships[:int(remaining)]
		}
		if len(relationships) == 0 {
			break
		}
		for _, relationship := range relationships {
			kind := ""
			if relationship.Kind != nil {
				kind = relationship.Kind.String()
			}
			if err := metricsBuilder.observeRelationship(relationship.StartID.String(), relationship.EndID.String(), kind); err != nil {
				return err
			}
		}
		processed += int64(len(relationships))
		nextProgressAt = logRetrieverEntityProgress("retriever metrics relationship phase progress", targetGraph.Name, phaseEdges, processed, entitySnapshot.EdgeCount, startedAt, nextProgressAt)
		lastID = relationships[len(relationships)-1].ID
		hasLastID = true
	}
	if processed != entitySnapshot.EdgeCount {
		return fmt.Errorf("collected %d relationships for graph %q but counted %d at scan start; destination graph changed during metrics collection or the ID scan was inconsistent", processed, targetGraph.Name, entitySnapshot.EdgeCount)
	}
	slog.Info("retriever metrics relationship phase completed",
		slog.String("graph", targetGraph.Name),
		slog.Int64("processed", processed),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
		slog.Float64("entities_per_second", perSecond(processed, time.Since(startedAt))),
	)
	return nil
}

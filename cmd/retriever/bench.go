package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/retriever"
)

type benchReport struct {
	Driver      string             `json:"driver"`
	GeneratedAt time.Time          `json:"generated_at"`
	Graphs      []benchGraphReport `json:"graphs"`
}

type benchGraphReport struct {
	Name    string        `json:"name"`
	Results []benchResult `json:"results"`
}

type benchResult struct {
	Workers                  int     `json:"workers"`
	BatchSize                int     `json:"batch_size"`
	SampleSize               int     `json:"sample_size,omitempty"`
	NodeCount                int64   `json:"node_count"`
	EdgeCount                int64   `json:"edge_count"`
	NodeProcessed            int64   `json:"node_processed"`
	EdgeProcessed            int64   `json:"edge_processed"`
	NodeWallMillis           int64   `json:"node_wall_millis"`
	EdgeWallMillis           int64   `json:"edge_wall_millis"`
	NodeDBReadMillis         int64   `json:"node_db_read_millis"`
	EdgeDBReadMillis         int64   `json:"edge_db_read_millis"`
	NodeEncodeCompressMillis int64   `json:"node_encode_compress_millis,omitempty"`
	EdgeEncodeCompressMillis int64   `json:"edge_encode_compress_millis,omitempty"`
	TotalWallMillis          int64   `json:"total_wall_millis"`
	NodesPerSecond           float64 `json:"nodes_per_second"`
	EdgesPerSecond           float64 `json:"edges_per_second"`
	EntitiesPerSecond        float64 `json:"entities_per_second"`
	UncompressedBytes        int64   `json:"uncompressed_bytes,omitempty"`
	CompressedBytes          int64   `json:"compressed_bytes,omitempty"`
}

type benchPhaseResult struct {
	Count                int64
	WallElapsed          time.Duration
	DBReadElapsed        time.Duration
	EncodeCompressTime   time.Duration
	UncompressedByteSize int64
	CompressedByteSize   int64
}

func Bench(ctx context.Context, db graph.Database, driverName string, targets []retriever.GraphTarget, options benchOptions) (benchReport, error) {
	if err := options.validate(); err != nil {
		return benchReport{}, err
	}

	startedAt := time.Now()
	slog.Info("retriever bench started",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("sample_size", options.SampleSize),
		slog.Any("workers", options.Workers),
		slog.String("compression", string(options.Compression)),
	)

	report := benchReport{
		Driver:      driverName,
		GeneratedAt: time.Now().UTC(),
		Graphs:      make([]benchGraphReport, 0, len(targets)),
	}

	for targetIndex, target := range targets {
		graphStartedAt := time.Now()

		slog.Info("retriever bench graph started",
			slog.String("graph", target.Name),
			slog.Int("graph_index", targetIndex+1),
			slog.Int("graph_count", len(targets)),
		)

		targetGraph := graph.Graph{
			Name: target.Name,
		}

		slog.Info("retriever bench counting graph entities",
			slog.String("graph", target.Name),
		)

		nodeCount, edgeCount, err := countGraphEntities(ctx, db, targetGraph)
		if err != nil {
			return benchReport{}, err
		}

		slog.Info("retriever bench graph counts ready",
			slog.String("graph", target.Name),
			slog.Int64("node_count", nodeCount),
			slog.Int64("edge_count", edgeCount),
		)

		graphReport := benchGraphReport{
			Name: target.Name,
		}

		for workerIndex, workerCount := range options.Workers {
			workerStartedAt := time.Now()

			slog.Info("retriever bench worker run started",
				slog.String("graph", target.Name),
				slog.Int("worker_count", workerCount),
				slog.Int("worker_index", workerIndex+1),
				slog.Int("worker_runs", len(options.Workers)),
				slog.Int("batch_size", options.BatchSize),
				slog.Int("sample_size", options.SampleSize),
			)

			plannedNodes := benchPlannedCount(nodeCount, options.SampleSize)

			slog.Info("retriever bench node phase started",
				slog.String("graph", target.Name),
				slog.Int("worker_count", workerCount),
				slog.Int64("node_count", nodeCount),
				slog.Int64("planned_count", plannedNodes),
			)

			nodeResult, err := benchNodes(ctx, db, targetGraph, nodeCount, workerCount, options)
			if err != nil {
				return benchReport{}, err
			}

			slog.Info("retriever bench node phase completed",
				slog.String("graph", target.Name),
				slog.Int("worker_count", workerCount),
				slog.Int64("processed", nodeResult.Count),
				slog.Duration("wall_elapsed", nodeResult.WallElapsed),
				slog.Duration("db_read_elapsed", nodeResult.DBReadElapsed),
				slog.Duration("encode_compress_elapsed", nodeResult.EncodeCompressTime),
				slog.Float64("entities_per_second", perSecond(nodeResult.Count, nodeResult.WallElapsed)),
			)

			plannedEdges := benchPlannedCount(edgeCount, options.SampleSize)

			slog.Info("retriever bench edge phase started",
				slog.String("graph", target.Name),
				slog.Int("worker_count", workerCount),
				slog.Int64("edge_count", edgeCount),
				slog.Int64("planned_count", plannedEdges),
			)

			edgeResult, err := benchEdges(ctx, db, targetGraph, edgeCount, workerCount, options)
			if err != nil {
				return benchReport{}, err
			}

			slog.Info("retriever bench edge phase completed",
				slog.String("graph", target.Name),
				slog.Int("worker_count", workerCount),
				slog.Int64("processed", edgeResult.Count),
				slog.Duration("wall_elapsed", edgeResult.WallElapsed),
				slog.Duration("db_read_elapsed", edgeResult.DBReadElapsed),
				slog.Duration("encode_compress_elapsed", edgeResult.EncodeCompressTime),
				slog.Float64("entities_per_second", perSecond(edgeResult.Count, edgeResult.WallElapsed)),
			)

			totalWall := nodeResult.WallElapsed + edgeResult.WallElapsed
			graphReport.Results = append(graphReport.Results, benchResult{
				Workers:                  workerCount,
				BatchSize:                options.BatchSize,
				SampleSize:               options.SampleSize,
				NodeCount:                nodeCount,
				EdgeCount:                edgeCount,
				NodeProcessed:            nodeResult.Count,
				EdgeProcessed:            edgeResult.Count,
				NodeWallMillis:           nodeResult.WallElapsed.Milliseconds(),
				EdgeWallMillis:           edgeResult.WallElapsed.Milliseconds(),
				NodeDBReadMillis:         nodeResult.DBReadElapsed.Milliseconds(),
				EdgeDBReadMillis:         edgeResult.DBReadElapsed.Milliseconds(),
				NodeEncodeCompressMillis: nodeResult.EncodeCompressTime.Milliseconds(),
				EdgeEncodeCompressMillis: edgeResult.EncodeCompressTime.Milliseconds(),
				TotalWallMillis:          totalWall.Milliseconds(),
				NodesPerSecond:           perSecond(nodeResult.Count, nodeResult.WallElapsed),
				EdgesPerSecond:           perSecond(edgeResult.Count, edgeResult.WallElapsed),
				EntitiesPerSecond:        perSecond(nodeResult.Count+edgeResult.Count, totalWall),
				UncompressedBytes:        nodeResult.UncompressedByteSize + edgeResult.UncompressedByteSize,
				CompressedBytes:          nodeResult.CompressedByteSize + edgeResult.CompressedByteSize,
			})

			slog.Info("retriever bench worker run completed",
				slog.String("graph", target.Name),
				slog.Int("worker_count", workerCount),
				slog.Duration("wall_elapsed", time.Since(workerStartedAt)),
				slog.Float64("entities_per_second", perSecond(nodeResult.Count+edgeResult.Count, totalWall)),
			)
		}

		report.Graphs = append(report.Graphs, graphReport)

		slog.Info("retriever bench graph completed",
			slog.String("graph", target.Name),
			slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
		)
	}

	slog.Info("retriever bench completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)

	return report, nil
}

type benchBatchProcessor[T any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	process func([]T) (benchPhaseResult, error)
	inline  bool
	jobs    chan []T
	wg      sync.WaitGroup
	mu      sync.Mutex
	result  benchPhaseResult
	err     error
}

func newBenchBatchProcessor[T any](ctx context.Context, workers int, process func([]T) (benchPhaseResult, error)) (*benchBatchProcessor[T], context.Context, error) {
	if workers <= 0 {
		return nil, nil, fmt.Errorf("bench workers must be > 0")
	}
	if process == nil {
		return nil, nil, fmt.Errorf("bench batch processor is required")
	}

	var (
		scanCtx, cancel = context.WithCancel(ctx)
		processor       = &benchBatchProcessor[T]{
			ctx:     scanCtx,
			cancel:  cancel,
			process: process,
			inline:  workers == 1,
		}
	)
	if processor.inline {
		return processor, scanCtx, nil
	}

	processor.jobs = make(chan []T, workers)

	for range workers {
		processor.wg.Add(1)
		go processor.run()
	}

	return processor, scanCtx, nil
}

func (s *benchBatchProcessor[T]) run() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case batch, ok := <-s.jobs:
			if !ok {
				return
			}

			result, err := s.process(batch)
			if err != nil {
				s.setError(err)
				return
			}

			s.addResult(result)
		}
	}
}

func (s *benchBatchProcessor[T]) handle(batch []T) error {
	if err := s.currentError(); err != nil {
		return err
	}

	if s.inline {
		result, err := s.process(batch)
		if err != nil {
			s.setError(err)
			return err
		}

		s.addResult(result)

		return nil
	}

	select {
	case <-s.ctx.Done():
		if err := s.currentError(); err != nil {
			return err
		}

		return s.ctx.Err()

	case s.jobs <- batch:
		return nil
	}
}

func (s *benchBatchProcessor[T]) closeAndWait() (benchPhaseResult, error) {
	if !s.inline {
		close(s.jobs)
		s.wg.Wait()
	}

	s.cancel()

	if err := s.currentError(); err != nil {
		return benchPhaseResult{}, err
	}

	return s.snapshot(), nil
}

func (s *benchBatchProcessor[T]) addDBReadElapsed(value time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.DBReadElapsed += value
}

func (s *benchBatchProcessor[T]) addResult(value benchPhaseResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.Count += value.Count
	s.result.WallElapsed += value.WallElapsed
	s.result.DBReadElapsed += value.DBReadElapsed
	s.result.EncodeCompressTime += value.EncodeCompressTime
	s.result.UncompressedByteSize += value.UncompressedByteSize
	s.result.CompressedByteSize += value.CompressedByteSize
}

func (s *benchBatchProcessor[T]) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		s.cancel()
	}
}

func (s *benchBatchProcessor[T]) currentError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *benchBatchProcessor[T]) snapshot() benchPhaseResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func benchNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, workers int, options benchOptions) (benchPhaseResult, error) {
	startedAt := time.Now()
	planned := benchPlannedCount(total, options.SampleSize)

	processor, scanCtx, err := newBenchBatchProcessor(ctx, workers, func(nodes []*graph.Node) (benchPhaseResult, error) {
		return benchNodeBatch(nodes, options)
	})
	if err != nil {
		return benchPhaseResult{}, err
	}

	_, scanErr := scanEntityBatches(entityScanOptions[*graph.Node]{
		Total:      planned,
		BatchSize:  options.BatchSize,
		EntityName: "node",
		Read: func(afterID graph.ID, hasAfterID bool, limit int) ([]*graph.Node, error) {
			readStarted := time.Now()
			nodes, err := readDatabaseNodes(scanCtx, db, targetGraph, afterID, hasAfterID, limit)
			processor.addDBReadElapsed(time.Since(readStarted))

			return nodes, err
		},
		ID: func(node *graph.Node) graph.ID {
			return node.ID
		},
		Handle: func(nodes []*graph.Node) error {
			return processor.handle(nodes)
		},
		LogProgress: func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
			progressResult := processor.snapshot()
			progressResult.Count = processed

			return logBenchPhaseProgress(targetGraph.Name, retriever.PhaseNodes, workers, progressResult, planned, startedAt, nextProgressAt)
		},
	})

	result, processErr := processor.closeAndWait()
	result.WallElapsed = time.Since(startedAt)

	if processErr != nil {
		return benchPhaseResult{}, processErr
	}

	if scanErr != nil {
		return benchPhaseResult{}, scanErr
	}

	return result, nil
}

func benchEdges(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, workers int, options benchOptions) (benchPhaseResult, error) {
	startedAt := time.Now()
	planned := benchPlannedCount(total, options.SampleSize)

	processor, scanCtx, err := newBenchBatchProcessor(ctx, workers, func(relationships []*graph.Relationship) (benchPhaseResult, error) {
		return benchRelationshipBatch(relationships, options)
	})
	if err != nil {
		return benchPhaseResult{}, err
	}

	_, scanErr := scanEntityBatches(entityScanOptions[*graph.Relationship]{
		Total:      planned,
		BatchSize:  options.BatchSize,
		EntityName: "relationship",
		Read: func(afterID graph.ID, hasAfterID bool, limit int) ([]*graph.Relationship, error) {
			readStarted := time.Now()
			relationships, err := readDatabaseRelationships(scanCtx, db, targetGraph, afterID, hasAfterID, limit)
			processor.addDBReadElapsed(time.Since(readStarted))

			return relationships, err
		},
		ID: func(relationship *graph.Relationship) graph.ID {
			return relationship.ID
		},
		Handle: func(relationships []*graph.Relationship) error {
			return processor.handle(relationships)
		},
		LogProgress: func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
			progressResult := processor.snapshot()
			progressResult.Count = processed

			return logBenchPhaseProgress(targetGraph.Name, retriever.PhaseEdges, workers, progressResult, planned, startedAt, nextProgressAt)
		},
	})

	result, processErr := processor.closeAndWait()
	result.WallElapsed = time.Since(startedAt)

	if processErr != nil {
		return benchPhaseResult{}, processErr
	}

	if scanErr != nil {
		return benchPhaseResult{}, scanErr
	}

	return result, nil
}

func benchNodeBatch(nodes []*graph.Node, options benchOptions) (benchPhaseResult, error) {
	return benchCompressedBatch(len(nodes), options, func() []retriever.FragmentNode {
		items := make([]retriever.FragmentNode, 0, len(nodes))
		for _, node := range nodes {
			kinds := node.Kinds.Strings()
			sort.Strings(kinds)

			items = append(items, retriever.FragmentNode{
				ID:         node.ID.String(),
				Kinds:      kinds,
				Properties: node.Properties.MapOrEmpty(),
			})
		}

		return items
	})
}

func benchRelationshipBatch(relationships []*graph.Relationship, options benchOptions) (benchPhaseResult, error) {
	return benchCompressedBatch(len(relationships), options, func() []retriever.FragmentEdge {
		items := make([]retriever.FragmentEdge, 0, len(relationships))
		for _, relationship := range relationships {
			kind := ""
			if relationship.Kind != nil {
				kind = relationship.Kind.String()
			}

			items = append(items, retriever.FragmentEdge{
				StartID:    relationship.StartID.String(),
				EndID:      relationship.EndID.String(),
				Kind:       kind,
				Properties: relationship.Properties.MapOrEmpty(),
			})
		}

		return items
	})
}

func benchCompressedBatch[T any](count int, options benchOptions, buildRecords func() []T) (benchPhaseResult, error) {
	result := benchPhaseResult{
		Count: int64(count),
	}
	if options.Compression == retriever.CompressionDisabled || count == 0 {
		return result, nil
	}

	encodeStarted := time.Now()
	uncompressedBytes, compressedBytes, err := retriever.CompressedJSONLinesSize(options.Compression, options.ZstdLevel, buildRecords())
	result.EncodeCompressTime = time.Since(encodeStarted)

	if err != nil {
		return benchPhaseResult{}, err
	}

	result.UncompressedByteSize = uncompressedBytes
	result.CompressedByteSize = compressedBytes

	return result, nil
}

func benchPlannedCount(total int64, sampleSize int) int64 {
	if total <= 0 {
		return 0
	}

	if sampleSize <= 0 {
		return total
	}

	sampleCount := int64(sampleSize)
	if sampleCount > total {
		return total
	}

	return sampleCount
}

func logBenchPhaseProgress(graphName string, phaseName retriever.Phase, workers int, result benchPhaseResult, planned int64, startedAt time.Time, nextProgressAt int64) int64 {
	if nextProgressAt == 0 || result.Count < nextProgressAt || result.Count >= planned {
		return nextProgressAt
	}

	slog.Info("retriever bench phase progress",
		slog.String("graph", graphName),
		slog.String("phase", string(phaseName)),
		slog.Int("worker_count", workers),
		slog.Int64("processed", result.Count),
		slog.Int64("planned_count", planned),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
		slog.Duration("db_read_elapsed", result.DBReadElapsed),
		slog.Duration("encode_compress_elapsed", result.EncodeCompressTime),
		slog.Float64("entities_per_second", perSecond(result.Count, time.Since(startedAt))),
	)

	return retrieverNextProgressAt(result.Count, planned, nextProgressAt)
}

func writeBenchReport(writer io.Writer, report benchReport) {
	for _, graphReport := range report.Graphs {
		fmt.Fprintf(writer, "graph: %s\n", graphReport.Name)

		for _, result := range graphReport.Results {
			fmt.Fprintf(
				writer,
				"  workers=%d batch=%d sample_size=%d nodes=%d/%d edges=%d/%d total_ms=%d entities_per_sec=%.2f db_read_ms=%d encode_compress_ms=%d\n",
				result.Workers,
				result.BatchSize,
				result.SampleSize,
				result.NodeProcessed,
				result.NodeCount,
				result.EdgeProcessed,
				result.EdgeCount,
				result.TotalWallMillis,
				result.EntitiesPerSecond,
				result.NodeDBReadMillis+result.EdgeDBReadMillis,
				result.NodeEncodeCompressMillis+result.EdgeEncodeCompressMillis,
			)
		}
	}
}

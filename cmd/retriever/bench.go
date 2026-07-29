package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
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
	Format                   string  `json:"format"`
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

func Bench(ctx context.Context, db graph.Database, driverName string, graphNames []string, options benchOptions) (benchReport, error) {
	if err := options.validate(); err != nil {
		return benchReport{}, err
	}

	startedAt := time.Now()
	slog.Info("retriever bench started",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(graphNames)),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("sample_size", options.SampleSize),
		slog.Any("workers", options.Workers),
		slog.Bool("jsonl", options.JSONL.Enabled),
		slog.String("jsonl_codec", string(options.JSONL.Codec)),
		slog.Bool("parquet", options.Parquet.Enabled),
	)

	report := benchReport{
		Driver:      driverName,
		GeneratedAt: time.Now().UTC(),
		Graphs:      make([]benchGraphReport, 0, len(graphNames)),
	}

	for targetIndex, graphName := range graphNames {
		graphStartedAt := time.Now()

		slog.Info("retriever bench graph started",
			slog.String("graph", graphName),
			slog.Int("graph_index", targetIndex+1),
			slog.Int("graph_count", len(graphNames)),
		)

		slog.Info("retriever bench counting graph entities",
			slog.String("graph", graphName),
		)

		source, err := dawgs.NewSource(db, graphName, options.BatchSize)
		if err != nil {
			return benchReport{}, err
		}
		snapshot, err := source.Snapshot(ctx)
		if err != nil {
			return benchReport{}, err
		}

		slog.Info("retriever bench graph counts ready",
			slog.String("graph", graphName),
			slog.Int64("node_count", snapshot.NodeCount),
			slog.Int64("edge_count", snapshot.RelationshipCount),
		)

		graphReport := benchGraphReport{
			Name: graphName,
		}

		for workerIndex, workerCount := range options.Workers {
			if options.JSONL.Enabled {
				result, err := benchJSONLRun(ctx, db, graphName, snapshot, workerCount, workerIndex, options)
				if err != nil {
					return benchReport{}, err
				}
				graphReport.Results = append(graphReport.Results, result)
			}
			if options.Parquet.Enabled {
				result, err := benchParquetRun(ctx, db, graphName, snapshot, workerCount, workerIndex, options)
				if err != nil {
					return benchReport{}, err
				}
				graphReport.Results = append(graphReport.Results, result)
			}
		}

		report.Graphs = append(report.Graphs, graphReport)

		slog.Info("retriever bench graph completed",
			slog.String("graph", graphName),
			slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
		)
	}

	slog.Info("retriever bench completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(graphNames)),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)

	return report, nil
}

func benchJSONLRun(ctx context.Context, db graph.Database, graphName string, snapshot dawgs.Snapshot, workers, workerIndex int, options benchOptions) (benchResult, error) {
	runStartedAt := time.Now()
	logBenchRunStarted(graphName, "jsonl", workers, workerIndex, options)
	nodeResult, err := benchNodes(
		ctx, db, graphName, "jsonl", snapshot.NodeCount, workers, options,
		func(path string, nodes []entity.Node) (benchPhaseResult, error) {
			return benchJSONLNodeBatch(path, nodes, options.JSONL)
		},
	)
	if err != nil {
		return benchResult{}, err
	}
	relationshipResult, err := benchRelationships(
		ctx, db, graphName, "jsonl", snapshot.RelationshipCount, workers, options,
		func(path string, relationships []entity.Relationship) (benchPhaseResult, error) {
			return benchJSONLRelationshipBatch(path, relationships, options.JSONL)
		},
	)
	if err != nil {
		return benchResult{}, err
	}
	result := newBenchResult("jsonl", snapshot, workers, options, nodeResult, relationshipResult)
	logBenchRunCompleted(graphName, result, time.Since(runStartedAt))
	return result, nil
}

func benchParquetRun(ctx context.Context, db graph.Database, graphName string, snapshot dawgs.Snapshot, workers, workerIndex int, options benchOptions) (benchResult, error) {
	runStartedAt := time.Now()
	logBenchRunStarted(graphName, "parquet", workers, workerIndex, options)
	nodeResult, err := benchNodes(
		ctx, db, graphName, "parquet", snapshot.NodeCount, workers, options,
		func(path string, nodes []entity.Node) (benchPhaseResult, error) {
			return benchParquetNodeBatch(path, nodes, options.Parquet)
		},
	)
	if err != nil {
		return benchResult{}, err
	}
	relationshipResult, err := benchRelationships(
		ctx, db, graphName, "parquet", snapshot.RelationshipCount, workers, options,
		func(path string, relationships []entity.Relationship) (benchPhaseResult, error) {
			return benchParquetRelationshipBatch(path, relationships, options.Parquet)
		},
	)
	if err != nil {
		return benchResult{}, err
	}
	result := newBenchResult("parquet", snapshot, workers, options, nodeResult, relationshipResult)
	logBenchRunCompleted(graphName, result, time.Since(runStartedAt))
	return result, nil
}

func logBenchRunStarted(graphName, format string, workers, workerIndex int, options benchOptions) {
	slog.Info("retriever bench worker run started",
		slog.String("graph", graphName),
		slog.String("format", format),
		slog.Int("worker_count", workers),
		slog.Int("worker_index", workerIndex+1),
		slog.Int("worker_runs", len(options.Workers)),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("sample_size", options.SampleSize),
	)
}

func newBenchResult(format string, snapshot dawgs.Snapshot, workers int, options benchOptions, nodeResult, relationshipResult benchPhaseResult) benchResult {
	totalWall := nodeResult.WallElapsed + relationshipResult.WallElapsed
	return benchResult{
		Format:                   format,
		Workers:                  workers,
		BatchSize:                options.BatchSize,
		SampleSize:               options.SampleSize,
		NodeCount:                snapshot.NodeCount,
		EdgeCount:                snapshot.RelationshipCount,
		NodeProcessed:            nodeResult.Count,
		EdgeProcessed:            relationshipResult.Count,
		NodeWallMillis:           nodeResult.WallElapsed.Milliseconds(),
		EdgeWallMillis:           relationshipResult.WallElapsed.Milliseconds(),
		NodeDBReadMillis:         nodeResult.DBReadElapsed.Milliseconds(),
		EdgeDBReadMillis:         relationshipResult.DBReadElapsed.Milliseconds(),
		NodeEncodeCompressMillis: nodeResult.EncodeCompressTime.Milliseconds(),
		EdgeEncodeCompressMillis: relationshipResult.EncodeCompressTime.Milliseconds(),
		TotalWallMillis:          totalWall.Milliseconds(),
		NodesPerSecond:           perSecond(nodeResult.Count, nodeResult.WallElapsed),
		EdgesPerSecond:           perSecond(relationshipResult.Count, relationshipResult.WallElapsed),
		EntitiesPerSecond:        perSecond(nodeResult.Count+relationshipResult.Count, totalWall),
		UncompressedBytes:        nodeResult.UncompressedByteSize + relationshipResult.UncompressedByteSize,
		CompressedBytes:          nodeResult.CompressedByteSize + relationshipResult.CompressedByteSize,
	}
}

func logBenchRunCompleted(graphName string, result benchResult, elapsed time.Duration) {
	slog.Info("retriever bench worker run completed",
		slog.String("graph", graphName),
		slog.String("format", result.Format),
		slog.Int("worker_count", result.Workers),
		slog.Duration("wall_elapsed", elapsed),
		slog.Float64("entities_per_second", result.EntitiesPerSecond),
	)
}

type benchBatchProcessor[T any] struct {
	parent  context.Context
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
			parent:  ctx,
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
	if err := s.parent.Err(); err != nil {
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

type benchArtifactFilesystem struct {
	mkdirTemp func(string, string) (string, error)
	removeAll func(string) error
}

func (s benchArtifactFilesystem) withDefaults() benchArtifactFilesystem {
	if s.mkdirTemp == nil {
		s.mkdirTemp = os.MkdirTemp
	}
	if s.removeAll == nil {
		s.removeAll = os.RemoveAll
	}
	return s
}

func benchNodes(
	ctx context.Context,
	db graph.Database,
	graphName string,
	format string,
	total int64,
	workers int,
	options benchOptions,
	write func(string, []entity.Node) (benchPhaseResult, error),
) (benchPhaseResult, error) {
	return benchNodesWithFilesystem(ctx, db, graphName, format, total, workers, options, write, benchArtifactFilesystem{})
}

func benchNodesWithFilesystem(
	ctx context.Context,
	db graph.Database,
	graphName string,
	format string,
	total int64,
	workers int,
	options benchOptions,
	write func(string, []entity.Node) (benchPhaseResult, error),
	filesystem benchArtifactFilesystem,
) (phaseResult benchPhaseResult, resultErr error) {
	filesystem = filesystem.withDefaults()
	tempDir, err := filesystem.mkdirTemp("", "retriever-bench-"+format+"-nodes-")
	if err != nil {
		return benchPhaseResult{}, fmt.Errorf("create %s node benchmark directory: %w", format, err)
	}
	defer func() {
		if err := filesystem.removeAll(tempDir); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove %s node benchmark directory %q: %w", format, tempDir, err))
		}
	}()

	source, err := dawgs.NewSource(db, graphName, options.BatchSize)
	if err != nil {
		return benchPhaseResult{}, err
	}
	var batchNumber atomic.Int64
	processor, scanCtx, err := newBenchBatchProcessor(ctx, workers, func(nodes []entity.Node) (benchPhaseResult, error) {
		path := filepath.Join(tempDir, fmt.Sprintf("worker-batch-%06d", batchNumber.Add(1)))
		return write(path, nodes)
	})
	if err != nil {
		return benchPhaseResult{}, err
	}

	startedAt := time.Now()
	planned := benchPlannedCount(total, options.SampleSize)
	processed := int64(0)
	nextProgressAt := retrieverInitialProgressAt(planned)
	slog.Info("retriever bench node phase started",
		slog.String("graph", graphName),
		slog.String("format", format),
		slog.Int("worker_count", workers),
		slog.Int64("node_count", total),
		slog.Int64("planned_count", planned),
	)

	for processed < planned {
		readStartedAt := time.Now()
		batch, readErr := source.NextNodes(scanCtx)
		processor.addDBReadElapsed(time.Since(readStartedAt))
		if readErr != nil {
			_, closeErr := processor.closeAndWait()
			return benchPhaseResult{}, errors.Join(readErr, closeErr)
		}
		if len(batch.Entities) == 0 {
			_, closeErr := processor.closeAndWait()
			return benchPhaseResult{}, errors.Join(
				fmt.Errorf("node benchmark scan ended after %d of %d entities", processed, planned),
				closeErr,
			)
		}

		remaining := planned - processed
		if int64(len(batch.Entities)) > remaining {
			batch.Entities = batch.Entities[:remaining]
		}
		if err := processor.handle(batch.Entities); err != nil {
			_, closeErr := processor.closeAndWait()
			return benchPhaseResult{}, errors.Join(err, closeErr)
		}
		processed += int64(len(batch.Entities))

		progress := processor.snapshot()
		progress.Count = processed
		nextProgressAt = logBenchPhaseProgress(graphName, "nodes", workers, progress, planned, startedAt, nextProgressAt)
	}

	phaseResult, err = processor.closeAndWait()
	phaseResult.WallElapsed = time.Since(startedAt)
	if err != nil {
		return benchPhaseResult{}, err
	}
	if phaseResult.Count != planned {
		return benchPhaseResult{}, fmt.Errorf("node benchmark wrote %d of %d planned entities", phaseResult.Count, planned)
	}
	slog.Info("retriever bench node phase completed",
		slog.String("graph", graphName),
		slog.String("format", format),
		slog.Int("worker_count", workers),
		slog.Int64("processed", phaseResult.Count),
		slog.Duration("wall_elapsed", phaseResult.WallElapsed),
		slog.Duration("db_read_elapsed", phaseResult.DBReadElapsed),
		slog.Duration("encode_compress_elapsed", phaseResult.EncodeCompressTime),
		slog.Float64("entities_per_second", perSecond(phaseResult.Count, phaseResult.WallElapsed)),
	)
	return phaseResult, nil
}

func benchRelationships(
	ctx context.Context,
	db graph.Database,
	graphName string,
	format string,
	total int64,
	workers int,
	options benchOptions,
	write func(string, []entity.Relationship) (benchPhaseResult, error),
) (benchPhaseResult, error) {
	return benchRelationshipsWithFilesystem(ctx, db, graphName, format, total, workers, options, write, benchArtifactFilesystem{})
}

func benchRelationshipsWithFilesystem(
	ctx context.Context,
	db graph.Database,
	graphName string,
	format string,
	total int64,
	workers int,
	options benchOptions,
	write func(string, []entity.Relationship) (benchPhaseResult, error),
	filesystem benchArtifactFilesystem,
) (phaseResult benchPhaseResult, resultErr error) {
	filesystem = filesystem.withDefaults()
	tempDir, err := filesystem.mkdirTemp("", "retriever-bench-"+format+"-relationships-")
	if err != nil {
		return benchPhaseResult{}, fmt.Errorf("create %s relationship benchmark directory: %w", format, err)
	}
	defer func() {
		if err := filesystem.removeAll(tempDir); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove %s relationship benchmark directory %q: %w", format, tempDir, err))
		}
	}()

	source, err := dawgs.NewSource(db, graphName, options.BatchSize)
	if err != nil {
		return benchPhaseResult{}, err
	}
	var batchNumber atomic.Int64
	processor, scanCtx, err := newBenchBatchProcessor(ctx, workers, func(relationships []entity.Relationship) (benchPhaseResult, error) {
		path := filepath.Join(tempDir, fmt.Sprintf("worker-batch-%06d", batchNumber.Add(1)))
		return write(path, relationships)
	})
	if err != nil {
		return benchPhaseResult{}, err
	}

	startedAt := time.Now()
	planned := benchPlannedCount(total, options.SampleSize)
	processed := int64(0)
	nextProgressAt := retrieverInitialProgressAt(planned)
	slog.Info("retriever bench relationship phase started",
		slog.String("graph", graphName),
		slog.String("format", format),
		slog.Int("worker_count", workers),
		slog.Int64("relationship_count", total),
		slog.Int64("planned_count", planned),
	)

	for processed < planned {
		readStartedAt := time.Now()
		batch, readErr := source.NextRelationships(scanCtx)
		processor.addDBReadElapsed(time.Since(readStartedAt))
		if readErr != nil {
			_, closeErr := processor.closeAndWait()
			return benchPhaseResult{}, errors.Join(readErr, closeErr)
		}
		if len(batch.Entities) == 0 {
			_, closeErr := processor.closeAndWait()
			return benchPhaseResult{}, errors.Join(
				fmt.Errorf("relationship benchmark scan ended after %d of %d entities", processed, planned),
				closeErr,
			)
		}

		remaining := planned - processed
		if int64(len(batch.Entities)) > remaining {
			batch.Entities = batch.Entities[:remaining]
		}
		if err := processor.handle(batch.Entities); err != nil {
			_, closeErr := processor.closeAndWait()
			return benchPhaseResult{}, errors.Join(err, closeErr)
		}
		processed += int64(len(batch.Entities))

		progress := processor.snapshot()
		progress.Count = processed
		nextProgressAt = logBenchPhaseProgress(graphName, "relationships", workers, progress, planned, startedAt, nextProgressAt)
	}

	phaseResult, err = processor.closeAndWait()
	phaseResult.WallElapsed = time.Since(startedAt)
	if err != nil {
		return benchPhaseResult{}, err
	}
	if phaseResult.Count != planned {
		return benchPhaseResult{}, fmt.Errorf("relationship benchmark wrote %d of %d planned entities", phaseResult.Count, planned)
	}
	slog.Info("retriever bench relationship phase completed",
		slog.String("graph", graphName),
		slog.String("format", format),
		slog.Int("worker_count", workers),
		slog.Int64("processed", phaseResult.Count),
		slog.Duration("wall_elapsed", phaseResult.WallElapsed),
		slog.Duration("db_read_elapsed", phaseResult.DBReadElapsed),
		slog.Duration("encode_compress_elapsed", phaseResult.EncodeCompressTime),
		slog.Float64("entities_per_second", perSecond(phaseResult.Count, phaseResult.WallElapsed)),
	)
	return phaseResult, nil
}

func benchJSONLNodeBatch(path string, nodes []entity.Node, config jsonl.Config) (benchPhaseResult, error) {
	startedAt := time.Now()
	artifact, err := jsonl.WriteNodes(path, filepath.Base(path), config, nodes)
	elapsed := time.Since(startedAt)
	if err != nil {
		return benchPhaseResult{}, err
	}
	return benchPhaseResult{
		Count:                artifact.Count,
		EncodeCompressTime:   elapsed,
		UncompressedByteSize: artifact.UncompressedBytes,
		CompressedByteSize:   artifact.StoredBytes,
	}, nil
}

func benchJSONLRelationshipBatch(path string, relationships []entity.Relationship, config jsonl.Config) (benchPhaseResult, error) {
	startedAt := time.Now()
	artifact, err := jsonl.WriteRelationships(path, filepath.Base(path), config, relationships)
	elapsed := time.Since(startedAt)
	if err != nil {
		return benchPhaseResult{}, err
	}
	return benchPhaseResult{
		Count:                artifact.Count,
		EncodeCompressTime:   elapsed,
		UncompressedByteSize: artifact.UncompressedBytes,
		CompressedByteSize:   artifact.StoredBytes,
	}, nil
}

func benchParquetNodeBatch(path string, nodes []entity.Node, config parquet.Config) (benchPhaseResult, error) {
	startedAt := time.Now()
	artifact, err := parquet.WriteNodes(path, filepath.Base(path), config, nodes)
	elapsed := time.Since(startedAt)
	if err != nil {
		return benchPhaseResult{}, err
	}
	return benchPhaseResult{
		Count:              artifact.Count,
		EncodeCompressTime: elapsed,
		CompressedByteSize: artifact.StoredBytes,
	}, nil
}

func benchParquetRelationshipBatch(path string, relationships []entity.Relationship, config parquet.Config) (benchPhaseResult, error) {
	startedAt := time.Now()
	artifact, err := parquet.WriteRelationships(path, filepath.Base(path), config, relationships)
	elapsed := time.Since(startedAt)
	if err != nil {
		return benchPhaseResult{}, err
	}
	return benchPhaseResult{
		Count:              artifact.Count,
		EncodeCompressTime: elapsed,
		CompressedByteSize: artifact.StoredBytes,
	}, nil
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

func logBenchPhaseProgress(graphName string, phaseName string, workers int, result benchPhaseResult, planned int64, startedAt time.Time, nextProgressAt int64) int64 {
	if nextProgressAt == 0 || result.Count < nextProgressAt || result.Count >= planned {
		return nextProgressAt
	}

	slog.Info("retriever bench phase progress",
		slog.String("graph", graphName),
		slog.String("phase", phaseName),
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
				"  format=%s workers=%d batch=%d sample_size=%d nodes=%d/%d edges=%d/%d total_ms=%d entities_per_sec=%.2f db_read_ms=%d encode_compress_ms=%d\n",
				result.Format,
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

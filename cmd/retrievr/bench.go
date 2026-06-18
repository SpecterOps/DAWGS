package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
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
	NodeCount                int64   `json:"node_count"`
	EdgeCount                int64   `json:"edge_count"`
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

func Bench(ctx context.Context, db graph.Database, driverName string, targets []graphTarget, options benchOptions) (benchReport, error) {
	if err := options.validate(); err != nil {
		return benchReport{}, err
	}

	report := benchReport{
		Driver:      driverName,
		GeneratedAt: time.Now().UTC(),
		Graphs:      make([]benchGraphReport, 0, len(targets)),
	}
	for _, target := range targets {
		targetGraph := graph.Graph{Name: target.Name}
		nodeCount, edgeCount, err := countGraphEntities(ctx, db, targetGraph)
		if err != nil {
			return benchReport{}, err
		}

		graphReport := benchGraphReport{Name: target.Name}
		for _, workerCount := range options.Workers {
			nodeResult, err := benchNodes(ctx, db, targetGraph, nodeCount, workerCount, options)
			if err != nil {
				return benchReport{}, err
			}
			edgeResult, err := benchEdges(ctx, db, targetGraph, edgeCount, workerCount, options)
			if err != nil {
				return benchReport{}, err
			}

			totalWall := nodeResult.WallElapsed + edgeResult.WallElapsed
			graphReport.Results = append(graphReport.Results, benchResult{
				Workers:                  workerCount,
				BatchSize:                options.BatchSize,
				NodeCount:                nodeCount,
				EdgeCount:                edgeCount,
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
		}
		report.Graphs = append(report.Graphs, graphReport)
	}
	return report, nil
}

func benchNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, workers int, options benchOptions) (benchPhaseResult, error) {
	startedAt := time.Now()
	result, err := runOffsetWorkers(ctx, int(total), workers, options.BatchSize, func(offset int) (benchPhaseResult, error) {
		readStarted := time.Now()
		nodes, err := readDatabaseNodesOffset(ctx, db, targetGraph, offset, options.BatchSize)
		readElapsed := time.Since(readStarted)
		if err != nil {
			return benchPhaseResult{}, err
		}

		workerResult := benchPhaseResult{
			Count:         int64(len(nodes)),
			DBReadElapsed: readElapsed,
		}
		if options.Compression != compressionNone && len(nodes) > 0 {
			items := make([]fragmentNode, 0, len(nodes))
			for _, node := range nodes {
				kinds := node.Kinds.Strings()
				sort.Strings(kinds)
				items = append(items, fragmentNode{
					ID:         node.ID.String(),
					Kinds:      kinds,
					Properties: node.Properties.MapOrEmpty(),
				})
			}
			encodeStarted := time.Now()
			uncompressedBytes, compressedBytes, err := compressedJSONSize(options.Compression, options.ZstdLevel, nodeFragment{
				Phase: phaseNodes,
				Items: items,
			})
			workerResult.EncodeCompressTime = time.Since(encodeStarted)
			if err != nil {
				return benchPhaseResult{}, err
			}
			workerResult.UncompressedByteSize = uncompressedBytes
			workerResult.CompressedByteSize = compressedBytes
		}
		return workerResult, nil
	})
	result.WallElapsed = time.Since(startedAt)
	return result, err
}

func benchEdges(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, workers int, options benchOptions) (benchPhaseResult, error) {
	startedAt := time.Now()
	result, err := runOffsetWorkers(ctx, int(total), workers, options.BatchSize, func(offset int) (benchPhaseResult, error) {
		readStarted := time.Now()
		relationships, err := readDatabaseRelationshipsOffset(ctx, db, targetGraph, offset, options.BatchSize)
		readElapsed := time.Since(readStarted)
		if err != nil {
			return benchPhaseResult{}, err
		}

		workerResult := benchPhaseResult{
			Count:         int64(len(relationships)),
			DBReadElapsed: readElapsed,
		}
		if options.Compression != compressionNone && len(relationships) > 0 {
			items := make([]fragmentEdge, 0, len(relationships))
			for _, relationship := range relationships {
				kind := ""
				if relationship.Kind != nil {
					kind = relationship.Kind.String()
				}
				items = append(items, fragmentEdge{
					StartID:    relationship.StartID.String(),
					EndID:      relationship.EndID.String(),
					Kind:       kind,
					Properties: relationship.Properties.MapOrEmpty(),
				})
			}
			encodeStarted := time.Now()
			uncompressedBytes, compressedBytes, err := compressedJSONSize(options.Compression, options.ZstdLevel, edgeFragment{
				Phase: phaseEdges,
				Items: items,
			})
			workerResult.EncodeCompressTime = time.Since(encodeStarted)
			if err != nil {
				return benchPhaseResult{}, err
			}
			workerResult.UncompressedByteSize = uncompressedBytes
			workerResult.CompressedByteSize = compressedBytes
		}
		return workerResult, nil
	})
	result.WallElapsed = time.Since(startedAt)
	return result, err
}

func runOffsetWorkers(ctx context.Context, total int, workers int, batchSize int, read func(offset int) (benchPhaseResult, error)) (benchPhaseResult, error) {
	if total == 0 {
		return benchPhaseResult{}, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	results := make(chan benchPhaseResult, workers)
	errors := make(chan error, 1)

	var waitGroup sync.WaitGroup
	for range workers {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			var workerResult benchPhaseResult
			for offset := range jobs {
				nextResult, err := read(offset)
				if err != nil {
					select {
					case errors <- err:
					default:
					}
					cancel()
					return
				}
				workerResult.Count += nextResult.Count
				workerResult.DBReadElapsed += nextResult.DBReadElapsed
				workerResult.EncodeCompressTime += nextResult.EncodeCompressTime
				workerResult.UncompressedByteSize += nextResult.UncompressedByteSize
				workerResult.CompressedByteSize += nextResult.CompressedByteSize
			}
			results <- workerResult
		}()
	}

	go func() {
		defer close(jobs)
		for offset := 0; offset < total; offset += batchSize {
			select {
			case <-ctx.Done():
				return
			case jobs <- offset:
			}
		}
	}()

	waitGroup.Wait()
	close(results)

	select {
	case err := <-errors:
		return benchPhaseResult{}, err
	default:
	}

	var combined benchPhaseResult
	for nextResult := range results {
		combined.Count += nextResult.Count
		combined.DBReadElapsed += nextResult.DBReadElapsed
		combined.EncodeCompressTime += nextResult.EncodeCompressTime
		combined.UncompressedByteSize += nextResult.UncompressedByteSize
		combined.CompressedByteSize += nextResult.CompressedByteSize
	}
	return combined, nil
}

func readDatabaseNodesOffset(ctx context.Context, db graph.Database, targetGraph graph.Graph, offset int, batchSize int) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		return tx.Nodes().
			OrderBy(query.NodeID()).
			Offset(offset).
			Limit(batchSize).
			Fetch(func(cursor graph.Cursor[*graph.Node]) error {
				for node := range cursor.Chan() {
					nodes = append(nodes, node)
				}
				return cursor.Error()
			})
	}); err != nil {
		return nil, fmt.Errorf("read node batch at offset %d: %w", offset, err)
	}
	return nodes, nil
}

func readDatabaseRelationshipsOffset(ctx context.Context, db graph.Database, targetGraph graph.Graph, offset int, batchSize int) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		return tx.Relationships().
			OrderBy(query.RelationshipID()).
			Offset(offset).
			Limit(batchSize).
			Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
				for relationship := range cursor.Chan() {
					relationships = append(relationships, relationship)
				}
				return cursor.Error()
			})
	}); err != nil {
		return nil, fmt.Errorf("read relationship batch at offset %d: %w", offset, err)
	}
	return relationships, nil
}

func perSecond(count int64, elapsed time.Duration) float64 {
	if count == 0 || elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func writeBenchReport(writer io.Writer, report benchReport) {
	for _, graphReport := range report.Graphs {
		fmt.Fprintf(writer, "graph: %s\n", graphReport.Name)
		for _, result := range graphReport.Results {
			fmt.Fprintf(
				writer,
				"  workers=%d batch=%d nodes=%d edges=%d total_ms=%d entities_per_sec=%.2f db_read_ms=%d encode_compress_ms=%d\n",
				result.Workers,
				result.BatchSize,
				result.NodeCount,
				result.EdgeCount,
				result.TotalWallMillis,
				result.EntitiesPerSecond,
				result.NodeDBReadMillis+result.EdgeDBReadMillis,
				result.NodeEncodeCompressMillis+result.EdgeEncodeCompressMillis,
			)
		}
	}
}

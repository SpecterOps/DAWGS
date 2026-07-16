package retriever

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/specterops/dawgs/graph"
)

type DumpResult struct {
	Manifest     Manifest
	ManifestPath string
	NodeCount    int64
	EdgeCount    int64
}

func Dump(ctx context.Context, db graph.Database, driverName string, targets []GraphTarget, options DumpOptions) (DumpResult, error) {
	return dumpWithSource(ctx, newDatabaseGraphSource(db), driverName, targets, options)
}

func dumpWithSource(ctx context.Context, source graphSource, driverName string, targets []GraphTarget, options DumpOptions) (DumpResult, error) {
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

	var (
		activeScrubber *scrubber

		scrubInfo = ScrubMetadata{
			Mode:             ScrubNone,
			NodeActionCounts: map[string]int{},
			EdgeActionCounts: map[string]int{},
		}
	)
	if options.Scrub == ScrubFull {
		nextScrubber, err := newScrubber(options.ScrubConfig, options.Salt)
		if err != nil {
			return DumpResult{}, err
		}
		activeScrubber = nextScrubber
		scrubInfo = activeScrubber.metadata()
	}

	slog.Info("retriever dump preparing output directory",
		slog.String("output_dir", options.OutputDir),
		slog.Bool("force", options.Force),
	)
	if err := prepareOutputDirectory(options.OutputDir, options.Force); err != nil {
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

	nextManifest := newManifest(driverName, options.Compression, options.ZstdLevel, scrubInfo, len(targets))
	nextMetrics := newMetricsManifest(len(targets))
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

		graphEntry, schemaEntry, metricsEntry, err := dumpGraph(ctx, source, target, options, activeScrubber)
		if err != nil {
			return DumpResult{}, err
		}

		nextManifest.Graphs = append(nextManifest.Graphs, graphEntry)
		nextManifest.Schema.Graphs = append(nextManifest.Schema.Graphs, schemaEntry)
		nextMetrics.Graphs = append(nextMetrics.Graphs, metricsEntry)

		addActionCounts(nextManifest.Scrub.NodeActionCounts, graphEntry.NodeActionCounts)
		addActionCounts(nextManifest.Scrub.EdgeActionCounts, graphEntry.EdgeActionCounts)

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

	nextManifest.Metrics = &nextMetrics

	slog.Info("retriever dump writing manifest",
		slog.String("output_dir", options.OutputDir),
		slog.Int64("node_count", totalNodes),
		slog.Int64("edge_count", totalEdges),
	)
	if err := writeManifest(options.OutputDir, nextManifest); err != nil {
		return DumpResult{}, err
	}

	manifestPath := filepath.Join(options.OutputDir, manifestFileName)

	slog.Info("retriever dump completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.String("manifest", manifestPath),
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
		Manifest:     nextManifest,
		ManifestPath: manifestPath,
		NodeCount:    totalNodes,
		EdgeCount:    totalEdges,
	}, nil
}

func prepareOutputDirectory(outputDir string, force bool) error {
	info, err := os.Stat(outputDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("output path %q exists and is not a directory", outputDir)
		}

		entries, err := os.ReadDir(outputDir)
		if err != nil {
			return fmt.Errorf("read output directory: %w", err)
		}

		if len(entries) > 0 {
			if !force {
				return fmt.Errorf("output directory %q is not empty; pass -force to replace it", outputDir)
			}

			if err := os.RemoveAll(outputDir); err != nil {
				return fmt.Errorf("replace output directory: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect output directory: %w", err)
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	return nil
}

func dumpGraph(ctx context.Context, source graphSource, target GraphTarget, options DumpOptions, activeScrubber *scrubber) (GraphManifest, GraphSchemaMetadata, GraphMetrics, error) {
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

	if activeScrubber != nil {
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

		observedNodes, err := collectScrubRegistry(ctx, source, targetGraph, options.BatchSize, activeScrubber, entitySnapshot, options.Progress, options.ProgressInterval)
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
		Name:             target.Name,
		NodeCount:        entitySnapshot.NodeCount,
		EdgeCount:        entitySnapshot.EdgeCount,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}

	nodeKinds := map[string]struct{}{}
	edgeKinds := map[string]struct{}{}
	metricsBuilder := newMetricsBuilder(target.Name, entitySnapshot.NodeCount)

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

	nodeFiles, err := dumpNodePhase(ctx, source, targetGraph, options, activeScrubber, nodeKinds, graphEntry.NodeActionCounts, entitySnapshot, metricsBuilder)
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

	edgeFiles, err := dumpEdgePhase(ctx, source, targetGraph, options, activeScrubber, edgeKinds, graphEntry.EdgeActionCounts, entitySnapshot, metricsBuilder)
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

	if fileTotal(nodeFiles) != entitySnapshot.NodeCount {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, EntityCountMismatchError{
			Operation: OperationDump,
			Graph:     target.Name,
			Phase:     PhaseNodes,
			Expected:  entitySnapshot.NodeCount,
			Actual:    fileTotal(nodeFiles),
			Message:   fmt.Sprintf("dumped %d nodes for graph %q but counted %d at scan start; source graph changed during dump or the ID scan was inconsistent", fileTotal(nodeFiles), target.Name, entitySnapshot.NodeCount),
		}
	}

	if fileTotal(edgeFiles) != entitySnapshot.EdgeCount {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, EntityCountMismatchError{
			Operation: OperationDump,
			Graph:     target.Name,
			Phase:     PhaseEdges,
			Expected:  entitySnapshot.EdgeCount,
			Actual:    fileTotal(edgeFiles),
			Message:   fmt.Sprintf("dumped %d relationships for graph %q but counted %d at scan start; source graph changed during dump or the ID scan was inconsistent", fileTotal(edgeFiles), target.Name, entitySnapshot.EdgeCount),
		}
	}

	metricsEntry := metricsBuilder.finalize()

	slog.Info("retriever dump metrics fingerprint computed",
		slog.String("graph", target.Name),
		slog.String("fingerprint", metricsEntry.Fingerprint),
		slog.Int64("node_count", metricsEntry.NodeCount),
		slog.Int64("edge_count", metricsEntry.EdgeCount),
	)

	schemaEntry := GraphSchemaMetadata{
		Name:      target.Name,
		NodeKinds: stringsFromKindSet(nodeKinds),
		EdgeKinds: stringsFromKindSet(edgeKinds),
	}

	return graphEntry, schemaEntry, metricsEntry, nil
}

func collectScrubRegistry(ctx context.Context, source graphSource, targetGraph graph.Graph, batchSize int, activeScrubber *scrubber, entitySnapshot graphEntitySnapshot, progress ProgressFunc, progressInterval int64) (int64, error) {
	processed, err := source.Nodes(targetGraph, entitySnapshot.NodeCount, batchSize, progressInterval, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump scrub pre-pass progress", targetGraph.Name, PhaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt, progress, progressInterval)
	}).Run(ctx, func(nodes []*graph.Node) error {
		for _, node := range nodes {
			activeScrubber.observeNode(node.Properties.MapOrEmpty())
		}
		return nil
	})
	if err != nil {
		return processed, fmt.Errorf("scrub pre-pass: %w", err)
	}

	return processed, nil
}

func dumpNodePhase(ctx context.Context, source graphSource, targetGraph graph.Graph, options DumpOptions, activeScrubber *scrubber, nodeKinds map[string]struct{}, graphActionCounts map[string]int, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder) ([]FileManifest, error) {
	if entitySnapshot.NodeCount == 0 {
		return nil, nil
	}

	var (
		files                []FileManifest
		fragmentWriter       *compressedJSONLinesWriter
		fragmentRelativePath string
		shardActionCounts    = map[string]int{}
		shardNumber          = 1
	)

	flush := func() error {
		if fragmentWriter == nil {
			return nil
		}

		fileEntry, err := closeFragmentWriter(fragmentWriter, fragmentRelativePath, PhaseNodes, shardActionCounts)
		fragmentWriter = nil
		if err != nil {
			return err
		}

		files = append(files, fileEntry)
		shardActionCounts = map[string]int{}
		shardNumber++

		return nil
	}

	if _, err := source.Nodes(targetGraph, entitySnapshot.NodeCount, options.BatchSize, options.ProgressInterval, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump node phase progress", targetGraph.Name, PhaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt, options.Progress, options.ProgressInterval)
	}).Run(ctx, func(nodes []*graph.Node) error {
		for _, node := range nodes {
			if fragmentWriter == nil {
				nextWriter, nextRelativePath, err := openFragmentWriter(options.OutputDir, targetGraph.Name, PhaseNodes, shardNumber, options)
				if err != nil {
					return err
				}

				fragmentWriter = nextWriter
				fragmentRelativePath = nextRelativePath
			}

			record := normalizeNode(node)
			addKindsToSet(nodeKinds, record.Kinds)

			if activeScrubber != nil {
				var actionCounts map[string]int
				record.Properties, actionCounts = activeScrubber.scrubProperties(record.Properties)
				addActionCounts(shardActionCounts, actionCounts)
				addActionCounts(graphActionCounts, actionCounts)
			}

			item := jsonlV1NodeFromNormalized(record)

			if err := metricsBuilder.observeFragmentNode(item); err != nil {
				return err
			}

			if err := fragmentWriter.Write(item); err != nil {
				return err
			}

			if fragmentWriter.Count() >= options.ShardSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		if fragmentWriter != nil {
			fragmentWriter.Abort()
		}

		return nil, err
	}

	if err := flush(); err != nil {
		return nil, err
	}

	return files, nil
}

func dumpEdgePhase(ctx context.Context, source graphSource, targetGraph graph.Graph, options DumpOptions, activeScrubber *scrubber, edgeKinds map[string]struct{}, graphActionCounts map[string]int, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder) ([]FileManifest, error) {
	if entitySnapshot.EdgeCount == 0 {
		return nil, nil
	}

	var (
		files                []FileManifest
		fragmentWriter       *compressedJSONLinesWriter
		fragmentRelativePath string
		shardActionCounts    = map[string]int{}
		shardNumber          = 1
	)

	flush := func() error {
		if fragmentWriter == nil {
			return nil
		}

		fileEntry, err := closeFragmentWriter(fragmentWriter, fragmentRelativePath, PhaseEdges, shardActionCounts)
		fragmentWriter = nil
		if err != nil {
			return err
		}

		files = append(files, fileEntry)
		shardActionCounts = map[string]int{}
		shardNumber++

		return nil
	}

	if _, err := source.Edges(targetGraph, entitySnapshot.EdgeCount, options.BatchSize, options.ProgressInterval, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump edge phase progress", targetGraph.Name, PhaseEdges, processed, entitySnapshot.EdgeCount, startedAt, nextProgressAt, options.Progress, options.ProgressInterval)
	}).Run(ctx, func(relationships []*graph.Relationship) error {
		for _, relationship := range relationships {
			if fragmentWriter == nil {
				nextWriter, nextRelativePath, err := openFragmentWriter(options.OutputDir, targetGraph.Name, PhaseEdges, shardNumber, options)
				if err != nil {
					return err
				}

				fragmentWriter = nextWriter
				fragmentRelativePath = nextRelativePath
			}

			record := normalizeEdge(relationship)
			if relationship.Kind != nil {
				edgeKinds[record.Kind] = struct{}{}
			}

			if activeScrubber != nil {
				var actionCounts map[string]int
				record.Properties, actionCounts = activeScrubber.scrubProperties(record.Properties)
				addActionCounts(shardActionCounts, actionCounts)
				addActionCounts(graphActionCounts, actionCounts)
			}

			item := jsonlV1EdgeFromNormalized(record)

			if err := metricsBuilder.observeFragmentEdge(item); err != nil {
				return err
			}

			if err := fragmentWriter.Write(item); err != nil {
				return err
			}

			if fragmentWriter.Count() >= options.ShardSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		if fragmentWriter != nil {
			fragmentWriter.Abort()
		}

		return nil, err
	}

	if err := flush(); err != nil {
		return nil, err
	}

	return files, nil
}

func writeNodeFragment(outputDir, graphName string, shardNumber int, options DumpOptions, items []FragmentNode, actionCounts map[string]int) (FileManifest, error) {
	relativePath, err := fragmentPath(graphName, PhaseNodes, shardNumber, options.Compression)
	if err != nil {
		return FileManifest{}, err
	}

	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	fileEntry, err := writeCompressedJSONLines(absolutePath, options.Compression, options.ZstdLevel, items)
	if err != nil {
		return FileManifest{}, err
	}

	fileEntry.Phase = PhaseNodes
	fileEntry.Path = relativePath
	fileEntry.Count = len(items)
	fileEntry.ActionCounts = cloneActionCounts(actionCounts)

	return fileEntry, nil
}

func writeEdgeFragment(outputDir, graphName string, shardNumber int, options DumpOptions, items []FragmentEdge, actionCounts map[string]int) (FileManifest, error) {
	relativePath, err := fragmentPath(graphName, PhaseEdges, shardNumber, options.Compression)
	if err != nil {
		return FileManifest{}, err
	}

	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	fileEntry, err := writeCompressedJSONLines(absolutePath, options.Compression, options.ZstdLevel, items)
	if err != nil {
		return FileManifest{}, err
	}

	fileEntry.Phase = PhaseEdges
	fileEntry.Path = relativePath
	fileEntry.Count = len(items)
	fileEntry.ActionCounts = cloneActionCounts(actionCounts)

	return fileEntry, nil
}

func fragmentPath(graphName string, fragmentPhase Phase, shardNumber int, codec CompressionCodec) (string, error) {
	if shardNumber <= 0 {
		return "", fmt.Errorf("shard number must be > 0")
	}

	extension, err := compressionExtension(codec)
	if err != nil {
		return "", err
	}

	var prefix string
	switch fragmentPhase {
	case PhaseNodes:
		prefix = "nodes"
	case PhaseEdges:
		prefix = "edges"
	default:
		return "", fmt.Errorf("unsupported fragment phase %q", fragmentPhase)
	}

	return path.Join("graphs", graphDirectoryName(graphName), fmt.Sprintf("%s-%06d.jsonl%s", prefix, shardNumber, extension)), nil
}

func openFragmentWriter(outputDir, graphName string, fragmentPhase Phase, shardNumber int, options DumpOptions) (*compressedJSONLinesWriter, string, error) {
	relativePath, err := fragmentPath(graphName, fragmentPhase, shardNumber, options.Compression)
	if err != nil {
		return nil, "", err
	}

	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	writer, err := newCompressedJSONLinesWriter(absolutePath, options.Compression, options.ZstdLevel)
	if err != nil {
		return nil, "", err
	}

	return writer, relativePath, nil
}

func closeFragmentWriter(writer *compressedJSONLinesWriter, relativePath string, fragmentPhase Phase, actionCounts map[string]int) (FileManifest, error) {
	fileEntry, err := writer.Close()
	if err != nil {
		return FileManifest{}, err
	}

	fileEntry.Phase = fragmentPhase
	fileEntry.Path = relativePath
	fileEntry.ActionCounts = cloneActionCounts(actionCounts)

	return fileEntry, nil
}

func fileTotal(files []FileManifest) int64 {
	var total int64
	for _, fileEntry := range files {
		total += int64(fileEntry.Count)
	}

	return total
}

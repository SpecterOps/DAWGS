package retriever

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/specterops/dawgs/graph"
)

type DumpResult struct {
	Manifest     Manifest
	ManifestPath string
	NodeCount    int64
	EdgeCount    int64
}

type graphEntitySnapshot struct {
	NodeCount int64 `json:"node_count"`
	EdgeCount int64 `json:"edge_count"`
}

func Dump(ctx context.Context, db graph.Database, driverName string, targets []GraphTarget, options DumpOptions) (DumpResult, error) {
	if err := options.validate(); err != nil {
		return DumpResult{}, err
	}

	if len(targets) == 0 {
		return DumpResult{}, fmt.Errorf("at least one graph target is required")
	}
	seenTargets := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target.Name == "" {
			return DumpResult{}, fmt.Errorf("graph target name is required")
		}
		if _, found := seenTargets[target.Name]; found {
			return DumpResult{}, fmt.Errorf("duplicate graph target %q", target.Name)
		}
		seenTargets[target.Name] = struct{}{}
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
	checkpointIdentity, err := newDumpCheckpointIdentity(driverName, targets, options, activeScrubber)
	if err != nil {
		return DumpResult{}, err
	}

	slog.Info("retriever dump preparing output directory",
		slog.String("output_dir", options.OutputDir),
		slog.Bool("force", options.Force),
		slog.Bool("resume", options.Resume),
	)

	var checkpoint dumpCheckpoint
	if options.Resume {
		checkpoint, err = loadCompatibleDumpCheckpoint(options.OutputDir, checkpointIdentity, len(targets))
		if err != nil {
			return DumpResult{}, err
		}
		if err := validateCompletedDumpSources(ctx, db, checkpoint.Manifest.Graphs); err != nil {
			return DumpResult{}, err
		}
	} else {
		if err := prepareOutputDirectory(options.OutputDir, options.Force); err != nil {
			return DumpResult{}, err
		}
		nextManifest := newManifest(driverName, options.Compression, options.ZstdLevel, scrubInfo, len(targets))
		nextMetrics := newMetricsManifest(len(targets))
		nextManifest.Metrics = &nextMetrics
		checkpoint = dumpCheckpoint{
			Version:  dumpCheckpointVersion,
			Identity: checkpointIdentity,
			Manifest: nextManifest,
		}
		if err := writeDumpCheckpoint(options.OutputDir, checkpoint); err != nil {
			return DumpResult{}, err
		}
	}

	slog.Info("retriever dump output directory ready",
		slog.String("output_dir", options.OutputDir),
		slog.Bool("resumed", options.Resume),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationDump,
		Message:   "retriever dump output directory ready",
		OutputDir: options.OutputDir,
	})

	var totalNodes, totalEdges int64
	for _, graphEntry := range checkpoint.Manifest.Graphs {
		totalNodes += graphEntry.NodeCount
		totalEdges += graphEntry.EdgeCount
	}

	for targetIndex := len(checkpoint.Manifest.Graphs); targetIndex < len(targets); targetIndex++ {
		target := targets[targetIndex]
		graphScrubber := activeScrubber
		if graphScrubber != nil {
			graphScrubber = graphScrubber.forGraph()
		}

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

		if checkpoint.Current == nil {
			checkpoint.Current = &dumpGraphCheckpoint{
				Index: targetIndex,
				Name:  target.Name,
				Phase: PhaseNodes,
			}
		}
		persistCheckpoint := func() error {
			return writeDumpCheckpoint(options.OutputDir, checkpoint)
		}

		graphEntry, schemaEntry, metricsEntry, err := dumpGraph(ctx, db, target, options, graphScrubber, checkpoint.Current, persistCheckpoint)
		if err != nil {
			return DumpResult{}, err
		}

		checkpoint.Manifest.Graphs = append(checkpoint.Manifest.Graphs, graphEntry)
		checkpoint.Manifest.Schema.Graphs = append(checkpoint.Manifest.Schema.Graphs, schemaEntry)
		checkpoint.Manifest.Metrics.Graphs = append(checkpoint.Manifest.Metrics.Graphs, metricsEntry)

		addActionCounts(checkpoint.Manifest.Scrub.NodeActionCounts, graphEntry.NodeActionCounts)
		addActionCounts(checkpoint.Manifest.Scrub.EdgeActionCounts, graphEntry.EdgeActionCounts)
		checkpoint.Current = nil
		if err := writeDumpCheckpoint(options.OutputDir, checkpoint); err != nil {
			return DumpResult{}, err
		}

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
	if err := writeManifest(options.OutputDir, checkpoint.Manifest); err != nil {
		return DumpResult{}, err
	}
	if err := removeDumpCheckpoint(options.OutputDir); err != nil {
		slog.Warn("retriever dump completed but checkpoint cleanup failed", slog.String("error", err.Error()))
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
		Manifest:     checkpoint.Manifest,
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

func dumpGraph(ctx context.Context, db graph.Database, target GraphTarget, options DumpOptions, activeScrubber *scrubber, checkpoint *dumpGraphCheckpoint, persistCheckpoint func() error) (GraphManifest, GraphSchemaMetadata, GraphMetrics, error) {
	targetGraph := graph.Graph{
		Name: target.Name,
	}

	countStartedAt := time.Now()
	slog.Info("retriever dump counting graph entities",
		slog.String("graph", target.Name),
	)
	currentSnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
	}
	if checkpoint.HasSnapshot {
		if checkpoint.Snapshot != currentSnapshot {
			return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, fmt.Errorf("dump checkpoint source counts for graph %q changed from nodes=%d edges=%d to nodes=%d edges=%d; refusing to resume an inconsistent keyset snapshot", target.Name, checkpoint.Snapshot.NodeCount, checkpoint.Snapshot.EdgeCount, currentSnapshot.NodeCount, currentSnapshot.EdgeCount)
		}
	} else {
		checkpoint.Snapshot = currentSnapshot
		checkpoint.HasSnapshot = true
		if err := persistCheckpoint(); err != nil {
			return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
		}
	}
	entitySnapshot := checkpoint.Snapshot

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

	graphEntry := GraphManifest{
		Name:             target.Name,
		NodeCount:        entitySnapshot.NodeCount,
		EdgeCount:        entitySnapshot.EdgeCount,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
		Files:            append([]FileManifest(nil), checkpoint.Files...),
	}
	nodeActionCounts := checkpointActionCounts(checkpoint.Files, PhaseNodes)
	edgeActionCounts := checkpointActionCounts(checkpoint.Files, PhaseEdges)

	nodeKinds := map[string]struct{}{}
	edgeKinds := map[string]struct{}{}
	metricsBuilder := newMetricsBuilder(target.Name, entitySnapshot.NodeCount)
	if err := restoreDumpGraphFragments(options.OutputDir, options.Compression, checkpoint, nodeKinds, edgeKinds, metricsBuilder); err != nil {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
	}

	nodeStartedAt := time.Now()
	logInfoWithRuntimeTelemetry("retriever dump node phase started",
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

	nodeFiles := filterPhaseFiles(checkpoint.Files, PhaseNodes)
	if checkpoint.Phase == PhaseNodes {
		nodeFiles, err = dumpNodePhase(ctx, db, targetGraph, options, activeScrubber, nodeKinds, &nodeActionCounts, entitySnapshot, metricsBuilder, nodeFiles, graph.ID(checkpoint.LastCommittedID), checkpoint.HasLastCommittedID, func(fileEntry FileManifest, lastID graph.ID) error {
			previousLength := len(checkpoint.Files)
			previousID := checkpoint.LastCommittedID
			previousHasID := checkpoint.HasLastCommittedID
			checkpoint.Files = append(checkpoint.Files, fileEntry)
			checkpoint.LastCommittedID = lastID.Uint64()
			checkpoint.HasLastCommittedID = true
			if err := persistCheckpoint(); err != nil {
				checkpoint.Files = checkpoint.Files[:previousLength]
				checkpoint.LastCommittedID = previousID
				checkpoint.HasLastCommittedID = previousHasID
				return err
			}
			return nil
		})
		if err != nil {
			return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
		}

		checkpoint.Phase = PhaseEdges
		checkpoint.LastCommittedID = 0
		checkpoint.HasLastCommittedID = false
		if err := persistCheckpoint(); err != nil {
			return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
		}
	}

	graphEntry.Files = append([]FileManifest(nil), checkpoint.Files...)

	logInfoWithRuntimeTelemetry("retriever dump node phase completed",
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
	logInfoWithRuntimeTelemetry("retriever dump edge phase started",
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

	edgeFiles := filterPhaseFiles(checkpoint.Files, PhaseEdges)
	edgeFiles, err = dumpEdgePhase(ctx, db, targetGraph, options, activeScrubber, edgeKinds, &edgeActionCounts, entitySnapshot, metricsBuilder, edgeFiles, graph.ID(checkpoint.LastCommittedID), checkpoint.HasLastCommittedID, func(fileEntry FileManifest, lastID graph.ID) error {
		previousLength := len(checkpoint.Files)
		previousID := checkpoint.LastCommittedID
		previousHasID := checkpoint.HasLastCommittedID
		checkpoint.Files = append(checkpoint.Files, fileEntry)
		checkpoint.LastCommittedID = lastID.Uint64()
		checkpoint.HasLastCommittedID = true
		if err := persistCheckpoint(); err != nil {
			checkpoint.Files = checkpoint.Files[:previousLength]
			checkpoint.LastCommittedID = previousID
			checkpoint.HasLastCommittedID = previousHasID
			return err
		}
		return nil
	})
	if err != nil {
		return GraphManifest{}, GraphSchemaMetadata{}, GraphMetrics{}, err
	}

	graphEntry.Files = append([]FileManifest(nil), checkpoint.Files...)
	graphEntry.NodeActionCounts = nodeActionCounts.mapValue()
	graphEntry.EdgeActionCounts = edgeActionCounts.mapValue()

	logInfoWithRuntimeTelemetry("retriever dump edge phase completed",
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

func countGraphEntities(ctx context.Context, db graph.Database, targetGraph graph.Graph) (int64, int64, error) {
	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return 0, 0, err
	}

	return entitySnapshot.NodeCount, entitySnapshot.EdgeCount, nil
}

func validateCompletedDumpSources(ctx context.Context, db graph.Database, graphEntries []GraphManifest) error {
	for _, graphEntry := range graphEntries {
		snapshot, err := countGraphEntitySnapshot(ctx, db, graph.Graph{Name: graphEntry.Name})
		if err != nil {
			return fmt.Errorf("count completed checkpoint graph %q: %w", graphEntry.Name, err)
		}
		if snapshot.NodeCount != graphEntry.NodeCount || snapshot.EdgeCount != graphEntry.EdgeCount {
			return fmt.Errorf("dump checkpoint source counts for completed graph %q changed from nodes=%d edges=%d to nodes=%d edges=%d; refusing to resume", graphEntry.Name, graphEntry.NodeCount, graphEntry.EdgeCount, snapshot.NodeCount, snapshot.EdgeCount)
		}
	}
	return nil
}

func restoreDumpGraphFragments(inputDir string, codec CompressionCodec, checkpoint *dumpGraphCheckpoint, nodeKinds, edgeKinds map[string]struct{}, metricsBuilder *metricsBuilder) error {
	var lastNodeID graph.ID
	var hasLastNodeID bool
	for _, fileEntry := range checkpoint.Files {
		switch fileEntry.Phase {
		case PhaseNodes:
			count, err := readNodeFragmentFile(inputDir, codec, fileEntry, func(item FragmentNode) error {
				addKindsToSet(nodeKinds, item.Kinds)
				parsedID, err := strconv.ParseUint(item.ID, 10, 64)
				if err != nil {
					return fmt.Errorf("resume node fragment %s has non-numeric Retriever source ID %q", fileEntry.Path, item.ID)
				}
				lastNodeID = graph.ID(parsedID)
				hasLastNodeID = true
				return metricsBuilder.observeDatabaseNode(lastNodeID, item.Kinds)
			})
			if err != nil {
				return fmt.Errorf("restore node fragment %s: %w", fileEntry.Path, err)
			}
			if count != fileEntry.Count {
				return fmt.Errorf("restore node fragment %s: decoded %d records, expected %d", fileEntry.Path, count, fileEntry.Count)
			}

		case PhaseEdges:
			count, err := readEdgeFragmentFile(inputDir, codec, fileEntry, func(item FragmentEdge) error {
				if item.Kind != "" {
					edgeKinds[item.Kind] = struct{}{}
				}
				startID, startErr := strconv.ParseUint(item.StartID, 10, 64)
				endID, endErr := strconv.ParseUint(item.EndID, 10, 64)
				if startErr != nil || endErr != nil {
					return fmt.Errorf("resume edge fragment %s has non-numeric Retriever endpoint IDs %q and %q", fileEntry.Path, item.StartID, item.EndID)
				}
				return metricsBuilder.observeDatabaseRelationship(graph.ID(startID), graph.ID(endID), item.Kind)
			})
			if err != nil {
				return fmt.Errorf("restore edge fragment %s: %w", fileEntry.Path, err)
			}
			if count != fileEntry.Count {
				return fmt.Errorf("restore edge fragment %s: decoded %d records, expected %d", fileEntry.Path, count, fileEntry.Count)
			}
		}
	}

	if checkpoint.Phase == PhaseNodes && checkpoint.HasLastCommittedID {
		if !hasLastNodeID || lastNodeID.Uint64() != checkpoint.LastCommittedID {
			return fmt.Errorf("dump checkpoint node cursor %d does not match the last committed fragment ID", checkpoint.LastCommittedID)
		}
	}

	return nil
}

func countGraphEntitySnapshot(ctx context.Context, db graph.Database, targetGraph graph.Graph) (graphEntitySnapshot, error) {
	var entitySnapshot graphEntitySnapshot
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)

		var err error
		if entitySnapshot.NodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}

		if entitySnapshot.EdgeCount, err = tx.Relationships().Count(); err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}

		return nil
	}); err != nil {
		return graphEntitySnapshot{}, err
	}

	return entitySnapshot, nil
}

type fragmentCommitFunc func(FileManifest, graph.ID) error

func dumpNodePhase(ctx context.Context, db graph.Database, targetGraph graph.Graph, options DumpOptions, activeScrubber *scrubber, nodeKinds map[string]struct{}, graphActionCounts *scrubActionCounts, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder, committedFiles []FileManifest, afterID graph.ID, hasAfterID bool, onCommit fragmentCommitFunc) ([]FileManifest, error) {
	if entitySnapshot.NodeCount == 0 {
		return nil, nil
	}

	var (
		files                = append([]FileManifest(nil), committedFiles...)
		fragmentWriter       *compressedJSONLinesWriter
		fragmentRelativePath string
		shardActionCounts    scrubActionCounts
		shardNumber          = len(committedFiles) + 1
		lastWrittenID        graph.ID
		hasLastWrittenID     bool
	)

	flush := func() error {
		if fragmentWriter == nil {
			return nil
		}

		fileEntry, err := closeFragmentWriter(fragmentWriter, fragmentRelativePath, PhaseNodes, shardActionCounts.mapValue())
		fragmentWriter = nil
		if err != nil {
			return err
		}

		files = append(files, fileEntry)
		if !hasLastWrittenID {
			_ = os.Remove(filepath.Join(options.OutputDir, filepath.FromSlash(fileEntry.Path)))
			files = files[:len(files)-1]
			return fmt.Errorf("node fragment %q closed without a committed source cursor", fileEntry.Path)
		}
		if onCommit != nil {
			if err := onCommit(fileEntry, lastWrittenID); err != nil {
				_ = os.Remove(filepath.Join(options.OutputDir, filepath.FromSlash(fileEntry.Path)))
				files = files[:len(files)-1]
				return err
			}
		}
		shardActionCounts = scrubActionCounts{}
		shardNumber++

		return nil
	}

	if _, err := scanDatabaseNodesFrom(ctx, db, targetGraph, entitySnapshot.NodeCount, options.BatchSize, options.ProgressInterval, afterID, hasAfterID, fileTotal(committedFiles), func(node *graph.Node) error {
		if fragmentWriter == nil {
			nextWriter, nextRelativePath, err := openFragmentWriter(options.OutputDir, targetGraph.Name, PhaseNodes, shardNumber, options)
			if err != nil {
				return err
			}

			fragmentWriter = nextWriter
			fragmentRelativePath = nextRelativePath
		}

		kinds := node.Kinds.Strings()
		sort.Strings(kinds)
		addKindsToSet(nodeKinds, kinds)

		properties := node.Properties.MapOrEmpty()
		if activeScrubber != nil {
			var actionCounts scrubActionCounts
			properties, actionCounts = activeScrubber.scrubPropertiesWithCounts(properties)
			shardActionCounts.addCounts(actionCounts)
			graphActionCounts.addCounts(actionCounts)
		}

		item := FragmentNode{
			ID:         node.ID.String(),
			Kinds:      kinds,
			Properties: properties,
		}

		if err := metricsBuilder.observeDatabaseNode(node.ID, kinds); err != nil {
			return err
		}

		if err := fragmentWriter.Write(item); err != nil {
			return err
		}
		lastWrittenID = node.ID
		hasLastWrittenID = true

		if fragmentWriter.Count() >= options.ShardSize {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	}, nil, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump node phase progress", targetGraph.Name, PhaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt, options.Progress, options.ProgressInterval)
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

func dumpEdgePhase(ctx context.Context, db graph.Database, targetGraph graph.Graph, options DumpOptions, activeScrubber *scrubber, edgeKinds map[string]struct{}, graphActionCounts *scrubActionCounts, entitySnapshot graphEntitySnapshot, metricsBuilder *metricsBuilder, committedFiles []FileManifest, afterID graph.ID, hasAfterID bool, onCommit fragmentCommitFunc) ([]FileManifest, error) {
	if entitySnapshot.EdgeCount == 0 {
		return nil, nil
	}

	var (
		files                = append([]FileManifest(nil), committedFiles...)
		fragmentWriter       *compressedJSONLinesWriter
		fragmentRelativePath string
		shardActionCounts    scrubActionCounts
		shardNumber          = len(committedFiles) + 1
		lastWrittenID        graph.ID
		hasLastWrittenID     bool
	)

	flush := func() error {
		if fragmentWriter == nil {
			return nil
		}

		fileEntry, err := closeFragmentWriter(fragmentWriter, fragmentRelativePath, PhaseEdges, shardActionCounts.mapValue())
		fragmentWriter = nil
		if err != nil {
			return err
		}

		files = append(files, fileEntry)
		if !hasLastWrittenID {
			_ = os.Remove(filepath.Join(options.OutputDir, filepath.FromSlash(fileEntry.Path)))
			files = files[:len(files)-1]
			return fmt.Errorf("edge fragment %q closed without a committed source cursor", fileEntry.Path)
		}
		if onCommit != nil {
			if err := onCommit(fileEntry, lastWrittenID); err != nil {
				_ = os.Remove(filepath.Join(options.OutputDir, filepath.FromSlash(fileEntry.Path)))
				files = files[:len(files)-1]
				return err
			}
		}
		shardActionCounts = scrubActionCounts{}
		shardNumber++

		return nil
	}

	if _, err := scanDatabaseRelationshipsFrom(ctx, db, targetGraph, entitySnapshot.EdgeCount, options.BatchSize, options.ProgressInterval, afterID, hasAfterID, fileTotal(committedFiles), func(relationship *graph.Relationship) error {
		if fragmentWriter == nil {
			nextWriter, nextRelativePath, err := openFragmentWriter(options.OutputDir, targetGraph.Name, PhaseEdges, shardNumber, options)
			if err != nil {
				return err
			}

			fragmentWriter = nextWriter
			fragmentRelativePath = nextRelativePath
		}

		kind := ""
		if relationship.Kind != nil {
			kind = relationship.Kind.String()
			edgeKinds[kind] = struct{}{}
		}

		properties := relationship.Properties.MapOrEmpty()
		if activeScrubber != nil {
			var actionCounts scrubActionCounts
			properties, actionCounts = activeScrubber.scrubPropertiesWithCounts(properties)
			shardActionCounts.addCounts(actionCounts)
			graphActionCounts.addCounts(actionCounts)
		}

		item := FragmentEdge{
			StartID:    relationship.StartID.String(),
			EndID:      relationship.EndID.String(),
			Kind:       kind,
			Properties: properties,
		}

		if err := metricsBuilder.observeDatabaseRelationship(relationship.StartID, relationship.EndID, kind); err != nil {
			return err
		}

		if err := fragmentWriter.Write(item); err != nil {
			return err
		}
		lastWrittenID = relationship.ID
		hasLastWrittenID = true

		if fragmentWriter.Count() >= options.ShardSize {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	}, nil, func(processed int64, startedAt time.Time, nextProgressAt int64) int64 {
		return logRetrieverEntityProgressInterval("retriever dump edge phase progress", targetGraph.Name, PhaseEdges, processed, entitySnapshot.EdgeCount, startedAt, nextProgressAt, options.Progress, options.ProgressInterval)
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

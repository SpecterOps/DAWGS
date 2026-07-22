package retriever

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
)

type LoadResult struct {
	GraphCount int
	NodeCount  int64
	EdgeCount  int64
}

type schemaAssertion struct {
	GraphName string
	Schema    graph.Schema
}

type resolvedFragmentEdge struct {
	StartID    graph.ID
	EndID      graph.ID
	Kind       graph.Kind
	Properties *graph.Properties
}

type nodeIDResolver struct {
	numeric  map[uint64]graph.ID
	fallback map[string]graph.ID
}

func newNodeIDResolver(expected int64) *nodeIDResolver {
	capacity := int(expected)
	if int64(capacity) != expected || capacity < 0 {
		capacity = 0
	}

	return &nodeIDResolver{numeric: make(map[uint64]graph.ID, capacity)}
}

func (s *nodeIDResolver) put(sourceID string, destinationID graph.ID) bool {
	if numericID, ok := canonicalNumericSourceID(sourceID); ok {
		if _, exists := s.numeric[numericID]; exists {
			return false
		}
		s.numeric[numericID] = destinationID
		return true
	}

	if s.fallback == nil {
		s.fallback = map[string]graph.ID{}
	}
	if _, exists := s.fallback[sourceID]; exists {
		return false
	}
	s.fallback[sourceID] = destinationID
	return true
}

func (s *nodeIDResolver) resolve(sourceID string) (graph.ID, bool) {
	if numericID, ok := canonicalNumericSourceID(sourceID); ok {
		resolved, found := s.numeric[numericID]
		return resolved, found
	}

	resolved, found := s.fallback[sourceID]
	return resolved, found
}

func canonicalNumericSourceID(sourceID string) (uint64, bool) {
	if sourceID == "" {
		return 0, false
	}

	value, err := strconv.ParseUint(sourceID, 10, 64)
	if err != nil || strconv.FormatUint(value, 10) != sourceID {
		return 0, false
	}

	return value, true
}

func Load(ctx context.Context, db graph.Database, driverName string, options LoadOptions) (LoadResult, error) {
	preparedOptions, cleanupInput, err := prepareLoadInput(options)
	if err != nil {
		return LoadResult{}, err
	}
	defer cleanupInput()

	options = preparedOptions

	startedAt := time.Now()
	slog.Info("retriever load started",
		slog.String("driver", driverName),
		slog.String("input_dir", options.InputDir),
		slog.Int("batch_size", options.BatchSize),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationLoad,
		Message:   "retriever load started",
		Driver:    driverName,
		InputDir:  options.InputDir,
		BatchSize: options.BatchSize,
	})

	readManifestStartedAt := time.Now()
	slog.Info("retriever load reading manifest",
		slog.String("input_dir", options.InputDir),
	)
	nextManifest, err := readLoadManifest(options.InputDir, driverName)
	if err != nil {
		return LoadResult{}, err
	}

	slog.Info("retriever load manifest ready",
		slog.String("input_dir", options.InputDir),
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.String("source_driver", nextManifest.Driver),
		slog.String("compression", string(nextManifest.Compression)),
		slog.Duration("wall_elapsed", time.Since(readManifestStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:   OperationLoad,
		Message:     "retriever load manifest ready",
		InputDir:    options.InputDir,
		GraphCount:  len(nextManifest.Graphs),
		FileCount:   manifestFileCount(nextManifest),
		Compression: nextManifest.Compression,
		Elapsed:     time.Since(readManifestStartedAt),
	})

	verifyStartedAt := time.Now()
	slog.Info("retriever load verifying fragments",
		slog.String("input_dir", options.InputDir),
		slog.Int("file_count", manifestFileCount(nextManifest)),
	)
	if err := verifyLoadFragments(options.InputDir, nextManifest); err != nil {
		return LoadResult{}, err
	}
	compressedBytes, decompressedBytes := manifestFragmentBytes(nextManifest)

	slog.Info("retriever load fragments verified",
		slog.String("input_dir", options.InputDir),
		slog.Int("file_count", manifestFileCount(nextManifest)),
		slog.Duration("wall_elapsed", time.Since(verifyStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:             OperationLoad,
		Message:               "retriever load fragments verified",
		InputDir:              options.InputDir,
		FileCount:             manifestFileCount(nextManifest),
		Elapsed:               time.Since(verifyStartedAt),
		CompressedBytesRead:   compressedBytes,
		DecompressedBytesRead: decompressedBytes,
		FragmentPasses:        1,
	})

	schemaStartedAt := time.Now()
	slog.Info("retriever load asserting schemas",
		slog.Int("graph_count", len(nextManifest.Graphs)),
	)
	if err := assertManifestSchemas(ctx, db, nextManifest); err != nil {
		return LoadResult{}, err
	}

	slog.Info("retriever load schemas ready",
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Duration("wall_elapsed", time.Since(schemaStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationLoad,
		Message:    "retriever load schemas ready",
		GraphCount: len(nextManifest.Graphs),
		Elapsed:    time.Since(schemaStartedAt),
	})

	emptyStartedAt := time.Now()
	slog.Info("retriever load checking target graphs",
		slog.Int("graph_count", len(nextManifest.Graphs)),
	)
	if err := requireEmptyLoadTargets(ctx, db, nextManifest.Graphs); err != nil {
		return LoadResult{}, err
	}

	slog.Info("retriever load target graphs ready",
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Duration("wall_elapsed", time.Since(emptyStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationLoad,
		Message:    "retriever load target graphs ready",
		GraphCount: len(nextManifest.Graphs),
		Elapsed:    time.Since(emptyStartedAt),
	})

	var result LoadResult
	result.GraphCount = len(nextManifest.Graphs)

	for graphIndex, graphEntry := range nextManifest.Graphs {
		nodeCount, edgeCount, err := loadManifestGraph(ctx, db, options, nextManifest.Compression, graphIndex, len(nextManifest.Graphs), graphEntry)
		if err != nil {
			return LoadResult{}, err
		}

		result.NodeCount += nodeCount
		result.EdgeCount += edgeCount
	}

	if options.VerifyMetrics {
		if err := verifyLoadedMetrics(ctx, db, nextManifest, options.BatchSize, options.Progress, options.ProgressInterval); err != nil {
			return LoadResult{}, err
		}
	}

	slog.Info("retriever load completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", result.GraphCount),
		slog.Int64("node_count", result.NodeCount),
		slog.Int64("edge_count", result.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:             OperationLoad,
		Message:               "retriever load completed",
		Driver:                driverName,
		InputDir:              options.InputDir,
		GraphCount:            result.GraphCount,
		NodeCount:             result.NodeCount,
		EdgeCount:             result.EdgeCount,
		Elapsed:               time.Since(startedAt),
		CompressedBytesRead:   2 * compressedBytes,
		DecompressedBytesRead: 2 * decompressedBytes,
		FragmentPasses:        2,
	})

	return result, nil
}

func readLoadManifest(inputDir string, driverName string) (Manifest, error) {
	nextManifest, err := readManifest(inputDir)
	if err != nil {
		return Manifest{}, err
	}
	if driverName == neo4j.DriverName && len(nextManifest.Graphs) > 1 {
		return Manifest{}, IncompatibleDriverError{
			Operation: OperationLoad,
			Driver:    driverName,
			Reason:    "cannot load a multi-graph collection into neo4j because Dawgs graph names are no-ops for that driver",
		}
	}

	return nextManifest, nil
}

func verifyLoadFragments(inputDir string, nextManifest Manifest) error {
	return verifyCollectionFragments(inputDir, nextManifest)
}

func verifyCollectionFragments(inputDir string, nextManifest Manifest) error {
	for _, graphEntry := range nextManifest.Graphs {
		nodeIDs := newNodeIDResolver(graphEntry.NodeCount)
		for _, fileEntry := range graphEntry.Files {
			switch fileEntry.Phase {
			case PhaseNodes:
				if _, err := decodeNodeFragmentFile(inputDir, nextManifest.Compression, fileEntry, true, func(item FragmentNode) error {
					if !nodeIDs.put(item.ID, 0) {
						return fmt.Errorf("duplicate source node ID %q in graph %q", item.ID, graphEntry.Name)
					}
					return nil
				}); err != nil {
					return err
				}

			case PhaseEdges:
				if _, err := decodeEdgeFragmentFile(inputDir, nextManifest.Compression, fileEntry, true, func(item FragmentEdge) error {
					_, err := resolveFragmentEdgeWithResolver(item, nodeIDs.resolve)
					return err
				}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

type graphEntitySnapshotCounter func(context.Context, graph.Database, graph.Graph) (graphEntitySnapshot, error)

func requireEmptyLoadTargets(ctx context.Context, db graph.Database, graphEntries []GraphManifest) error {
	return requireEmptyLoadTargetsWithCounter(ctx, db, graphEntries, countGraphEntitySnapshot)
}

func requireEmptyLoadTargetsWithCounter(ctx context.Context, db graph.Database, graphEntries []GraphManifest, countSnapshot graphEntitySnapshotCounter) error {
	for _, graphEntry := range graphEntries {
		entitySnapshot, err := countSnapshot(ctx, db, graph.Graph{
			Name: graphEntry.Name,
		})
		if err != nil {
			return fmt.Errorf("count existing entities for graph %q: %w", graphEntry.Name, err)
		}

		if entitySnapshot.NodeCount > 0 || entitySnapshot.EdgeCount > 0 {
			return NonEmptyTargetGraphError{
				GraphName: graphEntry.Name,
				NodeCount: entitySnapshot.NodeCount,
				EdgeCount: entitySnapshot.EdgeCount,
			}
		}
	}

	return nil
}

func loadManifestGraph(ctx context.Context, db graph.Database, options LoadOptions, codec CompressionCodec, graphIndex int, graphCount int, graphEntry GraphManifest) (int64, int64, error) {
	graphStartedAt := time.Now()
	slog.Info("retriever load graph started",
		slog.String("graph", graphEntry.Name),
		slog.Int("graph_index", graphIndex+1),
		slog.Int("graph_count", graphCount),
		slog.Int64("node_count", graphEntry.NodeCount),
		slog.Int64("edge_count", graphEntry.EdgeCount),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationLoad,
		Message:    "retriever load graph started",
		Graph:      graphEntry.Name,
		GraphIndex: graphIndex + 1,
		GraphCount: graphCount,
		InputDir:   options.InputDir,
		NodeCount:  graphEntry.NodeCount,
		EdgeCount:  graphEntry.EdgeCount,
	})

	nodeStartedAt := time.Now()
	logInfoWithRuntimeTelemetry("retriever load node phase started",
		slog.String("graph", graphEntry.Name),
		slog.Int64("node_count", graphEntry.NodeCount),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationLoad,
		Message:   "retriever load node phase started",
		Graph:     graphEntry.Name,
		Phase:     PhaseNodes,
		InputDir:  options.InputDir,
		Planned:   graphEntry.NodeCount,
	})

	nodeMap, nodeCount, err := loadGraphNodes(ctx, db, options.InputDir, codec, graphEntry, options.BatchSize, options.Progress, options.ProgressInterval)
	if err != nil {
		return 0, 0, err
	}
	nodeCompressedBytes, nodeDecompressedBytes := graphPhaseFragmentBytes(graphEntry, PhaseNodes)

	logInfoWithRuntimeTelemetry("retriever load node phase completed",
		slog.String("graph", graphEntry.Name),
		slog.Int64("processed", nodeCount),
		slog.Duration("wall_elapsed", time.Since(nodeStartedAt)),
		slog.Float64("entities_per_second", perSecond(nodeCount, time.Since(nodeStartedAt))),
	)
	options.Progress.emit(ProgressEvent{
		Operation:             OperationLoad,
		Message:               "retriever load node phase completed",
		Graph:                 graphEntry.Name,
		Phase:                 PhaseNodes,
		InputDir:              options.InputDir,
		Processed:             nodeCount,
		Planned:               graphEntry.NodeCount,
		Elapsed:               time.Since(nodeStartedAt),
		EntitiesPerSecond:     perSecond(nodeCount, time.Since(nodeStartedAt)),
		CompressedBytesRead:   nodeCompressedBytes,
		DecompressedBytesRead: nodeDecompressedBytes,
		FragmentPasses:        2,
	})

	edgeStartedAt := time.Now()
	logInfoWithRuntimeTelemetry("retriever load edge phase started",
		slog.String("graph", graphEntry.Name),
		slog.Int64("edge_count", graphEntry.EdgeCount),
		slog.Int("batch_size", options.BatchSize),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationLoad,
		Message:   "retriever load edge phase started",
		Graph:     graphEntry.Name,
		Phase:     PhaseEdges,
		InputDir:  options.InputDir,
		Planned:   graphEntry.EdgeCount,
		BatchSize: options.BatchSize,
	})

	edgeCount, err := loadGraphEdges(ctx, db, options.InputDir, codec, graphEntry, nodeMap, options.BatchSize, options.Progress, options.ProgressInterval)
	if err != nil {
		return 0, 0, err
	}
	edgeCompressedBytes, edgeDecompressedBytes := graphPhaseFragmentBytes(graphEntry, PhaseEdges)

	logInfoWithRuntimeTelemetry("retriever load edge phase completed",
		slog.String("graph", graphEntry.Name),
		slog.Int64("processed", edgeCount),
		slog.Duration("wall_elapsed", time.Since(edgeStartedAt)),
		slog.Float64("entities_per_second", perSecond(edgeCount, time.Since(edgeStartedAt))),
	)
	options.Progress.emit(ProgressEvent{
		Operation:             OperationLoad,
		Message:               "retriever load edge phase completed",
		Graph:                 graphEntry.Name,
		Phase:                 PhaseEdges,
		InputDir:              options.InputDir,
		Processed:             edgeCount,
		Planned:               graphEntry.EdgeCount,
		Elapsed:               time.Since(edgeStartedAt),
		EntitiesPerSecond:     perSecond(edgeCount, time.Since(edgeStartedAt)),
		CompressedBytesRead:   edgeCompressedBytes,
		DecompressedBytesRead: edgeDecompressedBytes,
		FragmentPasses:        2,
	})

	if nodeCount != graphEntry.NodeCount {
		return 0, 0, EntityCountMismatchError{
			Operation: OperationLoad,
			Graph:     graphEntry.Name,
			Phase:     PhaseNodes,
			Expected:  graphEntry.NodeCount,
			Actual:    nodeCount,
			Message:   fmt.Sprintf("loaded %d nodes for graph %q but manifest expected %d", nodeCount, graphEntry.Name, graphEntry.NodeCount),
		}
	}

	if edgeCount != graphEntry.EdgeCount {
		return 0, 0, EntityCountMismatchError{
			Operation: OperationLoad,
			Graph:     graphEntry.Name,
			Phase:     PhaseEdges,
			Expected:  graphEntry.EdgeCount,
			Actual:    edgeCount,
			Message:   fmt.Sprintf("loaded %d relationships for graph %q but manifest expected %d", edgeCount, graphEntry.Name, graphEntry.EdgeCount),
		}
	}

	slog.Info("retriever load graph completed",
		slog.String("graph", graphEntry.Name),
		slog.Int64("node_count", nodeCount),
		slog.Int64("edge_count", edgeCount),
		slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
	)
	options.Progress.emit(ProgressEvent{
		Operation:  OperationLoad,
		Message:    "retriever load graph completed",
		Graph:      graphEntry.Name,
		GraphIndex: graphIndex + 1,
		GraphCount: graphCount,
		InputDir:   options.InputDir,
		NodeCount:  nodeCount,
		EdgeCount:  edgeCount,
		Elapsed:    time.Since(graphStartedAt),
	})

	return nodeCount, edgeCount, nil
}

func verifyLoadedMetrics(ctx context.Context, db graph.Database, nextManifest Manifest, batchSize int, progress ProgressFunc, progressInterval int64) error {
	if nextManifest.Metrics == nil {
		return MissingMetricsError{Operation: OperationVerify}
	}

	verifyStartedAt := time.Now()
	slog.Info("retriever load metrics verification started",
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Int("batch_size", batchSize),
	)
	actualMetrics, _, err := collectDatabaseMetrics(ctx, db, nextManifest.Graphs, batchSize, progress, progressInterval)
	if err != nil {
		return err
	}

	differences := compareMetricsManifest(*nextManifest.Metrics, actualMetrics)
	if len(differences) > 0 {
		slog.Info("retriever load metrics verification failed",
			slog.Int("difference_count", len(differences)),
			slog.Duration("wall_elapsed", time.Since(verifyStartedAt)),
		)
		return MetricsMismatchError{
			Differences: differences,
		}
	}

	slog.Info("retriever load metrics verification passed",
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Duration("wall_elapsed", time.Since(verifyStartedAt)),
	)

	return nil
}

func prepareLoadInput(options LoadOptions) (LoadOptions, func(), error) {
	if err := options.validate(); err != nil {
		return LoadOptions{}, nil, err
	}

	options.InputDir = strings.TrimSpace(options.InputDir)

	if options.ArchiveReader == nil {
		return options, func() {}, nil
	}

	tempDir, err := os.MkdirTemp("", "retriever-load-archive-*")
	if err != nil {
		return LoadOptions{}, nil, fmt.Errorf("create load archive temp directory: %w", err)
	}

	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	slog.Info("retriever load archive unpacking started")
	options.Progress.emit(ProgressEvent{
		Operation: OperationLoad,
		Message:   "retriever load archive unpacking started",
		OutputDir: tempDir,
	})

	if err := UnpackEncryptedCollectionArchiveWithOptions(options.ArchiveReader, tempDir, options.ArchiveIdentity, ArchiveOptions{Progress: options.Progress}); err != nil {
		cleanup()
		return LoadOptions{}, nil, err
	}

	slog.Info("retriever load archive unpacking completed")
	options.Progress.emit(ProgressEvent{
		Operation: OperationLoad,
		Message:   "retriever load archive unpacking completed",
		OutputDir: tempDir,
	})

	options.InputDir = tempDir
	options.ArchiveReader = nil
	options.ArchiveIdentity = nil

	if err := options.validate(); err != nil {
		cleanup()
		return LoadOptions{}, nil, err
	}

	return options, cleanup, nil
}

func assertManifestSchemas(ctx context.Context, db graph.Database, value Manifest) error {
	assertions, err := schemaAssertionsFromManifest(value)
	if err != nil {
		return err
	}

	for _, assertion := range assertions {
		if err := db.AssertSchema(ctx, assertion.Schema); err != nil {
			return fmt.Errorf("assert schema for graph %q: %w", assertion.GraphName, err)
		}
	}

	return nil
}

func schemaAssertionsFromManifest(value Manifest) ([]schemaAssertion, error) {
	schemaByGraph := map[string]GraphSchemaMetadata{}
	for _, schemaEntry := range value.Schema.Graphs {
		schemaByGraph[schemaEntry.Name] = schemaEntry
	}

	assertions := make([]schemaAssertion, 0, len(value.Graphs))
	for _, graphEntry := range value.Graphs {
		schemaEntry, ok := schemaByGraph[graphEntry.Name]
		if !ok {
			return nil, fmt.Errorf("manifest missing schema metadata for graph %q", graphEntry.Name)
		}

		graphSchema := graphSchemaFromMetadata(schemaEntry)

		assertions = append(assertions, schemaAssertion{
			GraphName: graphEntry.Name,
			Schema: graph.Schema{
				Graphs:       []graph.Graph{graphSchema},
				DefaultGraph: graphSchema,
			},
		})
	}

	return assertions, nil
}

func manifestFileCount(value Manifest) int {
	var count int
	for _, graphEntry := range value.Graphs {
		count += len(graphEntry.Files)
	}

	return count
}

func loadGraphNodes(ctx context.Context, db graph.Database, inputDir string, codec CompressionCodec, graphEntry GraphManifest, batchSize int, progress ProgressFunc, progressInterval int64) (*nodeIDResolver, int64, error) {
	nodeMap := newNodeIDResolver(graphEntry.NodeCount)
	type pendingNode struct {
		sourceID string
		node     *graph.Node
	}
	var (
		loaded     int64
		processed  int64
		pending    = make([]pendingNode, 0, batchSize)
		pendingIDs = make(map[string]struct{}, batchSize)

		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAtInterval(graphEntry.NodeCount, progressInterval)
	)

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}

		nodes := make([]*graph.Node, len(pending))
		for index := range pending {
			nodes[index] = pending[index].node
		}

		var destinationIDs []graph.ID
		if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
			batch = batch.WithGraph(graph.Graph{Name: graphEntry.Name})
			creator, ok := batch.(graph.NodeBatchCreator)
			if !ok {
				return fmt.Errorf("database batch %T does not support correlated bulk node creation", batch)
			}

			var err error
			destinationIDs, err = creator.CreateNodes(nodes)
			return err
		}, graph.WithBatchSize(batchSize)); err != nil {
			return err
		}

		if len(destinationIDs) != len(pending) {
			return fmt.Errorf("bulk node create returned %d IDs for %d inputs", len(destinationIDs), len(pending))
		}
		for index, item := range pending {
			if !nodeMap.put(item.sourceID, destinationIDs[index]) {
				return fmt.Errorf("duplicate source node ID %q in graph %q", item.sourceID, graphEntry.Name)
			}
		}

		loaded += int64(len(pending))
		clear(pending)
		pending = pending[:0]
		clear(pendingIDs)
		return nil
	}

	for _, fileEntry := range graphEntry.Files {
		if fileEntry.Phase != PhaseNodes {
			continue
		}

		fragmentCount, err := readNodeFragmentFile(inputDir, codec, fileEntry, func(item FragmentNode) error {
			if _, exists := nodeMap.resolve(item.ID); exists {
				return fmt.Errorf("duplicate source node ID %q in graph %q", item.ID, graphEntry.Name)
			}
			if _, exists := pendingIDs[item.ID]; exists {
				return fmt.Errorf("duplicate source node ID %q in graph %q", item.ID, graphEntry.Name)
			}

			pending = append(pending, pendingNode{
				sourceID: item.ID,
				node:     graph.NewNode(0, graph.AsProperties(item.Properties), graph.StringsToKinds(item.Kinds)...),
			})
			pendingIDs[item.ID] = struct{}{}
			processed++
			if len(pending) >= batchSize {
				return flush()
			}

			return nil
		})
		if err != nil {
			return nil, loaded, fmt.Errorf("load node fragment %s: %w", fileEntry.Path, err)
		}

		if fragmentCount != fileEntry.Count {
			return nil, loaded, fmt.Errorf("load node fragment %s: decoded %d records", fileEntry.Path, fragmentCount)
		}
		nextProgressAt = logRetrieverEntityProgressInterval("retriever load node phase progress", graphEntry.Name, PhaseNodes, processed, graphEntry.NodeCount, startedAt, nextProgressAt, progress, progressInterval)
	}
	if err := flush(); err != nil {
		return nil, loaded, fmt.Errorf("load final node batch for graph %q: %w", graphEntry.Name, err)
	}

	return nodeMap, loaded, nil
}

func loadGraphEdges(ctx context.Context, db graph.Database, inputDir string, codec CompressionCodec, graphEntry GraphManifest, nodeMap *nodeIDResolver, batchSize int, progress ProgressFunc, progressInterval int64) (int64, error) {
	var (
		loaded int64

		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAtInterval(graphEntry.EdgeCount, progressInterval)
	)

	for _, fileEntry := range graphEntry.Files {
		if fileEntry.Phase != PhaseEdges {
			continue
		}

		var fragmentCount int
		if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
			batch = batch.WithGraph(graph.Graph{
				Name: graphEntry.Name,
			})

			count, err := readEdgeFragmentFile(inputDir, codec, fileEntry, func(item FragmentEdge) error {
				resolved, err := resolveFragmentEdgeWithResolver(item, nodeMap.resolve)
				if err != nil {
					return err
				}

				if err := batch.CreateRelationshipByIDs(resolved.StartID, resolved.EndID, resolved.Kind, resolved.Properties); err != nil {
					return fmt.Errorf("create edge (%s)-[%s]->(%s): %w", item.StartID, item.Kind, item.EndID, err)
				}

				return nil
			})
			fragmentCount = count

			return err
		}, graph.WithBatchSize(batchSize)); err != nil {
			return loaded, fmt.Errorf("load edge fragment %s: %w", fileEntry.Path, err)
		}

		loaded += int64(fragmentCount)
		nextProgressAt = logRetrieverEntityProgressInterval("retriever load edge phase progress", graphEntry.Name, PhaseEdges, loaded, graphEntry.EdgeCount, startedAt, nextProgressAt, progress, progressInterval)
	}

	return loaded, nil
}

func readNodeFragmentFile(inputDir string, codec CompressionCodec, fileEntry FileManifest, handle func(FragmentNode) error) (int, error) {
	return decodeNodeFragmentFile(inputDir, codec, fileEntry, false, handle)
}

func verifyNodeFragmentFile(inputDir string, codec CompressionCodec, fileEntry FileManifest) (int, error) {
	return decodeNodeFragmentFile(inputDir, codec, fileEntry, true, nil)
}

func decodeNodeFragmentFile(inputDir string, codec CompressionCodec, fileEntry FileManifest, verifyIntegrity bool, handle func(FragmentNode) error) (int, error) {
	if fileEntry.Phase != PhaseNodes {
		return 0, fmt.Errorf("fragment %s has phase %q, expected nodes", fileEntry.Path, fileEntry.Phase)
	}

	fragmentPath := filepath.Join(inputDir, filepath.FromSlash(fileEntry.Path))
	validate := func(item FragmentNode) error {
		if item.ID == "" {
			return fmt.Errorf("node fragment %s contains empty source ID", fileEntry.Path)
		}

		if handle != nil {
			return handle(item)
		}

		return nil
	}

	var count int
	var err error
	if verifyIntegrity {
		count, err = readVerifiedCompressedJSONLines(fragmentPath, codec, fileEntry.SHA256, fileEntry.CompressedBytes, validate)
	} else {
		count, err = readCompressedJSONLines(fragmentPath, codec, validate)
	}
	if err != nil {
		return count, fmt.Errorf("read node fragment %s: %w", fileEntry.Path, err)
	}

	if count != fileEntry.Count {
		return count, fmt.Errorf("fragment %s item count %d does not match manifest count %d", fileEntry.Path, count, fileEntry.Count)
	}

	return count, nil
}

func readEdgeFragmentFile(inputDir string, codec CompressionCodec, fileEntry FileManifest, handle func(FragmentEdge) error) (int, error) {
	return decodeEdgeFragmentFile(inputDir, codec, fileEntry, false, handle)
}

func verifyEdgeFragmentFile(inputDir string, codec CompressionCodec, fileEntry FileManifest) (int, error) {
	return decodeEdgeFragmentFile(inputDir, codec, fileEntry, true, nil)
}

func decodeEdgeFragmentFile(inputDir string, codec CompressionCodec, fileEntry FileManifest, verifyIntegrity bool, handle func(FragmentEdge) error) (int, error) {
	if fileEntry.Phase != PhaseEdges {
		return 0, fmt.Errorf("fragment %s has phase %q, expected edges", fileEntry.Path, fileEntry.Phase)
	}

	fragmentPath := filepath.Join(inputDir, filepath.FromSlash(fileEntry.Path))
	validate := func(item FragmentEdge) error {
		if item.StartID == "" {
			return fmt.Errorf("edge fragment %s contains empty start source ID", fileEntry.Path)
		}
		if item.EndID == "" {
			return fmt.Errorf("edge fragment %s contains empty end source ID", fileEntry.Path)
		}

		if handle != nil {
			return handle(item)
		}

		return nil
	}

	var count int
	var err error
	if verifyIntegrity {
		count, err = readVerifiedCompressedJSONLines(fragmentPath, codec, fileEntry.SHA256, fileEntry.CompressedBytes, validate)
	} else {
		count, err = readCompressedJSONLines(fragmentPath, codec, validate)
	}
	if err != nil {
		return count, fmt.Errorf("read edge fragment %s: %w", fileEntry.Path, err)
	}

	if count != fileEntry.Count {
		return count, fmt.Errorf("fragment %s item count %d does not match manifest count %d", fileEntry.Path, count, fileEntry.Count)
	}

	return count, nil
}

func resolveFragmentEdge(item FragmentEdge, nodeMap map[string]graph.ID) (resolvedFragmentEdge, error) {
	return resolveFragmentEdgeWithResolver(item, func(sourceID string) (graph.ID, bool) {
		id, found := nodeMap[sourceID]
		return id, found
	})
}

func resolveFragmentEdgeWithResolver(item FragmentEdge, resolve func(string) (graph.ID, bool)) (resolvedFragmentEdge, error) {
	startID, ok := resolve(item.StartID)
	if !ok {
		return resolvedFragmentEdge{}, fmt.Errorf("edge references missing start node %q", item.StartID)
	}

	endID, ok := resolve(item.EndID)
	if !ok {
		return resolvedFragmentEdge{}, fmt.Errorf("edge references missing end node %q", item.EndID)
	}

	return resolvedFragmentEdge{
		StartID:    startID,
		EndID:      endID,
		Kind:       graph.StringKind(item.Kind),
		Properties: graph.AsProperties(item.Properties),
	}, nil
}

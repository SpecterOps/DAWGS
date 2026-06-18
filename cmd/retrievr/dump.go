package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type dumpResult struct {
	Manifest     manifest
	ManifestPath string
	NodeCount    int64
	EdgeCount    int64
}

type graphEntitySnapshot struct {
	NodeCount int64
	EdgeCount int64
	MaxNodeID graph.ID
	MaxEdgeID graph.ID
	HasNodes  bool
	HasEdges  bool
}

func Dump(ctx context.Context, db graph.Database, driverName string, targets []graphTarget, options dumpOptions) (dumpResult, error) {
	if err := options.validate(); err != nil {
		return dumpResult{}, err
	}
	if len(targets) == 0 {
		return dumpResult{}, fmt.Errorf("at least one graph target is required")
	}

	startedAt := time.Now()
	slog.Info("retrievr dump started",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.String("output_dir", options.OutputDir),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("shard_size", options.ShardSize),
		slog.String("compression", string(options.Compression)),
		slog.String("scrub", string(options.Scrub)),
	)

	var (
		activeScrubber *scrubber
		scrubInfo      = scrubMetadata{
			Mode:             scrubNone,
			NodeActionCounts: map[string]int{},
			EdgeActionCounts: map[string]int{},
		}
	)
	if options.Scrub == scrubFull {
		nextScrubber, err := newScrubber(options.ScrubConfigPath, options.Salt)
		if err != nil {
			return dumpResult{}, err
		}
		activeScrubber = nextScrubber
		scrubInfo = activeScrubber.metadata()
	}

	slog.Info("retrievr dump preparing output directory",
		slog.String("output_dir", options.OutputDir),
		slog.Bool("force", options.Force),
	)
	if err := prepareOutputDirectory(options.OutputDir, options.Force); err != nil {
		return dumpResult{}, err
	}
	slog.Info("retrievr dump output directory ready",
		slog.String("output_dir", options.OutputDir),
	)

	nextManifest := newManifest(driverName, options.Compression, options.ZstdLevel, scrubInfo, len(targets))
	var totalNodes, totalEdges int64

	for targetIndex, target := range targets {
		graphStartedAt := time.Now()
		slog.Info("retrievr dump graph started",
			slog.String("graph", target.Name),
			slog.Int("graph_index", targetIndex+1),
			slog.Int("graph_count", len(targets)),
		)
		graphEntry, schemaEntry, err := dumpGraph(ctx, db, target, options, activeScrubber)
		if err != nil {
			return dumpResult{}, err
		}
		nextManifest.Graphs = append(nextManifest.Graphs, graphEntry)
		nextManifest.Schema.Graphs = append(nextManifest.Schema.Graphs, schemaEntry)
		addActionCounts(nextManifest.Scrub.NodeActionCounts, graphEntry.NodeActionCounts)
		addActionCounts(nextManifest.Scrub.EdgeActionCounts, graphEntry.EdgeActionCounts)
		totalNodes += graphEntry.NodeCount
		totalEdges += graphEntry.EdgeCount
		slog.Info("retrievr dump graph completed",
			slog.String("graph", target.Name),
			slog.Int64("node_count", graphEntry.NodeCount),
			slog.Int64("edge_count", graphEntry.EdgeCount),
			slog.Int("file_count", len(graphEntry.Files)),
			slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
		)
	}

	slog.Info("retrievr dump writing manifest",
		slog.String("output_dir", options.OutputDir),
		slog.Int64("node_count", totalNodes),
		slog.Int64("edge_count", totalEdges),
	)
	if err := writeManifest(options.OutputDir, nextManifest); err != nil {
		return dumpResult{}, err
	}
	manifestPath := filepath.Join(options.OutputDir, manifestFileName)
	slog.Info("retrievr dump completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", len(targets)),
		slog.String("manifest", manifestPath),
		slog.Int64("node_count", totalNodes),
		slog.Int64("edge_count", totalEdges),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)

	return dumpResult{
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

func dumpGraph(ctx context.Context, db graph.Database, target graphTarget, options dumpOptions, activeScrubber *scrubber) (graphManifest, graphSchemaMetadata, error) {
	targetGraph := graph.Graph{
		Name: target.Name,
	}

	countStartedAt := time.Now()
	slog.Info("retrievr dump counting graph entities",
		slog.String("graph", target.Name),
	)
	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return graphManifest{}, graphSchemaMetadata{}, err
	}
	slog.Info("retrievr dump graph counts ready",
		slog.String("graph", target.Name),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Uint64("max_node_id", entitySnapshot.MaxNodeID.Uint64()),
		slog.Uint64("max_edge_id", entitySnapshot.MaxEdgeID.Uint64()),
		slog.Duration("wall_elapsed", time.Since(countStartedAt)),
	)

	if activeScrubber != nil {
		scrubStartedAt := time.Now()
		slog.Info("retrievr dump scrub pre-pass started",
			slog.String("graph", target.Name),
			slog.Int64("node_count", entitySnapshot.NodeCount),
			slog.Int("batch_size", options.BatchSize),
		)
		observedNodes, err := collectScrubRegistry(ctx, db, targetGraph, options.BatchSize, activeScrubber, entitySnapshot)
		if err != nil {
			return graphManifest{}, graphSchemaMetadata{}, err
		}
		slog.Info("retrievr dump scrub pre-pass completed",
			slog.String("graph", target.Name),
			slog.Int64("processed", observedNodes),
			slog.Duration("wall_elapsed", time.Since(scrubStartedAt)),
		)
	}

	graphEntry := graphManifest{
		Name:             target.Name,
		NodeCount:        entitySnapshot.NodeCount,
		EdgeCount:        entitySnapshot.EdgeCount,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}
	nodeKinds := map[string]struct{}{}
	edgeKinds := map[string]struct{}{}

	nodeStartedAt := time.Now()
	slog.Info("retrievr dump node phase started",
		slog.String("graph", target.Name),
		slog.Int64("node_count", entitySnapshot.NodeCount),
		slog.Uint64("max_node_id", entitySnapshot.MaxNodeID.Uint64()),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("shard_size", options.ShardSize),
	)
	nodeFiles, err := dumpNodePhase(ctx, db, targetGraph, options, activeScrubber, nodeKinds, graphEntry.NodeActionCounts, entitySnapshot)
	if err != nil {
		return graphManifest{}, graphSchemaMetadata{}, err
	}
	graphEntry.Files = append(graphEntry.Files, nodeFiles...)
	slog.Info("retrievr dump node phase completed",
		slog.String("graph", target.Name),
		slog.Int64("processed", fileTotal(nodeFiles)),
		slog.Int("file_count", len(nodeFiles)),
		slog.Duration("wall_elapsed", time.Since(nodeStartedAt)),
	)

	edgeStartedAt := time.Now()
	slog.Info("retrievr dump edge phase started",
		slog.String("graph", target.Name),
		slog.Int64("edge_count", entitySnapshot.EdgeCount),
		slog.Uint64("max_edge_id", entitySnapshot.MaxEdgeID.Uint64()),
		slog.Int("batch_size", options.BatchSize),
		slog.Int("shard_size", options.ShardSize),
	)
	edgeFiles, err := dumpEdgePhase(ctx, db, targetGraph, options, activeScrubber, edgeKinds, graphEntry.EdgeActionCounts, entitySnapshot)
	if err != nil {
		return graphManifest{}, graphSchemaMetadata{}, err
	}
	graphEntry.Files = append(graphEntry.Files, edgeFiles...)
	slog.Info("retrievr dump edge phase completed",
		slog.String("graph", target.Name),
		slog.Int64("processed", fileTotal(edgeFiles)),
		slog.Int("file_count", len(edgeFiles)),
		slog.Duration("wall_elapsed", time.Since(edgeStartedAt)),
	)

	if fileTotal(nodeFiles) != entitySnapshot.NodeCount {
		return graphManifest{}, graphSchemaMetadata{}, fmt.Errorf("dumped %d nodes for graph %q but counted %d at scan start; source graph changed during dump or the ID scan was inconsistent", fileTotal(nodeFiles), target.Name, entitySnapshot.NodeCount)
	}
	if fileTotal(edgeFiles) != entitySnapshot.EdgeCount {
		return graphManifest{}, graphSchemaMetadata{}, fmt.Errorf("dumped %d relationships for graph %q but counted %d at scan start; source graph changed during dump or the ID scan was inconsistent", fileTotal(edgeFiles), target.Name, entitySnapshot.EdgeCount)
	}

	schemaEntry := graphSchemaMetadata{
		Name:      target.Name,
		NodeKinds: stringsFromKindSet(nodeKinds),
		EdgeKinds: stringsFromKindSet(edgeKinds),
	}
	return graphEntry, schemaEntry, nil
}

func collectScrubRegistry(ctx context.Context, db graph.Database, targetGraph graph.Graph, batchSize int, activeScrubber *scrubber, entitySnapshot graphEntitySnapshot) (int64, error) {
	if entitySnapshot.NodeCount == 0 {
		return 0, nil
	}

	var (
		lastID         graph.ID
		hasLastID      bool
		processed      int64
		startedAt      = time.Now()
		nextProgressAt = retrievrInitialProgressAt(entitySnapshot.NodeCount)
	)
	for {
		nodes, err := readDatabaseNodesBounded(ctx, db, targetGraph, lastID, hasLastID, entitySnapshot.MaxNodeID, entitySnapshot.HasNodes, batchSize)
		if err != nil {
			return processed, fmt.Errorf("scrub pre-pass: %w", err)
		}
		if len(nodes) == 0 {
			return processed, nil
		}
		for _, node := range nodes {
			activeScrubber.observeNode(node.Properties.MapOrEmpty())
		}
		processed += int64(len(nodes))
		nextProgressAt = logRetrievrEntityProgress("retrievr dump scrub pre-pass progress", targetGraph.Name, phaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt)
		lastID = nodes[len(nodes)-1].ID
		hasLastID = true
	}
}

func countGraphEntities(ctx context.Context, db graph.Database, targetGraph graph.Graph) (int64, int64, error) {
	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return 0, 0, err
	}
	return entitySnapshot.NodeCount, entitySnapshot.EdgeCount, nil
}

func countGraphEntitySnapshot(ctx context.Context, db graph.Database, targetGraph graph.Graph) (graphEntitySnapshot, error) {
	var entitySnapshot graphEntitySnapshot
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		var err error
		if entitySnapshot.NodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}
		if entitySnapshot.NodeCount > 0 {
			if node, err := tx.Nodes().
				OrderBy(query.Order(query.NodeID(), query.Descending())).
				Limit(1).
				First(); err != nil {
				return fmt.Errorf("read max node ID: %w", err)
			} else {
				entitySnapshot.MaxNodeID = node.ID
				entitySnapshot.HasNodes = true
			}
		}

		if entitySnapshot.EdgeCount, err = tx.Relationships().Count(); err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}
		if entitySnapshot.EdgeCount > 0 {
			if relationship, err := tx.Relationships().
				OrderBy(query.Order(query.RelationshipID(), query.Descending())).
				Limit(1).
				First(); err != nil {
				return fmt.Errorf("read max relationship ID: %w", err)
			} else {
				entitySnapshot.MaxEdgeID = relationship.ID
				entitySnapshot.HasEdges = true
			}
		}

		return nil
	}); err != nil {
		return graphEntitySnapshot{}, err
	}
	return entitySnapshot, nil
}

func dumpNodePhase(ctx context.Context, db graph.Database, targetGraph graph.Graph, options dumpOptions, activeScrubber *scrubber, nodeKinds map[string]struct{}, graphActionCounts map[string]int, entitySnapshot graphEntitySnapshot) ([]fileManifest, error) {
	if entitySnapshot.NodeCount == 0 {
		return nil, nil
	}

	var (
		files             []fileManifest
		items             []fragmentNode
		shardActionCounts = map[string]int{}
		shardNumber       = 1
		lastID            graph.ID
		hasLastID         bool
		processed         int64
		startedAt         = time.Now()
		nextProgressAt    = retrievrInitialProgressAt(entitySnapshot.NodeCount)
	)

	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		fileEntry, err := writeNodeFragment(options.OutputDir, targetGraph.Name, shardNumber, options, items, shardActionCounts)
		if err != nil {
			return err
		}
		files = append(files, fileEntry)
		items = nil
		shardActionCounts = map[string]int{}
		shardNumber++
		return nil
	}

	for {
		nodes, err := readDatabaseNodesBounded(ctx, db, targetGraph, lastID, hasLastID, entitySnapshot.MaxNodeID, entitySnapshot.HasNodes, options.BatchSize)
		if err != nil {
			return nil, err
		}
		if len(nodes) == 0 {
			break
		}

		for _, node := range nodes {
			kinds := node.Kinds.Strings()
			sort.Strings(kinds)
			addKindsToSet(nodeKinds, kinds)

			properties := node.Properties.MapOrEmpty()
			if activeScrubber != nil {
				var actionCounts map[string]int
				properties, actionCounts = activeScrubber.scrubProperties(properties)
				addActionCounts(shardActionCounts, actionCounts)
				addActionCounts(graphActionCounts, actionCounts)
			}
			items = append(items, fragmentNode{
				ID:         node.ID.String(),
				Kinds:      kinds,
				Properties: properties,
			})
			if len(items) >= options.ShardSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
		processed += int64(len(nodes))
		nextProgressAt = logRetrievrEntityProgress("retrievr dump node phase progress", targetGraph.Name, phaseNodes, processed, entitySnapshot.NodeCount, startedAt, nextProgressAt)
		lastID = nodes[len(nodes)-1].ID
		hasLastID = true
	}

	if err := flush(); err != nil {
		return nil, err
	}
	return files, nil
}

func dumpEdgePhase(ctx context.Context, db graph.Database, targetGraph graph.Graph, options dumpOptions, activeScrubber *scrubber, edgeKinds map[string]struct{}, graphActionCounts map[string]int, entitySnapshot graphEntitySnapshot) ([]fileManifest, error) {
	if entitySnapshot.EdgeCount == 0 {
		return nil, nil
	}

	var (
		files             []fileManifest
		items             []fragmentEdge
		shardActionCounts = map[string]int{}
		shardNumber       = 1
		lastID            graph.ID
		hasLastID         bool
		processed         int64
		startedAt         = time.Now()
		nextProgressAt    = retrievrInitialProgressAt(entitySnapshot.EdgeCount)
	)

	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		fileEntry, err := writeEdgeFragment(options.OutputDir, targetGraph.Name, shardNumber, options, items, shardActionCounts)
		if err != nil {
			return err
		}
		files = append(files, fileEntry)
		items = nil
		shardActionCounts = map[string]int{}
		shardNumber++
		return nil
	}

	for {
		relationships, err := readDatabaseRelationshipsBounded(ctx, db, targetGraph, lastID, hasLastID, entitySnapshot.MaxEdgeID, entitySnapshot.HasEdges, options.BatchSize)
		if err != nil {
			return nil, err
		}
		if len(relationships) == 0 {
			break
		}

		for _, relationship := range relationships {
			kind := ""
			if relationship.Kind != nil {
				kind = relationship.Kind.String()
				edgeKinds[kind] = struct{}{}
			}

			properties := relationship.Properties.MapOrEmpty()
			if activeScrubber != nil {
				var actionCounts map[string]int
				properties, actionCounts = activeScrubber.scrubProperties(properties)
				addActionCounts(shardActionCounts, actionCounts)
				addActionCounts(graphActionCounts, actionCounts)
			}
			items = append(items, fragmentEdge{
				StartID:    relationship.StartID.String(),
				EndID:      relationship.EndID.String(),
				Kind:       kind,
				Properties: properties,
			})
			if len(items) >= options.ShardSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
		processed += int64(len(relationships))
		nextProgressAt = logRetrievrEntityProgress("retrievr dump edge phase progress", targetGraph.Name, phaseEdges, processed, entitySnapshot.EdgeCount, startedAt, nextProgressAt)
		lastID = relationships[len(relationships)-1].ID
		hasLastID = true
	}

	if err := flush(); err != nil {
		return nil, err
	}
	return files, nil
}

func writeNodeFragment(outputDir, graphName string, shardNumber int, options dumpOptions, items []fragmentNode, actionCounts map[string]int) (fileManifest, error) {
	relativePath, err := fragmentPath(graphName, phaseNodes, shardNumber, options.Compression)
	if err != nil {
		return fileManifest{}, err
	}
	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	fileEntry, err := writeCompressedJSON(absolutePath, options.Compression, options.ZstdLevel, nodeFragment{
		Phase: phaseNodes,
		Items: items,
	})
	if err != nil {
		return fileManifest{}, err
	}
	fileEntry.Phase = phaseNodes
	fileEntry.Path = relativePath
	fileEntry.Count = len(items)
	fileEntry.ActionCounts = cloneActionCounts(actionCounts)
	return fileEntry, nil
}

func writeEdgeFragment(outputDir, graphName string, shardNumber int, options dumpOptions, items []fragmentEdge, actionCounts map[string]int) (fileManifest, error) {
	relativePath, err := fragmentPath(graphName, phaseEdges, shardNumber, options.Compression)
	if err != nil {
		return fileManifest{}, err
	}
	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	fileEntry, err := writeCompressedJSON(absolutePath, options.Compression, options.ZstdLevel, edgeFragment{
		Phase: phaseEdges,
		Items: items,
	})
	if err != nil {
		return fileManifest{}, err
	}
	fileEntry.Phase = phaseEdges
	fileEntry.Path = relativePath
	fileEntry.Count = len(items)
	fileEntry.ActionCounts = cloneActionCounts(actionCounts)
	return fileEntry, nil
}

func fragmentPath(graphName string, fragmentPhase phase, shardNumber int, codec compressionCodec) (string, error) {
	if shardNumber <= 0 {
		return "", fmt.Errorf("shard number must be > 0")
	}
	extension, err := compressionExtension(codec)
	if err != nil {
		return "", err
	}

	var prefix string
	switch fragmentPhase {
	case phaseNodes:
		prefix = "nodes"
	case phaseEdges:
		prefix = "edges"
	default:
		return "", fmt.Errorf("unsupported fragment phase %q", fragmentPhase)
	}

	return path.Join("graphs", graphDirectoryName(graphName), fmt.Sprintf("%s-%06d.ogfrag%s", prefix, shardNumber, extension)), nil
}

func fileTotal(files []fileManifest) int64 {
	var total int64
	for _, fileEntry := range files {
		total += int64(fileEntry.Count)
	}
	return total
}

func readDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Node, error) {
	return readDatabaseNodesBounded(ctx, db, targetGraph, afterID, hasAfterID, 0, false, batchSize)
}

func readDatabaseNodesBounded(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, maxID graph.ID, hasMaxID bool, batchSize int) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		nodeQuery := tx.Nodes().
			OrderBy(query.NodeID()).
			Limit(batchSize)
		if criteria := entityIDScanCriteria(query.NodeID(), afterID, hasAfterID, maxID, hasMaxID); criteria != nil {
			nodeQuery = nodeQuery.Filter(criteria)
		}

		return nodeQuery.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				nodes = append(nodes, node)
			}
			return cursor.Error()
		})
	}); err != nil {
		if hasAfterID {
			return nil, fmt.Errorf("read node batch after ID %d: %w", afterID.Uint64(), err)
		}
		return nil, fmt.Errorf("read initial node batch: %w", err)
	}
	return nodes, nil
}

func readDatabaseRelationships(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Relationship, error) {
	return readDatabaseRelationshipsBounded(ctx, db, targetGraph, afterID, hasAfterID, 0, false, batchSize)
}

func readDatabaseRelationshipsBounded(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, maxID graph.ID, hasMaxID bool, batchSize int) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		relationshipQuery := tx.Relationships().
			OrderBy(query.RelationshipID()).
			Limit(batchSize)
		if criteria := entityIDScanCriteria(query.RelationshipID(), afterID, hasAfterID, maxID, hasMaxID); criteria != nil {
			relationshipQuery = relationshipQuery.Filter(criteria)
		}

		return relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for relationship := range cursor.Chan() {
				relationships = append(relationships, relationship)
			}
			return cursor.Error()
		})
	}); err != nil {
		if hasAfterID {
			return nil, fmt.Errorf("read relationship batch after ID %d: %w", afterID.Uint64(), err)
		}
		return nil, fmt.Errorf("read initial relationship batch: %w", err)
	}
	return relationships, nil
}

func entityIDScanCriteria(idCriteria graph.Criteria, afterID graph.ID, hasAfterID bool, maxID graph.ID, hasMaxID bool) graph.Criteria {
	var criteria []graph.Criteria
	if hasAfterID {
		criteria = append(criteria, query.GreaterThan(idCriteria, afterID))
	}
	if hasMaxID {
		criteria = append(criteria, query.LessThanOrEquals(idCriteria, maxID))
	}

	switch len(criteria) {
	case 0:
		return nil
	case 1:
		return criteria[0]
	default:
		return query.And(criteria...)
	}
}

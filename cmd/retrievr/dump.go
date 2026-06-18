package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type dumpResult struct {
	Manifest     manifest
	ManifestPath string
	NodeCount    int64
	EdgeCount    int64
}

func Dump(ctx context.Context, db graph.Database, driverName string, targets []graphTarget, options dumpOptions) (dumpResult, error) {
	if err := options.validate(); err != nil {
		return dumpResult{}, err
	}
	if len(targets) == 0 {
		return dumpResult{}, fmt.Errorf("at least one graph target is required")
	}

	var activeScrubber *scrubber
	scrubInfo := scrubMetadata{
		Mode:             scrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}
	if options.Scrub == scrubFull {
		nextScrubber, err := newScrubber(options.ScrubConfigPath, options.Salt)
		if err != nil {
			return dumpResult{}, err
		}
		activeScrubber = nextScrubber
		scrubInfo = activeScrubber.metadata()
	}

	if err := prepareOutputDirectory(options.OutputDir, options.Force); err != nil {
		return dumpResult{}, err
	}

	nextManifest := newManifest(driverName, options.Compression, options.ZstdLevel, scrubInfo, len(targets))
	var totalNodes, totalEdges int64

	for _, target := range targets {
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
	}

	if err := writeManifest(options.OutputDir, nextManifest); err != nil {
		return dumpResult{}, err
	}

	return dumpResult{
		Manifest:     nextManifest,
		ManifestPath: filepath.Join(options.OutputDir, manifestFileName),
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
	targetGraph := graph.Graph{Name: target.Name}
	if activeScrubber != nil {
		if err := collectScrubRegistry(ctx, db, targetGraph, options.BatchSize, activeScrubber); err != nil {
			return graphManifest{}, graphSchemaMetadata{}, err
		}
	}

	nodeCount, edgeCount, err := countGraphEntities(ctx, db, targetGraph)
	if err != nil {
		return graphManifest{}, graphSchemaMetadata{}, err
	}

	graphEntry := graphManifest{
		Name:             target.Name,
		NodeCount:        nodeCount,
		EdgeCount:        edgeCount,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}
	nodeKinds := map[string]struct{}{}
	edgeKinds := map[string]struct{}{}

	nodeFiles, err := dumpNodePhase(ctx, db, targetGraph, options, activeScrubber, nodeKinds, graphEntry.NodeActionCounts)
	if err != nil {
		return graphManifest{}, graphSchemaMetadata{}, err
	}
	graphEntry.Files = append(graphEntry.Files, nodeFiles...)

	edgeFiles, err := dumpEdgePhase(ctx, db, targetGraph, options, activeScrubber, edgeKinds, graphEntry.EdgeActionCounts)
	if err != nil {
		return graphManifest{}, graphSchemaMetadata{}, err
	}
	graphEntry.Files = append(graphEntry.Files, edgeFiles...)

	if fileTotal(nodeFiles) != nodeCount {
		return graphManifest{}, graphSchemaMetadata{}, fmt.Errorf("dumped %d nodes for graph %q but counted %d", fileTotal(nodeFiles), target.Name, nodeCount)
	}
	if fileTotal(edgeFiles) != edgeCount {
		return graphManifest{}, graphSchemaMetadata{}, fmt.Errorf("dumped %d relationships for graph %q but counted %d", fileTotal(edgeFiles), target.Name, edgeCount)
	}

	schemaEntry := graphSchemaMetadata{
		Name:      target.Name,
		NodeKinds: stringsFromKindSet(nodeKinds),
		EdgeKinds: stringsFromKindSet(edgeKinds),
	}
	return graphEntry, schemaEntry, nil
}

func collectScrubRegistry(ctx context.Context, db graph.Database, targetGraph graph.Graph, batchSize int, activeScrubber *scrubber) error {
	var (
		lastID    graph.ID
		hasLastID bool
	)
	for {
		nodes, err := readDatabaseNodes(ctx, db, targetGraph, lastID, hasLastID, batchSize)
		if err != nil {
			return fmt.Errorf("scrub pre-pass: %w", err)
		}
		if len(nodes) == 0 {
			return nil
		}
		for _, node := range nodes {
			activeScrubber.observeNode(node.Properties.MapOrEmpty())
		}
		lastID = nodes[len(nodes)-1].ID
		hasLastID = true
	}
}

func countGraphEntities(ctx context.Context, db graph.Database, targetGraph graph.Graph) (int64, int64, error) {
	var nodeCount, edgeCount int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		var err error
		if nodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}
		if edgeCount, err = tx.Relationships().Count(); err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}
	return nodeCount, edgeCount, nil
}

func dumpNodePhase(ctx context.Context, db graph.Database, targetGraph graph.Graph, options dumpOptions, activeScrubber *scrubber, nodeKinds map[string]struct{}, graphActionCounts map[string]int) ([]fileManifest, error) {
	var (
		files             []fileManifest
		items             []fragmentNode
		shardActionCounts = map[string]int{}
		shardNumber       = 1
		lastID            graph.ID
		hasLastID         bool
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
		nodes, err := readDatabaseNodes(ctx, db, targetGraph, lastID, hasLastID, options.BatchSize)
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
		lastID = nodes[len(nodes)-1].ID
		hasLastID = true
	}

	if err := flush(); err != nil {
		return nil, err
	}
	return files, nil
}

func dumpEdgePhase(ctx context.Context, db graph.Database, targetGraph graph.Graph, options dumpOptions, activeScrubber *scrubber, edgeKinds map[string]struct{}, graphActionCounts map[string]int) ([]fileManifest, error) {
	var (
		files             []fileManifest
		items             []fragmentEdge
		shardActionCounts = map[string]int{}
		shardNumber       = 1
		lastID            graph.ID
		hasLastID         bool
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
		relationships, err := readDatabaseRelationships(ctx, db, targetGraph, lastID, hasLastID, options.BatchSize)
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
		lastID = relationships[len(relationships)-1].ID
		hasLastID = true
	}

	if err := flush(); err != nil {
		return nil, err
	}
	return files, nil
}

func writeNodeFragment(outputDir, graphName string, shardNumber int, options dumpOptions, items []fragmentNode, actionCounts map[string]int) (fileManifest, error) {
	extension, err := compressionExtension(options.Compression)
	if err != nil {
		return fileManifest{}, err
	}
	relativePath := path.Join("graphs", graphDirectoryName(graphName), fmt.Sprintf("nodes-%06d.ogfrag%s", shardNumber, extension))
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
	extension, err := compressionExtension(options.Compression)
	if err != nil {
		return fileManifest{}, err
	}
	relativePath := path.Join("graphs", graphDirectoryName(graphName), fmt.Sprintf("edges-%06d.ogfrag%s", shardNumber, extension))
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

func fileTotal(files []fileManifest) int64 {
	var total int64
	for _, fileEntry := range files {
		total += int64(fileEntry.Count)
	}
	return total
}

func readDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		nodeQuery := tx.Nodes().
			OrderBy(query.NodeID()).
			Limit(batchSize)
		if hasAfterID {
			nodeQuery = nodeQuery.Filter(query.GreaterThan(query.NodeID(), afterID))
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
	var relationships []*graph.Relationship
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		relationshipQuery := tx.Relationships().
			OrderBy(query.RelationshipID()).
			Limit(batchSize)
		if hasAfterID {
			relationshipQuery = relationshipQuery.Filter(query.GreaterThan(query.RelationshipID(), afterID))
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

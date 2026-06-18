package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
)

type loadResult struct {
	GraphCount int
	NodeCount  int64
	EdgeCount  int64
}

func Load(ctx context.Context, db graph.Database, driverName string, options loadOptions) (loadResult, error) {
	if err := options.validate(); err != nil {
		return loadResult{}, err
	}

	nextManifest, err := readManifest(options.InputDir)
	if err != nil {
		return loadResult{}, err
	}
	if driverName == neo4j.DriverName && len(nextManifest.Graphs) > 1 {
		return loadResult{}, fmt.Errorf("cannot load a multi-graph collection into neo4j because Dawgs graph names are no-ops for that driver")
	}
	if err := verifyManifestFiles(options.InputDir, nextManifest); err != nil {
		return loadResult{}, err
	}
	if err := assertManifestSchemas(ctx, db, nextManifest); err != nil {
		return loadResult{}, err
	}

	var result loadResult
	result.GraphCount = len(nextManifest.Graphs)
	for _, graphEntry := range nextManifest.Graphs {
		nodeMap, nodeCount, err := loadGraphNodes(ctx, db, options.InputDir, nextManifest.Compression, graphEntry)
		if err != nil {
			return loadResult{}, err
		}
		edgeCount, err := loadGraphEdges(ctx, db, options.InputDir, nextManifest.Compression, graphEntry, nodeMap, options.BatchSize)
		if err != nil {
			return loadResult{}, err
		}
		if nodeCount != graphEntry.NodeCount {
			return loadResult{}, fmt.Errorf("loaded %d nodes for graph %q but manifest expected %d", nodeCount, graphEntry.Name, graphEntry.NodeCount)
		}
		if edgeCount != graphEntry.EdgeCount {
			return loadResult{}, fmt.Errorf("loaded %d relationships for graph %q but manifest expected %d", edgeCount, graphEntry.Name, graphEntry.EdgeCount)
		}
		result.NodeCount += nodeCount
		result.EdgeCount += edgeCount
	}
	return result, nil
}

func assertManifestSchemas(ctx context.Context, db graph.Database, value manifest) error {
	schemaByGraph := map[string]graphSchemaMetadata{}
	for _, schemaEntry := range value.Schema.Graphs {
		schemaByGraph[schemaEntry.Name] = schemaEntry
	}

	for _, graphEntry := range value.Graphs {
		schemaEntry, ok := schemaByGraph[graphEntry.Name]
		if !ok {
			return fmt.Errorf("manifest missing schema metadata for graph %q", graphEntry.Name)
		}
		graphSchema := graphSchemaFromMetadata(schemaEntry)
		if err := db.AssertSchema(ctx, graph.Schema{
			Graphs:       []graph.Graph{graphSchema},
			DefaultGraph: graphSchema,
		}); err != nil {
			return fmt.Errorf("assert schema for graph %q: %w", graphEntry.Name, err)
		}
	}
	return nil
}

func loadGraphNodes(ctx context.Context, db graph.Database, inputDir string, codec compressionCodec, graphEntry graphManifest) (map[string]graph.ID, int64, error) {
	nodeMap := make(map[string]graph.ID, int(graphEntry.NodeCount))
	var loaded int64

	for _, fileEntry := range graphEntry.Files {
		if fileEntry.Phase != phaseNodes {
			continue
		}

		var fragment nodeFragment
		if err := readCompressedJSON(filepath.Join(inputDir, filepath.FromSlash(fileEntry.Path)), codec, &fragment); err != nil {
			return nil, loaded, fmt.Errorf("read node fragment %s: %w", fileEntry.Path, err)
		}
		if fragment.Phase != phaseNodes {
			return nil, loaded, fmt.Errorf("fragment %s has phase %q, expected nodes", fileEntry.Path, fragment.Phase)
		}
		if len(fragment.Items) != fileEntry.Count {
			return nil, loaded, fmt.Errorf("fragment %s item count %d does not match manifest count %d", fileEntry.Path, len(fragment.Items), fileEntry.Count)
		}

		if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			tx = tx.WithGraph(graph.Graph{Name: graphEntry.Name})
			for _, item := range fragment.Items {
				if item.ID == "" {
					return fmt.Errorf("node fragment %s contains empty source ID", fileEntry.Path)
				}
				if _, exists := nodeMap[item.ID]; exists {
					return fmt.Errorf("duplicate source node ID %q in graph %q", item.ID, graphEntry.Name)
				}
				dbNode, err := tx.CreateNode(graph.AsProperties(item.Properties), graph.StringsToKinds(item.Kinds)...)
				if err != nil {
					return fmt.Errorf("create node %q: %w", item.ID, err)
				}
				nodeMap[item.ID] = dbNode.ID
			}
			return nil
		}); err != nil {
			return nil, loaded, fmt.Errorf("load node fragment %s: %w", fileEntry.Path, err)
		}
		loaded += int64(len(fragment.Items))
	}

	return nodeMap, loaded, nil
}

func loadGraphEdges(ctx context.Context, db graph.Database, inputDir string, codec compressionCodec, graphEntry graphManifest, nodeMap map[string]graph.ID, batchSize int) (int64, error) {
	var loaded int64

	for _, fileEntry := range graphEntry.Files {
		if fileEntry.Phase != phaseEdges {
			continue
		}

		var fragment edgeFragment
		if err := readCompressedJSON(filepath.Join(inputDir, filepath.FromSlash(fileEntry.Path)), codec, &fragment); err != nil {
			return loaded, fmt.Errorf("read edge fragment %s: %w", fileEntry.Path, err)
		}
		if fragment.Phase != phaseEdges {
			return loaded, fmt.Errorf("fragment %s has phase %q, expected edges", fileEntry.Path, fragment.Phase)
		}
		if len(fragment.Items) != fileEntry.Count {
			return loaded, fmt.Errorf("fragment %s item count %d does not match manifest count %d", fileEntry.Path, len(fragment.Items), fileEntry.Count)
		}

		if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
			batch = batch.WithGraph(graph.Graph{Name: graphEntry.Name})
			for _, item := range fragment.Items {
				startID, ok := nodeMap[item.StartID]
				if !ok {
					return fmt.Errorf("edge references missing start node %q", item.StartID)
				}
				endID, ok := nodeMap[item.EndID]
				if !ok {
					return fmt.Errorf("edge references missing end node %q", item.EndID)
				}
				if err := batch.CreateRelationshipByIDs(startID, endID, graph.StringKind(item.Kind), graph.AsProperties(item.Properties)); err != nil {
					return fmt.Errorf("create edge (%s)-[%s]->(%s): %w", item.StartID, item.Kind, item.EndID, err)
				}
			}
			return nil
		}, graph.WithBatchSize(batchSize)); err != nil {
			return loaded, fmt.Errorf("load edge fragment %s: %w", fileEntry.Path, err)
		}
		loaded += int64(len(fragment.Items))
	}

	return loaded, nil
}

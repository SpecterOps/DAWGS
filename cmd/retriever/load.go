package main

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
)

type loadResult struct {
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

func Load(ctx context.Context, db graph.Database, driverName string, options loadOptions) (loadResult, error) {
	if err := options.validate(); err != nil {
		return loadResult{}, err
	}

	startedAt := time.Now()
	slog.Info("retriever load started",
		slog.String("driver", driverName),
		slog.String("input_dir", options.InputDir),
		slog.Int("batch_size", options.BatchSize),
	)

	readManifestStartedAt := time.Now()
	slog.Info("retriever load reading manifest",
		slog.String("input_dir", options.InputDir),
	)
	nextManifest, err := readManifest(options.InputDir)
	if err != nil {
		return loadResult{}, err
	}
	slog.Info("retriever load manifest ready",
		slog.String("input_dir", options.InputDir),
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.String("source_driver", nextManifest.Driver),
		slog.String("compression", string(nextManifest.Compression)),
		slog.Duration("wall_elapsed", time.Since(readManifestStartedAt)),
	)
	if driverName == neo4j.DriverName && len(nextManifest.Graphs) > 1 {
		return loadResult{}, fmt.Errorf("cannot load a multi-graph collection into neo4j because Dawgs graph names are no-ops for that driver")
	}

	verifyStartedAt := time.Now()
	slog.Info("retriever load verifying fragments",
		slog.String("input_dir", options.InputDir),
		slog.Int("file_count", manifestFileCount(nextManifest)),
	)
	if err := verifyManifestFiles(options.InputDir, nextManifest); err != nil {
		return loadResult{}, err
	}
	slog.Info("retriever load fragments verified",
		slog.String("input_dir", options.InputDir),
		slog.Int("file_count", manifestFileCount(nextManifest)),
		slog.Duration("wall_elapsed", time.Since(verifyStartedAt)),
	)

	schemaStartedAt := time.Now()
	slog.Info("retriever load asserting schemas",
		slog.Int("graph_count", len(nextManifest.Graphs)),
	)
	if err := assertManifestSchemas(ctx, db, nextManifest); err != nil {
		return loadResult{}, err
	}
	slog.Info("retriever load schemas ready",
		slog.Int("graph_count", len(nextManifest.Graphs)),
		slog.Duration("wall_elapsed", time.Since(schemaStartedAt)),
	)

	var result loadResult
	result.GraphCount = len(nextManifest.Graphs)
	for graphIndex, graphEntry := range nextManifest.Graphs {
		graphStartedAt := time.Now()
		slog.Info("retriever load graph started",
			slog.String("graph", graphEntry.Name),
			slog.Int("graph_index", graphIndex+1),
			slog.Int("graph_count", len(nextManifest.Graphs)),
			slog.Int64("node_count", graphEntry.NodeCount),
			slog.Int64("edge_count", graphEntry.EdgeCount),
		)

		nodeStartedAt := time.Now()
		slog.Info("retriever load node phase started",
			slog.String("graph", graphEntry.Name),
			slog.Int64("node_count", graphEntry.NodeCount),
		)
		nodeMap, nodeCount, err := loadGraphNodes(ctx, db, options.InputDir, nextManifest.Compression, graphEntry)
		if err != nil {
			return loadResult{}, err
		}
		slog.Info("retriever load node phase completed",
			slog.String("graph", graphEntry.Name),
			slog.Int64("processed", nodeCount),
			slog.Duration("wall_elapsed", time.Since(nodeStartedAt)),
			slog.Float64("entities_per_second", perSecond(nodeCount, time.Since(nodeStartedAt))),
		)

		edgeStartedAt := time.Now()
		slog.Info("retriever load edge phase started",
			slog.String("graph", graphEntry.Name),
			slog.Int64("edge_count", graphEntry.EdgeCount),
			slog.Int("batch_size", options.BatchSize),
		)
		edgeCount, err := loadGraphEdges(ctx, db, options.InputDir, nextManifest.Compression, graphEntry, nodeMap, options.BatchSize)
		if err != nil {
			return loadResult{}, err
		}
		slog.Info("retriever load edge phase completed",
			slog.String("graph", graphEntry.Name),
			slog.Int64("processed", edgeCount),
			slog.Duration("wall_elapsed", time.Since(edgeStartedAt)),
			slog.Float64("entities_per_second", perSecond(edgeCount, time.Since(edgeStartedAt))),
		)
		if nodeCount != graphEntry.NodeCount {
			return loadResult{}, fmt.Errorf("loaded %d nodes for graph %q but manifest expected %d", nodeCount, graphEntry.Name, graphEntry.NodeCount)
		}
		if edgeCount != graphEntry.EdgeCount {
			return loadResult{}, fmt.Errorf("loaded %d relationships for graph %q but manifest expected %d", edgeCount, graphEntry.Name, graphEntry.EdgeCount)
		}
		result.NodeCount += nodeCount
		result.EdgeCount += edgeCount
		slog.Info("retriever load graph completed",
			slog.String("graph", graphEntry.Name),
			slog.Int64("node_count", nodeCount),
			slog.Int64("edge_count", edgeCount),
			slog.Duration("wall_elapsed", time.Since(graphStartedAt)),
		)
	}
	slog.Info("retriever load completed",
		slog.String("driver", driverName),
		slog.Int("graph_count", result.GraphCount),
		slog.Int64("node_count", result.NodeCount),
		slog.Int64("edge_count", result.EdgeCount),
		slog.Duration("wall_elapsed", time.Since(startedAt)),
	)
	return result, nil
}

func assertManifestSchemas(ctx context.Context, db graph.Database, value manifest) error {
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

func schemaAssertionsFromManifest(value manifest) ([]schemaAssertion, error) {
	schemaByGraph := map[string]graphSchemaMetadata{}
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

func manifestFileCount(value manifest) int {
	var count int
	for _, graphEntry := range value.Graphs {
		count += len(graphEntry.Files)
	}
	return count
}

func loadGraphNodes(ctx context.Context, db graph.Database, inputDir string, codec compressionCodec, graphEntry graphManifest) (map[string]graph.ID, int64, error) {
	nodeMap := make(map[string]graph.ID, int(graphEntry.NodeCount))
	var (
		loaded         int64
		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAt(graphEntry.NodeCount)
	)

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
			tx = tx.WithGraph(graph.Graph{
				Name: graphEntry.Name,
			})
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
		nextProgressAt = logRetrieverEntityProgress("retriever load node phase progress", graphEntry.Name, phaseNodes, loaded, graphEntry.NodeCount, startedAt, nextProgressAt)
	}

	return nodeMap, loaded, nil
}

func loadGraphEdges(ctx context.Context, db graph.Database, inputDir string, codec compressionCodec, graphEntry graphManifest, nodeMap map[string]graph.ID, batchSize int) (int64, error) {
	var (
		loaded         int64
		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAt(graphEntry.EdgeCount)
	)

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
			batch = batch.WithGraph(graph.Graph{
				Name: graphEntry.Name,
			})
			for _, item := range fragment.Items {
				resolved, err := resolveFragmentEdge(item, nodeMap)
				if err != nil {
					return err
				}
				if err := batch.CreateRelationshipByIDs(resolved.StartID, resolved.EndID, resolved.Kind, resolved.Properties); err != nil {
					return fmt.Errorf("create edge (%s)-[%s]->(%s): %w", item.StartID, item.Kind, item.EndID, err)
				}
			}
			return nil
		}, graph.WithBatchSize(batchSize)); err != nil {
			return loaded, fmt.Errorf("load edge fragment %s: %w", fileEntry.Path, err)
		}
		loaded += int64(len(fragment.Items))
		nextProgressAt = logRetrieverEntityProgress("retriever load edge phase progress", graphEntry.Name, phaseEdges, loaded, graphEntry.EdgeCount, startedAt, nextProgressAt)
	}

	return loaded, nil
}

func resolveFragmentEdge(item fragmentEdge, nodeMap map[string]graph.ID) (resolvedFragmentEdge, error) {
	startID, ok := nodeMap[item.StartID]
	if !ok {
		return resolvedFragmentEdge{}, fmt.Errorf("edge references missing start node %q", item.StartID)
	}
	endID, ok := nodeMap[item.EndID]
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

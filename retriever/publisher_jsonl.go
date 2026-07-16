package retriever

import "context"

type collectionPublication struct {
	Manifest Manifest
	Path     string
}

type collectionPublisher interface {
	AddGraph(GraphManifest, GraphSchemaMetadata, GraphMetrics)
	Publish(context.Context) (collectionPublication, error)
}

type jsonlCollectionPublisher struct {
	workspace collectionWorkspace
	manifest  Manifest
	metrics   MetricsManifest
}

func newJSONLFileManifest(summary shardSummary, metadata jsonlFragmentMetadata) FileManifest {
	return FileManifest{
		Phase:             summary.ID.Phase,
		Path:              metadata.Path,
		Count:             metadata.Rows,
		CompressedBytes:   metadata.CompressedBytes,
		UncompressedBytes: metadata.UncompressedBytes,
		SHA256:            metadata.SHA256,
		ActionCounts:      cloneActionCounts(summary.ActionCounts),
	}
}

func newJSONLCollectionPublisher(workspace collectionWorkspace, driverName string, options DumpOptions, scrub ScrubMetadata, graphCount int) *jsonlCollectionPublisher {
	return &jsonlCollectionPublisher{
		workspace: workspace,
		manifest:  newManifest(driverName, options.Compression, options.ZstdLevel, scrub, graphCount),
		metrics:   newMetricsManifest(graphCount),
	}
}

func (s *jsonlCollectionPublisher) AddGraph(graphEntry GraphManifest, schemaEntry GraphSchemaMetadata, metricsEntry GraphMetrics) {
	s.manifest.Graphs = append(s.manifest.Graphs, graphEntry)
	s.manifest.Schema.Graphs = append(s.manifest.Schema.Graphs, schemaEntry)
	s.metrics.Graphs = append(s.metrics.Graphs, metricsEntry)
	addActionCounts(s.manifest.Scrub.NodeActionCounts, graphEntry.NodeActionCounts)
	addActionCounts(s.manifest.Scrub.EdgeActionCounts, graphEntry.EdgeActionCounts)
}

func (s *jsonlCollectionPublisher) Publish(ctx context.Context) (collectionPublication, error) {
	nextManifest := s.manifest
	nextMetrics := s.metrics
	nextManifest.Metrics = &nextMetrics

	payload, err := encodeManifest(nextManifest)
	if err != nil {
		return collectionPublication{}, err
	}

	manifestPath, err := s.workspace.Publish(ctx, manifestFileName, payload)
	if err != nil {
		return collectionPublication{}, err
	}

	return collectionPublication{
		Manifest: nextManifest,
		Path:     manifestPath,
	}, nil
}

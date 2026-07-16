package retriever

type graphObserver struct {
	graphName        string
	nodeCount        int64
	edgeCount        int64
	nodeKinds        map[string]struct{}
	edgeKinds        map[string]struct{}
	nodeActionCounts map[string]int
	edgeActionCounts map[string]int
	metrics          *metricsBuilder
}

func newGraphObserver(graphName string, expectedNodeCount int64) *graphObserver {
	return &graphObserver{
		graphName:        graphName,
		nodeKinds:        map[string]struct{}{},
		edgeKinds:        map[string]struct{}{},
		nodeActionCounts: map[string]int{},
		edgeActionCounts: map[string]int{},
		metrics:          newMetricsBuilder(graphName, expectedNodeCount),
	}
}

func (s *graphObserver) ObserveNode(record normalizedNode, actionCounts map[string]int) error {
	if err := s.metrics.observeFragmentNode(jsonlV1NodeFromNormalized(record)); err != nil {
		return err
	}

	addKindsToSet(s.nodeKinds, record.Kinds)
	addActionCounts(s.nodeActionCounts, actionCounts)
	s.nodeCount++
	return nil
}

func (s *graphObserver) ObserveEdge(record normalizedEdge, actionCounts map[string]int) error {
	if err := s.metrics.observeFragmentEdge(jsonlV1EdgeFromNormalized(record)); err != nil {
		return err
	}

	if record.Kind != "" {
		s.edgeKinds[record.Kind] = struct{}{}
	}
	addActionCounts(s.edgeActionCounts, actionCounts)
	s.edgeCount++
	return nil
}

func (s *graphObserver) Schema() GraphSchemaMetadata {
	return GraphSchemaMetadata{
		Name:      s.graphName,
		NodeKinds: stringsFromKindSet(s.nodeKinds),
		EdgeKinds: stringsFromKindSet(s.edgeKinds),
	}
}

func (s *graphObserver) Metrics() GraphMetrics {
	return s.metrics.finalize()
}

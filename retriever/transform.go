package retriever

import "github.com/specterops/dawgs/graph"

type transformedBatch[T any] struct {
	Records []transformedRecord[T]
}

type transformedRecord[T any] struct {
	Record       T
	ActionCounts map[string]int
}

type transformSession interface {
	Metadata() ScrubMetadata
	NeedsPreparation() bool
	PrepareNode(*graph.Node)
	TransformNodes([]*graph.Node) transformedBatch[normalizedNode]
	TransformEdges([]*graph.Relationship) transformedBatch[normalizedEdge]
}

func newTransformSession(options DumpOptions) (transformSession, error) {
	if options.Scrub != ScrubFull {
		return identityTransformSession{}, nil
	}

	activeScrubber, err := newScrubber(options.ScrubConfig, options.Salt)
	if err != nil {
		return nil, err
	}
	return fullScrubTransformSession{scrubber: activeScrubber}, nil
}

type identityTransformSession struct{}

func (identityTransformSession) Metadata() ScrubMetadata {
	return ScrubMetadata{
		Mode:             ScrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}
}

func (identityTransformSession) NeedsPreparation() bool {
	return false
}

func (identityTransformSession) PrepareNode(*graph.Node) {}

func (identityTransformSession) TransformNodes(nodes []*graph.Node) transformedBatch[normalizedNode] {
	records := make([]transformedRecord[normalizedNode], len(nodes))
	for index, node := range nodes {
		records[index].Record = normalizeNode(node)
	}
	return transformedBatch[normalizedNode]{Records: records}
}

func (identityTransformSession) TransformEdges(relationships []*graph.Relationship) transformedBatch[normalizedEdge] {
	records := make([]transformedRecord[normalizedEdge], len(relationships))
	for index, relationship := range relationships {
		records[index].Record = normalizeEdge(relationship)
	}
	return transformedBatch[normalizedEdge]{Records: records}
}

type fullScrubTransformSession struct {
	scrubber *scrubber
}

func (s fullScrubTransformSession) Metadata() ScrubMetadata {
	return s.scrubber.metadata()
}

func (fullScrubTransformSession) NeedsPreparation() bool {
	return true
}

func (s fullScrubTransformSession) PrepareNode(node *graph.Node) {
	s.scrubber.observeNode(node.Properties.MapOrEmpty())
}

func (s fullScrubTransformSession) TransformNodes(nodes []*graph.Node) transformedBatch[normalizedNode] {
	batch := transformedBatch[normalizedNode]{
		Records: make([]transformedRecord[normalizedNode], len(nodes)),
	}
	for index, node := range nodes {
		record := normalizeNode(node)
		record.Properties, batch.Records[index].ActionCounts = s.scrubber.scrubProperties(record.Properties)
		batch.Records[index].Record = record
	}
	return batch
}

func (s fullScrubTransformSession) TransformEdges(relationships []*graph.Relationship) transformedBatch[normalizedEdge] {
	batch := transformedBatch[normalizedEdge]{
		Records: make([]transformedRecord[normalizedEdge], len(relationships)),
	}
	for index, relationship := range relationships {
		record := normalizeEdge(relationship)
		record.Properties, batch.Records[index].ActionCounts = s.scrubber.scrubProperties(record.Properties)
		batch.Records[index].Record = record
	}
	return batch
}

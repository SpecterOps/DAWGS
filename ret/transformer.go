package ret

import (
	"fmt"

	"github.com/specterops/dawgs/graph"
)

type normalizedNode struct {
	ID         string
	Kinds      []string
	Properties map[string]any
}

type normalizedRelationship struct {
	ID         string
	StartID    string
	EndID      string
	Kind       string
	Properties map[string]any
}

type transformer struct {
	scrubber scrubber

	counts actionCounts
}

func newTransformer(scrubber scrubber) transformer {
	return transformer{
		scrubber: scrubber,
	}
}

func (s *transformer) TransformNodes(graphNodes []*graph.Node) ([]normalizedNode, error) {
	normalizedNodes := make([]normalizedNode, 0, len(graphNodes))

	for _, graphNode := range graphNodes {
		if graphNode == nil {
			return nil, fmt.Errorf("graph node is nil")
		}

		normalized := normalizeNode(*graphNode)
		counts := s.scrubber.ScrubNode(&normalized)
		s.counts.addCounts(counts)
	}

	return normalizedNodes, nil
}

func (s *transformer) TransformRelationships(graphRels []*graph.Relationship) ([]normalizedRelationship, error) {
	normalizedRels := make([]normalizedRelationship, 0, len(graphRels))

	for _, graphRel := range graphRels {
		if graphRel == nil {
			return nil, fmt.Errorf("graph relationship is nil")
		}

		normalized := normalizeRelationship(*graphRel)
		counts := s.scrubber.ScrubRelationship(&normalized)
		s.counts.addCounts(counts)
	}

	return normalizedRels, nil
}

func normalizeNode(graphNode graph.Node) normalizedNode {
	normalized := normalizedNode{
		ID:    graphNode.ID.String(),
		Kinds: graphNode.Kinds.Strings(),
	}

	if graphNode.Properties != nil {
		normalized.Properties = graphNode.Properties.Map
	} else {
		normalized.Properties = make(map[string]any)
	}

	return normalized
}

func normalizeRelationship(graphRelationship graph.Relationship) normalizedRelationship {
	normalized := normalizedRelationship{
		ID:      graphRelationship.ID.String(),
		StartID: graphRelationship.StartID.String(),
		EndID:   graphRelationship.EndID.String(),
		Kind:    graphRelationship.Kind.String(),
	}

	if graphRelationship.Properties != nil {
		normalized.Properties = graphRelationship.Properties.Map
	} else {
		normalized.Properties = make(map[string]any)
	}

	return normalized
}

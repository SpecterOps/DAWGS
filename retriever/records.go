package retriever

import (
	"maps"
	"sort"

	"github.com/specterops/dawgs/graph"
)

// normalizedNode is the output-neutral node representation owned by the dump
// pipeline. Its kinds and top-level property map do not alias the source graph
// entity; property values are treated as immutable.
type normalizedNode struct {
	ID         string
	Kinds      []string
	Properties map[string]any
}

// normalizedEdge is the output-neutral relationship representation owned by
// the dump pipeline. ID is retained for sinks that need source relationship
// identity even though the JSONL v1 adapter intentionally omits it.
type normalizedEdge struct {
	ID         string
	StartID    string
	EndID      string
	Kind       string
	Properties map[string]any
}

func normalizeNode(node *graph.Node) normalizedNode {
	kinds := node.Kinds.Strings()
	sort.Strings(kinds)

	return normalizedNode{
		ID:         node.ID.String(),
		Kinds:      kinds,
		Properties: maps.Clone(node.Properties.MapOrEmpty()),
	}
}

func normalizeEdge(relationship *graph.Relationship) normalizedEdge {
	kind := ""
	if relationship.Kind != nil {
		kind = relationship.Kind.String()
	}

	return normalizedEdge{
		ID:         relationship.ID.String(),
		StartID:    relationship.StartID.String(),
		EndID:      relationship.EndID.String(),
		Kind:       kind,
		Properties: maps.Clone(relationship.Properties.MapOrEmpty()),
	}
}

func jsonlV1NodeFromNormalized(node normalizedNode) FragmentNode {
	return FragmentNode{
		ID:         node.ID,
		Kinds:      node.Kinds,
		Properties: node.Properties,
	}
}

func jsonlV1EdgeFromNormalized(edge normalizedEdge) FragmentEdge {
	return FragmentEdge{
		StartID:    edge.StartID,
		EndID:      edge.EndID,
		Kind:       edge.Kind,
		Properties: edge.Properties,
	}
}

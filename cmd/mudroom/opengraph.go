package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/specterops/dawgs/opengraph"
)

const RulesVersion = "shape-only-v1"

type OpenGraphDocument struct {
	Metadata map[string]any   `json:"metadata,omitempty"`
	Nodes    []opengraph.Node `json:"nodes,omitempty"`
	Edges    []opengraph.Edge `json:"edges,omitempty"`
	Graph    *OpenGraphGraph  `json:"graph,omitempty"`
}

type OpenGraphGraph struct {
	Nodes []opengraph.Node `json:"nodes"`
	Edges []opengraph.Edge `json:"edges"`
}

type Manifest struct {
	GeneratedAt         time.Time              `json:"generated_at"`
	Mode                ScrubMode              `json:"mode"`
	RulesVersion        string                 `json:"rules_version"`
	SaltProvided        bool                   `json:"salt_provided"`
	NodeCount           int                    `json:"node_count"`
	RelationshipCount   int                    `json:"relationship_count"`
	NodeActions         map[PropertyAction]int `json:"node_actions"`
	RelationshipActions map[PropertyAction]int `json:"relationship_actions"`
	TopologyPreserved   bool                   `json:"topology_preserved"`
	Notes               []string               `json:"notes,omitempty"`
}

func DecodeOpenGraphDocument(reader io.Reader) (OpenGraphDocument, error) {
	var document OpenGraphDocument
	if err := json.NewDecoder(reader).Decode(&document); err != nil {
		return document, fmt.Errorf("decode OpenGraph document: %w", err)
	}
	return document, nil
}

func (s Scrubber) ScrubOpenGraph(reader io.Reader, writer io.Writer) (Manifest, error) {
	document, err := DecodeOpenGraphDocument(reader)
	if err != nil {
		return Manifest{}, err
	}

	scrubbed, manifest := s.ScrubOpenGraphDocument(document)
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(scrubbed); err != nil {
		return Manifest{}, fmt.Errorf("encode scrubbed OpenGraph document: %w", err)
	}

	return manifest, nil
}

func (s Scrubber) ScrubOpenGraphDocument(document OpenGraphDocument) (OpenGraphDocument, Manifest) {
	manifest := Manifest{
		GeneratedAt:         time.Now().UTC(),
		Mode:                ModeShapeOnly,
		RulesVersion:        RulesVersion,
		SaltProvided:        s.config.Salt != "",
		NodeActions:         map[PropertyAction]int{},
		RelationshipActions: map[PropertyAction]int{},
		TopologyPreserved:   true,
		Notes: []string{
			"Node IDs are preserved.",
			"Relationship endpoints are preserved.",
			"Node and relationship kinds are preserved.",
			"Property values are scrubbed according to shape_only rules.",
		},
	}

	document.Metadata = s.scrubMetadataProperties(document.Metadata)

	if document.Graph != nil {
		identifierRegistry := s.collectOpenGraphIdentifierRegistry(document.Graph.Nodes)
		document.Graph.Nodes = s.scrubNodes(document.Graph.Nodes, manifest.NodeActions, identifierRegistry)
		document.Graph.Edges = s.scrubEdges(document.Graph.Edges, manifest.RelationshipActions)
		manifest.NodeCount = len(document.Graph.Nodes)
		manifest.RelationshipCount = len(document.Graph.Edges)
		return document, manifest
	}

	identifierRegistry := s.collectOpenGraphIdentifierRegistry(document.Nodes)
	document.Nodes = s.scrubNodes(document.Nodes, manifest.NodeActions, identifierRegistry)
	document.Edges = s.scrubEdges(document.Edges, manifest.RelationshipActions)
	manifest.NodeCount = len(document.Nodes)
	manifest.RelationshipCount = len(document.Edges)
	return document, manifest
}

func (s Scrubber) scrubMetadataProperties(properties map[string]any) map[string]any {
	scrubbed, _ := s.ScrubProperties(properties)
	return scrubbed
}

func (s Scrubber) collectOpenGraphIdentifierRegistry(nodes []opengraph.Node) *graphIdentifierRegistry {
	registry := newGraphIdentifierRegistry()
	identifierNodes := make([]graphIdentifierNode, 0, len(nodes))
	for _, node := range nodes {
		nextNode := graphIdentifierNode{
			kinds: node.Kinds,
		}
		nextNode.objectID, _ = stringPropertyValue(node.Properties[s.config.GraphRules.ObjectIDKey])
		nextNode.domainName, _ = stringPropertyValue(node.Properties[s.config.GraphRules.DomainNameKey])
		s.collectGraphIdentifierDomain(nextNode, registry)
		identifierNodes = append(identifierNodes, nextNode)
	}

	for _, node := range identifierNodes {
		s.collectGraphIdentifierObjectID(node, registry, false)
	}

	return registry
}

func (s Scrubber) scrubNodes(nodes []opengraph.Node, actionCounts map[PropertyAction]int, identifierRegistry *graphIdentifierRegistry) []opengraph.Node {
	scrubbed := make([]opengraph.Node, 0, len(nodes))
	for _, node := range nodes {
		nextNode := node
		nextNode.Properties = s.scrubPropertyMap(node.Properties, actionCounts)
		s.applyGraphIdentifierRewrites(node.Properties, nextNode.Properties, identifierRegistry)
		scrubbed = append(scrubbed, nextNode)
	}
	return scrubbed
}

func (s Scrubber) scrubEdges(edges []opengraph.Edge, actionCounts map[PropertyAction]int) []opengraph.Edge {
	scrubbed := make([]opengraph.Edge, 0, len(edges))
	for _, edge := range edges {
		nextEdge := edge
		nextEdge.Properties = s.scrubPropertyMap(edge.Properties, actionCounts)
		scrubbed = append(scrubbed, nextEdge)
	}
	return scrubbed
}

func (s Scrubber) scrubPropertyMap(properties map[string]any, actionCounts map[PropertyAction]int) map[string]any {
	scrubbed, plans := s.ScrubProperties(properties)
	for _, plan := range plans {
		actionCounts[plan.Action] += 1
	}
	return scrubbed
}

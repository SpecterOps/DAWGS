package main

import (
	"fmt"
	"io"
	"reflect"

	"github.com/specterops/dawgs/opengraph"
)

type ValidationFinding struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

type ValidationResult struct {
	Valid             bool                `json:"valid"`
	NodeCount         int                 `json:"node_count"`
	RelationshipCount int                 `json:"relationship_count"`
	Findings          []ValidationFinding `json:"findings,omitempty"`
}

func ValidateOpenGraphReaders(originalReader, scrubbedReader io.Reader) (ValidationResult, error) {
	original, err := DecodeOpenGraphDocument(originalReader)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("decode original OpenGraph document: %w", err)
	}

	scrubbed, err := DecodeOpenGraphDocument(scrubbedReader)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("decode scrubbed OpenGraph document: %w", err)
	}

	return ValidateOpenGraphDocuments(original, scrubbed), nil
}

func ValidateOpenGraphDocuments(original, scrubbed OpenGraphDocument) ValidationResult {
	originalNodes, originalEdges := documentGraph(original)
	scrubbedNodes, scrubbedEdges := documentGraph(scrubbed)

	result := ValidationResult{
		Valid:             true,
		NodeCount:         len(originalNodes),
		RelationshipCount: len(originalEdges),
	}

	if len(originalNodes) != len(scrubbedNodes) {
		result.addFinding("nodes", fmt.Sprintf("node count changed from %d to %d", len(originalNodes), len(scrubbedNodes)))
	}
	if len(originalEdges) != len(scrubbedEdges) {
		result.addFinding("edges", fmt.Sprintf("relationship count changed from %d to %d", len(originalEdges), len(scrubbedEdges)))
	}

	for idx := 0; idx < min(len(originalNodes), len(scrubbedNodes)); idx++ {
		if originalNodes[idx].ID != scrubbedNodes[idx].ID {
			result.addFinding(fmt.Sprintf("nodes[%d].id", idx), "node ID changed")
		}
		if !reflect.DeepEqual(originalNodes[idx].Kinds, scrubbedNodes[idx].Kinds) {
			result.addFinding(fmt.Sprintf("nodes[%d].kinds", idx), "node kinds changed")
		}
	}

	for idx := 0; idx < min(len(originalEdges), len(scrubbedEdges)); idx++ {
		originalEdge := originalEdges[idx]
		scrubbedEdge := scrubbedEdges[idx]

		if originalEdge.Kind != scrubbedEdge.Kind {
			result.addFinding(fmt.Sprintf("edges[%d].kind", idx), "relationship kind changed")
		}
		if originalEdge.StartID != scrubbedEdge.StartID {
			result.addFinding(fmt.Sprintf("edges[%d].start_id", idx), "relationship start_id changed")
		}
		if originalEdge.EndID != scrubbedEdge.EndID {
			result.addFinding(fmt.Sprintf("edges[%d].end_id", idx), "relationship end_id changed")
		}
	}

	return result
}

func (s *ValidationResult) addFinding(path, message string) {
	s.Valid = false
	s.Findings = append(s.Findings, ValidationFinding{
		Path:    path,
		Message: message,
	})
}

func documentGraph(document OpenGraphDocument) ([]opengraph.Node, []opengraph.Edge) {
	if document.Graph != nil {
		return document.Graph.Nodes, document.Graph.Edges
	}
	return document.Nodes, document.Edges
}

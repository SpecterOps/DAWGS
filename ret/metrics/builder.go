package metrics

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/ret/entity"
)

// Builder incrementally aggregates graph metrics. Nodes must be observed before
// any relationship that references them.
type Builder struct {
	nodeCount         int64
	relationshipCount int64
	nodeKinds         map[string]string
	inbound           map[string]int64
	outbound          map[string]int64
	nodeSequences     map[string]int64
	relationshipKinds map[string]int64
	endpointShapes    map[string]int64
}

// NewBuilder creates an empty graph metrics builder.
func NewBuilder() *Builder {
	return &Builder{
		nodeKinds:         make(map[string]string),
		inbound:           make(map[string]int64),
		outbound:          make(map[string]int64),
		nodeSequences:     make(map[string]int64),
		relationshipKinds: make(map[string]int64),
		endpointShapes:    make(map[string]int64),
	}
}

// ObserveNode adds one normalized graph node to the aggregate.
func (s *Builder) ObserveNode(node entity.Node) error {
	if err := node.Validate(); err != nil {
		return fmt.Errorf("metrics node observation: %w", err)
	}
	if _, found := s.nodeKinds[node.SourceID]; found {
		return fmt.Errorf("metrics node observation has duplicate source ID %q", node.SourceID)
	}

	kinds := OrderedKindsKey(node.Kinds)
	s.nodeKinds[node.SourceID] = kinds
	s.inbound[node.SourceID] = 0
	s.outbound[node.SourceID] = 0
	s.nodeSequences[kinds]++
	s.nodeCount++

	return nil
}

// ObserveRelationship adds one normalized graph relationship to the aggregate.
func (s *Builder) ObserveRelationship(relationship entity.Relationship) error {
	if err := relationship.Validate(); err != nil {
		return fmt.Errorf("metrics relationship observation: %w", err)
	}

	startKinds, startOK := s.nodeKinds[relationship.StartID]
	endKinds, endOK := s.nodeKinds[relationship.EndID]
	if !startOK || !endOK {
		return fmt.Errorf("relationship %q references missing endpoint", relationship.SourceID)
	}

	s.relationshipKinds[relationship.Kind]++
	s.outbound[relationship.StartID]++
	s.inbound[relationship.EndID]++
	s.endpointShapes[endpointShapeKey(startKinds, relationship.Kind, endKinds)]++
	s.relationshipCount++

	return nil
}

// Finalize returns an independent graph metrics snapshot.
func (s *Builder) Finalize() GraphMetrics {
	value := GraphMetrics{
		NodeCount:               s.nodeCount,
		RelationshipCount:       s.relationshipCount,
		NodeKindSequences:       cloneHistogram(s.nodeSequences),
		RelationshipKinds:       cloneHistogram(s.relationshipKinds),
		InboundDegreeHistogram:  make(map[string]int64),
		OutboundDegreeHistogram: make(map[string]int64),
		EndpointShapeHistogram:  cloneHistogram(s.endpointShapes),
	}

	for sourceID := range s.nodeKinds {
		value.InboundDegreeHistogram[strconv.FormatInt(s.inbound[sourceID], 10)]++
		value.OutboundDegreeHistogram[strconv.FormatInt(s.outbound[sourceID], 10)]++
	}

	value.Fingerprint = fingerprint(value)

	return value
}

// OrderedKindsKey encodes every kind segment in order, preserving duplicates.
func OrderedKindsKey(kinds []string) string {
	var key strings.Builder
	for _, kind := range kinds {
		key.WriteString(strconv.Itoa(len(kind)))
		key.WriteByte(':')
		key.WriteString(kind)
	}

	return key.String()
}

func endpointShapeKey(startKinds, relationshipKind, endKinds string) string {
	return OrderedKindsKey([]string{startKinds, relationshipKind, endKinds})
}

func cloneHistogram(source map[string]int64) map[string]int64 {
	clone := make(map[string]int64, len(source))
	for key, count := range source {
		clone[key] = count
	}

	return clone
}

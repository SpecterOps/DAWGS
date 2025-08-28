package changelog

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/specterops/dawgs/graph"
)

type Change interface {
	IdentityKey() uint64   // identity hash
	Hash() (uint64, error) // content hash
}

var (
	ignoredPropertiesKeys = map[string]struct{}{
		"lastseen":      {},
		"objectid":      {},
		"lastcollected": {},
		"isinherited":   {},
		"domainsid":     {},
		"isacl":         {},
		"tenantid":      {},
	}
)

type NodeChange struct {
	NodeID     string
	Properties *graph.Properties
	Kinds      graph.Kinds
}

func NewNodeChange(nodeID string, kinds graph.Kinds, properties *graph.Properties) *NodeChange {
	return &NodeChange{
		NodeID:     nodeID,
		Kinds:      kinds,
		Properties: properties,
	}
}

func (s NodeChange) IdentityKey() uint64 {
	hash := xxhash.Sum64String(s.NodeID)
	return hash
}

func (s NodeChange) Hash() (uint64, error) {
	props := s.Properties
	if props == nil {
		props = graph.NewProperties()
	}

	if propertiesHash, err := props.Hash(ignoredPropertiesKeys); err != nil {
		return 0, fmt.Errorf("node properties hash error: %w", err)
	} else if kindsHash, err := s.Kinds.Hash(); err != nil {
		return 0, fmt.Errorf("node kinds hash error: %w", err)
	} else {
		combined := append(propertiesHash, kindsHash...)
		return xxhash.Sum64(combined), nil
	}
}

type EdgeChange struct {
	SourceNodeID string
	TargetNodeID string
	Kind         graph.Kind
	Properties   *graph.Properties
}

func NewEdgeChange(sourceNodeID, targetNodeID string, kind graph.Kind, properties *graph.Properties) *EdgeChange {
	return &EdgeChange{
		SourceNodeID: sourceNodeID,
		TargetNodeID: targetNodeID,
		Kind:         kind,
		Properties:   properties,
	}
}

func (s EdgeChange) IdentityKey() uint64 {
	identity := s.SourceNodeID + "|" + s.TargetNodeID + "|" + s.Kind.String()
	hash := xxhash.Sum64String(identity)
	return hash
}

func (s EdgeChange) Hash() (uint64, error) {
	props := s.Properties
	if props == nil {
		props = graph.NewProperties()
	}

	if dataHash, err := props.Hash(ignoredPropertiesKeys); err != nil {
		return 0, fmt.Errorf("edge properties hash error: %w", err)
	} else {
		return xxhash.Sum64(dataHash), nil
	}
}

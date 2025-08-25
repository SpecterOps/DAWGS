package changelog

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/specterops/dawgs/graph"
)

type ChangeType int

const (
	ChangeTypeAdded ChangeType = iota
	ChangeTypeModified
	ChangeTypeRemoved
	ChangeTypeNoChange // is this dumb
)

func (c ChangeType) String() string {
	switch c {
	case ChangeTypeModified:
		return "modified"
	case ChangeTypeAdded:
		return "added"
	case ChangeTypeRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

type Change interface {
	Type() ChangeType
	IdentityKey() uint64
	Hash() (uint64, error)
}

type NodeChange struct {
	changeType ChangeType
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

func (s NodeChange) Type() ChangeType {
	return s.changeType
}

func (s NodeChange) IdentityKey() uint64 {
	hash := xxhash.Sum64String(s.NodeID)
	return hash
}

func (s NodeChange) Hash() (uint64, error) {
	if propertiesHash, err := s.Properties.Hash(ignoredPropertiesKeys); err != nil {
		return 0, fmt.Errorf("node properties hash error: %w", err)
	} else if kindsHash, err := s.Kinds.Hash(); err != nil {
		return 0, fmt.Errorf("node kinds hash error: %w", err)
	} else {
		combined := append(propertiesHash, kindsHash...)
		return xxhash.Sum64(combined), nil
	}
}

type EdgeChange struct {
	ChangeType ChangeType

	SourceNodeID string
	TargetNodeID string
	Kind         graph.Kind
	Properties   *graph.Properties
}

func NewEdgeChange(changeType ChangeType, sourceNodeID, targetNodeID string, kind graph.Kind, properties *graph.Properties) *EdgeChange {
	return &EdgeChange{
		ChangeType:   changeType,
		SourceNodeID: sourceNodeID,
		TargetNodeID: targetNodeID,
		Kind:         kind,
		Properties:   properties,
	}
}

func (s EdgeChange) Type() ChangeType {
	return s.ChangeType
}

func (s EdgeChange) IdentityKey() uint64 {
	identity := s.SourceNodeID + s.TargetNodeID + s.Kind.String()
	hash := xxhash.Sum64String(identity)
	return hash
}

func (s EdgeChange) Hash() (uint64, error) {
	if dataHash, err := s.Properties.Hash(ignoredPropertiesKeys); err != nil {
		return 0, fmt.Errorf("edge properties hash error: %w", err)
	} else {
		return xxhash.Sum64(dataHash), nil
	}
}

func (s NodeChange) ShouldSubmit() bool {
	return s.changeType != ChangeTypeNoChange
}

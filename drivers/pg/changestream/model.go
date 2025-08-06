package changestream

import (
	"fmt"

	"github.com/specterops/dawgs/graph"
)

type ChangeType int

const (
	ChangeTypeModified ChangeType = iota
	ChangeTypeAdded
	ChangeTypeRemoved
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
	IdentityKey() string
	Hash() ([]byte, error)
	Query() string
}

type NodeChange struct {
	ChangeType ChangeType

	NodeID     string
	Kinds      graph.Kinds
	Properties *graph.Properties
}

func NewNodeChange(changeType ChangeType, nodeID string, kinds graph.Kinds, properties *graph.Properties) *NodeChange {
	return &NodeChange{
		ChangeType: changeType,
		NodeID:     nodeID,
		Kinds:      kinds,
		Properties: properties,
	}
}

func (s NodeChange) Type() ChangeType {
	return s.ChangeType
}

func (s NodeChange) IdentityKey() string {
	return s.NodeID
}

func (s NodeChange) Hash() ([]byte, error) {
	if propertiesHash, err := s.Properties.Hash(ignoredPropertiesKeys); err != nil {
		return nil, fmt.Errorf("node properties hash error: %w", err)
	} else if kindsHash, err := s.Kinds.Hash(); err != nil {
		return nil, fmt.Errorf("node kinds hash error: %w", err)
	} else {
		combined := append(propertiesHash, kindsHash...)
		return combined, nil
	}
}

func (s NodeChange) Query() string {
	return LAST_NODE_CHANGE_SQL
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

func (s EdgeChange) IdentityKey() string {
	return s.SourceNodeID + s.TargetNodeID + s.Kind.String()
}

func (s EdgeChange) Hash() ([]byte, error) {
	if propertiesHash, err := s.Properties.Hash(ignoredPropertiesKeys); err != nil {
		return nil, fmt.Errorf("edge properties hash error: %w", err)
	} else {
		return propertiesHash, nil
	}
}

func (s EdgeChange) Query() string {
	return LAST_EDGE_CHANGE_SQL
}

type ChangeStatus struct {
	Type           ChangeType
	PropertiesHash []byte
	Exists         bool
	Changed        bool
}

func (s ChangeStatus) ShouldSubmit() bool {
	return !s.Exists || s.Changed
}

package changestream

import (
	"github.com/cespare/xxhash/v2"
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

type EdgeChange struct {
	ChangeType ChangeType

	TargetNodeID  string
	RelatedNodeID string
	Kind          graph.Kind
	Properties    *graph.Properties
}

func NewEdgeChange(changeType ChangeType, targetNodeID, relatedNodeID string, kind graph.Kind, properties *graph.Properties) *EdgeChange {
	return &EdgeChange{
		ChangeType:    changeType,
		TargetNodeID:  targetNodeID,
		RelatedNodeID: relatedNodeID,
		Kind:          kind,
		Properties:    properties,
	}
}

func (s EdgeChange) IdentityKey() string {
	return s.TargetNodeID + s.RelatedNodeID + s.Kind.String()
}

func (s EdgeChange) IdentityHash() ([]byte, error) {
	digest := xxhash.New()

	if _, err := digest.Write([]byte(s.IdentityKey())); err != nil {
		return nil, err
	}

	return digest.Sum(nil), nil
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

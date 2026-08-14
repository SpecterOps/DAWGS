package jsonl

import "github.com/specterops/dawgs/ret/entity"

type record interface {
	NodeRecord | RelationshipRecord
}

// NodeRecord is the JSON representation of an entity.Node.
type NodeRecord struct {
	SourceID   string         `json:"source_id"`
	Kinds      []string       `json:"kinds"`
	Properties map[string]any `json:"properties"`
}

// RelationshipRecord is the JSON representation of an entity.Relationship.
// SourceID is intentionally omitted because JSONL relationships are identified
// by their endpoints and kind.
type RelationshipRecord struct {
	StartID    string         `json:"start_id"`
	EndID      string         `json:"end_id"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties"`
}

func nodeRecord(value entity.Node) NodeRecord {
	return NodeRecord{SourceID: value.SourceID, Kinds: value.Kinds, Properties: value.Properties}
}

func relationshipRecord(value entity.Relationship) RelationshipRecord {
	return RelationshipRecord{StartID: value.StartID, EndID: value.EndID, Kind: value.Kind, Properties: value.Properties}
}

func nodeEntity(value NodeRecord) entity.Node {
	return entity.Node{SourceID: value.SourceID, Kinds: value.Kinds, Properties: value.Properties}
}

func relationshipEntity(value RelationshipRecord) entity.Relationship {
	return entity.Relationship{StartID: value.StartID, EndID: value.EndID, Kind: value.Kind, Properties: value.Properties}
}

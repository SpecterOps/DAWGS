package parquet

import (
	"fmt"

	"github.com/specterops/dawgs/ret/entity"
)

type NodeRow struct {
	SourceID   string   `parquet:"source_id"`
	Kinds      []string `parquet:"kinds,list"`
	Properties any      `parquet:"properties,variant"`
}

type RelationshipRow struct {
	SourceID   string `parquet:"source_id"`
	StartID    string `parquet:"start_id"`
	EndID      string `parquet:"end_id"`
	Kind       string `parquet:"kind"`
	Properties any    `parquet:"properties,variant"`
}

func nodeRow(value entity.Node) NodeRow {
	return NodeRow{
		SourceID:   value.SourceID,
		Kinds:      entity.CloneKinds(value.Kinds),
		Properties: entity.CloneProperties(value.Properties),
	}
}

func relationshipRow(value entity.Relationship) RelationshipRow {
	return RelationshipRow{
		SourceID:   value.SourceID,
		StartID:    value.StartID,
		EndID:      value.EndID,
		Kind:       value.Kind,
		Properties: entity.CloneProperties(value.Properties),
	}
}

func (s NodeRow) entity() (entity.Node, error) {
	properties, err := propertyMap(s.Properties)
	if err != nil {
		return entity.Node{}, err
	}
	value := entity.Node{
		SourceID:   s.SourceID,
		Kinds:      entity.CloneKinds(s.Kinds),
		Properties: properties,
	}
	if err := value.Validate(); err != nil {
		return entity.Node{}, err
	}
	return value, nil
}

func (s RelationshipRow) entity() (entity.Relationship, error) {
	if s.SourceID == "" {
		return entity.Relationship{}, fmt.Errorf("relationship source ID is required")
	}
	properties, err := propertyMap(s.Properties)
	if err != nil {
		return entity.Relationship{}, err
	}
	value := entity.Relationship{
		SourceID:   s.SourceID,
		StartID:    s.StartID,
		EndID:      s.EndID,
		Kind:       s.Kind,
		Properties: properties,
	}
	if err := value.Validate(); err != nil {
		return entity.Relationship{}, err
	}
	return value, nil
}

func propertyMap(value any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	properties, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("properties VARIANT has type %T, want map[string]any or null", value)
	}
	return entity.CloneProperties(properties), nil
}

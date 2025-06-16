package model

import (
	"github.com/specterops/dawgs/graph"
)

type IndexChangeSet struct {
	NodeIndexesToRemove     []string
	EdgeIndexesToRemove     []string
	NodeConstraintsToRemove []string
	EdgeConstraintsToRemove []string
	NodeIndexesToAdd        map[string]graph.Index
	EdgeIndexesToAdd        map[string]graph.Index
	NodeConstraintsToAdd    map[string]graph.Constraint
	EdgeConstraintsToAdd    map[string]graph.Constraint
}

func NewIndexChangeSet() IndexChangeSet {
	return IndexChangeSet{
		NodeIndexesToAdd:     map[string]graph.Index{},
		NodeConstraintsToAdd: map[string]graph.Constraint{},
		EdgeIndexesToAdd:     map[string]graph.Index{},
		EdgeConstraintsToAdd: map[string]graph.Constraint{},
	}
}

type GraphPartition struct {
	Name        string
	Indexes     map[string]graph.Index
	Constraints map[string]graph.Constraint
}

func NewGraphPartition(name string) GraphPartition {
	return GraphPartition{
		Name:        name,
		Indexes:     map[string]graph.Index{},
		Constraints: map[string]graph.Constraint{},
	}
}

func NewGraphPartitionFromSchema(name string, indexes []graph.Index, constraints []graph.Constraint) GraphPartition {
	graphPartition := GraphPartition{
		Name:        name,
		Indexes:     make(map[string]graph.Index, len(indexes)),
		Constraints: make(map[string]graph.Constraint, len(constraints)),
	}

	for _, index := range indexes {
		graphPartition.Indexes[IndexName(name, index)] = index
	}

	for _, constraint := range constraints {
		graphPartition.Constraints[ConstraintName(name, constraint)] = constraint
	}

	return graphPartition
}

type GraphPartitions struct {
	Node GraphPartition
	Edge GraphPartition
}

type Graph struct {
	ID         int32
	Name       string
	Partitions GraphPartitions
}

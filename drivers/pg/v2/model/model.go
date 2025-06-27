package model

import (
	v2 "github.com/specterops/dawgs/v2"
)

type IndexChangeSet struct {
	NodeIndexesToRemove     []string
	EdgeIndexesToRemove     []string
	NodeConstraintsToRemove []string
	EdgeConstraintsToRemove []string
	NodeIndexesToAdd        map[string]v2.Index
	EdgeIndexesToAdd        map[string]v2.Index
	NodeConstraintsToAdd    map[string]v2.Constraint
	EdgeConstraintsToAdd    map[string]v2.Constraint
}

func NewIndexChangeSet() IndexChangeSet {
	return IndexChangeSet{
		NodeIndexesToAdd:     map[string]v2.Index{},
		NodeConstraintsToAdd: map[string]v2.Constraint{},
		EdgeIndexesToAdd:     map[string]v2.Index{},
		EdgeConstraintsToAdd: map[string]v2.Constraint{},
	}
}

type GraphPartition struct {
	Name        string
	Indexes     map[string]v2.Index
	Constraints map[string]v2.Constraint
}

func NewGraphPartition(name string) GraphPartition {
	return GraphPartition{
		Name:        name,
		Indexes:     map[string]v2.Index{},
		Constraints: map[string]v2.Constraint{},
	}
}

func NewGraphPartitionFromSchema(name string, indexes []v2.Index, constraints []v2.Constraint) GraphPartition {
	graphPartition := GraphPartition{
		Name:        name,
		Indexes:     make(map[string]v2.Index, len(indexes)),
		Constraints: make(map[string]v2.Constraint, len(constraints)),
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

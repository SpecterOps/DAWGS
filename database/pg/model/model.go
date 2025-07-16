package model

import (
	"github.com/specterops/dawgs/database"
)

type IndexChangeSet struct {
	NodeIndexesToRemove     []string
	EdgeIndexesToRemove     []string
	NodeConstraintsToRemove []string
	EdgeConstraintsToRemove []string
	NodeIndexesToAdd        map[string]database.Index
	EdgeIndexesToAdd        map[string]database.Index
	NodeConstraintsToAdd    map[string]database.Constraint
	EdgeConstraintsToAdd    map[string]database.Constraint
}

func NewIndexChangeSet() IndexChangeSet {
	return IndexChangeSet{
		NodeIndexesToAdd:     map[string]database.Index{},
		NodeConstraintsToAdd: map[string]database.Constraint{},
		EdgeIndexesToAdd:     map[string]database.Index{},
		EdgeConstraintsToAdd: map[string]database.Constraint{},
	}
}

type GraphPartition struct {
	Name        string
	Indexes     map[string]database.Index
	Constraints map[string]database.Constraint
}

func NewGraphPartition(name string) GraphPartition {
	return GraphPartition{
		Name:        name,
		Indexes:     map[string]database.Index{},
		Constraints: map[string]database.Constraint{},
	}
}

func NewGraphPartitionFromSchema(name string, indexes []database.Index, constraints []database.Constraint) GraphPartition {
	graphPartition := GraphPartition{
		Name:        name,
		Indexes:     make(map[string]database.Index, len(indexes)),
		Constraints: make(map[string]database.Constraint, len(constraints)),
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

package database

import "github.com/specterops/dawgs/graph"

type IndexType int

const (
	IndexTypeUnsupported IndexType = 0
	IndexTypeBTree       IndexType = 1
	IndexTypeTextSearch  IndexType = 2
)

func (s IndexType) String() string {
	switch s {
	case IndexTypeBTree:
		return "btree"

	case IndexTypeTextSearch:
		return "fts"

	case IndexTypeUnsupported:
		return "unsupported"

	default:
		return "invalid"
	}
}

type Index struct {
	Name  string
	Field string
	Type  IndexType
}

type Constraint Index

type Graph struct {
	Name            string
	Nodes           graph.Kinds
	Edges           graph.Kinds
	NodeConstraints []Constraint
	EdgeConstraints []Constraint
	NodeIndexes     []Index
	EdgeIndexes     []Index
}

type Schema struct {
	GraphSchemas     map[string]Graph
	DefaultGraphName string
}

func NewSchema(defaultGraphName string, graphSchemas ...Graph) Schema {
	graphSchemaMap := map[string]Graph{}

	for _, graphSchema := range graphSchemas {
		graphSchemaMap[graphSchema.Name] = graphSchema
	}

	return Schema{
		GraphSchemas:     graphSchemaMap,
		DefaultGraphName: defaultGraphName,
	}
}

func (s *Schema) DefaultGraph() (Graph, bool) {
	defaultGraph, hasDefaultGraph := s.GraphSchemas[s.DefaultGraphName]
	return defaultGraph, hasDefaultGraph
}

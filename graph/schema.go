package graph

type IndexType int

const (
	UnsupportedIndex IndexType = 0
	BTreeIndex       IndexType = 1
	TextSearchIndex  IndexType = 2
)

func (s IndexType) String() string {
	switch s {
	case BTreeIndex:
		return "btree"

	case TextSearchIndex:
		return "fts"

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
	Nodes           Kinds
	Edges           Kinds
	NodeConstraints []Constraint
	EdgeConstraints []Constraint
	NodeIndexes     []Index
	EdgeIndexes     []Index
}

type Schema struct {
	Graphs       []Graph
	DefaultGraph Graph
}

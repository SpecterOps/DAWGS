package v1compat

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
)

type String = graph.String
type Kind = graph.Kind
type Kinds = graph.Kinds
type KindBitmaps = graph.KindBitmaps
type ThreadSafeKindBitmap = graph.ThreadSafeKindBitmap
type ID = graph.ID
type Node = graph.Node
type NodeSet = graph.NodeSet
type ThreadSafeNodeSet = graph.ThreadSafeNodeSet
type NodeKindSet = graph.NodeKindSet
type NodeUpdate = graph.NodeUpdate
type Relationship = graph.Relationship
type RelationshipSet = graph.RelationshipSet
type RelationshipUpdate = graph.RelationshipUpdate
type Path = graph.Path
type PathSegment = graph.PathSegment
type PathSet = graph.PathSet
type Criteria = cypher.SyntaxNode
type Properties = graph.Properties
type PropertyMap = graph.PropertyMap
type PropertyValue = graph.PropertyValue
type Direction = graph.Direction
type Tree = graph.Tree
type Literal = graph.Literal
type Literals = graph.Literals

var (
	NewPathSet              = graph.NewPathSet
	StringKind              = graph.StringKind
	NewProperties           = graph.NewProperties
	NewNode                 = graph.NewNode
	NewNodeSet              = graph.NewNodeSet
	NewRelationship         = graph.NewRelationship
	NewRelationshipSet      = graph.NewRelationshipSet
	Uint32SliceToIDs        = graph.Uint32SliceToIDs
	Uint64SliceToIDs        = graph.Uint64SliceToIDs
	NewRootPathSegment      = graph.NewRootPathSegment
	NewThreadSafeNodeSet    = graph.NewThreadSafeNodeSet
	PrepareNode             = graph.PrepareNode
	PrepareRelationship     = graph.PrepareRelationship
	NewNodeKindSet          = graph.NewNodeKindSet
	NodeSetToDuplex         = graph.NodeSetToDuplex
	NodeIDsToDuplex         = graph.NodeIDsToDuplex
	StringsToKinds          = graph.StringsToKinds
	SortAndSliceNodeSet     = graph.SortAndSliceNodeSet
	FormatPathSegment       = graph.FormatPathSegment
	NewThreadSafeKindBitmap = graph.NewThreadSafeKindBitmap
	NewTree                 = graph.NewTree

	EmptyKind = graph.EmptyKind

	ErrNoResultsFound                  = graph.ErrNoResultsFound
	ErrMissingResultExpectation        = graph.ErrMissingResultExpectation
	ErrUnsupportedDatabaseOperation    = graph.ErrUnsupportedDatabaseOperation
	ErrPropertyNotFound                = graph.ErrPropertyNotFound
	ErrContextTimedOut                 = graph.ErrContextTimedOut
	ErrConcurrentConnectionSlotTimeOut = graph.ErrConcurrentConnectionSlotTimeOut
)

type Constraint = database.Constraint
type Schema = database.Schema
type Graph = database.Graph
type Index = database.Index

const (
	BTreeIndex      = database.IndexTypeBTree
	TextSearchIndex = database.IndexTypeTextSearch

	UnregisteredNodeID = graph.UnregisteredNodeID

	DirectionOutbound = graph.DirectionOutbound
	DirectionInbound  = graph.DirectionInbound
	DirectionBoth     = graph.DirectionBoth
)

func symbolMapToStringMap(props map[String]any) map[string]any {
	store := make(map[string]any, len(props))

	for k, v := range props {
		store[k.String()] = v
	}

	return store
}

func AsProperties[T PropertyMap | map[String]any | map[string]any](rawStore T) *Properties {
	var store map[string]any

	switch typedStore := any(rawStore).(type) {
	case PropertyMap:
		store = symbolMapToStringMap(typedStore)

	case map[String]any:
		store = symbolMapToStringMap(typedStore)

	case map[string]any:
		store = typedStore
	}

	return &Properties{
		Map:      store,
		Modified: make(map[string]struct{}),
		Deleted:  make(map[string]struct{}),
	}
}

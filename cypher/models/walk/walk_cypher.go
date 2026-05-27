package walk

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/graph"
)

func newCypherWalkCursorWithBranches[F any, FS []F](node cypher.SyntaxNode, branches FS) *Cursor[cypher.SyntaxNode] {
	cursor := &Cursor[cypher.SyntaxNode]{
		Node:     node,
		Branches: make([]cypher.SyntaxNode, 0, len(branches)),
	}

	addCypherBranches(cursor, branches)
	return cursor
}

func newCypherWalkCursorWithBranchPrefix[F any, FS []F](node cypher.SyntaxNode, prefix cypher.SyntaxNode, branches FS) *Cursor[cypher.SyntaxNode] {
	cursor := &Cursor[cypher.SyntaxNode]{
		Node:     node,
		Branches: make([]cypher.SyntaxNode, 0, len(branches)+1),
	}

	if !isNilNode(prefix) {
		cursor.AddBranches(prefix)
	}
	addCypherBranches(cursor, branches)
	return cursor
}

func newCypherWalkCursorWithMapItems(node cypher.SyntaxNode, mapLiteral cypher.MapLiteral) *Cursor[cypher.SyntaxNode] {
	cursor := &Cursor[cypher.SyntaxNode]{
		Node:     node,
		Branches: make([]cypher.SyntaxNode, 0, len(mapLiteral)),
	}

	_ = mapLiteral.ForEachItem(func(key string, value cypher.Expression) error {
		cursor.AddBranches(&cypher.MapItem{
			Key:   key,
			Value: value,
		})
		return nil
	})

	return cursor
}

func addCypherBranches[F any, FS []F](cursor *Cursor[cypher.SyntaxNode], branches FS) {
	for _, branch := range branches {
		cursor.AddBranches(cypher.SyntaxNode(branch))
	}
}

func newCypherStructuralWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], error) {
	if cursor, handled := newCypherStructuralValueWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherStructuralPatternWalkCursor(node); handled {
		return cursor, nil
	}

	return newCypherWalkCursor(node)
}

func newCypherStructuralValueWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.Limit:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Value != nil {
			nextCursor.AddBranches(typedNode.Value)
		}
		return nextCursor, true

	case *cypher.Skip:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Value != nil {
			nextCursor.AddBranches(typedNode.Value)
		}
		return nextCursor, true

	case *cypher.KindMatcher:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Reference != nil {
			nextCursor.AddBranches(typedNode.Reference)
		}
		if typedNode.Kinds != nil {
			nextCursor.AddBranches(typedNode.Kinds)
		}
		return nextCursor, true

	case *cypher.Properties:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}
		if typedNode.Parameter != nil {
			nextCursor.AddBranches(typedNode.Parameter)
		}
		if typedNode.Map != nil {
			nextCursor.AddBranches(typedNode.Map)
		}
		return nextCursor, true

	case *cypher.RemoveItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.KindMatcher != nil {
			nextCursor.AddBranches(typedNode.KindMatcher)
		}
		if typedNode.Property != nil {
			nextCursor.AddBranches(typedNode.Property)
		}
		return nextCursor, true

	case *cypher.IDInCollection:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Variable != nil {
			nextCursor.AddBranches(typedNode.Variable)
		}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		return nextCursor, true

	case *cypher.ProjectionItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		if typedNode.Alias != nil {
			nextCursor.AddBranches(typedNode.Alias)
		}
		return nextCursor, true

	case *cypher.PartialComparison:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator},
		}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	case *cypher.PartialArithmeticExpression:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator},
		}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	case *cypher.UnaryAddOrSubtractExpression:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator},
		}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherStructuralPatternWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.PatternPart:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: make([]cypher.SyntaxNode, 0, len(typedNode.PatternElements)+1),
		}
		if typedNode.Variable != nil {
			nextCursor.AddBranches(typedNode.Variable)
		}
		addCypherBranches(nextCursor, typedNode.PatternElements)
		return nextCursor, true

	case *cypher.RelationshipPattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}
		if typedNode.Variable != nil {
			nextCursor.AddBranches(typedNode.Variable)
		}
		if typedNode.Kinds != nil {
			nextCursor.AddBranches(typedNode.Kinds)
		}
		if typedNode.Range != nil {
			nextCursor.AddBranches(typedNode.Range)
		}
		if typedNode.Properties != nil {
			nextCursor.AddBranches(typedNode.Properties)
		}
		return nextCursor, true

	case *cypher.NodePattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}
		if typedNode.Variable != nil {
			nextCursor.AddBranches(typedNode.Variable)
		}
		if typedNode.Kinds != nil {
			nextCursor.AddBranches(typedNode.Kinds)
		}
		if typedNode.Properties != nil {
			nextCursor.AddBranches(typedNode.Properties)
		}
		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], error) {
	if cursor, handled := newCypherLeafWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherValueWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherPredicateWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherOperatorWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherProjectionWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherQueryWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherUpdatingWalkCursor(node); handled {
		return cursor, nil
	}
	if cursor, handled := newCypherPatternWalkCursor(node); handled {
		return cursor, nil
	}

	return nil, fmt.Errorf("unable to negotiate cypher model type %T into a translation cursor", node)
}

func newCypherLeafWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch node.(type) {
	case *cypher.RangeQuantifier, *cypher.PatternRange, cypher.Operator, *cypher.Limit, *cypher.Skip,
		graph.Kinds, *cypher.Parameter, *cypher.Literal, *cypher.Variable:
		return &Cursor[cypher.SyntaxNode]{
			Node: node,
		}, true

	default:
		return nil, false
	}
}

func newCypherValueWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.KindMatcher:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Reference != nil {
			nextCursor.AddBranches(typedNode.Reference)
		}
		return nextCursor, true

	case *cypher.PropertyLookup:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Atom != nil {
			nextCursor.AddBranches(typedNode.Atom)
		}
		return nextCursor, true

	case *cypher.MapItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Value != nil {
			nextCursor.AddBranches(typedNode.Value)
		}
		return nextCursor, true

	case *cypher.Properties:
		if typedNode.Parameter != nil {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: []cypher.SyntaxNode{typedNode.Parameter},
			}, true
		} else {
			return newCypherWalkCursorWithMapItems(node, typedNode.Map), true
		}

	case cypher.MapLiteral:
		return newCypherWalkCursorWithMapItems(node, typedNode), true

	case *cypher.ListLiteral:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Expressions()), true

	case *cypher.FunctionInvocation:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Arguments), true

	case *cypher.Parenthetical:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherPredicateWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.Quantifier:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Filter != nil {
			nextCursor.AddBranches(typedNode.Filter)
		}
		return nextCursor, true

	case *cypher.FilterExpression:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}

		if typedNode.Specifier != nil {
			nextCursor.AddBranches(typedNode.Specifier)
		}
		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		return nextCursor, true

	case *cypher.IDInCollection:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		return nextCursor, true

	case *cypher.Where:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), true

	case *cypher.Negation:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		return nextCursor, true

	case *cypher.Conjunction:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), true

	case *cypher.Disjunction:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), true

	case *cypher.ExclusiveDisjunction:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), true

	default:
		return nil, false
	}
}

func newCypherOperatorWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.ArithmeticExpression:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.Left, typedNode.Partials), true

	case *cypher.PartialArithmeticExpression:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator},
		}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	case *cypher.PartialComparison:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	case *cypher.Comparison:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.Left, typedNode.Partials), true

	case *cypher.UnaryAddOrSubtractExpression:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherProjectionWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.Order:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Items), true

	case *cypher.SortItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		return nextCursor, true

	case *cypher.Projection:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		addCypherBranches(nextCursor, typedNode.Items)

		if typedNode.Order != nil {
			nextCursor.AddBranches(typedNode.Order)
		}

		if typedNode.Skip != nil {
			nextCursor.AddBranches(typedNode.Skip)
		}

		if typedNode.Limit != nil {
			nextCursor.AddBranches(typedNode.Limit)
		}

		return nextCursor, true

	case *cypher.ProjectionItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherQueryWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	if cursor, handled := newCypherStatementWalkCursor(node); handled {
		return cursor, true
	}
	if cursor, handled := newCypherClauseWalkCursor(node); handled {
		return cursor, true
	}

	return nil, false
}

func newCypherStatementWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.MultiPartQuery:
		nextCursor := newCypherWalkCursorWithBranches(typedNode, typedNode.Parts)
		if typedNode.SinglePartQuery != nil {
			nextCursor.AddBranches(typedNode.SinglePartQuery)
		}
		return nextCursor, true

	case *cypher.MultiPartQueryPart:
		return newCypherMultiPartQueryPartWalkCursor(typedNode), true

	case *cypher.RegularQuery:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.SingleQuery != nil {
			nextCursor.AddBranches(typedNode.SingleQuery)
		}
		return nextCursor, true

	case *cypher.SingleQuery:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.SinglePartQuery != nil {
			nextCursor.AddBranches(typedNode.SinglePartQuery)
		}
		if typedNode.MultiPartQuery != nil {
			nextCursor.AddBranches(typedNode.MultiPartQuery)
		}
		return nextCursor, true

	case *cypher.SinglePartQuery:
		return newCypherSinglePartQueryWalkCursor(typedNode), true

	default:
		return nil, false
	}
}

func newCypherMultiPartQueryPartWalkCursor(node *cypher.MultiPartQueryPart) *Cursor[cypher.SyntaxNode] {
	nextCursor := &Cursor[cypher.SyntaxNode]{
		Node: node,
	}

	if len(node.ReadingClauses) > 0 {
		addCypherBranches(nextCursor, node.ReadingClauses)
	}

	if len(node.UpdatingClauses) > 0 {
		addCypherBranches(nextCursor, node.UpdatingClauses)
	}

	if node.With != nil {
		nextCursor.AddBranches(node.With)
	}

	return nextCursor
}

func newCypherSinglePartQueryWalkCursor(node *cypher.SinglePartQuery) *Cursor[cypher.SyntaxNode] {
	nextCursor := &Cursor[cypher.SyntaxNode]{
		Node: node,
	}

	if len(node.ReadingClauses) > 0 {
		addCypherBranches(nextCursor, node.ReadingClauses)
	}

	if len(node.UpdatingClauses) > 0 {
		addCypherBranches(nextCursor, node.UpdatingClauses)
	}

	if node.Return != nil {
		nextCursor.AddBranches(node.Return)
	}

	return nextCursor
}

func newCypherClauseWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.Unwind:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Expression != nil {
			nextCursor.AddBranches(typedNode.Expression)
		}
		if typedNode.Variable != nil {
			nextCursor.AddBranches(typedNode.Variable)
		}
		return nextCursor, true

	case *cypher.With:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Projection != nil {
			nextCursor.AddBranches(typedNode.Projection)
		}

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		return nextCursor, true

	case *cypher.Return:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Projection != nil {
			nextCursor.AddBranches(typedNode.Projection)
		}
		return nextCursor, true

	case *cypher.ReadingClause:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Match != nil {
			nextCursor.AddBranches(typedNode.Match)
		}

		if typedNode.Unwind != nil {
			nextCursor.AddBranches(typedNode.Unwind)
		}

		return nextCursor, true

	case *cypher.Match:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		addCypherBranches(nextCursor, typedNode.Pattern)

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherUpdatingWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.Create:
		return newCypherWalkCursorWithBranches(node, typedNode.Pattern), true

	case *cypher.RemoveItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Property != nil {
			nextCursor.AddBranches(typedNode.Property)
		}

		return nextCursor, true

	case *cypher.Remove:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Items), true

	case *cypher.Delete:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Expressions), true

	case *cypher.SetItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Left != nil {
			nextCursor.AddBranches(typedNode.Left)
		}
		if typedNode.Right != nil {
			nextCursor.AddBranches(typedNode.Right)
		}
		return nextCursor, true

	case *cypher.Set:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Items), true

	case *cypher.UpdatingClause:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Clause != nil {
			nextCursor.AddBranches(typedNode.Clause)
		}
		return nextCursor, true

	case *cypher.Merge:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.PatternPart, typedNode.MergeActions), true

	case *cypher.MergeAction:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Set != nil {
			nextCursor.AddBranches(typedNode.Set)
		}
		return nextCursor, true

	default:
		return nil, false
	}
}

func newCypherPatternWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], bool) {
	switch typedNode := node.(type) {
	case *cypher.PatternPredicate:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.PatternElements), true

	case *cypher.PatternPart:
		return newCypherWalkCursorWithBranches(node, typedNode.PatternElements), true

	case *cypher.PatternElement:
		nextCursor := &Cursor[cypher.SyntaxNode]{Node: node}
		if typedNode.Element != nil {
			nextCursor.AddBranches(typedNode.Element)
		}
		return nextCursor, true

	case *cypher.RelationshipPattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Properties != nil {
			nextCursor.AddBranches(typedNode.Properties)
		}

		return nextCursor, true

	case *cypher.NodePattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Properties != nil {
			nextCursor.AddBranches(typedNode.Properties)
		}

		return nextCursor, true

	default:
		return nil, false
	}
}

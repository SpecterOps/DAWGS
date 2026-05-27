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

	cursor.AddBranches(prefix)
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
	switch typedNode := node.(type) {
	case *cypher.Limit:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Value},
		}, nil

	case *cypher.Skip:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Value},
		}, nil

	case *cypher.KindMatcher:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Reference},
		}
		if typedNode.Kinds != nil {
			nextCursor.AddBranches(typedNode.Kinds)
		}
		return nextCursor, nil

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
		return nextCursor, nil

	case *cypher.RemoveItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.KindMatcher, typedNode.Property},
		}, nil

	case *cypher.IDInCollection:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Variable, typedNode.Expression},
		}, nil

	case *cypher.ProjectionItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression, typedNode.Alias},
		}, nil

	case *cypher.PatternPart:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.Variable, typedNode.PatternElements), nil

	case *cypher.RelationshipPattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}
		nextCursor.AddBranches(typedNode.Variable)
		if typedNode.Kinds != nil {
			nextCursor.AddBranches(typedNode.Kinds)
		}
		nextCursor.AddBranches(typedNode.Range, typedNode.Properties)
		return nextCursor, nil

	case *cypher.NodePattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}
		nextCursor.AddBranches(typedNode.Variable)
		if typedNode.Kinds != nil {
			nextCursor.AddBranches(typedNode.Kinds)
		}
		nextCursor.AddBranches(typedNode.Properties)
		return nextCursor, nil

	case *cypher.PartialComparison:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator, typedNode.Right},
		}, nil

	case *cypher.UnaryAddOrSubtractExpression:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator, typedNode.Right},
		}, nil

	default:
		return newCypherWalkCursor(node)
	}
}

func newCypherWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], error) {
	switch typedNode := node.(type) {
	// Types with no AST branches
	case *cypher.RangeQuantifier, *cypher.PatternRange, cypher.Operator, *cypher.Limit, *cypher.Skip, graph.Kinds, *cypher.Parameter:
		return &Cursor[cypher.SyntaxNode]{
			Node: node,
		}, nil

	case *cypher.KindMatcher:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Reference},
		}, nil

	case *cypher.PropertyLookup:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Atom},
		}, nil

	case *cypher.MapItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Value},
		}, nil

	case *cypher.Properties:
		if typedNode.Parameter != nil {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: []cypher.SyntaxNode{typedNode.Parameter},
			}, nil
		} else {
			return newCypherWalkCursorWithMapItems(node, typedNode.Map), nil
		}

	case *cypher.Literal:
		return &Cursor[cypher.SyntaxNode]{
			Node: node,
		}, nil

	case cypher.MapLiteral:
		return newCypherWalkCursorWithMapItems(node, typedNode), nil

	case *cypher.ListLiteral:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Expressions()), nil

	case *cypher.Create:
		return newCypherWalkCursorWithBranches(node, typedNode.Pattern), nil

	case *cypher.Unwind:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression, typedNode.Variable},
		}, nil

	case *cypher.RemoveItem:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Property != nil {
			nextCursor.AddBranches(typedNode.Property)
		}

		return nextCursor, nil

	case *cypher.Remove:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Items), nil

	case *cypher.Delete:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Expressions), nil

	case *cypher.SetItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Left, typedNode.Right},
		}, nil

	case *cypher.Set:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Items), nil

	case *cypher.UpdatingClause:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Clause},
		}, nil

	case *cypher.PatternPredicate:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.PatternElements), nil

	case *cypher.Order:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Items), nil

	case *cypher.SortItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression},
		}, nil

	case *cypher.MultiPartQuery:
		nextCursor := newCypherWalkCursorWithBranches(typedNode, typedNode.Parts)
		nextCursor.AddBranches(typedNode.SinglePartQuery)
		return nextCursor, nil

	case *cypher.MultiPartQueryPart:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if len(typedNode.ReadingClauses) > 0 {
			addCypherBranches(nextCursor, typedNode.ReadingClauses)
		}

		if len(typedNode.UpdatingClauses) > 0 {
			addCypherBranches(nextCursor, typedNode.UpdatingClauses)
		}

		if typedNode.With != nil {
			nextCursor.AddBranches(typedNode.With)
		}

		return nextCursor, nil

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

		return nextCursor, nil

	case *cypher.Quantifier:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Filter},
		}, nil

	case *cypher.FilterExpression:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Specifier},
		}

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		return nextCursor, nil

	case *cypher.IDInCollection:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression},
		}, nil

	case *cypher.FunctionInvocation:
		return newCypherWalkCursorWithBranches(typedNode, typedNode.Arguments), nil

	case *cypher.Parenthetical:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression},
		}, nil

	case *cypher.RegularQuery:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.SingleQuery},
		}, nil

	case *cypher.SingleQuery:
		if typedNode.SinglePartQuery != nil {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: []cypher.SyntaxNode{typedNode.SinglePartQuery},
			}, nil
		}

		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.MultiPartQuery},
		}, nil

	case *cypher.SinglePartQuery:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if len(typedNode.ReadingClauses) > 0 {
			addCypherBranches(nextCursor, typedNode.ReadingClauses)
		}

		if len(typedNode.UpdatingClauses) > 0 {
			addCypherBranches(nextCursor, typedNode.UpdatingClauses)
		}

		if typedNode.Return != nil {
			nextCursor.AddBranches(typedNode.Return)
		}

		return nextCursor, nil

	case *cypher.Return:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Projection},
		}, nil

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

		return nextCursor, nil

	case *cypher.ProjectionItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression},
		}, nil

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

		return nextCursor, nil

	case *cypher.Match:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		addCypherBranches(nextCursor, typedNode.Pattern)

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		return nextCursor, nil

	case *cypher.PatternPart:
		return newCypherWalkCursorWithBranches(node, typedNode.PatternElements), nil

	case *cypher.PatternElement:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Element},
		}, nil

	case *cypher.RelationshipPattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Properties != nil {
			nextCursor.AddBranches(typedNode.Properties)
		}

		return nextCursor, nil

	case *cypher.NodePattern:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if typedNode.Properties != nil {
			nextCursor.AddBranches(typedNode.Properties)
		}

		return nextCursor, nil

	case *cypher.Where:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), nil

	case *cypher.Variable:
		return &Cursor[cypher.SyntaxNode]{
			Node: node,
		}, nil

	case *cypher.ArithmeticExpression:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.Left, typedNode.Partials), nil

	case *cypher.PartialArithmeticExpression:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Operator, typedNode.Right},
		}, nil

	case *cypher.PartialComparison:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Right},
		}, nil

	case *cypher.Negation:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression},
		}, nil

	case *cypher.Conjunction:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), nil

	case *cypher.Disjunction:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), nil

	case *cypher.ExclusiveDisjunction:
		return newCypherWalkCursorWithBranches(node, typedNode.Expressions), nil

	case *cypher.Comparison:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.Left, typedNode.Partials), nil

	case *cypher.Merge:
		return newCypherWalkCursorWithBranchPrefix(node, typedNode.PatternPart, typedNode.MergeActions), nil

	case *cypher.MergeAction:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Set},
		}, nil

	case *cypher.UnaryAddOrSubtractExpression:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Right},
		}, nil

	default:
		return nil, fmt.Errorf("unable to negotiate cypher model type %T into a translation cursor", node)
	}
}

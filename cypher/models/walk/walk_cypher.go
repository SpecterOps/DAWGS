package walk

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/graph"
)

func cypherSyntaxNodeSliceTypeConvert[F any, FS []F](fs FS) ([]cypher.SyntaxNode, error) {
	return ConvertSliceType[cypher.SyntaxNode](fs)
}

func newCypherWalkCursor(node cypher.SyntaxNode) (*Cursor[cypher.SyntaxNode], error) {
	switch typedNode := node.(type) {
	// Types with no AST branches
	case *cypher.RangeQuantifier, cypher.Operator, *cypher.Limit, *cypher.Skip, graph.Kinds, *cypher.Parameter:
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
		} else if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Map.Items()); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case *cypher.Literal:
		switch typedValue := typedNode.Value.(type) {
		case *cypher.ListLiteral:
			if branches, err := cypherSyntaxNodeSliceTypeConvert(typedValue.Expressions()); err != nil {
				return nil, err
			} else {
				return &Cursor[cypher.SyntaxNode]{
					Node:     typedValue,
					Branches: branches,
				}, nil
			}

		default:
			return &Cursor[cypher.SyntaxNode]{
				Node: node,
			}, nil
		}

	case *cypher.Create:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Pattern); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

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
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Items); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: branches,
			}, nil
		}

	case *cypher.Delete:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Expressions); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: branches,
			}, nil
		}

	case *cypher.SetItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Left, typedNode.Right},
		}, nil

	case *cypher.Set:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Items); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: branches,
			}, nil
		}

	case *cypher.UpdatingClause:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Clause},
		}, nil

	case *cypher.PatternPredicate:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.PatternElements); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: branches,
			}, nil
		}

	case *cypher.Order:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Items); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: branches,
			}, nil
		}

	case *cypher.SortItem:
		return &Cursor[cypher.SyntaxNode]{
			Node:     node,
			Branches: []cypher.SyntaxNode{typedNode.Expression},
		}, nil

	case *cypher.MultiPartQuery:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Parts); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: append(branches, typedNode.SinglePartQuery),
			}, nil
		}

	case *cypher.MultiPartQueryPart:
		nextCursor := &Cursor[cypher.SyntaxNode]{
			Node: node,
		}

		if len(typedNode.ReadingClauses) > 0 {
			if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.ReadingClauses); err != nil {
				return nil, err
			} else {
				nextCursor.AddBranches(branches...)
			}
		}

		if len(typedNode.UpdatingClauses) > 0 {
			if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.UpdatingClauses); err != nil {
				return nil, err
			} else {
				nextCursor.AddBranches(branches...)
			}
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
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Arguments); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     typedNode,
				Branches: branches,
			}, nil
		}

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
			if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.ReadingClauses); err != nil {
				return nil, err
			} else {
				nextCursor.AddBranches(branches...)
			}
		}

		if len(typedNode.UpdatingClauses) > 0 {
			if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.UpdatingClauses); err != nil {
				return nil, err
			} else {
				nextCursor.AddBranches(branches...)
			}
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

		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Items); err != nil {
			return nil, err
		} else {
			nextCursor.AddBranches(branches...)
		}

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

		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Pattern); err != nil {
			return nil, err
		} else {
			nextCursor.AddBranches(branches...)
		}

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		return nextCursor, nil

	case *cypher.PatternPart:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.PatternElements); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

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
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Expressions); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case *cypher.Variable:
		return &Cursor[cypher.SyntaxNode]{
			Node: node,
		}, nil

	case *cypher.ArithmeticExpression:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Partials); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: append([]cypher.SyntaxNode{typedNode.Left}, branches...),
			}, nil
		}

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
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Expressions); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case *cypher.Disjunction:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Expressions); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case *cypher.Comparison:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.Partials); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: append([]cypher.SyntaxNode{typedNode.Left}, branches...),
			}, nil
		}

	case *cypher.Merge:
		if branches, err := cypherSyntaxNodeSliceTypeConvert(typedNode.MergeActions); err != nil {
			return nil, err
		} else {
			return &Cursor[cypher.SyntaxNode]{
				Node:     node,
				Branches: append([]cypher.SyntaxNode{typedNode.PatternPart}, branches...),
			}, nil
		}

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

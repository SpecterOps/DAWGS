package walk

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func pgsqlSyntaxNodeSliceTypeConvert[F any, FS []F](fs FS) ([]pgsql.SyntaxNode, error) {
	return ConvertSliceType[pgsql.SyntaxNode](fs)
}

func newSQLWalkCursor(node pgsql.SyntaxNode) (*Cursor[pgsql.SyntaxNode], error) {
	switch typedNode := node.(type) {
	case pgsql.Query:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node: node,
		}

		if typedNode.CommonTableExpressions != nil {
			nextCursor.Branches = append(nextCursor.Branches, *typedNode.CommonTableExpressions)
		}

		nextCursor.Branches = append(nextCursor.Branches, typedNode.Body)
		return nextCursor, nil

	case pgsql.With:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Expressions); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case pgsql.CommonTableExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Query, typedNode.Alias},
		}, nil

	case pgsql.Projection:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: typedNode.AsSyntaxNodes(),
		}, nil

	case *pgsql.OrderBy:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.Select:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node: node,
		}

		nextCursor.AddBranches(typedNode.Projection)

		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.From); err != nil {
			return nil, err
		} else {
			nextCursor.AddBranches(branches...)
		}

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		if typedNode.Having != nil {
			nextCursor.AddBranches(typedNode.Having)
		}

		return nextCursor, nil

	case pgsql.FromClause:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Source},
		}

		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Joins); err != nil {
			return nil, err
		} else {
			nextCursor.AddBranches(branches...)
		}

		return nextCursor, nil

	case *pgsql.AliasedExpression:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}

		if typedNode.Alias.Set {
			nextCursor.AddBranches(typedNode.Alias.Value)
		}

		return nextCursor, nil

	case pgsql.SetOperation:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.LOperand, typedNode.ROperand},
		}, nil

	case pgsql.AliasedExpression:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}

		if typedNode.Alias.Set {
			nextCursor.AddBranches(typedNode.Alias.Value)
		}

		return nextCursor, nil

	case pgsql.CompositeValue:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Values); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case pgsql.TableReference:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Name},
		}

		if typedNode.Alias.Set {
			nextCursor.AddBranches(typedNode.Alias.Value)
		}

		return nextCursor, nil

	case pgsql.TableAlias:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Name},
		}

		if typedNode.Shape != nil {
			nextCursor.AddBranches(typedNode.Shape)
		}

		return nextCursor, nil

	case pgsql.RecordShape:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Columns); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case pgsql.TypeCast:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case *pgsql.Parenthetical:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.FunctionCall:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Parameters); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case *pgsql.FunctionCall:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Parameters); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case pgsql.Variadic:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.CompoundIdentifier, pgsql.Operator, pgsql.Literal, pgsql.Identifier, pgsql.Parameter, *pgsql.Parameter:
		return &Cursor[pgsql.SyntaxNode]{
			Node: node,
		}, nil

	case pgsql.ArrayLiteral:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Values); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: branches,
			}, nil
		}

	case pgsql.ArrayExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case *pgsql.AnyExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.UnaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Operand},
		}, nil

	case *pgsql.UnaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Operand},
		}, nil

	case pgsql.BinaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.LOperand, typedNode.ROperand},
		}, nil

	case *pgsql.BinaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.LOperand, typedNode.ROperand},
		}, nil

	case pgsql.Join:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.JoinOperator},
		}, nil

	case pgsql.JoinOperator:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Constraint},
		}, nil

	case pgsql.RowColumnReference:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Identifier, typedNode.Column},
		}, nil

	case pgsql.ArrayIndex:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Indexes); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: append([]pgsql.SyntaxNode{typedNode.Expression}, branches...),
			}, nil
		}

	case *pgsql.ArrayIndex:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.Indexes); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: append([]pgsql.SyntaxNode{typedNode.Expression}, branches...),
			}, nil
		}

	case pgsql.ExistsExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Subquery},
		}, nil

	case pgsql.ProjectionFrom:
		if branches, err := pgsqlSyntaxNodeSliceTypeConvert(typedNode.From); err != nil {
			return nil, err
		} else {
			return &Cursor[pgsql.SyntaxNode]{
				Node:     node,
				Branches: append([]pgsql.SyntaxNode{typedNode.Projection}, branches...),
			}, nil
		}

	case pgsql.Subquery:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Query},
		}, nil

	case pgsql.SyntaxNodeFuture:
		cursor := &Cursor[pgsql.SyntaxNode]{
			Node: typedNode,
		}

		if unwrapped := typedNode.Unwrap(); unwrapped != nil {
			cursor.Branches = append(cursor.Branches, unwrapped)
		}

		return cursor, nil

	default:
		return nil, fmt.Errorf("unable to negotiate sql type %T into a translation cursor", node)
	}
}

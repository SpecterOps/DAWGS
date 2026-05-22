package optimize

import (
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// FlattenConjunction collects the leaf operands of a left-recursive AND chain.
func FlattenConjunction(expr pgsql.Expression) []pgsql.Expression {
	if bin, typeOK := expr.(*pgsql.BinaryExpression); !typeOK || bin.Operator != pgsql.OperatorAnd {
		return []pgsql.Expression{expr}
	} else {
		return append(FlattenConjunction(bin.LOperand), FlattenConjunction(bin.ROperand)...)
	}
}

// ExpressionReferencesOnlyLocalIdentifiers returns true only when every binding
// reference found in the expression is a member of localScope.
func ExpressionReferencesOnlyLocalIdentifiers(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	isLocal := true

	walk.PgSQL(expression, walk.NewSimpleVisitor[pgsql.SyntaxNode](
		func(node pgsql.SyntaxNode, handler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case pgsql.ExistsExpression:
				if !SubqueryReferencesOnlyLocalIdentifiers(typedNode.Subquery, localScope) {
					isLocal = false
					handler.SetDone()
				} else {
					handler.Consume()
				}

			case pgsql.CompoundIdentifier:
				if len(typedNode) > 0 && !localScope.Contains(typedNode[0]) {
					isLocal = false
					handler.SetDone()
				}

			case pgsql.Identifier:
				if !localScope.Contains(typedNode) {
					isLocal = false
					handler.SetDone()
				}

			case pgsql.RowColumnReference:
				if !ExpressionReferencesOnlyLocalIdentifiers(typedNode.Identifier, localScope) {
					isLocal = false
					handler.SetDone()
				} else {
					handler.Consume()
				}
			}
		},
	))

	return isLocal
}

func SubqueryReferencesOnlyLocalIdentifiers(subquery pgsql.Subquery, localScope *pgsql.IdentifierSet) bool {
	return QueryReferencesOnlyLocalIdentifiers(subquery.Query, localScope)
}

func QueryReferencesOnlyLocalIdentifiers(query pgsql.Query, localScope *pgsql.IdentifierSet) bool {
	if query.CommonTableExpressions != nil {
		return false
	}

	selectBody, isSelect := query.Body.(pgsql.Select)
	if !isSelect {
		return false
	}

	if !SelectReferencesOnlyLocalIdentifiers(selectBody, localScope) {
		return false
	}

	for _, orderBy := range query.OrderBy {
		if orderBy != nil && !ExpressionReferencesOnlyLocalIdentifiers(orderBy.Expression, localScope) {
			return false
		}
	}

	return (query.Offset == nil || ExpressionReferencesOnlyLocalIdentifiers(query.Offset, localScope)) &&
		(query.Limit == nil || ExpressionReferencesOnlyLocalIdentifiers(query.Limit, localScope))
}

func AddFromClauseBindings(localScope *pgsql.IdentifierSet, fromClauses []pgsql.FromClause) {
	for _, fromClause := range fromClauses {
		AddFromExpressionBinding(localScope, fromClause.Source)

		for _, join := range fromClause.Joins {
			AddFromExpressionBinding(localScope, join.Table)
		}
	}
}

func AddFromExpressionBinding(localScope *pgsql.IdentifierSet, expression pgsql.Expression) {
	switch typedExpression := expression.(type) {
	case pgsql.TableReference:
		if typedExpression.Binding.Set {
			localScope.Add(typedExpression.Binding.Value)
		}

	case pgsql.LateralSubquery:
		if typedExpression.Binding.Set {
			localScope.Add(typedExpression.Binding.Value)
		}
	}
}

func addFromClauseSourceBinding(localScope *pgsql.IdentifierSet, fromClause pgsql.FromClause) {
	AddFromExpressionBinding(localScope, fromClause.Source)
}

func SelectReferencesOnlyLocalIdentifiers(selectBody pgsql.Select, localScope *pgsql.IdentifierSet) bool {
	scopedIdentifiers := localScope.Copy()

	for _, fromClause := range selectBody.From {
		if !FromExpressionReferencesOnlyLocalIdentifiers(fromClause.Source) {
			return false
		}

		addFromClauseSourceBinding(scopedIdentifiers, fromClause)

		for _, join := range fromClause.Joins {
			if !FromExpressionReferencesOnlyLocalIdentifiers(join.Table) {
				return false
			}

			if join.JoinOperator.Constraint != nil &&
				!ExpressionReferencesOnlyLocalIdentifiers(join.JoinOperator.Constraint, scopedIdentifiers) {
				return false
			}

			AddFromExpressionBinding(scopedIdentifiers, join.Table)
		}
	}

	for _, projection := range selectBody.Projection {
		if !ExpressionReferencesOnlyLocalIdentifiers(projection, scopedIdentifiers) {
			return false
		}
	}

	for _, groupByExpression := range selectBody.GroupBy {
		if !ExpressionReferencesOnlyLocalIdentifiers(groupByExpression, scopedIdentifiers) {
			return false
		}
	}

	return (selectBody.Where == nil || ExpressionReferencesOnlyLocalIdentifiers(selectBody.Where, scopedIdentifiers)) &&
		(selectBody.Having == nil || ExpressionReferencesOnlyLocalIdentifiers(selectBody.Having, scopedIdentifiers))
}

func FromExpressionReferencesOnlyLocalIdentifiers(expression pgsql.Expression) bool {
	switch expression.(type) {
	case pgsql.TableReference:
		return true

	default:
		return false
	}
}

func IsLocalToScope(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	if expression == nil {
		return true
	}

	return ExpressionReferencesOnlyLocalIdentifiers(expression, localScope)
}

// PartitionConstraintByLocality splits a conjunction (A AND B AND ...) into
// two expressions: one whose every binding reference is contained in
// localScope (safe for JOIN ON), and one that references outside identifiers
// (must stay in WHERE).
//
// Only top-level AND operands are split. If an expression is not a
// BinaryExpression with OperatorAnd, the whole expression is tested as a unit.
func PartitionConstraintByLocality(expression pgsql.Expression, localScope *pgsql.IdentifierSet) (pgsql.Expression, pgsql.Expression) {
	var (
		joinConstraints  pgsql.Expression
		whereConstraints pgsql.Expression
		terms            = FlattenConjunction(expression)
	)

	for _, term := range terms {
		if IsLocalToScope(term, localScope) {
			joinConstraints = pgsql.OptionalAnd(joinConstraints, term)
		} else {
			whereConstraints = pgsql.OptionalAnd(whereConstraints, term)
		}
	}

	return joinConstraints, whereConstraints
}

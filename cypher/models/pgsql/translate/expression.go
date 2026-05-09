package translate

import (
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

func unwrapParenthetical(parenthetical pgsql.Expression) pgsql.Expression {
	next := parenthetical

	for next != nil {
		switch typedNext := next.(type) {
		case *pgsql.Parenthetical:
			next = typedNext.Expression

		default:
			return next
		}
	}

	return parenthetical
}

func (s *Translator) translatePropertyLookup(lookup *cypher.PropertyLookup) error {
	if translatedAtom, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else {
		switch typedTranslatedAtom := translatedAtom.(type) {
		case pgsql.Identifier:
			if fieldIdentifierLiteral, err := pgsql.AsLiteral(lookup.Symbol); err != nil {
				return err
			} else {
				s.treeTranslator.PushOperand(pgsql.CompoundIdentifier{typedTranslatedAtom, pgsql.ColumnProperties})
				s.treeTranslator.PushOperand(fieldIdentifierLiteral)

				if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorPropertyLookup); err != nil {
					return err
				}
			}

		case pgsql.FunctionCall:
			if fieldIdentifierLiteral, err := pgsql.AsLiteral(lookup.Symbol); err != nil {
				return err
			} else if componentName, typeOK := fieldIdentifierLiteral.Value.(string); !typeOK {
				return fmt.Errorf("expected a string component name in translated literal but received type: %T", fieldIdentifierLiteral.Value)
			} else {
				switch typedTranslatedAtom.Function {
				case pgsql.FunctionCurrentDate, pgsql.FunctionLocalTime, pgsql.FunctionCurrentTime, pgsql.FunctionLocalTimestamp, pgsql.FunctionNow:
					switch componentName {
					case cypher.ITTCEpochSeconds:
						s.treeTranslator.PushOperand(pgsql.FunctionCall{
							Function: pgsql.FunctionExtract,
							Parameters: []pgsql.Expression{pgsql.ProjectionFrom{
								Projection: []pgsql.SelectItem{
									pgsql.EpochIdentifier,
								},
								From: []pgsql.FromClause{{
									Source: translatedAtom,
								}},
							}},
							CastType: pgsql.Numeric,
						})

					case cypher.ITTCEpochMilliseconds:
						s.treeTranslator.PushOperand(pgsql.NewBinaryExpression(
							pgsql.FunctionCall{
								Function: pgsql.FunctionExtract,
								Parameters: []pgsql.Expression{pgsql.ProjectionFrom{
									Projection: []pgsql.SelectItem{
										pgsql.EpochIdentifier,
									},
									From: []pgsql.FromClause{{
										Source: translatedAtom,
									}},
								}},
								CastType: pgsql.Numeric,
							},
							pgsql.OperatorMultiply,
							pgsql.NewLiteral(1000, pgsql.Int4),
						))

					default:
						return fmt.Errorf("unsupported date time instant type component %s from function call %s", componentName, typedTranslatedAtom.Function)
					}

				default:
					return fmt.Errorf("unsupported instant type component %s from function call %s", componentName, typedTranslatedAtom.Function)
				}
			}
		}
	}

	return nil
}

func translateCypherAssignmentOperator(operator cypher.AssignmentOperator) (pgsql.Operator, error) {
	switch operator {
	case cypher.OperatorAssignment:
		return pgsql.OperatorAssignment, nil
	case cypher.OperatorLabelAssignment:
		return pgsql.OperatorKindAssignment, nil
	default:
		return pgsql.UnsetOperator, fmt.Errorf("unsupported assignment operator %s", operator)
	}
}

func ExtractSyntaxNodeReferences(root pgsql.SyntaxNode) (*pgsql.IdentifierSet, error) {
	dependencies := pgsql.NewIdentifierSet()

	return dependencies, walk.PgSQL(root, walk.NewSimpleVisitor[pgsql.SyntaxNode](
		func(node pgsql.SyntaxNode, errorHandler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case pgsql.Identifier:
				// Filter for reserved identifiers
				if !pgsql.IsReservedIdentifier(typedNode) {
					dependencies.Add(typedNode)
				}

			case pgsql.CompoundIdentifier:
				identifier := typedNode.Root()

				if !pgsql.IsReservedIdentifier(identifier) {
					dependencies.Add(identifier)
				}
			}
		},
	))
}

func rewriteStringWildCardLiteral(expression pgsql.Expression) (pgsql.Expression, error) {
	switch typedExpression := expression.(type) {
	case pgsql.Literal:
		if strValue, typeOK := typedExpression.Value.(string); !typeOK {
			return nil, fmt.Errorf("expected a string literal but received type: %T", typedExpression.Value)
		} else {
			rewritten := strings.NewReplacer(
				"\\", "\\\\",
				"%", "\\%",
				"_", "\\_",
			).Replace(strValue)
			return pgsql.NewLiteral(rewritten, pgsql.Text), nil
		}

	default:
		return expression, nil
	}
}

func rewritePropertyLookupOperator(propertyLookup *pgsql.BinaryExpression, dataType pgsql.DataType) pgsql.Expression {
	if dataType.IsArrayType() {
		// Ensure that array conversions use JSONB
		propertyLookup.Operator = pgsql.OperatorJSONField

		return pgsql.FunctionCall{
			Function:   pgsql.FunctionJSONBToTextArray,
			Parameters: []pgsql.Expression{propertyLookup},
			CastType:   dataType,
		}
	}

	switch dataType {
	case pgsql.Text:
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return propertyLookup

	case pgsql.Date, pgsql.TimestampWithoutTimeZone, pgsql.TimestampWithTimeZone, pgsql.TimeWithoutTimeZone, pgsql.TimeWithTimeZone:
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return pgsql.NewTypeCast(propertyLookup, dataType)

	case pgsql.UnknownDataType:
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return propertyLookup

	default:
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return pgsql.NewTypeCast(propertyLookup, dataType)
	}
}

func isJSONScalarEqualityType(dataType pgsql.DataType) bool {
	switch dataType {
	case pgsql.Boolean, pgsql.Float4, pgsql.Float8, pgsql.Int, pgsql.Int2, pgsql.Int4, pgsql.Int8, pgsql.Numeric, pgsql.Text:
		return true

	default:
		return false
	}
}

func rewriteJSONScalarEqualityOperand(expression pgsql.Expression) (pgsql.Expression, bool) {
	if literal, isLiteral := expression.(pgsql.Literal); isLiteral && literal.Null {
		return nil, false
	}

	if typedExpression, isTypeHinted := expression.(pgsql.TypeHinted); !isTypeHinted {
		return nil, false
	} else if dataType := typedExpression.TypeHint(); !isJSONScalarEqualityType(dataType) {
		return nil, false
	} else {
		return pgsql.FunctionCall{
			Function: pgsql.FunctionToJSONB,
			Parameters: []pgsql.Expression{
				pgsql.NewTypeCast(expression, dataType),
			},
			CastType: pgsql.JSONB,
		}, true
	}
}

func lookupRequiresElementType(typeHint pgsql.DataType, operator pgsql.Operator, otherOperand pgsql.SyntaxNode) bool {
	if typeHint.IsArrayType() {
		switch operator {
		case pgsql.OperatorIn:
			return true
		}

		switch otherOperand.(type) {
		case pgsql.AnyExpression:
			return true
		}
	}

	return false
}

func TypeCastExpression(expression pgsql.Expression, dataType pgsql.DataType) (pgsql.Expression, error) {
	if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(expression); isPropertyLookup {
		var lookupTypeHint = dataType

		if lookupRequiresElementType(dataType, propertyLookup.Operator, propertyLookup.ROperand) {
			// Take the base type of the array type hint: <unit> in <collection>
			lookupTypeHint = dataType.ArrayBaseType()
		}

		return rewritePropertyLookupOperator(propertyLookup, lookupTypeHint), nil
	}

	return pgsql.NewTypeCast(expression, dataType), nil
}

func rewritePropertyLookupOperands(expression *pgsql.BinaryExpression) error {
	var (
		leftPropertyLookup, hasLeftPropertyLookup   = expressionToPropertyLookupBinaryExpression(expression.LOperand)
		rightPropertyLookup, hasRightPropertyLookup = expressionToPropertyLookupBinaryExpression(expression.ROperand)
	)

	// Ensure that direct property comparisons prefer JSONB - JSONB. Non-comparison operators must keep their
	// property lookups text-oriented until rewriteBinaryExpression has enough context to disambiguate them.
	if hasLeftPropertyLookup && hasRightPropertyLookup &&
		(pgsql.OperatorIsComparator(expression.Operator) || expression.Operator == pgsql.OperatorCypherNotEquals) {
		leftPropertyLookup.Operator = pgsql.OperatorJSONField
		rightPropertyLookup.Operator = pgsql.OperatorJSONField

		return nil
	}

	if hasLeftPropertyLookup {
		// This check exists here to prevent from overwriting a property lookup that's part of a <value> in <list>
		// binary expression. This may want for better ergonomics in the future
		if anyExpression, isAnyExpression := expression.ROperand.(*pgsql.AnyExpression); isAnyExpression {
			expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, anyExpression.CastType.ArrayBaseType())
		} else if rOperandTypeHint, err := InferExpressionType(expression.ROperand); err != nil {
			return err
		} else {
			switch expression.Operator {
			case pgsql.OperatorIn:
				expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, rOperandTypeHint.ArrayBaseType())

			case pgsql.OperatorCypherStartsWith, pgsql.OperatorCypherEndsWith, pgsql.OperatorCypherContains, pgsql.OperatorRegexMatch:
				expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, pgsql.Text)

				// If the right operand is a literal, it may contain characters that have special meaning in PgSQL
				// but do not in Cypher. These characters must be escaped
				if rewrittenROperand, err := rewriteStringWildCardLiteral(expression.ROperand); err != nil {
					return err
				} else {
					expression.ROperand = rewrittenROperand
				}

			case pgsql.OperatorEquals, pgsql.OperatorCypherNotEquals:
				if rewrittenROperand, rewritten := rewriteJSONScalarEqualityOperand(expression.ROperand); rewritten {
					leftPropertyLookup.Operator = pgsql.OperatorJSONField
					expression.ROperand = rewrittenROperand
				} else if rOperandTypeHint == pgsql.AnyArray {
					break
				} else {
					expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, rOperandTypeHint)
				}

			default:
				expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, rOperandTypeHint)
			}
		}
	}

	if hasRightPropertyLookup {
		if lOperandTypeHint, err := InferExpressionType(expression.LOperand); err != nil {
			return err
		} else {
			switch expression.Operator {
			case pgsql.OperatorIn:
				if arrayType, err := lOperandTypeHint.ToArrayType(); err != nil {
					return err
				} else {
					expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, arrayType)
				}

			case pgsql.OperatorCypherStartsWith, pgsql.OperatorCypherEndsWith, pgsql.OperatorCypherContains, pgsql.OperatorRegexMatch:
				expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, pgsql.Text)

				// If the left operand is a literal, unlike the right operand case there is no need to rewrite
				// for special (like, ilike, etc.) character classes

			case pgsql.OperatorEquals, pgsql.OperatorCypherNotEquals:
				if rewrittenLOperand, rewritten := rewriteJSONScalarEqualityOperand(expression.LOperand); rewritten {
					expression.LOperand = rewrittenLOperand
					rightPropertyLookup.Operator = pgsql.OperatorJSONField
				} else if lOperandTypeHint == pgsql.AnyArray {
					break
				} else {
					expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, lOperandTypeHint)
				}

			default:
				expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, lOperandTypeHint)
			}
		}
	}

	return nil
}

func newFunctionCallComparatorError(functionCall pgsql.FunctionCall, operator pgsql.Operator, comparisonType pgsql.DataType) error {
	switch functionCall.Function {
	case pgsql.FunctionCoalesce:
		// This is a specific error statement for coalesce statements. These statements have ill-defined
		// type conversion semantics in Cypher. As such, exposing the type specificity of coalesce to the
		// user as a distinct error will help reduce the surprise of running on a non-Neo4j substrate.
		return fmt.Errorf("coalesce has type %s but is being compared against type %s - ensure that all arguments in the coalesce function match the type of the other side of the comparison", functionCall.CastType, comparisonType)
	}

	return nil
}

type Builder struct {
	stack []pgsql.Expression
}

func NewExpressionTreeBuilder() *Builder {
	return &Builder{}
}

func (s *Builder) Depth() int {
	return len(s.stack)
}

func (s *Builder) IsEmpty() bool {
	return len(s.stack) == 0
}

func (s *Builder) PopOperand(kindMapper *contextAwareKindMapper) (pgsql.Expression, error) {
	next := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]

	switch typedNext := next.(type) {
	case *pgsql.UnaryExpression:
		if err := applyUnaryExpressionTypeHints(typedNext); err != nil {
			return nil, err
		}

	case *pgsql.BinaryExpression:
		if err := applyBinaryExpressionTypeHints(kindMapper, typedNext); err != nil {
			return nil, err
		}
	}

	return next, nil
}

func (s *Builder) PeekOperand() pgsql.Expression {
	return s.stack[len(s.stack)-1]
}

func (s *Builder) PushOperand(operand pgsql.Expression) {
	s.stack = append(s.stack, operand)
}

func ConjoinExpressions(kindMapper *contextAwareKindMapper, expressions []pgsql.Expression) (pgsql.Expression, error) {
	var conjoined pgsql.Expression

	for _, expression := range expressions {
		if expression == nil {
			continue
		}

		if conjoined == nil {
			conjoined = expression
			continue
		}

		conjoinedBinaryExpression := pgsql.NewBinaryExpression(conjoined, pgsql.OperatorAnd, expression)

		if err := applyBinaryExpressionTypeHints(kindMapper, conjoinedBinaryExpression); err != nil {
			return nil, err
		}

		conjoined = conjoinedBinaryExpression
	}

	return conjoined, nil
}

type ExpressionTreeTranslator struct {
	UserConstraints        *ConstraintTracker
	TranslationConstraints *ConstraintTracker

	treeBuilder        *Builder
	kindMapper         *contextAwareKindMapper
	parentheticalDepth int
	disjunctionDepth   int
	conjunctionDepth   int
}

func NewExpressionTreeTranslator(kindMapper *contextAwareKindMapper) *ExpressionTreeTranslator {
	return &ExpressionTreeTranslator{
		UserConstraints:        NewConstraintTracker(),
		TranslationConstraints: NewConstraintTracker(),
		treeBuilder:            NewExpressionTreeBuilder(),
		kindMapper:             kindMapper,
	}
}

func mergeUserAndTranslationConstraints(userConstraints, translationConstraints *Constraint) *Constraint {
	if userConstraints.Expression != nil {
		// Fold the user constraints into the translation constraints wrapped in a parenthetical
		translationConstraints.Expression = pgsql.OptionalAnd(pgsql.NewParenthetical(userConstraints.Expression), translationConstraints.Expression)

		// Ensure that the identifier dependencies in the constraint are merged as well
		translationConstraints.Dependencies.MergeSet(userConstraints.Dependencies)
	}

	return translationConstraints
}

func (s *ExpressionTreeTranslator) HasAnyConstraints(scope *pgsql.IdentifierSet) (bool, error) {
	if hasUser, err := s.UserConstraints.HasConstraints(scope); err != nil || hasUser {
		return hasUser, err
	}
	return s.TranslationConstraints.HasConstraints(scope)
}

func (s *ExpressionTreeTranslator) ConsumeConstraintsFromVisibleSet(visible *pgsql.IdentifierSet) (*Constraint, error) {
	if userConstraints, err := s.UserConstraints.ConsumeSet(s.kindMapper, visible); err != nil {
		return nil, err
	} else if translationConstraints, err := s.TranslationConstraints.ConsumeSet(s.kindMapper, visible); err != nil {
		return nil, err
	} else {
		return mergeUserAndTranslationConstraints(userConstraints, translationConstraints), nil
	}
}

func (s *ExpressionTreeTranslator) ConsumeAllConstraints() (*Constraint, error) {
	if userConstraints, err := s.UserConstraints.ConsumeAll(s.kindMapper); err != nil {
		return nil, err
	} else if translationConstraints, err := s.TranslationConstraints.ConsumeAll(s.kindMapper); err != nil {
		return nil, err
	} else {
		return mergeUserAndTranslationConstraints(userConstraints, translationConstraints), nil
	}
}

func (s *ExpressionTreeTranslator) AddTranslationConstraint(requiredIdentifiers *pgsql.IdentifierSet, expression pgsql.Expression) error {
	return s.TranslationConstraints.Constrain(s.kindMapper, requiredIdentifiers, expression)
}

func (s *ExpressionTreeTranslator) AddUserConstraint(requiredIdentifiers *pgsql.IdentifierSet, expression pgsql.Expression) error {
	return s.UserConstraints.Constrain(s.kindMapper, requiredIdentifiers, expression)
}

func (s *ExpressionTreeTranslator) PushOperand(expression pgsql.Expression) {
	s.treeBuilder.PushOperand(expression)
}

func (s *ExpressionTreeTranslator) PeekOperand() pgsql.Expression {
	return s.treeBuilder.PeekOperand()
}

func (s *ExpressionTreeTranslator) PopOperand() (pgsql.Expression, error) {
	return s.treeBuilder.PopOperand(s.kindMapper)
}

func (s *ExpressionTreeTranslator) popOperandAsUserConstraint() error {
	if nextExpression, err := s.PopOperand(); err != nil {
		return err
	} else if identifierDeps, err := ExtractSyntaxNodeReferences(nextExpression); err != nil {
		return err
	} else {
		if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(nextExpression); isPropertyLookup {
			// If this is a bare property lookup rewrite it with the intended type of boolean
			nextExpression = rewritePropertyLookupOperator(propertyLookup, pgsql.Boolean)
		}

		return s.AddUserConstraint(identifierDeps, nextExpression)
	}
}

func (s *ExpressionTreeTranslator) PopRemainingExpressionsAsUserConstraints() error {
	// Pull the right operand only if one exists
	for !s.treeBuilder.IsEmpty() {
		if err := s.popOperandAsUserConstraint(); err != nil {
			return err
		}
	}

	return nil
}

func (s *ExpressionTreeTranslator) ConstrainDisjointOperandPair() error {
	// Always expect a left operand
	if s.treeBuilder.IsEmpty() {
		return fmt.Errorf("expected at least one operand for constraint extraction")
	}

	if rightOperand, err := s.treeBuilder.PopOperand(s.kindMapper); err != nil {
		return err
	} else if rightDependencies, err := ExtractSyntaxNodeReferences(rightOperand); err != nil {
		return err
	} else if s.treeBuilder.IsEmpty() {
		// If the tree builder is empty then this operand is at the top of the disjunction chain
		return s.AddUserConstraint(rightDependencies, rightOperand)
	} else if leftOperand, err := s.treeBuilder.PopOperand(s.kindMapper); err != nil {
		return err
	} else {
		newOrExpression := pgsql.NewBinaryExpression(
			leftOperand,
			pgsql.OperatorOr,
			rightOperand,
		)

		if err := applyBinaryExpressionTypeHints(s.kindMapper, newOrExpression); err != nil {
			return err
		}

		// This operation may not be complete; push it back on the stack
		s.PushOperand(newOrExpression)
		return nil
	}
}

func (s *ExpressionTreeTranslator) ConstrainConjoinedOperandPair() error {
	// Always expect a left operand
	if s.treeBuilder.IsEmpty() {
		return fmt.Errorf("expected at least one operand for constraint extraction")
	}

	if err := s.popOperandAsUserConstraint(); err != nil {
		return err
	}

	return nil
}

func (s *ExpressionTreeTranslator) PopBinaryExpression(operator pgsql.Operator) (*pgsql.BinaryExpression, error) {
	if rightOperand, err := s.PopOperand(); err != nil {
		return nil, err
	} else if leftOperand, err := s.PopOperand(); err != nil {
		return nil, err
	} else {
		newBinaryExpression := pgsql.NewBinaryExpression(leftOperand, operator, rightOperand)
		return newBinaryExpression, applyBinaryExpressionTypeHints(s.kindMapper, newBinaryExpression)
	}
}

func rewriteIdentityOperands(scope *Scope, newExpression *pgsql.BinaryExpression) error {
	switch typedLOperand := newExpression.LOperand.(type) {
	case pgsql.Identifier:
		// If the left side is an identifier we need to inspect the type of the identifier bound in our scope
		if boundLOperand, bound := scope.Lookup(typedLOperand); !bound {
			return fmt.Errorf("unknown identifier %s", typedLOperand)
		} else {
			switch typedROperand := newExpression.ROperand.(type) {
			case pgsql.Identifier:
				// If the right side is an identifier, inspect to see if the identifiers are an entity comparison.
				// For example: match (n1)-[]->(n2) where n1 <> n2 return n2
				if boundROperand, bound := scope.Lookup(typedROperand); !bound {
					return fmt.Errorf("unknown identifier %s", typedROperand)
				} else {
					switch boundLOperand.DataType {
					case pgsql.NodeCompositeArray:
						return fmt.Errorf("unsupported pgsql.NodeCompositeArray")

					case pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
						switch boundROperand.DataType {
						case pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
							// If this is a node entity comparison of some kind then the AST must be rewritten to use identity properties
							newExpression.LOperand = pgsql.CompoundIdentifier{typedLOperand, pgsql.ColumnID}
							newExpression.ROperand = pgsql.CompoundIdentifier{typedROperand, pgsql.ColumnID}

						case pgsql.NodeCompositeArray:
							const unnestElemAlias pgsql.Identifier = "_unnest_elem"
							newExpression.LOperand = pgsql.CompoundIdentifier{typedLOperand, pgsql.ColumnID}
							newExpression.ROperand = pgsql.Subquery{
								Query: pgsql.Query{
									Body: pgsql.Select{
										Projection: pgsql.Projection{
											pgsql.RowColumnReference{
												Identifier: unnestElemAlias,
												Column:     pgsql.ColumnID,
											},
										},
										From: []pgsql.FromClause{{
											Source: pgsql.AliasedExpression{
												Expression: pgsql.FunctionCall{
													Function:   pgsql.FunctionUnnest,
													Parameters: []pgsql.Expression{typedROperand},
												},
												Alias: pgsql.AsOptionalIdentifier(unnestElemAlias),
											},
										}},
									},
								},
							}

						default:
							return fmt.Errorf("invalid comparison between types %s and %s", boundLOperand.DataType, boundROperand.DataType)
						}

					case pgsql.EdgeCompositeArray:
						return fmt.Errorf("unsupported pgsql.EdgeCompositeArray")

					case pgsql.EdgeComposite, pgsql.ExpansionEdge:
						switch boundROperand.DataType {
						case pgsql.EdgeComposite, pgsql.ExpansionEdge:
							// If this is an edge entity comparison of some kind then the AST must be rewritten to use identity properties
							newExpression.LOperand = pgsql.CompoundIdentifier{typedLOperand, pgsql.ColumnID}
							newExpression.ROperand = pgsql.CompoundIdentifier{typedROperand, pgsql.ColumnID}

						case pgsql.EdgeCompositeArray:
							newExpression.LOperand = pgsql.CompoundIdentifier{typedLOperand, pgsql.ColumnID}
							newExpression.ROperand = pgsql.CompoundIdentifier{typedROperand, pgsql.ColumnID}

						default:
							return fmt.Errorf("invalid comparison between types %s and %s", boundLOperand.DataType, boundROperand.DataType)
						}

					case pgsql.PathComposite:
						return fmt.Errorf("comparison for path identifiers is unsupported")
					}
				}
			}
		}
	}

	return nil
}

func isPropertyLookup(expression pgsql.Expression) bool {
	_, isPropertyLookup := expressionToPropertyLookupBinaryExpression(expression)
	return isPropertyLookup
}

// isConcatenationOperation accepts two expressions and their inferred pgsql.DataType values and attempts to determine
// if the values are able to be concatenated.
//
// For further information regarding the conditional logic, please see the PgSQL upstream documentation:
// https://www.postgresql.org/docs/9.1/functions-string.html
func isConcatenationOperation(lOperand, rOperand pgsql.Expression, lOperandType, rOperandType pgsql.DataType) bool {
	// Any use of an array type automatically assumes concatenation
	if lOperandType.IsArrayType() || rOperandType.IsArrayType() {
		return true
	}

	// The case below must be able to infer operator intent from the following cases:
	// text + unknown
	// unknown + text
	// text + text

	if lOperandType == pgsql.Text || rOperandType == pgsql.Text {
		return true
	}

	// Dynamic properties have no schema-level type information. When both operands are dynamic property lookups,
	// prefer Cypher's string concatenation form instead of emitting invalid jsonb + jsonb SQL.
	if lOperandType == pgsql.UnknownDataType && rOperandType == pgsql.UnknownDataType {
		return isPropertyLookup(lOperand) && isPropertyLookup(rOperand)
	}

	return false
}

func isEmptyArrayLiteralPropertyComparison(expression *pgsql.BinaryExpression) (*pgsql.BinaryExpression, bool) {
	var (
		hasPropertyLookup    bool
		propertyLookup       *pgsql.BinaryExpression
		hasEmptyArrayLiteral bool
	)

	if leftPropertyLookup, hasLeftPropertyLookup := expressionToPropertyLookupBinaryExpression(expression.LOperand); hasLeftPropertyLookup {
		hasPropertyLookup = true
		propertyLookup = leftPropertyLookup
	} else if rightPropertyLookup, hasRightPropertyLookup := expressionToPropertyLookupBinaryExpression(expression.ROperand); hasRightPropertyLookup {
		hasPropertyLookup = true
		propertyLookup = rightPropertyLookup
	}

	if arrayLiteral, isArrayLiteral := expression.LOperand.(pgsql.ArrayLiteral); isArrayLiteral {
		if arrayLiteral.CastType == pgsql.AnyArray && len(arrayLiteral.Values) == 0 {
			hasEmptyArrayLiteral = true
		}
	} else if arrayLiteral, isArrayLiteral := expression.ROperand.(pgsql.ArrayLiteral); isArrayLiteral {
		if arrayLiteral.CastType == pgsql.AnyArray && len(arrayLiteral.Values) == 0 {
			hasEmptyArrayLiteral = true
		}
	}

	return propertyLookup, hasPropertyLookup && hasEmptyArrayLiteral
}

func isEmptyAnyArrayLiteral(expression pgsql.Expression) bool {
	arrayLiteral, isArrayLiteral := expression.(pgsql.ArrayLiteral)
	return isArrayLiteral && arrayLiteral.CastType == pgsql.AnyArray && len(arrayLiteral.Values) == 0
}

func isKnownEmptyArrayExpression(expression pgsql.Expression) bool {
	if isEmptyAnyArrayLiteral(expression) {
		return true
	}

	switch typedExpression := expression.(type) {
	case pgsql.Parameter:
		return typedExpression.CastType == pgsql.Null
	case *pgsql.Parameter:
		return typedExpression.CastType == pgsql.Null
	default:
		return false
	}
}

func jsonNullLiteral() pgsql.Expression {
	return pgsql.NewTypeCast(pgsql.NewLiteral(pgsql.StringLiteralNull, pgsql.Text), pgsql.JSONB)
}

func jsonEmptyArrayLiteral() pgsql.Expression {
	return pgsql.NewTypeCast(pgsql.NewLiteral(pgsql.StringLiteralEmptyArray, pgsql.Text), pgsql.JSONB)
}

func rewritePropertyLookupNullCheck(propertyLookup *pgsql.BinaryExpression, isNotNull bool) pgsql.Expression {
	propertyLookup.Operator = pgsql.OperatorJSONField

	existsExpression := pgsql.NewBinaryExpression(
		propertyLookup.LOperand,
		pgsql.OperatorJSONBFieldExists,
		propertyLookup.ROperand,
	)
	jsonNullExpression := pgsql.NewBinaryExpression(
		propertyLookup,
		pgsql.OperatorEquals,
		jsonNullLiteral(),
	)

	if isNotNull {
		return pgsql.NewParenthetical(pgsql.NewBinaryExpression(
			existsExpression,
			pgsql.OperatorAnd,
			pgsql.NewUnaryExpression(pgsql.OperatorNot, jsonNullExpression),
		))
	}

	return pgsql.NewParenthetical(pgsql.NewBinaryExpression(
		pgsql.NewUnaryExpression(pgsql.OperatorNot, existsExpression),
		pgsql.OperatorOr,
		jsonNullExpression,
	))
}

func buildEmptyArrayPropertyComparison(propertyLookup *pgsql.BinaryExpression, negated bool) *pgsql.BinaryExpression {
	propertyLookup.Operator = pgsql.OperatorJSONField

	existsExpression := pgsql.NewBinaryExpression(
		propertyLookup.LOperand,
		pgsql.OperatorJSONBFieldExists,
		propertyLookup.ROperand,
	)
	emptyArrayExpression := pgsql.NewBinaryExpression(
		propertyLookup,
		pgsql.OperatorEquals,
		jsonEmptyArrayLiteral(),
	)

	if negated {
		return pgsql.NewBinaryExpression(
			pgsql.NewBinaryExpression(
				existsExpression,
				pgsql.OperatorAnd,
				pgsql.NewUnaryExpression(pgsql.OperatorNot, emptyArrayExpression),
			),
			pgsql.OperatorAnd,
			pgsql.NewUnaryExpression(
				pgsql.OperatorNot,
				pgsql.NewBinaryExpression(propertyLookup, pgsql.OperatorEquals, jsonNullLiteral()),
			),
		)
	}

	return pgsql.NewBinaryExpression(
		existsExpression,
		pgsql.OperatorAnd,
		emptyArrayExpression,
	)
}

func escapeLikePatternExpression(expression pgsql.Expression) pgsql.Expression {
	var escapedExpression pgsql.Expression = pgsql.NewTypeCast(expression, pgsql.Text)

	for _, replacement := range []struct {
		old string
		new string
	}{
		{old: "\\", new: "\\\\"},
		{old: "%", new: "\\%"},
		{old: "_", new: "\\_"},
	} {
		escapedExpression = pgsql.FunctionCall{
			Function: pgsql.FunctionReplace,
			Parameters: []pgsql.Expression{
				escapedExpression,
				pgsql.NewLiteral(replacement.old, pgsql.Text),
				pgsql.NewLiteral(replacement.new, pgsql.Text),
			},
			CastType: pgsql.Text,
		}
	}

	return escapedExpression
}

func escapeLikePatternOperand(operand pgsql.Expression) (pgsql.Expression, error) {
	if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(operand); isPropertyLookup {
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return escapeLikePatternExpression(propertyLookup), nil
	}

	if parenthetical, isParenthetical := operand.(*pgsql.Parenthetical); isParenthetical {
		typeCastedOperand, err := TypeCastExpression(parenthetical, pgsql.Text)
		if err != nil {
			return nil, err
		}

		return escapeLikePatternExpression(typeCastedOperand), nil
	}

	return escapeLikePatternExpression(operand), nil
}

func buildContainsLikePattern(operand pgsql.Expression) (pgsql.Expression, error) {
	escapedOperand, err := escapeLikePatternOperand(operand)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		pgsql.NewLiteral("%", pgsql.Text),
		pgsql.OperatorConcatenate,
		pgsql.NewBinaryExpression(
			escapedOperand,
			pgsql.OperatorConcatenate,
			pgsql.NewLiteral("%", pgsql.Text),
		),
	), nil
}

func buildStartsWithLikePattern(operand pgsql.Expression) (pgsql.Expression, error) {
	escapedOperand, err := escapeLikePatternOperand(operand)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		escapedOperand,
		pgsql.OperatorConcatenate,
		pgsql.NewLiteral("%", pgsql.Text),
	), nil
}

func buildEndsWithLikePattern(operand pgsql.Expression) (pgsql.Expression, error) {
	escapedOperand, err := escapeLikePatternOperand(operand)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		pgsql.NewLiteral("%", pgsql.Text),
		pgsql.OperatorConcatenate,
		escapedOperand,
	), nil
}

func (s *ExpressionTreeTranslator) rewriteBinaryExpression(newExpression *pgsql.BinaryExpression) error {
	switch newExpression.Operator {
	case pgsql.OperatorAdd:
		// In the case of the use of the cypher `+` operator we must attempt to disambiguate if the intent
		// is to concatenate or to perform an addition
		if lOperandType, err := InferExpressionType(newExpression.LOperand); err != nil {
			return err
		} else if rOperandType, err := InferExpressionType(newExpression.ROperand); err != nil {
			return err
		} else if isConcatenationOperation(newExpression.LOperand, newExpression.ROperand, lOperandType, rOperandType) {
			newExpression.Operator = pgsql.OperatorConcatenate
		}

		s.PushOperand(newExpression)

	case pgsql.OperatorCypherContains:
		newExpression.Operator = pgsql.OperatorLike

		switch typedLOperand := newExpression.LOperand.(type) {
		case *pgsql.BinaryExpression:
			switch typedLOperand.Operator {
			case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
			default:
				return fmt.Errorf("unexpected operator %s for binary expression \"%s\" left operand", typedLOperand.Operator, newExpression.Operator)
			}
		}

		switch typedROperand := newExpression.ROperand.(type) {
		case pgsql.Literal:
			if rOperandDataType := typedROperand.TypeHint(); rOperandDataType != pgsql.Text {
				return fmt.Errorf("expected %s data type but found %s as right operand for operator %s", pgsql.Text, rOperandDataType, newExpression.Operator)
			} else if stringValue, isString := typedROperand.Value.(string); !isString {
				return fmt.Errorf("expected string but found %T as right operand for operator %s", typedROperand.Value, newExpression.Operator)
			} else {
				newExpression.ROperand = pgsql.NewLiteral("%"+stringValue+"%", rOperandDataType)
			}

		case *pgsql.Parenthetical:
			if pattern, err := buildContainsLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pattern
			}

		case *pgsql.BinaryExpression:
			if pattern, err := buildContainsLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pgsql.NewTypeCast(pattern, pgsql.Text)
			}

		default:
			if pattern, err := buildContainsLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pattern
			}
		}

		s.PushOperand(newExpression)

	case pgsql.OperatorCypherRegexMatch:
		newExpression.Operator = pgsql.OperatorRegexMatch
		s.PushOperand(newExpression)

	case pgsql.OperatorCypherStartsWith:
		newExpression.Operator = pgsql.OperatorLike

		switch typedLOperand := newExpression.LOperand.(type) {
		case *pgsql.BinaryExpression:
			switch typedLOperand.Operator {
			case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
			default:
				return fmt.Errorf("unexpected operator %s for binary expression \"%s\" left operand", typedLOperand.Operator, newExpression.Operator)
			}
		}

		switch typedROperand := newExpression.ROperand.(type) {
		case pgsql.Literal:
			if rOperandDataType := typedROperand.TypeHint(); rOperandDataType != pgsql.Text {
				return fmt.Errorf("expected %s data type but found %s as right operand for operator %s", pgsql.Text, rOperandDataType, newExpression.Operator)
			} else if stringValue, isString := typedROperand.Value.(string); !isString {
				return fmt.Errorf("expected string but found %T as right operand for operator %s", typedROperand.Value, newExpression.Operator)
			} else {
				newExpression.ROperand = pgsql.NewLiteral(stringValue+"%", rOperandDataType)
			}

		case *pgsql.Parenthetical:
			if pattern, err := buildStartsWithLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pattern
			}

		case *pgsql.BinaryExpression:
			if pattern, err := buildStartsWithLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pgsql.NewTypeCast(pattern, pgsql.Text)
			}

		default:
			if pattern, err := buildStartsWithLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pattern
			}
		}

		s.PushOperand(newExpression)

	case pgsql.OperatorCypherEndsWith:
		newExpression.Operator = pgsql.OperatorLike

		switch typedLOperand := newExpression.LOperand.(type) {
		case *pgsql.BinaryExpression:
			switch typedLOperand.Operator {
			case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
			default:
				return fmt.Errorf("unexpected operator %s for binary expression \"%s\" left operand", typedLOperand.Operator, newExpression.Operator)
			}
		}

		switch typedROperand := newExpression.ROperand.(type) {
		case pgsql.Literal:
			if rOperandDataType := typedROperand.TypeHint(); rOperandDataType != pgsql.Text {
				return fmt.Errorf("expected %s data type but found %s as right operand for operator %s", pgsql.Text, rOperandDataType, newExpression.Operator)
			} else if stringValue, isString := typedROperand.Value.(string); !isString {
				return fmt.Errorf("expected string but found %T as right operand for operator %s", typedROperand.Value, newExpression.Operator)
			} else {
				newExpression.ROperand = pgsql.NewLiteral("%"+stringValue, rOperandDataType)
			}

		case *pgsql.Parenthetical:
			if pattern, err := buildEndsWithLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pattern
			}

		case *pgsql.BinaryExpression:
			if pattern, err := buildEndsWithLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pgsql.NewTypeCast(pattern, pgsql.Text)
			}

		default:
			if pattern, err := buildEndsWithLikePattern(typedROperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pattern
			}
		}

		s.PushOperand(newExpression)

	case pgsql.OperatorIs:
		switch typedLOperand := newExpression.LOperand.(type) {
		case *pgsql.BinaryExpression:
			switch typedLOperand.Operator {
			case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
				switch typedROperand := newExpression.ROperand.(type) {
				case pgsql.Literal:
					if typedROperand.Null {
						s.PushOperand(rewritePropertyLookupNullCheck(typedLOperand, false))
						return nil
					}
				}
			}
		}

	case pgsql.OperatorIsNot:
		switch typedLOperand := newExpression.LOperand.(type) {
		case *pgsql.BinaryExpression:
			switch typedLOperand.Operator {
			case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
				switch typedROperand := newExpression.ROperand.(type) {
				case pgsql.Literal:
					if typedROperand.Null {
						s.PushOperand(rewritePropertyLookupNullCheck(typedLOperand, true))
						return nil
					}
				}
			}
		}

	case pgsql.OperatorIn:
		if isKnownEmptyArrayExpression(newExpression.ROperand) {
			s.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
			return nil
		}

		newExpression.Operator = pgsql.OperatorEquals

		switch typedROperand := newExpression.ROperand.(type) {
		case pgsql.TypeCast:
			switch typedInnerOperand := typedROperand.Expression.(type) {
			case *pgsql.BinaryExpression:
				if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(typedInnerOperand); isPropertyLookup {
					// Attempt to figure out the cast by looking at the left operand
					if leftHint, err := InferExpressionType(newExpression.LOperand); err != nil {
						return err
					} else if leftArrayHint, err := leftHint.ToArrayType(); err != nil {
						return err
					} else {
						// Ensure the lookup uses the JSONB type
						propertyLookup.Operator = pgsql.OperatorJSONField

						newExpression.ROperand = pgsql.NewAnyExpressionHinted(
							pgsql.FunctionCall{
								Function:   pgsql.FunctionJSONBToTextArray,
								Parameters: []pgsql.Expression{propertyLookup},
								CastType:   leftArrayHint,
							},
						)
					}
				}
			}

		case pgsql.TypeHinted:
			if lOperandTypeHint, err := InferExpressionType(newExpression.LOperand); err != nil {
				return err
			} else if lOperandTypeHint.IsArrayType() {
				newExpression.Operator = pgsql.OperatorPGArrayOverlap
			} else {
				newExpression.Operator = pgsql.OperatorEquals
				newExpression.ROperand = pgsql.NewAnyExpression(newExpression.ROperand, typedROperand.TypeHint())
			}

		default:
			// Attempt to figure out the cast by looking at the left operand
			if leftHint, err := InferExpressionType(newExpression.LOperand); err != nil {
				return err
			} else {
				newExpression.ROperand = pgsql.NewAnyExpression(newExpression.ROperand, leftHint)
			}
		}

		s.PushOperand(newExpression)

	case pgsql.OperatorEquals:
		if propertyLookup, hasEmptyArrayLiteralPropertyComparison := isEmptyArrayLiteralPropertyComparison(newExpression); hasEmptyArrayLiteralPropertyComparison {
			expandedExpression := buildEmptyArrayPropertyComparison(propertyLookup, false)

			if err := applyBinaryExpressionTypeHints(s.kindMapper, expandedExpression); err != nil {
				return err
			}

			s.PushOperand(pgsql.NewParenthetical(expandedExpression))
		} else {
			s.PushOperand(newExpression)
		}

	case pgsql.OperatorCypherNotEquals:
		if propertyLookup, hasEmptyArrayLiteralPropertyComparison := isEmptyArrayLiteralPropertyComparison(newExpression); hasEmptyArrayLiteralPropertyComparison {
			expandedExpression := buildEmptyArrayPropertyComparison(propertyLookup, true)

			if err := applyBinaryExpressionTypeHints(s.kindMapper, expandedExpression); err != nil {
				return err
			}

			s.PushOperand(pgsql.NewParenthetical(expandedExpression))
		} else {
			s.PushOperand(newExpression)
		}

	default:
		s.PushOperand(newExpression)
	}

	return nil
}

func (s *ExpressionTreeTranslator) PushParenthetical() {
	s.PushOperand(&pgsql.Parenthetical{})
	s.parentheticalDepth += 1
}

func (s *ExpressionTreeTranslator) PopParenthetical() (*pgsql.Parenthetical, error) {
	s.parentheticalDepth -= 1

	if operand, err := s.treeBuilder.PopOperand(s.kindMapper); err != nil {
		return nil, err
	} else if parentheticalExpr, typeOK := operand.(*pgsql.Parenthetical); !typeOK {
		return nil, fmt.Errorf("expected type *pgsql.Parenthetical but received %T", operand)
	} else {
		return parentheticalExpr, nil
	}
}

func (s *ExpressionTreeTranslator) VisitOperator(operator pgsql.Operator) {
	// Track this operator for expression tree extraction
	switch operator {
	case pgsql.OperatorAnd:
		s.conjunctionDepth += 1

	case pgsql.OperatorOr:
		s.disjunctionDepth += 1
	}
}

func (s *ExpressionTreeTranslator) CompleteBinaryExpression(scope *Scope, operator pgsql.Operator) error {
	switch operator {
	case pgsql.OperatorAnd:
		if s.parentheticalDepth == 0 && s.disjunctionDepth == 0 {
			return s.ConstrainConjoinedOperandPair()
		}

		s.conjunctionDepth -= 1

	case pgsql.OperatorOr:
		if s.parentheticalDepth == 0 && s.conjunctionDepth == 0 {
			return s.ConstrainDisjointOperandPair()
		}

		s.disjunctionDepth -= 1
	}

	if newExpression, err := s.PopBinaryExpression(operator); err != nil {
		return err
	} else if err := rewriteIdentityOperands(scope, newExpression); err != nil {
		return err
	} else {
		return s.rewriteBinaryExpression(newExpression)
	}
}

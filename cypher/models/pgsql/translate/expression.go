package translate

import (
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// unwrapParenthetical removes every enclosing parenthetical expression and returns the innermost operand.
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

// expressionHasCompositeProperties reports whether a data type exposes an entity properties field.
func expressionHasCompositeProperties(expressionType pgsql.DataType) bool {
	switch expressionType {
	case pgsql.NodeComposite, pgsql.EdgeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionEdge, pgsql.ExpansionTerminalNode:
		return true

	default:
		return false
	}
}

// isCompositePropertyLookupTarget reports whether a type-hinted expression exposes composite properties.
func isCompositePropertyLookupTarget(expression pgsql.TypeHinted) bool {
	return expressionHasCompositeProperties(expression.TypeHint())
}

// translateCompositePropertyLookup pushes a lookup of the properties field from a composite expression.
func (s *Translator) translateCompositePropertyLookup(target pgsql.Expression, lookup *cypher.PropertyLookup) error {
	if fieldIdentifierLiteral, err := pgsql.AsLiteral(lookup.Symbol); err != nil {
		return err
	} else {
		s.treeTranslator.PushOperand(pgsql.RowColumnReference{
			Identifier: target,
			Column:     pgsql.ColumnProperties,
		})
		s.treeTranslator.PushOperand(fieldIdentifierLiteral)

		return s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorPropertyLookup)
	}
}

// translatePropertyLookup lowers a validated Cypher property access according to its translated atom type.
func (s *Translator) translatePropertyLookup(lookup *cypher.PropertyLookup) error {
	if err := cypher.ValidatePropertyKeyName(lookup.Symbol); err != nil {
		return err
	}

	if translatedAtom, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else {
		switch typedTranslatedAtom := translatedAtom.(type) {
		case pgsql.Identifier:
			if fieldIdentifierLiteral, err := pgsql.AsLiteral(lookup.Symbol); err != nil {
				return err
			} else {
				s.treeTranslator.PushOperand(pgsql.CompoundIdentifier{typedTranslatedAtom, pgsql.ColumnProperties})
				s.treeTranslator.PushOperand(pgsql.PropertyKey{Literal: fieldIdentifierLiteral})

				if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorPropertyLookup); err != nil {
					return err
				}
			}

		default:
			if expressionType, err := s.inferExpressionType(translatedAtom); err != nil {
				return err
			} else if !expressionHasCompositeProperties(expressionType) {
				return fmt.Errorf("unsupported property lookup %s on expression type %s", lookup.Symbol, expressionType)
			} else if fieldIdentifierLiteral, err := pgsql.AsLiteral(lookup.Symbol); err != nil {
				return err
			} else {
				s.treeTranslator.PushOperand(pgsql.RowColumnReference{
					Identifier: translatedAtom,
					Column:     pgsql.ColumnProperties,
				})
				s.treeTranslator.PushOperand(fieldIdentifierLiteral)

				if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorPropertyLookup); err != nil {
					return err
				}
			}

		case pgsql.FunctionCall:
			if isCompositePropertyLookupTarget(typedTranslatedAtom) {
				return s.translateCompositePropertyLookup(typedTranslatedAtom, lookup)
			}

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

		case pgsql.TypeCast:
			if isCompositePropertyLookupTarget(typedTranslatedAtom) {
				return s.translateCompositePropertyLookup(typedTranslatedAtom, lookup)
			} else if fieldIdentifierLiteral, err := pgsql.AsLiteral(lookup.Symbol); err != nil {
				return err
			} else {
				// Temporal constructions such as datetime('...') or date(n.d) lower to a type cast;
				// their components are unsupported and must be rejected here instead of silently
				// dropping the cast operand, which underflows the expression operand stack.
				return fmt.Errorf("unsupported temporal type component %s on cast type %s", fieldIdentifierLiteral.Value, typedTranslatedAtom.CastType)
			}
		}
	}

	return nil
}

// translateCypherAssignmentOperator maps supported Cypher assignment operators to their PostgreSQL AST equivalents.
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

// rewriteStringWildCardLiteral escapes LIKE metacharacters in a literal string operand.
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

// rewritePropertyLookupOperator selects JSON text extraction, JSON extraction, and casts for the requested result type.
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

// isJSONScalarEqualityType reports whether a scalar can be normalized to JSONB for Cypher equality.
func isJSONScalarEqualityType(dataType pgsql.DataType) bool {
	switch dataType {
	case pgsql.Boolean, pgsql.Float4, pgsql.Float8, pgsql.Int, pgsql.Int2, pgsql.Int4, pgsql.Int8, pgsql.Numeric:
		return true

	default:
		return false
	}
}

// rewriteJSONScalarEqualityOperand converts a non-null supported scalar to comparable JSONB.
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

// rewriteStringEqualityOperand accepts a non-null text expression for string-specific equality handling.
func rewriteStringEqualityOperand(expression pgsql.Expression) (pgsql.Expression, bool) {
	if literal, isLiteral := expression.(pgsql.Literal); isLiteral && literal.Null {
		return nil, false
	}

	if typedExpression, isTypeHinted := expression.(pgsql.TypeHinted); !isTypeHinted {
		return nil, false
	} else if typedExpression.TypeHint() != pgsql.Text {
		return nil, false
	}

	return expression, true
}

// lookupRequiresElementType reports whether an array comparison expects a property's element type rather than its array type.
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

// TypeCastExpression applies a type hint, rewriting property comparisons when the operator requires element typing.
func TypeCastExpression(expression pgsql.Expression, dataType pgsql.DataType) (pgsql.Expression, error) {
	if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(expression); isPropertyLookup {
		lookupTypeHint := dataType

		if lookupRequiresElementType(dataType, propertyLookup.Operator, propertyLookup.ROperand) {
			// Take the base type of the array type hint: <unit> in <collection>
			lookupTypeHint = dataType.ArrayBaseType()
		}

		return rewritePropertyLookupOperator(propertyLookup, lookupTypeHint), nil
	}

	return pgsql.NewTypeCast(expression, dataType), nil
}

// jsonNullLiteral returns the JSONB representation of a JSON null value.
func jsonNullLiteral() pgsql.Expression {
	return pgsql.NewTypeCast(pgsql.NewLiteral(pgsql.StringLiteralNull, pgsql.Text), pgsql.JSONB)
}

// nullifyJSONPropertyLookup converts a JSON null property value to SQL NULL with NULLIF.
func nullifyJSONPropertyLookup(propertyLookup *pgsql.BinaryExpression) pgsql.Expression {
	return pgsql.FunctionCall{
		Function: pgsql.FunctionNullIf,
		Parameters: []pgsql.Expression{
			propertyLookup,
			jsonNullLiteral(),
		},
		CastType: pgsql.JSONB,
	}
}

// rewritePropertyLookupOperands assigns extraction operators and casts using the comparison's opposite operand.
func rewritePropertyLookupOperands(kindMapper *contextAwareKindMapper, expression *pgsql.BinaryExpression) error {
	var (
		leftPropertyLookup, hasLeftPropertyLookup   = expressionToPropertyLookupBinaryExpression(expression.LOperand)
		rightPropertyLookup, hasRightPropertyLookup = expressionToPropertyLookupBinaryExpression(expression.ROperand)
	)

	if expression.Operator == pgsql.OperatorAnd || expression.Operator == pgsql.OperatorOr {
		if hasLeftPropertyLookup {
			expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, pgsql.Boolean)
		}
		if hasRightPropertyLookup {
			expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, pgsql.Boolean)
		}
		return nil
	}

	// Ensure that direct property comparisons prefer JSONB - JSONB. Non-comparison operators must keep their
	// property lookups text-oriented until rewriteBinaryExpression has enough context to disambiguate them.
	if hasLeftPropertyLookup && hasRightPropertyLookup &&
		(pgsql.OperatorIsComparator(expression.Operator) || expression.Operator == pgsql.OperatorCypherNotEquals) {
		leftPropertyLookup.Operator = pgsql.OperatorJSONField
		rightPropertyLookup.Operator = pgsql.OperatorJSONField
		expression.LOperand = nullifyJSONPropertyLookup(leftPropertyLookup)
		expression.ROperand = nullifyJSONPropertyLookup(rightPropertyLookup)

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
				if rewrittenROperand, rewritten := rewriteStringEqualityOperand(expression.ROperand); rewritten {
					expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, pgsql.Text)
					expression.ROperand = rewrittenROperand
				} else if rewrittenROperand, rewritten := rewriteJSONScalarEqualityOperand(expression.ROperand); rewritten {
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
				if rewrittenLOperand, rewritten := rewriteStringEqualityOperand(expression.LOperand); rewritten {
					expression.LOperand = rewrittenLOperand
					expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, pgsql.Text)
				} else if rewrittenLOperand, rewritten := rewriteJSONScalarEqualityOperand(expression.LOperand); rewritten {
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

// newFunctionCallComparatorError returns a focused type-mismatch error for function comparisons with special Cypher semantics.
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

// PopOperand removes and returns the next operand from the expression stack. An
// empty stack is a producer bug rather than a caller-facing panic: every visitor
// that pops operands must have pushed them first, so the underflow is reported as
// an ordinary translation error instead.
func (s *Builder) PopOperand(kindMapper *contextAwareKindMapper) (pgsql.Expression, error) {
	if len(s.stack) == 0 {
		return nil, fmt.Errorf("expression operand stack is empty")
	}

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
		} else if rewrittenExpression, rewritten := buildStringPropertyEqualityPredicate(typedNext); rewritten {
			next = rewrittenExpression
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

	treeBuilder *Builder
	kindMapper  *contextAwareKindMapper
}

func NewExpressionTreeTranslator(kindMapper *contextAwareKindMapper) *ExpressionTreeTranslator {
	return &ExpressionTreeTranslator{
		UserConstraints:        NewConstraintTracker(),
		TranslationConstraints: NewConstraintTracker(),
		treeBuilder:            NewExpressionTreeBuilder(),
		kindMapper:             kindMapper,
	}
}

// mergeUserAndTranslationConstraints combines user predicates with translator-added safety constraints.
func mergeUserAndTranslationConstraints(userConstraints, translationConstraints *Constraint) *Constraint {
	if userConstraints.Expression != nil {
		// Fold the user constraints into the translation constraints wrapped in a parenthetical
		translationConstraints.Expression = pgsql.OptionalAnd(pgsql.NewParenthetical(userConstraints.Expression), translationConstraints.Expression)

		// Ensure that the identifier dependencies in the constraint are merged as well
		translationConstraints.Dependencies.MergeSet(userConstraints.Dependencies)
	}

	return translationConstraints
}

// HasAnyConstraints reports whether the supplied scope can evaluate any satisfiable user or translator constraint.
func (s *ExpressionTreeTranslator) HasAnyConstraints(scope *pgsql.IdentifierSet) (bool, error) {
	if hasUser, err := s.UserConstraints.HasConstraints(scope); err != nil {
		return false, err
	} else if hasUser {
		return true, nil
	}

	return s.TranslationConstraints.HasConstraints(scope)
}

func (s *ExpressionTreeTranslator) HasEndpointInequality(leftEndpoint, rightEndpoint pgsql.Identifier) bool {
	return s.UserConstraints.HasEndpointInequality(leftEndpoint, rightEndpoint) ||
		s.TranslationConstraints.HasEndpointInequality(leftEndpoint, rightEndpoint)
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

// popOperandAsUserConstraint removes the next operand, normalizes bare property truth tests, and records its dependencies.
func (s *ExpressionTreeTranslator) popOperandAsUserConstraint() error {
	if nextExpression, err := s.PopOperand(); err != nil {
		return err
	} else {
		return s.constrainExpression(nextExpression)
	}
}

// constrainExpression splits only a predicate's outer conjunction. Boolean expressions
// inside parentheses, function calls and other value expressions remain intact.
func (s *ExpressionTreeTranslator) constrainExpression(expression pgsql.Expression) error {
	if conjunction, ok := expression.(*pgsql.BinaryExpression); ok && conjunction.Operator == pgsql.OperatorAnd {
		// Consume right to left, preserving the constraint ordering used by the tracker.
		if err := s.constrainExpression(conjunction.ROperand); err != nil {
			return err
		}
		return s.constrainExpression(conjunction.LOperand)
	}

	if identifierDeps, err := ExtractSyntaxNodeReferences(expression); err != nil {
		return err
	} else {
		if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(expression); isPropertyLookup {
			// If this is a bare property lookup rewrite it with the intended type of boolean
			expression = rewritePropertyLookupOperator(propertyLookup, pgsql.Boolean)
		}

		return s.AddUserConstraint(identifierDeps, expression)
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

func (s *ExpressionTreeTranslator) popBinaryExpressionOperands(operator pgsql.Operator) (*pgsql.BinaryExpression, error) {
	if rightOperand, err := s.PopOperand(); err != nil {
		return nil, err
	} else if leftOperand, err := s.PopOperand(); err != nil {
		return nil, err
	} else {
		return pgsql.NewBinaryExpression(leftOperand, operator, rightOperand), nil
	}
}

func (s *ExpressionTreeTranslator) PopBinaryExpression(operator pgsql.Operator) (*pgsql.BinaryExpression, error) {
	expression, err := s.popBinaryExpressionOperands(operator)
	if err != nil {
		return nil, err
	}
	return expression, applyBinaryExpressionTypeHints(s.kindMapper, expression)
}

// rewriteIdentityOperands replaces entity comparisons with comparisons of their scalar identity fields.
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

						case pgsql.Int8Array:
							if newExpression.Operator == pgsql.OperatorIn {
								newExpression.LOperand = pgsql.CompoundIdentifier{typedLOperand, pgsql.ColumnID}
							} else {
								return fmt.Errorf("invalid comparison between types %s and %s", boundLOperand.DataType, boundROperand.DataType)
							}

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

						case pgsql.Int8Array:
							if newExpression.Operator == pgsql.OperatorIn {
								newExpression.LOperand = pgsql.CompoundIdentifier{typedLOperand, pgsql.ColumnID}
							} else {
								return fmt.Errorf("invalid comparison between types %s and %s", boundLOperand.DataType, boundROperand.DataType)
							}

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

// isPropertyLookup reports whether expression is a property-lookup binary expression, including wrapped forms.
func isPropertyLookup(expression pgsql.Expression) bool {
	_, isPropertyLookup := expressionToPropertyLookupBinaryExpression(expression)
	return isPropertyLookup
}

// isConcatenationOperation identifies concatenation when an operand has a known
// string or array type. Dynamic property pairs use runtime dispatch instead.
func isConcatenationOperation(lOperandType, rOperandType pgsql.DataType) bool {
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

	return false
}

// isEmptyArrayLiteralPropertyComparison finds a property lookup paired with an untyped empty array literal.
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

// isEmptyAnyArrayLiteral reports whether expression is an empty array with no inferred element type.
func isEmptyAnyArrayLiteral(expression pgsql.Expression) bool {
	arrayLiteral, isArrayLiteral := expression.(pgsql.ArrayLiteral)
	return isArrayLiteral && arrayLiteral.CastType == pgsql.AnyArray && len(arrayLiteral.Values) == 0
}

// isKnownEmptyArrayExpression reports whether expression is an untyped empty array or a parameter statically typed as NULL.
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

// jsonEmptyArrayLiteral returns the JSONB representation of an empty array.
func jsonEmptyArrayLiteral() pgsql.Expression {
	return pgsql.NewTypeCast(pgsql.NewLiteral(pgsql.StringLiteralEmptyArray, pgsql.Text), pgsql.JSONB)
}

// rewritePropertyLookupNullCheck preserves Cypher null semantics for missing keys and explicit JSON null values.
func rewritePropertyLookupNullCheck(propertyLookup *pgsql.BinaryExpression, isNotNull bool) pgsql.Expression {
	propertyLookup.Operator = pgsql.OperatorJSONField

	var (
		existsExpression = pgsql.NewBinaryExpression(
			propertyLookup.LOperand,
			pgsql.OperatorJSONBFieldExists,
			propertyLookup.ROperand,
		)
		jsonNullExpression = pgsql.NewBinaryExpression(
			propertyLookup,
			pgsql.OperatorEquals,
			jsonNullLiteral(),
		)
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

// jsonFieldPropertyLookup copies a property lookup using JSONB field extraction.
func jsonFieldPropertyLookup(propertyLookup *pgsql.BinaryExpression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(propertyLookup.LOperand, pgsql.OperatorJSONField, propertyLookup.ROperand)
}

// jsonTextPropertyLookup copies a property lookup using text field extraction.
func jsonTextPropertyLookup(propertyLookup *pgsql.BinaryExpression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(propertyLookup.LOperand, pgsql.OperatorJSONTextField, propertyLookup.ROperand)
}

// jsonbTypeof returns a call that inspects an expression's JSONB value type.
func jsonbTypeof(expression pgsql.Expression) pgsql.Expression {
	return pgsql.FunctionCall{
		Function:   pgsql.FunctionJSONBTypeof,
		Parameters: []pgsql.Expression{expression},
	}
}

// jsonbStringTypeCheck reports at SQL runtime whether a property contains a JSON string.
func jsonbStringTypeCheck(propertyLookup *pgsql.BinaryExpression) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		jsonbTypeof(jsonFieldPropertyLookup(propertyLookup)),
		pgsql.OperatorEquals,
		pgsql.NewLiteral("string", pgsql.Text),
	)
}

// toJSONBTextOperand converts expression through text to a JSONB scalar for type-safe comparison.
func toJSONBTextOperand(expression pgsql.Expression) pgsql.Expression {
	return pgsql.FunctionCall{
		Function: pgsql.FunctionToJSONB,
		Parameters: []pgsql.Expression{
			pgsql.NewTypeCast(expression, pgsql.Text),
		},
		CastType: pgsql.JSONB,
	}
}

// buildStringPropertyEqualityComparison compares a property's text extraction with a text operand in the original operand order.
func buildStringPropertyEqualityComparison(propertyLookup *pgsql.BinaryExpression, textOperand pgsql.Expression, propertyOnLeft bool, operator pgsql.Operator) pgsql.Expression {
	textPropertyLookup := jsonTextPropertyLookup(propertyLookup)

	if propertyOnLeft {
		return pgsql.NewBinaryExpression(textPropertyLookup, operator, textOperand)
	}

	return pgsql.NewBinaryExpression(textOperand, operator, textPropertyLookup)
}

// buildStringPropertyEqualityPredicate recognizes string/property equality and builds its type-aware predicate.
func buildStringPropertyEqualityPredicate(expression *pgsql.BinaryExpression) (pgsql.Expression, bool) {
	if !expression.Operator.IsIn(pgsql.OperatorEquals, pgsql.OperatorCypherNotEquals) {
		return nil, false
	}

	leftPropertyLookup, hasLeftPropertyLookup := expressionToPropertyLookupBinaryExpression(expression.LOperand)
	rightPropertyLookup, hasRightPropertyLookup := expressionToPropertyLookupBinaryExpression(expression.ROperand)

	if hasLeftPropertyLookup {
		if rewrittenROperand, rewritten := rewriteStringEqualityOperand(expression.ROperand); rewritten {
			rewritePropertyLookupOperator(leftPropertyLookup, pgsql.Text)
			return buildStringPropertyComparisonPredicate(leftPropertyLookup, rewrittenROperand, true, expression.Operator), true
		}
	}

	if hasRightPropertyLookup {
		if rewrittenLOperand, rewritten := rewriteStringEqualityOperand(expression.LOperand); rewritten {
			rewritePropertyLookupOperator(rightPropertyLookup, pgsql.Text)
			return buildStringPropertyComparisonPredicate(rightPropertyLookup, rewrittenLOperand, false, expression.Operator), true
		}
	}

	return nil, false
}

// buildStringPropertyComparisonPredicate guards text comparison by JSON type while preserving inequality for non-string values.
func buildStringPropertyComparisonPredicate(propertyLookup *pgsql.BinaryExpression, textOperand pgsql.Expression, propertyOnLeft bool, operator pgsql.Operator) pgsql.Expression {
	stringComparison := buildStringPropertyEqualityComparison(propertyLookup, textOperand, propertyOnLeft, operator)

	if operator == pgsql.OperatorEquals {
		return pgsql.NewParenthetical(pgsql.NewBinaryExpression(
			jsonbStringTypeCheck(propertyLookup),
			pgsql.OperatorAnd,
			stringComparison,
		))
	}

	var (
		nonStringTypeCheck = pgsql.NewBinaryExpression(
			jsonbTypeof(jsonFieldPropertyLookup(propertyLookup)),
			pgsql.OperatorCypherNotEquals,
			pgsql.NewLiteral("string", pgsql.Text),
		)
		nonStringComparison = pgsql.NewBinaryExpression(
			jsonFieldPropertyLookup(propertyLookup),
			pgsql.OperatorCypherNotEquals,
			toJSONBTextOperand(textOperand),
		)
	)

	return pgsql.NewParenthetical(pgsql.NewBinaryExpression(
		pgsql.NewBinaryExpression(
			jsonbStringTypeCheck(propertyLookup),
			pgsql.OperatorAnd,
			stringComparison,
		),
		pgsql.OperatorOr,
		pgsql.NewBinaryExpression(nonStringTypeCheck, pgsql.OperatorAnd, nonStringComparison),
	))
}

// buildEmptyArrayPropertyComparison compares a property with [] while retaining null taint and optional negation.
func buildEmptyArrayPropertyComparison(propertyLookup *pgsql.BinaryExpression, negated bool) *pgsql.BinaryExpression {
	var (
		emptyArrayExpression = pgsql.NewBinaryExpression(
			jsonFieldPropertyLookup(propertyLookup),
			pgsql.OperatorEquals,
			jsonEmptyArrayLiteral(),
		)
		nullExpression = pgsql.NewBinaryExpression(
			jsonFieldPropertyLookup(propertyLookup),
			pgsql.OperatorEquals,
			jsonNullLiteral(),
		)
		nullTaintExpression = pgsql.NewBinaryExpression(
			nullExpression,
			pgsql.OperatorAnd,
			pgsql.NullLiteral(),
		)
	)

	if negated {
		return pgsql.NewBinaryExpression(
			pgsql.NewBinaryExpression(
				pgsql.NewBinaryExpression(
					jsonFieldPropertyLookup(propertyLookup),
					pgsql.OperatorNotEquals,
					jsonEmptyArrayLiteral(),
				),
				pgsql.OperatorAnd,
				pgsql.NewBinaryExpression(
					jsonFieldPropertyLookup(propertyLookup),
					pgsql.OperatorNotEquals,
					jsonNullLiteral(),
				),
			),
			pgsql.OperatorOr,
			nullTaintExpression,
		)
	}

	return pgsql.NewBinaryExpression(
		emptyArrayExpression,
		pgsql.OperatorOr,
		nullTaintExpression,
	)
}

// cypherStringPredicateTextOperand converts a predicate operand to text while retaining null propagation.
func cypherStringPredicateTextOperand(operand pgsql.Expression) (pgsql.Expression, error) {
	if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(operand); isPropertyLookup {
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return propertyLookup, nil
	}

	if parenthetical, isParenthetical := operand.(*pgsql.Parenthetical); isParenthetical {
		return TypeCastExpression(parenthetical, pgsql.Text)
	}

	return pgsql.NewTypeCast(operand, pgsql.Text), nil
}

// cypherStringPredicateFunction maps a Cypher string predicate operator to its PostgreSQL helper function.
func cypherStringPredicateFunction(function pgsql.Identifier, lOperand, rOperand pgsql.Expression) (pgsql.Expression, error) {
	leftText, err := cypherStringPredicateTextOperand(lOperand)
	if err != nil {
		return nil, err
	}

	rightText, err := cypherStringPredicateTextOperand(rOperand)
	if err != nil {
		return nil, err
	}

	return pgsql.FunctionCall{
		Function: function,
		Parameters: []pgsql.Expression{
			leftText,
			rightText,
		},
		CastType: pgsql.Boolean,
	}, nil
}

// lowerPropertyLookupArgument returns the property lookup wrapped by a lower(...) function call, if any.
func lowerPropertyLookupArgument(operand pgsql.Expression) (pgsql.Expression, bool) {
	if typeCast, isTypeCast := operand.(pgsql.TypeCast); isTypeCast {
		operand = typeCast.Expression
	}

	if functionCall, isFunctionCall := operand.(pgsql.FunctionCall); isFunctionCall && functionCall.Function == pgsql.FunctionToLower && len(functionCall.Parameters) == 1 {
		if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(functionCall.Parameters[0]); isPropertyLookup {
			return propertyLookup, true
		}
	}

	return nil, false
}

// caseInsensitiveLikeExpression rewrites a like comparison against lower(property) into an ilike comparison against
// the bare property, which is eligible for trigram index acceleration, followed by the original comparison to preserve
// exact Cypher semantics. Comparisons that do not match this shape are returned unchanged.
func caseInsensitiveLikeExpression(likeExpression *pgsql.BinaryExpression, indexPattern pgsql.Expression) pgsql.Expression {
	if propertyLookup, isLowerPropertyLookup := lowerPropertyLookupArgument(likeExpression.LOperand); isLowerPropertyLookup {
		return pgsql.NewParenthetical(
			pgsql.NewBinaryExpression(
				pgsql.NewBinaryExpression(propertyLookup, pgsql.OperatorILike, indexPattern),
				pgsql.OperatorAnd,
				likeExpression,
			),
		)
	}

	return likeExpression
}

func cypherStringLikePattern(function pgsql.Identifier, rightText pgsql.Expression) (pgsql.Expression, error) {
	var (
		wildcard                  = pgsql.NewLiteral("%", pgsql.Text)
		pattern  pgsql.Expression = pgsql.FunctionCall{
			Function:   pgsql.FunctionCypherLikeEscape,
			Parameters: []pgsql.Expression{rightText},
			CastType:   pgsql.Text,
		}
	)

	switch function {
	case pgsql.FunctionCypherContains:
		pattern = pgsql.NewBinaryExpression(
			wildcard,
			pgsql.OperatorConcatenate,
			pgsql.NewBinaryExpression(pattern, pgsql.OperatorConcatenate, wildcard),
		)

	case pgsql.FunctionCypherStartsWith:
		pattern = pgsql.NewBinaryExpression(pattern, pgsql.OperatorConcatenate, wildcard)

	case pgsql.FunctionCypherEndsWith:
		pattern = pgsql.NewBinaryExpression(wildcard, pgsql.OperatorConcatenate, pattern)

	default:
		return nil, fmt.Errorf("unsupported string predicate function %s", function)
	}

	return pgsql.NewTypeCast(pattern, pgsql.Text), nil
}

// cypherStringPredicateExpression translates a Cypher string predicate with a non-literal right operand. Right
// operands that do not reference any row data are translated to an escaped like pattern so that the comparison
// remains eligible for index acceleration. All other right operands are translated to the matching helper function.
func cypherStringPredicateExpression(function pgsql.Identifier, lOperand, rOperand pgsql.Expression) (pgsql.Expression, error) {
	if !isStaticIDEqualityOperand(rOperand) {
		return cypherStringPredicateFunction(function, lOperand, rOperand)
	}

	leftText, err := cypherStringPredicateTextOperand(lOperand)
	if err != nil {
		return nil, err
	}

	rightText, err := cypherStringPredicateTextOperand(rOperand)
	if err != nil {
		return nil, err
	}

	pattern, err := cypherStringLikePattern(function, rightText)
	if err != nil {
		return nil, err
	}

	indexPattern, err := cypherStringLikePattern(function, rightText)
	if err != nil {
		return nil, err
	}

	return caseInsensitiveLikeExpression(pgsql.NewBinaryExpression(leftText, pgsql.OperatorLike, pattern), indexPattern), nil
}

// rewriteBinaryExpression applies operator-specific casts, wildcard escaping, and Cypher null semantics before pushing the result.
func (s *ExpressionTreeTranslator) rewriteBinaryExpression(newExpression *pgsql.BinaryExpression) error {
	if dynamic, err := requiresDynamicArithmetic(newExpression); err != nil {
		return err
	} else if dynamic {
		left, err := arithmeticJSONOperand(newExpression.LOperand)
		if err != nil {
			return err
		}
		right, err := arithmeticJSONOperand(newExpression.ROperand)
		if err != nil {
			return err
		}
		s.PushOperand(pgsql.FunctionCall{
			Function:   pgsql.FunctionCypherArithmetic,
			Parameters: []pgsql.Expression{left, right, pgsql.NewLiteral(string(newExpression.Operator), pgsql.Text)},
			CastType:   pgsql.JSONB,
		})
		return nil
	}

	switch newExpression.Operator {
	case pgsql.OperatorAdd:
		// In the case of the use of the cypher `+` operator we must attempt to disambiguate if the intent
		// is to concatenate or to perform an addition
		if lOperandType, err := InferExpressionType(newExpression.LOperand); err != nil {
			return err
		} else if rOperandType, err := InferExpressionType(newExpression.ROperand); err != nil {
			return err
		} else if isConcatenationOperation(lOperandType, rOperandType) {
			newExpression.Operator = pgsql.OperatorConcatenate
		}

		s.PushOperand(newExpression)

	case pgsql.OperatorCypherContains, pgsql.OperatorCypherStartsWith, pgsql.OperatorCypherEndsWith, pgsql.OperatorCypherRegexMatch:
		predicate, err := typedStringPredicate(newExpression)
		if err != nil {
			return err
		}
		s.PushOperand(predicate)

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

		// Literal, parameter and entity operands keep the assembled IS expression:
		// PostgreSQL evaluates it with Cypher's null predicate semantics.
		s.PushOperand(newExpression)

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

		// Literal, parameter and entity operands keep the assembled IS NOT expression:
		// PostgreSQL evaluates it with Cypher's null predicate semantics.
		s.PushOperand(newExpression)

	case pgsql.OperatorIn:
		if isKnownEmptyArrayExpression(newExpression.ROperand) {
			s.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
			return nil
		}
		if rightType, err := InferExpressionType(newExpression.ROperand); err != nil {
			return err
		} else if rightType == pgsql.JSONBArray {
			value, err := arithmeticJSONOperand(newExpression.LOperand)
			if err != nil {
				return err
			}
			s.PushOperand(pgsql.NewBinaryExpression(value, pgsql.OperatorEquals,
				pgsql.NewAnyExpression(newExpression.ROperand, pgsql.JSONBArray)))
			return nil
		}

		newExpression.Operator = pgsql.OperatorEquals

		switch typedROperand := newExpression.ROperand.(type) {
		case pgsql.TypeCast:
			rewroteTypeCast := false

			switch typedInnerOperand := typedROperand.Expression.(type) {
			case *pgsql.BinaryExpression:
				if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(typedInnerOperand); isPropertyLookup {
					// Attempt to figure out the cast by looking at the left operand
					if leftHint, err := InferExpressionType(newExpression.LOperand); err != nil {
						return err
					} else if leftHint.IsArrayType() {
						s.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
						return nil
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
						rewroteTypeCast = true
					}
				}
			}

			if !rewroteTypeCast {
				if lOperandTypeHint, err := InferExpressionType(newExpression.LOperand); err != nil {
					return err
				} else if lOperandTypeHint.IsArrayType() {
					s.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
					return nil
				} else {
					newExpression.ROperand = pgsql.NewAnyExpression(newExpression.ROperand, typedROperand.TypeHint())
				}
			}

		case pgsql.TypeHinted:
			if lOperandTypeHint, err := InferExpressionType(newExpression.LOperand); err != nil {
				return err
			} else if lOperandTypeHint.IsArrayType() {
				s.PushOperand(pgsql.NewLiteral(false, pgsql.Boolean))
				return nil
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
		if err := applyBinaryExpressionTypeHints(s.kindMapper, newExpression); err != nil {
			return err
		}

		if propertyLookup, hasEmptyArrayLiteralPropertyComparison := isEmptyArrayLiteralPropertyComparison(newExpression); hasEmptyArrayLiteralPropertyComparison {
			expandedExpression := buildEmptyArrayPropertyComparison(propertyLookup, false)

			if err := applyBinaryExpressionTypeHints(s.kindMapper, expandedExpression); err != nil {
				return err
			}

			s.PushOperand(pgsql.NewParenthetical(expandedExpression))
		} else if rewrittenExpression, rewritten := buildStringPropertyEqualityPredicate(newExpression); rewritten {
			s.PushOperand(rewrittenExpression)
		} else {
			s.PushOperand(newExpression)
		}

	case pgsql.OperatorCypherNotEquals:
		if err := applyBinaryExpressionTypeHints(s.kindMapper, newExpression); err != nil {
			return err
		}

		if propertyLookup, hasEmptyArrayLiteralPropertyComparison := isEmptyArrayLiteralPropertyComparison(newExpression); hasEmptyArrayLiteralPropertyComparison {
			expandedExpression := buildEmptyArrayPropertyComparison(propertyLookup, true)

			if err := applyBinaryExpressionTypeHints(s.kindMapper, expandedExpression); err != nil {
				return err
			}

			s.PushOperand(pgsql.NewParenthetical(expandedExpression))
		} else if rewrittenExpression, rewritten := buildStringPropertyEqualityPredicate(newExpression); rewritten {
			s.PushOperand(rewrittenExpression)
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
}

func (s *ExpressionTreeTranslator) PopParenthetical() (*pgsql.Parenthetical, error) {
	if operand, err := s.treeBuilder.PopOperand(s.kindMapper); err != nil {
		return nil, err
	} else if parentheticalExpr, typeOK := operand.(*pgsql.Parenthetical); !typeOK {
		return nil, fmt.Errorf("expected type *pgsql.Parenthetical but received %T", operand)
	} else {
		return parentheticalExpr, nil
	}
}

// VisitOperator is retained for callers that visit operators before their operands.
// Operators are assembled on completion; only explicit predicate extraction adds constraints.
func (s *ExpressionTreeTranslator) VisitOperator(operator pgsql.Operator) {}

// booleanOperandCount preserves the builder's omission of empty Boolean groups.
// Parsed Cypher always has operands, but builders can nest query.And() placeholders.
func booleanOperandCount(expressions cypher.ExpressionList) int {
	count := 0
	for _, expression := range expressions.GetAll() {
		switch typedExpression := expression.(type) {
		case *cypher.Conjunction:
			if booleanOperandCount(typedExpression) == 0 {
				continue
			}
		case *cypher.Disjunction:
			if booleanOperandCount(typedExpression) == 0 {
				continue
			}
		case *cypher.ExclusiveDisjunction:
			if booleanOperandCount(typedExpression) == 0 {
				continue
			}
		}
		count++
	}
	return count
}

func (s *ExpressionTreeTranslator) CompleteBinaryExpression(scope *Scope, operator pgsql.Operator) error {
	newExpression, err := s.popBinaryExpressionOperands(operator)
	if err != nil {
		return err
	}
	if operator == pgsql.OperatorIn {
		if membership, rewritten, err := rewriteEntityMembership(scope, newExpression); err != nil {
			return err
		} else if rewritten {
			s.PushOperand(membership)
			return nil
		}
	}
	if operator.IsIn(pgsql.OperatorAdd, pgsql.OperatorSubtract, pgsql.OperatorMultiply, pgsql.OperatorDivide, "%", "^") || isOrderedComparison(operator) || isStringPredicate(operator) || operator.IsIn(pgsql.OperatorEquals, pgsql.OperatorNotEquals, pgsql.OperatorCypherNotEquals, pgsql.OperatorIs, pgsql.OperatorIsNot, pgsql.OperatorIn) {
		// WITH carries types in the scope. Expose scalar types before operand
		// hinting can cast JSONB to text and erase runtime value information.
		for _, operand := range []*pgsql.Expression{&newExpression.LOperand, &newExpression.ROperand} {
			if identifier, ok := unwrapParenthetical(*operand).(pgsql.Identifier); ok {
				if dataType, found := scope.LookupDataType(identifier); found && (dataType == pgsql.JSONB || (operator == pgsql.OperatorIn && dataType == pgsql.JSONBArray) || ((isStringPredicate(operator) || isOrderedComparison(operator)) && dataType.IsKnown() && !expressionHasCompositeProperties(dataType))) {
					*operand = pgsql.NewTypeCast(pgsql.NewParenthetical(*operand), dataType)
					if dataType == pgsql.JSONB {
						*operand = pgsql.FunctionCall{Function: pgsql.FunctionNullIf,
							Parameters: []pgsql.Expression{*operand, jsonNullLiteral()}, CastType: pgsql.JSONB}
					}
				}
			}
		}
	}
	if err := exposeCollectArrayOperands(scope, newExpression); err != nil {
		return err
	}
	if operator.IsIn(pgsql.OperatorAnd, pgsql.OperatorOr) {
		for _, operand := range []*pgsql.Expression{&newExpression.LOperand, &newExpression.ROperand} {
			dataType, err := InferExpressionType(*operand)
			if err != nil {
				return err
			}
			if identifier, ok := unwrapParenthetical(*operand).(pgsql.Identifier); ok {
				if scopedType, found := scope.LookupDataType(identifier); found {
					dataType = scopedType
				}
			}
			if dataType == pgsql.JSONB {
				*operand = jsonBooleanOperand(*operand)
			}
		}
	}
	if err := applyBinaryExpressionTypeHints(s.kindMapper, newExpression); err != nil {
		return err
	}
	if err := rewriteIdentityOperands(scope, newExpression); err != nil {
		return err
	}
	return s.rewriteBinaryExpression(newExpression)
}

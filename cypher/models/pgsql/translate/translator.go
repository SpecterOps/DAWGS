package translate

import (
	"context"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

// DefaultGraphID is the graph_id used by callers that do not have a specific
// graph target available (tests, tooling, and visualization passes that only
// exercise translation output).
const DefaultGraphID int32 = 0

type Translator struct {
	walk.Visitor[cypher.SyntaxNode]

	ctx            context.Context
	kindMapper     *contextAwareKindMapper
	graphID        int32
	parameters     map[string]any
	translation    Result
	treeTranslator *ExpressionTreeTranslator
	query          *Query
	scope          *Scope
	unwindTargets  map[*cypher.Variable]struct{}
}

func NewTranslator(ctx context.Context, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32) *Translator {
	if parameters == nil {
		parameters = map[string]any{}
	}

	inputParameters := make(map[string]any, len(parameters))
	for key, value := range parameters {
		inputParameters[key] = value
	}

	translatedParameters := map[string]any{}
	ctxAwareKindMapper := newContextAwareKindMapper(ctx, kindMapper, translatedParameters)

	return &Translator{
		Visitor: walk.NewVisitor[cypher.SyntaxNode](),
		translation: Result{
			Parameters: translatedParameters,
		},
		ctx:            ctx,
		kindMapper:     ctxAwareKindMapper,
		graphID:        graphID,
		parameters:     inputParameters,
		treeTranslator: NewExpressionTreeTranslator(ctxAwareKindMapper),
		query:          &Query{},
		scope:          NewScope(),
		unwindTargets:  map[*cypher.Variable]struct{}{},
	}
}

func (s *Translator) Enter(expression cypher.SyntaxNode) {
	switch typedExpression := expression.(type) {
	case *cypher.RegularQuery, *cypher.SingleQuery, *cypher.PatternElement,
		*cypher.Comparison, *cypher.Skip, *cypher.Limit, cypher.Operator, *cypher.ArithmeticExpression,
		*cypher.NodePattern, *cypher.RelationshipPattern, *cypher.Remove, *cypher.Set,
		*cypher.ReadingClause, *cypher.UnaryAddOrSubtractExpression, *cypher.PropertyLookup,
		*cypher.Negation, *cypher.Where, *cypher.ListLiteral,
		*cypher.FunctionInvocation, *cypher.Order, *cypher.RemoveItem, *cypher.SetItem,
		*cypher.MapItem, *cypher.UpdatingClause, *cypher.Delete, *cypher.With,
		*cypher.Return, *cypher.MultiPartQuery, *cypher.Properties, *cypher.KindMatcher,
		*cypher.Quantifier, *cypher.IDInCollection:

	case *cypher.Unwind:
		if typedExpression.Variable != nil {
			// The UNWIND target is declared by the UNWIND clause itself, so later
			// variable visits for the same syntax node must not resolve through
			// the normal outer-scope lookup path.
			s.unwindTargets[typedExpression.Variable] = struct{}{}
		}

	case *cypher.Create:
		// CREATE pattern nodes and relationships are collected first, then
		// translated into mutation CTEs after the full pattern is known.
		currentQueryPart := s.query.CurrentPart()
		currentQueryPart.currentPattern = &Pattern{}
		currentQueryPart.isCreating = true

	case *cypher.MultiPartQueryPart:
		if err := s.prepareMultiPartQueryPart(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.SinglePartQuery:
		if err := s.prepareSinglePartQueryPart(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Match:
		s.query.CurrentPart().currentPattern = &Pattern{}

	case graph.Kinds:
		s.treeTranslator.PushOperand(pgsql.KindListLiteral{
			Values: typedExpression,
		})

	case *cypher.Parameter:
		var (
			cypherIdentifier = pgsql.Identifier(typedExpression.Symbol)
			binding, bound   = s.scope.AliasedLookup(cypherIdentifier)
		)

		if !bound {
			if parameterBinding, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
				s.SetError(err)
			} else {
				// Alias the old parameter identifier to the synthetic one
				if cypherIdentifier != "" {
					s.scope.Alias(cypherIdentifier, parameterBinding)
				}

				parameterValue := s.resolveParameterValue(typedExpression)

				// Create a new container for the parameter and its value
				if newParameter, err := pgsql.AsParameter(parameterBinding.Identifier, parameterValue); err != nil {
					s.SetError(err)
				} else if negotiatedValue, err := pgsql.NegotiateValue(parameterValue); err != nil {
					s.SetError(err)
				} else {
					// Lift the parameter value into the parameters map
					s.translation.Parameters[parameterBinding.Identifier.String()] = negotiatedValue
					parameterBinding.Parameter = newParameter
				}

				// Set the outer reference
				binding = parameterBinding
			}
		}

		s.treeTranslator.PushOperand(binding.Parameter)

	case *cypher.Variable:
		if binding, isUnwindTarget, err := s.prepareUnwindTarget(typedExpression); err != nil {
			s.SetError(err)
		} else if isUnwindTarget {
			s.treeTranslator.PushOperand(binding.Identifier)
		} else {
			identifier := pgsql.Identifier(typedExpression.Symbol)

			if binding, resolved := s.scope.AliasedLookup(identifier); !resolved {
				s.SetErrorf("unable to resolve or otherwise lookup identifer %s", identifier)
			} else {
				s.treeTranslator.PushOperand(binding.Identifier)
			}
		}

	case *cypher.Literal:
		literalValue := typedExpression.Value

		if stringValue, isString := typedExpression.Value.(string); isString {
			if decoded, err := decodeCypherStringLiteral(stringValue); err != nil {
				s.SetError(err)
			} else {
				literalValue = decoded
			}
		}

		if newLiteral, err := pgsql.AsLiteral(literalValue); err != nil {
			s.SetError(err)
		} else {
			newLiteral.Null = typedExpression.Null
			s.treeTranslator.PushOperand(newLiteral)
		}

	case *cypher.Parenthetical:
		s.treeTranslator.PushParenthetical()

	case *cypher.SortItem:
		if err := s.ensureSortItemProjectionAliases(); err != nil {
			s.SetError(err)
		}

		s.query.CurrentPart().SortItems = append(s.query.CurrentPart().SortItems, pgsql.NewOrderBy(typedExpression.Ascending))

	case *cypher.Projection:
		if err := s.prepareProjection(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.ProjectionItem:
		s.query.CurrentPart().PrepareProjection()

	case *cypher.PatternPredicate:
		if err := s.preparePatternPredicate(); err != nil {
			s.SetError(err)
		}

	case *cypher.PatternPart:
		if err := s.translatePatternPart(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.PartialComparison:
		s.treeTranslator.VisitOperator(pgsql.Operator(typedExpression.Operator))

	case *cypher.PartialArithmeticExpression:
		s.treeTranslator.VisitOperator(pgsql.Operator(typedExpression.Operator))

	case *cypher.Disjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			s.treeTranslator.VisitOperator(pgsql.OperatorOr)
		}

	case *cypher.Conjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			s.treeTranslator.VisitOperator(pgsql.OperatorAnd)
		}

	case *cypher.FilterExpression:
		if err := s.prepareFilterExpression(typedExpression); err != nil {
			s.SetError(err)
		}

	default:
		s.SetErrorf("unable to translate cypher type: %T", expression)
	}
}

func (s *Translator) resolveParameterValue(parameter *cypher.Parameter) any {
	if value, hasValue := s.parameters[parameter.Symbol]; hasValue {
		return value
	}

	return parameter.Value
}

func coalescePropertyLookupExpression(expression pgsql.Expression) pgsql.Expression {
	if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(expression); isPropertyLookup {
		return pgsql.FunctionCall{
			Function: pgsql.FunctionCoalesce,
			Parameters: []pgsql.Expression{
				propertyLookup,
				pgsql.NewLiteral("", pgsql.Text),
			},
			CastType: pgsql.Text,
		}
	}

	return expression
}

func rewriteNegatedStringPredicateExpression(expression pgsql.Expression) pgsql.Expression {
	switch typedExpression := expression.(type) {
	case *pgsql.Parenthetical:
		typedExpression.Expression = rewriteNegatedStringPredicateExpression(typedExpression.Expression)
		return typedExpression

	case *pgsql.BinaryExpression:
		switch typedExpression.Operator {
		case pgsql.OperatorLike, pgsql.OperatorILike:
			// If this is a string comparison operation then the negation requires wrapping the
			// operand references in coalesce functions. While this will kick out index acceleration
			// the negation will already damage the query planner's ability to utilize an index lookup.
			typedExpression.LOperand = coalescePropertyLookupExpression(typedExpression.LOperand)
			typedExpression.ROperand = coalescePropertyLookupExpression(typedExpression.ROperand)
		}

	case pgsql.FunctionCall:
		switch typedExpression.Function {
		case pgsql.FunctionCypherContains, pgsql.FunctionCypherStartsWith, pgsql.FunctionCypherEndsWith:
			for idx, parameter := range typedExpression.Parameters {
				typedExpression.Parameters[idx] = coalescePropertyLookupExpression(parameter)
			}
		}

		return typedExpression
	}

	return expression
}

func (s *Translator) Exit(expression cypher.SyntaxNode) {
	switch typedExpression := expression.(type) {

	case *cypher.IDInCollection:
		if err := s.translateIDInCollection(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.FilterExpression:
		if err := s.translateFilterExpression(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Quantifier:
		if err := s.buildQuantifier(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.NodePattern:
		if err := s.translateNodePattern(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.RelationshipPattern:
		if err := s.translateRelationshipPattern(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.MapItem:
		if value, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.query.CurrentPart().AddProperty(typedExpression.Key, value)
		}

	case *cypher.Properties:
		if typedExpression.Parameter != nil {
			if value, err := s.treeTranslator.PopOperand(); err != nil {
				s.SetError(err)
			} else {
				s.query.CurrentPart().AddPropertyParameter(value)
			}
		}

	case *cypher.PatternPredicate:
		if err := s.translatePatternPredicate(); err != nil {
			s.SetError(err)
		}

	case *cypher.RemoveItem:
		if err := s.translateRemoveItem(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Delete:
		if err := s.translateDelete(s.scope, typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Create:
		if err := s.translateCreate(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.SetItem:
		if err := s.translateSetItem(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.UpdatingClause:
		if err := s.translateUpdates(); err != nil {
			s.SetError(err)
		}

	case *cypher.ListLiteral:
		var (
			numExpressions = len(typedExpression.Expressions())
			literal        = pgsql.ArrayLiteral{
				Values:   make([]pgsql.Expression, numExpressions),
				CastType: pgsql.UnsetDataType,
			}
		)

		for idx := numExpressions - 1; idx >= 0; idx-- {
			if nextExpression, err := s.treeTranslator.PopOperand(); err != nil {
				s.SetError(err)
			} else {
				if typeHint, isTypeHinted := nextExpression.(pgsql.TypeHinted); isTypeHinted {
					if arrayCastType, err := typeHint.TypeHint().ToArrayType(); err != nil {
						s.SetError(err)
					} else if literal.CastType != pgsql.UnsetDataType && literal.CastType != arrayCastType {
						s.SetErrorf("expected array literal value type %s at index %d but found type %s", literal.CastType, idx, arrayCastType)
					} else {
						literal.CastType = arrayCastType
					}
				}

				literal.Values[idx] = nextExpression
			}
		}

		if numExpressions == 0 && literal.CastType == pgsql.UnsetDataType {
			literal.CastType = pgsql.AnyArray
		}

		if literal.CastType == pgsql.UnsetDataType {
			s.SetErrorf("array literal has no available type hints")
		} else {
			s.treeTranslator.PushOperand(literal)
		}

	case *cypher.SortItem:
		// Rewrite the order by constraints
		if lookupExpression, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if err := RewriteFrameBindings(s.scope, lookupExpression); err != nil {
			s.SetError(err)
		} else {
			if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(lookupExpression); isPropertyLookup {
				// If sorting, use the raw type of the JSONB field
				propertyLookup.Operator = pgsql.OperatorJSONField
			}

			s.query.CurrentPart().CurrentOrderBy().Expression = lookupExpression
		}

	case *cypher.KindMatcher:
		if err := s.translateKindMatcher(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Parenthetical:
		// Pull the sub-expression we wrap
		if wrappedExpression, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else if parenthetical, err := s.treeTranslator.PopParenthetical(); err != nil {
			s.SetError(err)
		} else {
			parenthetical.Expression = wrappedExpression
			s.treeTranslator.PushOperand(parenthetical)
		}

	case *cypher.FunctionInvocation:
		s.translateFunction(typedExpression)

	case *cypher.UnaryAddOrSubtractExpression:
		if operand, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(&pgsql.UnaryExpression{
				Operator: pgsql.Operator(typedExpression.Operator),
				Operand:  operand,
			})
		}

	case *cypher.Negation:
		if operand, err := s.treeTranslator.PopOperand(); err != nil {
			s.SetError(err)
		} else {
			s.treeTranslator.PushOperand(&pgsql.UnaryExpression{
				Operator: pgsql.OperatorNot,
				Operand:  rewriteNegatedStringPredicateExpression(operand),
			})
		}

	case *cypher.Where:
		// Assign the last operands as identifier set constraints
		if err := s.treeTranslator.PopRemainingExpressionsAsUserConstraints(); err != nil {
			s.SetError(err)
		}

	case *cypher.PropertyLookup:
		if err := s.translatePropertyLookup(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.PartialComparison:
		if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.Operator(typedExpression.Operator)); err != nil {
			s.SetError(err)
		}

	case *cypher.PartialArithmeticExpression:
		if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.Operator(typedExpression.Operator)); err != nil {
			s.SetError(err)
		}

	case *cypher.Disjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorOr); err != nil {
				s.SetError(err)
			}
		}

	case *cypher.Conjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorAnd); err != nil {
				s.SetError(err)
			}
		}

	case *cypher.ProjectionItem:
		if err := s.translateProjectionItem(s.scope, typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Match:
		if err := s.translateMatch(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Unwind:
		if err := s.translateUnwind(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.With:
		if err := s.translateWith(); err != nil {
			s.SetError(err)
		}

	case *cypher.MultiPartQueryPart:
		if err := s.translateMultiPartQueryPart(); err != nil {
			s.SetError(err)
		}

	case *cypher.SinglePartQuery:
		if err := s.buildSinglePartQuery(typedExpression); err != nil {
			s.SetError(err)
		}

		s.translation.Statement = *s.query.CurrentPart().Model

	case *cypher.MultiPartQuery:
		if err := s.buildMultiPartQuery(typedExpression.SinglePartQuery); err != nil {
			s.SetError(err)
		}
	}
}

type Result struct {
	Statement  pgsql.Statement
	Parameters map[string]any
}

func Translate(ctx context.Context, cypherQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32) (Result, error) {
	translator := NewTranslator(ctx, kindMapper, parameters, graphID)

	if err := walk.Cypher(cypherQuery, translator); err != nil {
		return Result{}, err
	}

	return translator.translation, nil
}

func decodeCypherStringLiteral(raw string) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("invalid cypher string literal: %q", raw)
	} else if quote := raw[0]; (quote != '\'' && quote != '"') || raw[len(raw)-1] != quote {
		return "", fmt.Errorf("invalid cypher string literal: missing or mismatched surrounding quotes: %q", raw)
	}
	// Cypher parser wraps string literals with ' characters
	body := raw[1 : len(raw)-1]
	var b strings.Builder
	b.Grow(len(body))
	for i := 0; i < len(body); i++ {
		if body[i] != '\\' {
			b.WriteByte(body[i])
			continue
		}
		if i+1 >= len(body) {
			return "", fmt.Errorf("dangling escape in string literal")
		}
		switch c := body[i+1]; c {
		case '\\', '\'', '"':
			b.WriteByte(c)
			i++
		case 'b', 'B':
			b.WriteByte('\b')
			i++
		case 'f', 'F':
			b.WriteByte('\f')
			i++
		case 'n', 'N':
			b.WriteByte('\n')
			i++
		case 'r', 'R':
			b.WriteByte('\r')
			i++
		case 't', 'T':
			b.WriteByte('\t')
			i++
		default:
			return "", fmt.Errorf("invalid escape \\%c", c)
		}
	}
	return b.String(), nil
}

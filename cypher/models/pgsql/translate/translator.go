package translate

import (
	"context"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

// DefaultGraphID selects graph zero for tests and tooling that do not target a concrete graph.
const DefaultGraphID int32 = 0

// Translator walks an optimized Cypher AST and constructs the corresponding PostgreSQL AST.
type Translator struct {
	// Visitor supplies traversal control and error propagation for the Cypher walk.
	walk.Visitor[cypher.SyntaxNode]

	// ctx carries cancellation and deadlines through translation.
	ctx context.Context
	// kindMapper resolves graph kind names within the translation context.
	kindMapper *contextAwareKindMapper
	// graphID identifies the concrete graph partitions targeted by generated SQL.
	graphID int32
	// parameters is an isolated copy of the caller's Cypher parameter values.
	parameters map[string]any
	// translation accumulates the statement, generated parameters, and diagnostics.
	translation Result
	// treeTranslator lowers the current Cypher expression tree into PostgreSQL expressions.
	treeTranslator *ExpressionTreeTranslator
	// query holds the PostgreSQL query model under construction.
	query *Query
	// scope tracks translated bindings and their materialization frames.
	scope *Scope
	// unwindTargets contains UNWIND variables awaiting source translation.
	unwindTargets map[*cypher.Variable]struct{}

	// collectIDMembershipAliases identifies collect projections eligible to carry scalar entity IDs.
	collectIDMembershipAliases map[pgsql.Identifier]struct{}
	// collectIDProjectionDepth tracks nesting within an ID-only collect projection.
	collectIDProjectionDepth int

	// appliedLoweringCounts counts emitted applications of each planned lowering.
	appliedLoweringCounts map[string]int
	// appliedShortestPathExecutors records the physical executor emitted for each optimized traversal.
	appliedShortestPathExecutors map[optimize.TraversalStepTarget]optimize.ShortestPathExecutor
	// appliedExpansionSearchStrategies records the physical search emitted for each optimized expansion.
	appliedExpansionSearchStrategies map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategy
	// emittedExpansionSearchPolicies records runtime selection policies emitted for optimized expansions.
	emittedExpansionSearchPolicies map[optimize.TraversalStepTarget]optimize.ExpansionSearchPolicy
	// patternTargets maps source pattern parts to their stable optimizer coordinates.
	patternTargets map[*cypher.PatternPart]optimize.PatternTarget
	// patternPredicateTargets maps source pattern predicates to their stable optimizer coordinates.
	patternPredicateTargets map[*cypher.PatternPredicate]optimize.PatternTarget
	// projectionPruningDecisions indexes planned projection omissions by traversal target.
	projectionPruningDecisions map[optimize.TraversalStepTarget]optimize.ProjectionPruningDecision
	// latePathDecisions indexes deferred path-materialization decisions by traversal target.
	latePathDecisions map[optimize.TraversalStepTarget][]optimize.LatePathMaterializationDecision
	// suffixPushdownDecisions indexes fixed-suffix pushdown decisions by traversal target.
	suffixPushdownDecisions map[optimize.TraversalStepTarget][]optimize.ExpansionSuffixPushdownDecision
	// predicatePlacementDecisions indexes predicate attachment decisions by traversal target.
	predicatePlacementDecisions map[optimize.TraversalStepTarget][]optimize.PredicatePlacementDecision
	// expandIntoDecisions indexes bound-endpoint expansion choices by traversal target.
	expandIntoDecisions map[optimize.TraversalStepTarget]optimize.ExpandIntoDecision
	// traversalDirectionDecisions indexes physical traversal direction choices by traversal target.
	traversalDirectionDecisions map[optimize.TraversalStepTarget]optimize.TraversalDirectionDecision
	// shortestPathStrategyDecisions indexes directional shortest-path search choices by traversal target.
	shortestPathStrategyDecisions map[optimize.TraversalStepTarget]optimize.ShortestPathStrategyDecision
	// shortestPathFilterDecisions indexes shortest-path filter decisions by traversal target.
	shortestPathFilterDecisions map[optimize.TraversalStepTarget][]optimize.ShortestPathFilterDecision
	// shortestPathExecutorDecisions indexes planned shortest-path executor choices by traversal target.
	shortestPathExecutorDecisions map[optimize.TraversalStepTarget]optimize.ShortestPathExecutorDecision
	// expansionSearchStrategyDecisions indexes planned variable-expansion strategies by traversal target.
	expansionSearchStrategyDecisions map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategyDecision
	// limitPushdownDecisions indexes planned traversal limits by source target.
	limitPushdownDecisions map[optimize.TraversalStepTarget][]optimize.LimitPushdownDecision
	// patternPredicateDecisions indexes planned existence lowering by traversal target.
	patternPredicateDecisions map[optimize.TraversalStepTarget]optimize.PatternPredicatePlacementDecision
	// exactRangeExpansionDecisions indexes fixed-depth unrolling choices by source target.
	exactRangeExpansionDecisions map[optimize.TraversalStepTarget]optimize.ExactRangeExpansionDecision
	// pathRelationshipPredicateDecisions indexes path quantifier lowering by stable quantifier target.
	pathRelationshipPredicateDecisions map[optimize.QuantifierTarget]optimize.PathRelationshipPredicateDecision
	// fieldRequirementDecisions indexes binding representation requirements by query part and symbol.
	fieldRequirementDecisions map[int]map[string]optimize.FieldRequirementDecision
	// quantifierTargets records stable coordinates for visited quantified traversals.
	quantifierTargets []optimize.QuantifierTarget
}

// NewTranslator initializes translation state for the supplied graph and copies the caller's parameter map.
func NewTranslator(ctx context.Context, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32) *Translator {
	if parameters == nil {
		parameters = map[string]any{}
	}

	inputParameters := make(map[string]any, len(parameters))
	for key, value := range parameters {
		inputParameters[key] = value
	}

	var (
		translatedParameters = map[string]any{}
		ctxAwareKindMapper   = newContextAwareKindMapper(ctx, kindMapper, translatedParameters)
	)

	translator := &Translator{
		Visitor: walk.NewVisitor[cypher.SyntaxNode](),
		translation: Result{
			Parameters:       translatedParameters,
			ParameterSources: map[string]string{},
			GraphID:          graphID,
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

	translator.scope.SetGraphID(graphID)
	return translator
}

// SetOptimizationPlan indexes lowering decisions by their stable targets for use during AST traversal.
func (s *Translator) SetOptimizationPlan(plan optimize.Plan) {
	s.patternTargets = optimize.IndexPatternTargets(plan.Query)
	s.patternPredicateTargets = optimize.IndexPatternPredicateTargets(plan.Query)
	s.projectionPruningDecisions = map[optimize.TraversalStepTarget]optimize.ProjectionPruningDecision{}
	s.latePathDecisions = map[optimize.TraversalStepTarget][]optimize.LatePathMaterializationDecision{}
	s.suffixPushdownDecisions = map[optimize.TraversalStepTarget][]optimize.ExpansionSuffixPushdownDecision{}
	s.predicatePlacementDecisions = map[optimize.TraversalStepTarget][]optimize.PredicatePlacementDecision{}
	s.expandIntoDecisions = map[optimize.TraversalStepTarget]optimize.ExpandIntoDecision{}
	s.traversalDirectionDecisions = map[optimize.TraversalStepTarget]optimize.TraversalDirectionDecision{}
	s.shortestPathStrategyDecisions = map[optimize.TraversalStepTarget]optimize.ShortestPathStrategyDecision{}
	s.shortestPathFilterDecisions = map[optimize.TraversalStepTarget][]optimize.ShortestPathFilterDecision{}
	s.shortestPathExecutorDecisions = map[optimize.TraversalStepTarget]optimize.ShortestPathExecutorDecision{}
	s.expansionSearchStrategyDecisions = map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategyDecision{}
	s.limitPushdownDecisions = map[optimize.TraversalStepTarget][]optimize.LimitPushdownDecision{}
	s.patternPredicateDecisions = map[optimize.TraversalStepTarget]optimize.PatternPredicatePlacementDecision{}
	s.exactRangeExpansionDecisions = map[optimize.TraversalStepTarget]optimize.ExactRangeExpansionDecision{}
	s.pathRelationshipPredicateDecisions = map[optimize.QuantifierTarget]optimize.PathRelationshipPredicateDecision{}
	s.fieldRequirementDecisions = map[int]map[string]optimize.FieldRequirementDecision{}

	for _, decision := range plan.LoweringPlan.ProjectionPruning {
		s.projectionPruningDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.LatePathMaterialization {
		s.latePathDecisions[decision.Target] = append(s.latePathDecisions[decision.Target], decision)
	}

	for _, decision := range plan.LoweringPlan.ExpansionSuffixPushdown {
		s.suffixPushdownDecisions[decision.Target] = append(s.suffixPushdownDecisions[decision.Target], decision)
	}

	for _, decision := range plan.LoweringPlan.PredicatePlacement {
		s.predicatePlacementDecisions[decision.Target] = append(s.predicatePlacementDecisions[decision.Target], decision)
	}

	for _, decision := range plan.LoweringPlan.ExpandInto {
		s.expandIntoDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.TraversalDirection {
		s.traversalDirectionDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.ShortestPathStrategy {
		s.shortestPathStrategyDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.ShortestPathFilter {
		s.shortestPathFilterDecisions[decision.Target] = append(s.shortestPathFilterDecisions[decision.Target], decision)
	}

	for _, decision := range plan.LoweringPlan.ShortestPathExecutor {
		s.shortestPathExecutorDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.ExpansionSearchStrategy {
		s.expansionSearchStrategyDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.LimitPushdown {
		s.limitPushdownDecisions[decision.Target] = append(s.limitPushdownDecisions[decision.Target], decision)
	}

	for _, decision := range plan.LoweringPlan.PatternPredicate {
		s.patternPredicateDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.ExactRangeExpansion {
		s.exactRangeExpansionDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.PathRelationshipPredicate {
		s.pathRelationshipPredicateDecisions[decision.Target] = decision
	}

	for _, decision := range plan.LoweringPlan.FieldRequirements {
		bySymbol := s.fieldRequirementDecisions[decision.QueryPartIndex]
		if bySymbol == nil {
			bySymbol = map[string]optimize.FieldRequirementDecision{}
			s.fieldRequirementDecisions[decision.QueryPartIndex] = bySymbol
		}
		bySymbol[decision.Symbol] = decision
	}
}

// Enter translates a Cypher syntax node when the walker reaches it.
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
		*cypher.IDInCollection:

	case *cypher.Quantifier:
		s.enterQuantifier()

	case *cypher.RangeQuantifier:
		if typedExpression.Value != string(pgsql.WildcardIdentifier) {
			s.SetErrorf("unsupported range quantifier expression: %s", typedExpression.Value)
		} else {
			s.treeTranslator.PushOperand(pgsql.WildcardIdentifier)
		}

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
					if typedExpression.Symbol != "" {
						s.translation.ParameterSources[parameterBinding.Identifier.String()] = typedExpression.Symbol
					}
					parameterBinding.Parameter = newParameter
				}

				// Set the outer reference
				binding = parameterBinding
			}
		}

		s.treeTranslator.PushOperand(binding.Parameter)

	case *cypher.Variable:
		if typedExpression.Symbol == cypher.TokenLiteralAsterisk {
			// Greedy projections are expanded to their named scope bindings when
			// the enclosing projection item is completed.
			s.treeTranslator.PushOperand(pgsql.Identifier(cypher.TokenLiteralAsterisk))
		} else if binding, isUnwindTarget, err := s.prepareUnwindTarget(typedExpression); err != nil {
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
		if typedExpression.Alias != nil {
			if _, collectIDs := s.collectIDMembershipAliases[pgsql.Identifier(typedExpression.Alias.Symbol)]; collectIDs {
				s.collectIDProjectionDepth++
			}
		}
		s.query.CurrentPart().PrepareProjection()

	case *cypher.PatternPredicate:
		if err := s.preparePatternPredicate(typedExpression); err != nil {
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

	case *cypher.ExclusiveDisjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			s.treeTranslator.VisitOperator(pgsql.OperatorNotEquals)
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

// resolveParameterValue returns the caller-supplied value for a Cypher parameter or reports an unknown parameter.
func (s *Translator) resolveParameterValue(parameter *cypher.Parameter) any {
	if value, hasValue := s.parameters[parameter.Symbol]; hasValue {
		return value
	}

	return parameter.Value
}

// coalescePropertyLookupExpression builds a coalesce call from a property lookup and translated fallback operands.
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

// rewriteNegatedStringPredicateExpression preserves Cypher null behavior when negating a string predicate.
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
		s.exitQuantifier()

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

	case *cypher.ExclusiveDisjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			if err := s.treeTranslator.CompleteBinaryExpression(s.scope, pgsql.OperatorNotEquals); err != nil {
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
		if typedExpression.Alias != nil {
			if _, collectIDs := s.collectIDMembershipAliases[pgsql.Identifier(typedExpression.Alias.Symbol)]; collectIDs {
				s.collectIDProjectionDepth--
			}
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

// Result contains the translated PostgreSQL statement, parameters, graph target, and optimization diagnostics.
type Result struct {
	// Statement is the translated PostgreSQL AST.
	Statement pgsql.Statement
	// Parameters contains SQL parameters generated during translation.
	Parameters map[string]any
	// ParameterSources maps generated SQL parameter names back to Cypher parameter names.
	ParameterSources map[string]string
	// Optimization summarizes planned, applied, and skipped lowering decisions.
	Optimization OptimizationSummary
	// GraphID identifies the graph partitions targeted by the statement.
	GraphID int32
}

// OptimizationSummary records which optimizer decisions were planned, applied, or skipped during translation.
type OptimizationSummary struct {
	// Rules contains the semantic optimizer rule results in execution order.
	Rules []optimize.RuleResult `json:"rules,omitempty"`
	// PredicateAttachments records optimizer-selected predicate scopes.
	PredicateAttachments []optimize.PredicateAttachment `json:"predicate_attachments,omitempty"`
	// PlannedLowerings summarizes lowering categories selected by the optimizer.
	PlannedLowerings []optimize.LoweringDecision `json:"planned_lowerings,omitempty"`
	// Lowerings summarizes lowering categories actually emitted by translation.
	Lowerings []optimize.LoweringDecision `json:"lowerings,omitempty"`
	// SkippedLowerings explains planned lowering applications that translation did not emit.
	SkippedLowerings []SkippedLowering `json:"skipped_lowerings,omitempty"`
	// TargetOutcomes reports selection and application results for each lowering target.
	TargetOutcomes []TargetLoweringOutcome `json:"target_outcomes,omitempty"`
	// LoweringPlan exposes the optimizer decisions used to translate the statement.
	LoweringPlan *optimize.LoweringPlan `json:"lowering_plan,omitempty"`
}

// TargetLoweringOutcome reports how one planned lowering target was qualified, selected, and applied.
type TargetLoweringOutcome struct {
	// Lowering names the lowering pass that produced this outcome.
	Lowering string `json:"lowering"`
	// TargetKind identifies the kind of syntax or binding targeted by the lowering.
	TargetKind string `json:"target_kind"`
	// TraversalTarget locates a traversal-step target when the lowering applies to one.
	TraversalTarget *optimize.TraversalStepTarget `json:"traversal_target,omitempty"`
	// QueryPartIndex locates a query-part target when the lowering applies to one.
	QueryPartIndex *int `json:"query_part_index,omitempty"`
	// Symbol identifies a binding target when the lowering applies to one.
	Symbol string `json:"symbol,omitempty"`
	// Family names the candidate-selection family that produced this outcome.
	Family string `json:"family,omitempty"`
	// TraversalFamily preserves the SP/ASP family for analysis-only decisions
	// whose outcome family must remain distinct from an executable traversal.
	TraversalFamily string `json:"traversal_family,omitempty"`
	// PlannedPolicy identifies the runtime policy intended for this candidate
	// family, whether or not it was emitted.
	PlannedPolicy string `json:"planned_policy,omitempty"`
	// EmittedPolicy identifies a runtime policy present in translated SQL. A
	// single incumbent or tool-forced arm has no emitted policy identity.
	EmittedPolicy string `json:"emitted_policy,omitempty"`
	// PlannedCandidates lists the candidates considered in preference order.
	PlannedCandidates []string `json:"planned_candidates,omitempty"`
	// EmittedCandidates lists the arms present in translated SQL. Runtime
	// telemetry separately records which arm executed.
	EmittedCandidates []string `json:"emitted_candidates,omitempty"`
	// ProbeCaps records bounded evidence inputs for an expansion policy.
	ProbeCaps *optimize.ExpansionSearchProbeCaps `json:"probe_caps,omitempty"`
	// Admission records the specialized-state gate and exact fallback chain.
	Admission *optimize.ExpansionSearchAdmission `json:"admission,omitempty"`
	// EndpointRoot and EndpointTerminal describe the bounded endpoint inputs
	// considered by analysis without implying that translation emitted them.
	EndpointRoot     *optimize.EndpointResolutionInput `json:"endpoint_root,omitempty"`
	EndpointTerminal *optimize.EndpointResolutionInput `json:"endpoint_terminal,omitempty"`
	// EndpointPairClass records a correlation class when endpoint resolution
	// must preserve a paired input rather than independent endpoint sets.
	EndpointPairClass optimize.EndpointResolutionClass `json:"endpoint_pair_class,omitempty"`
	// EndpointResolutionCaps records immutable 1/2/32/33 admission sentinels.
	EndpointResolutionCaps *optimize.EndpointResolutionCaps `json:"endpoint_resolution_caps,omitempty"`
	// PredicateClass and its source/index expose conservative traversal
	// predicate placement analysis as a first-class target outcome.
	PredicateClass  optimize.TraversalPredicateClass `json:"predicate_class,omitempty"`
	PredicateSource string                           `json:"predicate_source,omitempty"`
	PredicateIndex  *int                             `json:"predicate_index,omitempty"`
	// Scheduler identifies the selected shortest-path frontier scheduling policy.
	Scheduler string `json:"scheduler,omitempty"`
	// ExecutionBoundary identifies whether the selected executor is inline SQL,
	// a stored helper, or a guarded multi-arm statement.
	ExecutionBoundary string `json:"execution_boundary,omitempty"`
	// Candidate is the specialized candidate proposed by analysis.
	Candidate string `json:"candidate,omitempty"`
	// EligibilityFacts records named qualification checks for the candidate.
	EligibilityFacts []TargetEligibilityFact `json:"eligibility_facts,omitempty"`
	// ObservationMode describes how downstream clauses consume the target.
	ObservationMode string `json:"observation_mode,omitempty"`
	// Direction records the target's logical traversal direction.
	Direction string `json:"direction,omitempty"`
	// PhysicalExpansion records the stored edge endpoint used to advance traversal.
	PhysicalExpansion string `json:"physical_expansion,omitempty"`
	// RelationshipKindCount is the number of statically resolved relationship kinds.
	RelationshipKindCount int `json:"relationship_kind_count,omitempty"`
	// UntypedRelationship reports whether the pattern omitted relationship kinds.
	UntypedRelationship bool `json:"untyped_relationship,omitempty"`
	// TopologyClassification summarizes logical direction, physical direction, and depth.
	TopologyClassification string `json:"topology_classification,omitempty"`
	// Eligible reports the structural qualification result when one is available.
	Eligible *bool `json:"eligible,omitempty"`
	// StaticallyEligible reports the literal- and kind-based qualification result when available.
	StaticallyEligible *bool `json:"statically_eligible,omitempty"`
	// SelectionMode records whether selection was automatic or forced by tooling.
	SelectionMode string `json:"selection_mode,omitempty"`
	// SelectorVersion identifies the policy version that ranked candidates.
	SelectorVersion string `json:"selector_version,omitempty"`
	// Fallback names the candidate used if the preferred lowering was not applied.
	Fallback string `json:"fallback,omitempty"`
	// MinimumDepth is the target's inclusive lower traversal-depth bound.
	MinimumDepth *int64 `json:"minimum_depth,omitempty"`
	// MaximumDepth is the target's inclusive upper traversal-depth bound when finite.
	MaximumDepth *int64 `json:"maximum_depth,omitempty"`
	// StateLimit is the maximum intermediate-state count admitted by the candidate.
	StateLimit int64 `json:"state_limit,omitempty"`
	// FrontierLimit is the maximum current or queued frontier size admitted by a shortest-path candidate.
	FrontierLimit int64 `json:"frontier_limit,omitempty"`
	// PredecessorLimit is the maximum retained witness predecessor state admitted by a shortest-path candidate.
	PredecessorLimit int64 `json:"predecessor_limit,omitempty"`
	// EnumerationLimit is the maximum distinct ordered path count staged by an all-shortest-path candidate.
	EnumerationLimit int64 `json:"enumeration_limit,omitempty"`
	// OutputBytesLimit is the maximum staged ordered edge-array bytes admitted by an all-shortest-path candidate.
	OutputBytesLimit int64 `json:"output_bytes_limit,omitempty"`
	// EndpointLimit is the maximum endpoint-seed count admitted by the candidate.
	EndpointLimit int64 `json:"endpoint_limit,omitempty"`
	// SeedPredicateClass describes the predicate used to bound search seeds.
	SeedPredicateClass string `json:"seed_predicate_class,omitempty"`
	// PrefixLength is the number of fixed steps before the variable expansion.
	PrefixLength int `json:"prefix_length,omitempty"`
	// HasFinalLimit reports whether a final row limit influenced candidate selection.
	HasFinalLimit bool `json:"has_final_limit,omitempty"`
	// Selected names the candidate selected by the optimizer.
	Selected string `json:"selected,omitempty"`
	// Applied names the candidate actually emitted by translation.
	Applied string `json:"applied,omitempty"`
	// SkipReason explains why a planned candidate was not emitted.
	SkipReason string `json:"skip_reason,omitempty"`
}

// TargetEligibilityFact reports one named qualification result in a translated target outcome.
type TargetEligibilityFact struct {
	// Name identifies the qualification check.
	Name string `json:"name"`
	// Eligible reports whether the target passed the named check.
	Eligible bool `json:"eligible"`
}

type SkippedLowering struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
	Count  int    `json:"count,omitempty"`
}

// recordLowering increments the applied count for one lowering name.
func (s *Translator) recordLowering(name string) {
	if s.appliedLoweringCounts == nil {
		s.appliedLoweringCounts = map[string]int{}
	}
	s.appliedLoweringCounts[name]++

	for _, lowering := range s.translation.Optimization.Lowerings {
		if lowering.Name == name {
			return
		}
	}

	s.translation.Optimization.Lowerings = append(s.translation.Optimization.Lowerings, optimize.LoweringDecision{Name: name})
}

// recordShortestPathExecutor records the executor actually emitted for a traversal target.
func (s *Translator) recordShortestPathExecutor(target optimize.TraversalStepTarget, executor optimize.ShortestPathExecutor) {
	if s.appliedShortestPathExecutors == nil {
		s.appliedShortestPathExecutors = map[optimize.TraversalStepTarget]optimize.ShortestPathExecutor{}
	}
	s.appliedShortestPathExecutors[target] = executor
	s.recordLowering(optimize.LoweringShortestPathExecutor)
}

// recordExpansionSearchStrategy records the expansion strategy actually emitted for a traversal target.
func (s *Translator) recordExpansionSearchStrategy(target optimize.TraversalStepTarget, strategy optimize.ExpansionSearchStrategy) {
	if s.appliedExpansionSearchStrategies == nil {
		s.appliedExpansionSearchStrategies = map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategy{}
	}
	s.appliedExpansionSearchStrategies[target] = strategy
	s.recordLowering(optimize.LoweringExpansionSearchStrategy)
}

// recordExpansionSearchPolicy records a runtime expansion policy actually emitted for a traversal target.
func (s *Translator) recordExpansionSearchPolicy(target optimize.TraversalStepTarget, policy optimize.ExpansionSearchPolicy) {
	if s.emittedExpansionSearchPolicies == nil {
		s.emittedExpansionSearchPolicies = map[optimize.TraversalStepTarget]optimize.ExpansionSearchPolicy{}
	}
	s.emittedExpansionSearchPolicies[target] = policy
	s.recordLowering(optimize.LoweringExpansionSearchStrategy)
}

// appliedLoweringCountSnapshot merges optimizer-declared and translator-observed lowering counts into the snapshot used to diagnose unapplied plans.
func (s *Translator) appliedLoweringCountSnapshot() map[string]int {
	applied := map[string]int{}

	for _, lowering := range s.translation.Optimization.Lowerings {
		applied[lowering.Name] = 1
	}

	for name, count := range s.appliedLoweringCounts {
		applied[name] = count
	}

	return applied
}

// recordSkippedLowerings compares the plan with applied counts and emits aggregated skip diagnostics.
func (s *Translator) recordSkippedLowerings() {
	if s.translation.Optimization.LoweringPlan == nil {
		return
	}

	applied := s.appliedLoweringCountSnapshot()
	s.recordTargetOutcomes(*s.translation.Optimization.LoweringPlan)

	for _, planned := range plannedLoweringCounts(*s.translation.Optimization.LoweringPlan) {
		if planned.Count == 0 {
			continue
		}

		skippedCount := planned.Count - applied[planned.Name]
		if skippedCount <= 0 {
			continue
		}

		s.translation.Optimization.SkippedLowerings = append(s.translation.Optimization.SkippedLowerings, SkippedLowering{
			Name:   planned.Name,
			Reason: skippedLoweringReason(planned.Name, applied, *s.translation.Optimization.LoweringPlan),
			Count:  skippedCount,
		})
	}
}

// recordTargetOutcomes converts per-target plan decisions and applied choices into diagnostic outcomes.
func (s *Translator) recordTargetOutcomes(plan optimize.LoweringPlan) {
	if len(s.translation.Optimization.TargetOutcomes) != 0 {
		return
	}
	for _, decision := range plan.ShortestPathExecutor {
		target := decision.Target
		eligible, staticallyEligible := decision.StructurallyEligible, decision.StaticallyEligible
		minimumDepth, maximumDepth := decision.MinimumDepth, decision.MaximumDepth
		applied := string(s.appliedShortestPathExecutors[target])
		outcome := TargetLoweringOutcome{
			Lowering:               optimize.LoweringShortestPathExecutor,
			TargetKind:             "traversal",
			TraversalTarget:        &target,
			Family:                 decision.Family,
			PlannedCandidates:      shortestPathCandidateNames(decision.PlannedCandidates),
			Scheduler:              string(decision.Scheduler),
			ExecutionBoundary:      decision.ExecutionBoundary,
			EligibilityFacts:       shortestPathEligibilityFacts(decision.Eligibility),
			ObservationMode:        string(decision.ObservationMode),
			Direction:              decision.Direction.String(),
			PhysicalExpansion:      string(decision.PhysicalExpansion),
			RelationshipKindCount:  decision.RelationshipKindCount,
			UntypedRelationship:    decision.UntypedRelationship,
			TopologyClassification: string(decision.TopologyClassification),
			Eligible:               &eligible,
			StaticallyEligible:     &staticallyEligible,
			SelectionMode:          decision.SelectionMode,
			SelectorVersion:        decision.SelectorVersion,
			Selected:               string(decision.SelectedExecutor),
			Applied:                applied,
			Fallback:               string(decision.FallbackExecutor),
			SkipReason:             decision.FallbackReason,
			MinimumDepth:           &minimumDepth,
			MaximumDepth:           &maximumDepth,
			StateLimit:             decision.StateLimit,
			FrontierLimit:          decision.FrontierLimit,
			PredecessorLimit:       decision.PredecessorLimit,
			EnumerationLimit:       decision.EnumerationLimit,
			OutputBytesLimit:       decision.OutputBytesLimit,
		}
		if decision.SelectedExecutor == optimize.ShortestPathExecutorASPI1DAG && applied == string(optimize.ShortestPathExecutorASPI1DAG) {
			outcome.Candidate = string(optimize.ShortestPathExecutorASPI1DAG)
			outcome.EmittedPolicy = optimize.ShortestPathPolicyASPI1GuardedV1
			outcome.EmittedCandidates = []string{
				string(optimize.ShortestPathExecutorASPI1DAG),
				string(optimize.ShortestPathExecutorASPA1DAG),
			}
		}
		if decision.SelectedExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness && applied == string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness) {
			outcome.Candidate = string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness)
			outcome.EmittedPolicy = optimize.ShortestPathPolicyI1CanonicalGuardedV1
			outcome.EmittedCandidates = []string{
				string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
				string(optimize.ShortestPathExecutorS4CanonicalWitness),
			}
		}
		s.translation.Optimization.TargetOutcomes = append(s.translation.Optimization.TargetOutcomes, outcome)
	}
	for _, decision := range plan.ExpansionSearchStrategy {
		target := decision.Target
		eligible, staticallyEligible := decision.StructurallyEligible, decision.StaticallyEligible
		minimumDepth, maximumDepth := decision.MinimumDepth, decision.MaximumDepth
		applied := string(s.appliedExpansionSearchStrategies[target])
		probeCaps, admission := decision.ProbeCaps, decision.Admission
		s.translation.Optimization.TargetOutcomes = append(s.translation.Optimization.TargetOutcomes, TargetLoweringOutcome{
			Lowering:           optimize.LoweringExpansionSearchStrategy,
			TargetKind:         "traversal",
			TraversalTarget:    &target,
			Family:             decision.Family,
			PlannedPolicy:      string(decision.PlannedPolicy),
			EmittedPolicy:      string(decision.EmittedPolicy),
			PlannedCandidates:  expansionSearchCandidateNames(decision.PlannedCandidates),
			EmittedCandidates:  expansionSearchCandidateNames(decision.EmittedCandidates),
			ExecutionBoundary:  decision.ExecutionBoundary,
			ProbeCaps:          &probeCaps,
			Admission:          &admission,
			Candidate:          string(decision.CandidateStrategy),
			EligibilityFacts:   expansionSearchEligibilityFacts(decision.EligibilityFacts),
			ObservationMode:    string(decision.ObservationMode),
			Eligible:           &eligible,
			StaticallyEligible: &staticallyEligible,
			SelectionMode:      decision.SelectionMode,
			SelectorVersion:    decision.SelectorVersion,
			Selected:           string(decision.SelectedStrategy),
			Applied:            applied,
			Fallback:           string(decision.FallbackStrategy),
			SkipReason:         decision.FallbackReason,
			MinimumDepth:       &minimumDepth,
			MaximumDepth:       &maximumDepth,
			StateLimit:         decision.StateLimit,
			EndpointLimit:      decision.EndpointLimit,
			SeedPredicateClass: decision.SeedPredicateClass,
			PrefixLength:       decision.PrefixLength,
			HasFinalLimit:      decision.HasFinalLimit,
		})
	}
	for _, decision := range plan.EndpointResolution {
		target := decision.Target
		eligible, staticallyEligible := decision.StructurallyEligible, decision.StaticallyEligible
		root, terminal, caps := decision.Root, decision.Terminal, decision.Caps
		s.translation.Optimization.TargetOutcomes = append(s.translation.Optimization.TargetOutcomes, TargetLoweringOutcome{
			Lowering:               optimize.LoweringEndpointResolution,
			TargetKind:             "endpoint_resolution",
			TraversalTarget:        &target,
			Family:                 "endpoint_resolution",
			TraversalFamily:        decision.Family,
			PlannedCandidates:      endpointResolutionCandidateNames(decision.PlannedCandidates),
			EndpointRoot:           &root,
			EndpointTerminal:       &terminal,
			EndpointPairClass:      decision.PairClass,
			EndpointResolutionCaps: &caps,
			Candidate:              string(decision.CandidatePlan),
			EligibilityFacts:       endpointResolutionEligibilityFacts(decision.EligibilityFacts),
			Eligible:               &eligible,
			StaticallyEligible:     &staticallyEligible,
			SelectionMode:          decision.SelectionMode,
			SelectorVersion:        decision.SelectorVersion,
			Selected:               string(decision.SelectedPlan),
			Applied:                string(decision.SelectedPlan),
			Fallback:               string(decision.FallbackPlan),
			SkipReason:             decision.FallbackReason,
		})
	}
	for _, decision := range plan.TraversalPredicate {
		target, predicateIndex := decision.Target, decision.PredicateIndex
		eligible, staticallyEligible := decision.StructurallyEligible, decision.StaticallyEligible
		s.translation.Optimization.TargetOutcomes = append(s.translation.Optimization.TargetOutcomes, TargetLoweringOutcome{
			Lowering:           optimize.LoweringTraversalPredicateClassification,
			TargetKind:         "traversal_predicate",
			TraversalTarget:    &target,
			Family:             "traversal_predicate",
			PlannedCandidates:  traversalPredicateCandidateNames(decision.PlannedCandidates),
			PredicateClass:     decision.Class,
			PredicateSource:    decision.Source,
			PredicateIndex:     &predicateIndex,
			Candidate:          string(decision.CandidatePlan),
			EligibilityFacts:   traversalPredicateEligibilityFacts(decision.EligibilityFacts),
			Eligible:           &eligible,
			StaticallyEligible: &staticallyEligible,
			SelectionMode:      decision.SelectionMode,
			SelectorVersion:    decision.ClassifierVersion,
			Selected:           string(decision.SelectedPlan),
			Applied:            string(decision.SelectedPlan),
			Fallback:           string(decision.FallbackPlan),
			SkipReason:         decision.FallbackReason,
		})
	}
	for _, decision := range plan.FieldRequirements {
		queryPartIndex := decision.QueryPartIndex
		s.translation.Optimization.TargetOutcomes = append(s.translation.Optimization.TargetOutcomes, TargetLoweringOutcome{
			Lowering:       optimize.LoweringFieldRequirements,
			TargetKind:     "field_requirement",
			QueryPartIndex: &queryPartIndex,
			Symbol:         decision.Symbol,
			Selected:       "analysis_only",
			SkipReason:     "analysis_metadata_only",
		})
	}
}

// shortestPathCandidateNames converts executor candidates to their stable diagnostic names.
func shortestPathCandidateNames(candidates []optimize.ShortestPathExecutor) []string {
	names := make([]string, len(candidates))
	for idx, candidate := range candidates {
		names[idx] = string(candidate)
	}
	return names
}

// expansionSearchCandidateNames converts expansion candidates to their stable diagnostic names.
func expansionSearchCandidateNames(candidates []optimize.ExpansionSearchStrategy) []string {
	names := make([]string, len(candidates))
	for idx, candidate := range candidates {
		names[idx] = string(candidate)
	}
	return names
}

// endpointResolutionCandidateNames converts analysis-only endpoint plans to
// their stable diagnostic identities.
func endpointResolutionCandidateNames(candidates []optimize.EndpointResolutionPlan) []string {
	names := make([]string, len(candidates))
	for idx, candidate := range candidates {
		names[idx] = string(candidate)
	}
	return names
}

// traversalPredicateCandidateNames converts predicate-placement plans to
// their stable diagnostic identities.
func traversalPredicateCandidateNames(candidates []optimize.TraversalPredicatePlan) []string {
	names := make([]string, len(candidates))
	for idx, candidate := range candidates {
		names[idx] = string(candidate)
	}
	return names
}

// shortestPathEligibilityFacts converts executor qualification facts to public diagnostic records.
func shortestPathEligibilityFacts(facts []optimize.ShortestPathEligibilityFact) []TargetEligibilityFact {
	outcomes := make([]TargetEligibilityFact, len(facts))
	for idx, fact := range facts {
		outcomes[idx] = TargetEligibilityFact{
			Name:     fact.Name,
			Eligible: fact.Eligible,
		}
	}
	return outcomes
}

// expansionSearchEligibilityFacts converts search-strategy qualification facts to public diagnostic records.
func expansionSearchEligibilityFacts(facts []optimize.ExpansionSearchEligibilityFact) []TargetEligibilityFact {
	outcomes := make([]TargetEligibilityFact, len(facts))
	for idx, fact := range facts {
		outcomes[idx] = TargetEligibilityFact{
			Name:     fact.Name,
			Eligible: fact.Eligible,
		}
	}
	return outcomes
}

func endpointResolutionEligibilityFacts(facts []optimize.EndpointResolutionEligibilityFact) []TargetEligibilityFact {
	outcomes := make([]TargetEligibilityFact, len(facts))
	for idx, fact := range facts {
		outcomes[idx] = TargetEligibilityFact{Name: fact.Name, Eligible: fact.Eligible}
	}
	return outcomes
}

func traversalPredicateEligibilityFacts(facts []optimize.TraversalPredicateEligibilityFact) []TargetEligibilityFact {
	outcomes := make([]TargetEligibilityFact, len(facts))
	for idx, fact := range facts {
		outcomes[idx] = TargetEligibilityFact{Name: fact.Name, Eligible: fact.Eligible}
	}
	return outcomes
}

// plannedLoweringCounts converts each lowering target collection into a named count so planned work can be reconciled with applied work.
func plannedLoweringCounts(plan optimize.LoweringPlan) []SkippedLowering {
	return []SkippedLowering{
		{
			Name:  optimize.LoweringProjectionPruning,
			Count: len(plan.ProjectionPruning),
		},
		{
			Name:  optimize.LoweringLatePathMaterialization,
			Count: len(plan.LatePathMaterialization),
		},
		{
			Name:  optimize.LoweringExpandIntoDetection,
			Count: len(plan.ExpandInto),
		},
		{
			Name:  optimize.LoweringTraversalDirection,
			Count: len(plan.TraversalDirection),
		},
		{
			Name:  optimize.LoweringShortestPathStrategy,
			Count: len(plan.ShortestPathStrategy),
		},
		{
			Name:  optimize.LoweringShortestPathFilter,
			Count: len(plan.ShortestPathFilter),
		},
		{
			Name:  optimize.LoweringLimitPushdown,
			Count: len(plan.LimitPushdown),
		},
		{
			Name:  optimize.LoweringExpansionSuffixPushdown,
			Count: len(plan.ExpansionSuffixPushdown),
		},
		{
			Name:  optimize.LoweringExpansionSearchStrategy,
			Count: len(plan.ExpansionSearchStrategy),
		},
		{
			Name:  optimize.LoweringPredicatePlacement,
			Count: len(plan.PredicatePlacement) + len(plan.PatternPredicate),
		},
		{
			Name:  optimize.LoweringCountStoreFastPath,
			Count: len(plan.CountStoreFastPath),
		},
		{
			Name:  optimize.LoweringExactRangeExpansion,
			Count: len(plan.ExactRangeExpansion),
		},
		{
			Name:  optimize.LoweringPathRelationshipPredicate,
			Count: len(plan.PathRelationshipPredicate),
		},
		{
			Name:  optimize.LoweringAggregateTraversalCount,
			Count: len(plan.AggregateTraversalCount),
		},
		{
			Name:  optimize.LoweringFieldRequirements,
			Count: len(plan.FieldRequirements),
		},
		{
			Name:  optimize.LoweringShortestPathExecutor,
			Count: len(plan.ShortestPathExecutor),
		},
	}
}

// skippedLoweringReason explains why planned lowering work was not observed, including metadata-only analyses and lowerings superseded by a stronger fast path.
func skippedLoweringReason(name string, applied map[string]int, plan optimize.LoweringPlan) string {
	if name == optimize.LoweringFieldRequirements {
		return "analysis_metadata_only"
	}
	if applied[optimize.LoweringCountStoreFastPath] > 0 && name != optimize.LoweringCountStoreFastPath {
		return "superseded by CountStoreFastPath"
	}
	if applied[optimize.LoweringAggregateTraversalCount] > 0 && name != optimize.LoweringAggregateTraversalCount {
		return "superseded by AggregateTraversalCount"
	}

	switch name {
	case optimize.LoweringPredicatePlacement:
		return "planned predicate placements were not consumed by this translation shape"
	case optimize.LoweringTraversalDirection:
		if reason := skippedTraversalDirectionReason(plan); reason != "" {
			return reason
		}
	case optimize.LoweringExpansionSearchStrategy:
		for _, decision := range plan.ExpansionSearchStrategy {
			if decision.FallbackReason != "" {
				return decision.FallbackReason
			}
		}
	case optimize.LoweringShortestPathExecutor:
		for _, decision := range plan.ShortestPathExecutor {
			if decision.FallbackReason != "" {
				return decision.FallbackReason
			}
		}
	default:
		return "planned lowering did not change the emitted SQL"
	}

	return "planned lowering did not change the emitted SQL"
}

// skippedTraversalDirectionReason returns the first recorded reason a planned traversal direction was retained.
func skippedTraversalDirectionReason(plan optimize.LoweringPlan) string {
	for _, decision := range plan.TraversalDirection {
		if !decision.Flip && decision.Reason != "" {
			return decision.Reason
		}
	}

	return ""
}

// ToolOptions controls experimental lowering selection exposed only to repository tooling.
type ToolOptions struct {
	// ForceShortestPathExecutor requests a qualified shortest-path executor instead of automatic selection.
	ForceShortestPathExecutor optimize.ShortestPathExecutor
	// ForceExpansionSearchStrategy requests a qualified variable-expansion strategy instead of automatic selection.
	ForceExpansionSearchStrategy optimize.ExpansionSearchStrategy
	// ExpansionOrientationPolicy selects the immutable orientation selector
	// identity used by an enabled tournament or shadow mode. The zero value
	// preserves orientation-probe-v1.
	ExpansionOrientationPolicy optimize.ExpansionSearchPolicy
	// EnableExpansionOrientationTournament emits a guarded orientation policy
	// for one qualified fixed-suffix expansion. It defaults to
	// orientation-probe-v1 and is intentionally tool-only while selectors are
	// being shadow-qualified.
	EnableExpansionOrientationTournament bool
	// EnableExpansionOrientationShadow emits the same bounded orientation
	// probes and SQL-visible would_select metadata while executing only the
	// exact incumbent traversal arm.
	EnableExpansionOrientationShadow bool
	// DisableEndpointSeededReverse is an emergency production rollback switch.
	DisableEndpointSeededReverse bool
}

// ProductionOptions contains the deliberately narrow subset of experimental
// lowerings that may be enabled by the PostgreSQL driver's versioned,
// query-allowlisted canary policy. The zero value preserves all incumbent
// production choices.
type ProductionOptions struct {
	ShortestPathExecutor         optimize.ShortestPathExecutor
	ShortestPathCaps             *ProductionShortestPathCaps
	AuthorizedBucket             *ProductionTraversalBucket
	EnableExpansionOrientation   bool
	DisableEndpointSeededReverse bool
	DisableInlineASPDAG          bool
	DisableInlineSPWitness       bool
	SelectorVersion              string
}

// ProductionShortestPathCaps are immutable manifest-authorized limits. They
// are copied into the lowering decision and therefore into emitted SQL.
type ProductionShortestPathCaps struct {
	StateLimit       int64 `json:"state_limit"`
	PredecessorLimit int64 `json:"predecessor_limit"`
	EnumerationLimit int64 `json:"enumeration_limit"`
	OutputBytesLimit int64 `json:"output_bytes_limit"`
}

// ProductionTraversalBucket binds an exact-query authorization to the
// structural target characteristics independently qualified by evidence.
type ProductionTraversalBucket struct {
	Direction             string `json:"direction"`
	ObservationMode       string `json:"observation_mode"`
	MinimumDepth          int64  `json:"minimum_depth"`
	MaximumDepth          int64  `json:"maximum_depth"`
	RelationshipKindCount int    `json:"relationship_kind_count"`
	UntypedRelationship   bool   `json:"untyped_relationship"`
}

// Translate optimizes and translates a Cypher query for the selected graph using production lowering choices.
func Translate(ctx context.Context, cypherQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32) (Result, error) {
	return translate(ctx, cypherQuery, kindMapper, parameters, graphID, ToolOptions{})
}

// TranslateWithProductionOptions applies a validated canary policy. B
// executors remain unavailable unless the driver has independently established
// the required transaction snapshot; this function only controls lowering.
func TranslateWithProductionOptions(ctx context.Context, cypherQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32, options ProductionOptions) (Result, error) {
	if options.SelectorVersion == "" {
		return Result{}, fmt.Errorf("production traversal policy requires a selector version")
	}
	if options.ShortestPathExecutor != "" && !productionShortestPathExecutor(options.ShortestPathExecutor) {
		return Result{}, fmt.Errorf("shortest-path executor %q is not production-canary eligible", options.ShortestPathExecutor)
	}
	toolOptions := ToolOptions{
		ForceShortestPathExecutor:            options.ShortestPathExecutor,
		EnableExpansionOrientationTournament: options.EnableExpansionOrientation,
		DisableEndpointSeededReverse:         options.DisableEndpointSeededReverse,
	}
	optimizedPlan, err := optimize.Optimize(cypherQuery)
	if err != nil {
		return Result{}, err
	}
	if err := applyToolOptions(&optimizedPlan, toolOptions); err != nil {
		return Result{}, err
	}
	applyProductionShortestPathRollback(&optimizedPlan, options)
	if err := applyProductionShortestPathAuthorization(&optimizedPlan, options); err != nil {
		return Result{}, err
	}
	for idx := range optimizedPlan.LoweringPlan.ShortestPathExecutor {
		decision := &optimizedPlan.LoweringPlan.ShortestPathExecutor[idx]
		if decision.SelectionMode == "forced_tool" {
			decision.SelectionMode = "production_canary"
			decision.SelectorVersion = options.SelectorVersion
		}
	}
	for idx := range optimizedPlan.LoweringPlan.ExpansionSearchStrategy {
		decision := &optimizedPlan.LoweringPlan.ExpansionSearchStrategy[idx]
		if decision.SelectionMode == "guarded_tool" {
			decision.SelectionMode = "production_canary"
			decision.SelectorVersion = options.SelectorVersion
		}
	}
	return translateOptimized(ctx, optimizedPlan, kindMapper, parameters, graphID, toolOptions)
}

// TranslateForTool exposes qualified experimental lowerings to repository
// tooling without making them selectable through the production query API.
func TranslateForTool(ctx context.Context, cypherQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32, options ToolOptions) (Result, error) {
	return translate(ctx, cypherQuery, kindMapper, parameters, graphID, options)
}

// translate optimizes a Cypher query, applies optional tooling overrides, emits PostgreSQL, and records diagnostics.
func translate(ctx context.Context, cypherQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32, options ToolOptions) (Result, error) {
	optimizedPlan, err := optimize.Optimize(cypherQuery)
	if err != nil {
		return Result{}, err
	}
	if err := applyToolOptions(&optimizedPlan, options); err != nil {
		return Result{}, err
	}
	return translateOptimized(ctx, optimizedPlan, kindMapper, parameters, graphID, options)
}

func translateOptimized(ctx context.Context, optimizedPlan optimize.Plan, kindMapper pgsql.KindMapper, parameters map[string]any, graphID int32, options ToolOptions) (Result, error) {

	translator := NewTranslator(ctx, kindMapper, parameters, graphID)
	if membershipAliases, err := collectIDMembershipAliases(optimizedPlan.Query); err != nil {
		return Result{}, err
	} else {
		translator.collectIDMembershipAliases = membershipAliases
	}
	translator.SetOptimizationPlan(optimizedPlan)
	translator.translation.Optimization.Rules = optimizedPlan.Rules
	translator.translation.Optimization.PredicateAttachments = optimizedPlan.PredicateAttachments
	if !optimizedPlan.LoweringPlan.Empty() {
		loweringPlan := optimizedPlan.LoweringPlan
		translator.translation.Optimization.LoweringPlan = &loweringPlan
		translator.translation.Optimization.PlannedLowerings = loweringPlan.Decisions()
	}

	if translated, err := translator.translateCountStoreFastPath(optimizedPlan.Query, optimizedPlan.LoweringPlan); err != nil {
		return Result{}, err
	} else if translated {
		translator.recordSkippedLowerings()
		return translator.translation, nil
	}

	if translated, err := translator.translateAggregateTraversalCount(optimizedPlan.Query, optimizedPlan.LoweringPlan); err != nil {
		return Result{}, err
	} else if translated {
		translator.recordSkippedLowerings()
		return translator.translation, nil
	}

	if err := walk.Cypher(optimizedPlan.Query, translator); err != nil {
		return Result{}, err
	}
	if options.ForceExpansionSearchStrategy != "" && len(translator.appliedExpansionSearchStrategies) == 0 {
		return Result{}, fmt.Errorf("forced expansion-search strategy %q was selected but not emitted", options.ForceExpansionSearchStrategy)
	}
	if options.EnableExpansionOrientationTournament && len(translator.emittedExpansionSearchPolicies) == 0 {
		return Result{}, fmt.Errorf("expansion orientation tournament was selected but not emitted")
	}
	if options.EnableExpansionOrientationShadow && len(translator.emittedExpansionSearchPolicies) == 0 {
		return Result{}, fmt.Errorf("expansion orientation shadow was selected but not emitted")
	}
	if options.ForceShortestPathExecutor != "" && len(translator.appliedShortestPathExecutors) == 0 {
		return Result{}, fmt.Errorf("forced shortest-path executor %q was selected but not emitted", options.ForceShortestPathExecutor)
	}

	translator.recordSkippedLowerings()
	return translator.translation, nil
}

func productionShortestPathExecutor(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorASPI1DAG:
		return true
	default:
		return false
	}
}

func applyProductionShortestPathAuthorization(plan *optimize.Plan, options ProductionOptions) error {
	if options.ShortestPathExecutor == "" {
		return nil
	}
	if options.DisableInlineASPDAG && options.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG {
		return fmt.Errorf("inline ASP DAG is disabled by production policy")
	}
	for idx := range plan.LoweringPlan.ShortestPathExecutor {
		decision := &plan.LoweringPlan.ShortestPathExecutor[idx]
		if decision.SelectedExecutor != options.ShortestPathExecutor || decision.SelectionMode != "forced_tool" {
			continue
		}
		if options.AuthorizedBucket != nil {
			bucket := options.AuthorizedBucket
			if decision.Direction.String() != bucket.Direction ||
				string(decision.ObservationMode) != bucket.ObservationMode ||
				decision.MinimumDepth != bucket.MinimumDepth ||
				decision.MaximumDepth != bucket.MaximumDepth ||
				decision.RelationshipKindCount != bucket.RelationshipKindCount ||
				decision.UntypedRelationship != bucket.UntypedRelationship {
				return fmt.Errorf("production traversal target does not match its authorized promotion bucket")
			}
		}
		if options.ShortestPathExecutor == optimize.ShortestPathExecutorASPI1DAG || options.ShortestPathExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
			if options.AuthorizedBucket == nil {
				return fmt.Errorf("guarded inline shortest-path production policy requires an exact authorized bucket")
			}
			if options.ShortestPathCaps == nil {
				return fmt.Errorf("guarded inline shortest-path production policy requires immutable caps")
			}
			caps := options.ShortestPathCaps
			if caps.StateLimit <= 0 || caps.PredecessorLimit <= 0 || caps.EnumerationLimit <= 0 || caps.OutputBytesLimit <= 0 {
				return fmt.Errorf("guarded inline shortest-path production policy requires positive immutable caps")
			}
			decision.StateLimit = caps.StateLimit
			decision.PredecessorLimit = caps.PredecessorLimit
			decision.EnumerationLimit = caps.EnumerationLimit
			decision.OutputBytesLimit = caps.OutputBytesLimit
			decision.ExecutionBoundary = "guarded_dual_arm"
		}
		return nil
	}
	return fmt.Errorf("production shortest-path executor %q was not selected", options.ShortestPathExecutor)
}

// applyProductionShortestPathRollback is deliberately post-optimization: an
// emergency switch must rewrite both a policy-forced candidate and any future
// statically preferred candidate. Returning to the exact incumbent also resets
// candidate-only limits and boundary metadata so cached SQL cannot retain a
// disabled guarded arm.
func applyProductionShortestPathRollback(plan *optimize.Plan, options ProductionOptions) {
	for idx := range plan.LoweringPlan.ShortestPathExecutor {
		decision := &plan.LoweringPlan.ShortestPathExecutor[idx]
		switch {
		case options.DisableInlineASPDAG && decision.SelectedExecutor == optimize.ShortestPathExecutorASPI1DAG:
			decision.SelectedExecutor = optimize.ShortestPathExecutorASPA1DAG
			decision.FallbackExecutor = optimize.ShortestPathExecutorIncumbentWorkspace
		case options.DisableInlineSPWitness && decision.SelectedExecutor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness:
			decision.SelectedExecutor = optimize.ShortestPathExecutorS4CanonicalWitness
			decision.FallbackExecutor = optimize.ShortestPathExecutorIncumbentWorkspace
		default:
			continue
		}
		decision.Scheduler = decision.SelectedExecutor.Scheduler()
		decision.ExecutionBoundary = decision.SelectedExecutor.ExecutionBoundary()
		decision.SelectionMode = "production_kill_switch"
		decision.SelectorVersion = options.SelectorVersion
		decision.FallbackReason = "disabled_by_production_policy"
		decision.FrontierLimit = 0
		decision.PredecessorLimit = 0
		decision.EnumerationLimit = 0
		decision.OutputBytesLimit = 0
	}
}

// applyToolOptions applies supported forced executor and expansion-strategy requests to an optimized plan.
func applyToolOptions(plan *optimize.Plan, options ToolOptions) error {
	if options.EnableExpansionOrientationTournament && options.EnableExpansionOrientationShadow {
		return fmt.Errorf("expansion orientation tournament and shadow modes are mutually exclusive")
	}
	if (options.EnableExpansionOrientationTournament || options.EnableExpansionOrientationShadow) && options.ForceExpansionSearchStrategy != "" {
		return fmt.Errorf("expansion orientation policy and forced expansion-search strategy are mutually exclusive")
	}
	orientationPolicy, err := requestedExpansionOrientationPolicy(options)
	if err != nil {
		return err
	}
	if err := applyForcedShortestPathExecutor(plan, options.ForceShortestPathExecutor); err != nil {
		return err
	}
	if options.DisableEndpointSeededReverse {
		for idx := range plan.LoweringPlan.ExpansionSearchStrategy {
			decision := &plan.LoweringPlan.ExpansionSearchStrategy[idx]
			if decision.SelectedStrategy == optimize.ExpansionSearchEndpointSeededReverse {
				decision.SelectedStrategy = optimize.ExpansionSearchStepwiseForward
				decision.EmittedPolicy = ""
				decision.EmittedCandidates = []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward}
				decision.ExecutionBoundary = optimize.ExpansionSearchExecutionBoundaryInlineStatement
				decision.SelectionMode = "production_kill_switch"
				decision.SelectorVersion = "endpoint-seeded-disabled-v1"
				decision.FallbackReason = "disabled_by_production_policy"
			}
		}
	}
	if options.EnableExpansionOrientationTournament {
		return applyExpansionOrientationTournamentPolicy(plan, orientationPolicy)
	}
	if options.EnableExpansionOrientationShadow {
		return applyExpansionOrientationShadowPolicy(plan, orientationPolicy)
	}
	return applyForcedExpansionSearchStrategy(plan, options.ForceExpansionSearchStrategy)
}

func requestedExpansionOrientationPolicy(options ToolOptions) (optimize.ExpansionSearchPolicy, error) {
	policy := options.ExpansionOrientationPolicy
	if policy == "" {
		return optimize.ExpansionSearchPolicyOrientationProbeV1, nil
	}
	if !options.EnableExpansionOrientationTournament && !options.EnableExpansionOrientationShadow {
		return "", fmt.Errorf("expansion orientation policy %q requires tournament or shadow mode", policy)
	}
	if !supportedExpansionOrientationPolicy(policy) {
		return "", fmt.Errorf("unsupported expansion orientation policy %q", policy)
	}
	return policy, nil
}

func supportedExpansionOrientationPolicy(policy optimize.ExpansionSearchPolicy) bool {
	switch policy {
	case optimize.ExpansionSearchPolicyOrientationProbeV1,
		optimize.ExpansionSearchPolicyOrientationProbeV2:
		return true
	default:
		return false
	}
}

// applyForcedShortestPathExecutor selects the requested executor only when exactly one qualified shortest-path target supports it.
func applyForcedShortestPathExecutor(plan *optimize.Plan, executor optimize.ShortestPathExecutor) error {
	if executor == "" {
		return nil
	}
	if !supportedForcedShortestPathExecutor(executor) {
		return fmt.Errorf("unsupported forced shortest-path executor %q", executor)
	}
	if executor == optimize.ShortestPathExecutorIncumbentWorkspace || executor == optimize.ShortestPathExecutorS0Direct {
		forced := 0
		for idx := range plan.LoweringPlan.ShortestPathExecutor {
			decision := &plan.LoweringPlan.ShortestPathExecutor[idx]
			if !decision.StructurallyEligible {
				continue
			}
			if executor == optimize.ShortestPathExecutorS0Direct && (decision.MinimumDepth != 1 || decision.MaximumDepth < 1) {
				continue
			}
			decision.SelectedExecutor = executor
			decision.Scheduler = executor.Scheduler()
			decision.ExecutionBoundary = executor.ExecutionBoundary()
			decision.SelectionMode = "forced_tool"
			decision.SelectorVersion = "sp-tool-v1"
			decision.FallbackReason = ""
			forced++
		}
		if forced == 0 {
			if executor == optimize.ShortestPathExecutorS0Direct {
				return fmt.Errorf("forced shortest-path executor %q has no structurally eligible depth-one target", executor)
			}
			return fmt.Errorf("forced shortest-path executor %q has no structurally eligible target", executor)
		}
		return nil
	}
	expectedObservation := optimize.ShortestPathObservationDistance
	expectedDescription := "distance-only"
	if executor == optimize.ShortestPathExecutorS3EdgeM0 || executor == optimize.ShortestPathExecutorS4CanonicalWitness || executor == optimize.ShortestPathExecutorI1CanonicalWitness || executor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness || executor == optimize.ShortestPathExecutorB1AlternatingNodeWitness || executor == optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness {
		expectedObservation = optimize.ShortestPathObservationOnePath
		expectedDescription = "one-path"
	} else if executor == optimize.ShortestPathExecutorASPA1DAG || executor == optimize.ShortestPathExecutorASPI1DAG || executor == optimize.ShortestPathExecutorASPB1AlternatingNodeDAG || executor == optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG {
		expectedObservation = optimize.ShortestPathObservationAllPaths
		expectedDescription = "all-paths"
	}

	allShortestExecutor := executor == optimize.ShortestPathExecutorASPA1DAG ||
		executor == optimize.ShortestPathExecutorASPI1DAG ||
		executor == optimize.ShortestPathExecutorASPB1AlternatingNodeDAG ||
		executor == optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG
	forced := 0
	for idx := range plan.LoweringPlan.ShortestPathExecutor {
		decision := &plan.LoweringPlan.ShortestPathExecutor[idx]
		if !decision.StructurallyEligible {
			continue
		}
		if decision.ObservationMode != expectedObservation {
			continue
		}
		// Two-sided predecessor-DAG discovery is proven only for one distinct,
		// directed singleton endpoint pair with minimum depth exactly one. The
		// shared structural facts enforce every condition except this narrower
		// minimum-depth check. Tool forcing must not broaden that envelope.
		if allShortestExecutor && (decision.Family != "ASP" || decision.MinimumDepth != 1 || decision.MaximumDepth < 1 || decision.MaximumDepth > 64) {
			continue
		}

		decision.SelectedExecutor = executor
		decision.ExecutionBoundary = executor.ExecutionBoundary()
		if executor == optimize.ShortestPathExecutorASPI1DAG || executor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
			decision.ExecutionBoundary = "guarded_dual_arm"
			decision.FrontierLimit = 0
		}
		decision.Scheduler = executor.Scheduler()
		decision.SelectionMode = "forced_tool"
		decision.SelectorVersion = "sp-tool-v1"
		decision.FallbackReason = ""
		if allShortestExecutor {
			decision.SelectorVersion = "asp-tool-v1"
			if executor != optimize.ShortestPathExecutorASPA1DAG {
				decision.FallbackExecutor = optimize.ShortestPathExecutorASPA1DAG
			}
		}
		if executor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
			decision.SelectorVersion = "sp-i1-canonical-tool-v1"
			decision.FallbackExecutor = optimize.ShortestPathExecutorS4CanonicalWitness
		}
		forced++
	}
	if forced == 0 {
		return fmt.Errorf("forced shortest-path executor %q has no structurally eligible %s target", executor, expectedDescription)
	}
	return nil
}

func supportedForcedShortestPathExecutor(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorIncumbentWorkspace,
		optimize.ShortestPathExecutorS0Direct,
		optimize.ShortestPathExecutorS3Unidirectional,
		optimize.ShortestPathExecutorS3EdgeM0,
		optimize.ShortestPathExecutorS4CanonicalDistance,
		optimize.ShortestPathExecutorS4CanonicalWitness,
		optimize.ShortestPathExecutorASPA1DAG,
		optimize.ShortestPathExecutorASPI1DAG,
		optimize.ShortestPathExecutorI1CanonicalDistance,
		optimize.ShortestPathExecutorI1CanonicalWitness,
		optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorB1AlternatingNodeDistance,
		optimize.ShortestPathExecutorB1AlternatingNodeWitness,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
		optimize.ShortestPathExecutorASPB1AlternatingNodeDAG,
		optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG:
		return true
	default:
		return false
	}
}

// applyForcedExpansionSearchStrategy selects the requested strategy only when exactly one qualified expansion target supports it.
func applyForcedExpansionSearchStrategy(plan *optimize.Plan, strategy optimize.ExpansionSearchStrategy) error {
	if strategy == "" {
		return nil
	}
	if strategy != optimize.ExpansionSearchSuffixSeededReverse && strategy != optimize.ExpansionSearchEndpointSeededReverse {
		return fmt.Errorf("unsupported forced expansion-search strategy %q", strategy)
	}

	var matching []int
	for idx := range plan.LoweringPlan.ExpansionSearchStrategy {
		decision := plan.LoweringPlan.ExpansionSearchStrategy[idx]
		if !decision.StructurallyEligible {
			continue
		}
		if strategy == optimize.ExpansionSearchSuffixSeededReverse && decision.CandidateStrategy != optimize.ExpansionSearchSuffixSeededReverse {
			continue
		}
		if strategy == optimize.ExpansionSearchEndpointSeededReverse && decision.CandidateStrategy != optimize.ExpansionSearchEndpointSeededReverse {
			continue
		}
		matching = append(matching, idx)
	}
	if len(matching) == 0 {
		return fmt.Errorf("forced expansion-search strategy %q has no structurally eligible target", strategy)
	}
	if len(matching) != 1 {
		return fmt.Errorf("forced expansion-search strategy %q matched %d structurally eligible targets; expected exactly one", strategy, len(matching))
	}

	decision := &plan.LoweringPlan.ExpansionSearchStrategy[matching[0]]
	decision.SelectedStrategy = strategy
	decision.SelectionMode = "forced_tool"
	decision.EmittedPolicy = ""
	decision.EmittedCandidates = []optimize.ExpansionSearchStrategy{strategy}
	decision.ExecutionBoundary = optimize.ExpansionSearchExecutionBoundaryInlineStatement
	if strategy == optimize.ExpansionSearchSuffixSeededReverse {
		decision.SelectorVersion = "suffix-seeded-reverse-tool-v1"
	} else {
		decision.SelectorVersion = "endpoint-seeded-reverse-tool-v1"
		decision.EmittedPolicy = optimize.ExpansionSearchPolicyEndpointGuardV1
		decision.EmittedCandidates = []optimize.ExpansionSearchStrategy{
			optimize.ExpansionSearchStepwiseForward,
			optimize.ExpansionSearchEndpointSeededReverse,
		}
		decision.ExecutionBoundary = optimize.ExpansionSearchExecutionBoundaryGuardedDualArm
	}
	decision.FallbackReason = ""

	return nil
}

// applyExpansionOrientationTournament emits orientation-probe-v1 only when a
// single already-qualified fixed-suffix target exists. It preserves the
// compile-time incumbent identity because the runtime arm is not known during
// translation.
func applyExpansionOrientationTournament(plan *optimize.Plan) error {
	return applyExpansionOrientationTournamentPolicy(plan, optimize.ExpansionSearchPolicyOrientationProbeV1)
}

func applyExpansionOrientationTournamentPolicy(plan *optimize.Plan, policy optimize.ExpansionSearchPolicy) error {
	if !supportedExpansionOrientationPolicy(policy) {
		return fmt.Errorf("unsupported expansion orientation policy %q", policy)
	}
	var matching []int
	for idx, decision := range plan.LoweringPlan.ExpansionSearchStrategy {
		if decision.Family != "fixed_suffix_expansion" ||
			decision.CandidateStrategy != optimize.ExpansionSearchSuffixSeededReverse ||
			!decision.StructurallyEligible || !decision.StaticallyEligible {
			continue
		}
		matching = append(matching, idx)
	}
	if len(matching) == 0 {
		return fmt.Errorf("expansion orientation tournament has no structurally eligible fixed-suffix target")
	}
	if len(matching) != 1 {
		return fmt.Errorf("expansion orientation tournament matched %d structurally eligible fixed-suffix targets; expected exactly one", len(matching))
	}

	decision := &plan.LoweringPlan.ExpansionSearchStrategy[matching[0]]
	decision.SelectedStrategy = optimize.ExpansionSearchStepwiseForward
	decision.PlannedPolicy = policy
	decision.SelectionMode = "guarded_tool"
	decision.SelectorVersion = string(policy)
	decision.EmittedPolicy = policy
	decision.EmittedCandidates = []optimize.ExpansionSearchStrategy{
		optimize.ExpansionSearchStepwiseForward,
		optimize.ExpansionSearchSuffixSeededReverse,
	}
	decision.ExecutionBoundary = optimize.ExpansionSearchExecutionBoundaryGuardedDualArm
	decision.FallbackReason = ""

	return nil
}

// applyExpansionOrientationShadow emits orientation-probe-v1 for one
// qualified fixed-suffix target while retaining the exact incumbent as the
// only emitted traversal arm. The generated policy CTE records which arm the
// selector would have chosen without dispatching it.
func applyExpansionOrientationShadow(plan *optimize.Plan) error {
	return applyExpansionOrientationShadowPolicy(plan, optimize.ExpansionSearchPolicyOrientationProbeV1)
}

func applyExpansionOrientationShadowPolicy(plan *optimize.Plan, policy optimize.ExpansionSearchPolicy) error {
	if !supportedExpansionOrientationPolicy(policy) {
		return fmt.Errorf("unsupported expansion orientation policy %q", policy)
	}
	var matching []int
	for idx, decision := range plan.LoweringPlan.ExpansionSearchStrategy {
		if decision.Family != "fixed_suffix_expansion" ||
			decision.CandidateStrategy != optimize.ExpansionSearchSuffixSeededReverse ||
			!decision.StructurallyEligible || !decision.StaticallyEligible {
			continue
		}
		matching = append(matching, idx)
	}
	if len(matching) == 0 {
		return fmt.Errorf("expansion orientation shadow has no structurally eligible fixed-suffix target")
	}
	if len(matching) != 1 {
		return fmt.Errorf("expansion orientation shadow matched %d structurally eligible fixed-suffix targets; expected exactly one", len(matching))
	}

	decision := &plan.LoweringPlan.ExpansionSearchStrategy[matching[0]]
	decision.SelectedStrategy = optimize.ExpansionSearchStepwiseForward
	decision.PlannedPolicy = policy
	decision.SelectionMode = "shadow_tool"
	decision.SelectorVersion = string(policy)
	decision.EmittedPolicy = policy
	decision.EmittedCandidates = []optimize.ExpansionSearchStrategy{optimize.ExpansionSearchStepwiseForward}
	decision.ExecutionBoundary = optimize.ExpansionSearchExecutionBoundaryInlineStatement
	decision.FallbackReason = ""

	return nil
}

// decodeCypherStringLiteral decodes Cypher escape sequences by interpreting the token as a quoted Go string.
func decodeCypherStringLiteral(raw string) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("invalid cypher string literal: %q", raw)
	} else if quote := raw[0]; (quote != '\'' && quote != '"') || raw[len(raw)-1] != quote {
		return "", fmt.Errorf("invalid cypher string literal: missing or mismatched surrounding quotes: %q", raw)
	}
	// Cypher parser wraps string literals with ' characters
	var (
		body = raw[1 : len(raw)-1]
		b    strings.Builder
	)

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

package neo4j

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	cypherfmt "github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
)

type patternPropertyParameterRewriter struct {
	parameters          map[string]any
	rewrittenParameters map[string]any
	nextParameterID     int
	rewritten           bool
}

func rewritePatternPropertyParameters(query string, parameters map[string]any) (string, map[string]any, error) {
	if !strings.Contains(query, "$") {
		return query, parameters, nil
	}

	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return query, parameters, nil
	}

	rewriter := &patternPropertyParameterRewriter{
		parameters: parameters,
	}

	if err := rewriter.rewriteRegularQuery(parsed); err != nil {
		return "", nil, err
	}
	if !rewriter.rewritten {
		return query, parameters, nil
	}

	rewrittenQuery, err := cypherfmt.RegularQuery(parsed, false)
	if err != nil {
		return "", nil, err
	}

	return rewrittenQuery, rewriter.rewrittenParameters, nil
}

func rewriteQuery(query string, parameters map[string]any) (string, map[string]any, error) {
	if !queryMayNeedRewrite(query) {
		return query, parameters, nil
	}

	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return query, parameters, nil
	}

	var (
		rewrittenParameters = parameters
		rewritten           bool
	)

	if strings.Contains(query, "$") {
		parameterRewriter := &patternPropertyParameterRewriter{
			parameters: parameters,
		}

		if err := parameterRewriter.rewriteRegularQuery(parsed); err != nil {
			return "", nil, err
		}

		if parameterRewriter.rewritten {
			rewritten = true
			rewrittenParameters = parameterRewriter.rewrittenParameters
		}
	}

	temporalRewriter := &temporalPropertyComparisonRewriter{}
	temporalRewriter.rewriteRegularQuery(parsed)
	if temporalRewriter.rewritten {
		rewritten = true
	}

	if !rewritten {
		return query, parameters, nil
	}

	rewrittenQuery, err := cypherfmt.RegularQuery(parsed, false)
	if err != nil {
		return "", nil, err
	}

	return rewrittenQuery, rewrittenParameters, nil
}

func queryMayNeedRewrite(query string) bool {
	return strings.Contains(query, "$") || queryMayContainTemporalFunctionCall(query)
}

func queryMayContainTemporalFunctionCall(query string) bool {
	query = strings.ToLower(query)

	for _, functionName := range []string{
		cypher.DateFunction,
		cypher.TimeFunction,
		cypher.LocalTimeFunction,
		cypher.DateTimeFunction,
		cypher.LocalDateTimeFunction,
	} {
		if containsFunctionCall(query, functionName) {
			return true
		}
	}

	return false
}

func containsFunctionCall(query, functionName string) bool {
	for searchOffset := 0; searchOffset < len(query); {
		functionOffset := strings.Index(query[searchOffset:], functionName)
		if functionOffset == -1 {
			return false
		}

		functionOffset += searchOffset
		if functionOffset > 0 && isCypherIdentifierCharacter(query[functionOffset-1]) {
			searchOffset = functionOffset + 1
			continue
		}

		nextOffset := functionOffset + len(functionName)
		for nextOffset < len(query) && isCypherWhitespace(query[nextOffset]) {
			nextOffset++
		}

		if nextOffset < len(query) && query[nextOffset] == '(' {
			return true
		}

		searchOffset = functionOffset + 1
	}

	return false
}

func isCypherIdentifierCharacter(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '_'
}

func isCypherWhitespace(value byte) bool {
	switch value {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func (s *patternPropertyParameterRewriter) rewriteRegularQuery(query *cypher.RegularQuery) error {
	if query == nil || query.SingleQuery == nil {
		return nil
	}

	if singlePartQuery := query.SingleQuery.SinglePartQuery; singlePartQuery != nil {
		if err := s.rewriteReadingClauses(singlePartQuery.ReadingClauses); err != nil {
			return err
		}
	}

	if multiPartQuery := query.SingleQuery.MultiPartQuery; multiPartQuery != nil {
		for _, part := range multiPartQuery.Parts {
			if err := s.rewriteReadingClauses(part.ReadingClauses); err != nil {
				return err
			}
		}
		if multiPartQuery.SinglePartQuery != nil {
			if err := s.rewriteReadingClauses(multiPartQuery.SinglePartQuery.ReadingClauses); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *patternPropertyParameterRewriter) rewriteReadingClauses(readingClauses []*cypher.ReadingClause) error {
	for _, readingClause := range readingClauses {
		if readingClause.Match == nil {
			continue
		}

		for _, patternPart := range readingClause.Match.Pattern {
			for _, patternElement := range patternPart.PatternElements {
				if nodePattern, isNodePattern := patternElement.AsNodePattern(); isNodePattern {
					if err := s.rewriteProperties(&nodePattern.Properties); err != nil {
						return err
					}
				} else if relationshipPattern, isRelationshipPattern := patternElement.AsRelationshipPattern(); isRelationshipPattern {
					if err := s.rewriteProperties(&relationshipPattern.Properties); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (s *patternPropertyParameterRewriter) rewriteProperties(properties *cypher.Expression) error {
	if properties == nil || *properties == nil {
		return nil
	}

	cypherProperties, isProperties := (*properties).(*cypher.Properties)
	if !isProperties || cypherProperties.Parameter == nil {
		return nil
	}

	parameterName := cypherProperties.Parameter.Symbol
	parameterValue, hasParameter := s.parameters[parameterName]
	if !hasParameter {
		return fmt.Errorf("pattern property parameter %q was not provided", parameterName)
	}

	propertyMap, err := parameterPropertyMap(parameterValue)
	if err != nil {
		return fmt.Errorf("pattern property parameter %q: %w", parameterName, err)
	}
	if len(propertyMap) == 0 {
		*properties = nil
		s.rewritten = true
		return nil
	}

	literal := cypher.NewMapLiteral()
	keys := make([]string, 0, len(propertyMap))
	for key := range propertyMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		rewrittenParameter := s.nextParameterName()
		s.ensureRewrittenParameters()[rewrittenParameter] = propertyMap[key]
		literal[key] = &cypher.Parameter{Symbol: rewrittenParameter}
	}

	*properties = &cypher.Properties{Map: literal}
	s.rewritten = true
	return nil
}

func (s *patternPropertyParameterRewriter) ensureRewrittenParameters() map[string]any {
	if s.rewrittenParameters == nil {
		s.rewrittenParameters = make(map[string]any, len(s.parameters)+4)
		for key, value := range s.parameters {
			s.rewrittenParameters[key] = value
		}
	}

	return s.rewrittenParameters
}

func (s *patternPropertyParameterRewriter) nextParameterName() string {
	for {
		next := fmt.Sprintf("__dawgs_pattern_property_%d", s.nextParameterID)
		s.nextParameterID++

		if _, exists := s.ensureRewrittenParameters()[next]; !exists {
			return next
		}
	}
}

func parameterPropertyMap(value any) (map[string]any, error) {
	switch typedValue := value.(type) {
	case map[string]any:
		return typedValue, nil
	case graph.Properties:
		return typedValue.MapOrEmpty(), nil
	case *graph.Properties:
		return typedValue.MapOrEmpty(), nil
	}

	reflectedValue := reflect.ValueOf(value)
	if reflectedValue.Kind() != reflect.Map || reflectedValue.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("expected map with string keys but received %T", value)
	}

	propertyMap := make(map[string]any, reflectedValue.Len())
	iter := reflectedValue.MapRange()
	for iter.Next() {
		propertyMap[iter.Key().String()] = iter.Value().Interface()
	}

	return propertyMap, nil
}

type temporalPropertyComparisonRewriter struct {
	rewritten bool
}

func (s *temporalPropertyComparisonRewriter) rewriteRegularQuery(query *cypher.RegularQuery) {
	if query == nil || query.SingleQuery == nil {
		return
	}

	if singlePartQuery := query.SingleQuery.SinglePartQuery; singlePartQuery != nil {
		s.rewriteSinglePartQuery(singlePartQuery)
	}

	if multiPartQuery := query.SingleQuery.MultiPartQuery; multiPartQuery != nil {
		for _, part := range multiPartQuery.Parts {
			s.rewriteMultiPartQueryPart(part)
		}

		if multiPartQuery.SinglePartQuery != nil {
			s.rewriteSinglePartQuery(multiPartQuery.SinglePartQuery)
		}
	}
}

func (s *temporalPropertyComparisonRewriter) rewriteSinglePartQuery(query *cypher.SinglePartQuery) {
	if query == nil {
		return
	}

	s.rewriteReadingClauseExpressions(query.ReadingClauses)
	s.rewriteProjection(query.Return)

	for idx, expression := range query.UpdatingClauses {
		query.UpdatingClauses[idx] = s.rewriteExpression(expression)
	}
}

func (s *temporalPropertyComparisonRewriter) rewriteMultiPartQueryPart(part *cypher.MultiPartQueryPart) {
	if part == nil {
		return
	}

	s.rewriteReadingClauseExpressions(part.ReadingClauses)

	for _, updatingClause := range part.UpdatingClauses {
		if updatingClause != nil {
			updatingClause.Clause = s.rewriteExpression(updatingClause.Clause)
		}
	}

	s.rewriteWith(part.With)
}

func (s *temporalPropertyComparisonRewriter) rewriteReadingClauseExpressions(readingClauses []*cypher.ReadingClause) {
	for _, readingClause := range readingClauses {
		if readingClause == nil {
			continue
		}

		if readingClause.Match != nil {
			s.rewriteWhere(readingClause.Match.Where)
		}

		if readingClause.Unwind != nil {
			readingClause.Unwind.Expression = s.rewriteExpression(readingClause.Unwind.Expression)
		}
	}
}

func (s *temporalPropertyComparisonRewriter) rewriteWith(with *cypher.With) {
	if with == nil {
		return
	}

	s.rewriteProjectionItems(with.Projection)
	s.rewriteWhere(with.Where)
}

func (s *temporalPropertyComparisonRewriter) rewriteProjection(returnClause *cypher.Return) {
	if returnClause == nil {
		return
	}

	s.rewriteProjectionItems(returnClause.Projection)
}

func (s *temporalPropertyComparisonRewriter) rewriteProjectionItems(projection *cypher.Projection) {
	if projection == nil {
		return
	}

	for idx, item := range projection.Items {
		projection.Items[idx] = s.rewriteExpression(item)
	}

	if projection.Order != nil {
		for _, item := range projection.Order.Items {
			if item != nil {
				item.Expression = s.rewriteExpression(item.Expression)
			}
		}
	}

	if projection.Skip != nil {
		projection.Skip.Value = s.rewriteExpression(projection.Skip.Value)
	}

	if projection.Limit != nil {
		projection.Limit.Value = s.rewriteExpression(projection.Limit.Value)
	}
}

func (s *temporalPropertyComparisonRewriter) rewriteWhere(where *cypher.Where) {
	if where == nil {
		return
	}

	s.rewriteExpressionList(where)
}

func (s *temporalPropertyComparisonRewriter) rewriteExpressionList(expressions cypher.ExpressionList) {
	for idx := 0; idx < expressions.Len(); idx++ {
		expressions.Replace(idx, s.rewriteExpression(expressions.Get(idx)))
	}
}

func (s *temporalPropertyComparisonRewriter) rewriteExpression(expression cypher.Expression) cypher.Expression {
	switch typedExpression := expression.(type) {
	case *cypher.Comparison:
		return s.rewriteComparison(typedExpression)

	case *cypher.Parenthetical:
		typedExpression.Expression = s.rewriteExpression(typedExpression.Expression)

	case *cypher.Negation:
		typedExpression.Expression = s.rewriteExpression(typedExpression.Expression)

	case *cypher.Disjunction:
		s.rewriteExpressionList(typedExpression)

	case *cypher.ExclusiveDisjunction:
		s.rewriteExpressionList(typedExpression)

	case *cypher.Conjunction:
		s.rewriteExpressionList(typedExpression)

	case *cypher.ArithmeticExpression:
		typedExpression.Left = s.rewriteExpression(typedExpression.Left)
		for _, partial := range typedExpression.Partials {
			partial.Right = s.rewriteExpression(partial.Right)
		}

	case *cypher.UnaryAddOrSubtractExpression:
		typedExpression.Right = s.rewriteExpression(typedExpression.Right)

	case *cypher.FunctionInvocation:
		for idx, argument := range typedExpression.Arguments {
			typedExpression.Arguments[idx] = s.rewriteExpression(argument)
		}

	case *cypher.ProjectionItem:
		typedExpression.Expression = s.rewriteExpression(typedExpression.Expression)

	case *cypher.FilterExpression:
		if typedExpression.Specifier != nil {
			typedExpression.Specifier.Expression = s.rewriteExpression(typedExpression.Specifier.Expression)
		}
		s.rewriteWhere(typedExpression.Where)

	case *cypher.Quantifier:
		if typedExpression.Filter != nil {
			typedExpression.Filter = s.rewriteExpression(typedExpression.Filter).(*cypher.FilterExpression)
		}

	case *cypher.MapLiteral:
		for key, value := range *typedExpression {
			(*typedExpression)[key] = s.rewriteExpression(value)
		}

	case cypher.MapLiteral:
		for key, value := range typedExpression {
			typedExpression[key] = s.rewriteExpression(value)
		}

	case *cypher.ListLiteral:
		for idx, value := range *typedExpression {
			(*typedExpression)[idx] = s.rewriteExpression(value)
		}
	}

	return expression
}

func (s *temporalPropertyComparisonRewriter) rewriteComparison(comparison *cypher.Comparison) cypher.Expression {
	comparison.Left = s.rewriteExpression(comparison.Left)

	for idx, partial := range comparison.Partials {
		partial.Right = s.rewriteExpression(partial.Right)

		if !isTemporalComparisonOperator(partial.Operator) {
			continue
		}

		left := comparisonOperand(comparison, idx)
		right := partial.Right

		if temporalFunction, ok := temporalExpressionFunction(right); ok {
			if wrapped, didRewrite := temporalPropertyLookup(left, temporalFunction); didRewrite {
				setComparisonOperand(comparison, idx, wrapped)
				s.rewritten = true
			}
		}

		left = comparisonOperand(comparison, idx)
		if temporalFunction, ok := temporalExpressionFunction(left); ok {
			if wrapped, didRewrite := temporalPropertyLookup(right, temporalFunction); didRewrite {
				partial.Right = wrapped
				s.rewritten = true
			}
		}
	}

	return comparison
}

func comparisonOperand(comparison *cypher.Comparison, partialIdx int) cypher.Expression {
	if partialIdx == 0 {
		return comparison.Left
	}

	return comparison.Partials[partialIdx-1].Right
}

func setComparisonOperand(comparison *cypher.Comparison, partialIdx int, expression cypher.Expression) {
	if partialIdx == 0 {
		comparison.Left = expression
	} else {
		comparison.Partials[partialIdx-1].Right = expression
	}
}

func temporalPropertyLookup(expression cypher.Expression, temporalFunction string) (cypher.Expression, bool) {
	propertyLookup, isPropertyLookup := expression.(*cypher.PropertyLookup)
	if !isPropertyLookup {
		return expression, false
	}

	if _, isEntityVariable := propertyLookup.Atom.(*cypher.Variable); !isEntityVariable {
		return expression, false
	}

	return cypher.NewSimpleFunctionInvocation(temporalFunction, expression), true
}

func isTemporalComparisonOperator(operator cypher.Operator) bool {
	switch operator {
	case cypher.OperatorEquals,
		cypher.OperatorNotEquals,
		cypher.OperatorGreaterThan,
		cypher.OperatorGreaterThanOrEqualTo,
		cypher.OperatorLessThan,
		cypher.OperatorLessThanOrEqualTo:
		return true
	default:
		return false
	}
}

func temporalExpressionFunction(expression cypher.Expression) (string, bool) {
	switch typedExpression := expression.(type) {
	case *cypher.Parenthetical:
		return temporalExpressionFunction(typedExpression.Expression)

	case *cypher.FunctionInvocation:
		functionName := strings.ToLower(typedExpression.Name)
		if isTemporalFunction(functionName) {
			return functionName, true
		}

	case *cypher.ArithmeticExpression:
		return temporalArithmeticExpressionFunction(typedExpression)
	}

	return "", false
}

func temporalArithmeticExpressionFunction(expression *cypher.ArithmeticExpression) (string, bool) {
	functionName, isTemporal := temporalExpressionFunction(expression.Left)
	if !isTemporal {
		return "", false
	}

	for _, partial := range expression.Partials {
		if partial.Operator != cypher.OperatorAdd && partial.Operator != cypher.OperatorSubtract {
			return "", false
		}

		if !isDurationExpression(partial.Right) {
			return "", false
		}
	}

	return functionName, true
}

func isDurationExpression(expression cypher.Expression) bool {
	function, isFunction := expression.(*cypher.FunctionInvocation)
	if !isFunction {
		return false
	}

	return strings.EqualFold(function.Name, cypher.DurationFunction)
}

func isTemporalFunction(functionName string) bool {
	switch functionName {
	case cypher.DateFunction,
		cypher.TimeFunction,
		cypher.LocalTimeFunction,
		cypher.DateTimeFunction,
		cypher.LocalDateTimeFunction:
		return true
	default:
		return false
	}
}

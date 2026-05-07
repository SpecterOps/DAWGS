package v2

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

func isNodePattern(seen *identifierSet, identifiers runtimeIdentifiers) bool {
	return seen.Contains(identifiers.node)
}

func isRelationshipPattern(seen *identifierSet, identifiers runtimeIdentifiers) bool {
	var (
		hasStart        = seen.Contains(identifiers.start)
		hasRelationship = seen.Contains(identifiers.relationship)
		hasEnd          = seen.Contains(identifiers.end)
	)

	return hasStart || hasRelationship || hasEnd
}

func prepareNodePattern(match *cypher.Match, seen *identifierSet, identifiers runtimeIdentifiers) error {
	if isRelationshipPattern(seen, identifiers) {
		return fmt.Errorf("query mixes node and relationship query identifiers")
	}

	match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
		Variable: identifiers.Node(),
	})

	return nil
}

func validateRelationshipDirection(direction graph.Direction) error {
	switch direction {
	case graph.DirectionInbound, graph.DirectionOutbound:
		return nil
	default:
		return fmt.Errorf("unsupported relationship direction: %s", direction)
	}
}

func prepareRelationshipPattern(match *cypher.Match, seen *identifierSet, identifiers runtimeIdentifiers, relationshipKinds graph.Kinds, direction graph.Direction, shortestPaths, allShortestPaths bool) error {
	if shortestPaths && allShortestPaths {
		return errors.New("query is requesting both all shortest paths and shortest paths")
	}

	if err := validateRelationshipDirection(direction); err != nil {
		return err
	}

	var (
		newPatternPart   = match.NewPatternPart()
		startNodeSeen    = seen.Contains(identifiers.start)
		relationshipSeen = seen.Contains(identifiers.relationship)
		endNodeSeen      = seen.Contains(identifiers.end)
	)

	newPatternPart.ShortestPathPattern = shortestPaths
	newPatternPart.AllShortestPathsPattern = allShortestPaths

	if startNodeSeen {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: identifiers.Start(),
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	relationshipPattern := &cypher.RelationshipPattern{
		Kinds:     relationshipKinds,
		Direction: direction,
	}

	if relationshipSeen {
		relationshipPattern.Variable = identifiers.Relationship()
	}

	if shortestPaths || allShortestPaths {
		newPatternPart.Variable = identifiers.Path()
		relationshipPattern.Range = &cypher.PatternRange{}
	}

	newPatternPart.AddPatternElements(relationshipPattern)

	if endNodeSeen {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: identifiers.End(),
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	return nil
}

func prepareCreateRelationshipMatch(match *cypher.Match, seen *identifierSet, identifiers runtimeIdentifiers) error {
	if seen.Contains(identifiers.start) {
		match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
			Variable: identifiers.Start(),
		})
	}

	if seen.Contains(identifiers.end) {
		match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
			Variable: identifiers.End(),
		})
	}

	return nil
}

func isDetachDeleteQualifier(qualifier cypher.Expression, identifiers runtimeIdentifiers) bool {
	variable, typeOK := qualifier.(*cypher.Variable)
	if !typeOK {
		return false
	}

	switch variable.Symbol {
	case identifiers.node, identifiers.start, identifiers.end:
		return true
	default:
		return false
	}
}

func kindProjectionExpression(role string, identifier *cypher.Variable) (cypher.Expression, error) {
	switch role {
	case Identifiers.node, Identifiers.start, Identifiers.end:
		return cypher.NewSimpleFunctionInvocation(cypher.NodeLabelsFunction, identifier), nil

	case Identifiers.relationship:
		return cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, identifier), nil

	default:
		return nil, fmt.Errorf("invalid kind projection reference: %s", identifier.Symbol)
	}
}

func invalidExpression(err error) *cypher.FunctionInvocation {
	return cypher.WithErrors(cypher.NewSimpleFunctionInvocation("__invalid_expression__"), err)
}

func isNilPointer(value any) bool {
	if value == nil {
		return true
	}

	reflectValue := reflect.ValueOf(value)
	return reflectValue.Kind() == reflect.Ptr && reflectValue.IsNil()
}

func expressionOrError(value any) cypher.Expression {
	if expression, err := projectionExpression(value); err != nil {
		return invalidExpression(err)
	} else {
		return expression
	}
}

func variableReference(value any) (*cypher.Variable, error) {
	expression, err := projectionExpression(value)
	if err != nil {
		return nil, err
	}

	if variable, typeOK := expression.(*cypher.Variable); !typeOK {
		return nil, fmt.Errorf("expected variable reference, got %T", expression)
	} else {
		return variable, nil
	}
}

func propertyLookupOrError(reference any, propertyName string) cypher.Expression {
	if variable, err := variableReference(reference); err != nil {
		return invalidExpression(err)
	} else {
		return cypher.NewPropertyLookup(variable.Symbol, propertyName)
	}
}

func sortedPropertyKeys(properties map[string]any) []string {
	keys := make([]string, 0, len(properties))

	for key := range properties {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func projectionExpression(value any) (cypher.Expression, error) {
	if isNilPointer(value) {
		return nil, fmt.Errorf("expression is nil: %T", value)
	}

	switch typedValue := value.(type) {
	case kindContinuation:
		return kindProjectionExpression(typedValue.role, typedValue.identifier)

	case kindsContinuation:
		return kindProjectionExpression(typedValue.role, typedValue.identifier)

	case QualifiedExpression:
		return typedValue.qualifier(), nil

	case *cypher.ProjectionItem:
		if typedValue.Expression == nil {
			return nil, fmt.Errorf("projection item has nil expression")
		}

		return typedValue.Expression, nil

	case *cypher.Parameter:
		return typedValue, nil

	case *cypher.Literal:
		return typedValue, nil

	case *cypher.Variable:
		return typedValue, nil

	case *cypher.PropertyLookup:
		return typedValue, nil

	case *cypher.FunctionInvocation:
		return typedValue, nil

	case *cypher.Parenthetical:
		return typedValue, nil

	case *cypher.Comparison:
		return typedValue, nil

	case *cypher.Negation:
		return typedValue, nil

	case *cypher.Conjunction:
		return typedValue, nil

	case *cypher.Disjunction:
		return typedValue, nil

	case *cypher.ExclusiveDisjunction:
		return typedValue, nil

	case *cypher.KindMatcher:
		return typedValue, nil

	case *cypher.ListLiteral:
		return typedValue, nil

	case cypher.MapLiteral:
		return typedValue, nil

	case *cypher.PatternPredicate:
		return typedValue, nil

	case *cypher.ArithmeticExpression:
		return typedValue, nil

	case *cypher.UnaryAddOrSubtractExpression:
		return typedValue, nil

	case *cypher.FilterExpression:
		return typedValue, nil

	case *cypher.IDInCollection:
		return typedValue, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", value)
	}
}

func validateExpressionValue(expression cypher.Expression, context string) error {
	if isNilPointer(expression) {
		return fmt.Errorf("%s has nil expression", context)
	}

	return collectModelErrors(expression)
}

func projectionItemFromValue(value any) (*cypher.ProjectionItem, error) {
	if projectionItem, typeOK := value.(*cypher.ProjectionItem); typeOK {
		if projectionItem == nil {
			return nil, fmt.Errorf("projection item is nil")
		}

		if err := validateExpressionValue(projectionItem.Expression, "projection item"); err != nil {
			return nil, err
		}

		if err := collectModelErrors(projectionItem); err != nil {
			return nil, err
		}

		return projectionItem, nil
	}

	if expression, err := projectionExpression(value); err != nil {
		return nil, err
	} else {
		return cypher.NewProjectionItemWithExpr(expression), nil
	}
}

func sortItemFromValue(value any) (*cypher.SortItem, error) {
	if sortItem, typeOK := value.(*cypher.SortItem); typeOK {
		if sortItem == nil {
			return nil, fmt.Errorf("sort item is nil")
		}

		if err := validateExpressionValue(sortItem.Expression, "sort item"); err != nil {
			return nil, err
		}

		if err := collectModelErrors(sortItem); err != nil {
			return nil, err
		}

		return sortItem, nil
	}

	if expression, err := projectionExpression(value); err != nil {
		return nil, err
	} else {
		return &cypher.SortItem{
			Ascending:  true,
			Expression: expression,
		}, nil
	}
}

func projectionItemsFromReturn(returnClause *cypher.Return) ([]*cypher.ProjectionItem, error) {
	if returnClause == nil {
		return nil, fmt.Errorf("return clause is nil")
	}

	if returnClause.Projection == nil {
		return nil, fmt.Errorf("return clause has nil projection")
	}

	projectionItems := make([]*cypher.ProjectionItem, 0, len(returnClause.Projection.Items))

	for _, returnItem := range returnClause.Projection.Items {
		if projectionItem, err := projectionItemFromValue(returnItem); err != nil {
			return nil, err
		} else {
			projectionItems = append(projectionItems, projectionItem)
		}
	}

	return projectionItems, nil
}

func sortItemsFromOrder(order *cypher.Order) ([]*cypher.SortItem, error) {
	if order == nil {
		return nil, fmt.Errorf("order is nil")
	}

	sortItems := make([]*cypher.SortItem, 0, len(order.Items))

	for _, sortItem := range order.Items {
		if normalizedSortItem, err := sortItemFromValue(sortItem); err != nil {
			return nil, err
		} else {
			sortItems = append(sortItems, normalizedSortItem)
		}
	}

	return sortItems, nil
}

type identifierSet struct {
	identifiers map[string]struct{}
}

func newIdentifierSet() *identifierSet {
	return &identifierSet{
		identifiers: map[string]struct{}{},
	}
}

func (s *identifierSet) Add(identifier string) {
	s.identifiers[identifier] = struct{}{}
}

func (s *identifierSet) Len() int {
	return len(s.identifiers)
}

func (s *identifierSet) Clone() *identifierSet {
	clone := newIdentifierSet()
	clone.Or(s)
	return clone
}

func (s *identifierSet) Or(other *identifierSet) {
	for otherIdentifier := range other.identifiers {
		s.identifiers[otherIdentifier] = struct{}{}
	}
}

func (s *identifierSet) Remove(other *identifierSet) {
	for otherIdentifier := range other.identifiers {
		delete(s.identifiers, otherIdentifier)
	}
}

func (s *identifierSet) Contains(identifier string) bool {
	_, containsIdentifier := s.identifiers[identifier]
	return containsIdentifier
}

func (s *identifierSet) CollectFromExpression(expr cypher.Expression) error {
	if exprIdentifiers, err := extractCypherIdentifiers(expr); err != nil {
		return err
	} else {
		s.Or(exprIdentifiers)
		return nil
	}
}

func (s *identifierSet) CollectFromValue(value any) error {
	switch typedValue := value.(type) {
	case nil:
		return nil

	case QualifiedExpression:
		return s.CollectFromExpression(typedValue.qualifier())

	case kindContinuation:
		s.Add(typedValue.identifier.Symbol)
		return nil

	case kindsContinuation:
		s.Add(typedValue.identifier.Symbol)
		return nil

	case *cypher.Return:
		if projectionItems, err := projectionItemsFromReturn(typedValue); err != nil {
			return err
		} else {
			for _, projectionItem := range projectionItems {
				if err := s.CollectFromExpression(projectionItem); err != nil {
					return err
				}
			}
		}

	case *cypher.Order:
		if sortItems, err := sortItemsFromOrder(typedValue); err != nil {
			return err
		} else {
			for _, sortItem := range sortItems {
				if err := s.CollectFromExpression(sortItem); err != nil {
					return err
				}
			}
		}

	case *cypher.SortItem:
		if sortItem, err := sortItemFromValue(typedValue); err != nil {
			return err
		} else {
			return s.CollectFromExpression(sortItem)
		}

	case *cypher.Set:
		return s.CollectFromExpression(typedValue)

	case *cypher.SetItem:
		return s.CollectFromExpression(typedValue)

	case *cypher.Remove:
		return s.CollectFromExpression(typedValue)

	case *cypher.RemoveItem:
		return s.CollectFromExpression(typedValue)

	case *cypher.NodePattern:
		return s.CollectFromExpression(typedValue)

	case *cypher.RelationshipPattern:
		return s.CollectFromExpression(typedValue)

	case *cypher.Variable:
		return s.CollectFromExpression(typedValue)

	case *cypher.FunctionInvocation:
		return s.CollectFromExpression(typedValue)

	case *cypher.PropertyLookup:
		return s.CollectFromExpression(typedValue)

	case []any:
		for _, item := range typedValue {
			if err := s.CollectFromValue(item); err != nil {
				return err
			}
		}

	case []cypher.SyntaxNode:
		for _, item := range typedValue {
			if err := s.CollectFromValue(item); err != nil {
				return err
			}
		}

	case []cypher.Expression:
		for _, item := range typedValue {
			if err := s.CollectFromValue(item); err != nil {
				return err
			}
		}

	case []*cypher.SetItem:
		for _, item := range typedValue {
			if err := s.CollectFromValue(item); err != nil {
				return err
			}
		}

	case []*cypher.RemoveItem:
		for _, item := range typedValue {
			if err := s.CollectFromValue(item); err != nil {
				return err
			}
		}

	default:
		return nil
	}

	return nil
}

func collectIdentifiersFromValues(values ...any) (*identifierSet, error) {
	identifiers := newIdentifierSet()

	for _, value := range values {
		if err := identifiers.CollectFromValue(value); err != nil {
			return nil, err
		}
	}

	return identifiers, nil
}

type createScope struct {
	identifiers         *identifierSet
	createsRelationship bool
}

func collectCreateScope(identifiers runtimeIdentifiers, values ...any) (*createScope, error) {
	scope := &createScope{
		identifiers: newIdentifierSet(),
	}

	for _, value := range values {
		switch typedValue := value.(type) {
		case *cypher.RelationshipPattern:
			scope.createsRelationship = true
			scope.identifiers.Add(identifiers.start)
			scope.identifiers.Add(identifiers.end)

			if typedValue.Variable != nil {
				scope.identifiers.Add(typedValue.Variable.Symbol)
			}

		default:
			if err := scope.identifiers.CollectFromValue(value); err != nil {
				return nil, err
			}
		}
	}

	return scope, nil
}

type identifierExtractor struct {
	walk.Visitor[cypher.SyntaxNode]

	seen *identifierSet
}

func newIdentifierExtractor() *identifierExtractor {
	return &identifierExtractor{
		Visitor: walk.NewVisitor[cypher.SyntaxNode](),
		seen:    newIdentifierSet(),
	}
}

func (s *identifierExtractor) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Variable:
		s.seen.Add(typedNode.Symbol)

	case *cypher.NodePattern:
		if typedNode.Variable != nil {
			s.seen.Add(typedNode.Variable.Symbol)
		}

	case *cypher.RelationshipPattern:
		if typedNode.Variable != nil {
			s.seen.Add(typedNode.Variable.Symbol)
		}

	case *cypher.PatternPart:
		if typedNode.Variable != nil {
			s.seen.Add(typedNode.Variable.Symbol)
		}
	}
}

func extractCypherIdentifiers(expression cypher.Expression) (*identifierSet, error) {
	var (
		identifierExtractorVisitor = newIdentifierExtractor()
		err                        = walk.Cypher(expression, identifierExtractorVisitor)
	)

	return identifierExtractorVisitor.seen, err
}

func collectModelErrors(node cypher.SyntaxNode) error {
	var modelErrors []error

	if err := walk.Cypher(node, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if errorNode, typeOK := node.(cypher.Fallible); typeOK {
			modelErrors = append(modelErrors, errorNode.Errors()...)
		}
	})); err != nil {
		modelErrors = append(modelErrors, err)
	}

	return errors.Join(modelErrors...)
}

func collectModelErrorsFromKnownValues(values ...any) error {
	var modelErrors []error

	for _, value := range values {
		switch typedValue := value.(type) {
		case nil:
			continue

		case []any:
			if err := collectModelErrorsFromKnownValues(typedValue...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case []cypher.SyntaxNode:
			if err := collectModelErrorsFromKnownValues(anySlice(typedValue)...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case []cypher.Expression:
			if err := collectModelErrorsFromKnownValues(anySlice(typedValue)...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case []*cypher.SetItem:
			if err := collectModelErrorsFromKnownValues(anySlice(typedValue)...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case []*cypher.RemoveItem:
			if err := collectModelErrorsFromKnownValues(anySlice(typedValue)...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case []*cypher.ProjectionItem:
			if err := collectModelErrorsFromKnownValues(anySlice(typedValue)...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case []*cypher.SortItem:
			if err := collectModelErrorsFromKnownValues(anySlice(typedValue)...); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.Order:
			if _, err := sortItemsFromOrder(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.ProjectionItem:
			if _, err := projectionItemFromValue(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.Return:
			if _, err := projectionItemsFromReturn(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.SortItem:
			if _, err := sortItemFromValue(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.ArithmeticExpression,
			*cypher.Comparison,
			*cypher.Conjunction,
			*cypher.Create,
			*cypher.Delete,
			*cypher.Disjunction,
			*cypher.ExclusiveDisjunction,
			*cypher.FilterExpression,
			*cypher.FunctionInvocation,
			*cypher.IDInCollection,
			*cypher.KindMatcher,
			*cypher.ListLiteral,
			*cypher.Negation,
			*cypher.NodePattern,
			*cypher.Parenthetical,
			*cypher.PatternPredicate,
			*cypher.PropertyLookup,
			*cypher.RelationshipPattern,
			*cypher.Remove,
			*cypher.RemoveItem,
			*cypher.Set,
			*cypher.SetItem,
			*cypher.UnaryAddOrSubtractExpression,
			*cypher.UpdatingClause,
			*cypher.Variable:
			if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}
		}
	}

	return errors.Join(modelErrors...)
}

func anySlice[T any](values []T) []any {
	items := make([]any, len(values))

	for idx, value := range values {
		items[idx] = value
	}

	return items
}

type parameterMaterializer struct {
	walk.Visitor[cypher.SyntaxNode]

	parameters map[string]any
	nextIndex  int
}

func newParameterMaterializer() *parameterMaterializer {
	return &parameterMaterializer{
		Visitor:    walk.NewVisitor[cypher.SyntaxNode](),
		parameters: map[string]any{},
	}
}

func (s *parameterMaterializer) nextSymbol() string {
	for {
		symbol := "p" + strconv.Itoa(s.nextIndex)
		s.nextIndex++

		if _, taken := s.parameters[symbol]; !taken {
			return symbol
		}
	}
}

func (s *parameterMaterializer) Enter(node cypher.SyntaxNode) {
	parameter, typeOK := node.(*cypher.Parameter)
	if !typeOK {
		return
	}

	if parameter.Symbol == "" {
		parameter.Symbol = s.nextSymbol()
	}

	if existingValue, exists := s.parameters[parameter.Symbol]; exists && !reflect.DeepEqual(existingValue, parameter.Value) {
		s.SetErrorf("parameter %s is bound to multiple values", parameter.Symbol)
		return
	}

	s.parameters[parameter.Symbol] = parameter.Value
}

func materializeParameters(query *cypher.RegularQuery) (map[string]any, error) {
	materializer := newParameterMaterializer()

	if err := walk.Cypher(query, materializer); err != nil {
		return nil, err
	}

	return materializer.parameters, nil
}

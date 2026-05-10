package v2

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

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

func runtimeIdentifierSet(identifiers runtimeIdentifiers) *identifierSet {
	return newIdentifierSet(
		identifiers.path,
		identifiers.node,
		identifiers.start,
		identifiers.relationship,
		identifiers.end,
	)
}

func nodePatternIdentifierSet(identifiers runtimeIdentifiers) *identifierSet {
	return newIdentifierSet(identifiers.node)
}

func relationshipPatternIdentifierSet(identifiers runtimeIdentifiers, includePath bool) *identifierSet {
	allowedIdentifiers := newIdentifierSet(
		identifiers.start,
		identifiers.relationship,
		identifiers.end,
	)

	if includePath {
		allowedIdentifiers.Add(identifiers.path)
	}

	return allowedIdentifiers
}

func createRelationshipMatchIdentifierSet(identifiers runtimeIdentifiers) *identifierSet {
	return newIdentifierSet(identifiers.start, identifiers.end)
}

func validateKnownIdentifiers(seen *identifierSet, identifiers runtimeIdentifiers) error {
	if identifier, hasIdentifier := seen.FirstOutside(runtimeIdentifierSet(identifiers)); hasIdentifier {
		return fmt.Errorf("query contains unknown identifier %q", identifier)
	}

	return nil
}

func validateBoundIdentifiers(seen, bound *identifierSet) error {
	if identifier, hasIdentifier := seen.FirstOutside(bound); hasIdentifier {
		return fmt.Errorf("query contains unbound identifier %q", identifier)
	}

	return nil
}

func prepareNodePattern(match *cypher.Match, seen *identifierSet, identifiers runtimeIdentifiers) error {
	if isRelationshipPattern(seen, identifiers) {
		return fmt.Errorf("query mixes node and relationship query identifiers")
	}

	if err := validateBoundIdentifiers(seen, nodePatternIdentifierSet(identifiers)); err != nil {
		return err
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

func prepareRelationshipPattern(match *cypher.Match, seen *identifierSet, identifiers runtimeIdentifiers, relationshipKinds graph.Kinds, relationshipRange *cypher.PatternRange, direction graph.Direction, shortestPaths, allShortestPaths bool) error {
	if shortestPaths && allShortestPaths {
		return errors.New("query is requesting both all shortest paths and shortest paths")
	}

	if err := validateRelationshipDirection(direction); err != nil {
		return err
	}

	hasRangedRelationshipPattern := relationshipRange != nil || shortestPaths || allShortestPaths
	if err := validateBoundIdentifiers(seen, relationshipPatternIdentifierSet(identifiers, hasRangedRelationshipPattern)); err != nil {
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

	if shortestPaths || allShortestPaths || (relationshipRange != nil && seen.Contains(identifiers.path)) {
		newPatternPart.Variable = identifiers.Path()
	}

	if relationshipRange != nil {
		relationshipPattern.Range = cypher.Copy(relationshipRange)
	} else if shortestPaths || allShortestPaths {
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
	if err := validateBoundIdentifiers(seen, createRelationshipMatchIdentifierSet(identifiers)); err != nil {
		return err
	}

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
	if !typeOK || variable == nil {
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

func isCypherSymbolStart(char rune) bool {
	return char == '_' || unicode.IsLetter(char) || unicode.In(char, unicode.Nl, unicode.Pc)
}

func isCypherSymbolPart(char rune) bool {
	return isCypherSymbolStart(char) || unicode.IsDigit(char) || unicode.In(char, unicode.Mark, unicode.Sc)
}

func validateCypherSymbol(symbol, context string) error {
	if strings.TrimSpace(symbol) == "" {
		return fmt.Errorf("%s is empty", context)
	}

	if !utf8.ValidString(symbol) {
		return fmt.Errorf("%s has invalid symbol %q", context, symbol)
	}

	for idx, char := range symbol {
		if idx == 0 {
			if !isCypherSymbolStart(char) {
				return fmt.Errorf("%s has invalid symbol %q", context, symbol)
			}
		} else if !isCypherSymbolPart(char) {
			return fmt.Errorf("%s has invalid symbol %q", context, symbol)
		}
	}

	return nil
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

func copyExpression(expression cypher.Expression) cypher.Expression {
	return cypher.Copy(expression)
}

func copyProjectionItem(item *cypher.ProjectionItem) *cypher.ProjectionItem {
	return cypher.Copy(item)
}

func copySortItem(item *cypher.SortItem) *cypher.SortItem {
	return cypher.Copy(item)
}

func copySetItem(item *cypher.SetItem) *cypher.SetItem {
	return cypher.Copy(item)
}

func copyRemoveItem(item *cypher.RemoveItem) *cypher.RemoveItem {
	return cypher.Copy(item)
}

func copySkip(skip *cypher.Skip) *cypher.Skip {
	return cypher.Copy(skip)
}

func copyLimit(limit *cypher.Limit) *cypher.Limit {
	return cypher.Copy(limit)
}

func validateExpressionValue(expression cypher.Expression, context string) error {
	if isNilPointer(expression) {
		return fmt.Errorf("%s has nil expression", context)
	}

	return collectModelErrors(expression)
}

func validateAssignmentOperator(operator cypher.AssignmentOperator) error {
	switch operator {
	case cypher.OperatorAssignment, cypher.OperatorAdditionAssignment, cypher.OperatorLabelAssignment:
		return nil
	default:
		return fmt.Errorf("unsupported set item operator: %s", operator)
	}
}

func setItemFromValue(setItem *cypher.SetItem) (*cypher.SetItem, error) {
	if setItem == nil {
		return nil, fmt.Errorf("set item is nil")
	}

	if err := validateExpressionValue(setItem.Left, "set item left"); err != nil {
		return nil, err
	}

	if err := validateAssignmentOperator(setItem.Operator); err != nil {
		return nil, err
	}

	if err := validateExpressionValue(setItem.Right, "set item right"); err != nil {
		return nil, err
	}

	if err := collectModelErrors(setItem); err != nil {
		return nil, err
	}

	return copySetItem(setItem), nil
}

func setItemsFromSet(setClause *cypher.Set) ([]*cypher.SetItem, error) {
	if setClause == nil {
		return nil, fmt.Errorf("set clause is nil")
	}

	setItems := make([]*cypher.SetItem, 0, len(setClause.Items))

	for _, setItem := range setClause.Items {
		if normalizedSetItem, err := setItemFromValue(setItem); err != nil {
			return nil, err
		} else {
			setItems = append(setItems, normalizedSetItem)
		}
	}

	return setItems, nil
}

func removeItemFromValue(removeItem *cypher.RemoveItem) (*cypher.RemoveItem, error) {
	if removeItem == nil {
		return nil, fmt.Errorf("remove item is nil")
	}

	hasKindMatcher := removeItem.KindMatcher != nil
	hasProperty := !isNilPointer(removeItem.Property)

	switch {
	case hasKindMatcher && hasProperty:
		return nil, fmt.Errorf("remove item has multiple targets")

	case hasKindMatcher:
		if err := collectModelErrors(removeItem.KindMatcher); err != nil {
			return nil, err
		}

	case hasProperty:
		if err := validateExpressionValue(removeItem.Property, "remove item property"); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("remove item has no target")
	}

	if err := collectModelErrors(removeItem); err != nil {
		return nil, err
	}

	return copyRemoveItem(removeItem), nil
}

func removeItemsFromRemove(removeClause *cypher.Remove) ([]*cypher.RemoveItem, error) {
	if removeClause == nil {
		return nil, fmt.Errorf("remove clause is nil")
	}

	removeItems := make([]*cypher.RemoveItem, 0, len(removeClause.Items))

	for _, removeItem := range removeClause.Items {
		if normalizedRemoveItem, err := removeItemFromValue(removeItem); err != nil {
			return nil, err
		} else {
			removeItems = append(removeItems, normalizedRemoveItem)
		}
	}

	return removeItems, nil
}

func validateNodePattern(nodePattern *cypher.NodePattern) error {
	if nodePattern == nil {
		return fmt.Errorf("node pattern is nil")
	}

	return collectModelErrors(nodePattern)
}

func validateRelationshipPattern(relationshipPattern *cypher.RelationshipPattern) error {
	if relationshipPattern == nil {
		return fmt.Errorf("relationship pattern is nil")
	}

	if err := validateRelationshipDirection(relationshipPattern.Direction); err != nil {
		return err
	}

	return collectModelErrors(relationshipPattern)
}

func projectionItemFromValue(value any) (*cypher.ProjectionItem, error) {
	if projectionItem, typeOK := value.(*cypher.ProjectionItem); typeOK {
		if projectionItem == nil {
			return nil, fmt.Errorf("projection item is nil")
		}

		if err := validateExpressionValue(projectionItem.Expression, "projection item"); err != nil {
			return nil, err
		}

		if projectionItem.Alias != nil {
			if err := validateCypherSymbol(projectionItem.Alias.Symbol, "projection alias"); err != nil {
				return nil, err
			}
		}

		if err := collectModelErrors(projectionItem); err != nil {
			return nil, err
		}

		return copyProjectionItem(projectionItem), nil
	}

	if expression, err := projectionExpression(value); err != nil {
		return nil, err
	} else {
		return cypher.NewProjectionItemWithExpr(copyExpression(expression)), nil
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

		return copySortItem(sortItem), nil
	}

	if expression, err := projectionExpression(value); err != nil {
		return nil, err
	} else {
		return &cypher.SortItem{
			Ascending:  true,
			Expression: copyExpression(expression),
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

	if err := validateProjectionMetadata(returnClause.Projection); err != nil {
		return nil, err
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

func validateProjectionMetadata(projection *cypher.Projection) error {
	if projection.Order != nil {
		if _, err := sortItemsFromOrder(projection.Order); err != nil {
			return err
		}
	}

	if projection.Skip != nil {
		if err := validateExpressionValue(projection.Skip.Value, "projection skip"); err != nil {
			return err
		}
	}

	if projection.Limit != nil {
		if err := validateExpressionValue(projection.Limit.Value, "projection limit"); err != nil {
			return err
		}
	}

	return nil
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

func newIdentifierSet(identifiers ...string) *identifierSet {
	set := &identifierSet{
		identifiers: map[string]struct{}{},
	}

	for _, identifier := range identifiers {
		set.Add(identifier)
	}

	return set
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
	if s == nil || other == nil {
		return
	}

	for otherIdentifier := range other.identifiers {
		s.identifiers[otherIdentifier] = struct{}{}
	}
}

func (s *identifierSet) Remove(other *identifierSet) {
	if s == nil || other == nil {
		return
	}

	for otherIdentifier := range other.identifiers {
		delete(s.identifiers, otherIdentifier)
	}
}

func (s *identifierSet) Contains(identifier string) bool {
	_, containsIdentifier := s.identifiers[identifier]
	return containsIdentifier
}

func (s *identifierSet) FirstOutside(allowed *identifierSet) (string, bool) {
	var identifiers []string

	for identifier := range s.identifiers {
		if !allowed.Contains(identifier) {
			identifiers = append(identifiers, identifier)
		}
	}

	sort.Strings(identifiers)

	if len(identifiers) == 0 {
		return "", false
	}

	return identifiers[0], true
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

	case *cypher.ProjectionItem:
		if projectionItem, err := projectionItemFromValue(typedValue); err != nil {
			return err
		} else {
			return s.CollectFromExpression(projectionItem)
		}

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

		if err := s.CollectFromProjectionMetadata(typedValue.Projection); err != nil {
			return err
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
		if setItems, err := setItemsFromSet(typedValue); err != nil {
			return err
		} else {
			for _, setItem := range setItems {
				if err := s.CollectFromExpression(setItem); err != nil {
					return err
				}
			}
		}

	case *cypher.SetItem:
		if setItem, err := setItemFromValue(typedValue); err != nil {
			return err
		} else {
			return s.CollectFromExpression(setItem)
		}

	case *cypher.Remove:
		if removeItems, err := removeItemsFromRemove(typedValue); err != nil {
			return err
		} else {
			for _, removeItem := range removeItems {
				if err := s.CollectFromValue(removeItem); err != nil {
					return err
				}
			}
		}

	case *cypher.RemoveItem:
		if removeItem, err := removeItemFromValue(typedValue); err != nil {
			return err
		} else if removeItem.KindMatcher != nil {
			return s.CollectFromExpression(removeItem.KindMatcher)
		} else {
			return s.CollectFromExpression(removeItem)
		}

	case *cypher.NodePattern:
		if err := validateNodePattern(typedValue); err != nil {
			return err
		}

		return s.CollectFromExpression(typedValue)

	case *cypher.RelationshipPattern:
		if err := validateRelationshipPattern(typedValue); err != nil {
			return err
		}

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

func (s *identifierSet) CollectFromProjectionMetadata(projection *cypher.Projection) error {
	if projection == nil {
		return nil
	}

	if projection.Order != nil {
		if err := s.CollectFromValue(projection.Order); err != nil {
			return err
		}
	}

	if projection.Skip != nil {
		if err := s.CollectFromExpression(projection.Skip.Value); err != nil {
			return err
		}
	}

	if projection.Limit != nil {
		if err := s.CollectFromExpression(projection.Limit.Value); err != nil {
			return err
		}
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
			if err := validateRelationshipPattern(typedValue); err != nil {
				return nil, err
			}

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

		case *cypher.NodePattern:
			if err := validateNodePattern(typedValue); err != nil {
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

		case *cypher.RelationshipPattern:
			if err := validateRelationshipPattern(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.Remove:
			if _, err := removeItemsFromRemove(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.RemoveItem:
			if _, err := removeItemFromValue(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.Set:
			if _, err := setItemsFromSet(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			}

		case *cypher.SetItem:
			if _, err := setItemFromValue(typedValue); err != nil {
				modelErrors = append(modelErrors, err)
			} else if err := collectModelErrors(typedValue); err != nil {
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
			*cypher.Parenthetical,
			*cypher.PatternPredicate,
			*cypher.PropertyLookup,
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

func newParameterMaterializer(parameters map[string]any) *parameterMaterializer {
	materializedParameters := map[string]any{}

	for symbol, value := range parameters {
		materializedParameters[symbol] = value
	}

	return &parameterMaterializer{
		Visitor:    walk.NewVisitor[cypher.SyntaxNode](),
		parameters: materializedParameters,
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

type namedParameterCollector struct {
	walk.Visitor[cypher.SyntaxNode]

	parameters map[string]any
}

func newNamedParameterCollector() *namedParameterCollector {
	return &namedParameterCollector{
		Visitor:    walk.NewVisitor[cypher.SyntaxNode](),
		parameters: map[string]any{},
	}
}

func (s *namedParameterCollector) Enter(node cypher.SyntaxNode) {
	parameter, typeOK := node.(*cypher.Parameter)
	if !typeOK || parameter.Symbol == "" {
		return
	}

	if err := validateCypherSymbol(parameter.Symbol, "parameter"); err != nil {
		s.SetError(err)
		return
	}

	if existingValue, exists := s.parameters[parameter.Symbol]; exists && !reflect.DeepEqual(existingValue, parameter.Value) {
		s.SetErrorf("parameter %s is bound to multiple values", parameter.Symbol)
		return
	}

	s.parameters[parameter.Symbol] = parameter.Value
}

func collectNamedParameters(query *cypher.RegularQuery) (map[string]any, error) {
	collector := newNamedParameterCollector()

	if err := walk.Cypher(query, collector); err != nil {
		return nil, err
	}

	return collector.parameters, nil
}

func materializeParameters(query *cypher.RegularQuery) (map[string]any, error) {
	namedParameters, err := collectNamedParameters(query)
	if err != nil {
		return nil, err
	}

	materializer := newParameterMaterializer(namedParameters)

	if err := walk.Cypher(query, materializer); err != nil {
		return nil, err
	}

	return materializer.parameters, nil
}

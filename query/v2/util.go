package v2

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

func isNodePattern(seen *identifierSet) bool {
	return seen.Contains(Identifiers.node)
}

func isRelationshipPattern(seen *identifierSet) bool {
	var (
		hasStart        = seen.Contains(Identifiers.start)
		hasRelationship = seen.Contains(Identifiers.relationship)
		hasEnd          = seen.Contains(Identifiers.end)
	)

	return hasStart || hasRelationship || hasEnd
}

func prepareNodePattern(match *cypher.Match, seen *identifierSet) error {
	if isRelationshipPattern(seen) {
		return fmt.Errorf("query mixes node and relationship query identifiers")
	}

	match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
		Variable: Identifiers.Node(),
	})

	return nil
}

func prepareRelationshipPattern(match *cypher.Match, seen *identifierSet, relationshipKinds graph.Kinds, shortestPaths, allShortestPaths bool) error {
	if shortestPaths && allShortestPaths {
		return errors.New("query is requesting both all shortest paths and shortest paths")
	}

	var (
		newPatternPart   = match.NewPatternPart()
		startNodeSeen    = seen.Contains(Identifiers.start)
		relationshipSeen = seen.Contains(Identifiers.relationship)
		endNodeSeen      = seen.Contains(Identifiers.end)
	)

	newPatternPart.ShortestPathPattern = shortestPaths
	newPatternPart.AllShortestPathsPattern = allShortestPaths

	if startNodeSeen {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.Start(),
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	relationshipPattern := &cypher.RelationshipPattern{
		Kinds:     relationshipKinds,
		Direction: graph.DirectionOutbound,
	}

	if relationshipSeen {
		relationshipPattern.Variable = Identifiers.Relationship()
	}

	if shortestPaths || allShortestPaths {
		newPatternPart.Variable = Identifiers.Path()
		relationshipPattern.Range = &cypher.PatternRange{}
	}

	newPatternPart.AddPatternElements(relationshipPattern)

	if endNodeSeen {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.End(),
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	return nil
}

func prepareCreateRelationshipMatch(match *cypher.Match, seen *identifierSet) error {
	if seen.Contains(Identifiers.start) {
		match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.Start(),
		})
	}

	if seen.Contains(Identifiers.end) {
		match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.End(),
		})
	}

	return nil
}

func isDetachDeleteQualifier(qualifier cypher.Expression) bool {
	variable, typeOK := qualifier.(*cypher.Variable)
	if !typeOK {
		return false
	}

	switch variable.Symbol {
	case Identifiers.node, Identifiers.start, Identifiers.end:
		return true
	default:
		return false
	}
}

func kindProjectionExpression(identifier *cypher.Variable) (cypher.Expression, error) {
	switch identifier.Symbol {
	case Identifiers.node, Identifiers.start, Identifiers.end:
		return cypher.NewSimpleFunctionInvocation(cypher.NodeLabelsFunction, identifier), nil

	case Identifiers.relationship:
		return cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, identifier), nil

	default:
		return nil, fmt.Errorf("invalid kind projection reference: %s", identifier.Symbol)
	}
}

func projectionExpression(value any) (cypher.Expression, error) {
	switch typedValue := value.(type) {
	case QualifiedExpression:
		return typedValue.qualifier(), nil

	case kindContinuation:
		return kindProjectionExpression(typedValue.identifier)

	case kindsContinuation:
		return kindProjectionExpression(typedValue.identifier)

	case *cypher.ProjectionItem:
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

func projectionItemFromValue(value any) (*cypher.ProjectionItem, error) {
	if projectionItem, typeOK := value.(*cypher.ProjectionItem); typeOK {
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
		return s.CollectFromExpression(typedValue)

	case *cypher.Order:
		return s.CollectFromExpression(typedValue)

	case *cypher.SortItem:
		return s.CollectFromExpression(typedValue)

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

func collectCreateScope(values ...any) (*createScope, error) {
	scope := &createScope{
		identifiers: newIdentifierSet(),
	}

	for _, value := range values {
		switch typedValue := value.(type) {
		case *cypher.RelationshipPattern:
			scope.createsRelationship = true
			scope.identifiers.Add(Identifiers.start)
			scope.identifiers.Add(Identifiers.end)

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

	inDelete bool
	inUpdate bool
	inCreate bool
	inWhere  bool
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

	case *cypher.ProjectionItem:
		if typedNode.Alias != nil {
			s.seen.Add(typedNode.Alias.Symbol)
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

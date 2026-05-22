package translate

import (
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/graph"
)

const (
	countStoreNodeAlias pgsql.Identifier = "n0"
	countStoreEdgeAlias pgsql.Identifier = "e0"
)

type countStoreFastPathShape struct {
	Target optimize.CountStoreFastPathTarget
	Alias  string
	Kinds  graph.Kinds
}

func (s *Translator) translateCountStoreFastPath(query *cypher.RegularQuery, plan optimize.LoweringPlan) (bool, error) {
	if len(plan.CountStoreFastPath) == 0 {
		return false, nil
	}

	shape, ok := countStoreFastPathShapeForQuery(query)
	if !ok || shape.Target != plan.CountStoreFastPath[0].Target {
		return false, nil
	}

	countExpression := pgsql.FunctionCall{
		Function:   pgsql.FunctionCount,
		Parameters: []pgsql.Expression{pgsql.Wildcard{}},
		CastType:   pgsql.Int8,
	}

	var countProjection pgsql.SelectItem = countExpression
	if shape.Alias != "" {
		countProjection = pgsql.AliasedExpression{
			Expression: countExpression,
			Alias:      pgsql.AsOptionalIdentifier(pgsql.Identifier(shape.Alias)),
		}
	}

	fromClause, whereClause, err := s.countStoreFastPathFromAndWhere(shape)
	if err != nil {
		return false, err
	}

	s.translation.Statement = pgsql.Query{
		Body: pgsql.Select{
			Projection: pgsql.Projection{countProjection},
			From:       []pgsql.FromClause{fromClause},
			Where:      whereClause,
		},
	}
	s.recordLowering(optimize.LoweringCountStoreFastPath)
	return true, nil
}

func (s *Translator) countStoreFastPathFromAndWhere(shape countStoreFastPathShape) (pgsql.FromClause, pgsql.Expression, error) {
	switch shape.Target {
	case optimize.CountStoreFastPathNode:
		where, err := s.countStoreNodeKindConstraint(shape.Kinds)
		return pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.TableNode.AsCompoundIdentifier(),
				Binding: pgsql.AsOptionalIdentifier(countStoreNodeAlias),
			},
		}, where, err

	case optimize.CountStoreFastPathEdge:
		where, err := s.countStoreEdgeKindConstraint(shape.Kinds)
		return pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.TableEdge.AsCompoundIdentifier(),
				Binding: pgsql.AsOptionalIdentifier(countStoreEdgeAlias),
			},
		}, where, err

	default:
		return pgsql.FromClause{}, nil, nil
	}
}

func (s *Translator) countStoreNodeKindConstraint(kinds graph.Kinds) (pgsql.Expression, error) {
	if len(kinds) == 0 {
		return nil, nil
	}

	kindIDs, err := s.kindMapper.MapKinds(kinds)
	if err != nil {
		return nil, err
	}

	kindIDsLiteral, err := pgsql.AsLiteral(kindIDs)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{countStoreNodeAlias, pgsql.ColumnKindIDs},
		pgsql.OperatorPGArrayLHSContainsRHS,
		kindIDsLiteral,
	), nil
}

func (s *Translator) countStoreEdgeKindConstraint(kinds graph.Kinds) (pgsql.Expression, error) {
	if len(kinds) == 0 {
		return nil, nil
	}

	kindIDs, err := s.kindMapper.MapKinds(kinds)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{countStoreEdgeAlias, pgsql.ColumnKindID},
		pgsql.OperatorEquals,
		pgsql.NewAnyExpressionHinted(pgsql.NewLiteral(kindIDs, pgsql.Int2Array)),
	), nil
}

func countStoreFastPathShapeForQuery(query *cypher.RegularQuery) (countStoreFastPathShape, bool) {
	if query == nil || query.SingleQuery == nil || query.SingleQuery.SinglePartQuery == nil {
		return countStoreFastPathShape{}, false
	}

	queryPart := query.SingleQuery.SinglePartQuery
	if len(queryPart.UpdatingClauses) > 0 || len(queryPart.ReadingClauses) != 1 {
		return countStoreFastPathShape{}, false
	}

	countArgument, alias, ok := simpleCountProjection(queryPart.Return)
	if !ok {
		return countStoreFastPathShape{}, false
	}

	readingClause := queryPart.ReadingClauses[0]
	if readingClause == nil || readingClause.Match == nil {
		return countStoreFastPathShape{}, false
	}

	match := readingClause.Match
	if match.Optional || match.Where != nil || len(match.Pattern) != 1 {
		return countStoreFastPathShape{}, false
	}

	patternPart := match.Pattern[0]
	if patternPart == nil || patternPart.Variable != nil || patternPart.ShortestPathPattern || patternPart.AllShortestPathsPattern {
		return countStoreFastPathShape{}, false
	}

	if len(patternPart.PatternElements) == 1 {
		nodePattern, ok := patternPart.PatternElements[0].AsNodePattern()
		if !ok || nodePattern == nil || nodePattern.Properties != nil {
			return countStoreFastPathShape{}, false
		}

		bindingSymbol := countStoreVariableSymbol(nodePattern.Variable)
		if countArgument != cypher.TokenLiteralAsterisk && countArgument != bindingSymbol {
			return countStoreFastPathShape{}, false
		}

		return countStoreFastPathShape{
			Target: optimize.CountStoreFastPathNode,
			Alias:  alias,
			Kinds:  nodePattern.Kinds,
		}, true
	}

	if len(patternPart.PatternElements) != 3 {
		return countStoreFastPathShape{}, false
	}

	leftNode, leftOK := patternPart.PatternElements[0].AsNodePattern()
	relationship, relationshipOK := patternPart.PatternElements[1].AsRelationshipPattern()
	rightNode, rightOK := patternPart.PatternElements[2].AsNodePattern()
	if !leftOK || !relationshipOK || !rightOK {
		return countStoreFastPathShape{}, false
	}

	if constrainedCountStoreEndpoint(leftNode) || constrainedCountStoreEndpoint(rightNode) ||
		relationship == nil || relationship.Range != nil || relationship.Properties != nil ||
		relationship.Direction == graph.DirectionBoth {
		return countStoreFastPathShape{}, false
	}

	bindingSymbol := countStoreVariableSymbol(relationship.Variable)
	if countArgument != cypher.TokenLiteralAsterisk && countArgument != bindingSymbol {
		return countStoreFastPathShape{}, false
	}

	return countStoreFastPathShape{
		Target: optimize.CountStoreFastPathEdge,
		Alias:  alias,
		Kinds:  relationship.Kinds,
	}, true
}

func simpleCountProjection(returnClause *cypher.Return) (string, string, bool) {
	if returnClause == nil || returnClause.Projection == nil {
		return "", "", false
	}

	projection := returnClause.Projection
	if projection.Distinct || projection.All || projection.Order != nil || projection.Skip != nil || projection.Limit != nil || len(projection.Items) != 1 {
		return "", "", false
	}

	projectionItem, ok := projection.Items[0].(*cypher.ProjectionItem)
	if !ok || projectionItem == nil {
		return "", "", false
	}

	function, ok := projectionItem.Expression.(*cypher.FunctionInvocation)
	if !ok || function == nil || !strings.EqualFold(function.Name, cypher.CountFunction) ||
		function.Distinct || len(function.Namespace) > 0 || len(function.Arguments) != 1 {
		return "", "", false
	}

	variable, ok := function.Arguments[0].(*cypher.Variable)
	if !ok || variable == nil {
		return "", "", false
	}

	return variable.Symbol, countStoreVariableSymbol(projectionItem.Alias), true
}

func constrainedCountStoreEndpoint(nodePattern *cypher.NodePattern) bool {
	return nodePattern == nil || nodePattern.Variable != nil || len(nodePattern.Kinds) > 0 || nodePattern.Properties != nil
}

func countStoreVariableSymbol(variable *cypher.Variable) string {
	if variable == nil {
		return ""
	}

	return variable.Symbol
}

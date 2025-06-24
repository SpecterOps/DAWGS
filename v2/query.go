package v2

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

type runtimeIdentifiers struct {
	path         string
	node         string
	start        string
	relationship string
	end          string
}

func (s runtimeIdentifiers) Path() *cypher.Variable {
	return cypher.NewVariableWithSymbol(s.path)
}

func (s runtimeIdentifiers) Node() *cypher.Variable {
	return cypher.NewVariableWithSymbol(s.node)
}

func (s runtimeIdentifiers) Start() *cypher.Variable {
	return cypher.NewVariableWithSymbol(s.start)
}

func (s runtimeIdentifiers) Relationship() *cypher.Variable {
	return cypher.NewVariableWithSymbol(s.relationship)
}

func (s runtimeIdentifiers) End() *cypher.Variable {
	return cypher.NewVariableWithSymbol(s.end)
}

var Identifiers = runtimeIdentifiers{
	path:         "p",
	node:         "n",
	start:        "s",
	relationship: "r",
	end:          "e",
}

func joinedExpressionList(operator cypher.Operator, operands []cypher.Expression) cypher.Expression {
	expressionList := &cypher.Comparison{}

	if len(operands) > 0 {
		expressionList.Left = operands[0]

		for _, operand := range operands[1:] {
			expressionList.NewPartialComparison(operator, operand)
		}
	}

	return expressionList
}

func Not(operand cypher.Expression) cypher.Expression {
	switch typedOperand := operand.(type) {
	case *cypher.KindMatcher:
		// If the type doesn't match, this code does not handle the error. This will be caught during query build time
		// instead.
		if identifier, typeOK := typedOperand.Reference.(*cypher.Variable); typeOK && identifier.Symbol == Identifiers.relationship {
			if len(typedOperand.Kinds) == 1 {
				return cypher.NewComparison(
					cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, identifier),
					cypher.OperatorNotEquals,
					cypher.NewStringLiteral(typedOperand.Kinds[0].String()),
				)
			} else {
				return cypher.NewNegation(
					cypher.NewComparison(
						cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, identifier),
						cypher.OperatorIn,
						cypher.NewStringListLiteral(typedOperand.Kinds.Strings()),
					),
				)
			}
		}
	}

	return cypher.NewNegation(operand)
}

func And(operands ...cypher.Expression) cypher.Expression {
	return joinedExpressionList(cypher.OperatorAnd, operands)
}

func Or(operands ...cypher.Expression) cypher.Expression {
	return joinedExpressionList(cypher.OperatorOr, operands)
}

func Node() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: Identifiers.Node(),
	}
}

func Start() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: Identifiers.Start(),
	}
}

func Relationship() RelationshipContinuation {
	return &entity[RelationshipContinuation]{
		identifier: Identifiers.Relationship(),
	}
}

func End() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: Identifiers.End(),
	}
}

type QualifiedExpression interface {
	qualifier() cypher.Expression
}

type EntityContinuation interface {
	QualifiedExpression

	HasKind(kind graph.Kind) cypher.Expression
	HasKindIn(kinds graph.Kinds) cypher.Expression
	Property(name string) PropertyContinuation
}

type PropertyContinuation interface {
	QualifiedExpression

	Set(value any) cypher.Expression
	Remove() cypher.Expression
	Equals(value any) cypher.Expression
	GreaterThan(value any) cypher.Expression
	GreaterThanOrEqualTo(value any) cypher.Expression
	LessThan(value any) cypher.Expression
	LessThanOrEqualTo(value any) cypher.Expression
}

type entity[T any] struct {
	identifier   *cypher.Variable
	propertyName string
}

func (s *entity[T]) AddKinds(kinds graph.Kinds) cypher.Expression {
	return cypher.NewSetItem(
		s.identifier,
		cypher.OperatorLabelAssignment,
		kinds,
	)
}

func (s *entity[T]) RemoveKinds(kinds graph.Kinds) cypher.Expression {
	return cypher.RemoveKindsByMatcher(cypher.NewKindMatcher(s.identifier, kinds))
}

func (s *entity[T]) qualifier() cypher.Expression {
	if s.propertyName != "" {
		return cypher.NewPropertyLookup(s.identifier.Symbol, s.propertyName)
	}

	return s.identifier
}

func (s *entity[T]) HasKindIn(kinds graph.Kinds) cypher.Expression {
	return &cypher.KindMatcher{
		Reference: s.identifier,
		Kinds:     kinds,
	}
}

func (s *entity[T]) HasKind(kind graph.Kind) cypher.Expression {
	return s.HasKindIn(graph.Kinds{kind})
}

func (s *entity[T]) Property(name string) PropertyContinuation {
	s.propertyName = name
	return s
}

func (s *entity[T]) asPropertyComparison(operator cypher.Operator, rOperand any) cypher.Expression {
	return cypher.NewComparison(
		cypher.NewPropertyLookup(s.identifier.Symbol, s.propertyName),
		operator,
		cypher.NewLiteral(rOperand, rOperand == nil),
	)
}

func (s *entity[T]) Set(value any) cypher.Expression {
	return cypher.NewSetItem(
		cypher.NewPropertyLookup(s.identifier.Symbol, s.propertyName),
		cypher.OperatorAssignment,
		cypher.NewLiteral(value, value == nil),
	)
}

func (s *entity[T]) Remove() cypher.Expression {
	return cypher.RemoveProperty(cypher.NewPropertyLookup(s.identifier.Symbol, s.propertyName))
}

func (s *entity[T]) Equals(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorEquals, value)
}

func (s *entity[T]) GreaterThan(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorGreaterThan, value)
}

func (s *entity[T]) GreaterThanOrEqualTo(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorGreaterThanOrEqualTo, value)
}

func (s *entity[T]) LessThan(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorLessThan, value)
}

func (s *entity[T]) LessThanOrEqualTo(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorLessThanOrEqualTo, value)
}

func (s *entity[T]) Is(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorIs, value)
}

func (s *entity[T]) IsNot(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorIsNot, value)
}

func (s *entity[T]) StartsWith(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorStartsWith, value)
}

func (s *entity[T]) Contains(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorContains, value)
}

func (s *entity[T]) EndsWith(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorEndsWith, value)
}

func (s *entity[T]) In(value any) cypher.Expression {
	return s.asPropertyComparison(cypher.OperatorIn, value)
}

type PathContinuation interface {
	QualifiedExpression
}

type RelationshipContinuation interface {
	EntityContinuation
}

type NodeContinuation interface {
	EntityContinuation

	AddKinds(kinds graph.Kinds) cypher.Expression
	RemoveKinds(kinds graph.Kinds) cypher.Expression
}

type QueryBuilder interface {
	Where(constraints ...cypher.Expression) QueryBuilder
	OrderBy(sortItems ...cypher.Expression) QueryBuilder
	Skip(offset int) QueryBuilder
	Limit(limit int) QueryBuilder
	Return(projections ...any) QueryBuilder
	Update(updatingClauses ...any) QueryBuilder
	Delete(expressions ...any) QueryBuilder
	Build() (*cypher.RegularQuery, error)
}

type builder struct {
	errors       []error
	constraints  []cypher.Expression
	sortItems    []cypher.Expression
	projections  []any
	updates      []any
	setItems     []*cypher.SetItem
	removeItems  []*cypher.RemoveItem
	deleteItems  []cypher.Expression
	detachDelete bool
	skip         *int
	limit        *int
}

func Query() QueryBuilder {
	return &builder{}
}

func (s *builder) OrderBy(sortItems ...cypher.Expression) QueryBuilder {
	return s
}

func (s *builder) Skip(skip int) QueryBuilder {
	s.skip = &skip
	return s
}

func (s *builder) Limit(limit int) QueryBuilder {
	s.limit = &limit
	return s
}

func (s *builder) Return(projections ...any) QueryBuilder {
	s.projections = append(s.projections, projections...)
	return s
}

func (s *builder) Update(updates ...any) QueryBuilder {
	for _, nextUpdate := range updates {
		switch typedNextUpdate := nextUpdate.(type) {
		case *cypher.SetItem:
			s.setItems = append(s.setItems, typedNextUpdate)

		case *cypher.RemoveItem:
			s.removeItems = append(s.removeItems, typedNextUpdate)

		default:
			s.trackError(fmt.Errorf("unknown update type: %T", nextUpdate))
		}
	}

	return s
}

func (s *builder) Delete(deleteItems ...any) QueryBuilder {
	for _, nextDelete := range deleteItems {
		switch typedNextUpdate := nextDelete.(type) {
		case QualifiedExpression:
			qualifier := typedNextUpdate.qualifier()

			switch qualifier {
			case Identifiers.node, Identifiers.start, Identifiers.end:
				s.detachDelete = true
			}

			s.deleteItems = append(s.deleteItems, qualifier)

		case *cypher.Variable:
			switch typedNextUpdate.Symbol {
			case Identifiers.node, Identifiers.start, Identifiers.end:
				s.detachDelete = true
			}

			s.deleteItems = append(s.deleteItems, typedNextUpdate)

		default:
			s.trackError(fmt.Errorf("unknown delete type: %T", nextDelete))
		}
	}

	return s
}

func (s *builder) trackError(err error) {
	s.errors = append(s.errors, err)
}

func (s *builder) Where(constraints ...cypher.Expression) QueryBuilder {
	s.constraints = append(s.constraints, constraints...)
	return s
}

func (s *builder) Build() (*cypher.RegularQuery, error) {
	if len(s.errors) > 0 {
		return nil, errors.Join(s.errors...)
	}

	var (
		relationshipKinds             graph.Kinds
		regularQuery, singlePartQuery = cypher.NewRegularQueryWithSingleQuery()
		match                         = singlePartQuery.NewMatch(false)
		projection                    = singlePartQuery.NewProjection(false)
		seenIdentifiers               = map[string]struct{}{}
	)

	// If there are constraints, add them to the match with a where clause
	if len(s.constraints) > 0 {
		var (
			whereClause = match.NewWhere()
			constraints = &cypher.Comparison{}
		)

		for _, nextConstraint := range s.constraints {
			switch typedNextConstraint := nextConstraint.(type) {
			case *cypher.KindMatcher:
				if identifier, typeOK := typedNextConstraint.Reference.(*cypher.Variable); !typeOK {
					return nil, fmt.Errorf("expected type *cypher.Variable, got %T", typedNextConstraint)
				} else if identifier.Symbol == Identifiers.relationship {
					relationshipKinds = relationshipKinds.Add(typedNextConstraint.Kinds...)
					continue
				}
			}

			if constraints.Left == nil {
				constraints.Left = nextConstraint
			} else {
				constraints.NewPartialComparison(cypher.OperatorAnd, nextConstraint)
			}
		}

		whereClause.Add(constraints)
	}

	if len(s.projections) == 0 {
		return nil, fmt.Errorf("query has no projections specified")
	}

	for _, nextProjection := range s.projections {
		switch typedNextProjection := nextProjection.(type) {
		case QualifiedExpression:
			projection.Items = append(projection.Items, typedNextProjection.qualifier())
		default:
			projection.Items = append(projection.Items, typedNextProjection)
		}
	}

	if s.skip != nil && *s.skip > 0 {
		projection.Skip = cypher.NewSkip(*s.skip)
	}

	if s.limit != nil && *s.limit > 0 {
		projection.Limit = cypher.NewLimit(*s.limit)
	}

	if len(s.setItems) > 0 {
		singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(
			cypher.NewSet(s.setItems),
		))
	}

	if len(s.removeItems) > 0 {
		singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(
			cypher.NewRemove(s.removeItems),
		))
	}

	if len(s.deleteItems) > 0 {
		singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(
			cypher.NewDelete(
				s.detachDelete,
				s.deleteItems,
			),
		))
	}

	if err := walk.Cypher(singlePartQuery, walk.NewSimpleVisitor(func(node cypher.SyntaxNode, errorHandler walk.CancelableErrorHandler) {
		switch typedNode := node.(type) {
		case *cypher.Variable:
			seenIdentifiers[typedNode.Symbol] = struct{}{}
		}
	})); err != nil {
		return nil, err
	}

	if isNodePattern(seenIdentifiers) {
		if err := prepareNodePattern(match, seenIdentifiers); err != nil {
			return nil, err
		}
	} else if isRelationshipPattern(seenIdentifiers) {
		if err := prepareRelationshipPattern(match, seenIdentifiers, relationshipKinds); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("query has no node and relationship query identifiers specified")
	}

	return regularQuery, errors.Join(s.errors...)
}

package v2

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/models/cypher"
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

	ID() IdentityContinuation
	HasKind(kind graph.Kind) cypher.Expression
	HasKindIn(kinds graph.Kinds) cypher.Expression
	Property(name string) PropertyContinuation
}

type Comparable interface {
	Equals(value any) cypher.Expression
	GreaterThan(value any) cypher.Expression
	GreaterThanOrEqualTo(value any) cypher.Expression
	LessThan(value any) cypher.Expression
	LessThanOrEqualTo(value any) cypher.Expression
}

type PropertyContinuation interface {
	QualifiedExpression
	Comparable

	Set(value any) cypher.Expression
	Remove() cypher.Expression
}

type IdentityContinuation interface {
	QualifiedExpression
	Comparable
}

type comparisonContinuation struct {
	qualifierExpression cypher.Expression
}

func (s *comparisonContinuation) qualifier() cypher.Expression {
	return s.qualifierExpression
}

func (s *comparisonContinuation) asComparison(operator cypher.Operator, rOperand any) cypher.Expression {
	return cypher.NewComparison(
		s.qualifier(),
		operator,
		cypher.NewLiteral(rOperand, rOperand == nil),
	)
}

func (s *comparisonContinuation) Equals(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorEquals, value)
}

func (s *comparisonContinuation) GreaterThan(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorGreaterThan, value)
}

func (s *comparisonContinuation) GreaterThanOrEqualTo(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorGreaterThanOrEqualTo, value)
}

func (s *comparisonContinuation) LessThan(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorLessThan, value)
}

func (s *comparisonContinuation) LessThanOrEqualTo(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorLessThanOrEqualTo, value)
}

type propertyContinuation struct {
	comparisonContinuation
}

func (s *propertyContinuation) Set(value any) cypher.Expression {
	return cypher.NewSetItem(
		s.qualifier(),
		cypher.OperatorAssignment,
		cypher.NewLiteral(value, value == nil),
	)
}

func (s *propertyContinuation) Remove() cypher.Expression {
	return cypher.RemoveProperty(s.qualifier())
}

type entity[T any] struct {
	identifier *cypher.Variable
}

func (s *entity[T]) RelationshipPattern(kind graph.Kind, properties cypher.Expression, direction graph.Direction) cypher.Expression {
	return &cypher.RelationshipPattern{
		Variable:   s.identifier,
		Kinds:      graph.Kinds{kind},
		Direction:  direction,
		Properties: properties,
	}
}

func (s *entity[T]) NodePattern(kinds graph.Kinds, properties cypher.Expression) cypher.Expression {
	return &cypher.NodePattern{
		Variable:   s.identifier,
		Kinds:      kinds,
		Properties: properties,
	}
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

func (s *entity[T]) ID() IdentityContinuation {
	return &comparisonContinuation{
		qualifierExpression: &cypher.FunctionInvocation{
			Distinct:  false,
			Name:      cypher.IdentityFunction,
			Arguments: []cypher.Expression{s.identifier},
		},
	}
}

func (s *entity[T]) Property(propertyName string) PropertyContinuation {
	return &propertyContinuation{
		comparisonContinuation: comparisonContinuation{
			qualifierExpression: cypher.NewPropertyLookup(s.identifier.Symbol, propertyName),
		},
	}
}

type PathContinuation interface {
	QualifiedExpression
}

type RelationshipContinuation interface {
	EntityContinuation

	RelationshipPattern(kind graph.Kind, properties cypher.Expression, direction graph.Direction) cypher.Expression
}

type NodeContinuation interface {
	EntityContinuation

	NodePattern(kinds graph.Kinds, properties cypher.Expression) cypher.Expression
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
	Create(creationClauses ...any) QueryBuilder
	Delete(expressions ...any) QueryBuilder
	Build() (*PreparedQuery, error)
}

type builder struct {
	errors       []error
	constraints  []cypher.Expression
	sortItems    []cypher.Expression
	projections  []any
	creates      []any
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

func (s *builder) Create(creationClauses ...any) QueryBuilder {
	s.creates = append(s.creates, creationClauses...)
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

func (s *builder) buildCreates(singlePartQuery *cypher.SinglePartQuery) error {
	// Early exit to hide this part of the business logic while handling queries with no create statements
	if len(s.creates) == 0 {
		return nil
	}

	var (
		pattern      = &cypher.PatternPart{}
		createClause = &cypher.Create{
			// Note: Unique is Neo4j specific and will not be supported here. Use of constraints for
			// uniqueness is expected instead.
			Unique:  false,
			Pattern: []*cypher.PatternPart{pattern},
		}
	)

	for _, nextCreate := range s.creates {
		switch typedNextCreate := nextCreate.(type) {
		case QualifiedExpression:
			switch typedExpression := typedNextCreate.qualifier().(type) {
			case *cypher.Variable:
				switch typedExpression.Symbol {
				case Identifiers.node, Identifiers.start, Identifiers.end:
					pattern.AddPatternElements(&cypher.NodePattern{
						Variable: cypher.NewVariableWithSymbol(typedExpression.Symbol),
					})

				default:
					return fmt.Errorf("invalid variable reference for create: %s", typedExpression.Symbol)
				}
			}

		case *cypher.NodePattern:
			pattern.AddPatternElements(typedNextCreate)

		case *cypher.RelationshipPattern:
			pattern.AddPatternElements(&cypher.NodePattern{
				Variable: cypher.NewVariableWithSymbol(Identifiers.start),
			})

			pattern.AddPatternElements(typedNextCreate)

			pattern.AddPatternElements(&cypher.NodePattern{
				Variable: cypher.NewVariableWithSymbol(Identifiers.end),
			})

		default:
			createClause.AddError(fmt.Errorf("invalid type for create: %T", nextCreate))
		}
	}

	singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(createClause))
	return nil
}

func (s *builder) buildUpdatingClauses(singlePartQuery *cypher.SinglePartQuery) error {
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

	return s.buildCreates(singlePartQuery)
}

func (s *builder) buildProjection(singlePartQuery *cypher.SinglePartQuery) error {
	var (
		hasProjectedItems  = len(s.projections) > 0
		hasSkip            = s.skip != nil && *s.skip > 0
		hasLimit           = s.limit != nil && *s.limit > 0
		requiresProjection = hasProjectedItems || hasSkip || hasLimit
	)

	if requiresProjection {
		if !hasProjectedItems {
			return fmt.Errorf("query expected projected items")
		}

		projection := singlePartQuery.NewProjection(false)

		for _, nextProjection := range s.projections {
			switch typedNextProjection := nextProjection.(type) {
			case QualifiedExpression:
				projection.AddItem(cypher.NewProjectionItemWithExpr(typedNextProjection.qualifier()))
			default:
				projection.AddItem(cypher.NewProjectionItemWithExpr(typedNextProjection))
			}
		}

		if s.skip != nil && *s.skip > 0 {
			projection.Skip = cypher.NewSkip(*s.skip)
		}

		if s.limit != nil && *s.limit > 0 {
			projection.Limit = cypher.NewLimit(*s.limit)
		}
	}

	return nil
}

func stripASTParameters(query *cypher.RegularQuery) (map[string]any, error) {
	var (
		parameters = map[string]any{}
		err        = walk.Cypher(query, walk.NewSimpleVisitor(func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case *cypher.Parameter:
				if _, exists := parameters[typedNode.Symbol]; exists {
					errorHandler.SetErrorf("duplicate parameter: %s", typedNode.Symbol)
				} else {
					parameters[typedNode.Symbol] = typedNode.Value
					typedNode.Value = nil
				}
			}
		}))
	)

	return parameters, err
}

type PreparedQuery struct {
	Query      *cypher.RegularQuery
	Parameters map[string]any
}

func (s *builder) Build() (*PreparedQuery, error) {
	if len(s.errors) > 0 {
		return nil, errors.Join(s.errors...)
	}

	if len(s.projections) == 0 && len(s.setItems) == 0 && len(s.removeItems) == 0 && len(s.creates) == 0 && len(s.deleteItems) == 0 {
		return nil, fmt.Errorf("query has no action specified")
	}

	var (
		regularQuery, singlePartQuery = cypher.NewRegularQueryWithSingleQuery()
		match                         = &cypher.Match{}
		relationshipKinds             graph.Kinds
	)

	if err := s.buildUpdatingClauses(singlePartQuery); err != nil {
		return nil, err
	}

	if err := s.buildProjection(singlePartQuery); err != nil {
		return nil, err
	}

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

	if seen, err := extractQueryIdentifiers(singlePartQuery); err != nil {
		return nil, err
	} else if isNodePattern(seen) {
		if err := prepareNodePattern(match, seen); err != nil {
			return nil, err
		}
	} else if isRelationshipPattern(seen) {
		if err := prepareRelationshipPattern(match, seen, relationshipKinds); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("query has no node and relationship query identifiers specified")
	}

	if len(match.Pattern) > 0 {
		newReadingClause := cypher.NewReadingClause()
		newReadingClause.Match = match

		singlePartQuery.ReadingClauses = append(singlePartQuery.ReadingClauses, newReadingClause)
	}

	if parameters, err := stripASTParameters(regularQuery); err != nil {
		return nil, err
	} else {
		return &PreparedQuery{
			Query:      regularQuery,
			Parameters: parameters,
		}, nil
	}
}

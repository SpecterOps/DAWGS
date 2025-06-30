package query

import (
	"errors"
	"fmt"

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

func joinedExpressionList(operator cypher.Operator, operands []cypher.SyntaxNode) cypher.SyntaxNode {
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
	// switch typedOperand := operand.(type) {
	// case *cypher.KindMatcher:
	// 	// If the type doesn't match, this code does not handle the error. This will be caught during query build time
	// 	// instead.
	// 	if identifier, typeOK := typedOperand.Reference.(*cypher.Variable); typeOK && identifier.Symbol == Identifiers.relationship {
	// 		if len(typedOperand.Kinds) == 1 {
	// 			return cypher.NewComparison(
	// 				cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, identifier),
	// 				cypher.OperatorNotEquals,
	// 				cypher.NewStringLiteral(typedOperand.Kinds[0].String()),
	// 			)
	// 		} else {
	// 			return cypher.NewNegation(
	// 				cypher.NewComparison(
	// 					cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, identifier),
	// 					cypher.OperatorIn,
	// 					cypher.NewStringListLiteral(typedOperand.Kinds.Strings()),
	// 				),
	// 			)
	// 		}
	// 	}
	// }

	return cypher.NewNegation(operand)
}

func And(operands ...cypher.SyntaxNode) cypher.SyntaxNode {
	return joinedExpressionList(cypher.OperatorAnd, operands)
}

func Or(operands ...cypher.SyntaxNode) cypher.SyntaxNode {
	return joinedExpressionList(cypher.OperatorOr, operands)
}

func Node() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: Identifiers.Node(),
	}
}

func Path() PathContinuation {
	return &entity[PathContinuation]{
		identifier: Identifiers.Path(),
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

	Count() cypher.Expression
	ID() IdentityContinuation
	Property(name string) PropertyContinuation
}

type KindContinuation interface {
	Is(kind graph.Kind) cypher.Expression
	IsOneOf(kinds graph.Kinds) cypher.Expression
}

type KindsContinuation interface {
	Has(kind graph.Kind) cypher.Expression
	HasOneOf(kinds graph.Kinds) cypher.Expression
	Add(kinds graph.Kinds) cypher.Expression
	Remove(kinds graph.Kinds) cypher.Expression
}

type Comparable interface {
	In(value any) cypher.Expression
	Contains(value any) cypher.Expression
	Equals(value any) cypher.Expression
	GreaterThan(value any) cypher.Expression
	GreaterThanOrEqualTo(value any) cypher.Expression
	LessThan(value any) cypher.Expression
	LessThanOrEqualTo(value any) cypher.Expression
}

type PropertyContinuation interface {
	QualifiedExpression
	Comparable

	Set(value any) *cypher.SetItem
	Remove() *cypher.RemoveItem
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

func (s *comparisonContinuation) In(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorIn, value)
}

func (s *comparisonContinuation) Contains(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorContains, value)
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

func (s *propertyContinuation) Set(value any) *cypher.SetItem {
	return cypher.NewSetItem(
		s.qualifier(),
		cypher.OperatorAssignment,
		cypher.NewLiteral(value, value == nil),
	)
}

func (s *propertyContinuation) Remove() *cypher.RemoveItem {
	return cypher.RemoveProperty(s.qualifier())
}

type entity[T any] struct {
	identifier *cypher.Variable
}

func (s *entity[T]) Kind() KindContinuation {
	return kindContinuation{
		identifier: s.identifier,
	}
}

func (s *entity[T]) Kinds() KindsContinuation {
	return kindsContinuation{
		identifier: s.identifier,
	}
}

func (s *entity[T]) Count() cypher.Expression {
	return cypher.NewSimpleFunctionInvocation(cypher.CountFunction, s.identifier)
}

func (s *entity[T]) SetProperties(properties map[string]any) cypher.Expression {
	set := &cypher.Set{}

	for key, value := range properties {
		set.Items = append(set.Items, s.Property(key).Set(value))
	}

	return set
}

func (s *entity[T]) RemoveProperties(properties []string) cypher.Expression {
	remove := &cypher.Remove{}

	for _, key := range properties {
		remove.Items = append(remove.Items, s.Property(key).Remove())
	}

	return remove
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

func (s *entity[T]) qualifier() cypher.Expression {
	return s.identifier
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

type kindContinuation struct {
	identifier *cypher.Variable
}

func (s kindContinuation) Is(kind graph.Kind) cypher.Expression {
	return s.IsOneOf(graph.Kinds{kind})
}

func (s kindContinuation) IsOneOf(kinds graph.Kinds) cypher.Expression {
	return &cypher.KindMatcher{
		Reference: s.identifier,
		Kinds:     kinds,
	}
}

type kindsContinuation struct {
	identifier *cypher.Variable
}

func (s kindsContinuation) Has(kind graph.Kind) cypher.Expression {
	return s.HasOneOf(graph.Kinds{kind})
}

func (s kindsContinuation) HasOneOf(kinds graph.Kinds) cypher.Expression {
	return &cypher.KindMatcher{
		Reference: s.identifier,
		Kinds:     kinds,
	}
}

func (s kindsContinuation) Add(kinds graph.Kinds) cypher.Expression {
	return cypher.NewSetItem(
		s.identifier,
		cypher.OperatorLabelAssignment,
		kinds,
	)
}

func (s kindsContinuation) Remove(kinds graph.Kinds) cypher.Expression {
	return cypher.RemoveKindsByMatcher(cypher.NewKindMatcher(s.identifier, kinds))
}

type PathContinuation interface {
	QualifiedExpression

	Count() cypher.Expression
}

type RelationshipContinuation interface {
	EntityContinuation

	RelationshipPattern(kind graph.Kind, properties cypher.Expression, direction graph.Direction) cypher.Expression

	Kind() KindContinuation
	SetProperties(properties map[string]any) cypher.Expression
	RemoveProperties(properties []string) cypher.Expression
}

type NodeContinuation interface {
	EntityContinuation

	NodePattern(kinds graph.Kinds, properties cypher.Expression) cypher.Expression

	Kinds() KindsContinuation
	SetProperties(properties map[string]any) cypher.Expression
	RemoveProperties(properties []string) cypher.Expression
}

type QueryBuilder interface {
	Where(constraints ...cypher.SyntaxNode) QueryBuilder
	OrderBy(sortItems ...cypher.SyntaxNode) QueryBuilder
	Skip(offset int) QueryBuilder
	Limit(limit int) QueryBuilder
	Return(projections ...any) QueryBuilder
	Update(updatingClauses ...any) QueryBuilder
	Create(creationClauses ...any) QueryBuilder
	Delete(expressions ...any) QueryBuilder
	WithShortestPaths() QueryBuilder
	WithAllShortestPaths() QueryBuilder
	Build() (*PreparedQuery, error)
}

type builder struct {
	errors               []error
	constraints          []cypher.SyntaxNode
	sortItems            []cypher.SyntaxNode
	projections          []any
	creates              []any
	setItems             []*cypher.SetItem
	removeItems          []*cypher.RemoveItem
	deleteItems          []cypher.Expression
	detachDelete         bool
	shortestPathQuery    bool
	allShorestPathsQuery bool
	skip                 *int
	limit                *int
}

func New() QueryBuilder {
	return &builder{}
}

func (s *builder) WithShortestPaths() QueryBuilder {
	s.shortestPathQuery = true
	return s
}

func (s *builder) WithAllShortestPaths() QueryBuilder {
	s.allShorestPathsQuery = true
	return s
}

func (s *builder) OrderBy(sortItems ...cypher.SyntaxNode) QueryBuilder {
	s.sortItems = append(s.sortItems, sortItems...)
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
		case *cypher.Set:
			s.setItems = append(s.setItems, typedNextUpdate.Items...)

		case *cypher.SetItem:
			s.setItems = append(s.setItems, typedNextUpdate)

		case *cypher.Remove:
			s.removeItems = append(s.removeItems, typedNextUpdate.Items...)

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

func (s *builder) Where(constraints ...cypher.SyntaxNode) QueryBuilder {
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
			return fmt.Errorf("invalid type for create: %T", nextCreate)
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

func (s *builder) buildProjectionOrder() (*cypher.Order, error) {
	var orderByNode *cypher.Order

	if len(s.sortItems) > 0 {
		orderByNode = &cypher.Order{}

		for _, untypedSortItem := range s.sortItems {
			switch typedSortItem := untypedSortItem.(type) {
			case *cypher.Order:
				for _, sortItem := range typedSortItem.Items {
					orderByNode.Items = append(orderByNode.Items, sortItem)
				}

			case *cypher.SortItem:
				orderByNode.Items = append(orderByNode.Items, typedSortItem)
			}
		}
	}

	return orderByNode, nil
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
			case *cypher.Return:
				for _, returnItem := range typedNextProjection.Projection.Items {
					if typedReturnItem, typeOK := returnItem.(*cypher.ProjectionItem); !typeOK {
						return fmt.Errorf("invalid type for return: %T", returnItem)
					} else {
						projection.AddItem(typedReturnItem)
					}
				}

			case QualifiedExpression:
				projection.AddItem(cypher.NewProjectionItemWithExpr(typedNextProjection.qualifier()))

			case kindContinuation:
				var kindExpr cypher.Expression

				switch typedNextProjection.identifier.Symbol {
				case Identifiers.node, Identifiers.start, Identifiers.end:
					kindExpr = cypher.NewSimpleFunctionInvocation(cypher.NodeLabelsFunction, typedNextProjection.identifier)

				case Identifiers.relationship:
					kindExpr = cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, typedNextProjection.identifier)
				}

				projection.AddItem(cypher.NewProjectionItemWithExpr(kindExpr))

			case kindsContinuation:
				var kindExpr cypher.Expression

				switch typedNextProjection.identifier.Symbol {
				case Identifiers.node, Identifiers.start, Identifiers.end:
					kindExpr = cypher.NewSimpleFunctionInvocation(cypher.NodeLabelsFunction, typedNextProjection.identifier)

				case Identifiers.relationship:
					kindExpr = cypher.NewSimpleFunctionInvocation(cypher.EdgeTypeFunction, typedNextProjection.identifier)
				}

				projection.AddItem(cypher.NewProjectionItemWithExpr(kindExpr))

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

		if projectionOrder, err := s.buildProjectionOrder(); err != nil {
			return err
		} else if projectionOrder != nil {
			projection.Order = projectionOrder
		}
	}

	return nil
}

type PreparedQuery struct {
	Query      *cypher.RegularQuery
	Parameters map[string]any
}

func (s *builder) hasActions() bool {
	return len(s.projections) > 0 || len(s.setItems) > 0 || len(s.removeItems) > 0 || len(s.creates) > 0 || len(s.deleteItems) > 0
}

func (s *builder) Build() (*PreparedQuery, error) {
	if len(s.errors) > 0 {
		return nil, errors.Join(s.errors...)
	}

	if !s.hasActions() {
		return nil, fmt.Errorf("query has no action specified")
	}

	var (
		regularQuery, singlePartQuery = cypher.NewRegularQueryWithSingleQuery()
		match                         = &cypher.Match{}
		seenIdentifiers               = newIdentifierSet()
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

		if constraints.Left != nil {
			whereClause.Add(constraints)

			if err := seenIdentifiers.CollectFromExpression(whereClause); err != nil {
				return nil, err
			}
		}
	}

	if err := seenIdentifiers.CollectFromExpression(singlePartQuery); err != nil {
		return nil, err
	}

	// Skip pattern preparation if there is a create clause with no constraints
	if len(s.constraints) > 0 || len(s.creates) == 0 {
		if isNodePattern(seenIdentifiers) {
			if err := prepareNodePattern(match, seenIdentifiers); err != nil {
				return nil, err
			}
		} else if isRelationshipPattern(seenIdentifiers) {
			if err := prepareRelationshipPattern(match, seenIdentifiers, relationshipKinds, s.shortestPathQuery, s.allShorestPathsQuery); err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("query has no node and relationship query identifiers specified")
		}
	}

	if len(match.Pattern) > 0 {
		newReadingClause := cypher.NewReadingClause()
		newReadingClause.Match = match

		singlePartQuery.ReadingClauses = append(singlePartQuery.ReadingClauses, newReadingClause)
	}

	return &PreparedQuery{
		Query:      regularQuery,
		Parameters: map[string]any{},
	}, nil
}

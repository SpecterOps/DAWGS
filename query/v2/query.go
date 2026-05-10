package v2

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

type TraversalDepth struct {
	patternRange *cypher.PatternRange
	err          error
}

func traversalDepthBound(value int64) *int64 {
	return &value
}

func newTraversalDepth(start, end *int64) TraversalDepth {
	if start != nil && *start < 0 {
		return TraversalDepth{
			err: fmt.Errorf("traversal depth minimum must be non-negative: %d", *start),
		}
	}

	if end != nil && *end < 0 {
		return TraversalDepth{
			err: fmt.Errorf("traversal depth maximum must be non-negative: %d", *end),
		}
	}

	if start != nil && end != nil && *end < *start {
		return TraversalDepth{
			err: fmt.Errorf("traversal depth maximum %d is less than minimum %d", *end, *start),
		}
	}

	return TraversalDepth{
		patternRange: cypher.NewPatternRange(start, end),
	}
}

func (s TraversalDepth) rangePattern() *cypher.PatternRange {
	if s.patternRange == nil {
		return &cypher.PatternRange{}
	}

	return cypher.Copy(s.patternRange)
}

func AnyDepth() TraversalDepth {
	return newTraversalDepth(nil, nil)
}

func MinDepth(min int64) TraversalDepth {
	return newTraversalDepth(traversalDepthBound(min), nil)
}

func MaxDepth(max int64) TraversalDepth {
	return newTraversalDepth(nil, traversalDepthBound(max))
}

func DepthRange(min, max int64) TraversalDepth {
	return newTraversalDepth(traversalDepthBound(min), traversalDepthBound(max))
}

func ExactDepth(depth int64) TraversalDepth {
	depthBound := traversalDepthBound(depth)
	return newTraversalDepth(depthBound, depthBound)
}

// Accessors return fresh variables; compare symbols rather than pointer identity.
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

type Scope struct {
	identifiers runtimeIdentifiers
	errors      []error
}

func DefaultScope() Scope {
	return Scope{
		identifiers: Identifiers,
	}
}

func NewScope(path, node, start, relationship, end string) Scope {
	identifiers := runtimeIdentifiers{
		path:         path,
		node:         node,
		start:        start,
		relationship: relationship,
		end:          end,
	}

	return Scope{
		identifiers: identifiers,
		errors:      validateRuntimeIdentifiers(identifiers),
	}
}

func validateRuntimeIdentifiers(identifiers runtimeIdentifiers) []error {
	aliases := []struct {
		role  string
		value string
	}{
		{role: "path", value: identifiers.path},
		{role: "node", value: identifiers.node},
		{role: "start", value: identifiers.start},
		{role: "relationship", value: identifiers.relationship},
		{role: "end", value: identifiers.end},
	}

	var (
		errs []error
		seen = map[string]string{}
	)

	for _, alias := range aliases {
		if err := validateCypherSymbol(alias.value, "scope alias "+alias.role); err != nil {
			errs = append(errs, err)
			continue
		}

		if existingRole, exists := seen[alias.value]; exists {
			errs = append(errs, fmt.Errorf("scope aliases %s and %s both use %q", existingRole, alias.role, alias.value))
		} else {
			seen[alias.value] = alias.role
		}
	}

	return errs
}

func (s Scope) New() QueryBuilder {
	return newBuilder(s.identifiers, s.errors...)
}

func (s Scope) Node() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: s.identifiers.Node(),
		role:       Identifiers.node,
	}
}

func (s Scope) Path() PathContinuation {
	return &entity[PathContinuation]{
		identifier: s.identifiers.Path(),
		role:       Identifiers.path,
	}
}

func (s Scope) Start() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: s.identifiers.Start(),
		role:       Identifiers.start,
	}
}

func (s Scope) Relationship() RelationshipContinuation {
	return &entity[RelationshipContinuation]{
		identifier: s.identifiers.Relationship(),
		role:       Identifiers.relationship,
	}
}

func (s Scope) End() NodeContinuation {
	return &entity[NodeContinuation]{
		identifier: s.identifiers.End(),
		role:       Identifiers.end,
	}
}

func Literal(value any) *cypher.Literal {
	if value == nil {
		return cypher.NewLiteral(nil, true)
	}

	if strValue, typeOK := value.(string); typeOK {
		return cypher.NewStringLiteral(strValue)
	}

	return cypher.NewLiteral(value, false)
}

func Parameter(value any) *cypher.Parameter {
	if parameter, typeOK := value.(*cypher.Parameter); typeOK {
		return parameter
	}

	return &cypher.Parameter{
		Value: value,
	}
}

func NamedParameter(symbol string, value any) *cypher.Parameter {
	return cypher.NewParameter(symbol, value)
}

func valueExpression(value any) cypher.Expression {
	switch typedValue := value.(type) {
	case *cypher.Parameter:
		return typedValue
	case *cypher.Literal:
		return typedValue
	case *cypher.Variable:
		return typedValue
	case *cypher.PropertyLookup:
		return typedValue
	case *cypher.FunctionInvocation:
		return typedValue
	case *cypher.Parenthetical:
		return typedValue
	case *cypher.Comparison:
		return typedValue
	case *cypher.Negation:
		return typedValue
	case *cypher.Conjunction:
		return typedValue
	case *cypher.Disjunction:
		return typedValue
	case *cypher.ExclusiveDisjunction:
		return typedValue
	case *cypher.KindMatcher:
		return typedValue
	case *cypher.ListLiteral:
		return typedValue
	case cypher.MapLiteral:
		return typedValue
	case *cypher.PatternPredicate:
		return typedValue
	case *cypher.ArithmeticExpression:
		return typedValue
	case *cypher.UnaryAddOrSubtractExpression:
		return typedValue
	case *cypher.FilterExpression:
		return typedValue
	case *cypher.IDInCollection:
		return typedValue
	case QualifiedExpression:
		if expression, err := projectionExpression(typedValue); err != nil {
			return invalidExpression(err)
		} else {
			return expression
		}
	default:
		return Parameter(value)
	}
}

func joinedExpressionList(operator cypher.Operator, operands []cypher.SyntaxNode) ([]cypher.Expression, cypher.SyntaxNode) {
	if len(operands) == 0 {
		return nil, invalidExpression(fmt.Errorf("%s requires at least one operand", operator))
	}

	expressions := make([]cypher.Expression, len(operands))
	for idx, operand := range operands {
		expressions[idx] = operand
	}

	return expressions, nil
}

func comparisonHasLogicalOperator(comparison *cypher.Comparison) bool {
	if comparison == nil {
		return false
	}

	for _, partial := range comparison.Partials {
		switch partial.Operator {
		case cypher.OperatorAnd, cypher.OperatorOr:
			return true
		}
	}

	return false
}

func parenthesizeDisjunctiveExpression(expression cypher.Expression) cypher.Expression {
	switch typedExpression := expression.(type) {
	case *cypher.Parenthetical:
		return typedExpression
	case *cypher.Disjunction, *cypher.ExclusiveDisjunction:
		return cypher.NewParenthetical(typedExpression)
	case *cypher.Comparison:
		if comparisonHasLogicalOperator(typedExpression) {
			return cypher.NewParenthetical(typedExpression)
		}
	}

	return expression
}

func parenthesizeLogicalExpression(expression cypher.Expression) cypher.Expression {
	switch typedExpression := expression.(type) {
	case *cypher.Parenthetical:
		return typedExpression
	case *cypher.Conjunction, *cypher.Disjunction, *cypher.ExclusiveDisjunction:
		return cypher.NewParenthetical(typedExpression)
	case *cypher.Comparison:
		if comparisonHasLogicalOperator(typedExpression) {
			return cypher.NewParenthetical(typedExpression)
		}
	}

	return expression
}

func Not(operand cypher.Expression) cypher.Expression {
	return cypher.NewNegation(parenthesizeLogicalExpression(operand))
}

func And(operands ...cypher.SyntaxNode) cypher.SyntaxNode {
	expressions, errExpression := joinedExpressionList(cypher.OperatorAnd, operands)
	if errExpression != nil {
		return errExpression
	}

	for idx, expression := range expressions {
		expressions[idx] = parenthesizeDisjunctiveExpression(expression)
	}

	return cypher.NewConjunction(expressions...)
}

func Or(operands ...cypher.SyntaxNode) cypher.SyntaxNode {
	expressions, errExpression := joinedExpressionList(cypher.OperatorOr, operands)
	if errExpression != nil {
		return errExpression
	}

	return cypher.NewParenthetical(cypher.NewDisjunction(expressions...))
}

type SortDirection int

const (
	SortAscending SortDirection = iota
	SortDescending
)

func Asc(expression any) *cypher.SortItem {
	return Order(expression, SortAscending)
}

func Desc(expression any) *cypher.SortItem {
	return Order(expression, SortDescending)
}

func Order(expression any, direction SortDirection) *cypher.SortItem {
	return &cypher.SortItem{
		Ascending:  direction != SortDescending,
		Expression: expressionOrError(expression),
	}
}

func As(expression any, alias string) *cypher.ProjectionItem {
	return &cypher.ProjectionItem{
		Expression: expressionOrError(expression),
		Alias:      cypher.NewVariableWithSymbol(alias),
	}
}

func Node() NodeContinuation {
	return DefaultScope().Node()
}

func Path() PathContinuation {
	return DefaultScope().Path()
}

func Start() NodeContinuation {
	return DefaultScope().Start()
}

func Relationship() RelationshipContinuation {
	return DefaultScope().Relationship()
}

func End() NodeContinuation {
	return DefaultScope().End()
}

type QualifiedExpression interface {
	qualifier() cypher.Expression
}

type scopedExpression interface {
	QualifiedExpression

	roleName() string
}

type deleteTarget interface {
	QualifiedExpression

	deleteTarget()
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
	Add(kinds graph.Kinds) *cypher.SetItem
	Remove(kinds graph.Kinds) *cypher.RemoveItem
}

type Comparable interface {
	In(value any) cypher.Expression
	Contains(value any) cypher.Expression
	StartsWith(value any) cypher.Expression
	EndsWith(value any) cypher.Expression
	Equals(value any) cypher.Expression
	GreaterThan(value any) cypher.Expression
	GreaterThanOrEqualTo(value any) cypher.Expression
	LessThan(value any) cypher.Expression
	LessThanOrEqualTo(value any) cypher.Expression
	IsNull() cypher.Expression
	IsNotNull() cypher.Expression
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
		valueExpression(rOperand),
	)
}

func (s *comparisonContinuation) In(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorIn, value)
}

func (s *comparisonContinuation) Contains(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorContains, value)
}

func (s *comparisonContinuation) StartsWith(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorStartsWith, value)
}

func (s *comparisonContinuation) EndsWith(value any) cypher.Expression {
	return s.asComparison(cypher.OperatorEndsWith, value)
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

func (s *comparisonContinuation) IsNull() cypher.Expression {
	return cypher.NewComparison(s.qualifier(), cypher.OperatorIs, Literal(nil))
}

func (s *comparisonContinuation) IsNotNull() cypher.Expression {
	return cypher.NewComparison(s.qualifier(), cypher.OperatorIsNot, Literal(nil))
}

type propertyContinuation struct {
	comparisonContinuation
}

func (s *propertyContinuation) Set(value any) *cypher.SetItem {
	return cypher.NewSetItem(
		s.qualifier(),
		cypher.OperatorAssignment,
		valueExpression(value),
	)
}

func (s *propertyContinuation) Remove() *cypher.RemoveItem {
	return cypher.RemoveProperty(s.qualifier())
}

type entity[T any] struct {
	identifier *cypher.Variable
	role       string
}

func (s *entity[T]) Kind() KindContinuation {
	return kindContinuation{
		identifier: s.identifier,
		role:       s.role,
	}
}

func (s *entity[T]) Kinds() KindsContinuation {
	return kindsContinuation{
		identifier: s.identifier,
		role:       s.role,
	}
}

func (s *entity[T]) Count() cypher.Expression {
	return cypher.NewSimpleFunctionInvocation(cypher.CountFunction, s.identifier)
}

func (s *entity[T]) SetProperties(properties map[string]any) *cypher.Set {
	set := &cypher.Set{}

	for _, key := range sortedPropertyKeys(properties) {
		set.Items = append(set.Items, s.Property(key).Set(properties[key]))
	}

	return set
}

func (s *entity[T]) RemoveProperties(properties []string) *cypher.Remove {
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

func (s *entity[T]) deleteTarget() {}

func (s *entity[T]) roleName() string {
	return s.role
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
	role       string
}

func (s kindContinuation) qualifier() cypher.Expression {
	return s.identifier
}

func (s kindContinuation) roleName() string {
	return s.role
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
	role       string
}

func (s kindsContinuation) qualifier() cypher.Expression {
	return s.identifier
}

func (s kindsContinuation) roleName() string {
	return s.role
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

func (s kindsContinuation) Add(kinds graph.Kinds) *cypher.SetItem {
	return cypher.NewSetItem(
		s.identifier,
		cypher.OperatorLabelAssignment,
		kinds,
	)
}

func (s kindsContinuation) Remove(kinds graph.Kinds) *cypher.RemoveItem {
	return cypher.RemoveKindsByMatcher(cypher.NewKindMatcher(s.identifier, kinds, false))
}

type PathContinuation interface {
	QualifiedExpression

	Count() cypher.Expression
}

type RelationshipContinuation interface {
	EntityContinuation

	RelationshipPattern(kind graph.Kind, properties cypher.Expression, direction graph.Direction) cypher.Expression

	Kind() KindContinuation
	SetProperties(properties map[string]any) *cypher.Set
	RemoveProperties(properties []string) *cypher.Remove
}

type NodeContinuation interface {
	EntityContinuation

	NodePattern(kinds graph.Kinds, properties cypher.Expression) cypher.Expression

	Kinds() KindsContinuation
	SetProperties(properties map[string]any) *cypher.Set
	RemoveProperties(properties []string) *cypher.Remove
}

type QueryBuilder interface {
	Where(constraints ...cypher.SyntaxNode) QueryBuilder
	OrderBy(sortItems ...any) QueryBuilder
	Skip(offset int) QueryBuilder
	// Limit accepts zero, which renders LIMIT 0 and returns an empty result set.
	Limit(limit int) QueryBuilder
	Return(projections ...any) QueryBuilder
	ReturnDistinct(projections ...any) QueryBuilder
	Update(updatingClauses ...any) QueryBuilder
	Create(creationClauses ...any) QueryBuilder
	Delete(expressions ...any) QueryBuilder
	WithShortestPaths() QueryBuilder
	WithAllShortestPaths() QueryBuilder
	WithTraversalDepth(depth TraversalDepth) QueryBuilder
	WithRelationshipDirection(direction graph.Direction) QueryBuilder
	Build() (*PreparedQuery, error)
}

type updatingClauseKind int

const (
	updatingClauseSet updatingClauseKind = iota
	updatingClauseRemove
	updatingClauseDelete
	updatingClauseCreate
)

type pendingUpdatingClause struct {
	kind        updatingClauseKind
	creates     []any
	setItems    []*cypher.SetItem
	removeItems []*cypher.RemoveItem
	deleteItems []cypher.Expression
	detach      bool
}

type builder struct {
	errors                []error
	constraints           []cypher.SyntaxNode
	sortItems             []any
	projections           []any
	distinct              bool
	identifiers           runtimeIdentifiers
	updatingClauses       []pendingUpdatingClause
	creates               []any
	setItems              []*cypher.SetItem
	removeItems           []*cypher.RemoveItem
	deleteItems           []cypher.Expression
	detachDelete          bool
	relationshipDirection graph.Direction
	traversalDepth        *cypher.PatternRange
	shortestPathQuery     bool
	allShorestPathsQuery  bool
	skip                  *int
	limit                 *int
}

func New() QueryBuilder {
	return DefaultScope().New()
}

func newBuilder(identifiers runtimeIdentifiers, errs ...error) QueryBuilder {
	return &builder{
		identifiers:           identifiers,
		errors:                append([]error(nil), errs...),
		relationshipDirection: graph.DirectionOutbound,
	}
}

func (s *builder) WithShortestPaths() QueryBuilder {
	s.shortestPathQuery = true
	return s
}

func (s *builder) WithAllShortestPaths() QueryBuilder {
	s.allShorestPathsQuery = true
	return s
}

func (s *builder) WithTraversalDepth(depth TraversalDepth) QueryBuilder {
	if depth.err != nil {
		s.trackError(depth.err)
	} else {
		s.traversalDepth = depth.rangePattern()
	}

	return s
}

func (s *builder) WithRelationshipDirection(direction graph.Direction) QueryBuilder {
	if err := validateRelationshipDirection(direction); err != nil {
		s.trackError(err)
	} else {
		s.relationshipDirection = direction
	}

	return s
}

func (s *builder) OrderBy(sortItems ...any) QueryBuilder {
	s.sortItems = append(s.sortItems, sortItems...)
	return s
}

func (s *builder) Skip(skip int) QueryBuilder {
	if skip < 0 {
		s.trackError(fmt.Errorf("skip must be non-negative: %d", skip))
		return s
	}

	s.skip = &skip
	return s
}

func (s *builder) Limit(limit int) QueryBuilder {
	if limit < 0 {
		s.trackError(fmt.Errorf("limit must be non-negative: %d", limit))
		return s
	}

	s.limit = &limit
	return s
}

func (s *builder) Return(projections ...any) QueryBuilder {
	s.projections = append(s.projections, projections...)
	return s
}

func (s *builder) ReturnDistinct(projections ...any) QueryBuilder {
	s.distinct = true
	s.projections = append(s.projections, projections...)
	return s
}

func (s *builder) appendSetItems(items ...*cypher.SetItem) {
	if len(items) == 0 {
		return
	}

	lastClauseIdx := len(s.updatingClauses) - 1
	if lastClauseIdx >= 0 && s.updatingClauses[lastClauseIdx].kind == updatingClauseSet {
		s.updatingClauses[lastClauseIdx].setItems = append(s.updatingClauses[lastClauseIdx].setItems, items...)
	} else {
		s.updatingClauses = append(s.updatingClauses, pendingUpdatingClause{
			kind:     updatingClauseSet,
			setItems: items,
		})
	}
}

func (s *builder) appendRemoveItems(items ...*cypher.RemoveItem) {
	if len(items) == 0 {
		return
	}

	lastClauseIdx := len(s.updatingClauses) - 1
	if lastClauseIdx >= 0 && s.updatingClauses[lastClauseIdx].kind == updatingClauseRemove {
		s.updatingClauses[lastClauseIdx].removeItems = append(s.updatingClauses[lastClauseIdx].removeItems, items...)
	} else {
		s.updatingClauses = append(s.updatingClauses, pendingUpdatingClause{
			kind:        updatingClauseRemove,
			removeItems: items,
		})
	}
}

func (s *builder) appendDeleteItems(detach bool, items ...cypher.Expression) {
	if len(items) == 0 {
		return
	}

	// Consecutive deletes share one clause; any node delete makes the whole clause DETACH DELETE.
	lastClauseIdx := len(s.updatingClauses) - 1
	if lastClauseIdx >= 0 && s.updatingClauses[lastClauseIdx].kind == updatingClauseDelete {
		s.updatingClauses[lastClauseIdx].detach = s.updatingClauses[lastClauseIdx].detach || detach
		s.updatingClauses[lastClauseIdx].deleteItems = append(s.updatingClauses[lastClauseIdx].deleteItems, items...)
	} else {
		s.updatingClauses = append(s.updatingClauses, pendingUpdatingClause{
			kind:        updatingClauseDelete,
			deleteItems: items,
			detach:      detach,
		})
	}
}

func (s *builder) Create(creationClauses ...any) QueryBuilder {
	s.creates = append(s.creates, creationClauses...)

	if len(creationClauses) > 0 {
		s.updatingClauses = append(s.updatingClauses, pendingUpdatingClause{
			kind:    updatingClauseCreate,
			creates: creationClauses,
		})
	}

	return s
}

func (s *builder) Update(updates ...any) QueryBuilder {
	for _, nextUpdate := range updates {
		switch typedNextUpdate := nextUpdate.(type) {
		case *cypher.Set:
			if setItems, err := setItemsFromSet(typedNextUpdate); err != nil {
				s.trackError(err)
			} else {
				s.setItems = append(s.setItems, setItems...)
				s.appendSetItems(setItems...)
			}

		case *cypher.SetItem:
			if setItem, err := setItemFromValue(typedNextUpdate); err != nil {
				s.trackError(err)
			} else {
				s.setItems = append(s.setItems, setItem)
				s.appendSetItems(setItem)
			}

		case *cypher.Remove:
			if removeItems, err := removeItemsFromRemove(typedNextUpdate); err != nil {
				s.trackError(err)
			} else {
				s.removeItems = append(s.removeItems, removeItems...)
				s.appendRemoveItems(removeItems...)
			}

		case *cypher.RemoveItem:
			if removeItem, err := removeItemFromValue(typedNextUpdate); err != nil {
				s.trackError(err)
			} else {
				s.removeItems = append(s.removeItems, removeItem)
				s.appendRemoveItems(removeItem)
			}

		default:
			s.trackError(fmt.Errorf("unknown update type: %T", nextUpdate))
		}
	}

	return s
}

func (s *builder) Delete(deleteItems ...any) QueryBuilder {
	var pendingDeleteItems []cypher.Expression
	pendingDetachDelete := false

	for _, nextDelete := range deleteItems {
		switch typedNextUpdate := nextDelete.(type) {
		case deleteTarget:
			if isNilPointer(typedNextUpdate) {
				s.trackError(fmt.Errorf("delete target is nil"))
				continue
			}

			deleteItem, detach, err := deleteItemFromExpression(typedNextUpdate.qualifier(), s.identifiers)
			if err != nil {
				s.trackError(err)
				continue
			}

			if detach {
				s.detachDelete = true
				pendingDetachDelete = true
			}

			s.deleteItems = append(s.deleteItems, deleteItem)
			pendingDeleteItems = append(pendingDeleteItems, deleteItem)

		case *cypher.Variable:
			deleteItem, detach, err := deleteItemFromExpression(typedNextUpdate, s.identifiers)
			if err != nil {
				s.trackError(err)
				continue
			}

			if detach {
				s.detachDelete = true
				pendingDetachDelete = true
			}

			s.deleteItems = append(s.deleteItems, deleteItem)
			pendingDeleteItems = append(pendingDeleteItems, deleteItem)

		case *cypher.PropertyLookup:
			if err := validateExpressionValue(typedNextUpdate, "delete expression"); err != nil {
				s.trackError(err)
				continue
			}

			s.trackError(fmt.Errorf("delete target must be a node, relationship, or variable; use remove for properties"))

		case QualifiedExpression:
			if isNilPointer(typedNextUpdate) {
				s.trackError(fmt.Errorf("delete target is nil"))
				continue
			}

			if err := validateExpressionValue(typedNextUpdate.qualifier(), "delete expression"); err != nil {
				s.trackError(err)
				continue
			}

			s.trackError(fmt.Errorf("delete target must be a node, relationship, or variable; got %T", nextDelete))

		default:
			s.trackError(fmt.Errorf("unknown delete type: %T", nextDelete))
		}
	}

	s.appendDeleteItems(pendingDetachDelete, pendingDeleteItems...)
	return s
}

func deleteItemFromExpression(expression cypher.Expression, identifiers runtimeIdentifiers) (cypher.Expression, bool, error) {
	if err := validateExpressionValue(expression, "delete expression"); err != nil {
		return nil, false, err
	}

	variable, typeOK := expression.(*cypher.Variable)
	if !typeOK || variable == nil {
		return nil, false, fmt.Errorf("delete target must resolve to a variable, got %T", expression)
	}

	if variable.Symbol == identifiers.path {
		return nil, false, fmt.Errorf("delete target must be a node or relationship variable, got path variable %q", variable.Symbol)
	}

	return copyExpression(variable), isDetachDeleteQualifier(variable, identifiers), nil
}

func (s *builder) trackError(err error) {
	s.errors = append(s.errors, err)
}

func (s *builder) Where(constraints ...cypher.SyntaxNode) QueryBuilder {
	s.constraints = append(s.constraints, constraints...)
	return s
}

func patternEndsWithNodePattern(pattern *cypher.PatternPart) bool {
	numElements := len(pattern.PatternElements)
	if numElements == 0 {
		return false
	}

	return pattern.PatternElements[numElements-1].IsNodePattern()
}

func isCreateNodeValue(value any, identifiers runtimeIdentifiers) bool {
	switch typedValue := value.(type) {
	case QualifiedExpression:
		if variable, typeOK := typedValue.qualifier().(*cypher.Variable); typeOK {
			switch variable.Symbol {
			case identifiers.node, identifiers.start, identifiers.end:
				return true
			}
		}

	case *cypher.NodePattern:
		return typedValue != nil
	}

	return false
}

func nextCreateValueIsNode(creates []any, idx int, identifiers runtimeIdentifiers) bool {
	nextIdx := idx + 1
	return nextIdx < len(creates) && isCreateNodeValue(creates[nextIdx], identifiers)
}

func buildCreates(singlePartQuery *cypher.SinglePartQuery, identifiers runtimeIdentifiers, creates []any) error {
	if len(creates) == 0 {
		return nil
	}

	var (
		pattern      = &cypher.PatternPart{}
		createClause = &cypher.Create{
			Unique:  false,
			Pattern: []*cypher.PatternPart{pattern},
		}
	)

	for idx, nextCreate := range creates {
		switch typedNextCreate := nextCreate.(type) {
		case QualifiedExpression:
			switch typedExpression := typedNextCreate.qualifier().(type) {
			case *cypher.Variable:
				if typedExpression == nil {
					return fmt.Errorf("invalid variable reference for create: <nil>")
				}

				switch typedExpression.Symbol {
				case identifiers.node, identifiers.start, identifiers.end:
					pattern.AddPatternElements(&cypher.NodePattern{
						Variable: cypher.NewVariableWithSymbol(typedExpression.Symbol),
					})

				default:
					return fmt.Errorf("invalid variable reference for create: %s", typedExpression.Symbol)
				}

			default:
				return fmt.Errorf("invalid qualified expression for create: %T", typedExpression)
			}

		case *cypher.NodePattern:
			if err := validateNodePattern(typedNextCreate); err != nil {
				return err
			}

			pattern.AddPatternElements(cypher.Copy(typedNextCreate))

		case *cypher.RelationshipPattern:
			if err := validateRelationshipPattern(typedNextCreate); err != nil {
				return err
			}

			if !patternEndsWithNodePattern(pattern) {
				pattern.AddPatternElements(&cypher.NodePattern{
					Variable: identifiers.Start(),
				})
			}

			pattern.AddPatternElements(cypher.Copy(typedNextCreate))

			if !nextCreateValueIsNode(creates, idx, identifiers) {
				pattern.AddPatternElements(&cypher.NodePattern{
					Variable: identifiers.End(),
				})
			}

		default:
			return fmt.Errorf("invalid type for create: %T", nextCreate)
		}
	}

	singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(createClause))
	return nil
}

func (s *builder) buildUpdatingClauses(singlePartQuery *cypher.SinglePartQuery) error {
	for _, updatingClause := range s.updatingClauses {
		switch updatingClause.kind {
		case updatingClauseSet:
			singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(
				cypher.NewSet(updatingClause.setItems),
			))

		case updatingClauseRemove:
			singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(
				cypher.NewRemove(updatingClause.removeItems),
			))

		case updatingClauseDelete:
			singlePartQuery.UpdatingClauses = append(singlePartQuery.UpdatingClauses, cypher.NewUpdatingClause(
				cypher.NewDelete(
					updatingClause.detach,
					updatingClause.deleteItems,
				),
			))

		case updatingClauseCreate:
			if err := buildCreates(singlePartQuery, s.identifiers, updatingClause.creates); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *builder) buildProjectionOrder() (*cypher.Order, error) {
	var orderByNode *cypher.Order

	if len(s.sortItems) > 0 {
		orderByNode = &cypher.Order{}

		for _, untypedSortItem := range s.sortItems {
			switch typedSortItem := untypedSortItem.(type) {
			case *cypher.Order:
				if sortItems, err := sortItemsFromOrder(typedSortItem); err != nil {
					return nil, err
				} else {
					orderByNode.Items = append(orderByNode.Items, sortItems...)
				}

			case *cypher.SortItem:
				if sortItem, err := sortItemFromValue(typedSortItem); err != nil {
					return nil, err
				} else {
					orderByNode.Items = append(orderByNode.Items, sortItem)
				}

			default:
				if sortItem, err := sortItemFromValue(typedSortItem); err != nil {
					return nil, err
				} else {
					orderByNode.Items = append(orderByNode.Items, sortItem)
				}
			}
		}
	}

	return orderByNode, nil
}

func appendProjectionOrder(projection *cypher.Projection, sortItems ...*cypher.SortItem) {
	if len(sortItems) == 0 {
		return
	}

	if projection.Order == nil {
		projection.Order = &cypher.Order{}
	}

	projection.Order.Items = append(projection.Order.Items, sortItems...)
}

func applyReturnProjection(projection *cypher.Projection, returnClause *cypher.Return) error {
	if projectionItems, err := projectionItemsFromReturn(returnClause); err != nil {
		return err
	} else {
		projection.Distinct = projection.Distinct || returnClause.Projection.Distinct
		projection.All = projection.All || returnClause.Projection.All

		for _, projectionItem := range projectionItems {
			projection.AddItem(projectionItem)
		}
	}

	if returnClause.Projection.Order != nil {
		if sortItems, err := sortItemsFromOrder(returnClause.Projection.Order); err != nil {
			return err
		} else {
			appendProjectionOrder(projection, sortItems...)
		}
	}

	if returnClause.Projection.Skip != nil {
		projection.Skip = copySkip(returnClause.Projection.Skip)
	}

	if returnClause.Projection.Limit != nil {
		projection.Limit = copyLimit(returnClause.Projection.Limit)
	}

	return nil
}

func (s *builder) buildProjection(singlePartQuery *cypher.SinglePartQuery) error {
	var (
		hasProjectedItems  = len(s.projections) > 0
		hasSkip            = s.skip != nil
		hasLimit           = s.limit != nil
		requiresProjection = hasProjectedItems || hasSkip || hasLimit
	)

	if requiresProjection {
		if !hasProjectedItems {
			return fmt.Errorf("query expected projected items")
		}

		projection := singlePartQuery.NewProjection(s.distinct)

		for _, nextProjection := range s.projections {
			switch typedNextProjection := nextProjection.(type) {
			case *cypher.Return:
				if err := applyReturnProjection(projection, typedNextProjection); err != nil {
					return err
				}

			default:
				if projectionItem, err := projectionItemFromValue(typedNextProjection); err != nil {
					return err
				} else {
					projection.AddItem(projectionItem)
				}
			}
		}

		if s.skip != nil {
			projection.Skip = cypher.NewSkip(*s.skip)
		}

		if s.limit != nil {
			projection.Limit = cypher.NewLimit(*s.limit)
		}

		if projectionOrder, err := s.buildProjectionOrder(); err != nil {
			return err
		} else if projectionOrder != nil {
			appendProjectionOrder(projection, projectionOrder.Items...)
		}
	}

	return nil
}

func countRelationshipKindMatchers(constraints []cypher.SyntaxNode, identifiers runtimeIdentifiers) (int, error) {
	var count int

	for _, nextConstraint := range constraints {
		if kindMatcher, typeOK := nextConstraint.(*cypher.KindMatcher); typeOK {
			if identifier, typeOK := kindMatcher.Reference.(*cypher.Variable); !typeOK {
				return 0, fmt.Errorf("expected type *cypher.Variable, got %T", kindMatcher.Reference)
			} else if identifier.Symbol == identifiers.relationship {
				count++
			}
		}
	}

	return count, nil
}

type PreparedQuery struct {
	Query      *cypher.RegularQuery
	Parameters map[string]any
}

func (s *builder) hasActions() bool {
	return len(s.projections) > 0 || len(s.setItems) > 0 || len(s.removeItems) > 0 || len(s.creates) > 0 || len(s.deleteItems) > 0
}

func (s *builder) wantsShortestPathPattern() bool {
	return s.shortestPathQuery || s.allShorestPathsQuery
}

func (s *builder) wantsTraversalPattern() bool {
	return s.traversalDepth != nil
}

func (s *builder) usesRangedRelationshipPattern() bool {
	return s.wantsTraversalPattern() || s.wantsShortestPathPattern()
}

func (s *builder) Build() (*PreparedQuery, error) {
	if len(s.errors) > 0 {
		return nil, errors.Join(s.errors...)
	}

	if !s.hasActions() {
		return nil, fmt.Errorf("query has no action specified")
	}

	if err := collectModelErrorsFromKnownValues(s.constraints, s.creates, s.setItems, s.removeItems, s.deleteItems, s.projections, s.sortItems); err != nil {
		return nil, err
	}

	var (
		regularQuery, singlePartQuery = cypher.NewRegularQueryWithSingleQuery()
		match                         = &cypher.Match{}
		readIdentifiers               = newIdentifierSet()
		relationshipKinds             graph.Kinds
	)

	createScope, err := collectCreateScope(s.identifiers, s.creates...)
	if err != nil {
		return nil, err
	}

	if err := s.buildUpdatingClauses(singlePartQuery); err != nil {
		return nil, err
	}

	if err := s.buildProjection(singlePartQuery); err != nil {
		return nil, err
	}

	if len(s.constraints) > 0 {
		var (
			whereClause                      = match.NewWhere()
			constraints                      = cypher.NewConjunction()
			numRelationshipKindMatchers, err = countRelationshipKindMatchers(s.constraints, s.identifiers)
		)
		if err != nil {
			return nil, err
		}

		for _, nextConstraint := range s.constraints {
			switch typedNextConstraint := nextConstraint.(type) {
			case *cypher.KindMatcher:
				if identifier, typeOK := typedNextConstraint.Reference.(*cypher.Variable); !typeOK {
					return nil, fmt.Errorf("expected type *cypher.Variable, got %T", typedNextConstraint.Reference)
				} else if identifier.Symbol == s.identifiers.relationship && numRelationshipKindMatchers == 1 {
					relationshipKinds = relationshipKinds.Add(typedNextConstraint.Kinds...)
					readIdentifiers.Add(s.identifiers.relationship)
					continue
				}
			}

			constraintCopy := cypher.Copy(nextConstraint)
			constraints.Add(parenthesizeDisjunctiveExpression(constraintCopy))
		}

		if constraints.Len() > 0 {
			whereClause.Add(constraints)

			whereIdentifiers := newIdentifierSet()
			if err := whereIdentifiers.CollectFromExpression(whereClause); err != nil {
				return nil, err
			}

			if s.usesRangedRelationshipPattern() && whereIdentifiers.Contains(s.identifiers.relationship) {
				return nil, fmt.Errorf("ranged relationship patterns only support top-level relationship kind constraints")
			}

			readIdentifiers.Or(whereIdentifiers)
		}
	}

	actionIdentifiers, err := collectIdentifiersFromValues(s.setItems, s.removeItems, s.deleteItems, s.projections, s.sortItems)
	if err != nil {
		return nil, err
	}

	actionIdentifiers.Remove(createScope.identifiers)

	if s.usesRangedRelationshipPattern() && actionIdentifiers.Contains(s.identifiers.relationship) {
		return nil, fmt.Errorf("ranged relationship patterns do not support relationship projections or mutations; return the path instead")
	}

	matchIdentifiers := readIdentifiers.Clone()
	matchIdentifiers.Or(actionIdentifiers)

	if err := validateKnownIdentifiers(matchIdentifiers, s.identifiers); err != nil {
		return nil, err
	}

	if s.wantsTraversalPattern() && !isRelationshipPattern(matchIdentifiers, s.identifiers) && !matchIdentifiers.Contains(s.identifiers.path) {
		return nil, fmt.Errorf("recursive traversal query requires relationship query identifiers")
	}

	if s.wantsShortestPathPattern() && !isRelationshipPattern(matchIdentifiers, s.identifiers) {
		return nil, fmt.Errorf("shortest path query requires relationship query identifiers")
	}

	if len(s.constraints) > 0 || matchIdentifiers.Len() > 0 {
		if isNodePattern(matchIdentifiers, s.identifiers) {
			if err := prepareNodePattern(match, matchIdentifiers, s.identifiers); err != nil {
				return nil, err
			}
		} else if createScope.createsRelationship && !matchIdentifiers.Contains(s.identifiers.relationship) {
			if err := prepareCreateRelationshipMatch(match, matchIdentifiers, s.identifiers); err != nil {
				return nil, err
			}
		} else if isRelationshipPattern(matchIdentifiers, s.identifiers) || (s.wantsTraversalPattern() && matchIdentifiers.Contains(s.identifiers.path)) {
			if err := prepareRelationshipPattern(match, matchIdentifiers, s.identifiers, relationshipKinds, s.traversalDepth, s.relationshipDirection, s.shortestPathQuery, s.allShorestPathsQuery); err != nil {
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

	if err := collectModelErrors(regularQuery); err != nil {
		return nil, err
	}

	if parameters, err := materializeParameters(regularQuery); err != nil {
		return nil, err
	} else {
		return &PreparedQuery{
			Query:      regularQuery,
			Parameters: parameters,
		}, nil
	}
}

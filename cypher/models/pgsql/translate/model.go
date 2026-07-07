package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

const (
	expansionRootID        pgsql.Identifier = "root_id"
	expansionNextID        pgsql.Identifier = "next_id"
	expansionDepth         pgsql.Identifier = "depth"
	expansionSatisfied     pgsql.Identifier = "satisfied"
	expansionIsCycle       pgsql.Identifier = "is_cycle"
	expansionPath          pgsql.Identifier = "path"
	expansionForwardFront  pgsql.Identifier = "forward_front"
	expansionBackwardFront pgsql.Identifier = "backward_front"
	expansionNextFront     pgsql.Identifier = "next_front"
)

func expansionColumns() *pgsql.RecordShape {
	return pgsql.NewRecordShape([]pgsql.Identifier{
		expansionRootID,
		expansionNextID,
		expansionDepth,
		expansionSatisfied,
		expansionIsCycle,
		expansionPath,
	})
}

type NodeSelect struct {
	Frame       *Frame
	Binding     *BoundIdentifier
	Select      pgsql.Select
	Constraints pgsql.Expression
}

type ExpansionOptions struct {
	FindShortestPath     bool
	FindAllShortestPaths bool
	MinDepth             models.Optional[int64]
	MaxDepth             models.Optional[int64]
}

func newExpansionOptions(part *PatternPart, relationshipPattern *cypher.RelationshipPattern) ExpansionOptions {
	return ExpansionOptions{
		FindShortestPath:     part.ShortestPath,
		FindAllShortestPaths: part.AllShortestPaths,
		MinDepth:             models.OptionalPointer(relationshipPattern.Range.StartIndex),
		MaxDepth:             models.OptionalPointer(relationshipPattern.Range.EndIndex),
	}
}

type Expansion struct {
	Frame       *Frame
	PathBinding *BoundIdentifier
	Options     ExpansionOptions

	PrimerNodeConstraints              pgsql.Expression
	PrimerNodeSatisfactionProjection   pgsql.SelectItem
	PrimerNodeJoinCondition            pgsql.Expression
	EdgeConstraints                    pgsql.Expression
	EdgeJoinCondition                  pgsql.Expression
	RecursiveConstraints               pgsql.Expression
	ExpansionNodeJoinCondition         pgsql.Expression
	TerminalNodeConstraints            pgsql.Expression
	TerminalNodeSatisfactionProjection pgsql.SelectItem
	DeferredNodeSatisfactionConstraint pgsql.Expression
	UseMaterializedTerminalFilter      bool
	UseMaterializedEndpointPairFilter  bool
	HasExplicitEndpointInequality      bool

	PrimerQueryParameter            *BoundIdentifier
	BackwardPrimerQueryParameter    *BoundIdentifier
	RecursiveQueryParameter         *BoundIdentifier
	BackwardRecursiveQueryParameter *BoundIdentifier

	UseBidirectionalSearch bool

	EdgeStartIdentifier pgsql.Identifier
	EdgeStartColumn     pgsql.CompoundIdentifier
	EdgeEndIdentifier   pgsql.Identifier
	EdgeEndColumn       pgsql.CompoundIdentifier

	Projection []pgsql.SelectItem
}

func NewExpansionModel(part *PatternPart, relationshipPattern *cypher.RelationshipPattern) *Expansion {
	return &Expansion{
		Options: newExpansionOptions(part, relationshipPattern),
	}
}

func (s *Expansion) CompletePattern(traversalStep *TraversalStep) error {
	// This determines which side of the expansion is treated as the root (where the traversal begins)
	switch traversalStep.Direction {
	case graph.DirectionInbound:
		s.EdgeStartIdentifier = pgsql.ColumnEndID
		s.EdgeEndIdentifier = pgsql.ColumnStartID

	case graph.DirectionOutbound:
		s.EdgeStartIdentifier = pgsql.ColumnStartID
		s.EdgeEndIdentifier = pgsql.ColumnEndID

	default:
		return ErrUnsupportedExpansionDirection
	}

	s.EdgeStartColumn = pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, s.EdgeStartIdentifier}
	s.EdgeEndColumn = pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, s.EdgeEndIdentifier}

	return nil
}

func (s *Expansion) FlipDirection() {
	oldEdgeStartColumn := s.EdgeStartColumn
	s.EdgeStartColumn = s.EdgeEndColumn
	s.EdgeEndColumn = oldEdgeStartColumn
}

func (s *Expansion) CanExecuteBidirectionalSearch() bool {
	return s.PrimerNodeConstraints != nil && s.TerminalNodeConstraints != nil
}

func (s *TraversalStep) CanExecuteBidirectionalSearch() bool {
	if s.Expansion == nil {
		return false
	}

	return s.Expansion.CanExecuteBidirectionalSearch() ||
		(s.LeftNodeBound && s.RightNodeBound && s.Frame != nil && s.Frame.Previous != nil)
}

func (s *TraversalStep) hasPreviousFrameBinding() bool {
	return s.Frame != nil && s.Frame.Previous != nil
}

func (s *TraversalStep) usesBoundEndpointPairs() bool {
	return s.LeftNodeBound && s.RightNodeBound && s.hasPreviousFrameBinding()
}

func (s *TraversalStep) usesBoundTerminalIDs() bool {
	return s.RightNodeBound && s.hasPreviousFrameBinding()
}

func canMaterializeTerminalFilterForStep(traversalStep *TraversalStep, expansionModel *Expansion) bool {
	if traversalStep == nil || expansionModel == nil || traversalStep.RightNode == nil ||
		expansionModel.TerminalNodeConstraints == nil ||
		traversalStep.usesBoundEndpointPairs() ||
		traversalStep.usesBoundTerminalIDs() {
		return false
	}

	// Terminal filters are only useful as standalone SQL when they depend solely
	// on the terminal node; external references must stay in the main query.
	_, externalConstraints := partitionConstraintByLocality(
		expansionModel.TerminalNodeConstraints,
		pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier),
	)

	return externalConstraints == nil
}

func canMaterializeEndpointPairFilterForStep(traversalStep *TraversalStep, expansionModel *Expansion) bool {
	// Pair filters enumerate the exact root/terminal combinations the
	// bidirectional harness must resolve. Kind-only endpoint predicates are not
	// enough because they do not constrain the search columns used by the harness.
	if traversalStep == nil || expansionModel == nil ||
		traversalStep.LeftNode == nil ||
		traversalStep.RightNode == nil ||
		traversalStep.usesBoundEndpointPairs() ||
		expansionModel.PrimerNodeConstraints == nil ||
		expansionModel.TerminalNodeConstraints == nil ||
		!hasPairAwareEndpointConstraint(expansionModel.PrimerNodeConstraints, traversalStep.LeftNode.Identifier) ||
		!hasPairAwareEndpointConstraint(expansionModel.TerminalNodeConstraints, traversalStep.RightNode.Identifier) {
		return false
	}

	return true
}

func (s *TraversalStep) endpointSelectivity(scope *Scope, expression pgsql.Expression, bound bool) (int, error) {
	return optimize.NewSelectivityModel(scope).EndpointSelectivity(expression, bound, s.hasPreviousFrameBinding())
}

func isBidirectionalSearchAnchor(selectivity int) bool {
	return optimize.IsBidirectionalSearchAnchor(selectivity)
}

func hasIDEqualityConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	for _, term := range flattenConjunction(expression) {
		binaryExpression, isBinaryExpression := unwrapParenthetical(term).(*pgsql.BinaryExpression)
		if !isBinaryExpression || binaryExpression.Operator != pgsql.OperatorEquals {
			continue
		}

		var (
			leftIsID  = isIdentifierIDReference(binaryExpression.LOperand, identifier)
			rightIsID = isIdentifierIDReference(binaryExpression.ROperand, identifier)
		)

		if leftIsID && isStaticIDEqualityOperand(binaryExpression.ROperand) {
			return true
		}

		if rightIsID && isStaticIDEqualityOperand(binaryExpression.LOperand) {
			return true
		}
	}

	return false
}

func hasLocalIDEqualityConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	if !hasIDEqualityConstraint(expression, identifier) {
		return false
	}

	return hasLocalEndpointConstraint(expression, identifier)
}

func hasLocalEndpointConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	if expression == nil || !referencesIdentifier(expression, identifier) {
		return false
	}

	_, externalConstraints := partitionConstraintByLocality(expression, pgsql.AsIdentifierSet(identifier))
	return externalConstraints == nil
}

func referencesIdentifier(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	references := false

	_ = walk.PgSQL(expression, walk.NewSimpleVisitor[pgsql.SyntaxNode](
		func(node pgsql.SyntaxNode, handler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case pgsql.CompoundIdentifier:
				if len(typedNode) > 0 && typedNode[0] == identifier {
					references = true
					handler.SetDone()
				}

			case pgsql.Identifier:
				if typedNode == identifier {
					references = true
					handler.SetDone()
				}

			case pgsql.RowColumnReference:
				if referencesIdentifier(typedNode.Identifier, identifier) {
					references = true
					handler.SetDone()
				} else {
					handler.Consume()
				}
			}
		},
	))

	return references
}

func hasPairAwareEndpointConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	return hasLocalEndpointConstraint(expression, identifier) &&
		referencesEndpointSearchColumn(expression, identifier)
}

func referencesEndpointSearchColumn(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	references := false

	_ = walk.PgSQL(expression, walk.NewSimpleVisitor[pgsql.SyntaxNode](
		func(node pgsql.SyntaxNode, handler walk.VisitorHandler) {
			// kind_ids constrains labels but not the endpoint ID space the
			// bidirectional harness uses to enumerate root/terminal pairs.
			if compoundIdentifier, isCompoundIdentifier := node.(pgsql.CompoundIdentifier); isCompoundIdentifier &&
				len(compoundIdentifier) > 1 &&
				compoundIdentifier[0] == identifier &&
				compoundIdentifier[1] != pgsql.ColumnKindIDs {
				references = true
				handler.SetDone()
			}
		},
	))

	return references
}

func isStaticIDEqualityOperand(expression pgsql.Expression) bool {
	if expression == nil {
		return false
	}

	isStatic := true

	_ = walk.PgSQL(unwrapParenthetical(expression), walk.NewSimpleVisitor[pgsql.SyntaxNode](
		func(node pgsql.SyntaxNode, handler walk.VisitorHandler) {
			switch node.(type) {
			case pgsql.Identifier, pgsql.CompoundIdentifier, pgsql.RowColumnReference:
				isStatic = false
				handler.SetDone()
			}
		},
	))

	return isStatic
}

func isIdentifierIDReference(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	compoundIdentifier, isCompoundIdentifier := unwrapParenthetical(expression).(pgsql.CompoundIdentifier)
	return isCompoundIdentifier && len(compoundIdentifier) == 2 &&
		compoundIdentifier[0] == identifier &&
		compoundIdentifier[1] == pgsql.ColumnID
}

func (s *TraversalStep) CanExecuteSelectiveBidirectionalSearch(scope *Scope) (bool, error) {
	if s.Expansion == nil {
		return false, nil
	}

	if s.usesBoundEndpointPairs() {
		return true, nil
	}

	if !s.Expansion.CanExecuteBidirectionalSearch() {
		return false, nil
	}

	if s.LeftNode == nil || s.RightNode == nil {
		return false, nil
	}

	if hasLocalIDEqualityConstraint(s.Expansion.PrimerNodeConstraints, s.LeftNode.Identifier) &&
		hasLocalIDEqualityConstraint(s.Expansion.TerminalNodeConstraints, s.RightNode.Identifier) {
		return true, nil
	}

	// Bidirectional shortest-path search is only correct for multi-endpoint
	// queries when the harness knows the complete pair universe. Unbound
	// endpoint predicates can be selective, but they do not by themselves
	// define which (root, terminal) pairs must be completed.
	return false, nil
}

func (s *TraversalStep) CanExecutePairAwareBidirectionalSearch(scope *Scope) (bool, error) {
	if canExecute, err := s.CanExecuteSelectiveBidirectionalSearch(scope); canExecute || err != nil {
		return canExecute, err
	}

	if s.Expansion == nil ||
		(!s.Expansion.Options.FindShortestPath && !s.Expansion.Options.FindAllShortestPaths) ||
		!s.Expansion.CanExecuteBidirectionalSearch() ||
		s.LeftNode == nil ||
		s.RightNode == nil ||
		!hasPairAwareEndpointConstraint(s.Expansion.PrimerNodeConstraints, s.LeftNode.Identifier) ||
		!hasPairAwareEndpointConstraint(s.Expansion.TerminalNodeConstraints, s.RightNode.Identifier) {
		return false, nil
	}

	if primerSelectivity, err := s.endpointSelectivity(scope, s.Expansion.PrimerNodeConstraints, s.LeftNodeBound); err != nil {
		return false, err
	} else if !isBidirectionalSearchAnchor(primerSelectivity) {
		return false, nil
	}

	if terminalSelectivity, err := s.endpointSelectivity(scope, s.Expansion.TerminalNodeConstraints, s.RightNodeBound); err != nil {
		return false, err
	} else {
		return isBidirectionalSearchAnchor(terminalSelectivity), nil
	}
}

func flattenConjunction(expr pgsql.Expression) []pgsql.Expression {
	return optimize.FlattenConjunction(expr)
}

func expressionReferencesOnlyLocalIdentifiers(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	return optimize.ExpressionReferencesOnlyLocalIdentifiers(expression, localScope)
}

func subqueryReferencesOnlyLocalIdentifiers(subquery pgsql.Subquery, localScope *pgsql.IdentifierSet) bool {
	return optimize.SubqueryReferencesOnlyLocalIdentifiers(subquery, localScope)
}

func queryReferencesOnlyLocalIdentifiers(query pgsql.Query, localScope *pgsql.IdentifierSet) bool {
	return optimize.QueryReferencesOnlyLocalIdentifiers(query, localScope)
}

func addFromClauseBindings(localScope *pgsql.IdentifierSet, fromClauses []pgsql.FromClause) {
	optimize.AddFromClauseBindings(localScope, fromClauses)
}

func addFromExpressionBinding(localScope *pgsql.IdentifierSet, expression pgsql.Expression) {
	optimize.AddFromExpressionBinding(localScope, expression)
}

func selectReferencesOnlyLocalIdentifiers(selectBody pgsql.Select, localScope *pgsql.IdentifierSet) bool {
	return optimize.SelectReferencesOnlyLocalIdentifiers(selectBody, localScope)
}

func fromExpressionReferencesOnlyLocalIdentifiers(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	return optimize.FromExpressionReferencesOnlyLocalIdentifiers(expression, localScope)
}

func isLocalToScope(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	return optimize.IsLocalToScope(expression, localScope)
}

func partitionConstraintByLocality(expression pgsql.Expression, localScope *pgsql.IdentifierSet) (pgsql.Expression, pgsql.Expression) {
	return optimize.PartitionConstraintByLocality(expression, localScope)
}

type ProjectionPruningApplication struct {
	LeftNode     *BoundIdentifier
	Relationship *BoundIdentifier
	RightNode    *BoundIdentifier
	PathBinding  *BoundIdentifier
}

type TraversalStep struct {
	Frame                  *Frame
	SourceTarget           optimize.TraversalStepTarget
	HasSourceTarget        bool
	Direction              graph.Direction
	Expansion              *Expansion
	PathReversed           bool
	ProjectionPruning      ProjectionPruningApplication
	LeftNode               *BoundIdentifier
	LeftNodeBound          bool
	UseExpandInto          bool
	LeftNodeConstraints    pgsql.Expression
	LeftNodeJoinCondition  pgsql.Expression
	Edge                   *BoundIdentifier
	EdgeConstraints        *Constraint
	EdgeJoinCondition      pgsql.Expression
	RightNode              *BoundIdentifier
	RightNodeBound         bool
	RightNodeConstraints   pgsql.Expression
	RightNodeJoinCondition pgsql.Expression
	Projection             []pgsql.SelectItem
}

// StartNode will find the root node of this pattern segment based on the segment's direction
func (s *TraversalStep) StartNode() (*BoundIdentifier, error) {
	switch s.Direction {
	case graph.DirectionInbound:
		return s.RightNode, nil
	case graph.DirectionOutbound:
		return s.LeftNode, nil
	default:
		return nil, fmt.Errorf("unsupported direction: %v", s.Direction)
	}
}

// EndNode will find the terminal node of this pattern segment based on the segment's direction
func (s *TraversalStep) EndNode() (*BoundIdentifier, error) {
	switch s.Direction {
	case graph.DirectionInbound:
		return s.LeftNode, nil
	case graph.DirectionOutbound:
		return s.RightNode, nil
	default:
		return nil, fmt.Errorf("unsupported direction: %v", s.Direction)
	}
}

func (s *TraversalStep) FlipNodes() {
	if s.Expansion != nil {
		// If the expansion is set then column identifiers must also be swapped
		s.Expansion.FlipDirection()
	}

	var (
		oldLeftNode      = s.LeftNode
		oldLeftNodeBound = s.LeftNodeBound
	)

	s.LeftNode = s.RightNode
	s.LeftNodeBound = s.RightNodeBound
	s.RightNode = oldLeftNode
	s.RightNodeBound = oldLeftNodeBound

	switch s.Direction {
	case graph.DirectionOutbound:
		s.Direction = graph.DirectionInbound
	case graph.DirectionInbound:
		s.Direction = graph.DirectionOutbound
	}

	s.PathReversed = !s.PathReversed
}

type PatternPart struct {
	IsTraversal      bool
	ShortestPath     bool
	AllShortestPaths bool
	PatternBinding   *BoundIdentifier
	Target           optimize.PatternTarget
	HasTarget        bool
	TraversalSteps   []*TraversalStep
	NodeSelect       NodeSelect
	Constraints      *ConstraintTracker
	nextSourceStep   int
}

func (s *PatternPart) nextSourceTarget() (optimize.TraversalStepTarget, bool) {
	if s == nil {
		return optimize.TraversalStepTarget{}, false
	}

	stepIndex := s.nextSourceStep
	s.nextSourceStep++

	if !s.HasTarget {
		return optimize.TraversalStepTarget{}, false
	}

	return s.Target.TraversalStep(stepIndex), true
}

func (s *PatternPart) LastStep() *TraversalStep {
	return s.TraversalSteps[len(s.TraversalSteps)-1]
}

func (s *PatternPart) ContainsExpansions() bool {
	for _, traversalStep := range s.TraversalSteps {
		if traversalStep.Expansion != nil {
			return true
		}
	}

	return false
}

type Pattern struct {
	Parts []*PatternPart
}

func (s *Pattern) Reset() {
	s.Parts = s.Parts[:0]
}

func (s *Pattern) NewPart() *PatternPart {
	newPatternPart := &PatternPart{
		Constraints: NewConstraintTracker(),
	}

	s.Parts = append(s.Parts, newPatternPart)
	return newPatternPart
}

func (s *Pattern) CurrentPart() *PatternPart {
	return s.Parts[len(s.Parts)-1]
}

type Query struct {
	Parts []*QueryPart
}

func (s *Query) HasParts() bool {
	return len(s.Parts) > 0
}

func (s *Query) AddPart(part *QueryPart) {
	s.Parts = append(s.Parts, part)
}

func (s *Query) CurrentPart() *QueryPart {
	return s.Parts[len(s.Parts)-1]
}

type QueryPart struct {
	Model     *pgsql.Query
	Frame     *Frame
	Updates   []*Mutations
	SortItems []*pgsql.OrderBy
	Skip      pgsql.Expression
	Limit     pgsql.Expression

	numReadingClauses  int
	numUpdatingClauses int

	// The fields below are meant to be used to build each component as the source AST is walked. There's some
	// repetition of some of the exported fields above which is intentional and may be a good refactor target
	// in the future
	patternPredicates               []*pgsql.Future[*Pattern]
	pathEdgeIDArrayFutures          []*pgsql.Future[*BoundIdentifier]
	properties                      TranslatedProperties
	currentPattern                  *Pattern
	stashedPattern                  *Pattern
	projections                     *Projections
	mutations                       *Mutations
	fromClauses                     []pgsql.FromClause
	limitPushdownFrames             *pgsql.IdentifierSet
	referencedIdentifiers           *pgsql.IdentifierSet
	stashedExpressionTreeTranslator *ExpressionTreeTranslator
	stashedQuantifierArray          []pgsql.Expression
	stashedQuantifierUseExists      bool
	quantifierIndex                 int
	quantifierIdentifiers           *pgsql.IdentifierSet
	unwindClauses                   []UnwindClause
	isCreating                      bool
}

type UnwindClause struct {
	Expression pgsql.Expression
	Binding    *BoundIdentifier
}

type TranslatedProperties struct {
	Map       map[string]pgsql.Expression
	Parameter pgsql.Expression
}

func NewTranslatedProperties() TranslatedProperties {
	return TranslatedProperties{
		Map: map[string]pgsql.Expression{},
	}
}

func (s TranslatedProperties) IsEmpty() bool {
	return len(s.Map) == 0 && s.Parameter == nil
}

func NewQueryPart(numReadingClauses, numUpdatingClauses int) *QueryPart {
	return &QueryPart{
		Model: &pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
		},

		numReadingClauses:     numReadingClauses,
		numUpdatingClauses:    numUpdatingClauses,
		mutations:             NewMutations(),
		properties:            NewTranslatedProperties(),
		limitPushdownFrames:   pgsql.NewIdentifierSet(),
		referencedIdentifiers: pgsql.NewIdentifierSet(),
		quantifierIdentifiers: pgsql.NewIdentifierSet(),
	}
}

func (s *QueryPart) AddFromClause(clause pgsql.FromClause) {
	s.fromClauses = append(s.fromClauses, clause)
}

func (s *QueryPart) ConsumeFromClauses() []pgsql.FromClause {
	fromClauses := s.fromClauses
	s.fromClauses = nil

	return fromClauses
}

func (s *QueryPart) AllowLimitPushdown(frameIdentifier pgsql.Identifier) {
	s.limitPushdownFrames.Add(frameIdentifier)
}

func (s *QueryPart) CanPushDownLimitTo(frameIdentifier pgsql.Identifier) bool {
	return s.limitPushdownFrames.Contains(frameIdentifier)
}

func (s *QueryPart) AddUnwindClause(clause UnwindClause) {
	s.unwindClauses = append(s.unwindClauses, clause)
}

func (s *QueryPart) ConsumeUnwindClauses() []UnwindClause {
	clauses := s.unwindClauses
	s.unwindClauses = nil
	return clauses
}

func (s *QueryPart) RestoreStashedPattern() {
	s.currentPattern = s.stashedPattern
	s.stashedPattern = nil
}

func (s *QueryPart) StashCurrentPattern() {
	s.stashedPattern = s.ConsumeCurrentPattern()
}

func (s *QueryPart) AddPatternPredicateFuture(predicateFuture *pgsql.Future[*Pattern]) {
	s.patternPredicates = append(s.patternPredicates, predicateFuture)
}

func (s *QueryPart) AddPathEdgeIDArrayFuture(pathEdgeIDArrayFuture *pgsql.Future[*BoundIdentifier]) {
	s.pathEdgeIDArrayFutures = append(s.pathEdgeIDArrayFutures, pathEdgeIDArrayFuture)
}

func (s *QueryPart) ConsumeCurrentPattern() *Pattern {
	currentPattern := s.currentPattern
	s.currentPattern = &Pattern{}

	return currentPattern
}

func (s *QueryPart) HasProjections() bool {
	return s.projections != nil && len(s.projections.Items) > 0
}

func (s *QueryPart) PrepareProjections(distinct bool) {
	s.projections = &Projections{
		Distinct: distinct,
	}
}

func (s *QueryPart) PrepareMutations() {
	if s.mutations == nil {
		s.mutations = NewMutations()
	}
}

func (s *QueryPart) HasMutations() bool {
	return s.mutations != nil && s.mutations.Updates.Len() > 0
}

func (s *QueryPart) HasDeletions() bool {
	return s.mutations != nil && s.mutations.Deletions.Len() > 0
}

func (s *QueryPart) PrepareProjection() {
	s.projections.Items = append(s.projections.Items, &Projection{})
}

func (s *QueryPart) CurrentProjection() *Projection {
	return s.projections.Current()
}

func (s *QueryPart) HasProperties() bool {
	return !s.properties.IsEmpty()
}

func (s *QueryPart) AddProperty(key string, expression pgsql.Expression) {
	if s.properties.Map == nil {
		s.properties.Map = map[string]pgsql.Expression{}
	}

	s.properties.Map[key] = expression
}

func (s *QueryPart) AddPropertyParameter(expression pgsql.Expression) {
	s.properties.Parameter = expression
}

func (s *QueryPart) ConsumeProperties() TranslatedProperties {
	properties := s.properties
	s.properties = NewTranslatedProperties()

	return properties
}

func (s *QueryPart) CurrentOrderBy() *pgsql.OrderBy {
	return s.SortItems[len(s.SortItems)-1]
}

type Projection struct {
	SelectItem pgsql.SelectItem
	Alias      models.Optional[pgsql.Identifier]
}

func (s *Projection) SetIdentifier(identifier pgsql.Identifier) {
	s.SelectItem = identifier
}

func (s *Projection) SetAlias(alias pgsql.Identifier) {
	s.Alias = models.OptionalValue(alias)
}

type Removal struct {
	Field string
}

type LabelAssignment struct {
	Kinds pgsql.Expression
}

type PropertyAssignment struct {
	Field           string
	Operator        pgsql.Operator
	ValueExpression pgsql.Expression
}

type Update struct {
	Frame               *Frame
	JoinConstraint      pgsql.Expression
	Projection          []pgsql.SelectItem
	TargetBinding       *BoundIdentifier
	UpdateBinding       *BoundIdentifier
	Removals            *graph.IndexedSlice[string, Removal]
	PropertyAssignments *graph.IndexedSlice[string, PropertyAssignment]
	KindRemovals        graph.Kinds
	KindAssignments     graph.Kinds
}

type Delete struct {
	Frame         *Frame
	TargetBinding *BoundIdentifier
	UpdateBinding *BoundIdentifier
}

type NodeCreate struct {
	Binding    *BoundIdentifier
	Properties TranslatedProperties
	Kinds      graph.Kinds
}

type EdgeCreate struct {
	Binding    *BoundIdentifier
	Properties TranslatedProperties
	Kinds      graph.Kinds
	LeftNode   *BoundIdentifier
	RightNode  *BoundIdentifier
	Direction  graph.Direction
}

type Mutations struct {
	Deletions     *graph.IndexedSlice[pgsql.Identifier, *Delete]
	Updates       *graph.IndexedSlice[pgsql.Identifier, *Update]
	Creations     *graph.IndexedSlice[pgsql.Identifier, *NodeCreate]
	EdgeCreations *graph.IndexedSlice[pgsql.Identifier, *EdgeCreate]
}

func NewMutations() *Mutations {
	return &Mutations{
		Deletions:     graph.NewIndexedSlice[pgsql.Identifier, *Delete](),
		Updates:       graph.NewIndexedSlice[pgsql.Identifier, *Update](),
		Creations:     graph.NewIndexedSlice[pgsql.Identifier, *NodeCreate](),
		EdgeCreations: graph.NewIndexedSlice[pgsql.Identifier, *EdgeCreate](),
	}
}

func (s *Mutations) AddDeletion(scope *Scope, targetIdentifier pgsql.Identifier, frame *Frame) (*Delete, error) {
	if targetBinding, bound := scope.Lookup(targetIdentifier); !bound {
		return nil, fmt.Errorf("invalid identifier: %s", targetIdentifier)
	} else if updateBinding, err := scope.DefineNew(targetBinding.DataType); err != nil {
		return nil, err
	} else {
		deletion := &Delete{
			TargetBinding: targetBinding,
			UpdateBinding: updateBinding,
			Frame:         frame,
		}

		s.Deletions.Put(targetIdentifier, deletion)
		return deletion, nil
	}
}

func (s *Mutations) newIdentifierAssignment(scope *Scope, targetBinding *BoundIdentifier) (*Update, error) {
	if updateBinding, err := scope.DefineNew(targetBinding.DataType); err != nil {
		return nil, err
	} else {
		// Create a unique scope binding for this mutation since there may be assignments that also
		// target the same identifier later in the query
		newUpdates := &Update{
			TargetBinding:       targetBinding,
			UpdateBinding:       updateBinding,
			PropertyAssignments: graph.NewIndexedSlice[string, PropertyAssignment](),
			Removals:            graph.NewIndexedSlice[string, Removal](),
		}

		s.Updates.Put(targetBinding.Identifier, newUpdates)
		return newUpdates, nil
	}
}

func (s *Mutations) getIdentifierMutation(scope *Scope, targetIdentifier pgsql.Identifier) (*Update, error) {
	if targetBinding, bound := scope.Lookup(targetIdentifier); !bound {
		return nil, fmt.Errorf("invalid identifier: %s", targetIdentifier)
	} else if existingAssignments := s.Updates.Get(targetIdentifier); existingAssignments != nil {
		return existingAssignments, nil
	} else {
		return s.newIdentifierAssignment(scope, targetBinding)
	}
}

func (s *Mutations) AddPropertyRemoval(scope *Scope, propertyLookup PropertyLookup) error {
	if mutation, err := s.getIdentifierMutation(scope, propertyLookup.Reference.Root()); err != nil {
		return err
	} else {
		mutation.Removals.Put(propertyLookup.Field, Removal{
			Field: propertyLookup.Field,
		})
	}

	return nil
}

func (s *Mutations) AddPropertyAssignment(scope *Scope, propertyLookup PropertyLookup, operator pgsql.Operator, assignmentValueExpression pgsql.Expression) error {
	if mutation, err := s.getIdentifierMutation(scope, propertyLookup.Reference.Root()); err != nil {
		return err
	} else if err := RewriteFrameBindings(scope, assignmentValueExpression); err != nil {
		return err
	} else {
		mutation.PropertyAssignments.Put(propertyLookup.Field, PropertyAssignment{
			Field:           propertyLookup.Field,
			Operator:        operator,
			ValueExpression: assignmentValueExpression,
		})
	}

	return nil
}

func (s *Mutations) AddKindAssignment(scope *Scope, targetIdentifier pgsql.Identifier, kinds graph.Kinds) error {
	if mutation, err := s.getIdentifierMutation(scope, targetIdentifier); err != nil {
		return err
	} else {
		mutation.KindAssignments = append(mutation.KindAssignments, kinds...)
	}

	return nil
}

func (s *Mutations) AddKindRemoval(scope *Scope, targetIdentifier pgsql.Identifier, kinds graph.Kinds) error {
	if mutation, err := s.getIdentifierMutation(scope, targetIdentifier); err != nil {
		return err
	} else {
		mutation.KindRemovals = append(mutation.KindRemovals, kinds...)
	}

	return nil
}

type Projections struct {
	Distinct    bool
	Frame       *Frame
	Constraints pgsql.Expression
	Items       []*Projection
	GroupBy     []pgsql.Expression
}

func (s *Projections) Add(projection *Projection) {
	s.Items = append(s.Items, projection)
}

func (s *Projections) Current() *Projection {
	return s.Items[len(s.Items)-1]
}

func extractIdentifierFromCypherExpression(expression cypher.Expression) (pgsql.Identifier, bool, error) {
	if expression == nil {
		return "", false, nil
	}

	var variableExpression *cypher.Variable

	switch typedExpression := expression.(type) {
	case *cypher.NodePattern:
		variableExpression = typedExpression.Variable

	case *cypher.RelationshipPattern:
		variableExpression = typedExpression.Variable

	case *cypher.PatternPart:
		variableExpression = typedExpression.Variable

	case *cypher.ProjectionItem:
		variableExpression = typedExpression.Alias

	case *cypher.IDInCollection:
		variableExpression = typedExpression.Variable

	case *cypher.Variable:
		variableExpression = typedExpression

	default:
		return "", false, fmt.Errorf("unable to extract variable from expression type: %T", expression)
	}

	if variableExpression == nil {
		return "", false, nil
	}

	return pgsql.Identifier(variableExpression.Symbol), true, nil
}

type FromClauseBuilder struct {
	seen        map[pgsql.Identifier]struct{}
	fromClauses []pgsql.FromClause
}

func NewFromClauseBuilder() *FromClauseBuilder {
	return &FromClauseBuilder{
		seen: make(map[pgsql.Identifier]struct{}),
	}
}

func (s *FromClauseBuilder) AddIdentifier(frameID pgsql.Identifier) {
	if frameID == "" {
		return
	}

	if _, already := s.seen[frameID]; !already {
		s.seen[frameID] = struct{}{}
		s.fromClauses = append(s.fromClauses, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{frameID},
			},
		})
	}
}

func (s *FromClauseBuilder) AddBinding(binding *BoundIdentifier) {
	if binding != nil && binding.LastProjection != nil {
		s.AddIdentifier(binding.LastProjection.Binding.Identifier)
	}
}

func (s *FromClauseBuilder) Clauses() []pgsql.FromClause {
	return s.fromClauses
}

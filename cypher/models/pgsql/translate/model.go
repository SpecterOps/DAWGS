package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
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

type expansionOptions struct {
	FindShortestPath     bool
	FindAllShortestPaths bool
	MinDepth             models.Optional[int64]
	MaxDepth             models.Optional[int64]
}

func newExpansionOptions(part *PatternPart, relationshipPattern *cypher.RelationshipPattern) expansionOptions {
	return expansionOptions{
		FindShortestPath:     part.ShortestPath,
		FindAllShortestPaths: part.AllShortestPaths,
		MinDepth:             models.OptionalPointer(relationshipPattern.Range.StartIndex),
		MaxDepth:             models.OptionalPointer(relationshipPattern.Range.EndIndex),
	}
}

type Expansion struct {
	Frame       *Frame
	PathBinding *BoundIdentifier
	Options     expansionOptions

	PrimerNodeConstraints              pgsql.Expression
	PrimerNodeSatisfactionProjection   pgsql.SelectItem
	PrimerNodeJoinCondition            pgsql.Expression
	EdgeConstraints                    pgsql.Expression
	EdgeJoinCondition                  pgsql.Expression
	RecursiveConstraints               pgsql.Expression
	ExpansionNodeJoinCondition         pgsql.Expression
	TerminalNodeConstraints            pgsql.Expression
	TerminalNodeSatisfactionProjection pgsql.SelectItem

	PrimerQueryParameter            *BoundIdentifier
	BackwardPrimerQueryParameter    *BoundIdentifier
	RecursiveQueryParameter         *BoundIdentifier
	BackwardRecursiveQueryParameter *BoundIdentifier

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

type TraversalStep struct {
	Frame                  *Frame
	Direction              graph.Direction
	Expansion              *Expansion
	LeftNode               *BoundIdentifier
	LeftNodeBound          bool
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
}

type PatternPart struct {
	IsTraversal      bool
	ShortestPath     bool
	AllShortestPaths bool
	PatternBinding   *BoundIdentifier
	TraversalSteps   []*TraversalStep
	NodeSelect       NodeSelect
	Constraints      *ConstraintTracker
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
	properties                      map[string]pgsql.Expression
	currentPattern                  *Pattern
	stashedPattern                  *Pattern
	projections                     *Projections
	mutations                       *Mutations
	fromClauses                     []pgsql.FromClause
	stashedExpressionTreeTranslator *ExpressionTreeTranslator
	stashedQuantifierArray          []pgsql.Expression
	quantifierIdentifiers           *pgsql.IdentifierSet
}

func NewQueryPart(numReadingClauses, numUpdatingClauses int) *QueryPart {
	return &QueryPart{
		Model: &pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
		},

		numReadingClauses:     numReadingClauses,
		numUpdatingClauses:    numUpdatingClauses,
		mutations:             NewMutations(),
		properties:            map[string]pgsql.Expression{},
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
	return len(s.properties) > 0
}

func (s *QueryPart) AddProperty(key string, expression pgsql.Expression) {
	s.properties[key] = expression
}

func (s *QueryPart) ConsumeProperties() map[string]pgsql.Expression {
	properties := s.properties
	s.properties = map[string]pgsql.Expression{}

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

type Mutations struct {
	Deletions *graph.IndexedSlice[pgsql.Identifier, *Delete]
	Updates   *graph.IndexedSlice[pgsql.Identifier, *Update]
}

func NewMutations() *Mutations {
	return &Mutations{
		Deletions: graph.NewIndexedSlice[pgsql.Identifier, *Delete](),
		Updates:   graph.NewIndexedSlice[pgsql.Identifier, *Update](),
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
	GroupBy     []pgsql.SelectItem
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

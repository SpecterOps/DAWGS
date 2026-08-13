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
	// expansionRootID names the recursive-state column containing the traversal's initial node ID.
	expansionRootID pgsql.Identifier = "root_id"

	// expansionNextID names the recursive-state column containing the current frontier node ID.
	expansionNextID pgsql.Identifier = "next_id"

	// expansionDepth names the recursive-state column containing the number of traversed edges.
	expansionDepth pgsql.Identifier = "depth"

	// expansionSatisfied names the recursive-state column that marks a satisfied terminal predicate.
	expansionSatisfied pgsql.Identifier = "satisfied"

	// expansionIsCycle names the recursive-state column that marks an edge-reusing path.
	expansionIsCycle pgsql.Identifier = "is_cycle"

	// expansionPath names the recursive-state column containing ordered traversed edge IDs.
	expansionPath pgsql.Identifier = "path"

	// expansionForwardFront names the current forward frontier in bidirectional search.
	expansionForwardFront pgsql.Identifier = "forward_front"

	// expansionBackwardFront names the current backward frontier in bidirectional search.
	expansionBackwardFront pgsql.Identifier = "backward_front"

	// expansionNextFront names the staging relation for the next bidirectional-search frontier.
	expansionNextFront pgsql.Identifier = "next_front"
)

// expansionColumns returns the canonical root, frontier, depth, satisfaction, cycle, and path state shape.
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

// NodeSelect groups SQL model state that must remain consistent while translating node select.
type NodeSelect struct {
	// Frame supplies the frame input to the NodeSelect contract.
	Frame *Frame
	// Binding supplies the binding input to the NodeSelect contract.
	Binding *BoundIdentifier
	// Select supplies the select input to the NodeSelect contract.
	Select pgsql.Select
	// Constraints supplies the constraints input to the NodeSelect contract.
	Constraints pgsql.Expression
}

// ExpansionOptions configures expansion.
type ExpansionOptions struct {
	// FindShortestPath identifies the filesystem find shortest path.
	FindShortestPath bool
	// FindAllShortestPaths identifies the filesystem find all shortest paths.
	FindAllShortestPaths bool
	// MinDepth supplies the min depth input to the ExpansionOptions contract.
	MinDepth models.Optional[int64]
	// MaxDepth supplies the max depth input to the ExpansionOptions contract.
	MaxDepth models.Optional[int64]
}

// newExpansionOptions derives shortest-path and depth options from a pattern part and relationship range.
func newExpansionOptions(part *PatternPart, relationshipPattern *cypher.RelationshipPattern) ExpansionOptions {
	return ExpansionOptions{
		FindShortestPath:     part.ShortestPath,
		FindAllShortestPaths: part.AllShortestPaths,
		MinDepth:             models.OptionalPointer(relationshipPattern.Range.StartIndex),
		MaxDepth:             models.OptionalPointer(relationshipPattern.Range.EndIndex),
	}
}

// Expansion contains the bindings, constraints, and execution choices for one variable-length traversal.
type Expansion struct {
	// Frame is the scope frame that materializes the expansion result.
	Frame *Frame
	// PathBinding is the optional Cypher path variable backed by recursive path state.
	PathBinding *BoundIdentifier
	// Options records shortest-path mode and traversal depth bounds.
	Options ExpansionOptions

	// PrimerNodeConstraints restricts root nodes used to seed recursive traversal.
	PrimerNodeConstraints pgsql.Expression
	// PrimerNodeSatisfactionProjection evaluates terminal satisfaction at the seed node.
	PrimerNodeSatisfactionProjection pgsql.SelectItem
	// PrimerNodeJoinCondition joins the expansion seed to its root node.
	PrimerNodeJoinCondition pgsql.Expression
	// EdgeConstraints restricts relationships admitted into the expansion.
	EdgeConstraints pgsql.Expression
	// PreviousRelationshipUniqueness rejects relationships already traversed by preceding fixed steps.
	PreviousRelationshipUniqueness pgsql.Expression
	// EdgeJoinCondition joins a relationship to the current traversal frontier.
	EdgeJoinCondition pgsql.Expression
	// RecursiveConstraints restricts recursive states independently of edge and node predicates.
	RecursiveConstraints pgsql.Expression
	// ExpansionNodeJoinCondition joins the traversed relationship to its next node.
	ExpansionNodeJoinCondition pgsql.Expression
	// TerminalNodeConstraints restricts nodes considered valid expansion terminals.
	TerminalNodeConstraints pgsql.Expression
	// TerminalNodeSatisfactionProjection computes whether a recursive state satisfies terminal predicates.
	TerminalNodeSatisfactionProjection pgsql.SelectItem
	// DeferredNodeSatisfactionConstraint retains terminal predicates that require outer bindings.
	DeferredNodeSatisfactionConstraint pgsql.Expression
	// UseMaterializedTerminalFilter enables lookup against precomputed terminal node IDs.
	UseMaterializedTerminalFilter bool
	// UseMaterializedEndpointPairFilter enables lookup against precomputed root-terminal ID pairs.
	UseMaterializedEndpointPairFilter bool
	// HasExplicitEndpointInequality reports whether the source query already excludes identical endpoints.
	HasExplicitEndpointInequality bool

	// PrimerQueryParameter identifies the harness parameter containing the forward primer query.
	PrimerQueryParameter *BoundIdentifier
	// BackwardPrimerQueryParameter identifies the harness parameter containing the backward primer query.
	BackwardPrimerQueryParameter *BoundIdentifier
	// RecursiveQueryParameter identifies the harness parameter containing the forward recursive query.
	RecursiveQueryParameter *BoundIdentifier
	// BackwardRecursiveQueryParameter identifies the harness parameter containing the backward recursive query.
	BackwardRecursiveQueryParameter *BoundIdentifier

	// UseBidirectionalSearch reports whether shortest-path traversal expands from both endpoints.
	UseBidirectionalSearch bool
	// ShortestPathExecutor selects the physical implementation for this shortest-path expansion.
	ShortestPathExecutor optimize.ShortestPathExecutor
	// ShortestPathTarget locates this expansion in the optimizer's lowering plan.
	ShortestPathTarget optimize.TraversalStepTarget
	// ShortestPathStateLimit caps distinct seen state for compact executors.
	ShortestPathStateLimit int64
	// ShortestPathFrontierLimit caps current and queued frontier state.
	ShortestPathFrontierLimit int64
	// ShortestPathPredecessorLimit caps retained witness predecessors.
	ShortestPathPredecessorLimit int64
	// ShortestPathEnumerationLimit caps staged all-shortest-path arrays.
	ShortestPathEnumerationLimit int64
	// ShortestPathOutputBytesLimit caps staged all-shortest-path array bytes.
	ShortestPathOutputBytesLimit int64
	// SingletonRootID holds the statically resolved root ID when exactly one root is known.
	SingletonRootID pgsql.Expression
	// SingletonTerminalID holds the statically resolved terminal ID when exactly one terminal is known.
	SingletonTerminalID pgsql.Expression
	// RelationshipKindIDs contains the statically resolved relationship kinds admitted by the expansion.
	RelationshipKindIDs []int16

	// EdgeStartIdentifier is the unqualified edge endpoint column from which the chosen direction advances.
	EdgeStartIdentifier pgsql.Identifier
	// EdgeStartColumn is the qualified edge endpoint expression from which the chosen direction advances.
	EdgeStartColumn pgsql.CompoundIdentifier
	// EdgeEndIdentifier is the unqualified edge endpoint column reached by the chosen direction.
	EdgeEndIdentifier pgsql.Identifier
	// EdgeEndColumn is the qualified edge endpoint expression reached by the chosen direction.
	EdgeEndColumn pgsql.CompoundIdentifier

	// Projection contains the select items exposed by the completed expansion frame.
	Projection []pgsql.SelectItem
}

// UsesSingletonEndpointPair reports whether both expansion endpoints are statically singleton IDs.
func (s *Expansion) UsesSingletonEndpointPair() bool {
	return s != nil && s.SingletonRootID != nil && s.SingletonTerminalID != nil
}

// NewExpansionModel builds the SQL model fragment responsible for new expansion model.
func NewExpansionModel(part *PatternPart, relationshipPattern *cypher.RelationshipPattern) *Expansion {
	return &Expansion{
		Options: newExpansionOptions(part, relationshipPattern),
	}
}

// CompletePattern builds the SQL model fragment responsible for complete pattern.
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

// FlipDirection builds the SQL model fragment responsible for flip direction.
func (s *Expansion) FlipDirection() {
	oldEdgeStartColumn := s.EdgeStartColumn
	s.EdgeStartColumn = s.EdgeEndColumn
	s.EdgeEndColumn = oldEdgeStartColumn
}

// CanExecuteBidirectionalSearch builds the SQL model fragment responsible for can execute bidirectional search.
func (s *Expansion) CanExecuteBidirectionalSearch() bool {
	return s.PrimerNodeConstraints != nil && s.TerminalNodeConstraints != nil
}

// CanExecuteBidirectionalSearch builds the SQL model fragment responsible for can execute bidirectional search.
func (s *TraversalStep) CanExecuteBidirectionalSearch() bool {
	if s.Expansion == nil {
		return false
	}

	return s.Expansion.CanExecuteBidirectionalSearch() ||
		(s.LeftNodeBound && s.RightNodeBound && s.Frame != nil && s.Frame.Previous != nil)
}

// hasPreviousFrameBinding reports whether the step can reference bindings materialized by a prior frame.
func (s *TraversalStep) hasPreviousFrameBinding() bool {
	return s.Frame != nil && s.Frame.Previous != nil
}

// usesBoundEndpointPairs reports whether both endpoints come from a previous frame.
func (s *TraversalStep) usesBoundEndpointPairs() bool {
	return s.LeftNodeBound && s.RightNodeBound && s.hasPreviousFrameBinding()
}

// usesBoundTerminalIDs reports whether the terminal endpoint comes from a previous frame.
func (s *TraversalStep) usesBoundTerminalIDs() bool {
	return s.RightNodeBound && s.hasPreviousFrameBinding()
}

// canMaterializeTerminalFilterForStep reports whether terminal constraints are local and useful as an independent filter.
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

// canMaterializeEndpointPairFilterForStep reports whether both local endpoint constraints restrict harness search columns.
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

// endpointSelectivity scores an endpoint expression using binding and previous-frame context.
func (s *TraversalStep) endpointSelectivity(scope *Scope, expression pgsql.Expression, bound bool) (int, error) {
	return optimize.NewSelectivityModel(scope).EndpointSelectivity(expression, bound, s.hasPreviousFrameBinding())
}

// isBidirectionalSearchAnchor reports whether a selectivity score is strong enough to seed bidirectional search.
func isBidirectionalSearchAnchor(selectivity int) bool {
	return optimize.IsBidirectionalSearchAnchor(selectivity)
}

// hasIDEqualityConstraint reports whether identifier's ID equals a row-independent value in a conjunction.
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

// hasLocalIDEqualityConstraint reports whether an ID equality depends only on identifier and static values.
func hasLocalIDEqualityConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	if !hasIDEqualityConstraint(expression, identifier) {
		return false
	}

	return hasLocalEndpointConstraint(expression, identifier)
}

// hasLocalEndpointConstraint reports whether expression references identifier without any external binding.
func hasLocalEndpointConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	if expression == nil || !referencesIdentifier(expression, identifier) {
		return false
	}

	_, externalConstraints := partitionConstraintByLocality(expression, pgsql.AsIdentifierSet(identifier))
	return externalConstraints == nil
}

// referencesIdentifier reports whether expression contains a direct, compound, or row-column reference rooted at identifier.
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

// hasPairAwareEndpointConstraint reports whether a local constraint restricts endpoint values beyond node kinds.
func hasPairAwareEndpointConstraint(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	return hasLocalEndpointConstraint(expression, identifier) &&
		referencesEndpointSearchColumn(expression, identifier)
}

// referencesEndpointSearchColumn reports whether expression reads a non-kind field used to restrict endpoint search.
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

// isStaticIDEqualityOperand reports whether expression contains no row or identifier references.
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

// isIdentifierIDReference reports whether expression is exactly identifier.id.
func isIdentifierIDReference(expression pgsql.Expression, identifier pgsql.Identifier) bool {
	compoundIdentifier, isCompoundIdentifier := unwrapParenthetical(expression).(pgsql.CompoundIdentifier)
	return isCompoundIdentifier && len(compoundIdentifier) == 2 &&
		compoundIdentifier[0] == identifier &&
		compoundIdentifier[1] == pgsql.ColumnID
}

// isSingletonIDOperand reports whether expression denotes one non-null integer ID literal or parameter.
func isSingletonIDOperand(expression pgsql.Expression) bool {
	switch typedExpression := unwrapParenthetical(expression).(type) {
	case pgsql.Literal:
		return !typedExpression.Null
	case pgsql.Parameter, *pgsql.Parameter:
		return true
	case pgsql.TypeCast:
		switch typedExpression.CastType {
		case pgsql.Int, pgsql.Int2, pgsql.Int4, pgsql.Int8:
			return isSingletonIDOperand(typedExpression.Expression)
		default:
			return false
		}
	default:
		return false
	}
}

// singletonIDAnchor returns the sole static value equated with identifier.id, rejecting ambiguous multiple equalities.
func singletonIDAnchor(expression pgsql.Expression, identifier pgsql.Identifier) (pgsql.Expression, bool) {
	var anchor pgsql.Expression

	for _, term := range flattenConjunction(expression) {
		binaryExpression, isBinaryExpression := unwrapParenthetical(term).(*pgsql.BinaryExpression)
		if !isBinaryExpression || binaryExpression.Operator != pgsql.OperatorEquals {
			continue
		}

		var candidate pgsql.Expression
		switch {
		case isIdentifierIDReference(binaryExpression.LOperand, identifier) && isSingletonIDOperand(binaryExpression.ROperand):
			candidate = binaryExpression.ROperand
		case isIdentifierIDReference(binaryExpression.ROperand, identifier) && isSingletonIDOperand(binaryExpression.LOperand):
			candidate = binaryExpression.LOperand
		default:
			continue
		}

		if anchor != nil {
			// Multiple ID equalities may be contradictory and require the generic
			// validation path until the singleton validator can retain every term.
			return nil, false
		}
		anchor = candidate
	}

	return anchor, anchor != nil
}

// replaceSingletonIDAnchor substitutes replacement for the static side of identifier's singleton ID equality.
func replaceSingletonIDAnchor(expression pgsql.Expression, identifier pgsql.Identifier, replacement pgsql.Expression) pgsql.Expression {
	switch typedExpression := expression.(type) {
	case *pgsql.Parenthetical:
		typedExpression.Expression = replaceSingletonIDAnchor(typedExpression.Expression, identifier, replacement)
		return typedExpression

	case *pgsql.BinaryExpression:
		if typedExpression.Operator == pgsql.OperatorEquals {
			switch {
			case isIdentifierIDReference(typedExpression.LOperand, identifier) && isSingletonIDOperand(typedExpression.ROperand):
				typedExpression.ROperand = replacement
				return typedExpression
			case isIdentifierIDReference(typedExpression.ROperand, identifier) && isSingletonIDOperand(typedExpression.LOperand):
				typedExpression.LOperand = replacement
				return typedExpression
			}
		}

		typedExpression.LOperand = replaceSingletonIDAnchor(typedExpression.LOperand, identifier, replacement)
		typedExpression.ROperand = replaceSingletonIDAnchor(typedExpression.ROperand, identifier, replacement)
		return typedExpression

	default:
		return expression
	}
}

// CanExecuteSelectiveBidirectionalSearch builds the SQL model fragment responsible for can execute selective bidirectional search.
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

// CanExecutePairAwareBidirectionalSearch builds the SQL model fragment responsible for can execute pair aware bidirectional search.
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

// flattenConjunction returns the independent terms of a nested PostgreSQL AND expression.
func flattenConjunction(expr pgsql.Expression) []pgsql.Expression {
	return optimize.FlattenConjunction(expr)
}

// expressionReferencesOnlyLocalIdentifiers reports whether every binding referenced by expression belongs to localScope.
func expressionReferencesOnlyLocalIdentifiers(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	return optimize.ExpressionReferencesOnlyLocalIdentifiers(expression, localScope)
}

// subqueryReferencesOnlyLocalIdentifiers reports whether a subquery has no dependencies outside localScope.
func subqueryReferencesOnlyLocalIdentifiers(subquery pgsql.Subquery, localScope *pgsql.IdentifierSet) bool {
	return optimize.SubqueryReferencesOnlyLocalIdentifiers(subquery, localScope)
}

// queryReferencesOnlyLocalIdentifiers reports whether a query has no dependencies outside localScope.
func queryReferencesOnlyLocalIdentifiers(query pgsql.Query, localScope *pgsql.IdentifierSet) bool {
	return optimize.QueryReferencesOnlyLocalIdentifiers(query, localScope)
}

// addFromClauseBindings adds every alias introduced by fromClauses to localScope.
func addFromClauseBindings(localScope *pgsql.IdentifierSet, fromClauses []pgsql.FromClause) {
	optimize.AddFromClauseBindings(localScope, fromClauses)
}

// addFromExpressionBinding adds the alias introduced by a FROM expression to localScope.
func addFromExpressionBinding(localScope *pgsql.IdentifierSet, expression pgsql.Expression) {
	optimize.AddFromExpressionBinding(localScope, expression)
}

// selectReferencesOnlyLocalIdentifiers reports whether a SELECT body has no dependencies outside localScope.
func selectReferencesOnlyLocalIdentifiers(selectBody pgsql.Select, localScope *pgsql.IdentifierSet) bool {
	return optimize.SelectReferencesOnlyLocalIdentifiers(selectBody, localScope)
}

// fromExpressionReferencesOnlyLocalIdentifiers reports whether a FROM expression has no dependencies outside localScope.
func fromExpressionReferencesOnlyLocalIdentifiers(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	return optimize.FromExpressionReferencesOnlyLocalIdentifiers(expression, localScope)
}

// isLocalToScope reports whether expression can be evaluated using only identifiers in localScope.
func isLocalToScope(expression pgsql.Expression, localScope *pgsql.IdentifierSet) bool {
	return optimize.IsLocalToScope(expression, localScope)
}

// partitionConstraintByLocality separates conjuncts evaluable in localScope from those requiring outer bindings.
func partitionConstraintByLocality(expression pgsql.Expression, localScope *pgsql.IdentifierSet) (pgsql.Expression, pgsql.Expression) {
	return optimize.PartitionConstraintByLocality(expression, localScope)
}

// ProjectionPruningApplication groups SQL model state that must remain consistent while translating projection pruning application.
type ProjectionPruningApplication struct {
	// LeftNode supplies the left node input to the ProjectionPruningApplication contract.
	LeftNode *BoundIdentifier
	// Relationship supplies the relationship input to the ProjectionPruningApplication contract.
	Relationship *BoundIdentifier
	// RightNode supplies the right node input to the ProjectionPruningApplication contract.
	RightNode *BoundIdentifier
	// PathBinding supplies the path binding input to the ProjectionPruningApplication contract.
	PathBinding *BoundIdentifier
}

// TraversalStep groups SQL model state that must remain consistent while translating traversal step.
type TraversalStep struct {
	// Frame supplies the frame input to the TraversalStep contract.
	Frame *Frame
	// SourceTarget supplies the source target input to the TraversalStep contract.
	SourceTarget optimize.TraversalStepTarget
	// HasSourceTarget indicates whether has source target applies.
	HasSourceTarget bool
	// Direction selects the traversal orientation covered by the contract.
	Direction graph.Direction
	// Expansion supplies the expansion input to the TraversalStep contract.
	Expansion *Expansion
	// PathReversed indicates whether path reversed applies.
	PathReversed bool

	// OmitPreviousFrameSource suppresses comma-joining the previous frame as a FROM source. Pattern
	// predicate roots require this so that references to the enclosing query part's frame remain
	// correlated to the outer row instead of re-scanning the outer CTE.
	OmitPreviousFrameSource bool

	// ProjectionPruning supplies the projection pruning input to the TraversalStep contract.
	ProjectionPruning ProjectionPruningApplication
	// LeftNode supplies the left node input to the TraversalStep contract.
	LeftNode *BoundIdentifier
	// LeftNodeBound indicates whether left node bound applies.
	LeftNodeBound bool
	// UseExpandInto indicates whether use expand into applies.
	UseExpandInto bool
	// LeftNodeConstraints supplies the left node constraints input to the TraversalStep contract.
	LeftNodeConstraints pgsql.Expression
	// LeftNodeJoinCondition supplies the left node join condition input to the TraversalStep contract.
	LeftNodeJoinCondition pgsql.Expression
	// Edge supplies the edge input to the TraversalStep contract.
	Edge *BoundIdentifier
	// EdgeConstraints supplies the edge constraints input to the TraversalStep contract.
	EdgeConstraints *Constraint
	// EdgeJoinCondition supplies the edge join condition input to the TraversalStep contract.
	EdgeJoinCondition pgsql.Expression
	// RightNode supplies the right node input to the TraversalStep contract.
	RightNode *BoundIdentifier
	// RightNodeBound indicates whether right node bound applies.
	RightNodeBound bool
	// RightNodeConstraints supplies the right node constraints input to the TraversalStep contract.
	RightNodeConstraints pgsql.Expression
	// RightNodeJoinCondition supplies the right node join condition input to the TraversalStep contract.
	RightNodeJoinCondition pgsql.Expression
	// Projection supplies the projection input to the TraversalStep contract.
	Projection []pgsql.SelectItem
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

// FlipNodes builds the SQL model fragment responsible for flip nodes.
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

// PatternPart groups SQL model state that must remain consistent while translating pattern part.
type PatternPart struct {
	// IsTraversal indicates whether is traversal applies.
	IsTraversal bool
	// ShortestPath identifies the filesystem shortest path.
	ShortestPath bool
	// AllShortestPaths identifies the filesystem all shortest paths.
	AllShortestPaths bool
	// PathDirectionReversed is set when the optimizer reversed the originating cypher pattern's
	// element order and relationship directions. Path materialization uses it to restore the
	// original left-to-right logical order for a bound path.
	PathDirectionReversed bool
	// PatternBinding supplies the pattern binding input to the PatternPart contract.
	PatternBinding *BoundIdentifier
	// Target supplies the target input to the PatternPart contract.
	Target optimize.PatternTarget
	// HasTarget indicates whether has target applies.
	HasTarget bool
	// TraversalSteps supplies the traversal steps input to the PatternPart contract.
	TraversalSteps []*TraversalStep
	// NodeSelect supplies the node select input to the PatternPart contract.
	NodeSelect NodeSelect
	// Constraints supplies the constraints input to the PatternPart contract.
	Constraints *ConstraintTracker
	// nextSourceStep retains the next source step while PatternPart is assembled or evaluated.
	nextSourceStep int
}

// nextSourceTarget returns the optimizer coordinates for the next traversal step and advances the step cursor.
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

// LastStep builds the SQL model fragment responsible for last step.
func (s *PatternPart) LastStep() *TraversalStep {
	return s.TraversalSteps[len(s.TraversalSteps)-1]
}

// ContainsExpansions builds the SQL model fragment responsible for contains expansions.
func (s *PatternPart) ContainsExpansions() bool {
	for _, traversalStep := range s.TraversalSteps {
		if traversalStep.Expansion != nil {
			return true
		}
	}

	return false
}

// Pattern groups SQL model state that must remain consistent while translating pattern.
type Pattern struct {
	// Parts supplies the parts input to the Pattern contract.
	Parts []*PatternPart
}

// Reset builds the SQL model fragment responsible for reset.
func (s *Pattern) Reset() {
	s.Parts = s.Parts[:0]
}

// NewPart builds the SQL model fragment responsible for new part.
func (s *Pattern) NewPart() *PatternPart {
	newPatternPart := &PatternPart{
		Constraints: NewConstraintTracker(),
	}

	s.Parts = append(s.Parts, newPatternPart)
	return newPatternPart
}

// CurrentPart builds the SQL model fragment responsible for current part.
func (s *Pattern) CurrentPart() *PatternPart {
	return s.Parts[len(s.Parts)-1]
}

// Query groups SQL model state that must remain consistent while translating query.
type Query struct {
	// Parts supplies the parts input to the Query contract.
	Parts []*QueryPart
}

// HasParts builds the SQL model fragment responsible for has parts.
func (s *Query) HasParts() bool {
	return len(s.Parts) > 0
}

// AddPart builds the SQL model fragment responsible for add part.
func (s *Query) AddPart(part *QueryPart) {
	s.Parts = append(s.Parts, part)
}

// CurrentPart builds the SQL model fragment responsible for current part.
func (s *Query) CurrentPart() *QueryPart {
	return s.Parts[len(s.Parts)-1]
}

// QueryPart groups SQL model state that must remain consistent while translating query part.
type QueryPart struct {
	// Model supplies the model input to the QueryPart contract.
	Model *pgsql.Query
	// Frame supplies the frame input to the QueryPart contract.
	Frame *Frame
	// Updates supplies the updates input to the QueryPart contract.
	Updates []*Mutations
	// SortItems supplies the sort items input to the QueryPart contract.
	SortItems []*pgsql.OrderBy
	// Skip supplies the skip input to the QueryPart contract.
	Skip pgsql.Expression
	// Limit supplies the limit input to the QueryPart contract.
	Limit pgsql.Expression

	// numReadingClauses retains the num reading clauses while QueryPart is assembled or evaluated.
	numReadingClauses int
	// numUpdatingClauses retains the num updating clauses while QueryPart is assembled or evaluated.
	numUpdatingClauses int

	// The fields below are meant to be used to build each component as the source AST is walked. There's some
	// repetition of some of the exported fields above which is intentional and may be a good refactor target
	// in the future
	patternPredicates []*pgsql.Future[*Pattern]
	// pathEdgeIDArrayFutures retains the path edge id array futures while QueryPart is assembled or evaluated.
	pathEdgeIDArrayFutures []*pgsql.Future[*BoundIdentifier]
	// properties retains the properties while QueryPart is assembled or evaluated.
	properties TranslatedProperties
	// currentPattern retains the current pattern while QueryPart is assembled or evaluated.
	currentPattern *Pattern
	// stashedPattern retains the stashed pattern while QueryPart is assembled or evaluated.
	stashedPattern *Pattern
	// projections retains the projections while QueryPart is assembled or evaluated.
	projections *Projections
	// mutations retains the mutations while QueryPart is assembled or evaluated.
	mutations *Mutations
	// fromClauses retains the from clauses while QueryPart is assembled or evaluated.
	fromClauses []pgsql.FromClause
	// limitPushdownFrames retains the limit pushdown frames while QueryPart is assembled or evaluated.
	limitPushdownFrames *pgsql.IdentifierSet
	// referencedIdentifiers retains the referenced identifiers while QueryPart is assembled or evaluated.
	referencedIdentifiers *pgsql.IdentifierSet
	// stashedExpressionTreeTranslator retains the stashed expression tree translator while QueryPart is assembled or evaluated.
	stashedExpressionTreeTranslator *ExpressionTreeTranslator
	// stashedQuantifierArray retains the stashed quantifier array while QueryPart is assembled or evaluated.
	stashedQuantifierArray []pgsql.Expression
	// stashedQuantifierUseExists indicates whether stashed quantifier use exists applies.
	stashedQuantifierUseExists bool
	// quantifierIndex retains the quantifier index while QueryPart is assembled or evaluated.
	quantifierIndex int
	// quantifierIdentifiers retains the quantifier identifiers while QueryPart is assembled or evaluated.
	quantifierIdentifiers *pgsql.IdentifierSet
	// unwindClauses retains the unwind clauses while QueryPart is assembled or evaluated.
	unwindClauses []UnwindClause
	// isCreating indicates whether is creating applies.
	isCreating bool
}

// UnwindClause groups SQL model state that must remain consistent while translating unwind clause.
type UnwindClause struct {
	// Expression supplies the expression input to the UnwindClause contract.
	Expression pgsql.Expression
	// Binding supplies the binding input to the UnwindClause contract.
	Binding *BoundIdentifier
}

// TranslatedProperties groups SQL model state that must remain consistent while translating translated properties.
type TranslatedProperties struct {
	// Map supplies the map input to the TranslatedProperties contract.
	Map map[string]pgsql.Expression
	// Parameter supplies the parameter input to the TranslatedProperties contract.
	Parameter pgsql.Expression
}

// NewTranslatedProperties builds the SQL model fragment responsible for new translated properties.
func NewTranslatedProperties() TranslatedProperties {
	return TranslatedProperties{
		Map: map[string]pgsql.Expression{},
	}
}

// IsEmpty builds the SQL model fragment responsible for is empty.
func (s TranslatedProperties) IsEmpty() bool {
	return len(s.Map) == 0 && s.Parameter == nil
}

// NewQueryPart builds the SQL model fragment responsible for new query part.
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

// AddFromClause builds the SQL model fragment responsible for add from clause.
func (s *QueryPart) AddFromClause(clause pgsql.FromClause) {
	s.fromClauses = append(s.fromClauses, clause)
}

// ConsumeFromClauses builds the SQL model fragment responsible for consume from clauses.
func (s *QueryPart) ConsumeFromClauses() []pgsql.FromClause {
	fromClauses := s.fromClauses
	s.fromClauses = nil

	return fromClauses
}

// AllowLimitPushdown builds the SQL model fragment responsible for allow limit pushdown.
func (s *QueryPart) AllowLimitPushdown(frameIdentifier pgsql.Identifier) {
	s.limitPushdownFrames.Add(frameIdentifier)
}

// CanPushDownLimitTo builds the SQL model fragment responsible for can push down limit to.
func (s *QueryPart) CanPushDownLimitTo(frameIdentifier pgsql.Identifier) bool {
	return s.limitPushdownFrames.Contains(frameIdentifier)
}

// AddUnwindClause builds the SQL model fragment responsible for add unwind clause.
func (s *QueryPart) AddUnwindClause(clause UnwindClause) {
	s.unwindClauses = append(s.unwindClauses, clause)
}

// ConsumeUnwindClauses builds the SQL model fragment responsible for consume unwind clauses.
func (s *QueryPart) ConsumeUnwindClauses() []UnwindClause {
	clauses := s.unwindClauses
	s.unwindClauses = nil
	return clauses
}

// RestoreStashedPattern builds the SQL model fragment responsible for restore stashed pattern.
func (s *QueryPart) RestoreStashedPattern() {
	s.currentPattern = s.stashedPattern
	s.stashedPattern = nil
}

// StashCurrentPattern builds the SQL model fragment responsible for stash current pattern.
func (s *QueryPart) StashCurrentPattern() {
	s.stashedPattern = s.ConsumeCurrentPattern()
}

// AddPatternPredicateFuture builds the SQL model fragment responsible for add pattern predicate future.
func (s *QueryPart) AddPatternPredicateFuture(predicateFuture *pgsql.Future[*Pattern]) {
	s.patternPredicates = append(s.patternPredicates, predicateFuture)
}

// AddPathEdgeIDArrayFuture builds the SQL model fragment responsible for add path edge id array future.
func (s *QueryPart) AddPathEdgeIDArrayFuture(pathEdgeIDArrayFuture *pgsql.Future[*BoundIdentifier]) {
	s.pathEdgeIDArrayFutures = append(s.pathEdgeIDArrayFutures, pathEdgeIDArrayFuture)
}

// ConsumeCurrentPattern builds the SQL model fragment responsible for consume current pattern.
func (s *QueryPart) ConsumeCurrentPattern() *Pattern {
	currentPattern := s.currentPattern
	s.currentPattern = &Pattern{}

	return currentPattern
}

// HasProjections builds the SQL model fragment responsible for has projections.
func (s *QueryPart) HasProjections() bool {
	return s.projections != nil && len(s.projections.Items) > 0
}

// PrepareProjections builds the SQL model fragment responsible for prepare projections.
func (s *QueryPart) PrepareProjections(distinct bool) {
	s.projections = &Projections{
		Distinct: distinct,
	}
}

// PrepareMutations builds the SQL model fragment responsible for prepare mutations.
func (s *QueryPart) PrepareMutations() {
	if s.mutations == nil {
		s.mutations = NewMutations()
	}
}

// HasMutations builds the SQL model fragment responsible for has mutations.
func (s *QueryPart) HasMutations() bool {
	return s.mutations != nil && s.mutations.Updates.Len() > 0
}

// HasDeletions builds the SQL model fragment responsible for has deletions.
func (s *QueryPart) HasDeletions() bool {
	return s.mutations != nil && s.mutations.Deletions.Len() > 0
}

// PrepareProjection constructs the SQL model used for prepare projection.
func (s *QueryPart) PrepareProjection() {
	s.projections.Items = append(s.projections.Items, &Projection{})
}

// CurrentProjection constructs the SQL model used for current projection.
func (s *QueryPart) CurrentProjection() *Projection {
	return s.projections.Current()
}

// HasProperties builds the SQL model fragment responsible for has properties.
func (s *QueryPart) HasProperties() bool {
	return !s.properties.IsEmpty()
}

// AddProperty builds the SQL model fragment responsible for add property.
func (s *QueryPart) AddProperty(key string, expression pgsql.Expression) {
	if s.properties.Map == nil {
		s.properties.Map = map[string]pgsql.Expression{}
	}

	s.properties.Map[key] = expression
}

// AddPropertyParameter builds the SQL model fragment responsible for add property parameter.
func (s *QueryPart) AddPropertyParameter(expression pgsql.Expression) {
	s.properties.Parameter = expression
}

// ConsumeProperties builds the SQL model fragment responsible for consume properties.
func (s *QueryPart) ConsumeProperties() TranslatedProperties {
	properties := s.properties
	s.properties = NewTranslatedProperties()

	return properties
}

// CurrentOrderBy builds the SQL model fragment responsible for current order by.
func (s *QueryPart) CurrentOrderBy() *pgsql.OrderBy {
	return s.SortItems[len(s.SortItems)-1]
}

// Projection groups SQL model state that must remain consistent while translating projection.
type Projection struct {
	// SelectItem supplies the select item input to the Projection contract.
	SelectItem pgsql.SelectItem
	// Alias supplies the alias input to the Projection contract.
	Alias models.Optional[pgsql.Identifier]
}

// SetIdentifier builds the SQL model fragment responsible for set identifier.
func (s *Projection) SetIdentifier(identifier pgsql.Identifier) {
	s.SelectItem = identifier
}

// SetAlias builds the SQL model fragment responsible for set alias.
func (s *Projection) SetAlias(alias pgsql.Identifier) {
	s.Alias = models.OptionalValue(alias)
}

// Removal groups SQL model state that must remain consistent while translating removal.
type Removal struct {
	// Field supplies the field input to the Removal contract.
	Field string
}

// LabelAssignment groups SQL model state that must remain consistent while translating label assignment.
type LabelAssignment struct {
	// Kinds supplies the kinds input to the LabelAssignment contract.
	Kinds pgsql.Expression
}

// PropertyAssignment groups SQL model state that must remain consistent while translating property assignment.
type PropertyAssignment struct {
	// Field supplies the field input to the PropertyAssignment contract.
	Field string
	// Operator supplies the operator input to the PropertyAssignment contract.
	Operator pgsql.Operator
	// ValueExpression supplies the value expression input to the PropertyAssignment contract.
	ValueExpression pgsql.Expression
}

// Update groups SQL model state that must remain consistent while translating update.
type Update struct {
	// Frame supplies the frame input to the Update contract.
	Frame *Frame
	// JoinConstraint supplies the join constraint input to the Update contract.
	JoinConstraint pgsql.Expression
	// Projection supplies the projection input to the Update contract.
	Projection []pgsql.SelectItem
	// TargetBinding supplies the target binding input to the Update contract.
	TargetBinding *BoundIdentifier
	// UpdateBinding supplies the update binding input to the Update contract.
	UpdateBinding *BoundIdentifier
	// Removals supplies the removals input to the Update contract.
	Removals *graph.IndexedSlice[string, Removal]
	// PropertyAssignments supplies the property assignments input to the Update contract.
	PropertyAssignments *graph.IndexedSlice[string, PropertyAssignment]
	// KindRemovals supplies the kind removals input to the Update contract.
	KindRemovals graph.Kinds
	// KindAssignments supplies the kind assignments input to the Update contract.
	KindAssignments graph.Kinds
}

// Delete groups SQL model state that must remain consistent while translating delete.
type Delete struct {
	// Frame supplies the frame input to the Delete contract.
	Frame *Frame
	// TargetBinding supplies the target binding input to the Delete contract.
	TargetBinding *BoundIdentifier
	// UpdateBinding supplies the update binding input to the Delete contract.
	UpdateBinding *BoundIdentifier
}

// NodeCreate groups SQL model state that must remain consistent while translating node create.
type NodeCreate struct {
	// Binding supplies the binding input to the NodeCreate contract.
	Binding *BoundIdentifier
	// Properties supplies the properties input to the NodeCreate contract.
	Properties TranslatedProperties
	// Kinds supplies the kinds input to the NodeCreate contract.
	Kinds graph.Kinds
}

// EdgeCreate groups SQL model state that must remain consistent while translating edge create.
type EdgeCreate struct {
	// Binding supplies the binding input to the EdgeCreate contract.
	Binding *BoundIdentifier
	// Properties supplies the properties input to the EdgeCreate contract.
	Properties TranslatedProperties
	// Kinds supplies the kinds input to the EdgeCreate contract.
	Kinds graph.Kinds
	// LeftNode supplies the left node input to the EdgeCreate contract.
	LeftNode *BoundIdentifier
	// RightNode supplies the right node input to the EdgeCreate contract.
	RightNode *BoundIdentifier
	// Direction selects the traversal orientation covered by the contract.
	Direction graph.Direction
}

// Mutations groups SQL model state that must remain consistent while translating mutations.
type Mutations struct {
	// Deletions supplies the deletions input to the Mutations contract.
	Deletions *graph.IndexedSlice[pgsql.Identifier, *Delete]
	// Updates supplies the updates input to the Mutations contract.
	Updates *graph.IndexedSlice[pgsql.Identifier, *Update]
	// Creations supplies the creations input to the Mutations contract.
	Creations *graph.IndexedSlice[pgsql.Identifier, *NodeCreate]
	// EdgeCreations supplies the edge creations input to the Mutations contract.
	EdgeCreations *graph.IndexedSlice[pgsql.Identifier, *EdgeCreate]
}

// NewMutations builds the SQL model fragment responsible for new mutations.
func NewMutations() *Mutations {
	return &Mutations{
		Deletions:     graph.NewIndexedSlice[pgsql.Identifier, *Delete](),
		Updates:       graph.NewIndexedSlice[pgsql.Identifier, *Update](),
		Creations:     graph.NewIndexedSlice[pgsql.Identifier, *NodeCreate](),
		EdgeCreations: graph.NewIndexedSlice[pgsql.Identifier, *EdgeCreate](),
	}
}

// AddDeletion builds the SQL model fragment responsible for add deletion.
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

// newIdentifierAssignment allocates a distinct update binding and empty assignment collections for targetBinding.
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

// getIdentifierMutation returns the existing update for targetIdentifier or creates its first assignment state.
func (s *Mutations) getIdentifierMutation(scope *Scope, targetIdentifier pgsql.Identifier) (*Update, error) {
	if targetBinding, bound := scope.Lookup(targetIdentifier); !bound {
		return nil, fmt.Errorf("invalid identifier: %s", targetIdentifier)
	} else if existingAssignments := s.Updates.Get(targetIdentifier); existingAssignments != nil {
		return existingAssignments, nil
	} else {
		return s.newIdentifierAssignment(scope, targetBinding)
	}
}

// AddPropertyRemoval builds the SQL model fragment responsible for add property removal.
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

// AddPropertyAssignment builds the SQL model fragment responsible for add property assignment.
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

// AddKindAssignment builds the SQL model fragment responsible for add kind assignment.
func (s *Mutations) AddKindAssignment(scope *Scope, targetIdentifier pgsql.Identifier, kinds graph.Kinds) error {
	if mutation, err := s.getIdentifierMutation(scope, targetIdentifier); err != nil {
		return err
	} else {
		mutation.KindAssignments = append(mutation.KindAssignments, kinds...)
	}

	return nil
}

// AddKindRemoval builds the SQL model fragment responsible for add kind removal.
func (s *Mutations) AddKindRemoval(scope *Scope, targetIdentifier pgsql.Identifier, kinds graph.Kinds) error {
	if mutation, err := s.getIdentifierMutation(scope, targetIdentifier); err != nil {
		return err
	} else {
		mutation.KindRemovals = append(mutation.KindRemovals, kinds...)
	}

	return nil
}

// Projections groups SQL model state that must remain consistent while translating projections.
type Projections struct {
	// Distinct indicates whether distinct applies.
	Distinct bool
	// Frame supplies the frame input to the Projections contract.
	Frame *Frame
	// Constraints supplies the constraints input to the Projections contract.
	Constraints pgsql.Expression
	// Items supplies the items input to the Projections contract.
	Items []*Projection
	// GroupBy supplies the group by input to the Projections contract.
	GroupBy []pgsql.Expression
}

// Add builds the SQL model fragment responsible for add.
func (s *Projections) Add(projection *Projection) {
	s.Items = append(s.Items, projection)
}

// Current builds the SQL model fragment responsible for current.
func (s *Projections) Current() *Projection {
	return s.Items[len(s.Items)-1]
}

// extractIdentifierFromCypherExpression returns the variable or alias directly declared by a supported Cypher expression.
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

// FromClauseBuilder groups SQL model state that must remain consistent while translating from clause builder.
type FromClauseBuilder struct {
	// seen retains the seen while FromClauseBuilder is assembled or evaluated.
	seen map[pgsql.Identifier]struct{}
	// fromClauses retains the from clauses while FromClauseBuilder is assembled or evaluated.
	fromClauses []pgsql.FromClause
}

// NewFromClauseBuilder builds the SQL model fragment responsible for new from clause builder.
func NewFromClauseBuilder() *FromClauseBuilder {
	return &FromClauseBuilder{
		seen: make(map[pgsql.Identifier]struct{}),
	}
}

// AddIdentifier builds the SQL model fragment responsible for add identifier.
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

// AddBinding builds the SQL model fragment responsible for add binding.
func (s *FromClauseBuilder) AddBinding(binding *BoundIdentifier) {
	if binding != nil && binding.LastProjection != nil {
		s.AddIdentifier(binding.LastProjection.Binding.Identifier)
	}
}

// Clauses builds the SQL model fragment responsible for clauses.
func (s *FromClauseBuilder) Clauses() []pgsql.FromClause {
	return s.fromClauses
}

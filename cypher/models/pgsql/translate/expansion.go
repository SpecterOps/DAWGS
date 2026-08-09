package translate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/specterops/dawgs/graph"
)

const translateDefaultMaxTraversalDepth int64 = 15

var (
	expansionRootFilter      = pgsql.Identifier("traversal_root_filter")
	expansionTerminalFilter  = pgsql.Identifier("traversal_terminal_filter")
	expansionPairFilter      = pgsql.Identifier("traversal_pair_filter")
	expansionTerminalID      = pgsql.Identifier("terminal_id")
	expansionVisited         = pgsql.Identifier("visited")
	expansionForwardVisited  = pgsql.Identifier("forward_visited")
	expansionBackwardVisited = pgsql.Identifier("backward_visited")
)

func expansionEdgeJoinCondition(traversalStep *TraversalStep) (pgsql.Expression, error) {
	return pgd.Equals(
		pgd.EntityID(traversalStep.LeftNode.Identifier),
		traversalStep.Expansion.EdgeStartColumn,
	), nil
}

func expansionConstraints(traversalStep *TraversalStep) pgsql.Expression {
	expansionModel := traversalStep.Expansion

	return pgd.And(
		pgd.LessThan(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionDepth),
			pgd.IntLiteral(expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth)),
		),
		pgd.Not(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionIsCycle),
		),
	)
}

var (
	ErrUnsupportedExpansionDirection = errors.New("unsupported expansion direction")
)

type ExpansionBuilder struct {
	PrimerStatement     pgsql.Select
	RecursiveStatement  pgsql.Select
	ProjectionStatement pgsql.Select
	ZeroDepthStatement  *pgsql.Select
	UseUnionAll         bool

	queryParameters map[string]any
	graphID         int32
	traversalStep   *TraversalStep
	model           *Expansion
	unwindClauses   []UnwindClause
	unwindSources   []pgsql.FromClause
}

func NewExpansionBuilder(queryParameters map[string]any, traversalStep *TraversalStep, graphID int32) (*ExpansionBuilder, error) {
	if traversalStep.Expansion == nil {
		return nil, errors.New("traversal step must have expansion set")
	}

	return &ExpansionBuilder{
		queryParameters: queryParameters,
		graphID:         graphID,
		traversalStep:   traversalStep,
		model:           traversalStep.Expansion,
	}, nil
}

func (s *ExpansionBuilder) SetUnwindClauses(clauses []UnwindClause) {
	s.unwindClauses = clauses
	s.unwindSources = unwindFromClauses(clauses)
}

func nextFrontInsert(body pgsql.SetExpression) pgsql.Insert {
	return pgsql.Insert{
		Table: pgsql.TableReference{
			Name: expansionNextFront.AsCompoundIdentifier(),
		},
		Shape: expansionColumns(),
		Source: &pgsql.Query{
			Body: body,
		},
	}
}

func expansionNodeTableReference(binding pgsql.Identifier) pgsql.TableReference {
	return pgsql.TableReference{
		Name:    pgsql.TableNode.AsCompoundIdentifier(),
		Binding: models.OptionalValue(binding),
	}
}

func expansionEdgeTableReference(binding pgsql.Identifier) pgsql.TableReference {
	return pgsql.TableReference{
		Name:    pgsql.TableEdge.AsCompoundIdentifier(),
		Binding: models.OptionalValue(binding),
	}
}

type expansionSeed struct {
	identifier pgsql.Identifier
	query      pgsql.Select
}

func expansionSeedIdentifier(expansionIdentifier pgsql.Identifier) pgsql.Identifier {
	return pgsql.Identifier(string(expansionIdentifier) + "_seed")
}

func expansionSeedColumns() *pgsql.RecordShape {
	return pgsql.NewRecordShape([]pgsql.Identifier{
		expansionRootID,
	})
}

func newExpansionSeed(identifier pgsql.Identifier, rootExpression pgsql.Expression, from []pgsql.FromClause, where pgsql.Expression) expansionSeed {
	return expansionSeed{
		identifier: identifier,
		query: pgsql.Select{
			Projection: []pgsql.SelectItem{
				pgsql.AliasedExpression{
					Expression: rootExpression,
					Alias:      models.OptionalValue(expansionRootID),
				},
			},
			From:  from,
			Where: where,
		},
	}
}

func newExpansionNodeSeed(identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	return newExpansionSeed(identifier, pgd.EntityID(nodeIdentifier), []pgsql.FromClause{{
		Source: expansionNodeTableReference(nodeIdentifier),
	}}, constraints)
}

func newExpansionNodeFilterSeed(identifier, filterIdentifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	var (
		filterAlias = pgsql.Identifier(string(identifier) + "_filter")
		filterID    = pgsql.CompoundIdentifier{filterAlias, pgsql.ColumnID}
	)

	if constraints == nil {
		return newExpansionSeed(identifier, filterID, []pgsql.FromClause{{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{filterIdentifier},
				Binding: models.OptionalValue(filterAlias),
			},
		}}, nil)
	}

	seed := newExpansionSeed(identifier, pgd.EntityID(nodeIdentifier), []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{filterIdentifier},
			Binding: models.OptionalValue(filterAlias),
		},
		Joins: []pgsql.Join{{
			Table: expansionNodeTableReference(nodeIdentifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgd.Equals(
					pgd.EntityID(nodeIdentifier),
					filterID,
				),
			},
		}},
	}}, constraints)
	seed.query.Distinct = true

	return seed
}

func newExpansionBoundNodeSeed(identifier pgsql.Identifier, previousFrame *Frame, binding *BoundIdentifier, constraints pgsql.Expression) expansionSeed {
	seed := newExpansionSeed(identifier, boundEndpointIDReference(previousFrame, binding), []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
		},
	}}, constraints)

	seed.query.Distinct = true
	return seed
}

func fromClausesContainSource(fromClauses []pgsql.FromClause, identifier pgsql.Identifier) bool {
	for _, fromClause := range fromClauses {
		if tableReference, isTableReference := fromClause.Source.(pgsql.TableReference); isTableReference &&
			len(tableReference.Name) == 1 &&
			tableReference.Name[0] == identifier {
			return true
		}
	}

	return false
}

func prependFrameSourceIfMissing(fromClauses []pgsql.FromClause, frame *Frame) []pgsql.FromClause {
	if frame == nil || fromClausesContainSource(fromClauses, frame.Binding.Identifier) {
		return fromClauses
	}

	return append([]pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{frame.Binding.Identifier},
		},
	}}, fromClauses...)
}

func expressionReferencesUnwindBinding(expression pgsql.Expression, unwindClauses []UnwindClause) (bool, error) {
	if expression == nil || len(unwindClauses) == 0 {
		return false, nil
	}

	references, err := ExtractSyntaxNodeReferences(expression)
	if err != nil {
		return false, err
	}

	for _, clause := range unwindClauses {
		if clause.Binding != nil && references.Contains(clause.Binding.Identifier) {
			return true, nil
		}
	}

	return false, nil
}

func (s *ExpansionBuilder) seedEndpointConstraintSplit(expression pgsql.Expression, nodeIdentifier pgsql.Identifier, previousFrameIdentifier pgsql.Identifier) (pgsql.Expression, pgsql.Expression) {
	var (
		seedExpression = rewriteBoundEndpointSeedReference(expression, previousFrameIdentifier, nodeIdentifier)
		localScope     = pgsql.AsIdentifierSet(nodeIdentifier)
	)

	for _, clause := range s.unwindClauses {
		if clause.Binding != nil {
			localScope.Add(clause.Binding.Identifier)
		}
	}

	return partitionConstraintByLocality(seedExpression, localScope)
}

func (s *ExpansionBuilder) appendUnwindSourcesIfReferenced(selectBody *pgsql.Select, expressions ...pgsql.Expression) error {
	for _, expression := range expressions {
		if referencesUnwind, err := expressionReferencesUnwindBinding(expression, s.unwindClauses); err != nil {
			return err
		} else if referencesUnwind {
			var previousFrame *Frame
			if s.traversalStep != nil && s.traversalStep.Frame != nil {
				previousFrame = s.traversalStep.Frame.Previous
			}

			selectBody.From = prependFrameSourceIfMissing(selectBody.From, previousFrame)
			selectBody.From = append(selectBody.From, s.unwindSources...)
			return nil
		}
	}

	return nil
}

func (s *ExpansionBuilder) appendUnwindSources(selectBody *pgsql.Select) {
	selectBody.From = append(selectBody.From, s.unwindSources...)
}

func newExpansionRootIDsParameterSeed(identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	return newExpansionNodeFilterSeed(identifier, expansionRootFilter, nodeIdentifier, constraints)
}

func newExpansionTerminalIDsParameterSeed(identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	return newExpansionNodeFilterSeed(identifier, expansionTerminalFilter, nodeIdentifier, constraints)
}

func newExpansionArrayParameterSeed(identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression, parameterPosition int) expansionSeed {
	parameterAlias := pgsql.Identifier(string(identifier) + "_parameter")
	parameterID := pgsql.CompoundIdentifier{parameterAlias, pgsql.ColumnID}
	seed := newExpansionSeed(identifier, pgd.EntityID(nodeIdentifier), []pgsql.FromClause{{
		Source: pgsql.FormattingLiteral(fmt.Sprintf(
			"unnest($%d::int8[]) as %s(id)",
			parameterPosition,
			parameterAlias,
		)),
		Joins: []pgsql.Join{{
			Table: expansionNodeTableReference(nodeIdentifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgd.Equals(
					pgd.EntityID(nodeIdentifier),
					parameterID,
				),
			},
		}},
	}}, constraints)
	seed.query.Distinct = true
	return seed
}

func (s expansionSeed) CTE() pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  s.identifier,
			Shape: expansionSeedColumns(),
		},
		Materialized: &pgsql.Materialized{Materialized: false},
		Query: pgsql.Query{
			Body: s.query,
		},
	}
}

func (s expansionSeed) rootID() pgsql.CompoundIdentifier {
	return pgsql.CompoundIdentifier{s.identifier, expansionRootID}
}

func (s expansionSeed) fromClause(joins ...pgsql.Join) pgsql.FromClause {
	return pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{s.identifier},
		},
		Joins: joins,
	}
}

func (s expansionSeed) edgeJoin(edgeIdentifier pgsql.Identifier, edgeStartColumn pgsql.CompoundIdentifier) pgsql.Join {
	return pgsql.Join{
		Table: expansionEdgeTableReference(edgeIdentifier),
		JoinOperator: pgsql.JoinOperator{
			JoinType:   pgsql.JoinTypeInner,
			Constraint: pgd.Equals(edgeStartColumn, s.rootID()),
		},
	}
}

func expansionEdgeFromClause(edgeIdentifier pgsql.Identifier, joins ...pgsql.Join) pgsql.FromClause {
	return pgsql.FromClause{
		Source: expansionEdgeTableReference(edgeIdentifier),
		Joins:  joins,
	}
}

func recursiveExpansionEdgeProjection(edgeIdentifier pgsql.Identifier) pgsql.Projection {
	projection := make(pgsql.Projection, len(pgsql.EdgeTableColumns))

	for idx, column := range pgsql.EdgeTableColumns {
		projection[idx] = pgsql.CompoundIdentifier{edgeIdentifier, column}
	}

	return projection
}

func expansionEdgeNotInPath(edgeIdentifier, frameIdentifier pgsql.Identifier) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(
		pgd.EntityID(edgeIdentifier),
		pgsql.OperatorNotEquals,
		pgsql.NewAllExpression(
			pgsql.CompoundIdentifier{frameIdentifier, expansionPath},
		),
	)
}

func recursiveExpansionEdgeLookupJoin(traversalStep *TraversalStep) pgsql.Join {
	var (
		expansionModel = traversalStep.Expansion
		edgeIdentifier = traversalStep.Edge.Identifier
		edgeLookup     = pgsql.Select{
			Projection: recursiveExpansionEdgeProjection(edgeIdentifier),
			From: []pgsql.FromClause{{
				Source: expansionEdgeTableReference(edgeIdentifier),
			}},
			Where: pgsql.OptionalAnd(
				pgsql.OptionalAnd(
					pgd.Equals(
						expansionModel.EdgeStartColumn,
						pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
					),
					expansionEdgeNotInPath(edgeIdentifier, expansionModel.Frame.Binding.Identifier),
				),
				expansionModel.EdgeConstraints,
			),
		}
	)

	return pgsql.Join{
		Table: pgsql.LateralSubquery{
			Query: pgsql.Query{
				Body: edgeLookup,
				// OFFSET 0 keeps PostgreSQL from flattening this correlated lookup into a merge over the full edge index.
				Offset: pgsql.NewLiteral(0, pgsql.Int),
			},
			Binding: models.OptionalValue(edgeIdentifier),
		},
		JoinOperator: pgsql.JoinOperator{
			JoinType:   pgsql.JoinTypeInner,
			Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
		},
	}
}

func expansionNodeProjection(binding *BoundIdentifier) pgsql.Projection {
	if binding.IDOnly {
		return pgsql.Projection{pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnID}}
	}

	projection := make(pgsql.Projection, len(pgsql.NodeTableColumns))

	for idx, column := range pgsql.NodeTableColumns {
		projection[idx] = pgsql.CompoundIdentifier{binding.Identifier, column}
	}

	return projection
}

func expansionNodeLookupJoin(binding *BoundIdentifier, nodeID pgsql.Expression) pgsql.Join {
	nodeLookup := pgsql.Select{
		Projection: expansionNodeProjection(binding),
		From: []pgsql.FromClause{{
			Source: expansionNodeTableReference(binding.Identifier),
		}},
		Where: pgd.Equals(
			pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnID},
			nodeID,
		),
	}

	return pgsql.Join{
		Table: pgsql.LateralSubquery{
			Query: pgsql.Query{
				Body: nodeLookup,
				// OFFSET 0 keeps PostgreSQL from flattening this correlated lookup into a full-table hash join.
				Offset: pgsql.NewLiteral(0, pgsql.Int),
			},
			Binding: models.OptionalValue(binding.Identifier),
		},
		JoinOperator: pgsql.JoinOperator{
			JoinType:   pgsql.JoinTypeInner,
			Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
		},
	}
}

// rewriteBoundEndpointSeedReference converts references to a bound endpoint in
// the previous frame into references that are local to the seed query. Anything
// that still references outside scope is left for seedEndpointConstraintSplit to
// keep out of the seed.
func rewriteBoundEndpointSeedReference(expression pgsql.Expression, previousFrameIdentifier, nodeIdentifier pgsql.Identifier) pgsql.Expression {
	if expression == nil {
		return nil
	}

	switch typedExpression := expression.(type) {
	case pgsql.CompoundIdentifier:
		if previousFrameIdentifier != "" && len(typedExpression) == 2 && typedExpression[0] == previousFrameIdentifier && typedExpression[1] == nodeIdentifier {
			return nodeIdentifier
		}

		return expression

	case pgsql.RowColumnReference:
		if previousFrameIdentifier != "" {
			if compound, ok := typedExpression.Identifier.(pgsql.CompoundIdentifier); ok &&
				len(compound) == 2 &&
				compound[0] == previousFrameIdentifier &&
				compound[1] == nodeIdentifier {
				return pgsql.CompoundIdentifier{nodeIdentifier, typedExpression.Column}
			}
		}

		return pgsql.RowColumnReference{
			Identifier: rewriteBoundEndpointSeedReference(typedExpression.Identifier, previousFrameIdentifier, nodeIdentifier),
			Column:     typedExpression.Column,
		}

	case pgsql.TypeCast:
		return pgsql.TypeCast{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
			CastType:   typedExpression.CastType,
		}

	case pgsql.Variadic:
		return pgsql.Variadic{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
		}

	case pgsql.CompositeValue:
		values := make([]pgsql.Expression, len(typedExpression.Values))
		for idx, value := range typedExpression.Values {
			values[idx] = rewriteBoundEndpointSeedReference(value, previousFrameIdentifier, nodeIdentifier)
		}

		return pgsql.CompositeValue{
			Values:   values,
			DataType: typedExpression.DataType,
		}

	case pgsql.FunctionCall:
		parameters := make([]pgsql.Expression, len(typedExpression.Parameters))
		for idx, parameter := range typedExpression.Parameters {
			parameters[idx] = rewriteBoundEndpointSeedReference(parameter, previousFrameIdentifier, nodeIdentifier)
		}

		return pgsql.FunctionCall{
			Bare:       typedExpression.Bare,
			Distinct:   typedExpression.Distinct,
			Function:   typedExpression.Function,
			Parameters: parameters,
			OrderBy:    typedExpression.OrderBy,
			Over:       typedExpression.Over,
			CastType:   typedExpression.CastType,
		}

	case *pgsql.FunctionCall:
		if typedExpression == nil {
			return nil
		}

		rewritten := rewriteBoundEndpointSeedReference(*typedExpression, previousFrameIdentifier, nodeIdentifier).(pgsql.FunctionCall)
		return &rewritten

	case pgsql.ArrayExpression:
		return pgsql.ArrayExpression{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
		}

	case pgsql.ArrayLiteral:
		values := make([]pgsql.Expression, len(typedExpression.Values))
		for idx, value := range typedExpression.Values {
			values[idx] = rewriteBoundEndpointSeedReference(value, previousFrameIdentifier, nodeIdentifier)
		}

		return pgsql.ArrayLiteral{
			Values:   values,
			CastType: typedExpression.CastType,
		}

	case pgsql.ArrayIndex:
		indexes := make([]pgsql.Expression, len(typedExpression.Indexes))
		for idx, index := range typedExpression.Indexes {
			indexes[idx] = rewriteBoundEndpointSeedReference(index, previousFrameIdentifier, nodeIdentifier)
		}

		return pgsql.ArrayIndex{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
			Indexes:    indexes,
			CastType:   typedExpression.CastType,
		}

	case *pgsql.ArrayIndex:
		if typedExpression == nil {
			return nil
		}

		rewritten := rewriteBoundEndpointSeedReference(*typedExpression, previousFrameIdentifier, nodeIdentifier).(pgsql.ArrayIndex)
		return &rewritten

	case pgsql.ArraySlice:
		var lower, upper pgsql.Expression
		if typedExpression.Lower != nil {
			lower = rewriteBoundEndpointSeedReference(typedExpression.Lower, previousFrameIdentifier, nodeIdentifier)
		}
		if typedExpression.Upper != nil {
			upper = rewriteBoundEndpointSeedReference(typedExpression.Upper, previousFrameIdentifier, nodeIdentifier)
		}

		return pgsql.ArraySlice{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
			Lower:      lower,
			Upper:      upper,
			CastType:   typedExpression.CastType,
		}

	case *pgsql.ArraySlice:
		if typedExpression == nil {
			return nil
		}

		rewritten := rewriteBoundEndpointSeedReference(*typedExpression, previousFrameIdentifier, nodeIdentifier).(pgsql.ArraySlice)
		return &rewritten

	case pgsql.AnyExpression:
		return pgsql.AnyExpression{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
			CastType:   typedExpression.CastType,
		}

	case *pgsql.AnyExpression:
		if typedExpression == nil {
			return nil
		}

		rewritten := rewriteBoundEndpointSeedReference(*typedExpression, previousFrameIdentifier, nodeIdentifier).(pgsql.AnyExpression)
		return &rewritten

	case pgsql.AllExpression:
		return pgsql.NewAllExpression(rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier))

	case pgsql.UnaryExpression:
		return pgsql.UnaryExpression{
			Operator: typedExpression.Operator,
			Operand:  rewriteBoundEndpointSeedReference(typedExpression.Operand, previousFrameIdentifier, nodeIdentifier),
		}

	case *pgsql.UnaryExpression:
		if typedExpression == nil {
			return nil
		}

		rewritten := rewriteBoundEndpointSeedReference(*typedExpression, previousFrameIdentifier, nodeIdentifier).(pgsql.UnaryExpression)
		return &rewritten

	case pgsql.BinaryExpression:
		return pgsql.BinaryExpression{
			Operator: typedExpression.Operator,
			LOperand: rewriteBoundEndpointSeedReference(typedExpression.LOperand, previousFrameIdentifier, nodeIdentifier),
			ROperand: rewriteBoundEndpointSeedReference(typedExpression.ROperand, previousFrameIdentifier, nodeIdentifier),
		}

	case *pgsql.BinaryExpression:
		if typedExpression == nil {
			return nil
		}

		return &pgsql.BinaryExpression{
			Operator: typedExpression.Operator,
			LOperand: rewriteBoundEndpointSeedReference(typedExpression.LOperand, previousFrameIdentifier, nodeIdentifier),
			ROperand: rewriteBoundEndpointSeedReference(typedExpression.ROperand, previousFrameIdentifier, nodeIdentifier),
		}

	case *pgsql.Parenthetical:
		if typedExpression == nil {
			return nil
		}

		return &pgsql.Parenthetical{
			Expression: rewriteBoundEndpointSeedReference(typedExpression.Expression, previousFrameIdentifier, nodeIdentifier),
		}

	default:
		return expression
	}
}

func seededFrontPrimerQuery(seed expansionSeed, primer pgsql.Select) pgsql.Query {
	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Expressions: []pgsql.CommonTableExpression{seed.CTE()},
		},
		Body: primer,
	}
}

func frontPrimerQuery(seed *expansionSeed, primer pgsql.Select) pgsql.Query {
	if seed == nil {
		return pgsql.Query{Body: primer}
	}

	return seededFrontPrimerQuery(*seed, primer)
}

func expansionAllowsZeroDepth(expansionModel *Expansion) bool {
	return expansionModel.Options.MinDepth.Set && expansionModel.Options.MinDepth.Value == 0
}

func zeroDepthNodeJoin(nodeIdentifier pgsql.Identifier, nodeID pgsql.Expression) pgsql.Join {
	return pgsql.Join{
		Table: expansionNodeTableReference(nodeIdentifier),
		JoinOperator: pgsql.JoinOperator{
			JoinType:   pgsql.JoinTypeInner,
			Constraint: pgd.Equals(pgd.EntityID(nodeIdentifier), nodeID),
		},
	}
}

func zeroDepthTerminalSatisfaction(traversalStep *TraversalStep) pgsql.Expression {
	localSatisfaction, _ := expansionTerminalSatisfactionLocality(traversalStep)
	if localSatisfaction == nil {
		return pgsql.NewLiteral(true, pgsql.Boolean)
	}

	// Depth 0 has no relationship row in scope, so edge-dependent terminal
	// satisfaction can only be met by a later recursive step.
	if referencesIdentifier(localSatisfaction, traversalStep.Edge.Identifier) {
		return pgsql.NewLiteral(false, pgsql.Boolean)
	}

	return localSatisfaction
}

func (s *ExpansionBuilder) buildZeroDepthExpansionSelect(seed *expansionSeed) (pgsql.Select, error) {
	var (
		expansionModel      = s.traversalStep.Expansion
		rootIDExpression    pgsql.Expression
		fromClause          pgsql.FromClause
		satisfiedExpression pgsql.Expression = pgsql.NewLiteral(false, pgsql.Boolean)
	)

	if seed != nil {
		rootIDExpression = seed.rootID()
		fromClause = seed.fromClause()
	} else {
		rootIDExpression = pgd.EntityID(s.traversalStep.LeftNode.Identifier)
		fromClause = pgsql.FromClause{
			Source: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
		}
	}

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		satisfiedExpression = zeroDepthTerminalSatisfaction(s.traversalStep)

		if seed != nil && referencesIdentifier(satisfiedExpression, s.traversalStep.LeftNode.Identifier) {
			fromClause.Joins = append(fromClause.Joins, zeroDepthNodeJoin(s.traversalStep.LeftNode.Identifier, rootIDExpression))
		}

		if s.traversalStep.RightNode.Identifier != s.traversalStep.LeftNode.Identifier &&
			referencesIdentifier(satisfiedExpression, s.traversalStep.RightNode.Identifier) {
			fromClause.Joins = append(fromClause.Joins, zeroDepthNodeJoin(s.traversalStep.RightNode.Identifier, rootIDExpression))
		}
	}

	satisfiedSelectItem, err := pgsql.As[pgsql.SelectItem](satisfiedExpression)
	if err != nil {
		return pgsql.Select{}, err
	}

	rootIDSelectItem, err := pgsql.As[pgsql.SelectItem](rootIDExpression)
	if err != nil {
		return pgsql.Select{}, err
	}

	return pgsql.Select{
		Projection: []pgsql.SelectItem{
			rootIDSelectItem,
			rootIDSelectItem,
			pgsql.NewLiteral(0, pgsql.Int),
			satisfiedSelectItem,
			pgsql.NewLiteral(false, pgsql.Boolean),
			pgsql.ArrayLiteral{CastType: pgsql.Int8Array},
		},
		From: []pgsql.FromClause{fromClause},
	}, nil
}

func (s *ExpansionBuilder) usesBoundRootIDs() bool {
	return s.traversalStep.LeftNodeBound && s.traversalStep.Frame != nil && s.traversalStep.Frame.Previous != nil
}

func (s *ExpansionBuilder) usesBoundTerminalIDs() bool {
	return s.traversalStep.RightNodeBound && s.traversalStep.Frame != nil && s.traversalStep.Frame.Previous != nil
}

func (s *ExpansionBuilder) usesBoundEndpointPairs() bool {
	return s.usesBoundRootIDs() && s.usesBoundTerminalIDs()
}

func (s *ExpansionBuilder) boundNodeIDsFilterStatement(filterIdentifier pgsql.Identifier, nodeIdentifier pgsql.Identifier) pgsql.Insert {
	var (
		previousFrameIdentifier = s.traversalStep.Frame.Previous.Binding.Identifier
		nodeIDExpression        = pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{previousFrameIdentifier, nodeIdentifier},
			Column:     pgsql.ColumnID,
		}
	)

	return pgsql.Insert{
		Table: pgsql.TableReference{
			Name: filterIdentifier.AsCompoundIdentifier(),
		},
		Shape: pgsql.NewRecordShape([]pgsql.Identifier{pgsql.ColumnID}),
		Source: &pgsql.Query{
			Body: pgsql.Select{
				Distinct: true,
				Projection: []pgsql.SelectItem{
					nodeIDExpression,
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{previousFrameIdentifier},
					},
				}},
				Where: pgsql.NewBinaryExpression(
					nodeIDExpression,
					pgsql.OperatorIsNot,
					pgsql.NullLiteral(),
				),
			},
		},
	}
}

func (s *ExpansionBuilder) boundRootIDsFilterStatement() (pgsql.Insert, bool) {
	if !s.usesBoundRootIDs() {
		return pgsql.Insert{}, false
	}

	return s.boundNodeIDsFilterStatement(expansionRootFilter, s.traversalStep.LeftNode.Identifier), true
}

func (s *ExpansionBuilder) boundTerminalIDsFilterStatement() (pgsql.Insert, bool) {
	if !s.usesBoundTerminalIDs() {
		return pgsql.Insert{}, false
	}

	return s.boundNodeIDsFilterStatement(expansionTerminalFilter, s.traversalStep.RightNode.Identifier), true
}

func (s *ExpansionBuilder) unboundTerminalIDsFilterStatement() (pgsql.Insert, bool) {
	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UseMaterializedTerminalFilter {
		return pgsql.Insert{}, false
	}

	return s.nodeIDsFilterStatement(expansionTerminalFilter, s.traversalStep.RightNode.Identifier, expansionModel.TerminalNodeConstraints), true
}

func (s *ExpansionBuilder) nodeIDsFilterStatement(filterIdentifier pgsql.Identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) pgsql.Insert {
	nodeIDExpression := pgsql.CompoundIdentifier{nodeIdentifier, pgsql.ColumnID}

	return pgsql.Insert{
		Table: pgsql.TableReference{
			Name: filterIdentifier.AsCompoundIdentifier(),
		},
		Shape: pgsql.NewRecordShape([]pgsql.Identifier{pgsql.ColumnID}),
		Source: &pgsql.Query{
			Body: pgsql.Select{
				Distinct: true,
				Projection: []pgsql.SelectItem{
					nodeIDExpression,
				},
				From: []pgsql.FromClause{{
					Source: expansionNodeTableReference(nodeIdentifier),
				}},
				Where: pgsql.OptionalAnd(
					constraints,
					pgsql.NewBinaryExpression(
						nodeIDExpression,
						pgsql.OperatorIsNot,
						pgsql.NullLiteral(),
					),
				),
			},
		},
	}
}

func (s *ExpansionBuilder) boundEndpointPairFilterStatement() (pgsql.Insert, bool) {
	if !s.usesBoundEndpointPairs() {
		return pgsql.Insert{}, false
	}

	var (
		previousFrameIdentifier = s.traversalStep.Frame.Previous.Binding.Identifier
		rootIDExpression        = pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{previousFrameIdentifier, s.traversalStep.LeftNode.Identifier},
			Column:     pgsql.ColumnID,
		}
		terminalIDExpression = pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{previousFrameIdentifier, s.traversalStep.RightNode.Identifier},
			Column:     pgsql.ColumnID,
		}
	)

	return pgsql.Insert{
		Table: pgsql.TableReference{
			Name: expansionPairFilter.AsCompoundIdentifier(),
		},
		Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionRootID, expansionTerminalID}),
		Source: &pgsql.Query{
			Body: pgsql.Select{
				Distinct: true,
				Projection: []pgsql.SelectItem{
					rootIDExpression,
					terminalIDExpression,
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{previousFrameIdentifier},
					},
				}},
				Where: pgd.And(
					pgsql.NewBinaryExpression(
						rootIDExpression,
						pgsql.OperatorIsNot,
						pgsql.NullLiteral(),
					),
					pgsql.NewBinaryExpression(
						terminalIDExpression,
						pgsql.OperatorIsNot,
						pgsql.NullLiteral(),
					),
				),
			},
		},
	}, true
}

func (s *ExpansionBuilder) materializedEndpointPairFilterStatement() (pgsql.Insert, bool) {
	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UseMaterializedEndpointPairFilter {
		return pgsql.Insert{}, false
	}

	var (
		rootIDExpression     = pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}
		terminalIDExpression = pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}
		pairConstraints      = pgsql.OptionalAnd(expansionModel.PrimerNodeConstraints, expansionModel.TerminalNodeConstraints)
	)

	pairConstraints = pgsql.OptionalAnd(pairConstraints, pgsql.NewBinaryExpression(
		rootIDExpression,
		pgsql.OperatorIsNot,
		pgsql.NullLiteral(),
	))
	pairConstraints = pgsql.OptionalAnd(pairConstraints, pgsql.NewBinaryExpression(
		terminalIDExpression,
		pgsql.OperatorIsNot,
		pgsql.NullLiteral(),
	))

	return pgsql.Insert{
		Table: pgsql.TableReference{
			Name: expansionPairFilter.AsCompoundIdentifier(),
		},
		Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionRootID, expansionTerminalID}),
		Source: &pgsql.Query{
			Body: pgsql.Select{
				Distinct: true,
				Projection: []pgsql.SelectItem{
					rootIDExpression,
					terminalIDExpression,
				},
				From: []pgsql.FromClause{{
					Source: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
				}, {
					Source: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
				}},
				Where: pairConstraints,
			},
		},
	}, true
}

func boundTerminalFilterSatisfaction(expansionModel *Expansion) pgsql.Expression {
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgd.IntLiteral(1),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: pgsql.CompoundIdentifier{expansionTerminalFilter},
						},
					}},
					Where: pgd.Equals(
						pgsql.CompoundIdentifier{expansionTerminalFilter, pgsql.ColumnID},
						expansionModel.EdgeEndColumn,
					),
				},
			},
		},
		Negated: false,
	}
}

func boundTerminalPairFilterSatisfaction(rootIDExpression pgsql.Expression, terminalIDExpression pgsql.Expression) pgsql.Expression {
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgd.IntLiteral(1),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: pgsql.CompoundIdentifier{expansionPairFilter},
						},
					}},
					Where: pgd.And(
						pgd.Equals(
							pgsql.CompoundIdentifier{expansionPairFilter, expansionRootID},
							rootIDExpression,
						),
						pgd.Equals(
							pgsql.CompoundIdentifier{expansionPairFilter, expansionTerminalID},
							terminalIDExpression,
						),
					),
				},
			},
		},
		Negated: false,
	}
}

func boundRootFilterSatisfaction(expansionModel *Expansion) pgsql.Expression {
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgd.IntLiteral(1),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: pgsql.CompoundIdentifier{expansionRootFilter},
						},
					}},
					Where: pgd.Equals(
						pgsql.CompoundIdentifier{expansionRootFilter, pgsql.ColumnID},
						expansionModel.EdgeStartColumn,
					),
				},
			},
		},
		Negated: false,
	}
}

func shortestPathVisitedPruningCondition(visitedTable pgsql.Identifier, rootIDExpression pgsql.Expression, nextIDExpression pgsql.Expression) pgsql.Expression {
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgd.IntLiteral(1),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: pgsql.CompoundIdentifier{visitedTable},
						},
					}},
					Where: pgd.And(
						pgd.Equals(
							pgsql.CompoundIdentifier{visitedTable, expansionRootID},
							rootIDExpression,
						),
						pgd.Equals(
							pgsql.CompoundIdentifier{visitedTable, pgsql.ColumnID},
							nextIDExpression,
						),
					),
				},
			},
		},
		Negated: true,
	}
}

func forwardContinuationSatisfaction(expansionModel *Expansion) pgsql.Expression {
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgd.IntLiteral(1),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: pgsql.TableEdge.AsCompoundIdentifier(),
						},
					}},
					Where: pgd.Equals(
						expansionModel.EdgeEndIdentifier,
						expansionModel.EdgeStartColumn,
					),
				},
			},
		},
		Negated: false,
	}
}

func (s *ExpansionBuilder) forwardTerminalSatisfaction(expansionModel *Expansion, rootIDExpression pgsql.Expression) pgsql.SelectItem {
	var satisfied pgsql.Expression

	if s.usesBoundEndpointPairs() || expansionModel.UseMaterializedEndpointPairFilter {
		satisfied = boundTerminalPairFilterSatisfaction(rootIDExpression, expansionModel.EdgeEndColumn)
	} else if s.usesBoundTerminalIDs() || expansionModel.UseMaterializedTerminalFilter {
		satisfied = boundTerminalFilterSatisfaction(expansionModel)
	}

	if expansionModel.TerminalNodeSatisfactionProjection != nil &&
		!expansionModel.UseMaterializedTerminalFilter &&
		!expansionModel.UseMaterializedEndpointPairFilter {
		satisfied = pgsql.OptionalAnd(satisfied, expansionModel.TerminalNodeSatisfactionProjection)
	} else if satisfied == nil {
		satisfied = forwardContinuationSatisfaction(expansionModel)
	}

	satisfiedSelectItem, _ := pgsql.As[pgsql.SelectItem](satisfied)
	return satisfiedSelectItem
}

func forwardTerminalSatisfactionProjection(expansionModel *Expansion) pgsql.Expression {
	if expansionModel.TerminalNodeSatisfactionProjection != nil &&
		!expansionModel.UseMaterializedTerminalFilter &&
		!expansionModel.UseMaterializedEndpointPairFilter {
		return pgsql.Expression(expansionModel.TerminalNodeSatisfactionProjection)
	}

	return nil
}

func backwardContinuationSatisfaction(expansionModel *Expansion) pgsql.Expression {
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgd.IntLiteral(1),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: pgsql.TableEdge.AsCompoundIdentifier(),
						},
					}},
					Where: pgd.Equals(
						expansionModel.EdgeStartIdentifier,
						expansionModel.EdgeEndColumn,
					),
				},
			},
		},
		Negated: false,
	}
}

func (s *ExpansionBuilder) backwardTerminalSatisfaction(expansionModel *Expansion, terminalIDExpression pgsql.Expression) pgsql.SelectItem {
	var satisfied pgsql.Expression

	if s.usesBoundEndpointPairs() || expansionModel.UseMaterializedEndpointPairFilter {
		satisfied = boundTerminalPairFilterSatisfaction(expansionModel.EdgeStartColumn, terminalIDExpression)
	} else if s.usesBoundRootIDs() {
		satisfied = boundRootFilterSatisfaction(expansionModel)
	}

	if expansionModel.PrimerNodeSatisfactionProjection != nil && !expansionModel.UseMaterializedEndpointPairFilter {
		satisfied = pgsql.OptionalAnd(satisfied, expansionModel.PrimerNodeSatisfactionProjection)
	} else if satisfied == nil {
		satisfied = backwardContinuationSatisfaction(expansionModel)
	}

	satisfiedSelectItem, _ := pgsql.As[pgsql.SelectItem](satisfied)
	return satisfiedSelectItem
}

func backwardTerminalSatisfactionProjection(expansionModel *Expansion) pgsql.Expression {
	if expansionModel.PrimerNodeSatisfactionProjection != nil && !expansionModel.UseMaterializedEndpointPairFilter {
		return pgsql.Expression(expansionModel.PrimerNodeSatisfactionProjection)
	}

	return nil
}

func (s *ExpansionBuilder) prepareForwardFrontPrimerQuery(expansionModel *Expansion) (pgsql.Query, pgsql.Expression, error) {
	var (
		primerSeedConstraints     pgsql.Expression
		primerProjectionPredicate pgsql.Expression
		previousFrameIdentifier   pgsql.Identifier
		seed                      *expansionSeed
		nextQuery                 = pgsql.Select{
			Where: expansionModel.EdgeConstraints,
		}
	)

	if s.traversalStep.LeftNodeBound && s.traversalStep.Frame != nil && s.traversalStep.Frame.Previous != nil {
		previousFrameIdentifier = s.traversalStep.Frame.Previous.Binding.Identifier
	}

	primerSeedConstraints, primerProjectionPredicate = s.seedEndpointConstraintSplit(
		expansionModel.PrimerNodeConstraints,
		s.traversalStep.LeftNode.Identifier,
		previousFrameIdentifier,
	)

	if expansionModel.UsesSingletonEndpointPair() {
		rootIDsSeed := newExpansionArrayParameterSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			s.traversalStep.LeftNode.Identifier,
			primerSeedConstraints,
			1,
		)
		seed = &rootIDsSeed
	} else if s.usesBoundRootIDs() {
		rootIDsSeed := newExpansionRootIDsParameterSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			s.traversalStep.LeftNode.Identifier,
			primerSeedConstraints,
		)
		seed = &rootIDsSeed
	} else if primerSeedConstraints != nil {
		nodeSeed := newExpansionNodeSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			s.traversalStep.LeftNode.Identifier,
			primerSeedConstraints,
		)
		seed = &nodeSeed
	}

	if seed != nil {
		if err := s.appendUnwindSourcesIfReferenced(&seed.query, primerSeedConstraints); err != nil {
			return pgsql.Query{}, nil, err
		}
	}

	// The returned projection predicate is the part of the endpoint predicate
	// that cannot be evaluated in the seed CTE because it still references an
	// outer frame.
	nextQuery.Projection = []pgsql.SelectItem{
		s.model.EdgeStartColumn,
		s.model.EdgeEndColumn,
		pgd.IntLiteral(1),
	}

	nextQuery.Projection = append(nextQuery.Projection, s.forwardTerminalSatisfaction(expansionModel, expansionModel.EdgeStartColumn))

	nextQuery.Projection = append(nextQuery.Projection,
		pgd.Equals(
			pgd.StartID(s.traversalStep.Edge.Identifier),
			pgd.EndID(s.traversalStep.Edge.Identifier),
		),
		pgd.ExpressionArrayLiteral(
			pgd.EntityID(s.traversalStep.Edge.Identifier),
		),
	)

	var nextQueryFrom pgsql.FromClause

	if seed != nil {
		nextQueryFrom = seed.fromClause(seed.edgeJoin(s.traversalStep.Edge.Identifier, expansionModel.EdgeStartColumn))
	} else {
		nextQueryFrom = expansionEdgeFromClause(s.traversalStep.Edge.Identifier)
	}

	if expansionModel.TerminalNodeConstraints != nil &&
		!expansionModel.UseMaterializedTerminalFilter &&
		!expansionModel.UseMaterializedEndpointPairFilter {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	if err := s.appendUnwindSourcesIfReferenced(&nextQuery, expansionModel.EdgeConstraints, forwardTerminalSatisfactionProjection(expansionModel)); err != nil {
		return pgsql.Query{}, nil, err
	}

	if !expansionModel.HasExplicitEndpointInequality && !expansionModel.UsesSingletonEndpointPair() {
		nextQuery.Where = pgsql.OptionalAnd(
			nextQuery.Where,
			shortestPathSeedSelfEndpointGuard(s.model.EdgeStartColumn, expansionModel.UseMaterializedEndpointPairFilter),
		)
	}

	return frontPrimerQuery(seed, nextQuery), primerProjectionPredicate, nil
}

func (s *ExpansionBuilder) prepareForwardFrontRecursiveQuery(expansionModel *Expansion) (pgsql.Select, error) {
	nextQuery := pgsql.Select{
		Where: expansionModel.EdgeConstraints,
	}

	nextQuery.Projection = []pgsql.SelectItem{
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
		s.model.EdgeEndColumn,
		pgd.Add(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionDepth),
			pgd.IntLiteral(1)),
	}

	nextQuery.Projection = append(nextQuery.Projection, s.forwardTerminalSatisfaction(expansionModel, pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID)))

	nextQuery.Projection = append(nextQuery.Projection, pgsql.NewLiteral(false, pgsql.Boolean))

	pathProjection := pgd.Concatenate(
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath),
		pgd.EntityID(s.traversalStep.Edge.Identifier),
	)
	if s.traversalStep.PathReversed && !expansionModel.UseBidirectionalSearch {
		pathProjection = pgd.Concatenate(
			pgd.EntityID(s.traversalStep.Edge.Identifier),
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath),
		)
	}

	nextQuery.Projection = append(nextQuery.Projection, pathProjection)

	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionForwardFront},
			Binding: models.OptionalValue(expansionModel.Frame.Binding.Identifier),
		},

		Joins: []pgsql.Join{{
			Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					s.model.EdgeStartColumn,
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}

	if expansionModel.TerminalNodeConstraints != nil &&
		!expansionModel.UseMaterializedTerminalFilter &&
		!expansionModel.UseMaterializedEndpointPairFilter {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	nextQuery.Where = pgsql.OptionalAnd(nextQuery.Where, expansionEdgeNotInPath(
		s.traversalStep.Edge.Identifier,
		expansionModel.Frame.Binding.Identifier,
	))

	if expansionModel.Options.FindShortestPath {
		visitedTable := expansionVisited
		if expansionModel.UseBidirectionalSearch {
			visitedTable = expansionForwardVisited
		}

		nextQuery.Where = pgsql.OptionalAnd(nextQuery.Where, shortestPathVisitedPruningCondition(
			visitedTable,
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
			s.model.EdgeEndColumn,
		))
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	if err := s.appendUnwindSourcesIfReferenced(&nextQuery, expansionModel.EdgeConstraints, forwardTerminalSatisfactionProjection(expansionModel)); err != nil {
		return pgsql.Select{}, err
	}

	return nextQuery, nil
}

func (s *ExpansionBuilder) prepareBackwardFrontPrimerQuery(expansionModel *Expansion) (pgsql.Query, pgsql.Expression, error) {
	var (
		terminalSeedConstraints     pgsql.Expression
		terminalProjectionPredicate pgsql.Expression
		previousFrameIdentifier     pgsql.Identifier
		seed                        *expansionSeed
		nextQuery                   = pgsql.Select{
			Where: expansionModel.EdgeConstraints,
		}
	)

	if s.traversalStep.RightNodeBound && s.traversalStep.Frame != nil && s.traversalStep.Frame.Previous != nil {
		previousFrameIdentifier = s.traversalStep.Frame.Previous.Binding.Identifier
	}

	terminalSeedConstraints, terminalProjectionPredicate = s.seedEndpointConstraintSplit(
		expansionModel.TerminalNodeConstraints,
		s.traversalStep.RightNode.Identifier,
		previousFrameIdentifier,
	)

	if expansionModel.UsesSingletonEndpointPair() {
		terminalIDsSeed := newExpansionArrayParameterSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			s.traversalStep.RightNode.Identifier,
			terminalSeedConstraints,
			2,
		)
		seed = &terminalIDsSeed
	} else if s.usesBoundTerminalIDs() {
		terminalIDsSeed := newExpansionTerminalIDsParameterSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			s.traversalStep.RightNode.Identifier,
			terminalSeedConstraints,
		)
		seed = &terminalIDsSeed
	} else if terminalSeedConstraints != nil {
		nodeSeed := newExpansionNodeSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			s.traversalStep.RightNode.Identifier,
			terminalSeedConstraints,
		)
		seed = &nodeSeed
	}

	if seed != nil {
		if err := s.appendUnwindSourcesIfReferenced(&seed.query, terminalSeedConstraints); err != nil {
			return pgsql.Query{}, nil, err
		}
	}

	// The returned projection predicate is applied after the harness materializes
	// endpoints, where any outer-frame references are back in scope.
	nextQuery.Projection = []pgsql.SelectItem{
		s.model.EdgeEndColumn,
		s.model.EdgeStartColumn,
		pgd.IntLiteral(1),
	}

	nextQuery.Projection = append(nextQuery.Projection, s.backwardTerminalSatisfaction(expansionModel, expansionModel.EdgeEndColumn))

	nextQuery.Projection = append(nextQuery.Projection,
		pgd.Equals(
			pgd.StartID(s.traversalStep.Edge.Identifier),
			pgd.EndID(s.traversalStep.Edge.Identifier),
		),
		pgd.ExpressionArrayLiteral(
			pgd.EntityID(s.traversalStep.Edge.Identifier),
		),
	)

	var nextQueryFrom pgsql.FromClause

	if seed != nil {
		nextQueryFrom = seed.fromClause(seed.edgeJoin(s.traversalStep.Edge.Identifier, expansionModel.EdgeEndColumn))
	} else {
		nextQueryFrom = expansionEdgeFromClause(s.traversalStep.Edge.Identifier)
	}

	if expansionModel.PrimerNodeConstraints != nil && !expansionModel.UseMaterializedEndpointPairFilter {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.PrimerNodeJoinCondition,
			},
		})
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	if err := s.appendUnwindSourcesIfReferenced(&nextQuery, expansionModel.EdgeConstraints, backwardTerminalSatisfactionProjection(expansionModel)); err != nil {
		return pgsql.Query{}, nil, err
	}

	return frontPrimerQuery(seed, nextQuery), terminalProjectionPredicate, nil
}

func (s *ExpansionBuilder) prepareBackwardFrontRecursiveQuery(expansionModel *Expansion) (pgsql.Select, error) {
	nextQuery := pgsql.Select{
		Where: expansionModel.EdgeConstraints,
	}

	nextQuery.Projection = []pgsql.SelectItem{
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
		s.model.EdgeStartColumn,
		pgd.Add(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionDepth),
			pgd.IntLiteral(1)),
	}

	nextQuery.Projection = append(nextQuery.Projection, s.backwardTerminalSatisfaction(expansionModel, pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID)))

	nextQuery.Projection = append(nextQuery.Projection, pgsql.NewLiteral(false, pgsql.Boolean))

	nextQuery.Projection = append(nextQuery.Projection, pgd.Concatenate(
		pgd.EntityID(s.traversalStep.Edge.Identifier),
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath),
	))

	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionBackwardFront},
			Binding: models.OptionalValue(expansionModel.Frame.Binding.Identifier),
		},

		Joins: []pgsql.Join{{
			Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					s.model.EdgeEndColumn,
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}

	if expansionModel.PrimerNodeConstraints != nil && !expansionModel.UseMaterializedEndpointPairFilter {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.PrimerNodeJoinCondition,
			},
		})
	}

	nextQuery.Where = pgsql.OptionalAnd(nextQuery.Where, expansionEdgeNotInPath(
		s.traversalStep.Edge.Identifier,
		expansionModel.Frame.Binding.Identifier,
	))

	if expansionModel.Options.FindShortestPath {
		nextQuery.Where = pgsql.OptionalAnd(nextQuery.Where, shortestPathVisitedPruningCondition(
			expansionBackwardVisited,
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
			s.model.EdgeStartColumn,
		))
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	if err := s.appendUnwindSourcesIfReferenced(&nextQuery, expansionModel.EdgeConstraints, backwardTerminalSatisfactionProjection(expansionModel)); err != nil {
		return pgsql.Select{}, err
	}

	return nextQuery, nil
}

func shortestPathSearchCTE(functionName pgsql.Identifier, expansionModel *Expansion, harnessParameters []pgsql.Expression) pgsql.CommonTableExpression {
	return shortestPathSearchCTEFrom(functionName, expansionModel, harnessParameters, "singleton_endpoints", expansionModel.Frame.Binding.Identifier)
}

func shortestPathSearchCTEFrom(functionName pgsql.Identifier, expansionModel *Expansion, harnessParameters []pgsql.Expression, validatedEndpoints, searchAlias pgsql.Identifier) pgsql.CommonTableExpression {

	if expansionModel.UsesSingletonEndpointPair() {
		harnessParameters = append([]pgsql.Expression(nil), harnessParameters...)
		rootArrayIndex := len(harnessParameters) - 3
		terminalArrayIndex := len(harnessParameters) - 2
		harnessParameters[rootArrayIndex] = pgsql.ArrayLiteral{
			Values: []pgsql.Expression{
				pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
			},
			CastType: pgsql.Int8Array,
		}
		harnessParameters[terminalArrayIndex] = pgsql.ArrayLiteral{
			Values: []pgsql.Expression{
				pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
			},
			CastType: pgsql.Int8Array,
		}
	}

	var (
		innerQuery = pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.Wildcard{},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.FunctionCall{
						Function:   functionName,
						Parameters: harnessParameters,
					},
				}},
			},
		}
	)
	if expansionModel.UsesSingletonEndpointPair() {
		selectBody := innerQuery.Body.(pgsql.Select)
		selectBody.Projection = []pgsql.SelectItem{
			pgsql.CompoundIdentifier{functionName, pgsql.WildcardIdentifier},
		}
		selectBody.From = append([]pgsql.FromClause{{
			Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()},
		}}, selectBody.From...)
		innerQuery.Body = selectBody
	}

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  searchAlias,
			Shape: expansionColumns(),
		},
		Query: innerQuery,
	}
}

func singletonEndpointValidationCTE(traversalStep *TraversalStep, expansionModel *Expansion) pgsql.CommonTableExpression {
	const validatedEndpoints pgsql.Identifier = "singleton_endpoints"

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: validatedEndpoints},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: []pgsql.SelectItem{
				&pgsql.AliasedExpression{
					Expression: pgd.EntityID(traversalStep.LeftNode.Identifier),
					Alias:      models.OptionalValue(expansionRootID),
				},
				&pgsql.AliasedExpression{
					Expression: pgd.EntityID(traversalStep.RightNode.Identifier),
					Alias:      models.OptionalValue(expansionTerminalID),
				},
			},
			From: []pgsql.FromClause{
				{Source: expansionNodeTableReference(traversalStep.LeftNode.Identifier)},
				{Source: expansionNodeTableReference(traversalStep.RightNode.Identifier)},
			},
			Where: pgsql.OptionalAnd(expansionModel.PrimerNodeConstraints, expansionModel.TerminalNodeConstraints),
		}},
	}
}

func boundEndpointProjectionConstraint(prevFrameID pgsql.Identifier, binding *BoundIdentifier, expansionFrameID, expansionColumn pgsql.Identifier) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		projectedNodeIDReference(prevFrameID, binding),
		pgsql.OperatorEquals,
		pgsql.CompoundIdentifier{expansionFrameID, expansionColumn},
	)
}

func (s *ExpansionBuilder) applyBoundEndpointProjectionConstraints(projectionQuery *pgsql.Select, expansionModel *Expansion) {
	if s.traversalStep.Frame == nil || s.traversalStep.Frame.Previous == nil {
		return
	}

	if !s.traversalStep.LeftNodeBound && !s.traversalStep.RightNodeBound {
		return
	}

	prevFrameID := s.traversalStep.Frame.Previous.Binding.Identifier

	ensureProjectionFrameSource(projectionQuery, prevFrameID)

	if s.traversalStep.LeftNodeBound {
		projectionQuery.Where = pgsql.OptionalAnd(projectionQuery.Where,
			boundEndpointProjectionConstraint(
				prevFrameID,
				s.traversalStep.LeftNode,
				expansionModel.Frame.Binding.Identifier,
				expansionRootID,
			),
		)
	}

	if s.traversalStep.RightNodeBound {
		projectionQuery.Where = pgsql.OptionalAnd(projectionQuery.Where,
			boundEndpointProjectionConstraint(
				prevFrameID,
				s.traversalStep.RightNode,
				expansionModel.Frame.Binding.Identifier,
				expansionNextID,
			),
		)
	}
}

func ensureProjectionFrameSource(projectionQuery *pgsql.Select, frameIdentifier pgsql.Identifier) {
	for _, from := range projectionQuery.From {
		if tableReference, ok := from.Source.(pgsql.TableReference); ok && len(tableReference.Name) == 1 && tableReference.Name[0] == frameIdentifier {
			return
		}
	}

	projectionQuery.From = append([]pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{frameIdentifier},
		},
	}}, projectionQuery.From...)
}

func (s *ExpansionBuilder) applyShortestPathSeedProjectionConstraints(projectionQuery *pgsql.Select, projectionConstraints pgsql.Expression) {
	if projectionConstraints == nil {
		return
	}

	if s.traversalStep.Frame != nil && s.traversalStep.Frame.Previous != nil {
		prevFrameID := s.traversalStep.Frame.Previous.Binding.Identifier
		if referencesIdentifier(projectionConstraints, prevFrameID) {
			ensureProjectionFrameSource(projectionQuery, prevFrameID)
		}
	}

	projectionQuery.Where = pgsql.OptionalAnd(projectionQuery.Where, projectionConstraints)
}

// Match Neo4j's shortest-path behavior by surfacing an error for result rows
// where the resolved root and terminal endpoints are the same node.
func shortestPathSelfEndpointGuard(expansionFrame pgsql.Identifier) pgsql.Expression {
	var (
		rootID     = pgsql.CompoundIdentifier{expansionFrame, expansionRootID}
		terminalID = pgsql.CompoundIdentifier{expansionFrame, expansionNextID}
	)

	return shortestPathSelfEndpointGuardCase(rootID, terminalID)
}

func shortestPathSelfEndpointGuardCase(rootID, terminalID pgsql.Expression) pgsql.Expression {
	return shortestPathSelfEndpointConditionGuard(
		pgsql.NewBinaryExpression(rootID, pgsql.OperatorNotEquals, terminalID),
		rootID,
		terminalID,
	)
}

func shortestPathSelfEndpointConditionGuard(condition pgsql.Expression, rootID, terminalID pgsql.Expression) pgsql.Expression {
	return &pgsql.Case{
		Conditions: []pgsql.Expression{
			condition,
		},
		Then: []pgsql.Expression{
			pgsql.NewLiteral(true, pgsql.Boolean),
		},
		Else: pgsql.FunctionCall{
			Function: pgsql.FunctionShortestPathSelfEndpointError,
			Parameters: []pgsql.Expression{
				rootID,
				terminalID,
			},
		},
	}
}

// PostgreSQL has no portable expression-level RAISE. Keep the normal path
// visible in generated SQL and call the schema helper only for the error path.
func shortestPathTerminalFilterSelfEndpointGuard(rootID pgsql.Expression) pgsql.Expression {
	matchingTerminalCount := pgsql.Subquery{
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.FunctionCall{
						Function: pgsql.FunctionCount,
						Parameters: []pgsql.Expression{
							pgsql.Wildcard{},
						},
						CastType: pgsql.Int8,
					},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{expansionTerminalFilter},
					},
				}},
				Where: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{expansionTerminalFilter, pgsql.ColumnID},
					pgsql.OperatorEquals,
					rootID,
				),
			},
		},
	}

	return &pgsql.Case{
		Conditions: []pgsql.Expression{
			pgsql.NewBinaryExpression(
				matchingTerminalCount,
				pgsql.OperatorEquals,
				pgsql.NewLiteral(0, pgsql.Int8),
			),
		},
		Then: []pgsql.Expression{
			pgsql.NewLiteral(true, pgsql.Boolean),
		},
		Else: pgsql.FunctionCall{
			Function: pgsql.FunctionShortestPathSelfEndpointError,
			Parameters: []pgsql.Expression{
				rootID,
				rootID,
			},
		},
	}
}

func shortestPathEndpointPairFilterSelfEndpointGuard(rootID pgsql.Expression) pgsql.Expression {
	matchingEndpointPairCount := pgsql.Subquery{
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.FunctionCall{
						Function: pgsql.FunctionCount,
						Parameters: []pgsql.Expression{
							pgsql.Wildcard{},
						},
						CastType: pgsql.Int8,
					},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{expansionPairFilter},
					},
				}},
				Where: pgsql.OptionalAnd(
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{expansionPairFilter, expansionRootID},
						pgsql.OperatorEquals,
						rootID,
					),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{expansionPairFilter, expansionTerminalID},
						pgsql.OperatorEquals,
						rootID,
					),
				),
			},
		},
	}

	return shortestPathSelfEndpointConditionGuard(
		pgsql.NewBinaryExpression(
			matchingEndpointPairCount,
			pgsql.OperatorEquals,
			pgsql.NewLiteral(0, pgsql.Int8),
		),
		rootID,
		rootID,
	)
}

func shortestPathSeedSelfEndpointGuard(rootID pgsql.Expression, useEndpointPairFilter bool) pgsql.Expression {
	if useEndpointPairFilter {
		return shortestPathEndpointPairFilterSelfEndpointGuard(rootID)
	}

	return shortestPathTerminalFilterSelfEndpointGuard(rootID)
}

func (s *ExpansionBuilder) applyShortestPathSelfEndpointGuard(projectionQuery *pgsql.Select, expansionModel *Expansion) {
	if expansionModel.HasExplicitEndpointInequality || expansionAllowsZeroDepth(expansionModel) {
		return
	}

	projectionQuery.Where = pgsql.OptionalAnd(
		projectionQuery.Where,
		shortestPathSelfEndpointGuard(expansionModel.Frame.Binding.Identifier),
	)
}

func (s *ExpansionBuilder) buildShortestPathsHarnessCall(harnessFunctionName pgsql.Identifier) (pgsql.Query, error) {
	var (
		expansionModel  = s.traversalStep.Expansion
		projectionQuery pgsql.Select
	)

	expansionModel.UseMaterializedTerminalFilter = s.canMaterializeTerminalFilter(expansionModel)

	forwardFrontPrimerQuery, forwardSeedProjectionConstraints, err := s.prepareForwardFrontPrimerQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	forwardFrontRecursiveQuery, err := s.prepareForwardFrontRecursiveQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	projectionQuery.Projection = expansionModel.Projection

	// Select the expansion components for the projection statement
	projectionQuery.From = []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{{
			Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				),
			},
		}, {
			Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}}

	s.applyBoundEndpointProjectionConstraints(&projectionQuery, expansionModel)
	s.applyShortestPathSeedProjectionConstraints(&projectionQuery, forwardSeedProjectionConstraints)
	s.appendUnwindSources(&projectionQuery)
	s.applyShortestPathSelfEndpointGuard(&projectionQuery, expansionModel)

	if harnessParameters, err := s.shortestPathsParameters(expansionModel, forwardFrontPrimerQuery, forwardFrontRecursiveQuery); err != nil {
		return pgsql.Query{}, err
	} else {
		query := pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
			Body:                   projectionQuery,
		}

		if expansionModel.UsesSingletonEndpointPair() {
			query.AddCTE(singletonEndpointValidationCTE(s.traversalStep, expansionModel))
		}
		query.AddCTE(shortestPathSearchCTE(harnessFunctionName, expansionModel, harnessParameters))
		return query, nil
	}
}

func (s *ExpansionBuilder) BuildShortestPathsRoot() (pgsql.Query, error) {
	return s.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalSPHarness)
}

func shortestDistanceColumns(idOnly bool) *pgsql.RecordShape {
	if idOnly {
		return pgsql.NewRecordShape([]pgsql.Identifier{expansionNextID, expansionDepth})
	}
	return pgsql.NewRecordShape([]pgsql.Identifier{expansionRootID, expansionNextID, expansionDepth})
}

func shortestDistanceEndpointID(validatedEndpoints, endpointID pgsql.Identifier) pgsql.Subquery {
	return pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.CompoundIdentifier{validatedEndpoints, endpointID}},
		From:       []pgsql.FromClause{{Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()}}},
	}}}
}

func shortestDistanceIDProjection(projection pgsql.Projection, traversalStep *TraversalStep, stateID, validatedEndpoints pgsql.Identifier) pgsql.Projection {
	result := append(pgsql.Projection(nil), projection...)
	for idx, item := range result {
		aliased, ok := item.(*pgsql.AliasedExpression)
		if !ok {
			continue
		}
		identifier, ok := aliased.Expression.(pgsql.CompoundIdentifier)
		if !ok || len(identifier) != 2 || identifier[1] != pgsql.ColumnID {
			continue
		}
		var replacement pgsql.Expression
		switch identifier[0] {
		case traversalStep.LeftNode.Identifier:
			replacement = shortestDistanceEndpointID(validatedEndpoints, expansionRootID)
		case traversalStep.RightNode.Identifier:
			replacement = pgsql.CompoundIdentifier{stateID, expansionNextID}
		default:
			continue
		}
		copy := *aliased
		copy.Expression = replacement
		result[idx] = &copy
	}
	return result
}

// BuildShortestDistanceRoot emits the bounded, distance-only SP-S3-U-D
// recursive search. ID-only endpoint projections use only next ID and depth;
// other projections retain the constant root ID. Neither shape contains path,
// predecessor, visited-edge, cycle, or materialization columns.
func (s *ExpansionBuilder) BuildShortestDistanceRoot() (pgsql.Query, error) {
	const validatedEndpoints pgsql.Identifier = "singleton_endpoints"

	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UsesSingletonEndpointPair() {
		return pgsql.Query{}, errors.New("SP-S3-U-D requires one validated endpoint pair")
	}
	if !expansionModel.Options.MaxDepth.Set {
		return pgsql.Query{}, errors.New("SP-S3-U-D requires a bounded maximum depth")
	}

	endpointCTE := singletonEndpointValidationCTE(s.traversalStep, expansionModel)
	if expansionModel.Options.MinDepth.GetOr(1) > 0 {
		endpointSelect := endpointCTE.Query.Body.(pgsql.Select)
		endpointSelect.Where = pgsql.OptionalAnd(endpointSelect.Where, shortestPathSelfEndpointGuardCase(
			pgd.EntityID(s.traversalStep.LeftNode.Identifier),
			pgd.EntityID(s.traversalStep.RightNode.Identifier),
		))
		endpointCTE.Query.Body = endpointSelect
	}

	stateID := expansionModel.Frame.Binding.Identifier
	idOnly := s.traversalStep.LeftNode.IDOnly && s.traversalStep.RightNode.IDOnly
	anchorProjection := pgsql.Projection{
		pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
		pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
		pgsql.NewLiteral(int64(0), pgsql.Int8),
	}
	if idOnly {
		anchorProjection = pgsql.Projection{
			pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
			pgsql.NewLiteral(int64(0), pgsql.Int8),
		}
	}
	anchor := pgsql.Select{
		Projection: anchorProjection,
		From:       []pgsql.FromClause{{Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()}}},
	}

	recursiveProjection := pgsql.Projection{
		pgsql.CompoundIdentifier{stateID, expansionRootID},
		expansionModel.EdgeEndColumn,
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{stateID, expansionDepth},
			pgsql.OperatorAdd,
			pgsql.NewLiteral(int64(1), pgsql.Int8),
		),
	}
	if idOnly {
		recursiveProjection = recursiveProjection[1:]
	}
	recursive := pgsql.Select{
		Projection: recursiveProjection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						expansionModel.EdgeStartColumn,
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{stateID, expansionNextID},
					),
				},
			}},
		}},
		Where: pgsql.OptionalAnd(
			expansionModel.EdgeConstraints,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{stateID, expansionDepth},
				pgsql.OperatorLessThan,
				pgsql.NewLiteral(expansionModel.Options.MaxDepth.Value, pgsql.Int8),
			),
		),
	}

	projectionItems := pgsql.Projection(expansionModel.Projection)
	var endpointConstraint pgsql.Expression
	joins := []pgsql.Join{{
		Table: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()},
		JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{stateID, expansionRootID}, pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
			),
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{stateID, expansionNextID}, pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
			),
		)},
	}}
	if idOnly {
		projectionItems = shortestDistanceIDProjection(projectionItems, s.traversalStep, stateID, validatedEndpoints)
		joins = nil
		endpointConstraint = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{stateID, expansionNextID},
			pgsql.OperatorEquals,
			shortestDistanceEndpointID(validatedEndpoints, expansionTerminalID),
		)
	} else {
		joins = append(joins,
			pgsql.Join{
				Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
				JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{stateID, expansionRootID},
				)},
			},
			pgsql.Join{
				Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
				JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{stateID, expansionNextID},
				)},
			},
		)
	}

	projection := pgsql.Select{
		Projection: projectionItems,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins:  joins,
		}},
		Where: pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{stateID, expansionDepth},
				pgsql.OperatorGreaterThanOrEqualTo,
				pgsql.NewLiteral(expansionModel.Options.MinDepth.GetOr(1), pgsql.Int8),
			),
			endpointConstraint,
		),
	}

	query := pgsql.Query{
		CommonTableExpressions: &pgsql.With{Recursive: true},
		Body:                   projection,
		OrderBy: []*pgsql.OrderBy{{
			Expression: pgsql.CompoundIdentifier{stateID, expansionDepth},
			Ascending:  true,
		}},
		Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
	}
	query.AddCTE(endpointCTE)
	query.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: stateID, Shape: shortestDistanceColumns(idOnly)},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: anchor,
			ROperand: recursive,
			Operator: pgsql.OperatorUnion,
		}},
	})

	return query, nil
}

func shortestPathNodeComposite(identifier pgsql.Identifier) pgsql.CompositeValue {
	value := pgsql.CompositeValue{DataType: pgsql.NodeComposite}
	for _, column := range pgsql.NodeTableColumns {
		value.Values = append(value.Values, pgsql.CompoundIdentifier{identifier, column})
	}
	return value
}

func shortestPathM0Hydration(stateID pgsql.Identifier, direction graph.Direction) pgsql.LateralSubquery {
	const (
		pathIndex     pgsql.Identifier = "m0_path_index"
		pathEdge      pgsql.Identifier = "m0_edge"
		pathTerminal  pgsql.Identifier = "m0_terminal"
		hydrated      pgsql.Identifier = "m0_hydrated"
		hydratedNodes pgsql.Identifier = "nodes"
		hydratedEdges pgsql.Identifier = "edges"
		hydratedCount pgsql.Identifier = "hydrated_count"
	)

	pathIDs := pgsql.CompoundIdentifier{stateID, expansionPath}
	edgeID := &pgsql.ArrayIndex{
		Expression: pgsql.NewParenthetical(pathIDs),
		Indexes:    []pgsql.Expression{pathIndex},
		CastType:   pgsql.Int8,
	}
	nextNodeColumn := pgsql.ColumnEndID
	if direction == graph.DirectionInbound {
		nextNodeColumn = pgsql.ColumnStartID
	}
	joins := []pgsql.Join{{
		Table: expansionEdgeTableReference(pathEdge),
		JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{pathEdge, pgsql.ColumnID}, pgsql.OperatorEquals, edgeID,
		)},
	}, {
		Table: expansionNodeTableReference(pathTerminal),
		JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{pathTerminal, pgsql.ColumnID}, pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{pathEdge, nextNodeColumn},
		)},
	}}

	return pgsql.LateralSubquery{
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{
				&pgsql.AliasedExpression{Expression: pgsql.FunctionCall{
					Function: pgsql.FunctionArrayAggregate, Parameters: []pgsql.Expression{shortestPathNodeComposite(pathTerminal)},
					OrderBy: []*pgsql.OrderBy{{Expression: pathIndex, Ascending: true}}, CastType: pgsql.NodeCompositeArray,
				}, Alias: pgsql.AsOptionalIdentifier(hydratedNodes)},
				&pgsql.AliasedExpression{Expression: pgsql.FunctionCall{
					Function: pgsql.FunctionArrayAggregate, Parameters: []pgsql.Expression{edgeCompositeValue(pathEdge)},
					OrderBy: []*pgsql.OrderBy{{Expression: pathIndex, Ascending: true}}, CastType: pgsql.EdgeCompositeArray,
				}, Alias: pgsql.AsOptionalIdentifier(hydratedEdges)},
				&pgsql.AliasedExpression{Expression: pgsql.FunctionCall{
					Function: pgsql.FunctionCount, Parameters: []pgsql.Expression{pgsql.Wildcard{}}, CastType: pgsql.Int8,
				}, Alias: pgsql.AsOptionalIdentifier(hydratedCount)},
			},
			From: []pgsql.FromClause{{
				Source: pgsql.AliasedExpression{
					Expression: pgsql.FunctionCall{Function: pgsql.FunctionGenerateSubscripts, Parameters: []pgsql.Expression{pathIDs, pgsql.NewLiteral(1, pgsql.Int)}},
					Alias:      pgsql.AsOptionalIdentifier(pathIndex),
				},
				Joins: joins,
			}},
		}},
		Binding: pgsql.AsOptionalIdentifier(hydrated),
	}
}

func shortestPathM0Projection(projection pgsql.Projection, stateID pgsql.Identifier, path pgsql.Expression) pgsql.Projection {
	result := append(pgsql.Projection(nil), projection...)
	for idx, item := range result {
		aliased, ok := item.(*pgsql.AliasedExpression)
		if !ok {
			continue
		}
		identifier, ok := aliased.Expression.(pgsql.CompoundIdentifier)
		if !ok || len(identifier) != 2 || identifier[0] != stateID || identifier[1] != expansionPath {
			continue
		}
		copy := *aliased
		copy.Expression = path
		result[idx] = &copy
	}
	return result
}

// BuildShortestPathEdgeM0Root emits the bounded one-path SP-S3-U-E search and
// direction-aware MAT-M0 hydration. Recursive state contains only the current
// node, depth, and ordered edge IDs; node order is derived from edge endpoints.
func (s *ExpansionBuilder) BuildShortestPathEdgeM0Root() (pgsql.Query, error) {
	const (
		validatedEndpoints pgsql.Identifier = "singleton_endpoints"
		hydrated           pgsql.Identifier = "m0_hydrated"
		hydratedNodes      pgsql.Identifier = "nodes"
		hydratedEdges      pgsql.Identifier = "edges"
		hydratedCount      pgsql.Identifier = "hydrated_count"
	)

	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UsesSingletonEndpointPair() {
		return pgsql.Query{}, errors.New("SP-S3-U-E+MAT-M0 requires one validated endpoint pair")
	}
	if !expansionModel.Options.MaxDepth.Set {
		return pgsql.Query{}, errors.New("SP-S3-U-E+MAT-M0 requires a bounded maximum depth")
	}

	endpointCTE := singletonEndpointValidationCTE(s.traversalStep, expansionModel)
	if expansionModel.Options.MinDepth.GetOr(1) > 0 {
		endpointSelect := endpointCTE.Query.Body.(pgsql.Select)
		endpointSelect.Where = pgsql.OptionalAnd(endpointSelect.Where, shortestPathSelfEndpointGuardCase(
			pgd.EntityID(s.traversalStep.LeftNode.Identifier),
			pgd.EntityID(s.traversalStep.RightNode.Identifier),
		))
		endpointCTE.Query.Body = endpointSelect
	}

	stateID := expansionModel.Frame.Binding.Identifier
	pathIDs := pgsql.CompoundIdentifier{stateID, expansionPath}
	anchor := pgsql.Select{
		Projection: pgsql.Projection{
			pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
			pgsql.NewLiteral(int64(0), pgsql.Int8),
			pgsql.ArrayLiteral{CastType: pgsql.Int8Array},
		},
		From: []pgsql.FromClause{{Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()}}},
	}
	recursive := pgsql.Select{
		Projection: pgsql.Projection{
			expansionModel.EdgeEndColumn,
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{stateID, expansionDepth}, pgsql.OperatorAdd, pgsql.NewLiteral(int64(1), pgsql.Int8)),
			pgsql.NewBinaryExpression(pathIDs, pgsql.OperatorConcatenate, pgsql.ArrayLiteral{
				Values: []pgsql.Expression{pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnID}}, CastType: pgsql.Int8Array,
			}),
		},
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					expansionModel.EdgeStartColumn, pgsql.OperatorEquals, pgsql.CompoundIdentifier{stateID, expansionNextID},
				)},
			}},
		}},
		Where: pgsql.OptionalAnd(
			expansionModel.EdgeConstraints,
			pgsql.OptionalAnd(
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{stateID, expansionDepth}, pgsql.OperatorLessThan, pgsql.NewLiteral(expansionModel.Options.MaxDepth.Value, pgsql.Int8)),
				relationshipIDNotInPath(pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnID}, pathIDs),
			),
		),
	}

	hydration := shortestPathM0Hydration(stateID, s.traversalStep.Direction)
	rootArray := pgsql.ArrayLiteral{Values: []pgsql.Expression{shortestPathNodeComposite(s.traversalStep.LeftNode.Identifier)}, CastType: pgsql.NodeCompositeArray}
	nodes := pgsql.FunctionCall{Function: pgsql.FunctionCoalesce, Parameters: []pgsql.Expression{
		pgsql.CompoundIdentifier{hydrated, hydratedNodes}, pgsql.ArrayLiteral{CastType: pgsql.NodeCompositeArray},
	}}
	edges := pgsql.FunctionCall{Function: pgsql.FunctionCoalesce, Parameters: []pgsql.Expression{
		pgsql.CompoundIdentifier{hydrated, hydratedEdges}, pgsql.ArrayLiteral{CastType: pgsql.EdgeCompositeArray},
	}}
	path := pgsql.CompositeValue{DataType: pgsql.PathComposite, Values: []pgsql.Expression{
		pgsql.NewBinaryExpression(rootArray, pgsql.OperatorConcatenate, nodes),
		edges,
	}}

	projection := pgsql.Select{
		Projection: shortestPathM0Projection(expansionModel.Projection, stateID, path),
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{Table: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()}, JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{stateID, expansionNextID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
				)}},
				{Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
				)}},
				{Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{stateID, expansionNextID},
				)}},
				{Table: hydration, JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewLiteral(true, pgsql.Boolean)}},
			},
		}},
		Where: pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{stateID, expansionDepth}, pgsql.OperatorGreaterThanOrEqualTo, pgsql.NewLiteral(expansionModel.Options.MinDepth.GetOr(1), pgsql.Int8)),
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{hydrated, hydratedCount}, pgsql.OperatorEquals, pgsql.FunctionCall{Function: pgsql.FunctionCardinality, Parameters: []pgsql.Expression{pathIDs}}),
		),
	}

	query := pgsql.Query{
		CommonTableExpressions: &pgsql.With{Recursive: true}, Body: projection,
		OrderBy: []*pgsql.OrderBy{{Expression: pgsql.CompoundIdentifier{stateID, expansionDepth}, Ascending: true}, {Expression: pathIDs, Ascending: true}},
		Limit:   pgsql.NewLiteral(int64(1), pgsql.Int8),
	}
	query.AddCTE(endpointCTE)
	query.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: stateID, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionNextID, expansionDepth, expansionPath})},
		Query: pgsql.Query{Body: pgsql.SetOperation{LOperand: anchor, ROperand: recursive, Operator: pgsql.OperatorUnion, All: true}},
	})
	return query, nil
}

func (s *ExpansionBuilder) BuildAllShortestPathsRoot() (pgsql.Query, error) {
	return s.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalASPHarness)
}

func compactShortestExecutor(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorASPA1DAG,
		optimize.ShortestPathExecutorS4CanonicalDistance,
		optimize.ShortestPathExecutorS4CanonicalWitness:
		return true
	default:
		return false
	}
}

// buildCompactBoundShortestPathsRoot invokes a typed, static bound-pair
// executor and keeps the legacy expansion row shape at its boundary. That lets
// existing projection and path materialization code consume compact search
// results without carrying entity composites through discovery.
func (s *ExpansionBuilder) buildCompactBoundShortestPathsRoot(functionName pgsql.Identifier, stateLimit bool) (pgsql.Query, error) {
	const validatedEndpoints pgsql.Identifier = "singleton_endpoints"

	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UsesSingletonEndpointPair() {
		return pgsql.Query{}, fmt.Errorf("%s requires one validated endpoint pair", functionName)
	}

	endpointCTE := singletonEndpointValidationCTE(s.traversalStep, expansionModel)
	if expansionModel.Options.MinDepth.GetOr(1) > 0 {
		endpointSelect := endpointCTE.Query.Body.(pgsql.Select)
		endpointSelect.Where = pgsql.OptionalAnd(endpointSelect.Where, shortestPathSelfEndpointGuardCase(
			pgd.EntityID(s.traversalStep.LeftNode.Identifier),
			pgd.EntityID(s.traversalStep.RightNode.Identifier),
		))
		endpointCTE.Query.Body = endpointSelect
	}

	maxDepth := expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth)
	parameters := []pgsql.Expression{
		pgsql.NewLiteral(s.graphID, pgsql.Int4),
		pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
		pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
		pgsql.NewLiteral(expansionModel.Options.MinDepth.GetOr(1), pgsql.Int4),
		pgsql.NewLiteral(maxDepth, pgsql.Int4),
		pgsql.NewLiteral(append([]int16(nil), expansionModel.RelationshipKindIDs...), pgsql.Int2Array),
		pgsql.NewLiteral(s.traversalStep.Direction == graph.DirectionInbound, pgsql.Boolean),
	}
	if stateLimit {
		const compactStateLimit int64 = 100_000
		parameters = append(parameters, pgsql.NewLiteral(compactStateLimit, pgsql.Int8))
	}

	stateID := expansionModel.Frame.Binding.Identifier
	search := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: stateID, Shape: expansionColumns()},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{pgsql.CompoundIdentifier{functionName, pgsql.WildcardIdentifier}},
			From: []pgsql.FromClause{
				{Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()}},
				{Source: pgsql.FunctionCall{Function: functionName, Parameters: parameters}},
			},
		}},
	}

	projection := pgsql.Select{
		Projection: expansionModel.Projection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{stateID, expansionRootID},
				)}},
				{Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{stateID, expansionNextID},
				)}},
			},
		}},
	}

	query := pgsql.Query{CommonTableExpressions: &pgsql.With{}, Body: projection}
	query.AddCTE(endpointCTE)
	query.AddCTE(search)
	return query, nil
}

func (s *ExpansionBuilder) BuildAllShortestPathsDAGRoot() (pgsql.Query, error) {
	return s.buildCompactBoundShortestPathsRoot(pgsql.FunctionAllShortestPathsDAG, false)
}

func (s *ExpansionBuilder) BuildCompactShortestPathRoot() (pgsql.Query, error) {
	return s.buildCompactBoundShortestPathsRoot(pgsql.FunctionShortestPathCompact, true)
}

func (s *ExpansionBuilder) canMaterializeTerminalFilter(expansionModel *Expansion) bool {
	return canMaterializeTerminalFilterForStep(s.traversalStep, expansionModel)
}

func (s *ExpansionBuilder) canMaterializeEndpointPairFilter(expansionModel *Expansion) bool {
	return canMaterializeEndpointPairFilterForStep(s.traversalStep, expansionModel)
}

func (s *ExpansionBuilder) buildBiDirectionalShortestPathsHarnessCall(harnessFunctionName pgsql.Identifier) (pgsql.Query, error) {
	var (
		expansionModel  = s.traversalStep.Expansion
		projectionQuery pgsql.Select
	)

	if !expansionModel.UsesSingletonEndpointPair() {
		expansionModel.UseMaterializedEndpointPairFilter = s.canMaterializeEndpointPairFilter(expansionModel)
	}

	forwardFrontPrimerQuery, forwardSeedProjectionConstraints, err := s.prepareForwardFrontPrimerQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	forwardFrontRecursiveQuery, err := s.prepareForwardFrontRecursiveQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	backwardFrontPrimerQuery, backwardSeedProjectionConstraints, err := s.prepareBackwardFrontPrimerQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	backwardFrontRecursiveQuery, err := s.prepareBackwardFrontRecursiveQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	projectionQuery.Projection = expansionModel.Projection

	// Select the expansion components for the projection statement
	projectionQuery.From = []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{{
			Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				),
			},
		}, {
			Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}}

	s.applyBoundEndpointProjectionConstraints(&projectionQuery, expansionModel)
	s.applyShortestPathSeedProjectionConstraints(&projectionQuery, pgsql.OptionalAnd(forwardSeedProjectionConstraints, backwardSeedProjectionConstraints))
	s.appendUnwindSources(&projectionQuery)
	s.applyShortestPathSelfEndpointGuard(&projectionQuery, expansionModel)

	if harnessParameters, err := s.bidirectionalShortestPathsParameters(
		expansionModel,
		forwardFrontPrimerQuery,
		forwardFrontRecursiveQuery,
		backwardFrontPrimerQuery,
		backwardFrontRecursiveQuery,
		harnessFunctionName == pgsql.FunctionBidirectionalSPHarness,
	); err != nil {
		return pgsql.Query{}, err
	} else {
		query := pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
			Body:                   projectionQuery,
		}

		if expansionModel.UsesSingletonEndpointPair() {
			query.AddCTE(singletonEndpointValidationCTE(s.traversalStep, expansionModel))
		}
		query.AddCTE(shortestPathSearchCTE(harnessFunctionName, expansionModel, harnessParameters))
		return query, nil
	}
}

func (s *ExpansionBuilder) BuildBiDirectionalShortestPathsRoot() (pgsql.Query, error) {
	return s.buildBiDirectionalShortestPathsHarnessCall(pgsql.FunctionBidirectionalSPHarness)
}

// BuildBiDirectionalShortestPathsRootWithDirectPreflight emits the tool-only
// SP-S0-DIRECT arm. A materialized one-edge probe returns immediately when it
// finds a valid bound-endpoint witness. The workspace-backed incumbent is
// dependent on a zero-or-one-row fallback endpoint CTE, so PostgreSQL cannot
// invoke it on a direct hit. Both branches execute in one statement snapshot.
func (s *ExpansionBuilder) BuildBiDirectionalShortestPathsRootWithDirectPreflight() (pgsql.Query, error) {
	const (
		validatedEndpoints pgsql.Identifier = "singleton_endpoints"
		directHit          pgsql.Identifier = "direct_shortest"
		fallbackEndpoints  pgsql.Identifier = "fallback_endpoints"
		workspaceSearch    pgsql.Identifier = "workspace_shortest"
	)

	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UsesSingletonEndpointPair() {
		return pgsql.Query{}, errors.New("SP-S0-DIRECT requires one validated endpoint pair")
	}
	if expansionModel.Options.MinDepth.GetOr(1) != 1 || expansionModel.Options.MaxDepth.GetOr(0) < 1 {
		return pgsql.Query{}, errors.New("SP-S0-DIRECT requires minimum depth one and a positive bounded maximum depth")
	}

	forwardFrontPrimerQuery, forwardSeedProjectionConstraints, err := s.prepareForwardFrontPrimerQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}
	forwardFrontRecursiveQuery, err := s.prepareForwardFrontRecursiveQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}
	backwardFrontPrimerQuery, backwardSeedProjectionConstraints, err := s.prepareBackwardFrontPrimerQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}
	backwardFrontRecursiveQuery, err := s.prepareBackwardFrontRecursiveQuery(expansionModel)
	if err != nil {
		return pgsql.Query{}, err
	}

	harnessParameters, err := s.bidirectionalShortestPathsParameters(
		expansionModel,
		forwardFrontPrimerQuery,
		forwardFrontRecursiveQuery,
		backwardFrontPrimerQuery,
		backwardFrontRecursiveQuery,
		true,
	)
	if err != nil {
		return pgsql.Query{}, err
	}

	projectionQuery := pgsql.Select{
		Projection: expansionModel.Projection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: expansionModel.Frame.Binding.Identifier.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				)}},
				{Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				)}},
			},
		}},
	}
	s.applyShortestPathSeedProjectionConstraints(&projectionQuery, pgsql.OptionalAnd(forwardSeedProjectionConstraints, backwardSeedProjectionConstraints))
	s.appendUnwindSources(&projectionQuery)
	s.applyShortestPathSelfEndpointGuard(&projectionQuery, expansionModel)

	directQuery := pgsql.Query{
		Body: pgsql.Select{
			Projection: pgsql.Projection{
				pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
				pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
				pgsql.NewLiteral(int64(1), pgsql.Int8),
				pgsql.NewLiteral(true, pgsql.Boolean),
				pgd.Equals(pgd.StartID(s.traversalStep.Edge.Identifier), pgd.EndID(s.traversalStep.Edge.Identifier)),
				pgd.ExpressionArrayLiteral(pgd.EntityID(s.traversalStep.Edge.Identifier)),
			},
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()},
				Joins: []pgsql.Join{{
					Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier),
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.OptionalAnd(
						pgd.Equals(expansionModel.EdgeStartColumn, pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID}),
						pgd.Equals(expansionModel.EdgeEndColumn, pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID}),
					)},
				}},
			}},
			Where: expansionModel.EdgeConstraints,
		},
		OrderBy: []*pgsql.OrderBy{{Expression: pgd.EntityID(s.traversalStep.Edge.Identifier), Ascending: true}},
		Limit:   pgsql.NewLiteral(int64(1), pgsql.Int8),
	}

	fallbackEndpointQuery := pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.Wildcard{}},
		From:       []pgsql.FromClause{{Source: pgsql.TableReference{Name: validatedEndpoints.AsCompoundIdentifier()}}},
		Where: pgsql.ExistsExpression{Negated: true, Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)},
			From:       []pgsql.FromClause{{Source: pgsql.TableReference{Name: directHit.AsCompoundIdentifier()}}},
		}}}},
	}}

	stateQuery := pgsql.Query{Body: pgsql.SetOperation{
		LOperand: pgsql.Select{Projection: pgsql.Projection{pgsql.Wildcard{}}, From: []pgsql.FromClause{{Source: pgsql.TableReference{Name: directHit.AsCompoundIdentifier()}}}},
		ROperand: pgsql.Select{Projection: pgsql.Projection{pgsql.Wildcard{}}, From: []pgsql.FromClause{{Source: pgsql.TableReference{Name: workspaceSearch.AsCompoundIdentifier()}}}},
		Operator: pgsql.OperatorUnion,
		All:      true,
	}}

	query := pgsql.Query{CommonTableExpressions: &pgsql.With{}, Body: projectionQuery}
	query.AddCTE(singletonEndpointValidationCTE(s.traversalStep, expansionModel))
	query.AddCTE(pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: directHit, Shape: expansionColumns()},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        directQuery,
	})
	query.AddCTE(pgsql.CommonTableExpression{Alias: pgsql.TableAlias{Name: fallbackEndpoints}, Query: fallbackEndpointQuery})
	query.AddCTE(shortestPathSearchCTEFrom(pgsql.FunctionBidirectionalSPHarness, expansionModel, harnessParameters, fallbackEndpoints, workspaceSearch))
	query.AddCTE(pgsql.CommonTableExpression{Alias: pgsql.TableAlias{Name: expansionModel.Frame.Binding.Identifier, Shape: expansionColumns()}, Query: stateQuery})

	return query, nil
}

func (s *ExpansionBuilder) BuildBiDirectionalAllShortestPathsRoot() (pgsql.Query, error) {
	return s.buildBiDirectionalShortestPathsHarnessCall(pgsql.FunctionBidirectionalASPHarness)
}

func (s *ExpansionBuilder) boundEndpointFilterParameters() ([]pgsql.Expression, error) {
	var (
		rootFilterStatement, hasRootFilter         = s.boundRootIDsFilterStatement()
		terminalFilterStatement, hasTerminalFilter = s.boundTerminalIDsFilterStatement()
		pairFilterStatement, hasPairFilter         = s.boundEndpointPairFilterStatement()
	)

	if !hasPairFilter {
		pairFilterStatement, hasPairFilter = s.materializedEndpointPairFilterStatement()
	}

	if !hasTerminalFilter {
		terminalFilterStatement, hasTerminalFilter = s.unboundTerminalIDsFilterStatement()
	}

	if !hasRootFilter && !hasTerminalFilter && !hasPairFilter {
		return nil, nil
	}

	// Pair filters supersede separate root/terminal filters because they encode
	// the allowed combinations, not just independent endpoint sets.
	var (
		rootFilter     string
		terminalFilter string
		pairFilter     string
	)

	if hasPairFilter {
		if formattedFilter, err := format.Statement(pairFilterStatement, format.NewOutputBuilder().WithTargetGraph(s.graphID).WithMaterializedParameters(s.queryParameters)); err != nil {
			return nil, err
		} else {
			pairFilter = formattedFilter
		}
	} else if hasRootFilter {
		if formattedFilter, err := format.Statement(rootFilterStatement, format.NewOutputBuilder().WithTargetGraph(s.graphID).WithMaterializedParameters(s.queryParameters)); err != nil {
			return nil, err
		} else {
			rootFilter = formattedFilter
		}
	}

	if !hasPairFilter && hasTerminalFilter {
		if formattedFilter, err := format.Statement(terminalFilterStatement, format.NewOutputBuilder().WithTargetGraph(s.graphID).WithMaterializedParameters(s.queryParameters)); err != nil {
			return nil, err
		} else {
			terminalFilter = formattedFilter
		}
	}

	filterParameters := []pgsql.Expression{
		pgsql.NewTypeCast(pgsql.NewLiteral(rootFilter, pgsql.Text), pgsql.Text),
		pgsql.NewTypeCast(pgsql.NewLiteral(terminalFilter, pgsql.Text), pgsql.Text),
	}

	if hasPairFilter {
		filterParameters = append(filterParameters, pgsql.NewTypeCast(pgsql.NewLiteral(pairFilter, pgsql.Text), pgsql.Text))
	}

	return filterParameters, nil
}

func (s *ExpansionBuilder) shortestPathsParameters(expansionModel *Expansion, forwardFrontPrimerQuery pgsql.SetExpression, forwardFrontRecursiveQuery pgsql.SetExpression) ([]pgsql.Expression, error) {
	var (
		harnessParameters []pgsql.Expression
		formatFragment    = func(query pgsql.SetExpression) (string, error) {
			return format.Statement(
				nextFrontInsert(query),
				format.NewOutputBuilder().WithTargetGraph(s.graphID).WithMaterializedParameters(s.queryParameters))
		}
	)

	if formattedQuery, err := formatFragment(forwardFrontPrimerQuery); err != nil {
		return nil, err
	} else {
		// Put this in the translation's parameter bag which is transmitted down to the DB
		s.queryParameters[expansionModel.PrimerQueryParameter.Identifier.String()] = formattedQuery

		// Track this as a function parameter for the harness
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.PrimerQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(forwardFrontRecursiveQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.RecursiveQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.RecursiveQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	harnessParameters = append(harnessParameters, pgsql.NewLiteral(expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth), pgsql.Int))

	if filterParameters, err := s.boundEndpointFilterParameters(); err != nil {
		return nil, err
	} else {
		harnessParameters = append(harnessParameters, filterParameters...)
	}

	return harnessParameters, nil
}

func shortestPathWorkspaceFragment(fragment string) string {
	return strings.NewReplacer(
		"on conflict on constraint forward_visited_pkey", "on conflict on constraint bsp_forward_visited_pkey",
		"on conflict on constraint backward_visited_pkey", "on conflict on constraint bsp_backward_visited_pkey",
		"forward_visited", "pg_temp.bsp_forward_visited",
		"backward_visited", "pg_temp.bsp_backward_visited",
		"forward_front", "pg_temp.bsp_forward_front",
		"backward_front", "pg_temp.bsp_backward_front",
		"next_front", "pg_temp.bsp_next_front",
	).Replace(fragment)
}

func (s *ExpansionBuilder) bidirectionalShortestPathsParameters(expansionModel *Expansion, forwardFrontPrimerQuery pgsql.SetExpression, forwardFrontRecursiveQuery pgsql.SetExpression, backwardFrontPrimerQuery pgsql.SetExpression, backwardFrontRecursiveQuery pgsql.SetExpression, useReusableWorkspace bool) ([]pgsql.Expression, error) {
	var (
		harnessParameters []pgsql.Expression
		formatFragment    = func(query pgsql.SetExpression) (string, error) {
			fragment, err := format.Statement(
				nextFrontInsert(query),
				format.NewOutputBuilder().WithTargetGraph(s.graphID).WithMaterializedParameters(s.queryParameters))
			if err != nil {
				return "", err
			}
			if useReusableWorkspace {
				fragment = shortestPathWorkspaceFragment(fragment)
			}
			return fragment, nil
		}
	)

	if formattedQuery, err := formatFragment(forwardFrontPrimerQuery); err != nil {
		return nil, err
	} else {
		// Put this in the translation's parameter bag which is transmitted down to the DB
		s.queryParameters[expansionModel.PrimerQueryParameter.Identifier.String()] = formattedQuery

		// Track this as a function parameter for the harness
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.PrimerQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(forwardFrontRecursiveQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.RecursiveQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.RecursiveQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(backwardFrontPrimerQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.BackwardPrimerQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.BackwardPrimerQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(backwardFrontRecursiveQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.BackwardRecursiveQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.BackwardRecursiveQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	harnessParameters = append(harnessParameters, pgsql.NewLiteral(expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth), pgsql.Int))
	if expansionModel.UsesSingletonEndpointPair() {
		harnessParameters = append(harnessParameters,
			pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{expansionModel.SingletonRootID},
				CastType: pgsql.Int8Array,
			},
			pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{expansionModel.SingletonTerminalID},
				CastType: pgsql.Int8Array,
			},
		)
		if useReusableWorkspace {
			harnessParameters = append(harnessParameters, pgsql.NewLiteral(expansionAllowsZeroDepth(expansionModel), pgsql.Boolean))
		}
		return harnessParameters, nil
	}

	if filterParameters, err := s.boundEndpointFilterParameters(); err != nil {
		return nil, err
	} else {
		if useReusableWorkspace {
			for idx, filterParameter := range filterParameters {
				typeCast, isTypeCast := filterParameter.(pgsql.TypeCast)
				if !isTypeCast {
					continue
				}
				literal, isLiteral := typeCast.Expression.(pgsql.Literal)
				if !isLiteral {
					continue
				}
				if value, isString := literal.Value.(string); isString {
					literal.Value = strings.NewReplacer(
						"traversal_root_filter", "pg_temp.bsp_root_filter",
						"traversal_terminal_filter", "pg_temp.bsp_terminal_filter",
						"traversal_pair_filter", "pg_temp.bsp_pair_filter",
					).Replace(value)
					typeCast.Expression = literal
					filterParameters[idx] = typeCast
				}
			}
		}
		harnessParameters = append(harnessParameters, filterParameters...)
	}
	if useReusableWorkspace {
		harnessParameters = append(harnessParameters, pgsql.NewLiteral(expansionAllowsZeroDepth(expansionModel), pgsql.Boolean))
	}

	return harnessParameters, nil
}

func (s *ExpansionBuilder) Build(expansionIdentifier pgsql.Identifier, commonTableExpressions ...pgsql.CommonTableExpression) pgsql.Query {
	expansionBody := pgsql.SetExpression(pgsql.SetOperation{
		LOperand: s.PrimerStatement,
		ROperand: s.RecursiveStatement,
		Operator: pgsql.OperatorUnion,
		All:      s.UseUnionAll,
	})

	if s.ZeroDepthStatement != nil {
		recursiveStatement := s.RecursiveStatement
		recursiveStatement.Where = pgsql.OptionalAnd(
			recursiveStatement.Where,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{expansionIdentifier, expansionDepth},
				pgsql.OperatorGreaterThan,
				pgsql.NewLiteral(0, pgsql.Int),
			),
		)

		expansionBody = pgsql.SetOperation{
			LOperand: pgsql.SetOperation{
				LOperand: *s.ZeroDepthStatement,
				ROperand: s.PrimerStatement,
				Operator: pgsql.OperatorUnion,
				All:      s.UseUnionAll,
			},
			ROperand: recursiveStatement,
			Operator: pgsql.OperatorUnion,
			All:      s.UseUnionAll,
		}
	}

	query := pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive: true,
		},
		Body: s.ProjectionStatement,
	}

	for _, commonTableExpression := range commonTableExpressions {
		query.AddCTE(commonTableExpression)
	}

	query.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  expansionIdentifier,
			Shape: expansionColumns(),
		},
		Query: pgsql.Query{
			Body: expansionBody,
		},
	})

	return query
}

func projectionAliasExpressions(projection pgsql.Projection) map[pgsql.Identifier]pgsql.Expression {
	aliases := make(map[pgsql.Identifier]pgsql.Expression)

	for _, selectItem := range projection {
		switch typedSelectItem := selectItem.(type) {
		case *pgsql.AliasedExpression:
			if typedSelectItem.Alias.Set {
				aliases[typedSelectItem.Alias.Value] = typedSelectItem.Expression
			}

		case pgsql.AliasedExpression:
			if typedSelectItem.Alias.Set {
				aliases[typedSelectItem.Alias.Value] = typedSelectItem.Expression
			}

		case pgsql.Identifier:
			aliases[typedSelectItem] = typedSelectItem

		case pgsql.CompoundIdentifier:
			if len(typedSelectItem) > 0 {
				aliases[typedSelectItem[len(typedSelectItem)-1]] = typedSelectItem
			}
		}
	}

	return aliases
}

func rewriteCurrentFrameProjectionSetExpression(setExpression pgsql.SetExpression, frameID pgsql.Identifier, aliases map[pgsql.Identifier]pgsql.Expression) pgsql.SetExpression {
	switch typedSetExpression := setExpression.(type) {
	case pgsql.Select:
		return rewriteCurrentFrameProjectionSelect(typedSetExpression, frameID, aliases)

	case pgsql.SetOperation:
		typedSetExpression.LOperand = rewriteCurrentFrameProjectionSetExpression(typedSetExpression.LOperand, frameID, aliases)
		typedSetExpression.ROperand = rewriteCurrentFrameProjectionSetExpression(typedSetExpression.ROperand, frameID, aliases)
		return typedSetExpression

	default:
		return setExpression
	}
}

func rewriteCurrentFrameProjectionQuery(query pgsql.Query, frameID pgsql.Identifier, aliases map[pgsql.Identifier]pgsql.Expression) pgsql.Query {
	query.Body = rewriteCurrentFrameProjectionSetExpression(query.Body, frameID, aliases)

	for idx, orderBy := range query.OrderBy {
		if orderBy != nil {
			query.OrderBy[idx].Expression = rewriteCurrentFrameProjectionReferences(orderBy.Expression, frameID, aliases)
		}
	}

	query.Offset = rewriteCurrentFrameProjectionReferences(query.Offset, frameID, aliases)
	query.Limit = rewriteCurrentFrameProjectionReferences(query.Limit, frameID, aliases)

	return query
}

func rewriteCurrentFrameProjectionSelect(selectBody pgsql.Select, frameID pgsql.Identifier, aliases map[pgsql.Identifier]pgsql.Expression) pgsql.Select {
	for idx, selectItem := range selectBody.Projection {
		if rewritten, isSelectItem := rewriteCurrentFrameProjectionReferences(selectItem, frameID, aliases).(pgsql.SelectItem); isSelectItem {
			selectBody.Projection[idx] = rewritten
		}
	}

	for idx := range selectBody.From {
		selectBody.From[idx].Source = rewriteCurrentFrameProjectionReferences(selectBody.From[idx].Source, frameID, aliases)

		for joinIdx := range selectBody.From[idx].Joins {
			selectBody.From[idx].Joins[joinIdx].Table = rewriteCurrentFrameProjectionReferences(selectBody.From[idx].Joins[joinIdx].Table, frameID, aliases)
			selectBody.From[idx].Joins[joinIdx].JoinOperator.Constraint = rewriteCurrentFrameProjectionReferences(selectBody.From[idx].Joins[joinIdx].JoinOperator.Constraint, frameID, aliases)
		}
	}

	selectBody.Where = rewriteCurrentFrameProjectionReferences(selectBody.Where, frameID, aliases)

	for idx, groupByExpression := range selectBody.GroupBy {
		selectBody.GroupBy[idx] = rewriteCurrentFrameProjectionReferences(groupByExpression, frameID, aliases)
	}

	selectBody.Having = rewriteCurrentFrameProjectionReferences(selectBody.Having, frameID, aliases)

	return selectBody
}

func rewriteCurrentFrameProjectionReferences(expression pgsql.Expression, frameID pgsql.Identifier, aliases map[pgsql.Identifier]pgsql.Expression) pgsql.Expression {
	if expression == nil {
		return nil
	}

	switch typedExpression := expression.(type) {
	case pgsql.CompoundIdentifier:
		if len(typedExpression) == 2 && typedExpression[0] == frameID {
			if replacement, hasReplacement := aliases[typedExpression[1]]; hasReplacement {
				return replacement
			}
		}

		return typedExpression

	case pgsql.RowColumnReference:
		typedExpression.Identifier = rewriteCurrentFrameProjectionReferences(typedExpression.Identifier, frameID, aliases)
		return typedExpression

	case pgsql.UnaryExpression:
		typedExpression.Operand = rewriteCurrentFrameProjectionReferences(typedExpression.Operand, frameID, aliases)
		return typedExpression

	case *pgsql.UnaryExpression:
		typedExpression.Operand = rewriteCurrentFrameProjectionReferences(typedExpression.Operand, frameID, aliases)
		return typedExpression

	case pgsql.BinaryExpression:
		typedExpression.LOperand = rewriteCurrentFrameProjectionReferences(typedExpression.LOperand, frameID, aliases)
		typedExpression.ROperand = rewriteCurrentFrameProjectionReferences(typedExpression.ROperand, frameID, aliases)
		return typedExpression

	case *pgsql.BinaryExpression:
		typedExpression.LOperand = rewriteCurrentFrameProjectionReferences(typedExpression.LOperand, frameID, aliases)
		typedExpression.ROperand = rewriteCurrentFrameProjectionReferences(typedExpression.ROperand, frameID, aliases)
		return typedExpression

	case pgsql.FunctionCall:
		for idx, parameter := range typedExpression.Parameters {
			typedExpression.Parameters[idx] = rewriteCurrentFrameProjectionReferences(parameter, frameID, aliases)
		}
		for _, orderBy := range typedExpression.OrderBy {
			if orderBy != nil {
				orderBy.Expression = rewriteCurrentFrameProjectionReferences(orderBy.Expression, frameID, aliases)
			}
		}
		return typedExpression

	case *pgsql.FunctionCall:
		for idx, parameter := range typedExpression.Parameters {
			typedExpression.Parameters[idx] = rewriteCurrentFrameProjectionReferences(parameter, frameID, aliases)
		}
		for _, orderBy := range typedExpression.OrderBy {
			if orderBy != nil {
				orderBy.Expression = rewriteCurrentFrameProjectionReferences(orderBy.Expression, frameID, aliases)
			}
		}
		return typedExpression

	case pgsql.TypeCast:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case pgsql.CompositeValue:
		for idx, value := range typedExpression.Values {
			typedExpression.Values[idx] = rewriteCurrentFrameProjectionReferences(value, frameID, aliases)
		}
		return typedExpression

	case *pgsql.Parenthetical:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case *pgsql.EdgeArrayFromPathIDs:
		typedExpression.PathIDs = rewriteCurrentFrameProjectionReferences(typedExpression.PathIDs, frameID, aliases)
		typedExpression.GraphID = rewriteCurrentFrameProjectionReferences(typedExpression.GraphID, frameID, aliases)
		return typedExpression

	case pgsql.ArrayLiteral:
		for idx, value := range typedExpression.Values {
			typedExpression.Values[idx] = rewriteCurrentFrameProjectionReferences(value, frameID, aliases)
		}
		return typedExpression

	case pgsql.ArrayExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case pgsql.ArrayIndex:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		for idx, index := range typedExpression.Indexes {
			typedExpression.Indexes[idx] = rewriteCurrentFrameProjectionReferences(index, frameID, aliases)
		}
		return typedExpression

	case *pgsql.ArrayIndex:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		for idx, index := range typedExpression.Indexes {
			typedExpression.Indexes[idx] = rewriteCurrentFrameProjectionReferences(index, frameID, aliases)
		}
		return typedExpression

	case pgsql.ArraySlice:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		typedExpression.Lower = rewriteCurrentFrameProjectionReferences(typedExpression.Lower, frameID, aliases)
		typedExpression.Upper = rewriteCurrentFrameProjectionReferences(typedExpression.Upper, frameID, aliases)
		return typedExpression

	case *pgsql.ArraySlice:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		typedExpression.Lower = rewriteCurrentFrameProjectionReferences(typedExpression.Lower, frameID, aliases)
		typedExpression.Upper = rewriteCurrentFrameProjectionReferences(typedExpression.Upper, frameID, aliases)
		return typedExpression

	case pgsql.AllExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case *pgsql.AllExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case pgsql.AnyExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case *pgsql.AnyExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case pgsql.Case:
		typedExpression.Operand = rewriteCurrentFrameProjectionReferences(typedExpression.Operand, frameID, aliases)
		for idx, condition := range typedExpression.Conditions {
			typedExpression.Conditions[idx] = rewriteCurrentFrameProjectionReferences(condition, frameID, aliases)
		}
		for idx, then := range typedExpression.Then {
			typedExpression.Then[idx] = rewriteCurrentFrameProjectionReferences(then, frameID, aliases)
		}
		typedExpression.Else = rewriteCurrentFrameProjectionReferences(typedExpression.Else, frameID, aliases)
		return typedExpression

	case *pgsql.Case:
		typedExpression.Operand = rewriteCurrentFrameProjectionReferences(typedExpression.Operand, frameID, aliases)
		for idx, condition := range typedExpression.Conditions {
			typedExpression.Conditions[idx] = rewriteCurrentFrameProjectionReferences(condition, frameID, aliases)
		}
		for idx, then := range typedExpression.Then {
			typedExpression.Then[idx] = rewriteCurrentFrameProjectionReferences(then, frameID, aliases)
		}
		typedExpression.Else = rewriteCurrentFrameProjectionReferences(typedExpression.Else, frameID, aliases)
		return typedExpression

	case pgsql.ExistsExpression:
		typedExpression.Subquery.Query = rewriteCurrentFrameProjectionQuery(typedExpression.Subquery.Query, frameID, aliases)
		return typedExpression

	case pgsql.Subquery:
		typedExpression.Query = rewriteCurrentFrameProjectionQuery(typedExpression.Query, frameID, aliases)
		return typedExpression

	case pgsql.Query:
		return rewriteCurrentFrameProjectionQuery(typedExpression, frameID, aliases)

	case pgsql.Select:
		return rewriteCurrentFrameProjectionSelect(typedExpression, frameID, aliases)

	case pgsql.SetOperation:
		typedExpression.LOperand = rewriteCurrentFrameProjectionSetExpression(typedExpression.LOperand, frameID, aliases)
		typedExpression.ROperand = rewriteCurrentFrameProjectionSetExpression(typedExpression.ROperand, frameID, aliases)
		return typedExpression

	case pgsql.ProjectionFrom:
		for idx, selectItem := range typedExpression.Projection {
			if rewritten, isSelectItem := rewriteCurrentFrameProjectionReferences(selectItem, frameID, aliases).(pgsql.SelectItem); isSelectItem {
				typedExpression.Projection[idx] = rewritten
			}
		}
		for idx := range typedExpression.From {
			typedExpression.From[idx].Source = rewriteCurrentFrameProjectionReferences(typedExpression.From[idx].Source, frameID, aliases)
			for joinIdx := range typedExpression.From[idx].Joins {
				typedExpression.From[idx].Joins[joinIdx].JoinOperator.Constraint = rewriteCurrentFrameProjectionReferences(typedExpression.From[idx].Joins[joinIdx].JoinOperator.Constraint, frameID, aliases)
			}
		}
		return typedExpression

	case pgsql.AliasedExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case *pgsql.AliasedExpression:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case pgsql.Variadic:
		typedExpression.Expression = rewriteCurrentFrameProjectionReferences(typedExpression.Expression, frameID, aliases)
		return typedExpression

	case pgsql.LateralSubquery:
		typedExpression.Query = rewriteCurrentFrameProjectionQuery(typedExpression.Query, frameID, aliases)
		return typedExpression

	default:
		return expression
	}
}

// isSelfLoopEndpoints reports whether a traversal step's left and right nodes are the same Cypher
// variable. For a self-loop such as (n)-[*..]->(n) the translator reuses a single BoundIdentifier for
// both endpoints.
func isSelfLoopEndpoints(traversalStep *TraversalStep) bool {
	return traversalStep.LeftNode.Identifier == traversalStep.RightNode.Identifier
}

// isUnboundSelfLoop reports whether a traversal step is a self-loop whose node is not genuinely carried
// by the previous frame. A self-loop marks the node as bound because both endpoints share a binding, but
// in MATCH (x) MATCH (n)-[*..]->(n) the node n is not exported by the previous frame and must be seeded
// independently. In MATCH (n) WITH n MATCH (n)-[*..]->(n) the node is exported, so it stays tied to the
// previous frame and this returns false.
func isUnboundSelfLoop(traversalStep *TraversalStep) bool {
	if !isSelfLoopEndpoints(traversalStep) {
		return false
	}

	if traversalStep.Frame == nil || traversalStep.Frame.Previous == nil {
		return true
	}

	return !traversalStep.Frame.Previous.Exported.Contains(traversalStep.LeftNode.Identifier)
}

// expansionProjectionNodeJoins builds the projection node-lookup joins for an expansion frame. When the
// endpoints are the same variable a single join on root_id is emitted to avoid a duplicate table alias;
// otherwise the usual root_id/next_id pair is returned.
func expansionProjectionNodeJoins(traversalStep *TraversalStep, frameID pgsql.Identifier) []pgsql.Join {
	rootJoin := expansionNodeLookupJoin(
		traversalStep.LeftNode,
		pgsql.CompoundIdentifier{frameID, expansionRootID},
	)

	if isSelfLoopEndpoints(traversalStep) {
		return []pgsql.Join{rootJoin}
	}

	nextJoin := expansionNodeLookupJoin(
		traversalStep.RightNode,
		pgsql.CompoundIdentifier{frameID, expansionNextID},
	)

	return []pgsql.Join{rootJoin, nextJoin}
}

// selfLoopIdentityConstraint returns a root_id = next_id predicate for self-loop endpoints, restricting
// the projection to walks that returned to their origin. It returns nil for non-self-loops.
func selfLoopIdentityConstraint(traversalStep *TraversalStep, frameID pgsql.Identifier) pgsql.Expression {
	if !isSelfLoopEndpoints(traversalStep) {
		return nil
	}

	return pgd.Equals(
		pgsql.CompoundIdentifier{frameID, expansionRootID},
		pgsql.CompoundIdentifier{frameID, expansionNextID},
	)
}

func (s *Translator) buildExpansionPatternRoot(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder) (pgsql.Query, error) {
	var (
		traversalStep  = traversalStepContext.CurrentStep
		expansionModel = traversalStep.Expansion
		seedIdentifier = expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier)
		unwindClauses  = s.query.CurrentPart().ConsumeUnwindClauses()
		unwindSources  = unwindFromClauses(unwindClauses)
	)

	// Determine local scope of the primer: the edge and both nodes.
	primerLocal, primerExternal := partitionConstraintByLocality(
		expansionModel.PrimerNodeConstraints,
		pgsql.AsIdentifierSet(
			traversalStep.LeftNode.Identifier,
			traversalStep.Edge.Identifier,
			traversalStep.RightNode.Identifier,
		),
	)

	var (
		seedConstraints = pgsql.OptionalAnd(primerLocal, primerExternal)
		seed            *expansionSeed
	)

	// A self-loop marks LeftNodeBound even when its node is new, so seed an unbound self-loop from all
	// nodes rather than the previous frame. A self-loop carried in (e.g. via WITH) stays bound.
	if traversalStep.LeftNodeBound && !isUnboundSelfLoop(traversalStep) {
		if traversalStep.Frame.Previous == nil {
			return pgsql.Query{}, fmt.Errorf("left node is marked as bound but there is no previous frame to reference")
		}

		boundSeed := newExpansionBoundNodeSeed(seedIdentifier, traversalStep.Frame.Previous, traversalStep.LeftNode, seedConstraints)
		seed = &boundSeed
		expansion.UseUnionAll = true
	} else if seedConstraints != nil || isUnboundSelfLoop(traversalStep) {
		nodeSeed := newExpansionNodeSeed(seedIdentifier, traversalStep.LeftNode.Identifier, seedConstraints)
		seed = &nodeSeed
		expansion.UseUnionAll = primerExternal == nil

		// External terms reference a prior CTE (e.g. s0.i0). Cross-join it into the
		// seed so it is in scope before the traversal primer joins edges.
		if primerExternal != nil && traversalStep.Frame.Previous != nil {
			nodeSeed.query.From = append([]pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
				},
			}}, nodeSeed.query.From...)
			seed = &nodeSeed
		}
	} else {
		expansion.UseUnionAll = true
	}

	if seed != nil {
		if seedNeedsUnwind, err := expressionReferencesUnwindBinding(seedConstraints, unwindClauses); err != nil {
			return pgsql.Query{}, err
		} else if seedNeedsUnwind {
			seed.query.From = prependFrameSourceIfMissing(seed.query.From, traversalStep.Frame.Previous)
			seed.query.From = append(seed.query.From, unwindSources...)
		}
	}

	expansion.PrimerStatement.Where = expansionModel.EdgeConstraints

	expansion.ProjectionStatement.Projection = expansionModel.Projection
	expansion.RecursiveStatement.Where = expansionModel.RecursiveConstraints
	if projection, err := s.buildExpansionPrimerProjection(traversalStep); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.PrimerStatement.Projection = projection
	}

	if projection, err := s.buildExpansionRecursiveProjection(traversalStep); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.RecursiveStatement.Projection = projection
	}

	var nextQueryFrom pgsql.FromClause

	if seed != nil {
		nextQueryFrom = seed.fromClause(seed.edgeJoin(traversalStep.Edge.Identifier, expansionModel.EdgeStartColumn))
	} else {
		nextQueryFrom = expansionEdgeFromClause(traversalStep.Edge.Identifier)
	}

	// If there are terminal node constraints then the right node must be joined
	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: expansionNodeTableReference(traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, nextQueryFrom)
	if primerNeedsUnwind, err := expressionReferencesUnwindBinding(expansionModel.EdgeConstraints, unwindClauses); err != nil {
		return pgsql.Query{}, err
	} else if primerNeedsUnwind {
		expansion.PrimerStatement.From = prependFrameSourceIfMissing(expansion.PrimerStatement.From, traversalStep.Frame.Previous)
		expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, unwindSources...)
	}

	if expansionAllowsZeroDepth(expansionModel) {
		zeroDepthStatement, err := expansion.buildZeroDepthExpansionSelect(seed)
		if err != nil {
			return pgsql.Query{}, err
		}

		expansion.ZeroDepthStatement = &zeroDepthStatement
	}

	// Build recursive step joins. The terminal node join is only added when the
	// expansion carries terminal-node constraints, which are the only cases where
	// node columns appear in the recursive body.
	recursiveJoins := []pgsql.Join{recursiveExpansionEdgeLookupJoin(traversalStep)}

	if expansionModel.TerminalNodeConstraints != nil {
		recursiveJoins = append(recursiveJoins, pgsql.Join{
			Table: expansionNodeTableReference(traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.RecursiveStatement.From = append(expansion.RecursiveStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
		},
		Joins: recursiveJoins,
	})

	var previousProjectionFrameID pgsql.Identifier

	// The current query part may not have a frame associated with it if is a single part query component
	if traversalStep.Frame.Previous != nil && (s.query.CurrentPart().Frame == nil || traversalStep.Frame.Previous.Binding.Identifier != s.query.CurrentPart().Frame.Binding.Identifier) {
		previousProjectionFrameID = traversalStep.Frame.Previous.Binding.Identifier
		expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{previousProjectionFrameID},
				Binding: models.EmptyOptional[pgsql.Identifier](),
			},
		})
	}

	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, unwindSources...)

	// Select the expansion components for the projection statement
	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{
			expansionNodeLookupJoin(
				traversalStep.LeftNode,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
			),
			expansionNodeLookupJoin(
				traversalStep.RightNode,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
			),
		},
	})

	if projectionConstraints, err := s.buildExpansionProjectionConstraints(traversalStepContext); err != nil {
		return pgsql.Query{}, err
	} else {
		projectionConstraints = pgsql.OptionalAnd(
			projectionConstraints,
			selfLoopIdentityConstraint(traversalStep, expansionModel.Frame.Binding.Identifier),
		)
		// Skip these gates for an unbound self-loop: its node is not a column of the previous frame, so the
		// gates would emit an invalid (prevFrame.n) reference, and selfLoopIdentityConstraint already ties
		// the endpoints. A carried self-loop keeps the gates.
		if previousProjectionFrameID != "" && traversalStep.LeftNodeBound && !isUnboundSelfLoop(traversalStep) {
			projectionConstraints = pgsql.OptionalAnd(
				projectionConstraints,
				boundEndpointProjectionConstraint(
					previousProjectionFrameID,
					traversalStep.LeftNode,
					expansionModel.Frame.Binding.Identifier,
					expansionRootID,
				),
			)
		}
		if previousProjectionFrameID != "" && traversalStep.RightNodeBound && !isUnboundSelfLoop(traversalStep) {
			projectionConstraints = pgsql.OptionalAnd(
				projectionConstraints,
				boundEndpointProjectionConstraint(
					previousProjectionFrameID,
					traversalStep.RightNode,
					expansionModel.Frame.Binding.Identifier,
					expansionNextID,
				),
			)
		}

		projectionConstraints = rewriteCurrentFrameProjectionReferences(
			projectionConstraints,
			traversalStep.Frame.Binding.Identifier,
			projectionAliasExpressions(expansion.ProjectionStatement.Projection),
		)
		expansion.ProjectionStatement.Where = projectionConstraints
	}

	if seed != nil {
		return expansion.Build(expansionModel.Frame.Binding.Identifier, seed.CTE()), nil
	}

	return expansion.Build(expansionModel.Frame.Binding.Identifier), nil
}

func (s *Translator) buildExpansionPatternStep(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder) (pgsql.Query, error) {
	var (
		traversalStep  = traversalStepContext.CurrentStep
		expansionModel = traversalStep.Expansion
		seed           = newExpansionBoundNodeSeed(
			expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier),
			traversalStep.Frame.Previous,
			traversalStep.LeftNode,
			expansionModel.PrimerNodeConstraints,
		)
	)

	expansion.ProjectionStatement.Projection = expansionModel.Projection
	expansion.UseUnionAll = true
	expansion.PrimerStatement.Where = expansionModel.EdgeConstraints
	expansion.RecursiveStatement.Where = expansionModel.RecursiveConstraints
	if projection, err := s.buildExpansionPrimerProjection(traversalStep); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.PrimerStatement.Projection = projection
	}

	if projection, err := s.buildExpansionRecursiveProjection(traversalStep); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.RecursiveStatement.Projection = projection
	}

	primerJoins := []pgsql.Join{
		seed.edgeJoin(traversalStep.Edge.Identifier, expansionModel.EdgeStartColumn),
	}

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		primerJoins = append(primerJoins, pgsql.Join{
			Table: expansionNodeTableReference(traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, seed.fromClause(primerJoins...))

	if expansionAllowsZeroDepth(expansionModel) {
		zeroDepthStatement, err := expansion.buildZeroDepthExpansionSelect(&seed)
		if err != nil {
			return pgsql.Query{}, err
		}

		expansion.ZeroDepthStatement = &zeroDepthStatement
	}

	// Build recursive step joins. The terminal node join is only added when the
	// expansion carries terminal-node constraints, which are the only cases where
	// node columns appear in the recursive body.
	recursiveJoins := []pgsql.Join{recursiveExpansionEdgeLookupJoin(traversalStep)}

	// If there are terminal node constraints then the right node must be joined
	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		recursiveJoins = append(recursiveJoins, pgsql.Join{
			Table: expansionNodeTableReference(traversalStep.RightNode.Identifier),
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.RecursiveStatement.From = append(expansion.RecursiveStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
		},
		Joins: recursiveJoins,
	})

	// Select the expansion components for the projection statement
	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
	})

	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{
			expansionNodeLookupJoin(
				traversalStep.LeftNode,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
			),
			expansionNodeLookupJoin(
				traversalStep.RightNode,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
			),
		},
	})

	if projectionConstraints, err := s.buildExpansionProjectionConstraints(traversalStepContext); err != nil {
		return pgsql.Query{}, err
	} else {
		projectionConstraints = pgsql.OptionalAnd(
			projectionConstraints,
			selfLoopIdentityConstraint(traversalStep, expansionModel.Frame.Binding.Identifier),
		)
		projectionConstraints = rewriteCurrentFrameProjectionReferences(
			projectionConstraints,
			traversalStep.Frame.Binding.Identifier,
			projectionAliasExpressions(expansion.ProjectionStatement.Projection),
		)
		expansion.ProjectionStatement.Where = projectionConstraints
	}

	return expansion.Build(expansionModel.Frame.Binding.Identifier, seed.CTE()), nil
}

func expansionTerminalSatisfactionLocality(traversalStep *TraversalStep) (pgsql.Expression, pgsql.Expression) {
	return partitionConstraintByLocality(
		pgsql.Expression(traversalStep.Expansion.TerminalNodeSatisfactionProjection),
		pgsql.AsIdentifierSet(
			traversalStep.LeftNode.Identifier,
			traversalStep.Edge.Identifier,
			traversalStep.RightNode.Identifier,
		),
	)
}

func applyExpansionSuffixPushdown(part *PatternPart) (int, error) {
	var applied int

	for idx := 0; idx+1 < len(part.TraversalSteps); idx++ {
		var (
			currentStep = part.TraversalSteps[idx]
			suffixSteps = part.TraversalSteps[idx+1:]
		)

		if candidateApplied, err := applyExpansionSuffixPushdownCandidate(currentStep, suffixSteps); err != nil {
			return applied, err
		} else if candidateApplied {
			applied++
		}
	}

	return applied, nil
}

func applyExpansionSuffixPushdownCandidate(currentStep *TraversalStep, suffixSteps []*TraversalStep) (bool, error) {
	if suffixSatisfaction, satisfied := expansionSuffixTerminalSatisfaction(currentStep, suffixSteps); satisfied {
		currentStep.Expansion.TerminalNodeConstraints = pgsql.OptionalAnd(
			currentStep.Expansion.TerminalNodeConstraints,
			suffixSatisfaction,
		)

		if terminalCriteriaProjection, err := pgsql.As[pgsql.SelectItem](currentStep.Expansion.TerminalNodeConstraints); err != nil {
			return false, err
		} else {
			currentStep.Expansion.TerminalNodeSatisfactionProjection = terminalCriteriaProjection
		}

		return true, nil
	}

	return false, nil
}

func suffixEdgeLeftEndpoint(edgeIdentifier pgsql.Identifier, direction graph.Direction) (pgsql.Expression, bool) {
	switch direction {
	case graph.DirectionOutbound:
		return pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnStartID}, true
	case graph.DirectionInbound:
		return pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnEndID}, true
	default:
		return nil, false
	}
}

func suffixEdgeRightEndpoint(edgeIdentifier pgsql.Identifier, direction graph.Direction) (pgsql.Expression, bool) {
	switch direction {
	case graph.DirectionOutbound:
		return pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnEndID}, true
	case graph.DirectionInbound:
		return pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnStartID}, true
	default:
		return nil, false
	}
}

func suffixBoundNodeIDReference(currentStep *TraversalStep, node *BoundIdentifier) (pgsql.Expression, bool) {
	if currentStep == nil ||
		currentStep.Frame == nil ||
		currentStep.Frame.Previous == nil ||
		currentStep.Frame.Previous.Binding == nil ||
		node == nil ||
		!currentStep.Frame.Previous.Known().Contains(node.Identifier) {
		return nil, false
	}

	return projectedNodeIDReference(currentStep.Frame.Previous.Binding.Identifier, node), true
}

func suffixStepEdgeConstraints(step *TraversalStep) pgsql.Expression {
	if step == nil || step.EdgeConstraints == nil {
		return nil
	}

	localConstraints, _ := partitionConstraintByLocality(
		step.EdgeConstraints.Expression,
		pgsql.AsIdentifierSet(step.Edge.Identifier),
	)

	return localConstraints
}

func expansionSuffixTerminalSatisfaction(currentStep *TraversalStep, suffixSteps []*TraversalStep) (pgsql.Expression, bool) {
	if currentStep == nil ||
		currentStep.Expansion == nil ||
		currentStep.RightNode == nil ||
		len(suffixSteps) == 0 ||
		suffixSteps[0] == nil ||
		suffixSteps[0].LeftNode == nil ||
		currentStep.RightNode.Identifier != suffixSteps[0].LeftNode.Identifier {
		return nil, false
	}

	var (
		fromClause pgsql.FromClause
		where      pgsql.Expression
		previousID pgsql.Expression = pgsql.CompoundIdentifier{currentStep.RightNode.Identifier, pgsql.ColumnID}
	)

	for idx, step := range suffixSteps {
		if step == nil ||
			step.Expansion != nil ||
			step.LeftNode == nil ||
			step.Edge == nil ||
			step.RightNode == nil ||
			step.Direction == graph.DirectionBoth {
			break
		}

		if idx > 0 && suffixSteps[idx-1].RightNode.Identifier != step.LeftNode.Identifier {
			break
		}

		leftEndpoint, validDirection := suffixEdgeLeftEndpoint(step.Edge.Identifier, step.Direction)
		if !validDirection {
			return nil, false
		}

		edgeJoin := pgd.Equals(previousID, leftEndpoint)
		if idx == 0 {
			fromClause = expansionEdgeFromClause(step.Edge.Identifier)
			where = pgsql.OptionalAnd(where, edgeJoin)
		} else {
			fromClause.Joins = append(fromClause.Joins, pgsql.Join{
				Table: expansionEdgeTableReference(step.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: edgeJoin,
				},
			})
		}

		where = pgsql.OptionalAnd(where, suffixStepEdgeConstraints(step))

		rightEndpoint, validDirection := suffixEdgeRightEndpoint(step.Edge.Identifier, step.Direction)
		if !validDirection {
			return nil, false
		}

		if step.RightNodeBound {
			boundRightNodeID, hasBoundRightNodeID := suffixBoundNodeIDReference(currentStep, step.RightNode)
			if !hasBoundRightNodeID {
				return nil, false
			}

			where = pgsql.OptionalAnd(where, step.RightNodeConstraints)
			where = pgsql.OptionalAnd(where, pgd.Equals(rightEndpoint, boundRightNodeID))
			previousID = boundRightNodeID
		} else {
			fromClause.Joins = append(fromClause.Joins, pgsql.Join{
				Table: expansionNodeTableReference(step.RightNode.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(
						step.RightNodeConstraints,
						pgd.Equals(pgsql.CompoundIdentifier{step.RightNode.Identifier, pgsql.ColumnID}, rightEndpoint),
					),
				},
			})

			previousID = pgsql.CompoundIdentifier{step.RightNode.Identifier, pgsql.ColumnID}
		}
	}

	if fromClause.Source == nil {
		return nil, false
	}

	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: pgsql.Projection{pgd.IntLiteral(1)},
					From:       []pgsql.FromClause{fromClause},
					Where:      where,
				},
			},
		},
	}, true
}

func expansionLocalTerminalSatisfactionProjection(traversalStep *TraversalStep) (pgsql.SelectItem, error) {
	localSatisfiedConstraint, _ := expansionTerminalSatisfactionLocality(traversalStep)

	if localSatisfiedConstraint == nil {
		return pgsql.NewLiteral(true, pgsql.Boolean), nil
	}

	return pgsql.As[pgsql.SelectItem](localSatisfiedConstraint)
}

func (s *Translator) buildExpansionPrimerProjection(traversalStep *TraversalStep) ([]pgsql.SelectItem, error) {
	expansionModel := traversalStep.Expansion

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		satisfiedProjection, err := expansionLocalTerminalSatisfactionProjection(traversalStep)
		if err != nil {
			return nil, err
		}

		return []pgsql.SelectItem{
			expansionModel.EdgeStartColumn,
			expansionModel.EdgeEndColumn,
			pgsql.NewLiteral(1, pgsql.Int),
			satisfiedProjection,
			pgsql.NewBinaryExpression(
				expansionModel.EdgeStartColumn,
				pgsql.OperatorEquals,
				expansionModel.EdgeEndColumn,
			),
			pgsql.ArrayLiteral{
				Values: []pgsql.Expression{
					pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
				},
			},
		}, nil
	} else {
		return []pgsql.SelectItem{
			expansionModel.EdgeStartColumn,
			expansionModel.EdgeEndColumn,
			pgsql.NewLiteral(1, pgsql.Int),
			pgsql.NewLiteral(false, pgsql.Boolean),
			pgsql.NewBinaryExpression(
				expansionModel.EdgeStartColumn,
				pgsql.OperatorEquals,
				expansionModel.EdgeEndColumn,
			),
			pgsql.ArrayLiteral{
				Values: []pgsql.Expression{
					pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
				},
			},
		}, nil
	}
}

func expansionRecursivePathExpression(traversalStep *TraversalStep) *pgsql.BinaryExpression {
	var (
		expansionModel = traversalStep.Expansion
		path           = pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionPath}
		edgeID         = pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID}
	)

	if traversalStep.PathReversed {
		return pgsql.NewBinaryExpression(edgeID, pgsql.OperatorConcatenate, path)
	}

	return pgsql.NewBinaryExpression(path, pgsql.OperatorConcatenate, edgeID)
}

func (s *Translator) buildExpansionRecursiveProjection(traversalStep *TraversalStep) ([]pgsql.SelectItem, error) {
	expansionModel := traversalStep.Expansion

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		// Split up constraints that can not be satisfied by the local scope of the expansion. This is done to ensure
		// that cross-entity references and other extra-scope comparisons are added external to the expansion frame.
		localSatisfiedConstraint, externalSatisfiedConstraint := expansionTerminalSatisfactionLocality(traversalStep)

		// Store the external constraints to be inserted during the final projection and where clause
		expansionModel.DeferredNodeSatisfactionConstraint = externalSatisfiedConstraint

		if localSatisfiedConstraint == nil {
			localSatisfiedConstraint = pgsql.NewLiteral(true, pgsql.Boolean)
		}

		if satisfiedSelectItem, err := pgsql.As[pgsql.SelectItem](localSatisfiedConstraint); err != nil {
			return nil, err
		} else {
			return []pgsql.SelectItem{
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				expansionModel.EdgeEndColumn,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionDepth},
					pgsql.OperatorAdd,
					pgsql.NewLiteral(1, pgsql.Int),
				),
				satisfiedSelectItem,
				pgsql.NewLiteral(false, pgsql.Boolean),
				expansionRecursivePathExpression(traversalStep),
			}, nil
		}
	} else {
		return []pgsql.SelectItem{
			pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
			expansionModel.EdgeEndColumn,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionDepth},
				pgsql.OperatorAdd,
				pgsql.NewLiteral(1, pgsql.Int),
			),
			pgsql.NewLiteral(false, pgsql.Boolean),
			pgsql.NewLiteral(false, pgsql.Boolean),
			expansionRecursivePathExpression(traversalStep),
		}, nil
	}
}

func (s *Translator) buildExpansionProjectionConstraints(traversalStepContext TraversalStepContext) (pgsql.Expression, error) {
	var (
		currentStep           = traversalStepContext.CurrentStep
		previousStep          = traversalStepContext.PreviousStep
		expansionModel        = currentStep.Expansion
		projectionConstraints pgsql.Expression
		constraints           *Constraint
		err                   error
		joinCondition         pgsql.Expression
	)

	if previousStep != nil {
		joinCondition = pgd.Equals(
			projectedNodeIDReference(previousStep.Frame.Binding.Identifier, currentStep.LeftNode),
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
		)
	}

	if constraints, err = s.treeTranslator.ConsumeConstraintsFromVisibleSet(expansionModel.Frame.Visible); err != nil {
		return projectionConstraints, err
	} else {
		// Constraints that target the terminal node may crop up here where it's finally in scope. Additionally,
		// only accept paths that are marked satisfied from the recursive descent CTE
		if expansionModel.TerminalNodeSatisfactionProjection != nil {
			expressions := []pgsql.Expression{
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionSatisfied},
				constraints.Expression,
				joinCondition,
			}

			if projectionConstraints, err = ConjoinExpressions(s.kindMapper, expressions); err != nil {
				return projectionConstraints, err
			}

			// Append any deferred (non-local) constraints onto the projection constraints
			if expansionModel.DeferredNodeSatisfactionConstraint != nil {
				projectionConstraints = pgsql.OptionalAnd(projectionConstraints, expansionModel.DeferredNodeSatisfactionConstraint)
			}
		} else {
			if projectionConstraints, err = ConjoinExpressions(s.kindMapper, []pgsql.Expression{constraints.Expression, joinCondition}); err != nil {
				return projectionConstraints, err
			}
		}
	}

	// Check for min-path depth as this will also filter the final expansion projection
	if expansionModel.Options.MinDepth.Set && expansionModel.Options.MinDepth.Value > 1 {
		projectionConstraints = pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionDepth},
				pgsql.OperatorGreaterThanOrEqualTo,
				pgsql.NewLiteral(expansionModel.Options.MinDepth.Value, pgsql.Int),
			),
			projectionConstraints,
		)
	}

	// Exclude expansion paths that reuse a relationship consumed by a preceding fixed step.
	if expansionModel.PreviousRelationshipUniqueness != nil {
		projectionConstraints = pgsql.OptionalAnd(projectionConstraints, expansionModel.PreviousRelationshipUniqueness)
	}

	return projectionConstraints, nil
}

func (s *Translator) translateTraversalPatternPartWithExpansion(part *PatternPart, stepIndex int, isFirstTraversalStep bool, traversalStep *TraversalStep, allowProjectionPruning bool) error {
	expansionModel := traversalStep.Expansion

	// Translate the expansion's constraints - this has the side effect of making the pattern identifiers visible in
	// the current scope frame
	if err := s.translateExpansionConstraints(part, stepIndex, isFirstTraversalStep, traversalStep, expansionModel); err != nil {
		return err
	}
	if decision, selected := s.shortestPathExecutorDecision(part, stepIndex); selected {
		expansionModel.ShortestPathExecutor = decision.SelectedExecutor
		expansionModel.ShortestPathTarget = decision.Target
		if !expansionModel.Options.MaxDepth.Set && decision.MaximumDepth > 0 {
			expansionModel.Options.MaxDepth = models.OptionalValue(decision.MaximumDepth)
		}
		if decision.SelectedExecutor == optimize.ShortestPathExecutorS3Unidirectional || decision.SelectedExecutor == optimize.ShortestPathExecutorS4CanonicalDistance {
			expansionModel.PathBinding.DistanceOnly = true
			expansionModel.PathBinding.DataType = pgsql.Int
			if part.PatternBinding != nil {
				part.PatternBinding.DistanceOnly = true
				part.PatternBinding.DataType = pgsql.Int
			}
		}
	}

	// Export the path from the traversal's scope
	traversalStep.Frame.Export(expansionModel.PathBinding.Identifier)
	if allowProjectionPruning {
		_, hasDecision := s.projectionPruningDecision(part, stepIndex)
		if hasDecision && pruneExpansionStepProjectionExports(part, stepIndex, traversalStep) {
			s.recordLowering(optimize.LoweringProjectionPruning)
		}

		if _, hasDecision := s.latePathMaterializationDecision(part, stepIndex, optimize.LatePathMaterializationExpansionPath); hasDecision &&
			traversalStep.Frame.Exported.Contains(expansionModel.PathBinding.Identifier) {
			s.recordLowering(optimize.LoweringLatePathMaterialization)
		}
	}

	// Push a new frame that contains currently projected scope from the expansion recursive CTE
	if expansionFrame, err := s.scope.PushFrame(); err != nil {
		return err
	} else {
		expansionModel.Frame = expansionFrame
	}

	// Enforce relationship uniqueness against any preceding fixed steps. The expansion's own path
	// array already excludes edges reused within the recursion; this additionally excludes edges
	// consumed by fixed steps that precede the expansion (e.g. after a pattern reversal).
	expansionModel.PreviousRelationshipUniqueness = expansionPreviousRelationshipUniquenessConstraint(s.scope, part, stepIndex, traversalStep)

	if expansionModel.TerminalNodeConstraints != nil {
		if terminalCriteriaProjection, err := pgsql.As[pgsql.SelectItem](expansionModel.TerminalNodeConstraints); err != nil {
			return err
		} else {
			expansionModel.TerminalNodeSatisfactionProjection = terminalCriteriaProjection
		}
	}

	// Expansion edge join condition
	expansionModel.RecursiveConstraints = expansionConstraints(traversalStep)

	if err := RewriteFrameBindings(s.scope, expansionModel.RecursiveConstraints); err != nil {
		return err
	}

	// Remove the previous projections of the root and terminal node to reproject them after expansion
	traversalStep.LeftNode.Dematerialize()
	traversalStep.RightNode.Dematerialize()
	leftNodeIDOnly := s.applyIDOnlyNodeProjection(part, stepIndex, traversalStep.LeftNode)
	rightNodeIDOnly := s.applyIDOnlyNodeProjection(part, stepIndex, traversalStep.RightNode)
	if leftNodeIDOnly || rightNodeIDOnly {
		s.recordLowering(optimize.LoweringFieldRequirements)
	}

	if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
		return err
	} else {
		// Zip through all projected identifiers and update their last projected frame
		for _, binding := range boundProjections.Bindings {
			binding.MaterializedBy(expansionModel.Frame)
		}

		expansionModel.Projection = boundProjections.Items
	}

	if err := s.scope.PopFrame(); err != nil {
		return err
	}

	if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
		return err
	} else {
		// Zip through all projected identifiers and update their last projected frame
		for _, binding := range boundProjections.Bindings {
			binding.MaterializedBy(traversalStep.Frame)
		}

		traversalStep.Projection = boundProjections.Items
	}
	if expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3EdgeM0 {
		expansionModel.PathBinding.DataType = pgsql.PathComposite
		if part.PatternBinding != nil {
			part.PatternBinding.DataType = pgsql.PathComposite
		}
	}

	if expansionModel.Options.FindShortestPath || expansionModel.Options.FindAllShortestPaths {
		if err := s.translateShortestPathTraversal(part, stepIndex, traversalStep, expansionModel); err != nil {
			return err
		}
	}

	return nil
}

func (s *Translator) translateExpansionConstraints(part *PatternPart, stepIndex int, isFirstTraversalStep bool, step *TraversalStep, expansionModel *Expansion) error {
	if constraints, err := consumePatternConstraints(isFirstTraversalStep, recursivePattern, step, s.treeTranslator); err != nil {
		return err
	} else {
		// If one side of the expansion has constraints but the other does not this may be an opportunity to reorder the traversal
		// to start with tighter search bounds
		if err := s.applyPatternConstraintBalance(part, stepIndex, &constraints, step); err != nil {
			return err
		}

		s.recordPredicatePlacementConsumption(part, stepIndex, step, constraints)

		// Left node
		if leftNodeJoinCondition, err := leftNodeTraversalStepConstraint(step); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, leftNodeJoinCondition); err != nil {
			return err
		} else {
			expansionModel.PrimerNodeJoinCondition = leftNodeJoinCondition
		}

		if constraints.LeftNode.Expression != nil {
			if err := RewriteFrameBindings(s.scope, constraints.LeftNode.Expression); err != nil {
				return err
			}

			expansionModel.PrimerNodeConstraints = constraints.LeftNode.Expression

			if primerCriteriaProjection, err := pgsql.As[pgsql.SelectItem](expansionModel.PrimerNodeConstraints); err != nil {
				return err
			} else {
				expansionModel.PrimerNodeSatisfactionProjection = primerCriteriaProjection
			}
		}

		// Expansion edge constraints
		if constraints.Edge.Expression != nil {
			expansionModel.EdgeConstraints = constraints.Edge.Expression

			if err := RewriteFrameBindings(s.scope, expansionModel.EdgeConstraints); err != nil {
				return err
			}
		}

		if !isFirstTraversalStep {
			if edgeJoinCondition, err := expansionEdgeJoinCondition(step); err != nil {
				return err
			} else if err := RewriteFrameBindings(s.scope, edgeJoinCondition); err != nil {
				return err
			} else {
				expansionModel.EdgeJoinCondition = edgeJoinCondition
			}
		}

		// Right node
		if rightNodeJoinCondition, err := rightNodeTraversalStepJoinCondition(step); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, rightNodeJoinCondition); err != nil {
			return err
		} else {
			expansionModel.ExpansionNodeJoinCondition = rightNodeJoinCondition
		}

		if constraints.RightNode.Expression != nil {
			if err := RewriteFrameBindings(s.scope, constraints.RightNode.Expression); err != nil {
				return err
			} else {
				expansionModel.TerminalNodeConstraints = constraints.RightNode.Expression
			}
		}
	}

	return nil
}

func (s *Translator) translateShortestPathTraversal(part *PatternPart, stepIndex int, traversalStep *TraversalStep, expansionModel *Expansion) error {
	var (
		useBidirectionalSearch bool
		err                    error
	)

	useBidirectionalSearch, err = s.useBidirectionalShortestPathStrategy(part, stepIndex, traversalStep)

	if err != nil {
		return err
	}

	expansionModel.UseBidirectionalSearch = useBidirectionalSearch && expansionModel.ShortestPathExecutor != optimize.ShortestPathExecutorS3Unidirectional && expansionModel.ShortestPathExecutor != optimize.ShortestPathExecutorS3EdgeM0 && !compactShortestExecutor(expansionModel.ShortestPathExecutor)
	expansionModel.HasExplicitEndpointInequality = s.treeTranslator.HasEndpointInequality(
		traversalStep.LeftNode.Identifier,
		traversalStep.RightNode.Identifier,
	)
	s.applyShortestPathFilterMaterialization(part, stepIndex, traversalStep, expansionModel)
	if (compactShortestExecutor(expansionModel.ShortestPathExecutor) || expansionModel.UseBidirectionalSearch || expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3Unidirectional || expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3EdgeM0) &&
		!traversalStep.LeftNodeBound &&
		!traversalStep.RightNodeBound &&
		(!expansionModel.Options.MinDepth.Set || expansionModel.Options.MinDepth.Value > 0 || expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3Unidirectional || expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3EdgeM0 || compactShortestExecutor(expansionModel.ShortestPathExecutor)) {
		rootAnchor, hasRootAnchor := singletonIDAnchor(expansionModel.PrimerNodeConstraints, traversalStep.LeftNode.Identifier)
		terminalAnchor, hasTerminalAnchor := singletonIDAnchor(expansionModel.TerminalNodeConstraints, traversalStep.RightNode.Identifier)
		if hasRootAnchor && hasTerminalAnchor {
			var err error
			if expansionModel.SingletonRootID, err = s.liftSingletonIDAnchor(rootAnchor); err != nil {
				return err
			}
			expansionModel.PrimerNodeConstraints = replaceSingletonIDAnchor(
				expansionModel.PrimerNodeConstraints,
				traversalStep.LeftNode.Identifier,
				expansionModel.SingletonRootID,
			)
			if expansionModel.SingletonTerminalID, err = s.liftSingletonIDAnchor(terminalAnchor); err != nil {
				return err
			}
			expansionModel.TerminalNodeConstraints = replaceSingletonIDAnchor(
				expansionModel.TerminalNodeConstraints,
				traversalStep.RightNode.Identifier,
				expansionModel.SingletonTerminalID,
			)
			expansionModel.UseMaterializedEndpointPairFilter = false
		}
	}

	if expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3Unidirectional || expansionModel.ShortestPathExecutor == optimize.ShortestPathExecutorS3EdgeM0 || compactShortestExecutor(expansionModel.ShortestPathExecutor) {
		return nil
	}

	// If this query is a shortest-path look up, the translator will have to use a function harness for
	// traversal. As such, query fragments for the traversal harness will have to be passed by the parameters
	// defined below.
	if primerQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
		return err
	} else {
		expansionModel.PrimerQueryParameter = primerQueryParameter
	}

	if recursiveQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
		return err
	} else {
		expansionModel.RecursiveQueryParameter = recursiveQueryParameter
	}

	// Bidirectional BFS searches require an additional set of query fragments to represent the backward traversal
	// front of the search.
	if expansionModel.UseBidirectionalSearch {
		if reversePrimerQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
			return err
		} else {
			expansionModel.BackwardPrimerQueryParameter = reversePrimerQueryParameter
		}

		if reverseRecursiveQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
			return err
		} else {
			expansionModel.BackwardRecursiveQueryParameter = reverseRecursiveQueryParameter
		}
	}

	return nil
}

func (s *Translator) liftSingletonIDAnchor(expression pgsql.Expression) (pgsql.Expression, error) {
	switch typedExpression := unwrapParenthetical(expression).(type) {
	case pgsql.Literal:
		parameterBinding, err := s.scope.DefineNew(pgsql.ParameterIdentifier)
		if err != nil {
			return nil, err
		}
		parameter, err := pgsql.AsParameter(parameterBinding.Identifier, typedExpression.Value)
		if err != nil {
			return nil, err
		}
		parameter.CastType = pgsql.Int8
		parameterBinding.Parameter = parameter
		s.translation.Parameters[parameterBinding.Identifier.String()] = typedExpression.Value
		return parameter, nil

	case pgsql.Parameter:
		typedExpression.CastType = pgsql.Int8
		return typedExpression, nil
	case *pgsql.Parameter:
		copy := *typedExpression
		copy.CastType = pgsql.Int8
		return &copy, nil
	case pgsql.TypeCast:
		return s.liftSingletonIDAnchor(typedExpression.Expression)
	default:
		return nil, fmt.Errorf("unsupported singleton endpoint expression: %T", expression)
	}
}

func (s *Translator) translateNonTraversalPatternPart(part *PatternPart) error {
	if nextFrame, err := s.scope.PushFrame(); err != nil {
		return err
	} else {
		part.NodeSelect.Frame = nextFrame

		nextFrame.Export(part.NodeSelect.Binding.Identifier)

		set := nextFrame.Known().Copy()
		if s.query.CurrentPart().quantifierIdentifiers != nil && s.query.CurrentPart().quantifierIdentifiers.Len() > 0 {
			set = set.MergeSet(s.query.CurrentPart().quantifierIdentifiers)
		}
		if constraint, err := s.treeTranslator.ConsumeConstraintsFromVisibleSet(set); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, constraint.Expression); err != nil {
			return err
		} else {
			part.NodeSelect.Constraints = constraint.Expression
		}

		if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
			return err
		} else {
			// Zip through all projected identifiers and update their last projected frame
			for _, binding := range boundProjections.Bindings {
				binding.MaterializedBy(nextFrame)
			}

			part.NodeSelect.Select.Projection = boundProjections.Items
		}
	}

	return nil
}

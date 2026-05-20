package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
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
	traversalStep   *TraversalStep
	model           *Expansion
}

func NewExpansionBuilder(queryParameters map[string]any, traversalStep *TraversalStep) (*ExpansionBuilder, error) {
	if traversalStep.Expansion == nil {
		return nil, errors.New("traversal step must have expansion set")
	}

	return &ExpansionBuilder{
		queryParameters: queryParameters,
		traversalStep:   traversalStep,
		model:           traversalStep.Expansion,
	}, nil
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
	filterAlias := pgsql.Identifier(string(identifier) + "_filter")
	filterID := pgsql.CompoundIdentifier{filterAlias, pgsql.ColumnID}

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

func newExpansionBoundNodeSeed(identifier pgsql.Identifier, previousFrame *Frame, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	seed := newExpansionSeed(identifier, pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{
			previousFrame.Binding.Identifier,
			nodeIdentifier,
		},
		Column: pgsql.ColumnID,
	}, []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
		},
	}}, constraints)

	seed.query.Distinct = true
	return seed
}

func newExpansionRootIDsParameterSeed(identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	return newExpansionNodeFilterSeed(identifier, expansionRootFilter, nodeIdentifier, constraints)
}

func newExpansionTerminalIDsParameterSeed(identifier, nodeIdentifier pgsql.Identifier, constraints pgsql.Expression) expansionSeed {
	return newExpansionNodeFilterSeed(identifier, expansionTerminalFilter, nodeIdentifier, constraints)
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

func expansionNodeProjection(nodeIdentifier pgsql.Identifier) pgsql.Projection {
	projection := make(pgsql.Projection, len(pgsql.NodeTableColumns))

	for idx, column := range pgsql.NodeTableColumns {
		projection[idx] = pgsql.CompoundIdentifier{nodeIdentifier, column}
	}

	return projection
}

func expansionNodeLookupJoin(nodeIdentifier pgsql.Identifier, nodeID pgsql.Expression) pgsql.Join {
	nodeLookup := pgsql.Select{
		Projection: expansionNodeProjection(nodeIdentifier),
		From: []pgsql.FromClause{{
			Source: expansionNodeTableReference(nodeIdentifier),
		}},
		Where: pgd.Equals(
			pgsql.CompoundIdentifier{nodeIdentifier, pgsql.ColumnID},
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
			Binding: models.OptionalValue(nodeIdentifier),
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

func seedEndpointConstraintSplit(expression pgsql.Expression, nodeIdentifier pgsql.Identifier, previousFrameIdentifier pgsql.Identifier) (pgsql.Expression, pgsql.Expression) {
	// Harness seed fragments only range over the endpoint node alias and an optional ID filter.
	// Reframe safe endpoint references first, then leave anything still non-local for the outer projection.
	seedExpression := rewriteBoundEndpointSeedReference(expression, previousFrameIdentifier, nodeIdentifier)
	return partitionConstraintByLocality(seedExpression, pgsql.AsIdentifierSet(nodeIdentifier))
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
	previousFrameIdentifier := s.traversalStep.Frame.Previous.Binding.Identifier
	nodeIDExpression := pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{previousFrameIdentifier, nodeIdentifier},
		Column:     pgsql.ColumnID,
	}

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

	previousFrameIdentifier := s.traversalStep.Frame.Previous.Binding.Identifier
	rootIDExpression := pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{previousFrameIdentifier, s.traversalStep.LeftNode.Identifier},
		Column:     pgsql.ColumnID,
	}
	terminalIDExpression := pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{previousFrameIdentifier, s.traversalStep.RightNode.Identifier},
		Column:     pgsql.ColumnID,
	}

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

	rootIDExpression := pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}
	terminalIDExpression := pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}
	pairConstraints := pgsql.OptionalAnd(expansionModel.PrimerNodeConstraints, expansionModel.TerminalNodeConstraints)
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

func (s *ExpansionBuilder) prepareForwardFrontPrimerQuery(expansionModel *Expansion) (pgsql.Query, pgsql.Expression) {
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

	primerSeedConstraints, primerProjectionPredicate = seedEndpointConstraintSplit(
		expansionModel.PrimerNodeConstraints,
		s.traversalStep.LeftNode.Identifier,
		previousFrameIdentifier,
	)

	if s.usesBoundRootIDs() {
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

	if !expansionModel.HasExplicitEndpointInequality {
		nextQuery.Where = pgsql.OptionalAnd(
			nextQuery.Where,
			shortestPathSeedSelfEndpointGuard(s.model.EdgeStartColumn, expansionModel.UseMaterializedEndpointPairFilter),
		)
	}

	return frontPrimerQuery(seed, nextQuery), primerProjectionPredicate
}

func (s *ExpansionBuilder) prepareForwardFrontRecursiveQuery(expansionModel *Expansion) pgsql.Select {
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
	return nextQuery
}

func (s *ExpansionBuilder) prepareBackwardFrontPrimerQuery(expansionModel *Expansion) (pgsql.Query, pgsql.Expression) {
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

	terminalSeedConstraints, terminalProjectionPredicate = seedEndpointConstraintSplit(
		expansionModel.TerminalNodeConstraints,
		s.traversalStep.RightNode.Identifier,
		previousFrameIdentifier,
	)

	if s.usesBoundTerminalIDs() {
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
	return frontPrimerQuery(seed, nextQuery), terminalProjectionPredicate
}

func (s *ExpansionBuilder) prepareBackwardFrontRecursiveQuery(expansionModel *Expansion) pgsql.Select {
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
	return nextQuery
}

func shortestPathSearchCTE(functionName pgsql.Identifier, expansionModel *Expansion, harnessParameters []pgsql.Expression) pgsql.CommonTableExpression {
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

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  expansionModel.Frame.Binding.Identifier,
			Shape: expansionColumns(),
		},
		Query: innerQuery,
	}
}

func boundEndpointProjectionConstraint(prevFrameID, nodeIdentifier, expansionFrameID, expansionColumn pgsql.Identifier) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{prevFrameID, nodeIdentifier},
			Column:     pgsql.ColumnID,
		},
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
				s.traversalStep.LeftNode.Identifier,
				expansionModel.Frame.Binding.Identifier,
				expansionRootID,
			),
		)
	}

	if s.traversalStep.RightNodeBound {
		projectionQuery.Where = pgsql.OptionalAnd(projectionQuery.Where,
			boundEndpointProjectionConstraint(
				prevFrameID,
				s.traversalStep.RightNode.Identifier,
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
	rootID := pgsql.CompoundIdentifier{expansionFrame, expansionRootID}
	terminalID := pgsql.CompoundIdentifier{expansionFrame, expansionNextID}

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
	if expansionModel.HasExplicitEndpointInequality {
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

	var (
		forwardFrontPrimerQuery, forwardSeedProjectionConstraints = s.prepareForwardFrontPrimerQuery(expansionModel)
		forwardFrontRecursiveQuery                                = s.prepareForwardFrontRecursiveQuery(expansionModel)
	)

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
	s.applyShortestPathSelfEndpointGuard(&projectionQuery, expansionModel)

	if harnessParameters, err := s.shortestPathsParameters(expansionModel, forwardFrontPrimerQuery, forwardFrontRecursiveQuery); err != nil {
		return pgsql.Query{}, err
	} else {
		query := pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
			Body:                   projectionQuery,
		}

		query.AddCTE(shortestPathSearchCTE(harnessFunctionName, expansionModel, harnessParameters))
		return query, nil
	}
}

func (s *ExpansionBuilder) BuildShortestPathsRoot() (pgsql.Query, error) {
	return s.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalSPHarness)
}

func (s *ExpansionBuilder) BuildAllShortestPathsRoot() (pgsql.Query, error) {
	return s.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalASPHarness)
}

func (s *ExpansionBuilder) canMaterializeTerminalFilter(expansionModel *Expansion) bool {
	if expansionModel.TerminalNodeConstraints == nil || s.usesBoundEndpointPairs() || s.usesBoundTerminalIDs() {
		return false
	}

	// Terminal filters are only useful as standalone SQL when they depend solely
	// on the terminal node; external references must stay in the main query.
	_, externalConstraints := partitionConstraintByLocality(
		expansionModel.TerminalNodeConstraints,
		pgsql.AsIdentifierSet(s.traversalStep.RightNode.Identifier),
	)

	return externalConstraints == nil
}

func (s *ExpansionBuilder) canMaterializeEndpointPairFilter(expansionModel *Expansion) bool {
	// Pair filters enumerate the exact root/terminal combinations the
	// bidirectional harness must resolve. Kind-only endpoint predicates are not
	// enough because they do not constrain the search columns used by the harness.
	if s.usesBoundEndpointPairs() ||
		expansionModel.PrimerNodeConstraints == nil ||
		expansionModel.TerminalNodeConstraints == nil ||
		!hasLocalEndpointConstraint(expansionModel.PrimerNodeConstraints, s.traversalStep.LeftNode.Identifier) ||
		!hasLocalEndpointConstraint(expansionModel.TerminalNodeConstraints, s.traversalStep.RightNode.Identifier) {
		return false
	}

	return true
}

func (s *ExpansionBuilder) buildBiDirectionalShortestPathsHarnessCall(harnessFunctionName pgsql.Identifier) (pgsql.Query, error) {
	var (
		expansionModel  = s.traversalStep.Expansion
		projectionQuery pgsql.Select
	)

	expansionModel.UseMaterializedEndpointPairFilter = s.canMaterializeEndpointPairFilter(expansionModel)

	var (
		forwardFrontPrimerQuery, forwardSeedProjectionConstraints   = s.prepareForwardFrontPrimerQuery(expansionModel)
		forwardFrontRecursiveQuery                                  = s.prepareForwardFrontRecursiveQuery(expansionModel)
		backwardFrontPrimerQuery, backwardSeedProjectionConstraints = s.prepareBackwardFrontPrimerQuery(expansionModel)
		backwardFrontRecursiveQuery                                 = s.prepareBackwardFrontRecursiveQuery(expansionModel)
	)

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
	s.applyShortestPathSelfEndpointGuard(&projectionQuery, expansionModel)

	if harnessParameters, err := s.bidirectionalAllShortestPathsParameters(expansionModel, forwardFrontPrimerQuery, forwardFrontRecursiveQuery, backwardFrontPrimerQuery, backwardFrontRecursiveQuery); err != nil {
		return pgsql.Query{}, err
	} else {
		query := pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
			Body:                   projectionQuery,
		}

		query.AddCTE(shortestPathSearchCTE(harnessFunctionName, expansionModel, harnessParameters))
		return query, nil
	}
}

func (s *ExpansionBuilder) BuildBiDirectionalShortestPathsRoot() (pgsql.Query, error) {
	return s.buildBiDirectionalShortestPathsHarnessCall(pgsql.FunctionBidirectionalSPHarness)
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
		if formattedFilter, err := format.Statement(pairFilterStatement, format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters)); err != nil {
			return nil, err
		} else {
			pairFilter = formattedFilter
		}
	} else if hasRootFilter {
		if formattedFilter, err := format.Statement(rootFilterStatement, format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters)); err != nil {
			return nil, err
		} else {
			rootFilter = formattedFilter
		}
	}

	if !hasPairFilter && hasTerminalFilter {
		if formattedFilter, err := format.Statement(terminalFilterStatement, format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters)); err != nil {
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
				format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters))
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

func (s *ExpansionBuilder) bidirectionalAllShortestPathsParameters(expansionModel *Expansion, forwardFrontPrimerQuery pgsql.SetExpression, forwardFrontRecursiveQuery pgsql.SetExpression, backwardFrontPrimerQuery pgsql.SetExpression, backwardFrontRecursiveQuery pgsql.SetExpression) ([]pgsql.Expression, error) {
	var (
		harnessParameters []pgsql.Expression
		formatFragment    = func(query pgsql.SetExpression) (string, error) {
			return format.Statement(
				nextFrontInsert(query),
				format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters))
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

	if filterParameters, err := s.boundEndpointFilterParameters(); err != nil {
		return nil, err
	} else {
		harnessParameters = append(harnessParameters, filterParameters...)
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

func (s *Translator) buildExpansionPatternRoot(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder) (pgsql.Query, error) {
	var (
		traversalStep  = traversalStepContext.CurrentStep
		expansionModel = traversalStep.Expansion
		seedIdentifier = expansionSeedIdentifier(expansionModel.Frame.Binding.Identifier)
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

	seedConstraints := pgsql.OptionalAnd(primerLocal, primerExternal)
	var seed *expansionSeed

	if traversalStep.LeftNodeBound {
		if traversalStep.Frame.Previous == nil {
			return pgsql.Query{}, fmt.Errorf("left node is marked as bound but there is no previous frame to reference")
		}

		boundSeed := newExpansionBoundNodeSeed(seedIdentifier, traversalStep.Frame.Previous, traversalStep.LeftNode.Identifier, seedConstraints)
		seed = &boundSeed
		expansion.UseUnionAll = true
	} else if seedConstraints != nil {
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

	// Select the expansion components for the projection statement
	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{
			expansionNodeLookupJoin(
				traversalStep.LeftNode.Identifier,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
			),
			expansionNodeLookupJoin(
				traversalStep.RightNode.Identifier,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
			),
		},
	})

	if projectionConstraints, err := s.buildExpansionProjectionConstraints(traversalStepContext); err != nil {
		return pgsql.Query{}, err
	} else {
		if previousProjectionFrameID != "" && traversalStep.LeftNodeBound {
			projectionConstraints = pgsql.OptionalAnd(
				projectionConstraints,
				boundEndpointProjectionConstraint(
					previousProjectionFrameID,
					traversalStep.LeftNode.Identifier,
					expansionModel.Frame.Binding.Identifier,
					expansionRootID,
				),
			)
		}

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
			traversalStep.LeftNode.Identifier,
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
				traversalStep.LeftNode.Identifier,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
			),
			expansionNodeLookupJoin(
				traversalStep.RightNode.Identifier,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
			),
		},
	})

	if projectionConstraints, err := s.buildExpansionProjectionConstraints(traversalStepContext); err != nil {
		return pgsql.Query{}, err
	} else {
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
			pgsql.RowColumnReference{
				Identifier: pgsql.CompoundIdentifier{previousStep.Frame.Binding.Identifier, currentStep.LeftNode.Identifier},
				Column:     pgsql.ColumnID,
			},
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

	return projectionConstraints, nil
}

func (s *Translator) translateTraversalPatternPartWithExpansion(part *PatternPart, isFirstTraversalStep bool, traversalStep *TraversalStep) error {
	expansionModel := traversalStep.Expansion

	// Translate the expansion's constraints - this has the side effect of making the pattern identifiers visible in
	// the current scope frame
	if err := s.translateExpansionConstraints(isFirstTraversalStep, traversalStep, expansionModel); err != nil {
		return err
	}

	// Export the path from the traversal's scope
	traversalStep.Frame.Export(expansionModel.PathBinding.Identifier)
	pruneExpansionStepProjectionExports(s.query.CurrentPart(), part, traversalStep)

	// Push a new frame that contains currently projected scope from the expansion recursive CTE
	if expansionFrame, err := s.scope.PushFrame(); err != nil {
		return err
	} else {
		expansionModel.Frame = expansionFrame
	}

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

	if expansionModel.Options.FindShortestPath || expansionModel.Options.FindAllShortestPaths {
		if err := s.translateShortestPathTraversal(traversalStep, expansionModel); err != nil {
			return err
		}
	}

	return nil
}

func (s *Translator) translateExpansionConstraints(isFirstTraversalStep bool, step *TraversalStep, expansionModel *Expansion) error {
	if constraints, err := consumePatternConstraints(isFirstTraversalStep, recursivePattern, step, s.treeTranslator); err != nil {
		return err
	} else {
		// If one side of the expansion has constraints but the other does not this may be an opportunity to reorder the traversal
		// to start with tighter search bounds
		if err := constraints.OptimizePatternConstraintBalance(s.scope, step); err != nil {
			return err
		}

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

func (s *Translator) translateShortestPathTraversal(traversalStep *TraversalStep, expansionModel *Expansion) error {
	var (
		useBidirectionalSearch bool
		err                    error
	)

	useBidirectionalSearch, err = traversalStep.CanExecutePairAwareBidirectionalSearch(s.scope)

	if err != nil {
		return err
	}

	expansionModel.UseBidirectionalSearch = useBidirectionalSearch
	expansionModel.HasExplicitEndpointInequality = s.treeTranslator.HasEndpointInequality(
		traversalStep.LeftNode.Identifier,
		traversalStep.RightNode.Identifier,
	)

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

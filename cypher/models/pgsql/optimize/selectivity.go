package optimize

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

const (
	// Below are a select set of constants to represent different weights to represent, roughly, the selectivity
	// of a given PGSQL expression. These weights are meant to be inexact and are only useful in comparison to other
	// summed weights.
	//
	// The goal of these weights are to enable reordering of queries such that the more selective side of a traversal
	// step is expanded first. Eventually, these weights may also enable reordering of multipart queries.

	// Entity ID references are a safe selectivity bet. A direct reference will typically take the form of:
	// `n0.id = 1` or some other direct comparison against the entity's ID. All entity IDs are covered by a unique
	// b-tree index, making them both highly selective and lucrative to weight higher.
	selectivityWeightEntityIDReference = 125

	// Unique node properties are both covered by a compatible index and unique, making them highly selective.
	selectivityWeightUniqueNodeProperty = 100

	// Bound identifiers are heavily weighted for preserving join order integrity.
	selectivityWeightBoundIdentifier = 700

	// Operators that narrow the search space are given a higher selectivity.
	selectivityWeightNarrowSearch = 30

	// Operators that perform string searches are given a higher selectivity.
	selectivityWeightStringSearch = 20

	// Operators that perform range comparisons are reasonably selective.
	selectivityWeightRangeComparison = 10

	// Conjunctions can narrow search space, especially when compounded, but may be order dependent and unreliable as
	// a good selectivity heuristic.
	selectivityWeightConjunction = 5

	// Exclusions can narrow the search space but often only slightly.
	selectivityWeightNotEquals = 1

	// Disjunctions expand search space by adding a secondary, conditional operation.
	selectivityWeightDisjunction = -100

	// selectivityFlipThreshold is the minimum score advantage the right-hand node must hold
	// over the left-hand node before constraint balancing commits to a traversal direction flip.
	// It is set to selectivityWeightNarrowSearch so that structural AST noise, in particular the
	// per-AND-node conjunction bonus, cannot trigger a flip on its own. A single meaningful
	// narrowing predicate (=, IN, kind filter) on the right side is sufficient to clear this
	// bar; a bare AND connector (weight 5) or a range comparison on an unindexed property
	// (weight 10) is not.
	selectivityFlipThreshold = selectivityWeightNarrowSearch

	// selectivityBidirectionalAnchorThreshold is the minimum score each endpoint must carry
	// before shortest-path translation starts a bidirectional search from both sides. This
	// keeps broad label-only endpoints out of bidirectional BFS; a single kind predicate
	// scores below this threshold, while a materially narrower property predicate can clear it.
	selectivityBidirectionalAnchorThreshold = selectivityWeightNarrowSearch * 2
)

// knownNodePropertySelectivity is a hack to enable the selectivity measurement to take advantage of known property
// indexes or uniqueness constraints.
//
// Eventually, this should be replaced by a tool that can introspect a graph schema and derive this map.
var knownNodePropertySelectivity = map[string]int{
	"objectid":    selectivityWeightUniqueNodeProperty, // Object ID contains a unique constraint giving this a high degree of selectivity.
	"name":        selectivityWeightUniqueNodeProperty, // Name contains a unique constraint giving this a high degree of selectivity.
	"system_tags": selectivityWeightNarrowSearch,       // Searches that use the system_tags property are likely to have a higher degree of selectivity.
}

type BindingLookup interface {
	LookupDataType(identifier pgsql.Identifier) (pgsql.DataType, bool)
}

type SelectivityModel struct {
	bindings BindingLookup
}

func NewSelectivityModel(bindings BindingLookup) SelectivityModel {
	return SelectivityModel{
		bindings: bindings,
	}
}

type propertyLookup struct {
	reference pgsql.CompoundIdentifier
	field     string
}

type measureSelectivityVisitor struct {
	walk.Visitor[pgsql.SyntaxNode]

	model            SelectivityModel
	selectivityStack []int
}

func newMeasureSelectivityVisitor(model SelectivityModel) *measureSelectivityVisitor {
	return &measureSelectivityVisitor{
		Visitor:          walk.NewVisitor[pgsql.SyntaxNode](),
		model:            model,
		selectivityStack: []int{0},
	}
}

func (s *measureSelectivityVisitor) Selectivity() int {
	return s.selectivityStack[0]
}

func (s *measureSelectivityVisitor) popSelectivity() int {
	value := s.selectivityStack[len(s.selectivityStack)-1]
	s.selectivityStack = s.selectivityStack[:len(s.selectivityStack)-1]

	return value
}

func (s *measureSelectivityVisitor) pushSelectivity(value int) {
	s.selectivityStack = append(s.selectivityStack, value)
}

func (s *measureSelectivityVisitor) addSelectivity(value int) {
	if len(s.selectivityStack) == 0 {
		s.pushSelectivity(value)
	} else {
		s.selectivityStack[len(s.selectivityStack)-1] += value
	}
}

func isColumnIDRef(expression pgsql.Expression) bool {
	switch typedExpression := expression.(type) {
	case pgsql.CompoundIdentifier:
		if typedExpression.HasField() {
			switch typedExpression.Field() {
			case pgsql.ColumnID:
				return true
			}
		}
	}

	return false
}

func binaryExpressionToPropertyLookup(expression *pgsql.BinaryExpression) (propertyLookup, error) {
	if reference, typeOK := expression.LOperand.(pgsql.CompoundIdentifier); !typeOK {
		return propertyLookup{}, fmt.Errorf("expected left operand for property lookup to be a compound identifier but found type: %T", expression.LOperand)
	} else if field, typeOK := expression.ROperand.(pgsql.Literal); !typeOK {
		return propertyLookup{}, fmt.Errorf("expected right operand for property lookup to be a literal but found type: %T", expression.ROperand)
	} else if field.CastType != pgsql.Text {
		return propertyLookup{}, fmt.Errorf("expected property lookup field a string literal but found data type: %s", field.CastType)
	} else if stringField, typeOK := field.Value.(string); !typeOK {
		return propertyLookup{}, fmt.Errorf("expected property lookup field a string literal but found data type: %T", field)
	} else {
		return propertyLookup{
			reference: reference,
			field:     stringField,
		}, nil
	}
}

func (s *measureSelectivityVisitor) Enter(node pgsql.SyntaxNode) {
	switch typedNode := node.(type) {
	case *pgsql.UnaryExpression:
		switch typedNode.Operator {
		case pgsql.OperatorNot:
			s.pushSelectivity(0)
		}

	case *pgsql.BinaryExpression:
		var (
			lOperandIsID = isColumnIDRef(typedNode.LOperand)
			rOperandIsID = isColumnIDRef(typedNode.ROperand)
		)

		if lOperandIsID && !rOperandIsID {
			// Point lookup: n0.id = <literal or param>; highly selective.
			s.addSelectivity(selectivityWeightEntityIDReference)
		} else if rOperandIsID && !lOperandIsID {
			// Canonically unusual, but handle it the same.
			s.addSelectivity(selectivityWeightEntityIDReference)
		}

		// If both sides are ID refs, this is a join condition; do not score as a point lookup.
		switch typedNode.Operator {
		case pgsql.OperatorOr:
			s.addSelectivity(selectivityWeightDisjunction)

		case pgsql.OperatorNotEquals:
			s.addSelectivity(selectivityWeightNotEquals)

		case pgsql.OperatorAnd:
			s.addSelectivity(selectivityWeightConjunction)

		case pgsql.OperatorLessThan, pgsql.OperatorGreaterThan, pgsql.OperatorLessThanOrEqualTo, pgsql.OperatorGreaterThanOrEqualTo:
			s.addSelectivity(selectivityWeightRangeComparison)

		case pgsql.OperatorLike, pgsql.OperatorILike, pgsql.OperatorRegexMatch, pgsql.OperatorSimilarTo:
			s.addSelectivity(selectivityWeightStringSearch)

		case pgsql.OperatorIn, pgsql.OperatorEquals, pgsql.OperatorIs:
			s.addSelectivity(selectivityWeightNarrowSearch)

		case pgsql.OperatorPGArrayOverlap, pgsql.OperatorArrayOverlap:
			s.addSelectivity(selectivityWeightNarrowSearch)

		case pgsql.OperatorPGArrayLHSContainsRHS:
			// @> is strictly more selective than &&: all kind_ids must be present.
			s.addSelectivity(selectivityWeightNarrowSearch + selectivityWeightConjunction)

		case pgsql.OperatorJSONField, pgsql.OperatorJSONTextField, pgsql.OperatorPropertyLookup:
			if propertyLookup, err := binaryExpressionToPropertyLookup(typedNode); err != nil {
				s.SetError(err)
			} else {
				leftIdentifier := propertyLookup.reference.Root()
				if s.model.bindings == nil {
					return
				}

				if dataType, bound := s.model.bindings.LookupDataType(leftIdentifier); !bound {
					s.SetErrorf("unable to lookup identifier %s", leftIdentifier)
				} else {
					switch dataType {
					case pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode, pgsql.NodeComposite:
						if selectivity, hasKnownSelectivity := knownNodePropertySelectivity[propertyLookup.field]; hasKnownSelectivity {
							s.addSelectivity(selectivity)
						}
					}
				}
			}
		}
	}
}

func (s *measureSelectivityVisitor) Exit(node pgsql.SyntaxNode) {
	switch typedNode := node.(type) {
	case *pgsql.UnaryExpression:
		switch typedNode.Operator {
		case pgsql.OperatorNot:
			selectivity := s.popSelectivity()
			s.addSelectivity(-selectivity)
		}
	}
}

func (s SelectivityModel) Measure(expression pgsql.Expression) (int, error) {
	visitor := newMeasureSelectivityVisitor(s)

	if expression != nil {
		if err := walk.PgSQL(expression, visitor); err != nil {
			return 0, err
		}
	}

	return visitor.Selectivity(), nil
}

func (s SelectivityModel) ShouldFlipTraversalDirection(leftBound, rightBound bool, leftExpression, rightExpression pgsql.Expression) (bool, error) {
	if leftBound {
		return false, nil
	}

	if rightBound {
		return true, nil
	}

	leftSelectivity, err := s.Measure(leftExpression)
	if err != nil {
		return false, err
	}

	rightSelectivity, err := s.Measure(rightExpression)
	if err != nil {
		return false, err
	}

	return rightSelectivity-leftSelectivity >= selectivityFlipThreshold, nil
}

func (s SelectivityModel) EndpointSelectivity(expression pgsql.Expression, bound, hasPreviousFrameBinding bool) (int, error) {
	selectivity, err := s.Measure(expression)
	if err != nil {
		return 0, err
	}

	if bound && hasPreviousFrameBinding {
		selectivity += selectivityWeightBoundIdentifier
	}

	return selectivity, nil
}

func MeasureSelectivity(bindings BindingLookup, expression pgsql.Expression) (int, error) {
	return NewSelectivityModel(bindings).Measure(expression)
}

func IsBidirectionalSearchAnchor(selectivity int) bool {
	return selectivity >= selectivityBidirectionalAnchorThreshold
}

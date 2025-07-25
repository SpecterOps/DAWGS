package ops

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cardinality"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type LimitSkipTracker struct {
	Limit int
	seen  int
	Skip  int
}

func (s *LimitSkipTracker) AtLimit() bool {
	return s.Limit > 0 && s.seen >= s.Limit
}

func (s *LimitSkipTracker) ShouldCollect() bool {
	// If a skip was set then we tick down until s.Skip is no longer greater than 0
	if s.Skip > 0 {
		s.Skip--
		return false
	}

	// If a limit was set, and we're not yet at it, increment the seen count and then return true
	if !s.AtLimit() {
		s.seen++
		return true
	}

	// Otherwise, we are at our limit and should no longer collect
	return false
}

var (
	ErrHaltTraversal         = errors.New("halt traversal")
	ErrGraphQueryMemoryLimit = errors.New("graph query required more memory than allowed")
)

// PathFilter is invoked on completed paths identified during a graph traversal. It may return a boolean value
// representing if the path was consumed. If consumed, a rendered path is then tracked for traversal plan limit
// specifications.
type PathFilter func(ctx *TraversalContext, segment *graph.PathSegment) bool

// PathVisitor is invoked on completed paths identified during a graph traversal. It may return an error value in the
// case where a fatal error condition has been encountered, rendering further traversal moot.
type PathVisitor func(ctx *TraversalContext, segment *graph.PathSegment) error

// DepthExceptionHandler is invoked on paths that exceed depth traversal plan depth limits.
type DepthExceptionHandler func(ctx *TraversalContext, segment *graph.PathSegment)

type SegmentFilter func(ctx *TraversalContext, segment *graph.PathSegment) bool

type NodeFilter func(node *graph.Node) bool

type TraversalPlan struct {
	Root                  *graph.Node
	Direction             graph.Direction
	BranchQuery           graph.CriteriaProvider
	DepthExceptionHandler DepthExceptionHandler
	ExpansionFilter       func(segment *graph.PathSegment) bool
	DescentFilter         SegmentFilter
	PathFilter            PathFilter
	Skip                  int
	Limit                 int
}

func nextTraversal(tx graph.Transaction, segment *graph.PathSegment, direction graph.Direction, branchFilter graph.CriteriaProvider, requireOrder bool, expansionFilter func(segment *graph.PathSegment) bool) ([]*graph.PathSegment, error) {
	var (
		branches []*graph.PathSegment
	)

	// If we don't want to expand the relationships for this segment, return an empty slice
	if expansionFilter != nil && !expansionFilter(segment) {
		return branches, nil
	}

	nextTraversalQuery := tx.Relationships().Filterf(func() graph.Criteria {
		var filters []graph.Criteria

		if branchFilter != nil {
			filters = append(filters, branchFilter())
		}

		switch direction {
		case graph.DirectionOutbound:
			filters = append(filters, query.InIDs(query.StartID(), segment.Node.ID))

		case graph.DirectionInbound:
			filters = append(filters, query.InIDs(query.EndID(), segment.Node.ID))
		}

		return query.And(
			filters...,
		)
	})

	if requireOrder {
		nextTraversalQuery.OrderBy(query.Order(query.Relationship(), query.Ascending()))
	}

	switch direction {
	case graph.DirectionOutbound:
		return branches, ForEachEndNode(nextTraversalQuery, func(relationship *graph.Relationship, node *graph.Node) error {
			branches = append(branches, segment.Descend(node, relationship))
			return nil
		})

	case graph.DirectionInbound:
		return branches, ForEachStartNode(nextTraversalQuery, func(relationship *graph.Relationship, node *graph.Node) error {
			branches = append(branches, segment.Descend(node, relationship))
			return nil
		})

	default:
		return nil, fmt.Errorf("invalid direction %d", direction)
	}
}

type TraversalContext struct {
	LimitSkipTracker LimitSkipTracker
}

func Traversal(tx graph.Transaction, plan TraversalPlan, pathVisitor PathVisitor) error {
	var (
		requireTraversalOrder = plan.Limit > 0 || plan.Skip > 0
		rootSegment           = graph.NewRootPathSegment(plan.Root)
		stack                 = []*graph.PathSegment{rootSegment}
		ctx                   *TraversalContext
	)

	ctx = &TraversalContext{
		LimitSkipTracker: LimitSkipTracker{
			Limit: plan.Limit,
			Skip:  plan.Skip,
		},
	}

	for len(stack) > 0 {
		next := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if tx.GraphQueryMemoryLimit() > 0 {
			if pathTreeSize := rootSegment.SizeOf(); pathTreeSize > tx.GraphQueryMemoryLimit() {
				return fmt.Errorf("%w - Limit: %.2f MB - Memory In-Use: %.2f MB", ErrGraphQueryMemoryLimit, tx.GraphQueryMemoryLimit().Mebibytes(), pathTreeSize.Mebibytes())
			}
		}

		if descendents, err := nextTraversal(tx, next, plan.Direction, plan.BranchQuery, requireTraversalOrder, plan.ExpansionFilter); err != nil {
			// If the error value is the halt traversal sentinel then don't relay any error upstream
			if errors.Is(err, ErrHaltTraversal) {
				break
			}

			return err
		} else {
			stackLengthBeforeDescent := len(stack)

			if plan.DescentFilter != nil {
				// If there's a descent filter specified we need to unwind all possible descent candidates and test
				// them. To avoid the annoying additional memory pressure that comes from the `range` keyword we iterate
				// in-place.
				for idx := 0; idx < len(descendents); idx++ {
					if nextDescendent := descendents[idx]; plan.DescentFilter(ctx, nextDescendent) {
						stack = append(stack, nextDescendent)
					}
				}
			} else {
				// No filter means descend into all potential paths
				stack = append(stack, descendents...)
			}

			// If this node does not have descendents then it's a path terminal
			if pathVisitor != nil && stackLengthBeforeDescent == len(stack) && next.Depth() > 0 && (plan.PathFilter == nil || plan.PathFilter(ctx, next)) {
				if err := pathVisitor(ctx, next); err != nil {
					return err
				}
			}
		}

		// Break if we're at our limit
		if ctx.LimitSkipTracker.AtLimit() {
			break
		}
	}

	return nil
}

// TraverseIntermediaryPaths NodeFilter is used to select candidate nodes for adding to the results
func TraverseIntermediaryPaths(tx graph.Transaction, plan TraversalPlan, nodeFilter NodeFilter) (graph.PathSet, error) {
	var (
		paths         = graph.NewPathSet()
		descentFilter = plan.DescentFilter
	)

	plan.DescentFilter = func(ctx *TraversalContext, segment *graph.PathSegment) bool {
		if descentFilter != nil && !descentFilter(ctx, segment) {
			return false
		}

		if nodeFilter(segment.Node) && ctx.LimitSkipTracker.ShouldCollect() {
			paths.AddPath(segment.Path())
		}

		return true
	}

	return paths, Traversal(tx, plan, nil)
}

// AcyclicTraverseNodes Does a traversal, but includes nodes that are intermediaries and terminals
func AcyclicTraverseNodes(tx graph.Transaction, plan TraversalPlan, nodeFilter NodeFilter) (graph.NodeSet, error) {
	var (
		nodes         = graph.NewNodeSet()
		descentFilter = plan.DescentFilter
		visitedBitmap = cardinality.NewBitmap64()
	)

	// Prevent expansion of already-visited nodes
	plan.ExpansionFilter = func(segment *graph.PathSegment) bool {
		return visitedBitmap.CheckedAdd(segment.Node.ID.Uint64())
	}

	// Wrap our descent filter so we can test candidates
	plan.DescentFilter = func(ctx *TraversalContext, segment *graph.PathSegment) bool {
		if descentFilter != nil && !descentFilter(ctx, segment) {
			return false
		}

		if (nodeFilter == nil || nodeFilter(segment.Node)) && ctx.LimitSkipTracker.ShouldCollect() {
			nodes.Add(segment.Node)
		}

		return true
	}

	//Remember to test our root node as well
	if nodeFilter == nil || nodeFilter(plan.Root) {
		nodes.Add(plan.Root)
	}

	return nodes, Traversal(tx, plan, nil)
}

func AcyclicTraverseTerminals(tx graph.Transaction, plan TraversalPlan) (graph.NodeSet, error) {
	var (
		terminals     = graph.NewNodeSet()
		visitedBitmap = cardinality.NewBitmap64()
	)

	// Prevent expansion of already-visited nodes
	plan.ExpansionFilter = func(segment *graph.PathSegment) bool {
		return visitedBitmap.CheckedAdd(segment.Node.ID.Uint64())
	}

	return terminals, Traversal(tx, plan, func(ctx *TraversalContext, segment *graph.PathSegment) error {
		if ctx.LimitSkipTracker.ShouldCollect() {
			// Add the path terminal
			terminals.Add(segment.Node)
		}

		return nil
	})
}

func TraversePaths(tx graph.Transaction, plan TraversalPlan) (graph.PathSet, error) {
	var (
		paths         = graph.NewPathSet()
		descentFilter = plan.DescentFilter
	)

	// Wrap the existing descent filter to avoid revisiting nodes during traversal
	plan.DescentFilter = func(ctx *TraversalContext, segment *graph.PathSegment) bool {
		if descentFilter == nil || descentFilter(ctx, segment) {
			return !segment.IsCycle()
		}

		return false
	}

	return paths, Traversal(tx, plan, func(ctx *TraversalContext, segment *graph.PathSegment) error {
		if ctx.LimitSkipTracker.ShouldCollect() {
			paths.AddPath(segment.Path())
		}

		return nil
	})
}

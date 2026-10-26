package traversal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// The DAWGS-074 regressions pin direction-changing pattern steps. The pattern
// driver formerly reused the current expansion's fetch direction for the next
// expansion, so an Outbound step followed by an Inbound step (a→b←c) fetched
// the matched edge's end node (the current node itself) instead of its start
// node. Cycle filtering then dropped every descendant and the pattern never
// matched.
//
// The DAWGS-095 regressions pin optional following steps. The driver formerly
// eager-fetched an optional following expansion and also preserved the
// boundary segment, so the optional expansion ran twice and every terminal
// beneath it was duplicated in callback cardinality.

type scriptedFetch struct {
	// edges represents the relationships matching this fetch's criteria
	edges []*graph.Relationship

	// err is surfaced through the cursor after all edges are consumed
	err error
}

type fetchObservation struct {
	direction graph.Direction
	edges     []*graph.Relationship
}

// patternRelationshipQuery serves scripted fetches in call order. Like a real
// database, the fetch direction selects which endpoint node is returned for
// each matched relationship: inbound yields the end node, outbound the start
// node.
type patternRelationshipQuery struct {
	graph.RelationshipQuery

	nodes    map[graph.ID]*graph.Node
	script   []scriptedFetch
	observed []fetchObservation
}

func (s *patternRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	return s
}

func (s *patternRelationshipQuery) FetchDirection(direction graph.Direction, delegate func(cursor graph.Cursor[graph.DirectionalResult]) error) error {
	if len(s.script) == 0 {
		return fmt.Errorf("unexpected FetchDirection call: no scripted fetch remains")
	}

	next := s.script[0]
	s.script = s.script[1:]
	s.observed = append(s.observed, fetchObservation{direction: direction, edges: next.edges})

	results := make([]graph.DirectionalResult, 0, len(next.edges))
	for _, edge := range next.edges {
		endpointID, err := direction.PickID(edge.StartID, edge.EndID)
		if err != nil {
			return err
		}

		node, ok := s.nodes[endpointID]
		if !ok {
			return fmt.Errorf("scripted fetch references unknown node %d", endpointID)
		}

		results = append(results, graph.NewDirectionalResult(direction, edge, node))
	}

	return delegate(newSliceCursor(results, next.err))
}

type patternTransaction struct {
	graph.Transaction

	query *patternRelationshipQuery
}

func (s *patternTransaction) Relationships() graph.RelationshipQuery {
	return s.query
}

type sliceCursor[T any] struct {
	values chan T
	err    error
}

func newSliceCursor[T any](values []T, err error) *sliceCursor[T] {
	cursor := &sliceCursor[T]{
		values: make(chan T, len(values)),
		err:    err,
	}

	for _, value := range values {
		cursor.values <- value
	}

	close(cursor.values)
	return cursor
}

func (s *sliceCursor[T]) Error() error { return s.err }
func (s *sliceCursor[T]) Close()       {}
func (s *sliceCursor[T]) Chan() chan T { return s.values }

type patternHarness struct {
	tx        *patternTransaction
	terminals []graph.Path
}

func newPatternHarness(t *testing.T, nodes []*graph.Node, script []scriptedFetch) *patternHarness {
	t.Helper()

	nodeByID := make(map[graph.ID]*graph.Node, len(nodes))
	for _, node := range nodes {
		nodeByID[node.ID] = node
	}

	return &patternHarness{
		tx: &patternTransaction{
			query: &patternRelationshipQuery{
				nodes:  nodeByID,
				script: script,
			},
		},
		terminals: nil,
	}
}

func (s *patternHarness) drive(t *testing.T, driver Driver, root *graph.PathSegment) error {
	t.Helper()

	queue := []*graph.PathSegment{root}

	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]

		descendants, err := driver(context.Background(), s.tx, next)
		if err != nil {
			return err
		}

		queue = append(queue, descendants...)
	}

	if len(s.tx.query.script) != 0 {
		t.Fatalf("%d scripted fetches were never consumed", len(s.tx.query.script))
	}

	return nil
}

func (s *patternHarness) requireFetches(t *testing.T, expected []graph.Direction) {
	t.Helper()

	observed := make([]graph.Direction, 0, len(s.tx.query.observed))
	for _, observation := range s.tx.query.observed {
		observed = append(observed, observation.direction)
	}

	require.Equal(t, expected, observed)
}

func requireNodeIDs(t *testing.T, path graph.Path, expected ...graph.ID) {
	t.Helper()

	got := make([]graph.ID, 0, len(path.Nodes))
	for _, node := range path.Nodes {
		got = append(got, node.ID)
	}

	require.Equal(t, expected, got)
}

func requireEdgeIDs(t *testing.T, path graph.Path, expected ...graph.ID) {
	t.Helper()

	got := make([]graph.ID, 0, len(path.Edges))
	for _, edge := range path.Edges {
		got = append(got, edge.ID)
	}

	require.Equal(t, expected, got)
}

func patternNode(id graph.ID) *graph.Node {
	return graph.NewNode(id, nil, kindA)
}

func patternEdge(id, start, end graph.ID) *graph.Relationship {
	return graph.NewRelationship(id, start, end, nil, kindR)
}

// TestPatternDriverDirectionChangeOutboundInbound reproduces the recorded
// trigger: OutboundWithDepth(1,1).InboundWithDepth(1,1) over a→b←c. Before the
// repair the second fetch reused the outbound step's inbound fetch direction,
// fetched b itself for the c→b edge and filtered it as a cycle, so no terminal
// path was ever produced.
func TestPatternDriverDirectionChangeOutboundInbound(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 2, 1)}},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			InboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
}

// TestPatternDriverDirectionChangeInboundOutbound covers the mirrored change:
// InboundWithDepth(1,1).OutboundWithDepth(1,1) over a←b→c. The outbound step
// must fetch the end nodes of the edges starting at b; the b→a edge yields a
// genuine cycle that is filtered while the b→c edge completes the path.
func TestPatternDriverDirectionChangeInboundOutbound(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 1, 0)}},
			{edges: []*graph.Relationship{patternEdge(10, 1, 0), patternEdge(11, 1, 2)}},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			InboundWithDepth(1, 1).
			OutboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionOutbound, graph.DirectionInbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
}

// TestPatternDriverSameDirectionControl verifies that a pattern whose steps
// keep the same direction is unaffected by the direction-change repair.
func TestPatternDriverSameDirectionControl(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 1, 2)}},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			OutboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionInbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
}

// TestPatternDriverAlternatingThreeSteps chains Outbound→Inbound→Outbound over
// a→b←c→d so each fetch after the first uses the following step's direction.
func TestPatternDriverAlternatingThreeSteps(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2), patternNode(3)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 2, 1)}},
			{edges: []*graph.Relationship{patternEdge(12, 2, 3)}},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			InboundWithDepth(1, 1).
			OutboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound, graph.DirectionInbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2, 3)
	requireEdgeIDs(t, harness.terminals[0], 10, 11, 12)
}

// TestPatternDriverDirectionChangeIntoRange performs the direction change into
// a second step with its own depth range (min 1, max 2) over a→b←c←d, checking
// that the changed-direction step continues expanding with its own bounds.
func TestPatternDriverDirectionChangeIntoRange(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2), patternNode(3)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 2, 1)}},
			{edges: []*graph.Relationship{patternEdge(12, 3, 2)}},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			InboundWithDepth(1, 2).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound, graph.DirectionOutbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2, 3)
	requireEdgeIDs(t, harness.terminals[0], 10, 11, 12)
}

// TestPatternDriverOptionalStepDuplicatesCallbacks reproduces the recorded
// DAWGS-095 trigger: OutboundWithDepth(1,1).OutboundWithDepth(0,0) over
// a→b→c. The optional preservation branch formerly re-queued b while the eager
// next-step fetch had already queued c, so the optional expansion ran twice
// and the delegate received terminal c twice (IDs [2 2], five fetches).
func TestPatternDriverOptionalStepDuplicatesCallbacks(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 1, 2)}},
			{},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			OutboundWithDepth(0, 0).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionInbound, graph.DirectionInbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
}

// TestPatternDriverOptionalStepWithoutMatch verifies that an optional step
// with no matches hands the preserved boundary segment to the delegate exactly
// once.
func TestPatternDriverOptionalStepWithoutMatch(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			OutboundWithDepth(0, 0).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionInbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1)
	requireEdgeIDs(t, harness.terminals[0], 10)
}

// TestPatternDriverOptionalStepMultipleMatches verifies that each match of an
// optional step is reported exactly once when several edges satisfy the
// optional expansion. Before the repair each matched terminal was reported
// twice.
func TestPatternDriverOptionalStepMultipleMatches(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2), patternNode(3)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{
				edges: []*graph.Relationship{
					patternEdge(11, 1, 2),
					patternEdge(12, 1, 3),
				},
			},
			{},
			{},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			OutboundWithDepth(0, 0).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))

	require.Len(t, harness.terminals, 2)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
	requireNodeIDs(t, harness.terminals[1], 0, 1, 3)
	requireEdgeIDs(t, harness.terminals[1], 10, 12)
}

// TestPatternDriverOptionalMiddleStep chains a required step, an optional
// step and a required step over a→b→c→d. The optional step's matches and its
// preserved boundary each continue through the required tail exactly once;
// before the repair the tail terminal was duplicated by the optional step's
// second expansion.
func TestPatternDriverOptionalMiddleStep(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2), patternNode(3)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 1, 2)}},
			{edges: []*graph.Relationship{patternEdge(11, 1, 2)}},
			{edges: []*graph.Relationship{patternEdge(12, 2, 3)}},
			{edges: []*graph.Relationship{patternEdge(12, 2, 3)}},
			{},
			{},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			OutboundWithDepth(0, 0).
			OutboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))

	require.Len(t, harness.terminals, 2)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
	requireNodeIDs(t, harness.terminals[1], 0, 1, 2, 3)
	requireEdgeIDs(t, harness.terminals[1], 10, 11, 12)
}

// TestPatternDriverOptionalDirectionChange checks that the preserved boundary
// segment of an optional direction-changed following step fetches with that
// step's own fetch direction and completes the path with exact callback
// cardinality.
func TestPatternDriverOptionalDirectionChange(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1), patternNode(2)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{edges: []*graph.Relationship{patternEdge(11, 2, 1)}},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			InboundWithDepth(0, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound})

	require.Len(t, harness.terminals, 1)
	requireNodeIDs(t, harness.terminals[0], 0, 1, 2)
	requireEdgeIDs(t, harness.terminals[0], 10, 11)
}

// TestPatternDriverRequiredSecondStepWithoutMatch verifies that a direction
// change with no matching second-step edge ends the path without invoking the
// terminal delegate.
func TestPatternDriverRequiredSecondStepWithoutMatch(t *testing.T) {
	var (
		nodes = []*graph.Node{patternNode(0), patternNode(1)}

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			InboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	require.NoError(t, harness.drive(t, driver, graph.NewRootPathSegment(nodes[0])))
	harness.requireFetches(t, []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound})
	require.Empty(t, harness.terminals)
}

// TestPatternDriverFetchErrorPropagation verifies that cursor errors from a
// direction-changed following step's fetch surface from the driver.
func TestPatternDriverFetchErrorPropagation(t *testing.T) {
	var (
		nodes    = []*graph.Node{patternNode(0), patternNode(1), patternNode(2)}
		fetchErr = errors.New("directional fetch failed")

		script = []scriptedFetch{
			{edges: []*graph.Relationship{patternEdge(10, 0, 1)}},
			{err: fetchErr},
		}

		harness = newPatternHarness(t, nodes, script)
		driver  = NewPattern().
			OutboundWithDepth(1, 1).
			InboundWithDepth(1, 1).
			Do(func(terminal *graph.PathSegment) error {
				harness.terminals = append(harness.terminals, terminal.Path())
				return nil
			})
	)

	err := harness.drive(t, driver, graph.NewRootPathSegment(nodes[0]))
	require.ErrorIs(t, err, fetchErr)
	require.Empty(t, harness.terminals)
}

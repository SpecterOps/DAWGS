package sonic

import (
	"github.com/specterops/dawgs/graph"
)

// --- NodeQuery ---

type nodeQuery struct {
	db      *Database
	filters []graph.Criteria
}

func (q *nodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	q.filters = append(q.filters, criteria)
	return q
}

func (q *nodeQuery) Filterf(delegate graph.CriteriaProvider) graph.NodeQuery {
	return q.Filter(delegate())
}

func (q *nodeQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	for _, c := range finalCriteria {
		q.filters = append(q.filters, c)
	}
	nodes := q.collect()
	rows := make([][]any, len(nodes))
	for i, n := range nodes {
		rows[i] = []any{n}
	}
	return delegate(&sonicResult{rows: rows, keys: []string{"n"}})
}

func (q *nodeQuery) Delete() error {
	nodes := q.collect()

	q.db.mu.Lock()
	defer q.db.mu.Unlock()

	for _, n := range nodes {
		// Delete attached edges
		for _, edgeID := range q.db.outEdges[n.ID] {
			delete(q.db.edges, edgeID)
		}
		for _, edgeID := range q.db.inEdges[n.ID] {
			delete(q.db.edges, edgeID)
		}
		delete(q.db.outEdges, n.ID)
		delete(q.db.inEdges, n.ID)
		delete(q.db.nodes, n.ID)
	}
	return nil
}

func (q *nodeQuery) Update(properties *graph.Properties) error {
	nodes := q.collect()

	q.db.mu.Lock()
	defer q.db.mu.Unlock()

	for _, n := range nodes {
		for key, val := range properties.Map {
			n.Properties.Set(key, val)
		}
		for key := range properties.Deleted {
			n.Properties.Delete(key)
		}
		q.db.nodes[n.ID] = n
	}
	return nil
}

func (q *nodeQuery) OrderBy(criteria ...graph.Criteria) graph.NodeQuery {
	// TODO: implement ordering
	return q
}

func (q *nodeQuery) Offset(skip int) graph.NodeQuery {
	// TODO: implement offset
	return q
}

func (q *nodeQuery) Limit(limit int) graph.NodeQuery {
	// TODO: implement limit
	return q
}

func (q *nodeQuery) Count() (int64, error) {
	return int64(len(q.collect())), nil
}

func (q *nodeQuery) First() (*graph.Node, error) {
	nodes := q.collect()
	if len(nodes) == 0 {
		return nil, graph.ErrNoResultsFound
	}
	return nodes[0], nil
}

func (q *nodeQuery) Fetch(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error {
	nodes := q.collect()
	ch := make(chan *graph.Node, len(nodes))
	for _, n := range nodes {
		ch <- n
	}
	close(ch)
	return delegate(&sliceCursor[*graph.Node]{ch: ch})
}

func (q *nodeQuery) FetchIDs(delegate func(cursor graph.Cursor[graph.ID]) error) error {
	nodes := q.collect()
	ch := make(chan graph.ID, len(nodes))
	for _, n := range nodes {
		ch <- n.ID
	}
	close(ch)
	return delegate(&sliceCursor[graph.ID]{ch: ch})
}

func (q *nodeQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.KindsResult]) error) error {
	nodes := q.collect()
	ch := make(chan graph.KindsResult, len(nodes))
	for _, n := range nodes {
		ch <- graph.KindsResult{ID: n.ID, Kinds: n.Kinds}
	}
	close(ch)
	return delegate(&sliceCursor[graph.KindsResult]{ch: ch})
}

func (q *nodeQuery) collect() []*graph.Node {
	q.db.mu.RLock()
	defer q.db.mu.RUnlock()

	var results []*graph.Node
	for _, n := range q.db.nodes {
		if q.matchNode(n) {
			results = append(results, n)
		}
	}
	return results
}

func (q *nodeQuery) matchNode(n *graph.Node) bool {
	if len(q.filters) == 0 {
		return true
	}
	for _, f := range q.filters {
		if !evalNodeCriteria(q.db, n, f) {
			return false
		}
	}
	return true
}

// --- RelationshipQuery ---

type relQuery struct {
	db      *Database
	filters []graph.Criteria
}

func (q *relQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	q.filters = append(q.filters, criteria)
	return q
}

func (q *relQuery) Filterf(delegate graph.CriteriaProvider) graph.RelationshipQuery {
	return q.Filter(delegate())
}

func (q *relQuery) Update(properties *graph.Properties) error {
	rels := q.collect()

	q.db.mu.Lock()
	defer q.db.mu.Unlock()

	for _, r := range rels {
		for key, val := range properties.Map {
			r.Properties.Set(key, val)
		}
		for key := range properties.Deleted {
			r.Properties.Delete(key)
		}
		q.db.edges[r.ID] = r
	}
	return nil
}

func (q *relQuery) Delete() error {
	rels := q.collect()

	q.db.mu.Lock()
	defer q.db.mu.Unlock()

	for _, r := range rels {
		delete(q.db.edges, r.ID)
		q.db.outEdges[r.StartID] = removeID(q.db.outEdges[r.StartID], r.ID)
		q.db.inEdges[r.EndID] = removeID(q.db.inEdges[r.EndID], r.ID)
	}
	return nil
}

func (q *relQuery) OrderBy(criteria ...graph.Criteria) graph.RelationshipQuery {
	return q
}

func (q *relQuery) Offset(skip int) graph.RelationshipQuery {
	return q
}

func (q *relQuery) Limit(limit int) graph.RelationshipQuery {
	return q
}

func (q *relQuery) Count() (int64, error) {
	return int64(len(q.collect())), nil
}

func (q *relQuery) First() (*graph.Relationship, error) {
	rels := q.collect()
	if len(rels) == 0 {
		return nil, graph.ErrNoResultsFound
	}
	return rels[0], nil
}

func (q *relQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	for _, c := range finalCriteria {
		q.filters = append(q.filters, c)
	}

	rels := q.collect()

	q.db.mu.RLock()
	rows := make([][]any, 0, len(rels))
	for _, r := range rels {
		startNode := q.db.nodes[r.StartID]
		endNode := q.db.nodes[r.EndID]
		rows = append(rows, []any{startNode, r, endNode})
	}
	q.db.mu.RUnlock()

	return delegate(&sonicResult{rows: rows, keys: []string{"s", "r", "e"}})
}

func (q *relQuery) Fetch(delegate func(cursor graph.Cursor[*graph.Relationship]) error) error {
	rels := q.collect()
	ch := make(chan *graph.Relationship, len(rels))
	for _, r := range rels {
		ch <- r
	}
	close(ch)
	return delegate(&sliceCursor[*graph.Relationship]{ch: ch})
}

func (q *relQuery) FetchDirection(direction graph.Direction, delegate func(cursor graph.Cursor[graph.DirectionalResult]) error) error {
	rels := q.collect()
	ch := make(chan graph.DirectionalResult, len(rels))
	for _, r := range rels {
		var node *graph.Node
		switch direction {
		case graph.DirectionOutbound:
			node = q.db.nodes[r.EndID]
		case graph.DirectionInbound:
			node = q.db.nodes[r.StartID]
		}
		ch <- graph.DirectionalResult{
			Direction:    direction,
			Relationship: r,
			Node:         node,
		}
	}
	close(ch)
	return delegate(&sliceCursor[graph.DirectionalResult]{ch: ch})
}

func (q *relQuery) FetchIDs(delegate func(cursor graph.Cursor[graph.ID]) error) error {
	rels := q.collect()
	ch := make(chan graph.ID, len(rels))
	for _, r := range rels {
		ch <- r.ID
	}
	close(ch)
	return delegate(&sliceCursor[graph.ID]{ch: ch})
}

func (q *relQuery) FetchTriples(delegate func(cursor graph.Cursor[graph.RelationshipTripleResult]) error) error {
	rels := q.collect()
	ch := make(chan graph.RelationshipTripleResult, len(rels))
	for _, r := range rels {
		ch <- graph.RelationshipTripleResult{ID: r.ID, StartID: r.StartID, EndID: r.EndID}
	}
	close(ch)
	return delegate(&sliceCursor[graph.RelationshipTripleResult]{ch: ch})
}

func (q *relQuery) FetchAllShortestPaths(delegate func(cursor graph.Cursor[graph.Path]) error) error {
	pc := extractPathConstraints(q.filters)

	q.db.mu.RLock()
	paths := q.db.bfsAllShortestPaths(pc)
	q.db.mu.RUnlock()

	ch := make(chan graph.Path, len(paths))
	for _, p := range paths {
		ch <- p
	}
	close(ch)
	return delegate(&sliceCursor[graph.Path]{ch: ch})
}

func (q *relQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.RelationshipKindsResult]) error) error {
	rels := q.collect()
	ch := make(chan graph.RelationshipKindsResult, len(rels))
	for _, r := range rels {
		ch <- graph.RelationshipKindsResult{
			RelationshipTripleResult: graph.RelationshipTripleResult{ID: r.ID, StartID: r.StartID, EndID: r.EndID},
			Kind:                     r.Kind,
		}
	}
	close(ch)
	return delegate(&sliceCursor[graph.RelationshipKindsResult]{ch: ch})
}

func (q *relQuery) collect() []*graph.Relationship {
	q.db.mu.RLock()
	defer q.db.mu.RUnlock()

	var results []*graph.Relationship
	for _, r := range q.db.edges {
		if q.matchRel(r) {
			results = append(results, r)
		}
	}
	return results
}

func (q *relQuery) matchRel(r *graph.Relationship) bool {
	if len(q.filters) == 0 {
		return true
	}
	for _, f := range q.filters {
		if !evalRelCriteria(q.db, r, f) {
			return false
		}
	}
	return true
}

// --- Cursor ---

type sliceCursor[T any] struct {
	ch  chan T
	err error
}

func (c *sliceCursor[T]) Chan() chan T {
	return c.ch
}

func (c *sliceCursor[T]) Error() error {
	return c.err
}

func (c *sliceCursor[T]) Close() {
}

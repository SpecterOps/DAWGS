package sonic

import (
	cypher "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// pathConstraints holds the extracted constraints from query filters for pathfinding.
type pathConstraints struct {
	startIDs  map[graph.ID]struct{}
	endIDs    map[graph.ID]struct{}
	edgeKinds map[graph.Kind]struct{} // nil means all kinds allowed
}

// extractPathConstraints walks the filter criteria to pull out start node IDs,
// end node IDs, and allowed edge kinds.
func extractPathConstraints(filters []graph.Criteria) pathConstraints {
	pc := pathConstraints{
		startIDs: make(map[graph.ID]struct{}),
		endIDs:   make(map[graph.ID]struct{}),
	}

	for _, f := range filters {
		extractFromCriteria(f, &pc)
	}

	return pc
}

func extractFromCriteria(criteria graph.Criteria, pc *pathConstraints) {
	switch c := criteria.(type) {
	case *cypher.Conjunction:
		for _, expr := range c.Expressions {
			extractFromCriteria(expr, pc)
		}

	case *cypher.Parenthetical:
		extractFromCriteria(c.Expression, pc)

	case *cypher.Comparison:
		extractFromComparison(c, pc)

	case *cypher.KindMatcher:
		extractFromKindMatcher(c, pc)
	}
}

func extractFromComparison(cmp *cypher.Comparison, pc *pathConstraints) {
	if len(cmp.Partials) == 0 {
		return
	}

	partial := cmp.Partials[0]

	// We're looking for patterns like: id(s) IN [...] or id(e) IN [...]
	if partial.Operator != cypher.OperatorIn && partial.Operator != cypher.OperatorEquals {
		return
	}

	// Check if the left side is id(s) or id(e)
	funcInv, ok := cmp.Left.(*cypher.FunctionInvocation)
	if !ok || funcInv.Name != "id" || len(funcInv.Arguments) == 0 {
		return
	}

	v, ok := funcInv.Arguments[0].(*cypher.Variable)
	if !ok {
		return
	}

	var targetSet map[graph.ID]struct{}
	switch v.Symbol {
	case query.EdgeStartSymbol:
		targetSet = pc.startIDs
	case query.EdgeEndSymbol:
		targetSet = pc.endIDs
	default:
		return
	}

	// Extract the IDs from the right side
	if partial.Operator == cypher.OperatorEquals {
		if param, ok := partial.Right.(*cypher.Parameter); ok {
			if id, ok := toID(param.Value); ok {
				targetSet[id] = struct{}{}
			}
		} else if lit, ok := partial.Right.(*cypher.Literal); ok {
			if id, ok := toID(lit.Value); ok {
				targetSet[id] = struct{}{}
			}
		}
		return
	}

	// OperatorIn — right side is a Parameter whose Value is a slice of IDs
	if param, ok := partial.Right.(*cypher.Parameter); ok {
		extractIDs(param.Value, targetSet)
	} else if lit, ok := partial.Right.(*cypher.Literal); ok {
		extractIDs(lit.Value, targetSet)
	}
}

func extractIDs(val any, target map[graph.ID]struct{}) {
	switch ids := val.(type) {
	case []graph.ID:
		for _, id := range ids {
			target[id] = struct{}{}
		}
	case []int64:
		for _, id := range ids {
			target[graph.ID(id)] = struct{}{}
		}
	case []uint64:
		for _, id := range ids {
			target[graph.ID(id)] = struct{}{}
		}
	case []any:
		for _, v := range ids {
			if id, ok := toID(v); ok {
				target[id] = struct{}{}
			}
		}
	}
}

func extractFromKindMatcher(km *cypher.KindMatcher, pc *pathConstraints) {
	v, ok := km.Reference.(*cypher.Variable)
	if !ok || v.Symbol != query.EdgeSymbol {
		return
	}

	if pc.edgeKinds == nil {
		pc.edgeKinds = make(map[graph.Kind]struct{})
	}
	for _, k := range km.Kinds {
		pc.edgeKinds[k] = struct{}{}
	}
}

// bfsAllShortestPaths finds all shortest paths from any start node to any end node.
// It uses bidirectional-style BFS: standard BFS from all start nodes simultaneously,
// stopping at the depth where we first reach an end node. All paths at that depth are returned.
func (db *Database) bfsAllShortestPaths(pc pathConstraints) []graph.Path {
	if len(pc.startIDs) == 0 || len(pc.endIDs) == 0 {
		return nil
	}

	type bfsEntry struct {
		nodeID graph.ID
		depth  int
	}

	// parents[nodeID] = list of (parent, edge) pairs that reach nodeID at shortest depth
	parents := make(map[graph.ID][]parentInfo)
	// shortest depth at which each node was discovered
	depthOf := make(map[graph.ID]int)

	queue := make([]bfsEntry, 0, 256)

	// Seed BFS with all start nodes
	for startID := range pc.startIDs {
		if _, exists := db.nodes[startID]; !exists {
			continue
		}
		queue = append(queue, bfsEntry{nodeID: startID, depth: 0})
		depthOf[startID] = 0
	}

	foundDepth := -1 // depth at which we first hit an end node
	reachedEnds := make(map[graph.ID]struct{})

	for len(queue) > 0 {
		entry := queue[0]
		queue = queue[1:]

		// If we already found paths and this entry is deeper, stop
		if foundDepth >= 0 && entry.depth > foundDepth {
			break
		}

		// Expand outgoing edges
		for _, edgeID := range db.outEdges[entry.nodeID] {
			edge := db.edges[edgeID]
			if edge == nil {
				continue
			}

			// Check edge kind constraint
			if pc.edgeKinds != nil {
				if _, allowed := pc.edgeKinds[edge.Kind]; !allowed {
					continue
				}
			}

			neighborID := edge.EndID
			neighborDepth := entry.depth + 1

			// If we've already found paths at a shorter depth, skip deeper exploration
			if foundDepth >= 0 && neighborDepth > foundDepth {
				continue
			}

			prevDepth, visited := depthOf[neighborID]
			if visited && prevDepth < neighborDepth {
				// Already reached at a shorter depth, skip
				continue
			}

			if !visited || prevDepth == neighborDepth {
				// First visit or same depth — record this parent
				if !visited {
					depthOf[neighborID] = neighborDepth
					queue = append(queue, bfsEntry{nodeID: neighborID, depth: neighborDepth})
				}
				parents[neighborID] = append(parents[neighborID], parentInfo{
					parentNodeID: entry.nodeID,
					edgeID:       edgeID,
				})

				// Check if we reached an end node
				if _, isEnd := pc.endIDs[neighborID]; isEnd {
					foundDepth = neighborDepth
					reachedEnds[neighborID] = struct{}{}
				}
			}
		}
	}

	if len(reachedEnds) == 0 {
		return nil
	}

	// Reconstruct all shortest paths by backtracking from end nodes
	var paths []graph.Path
	for endID := range reachedEnds {
		paths = append(paths, db.reconstructPaths(endID, parents)...)
	}

	return paths
}

// reconstructPaths backtracks from endID through the parents map to build all shortest paths.
func (db *Database) reconstructPaths(endID graph.ID, parents map[graph.ID][]parentInfo) []graph.Path {
	type partial struct {
		nodeIDs []graph.ID
		edgeIDs []graph.ID
	}

	// Start with the end node and work backwards
	current := []partial{{nodeIDs: []graph.ID{endID}}}

	for {
		var next []partial
		allDone := true

		for _, p := range current {
			headNode := p.nodeIDs[len(p.nodeIDs)-1]
			pInfos := parents[headNode]

			if len(pInfos) == 0 {
				// Reached a start node — this partial is complete
				next = append(next, p)
				continue
			}

			allDone = false
			for _, pi := range pInfos {
				newNodeIDs := make([]graph.ID, len(p.nodeIDs)+1)
				copy(newNodeIDs, p.nodeIDs)
				newNodeIDs[len(p.nodeIDs)] = pi.parentNodeID

				newEdgeIDs := make([]graph.ID, len(p.edgeIDs)+1)
				copy(newEdgeIDs, p.edgeIDs)
				newEdgeIDs[len(p.edgeIDs)] = pi.edgeID

				next = append(next, partial{nodeIDs: newNodeIDs, edgeIDs: newEdgeIDs})
			}
		}

		current = next
		if allDone {
			break
		}
	}

	// Convert partials to graph.Path (reverse since we built them end-to-start)
	paths := make([]graph.Path, 0, len(current))
	for _, p := range current {
		path := graph.Path{
			Nodes: make([]*graph.Node, len(p.nodeIDs)),
			Edges: make([]*graph.Relationship, len(p.edgeIDs)),
		}

		// Reverse nodes (they were built end→start)
		for i, nid := range p.nodeIDs {
			path.Nodes[len(p.nodeIDs)-1-i] = db.nodes[nid]
		}
		// Reverse edges
		for i, eid := range p.edgeIDs {
			path.Edges[len(p.edgeIDs)-1-i] = db.edges[eid]
		}

		paths = append(paths, path)
	}

	return paths
}

type parentInfo struct {
	parentNodeID graph.ID
	edgeID       graph.ID
}

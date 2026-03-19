package sonic

import (
	"fmt"
	"sort"
	"strings"

	cypher "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

const maxBindings = 100000

// binding represents a single row in the binding table: variable name → value.
type binding map[string]any

// copyBinding creates a shallow copy of a binding row.
func copyBinding(b binding) binding {
	c := make(binding, len(b))
	for k, v := range b {
		c[k] = v
	}
	return c
}

// matchState tracks state for a single MATCH clause.
type matchState struct {
	snapshot []binding // pre-MATCH bindings for OPTIONAL MATCH fallback
	optional bool
	newVars  []string // variables introduced by this MATCH
}

// projItem holds a parsed projection item.
type projItem struct {
	expr  cypher.Expression
	alias string
}

// returnContext holds RETURN/WITH clause modifiers.
type returnContext struct {
	limit    int
	skip     int
	distinct bool
	orderBy  []*cypher.SortItem
}

// executor walks a Cypher AST and evaluates it against the in-memory database.
type executor struct {
	walk.Visitor[cypher.SyntaxNode]

	db       *Database
	bindings []binding
	params   map[string]any
	result   *sonicResult

	// Clause-level state
	matchStack []matchState
	projItems  []projItem
	returnCtx  *returnContext
	inReturn   bool
	inWith     bool

	// Track the current PatternPart for allShortestPaths detection
	currentPatternPart *cypher.PatternPart

	// Track current match for resolveParameters
	currentMatch *cypher.Match

	// Set when allShortestPaths has consumed the WHERE for path constraints
	whereConsumedByPath bool
}

func newExecutor(db *Database, params map[string]any) *executor {
	return &executor{
		Visitor:  walk.NewVisitor[cypher.SyntaxNode](),
		db:       db,
		bindings: []binding{{}}, // seed with one empty binding row
		params:   params,
	}
}

// Enter is called on first visit to an AST node.
func (ex *executor) Enter(node cypher.SyntaxNode) {
	switch n := node.(type) {
	case *cypher.RegularQuery:
		// top-level — no action

	case *cypher.SingleQuery:
		// no action

	case *cypher.SinglePartQuery:
		// no action

	case *cypher.MultiPartQuery:
		// no action

	case *cypher.MultiPartQueryPart:
		// no action

	case *cypher.ReadingClause:
		// no action

	case *cypher.Match:
		ex.currentMatch = n
		// Resolve parameters in WHERE clause
		if n.Where != nil {
			resolveParameters(n.Where, ex.params)
		}
		ms := matchState{
			optional: n.Optional,
		}
		if n.Optional {
			ms.snapshot = make([]binding, len(ex.bindings))
			for i, b := range ex.bindings {
				ms.snapshot[i] = copyBinding(b)
			}
		}
		ex.matchStack = append(ex.matchStack, ms)

	case *cypher.PatternPart:
		ex.currentPatternPart = n
		// If allShortestPaths, consume — we'll handle it entirely on Exit
		if n.AllShortestPathsPattern {
			ex.Consume()
		}

	case *cypher.PatternElement:
		// no action — walker descends to NodePattern/RelationshipPattern

	case *cypher.NodePattern:
		// no action — expansion happens in Exit

	case *cypher.RelationshipPattern:
		// no action — expansion happens in Exit

	case *cypher.Where:
		// Consume — we don't want the walker to descend into expression children.
		// We evaluate expression trees ourselves in Exit(Where).
		ex.Consume()

	case *cypher.Return:
		ex.inReturn = true
		ex.projItems = nil
		ex.returnCtx = &returnContext{}

	case *cypher.With:
		ex.inWith = true
		ex.projItems = nil
		ex.returnCtx = &returnContext{}

	case *cypher.Projection:
		// Consume — we handle projection items ourselves
		ex.Consume()
		ex.parseProjection(n)

	// --- Leaf / expression nodes: no-op in walker ---
	case *cypher.Variable, *cypher.Literal, *cypher.Parameter,
		*cypher.Comparison, *cypher.Conjunction, *cypher.Disjunction,
		*cypher.Negation, *cypher.Parenthetical, *cypher.KindMatcher,
		*cypher.FunctionInvocation, *cypher.PropertyLookup,
		*cypher.PartialComparison, *cypher.ArithmeticExpression,
		*cypher.PartialArithmeticExpression, *cypher.UnaryAddOrSubtractExpression,
		*cypher.ProjectionItem, *cypher.Order, *cypher.SortItem,
		*cypher.Skip, *cypher.Limit, *cypher.RangeQuantifier,
		cypher.Operator, graph.Kinds, cypher.MapLiteral, *cypher.ListLiteral,
		*cypher.ExclusiveDisjunction, *cypher.PatternPredicate:
		// no-op

	// --- Mutation nodes: error ---
	case *cypher.UpdatingClause:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.Create:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.Delete:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.Set:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.SetItem:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.Remove:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.RemoveItem:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.Merge:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	case *cypher.MergeAction:
		ex.SetErrorf("sonic: write operations in Cypher not yet supported")

	// --- Unsupported constructs ---
	case *cypher.Unwind:
		ex.SetErrorf("sonic: UNWIND not yet supported")

	case *cypher.Quantifier:
		ex.SetErrorf("sonic: quantifier expressions not yet supported")

	case *cypher.FilterExpression:
		ex.SetErrorf("sonic: filter expressions not yet supported")

	case *cypher.IDInCollection:
		ex.SetErrorf("sonic: ID IN collection not yet supported")

	case *cypher.MapItem:
		// no-op (used inside Properties)

	case *cypher.Properties:
		// no-op

	default:
		ex.SetErrorf("sonic: unsupported cypher construct: %T", node)
	}
}

// Visit is called when returning to a node after processing a child.
func (ex *executor) Visit(node cypher.SyntaxNode) {
	// no-op for all nodes
}

// Exit is called after all children of a node have been processed.
func (ex *executor) Exit(node cypher.SyntaxNode) {
	switch n := node.(type) {
	case *cypher.NodePattern:
		ex.expandNodePattern(n)

	case *cypher.RelationshipPattern:
		ex.expandRelationshipPattern(n)

	case *cypher.PatternPart:
		if n.AllShortestPathsPattern {
			ex.handleAllShortestPaths(n)
		}
		ex.currentPatternPart = nil

	case *cypher.Where:
		ex.filterBindings(n)

	case *cypher.Match:
		ex.finalizeMatch()
		ex.currentMatch = nil

	case *cypher.With:
		ex.finalizeWith()
		ex.inWith = false

	case *cypher.Return:
		ex.finalizeReturn()
		ex.inReturn = false

	case *cypher.SinglePartQuery:
		ex.finalizeResult()

	case *cypher.MultiPartQueryPart:
		// bindings carry forward — no special action needed
	}
}

// parseProjection extracts projection items and modifiers from a Projection node.
func (ex *executor) parseProjection(proj *cypher.Projection) {
	if proj == nil {
		return
	}

	if proj.Distinct {
		ex.returnCtx.distinct = true
	}

	// Parse items
	if proj.All {
		// RETURN * — project all bound variables
		// We'll handle this in finalizeReturn/finalizeWith
	} else {
		for _, item := range proj.Items {
			if pi, ok := item.(*cypher.ProjectionItem); ok {
				alias := ""
				if pi.Alias != nil {
					alias = pi.Alias.Symbol
				} else if v, ok := pi.Expression.(*cypher.Variable); ok {
					alias = v.Symbol
				} else if pl, ok := pi.Expression.(*cypher.PropertyLookup); ok {
					alias = pl.Symbol
				}
				ex.projItems = append(ex.projItems, projItem{
					expr:  pi.Expression,
					alias: alias,
				})
			}
		}
	}

	// Parse ORDER BY
	if proj.Order != nil {
		ex.returnCtx.orderBy = proj.Order.Items
	}

	// Parse SKIP
	if proj.Skip != nil {
		if lit, ok := proj.Skip.Value.(*cypher.Literal); ok {
			ex.returnCtx.skip = toInt(lit.Value)
		}
	}

	// Parse LIMIT
	if proj.Limit != nil {
		if lit, ok := proj.Limit.Value.(*cypher.Literal); ok {
			ex.returnCtx.limit = toInt(lit.Value)
		}
	}
}

func toInt(v any) int {
	switch tv := v.(type) {
	case int:
		return tv
	case int64:
		return int(tv)
	case float64:
		return int(tv)
	default:
		return 0
	}
}

// anonNodeCounter is used to generate unique keys for anonymous node patterns.
var anonNodeCounter int

// expandNodePattern expands bindings against matching nodes.
func (ex *executor) expandNodePattern(np *cypher.NodePattern) {
	varName := ""
	if np.Variable != nil {
		varName = np.Variable.Symbol
	} else {
		// Anonymous node — use a synthetic binding key so findAnchorNode can find it
		anonNodeCounter++
		varName = fmt.Sprintf("__anon_node_%d__", anonNodeCounter)
	}

	ex.db.mu.RLock()
	defer ex.db.mu.RUnlock()

	var expanded []binding

	for _, row := range ex.bindings {
		// If the variable is already bound, just filter — don't expand
		if varName != "" {
			if existing, ok := row[varName]; ok {
				if node, ok := existing.(*graph.Node); ok {
					if ex.nodeMatchesPattern(node, np) {
						expanded = append(expanded, row)
					}
					continue
				}
			}
		}

		// If __next_node__ is set (from a preceding relationship expansion),
		// bind from it instead of scanning all nodes.
		if nextNode, ok := row["__next_node__"]; ok {
			if node, ok := nextNode.(*graph.Node); ok {
				if ex.nodeMatchesPattern(node, np) {
					newRow := copyBinding(row)
					delete(newRow, "__next_node__")
					if varName != "" {
						newRow[varName] = node
					}
					expanded = append(expanded, newRow)
				}
				continue
			}
		}

		// Expand against all nodes
		for _, node := range ex.db.nodes {
			if !ex.nodeMatchesPattern(node, np) {
				continue
			}

			newRow := copyBinding(row)
			if varName != "" {
				newRow[varName] = node
			}
			expanded = append(expanded, newRow)

			if len(expanded) > maxBindings {
				ex.SetErrorf("sonic: binding count exceeded %d — query too broad", maxBindings)
				return
			}
		}
	}

	ex.bindings = expanded

	// Track new variable in match state
	if varName != "" && len(ex.matchStack) > 0 {
		ms := &ex.matchStack[len(ex.matchStack)-1]
		ms.newVars = append(ms.newVars, varName)
	}
}

// nodeMatchesPattern checks if a node satisfies a NodePattern's constraints.
func (ex *executor) nodeMatchesPattern(node *graph.Node, np *cypher.NodePattern) bool {
	if len(np.Kinds) > 0 {
		// Node patterns use AND semantics — node must have ALL specified kinds
		for _, k := range np.Kinds {
			if !node.Kinds.ContainsOneOf(k) {
				return false
			}
		}
	}
	return true
}

// expandRelationshipPattern expands bindings by following edges.
func (ex *executor) expandRelationshipPattern(rp *cypher.RelationshipPattern) {
	// Variable-length paths: (a)-[r*1..3]->(b)
	if rp.Range != nil {
		ex.expandVariableLengthPattern(rp)
		return
	}

	relVar := ""
	if rp.Variable != nil {
		relVar = rp.Variable.Symbol
	}

	ex.db.mu.RLock()
	defer ex.db.mu.RUnlock()

	var expanded []binding

	for _, row := range ex.bindings {
		// Find the most recently bound node in this row to use as the anchor
		anchorNode := ex.findAnchorNode(row)
		if anchorNode == nil {
			// No anchor — expand from all edges (first element in pattern was a rel?)
			continue
		}

		// Get candidate edges based on direction
		var edgeIDs []graph.ID
		switch rp.Direction {
		case graph.DirectionOutbound:
			edgeIDs = ex.db.outEdges[anchorNode.ID]
		case graph.DirectionInbound:
			edgeIDs = ex.db.inEdges[anchorNode.ID]
		default: // DirectionBoth
			edgeIDs = append(edgeIDs, ex.db.outEdges[anchorNode.ID]...)
			edgeIDs = append(edgeIDs, ex.db.inEdges[anchorNode.ID]...)
		}

		for _, edgeID := range edgeIDs {
			edge := ex.db.edges[edgeID]
			if edge == nil {
				continue
			}

			// Check kind constraints (disjunction for relationships)
			if len(rp.Kinds) > 0 {
				if !rp.Kinds.ContainsOneOf(edge.Kind) {
					continue
				}
			}

			// Determine the other end node
			var otherNode *graph.Node
			switch rp.Direction {
			case graph.DirectionOutbound:
				otherNode = ex.db.nodes[edge.EndID]
			case graph.DirectionInbound:
				otherNode = ex.db.nodes[edge.StartID]
			default: // DirectionBoth
				if edge.StartID == anchorNode.ID {
					otherNode = ex.db.nodes[edge.EndID]
				} else {
					otherNode = ex.db.nodes[edge.StartID]
				}
			}

			if otherNode == nil {
				continue
			}

			newRow := copyBinding(row)
			if relVar != "" {
				newRow[relVar] = edge
			}
			// The other end node will be bound by the next NodePattern
			// Store it temporarily so the next NodePattern can pick it up
			newRow["__next_node__"] = otherNode
			expanded = append(expanded, newRow)

			if len(expanded) > maxBindings {
				ex.SetErrorf("sonic: binding count exceeded %d — query too broad", maxBindings)
				return
			}
		}
	}

	ex.bindings = expanded

	if relVar != "" && len(ex.matchStack) > 0 {
		ms := &ex.matchStack[len(ex.matchStack)-1]
		ms.newVars = append(ms.newVars, relVar)
	}
}

const maxVarLengthDepth = 50

// expandVariableLengthPattern handles relationship patterns with a range like [*1..3].
// It performs BFS from the anchor node, collecting all paths whose length falls
// within [minHops, maxHops]. Each valid path produces a new binding row with:
//   - relVar → []*graph.Relationship (the edges traversed)
//   - __next_node__ → the terminal node
func (ex *executor) expandVariableLengthPattern(rp *cypher.RelationshipPattern) {
	minHops := int64(1)
	maxHops := int64(maxVarLengthDepth)

	if rp.Range.StartIndex != nil {
		minHops = *rp.Range.StartIndex
	}
	if rp.Range.EndIndex != nil {
		maxHops = *rp.Range.EndIndex
	}
	if maxHops > maxVarLengthDepth {
		maxHops = maxVarLengthDepth
	}
	if minHops < 0 {
		minHops = 0
	}

	relVar := ""
	if rp.Variable != nil {
		relVar = rp.Variable.Symbol
	}

	ex.db.mu.RLock()
	defer ex.db.mu.RUnlock()

	var expanded []binding

	for _, row := range ex.bindings {
		anchorNode := ex.findAnchorNode(row)
		if anchorNode == nil {
			continue
		}

		// BFS with path tracking. Each entry is a (nodeID, path-of-edges) pair.
		type bfsState struct {
			nodeID graph.ID
			edges  []*graph.Relationship
		}

		queue := []bfsState{{nodeID: anchorNode.ID}}
		// visited tracks the set of nodes in the *current* path to prevent cycles.
		// We rebuild this per-path, so we use the queue entries themselves.

		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]

			depth := int64(len(cur.edges))

			// If within valid range, emit a binding
			if depth >= minHops {
				terminalNode := ex.db.nodes[cur.nodeID]
				if terminalNode != nil {
					newRow := copyBinding(row)
					if relVar != "" {
						// Bind as slice of relationships
						edgeCopy := make([]*graph.Relationship, len(cur.edges))
						copy(edgeCopy, cur.edges)
						newRow[relVar] = edgeCopy
					}
					newRow["__next_node__"] = terminalNode
					expanded = append(expanded, newRow)

					if len(expanded) > maxBindings {
						ex.SetErrorf("sonic: binding count exceeded %d — query too broad", maxBindings)
						return
					}
				}
			}

			// If at max depth, don't expand further
			if depth >= maxHops {
				continue
			}

			// Collect nodes already in this path to prevent cycles
			visited := make(map[graph.ID]struct{}, len(cur.edges)+1)
			visited[anchorNode.ID] = struct{}{}
			for _, e := range cur.edges {
				visited[e.StartID] = struct{}{}
				visited[e.EndID] = struct{}{}
			}

			// Expand neighbors
			var edgeIDs []graph.ID
			switch rp.Direction {
			case graph.DirectionOutbound:
				edgeIDs = ex.db.outEdges[cur.nodeID]
			case graph.DirectionInbound:
				edgeIDs = ex.db.inEdges[cur.nodeID]
			default:
				edgeIDs = append(edgeIDs, ex.db.outEdges[cur.nodeID]...)
				edgeIDs = append(edgeIDs, ex.db.inEdges[cur.nodeID]...)
			}

			for _, edgeID := range edgeIDs {
				edge := ex.db.edges[edgeID]
				if edge == nil {
					continue
				}

				// Kind filter (disjunction)
				if len(rp.Kinds) > 0 && !rp.Kinds.ContainsOneOf(edge.Kind) {
					continue
				}

				// Determine neighbor
				var neighborID graph.ID
				switch rp.Direction {
				case graph.DirectionOutbound:
					neighborID = edge.EndID
				case graph.DirectionInbound:
					neighborID = edge.StartID
				default:
					if edge.StartID == cur.nodeID {
						neighborID = edge.EndID
					} else {
						neighborID = edge.StartID
					}
				}

				// Cycle check — skip if neighbor already in path
				if _, inPath := visited[neighborID]; inPath {
					continue
				}

				newEdges := make([]*graph.Relationship, len(cur.edges)+1)
				copy(newEdges, cur.edges)
				newEdges[len(cur.edges)] = edge

				queue = append(queue, bfsState{
					nodeID: neighborID,
					edges:  newEdges,
				})
			}
		}
	}

	ex.bindings = expanded

	if relVar != "" && len(ex.matchStack) > 0 {
		ms := &ex.matchStack[len(ex.matchStack)-1]
		ms.newVars = append(ms.newVars, relVar)
	}
}

// findAnchorNode returns the most recently bound node in a binding row.
// It looks for the __next_node__ temporary or the last bound *graph.Node.
func (ex *executor) findAnchorNode(row binding) *graph.Node {
	// Check for __next_node__ left by a previous relationship expansion
	if n, ok := row["__next_node__"]; ok {
		if node, ok := n.(*graph.Node); ok {
			return node
		}
	}

	// Find last bound node — iterate over matchStack newVars in reverse
	if len(ex.matchStack) > 0 {
		ms := &ex.matchStack[len(ex.matchStack)-1]
		for i := len(ms.newVars) - 1; i >= 0; i-- {
			if v, ok := row[ms.newVars[i]]; ok {
				if node, ok := v.(*graph.Node); ok {
					return node
				}
			}
		}
	}

	// Fallback: find any bound node (last one added)
	var lastNode *graph.Node
	for _, v := range row {
		if node, ok := v.(*graph.Node); ok {
			lastNode = node
		}
	}
	return lastNode
}

// filterBindings applies WHERE expressions to filter binding rows.
func (ex *executor) filterBindings(where *cypher.Where) {
	if where == nil || len(where.Expressions) == 0 {
		return
	}

	// If allShortestPaths already consumed the WHERE for path constraints, skip
	if ex.whereConsumedByPath {
		ex.whereConsumedByPath = false
		return
	}

	var filtered []binding
	for _, row := range ex.bindings {
		matched := true
		for _, expr := range where.Expressions {
			if !evalBindingExpr(ex.db, row, expr) {
				matched = false
				break
			}
		}
		if matched {
			filtered = append(filtered, row)
		}
	}
	ex.bindings = filtered
}

// finalizeMatch pops the match state and handles OPTIONAL MATCH fallback.
func (ex *executor) finalizeMatch() {
	if len(ex.matchStack) == 0 {
		return
	}

	ms := ex.matchStack[len(ex.matchStack)-1]
	ex.matchStack = ex.matchStack[:len(ex.matchStack)-1]

	if ms.optional && len(ex.bindings) == 0 {
		// OPTIONAL MATCH: restore snapshot with nil-filled new variables
		restored := make([]binding, len(ms.snapshot))
		for i, b := range ms.snapshot {
			restored[i] = copyBinding(b)
			for _, v := range ms.newVars {
				restored[i][v] = nil
			}
		}
		ex.bindings = restored
	}

	// Clean up __next_node__ temporaries
	for _, row := range ex.bindings {
		delete(row, "__next_node__")
	}
}

// finalizeWith projects bindings through a WITH clause (scope barrier).
func (ex *executor) finalizeWith() {
	if ex.returnCtx == nil {
		return
	}

	// Apply ORDER BY before projection
	if len(ex.returnCtx.orderBy) > 0 {
		ex.sortBindings()
	}

	// Project bindings
	if len(ex.projItems) > 0 {
		projected := make([]binding, 0, len(ex.bindings))
		for _, row := range ex.bindings {
			newRow := make(binding)
			for _, item := range ex.projItems {
				val := resolveBindingValue(ex.db, row, item.expr)
				newRow[item.alias] = val
			}
			projected = append(projected, newRow)
		}
		ex.bindings = projected
	}

	// Apply DISTINCT
	if ex.returnCtx.distinct {
		ex.bindings = deduplicateBindings(ex.bindings, ex.projItemAliases())
	}

	// Apply SKIP
	if ex.returnCtx.skip > 0 && ex.returnCtx.skip < len(ex.bindings) {
		ex.bindings = ex.bindings[ex.returnCtx.skip:]
	} else if ex.returnCtx.skip >= len(ex.bindings) {
		ex.bindings = nil
	}

	// Apply LIMIT
	if ex.returnCtx.limit > 0 && len(ex.bindings) > ex.returnCtx.limit {
		ex.bindings = ex.bindings[:ex.returnCtx.limit]
	}

	ex.returnCtx = nil
	ex.projItems = nil
}

// finalizeReturn builds the sonicResult from current bindings.
func (ex *executor) finalizeReturn() {
	if ex.returnCtx == nil {
		return
	}

	// Apply ORDER BY before projection
	if len(ex.returnCtx.orderBy) > 0 {
		ex.sortBindings()
	}

	// Determine columns
	var keys []string
	if len(ex.projItems) > 0 {
		for _, item := range ex.projItems {
			keys = append(keys, item.alias)
		}
	} else {
		// RETURN * — use all bound variables
		keys = ex.allBoundVariables()
	}

	// Apply DISTINCT
	if ex.returnCtx.distinct {
		ex.bindings = deduplicateBindings(ex.bindings, keys)
	}

	// Apply SKIP
	if ex.returnCtx.skip > 0 && ex.returnCtx.skip < len(ex.bindings) {
		ex.bindings = ex.bindings[ex.returnCtx.skip:]
	} else if ex.returnCtx.skip >= len(ex.bindings) {
		ex.bindings = nil
	}

	// Apply LIMIT
	if ex.returnCtx.limit > 0 && len(ex.bindings) > ex.returnCtx.limit {
		ex.bindings = ex.bindings[:ex.returnCtx.limit]
	}

	// Build result rows
	rows := make([][]any, 0, len(ex.bindings))
	for _, row := range ex.bindings {
		vals := make([]any, len(keys))
		for i, k := range keys {
			if len(ex.projItems) > 0 {
				vals[i] = resolveBindingValue(ex.db, row, ex.projItems[i].expr)
			} else {
				vals[i] = row[k]
			}
		}
		rows = append(rows, vals)
	}

	ex.result = &sonicResult{rows: rows, keys: keys}
	ex.returnCtx = nil
	ex.projItems = nil
}

// finalizeResult sets a default empty result if none was built.
func (ex *executor) finalizeResult() {
	if ex.result == nil {
		ex.result = emptyResult()
	}
}

// handleAllShortestPaths handles allShortestPaths pattern parts.
func (ex *executor) handleAllShortestPaths(pp *cypher.PatternPart) {
	// Collect WHERE expressions from the current match
	var filters []graph.Criteria
	if ex.currentMatch != nil && ex.currentMatch.Where != nil {
		for _, expr := range ex.currentMatch.Where.Expressions {
			filters = append(filters, expr)
		}
	}

	pc := extractPathConstraints(filters)

	// Extract kind constraints from the relationship pattern
	for _, elem := range pp.PatternElements {
		if rp, ok := elem.AsRelationshipPattern(); ok && len(rp.Kinds) > 0 {
			if pc.edgeKinds == nil {
				pc.edgeKinds = make(map[graph.Kind]struct{})
			}
			for _, k := range rp.Kinds {
				pc.edgeKinds[k] = struct{}{}
			}
		}
	}

	ex.db.mu.RLock()
	paths := ex.db.bfsAllShortestPaths(pc)
	ex.db.mu.RUnlock()

	// Bind paths to the pattern variable
	pathVar := ""
	if pp.Variable != nil {
		pathVar = pp.Variable.Symbol
	}

	var expanded []binding
	for _, row := range ex.bindings {
		for _, p := range paths {
			pathCopy := p
			newRow := copyBinding(row)
			if pathVar != "" {
				newRow[pathVar] = &pathCopy
			}
			expanded = append(expanded, newRow)
		}
	}

	ex.bindings = expanded

	if pathVar != "" && len(ex.matchStack) > 0 {
		ms := &ex.matchStack[len(ex.matchStack)-1]
		ms.newVars = append(ms.newVars, pathVar)
	}

	// Mark WHERE as consumed — path constraints already extracted
	ex.whereConsumedByPath = true
}

// sortBindings sorts bindings by ORDER BY items.
func (ex *executor) sortBindings() {
	if ex.returnCtx == nil || len(ex.returnCtx.orderBy) == 0 {
		return
	}

	sort.SliceStable(ex.bindings, func(i, j int) bool {
		for _, si := range ex.returnCtx.orderBy {
			vi := resolveBindingValue(ex.db, ex.bindings[i], si.Expression)
			vj := resolveBindingValue(ex.db, ex.bindings[j], si.Expression)

			cmp := compareOrdered(vi, vj)
			if cmp == 0 {
				// Try string comparison as fallback
				si := fmt.Sprint(vi)
				sj := fmt.Sprint(vj)
				if si == sj {
					continue
				}
				if si < sj {
					cmp = -1
				} else {
					cmp = 1
				}
			}

			if !si.Ascending {
				cmp = -cmp
			}
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		return false
	})
}

// allBoundVariables returns all variable names from bindings (for RETURN *).
func (ex *executor) allBoundVariables() []string {
	seen := make(map[string]struct{})
	var keys []string
	for _, row := range ex.bindings {
		for k := range row {
			if strings.HasPrefix(k, "__") {
				continue
			}
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// projItemAliases returns the alias names from projItems.
func (ex *executor) projItemAliases() []string {
	aliases := make([]string, len(ex.projItems))
	for i, item := range ex.projItems {
		aliases[i] = item.alias
	}
	return aliases
}

// deduplicateBindings removes duplicate binding rows based on the given keys.
func deduplicateBindings(bindings []binding, keys []string) []binding {
	seen := make(map[string]struct{})
	var result []binding

	for _, row := range bindings {
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%v", row[k]))
		}
		key := strings.Join(parts, "\x00")
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

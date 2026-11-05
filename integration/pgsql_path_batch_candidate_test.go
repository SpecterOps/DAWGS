//go:build manual_integration

package integration

import (
	"fmt"
	"maps"
	"reflect"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	pgsqlFormat "github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
)

// pathBatchSQL is an experimental, test-only materializer. The compact input
// columns are root nodecomposite, edge_ids int8[], known_nodes nodecomposite[],
// and is_null bool. Each occurrence gets an independent row ID: only persistent
// entity lookup keys are deduplicated. DirectionBoth walks the ordered edges,
// supporting mixed direction and repeated nodes. A directed hint is valid only
// when every edge follows that direction in the logical path.
func pathBatchSQL(skeletonSQL string, graphID int32, direction graph.Direction) string {
	nodeOccurrences := `
path_walk(rid, idx, current_id, node_ids) as (
  select rid, 0::bigint, (root).id, array[(root).id]::int8[]
  from path_rows where not is_null and edge_ids is not null and known_nodes is not null
  union all
  select w.rid, w.idx+1,
         case when w.current_id=o.start_id then o.end_id else o.start_id end,
         w.node_ids || case when w.current_id=o.start_id then o.end_id else o.start_id end
  from path_walk w join edge_occ o on o.rid=w.rid and o.ordinality=w.idx+1
  where w.current_id in (o.start_id,o.end_id)
),
node_occ as materialized (
  select w.rid, ids.ordinality, ids.id
  from path_walk w join path_rows p on p.rid=w.rid and w.idx=cardinality(p.edge_ids)
  cross join lateral unnest(w.node_ids) with ordinality ids(id,ordinality)
),`
	if direction == graph.DirectionOutbound || direction == graph.DirectionInbound {
		nextID := "end_id"
		if direction == graph.DirectionInbound {
			nextID = "start_id"
		}
		nodeOccurrences = `
node_occ as materialized (
  select rid, 1::bigint as ordinality, (root).id as id from path_rows where not is_null
  union all
  select rid, ordinality+1, ` + nextID + ` from edge_occ
),`
	}
	return fmt.Sprintf(`with recursive
path_rows as materialized (
  select row_number() over () as rid, compact.* from (%s) compact
),
needed_edges as materialized (
  select distinct ids.id from path_rows p
  cross join lateral unnest(p.edge_ids) ids(id) where not p.is_null
),
edge_records as materialized (
  select e.id, e.start_id, e.end_id,
         (e.id,e.start_id,e.end_id,e.kind_id,e.properties)::edgecomposite as edge_value
  from needed_edges k join edge e on e.id=k.id and e.graph_id=%d::int4
),
edge_occ as materialized (
  select p.rid, ids.ordinality, e.id, e.start_id, e.end_id
  from path_rows p cross join lateral unnest(p.edge_ids) with ordinality ids(id,ordinality)
  join edge_records e on e.id=ids.id where not p.is_null
),
%s
needed_nodes as materialized (select distinct id from node_occ),
node_records as materialized (
  select n.id,(n.id,n.kind_ids,n.properties)::nodecomposite as node_value
  from needed_nodes k join node n on n.id=k.id and n.graph_id=%d::int4
),
known_node_records as materialized (
  select distinct on (p.rid,k.id) p.rid,k.id,
         (k.id,k.kind_ids,k.properties)::nodecomposite as node_value
  from path_rows p cross join lateral unnest(p.known_nodes)
    with ordinality k(id,kind_ids,properties,ordinality)
  where not p.is_null order by p.rid,k.id,k.ordinality
),
node_arrays as (
  select o.rid,array_agg(coalesce(k.node_value,n.node_value) order by o.ordinality)::nodecomposite[] as nodes
  from node_occ o left join known_node_records k on k.rid=o.rid and k.id=o.id
  left join node_records n on n.id=o.id
  where k.id is not null or n.id is not null group by o.rid
),
edge_arrays as (
  select o.rid,array_agg(e.edge_value order by o.ordinality)::edgecomposite[] as edges
  from edge_occ o join edge_records e on e.id=o.id group by o.rid
)
select case when p.is_null or p.edge_ids is null or p.known_nodes is null or (p.root).id is null
  or coalesce(cardinality(n.nodes),0) != cardinality(p.edge_ids)+1
  or coalesce(cardinality(e.edges),0) != cardinality(p.edge_ids)
  then null::pathcomposite
  else (n.nodes,coalesce(e.edges,array[]::edgecomposite[]))::pathcomposite end as p
from path_rows p left join node_arrays n on n.rid=p.rid
left join edge_arrays e on e.rid=p.rid`, strings.TrimSuffix(strings.TrimSpace(skeletonSQL), ";"), graphID, nodeOccurrences, graphID)
}

// pathBatchSkeleton rewrites the final SELECT projection, using the PostgreSQL
// AST rather than matching one fixture's emitted SQL. A nonempty reason means
// unsupported; callers must never count that case as a candidate comparison.
// Modifiers and already materialized paths are deliberately excluded. Earlier
// path assembly/consumption is also excluded, since batching only the final
// projection would misrepresent the amount of shared work.
func pathBatchSkeleton(translation translate.Result) (string, map[string]any, string) {
	return pathBatchSkeletonWithModifiers(translation, false)
}

// The opt-in page experiment retains selection modifiers before hydration.
// Callers compare unordered truncating pages by multiset membership/count,
// not by assuming two legal LIMIT executions select the same paths.
func pathBatchLimitedSkeleton(translation translate.Result) (string, map[string]any, string) {
	query, ok := translation.Statement.(pgsql.Query)
	if !ok {
		return "", nil, "non-query statement"
	}
	if query.Limit == nil && query.Offset == nil && len(query.OrderBy) == 0 {
		return "", nil, "no final selection modifiers"
	}
	selection, ok := query.Body.(pgsql.Select)
	if !ok {
		return "", nil, "set operation"
	}
	aliases := map[pgsql.Identifier]bool{}
	for _, item := range selection.Projection {
		if alias, ok := pathBatchExpression(item).(pgsql.AliasedExpression); ok && alias.Alias.Set {
			aliases[alias.Alias.Value] = true
		}
	}
	for _, ordering := range query.OrderBy {
		reason := ""
		pathBatchVisit(ordering.Expression, func(node any) {
			switch value := node.(type) {
			case pgsql.FunctionCall:
				reason = "ORDER BY function may consume a path or be volatile"
			case pgsql.CompositeValue:
				if value.DataType == pgsql.PathComposite {
					reason = "ORDER BY materialized path"
				}
			case pgsql.EdgeArrayFromPathIDs:
				reason = "ORDER BY hydrated edges"
			case pgsql.Identifier:
				if aliases[value] {
					reason = "ORDER BY final result alias"
				}
			case pgsql.Literal:
				if _, integer := asInt64(value.Value); integer && !value.Null {
					reason = "ORDER BY ordinal may refer to the path result"
				}
			}
		})
		if reason != "" {
			return "", nil, reason
		}
	}
	return pathBatchSkeletonWithModifiers(translation, true)
}

func pathBatchSkeletonWithModifiers(translation translate.Result, preserveModifiers bool) (string, map[string]any, string) {
	if len(translation.Stages) != 0 {
		return "", nil, "multipart mutation"
	}
	query, ok := translation.Statement.(pgsql.Query)
	if !ok {
		return "", nil, "non-query statement"
	}
	if !preserveModifiers && (query.Limit != nil || query.Offset != nil || len(query.OrderBy) != 0) {
		return "", nil, "final LIMIT/OFFSET/ORDER BY"
	}
	selection, ok := query.Body.(pgsql.Select)
	if !ok {
		return "", nil, "set operation"
	}
	if selection.Distinct || len(selection.GroupBy) != 0 || selection.Having != nil {
		return "", nil, "final DISTINCT/grouping"
	}
	if len(selection.Projection) != 1 {
		return "", nil, "multiple result columns"
	}
	expression := pathBatchExpression(pgsql.Expression(selection.Projection[0]))
	if alias, ok := expression.(pgsql.AliasedExpression); ok {
		expression = pathBatchExpression(alias.Expression)
	}
	isNull := pgsql.Expression(pgsql.NewLiteral(false, pgsql.Boolean))
	if guarded, ok := expression.(pgsql.Case); ok {
		if guarded.Operand != nil || len(guarded.Conditions) != 1 || len(guarded.Then) != 1 {
			return "", nil, "complex final CASE"
		}
		if null, ok := guarded.Then[0].(pgsql.Literal); !ok || !null.Null {
			return "", nil, "non-null final CASE branch"
		}
		isNull, expression = guarded.Conditions[0], pathBatchExpression(guarded.Else)
	}
	var root, edgeIDs, knownNodes pgsql.Expression
	switch value := expression.(type) {
	case pgsql.FunctionCall:
		if len(value.Parameters) != 4 || value.Function != pgsql.FunctionOrderedEdgeIDsToPath && value.Function != pgsql.FunctionOrderedEdgesToPath {
			return "", nil, "final expression is not an ordered edge-ID path"
		}
		root, edgeIDs, knownNodes = value.Parameters[1], value.Parameters[2], value.Parameters[3]
		if value.Function == pgsql.FunctionOrderedEdgesToPath {
			var ok bool
			if edgeIDs, ok = pathBatchCompactEdgeIDs(edgeIDs); !ok {
				return "", nil, "generic path edge array cannot be compacted"
			}
		}
	case pgsql.CompositeValue:
		if value.DataType != pgsql.PathComposite || len(value.Values) != 2 {
			return "", nil, "final composite is not a path"
		}
		nodes, nodesOK := value.Values[0].(pgsql.ArrayLiteral)
		edges, edgesOK := value.Values[1].(pgsql.ArrayLiteral)
		if !nodesOK || !edgesOK || len(nodes.Values) == 0 {
			return "", nil, "nonliteral direct path components"
		}
		root, knownNodes = nodes.Values[0], nodes
		ids := pgsql.ArrayLiteral{CastType: pgsql.Int8Array}
		for _, edge := range edges.Values {
			ids.Values = append(ids.Values, pgsql.RowColumnReference{Identifier: edge, Column: pgsql.ColumnID})
		}
		edgeIDs = ids
	default:
		return "", nil, "final result is not a directly assembled path"
	}
	assemblies := 0
	reason := ""
	pathBatchVisit(query, func(node any) {
		switch value := node.(type) {
		case pgsql.Insert, pgsql.Delete, pgsql.Update, pgsql.Merge:
			reason = "mutation"
		case pgsql.FunctionCall:
			if value.CastType == pgsql.PathComposite {
				assemblies++
			}
		case pgsql.CompositeValue:
			if value.DataType == pgsql.PathComposite {
				assemblies++
			}
		}
	})
	if reason != "" {
		return "", nil, reason
	}
	if assemblies != 1 {
		return "", nil, "earlier/additional path materialization"
	}
	selection.Projection = pgsql.Projection{
		pgsql.AliasedExpression{Expression: root, Alias: pgsql.AsOptionalIdentifier("root")},
		pgsql.AliasedExpression{Expression: edgeIDs, Alias: pgsql.AsOptionalIdentifier("edge_ids")},
		pgsql.AliasedExpression{Expression: knownNodes, Alias: pgsql.AsOptionalIdentifier("known_nodes")},
		pgsql.AliasedExpression{Expression: isNull, Alias: pgsql.AsOptionalIdentifier("is_null")},
	}
	if preserveModifiers && len(query.OrderBy) != 0 {
		var orderingSQL []string
		for _, ordering := range query.OrderBy {
			formatted, err := pgsqlFormat.Expression(ordering, pgsqlFormat.NewOutputBuilder().WithTargetGraph(translation.GraphID).WithMaterializedParameters(translation.Parameters))
			if err != nil {
				return "", nil, "format page order: " + err.Error()
			}
			orderingSQL = append(orderingSQL, formatted.Statement)
		}
		// The general SQL formatter currently does not emit FunctionCall.Over.
		// Format the validated scalar sort expressions with escaped literals,
		// avoiding parameter-name collisions with the surrounding query.
		selection.Projection = append(selection.Projection, pgsql.AliasedExpression{
			Expression: pgsql.FormattingLiteral("row_number() over (order by " + strings.Join(orderingSQL, ", ") + ")"),
			Alias:      pgsql.AsOptionalIdentifier("page_rank"),
		})
	}
	query.Body = selection
	translation.Statement = query
	formatted, err := translate.Translated(translation)
	if err != nil {
		return "", nil, "format compact rows: " + err.Error()
	}
	parameters := maps.Clone(translation.Parameters)
	if parameters == nil {
		parameters = map[string]any{}
	}
	maps.Copy(parameters, formatted.Parameters)
	return strings.TrimSuffix(formatted.Statement, ";"), parameters, ""
}

// Mixed read paths may use the conservative edge-composite helper. Recover
// IDs structurally so the compact query does not retain the helper's per-row
// EdgeArrayFromPathIDs hydration under a different SQL spelling.
func pathBatchCompactEdgeIDs(expression pgsql.Expression) (pgsql.Expression, bool) {
	switch value := expression.(type) {
	case *pgsql.EdgeArrayFromPathIDs:
		return value.PathIDs, true
	case *pgsql.Parenthetical:
		return pathBatchCompactEdgeIDs(value.Expression)
	case *pgsql.BinaryExpression:
		return pathBatchCompactEdgeIDs(*value)
	case pgsql.BinaryExpression:
		if value.Operator != pgsql.OperatorConcatenate {
			return nil, false
		}
		left, leftOK := pathBatchCompactEdgeIDs(value.LOperand)
		right, rightOK := pathBatchCompactEdgeIDs(value.ROperand)
		if !leftOK || !rightOK {
			return nil, false
		}
		return pgsql.NewBinaryExpression(left, pgsql.OperatorConcatenate, right), true
	case pgsql.ArrayLiteral:
		if value.CastType != pgsql.EdgeCompositeArray {
			return nil, false
		}
		ids := pgsql.ArrayLiteral{CastType: pgsql.Int8Array}
		for _, edge := range value.Values {
			ids.Values = append(ids.Values, pgsql.RowColumnReference{Identifier: edge, Column: pgsql.ColumnID})
		}
		return ids, true
	default:
		return nil, false
	}
}

func pathBatchExpression(expression pgsql.Expression) pgsql.Expression {
	switch value := expression.(type) {
	case *pgsql.AliasedExpression:
		return *value
	case *pgsql.Case:
		return *value
	case *pgsql.FunctionCall:
		return *value
	case *pgsql.CompositeValue:
		return *value
	case *pgsql.Parenthetical:
		return pathBatchExpression(value.Expression)
	default:
		return expression
	}
}

// The production walker omits some formatting-only AST nodes (RecordShape).
// This test visitor includes exported fields and avoids pointer cycles.
func pathBatchVisit(node any, visit func(any)) {
	active := map[uintptr]bool{}
	var traverse func(reflect.Value)
	traverse = func(value reflect.Value) {
		if !value.IsValid() {
			return
		}
		if value.Kind() == reflect.Interface {
			if !value.IsNil() {
				traverse(value.Elem())
			}
			return
		}
		if value.Kind() == reflect.Pointer {
			if value.IsNil() || active[value.Pointer()] {
				return
			}
			active[value.Pointer()] = true
			traverse(value.Elem())
			delete(active, value.Pointer())
			return
		}
		if value.CanInterface() {
			visit(value.Interface())
		}
		switch value.Kind() {
		case reflect.Struct:
			for index := range value.NumField() {
				if value.Type().Field(index).IsExported() {
					traverse(value.Field(index))
				}
			}
		case reflect.Slice, reflect.Array:
			for index := range value.Len() {
				traverse(value.Index(index))
			}
		}
	}
	traverse(reflect.ValueOf(node))
}

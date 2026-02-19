package pg

import (
	"strconv"
	"strings"
)

const (
	createNodeStatement               = `insert into node (graph_id, kind_ids, properties) values (@graph_id, @kind_ids, @properties) returning id;`
	upsertNodeStatement               = `insert into node (graph_id, kind_ids, properties) values (@graph_id, @kind_ids, @properties) on conflict (properties ->> 'objectid') do nothing;`
	createNodeWithoutIDBatchStatement = `insert into node (graph_id, kind_ids, properties) select $1, unnest($2::text[])::int2[], unnest($3::jsonb[])`
	createNodeWithIDBatchStatement    = `insert into node (graph_id, id, kind_ids, properties) select $1, unnest($2::int8[]), unnest($3::text[])::int2[], unnest($4::jsonb[])`
	deleteNodeWithIDStatement         = `delete from node where node.id = any($1)`

	createEdgeStatement = `insert into edge (graph_id, start_id, end_id, kind_id, properties) values (@graph_id, @start_id, @end_id, @kind_id, @properties) returning id;`

	// TODO: The query below is not a pure creation statement as it contains an `on conflict` clause to dance around
	//	     Azure post-processing. This was done because Azure post will submit the same creation request hundreds of
	// 		 times for the same edge. In PostgreSQL this results in a constraint violation. For now this is best-effort
	//		 until Azure post-processing can be refactored.
	createEdgeBatchStatement  = `insert into edge as e (graph_id, start_id, end_id, kind_id, properties) select $1, unnest($2::int8[]), unnest($3::int8[]), unnest($4::int2[]), unnest($5::jsonb[]) on conflict (graph_id, start_id, end_id, kind_id) do update set properties = e.properties || excluded.properties;`
	deleteEdgeWithIDStatement = `delete from edge as e where e.id = any($1)`

	edgePropertySetOnlyStatement      = `update edge set properties = properties || $1::jsonb where edge.id = $2`
	edgePropertyDeleteOnlyStatement   = `update edge set properties = properties - $1::text[] where edge.id = $2`
	edgePropertySetAndDeleteStatement = `update edge set properties = properties || $1::jsonb - $2::text[] where edge.id = $3`
)

func formatNodeInsertSQL(graphID int32) string {
	builder := strings.Builder{}
	builder.WriteString("insert into node_")
	builder.WriteString(strconv.Itoa(int(graphID)))
	builder.WriteString(" as n (graph_id, kind_ids, properties) values (@graph_id, @kind_ids, @properties) on conflict ((properties ->> 'objectid')) do nothing;")
	return builder.String()
}

func formatNodeUpsertSQL(graphID int32) string {
	builder := strings.Builder{}
	builder.WriteString("insert into node_")
	builder.WriteString(strconv.Itoa(int(graphID)))
	builder.WriteString(" as n (graph_id, kind_ids, properties) values (@graph_id, @kind_ids, @properties) on conflict ((properties ->> 'objectid')) do update set kind_ids = uniq(sort(n.kind_ids || @kind_ids)), properties = n.properties || excluded.properties;")
	return builder.String()
}

func formatEdgeInsertSQL(graphID int32, startMatchProperty, endMatchProperty string) string {
	builder := strings.Builder{}
	builder.WriteString("with s0 as (select id as start from node n where n.properties ->> '")
	builder.WriteString(startMatchProperty)
	builder.WriteString("' = @start_match_value), s1 as (select s0.start as start, n.id as end from node n, s0 where n.properties ->> '")
	builder.WriteString(endMatchProperty)
	builder.WriteString("' = @end_match_value) insert into edge_")
	builder.WriteString(strconv.Itoa(int(graphID)))
	builder.WriteString(" as e (graph_id, start_id, end_id, kind_id, properties) select @graph_id, s1.start, s1.end, @kind_id, @properties from s1 on conflict (graph_id, start_id, end_id, kind_id) do nothing;")
	return builder.String()
}

func formatEdgeUpsertSQL(graphID int32, startMatchProperty, endMatchProperty string) string {
	builder := strings.Builder{}
	builder.WriteString("with s0 as (select id as start from node n where n.properties ->> '")
	builder.WriteString(startMatchProperty)
	builder.WriteString("' = @start_match_value), s1 as (select s0.start as start, n.id as end from node n, s0 where n.properties ->> '")
	builder.WriteString(endMatchProperty)
	builder.WriteString("' = @end_match_value) insert into edge_")
	builder.WriteString(strconv.Itoa(int(graphID)))
	builder.WriteString(" as e (graph_id, start_id, end_id, kind_id, properties) select @graph_id, s1.start, s1.end, @kind_id, @properties from s1 on conflict (graph_id, start_id, end_id, kind_id) do update set properties = e.properties || excluded.properties;")
	return builder.String()
}

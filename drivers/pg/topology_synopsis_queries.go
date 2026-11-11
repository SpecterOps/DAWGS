package pg

// topologySynopsisOperation associates a detail statement with its argument requirements.
type topologySynopsisOperation struct {
	// query is the statement executed within the publication transaction.
	query string
	// withEpoch adds the new publication epoch after the graph identifier.
	withEpoch bool
}

const (
	// lockTopologySynopsisGraphQuery locks the graph mutation epoch before collecting synopsis data.
	lockTopologySynopsisGraphQuery = `select graph.id::int4, graph_traversal_epoch.epoch::bigint from graph join graph_traversal_epoch on graph_traversal_epoch.graph_id = graph.id where graph.name = $1 for update of graph_traversal_epoch`

	// countTopologySynopsisGraphQuery counts nodes and edges in the target graph.
	countTopologySynopsisGraphQuery = `select count(*)::bigint, (select count(*)::bigint from edge where graph_id = $1) from node where graph_id = $1`

	// nextTopologySynopsisEpochQuery reads the next synopsis publication generation.
	nextTopologySynopsisEpochQuery = `select coalesce(epoch, 0)::bigint + 1 from graph_traversal_synopsis_generation where graph_id = $1`

	// deleteTopologySynopsisDegreesQuery removes degree buckets from the prior publication.
	deleteTopologySynopsisDegreesQuery = `delete from graph_traversal_synopsis_degree where graph_id = $1`

	// deleteTopologySynopsisEdgeCountsQuery removes edge counts from the prior publication.
	deleteTopologySynopsisEdgeCountsQuery = `delete from graph_traversal_synopsis_edge_count where graph_id = $1`

	// deleteTopologySynopsisNodeCountsQuery removes node counts from the prior publication.
	deleteTopologySynopsisNodeCountsQuery = `delete from graph_traversal_synopsis_node_count where graph_id = $1`

	// insertTopologySynopsisNodeCountsQuery collects node counts per kind for the new epoch.
	insertTopologySynopsisNodeCountsQuery = `insert into graph_traversal_synopsis_node_count (graph_id, epoch, kind_id, node_count)
select $1::int4, $2::bigint, kind_id, count(*)::bigint
from node cross join lateral unnest(kind_ids) as kinds(kind_id)
where graph_id = $1::int4 group by kind_id`

	// insertTopologySynopsisEdgeCountsQuery collects directional edge and endpoint counts for the new epoch.
	insertTopologySynopsisEdgeCountsQuery = `insert into graph_traversal_synopsis_edge_count (graph_id, epoch, direction, kind_id, edge_count, distinct_start_count, distinct_end_count)
select $1::int4, $2::bigint, direction, kind_id, edge_count, distinct_start_count, distinct_end_count
from (
  select 'outbound'::text as direction, kind_id, count(*)::bigint as edge_count, count(distinct start_id)::bigint as distinct_start_count, count(distinct end_id)::bigint as distinct_end_count from edge where graph_id = $1::int4 group by kind_id
  union all
  select 'inbound'::text, kind_id, count(*)::bigint, count(distinct end_id)::bigint, count(distinct start_id)::bigint from edge where graph_id = $1::int4 group by kind_id
) counts`

	// insertTopologySynopsisDegreesQuery collects directional degree buckets for the new epoch.
	insertTopologySynopsisDegreesQuery = `insert into graph_traversal_synopsis_degree (graph_id, epoch, direction, kind_id, bucket, node_count)
with degree as (
  select 'outbound'::text as direction, kind_id, start_id as node_id, count(*)::bigint as degree from edge where graph_id = $1::int4 group by kind_id, start_id
  union all
  select 'inbound'::text, kind_id, end_id, count(*)::bigint from edge where graph_id = $1::int4 group by kind_id, end_id
)
select $1::int4, $2::bigint, direction, kind_id,
       case when degree = 1 then 'one' when degree <= 4 then 'two_to_four' when degree <= 16 then 'five_to_sixteen' else 'seventeen_plus' end,
       count(*)::bigint
from degree group by direction, kind_id, case when degree = 1 then 'one' when degree <= 4 then 'two_to_four' when degree <= 16 then 'five_to_sixteen' else 'seventeen_plus' end`

	// publishTopologySynopsisQuery publishes the generation only while its mutation epoch remains current.
	publishTopologySynopsisQuery = `
insert into graph_traversal_synopsis_generation
  (graph_id, epoch, source_mutation_epoch, estimator_version, schema_version, status, node_count, edge_count, refresh_started_at, refresh_completed_at, refresh_mode)
select $1, $2, $3, 'topology-fixed-suffix-counts-v1', 'topology-synopsis-schema-v2', 'ready', $4, $5, clock_timestamp(), clock_timestamp(), 'full'
where (select epoch from graph_traversal_epoch where graph_id = $1) = $3
on conflict (graph_id) do update
set epoch = excluded.epoch, source_mutation_epoch = excluded.source_mutation_epoch,
    estimator_version = excluded.estimator_version, schema_version = excluded.schema_version,
    status = excluded.status, node_count = excluded.node_count, edge_count = excluded.edge_count,
    built_at = clock_timestamp(), refresh_started_at = excluded.refresh_started_at,
    refresh_completed_at = excluded.refresh_completed_at, refresh_mode = excluded.refresh_mode
where (select epoch from graph_traversal_epoch where graph_id = $1) = $3
returning graph_id::int4, epoch, source_mutation_epoch,
          (select epoch from graph_traversal_epoch where graph_id = $1),
          estimator_version, schema_version, status, node_count, edge_count`

	// readTopologySynopsisQuery reads the published synopsis alongside the visible mutation epoch.
	readTopologySynopsisQuery = `
select epoch.graph_id::int4, coalesce(synopsis.epoch, 0), coalesce(synopsis.source_mutation_epoch, 0),
       epoch.epoch, coalesce(synopsis.estimator_version, ''), coalesce(synopsis.schema_version, ''), coalesce(synopsis.status, ''),
       coalesce(synopsis.node_count, 0), coalesce(synopsis.edge_count, 0)
from graph_traversal_epoch epoch
left join graph_traversal_synopsis_generation synopsis on synopsis.graph_id = epoch.graph_id
where epoch.graph_id = $1`
)

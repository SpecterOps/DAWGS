package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
)

// TraversalTopologySynopsis is the current graph-scoped, atomically published
// topology summary. It is advisory only: callers must treat unavailable or
// stale state as an incumbent-only routing outcome.
type TraversalTopologySynopsis struct {
	GraphID              int32  `json:"graph_id"`
	Epoch                uint64 `json:"epoch"`
	SourceMutationEpoch  uint64 `json:"source_mutation_epoch"`
	CurrentMutationEpoch uint64 `json:"current_mutation_epoch"`
	EstimatorVersion     string `json:"estimator_version"`
	SchemaVersion        string `json:"schema_version"`
	Status               string `json:"status"`
	NodeCount            int64  `json:"node_count"`
	EdgeCount            int64  `json:"edge_count"`
}

// Available reports whether this synopsis was atomically published for the
// current visible graph mutation epoch.
func (s TraversalTopologySynopsis) Available() bool {
	return s.Epoch != 0 && s.Status == "ready" && s.SourceMutationEpoch == s.CurrentMutationEpoch
}

// RefreshTraversalTopologySynopsis publishes a versioned graph topology
// synopsis. It runs outside query latency, serializes refreshes per graph, and
// commits only when the source mutation epoch is still current.
func (s *Driver) RefreshTraversalTopologySynopsis(ctx context.Context, target graph.Graph) (TraversalTopologySynopsis, error) {
	if s == nil || s.pool == nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("PostgreSQL driver is not initialized")
	}
	if target.Name == "" {
		return TraversalTopologySynopsis{}, fmt.Errorf("topology synopsis requires a named graph")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("acquire connection for topology synopsis: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("begin topology synopsis refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var graphID int32
	var sourceEpoch, nodeCount, edgeCount, nextEpoch int64
	if err := tx.QueryRow(ctx, `select graph.id::int4, graph_traversal_epoch.epoch::bigint from graph join graph_traversal_epoch on graph_traversal_epoch.graph_id = graph.id where graph.name = $1`, target.Name).Scan(&graphID, &sourceEpoch); err != nil {
		if err == pgx.ErrNoRows {
			return TraversalTopologySynopsis{}, fmt.Errorf("topology synopsis graph %q does not exist", target.Name)
		}
		return TraversalTopologySynopsis{}, fmt.Errorf("read topology synopsis graph: %w", err)
	}
	if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock($1)", graphID); err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("lock topology synopsis refresh: %w", err)
	}
	if err := tx.QueryRow(ctx, `select count(*)::bigint, (select count(*)::bigint from edge where graph_id = $1) from node where graph_id = $1`, graphID).Scan(&nodeCount, &edgeCount); err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("count topology synopsis graph: %w", err)
	}
	if err := tx.QueryRow(ctx, `select coalesce(epoch, 0)::bigint + 1 from graph_traversal_synopsis_generation where graph_id = $1`, graphID).Scan(&nextEpoch); err == pgx.ErrNoRows {
		nextEpoch = 1
	} else if err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("read topology synopsis generation: %w", err)
	}
	for _, statement := range []string{
		`delete from graph_traversal_synopsis_degree where graph_id = $1`,
		`delete from graph_traversal_synopsis_edge_count where graph_id = $1`,
		`delete from graph_traversal_synopsis_node_count where graph_id = $1`,
		`insert into graph_traversal_synopsis_node_count (graph_id, epoch, kind_id, node_count)
select $1::int4, $2::bigint, kind_id, count(*)::bigint
from node cross join lateral unnest(kind_ids) as kinds(kind_id)
where graph_id = $1::int4 group by kind_id`,
		`insert into graph_traversal_synopsis_edge_count (graph_id, epoch, direction, kind_id, edge_count, distinct_start_count, distinct_end_count)
select $1::int4, $2::bigint, direction, kind_id, edge_count, distinct_start_count, distinct_end_count
from (
  select 'outbound'::text as direction, kind_id, count(*)::bigint as edge_count, count(distinct start_id)::bigint as distinct_start_count, count(distinct end_id)::bigint as distinct_end_count from edge where graph_id = $1::int4 group by kind_id
  union all
  select 'inbound'::text, kind_id, count(*)::bigint, count(distinct end_id)::bigint, count(distinct start_id)::bigint from edge where graph_id = $1::int4 group by kind_id
) counts`,
		`insert into graph_traversal_synopsis_degree (graph_id, epoch, direction, kind_id, bucket, node_count)
with degree as (
  select 'outbound'::text as direction, kind_id, start_id as node_id, count(*)::bigint as degree from edge where graph_id = $1::int4 group by kind_id, start_id
  union all
  select 'inbound'::text, kind_id, end_id, count(*)::bigint from edge where graph_id = $1::int4 group by kind_id, end_id
)
select $1::int4, $2::bigint, direction, kind_id,
       case when degree = 1 then 'one' when degree <= 4 then 'two_to_four' when degree <= 16 then 'five_to_sixteen' else 'seventeen_plus' end,
       count(*)::bigint
from degree group by direction, kind_id, case when degree = 1 then 'one' when degree <= 4 then 'two_to_four' when degree <= 16 then 'five_to_sixteen' else 'seventeen_plus' end`,
	} {
		args := []any{graphID}
		if statement[0:6] == "insert" {
			args = append(args, nextEpoch)
		}
		if _, err := tx.Exec(ctx, statement, args...); err != nil {
			return TraversalTopologySynopsis{}, fmt.Errorf("publish topology synopsis detail: %w", err)
		}
	}
	const publish = `
insert into graph_traversal_synopsis_generation
  (graph_id, epoch, source_mutation_epoch, estimator_version, schema_version, status, node_count, edge_count, refresh_started_at, refresh_completed_at, refresh_mode)
select $1, $2, $3, 'topology-synopsis-v2', 'topology-synopsis-schema-v2', 'ready', $4, $5, clock_timestamp(), clock_timestamp(), 'full'
where (select epoch from graph_traversal_epoch where graph_id = $1) = $3
on conflict (graph_id) do update
set epoch = excluded.epoch, source_mutation_epoch = excluded.source_mutation_epoch,
    estimator_version = excluded.estimator_version, schema_version = excluded.schema_version,
    status = excluded.status, node_count = excluded.node_count, edge_count = excluded.edge_count,
    built_at = clock_timestamp(), refresh_started_at = excluded.refresh_started_at,
    refresh_completed_at = excluded.refresh_completed_at, refresh_mode = excluded.refresh_mode
where (select epoch from graph_traversal_epoch where graph_id = $1) = $3
returning graph_id::int4, epoch, source_mutation_epoch, source_mutation_epoch,
          estimator_version, schema_version, status, node_count, edge_count`
	var synopsis TraversalTopologySynopsis
	if err := tx.QueryRow(ctx, publish, graphID, nextEpoch, sourceEpoch, nodeCount, edgeCount).Scan(
		&synopsis.GraphID, &synopsis.Epoch, &synopsis.SourceMutationEpoch, &synopsis.CurrentMutationEpoch,
		&synopsis.EstimatorVersion, &synopsis.SchemaVersion, &synopsis.Status, &synopsis.NodeCount, &synopsis.EdgeCount,
	); err != nil {
		if err == pgx.ErrNoRows {
			return TraversalTopologySynopsis{}, fmt.Errorf("topology synopsis became stale during refresh")
		}
		return TraversalTopologySynopsis{}, fmt.Errorf("publish topology synopsis generation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("commit topology synopsis refresh: %w", err)
	}
	return synopsis, nil
}

func (s *transaction) traversalTopologySynopsis(graphID int32) (TraversalTopologySynopsis, error) {
	var synopsis TraversalTopologySynopsis
	const statement = `
select epoch.graph_id::int4, coalesce(synopsis.epoch, 0), coalesce(synopsis.source_mutation_epoch, 0),
       epoch.epoch, coalesce(synopsis.estimator_version, ''), coalesce(synopsis.schema_version, ''), coalesce(synopsis.status, ''),
       coalesce(synopsis.node_count, 0), coalesce(synopsis.edge_count, 0)
from graph_traversal_epoch epoch
left join graph_traversal_synopsis_generation synopsis on synopsis.graph_id = epoch.graph_id
where epoch.graph_id = $1`
	err := s.driver().QueryRow(s.ctx, statement, graphID).Scan(
		&synopsis.GraphID, &synopsis.Epoch, &synopsis.SourceMutationEpoch,
		&synopsis.CurrentMutationEpoch, &synopsis.EstimatorVersion, &synopsis.SchemaVersion, &synopsis.Status,
		&synopsis.NodeCount, &synopsis.EdgeCount,
	)
	if err != nil {
		return TraversalTopologySynopsis{}, err
	}
	return synopsis, nil
}

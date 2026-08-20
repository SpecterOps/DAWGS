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
	Status               string `json:"status"`
	NodeCount            int64  `json:"node_count"`
	EdgeCount            int64  `json:"edge_count"`
}

// Available reports whether this synopsis was atomically published for the
// current visible graph mutation epoch.
func (s TraversalTopologySynopsis) Available() bool {
	return s.Epoch != 0 && s.Status == "ready" && s.SourceMutationEpoch == s.CurrentMutationEpoch
}

// RefreshTraversalTopologySynopsis publishes a conservative graph-wide count
// synopsis using one database statement. It is an explicit management action,
// never part of ordinary query latency or translation.
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

	const statement = `
with target as (
  select id::int4 as graph_id from graph where name = $1
), counts as (
  select target.graph_id,
         (select epoch from graph_traversal_epoch where graph_id = target.graph_id)::bigint as mutation_epoch,
         (select count(*) from node where graph_id = target.graph_id)::bigint as node_count,
         (select count(*) from edge where graph_id = target.graph_id)::bigint as edge_count
  from target
), published as (
  insert into graph_traversal_synopsis_generation
    (graph_id, epoch, source_mutation_epoch, estimator_version, status, node_count, edge_count)
  select graph_id, coalesce((select epoch from graph_traversal_synopsis_generation where graph_id = counts.graph_id), 0) + 1,
         mutation_epoch, 'topology-synopsis-v1', 'ready', node_count, edge_count
  from counts
  on conflict (graph_id) do update
  set epoch = graph_traversal_synopsis_generation.epoch + 1,
      source_mutation_epoch = excluded.source_mutation_epoch,
      estimator_version = excluded.estimator_version,
      status = excluded.status,
      node_count = excluded.node_count,
      edge_count = excluded.edge_count,
      built_at = clock_timestamp()
  returning graph_id::int4, epoch, source_mutation_epoch, estimator_version, status, node_count, edge_count
)
select published.graph_id, published.epoch, published.source_mutation_epoch,
       counts.mutation_epoch, published.estimator_version, published.status,
       published.node_count, published.edge_count
from published join counts using (graph_id)`

	var synopsis TraversalTopologySynopsis
	if err := conn.QueryRow(ctx, statement, target.Name).Scan(
		&synopsis.GraphID, &synopsis.Epoch, &synopsis.SourceMutationEpoch,
		&synopsis.CurrentMutationEpoch, &synopsis.EstimatorVersion, &synopsis.Status,
		&synopsis.NodeCount, &synopsis.EdgeCount,
	); err != nil {
		if err == pgx.ErrNoRows {
			return TraversalTopologySynopsis{}, fmt.Errorf("topology synopsis graph %q does not exist", target.Name)
		}
		return TraversalTopologySynopsis{}, fmt.Errorf("publish topology synopsis: %w", err)
	}
	return synopsis, nil
}

func (s *transaction) traversalTopologySynopsis(graphID int32) (TraversalTopologySynopsis, error) {
	var synopsis TraversalTopologySynopsis
	const statement = `
select epoch.graph_id::int4, coalesce(synopsis.epoch, 0), coalesce(synopsis.source_mutation_epoch, 0),
       epoch.epoch, coalesce(synopsis.estimator_version, ''), coalesce(synopsis.status, ''),
       coalesce(synopsis.node_count, 0), coalesce(synopsis.edge_count, 0)
from graph_traversal_epoch epoch
left join graph_traversal_synopsis_generation synopsis on synopsis.graph_id = epoch.graph_id
where epoch.graph_id = $1`
	err := s.driver().QueryRow(s.ctx, statement, graphID).Scan(
		&synopsis.GraphID, &synopsis.Epoch, &synopsis.SourceMutationEpoch,
		&synopsis.CurrentMutationEpoch, &synopsis.EstimatorVersion, &synopsis.Status,
		&synopsis.NodeCount, &synopsis.EdgeCount,
	)
	if err != nil {
		return TraversalTopologySynopsis{}, err
	}
	return synopsis, nil
}

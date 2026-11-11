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
	// GraphID identifies the graph summarized by this record.
	GraphID int32 `json:"graph_id"`

	// Epoch identifies this atomically published synopsis generation.
	Epoch uint64 `json:"epoch"`

	// SourceMutationEpoch is the graph mutation epoch observed while building the synopsis.
	SourceMutationEpoch uint64 `json:"source_mutation_epoch"`

	// CurrentMutationEpoch is the graph mutation epoch visible when the synopsis was read.
	CurrentMutationEpoch uint64 `json:"current_mutation_epoch"`

	// EstimatorVersion identifies the fixed estimator whose assumptions produced the synopsis.
	EstimatorVersion string `json:"estimator_version"`

	// SchemaVersion identifies the synopsis storage schema used to publish this record.
	SchemaVersion string `json:"schema_version"`

	// Status reports whether publication completed successfully.
	Status string `json:"status"`

	// NodeCount is the graph's node count recorded by the refresh.
	NodeCount int64 `json:"node_count"`

	// EdgeCount is the graph's edge count recorded by the refresh.
	EdgeCount int64 `json:"edge_count"`
}

// Available reports whether this synopsis was atomically published for the
// current visible graph mutation epoch.
func (s TraversalTopologySynopsis) Available() bool {
	return s.Epoch != 0 && s.Status == "ready" && s.SourceMutationEpoch == s.CurrentMutationEpoch
}

// RefreshTraversalTopologySynopsis publishes a versioned graph topology
// synopsis. The graph epoch row is locked before collection, so the synopsis
// is current at its commit boundary; a mutation committed afterwards can
// immediately make it stale.
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

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("begin topology synopsis refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		graphID                                      int32
		sourceEpoch, nodeCount, edgeCount, nextEpoch int64
	)
	if err := tx.QueryRow(ctx, lockTopologySynopsisGraphQuery, target.Name).Scan(&graphID, &sourceEpoch); err != nil {
		if err == pgx.ErrNoRows {
			return TraversalTopologySynopsis{}, fmt.Errorf("topology synopsis graph %q does not exist", target.Name)
		}
		return TraversalTopologySynopsis{}, fmt.Errorf("read topology synopsis graph: %w", err)
	}
	if err := tx.QueryRow(ctx, countTopologySynopsisGraphQuery, graphID).Scan(&nodeCount, &edgeCount); err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("count topology synopsis graph: %w", err)
	}
	if err := tx.QueryRow(ctx, nextTopologySynopsisEpochQuery, graphID).Scan(&nextEpoch); err == pgx.ErrNoRows {
		nextEpoch = 1
	} else if err != nil {
		return TraversalTopologySynopsis{}, fmt.Errorf("read topology synopsis generation: %w", err)
	}
	for _, operation := range []topologySynopsisOperation{
		{
			query: deleteTopologySynopsisDegreesQuery,
		},
		{
			query: deleteTopologySynopsisEdgeCountsQuery,
		},
		{
			query: deleteTopologySynopsisNodeCountsQuery,
		},
		{
			query:     insertTopologySynopsisNodeCountsQuery,
			withEpoch: true,
		},
		{
			query:     insertTopologySynopsisEdgeCountsQuery,
			withEpoch: true,
		},
		{
			query:     insertTopologySynopsisDegreesQuery,
			withEpoch: true,
		},
	} {
		args := []any{graphID}
		if operation.withEpoch {
			args = append(args, nextEpoch)
		}

		if _, err := tx.Exec(ctx, operation.query, args...); err != nil {
			return TraversalTopologySynopsis{}, fmt.Errorf("publish topology synopsis detail: %w", err)
		}
	}

	var synopsis TraversalTopologySynopsis
	if err := tx.QueryRow(ctx, publishTopologySynopsisQuery, graphID, nextEpoch, sourceEpoch, nodeCount, edgeCount).Scan(
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

// traversalTopologySynopsis reads the current published synopsis visible to this transaction.
func (s *transaction) traversalTopologySynopsis(graphID int32) (TraversalTopologySynopsis, error) {
	var synopsis TraversalTopologySynopsis
	err := s.driver().QueryRow(s.ctx, readTopologySynopsisQuery, graphID).Scan(
		&synopsis.GraphID, &synopsis.Epoch, &synopsis.SourceMutationEpoch,
		&synopsis.CurrentMutationEpoch, &synopsis.EstimatorVersion, &synopsis.SchemaVersion, &synopsis.Status,
		&synopsis.NodeCount, &synopsis.EdgeCount,
	)
	if err != nil {
		return TraversalTopologySynopsis{}, err
	}
	return synopsis, nil
}

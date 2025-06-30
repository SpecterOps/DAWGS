package v2

import (
	"context"

	"github.com/specterops/dawgs/graph"
)

type V1Database interface {
	Database

	// FetchKinds retrieves the complete list of kinds available to the database.
	FetchKinds(ctx context.Context) (graph.Kinds, error)

	// RefreshKinds refreshes the in memory kinds maps
	RefreshKinds(ctx context.Context) error
}

type v1Database struct {
	internal Database
}

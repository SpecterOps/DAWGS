package graph

import (
	"context"
)

// TargetStorage identifies a region of graph storage that an
// optimization pass may target.
type TargetStorage int

const (
	Nodes TargetStorage = iota
	Relationships
)

// OptimizeConfig is the resolved configuration for a single Optimize call.
// Drivers consume this struct after all OptimizeOption values have been
// applied. Options unsupported by a given driver should be ignored.
type OptimizeConfig struct {
	// Targets is the set of storage regions to optimize. An empty slice means
	// "all regions the driver knows about".
	Targets []TargetStorage
}

// OptimizeOption mutates an OptimizeConfig.
type OptimizeOption func(*OptimizeConfig)

// OptimizeTargets restricts the optimization pass to the given storage regions.
func OptimizeTargets(targets ...TargetStorage) OptimizeOption {
	return func(c *OptimizeConfig) {
		c.Targets = append(c.Targets, targets...)
	}
}

// StorageMaintainer is an optional capability implemented by drivers that can
// perform storage maintenance. Consumers should detect support with a
// type assertion against a graph.Database:
//
//	if m, ok := db.(graph.StorageMaintainer); ok {
//	    err := m.Optimize(ctx, graph.OptimizeTargets(graph.Nodes, graph.Relationships))
//	    ...
//	}
type StorageMaintainer interface {
	Optimize(ctx context.Context, opts ...OptimizeOption) error
}

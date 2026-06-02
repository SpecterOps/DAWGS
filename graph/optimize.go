package graph

import (
	"context"
)

// TargetStorage identifies a region of graph storage that an optimization
// pass may target.
type TargetStorage int

const (
	Nodes TargetStorage = iota
	Relationships
)

// OptimizeConfig is the resolved configuration for a single OptimizeStorage call.
// Drivers apply every OptimizeOption to this struct before reading it.
type OptimizeConfig struct {
	// Targets is the set of storage regions to optimize. A nil or empty
	// slice instructs the driver to optimize every region it knows about.
	Targets []TargetStorage
}

// OptimizeOption mutates an OptimizeConfig and is applied in the order
// passed to Optimize.
type OptimizeOption func(*OptimizeConfig)

// OptimizeTargets restricts the optimization pass to the given storage
// regions. Repeated calls append targets rather than replacing them.
// Calling OptimizeTargets with no arguments does not change the resolved
// target set; if no targets are configured by the time OptimizeStorage runs, the
// driver treats that as "all regions it knows about".
func OptimizeTargets(targets ...TargetStorage) OptimizeOption {
	return func(c *OptimizeConfig) {
		c.Targets = append(c.Targets, targets...)
	}
}

// StorageOptimizer is an optional capability implemented by drivers that
// can perform storage optimization. Drivers that cannot perform meaningful optimization
// must not implement this interface; a failed type assertion is the
// signal for "this driver does not support storage optimization".
// A non-nil error returned from OptimizeStorage signals a driver-specific failure
// during a supported call.
//
// Consumers detect support with a type assertion against a graph.Database:
//
//	if m, ok := db.(graph.StorageOptimizer); ok {
//	    err := m.Optimize(ctx, graph.OptimizeTargets(graph.Nodes, graph.Relationships))
//	    ...
//	}
type StorageOptimizer interface {
	// OptimizeStorage runs a storage optimization pass against the regions
	// identified by opts. With no options, every region the driver
	// recognizes is optimized.
	OptimizeStorage(ctx context.Context, opts ...OptimizeOption) error
}

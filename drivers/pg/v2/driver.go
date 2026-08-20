package v2

import (
	"context"
	"sync"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

// Driver is the explicit opt-in PostgreSQL v2 shell. Stable PostgreSQL
// behavior is delegated to pg.Driver; only cache ownership and its lifecycle
// coordination are implemented here.
type Driver struct {
	delegate *pg.Driver
	pool     *Pool

	closeOnce sync.Once
	closeErr  error
}

var _ graph.Database = (*Driver)(nil)

// NewDriver constructs a v2 driver for pool. Pool must have been returned by
// NewPool so its cache provider owns the exact physical connections used by
// the delegate.
func NewDriver(graphQueryMemoryLimit size.Size, pool *Pool) *Driver {
	return &Driver{
		delegate: pg.NewDriverWithTranslationCacheProvider(graphQueryMemoryLimit, pool.pool, pool.provider),
		pool:     pool,
	}
}

// SetWriteFlushSize forwards the stable PostgreSQL operation.
func (s *Driver) SetWriteFlushSize(interval int) {
	s.delegate.SetWriteFlushSize(interval)
}

// SetBatchWriteSize forwards the stable PostgreSQL operation.
func (s *Driver) SetBatchWriteSize(interval int) {
	s.delegate.SetBatchWriteSize(interval)
}

// ReadTransaction forwards the stable PostgreSQL operation.
func (s *Driver) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return s.delegate.ReadTransaction(ctx, delegate, options...)
}

// WriteTransaction forwards the stable PostgreSQL operation.
func (s *Driver) WriteTransaction(ctx context.Context, delegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return s.delegate.WriteTransaction(ctx, delegate, options...)
}

// BatchOperation forwards the stable PostgreSQL operation.
func (s *Driver) BatchOperation(ctx context.Context, delegate graph.BatchDelegate, options ...graph.BatchOption) error {
	return s.delegate.BatchOperation(ctx, delegate, options...)
}

// AssertSchema forwards schema assertion and advances generation only after a
// successful assertion. The delegated pool reset closes idle connections and
// the v2 BeforeClose hook retires their cache state.
func (s *Driver) AssertSchema(ctx context.Context, schema graph.Schema) error {
	if err := s.delegate.AssertSchema(ctx, schema); err != nil {
		return err
	}
	s.pool.provider.advanceSchemaGeneration()
	return nil
}

// SetDefaultGraph forwards the stable PostgreSQL operation.
func (s *Driver) SetDefaultGraph(ctx context.Context, schema graph.Graph) error {
	return s.delegate.SetDefaultGraph(ctx, schema)
}

// Run forwards the stable PostgreSQL operation.
func (s *Driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	return s.delegate.Run(ctx, query, parameters)
}

// Close closes the v1 delegate and pool first, then releases any provider
// state not already retired through BeforeClose. It is safe to call repeatedly.
func (s *Driver) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.delegate != nil {
			s.closeErr = s.delegate.Close(ctx)
		}
		if s.pool != nil {
			s.pool.closeProvider()
		}
	})
	return s.closeErr
}

// FetchKinds forwards the stable PostgreSQL operation.
func (s *Driver) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	return s.delegate.FetchKinds(ctx)
}

// RefreshKinds forwards kind-cache refresh and advances schema generation
// after a successful refresh.
func (s *Driver) RefreshKinds(ctx context.Context) error {
	if err := s.delegate.RefreshKinds(ctx); err != nil {
		return err
	}
	s.pool.provider.advanceSchemaGeneration()
	return nil
}

// RefreshTraversalTopologySynopsis explicitly publishes an advisory graph
// summary for shadow selection. It does not enable or alter a SQL strategy.
func (s *Driver) RefreshTraversalTopologySynopsis(ctx context.Context, target graph.Graph) (pg.TraversalTopologySynopsis, error) {
	return s.delegate.RefreshTraversalTopologySynopsis(ctx, target)
}

// OptimizeStorage forwards the stable PostgreSQL operation.
func (s *Driver) OptimizeStorage(ctx context.Context) error {
	return s.delegate.OptimizeStorage(ctx)
}

// KindMapper forwards the PostgreSQL-specific kind mapper extension.
func (s *Driver) KindMapper() pg.KindMapper {
	return s.delegate.KindMapper()
}

// DefaultGraph forwards the PostgreSQL-specific default graph metadata.
func (s *Driver) DefaultGraph() (model.Graph, bool) {
	return s.delegate.DefaultGraph()
}

// TranslationCacheStats reports v2 connection-local cache statistics.
func (s *Driver) TranslationCacheStats() Stats {
	if s == nil || s.pool == nil {
		return Stats{}
	}
	return s.pool.provider.stats()
}

// WarmStatements prepares explicitly selected hot PostgreSQL statements on
// currently idle physical connections without executing them. It is opt-in and
// should be called after schema assertion.
func (s *Driver) WarmStatements(ctx context.Context, statements ...string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return s.pool.WarmStatements(ctx, statements...)
}

// ParseCacheStats forwards v1's still-driver-wide parse-cache statistics.
func (s *Driver) ParseCacheStats() pg.ParseCacheStats {
	if s == nil || s.delegate == nil {
		return pg.ParseCacheStats{}
	}
	return s.delegate.ParseCacheStats()
}

// FetchSchema forwards the PostgreSQL-specific schema surface.
func (s *Driver) FetchSchema(ctx context.Context) (graph.Schema, error) {
	return s.delegate.FetchSchema(ctx)
}

// WipeGraph forwards the PostgreSQL-specific graph maintenance operation.
func (s *Driver) WipeGraph(ctx context.Context, retain graph.TransactionDelegate) error {
	return s.delegate.WipeGraph(ctx, retain)
}

// DeleteNodesByKinds forwards the PostgreSQL-specific set-based delete.
func (s *Driver) DeleteNodesByKinds(ctx context.Context, includeAny, excludeAny graph.Kinds) error {
	return s.delegate.DeleteNodesByKinds(ctx, includeAny, excludeAny)
}

// DeleteRelationshipsByKinds forwards the PostgreSQL-specific set-based delete.
func (s *Driver) DeleteRelationshipsByKinds(ctx context.Context, kinds graph.Kinds) error {
	return s.delegate.DeleteRelationshipsByKinds(ctx, kinds)
}

// SetTraversalPolicy forwards the stable PostgreSQL traversal policy. Policy
// identity already partitions translated SQL, so no schema-generation change
// is required.
func (s *Driver) SetTraversalPolicy(policy pg.TraversalPolicy) error {
	return s.delegate.SetTraversalPolicy(policy)
}

// TraversalPolicy returns the active PostgreSQL traversal policy.
func (s *Driver) TraversalPolicy() pg.TraversalPolicy {
	return s.delegate.TraversalPolicy()
}

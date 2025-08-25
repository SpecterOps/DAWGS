package changelog

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/util/channels"
)

const (
	// Limit batch sizes
	BATCH_SIZE = 1_000
)

// todo: figure these out, esp. last seen for reconciliation
var (
	ignoredPropertiesKeys = map[string]struct{}{
		"lastseen": {},
		// common.ObjectID.String():      {},
		// common.LastSeen.String():      {},
		// common.LastCollected.String(): {},
		// common.IsInherited.String():   {},
		// ad.DomainSID.String():         {},
		// ad.IsACL.String():             {},
		// azure.TenantID.String():       {},
	}
)

type Changelog struct {
	cache     cache
	db        db
	loop      loop
	batchSize int
}

func NewChangelog(pgxPool *pgxpool.Pool, kindMapper pg.KindMapper, batchSize int) *Changelog {
	cache := newCache()
	db := newDB(pgxPool, kindMapper)

	return &Changelog{
		cache:     cache,
		db:        db,
		batchSize: batchSize,
	}
}

// Start begins a long-running loop that buffers and flushes node/edge updates
func (s *Changelog) Start(ctx context.Context) {
	s.loop = newLoop(ctx, &s.db, s.batchSize)

	go func() {
		if err := s.loop.start(ctx); err != nil {
			slog.ErrorContext(ctx, "changelog loop exited with error", "err", err)
		}
	}()
}

func (s *Changelog) GetStats() CacheStats {
	return s.cache.getStats()
}

func (s *Changelog) FlushStats() {
	stats := s.cache.resetStats()
	slog.Info("changelog metrics",
		"hits", stats.Hits,
		"misses", stats.Misses,
	)
}

func (s *Changelog) ResolveChange(ctx context.Context, change Change) (bool, error) {
	return s.cache.shouldSubmit(change)
}

func (s *Changelog) Submit(ctx context.Context, change Change) bool {
	return channels.Submit(ctx, s.loop.WriterC, change)
}

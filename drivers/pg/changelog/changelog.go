package changelog

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/util/channels"
)

type Changelog struct {
	cache   cache
	db      db
	loop    loop
	options Options
}

type Options struct {
	BatchSize     int
	FlushInterval time.Duration
}

func DefaultOptions() Options {
	return Options{
		BatchSize:     1_000,
		FlushInterval: 5 * time.Second,
	}
}

func NewChangelog(pgxPool *pgxpool.Pool, kindMapper pg.KindMapper, opts Options) *Changelog {
	cache := newCache()
	db := newDB(pgxPool, kindMapper)

	return &Changelog{
		cache:   cache,
		db:      db,
		options: opts,
	}
}

// Start begins a long-running loop that buffers and flushes node/edge updates
func (s *Changelog) Start(ctx context.Context) {
	s.loop = newLoop(ctx, &s.db, s.options.BatchSize, s.options.FlushInterval)

	go func() {
		if err := s.loop.start(ctx); err != nil {
			slog.ErrorContext(ctx, "changelog loop exited with error", "err", err)
		}
	}()
}

func (s *Changelog) GetStats() cacheStats {
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
	return channels.Submit(ctx, s.loop.writerC, change)
}

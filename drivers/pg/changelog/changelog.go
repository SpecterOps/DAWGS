package changelog

import (
	"context"
	"fmt"

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
	Cache changeCache
	DB    db
	Loop  loop
}

func NewChangelog(ctx context.Context, pgxPool *pgxpool.Pool, batchSize int, kindMapper pg.KindMapper) (*Changelog, error) {
	cache := newChangeCache()
	db := newLogDB(pgxPool, kindMapper)
	loop := newLoop(ctx, &db, batchSize)

	go loop.start(ctx)

	return &Changelog{
		Cache: cache,
		DB:    db,
		Loop:  loop,
	}, nil
}

func (s *Changelog) Size() int {
	return len(s.Cache.data)
}

func (s *Changelog) ResolveChange(ctx context.Context, proposedChange Change) (bool, error) {
	if shouldSubmit, err := s.Cache.checkCache(proposedChange); err != nil {
		return shouldSubmit, fmt.Errorf("check cache: %w", err)
	} else {
		return shouldSubmit, nil
	}
}

func (s *Changelog) Submit(ctx context.Context, change Change) bool {
	return channels.Submit(ctx, s.Loop.WriterC, change)
}

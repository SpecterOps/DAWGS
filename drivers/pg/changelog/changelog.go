package changelog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
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

type ChangeLogger interface {
	Append(change Change) error
	BuildGraph() error
}

// Changelog *will* implement ChangeLogger
type Changelog struct {
	Cache changeCache
	DB    db
	Loop  loop
}

func NewChangelog(ctx context.Context, pgxPool *pgxpool.Pool, batchSize int) (*Changelog, error) {
	cache := newChangeCache()
	db, err := newLogDB(ctx, pgxPool)
	loop := newLoop(ctx, &db, batchSize)

	if err != nil {
		return &Changelog{}, fmt.Errorf("initializing log DB: %w", err)
	}

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

// ResolveNodeChange decorated proposedChange with diff details by comparing it to the last seen record in the DB.
// it mutates proposedChange to set:
// - changeType: Added, Modified, or NoChange
// - ModifiedProperties: key-value pairs that are new or changed
// - Deleted: list of removed property keys
// Kinds are treated as upsert-always, this function does not diff them kinds.
func (s *Changelog) ResolveNodeChange(ctx context.Context, proposedChange *NodeChange) (bool, error) {
	if shouldSubmit, err := s.Cache.checkCache(proposedChange); err != nil {
		return shouldSubmit, fmt.Errorf("check cache: %w", err)
	} else {
		return shouldSubmit, nil
	}
}

func (s *Changelog) Submit(ctx context.Context, change Change) bool {
	return channels.Submit(ctx, s.Loop.WriterC, change)
}

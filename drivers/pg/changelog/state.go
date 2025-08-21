package changelog

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type GetFlagByKeyer interface {
	// GetFlagByKey attempts to fetch a FeatureFlag by its key.
	GetFlagByKey(context.Context, string) (bool, error)
}

type stateManager struct {
	lastTablePartitionAssertion time.Time
	featureFlagCheckInterval    time.Duration
	stateLock                   *sync.RWMutex
}

func newStateManager() *stateManager {
	return &stateManager{
		featureFlagCheckInterval: time.Minute,
		stateLock:                &sync.RWMutex{},
	}
}

func (s *stateManager) checkChangelogPartitions(ctx context.Context, now time.Time, pgxPool *pgxpool.Pool) error {
	if !shouldAssertNextPartition(s.lastTablePartitionAssertion) {
		// Early exit as the last check remains authoritative
		return nil
	}

	slog.InfoContext(ctx, fmt.Sprintf("Assert changelog partition: %s", now))

	if err := AssertChangelogPartition(ctx, pgxPool); err != nil {
		return fmt.Errorf("asserting changelog partition: %w", err)
	}

	s.lastTablePartitionAssertion = now
	return nil
}

package changestream

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
	flags                       GetFlagByKeyer // todo: how to pass this bloodhound type into dawgs?
	enabled                     bool
}

func newStateManager(flags GetFlagByKeyer) *stateManager {
	return &stateManager{
		featureFlagCheckInterval: time.Minute,
		stateLock:                &sync.RWMutex{},
		flags:                    flags,
		enabled:                  false,
	}
}

func (s *stateManager) isEnabled() bool {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	return s.enabled
}

func (s *stateManager) enableDaemon() {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	// Don't spam the log and allow the function to be reentrant
	if !s.enabled {
		slog.Info("enabling changelog")
		s.enabled = true
	}
}

func (s *stateManager) disableDaemon() {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	// Don't spam the log and allow the function to be reentrant
	if s.enabled {
		slog.Info("disabling changelog")
		s.enabled = false
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

// todo: unexport
func (s *stateManager) CheckFeatureFlag(ctx context.Context) error {
	// todo: the initial POC had the following guard in to reduce # of calls.
	// im not sure if we want to keep it
	// if since := now.Sub(s.lastFeatureFlagCheck); since < s.featureFlagCheckInterval {
	// 	// Early exit as the last check remains authoritative
	// 	return nil
	// }

	if isEnabled, err := s.flags.GetFlagByKey(ctx, "changelog"); err != nil {
		return err
	} else if isEnabled {
		s.enableDaemon()
	} else {
		s.disableDaemon()
	}

	// Update the last flag check time
	// s.lastFeatureFlagCheck = now
	return nil
}

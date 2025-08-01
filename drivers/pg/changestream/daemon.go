package changestream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/util/channels"
)

var (
	// todo: why are the following properties safe to always ignore when computing hashes?
	// todo: the common, ad, and azure packages where these string constants are defined are
	// in ce repo. whats a good way to pull these constants in to dawgs?
	ignoredPropertiesKeys = map[string]struct{}{
		// common.ObjectID.String():      {},
		// common.LastSeen.String():      {},
		// common.LastCollected.String(): {},
		// common.IsInherited.String():   {},
		// ad.DomainSID.String():         {},
		// ad.IsACL.String():             {},
		// azure.TenantID.String():       {},
	}
)

type Log interface {
	ResolveNodeChangeStatus(ctx context.Context, proposedChange *NodeChange) (ChangeStatus, error)
	CheckCachedNodeChange(proposedChange *NodeChange) (ChangeStatus, error)
	LastEdgeChange(ctx context.Context, proposedChange *EdgeChange) (ChangeStatus, error)
	CachedLastEdgeChange(proposedChange *EdgeChange) (ChangeStatus, error)
	Submit(ctx context.Context, change Change) bool
}

type Daemon struct {
	// ingest writes to writerC.
	writerC chan<- Change
	// we continuously read changes from readerC to insert into pg
	readerC <-chan Change

	pgxPool          *pgxpool.Pool
	kindMapper       pg.KindMapper
	changeCacheLock  *sync.RWMutex
	changeCache      map[string]ChangeStatus
	State            stateManager // todo: can we make this private?
	nodeChangeBuffer []*NodeChange
	edgeChangeBuffer []*EdgeChange
}

func NewDaemon(ctx context.Context, flags GetFlagByKeyer, pgxPool *pgxpool.Pool) *Daemon {
	return &Daemon{
		changeCache:     make(map[string]ChangeStatus),
		changeCacheLock: &sync.RWMutex{},
		State:           newStateManager(flags),
		pgxPool:         pgxPool,
	}
}

func (s *Daemon) PGX() *pgxpool.Pool {
	return s.pgxPool
}

func (s *Daemon) CheckCachedNodeChange(proposedChange *NodeChange) (ChangeStatus, error) {
	var (
		lastChange  ChangeStatus
		identityKey = proposedChange.IdentityKey()
	)

	if propertiesHash, err := proposedChange.Properties.Hash(ignoredPropertiesKeys); err != nil {
		return lastChange, err
	} else {
		// Track the properties hash and kind IDs
		lastChange.PropertiesHash = propertiesHash
	}

	if cachedChange, hasCachedChange := s.LastCachedChange(identityKey); hasCachedChange {
		lastChange.Changed = !bytes.Equal(lastChange.PropertiesHash, cachedChange.PropertiesHash)
		lastChange.Exists = true
	} else {
		// If the change log is disabled then mark every non-cached lookup as changed
		lastChange.Changed = !s.State.isEnabled()
	}

	// Ensure this makes it into the cache before returning
	s.PutCachedChange(identityKey, lastChange)
	return lastChange, nil
}

func (s *Daemon) ResolveNodeChangeStatus(ctx context.Context, proposedChange *NodeChange) (ChangeStatus, error) {
	lastChange, err := s.CheckCachedNodeChange(proposedChange)

	if err != nil || lastChange.Exists {
		return lastChange, err
	}

	if s.State.isEnabled() {
		var (
			// todo: define lastNodeChangeSQL
			lastChangeRow = s.pgxPool.QueryRow(ctx, LAST_NODE_CHANGE_SQL, proposedChange.NodeID, lastChange.PropertiesHash)
			err           = lastChangeRow.Scan(&lastChange.Changed, &lastChange.Type)
		)

		// Assume that the change that exists in some form and error inspect for the negative case
		lastChange.Exists = true

		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				// Exit here as this is an unexpected error
				return lastChange, err
			}

			// No rows found means the change does not exist
			lastChange.Exists = false
		}

		// Ensure this makes it into the cache before returning
		s.PutCachedChange(proposedChange.IdentityKey(), lastChange)
	}

	return lastChange, nil
}

func (s *Daemon) Submit(ctx context.Context, change Change) bool {
	return channels.Submit(ctx, s.writerC, change)
}

func (s *Daemon) LastCachedChange(cacheKey string) (ChangeStatus, bool) {
	s.changeCacheLock.RLock()
	defer s.changeCacheLock.RUnlock()

	cachedChange, hasCachedChange := s.changeCache[cacheKey]
	return cachedChange, hasCachedChange
}

func (s *Daemon) PutCachedChange(cacheKey string, cachedLookup ChangeStatus) {
	s.changeCacheLock.Lock()
	defer s.changeCacheLock.Unlock()

	s.changeCache[cacheKey] = cachedLookup
}

func (s *Daemon) RunLoop(ctx context.Context) error {
	if _, err := s.pgxPool.Exec(ctx, ASSERT_TABLE_SQL); err != nil {
		return fmt.Errorf("failed asserting changelog tablespace: %w", err)
	}
	if err := AssertChangelogPartition(ctx, s.pgxPool); err != nil {
		return fmt.Errorf("failed asserting changelog partition: %w", err)
	}

	go s.runLoopBody(ctx, 5*time.Second)
	return nil
}

func (s *Daemon) runLoopBody(ctx context.Context, flushInterval time.Duration) {
	ticker := time.NewTicker(flushInterval)
	defer func() {
		s.pgxPool.Close()
		close(s.writerC)
		ticker.Stop()
		slog.InfoContext(ctx, "Shutting down change stream")
	}()

	slog.InfoContext(ctx, "Starting change stream")

	// lastNodeWatermark := 0
	// lastEdgeWatermark := 0

	for {
		now := time.Now()

		select {
		case <-ctx.Done():
			return

		case change := <-s.readerC:
			s.handleIncomingChange(ctx, change, now)

		case <-ticker.C:
			// s.maybeFlush(ctx, now, &lastNodeWatermark, &lastEdgeWatermark)
		}
	}
}

func (s *Daemon) handleIncomingChange(ctx context.Context, change Change, now time.Time) {
	if err := s.State.CheckFeatureFlag(ctx); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("feature flag check failed: %v", err))
		return
	}

	if !s.State.isEnabled() {
		return
	}

	if err := s.State.checkChangelogPartitions(ctx, now, s.pgxPool); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("partition check failed: %v", err))
		return
	}

	// s.queueChange(ctx, now, change)
}

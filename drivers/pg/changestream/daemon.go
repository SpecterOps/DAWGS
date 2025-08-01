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

const (
	// Limit batch sizes
	BATCH_SIZE = 1_000
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

func NewDaemon(ctx context.Context, flags GetFlagByKeyer, pgxPool *pgxpool.Pool, kindMapper pg.KindMapper) *Daemon {
	return &Daemon{
		changeCache:     make(map[string]ChangeStatus),
		changeCacheLock: &sync.RWMutex{},
		State:           newStateManager(flags),
		pgxPool:         pgxPool,
		kindMapper:      kindMapper,
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
	// initialize the node_change_stream, edge_change_stream tables
	if _, err := s.pgxPool.Exec(ctx, ASSERT_TABLE_SQL); err != nil {
		return fmt.Errorf("failed asserting changelog tablespace: %w", err)
	}

	// todo: return to partitioning once initial implementation is working on one table
	// if err := AssertChangelogPartition(ctx, s.pgxPool); err != nil {
	// 	return fmt.Errorf("failed asserting changelog partition: %w", err)
	// }

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

	lastNodeWatermark := 0
	// lastEdgeWatermark := 0

	for {
		now := time.Now()

		select {
		case <-ctx.Done():
			return

		case change := <-s.readerC:
			s.handleIncomingChange(ctx, change, now)

		case <-ticker.C:
			lastNodeWatermark = s.maybeFlush(ctx, lastNodeWatermark)
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

	// if err := s.State.checkChangelogPartitions(ctx, now, s.pgxPool); err != nil {
	// 	slog.ErrorContext(ctx, fmt.Sprintf("partition check failed: %v", err))
	// 	return
	// }

	s.QueueChange(ctx, now, change)
}

func (s *Daemon) QueueChange(ctx context.Context, now time.Time, nextChange Change) {
	switch typedChange := nextChange.(type) {
	case *NodeChange:
		s.nodeChangeBuffer = append(s.nodeChangeBuffer, typedChange)

		if len(s.nodeChangeBuffer) >= BATCH_SIZE {
			if err := s.FlushNodeChanges(ctx); err != nil {
				slog.ErrorContext(ctx, fmt.Sprintf("writing changes to change stream failed: %v", err))
			}
		}

	case *EdgeChange:

	default:
		slog.ErrorContext(ctx, fmt.Sprintf("Unexpected change type: %T", nextChange))
	}
}

func (s *Daemon) FlushNodeChanges(ctx context.Context) error {
	// Early exit check for empty buffer flushes
	if len(s.nodeChangeBuffer) == 0 {
		return nil
	}

	var (
		numChanges  = len(s.nodeChangeBuffer)
		now         = time.Now()
		copyColumns = []string{
			"node_id",
			"kind_ids",
			"properties_hash",
			"property_fields",
			"change_type",
			"created_at",
		}

		iteratorFunc = func(idx int) ([]any, error) {
			nextNodeChange := s.nodeChangeBuffer[idx]

			if mappedKindIDs, err := s.kindMapper.MapKinds(ctx, nextNodeChange.Kinds); err != nil {
				return nil, fmt.Errorf("node kind ID mapping error: %w", err)
			} else if propertiesHash, err := nextNodeChange.Properties.Hash(ignoredPropertiesKeys); err != nil {
				return nil, fmt.Errorf("node properties hash error: %w", err)
			} else {
				rows := []any{
					nextNodeChange.NodeID,
					mappedKindIDs,
					propertiesHash,
					nextNodeChange.Properties.Keys(nil),
					nextNodeChange.Type(),
					now,
				}

				return rows, nil
			}
		}
	)

	slog.DebugContext(ctx, fmt.Sprintf("flushing %d node changes", numChanges))

	// pgx.Identifier{partitionName}
	if _, err := s.pgxPool.CopyFrom(ctx, pgx.Identifier{"node_change_stream"}, copyColumns, pgx.CopyFromSlice(numChanges, iteratorFunc)); err != nil {
		slog.Info(fmt.Sprintf("change stream node change insert error: %v", err))
	}

	// TODO: Should the buffer clear be dependent on no errors?
	s.nodeChangeBuffer = s.nodeChangeBuffer[:0]
	return nil
}

// maybeFlush will only flush changes when the buffer has not increased in size in two ticks.
// if the buffer is the same size that implies no new changes have arrived
func (s *Daemon) maybeFlush(ctx context.Context, lastNodeWatermark int) int {
	numNodeChanges := len(s.nodeChangeBuffer)

	hasNodeChangesToFlush := numNodeChanges > 0 && numNodeChanges == lastNodeWatermark

	if !hasNodeChangesToFlush { // &&!hasEdgeChangesToFlush
		// No eligible flush needed
		lastNodeWatermark = numNodeChanges
		return lastNodeWatermark
	}

	// todo: add partitioning l8r
	// if err := s.State.checkChangelogPartitions(ctx, now, s.pgxPool); err != nil {
	// 	slog.ErrorContext(ctx, fmt.Sprintf("change log state check error: %v", err))
	// 	return
	// }

	if hasNodeChangesToFlush {
		if err := s.FlushNodeChanges(ctx); err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("writing node changes to change stream failed: %v", err))
		}
	}

	lastNodeWatermark = len(s.nodeChangeBuffer)

	return lastNodeWatermark
}

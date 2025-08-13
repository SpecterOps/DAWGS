package changestream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
)

const (
	// Limit batch sizes
	BATCH_SIZE = 1_000
)

var (
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

// todo: use golang-LRU cache
// time multiple ingest runs with no cache, size 1_000, 100_000
type ChangeCache struct {
	data  map[string]ChangeStatus
	mutex *sync.RWMutex
}

func newChangeCache() ChangeCache {
	return ChangeCache{
		data:  make(map[string]ChangeStatus),
		mutex: &sync.RWMutex{},
	}
}

func (s *ChangeCache) get(key string) (ChangeStatus, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *ChangeCache) put(key string, value ChangeStatus) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[key] = value
}

func (s *ChangeCache) evaluateNodeChange(proposedChange *NodeChange) (ChangeStatus, error) {
	var (
		status      ChangeStatus
		identityKey = proposedChange.IdentityKey()
	)

	if hash, err := proposedChange.Hash(); err != nil {
		return status, err
	} else {
		// Track the properties hash and kind IDs
		status.PropertiesHash = hash
	}

	if cachedChange, ok := s.get(identityKey); ok {
		status.Changed = !bytes.Equal(status.PropertiesHash, cachedChange.PropertiesHash)
		status.Exists = true
	} else {
		// mark every non-cached lookup as changed
		status.Changed = true
	}

	// Ensure this makes it into the cache before returning
	s.put(identityKey, status)
	return status, nil
}

func (s *ChangeCache) evaluateChange(proposedChange Change) (ChangeStatus, error) {
	var (
		status      ChangeStatus
		identityKey = proposedChange.IdentityKey()
	)

	if hash, err := proposedChange.Hash(); err != nil {
		return status, err
	} else {
		// Track the properties hash and kind IDs
		status.PropertiesHash = hash
	}

	if cachedChange, ok := s.get(identityKey); ok {
		status.Changed = !bytes.Equal(status.PropertiesHash, cachedChange.PropertiesHash)
		status.Exists = true
	} else {
		// mark every non-cached lookup as changed
		status.Changed = true
	}

	// Ensure this makes it into the cache before returning
	s.put(identityKey, status)
	return status, nil
}

type ChangeWriter struct {
	PGX        *pgxpool.Pool
	KindMapper pg.KindMapper
	Encoder    TextArrayEncoder
}

func newChangeWriter(pgxPool *pgxpool.Pool, kindMapper pg.KindMapper) ChangeWriter {
	return ChangeWriter{
		PGX:        pgxPool,
		KindMapper: kindMapper,
		Encoder: TextArrayEncoder{
			buffer: &bytes.Buffer{},
		},
	}
}

type TextArrayEncoder struct {
	buffer *bytes.Buffer
}

func (s *TextArrayEncoder) Encode(values []string) string {
	s.buffer.Reset()
	s.buffer.WriteRune('{')

	for idx, value := range values {
		if idx > 0 {
			s.buffer.WriteRune(',')
		}

		s.buffer.WriteRune('\'')
		s.buffer.WriteString(value)
		s.buffer.WriteRune('\'')
	}

	s.buffer.WriteRune('}')
	return s.buffer.String()
}

func (s *ChangeWriter) flushNodeChanges(ctx context.Context, changes []*NodeChange) error {
	// Early exit check for empty buffer flushes
	if len(changes) == 0 {
		return nil
	}

	var (
		numChanges  = len(changes)
		now         = time.Now()
		copyColumns = []string{
			"node_id",
			"kind_ids",
			"modified_properties",
			"deleted_properties",
			"hash",
			"change_type",
			"created_at",
		}
	)

	iterator := func(i int) ([]any, error) {
		c := changes[i]

		if mappedKindIDs, err := s.KindMapper.MapKinds(ctx, c.Kinds); err != nil {
			return nil, fmt.Errorf("node kind ID mapping error: %w", err)
		} else if hash, err := c.Hash(); err != nil {
			return nil, err
		} else if modifiedProps, err := modifiedPropertiesJSON(c.Properties); err != nil {
			return nil, fmt.Errorf("failed creating node change property JSON: %w", err)
		} else {
			rows := []any{
				c.NodeID,
				mappedKindIDs,
				modifiedProps,
				s.Encoder.Encode(c.Properties.DeletedList()),
				hash,
				c.Type(),
				now,
			}

			return rows, nil
		}
	}

	if _, err := s.PGX.CopyFrom(ctx, pgx.Identifier{"node_change_stream"}, copyColumns, pgx.CopyFromSlice(numChanges, iterator)); err != nil {
		return fmt.Errorf("change stream node change insert error: %v", err)
	} else {
		return nil
	}
}

func modifiedPropertiesJSON(properties *graph.Properties) ([]byte, error) {
	modifiedProperties := make(map[string]any, len(properties.Modified))

	for modifiedKey := range properties.Modified {
		modifiedProperties[modifiedKey] = properties.Map[modifiedKey]
	}

	return json.Marshal(modifiedProperties)
}

type loop struct {
	State         *stateManager
	ReaderC       <-chan Change
	WriterC       chan<- Change
	ChangeWriter  ChangeWriter
	Cache         ChangeCache
	FlushInterval time.Duration
	NodeBuffer    []*NodeChange
	BatchSize     int
}

func newLoop(ctx context.Context, state *stateManager, writer ChangeWriter, cache ChangeCache, batchSize int) loop {
	writerC, readerC := channels.BufferedPipe[Change](ctx)

	return loop{
		State:         state,
		ReaderC:       readerC,
		WriterC:       writerC,
		ChangeWriter:  writer,
		Cache:         cache,
		FlushInterval: 5 * time.Second,
		NodeBuffer:    make([]*NodeChange, 0),
		BatchSize:     batchSize,
	}
}

func (s *loop) start(ctx context.Context) error {
	ticker := time.NewTicker(s.FlushInterval)

	defer func() {
		close(s.WriterC)
		ticker.Stop()
		slog.InfoContext(ctx, "Shutting down change stream")
	}()

	// initialize the node_change_stream, edge_change_stream tables
	if _, err := s.ChangeWriter.PGX.Exec(ctx, ASSERT_NODE_CS_TABLE_SQL); err != nil {
		return fmt.Errorf("failed asserting node_change_stream tablespace: %w", err)
	}

	if _, err := s.ChangeWriter.PGX.Exec(ctx, ASSERT_EDGE_CS_TABLE_SQL); err != nil {
		return fmt.Errorf("failed asserting edge_change_stream tablespace: %w", err)
	}

	slog.InfoContext(ctx, "Starting change stream")

	lastNodeWatermark := 0

	for {
		select {
		case <-ctx.Done():
			return nil

		case change := <-s.ReaderC:
			if !s.State.isEnabled() {
				continue
			}

			switch typed := change.(type) {
			case *NodeChange:
				s.NodeBuffer = append(s.NodeBuffer, typed)
				if len(s.NodeBuffer) >= s.BatchSize {
					// todo: error handling on flush?
					if err := s.ChangeWriter.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
						slog.Warn(err.Error())
					}
					s.NodeBuffer = s.NodeBuffer[:0]
				}
			}

		case <-ticker.C:
			lastNodeWatermark = s.maybeFlush(ctx, lastNodeWatermark)
		}
	}
}

// maybeFlush checks if the node buffer has remained the same size over two ticks.
// This implies inactivity—no new changes—and triggers a flush of any remaining changes.
// The function returns the new watermark for tracking buffer growth across ticks.
func (s *loop) maybeFlush(ctx context.Context, lastNodeWatermark int) int {
	numNodeChanges := len(s.NodeBuffer)

	hasNodeChangesToFlush := numNodeChanges > 0 && numNodeChanges == lastNodeWatermark

	if !hasNodeChangesToFlush { // &&!hasEdgeChangesToFlush
		// No eligible flush needed
		lastNodeWatermark = numNodeChanges
		return lastNodeWatermark
	}

	if hasNodeChangesToFlush {
		if err := s.ChangeWriter.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
			slog.Warn(err.Error())
		}
		s.NodeBuffer = s.NodeBuffer[:0]
	}

	lastNodeWatermark = len(s.NodeBuffer)

	return lastNodeWatermark
}

// todo: ChangeLog is the public export for this package to be consumed by bloodhound.
type Changelog struct {
	Cache  ChangeCache
	Writer ChangeWriter
	Loop   loop
}

func NewChangelogDaemon(ctx context.Context, flags GetFlagByKeyer, pgxPool *pgxpool.Pool, kindMapper pg.KindMapper, batchSize int) *Changelog {
	cache := newChangeCache()
	writer := newChangeWriter(pgxPool, kindMapper)
	state := newStateManager(flags)
	loop := newLoop(ctx, state, writer, cache, batchSize)

	// prime the feature flag upon initalization
	// because state is not a pointer value this doesn't actually update state
	// so i changed it to a pointer
	state.CheckFeatureFlag(ctx)

	go loop.start(ctx)

	return &Changelog{
		Cache:  cache,
		Writer: writer,
		Loop:   loop,
	}
}

func (s *Changelog) ResolveNodeChangeStatus(ctx context.Context, proposedChange *NodeChange) (ChangeStatus, error) {
	// todo: if in cache, we early return. this ignores prop diffing
	lastChange, err := s.Cache.evaluateNodeChange(proposedChange)

	if err != nil || lastChange.Exists {
		return lastChange, err
	}

	// todo: move state should be top level so no gross nesting
	if s.Loop.State.isEnabled() {
		var (
			lastChangeRow = s.Writer.PGX.QueryRow(ctx, LAST_NODE_CHANGE_SQL, proposedChange.NodeID, lastChange.PropertiesHash)
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
		s.Cache.put(proposedChange.IdentityKey(), lastChange)
	}

	return lastChange, nil

}

func (s *Changelog) ResolveChangeStatus(ctx context.Context, proposedChange Change) (ChangeStatus, error) {
	lastChange, err := s.Cache.evaluateChange(proposedChange)

	if err != nil || lastChange.Exists {
		return lastChange, err
	}

	// todo: move state should be top level so no gross nesting
	if s.Loop.State.isEnabled() {
		var (
			lastChangeRow = s.Writer.PGX.QueryRow(ctx, proposedChange.Query(), proposedChange.IdentityKey(), lastChange.PropertiesHash)
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
		s.Cache.put(proposedChange.IdentityKey(), lastChange)
	}

	return lastChange, nil

}

func (s *Changelog) Submit(ctx context.Context, change Change) bool {
	return channels.Submit(ctx, s.Loop.WriterC, change)
}

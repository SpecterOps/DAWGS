package changestream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
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
type changeCache struct {
	data  map[string]*NodeChange
	mutex *sync.RWMutex
}

func newChangeCache() changeCache {
	return changeCache{
		data:  make(map[string]*NodeChange),
		mutex: &sync.RWMutex{},
	}
}

func (s *changeCache) get(key string) (*NodeChange, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *changeCache) put(key string, value *NodeChange) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[key] = value
}

// checkCache attempts to resolve the proposed change using the cached snapshot.
// It MUTATES proposedChange when it can fully resolve (NoChange or Modified) and returns handled=true.
// On cache miss (no entry), it returns handled=false and does not mutate proposedChange.
func (s *changeCache) checkCache(proposedChange *NodeChange) (bool, error) {
	key := proposedChange.IdentityKey()

	proposedHash, err := proposedChange.Hash()
	if err != nil {
		return false, fmt.Errorf("hash proposed change: %w", err)
	}

	// try to diff against the cached snapshot
	cached, ok := s.get(key)
	if !ok {
		return false, nil // let caller hit DB
	}

	if prevHash, err := cached.Hash(); err != nil {
		return false, nil
	} else if bytes.Equal(prevHash, proposedHash) { // hash equal -> NoChange
		proposedChange.changeType = ChangeTypeNoChange
		return true, nil
	}

	// hash differs -> modified,
	// diff against cached properties
	oldProps := cached.Properties.MapOrEmpty()
	newProps := proposedChange.Properties.MapOrEmpty()
	modified, deleted := diffProps(oldProps, newProps)

	proposedChange.changeType = ChangeTypeModified
	proposedChange.ModifiedProperties = modified
	proposedChange.Deleted = deleted

	// Update cache to the new snapshot so next call can short-circuit
	s.put(key, proposedChange)

	return true, nil
}

type db struct {
	PGX        *pgxpool.Pool
	kindMapper pg.KindMapper
	encoder    textArrayEncoder
}

func newLogDB(ctx context.Context, pgxPool *pgxpool.Pool, kindMapper pg.KindMapper) (db, error) {
	// initialize the node_change_stream, edge_change_stream tables
	if _, err := pgxPool.Exec(ctx, ASSERT_NODE_CS_TABLE_SQL); err != nil {
		return db{}, fmt.Errorf("failed asserting node_change_stream tablespace: %w", err)
	}

	if _, err := pgxPool.Exec(ctx, ASSERT_EDGE_CS_TABLE_SQL); err != nil {
		return db{}, fmt.Errorf("failed asserting edge_change_stream tablespace: %w", err)
	}

	return db{
		PGX:        pgxPool,
		kindMapper: kindMapper,
		encoder: textArrayEncoder{
			buffer: &bytes.Buffer{},
		},
	}, nil
}

type lastNodeState struct {
	Exists     bool
	Hash       []byte
	Properties map[string]any
}

func (s *db) fetchLastNodeState(ctx context.Context, nodeID string) (lastNodeState, error) {
	var (
		storedModifiedProps []byte
		storedHash          []byte
	)

	err := s.PGX.QueryRow(ctx, LAST_NODE_CHANGE_SQL_2, nodeID).Scan(&storedModifiedProps, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return lastNodeState{Exists: false}, nil
	} else if err != nil {
		return lastNodeState{}, fmt.Errorf("fetch last node state: %w", err)
	}

	props := map[string]any{}
	if len(storedModifiedProps) > 0 { //todo: error handling around stored being nil?
		if uerr := json.Unmarshal(storedModifiedProps, &props); uerr != nil {
			return lastNodeState{}, fmt.Errorf("unmarshal stored properties: %w", uerr)
		}
	}

	return lastNodeState{
		Exists:     true,
		Hash:       storedHash,
		Properties: props,
	}, nil
}

type textArrayEncoder struct {
	buffer *bytes.Buffer
}

func (s *textArrayEncoder) Encode(values []string) string {
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

func (s *db) flushNodeChanges(ctx context.Context, changes []*NodeChange) error {
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

		if mappedKindIDs, err := s.kindMapper.MapKinds(ctx, c.Kinds); err != nil {
			return nil, fmt.Errorf("node kind ID mapping error: %w", err)
		} else if hash, err := c.Hash(); err != nil {
			return nil, err
		} else if modifiedProps, err := json.Marshal(c.ModifiedProperties); err != nil {
			return nil, fmt.Errorf("failed creating node change property JSON: %w", err)
		} else {
			rows := []any{
				c.NodeID,
				mappedKindIDs,
				modifiedProps,
				s.encoder.Encode(c.Properties.DeletedProperties()),
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

func mergeNodeChanges(changes []*NodeChange) ([]*NodeChange, error) {
	if len(changes) == 0 {
		return nil, nil
	}

	// Group by node_id; keep order of first appearance
	type group struct{ idxs []int }
	groups := make(map[string]*group, len(changes))
	order := make([]string, 0, len(changes)) // todo; is this necessary?

	for idx, change := range changes {
		g, ok := groups[change.NodeID]
		if !ok {
			g = &group{}
			groups[change.NodeID] = g
			order = append(order, change.NodeID)
		}
		g.idxs = append(g.idxs, idx)
	}

	// out will hold the result of our merging routine
	out := make([]*NodeChange, 0, len(groups))

	for _, nodeID := range order {
		idxs := groups[nodeID].idxs
		baseline := changes[idxs[0]]

		// baseline FULL properties and kinds
		baseProps := baseline.Properties.Clone()
		baseKinds := baseline.Kinds.Copy()

		for _, idx := range idxs[1:] {
			change := changes[idx]

			// apply modified
			for k, v := range change.ModifiedProperties {
				baseProps.Set(k, v)
			}

			// apply deletions
			for _, k := range change.Deleted {
				// delete from modified if possible, otherwise append to deletedlist
				if _, ok := baseProps.Modified[k]; ok {
					delete(baseProps.Modified, k)
				} else {
					baseProps.Deleted[k] = struct{}{}
				}
			}

			// union kinds. i think this handler removes dupes
			baseKinds = baseKinds.Add(change.Kinds...)
		}

		// this is how we handle it in the resolver, but perhaps graph.properties API gives us some niceties...
		// modified, deleted := diffProps(baseProps)

		finalType := baseline.Type()
		switch {
		case finalType == ChangeTypeAdded:
			// keep. this is a brand-new node this flush
		case len(baseProps.ModifiedProperties()) == 0 || len(baseProps.Deleted) == 0:
			finalType = ChangeTypeNoChange
		default:
			finalType = ChangeTypeModified
		}

		mergedChange := &NodeChange{
			NodeID:             nodeID,
			Kinds:              baseKinds,
			ModifiedProperties: baseProps.ModifiedProperties(),
			Deleted:            baseProps.DeletedProperties(),
			changeType:         finalType,
			Properties:         baseProps,
		}

		out = append(out, mergedChange)
	}

	return out, nil
}

type loop struct {
	State         *stateManager
	ReaderC       <-chan Change
	WriterC       chan<- Change
	DB            db
	Cache         changeCache
	FlushInterval time.Duration
	NodeBuffer    []*NodeChange
	BatchSize     int
}

func newLoop(ctx context.Context, state *stateManager, db db, cache changeCache, batchSize int) loop {
	writerC, readerC := channels.BufferedPipe[Change](ctx)

	return loop{
		State:         state,
		ReaderC:       readerC,
		WriterC:       writerC,
		DB:            db,
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
					if err := s.DB.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
						slog.Warn(err.Error())
					}
					s.NodeBuffer = s.NodeBuffer[:0]
				}
			}

		case <-ticker.C:
			lastNodeWatermark = s.tryFlush(ctx, lastNodeWatermark)
		}
	}
}

// tryFlush checks if the node buffer has remained the same size over two ticks.
// This implies inactivity—no new changes—and triggers a flush of any remaining changes.
// The function returns the new watermark for tracking buffer growth across ticks.
func (s *loop) tryFlush(ctx context.Context, lastNodeWatermark int) int {
	numNodeChanges := len(s.NodeBuffer)

	hasNodeChangesToFlush := numNodeChanges > 0 && numNodeChanges == lastNodeWatermark

	if !hasNodeChangesToFlush { // &&!hasEdgeChangesToFlush
		// No eligible flush needed
		lastNodeWatermark = numNodeChanges
		return lastNodeWatermark
	}

	if hasNodeChangesToFlush {
		if err := s.DB.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
			slog.Warn(err.Error())
		}
		s.NodeBuffer = s.NodeBuffer[:0]
	}

	lastNodeWatermark = len(s.NodeBuffer)

	return lastNodeWatermark
}

// todo: ChangeLog is the public export for this package to be consumed by bloodhound.
type Changelog struct {
	Cache changeCache
	DB    db
	Loop  loop
}

func NewChangelogDaemon(ctx context.Context, flags GetFlagByKeyer, pgxPool *pgxpool.Pool, kindMapper pg.KindMapper, batchSize int) *Changelog {
	cache := newChangeCache()
	db, err := newLogDB(ctx, pgxPool, kindMapper)
	state := newStateManager(flags)
	loop := newLoop(ctx, state, db, cache, batchSize)

	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("failed initializing log DB: %v", err))
	}

	// todo: probably rip this out
	// prime the feature flag upon initalization
	// because state is not a pointer value this doesn't actually update state
	// so i changed it to a pointer
	state.CheckFeatureFlag(ctx)

	go loop.start(ctx)

	return &Changelog{
		Cache: cache,
		DB:    db,
		Loop:  loop,
	}
}

// ResolveNodeChange decorated proposedChange with diff details by comparing it to the last seen record in the DB.
// it mutates proposedChange to set:
// - changeType: Added, Modified, or NoChange
// - ModifiedProperties: key-value pairs that are new or changed
// - Deleted: list of removed property keys
// Kinds are treated as upsert-always, this function does not diff them kinds.
func (s *Changelog) ResolveNodeChange(ctx context.Context, proposedChange *NodeChange) error {
	if handled, err := s.Cache.checkCache(proposedChange); err != nil {
		return fmt.Errorf("check cache: %w", err)
	} else if handled {
		return nil
	}

	// DB Fallback: load the last stored state from the changelog
	last, err := s.DB.fetchLastNodeState(ctx, proposedChange.NodeID)
	if err != nil {
		return err
	}

	// no prior record -> Add
	if !last.Exists {
		proposedChange.changeType = ChangeTypeAdded
		// Properties: everything is "new"
		proposedChange.ModifiedProperties = proposedChange.Properties.MapOrEmpty()
		proposedChange.Deleted = nil
		// todo: i think we need to store a hash on the proposedChange for caching
		_, _ = proposedChange.Hash()
		return nil
	}

	// we have a prior row: compute proposed combined hash once.
	proposedHash, err := proposedChange.Hash()
	if err != nil {
		return fmt.Errorf("hash proposed change: %w", err)
	}

	// if hashes match (props+kinds), it's a no-op.
	if bytes.Equal(proposedHash, last.Hash) {
		proposedChange.changeType = ChangeTypeNoChange
		return nil
	}

	// modified, compute property diffs
	proposedChange.changeType = ChangeTypeModified

	// property diff
	oldProps := last.Properties
	newProps := proposedChange.Properties.MapOrEmpty()
	modifiedProps, deletedProps := diffProps(oldProps, newProps)

	proposedChange.ModifiedProperties = modifiedProps
	proposedChange.Deleted = deletedProps

	// update cache with latest snapshot
	s.Cache.put(proposedChange.IdentityKey(), proposedChange)
	return nil
}

// TODO: this does not treat int(1) === float64(1) so thats probs an issue
// it needs to normalize slices (probably by sorting them) and it needs to normalize number-ish values so that int64(5) == float64(5), for example
// diffProps returns modified key→value pairs and a list of deleted keys.
func diffProps(oldProps, newProps map[string]any) (map[string]any, []string) {
	var (
		modified = map[string]any{}
		deleted  = []string{}
	)

	for k, v := range newProps {
		if oldVal, ok := oldProps[k]; !ok || !reflect.DeepEqual(v, oldVal) {
			modified[k] = v
		}
	}

	for k := range oldProps {
		if _, ok := newProps[k]; !ok {
			deleted = append(deleted, k)
		}
	}

	sort.Strings(deleted)

	return modified, deleted
}

func (s *Changelog) Submit(ctx context.Context, change Change) bool {
	return channels.Submit(ctx, s.Loop.WriterC, change)
}

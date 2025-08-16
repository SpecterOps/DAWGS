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
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util"
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

type db struct {
	Conn       DBConn
	kindMapper pg.KindMapper
}

// DBConn contains the methods we actually use from pgxpool. putting these here makes testing easier
type DBConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
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
		Conn:       pgxPool,
		kindMapper: kindMapper,
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

	err := s.Conn.QueryRow(ctx, LAST_NODE_CHANGE_SQL_2, nodeID).Scan(&storedModifiedProps, &storedHash)
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

func (s *db) flushNodeChanges(ctx context.Context, changes []*NodeChange) (int64, error) {
	// Early exit check for empty buffer flushes
	if len(changes) == 0 {
		return 0, nil
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
				c.Deleted, // i think encoding janked it up, leave as plain []string
				hash,
				c.Type(),
				now,
			}

			return rows, nil
		}
	}

	if _, err := s.Conn.CopyFrom(ctx, pgx.Identifier{"node_change_stream"}, copyColumns, pgx.CopyFromSlice(numChanges, iterator)); err != nil {
		return 0, fmt.Errorf("flushing nodes: %w", err)
	} else if latestID, err := s.latestNodeChangeID(ctx); err != nil {
		return 0, fmt.Errorf("reading latest nodeid: %w", err)
	} else {
		return latestID, nil
	}
}

func (s *db) latestNodeChangeID(ctx context.Context) (int64, error) {
	return s.latestChangeID(ctx, LATEST_NODE_CHANGE_SQL)
}

func (s *db) latestChangeID(ctx context.Context, query string) (int64, error) {
	var (
		lastChangeID  int64
		lastChangeRow = s.Conn.QueryRow(ctx, query)
		err           = lastChangeRow.Scan(&lastChangeID)
	)

	if err != nil {
		return -1, err
	}

	return lastChangeID, nil
}

// todo: add this to flushNodeChanges as an optimization. it will compact intra-buffer changes to the same node
// into a single change prior to flush
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

// todo: ChangeLog is the public export for this package to be consumed by bloodhound.
type Changelog struct {
	Cache changeCache
	DB    db
	Loop  loop
}

func NewChangelogDaemon(ctx context.Context, pgxPool *pgxpool.Pool, kindMapper pg.KindMapper, batchSize int, notificationC chan<- Notification) (*Changelog, error) {
	cache := newChangeCache()
	db, err := newLogDB(ctx, pgxPool, kindMapper)
	// state := newStateManager(flags)
	loop := newLoop(ctx, &db, notificationC, batchSize)

	if err != nil {
		return &Changelog{}, fmt.Errorf("initializing log DB: %w", err)
	}

	// todo: probably rip this out
	// state.CheckFeatureFlag(ctx)

	go loop.start(ctx)

	return &Changelog{
		Cache: cache,
		DB:    db,
		Loop:  loop,
	}, nil
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
		s.Cache.put(proposedChange.IdentityKey(), proposedChange)
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

func (s *Changelog) ReplayNodeChanges(ctx context.Context, sinceID int64, visitor func(change NodeChange)) error {
	if nodeChangesResult, err := s.DB.Conn.Query(ctx, SELECT_NODE_CHANGE_RANGE_SQL, sinceID); err != nil {
		return err
	} else {
		defer nodeChangesResult.Close()

		for nodeChangesResult.Next() {
			var (
				nodeID             string
				kindIDs            []int16
				deletedProperties  []string
				changeType         ChangeType
				modifiedProperties = map[string]any{}
			)

			if err := nodeChangesResult.Scan(&changeType, &nodeID, &kindIDs, &modifiedProperties, &deletedProperties); err != nil {
				return err
			}

			modifiedPropertyKeyIndex := make(map[string]struct{}, len(modifiedProperties))

			for key := range modifiedProperties {
				modifiedPropertyKeyIndex[key] = struct{}{}
			}

			deletedPropertyKeys := make(map[string]struct{}, len(deletedProperties))

			for _, key := range deletedProperties {
				deletedPropertyKeys[key] = struct{}{}
			}

			if mappedKinds, err := s.DB.kindMapper.MapKindIDs(ctx, kindIDs); err != nil {
				return err
			} else {
				visitor(NodeChange{
					changeType: changeType,
					NodeID:     nodeID,
					Kinds:      mappedKinds,
					Properties: &graph.Properties{
						Map:      modifiedProperties,
						Deleted:  deletedPropertyKeys,
						Modified: modifiedPropertyKeyIndex,
					},
					// todo: there is some drift with how these standalone properties are used vs. the nested fields in the Properties obj
					ModifiedProperties: modifiedProperties,
					Deleted:            deletedProperties,
				})
			}
		}

		return nodeChangesResult.Err()
	}
}

// ApplyNodeChanges replays node changes with id > sinceID and applies them to the graph
// in a single database transaction.
//
//   - set "objectid" to change.NodeID in the properties (as in your main).
//   - all writes happen in one tx; any row failure causes a rollback and error.
func (s *Changelog) ApplyNodeChanges(ctx context.Context, graphID int64, sinceID int64) error {
	errs := util.NewErrorCollector()
	// Start one transaction for the whole batch we’re about to apply
	tx, err := s.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// If anything fails, roll back
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// replay rows > sinceID and apply them row-by-row
	replayErr := s.ReplayNodeChanges(ctx, sinceID, func(change NodeChange) {
		// ensure objectid mirrors
		if change.ModifiedProperties == nil {
			change.ModifiedProperties = make(map[string]any, 1)
		}
		change.ModifiedProperties["objectid"] = change.NodeID

		kindIDs, err := s.DB.kindMapper.MapKinds(ctx, change.Kinds)
		if err != nil {
			errs.Add(fmt.Errorf("map kinds: %w", err))
		}

		propsJSON, err := json.Marshal(change.ModifiedProperties)
		if err != nil {
			errs.Add(fmt.Errorf("marshal properties: %w", err))
		}

		// apply to graph
		switch change.Type() {
		case ChangeTypeAdded:
			// INSERT
			if _, err := tx.Exec(ctx, InsertNodeFromChangeSQL, graphID, kindIDs, propsJSON); err != nil {
				errs.Add(fmt.Errorf("insert node %s: %w", change.NodeID, err))
			}

		case ChangeTypeModified:
			// UPDATE with upserted props + deletions
			if _, err := tx.Exec(ctx, UpdateNodeFromChangeSQL, change.NodeID, kindIDs, propsJSON, change.Deleted); err != nil {
				errs.Add(fmt.Errorf("update node %s: %w", change.NodeID, err))
			}

		case ChangeTypeRemoved:
			errs.Add(fmt.Errorf("delete not implemented for node %s", change.NodeID))
		}
	})

	// handle visitor errors and replay errors
	visitorErrors := errs.Combined()
	if replayErr != nil {
		return replayErr
	} else if visitorErrors != nil {
		return fmt.Errorf("applying node changes: %w", visitorErrors)
	}

	// if we made it here, commit all changes
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// hook for bloodhound datapipe daemon
// todo: the  caller should maintain `sinceID` across runs (e.g., in-memory now, maybe need a checkpoint table).
func RunNodeApplierLoop(
	ctx context.Context,
	notifications <-chan Notification,
	changelog *Changelog,
	graphID int64,
	sinceID int64,
) error {

	startID := sinceID

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case n, ok := <-notifications:
			if !ok {
				return nil // channel closed, exit
			}

			if n.Type != NotificationNode {
				continue
			}

			slog.Info("applying node changes",
				slog.Int64("from_revison", startID),
				slog.Int64("to_revison", n.RevisionID),
			)

			if err := changelog.ApplyNodeChanges(ctx, graphID, startID); err != nil {
				return fmt.Errorf("apply node changes (from %d to %d): %w", startID, n.RevisionID, err)
			}

			// next tick will start from here
			startID = n.RevisionID
		}
	}
}

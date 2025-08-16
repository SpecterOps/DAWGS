package changestream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sort"

	pgx "github.com/jackc/pgx/v5"
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

type Changelog struct {
	Cache changeCache
	DB    db
	Loop  loop
}

func NewChangelog(ctx context.Context, pgxPool *pgxpool.Pool, kindMapper pg.KindMapper, batchSize int, notificationC chan<- Notification) (*Changelog, error) {
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
	newProps := proposedChange.Properties
	proposedChange.Properties = DiffProps(oldProps, newProps)

	// update cache with latest snapshot
	s.Cache.put(proposedChange.IdentityKey(), proposedChange)
	return nil
}

// DiffProps hooks into graph.Properties api instead of diffProps which is jank
// todo: does this need to normalize slices so that out-of-order slices don't count as a change. consider sorting
func DiffProps(oldProps, newProps *graph.Properties) *graph.Properties {
	var (
		modified = map[string]any{}
		deleted  []string
	)

	// Collect modified values
	for k, v := range newProps.Map {
		if oldVal, ok := oldProps.Map[k]; !ok || !reflect.DeepEqual(v, oldVal) {
			modified[k] = v
		}
	}

	// Collect deleted values
	for k := range oldProps.Map {
		if _, ok := newProps.Map[k]; !ok {
			deleted = append(deleted, k)
		}
	}

	sort.Strings(deleted)

	// Build the Modified/Deleted key sets
	modifiedKeys := make(map[string]struct{}, len(modified))
	for k := range modified {
		modifiedKeys[k] = struct{}{}
	}

	deletedKeys := make(map[string]struct{}, len(deleted))
	for _, k := range deleted {
		deletedKeys[k] = struct{}{}
	}

	return &graph.Properties{
		Map:      modified,
		Deleted:  deletedKeys,
		Modified: modifiedKeys,
	}
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
		if change.Properties == nil {
			change.Properties = graph.NewProperties()
		}
		change.Properties.Set("objectid", change.NodeID)

		kindIDs, err := s.DB.kindMapper.MapKinds(ctx, change.Kinds)
		if err != nil {
			errs.Add(fmt.Errorf("map kinds: %w", err))
		}

		propsJSON, err := json.Marshal(change.Properties.MapOrEmpty())
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
			if _, err := tx.Exec(ctx, UpdateNodeFromChangeSQL, change.NodeID, kindIDs, propsJSON, change.Properties.DeletedProperties()); err != nil {
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

			// probably dont want to bubble this error. in order to keep loop healthy just log it.
			if err := changelog.ApplyNodeChanges(ctx, graphID, startID); err != nil {
				slog.Warn(fmt.Sprintf("apply node changes (from %d to %d): %v", startID, n.RevisionID, err))
			}

			// next tick will start from here
			startID = n.RevisionID
		}
	}
}

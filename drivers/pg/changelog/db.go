package changelog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
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
	Properties *graph.Properties
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

	rawProps := map[string]any{}
	if len(storedModifiedProps) > 0 {
		if uerr := json.Unmarshal(storedModifiedProps, &rawProps); uerr != nil {
			return lastNodeState{}, fmt.Errorf("unmarshal stored properties: %w", uerr)
		}
	}

	// Construct graph.Properties
	modified := make(map[string]struct{}, len(rawProps))
	for k := range rawProps {
		modified[k] = struct{}{}
	}

	properties := &graph.Properties{
		Map:      rawProps,
		Modified: modified,
		Deleted:  map[string]struct{}{}, // we aren't really considering this for previous state. is that okay?
	}

	return lastNodeState{
		Exists:     true,
		Hash:       storedHash,
		Properties: properties,
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
		} else if modifiedProps, err := json.Marshal(c.Properties.MapOrEmpty()); err != nil {
			return nil, fmt.Errorf("failed creating node change property JSON: %w", err)
		} else {
			rows := []any{
				c.NodeID,
				mappedKindIDs,
				modifiedProps,
				c.Properties.DeletedProperties(),
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

// this is an optimization
// todo: add this to flushNodeChanges as an optimization. it will compact intra-buffer changes to the same node
// into a single change prior to flush
// func mergeNodeChanges(changes []*NodeChange) ([]*NodeChange, error) {
// 	if len(changes) == 0 {
// 		return nil, nil
// 	}

// 	// Group by node_id; keep order of first appearance
// 	type group struct{ idxs []int }
// 	groups := make(map[string]*group, len(changes))
// 	order := make([]string, 0, len(changes)) // todo; is this necessary?

// 	for idx, change := range changes {
// 		g, ok := groups[change.NodeID]
// 		if !ok {
// 			g = &group{}
// 			groups[change.NodeID] = g
// 			order = append(order, change.NodeID)
// 		}
// 		g.idxs = append(g.idxs, idx)
// 	}

// 	// out will hold the result of our merging routine
// 	out := make([]*NodeChange, 0, len(groups))

// 	for _, nodeID := range order {
// 		idxs := groups[nodeID].idxs
// 		baseline := changes[idxs[0]]

// 		// baseline FULL properties and kinds
// 		baseProps := baseline.Properties.Clone()
// 		baseKinds := baseline.Kinds.Copy()

// 		for _, idx := range idxs[1:] {
// 			change := changes[idx]

// 			// apply modified
// 			for k, v := range change.ModifiedProperties {
// 				baseProps.Set(k, v)
// 			}

// 			// apply deletions
// 			for _, k := range change.Deleted {
// 				// delete from modified if possible, otherwise append to deletedlist
// 				if _, ok := baseProps.Modified[k]; ok {
// 					delete(baseProps.Modified, k)
// 				} else {
// 					baseProps.Deleted[k] = struct{}{}
// 				}
// 			}

// 			// union kinds. i think this handler removes dupes
// 			baseKinds = baseKinds.Add(change.Kinds...)
// 		}

// 		// this is how we handle it in the resolver, but perhaps graph.properties API gives us some niceties...
// 		// modified, deleted := diffProps(baseProps)

// 		finalType := baseline.Type()
// 		switch {
// 		case finalType == ChangeTypeAdded:
// 			// keep. this is a brand-new node this flush
// 		case len(baseProps.ModifiedProperties()) == 0 || len(baseProps.Deleted) == 0:
// 			finalType = ChangeTypeNoChange
// 		default:
// 			finalType = ChangeTypeModified
// 		}

// 		mergedChange := &NodeChange{
// 			NodeID:             nodeID,
// 			Kinds:              baseKinds,
// 			ModifiedProperties: baseProps.ModifiedProperties(),
// 			Deleted:            baseProps.DeletedProperties(),
// 			changeType:         finalType,
// 			Properties:         baseProps,
// 		}

// 		out = append(out, mergedChange)
// 	}

// 	return out, nil
// }

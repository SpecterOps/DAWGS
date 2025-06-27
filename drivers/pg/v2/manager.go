package v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	v2 "github.com/specterops/dawgs/v2"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg/v2/model"
	"github.com/specterops/dawgs/drivers/pg/v2/query"
	"github.com/specterops/dawgs/graph"
)

type KindMapper interface {
	MapKindID(ctx context.Context, kindID int16) (graph.Kind, error)
	MapKindIDs(ctx context.Context, kindIDs []int16) (graph.Kinds, error)
	MapKind(ctx context.Context, kind graph.Kind) (int16, error)
	MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error)
	AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error)
}

func KindMapperFromGraphDatabase(graphDB v2.Database) (KindMapper, error) {
	switch typedGraphDB := graphDB.(type) {
	case *database:
		return typedGraphDB.schemaManager, nil
	default:
		return nil, fmt.Errorf("unsupported graph database type: %T", typedGraphDB)
	}
}

type SchemaManager struct {
	defaultGraph    v2.Graph
	pool            *pgxpool.Pool
	hasDefaultGraph bool
	graphs          map[string]model.Graph
	kindsByID       map[graph.Kind]int16
	kindIDsByKind   map[int16]graph.Kind
	lock            *sync.RWMutex
}

func NewSchemaManager(pool *pgxpool.Pool) *SchemaManager {
	return &SchemaManager{
		pool:            pool,
		hasDefaultGraph: false,
		graphs:          map[string]model.Graph{},
		kindsByID:       map[graph.Kind]int16{},
		kindIDsByKind:   map[int16]graph.Kind{},
		lock:            &sync.RWMutex{},
	}
}

func (s *SchemaManager) transaction(ctx context.Context, transactionLogic func(transaction pgx.Tx) error) error {
	if acquiredConn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer acquiredConn.Release()

		if transaction, err := acquiredConn.BeginTx(ctx, pgx.TxOptions{
			AccessMode: pgx.ReadWrite,
		}); err != nil {
			return err
		} else {
			defer func() {
				if err := transaction.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
					slog.DebugContext(ctx, "failed to rollback transaction", slog.String("err", err.Error()))
				}
			}()

			if err := transactionLogic(transaction); err != nil {
				return err
			}

			return transaction.Commit(ctx)
		}
	}
}

func (s *SchemaManager) fetch(ctx context.Context, tx pgx.Tx) error {
	if kinds, err := query.On(tx).SelectKinds(ctx); err != nil {
		return err
	} else {
		s.kindsByID = kinds

		for kind, kindID := range s.kindsByID {
			s.kindIDsByKind[kindID] = kind
		}
	}

	return nil
}

func (s *SchemaManager) GetKindIDsByKind() map[int16]graph.Kind {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.kindIDsByKind
}

func (s *SchemaManager) Fetch(ctx context.Context) error {
	return s.transaction(ctx, func(transaction pgx.Tx) error {
		return s.fetch(ctx, transaction)
	})
}

func (s *SchemaManager) defineKinds(ctx context.Context, tx pgx.Tx, kinds graph.Kinds) error {
	for _, kind := range kinds {
		if kindID, err := query.On(tx).InsertOrGetKind(ctx, kind); err != nil {
			return err
		} else {
			s.kindsByID[kind] = kindID
			s.kindIDsByKind[kindID] = kind
		}
	}

	return nil
}

func (s *SchemaManager) mapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	var (
		missingKinds = make(graph.Kinds, 0, len(kinds))
		ids          = make([]int16, 0, len(kinds))
	)

	for _, kind := range kinds {
		if id, hasID := s.kindsByID[kind]; hasID {
			ids = append(ids, id)
		} else {
			missingKinds = append(missingKinds, kind)
		}
	}

	return ids, missingKinds
}

func (s *SchemaManager) MapKind(ctx context.Context, kind graph.Kind) (int16, error) {
	s.lock.RLock()

	if id, hasID := s.kindsByID[kind]; hasID {
		s.lock.RUnlock()
		return id, nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return -1, err
	}

	if id, hasID := s.kindsByID[kind]; hasID {
		return id, nil
	} else {
		return -1, fmt.Errorf("unable to map kind: %s", kind.String())
	}
}

func (s *SchemaManager) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	s.lock.RLock()

	if mappedKinds, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		s.lock.RUnlock()
		return mappedKinds, nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return nil, err
	}

	if mappedKinds, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		return mappedKinds, nil
	} else {
		return nil, fmt.Errorf("unable to map kinds: %s", strings.Join(missingKinds.Strings(), ", "))
	}
}

func (s *SchemaManager) mapKindIDs(kindIDs []int16) (graph.Kinds, []int16) {
	var (
		missingIDs = make([]int16, 0, len(kindIDs))
		kinds      = make(graph.Kinds, 0, len(kindIDs))
	)

	for _, kindID := range kindIDs {
		if kind, hasKind := s.kindIDsByKind[kindID]; hasKind {
			kinds = append(kinds, kind)
		} else {
			missingIDs = append(missingIDs, kindID)
		}
	}

	return kinds, missingIDs
}

func (s *SchemaManager) MapKindID(ctx context.Context, kindID int16) (graph.Kind, error) {
	if kindIDs, err := s.MapKindIDs(ctx, []int16{kindID}); err != nil {
		return nil, err
	} else {
		return kindIDs[0], nil
	}
}

func (s *SchemaManager) MapKindIDs(ctx context.Context, kindIDs []int16) (graph.Kinds, error) {
	s.lock.RLock()

	if kinds, missingKinds := s.mapKindIDs(kindIDs); len(missingKinds) == 0 {
		s.lock.RUnlock()
		return kinds, nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return nil, err
	}

	if kinds, missingKinds := s.mapKindIDs(kindIDs); len(missingKinds) == 0 {
		return kinds, nil
	} else {
		return nil, fmt.Errorf("unable to map kind ids: %v", missingKinds)
	}
}

func (s *SchemaManager) assertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	// Acquire a write-lock and release on-exit
	s.lock.Lock()
	defer s.lock.Unlock()

	// We have to re-acquire the missing kinds since there's a potential for another writer to acquire the write-lock
	// in between release of the read-lock and acquisition of the write-lock for this operation
	if _, missingKinds := s.mapKinds(kinds); len(missingKinds) > 0 {
		if err := s.transaction(ctx, func(transaction pgx.Tx) error {
			// Previously calls like this required - pgx.QueryExecModeSimpleProtocol while that seems to no longer be
			// the case, this comment has been left here in case the issue reappears
			return s.defineKinds(ctx, transaction, missingKinds)
		}); err != nil {
			return nil, err
		}
	}

	// Lookup the kinds again from memory as they should now be up to date
	kindIDs, _ := s.mapKinds(kinds)
	return kindIDs, nil
}

func (s *SchemaManager) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	// Acquire a read-lock first to fast-pass validate if we're missing any kind definitions
	s.lock.RLock()

	if kindIDs, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		// All kinds are defined. Release the read-lock here before returning
		s.lock.RUnlock()
		return kindIDs, nil
	}

	// Release the read-lock here so that we can acquire a write-lock
	s.lock.RUnlock()
	return s.assertKinds(ctx, kinds)
}

func (s *SchemaManager) DefaultGraph() (v2.Graph, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.defaultGraph, s.hasDefaultGraph
}

func (s *SchemaManager) assertGraph(ctx context.Context, schema v2.Graph) (model.Graph, error) {
	var assertedGraph model.Graph

	if err := s.transaction(ctx, func(transaction pgx.Tx) error {
		queries := query.On(transaction)

		// Validate the schema if the graph already exists in the database
		if definition, err := queries.SelectGraphByName(ctx, schema.Name); err != nil {
			// ErrNoRows is ignored as it signifies that this graph must be created
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}

			if newDefinition, err := queries.CreateGraph(ctx, schema); err != nil {
				return err
			} else {
				assertedGraph = newDefinition
			}
		} else if assertedDefinition, err := queries.AssertGraph(ctx, schema, definition); err != nil {
			return err
		} else {
			// Graph exists and may have been updated
			assertedGraph = assertedDefinition
		}

		return nil
	}); err != nil {
		return model.Graph{}, err
	}

	// Cache the graph definition and return it
	s.graphs[schema.Name] = assertedGraph
	return assertedGraph, nil
}

func (s *SchemaManager) assertSchema(ctx context.Context, schema v2.Schema) error {
	if defaultGraph, hasDefaultGraph := schema.DefaultGraph(); !hasDefaultGraph {
		return fmt.Errorf("no default graph specified in schema")
	} else {
		s.defaultGraph = defaultGraph
		s.hasDefaultGraph = true
	}

	return s.transaction(ctx, func(transaction pgx.Tx) error {
		if err := query.On(transaction).CreateSchema(ctx); err != nil {
			return err
		}

		if err := s.fetch(ctx, transaction); err != nil {
			return err
		}

		for _, graphSchema := range schema.Graphs {
			if _, missingKinds := s.mapKinds(graphSchema.Nodes); len(missingKinds) > 0 {
				if err := s.defineKinds(ctx, transaction, missingKinds); err != nil {
					return err
				}
			}

			if _, missingKinds := s.mapKinds(graphSchema.Edges); len(missingKinds) > 0 {
				if err := s.defineKinds(ctx, transaction, missingKinds); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (s *SchemaManager) AssertGraph(ctx context.Context, schema v2.Graph) (model.Graph, error) {
	// Acquire a read-lock first to fast-pass validate if we're missing the graph definitions
	s.lock.RLock()

	if graphInstance, isDefined := s.graphs[schema.Name]; isDefined {
		// The graph is defined. Release the read-lock here before returning
		s.lock.RUnlock()
		return graphInstance, nil
	}

	// Release the read-lock here so that we can acquire a write-lock next
	s.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()

	if graphInstance, isDefined := s.graphs[schema.Name]; isDefined {
		// The graph was defined by a different actor between the read unlock and the write lock, return it
		return graphInstance, nil
	}

	return s.assertGraph(ctx, schema)
}

func (s *SchemaManager) AssertSchema(ctx context.Context, schema v2.Schema) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Previously calls like this required - pgx.QueryExecModeSimpleProtocol while that seems to no longer be
	// the case, this comment has been left here in case the issue reappears
	return s.assertSchema(ctx, schema)
}

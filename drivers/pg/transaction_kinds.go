package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
)

// transactionKindMapper owns metadata visible to one connection. Uncommitted
// kind IDs never enter the shared cache, and misses never lease another connection.
type transactionKindMapper struct {
	tx                  *transaction
	kinds               map[graph.Kind]int16
	ids                 map[int16]graph.Kind
	hasUncommittedKinds bool
}

// metadataTransaction keeps catalog-only queries from invalidating graph
// topology observations on the owning transaction.
type metadataTransaction struct{ *transaction }

func (s metadataTransaction) Raw(statement string, parameters map[string]any) graph.Result {
	return s.transaction.raw(statement, parameters)
}

func (s *transactionKindMapper) refresh() error {
	kinds, err := query.On(metadataTransaction{s.tx}).SelectKinds()
	if err != nil {
		return err
	}
	s.kinds = kinds
	s.ids = make(map[int16]graph.Kind, len(kinds))
	for kind, id := range kinds {
		s.ids[id] = kind
	}
	return nil
}

func (s *transactionKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	if s.kinds == nil {
		if err := s.refresh(); err != nil {
			return nil, err
		}
	}
	ids, missing := s.mapKinds(kinds)
	if len(missing) > 0 {
		if err := s.refresh(); err != nil {
			return nil, err
		}
		ids, missing = s.mapKinds(kinds)
	}
	if len(missing) > 0 {
		return nil, &pgsql.MissingKindsError{Kinds: missing, KindIDs: ids}
	}
	return ids, nil
}

func (s *transactionKindMapper) mapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	ids := make([]int16, 0, len(kinds))
	var missing graph.Kinds
	for _, kind := range kinds {
		if id, ok := s.kinds[kind]; ok {
			ids = append(ids, id)
		} else {
			missing = append(missing, kind)
		}
	}
	return ids, missing
}

func (s *transactionKindMapper) MapKind(ctx context.Context, kind graph.Kind) (int16, error) {
	ids, err := s.MapKinds(ctx, graph.Kinds{kind})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

func (s *transactionKindMapper) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	if s.kinds == nil {
		if err := s.refresh(); err != nil {
			return nil, err
		}
	}
	registered := false
	for _, kind := range kinds {
		if _, ok := s.kinds[kind]; ok {
			continue
		}
		id, err := query.On(metadataTransaction{s.tx}).InsertOrGetKind(kind)
		// A concurrent insertion can win after this statement's snapshot. A new
		// statement observes the committed winner at READ COMMITTED; stronger
		// isolation returns a serialization error instead of publishing a stale ID.
		if errors.Is(err, pgx.ErrNoRows) {
			id, err = query.On(metadataTransaction{s.tx}).InsertOrGetKind(kind)
		}
		if err != nil {
			return nil, err
		}
		if s.tx.tx != nil {
			s.hasUncommittedKinds = true
		}
		registered = true
		s.kinds[kind] = id
		s.ids[id] = kind
	}
	if registered && s.tx.tx == nil {
		s.publish()
	}
	ids, _ := s.mapKinds(kinds)
	return ids, nil
}

func (s *transactionKindMapper) MapKindIDs(ctx context.Context, ids []int16) (graph.Kinds, error) {
	if s.ids == nil {
		if err := s.refresh(); err != nil {
			return nil, err
		}
	}
	kinds := make(graph.Kinds, len(ids))
	for i, id := range ids {
		kind, ok := s.ids[id]
		if !ok {
			return nil, fmt.Errorf("unable to map kind id: %d", id)
		}
		kinds[i] = kind
	}
	return kinds, nil
}

func (s *transactionKindMapper) MapKindID(ctx context.Context, id int16) (graph.Kind, error) {
	kinds, err := s.MapKindIDs(ctx, []int16{id})
	if err != nil {
		return nil, err
	}
	return kinds[0], nil
}

// publish merges only metadata that has committed on this connection.
func (s *transactionKindMapper) publish() {
	if s == nil || s.tx.schemaManager == nil {
		return
	}
	manager := s.tx.schemaManager
	manager.lock.Lock()
	defer manager.lock.Unlock()
	for kind, id := range s.kinds {
		manager.kindsByID[kind] = id
		manager.kindIDsByKind[id] = kind
	}
	s.hasUncommittedKinds = false
}

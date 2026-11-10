package pg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
)

var emptyKindSnapshot = &kindSnapshot{}

// transactionKindMapper retains a shared immutable snapshot and allocates small
// private overlays only when it learns metadata. Registrations remain private
// until the actual owning transaction commits.
type transactionKindMapper struct {
	tx                  *transaction
	snapshot            *kindSnapshot
	kinds               map[graph.Kind]int16
	ids                 map[int16]graph.Kind
	registeredKinds     map[graph.Kind]int16
	registeredIDs       map[int16]graph.Kind
	registeredEpoch     uint64
	hasUncommittedKinds bool
	// privateCatalog is an authoritative view from the owning transaction,
	// used by schema assertion and stable-snapshot catalog reconciliation.
	privateCatalog bool
}

// metadataTransaction bypasses automatic refresh and topology invalidation.
type metadataTransaction struct {
	// transaction provides the delegated transaction operations.
	*transaction
}

func (s metadataTransaction) Raw(statement string, parameters map[string]any) graph.Result {
	return s.transaction.raw(statement, parameters)
}

func (s *transactionKindMapper) base() *kindSnapshot {
	if s.snapshot == nil {
		s.snapshot = emptyKindSnapshot
		if s.tx != nil && s.tx.schemaManager != nil {
			s.snapshot = s.tx.schemaManager.kindCache.current.Load()
		}
	}
	return s.snapshot
}

func (s *transactionKindMapper) latest() *kindSnapshot {
	base := s.base()
	if s.privateCatalog {
		return base
	}
	if s.tx != nil && s.tx.schemaManager != nil {
		latest := s.tx.schemaManager.kindCache.current.Load()
		if latest.epoch != base.epoch && s.tx.tx != nil && stableSnapshotIsolation(s.tx.isolation) {
			return base
		}
		return latest
	}
	return base
}

// adopt is called only at a query boundary. Stable transactions reconcile a
// changed epoch against their own database snapshot; READ COMMITTED transactions
// discard incompatible lookups while revalidating their own registrations.
func (s *transactionKindMapper) adopt() error {
	if s == nil || s.tx == nil || s.tx.schemaManager == nil || s.privateCatalog {
		return nil
	}
	latest := s.tx.schemaManager.kindCache.current.Load()
	if latest.epoch != s.base().epoch && s.tx.tx != nil && stableSnapshotIsolation(s.tx.isolation) {
		// BEGIN does not establish PostgreSQL's snapshot. Read the catalog
		// through this transaction to match whichever snapshot its first data
		// statement acquired, including when this read is that first statement.
		started := time.Now()
		kinds, err := query.On(metadataTransaction{s.tx}).SelectKinds()
		s.tx.recordKindWork("kind_refresh", started)
		if err != nil {
			return err
		}
		ids := make(map[int16]graph.Kind, len(kinds))
		for kind, id := range kinds {
			ids[id] = kind
		}
		// Keep the original publication fence: this private view must never
		// resurrect mappings removed from shared metadata by the refresh.
		s.snapshot = &kindSnapshot{kinds: kinds, ids: ids, epoch: s.base().epoch, loaded: true}
		s.kinds, s.ids = nil, nil
		s.privateCatalog = true
		return nil
	}
	if latest.epoch != s.base().epoch {
		if len(s.registeredKinds) > 0 {
			// InsertOrGetKind can return an already committed mapping. Check
			// which registrations are still visible rather than assuming they
			// are all protected by this transaction's uncommitted insertions.
			names := make([]string, 0, len(s.registeredKinds))
			for kind := range s.registeredKinds {
				names = append(names, kind.String())
			}
			started := time.Now()
			kinds, err := query.On(metadataTransaction{s.tx}).SelectKindsByNames(names)
			s.tx.recordKindWork("kind_lookup", started)
			if err != nil {
				return err
			}
			s.registeredKinds, s.registeredIDs = nil, nil
			if len(kinds) > 0 {
				s.registeredKinds = kinds
				s.registeredIDs = make(map[int16]graph.Kind, len(kinds))
				for kind, id := range kinds {
					s.registeredIDs[id] = kind
				}
			} else {
				s.hasUncommittedKinds = false
			}
		}
		s.kinds, s.ids = nil, nil
	}
	s.snapshot = latest
	return nil
}

func (s *transactionKindMapper) kindID(kind graph.Kind) (int16, bool) {
	if id, ok := s.registeredKinds[kind]; ok {
		return id, true
	}
	if id, ok := s.kinds[kind]; ok {
		return id, true
	}
	if id, ok := s.base().kinds[kind]; ok {
		return id, true
	}
	id, ok := s.latest().kinds[kind]
	return id, ok
}

func (s *transactionKindMapper) learn(kind graph.Kind, id int16) {
	s.base()
	if s.kinds == nil {
		s.kinds = make(map[graph.Kind]int16)
		s.ids = make(map[int16]graph.Kind)
	}
	s.kinds[kind], s.ids[id] = id, kind
}

func (s *transactionKindMapper) register(kind graph.Kind, id int16) {
	s.base()
	if s.registeredKinds == nil {
		s.registeredKinds = make(map[graph.Kind]int16)
		s.registeredIDs = make(map[int16]graph.Kind)
		s.registeredEpoch = s.base().epoch
	}
	s.registeredKinds[kind], s.registeredIDs[id] = id, kind
}

func (s *transactionKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	if len(kinds) == 0 {
		return []int16{}, nil
	}
	ids, missing := s.mapKinds(kinds)
	if len(missing) == 0 {
		return ids, nil
	}
	names := make([]string, 0, len(missing))
	seen := make(map[graph.Kind]struct{}, len(missing))
	for _, kind := range missing {
		if _, ok := seen[kind]; !ok {
			names = append(names, kind.String())
			seen[kind] = struct{}{}
		}
	}
	started := time.Now()
	found, err := query.On(metadataTransaction{s.tx}).SelectKindsByNames(names)
	s.tx.recordKindWork("kind_lookup", started)
	if err != nil {
		return nil, err
	}
	for kind, id := range found {
		s.learn(kind, id)
	}
	if s.tx.tx == nil {
		s.publish()
	}
	ids, missing = s.mapKinds(kinds)
	if len(missing) > 0 {
		return nil, &pgsql.MissingKindsError{Kinds: missing, KindIDs: ids}
	}
	return ids, nil
}

func (s *transactionKindMapper) mapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	ids := make([]int16, 0, len(kinds))
	var missing graph.Kinds
	for _, kind := range kinds {
		if id, ok := s.kindID(kind); ok {
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
	for _, kind := range kinds {
		if _, ok := s.kindID(kind); ok {
			continue
		}
		started := time.Now()
		id, err := query.On(metadataTransaction{s.tx}).InsertOrGetKind(kind)
		// A concurrent insertion may commit after this statement's snapshot.
		if errors.Is(err, pgx.ErrNoRows) {
			id, err = query.On(metadataTransaction{s.tx}).InsertOrGetKind(kind)
		}
		s.tx.recordKindWork("kind_register", started)
		if err != nil {
			return nil, err
		}
		s.register(kind, id)
		if s.tx.tx != nil {
			s.hasUncommittedKinds = true
		} else {
			s.publish()
		}
	}
	ids, _ := s.mapKinds(kinds)
	return ids, nil
}

// MapKindIDs is memory-only, even for an empty cache or active row stream. A
// miss keeps the existing mapping failure and requests work at a later boundary.
func (s *transactionKindMapper) MapKindIDs(ctx context.Context, ids []int16) (graph.Kinds, error) {
	kinds := make(graph.Kinds, len(ids))
	for i, id := range ids {
		kind, ok := s.registeredIDs[id]
		if !ok {
			kind, ok = s.ids[id]
		}
		if !ok {
			kind, ok = s.base().ids[id]
		}
		if !ok {
			kind, ok = s.latest().ids[id]
		}
		if !ok {
			if s.tx != nil && s.tx.schemaManager != nil {
				s.tx.schemaManager.kindCache.invalidate()
			}
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

// bypassTranslations prevents private or invalidated catalog views from entering
// either compiler cache after a destructive reload.
func (s *transactionKindMapper) bypassTranslations() bool {
	// A private lookup may see an opaque raw catalog write in this transaction.
	// Its ID is no more suitable for translation retention than a registration.
	return s.privateCatalog || s.hasUncommittedKinds || (len(s.registeredKinds) > 0 && s.registeredEpoch != s.base().epoch) ||
		(s.tx != nil && s.tx.tx != nil && len(s.kinds) > 0) ||
		(s.tx != nil && s.tx.schemaManager != nil && s.base().epoch != s.tx.schemaManager.kindCache.current.Load().epoch)
}

func (s *transactionKindMapper) publish() {
	if s == nil || s.tx == nil || s.tx.schemaManager == nil || len(s.kinds)+len(s.registeredKinds) == 0 {
		return
	}
	cache := s.tx.schemaManager.kindCache
	changes := s.kinds
	if len(changes) == 0 {
		changes = s.registeredKinds
	} else if len(s.registeredKinds) > 0 {
		// Combine only the small private deltas, never an old shared snapshot.
		changes = maps.Clone(changes)
		maps.Copy(changes, s.registeredKinds)
	}
	epoch := s.base().epoch
	if len(s.registeredKinds) > 0 {
		// Registrations retained across an epoch change are still usable by
		// their owner, but cannot republish a catalog view from that epoch.
		epoch = s.registeredEpoch
	}
	if cache.merge(changes, epoch) {
		s.snapshot = cache.current.Load()
		s.kinds, s.ids = nil, nil
		s.registeredKinds, s.registeredIDs = nil, nil
	}
	s.hasUncommittedKinds = false
}

// serviceKinds reloads shared metadata only with an idle autocommit connection.
// Cold callers wait for initialization; warm callers keep their cached view
// during a reload. Stable transactions reconcile only their private catalog.
func (s *transaction) serviceKinds() error {
	if s.schemaManager == nil {
		return nil
	}
	cache := s.schemaManager.kindCache
	if cache.pending() && s.tx == nil && s.conn != nil && !s.conn.Conn().PgConn().IsBusy() && s.conn.Conn().PgConn().TxStatus() == 'I' {
		cold := !cache.current.Load().loaded
		if cold {
			if err := cache.lockRefresh(s.ctx); err != nil {
				return err
			}
		} else if !cache.tryLockRefresh() {
			return s.kindMapper.adopt()
		}
		defer cache.unlockRefresh()
		for cache.pending() {
			if _, err := s.reloadKinds(false); err != nil {
				return err
			}
			if !cold || cache.current.Load().loaded {
				break
			}
		}
	}
	return s.kindMapper.adopt()
}

// reloadKinds performs I/O outside both metadata locks. Only an unchanged
// publication generation may install the returned full catalog view.
func (s *transaction) reloadKinds(force bool) (bool, error) {
	cache := s.schemaManager.kindCache
	base, generation := cache.current.Load(), cache.requested.Load()
	started := time.Now()
	kinds, err := query.On(metadataTransaction{s}).SelectKinds()
	s.recordKindWork("kind_refresh", started)
	if err != nil {
		return false, err
	}
	installed := cache.replace(base, generation, kinds, force, s.schemaManager.invalidateKindTranslations)
	if !installed {
		cache.invalidate()
	}
	return installed, nil
}

// Existing collectors receive one sample per actual metadata operation. Warm
// bookkeeping does not read a clock or emit samples.
func (s *transaction) recordKindWork(class string, started time.Time) {
	if s.schemaManager != nil {
		s.recordSQLGenerationProfile(SQLGenerationProfile{QueryClass: class, Dispatch: time.Since(started)})
	}
}

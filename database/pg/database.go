package pg

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/util/size"
)

func beginTx(ctx context.Context, conn *pgxpool.Conn, options []database.Option) (pgx.Tx, error) {
	var (
		// Default to read-write
		txAccessMode = pgx.ReadWrite
	)

	for _, option := range options {
		if option == database.OptionReadOnly {
			txAccessMode = pgx.ReadOnly
		}
	}

	return conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: txAccessMode,
	})
}

type instance struct {
	pool                  *pgxpool.Pool
	graphQueryMemoryLimit size.Size
	schemaManager         *SchemaManager
}

func New(internalDriver *pgxpool.Pool, cfg dawgs.Config) database.Instance {
	return &instance{
		pool:                  internalDriver,
		graphQueryMemoryLimit: cfg.GraphQueryMemoryLimit,
		schemaManager:         NewSchemaManager(internalDriver),
	}
}

func (s *instance) AssertSchema(ctx context.Context, schema database.Schema) error {
	return s.schemaManager.AssertSchema(ctx, schema)
}

func (s *instance) Session(ctx context.Context, driverLogic database.QueryLogic, options ...database.Option) error {
	if acquiredConn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer acquiredConn.Release()
		return driverLogic(ctx, newInternalDriver(acquiredConn, s.schemaManager))
	}
}

func (s *instance) Transaction(ctx context.Context, driverLogic database.QueryLogic, options ...database.Option) error {
	if acquiredConn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer acquiredConn.Release()

		if transaction, err := beginTx(ctx, acquiredConn, options); err != nil {
			return err
		} else {
			defer func() {
				if err := transaction.Rollback(ctx); err != nil {
					slog.DebugContext(ctx, "failed to rollback transaction", slog.String("err", err.Error()))
				}
			}()

			if err := driverLogic(ctx, newInternalDriver(transaction, s.schemaManager)); err != nil {
				return err
			}

			return transaction.Commit(ctx)
		}
	}
}

func (s *instance) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	var (
		kindIDsByKind = s.schemaManager.GetKindIDsByKind()
		kinds         = make(graph.Kinds, 0, len(kindIDsByKind))
	)

	for _, kind := range kindIDsByKind {
		kinds = append(kinds, kind)
	}

	return kinds, nil
}

func (s *instance) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}

func KindMapperFromInstance(dbInst database.Instance) (KindMapper, error) {
	if pgInstance, typeOK := dbInst.(*instance); !typeOK {
		return nil, fmt.Errorf("dawgs pg: uanble to get kind mapper from instance type: %T", dbInst)
	} else {
		return pgInstance.schemaManager, nil
	}
}

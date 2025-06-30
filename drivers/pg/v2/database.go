package v2

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/util/size"
	v2 "github.com/specterops/dawgs/v2"
)

func beginTx(ctx context.Context, conn *pgxpool.Conn, options []v2.Option) (pgx.Tx, error) {
	var (
		// Default to read-write
		txAccessMode = pgx.ReadWrite
	)

	for _, option := range options {
		if option == v2.OptionReadOnly {
			txAccessMode = pgx.ReadOnly
		}
	}

	return conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: txAccessMode,
	})
}

type database struct {
	pool                  *pgxpool.Pool
	graphQueryMemoryLimit size.Size
	schemaManager         *SchemaManager
}

func NewDatabase(internalDriver *pgxpool.Pool, cfg v2.Config) v2.Database {
	return &database{
		pool:                  internalDriver,
		graphQueryMemoryLimit: cfg.GraphQueryMemoryLimit,
		schemaManager:         NewSchemaManager(internalDriver),
	}
}

func (s *database) AssertSchema(ctx context.Context, schema v2.Schema) error {
	return s.schemaManager.AssertSchema(ctx, schema)
}

func (s *database) Session(ctx context.Context, driverLogic v2.DriverLogic, options ...v2.Option) error {
	if acquiredConn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer acquiredConn.Release()
		return driverLogic(ctx, newInternalDriver(acquiredConn, s.schemaManager))
	}
}

func (s *database) Transaction(ctx context.Context, driverLogic v2.DriverLogic, options ...v2.Option) error {
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

func (s *database) Close() error {
	s.pool.Close()
	return nil
}

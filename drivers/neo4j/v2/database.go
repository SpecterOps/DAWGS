package v2

import (
	"context"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
	"github.com/specterops/dawgs/util/size"
	v2 "github.com/specterops/dawgs/v2"
)

type database struct {
	internalDriver            neo4j.DriverWithContext
	defaultTransactionTimeout time.Duration
	limiter                   channels.ConcurrencyLimiter
	writeFlushSize            int
	batchWriteSize            int
	graphQueryMemoryLimit     size.Size
}

func NewDatabase(internalDriver neo4j.DriverWithContext, cfg v2.Config) v2.Database {
	return &database{
		internalDriver:            internalDriver,
		defaultTransactionTimeout: DefaultNeo4jTransactionTimeout,
		limiter:                   channels.NewConcurrencyLimiter(DefaultConcurrentConnections),
		writeFlushSize:            DefaultWriteFlushSize,
		batchWriteSize:            DefaultBatchWriteSize,
		graphQueryMemoryLimit:     cfg.GraphQueryMemoryLimit,
	}
}

func (s *database) acquireInternalSession(ctx context.Context, options []v2.Option) (neo4j.SessionWithContext, error) {
	// Attempt to acquire a connection slot or wait for a bit until one becomes available
	if !s.limiter.Acquire(ctx) {
		return nil, graph.ErrConcurrentConnectionSlotTimeOut
	}

	var (
		sessionCfg = neo4j.SessionConfig{
			// Default to a write enabled session if no options are supplied
			AccessMode: neo4j.AccessModeWrite,
		}
	)

	for _, option := range options {
		if option == v2.SessionOptionReadOnly {
			sessionCfg.AccessMode = neo4j.AccessModeRead
		}
	}

	return s.internalDriver.NewSession(ctx, sessionCfg), nil
}

func (s *database) Session(ctx context.Context, driverLogic v2.DriverLogic, options ...v2.Option) error {
	if session, err := s.acquireInternalSession(ctx, options); err != nil {
		return err
	} else {
		// Release the connection slot when this function exits
		defer s.limiter.Release()

		defer func() {
			if err := session.Close(ctx); err != nil {
				slog.DebugContext(ctx, "failed to close session", slog.String("err", err.Error()))
			}
		}()

		return driverLogic(ctx, newInternalDriver(&sessionDriver{
			session: session,
		}))
	}
}

func (s *database) Transaction(ctx context.Context, driverLogic v2.DriverLogic, options ...v2.Option) error {
	if session, err := s.acquireInternalSession(ctx, options); err != nil {
		return err
	} else {
		// Release the connection slot when this function exits
		defer s.limiter.Release()

		defer func() {
			if err := session.Close(ctx); err != nil {
				slog.DebugContext(ctx, "failed to close session", slog.String("err", err.Error()))
			}
		}()

		// Acquire a new transaction
		if transaction, err := session.BeginTransaction(ctx); err != nil {
			return err
		} else {
			defer func() {
				if err := transaction.Rollback(ctx); err != nil {
					slog.DebugContext(ctx, "failed to rollback transaction", slog.String("err", err.Error()))
				}
			}()

			if err := driverLogic(ctx, newInternalDriver(transaction)); err != nil {
				return err
			}

			return transaction.Commit(ctx)
		}
	}
}

func (s *database) Close() error {
	//TODO implement me
	panic("implement me")
}

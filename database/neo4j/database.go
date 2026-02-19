package neo4j

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
	"github.com/specterops/dawgs/util/size"
)

type instance struct {
	internalDriver            neo4j.DriverWithContext
	defaultTransactionTimeout time.Duration
	limiter                   channels.ConcurrencyLimiter
	graphQueryMemoryLimit     size.Size
	schemaManager             *SchemaManager
}

func New(internalDriver neo4j.DriverWithContext, cfg dawgs.Config) database.Instance {
	return &instance{
		internalDriver:            internalDriver,
		defaultTransactionTimeout: DefaultNeo4jTransactionTimeout,
		limiter:                   channels.NewConcurrencyLimiter(DefaultConcurrentConnections),
		graphQueryMemoryLimit:     cfg.GraphQueryMemoryLimit,
		schemaManager:             NewSchemaManager(internalDriver),
	}
}

func (s *instance) AssertSchema(ctx context.Context, schema database.Schema) error {
	return s.schemaManager.AssertSchema(ctx, schema)
}

func (s *instance) acquireInternalSession(ctx context.Context, options []database.Option) (neo4j.SessionWithContext, error) {
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
		if option == database.OptionReadOnly {
			sessionCfg.AccessMode = neo4j.AccessModeRead
		}
	}

	return s.internalDriver.NewSession(ctx, sessionCfg), nil
}

func (s *instance) Session(ctx context.Context, driverLogic database.QueryLogic, options ...database.Option) error {
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

func (s *instance) Transaction(ctx context.Context, driverLogic database.QueryLogic, options ...database.Option) error {
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

func (s *instance) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	var kinds graph.Kinds

	if session, err := s.acquireInternalSession(ctx, nil); err != nil {
		return nil, err
	} else {
		// Release the connection slot when this function exits
		defer s.limiter.Release()

		defer func() {
			if err := session.Close(ctx); err != nil {
				slog.DebugContext(ctx, "failed to close session", slog.String("err", err.Error()))
			}
		}()

		consumeKindResult := func(result neo4j.ResultWithContext) error {
			defer result.Consume(ctx)

			for result.Next(ctx) {
				values := result.Record().Values

				if len(values) == 0 {
					return fmt.Errorf("expected at least one value for labels")
				} else if kindStr, typeOK := values[0].(string); !typeOK {
					return fmt.Errorf("unexpected label type from Neo4j: %T", values[0])
				} else {
					kinds = append(kinds, graph.StringKind(kindStr))
				}
			}

			return nil
		}

		if result, err := session.Run(ctx, "call db.labels();", nil); err != nil {
			return nil, err
		} else if err := consumeKindResult(result); err != nil {
			return nil, err
		}

		if result, err := session.Run(ctx, "call db.relationshipTypes();", nil); err != nil {
			return nil, err
		} else if err := consumeKindResult(result); err != nil {
			return nil, err
		}
	}

	return kinds, nil
}

func (s *instance) Close(ctx context.Context) error {
	return s.internalDriver.Close(ctx)
}

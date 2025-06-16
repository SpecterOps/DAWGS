package neo4j

import (
	"context"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
	"github.com/specterops/dawgs/util/size"
)

const (
	DriverName = "neo4j"
)

func readCfg() neo4j.SessionConfig {
	return neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	}
}

func writeCfg() neo4j.SessionConfig {
	return neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	}
}

type driver struct {
	driver                    neo4j.Driver
	limiter                   channels.ConcurrencyLimiter
	defaultTransactionTimeout time.Duration
	batchWriteSize            int
	writeFlushSize            int
	graphQueryMemoryLimit     size.Size
}

func (s *driver) SetBatchWriteSize(size int) {
	s.batchWriteSize = size
}

func (s *driver) SetWriteFlushSize(size int) {
	s.writeFlushSize = size
}

func (s *driver) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate) error {
	// Attempt to acquire a connection slot or wait for a bit until one becomes available
	if !s.limiter.Acquire(ctx) {
		return graph.ErrContextTimedOut
	} else {
		defer s.limiter.Release()
	}

	var (
		cfg = graph.TransactionConfig{
			Timeout: s.defaultTransactionTimeout,
		}

		session = s.driver.NewSession(writeCfg())
		batch   = newBatchOperation(ctx, session, cfg, s.writeFlushSize, s.batchWriteSize, s.graphQueryMemoryLimit)
	)

	defer session.Close()
	defer batch.Close()

	if err := batchDelegate(batch); err != nil {
		return err
	}

	return batch.Commit()
}

func (s *driver) Close(ctx context.Context) error {
	return s.driver.Close()
}

func (s *driver) transaction(ctx context.Context, txDelegate graph.TransactionDelegate, session neo4j.Session, options []graph.TransactionOption) error {
	// Attempt to acquire a connection slot or wait for a bit until one becomes available
	if !s.limiter.Acquire(ctx) {
		return graph.ErrContextTimedOut
	} else {
		defer s.limiter.Release()
	}

	cfg := graph.TransactionConfig{
		Timeout: s.defaultTransactionTimeout,
	}

	// Apply the transaction options
	for _, option := range options {
		option(&cfg)
	}

	tx := newTransaction(ctx, session, cfg, s.writeFlushSize, s.batchWriteSize, s.graphQueryMemoryLimit)
	defer tx.Close()

	if err := txDelegate(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *driver) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	session := s.driver.NewSession(readCfg())
	defer session.Close()

	return s.transaction(ctx, txDelegate, session, options)
}

func (s *driver) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	session := s.driver.NewSession(writeCfg())
	defer session.Close()

	return s.transaction(ctx, txDelegate, session, options)
}

func (s *driver) AssertSchema(ctx context.Context, schema graph.Schema) error {
	return assertSchema(ctx, s, schema)
}

func (s *driver) SetDefaultGraph(ctx context.Context, schema graph.Graph) error {
	// Note: Neo4j does not support isolated physical graph namespaces. Namespacing can be emulated with Kinds but will
	// not be supported for this driver since the fallback behavior is no different from storing all graph data in the
	// same namespace.
	//
	// This is different for the PostgreSQL driver, specifically, since the driver in question supports on-disk
	// isolation of graph namespaces.
	return nil
}

func (s *driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	return s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(query, parameters)
		defer result.Close()

		return result.Error()
	})
}

func (s *driver) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	var kinds graph.Kinds

	if err := s.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if result := tx.Raw("CALL db.labels()", nil); result.Error() != nil {
			return result.Error()
		} else {
			for result.Next() {
				var kind string
				if err := result.Scan(&kind); err != nil {
					return err
				} else {
					kinds = append(kinds, graph.StringKind(kind))
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return kinds, nil
}

func (s *driver) RefreshKinds(_ context.Context) error {
	// This isn't needed for neo4j
	return nil
}

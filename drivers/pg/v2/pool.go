package v2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
)

// poolInitConnectionTimeout bounds initial pgx pool connection setup.
const poolInitConnectionTimeout = 10 * time.Second

// Pool owns a pgx pool and its matching connection-local cache provider. It
// is constructed only by NewPool so the two lifecycles cannot be mismatched.
type Pool struct {
	// pool leases and manages pgx physical connections.
	pool *pgxpool.Pool

	// provider owns cache state associated with each physical connection.
	provider *connectionCacheProvider

	// warmups supplies the persistent statement warm-up set to new connections.
	warmups *statementWarmupPolicy

	// closeOnce prevents duplicate pool and provider teardown.
	closeOnce sync.Once
}

// statementWarmupPolicy retains only normalized SQL selected by an operator.
// It is deliberately empty by default and is shared with AfterConnect so new
// physical connections receive the same warming policy as current ones.
type statementWarmupPolicy struct {
	// lock serializes snapshots and replacements of the operator-selected warm set.
	lock sync.RWMutex

	// statements contains normalized SQL identities and text for future connections.
	statements []preparedStatementWarmup

	// generation advances on every replacement, including a clear. Connections
	// use it to ensure initialization does not finish with an obsolete policy.
	generation uint64
}

// snapshot returns the current generation and an independent view of its warm set.
func (s *statementWarmupPolicy) snapshot() (uint64, []preparedStatementWarmup) {
	if s == nil {
		return 0, nil
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.generation, append([]preparedStatementWarmup(nil), s.statements...)
}

// replace atomically installs a copy of the supplied warm set.
func (s *statementWarmupPolicy) replace(statements []preparedStatementWarmup) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.statements = append([]preparedStatementWarmup(nil), statements...)
	s.generation++
}

// isCurrent reports whether generation is still the published policy.
func (s *statementWarmupPolicy) isCurrent(generation uint64) bool {
	if s == nil {
		return generation == 0
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.generation == generation
}

// warmCurrentStatementPolicy keeps a newly established connection from
// completing initialization against a policy superseded during preparation.
func warmCurrentStatementPolicy(warmups *statementWarmupPolicy, warm func([]preparedStatementWarmup) error) error {
	for {
		generation, statements := warmups.snapshot()
		if err := warm(statements); err != nil {
			return err
		}
		if warmups.isCurrent(generation) {
			return nil
		}
	}
}

// poolLifecycleHooks groups the driver lifecycle hooks composed into pgx.
type poolLifecycleHooks struct {
	// afterConnect initializes a newly established physical connection.
	afterConnect func(context.Context, *pgx.Conn) error

	// afterRelease validates a connection before it returns to the pool.
	afterRelease func(*pgx.Conn) bool
}

// productionPoolLifecycleHooks returns the stable PostgreSQL connection lifecycle hooks.
func productionPoolLifecycleHooks() poolLifecycleHooks {
	return poolLifecycleHooks{
		afterConnect: pg.AfterPooledConnectionEstablished,
		afterRelease: pg.AfterPooledConnectionRelease,
	}
}

// composePoolConfig copies caller configuration and composes v2 cache lifecycle hooks.
func composePoolConfig(poolConfig *pgxpool.Config, v2Config Config, provider *connectionCacheProvider, warmups *statementWarmupPolicy, hooks poolLifecycleHooks) (*pgxpool.Config, error) {
	if poolConfig == nil || poolConfig.ConnConfig == nil {
		return nil, fmt.Errorf("PostgreSQL pool config is required")
	}
	if provider == nil {
		return nil, fmt.Errorf("connection cache provider is required")
	}
	if err := v2Config.validate(); err != nil {
		return nil, err
	}

	configuredPool := poolConfig.Copy()
	callerAfterConnect := configuredPool.AfterConnect
	callerAfterRelease := configuredPool.AfterRelease
	callerBeforeClose := configuredPool.BeforeClose

	poolLimits := v2Config.resolvedPoolConfig()
	configuredPool.MinConns = poolLimits.MinConnections
	configuredPool.MaxConns = poolLimits.MaxConnections
	configuredPool.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if hooks.afterConnect != nil {
			if err := hooks.afterConnect(ctx, conn); err != nil {
				return err
			}
		}
		if callerAfterConnect != nil {
			if err := callerAfterConnect(ctx, conn); err != nil {
				return err
			}
		}
		provider.registerConnection(conn)
		if err := warmCurrentStatementPolicy(warmups, func(statements []preparedStatementWarmup) error {
			return provider.warmStatementsForConnection(conn, statements, func(name, sql string) error {
				_, err := conn.Prepare(ctx, name, sql)
				return err
			})
		}); err != nil {
			return err
		}
		return nil
	}
	configuredPool.AfterRelease = func(conn *pgx.Conn) bool {
		if hooks.afterRelease != nil && !hooks.afterRelease(conn) {
			return false
		}
		if callerAfterRelease != nil && !callerAfterRelease(conn) {
			return false
		}
		return true
	}
	configuredPool.BeforeClose = func(conn *pgx.Conn) {
		provider.removeConnection(conn)
		if callerBeforeClose != nil {
			callerBeforeClose(conn)
		}
	}
	return configuredPool, nil
}

// NewPool constructs an opt-in v2 pool. It copies poolConfig before composing
// required and caller lifecycle hooks, leaving the caller's configuration
// reusable. Config{} explicitly disables translation retention; use
// DefaultConfig for the conservative 64-entry per-connection default.
func NewPool(ctx context.Context, poolConfig *pgxpool.Config, config Config) (*Pool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pool context is required")
	}
	provider, err := newConnectionCacheProvider(config)
	if err != nil {
		return nil, err
	}
	warmups := &statementWarmupPolicy{}
	configuredPool, err := composePoolConfig(poolConfig, config, provider, warmups, productionPoolLifecycleHooks())
	if err != nil {
		provider.close()
		return nil, err
	}

	poolCtx, cancel := context.WithTimeout(ctx, poolInitConnectionTimeout)
	defer cancel()
	underlying, err := pgxpool.NewWithConfig(poolCtx, configuredPool)
	if err != nil {
		provider.close()
		return nil, err
	}
	return &Pool{
		pool:     underlying,
		provider: provider,
		warmups:  warmups,
	}, nil
}

// SetStatementWarmupPolicy replaces the opt-in warm set, prepares it on
// currently idle connections, and applies it to every subsequently created
// physical connection. Passing no statements clears the future warm set.
func (s *Pool) SetStatementWarmupPolicy(ctx context.Context, statements ...string) error {
	if s == nil || s.pool == nil || s.provider == nil || s.warmups == nil {
		return fmt.Errorf("PostgreSQL v2 pool is not initialized")
	}
	warmups, err := normalizePreparedStatementWarmups(statements)
	if err != nil {
		return err
	}
	s.warmups.replace(warmups)
	return s.warmPreparedStatements(ctx, warmups)
}

// NewDefaultPool constructs an opt-in v2 pool with DefaultConfig.
func NewDefaultPool(ctx context.Context, poolConfig *pgxpool.Config) (*Pool, error) {
	return NewPool(ctx, poolConfig, DefaultConfig())
}

// Close closes the underlying pool and all remaining provider state. Drivers
// normally own this operation through Driver.Close.
func (s *Pool) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.pool != nil {
			s.pool.Close()
		}
		if s.provider != nil {
			s.provider.close()
		}
	})
}

// Reset closes idle physical connections and causes acquired connections to be
// closed when released. BeforeClose retires every affected connection cache.
// Use it after an out-of-band schema change that can affect registered types
// or generated SQL.
func (s *Pool) Reset() {
	if s != nil && s.pool != nil {
		s.pool.Reset()
	}
}

// WarmStatements prepares the supplied PostgreSQL SQL on every currently
// idle physical connection. It never executes the SQL. Call it after schema
// assertion, ideally while the pool is otherwise quiescent; newly created
// connections warm lazily through pgx's normal CacheStatement behavior.
func (s *Pool) WarmStatements(ctx context.Context, statements ...string) error {
	if s == nil || s.pool == nil || s.provider == nil {
		return fmt.Errorf("PostgreSQL v2 pool is not initialized")
	}
	warmups, err := normalizePreparedStatementWarmups(statements)
	if err != nil {
		return err
	}
	return s.warmPreparedStatements(ctx, warmups)
}

// warmPreparedStatements prepares a normalized warm set on idle connections.
func (s *Pool) warmPreparedStatements(ctx context.Context, warmups []preparedStatementWarmup) error {
	if len(warmups) == 0 {
		return nil
	}

	connections := s.pool.AcquireAllIdle(ctx)
	if len(connections) == 0 {
		connection, err := s.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		connections = []*pgxpool.Conn{connection}
	}
	defer func() {
		for _, connection := range connections {
			connection.Release()
		}
	}()

	var errs []error
	for _, connection := range connections {
		if err := s.provider.warmStatementsForConnection(connection.Conn(), warmups, func(name, sql string) error {
			_, err := connection.Conn().Prepare(ctx, name, sql)
			return err
		}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// closeProvider retires cache state after the delegate has closed the pgx pool.
func (s *Pool) closeProvider() {
	if s != nil && s.provider != nil {
		s.provider.close()
	}
}

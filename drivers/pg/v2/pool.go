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

const poolInitConnectionTimeout = 10 * time.Second

// Pool owns a pgx pool and its matching connection-local cache provider. It
// is constructed only by NewPool so the two lifecycles cannot be mismatched.
type Pool struct {
	pool     *pgxpool.Pool
	provider *connectionCacheProvider
	warmups  *statementWarmupPolicy

	closeOnce sync.Once
}

// statementWarmupPolicy retains only normalized SQL selected by an operator.
// It is deliberately empty by default and is shared with AfterConnect so new
// physical connections receive the same warming policy as current ones.
type statementWarmupPolicy struct {
	lock       sync.RWMutex
	statements []preparedStatementWarmup
}

func (s *statementWarmupPolicy) snapshot() []preparedStatementWarmup {
	if s == nil {
		return nil
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return append([]preparedStatementWarmup(nil), s.statements...)
}

func (s *statementWarmupPolicy) replace(statements []preparedStatementWarmup) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.statements = append([]preparedStatementWarmup(nil), statements...)
}

type poolLifecycleHooks struct {
	afterConnect func(context.Context, *pgx.Conn) error
	afterRelease func(*pgx.Conn) bool
}

func productionPoolLifecycleHooks() poolLifecycleHooks {
	return poolLifecycleHooks{
		afterConnect: pg.AfterPooledConnectionEstablished,
		afterRelease: pg.AfterPooledConnectionRelease,
	}
}

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
		if statements := warmups.snapshot(); len(statements) > 0 {
			if err := provider.warmStatementsForConnection(conn, statements, func(name, sql string) error {
				_, err := conn.Prepare(ctx, name, sql)
				return err
			}); err != nil {
				return err
			}
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
	if err := s.WarmStatements(ctx, statements...); err != nil {
		return err
	}
	s.warmups.replace(warmups)
	return nil
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

func (s *Pool) closeProvider() {
	if s != nil && s.provider != nil {
		s.provider.close()
	}
}

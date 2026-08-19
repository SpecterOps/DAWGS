package v2

import (
	"context"
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

	closeOnce sync.Once
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

func composePoolConfig(poolConfig *pgxpool.Config, provider *connectionCacheProvider, hooks poolLifecycleHooks) (*pgxpool.Config, error) {
	if poolConfig == nil || poolConfig.ConnConfig == nil {
		return nil, fmt.Errorf("PostgreSQL pool config is required")
	}
	if provider == nil {
		return nil, fmt.Errorf("connection cache provider is required")
	}

	config := poolConfig.Copy()
	callerAfterConnect := config.AfterConnect
	callerAfterRelease := config.AfterRelease
	callerBeforeClose := config.BeforeClose

	// Mirror the current v1 pool sizing. Pool tuning is deliberately outside
	// the v2 cache-lifetime experiment.
	config.MinConns = 5
	config.MaxConns = 50
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
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
		return nil
	}
	config.AfterRelease = func(conn *pgx.Conn) bool {
		if hooks.afterRelease != nil && !hooks.afterRelease(conn) {
			return false
		}
		if callerAfterRelease != nil && !callerAfterRelease(conn) {
			return false
		}
		return true
	}
	config.BeforeClose = func(conn *pgx.Conn) {
		provider.removeConnection(conn)
		if callerBeforeClose != nil {
			callerBeforeClose(conn)
		}
	}
	return config, nil
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
	configuredPool, err := composePoolConfig(poolConfig, provider, productionPoolLifecycleHooks())
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
	}, nil
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

func (s *Pool) closeProvider() {
	if s != nil && s.provider != nil {
		s.provider.close()
	}
}

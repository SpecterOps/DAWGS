package v2

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func testPoolConfig(t *testing.T) *pgxpool.Config {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgresql://localhost:5432/dawgs")
	require.NoError(t, err)
	return config
}

func TestComposePoolConfigCopiesAndOrdersHooks(t *testing.T) {
	provider, err := newConnectionCacheProvider(DefaultConfig())
	require.NoError(t, err)
	config := testPoolConfig(t)
	var order []string
	config.MinConns = 1
	config.MaxConns = 2
	config.AfterConnect = func(context.Context, *pgx.Conn) error {
		order = append(order, "caller-connect")
		return nil
	}
	config.AfterRelease = func(*pgx.Conn) bool {
		order = append(order, "caller-release")
		return true
	}
	config.BeforeClose = func(conn *pgx.Conn) {
		require.Nil(t, provider.CacheForConnection(conn))
		order = append(order, "caller-close")
	}
	hooks := poolLifecycleHooks{
		afterConnect: func(context.Context, *pgx.Conn) error {
			order = append(order, "dawgs-connect")
			return nil
		},
		afterRelease: func(*pgx.Conn) bool {
			order = append(order, "dawgs-release")
			return true
		},
	}

	composed, err := composePoolConfig(config, DefaultConfig(), provider, &statementWarmupPolicy{}, hooks)
	require.NoError(t, err)
	require.NotSame(t, config, composed)
	require.Equal(t, int32(1), config.MinConns)
	require.Equal(t, int32(2), config.MaxConns)
	require.Equal(t, int32(5), composed.MinConns)
	require.Equal(t, int32(50), composed.MaxConns)

	conn := &pgx.Conn{}
	require.NoError(t, composed.AfterConnect(context.Background(), conn))
	require.NotNil(t, provider.CacheForConnection(conn))
	require.True(t, composed.AfterRelease(conn))
	composed.BeforeClose(conn)
	require.Equal(t, []string{"dawgs-connect", "caller-connect", "dawgs-release", "caller-release", "caller-close"}, order)
}

func TestComposePoolConfigPreservesHookFailuresAndRejection(t *testing.T) {
	provider, err := newConnectionCacheProvider(DefaultConfig())
	require.NoError(t, err)
	conn := &pgx.Conn{}

	t.Run("failed required connect does not register state", func(t *testing.T) {
		config := testPoolConfig(t)
		expected := errors.New("required setup failed")
		composed, err := composePoolConfig(config, DefaultConfig(), provider, &statementWarmupPolicy{}, poolLifecycleHooks{
			afterConnect: func(context.Context, *pgx.Conn) error { return expected },
		})
		require.NoError(t, err)
		require.ErrorIs(t, composed.AfterConnect(context.Background(), conn), expected)
		require.Nil(t, provider.CacheForConnection(conn))
	})

	t.Run("failed caller connect does not register state", func(t *testing.T) {
		config := testPoolConfig(t)
		expected := errors.New("caller setup failed")
		config.AfterConnect = func(context.Context, *pgx.Conn) error { return expected }
		composed, err := composePoolConfig(config, DefaultConfig(), provider, &statementWarmupPolicy{}, poolLifecycleHooks{
			afterConnect: func(context.Context, *pgx.Conn) error { return nil },
		})
		require.NoError(t, err)
		require.ErrorIs(t, composed.AfterConnect(context.Background(), conn), expected)
		require.Nil(t, provider.CacheForConnection(conn))
	})

	t.Run("required release rejection skips caller", func(t *testing.T) {
		config := testPoolConfig(t)
		called := false
		config.AfterRelease = func(*pgx.Conn) bool {
			called = true
			return true
		}
		composed, err := composePoolConfig(config, DefaultConfig(), provider, &statementWarmupPolicy{}, poolLifecycleHooks{
			afterRelease: func(*pgx.Conn) bool { return false },
		})
		require.NoError(t, err)
		require.False(t, composed.AfterRelease(conn))
		require.False(t, called)
	})

	t.Run("caller release rejection is preserved", func(t *testing.T) {
		config := testPoolConfig(t)
		config.AfterRelease = func(*pgx.Conn) bool { return false }
		composed, err := composePoolConfig(config, DefaultConfig(), provider, &statementWarmupPolicy{}, poolLifecycleHooks{
			afterRelease: func(*pgx.Conn) bool { return true },
		})
		require.NoError(t, err)
		require.False(t, composed.AfterRelease(conn))
	})
}

func TestDefaultConfigUsesConservativePerConnectionCapacity(t *testing.T) {
	require.Equal(t, defaultTranslationCacheEntries, DefaultConfig().TranslationCacheEntries)
	require.Equal(t, &PoolConfig{MinConnections: defaultMinConnections, MaxConnections: defaultMaxConnections}, DefaultConfig().Pool)
}

func TestConfigValidatesAndAppliesExplicitPoolLimits(t *testing.T) {
	config := Config{
		TranslationCacheEntries: 3,
		Pool:                    &PoolConfig{MinConnections: 0, MaxConnections: 2},
	}
	provider, err := newConnectionCacheProvider(config)
	require.NoError(t, err)
	composed, err := composePoolConfig(testPoolConfig(t), config, provider, &statementWarmupPolicy{}, poolLifecycleHooks{})
	require.NoError(t, err)
	require.Equal(t, int32(0), composed.MinConns)
	require.Equal(t, int32(2), composed.MaxConns)

	for _, invalid := range []Config{
		{TranslationCacheEntries: -1},
		{Pool: &PoolConfig{MinConnections: -1, MaxConnections: 1}},
		{Pool: &PoolConfig{MinConnections: 0, MaxConnections: 0}},
		{Pool: &PoolConfig{MinConnections: 2, MaxConnections: 1}},
	} {
		require.Error(t, invalid.validate())
	}
}

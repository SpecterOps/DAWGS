// Package v2 provides an explicit opt-in PostgreSQL driver with translation
// caches owned by individual physical PostgreSQL connections.
package v2

import "fmt"

const (
	defaultTranslationCacheEntries = 64
	defaultMinConnections          = 5
	defaultMaxConnections          = 50
)

// PoolConfig controls the pgx pool size owned by a v2 driver. It is optional
// in Config so existing callers retain the v1-compatible 5-50 connection
// defaults. Supplying PoolConfig permits a minimum of zero connections.
type PoolConfig struct {
	MinConnections int32
	MaxConnections int32
}

// Config configures the v2 connection-resident translation cache.
type Config struct {
	// TranslationCacheEntries is the exact SIEVE entry capacity for each live
	// physical PostgreSQL connection. Zero disables retention.
	TranslationCacheEntries int

	// Pool optionally overrides the v1-compatible connection limits. Nil uses
	// DefaultConfig's limits; a non-nil value must have MaxConnections >= 1
	// and MinConnections <= MaxConnections.
	Pool *PoolConfig
}

// DefaultConfig returns the conservative v2 defaults. The aggregate upper
// bound is TranslationCacheEntries multiplied by the number of live physical
// PostgreSQL connections.
func DefaultConfig() Config {
	return Config{
		TranslationCacheEntries: defaultTranslationCacheEntries,
		Pool: &PoolConfig{
			MinConnections: defaultMinConnections,
			MaxConnections: defaultMaxConnections,
		},
	}
}

func (s Config) validate() error {
	if s.TranslationCacheEntries < 0 {
		return fmt.Errorf("translation cache entries must not be negative: %d", s.TranslationCacheEntries)
	}
	if s.Pool != nil {
		if s.Pool.MinConnections < 0 {
			return fmt.Errorf("pool minimum connections must not be negative: %d", s.Pool.MinConnections)
		}
		if s.Pool.MaxConnections < 1 {
			return fmt.Errorf("pool maximum connections must be at least 1: %d", s.Pool.MaxConnections)
		}
		if s.Pool.MinConnections > s.Pool.MaxConnections {
			return fmt.Errorf("pool minimum connections %d exceeds maximum connections %d", s.Pool.MinConnections, s.Pool.MaxConnections)
		}
	}
	return nil
}

func (s Config) resolvedPoolConfig() PoolConfig {
	if s.Pool != nil {
		return *s.Pool
	}
	return PoolConfig{MinConnections: defaultMinConnections, MaxConnections: defaultMaxConnections}
}

package pg

import "fmt"

const (
	// defaultTranslationCacheEntries bounds retained translations on one physical connection.
	defaultTranslationCacheEntries = 64

	// defaultSharedShortestPathTemplateEntries bounds templates shared across pool connections.
	defaultSharedShortestPathTemplateEntries = 128

	// defaultMinConnections preserves the v1-compatible lower pool bound.
	defaultMinConnections = 5

	// defaultMaxConnections preserves the v1-compatible upper pool bound.
	defaultMaxConnections = 50
)

// PoolConfig controls the pgx pool size owned by a PostgreSQL driver. It is optional
// in RuntimeConfig so existing callers retain the 5-50 connection
// defaults. Supplying PoolConfig permits a minimum of zero connections.
type PoolConfig struct {
	// MinConnections keeps this many idle physical connections available when possible.
	MinConnections int32

	// MaxConnections caps the number of physical PostgreSQL connections.
	MaxConnections int32
}

// RuntimeConfig configures the connection-resident translation cache.
type RuntimeConfig struct {
	// TranslationCacheEntries is the exact SIEVE entry capacity for each live
	// physical PostgreSQL connection. Zero disables retention.
	TranslationCacheEntries int

	// SharedShortestPathTemplateEntries bounds immutable shortest-path SQL
	// templates shared by V2 physical connections. Zero disables this L2 tier.
	SharedShortestPathTemplateEntries int

	// Pool optionally overrides the v1-compatible connection limits. Nil uses
	// DefaultConfig's limits; a non-nil value must have MaxConnections >= 1
	// and MinConnections <= MaxConnections.
	Pool *PoolConfig
}

// DefaultRuntimeConfig returns the conservative PostgreSQL defaults. The aggregate upper
// bound is TranslationCacheEntries multiplied by the number of live physical
// PostgreSQL connections.
func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		TranslationCacheEntries:           defaultTranslationCacheEntries,
		SharedShortestPathTemplateEntries: defaultSharedShortestPathTemplateEntries,
		Pool: &PoolConfig{
			MinConnections: defaultMinConnections,
			MaxConnections: defaultMaxConnections,
		},
	}
}

// validate reports whether cache and pool limits can be safely applied to pgx.
func (s RuntimeConfig) validate() error {
	if s.TranslationCacheEntries < 0 {
		return fmt.Errorf("translation cache entries must not be negative: %d", s.TranslationCacheEntries)
	}
	if s.SharedShortestPathTemplateEntries < 0 {
		return fmt.Errorf("shared shortest-path template entries must not be negative: %d", s.SharedShortestPathTemplateEntries)
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

// resolvedPoolConfig returns either the explicit limits or the v1-compatible defaults.
func (s RuntimeConfig) resolvedPoolConfig() PoolConfig {
	if s.Pool != nil {
		return *s.Pool
	}
	return PoolConfig{MinConnections: defaultMinConnections, MaxConnections: defaultMaxConnections}
}

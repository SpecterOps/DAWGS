package pg

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestDriverReportsProviderStatisticsWithoutSensitiveState(t *testing.T) {
	provider, err := newConnectionCacheProvider(DefaultRuntimeConfig())
	require.NoError(t, err)
	driver := &Driver{runtime: &poolRuntime{provider: provider}}

	stats := driver.TranslationCacheStats()
	require.Equal(t, DefaultRuntimeConfig().TranslationCacheEntries, stats.CapacityPerConnection)
	require.Equal(t, int32(defaultMinConnections), stats.MinConnections)
	require.Equal(t, int32(defaultMaxConnections), stats.MaxConnections)
	require.Zero(t, stats.LiveConnections)
	require.Empty(t, stats.Connections)
}

func TestDriverImplementsGraphDatabase(t *testing.T) {
	var database graph.Database = &Driver{}
	require.NotNil(t, database)
}

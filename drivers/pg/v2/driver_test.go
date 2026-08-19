package v2

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestDriverReportsProviderStatisticsWithoutSensitiveState(t *testing.T) {
	provider, err := newConnectionCacheProvider(DefaultConfig())
	require.NoError(t, err)
	driver := &Driver{pool: &Pool{provider: provider}}

	stats := driver.TranslationCacheStats()
	require.Equal(t, DefaultConfig().TranslationCacheEntries, stats.CapacityPerConnection)
	require.Zero(t, stats.LiveConnections)
	require.Empty(t, stats.Connections)
}

func TestDriverImplementsGraphDatabase(t *testing.T) {
	var database graph.Database = &Driver{}
	require.NotNil(t, database)
}

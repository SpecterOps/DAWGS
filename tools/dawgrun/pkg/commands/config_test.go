package commands

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
	"github.com/stretchr/testify/require"
)

type fakeDatabase struct {
	closed bool
}

func (s *fakeDatabase) SetWriteFlushSize(int) {}

func (s *fakeDatabase) SetBatchWriteSize(int) {}

func (s *fakeDatabase) ReadTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}

func (s *fakeDatabase) WriteTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}

func (s *fakeDatabase) BatchOperation(context.Context, graph.BatchDelegate, ...graph.BatchOption) error {
	return nil
}

func (s *fakeDatabase) AssertSchema(context.Context, graph.Schema) error {
	return nil
}

func (s *fakeDatabase) SetDefaultGraph(context.Context, graph.Graph) error {
	return nil
}

func (s *fakeDatabase) Run(context.Context, string, map[string]any) error {
	return nil
}

func (s *fakeDatabase) Close(context.Context) error {
	s.closed = true
	return nil
}

func (s *fakeDatabase) FetchKinds(context.Context) (graph.Kinds, error) {
	return nil, nil
}

func (s *fakeDatabase) RefreshKinds(context.Context) error {
	return nil
}

func TestConfigRoundTrip(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	expected := types.Config{
		Connections: map[string]types.ConnectionConfig{
			"local": {
				Driver:           "pg",
				ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
			},
			"neo": {
				Driver:           "neo4j",
				ConnectionString: "neo4j://neo4j:password@localhost:7687",
			},
		},
	}

	require.NoError(t, expected.Save(configPath))

	actual, err := types.LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestLoadConfigLegacyStringConnections(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(configPath, []byte(`{"connections":{"local":"postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable"}}`), 0o600))

	actual, err := types.LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, map[string]types.ConnectionConfig{
		"local": {
			ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
		},
	}, actual.Connections)
}

func TestDefaultConfigPath(t *testing.T) {
	require.Equal(t, filepath.Join("base", "config.json"), types.DefaultConfigPath("base"))
}

func TestListConnectionsCommandShowsNoConnections(t *testing.T) {
	ctx := NewCommandContext(context.Background(), nil, NewScope(), t.TempDir())

	require.NoError(t, listConnectionsCmd().Fn(ctx, nil))
	require.Equal(t, "No connections\n", ctx.OutputString())
}

func TestListConnectionsCommandShowsConfiguredUnopenedConnections(t *testing.T) {
	scope := NewScope()
	scope.SetConnectionStrings(map[string]string{
		"prod":    "postgres://dawgs:dawgs@prod:5432/dawgs?sslmode=disable",
		"staging": "postgres://dawgs:dawgs@staging:5432/dawgs?sslmode=disable",
	})
	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())

	require.NoError(t, listConnectionsCmd().Fn(ctx, nil))
	require.Equal(t, "Configured connections (2, unopened):\n  prod\n  staging\n", ctx.OutputString())
	require.NotContains(t, ctx.OutputString(), "postgres://")
}

func TestListConnectionsCommandShowsOpenAndConfiguredUnopenedConnections(t *testing.T) {
	scope := NewScope()
	scope.SetConnectionStrings(map[string]string{
		"local": "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
		"prod":  "postgres://dawgs:dawgs@prod:5432/dawgs?sslmode=disable",
	})
	scope.AddConnection("local", &fakeDatabase{}, types.ConnectionConfig{
		Driver:           "pg",
		ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
	})
	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())

	require.NoError(t, listConnectionsCmd().Fn(ctx, nil))
	require.Equal(t, "Open connections (1):\n  local\n\nConfigured connections (1, unopened):\n  prod\n", ctx.OutputString())
}

func TestSaveConnectionsCommandWritesOpenConnections(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	scope := NewScope()
	scope.SetConnectionStrings(map[string]string{
		"configured-only": "neo4j://neo4j:password@localhost:7687",
	})
	scope.AddConnection("local", &fakeDatabase{}, types.ConnectionConfig{
		Driver:           "pg",
		ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
	})
	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())

	require.NoError(t, saveConnectionsCmd().Fn(ctx, []string{configPath}))

	config, err := types.LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, map[string]types.ConnectionConfig{
		"local": {
			Driver:           "pg",
			ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
		},
	}, config.Connections)
	require.Contains(t, ctx.OutputString(), "Saved 1 connection(s)")
}

func TestSaveConnectionsCommandWritesDriverOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	scope := NewScope()
	scope.openDatabase = func(_ context.Context, connStr string, options openConnectionOptions) (graph.Database, string, error) {
		require.Equal(t, "host=localhost user=dawgs dbname=dawgs", connStr)
		require.Equal(t, "pg", options.driverName)

		return &fakeDatabase{}, options.driverName, nil
	}
	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())

	require.NoError(t, openCmd().Fn(ctx, []string{"-driver", "pg", "schemeless", "host=localhost user=dawgs dbname=dawgs"}))
	require.NoError(t, saveConnectionsCmd().Fn(ctx, []string{configPath}))

	config, err := types.LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, map[string]types.ConnectionConfig{
		"schemeless": {
			Driver:           "pg",
			ConnectionString: "host=localhost user=dawgs dbname=dawgs",
		},
	}, config.Connections)
}

func TestLoadConnectionsCommandOpensConfiguredConnections(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, (types.Config{
		Connections: map[string]types.ConnectionConfig{
			"local": {
				ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
			},
			"neo": {
				Driver:           "neo4j",
				ConnectionString: "neo4j://neo4j:password@localhost:7687",
			},
		},
	}).Save(configPath))

	scope := NewScope()
	scope.openDatabase = func(_ context.Context, connStr string, options openConnectionOptions) (graph.Database, string, error) {
		driverName := options.driverName
		if driverName == "" {
			var err error
			driverName, err = driverFromConnectionString(connStr)
			require.NoError(t, err)
		}

		return &fakeDatabase{}, driverName, nil
	}
	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())

	require.NoError(t, loadConnectionsCmd().Fn(ctx, []string{configPath}))

	require.Equal(t, 2, scope.GetNumConnections())
	require.Equal(t, map[string]types.ConnectionConfig{
		"local": {
			Driver:           "pg",
			ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
		},
		"neo": {
			Driver:           "neo4j",
			ConnectionString: "neo4j://neo4j:password@localhost:7687",
		},
	}, scope.GetOpenConnectionConfigs())
	require.Contains(t, ctx.OutputString(), "Loaded 2 connection(s)")
}

func TestLoadConnectionsCommandPassesStoredDriver(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, (types.Config{
		Connections: map[string]types.ConnectionConfig{
			"schemeless": {
				Driver:           "pg",
				ConnectionString: "host=localhost user=dawgs dbname=dawgs",
			},
		},
	}).Save(configPath))

	scope := NewScope()
	scope.openDatabase = func(_ context.Context, connStr string, options openConnectionOptions) (graph.Database, string, error) {
		require.Equal(t, "host=localhost user=dawgs dbname=dawgs", connStr)
		require.Equal(t, "pg", options.driverName)

		return &fakeDatabase{}, options.driverName, nil
	}
	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())

	require.NoError(t, loadConnectionsCmd().Fn(ctx, []string{configPath}))
	require.Equal(t, map[string]types.ConnectionConfig{
		"schemeless": {
			Driver:           "pg",
			ConnectionString: "host=localhost user=dawgs dbname=dawgs",
		},
	}, scope.GetOpenConnectionConfigs())
}

func TestEnsureConnectionOpensConfiguredConnection(t *testing.T) {
	scope := NewScope()
	scope.SetConnectionConfigs(map[string]types.ConnectionConfig{
		"local": {
			Driver:           "pg",
			ConnectionString: "host=localhost user=dawgs dbname=dawgs",
		},
	})

	opened := 0
	scope.openDatabase = func(_ context.Context, connStr string, options openConnectionOptions) (graph.Database, string, error) {
		opened++
		require.True(t, options.quiet)
		require.Equal(t, "pg", options.driverName)
		require.Equal(t, "host=localhost user=dawgs dbname=dawgs", connStr)

		return &fakeDatabase{}, "pg", nil
	}

	ctx := NewCommandContext(context.Background(), nil, scope, t.TempDir())
	conn, err := ctx.EnsureConnection("local")
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, 1, opened)
	require.Empty(t, ctx.OutputString())
}

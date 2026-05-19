package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
	"github.com/stretchr/testify/require"
)

func TestRunCommandExecutesCommand(t *testing.T) {
	output, err := runCommand(context.Background(), nil, commands.NewScope(), t.TempDir(), []string{"help", "save-connections"})

	require.NoError(t, err)
	require.Contains(t, output, "HELP: save-connections")
}

func TestLoadDefaultConfigConnectionStrings(t *testing.T) {
	appConfigBaseDir := t.TempDir()
	require.NoError(t, commands.SaveConfig(filepath.Join(appConfigBaseDir, "config.json"), commands.Config{
		Connections: map[string]commands.ConnectionConfig{
			"local": {
				Driver:           "pg",
				ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
			},
		},
	}))

	scope := commands.NewScope()
	require.NoError(t, loadDefaultConfigConnectionStrings(scope, appConfigBaseDir))

	connStr, ok := scope.GetConnectionString("local")
	require.True(t, ok)
	require.Equal(t, "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable", connStr)
	connConfig, ok := scope.GetConnectionConfig("local")
	require.True(t, ok)
	require.Equal(t, "pg", connConfig.Driver)
	require.Zero(t, scope.GetNumConnections())
}

func TestLoadDefaultConfigConnectionStringsIgnoresMissingConfig(t *testing.T) {
	scope := commands.NewScope()

	require.NoError(t, loadDefaultConfigConnectionStrings(scope, t.TempDir()))
	require.Zero(t, scope.GetNumConnections())
}

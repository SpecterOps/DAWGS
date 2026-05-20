package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRunCommandExecutesCommand(t *testing.T) {
	output, err := runCommand(context.Background(), nil, commands.NewScope(commands.RunModeREPL), t.TempDir(), []string{"help", "save-connections"})

	require.NoError(t, err)
	require.Contains(t, output, "HELP: save-connections")
}

func TestRunCommandWithOptionsDisablesStyledOutput(t *testing.T) {
	output, err := runCommandWithOptions(context.Background(), nil, commands.NewScope(commands.RunModeCLI), t.TempDir(), []string{"parse", "return", "1"}, runCommandOptions{
		styledOutputEnabled: false,
	})

	require.NoError(t, err)
	require.NotContains(t, output, "\x1b[")
	require.Contains(t, output, "RegularQuery")
}

func TestLoadDefaultConfigConnectionStrings(t *testing.T) {
	appConfigBaseDir := t.TempDir()
	require.NoError(t, (types.Config{
		Connections: map[string]types.ConnectionConfig{
			"local": {
				Driver:           "pg",
				ConnectionString: "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable",
			},
		},
	}).Save(filepath.Join(appConfigBaseDir, "config.json")))

	scope := commands.NewScope(commands.RunModeCLI)
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
	scope := commands.NewScope(commands.RunModeCLI)

	require.NoError(t, loadDefaultConfigConnectionStrings(scope, t.TempDir()))
	require.Zero(t, scope.GetNumConnections())
}

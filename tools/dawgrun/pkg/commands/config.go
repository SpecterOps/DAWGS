package commands

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
)

const configFileName = "config.json"

func loadConnectionsCmd() CommandDesc {
	return CommandDesc{
		args: []string{"[config-file]"},
		help: "Loads named backend connections from dawgrun config",
		desc: "Reads the connections object from config.json, then opens each named connection in the current session.",

		Fn: func(ctx *CommandContext, fields []string) error {
			configPath, err := resolveConfigPath(ctx, fields, "load-connections [config-file]")
			if err != nil {
				return err
			}

			config, err := types.LoadConfig(configPath)
			if err != nil {
				return err
			}

			if len(config.Connections) == 0 {
				fmt.Fprintf(ctx.output, "No connections found in %s\n", configPath)
				return nil
			}

			for _, connName := range slices.Sorted(maps.Keys(config.Connections)) {
				connConfig := config.Connections[connName]
				driverName, err := openConnection(ctx, connName, connConfig.ConnectionString, openConnectionOptions{
					driverName:       connConfig.Driver,
					defaultGraphName: "default",
					quiet:            true,
				})
				if err != nil {
					return fmt.Errorf("could not load connection %s from %s: %w", connName, configPath, err)
				}

				fmt.Fprintf(ctx.output, "Loaded %s connection '%s'\n", driverName, connName)
			}

			fmt.Fprintf(ctx.output, "Loaded %d connection(s) from %s\n", len(config.Connections), configPath)
			return nil
		},
	}
}

func saveConnectionsCmd() CommandDesc {
	return CommandDesc{
		args: []string{"[config-file]"},
		help: "Saves open named backend connections to dawgrun config",
		desc: "Writes the currently open connection names and connection strings to config.json.",

		Fn: func(ctx *CommandContext, fields []string) error {
			configPath, err := resolveConfigPath(ctx, fields, "save-connections [config-file]")
			if err != nil {
				return err
			}

			config := types.Config{
				Connections: ctx.scope.GetOpenConnectionConfigs(),
			}
			if err := config.Save(configPath); err != nil {
				return err
			}

			fmt.Fprintf(ctx.output, "Saved %d connection(s) to %s\n", len(config.Connections), configPath)
			return nil
		},
	}
}

func resolveConfigPath(ctx *CommandContext, fields []string, usage string) (string, error) {
	if len(fields) > 1 {
		return "", fmt.Errorf("invalid usage: %s", usage)
	}

	if len(fields) == 1 {
		return fields[0], nil
	}

	return filepath.Clean(ctx.defaultConfigPath()), nil
}

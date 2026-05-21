package commands

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
)

const configFileName = "config.json"

func loadConnectionsCmd() CommandDesc {
	return CommandDesc{
		args:             []string{"[config-file]"},
		help:             "Loads named backend connections from dawgrun config",
		desc:             "Reads the connections object from config.json, then opens each named connection in the current session.",
		DisallowRunModes: []RunMode{RunModeCLI},

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

			var connErrs error
			openedConns := 0
			for _, connName := range slices.Sorted(maps.Keys(config.Connections)) {
				connConfig := config.Connections[connName]
				driverName, err := openConnection(ctx, connName, connConfig.ConnectionString, openConnectionOptions{
					driverName:       connConfig.Driver,
					defaultGraphName: "default",
					quiet:            true,
				})
				if err != nil {
					connErrs = errors.Join(connErrs, fmt.Errorf("could not load connection %s from %s: %w", connName, configPath, err))
					continue
				}

				fmt.Fprintf(ctx.output, "Loaded %s connection '%s'\n", driverName, connName)
				openedConns += 1
			}

			fmt.Fprintf(ctx.output, "Loaded %d connection(s) from %s\n", openedConns, configPath)
			return connErrs
		},
	}
}

func saveConnectionsCmd() CommandDesc {
	return CommandDesc{
		args:             []string{"[config-file]"},
		help:             "Saves open named backend connections to dawgrun config",
		desc:             "Writes the currently open connection names and connection strings to config.json.",
		DisallowRunModes: []RunMode{RunModeCLI},

		Fn: func(ctx *CommandContext, fields []string) error {
			configPath, err := resolveConfigPath(ctx, fields, "save-connections [config-file]")
			if err != nil {
				return err
			}

			openConnectionConfigs := ctx.scope.GetOpenConnectionConfigs()
			if len(openConnectionConfigs) == 0 {
				if err := validateEmptyConnectionSave(configPath); err != nil {
					return err
				}
			}

			config := types.Config{
				Connections: openConnectionConfigs,
			}
			if err := config.Save(configPath); err != nil {
				return err
			}

			fmt.Fprintf(ctx.output, "Saved %d connection(s) to %s\n", len(config.Connections), configPath)
			return nil
		},
	}
}

func validateEmptyConnectionSave(configPath string) error {
	existingConfig, err := types.LoadConfig(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	if len(existingConfig.Connections) > 0 {
		return fmt.Errorf("refusing to overwrite %s with no open connections", configPath)
	}

	return nil
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

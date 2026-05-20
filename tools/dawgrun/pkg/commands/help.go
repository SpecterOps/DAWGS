package commands

import (
	"flag"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/texttools"
)

func getFlagSetHelpDetails(flagSet *flag.FlagSet) string {
	flagDefaults := new(strings.Builder)
	oldOutput := flagSet.Output()

	flagSet.SetOutput(flagDefaults)
	flagSet.PrintDefaults()
	flagSet.SetOutput(oldOutput)

	return flagDefaults.String()
}

func helpCmd() CommandDesc {
	return CommandDesc{
		args: []string{"[command]"},
		help: "This help message, but also more detailed help for individual commands",
		desc: "Get help for all commands",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) == 0 {
				// HELP OVERVIEW
				maxNameLength := 0
				for name := range cmdRegistry {
					if nameLen := len(name); nameLen > maxNameLength {
						maxNameLength = nameLen
					}
				}

				sortedCommands := slices.Sorted(maps.Keys(cmdRegistry))
				for _, name := range sortedCommands {
					cmd := cmdRegistry[name]
					if slices.Contains(cmd.DisallowRunModes, ctx.scope.GetRunMode()) {
						continue
					}

					commandLeft := fmt.Sprintf(
						"%s%s%s",
						strings.Repeat(" ", 4),
						name,
						strings.Repeat(" ", maxNameLength-(len(name))),
					)
					spacerPad := strings.Repeat(" ", len(commandLeft))
					fmt.Fprintf(ctx.output, "%s%s%s\n", commandLeft, spacerPad, cmd.help)
				}
			} else if strings.ToLower(fields[0]) == "all" {
				// ALL COMMAND HELP
				for name, cmd := range cmdRegistry {
					// skip commands that are disabled for this run mode
					if slices.Contains(cmd.DisallowRunModes, ctx.scope.GetRunMode()) {
						continue
					}

					fmt.Fprintf(ctx.output, "%s", renderHelp(name, cmd))
				}
			} else {
				// INDIVIDUAL COMMAND HELP
				name := fields[0]
				cmd, ok := cmdRegistry[name]
				if !ok || slices.Contains(cmd.DisallowRunModes, ctx.scope.GetRunMode()) {
					return fmt.Errorf("unknown command: %s", name)
				}

				fmt.Fprintf(ctx.output, "%s", renderHelp(name, cmd))
			}

			return nil
		},
	}
}

func renderHelp(name string, cmd CommandDesc) string {
	builder := new(strings.Builder)

	wrappedHelp := texttools.IndentLines(wordwrap.WrapString(cmd.help, 80), 1)
	fmt.Fprintf(builder, "\nHELP: %s %s\n\n", name, strings.Join(cmd.args, " "))
	fmt.Fprintf(builder, "%s\n", wrappedHelp)
	if strings.TrimSpace(cmd.desc) != "" {
		fmt.Fprint(builder, "\n")
		fmt.Fprintf(builder, "%s\n", texttools.IndentLines(wordwrap.WrapString(cmd.desc, 80), 1))
	}
	if cmd.flags != nil {
		flagDefaults := getFlagSetHelpDetails(cmd.flags)
		fmt.Fprintf(builder, "\n%s\n%s\n",
			texttools.IndentLines("flags:", 1),
			texttools.IndentLines(wordwrap.WrapString(flagDefaults, 80), 2),
		)
	}
	fmt.Fprint(builder, "END HELP\n")

	return builder.String()
}

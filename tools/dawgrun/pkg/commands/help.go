package commands

import (
	"flag"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/mitchellh/go-wordwrap"
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
					commandLeft := fmt.Sprintf(
						"%s%s%s",
						strings.Repeat(" ", 4),
						name,
						strings.Repeat(" ", maxNameLength-(len(name))),
					)
					cmd := cmdRegistry[name]
					spacerPad := strings.Repeat(" ", len(commandLeft))
					fmt.Fprintf(ctx.output, "%s%s%s\n", commandLeft, spacerPad, cmd.help)
				}
			} else {
				// COMMAND HELP
				name := fields[0]
				cmd, ok := cmdRegistry[name]
				if !ok {
					return fmt.Errorf("unknown command: %s", name)
				}

				wrappedHelp := indentLines(wordwrap.WrapString(cmd.help, 80), 1)
				fmt.Fprintf(ctx.output, "\nHELP: %s %s\n\n", name, strings.Join(cmd.args, " "))
				fmt.Fprintf(ctx.output, "%s\n", wrappedHelp)
				if strings.TrimSpace(cmd.desc) != "" {
					fmt.Fprint(ctx.output, "\n")
					fmt.Fprintf(ctx.output, "%s\n", indentLines(wordwrap.WrapString(cmd.desc, 80), 1))
				}
				if cmd.flags != nil {
					flagDefaults := getFlagSetHelpDetails(cmd.flags)
					fmt.Fprintf(ctx.output, "\n%s\n%s\n",
						indentLines("flags:", 1),
						indentLines(wordwrap.WrapString(flagDefaults, 80), 2),
					)
				}
				fmt.Fprint(ctx.output, "END HELP\n")
			}

			return nil
		},
	}
}

package commands

import (
	"fmt"
	"strings"
)

func quitCmd() CommandDesc {
	return CommandDesc{
		args: []string{},
		help: "Quit",
		desc: "Exits the REPL session",

		Fn: func(ctx *CommandContext, fields []string) error {
			ctx.instance.Quit()
			return nil
		},
	}
}

func runtimeTraceCmd() CommandDesc {
	usage := "runtime-trace <start|stop> [tracefile]"

	return CommandDesc{
		args: []string{"start|stop", "[tracefile]"},
		help: "Manage runtime tracing",
		desc: `start [tracefile] - Start tracing with output to [tracefile] if provided, otherwise trace.out
stop - Stop runtime tracing and close the trace file`,

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) == 0 {
				return fmt.Errorf("invalid usage: %s", usage)
			}
			subcmd := strings.ToLower(fields[0])
			switch subcmd {
			case "start":
				var traceOut string
				if len(fields) > 1 {
					traceOut = fields[1]
				}

				result, err := ctx.session.StartRuntimeTrace(traceOut)
				if err != nil {
					return err
				}

				fmt.Fprintf(ctx.output, "Started runtime tracing to %s", result.Path)
				return nil
			case "stop":
				if err := ctx.session.StopRuntimeTrace(); err != nil {
					return err
				}

				fmt.Fprint(ctx.output, "Stopped runtime tracing")
				return nil
			default:
				return fmt.Errorf("invalid usage: %s", usage)
			}
		},
	}
}

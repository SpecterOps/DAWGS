package commands

import (
	"fmt"
	"os"
	"runtime/trace"
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
	state := make(map[string]any)
	state["run"] = false
	state["tracefile"] = nil

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
				if running, ok := state["run"].(bool); ok && running {
					return fmt.Errorf("runtime tracing is already enabled")
				}

				var traceOut string
				if len(fields) > 1 {
					traceOut = fields[1]
				} else {
					traceOut = "trace.out"
				}

				traceFile, err := os.Create(traceOut)
				if err != nil {
					return fmt.Errorf("error creating tracefile: %w", err)
				}

				if err := trace.Start(traceFile); err != nil {
					return fmt.Errorf("could not start tracing: %w", err)
				}

				state["run"] = true
				state["tracefile"] = traceFile

				fmt.Fprintf(ctx.output, "Started runtime tracing to %s", traceOut)
				return nil
			case "stop":
				if running, ok := state["run"].(bool); ok && !running {
					return fmt.Errorf("runtime tracing is not running")
				}

				trace.Stop()
				traceFile, ok := state["tracefile"].(*os.File)
				if !ok {
					return fmt.Errorf("could not get open tracing file")
				}

				traceFile.Close()

				state["run"] = true
				state["tracefile"] = nil

				fmt.Fprint(ctx.output, "Stopped runtime tracing")
				return nil
			default:
				return fmt.Errorf("invalid usage: %s", usage)
			}
		},
	}
}

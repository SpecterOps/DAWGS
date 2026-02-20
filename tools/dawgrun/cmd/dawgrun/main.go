package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"path"
	"runtime/trace"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/google/shlex"
	repl "github.com/openengineer/go-repl"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
)

var (
	//go:embed art.txt
	art string

	//go:embed banner.txt
	banner string

	bannerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#87dc70")).
			Background(lipgloss.Color("#533d25"))
)

func main() {
	fmt.Printf("\n%s\n%s",
		art,
		bannerStyle.Render(banner),
	)
	fmt.Printf("\n\n\n")
	fmt.Printf(" ::: DAWGRUN REPL ::: Type 'help' for more info\n")

	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		panic(fmt.Errorf("could not get user config dir: %w", err))
	}

	// TODO: Someday, also load a config file from here for highlighting color scheme, colors enablement, etc
	appConfigBaseDir := path.Join(userConfigDir, "dawgrun")
	if err := os.MkdirAll(appConfigBaseDir, 0o750); err != nil {
		panic(fmt.Errorf("could not create dawgrun config dir: %w", err))
	}

	handler := new(handler)
	handler.cmdScope = commands.NewScope()
	handler.r = repl.NewRepl(handler, &repl.Options{
		HistoryFilePath: path.Join(appConfigBaseDir, "history.txt"),
		HistoryMaxLines: 1000,
		StatusWidgets: &repl.StatusWidgetFns{
			Right: makeConnectionsStatusWidget(handler.cmdScope),
		},
	})

	if err := handler.r.Loop(); err != nil {
		slog.Error("repl encountered error: %w", slog.String("error", err.Error()))
	}
}

var _ repl.Handler = (*handler)(nil)

type handler struct {
	r        *repl.Repl
	cmdScope *commands.Scope
}

func (h *handler) Prompt() string {
	return "dawgrun > "
}

func (h *handler) Tab(buffer string) string {
	return ""
}

func (h *handler) Eval(line string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fields, err := shlex.Split(line)
	if err != nil {
		return fmt.Sprintf("Unparseable command: '%s': %s", line, err)
	}

	if len(fields) == 0 {
		return "Woof!"
	}

	command := strings.ToLower(fields[0])
	rest := fields[1:]
	if cmd, ok := commands.Registry()[command]; ok {
		cmdCtx := commands.NewCommandContext(ctx, h.r, h.cmdScope)
		defer trace.StartRegion(cmdCtx, fmt.Sprintf("command-%s", command)).End()
		err := cmd.Fn(cmdCtx, rest)
		if err != nil {
			return fmt.Sprintf("%s failed: %v", command, err)
		}

		out := cmdCtx.OutputString()
		if !strings.HasSuffix(out, "\n") {
			return out + "\n"
		} else {
			return out
		}
	}

	return fmt.Sprintf("Unknown command %s; try `help`?", command)
}

func makeConnectionsStatusWidget(cmdScope *commands.Scope) repl.StatusWidgetFn {
	return func(r *repl.Repl) string {
		if numConns := cmdScope.NumConns(); numConns == 0 {
			return "No connections"
		} else {
			return fmt.Sprintf("%d connection(s)", numConns)
		}
	}
}

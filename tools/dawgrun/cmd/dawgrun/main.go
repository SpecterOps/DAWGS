package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/trace"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/shlex"
	repl "github.com/specterops/go-repl"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
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
	// Configure spew's display options
	spew.Config.DisablePointerAddresses = true

	appConfigBaseDir, err := ensureAppConfigBaseDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	// CLI mode if args, otherwise interactive
	if len(os.Args) > 1 {
		os.Exit(cliMain(appConfigBaseDir, os.Args[1:]))
	} else {
		replMain(appConfigBaseDir)
	}
}

func cliMain(appConfigBaseDir string, args []string) int {
	scope := commands.NewScope()
	defer func() {
		if err := scope.CloseConnections(context.Background()); err != nil {
			slog.Error("could not close cli connections", slog.String("error", err.Error()))
		}
	}()

	if err := loadDefaultConfigConnectionStrings(scope, appConfigBaseDir); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return 1
	}

	output, err := runCommand(context.Background(), nil, scope, appConfigBaseDir, args)
	if output != "" {
		fmt.Print(output)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return 1
	}

	return 0
}

func replMain(appConfigBaseDir string) {
	fmt.Printf("\n%s\n%s",
		art,
		bannerStyle.Render(banner),
	)
	fmt.Printf("\n\n\n")
	fmt.Printf(" ::: DAWGRUN REPL ::: Type 'help' for more info\n")

	handler := new(handler)
	handler.appConfigBaseDir = appConfigBaseDir
	handler.cmdScope = commands.NewScope()
	handler.r = repl.NewRepl(handler, &repl.Options{
		HistoryFilePath: filepath.Join(appConfigBaseDir, "history.txt"),
		HistoryMaxLines: 1000,
		StatusWidgets: &repl.StatusWidgetFns{
			Right: makeConnectionsStatusWidget(handler.cmdScope),
		},
	})

	if err := handler.r.Loop(); err != nil {
		slog.Error("repl encountered error", slog.String("error", err.Error()))
	}
}

func ensureAppConfigBaseDir() (string, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("could not get user config dir: %w", err)
	}

	appConfigBaseDir := filepath.Join(userConfigDir, "dawgrun")
	if err := os.MkdirAll(appConfigBaseDir, 0o750); err != nil {
		return "", fmt.Errorf("could not create dawgrun config dir: %w", err)
	}

	return appConfigBaseDir, nil
}

func loadDefaultConfigConnectionStrings(scope *commands.Scope, appConfigBaseDir string) error {
	configPath := types.DefaultConfigPath(appConfigBaseDir)
	config, err := types.LoadConfig(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	scope.SetConnectionConfigs(config.Connections)
	return nil
}

var (
	_ repl.Handler   = (*handler)(nil)
	_ repl.Completer = (*handler)(nil)
)

type handler struct {
	r                *repl.Repl
	cmdScope         *commands.Scope
	appConfigBaseDir string
}

func (h *handler) Prompt() string {
	return "dawgrun > "
}

func (h *handler) Tab(buffer string) string {
	return ""
}

func (h *handler) Complete(buffer string) repl.Completion {
	prefix, ok := commandCompletionPrefix(buffer)
	if !ok {
		return repl.Completion{}
	}

	prefix = strings.ToLower(prefix)
	commandNames := commands.SortedCommandNames()
	matches := make([]string, 0, len(commandNames))

	for _, commandName := range commandNames {
		if strings.HasPrefix(commandName, prefix) {
			matches = append(matches, commandName)
		}
	}

	if len(matches) == 0 {
		return repl.Completion{}
	}

	if len(matches) == 1 {
		match := matches[0]
		if match == prefix {
			return repl.Completion{Insert: " "}
		}

		return repl.Completion{Insert: match[len(prefix):] + " "}
	}

	return repl.Completion{
		Message:    "Commands:",
		Candidates: matches,
	}
}

func commandCompletionPrefix(buffer string) (string, bool) {
	trimmed := strings.TrimLeft(buffer, " \t")
	if trimmed == "" {
		return "", true
	}

	if strings.HasSuffix(trimmed, " ") || strings.HasSuffix(trimmed, "\t") {
		return "", false
	}

	fields := strings.Fields(trimmed)
	if len(fields) != 1 {
		return "", false
	}

	return fields[0], true
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

	output, err := runCommand(ctx, h.r, h.cmdScope, h.appConfigBaseDir, fields)
	if err != nil {
		return err.Error()
	}

	return output
}

func runCommand(ctx context.Context, instance *repl.Repl, scope *commands.Scope, appConfigBaseDir string, fields []string) (string, error) {
	if len(fields) == 0 {
		return "", fmt.Errorf("no command provided")
	}

	command := strings.ToLower(fields[0])
	rest := fields[1:]
	if cmd, ok := commands.Registry()[command]; ok {
		cmdCtx := commands.NewCommandContext(ctx, instance, scope, appConfigBaseDir)
		defer trace.StartRegion(cmdCtx, fmt.Sprintf("command-%s", command)).End()
		err := cmd.Fn(cmdCtx, rest)
		// Reset command's associated flags after exec
		defer func() {
			if cmd.ClearFlagsFn != nil {
				cmd.ClearFlagsFn()
			}
		}()
		if err != nil {
			return "", fmt.Errorf("%s failed: %w", command, err)
		}

		out := cmdCtx.OutputString()
		if out != "" && !strings.HasSuffix(out, "\n") {
			return out + "\n", nil
		}

		return out, nil
	}

	return "", fmt.Errorf("unknown command %s; try `help`?", command)
}

func makeConnectionsStatusWidget(cmdScope *commands.Scope) repl.StatusWidgetFn {
	return func(r *repl.Repl) string {
		if numConns := cmdScope.GetNumConnections(); numConns == 0 {
			return "No connections"
		} else {
			return fmt.Sprintf("%d connection(s)", numConns)
		}
	}
}

package commands

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"golang.org/x/term"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/session"
)

const (
	queryCypherOutputFormatTable = "table"
	queryCypherOutputFormatJSON  = "json"
)

func parseCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<...query>"},
		help: "Parses and dumps a Cypher query to AST form.",

		Fn: func(ctx *CommandContext, fields []string) error {
			result, err := ctx.session.ParseCypher(strings.Join(fields, " "))
			if err != nil {
				return fmt.Errorf("error trying to parse query '%s': %w", fields, err)
			}

			ctx.output.WriteHighlighted(result["ast"], "golang")
			return nil
		},
	}
}

func translateToPsqlCmd() CommandDesc {
	flagSet := flag.NewFlagSet("translate-psql", flag.ContinueOnError)

	var (
		kindMapperConnRef = ""
		dumpTranslatedAst = false
	)

	flagSet.StringVar(&kindMapperConnRef, "conn", "", "Connection reference for choosing a kind mapper")
	flagSet.BoolVar(&dumpTranslatedAst, "dump-pg-ast", false, "Whether to dump the translator's constructed AST")

	return CommandDesc{
		args:  []string{"[flags]", "<...query>"},
		help:  "Parses a query and converts it to the underlying PostgreSQL query",
		desc:  "Does a bunch of magic to fully translate a Cypher query into a PostgreSQL query",
		flags: flagSet,

		ClearFlagsFn: func() {
			kindMapperConnRef = ""
			dumpTranslatedAst = false
		},
		Fn: func(ctx *CommandContext, fields []string) error {
			if err := flagSet.Parse(fields); err != nil {
				return fmt.Errorf("could not parse flags: %w", err)
			}

			fields = flagSet.Args()
			result, err := ctx.session.TranslateCypherToPGSQL(ctx, strings.Join(fields, " "), kindMapperConnRef, dumpTranslatedAst)
			if err != nil {
				return fmt.Errorf("could not translate cypher query to pgsql: %w", err)
			}
			if dumpTranslatedAst {
				fmt.Fprintf(ctx.output, "TRANSLATOR AST\n\n")
				ctx.output.WriteHighlighted(result.PGAst, "golang")
				fmt.Fprintf(ctx.output, "\n")
			}

			ctx.output.WriteHighlighted(result.SQL, "postgres")
			return nil
		},
	}
}

func explainAsPsqlCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<conn>", "<...query>"},
		help: "Explains a translated query over an active PG connection",
		desc: "Asks the PG query planner to explain the (translated) Cypher query in PG terms",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage, requires: <connection name> <query>")
			}

			connName := fields[0]
			result, err := ctx.session.ExplainCypherPGSQL(ctx, connName, strings.Join(fields[1:], " "))
			if err != nil {
				return fmt.Errorf("could not run EXPLAIN query: %w", err)
			}

			ctx.output.WriteHighlighted(result.SQL, "postgres")
			fmt.Fprint(ctx.output, "\n\n")
			for _, planLine := range result.Plan {
				fmt.Fprintf(ctx.output, "  %s\n", planLine)
			}

			return nil
		},
	}
}

func queryCypherCmd() CommandDesc {
	flagSet := flag.NewFlagSet("query-cypher", flag.ContinueOnError)

	outputFormat := queryCypherOutputFormatTable
	flagSet.StringVar(&outputFormat, "format", queryCypherOutputFormatTable, "Output format: table or json")

	return CommandDesc{
		args:  []string{"[flags]", "<conn>", "<...query>"},
		help:  "Executes a Cypher query and renders table or JSON output",
		desc:  "Runs a Cypher query over an active backend connection and prints fetched rows",
		flags: flagSet,

		ClearFlagsFn: func() {
			outputFormat = queryCypherOutputFormatTable
		},
		Fn: func(ctx *CommandContext, fields []string) error {
			outputFormat = queryCypherOutputFormatTable
			if err := flagSet.Parse(fields); err != nil {
				return fmt.Errorf("could not parse flags: %w", err)
			}

			fields = flagSet.Args()
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage, requires: <connection name> <query>")
			}

			outputFormat = strings.ToLower(strings.TrimSpace(outputFormat))
			switch outputFormat {
			case queryCypherOutputFormatTable, queryCypherOutputFormatJSON:
			default:
				return fmt.Errorf("invalid output format %q; expected one of: %s, %s", outputFormat, queryCypherOutputFormatTable, queryCypherOutputFormatJSON)
			}

			connName := fields[0]
			result, err := ctx.session.QueryCypher(ctx, connName, strings.Join(fields[1:], " "))
			if err != nil {
				return err
			}

			switch outputFormat {
			case queryCypherOutputFormatJSON:
				return queryCypherSessionOutputJSON(ctx, result)
			case queryCypherOutputFormatTable:
				return queryCypherSessionOutputTable(ctx, result)
			}

			return fmt.Errorf("unknown output format: %s", outputFormat)
		},
	}
}

func queryCypherSessionOutputJSON(ctx *CommandContext, result session.QueryResult) error {
	rowOutput, err := json.MarshalIndent(result.Rows, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshalling JSON rows: %w", err)
	}

	fmt.Fprintln(ctx.output, string(rowOutput))
	return nil
}

func queryCypherSessionOutputTable(ctx *CommandContext, result session.QueryResult) error {
	if result.RowCount == 0 {
		fmt.Fprint(ctx.output, "(0 rows)\n")
		return nil
	}

	outputTable := table.NewWriter()
	style := table.StyleRounded
	style.Options.SeparateRows = true
	style.Size.WidthMax = cypherResultTableWidth()
	outputTable.SetStyle(style)
	outputTable.AppendHeader(buildCypherResultHeader(result.Columns))
	outputTable.SetColumnConfigs(buildCypherResultColumnConfigs(len(result.Columns), cypherResultTableWidth()))

	for _, row := range result.Rows {
		outputTable.AppendRow(buildSessionCypherResultRow(result.Columns, row))
	}

	fmt.Fprint(ctx.output, outputTable.Render())
	fmt.Fprintf(ctx.output, "\n(%d rows)\n", result.RowCount)
	return nil
}

func buildSessionCypherResultRow(columns []string, values map[string]any) table.Row {
	row := make(table.Row, len(columns))
	for idx, column := range columns {
		row[idx] = formatCypherResultValue(values[column])
	}

	return row
}

func cypherResultTableWidth() int {
	const (
		fallbackWidth = 120
	)

	if width, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		return width - 2
	}

	return fallbackWidth
}

func buildCypherResultColumnConfigs(columnCount, tableWidth int) []table.ColumnConfig {
	if columnCount == 0 {
		return nil
	}

	const (
		minColumnWidth = 12
		innerPadding   = 3
	)

	availableWidth := tableWidth - 1 - (columnCount * innerPadding)
	columnWidth := availableWidth / columnCount
	if columnWidth < minColumnWidth {
		columnWidth = minColumnWidth
	}

	configs := make([]table.ColumnConfig, 0, columnCount)
	for idx := 0; idx < columnCount; idx++ {
		configs = append(configs, table.ColumnConfig{
			Number:           idx + 1,
			WidthMax:         columnWidth,
			WidthMaxEnforcer: text.WrapHard,
		})
	}

	return configs
}

func buildCypherResultHeader(columns []string) table.Row {
	row := make(table.Row, len(columns))
	for idx, key := range columns {
		row[idx] = key
	}

	return row
}

func formatCypherResultValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return "<nil>"
	case bool,
		int,
		int8,
		int16,
		int32,
		int64,
		uint,
		uint8,
		uint16,
		uint32,
		uint64,
		float32,
		float64,
		string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	default:
		if marshaled, err := json.Marshal(typed); err == nil {
			return string(marshaled)
		}

		return fmt.Sprintf("%v", typed)
	}
}

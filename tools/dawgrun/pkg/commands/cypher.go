package commands

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/kanmu/go-sqlfmt/sqlfmt"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"golang.org/x/term"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
)

func parseCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<...query>"},
		help: "Parses and dumps a Cypher query to AST form.",

		Fn: func(ctx *CommandContext, fields []string) error {
			query, err := parseQueryArray(fields)
			if err != nil {
				return fmt.Errorf("error trying to parse query '%s': %w", fields, err)
			}

			ctx.output.WriteHighlighted(spew.Sdump(query), "golang", "monokai")
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

		Fn: func(ctx *CommandContext, fields []string) error {
			if err := flagSet.Parse(fields); err != nil {
				return fmt.Errorf("could not parse flags: %w", err)
			}

			fields = flagSet.Args()
			query, err := parseQueryArray(fields)
			if err != nil {
				return fmt.Errorf("error trying to parse query '%s': %w", fields, err)
			}

			kindMapper := stubs.EmptyMapper()
			if kindMapperConnRef != "" {
				// Fetch kinds regardless of if it's already loaded.
				kindMap, err := loadKindMap(ctx, kindMapperConnRef)
				if err != nil {
					return fmt.Errorf("could not load kind map for explain: %w", err)
				}
				kindMapper = stubs.MapperFromKindMap(kindMap)
			}

			result, err := translate.Translate(ctx, query, kindMapper, nil)
			if err != nil {
				return fmt.Errorf("could not translate cypher query to pgsql: %w", err)
			}
			if dumpTranslatedAst {
				fmt.Fprintf(ctx.output, "TRANSLATOR AST\n\n")
				ctx.output.WriteHighlighted(spew.Sdump(result.Statement), "golang", "monokai")
				fmt.Fprintf(ctx.output, "\n")
			}

			// Certain queries will materialize parameters into the output when translated, so we need to build
			// an OutputBuilder so we can carry forward those params.
			queryBuilder := format.NewOutputBuilder()
			if result.Parameters != nil {
				queryBuilder.WithMaterializedParameters(result.Parameters)
			}

			sqlQuery, err := format.Statement(result.Statement, queryBuilder)
			if err != nil {
				return fmt.Errorf("could not format translated statement into a string query: %w", err)
			}

			formattedQuery, err := sqlfmt.Format(sqlQuery, &sqlfmt.Options{
				Distance: 0,
			})
			if err != nil {
				ctx.output.Warnf("could not format query: %s", err.Error())
				formattedQuery = sqlQuery
			}

			ctx.output.WriteHighlighted(formattedQuery, "postgres", "monokai")
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
			conn, ok := ctx.scope.connections[connName]
			if !ok {
				return fmt.Errorf("connection %s not found; did you `open` it?", connName)
			}

			// Fetch kinds regardless of if it's already loaded.
			kindMap, err := loadKindMap(ctx, connName)
			if err != nil {
				return fmt.Errorf("could not load kind map for explain: %w", err)
			}

			query, err := parseQueryArray(fields[1:])
			if err != nil {
				return fmt.Errorf("could not parse query: %w", err)
			}

			// Populate a DumbKindMapper from the database's kinds table
			kindMapper := stubs.MapperFromKindMap(kindMap)
			result, err := translate.Translate(ctx, query, kindMapper, nil)
			if err != nil {
				return fmt.Errorf("could not translate cypher query to pgsql: %w", err)
			}

			// Certain queries will materialize parameters into the output when translated, so we need to build
			// an OutputBuilder so we can carry forward those params.
			queryBuilder := format.NewOutputBuilder()
			if result.Parameters != nil {
				queryBuilder.WithMaterializedParameters(result.Parameters)
			}

			sqlQuery, err := format.Statement(result.Statement, queryBuilder)
			if err != nil {
				return fmt.Errorf("could not format translated statement into a string query: %w", err)
			}

			formattedQuery, err := sqlfmt.Format(sqlQuery, &sqlfmt.Options{
				Distance: 2,
			})
			if err != nil {
				ctx.output.Warnf("could not format query: %s", err.Error())
				formattedQuery = sqlQuery
			}
			explainSQLQuery := fmt.Sprintf("EXPLAIN %s", formattedQuery)
			ctx.output.WriteHighlighted(explainSQLQuery, "postgres", "monokai")
			fmt.Fprint(ctx.output, "\n\n")

			err = conn.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result := tx.Raw(explainSQLQuery, nil)
				if err := result.Error(); err != nil {
					return fmt.Errorf("error running raw query: '%s': %w", explainSQLQuery, err)
				}
				defer result.Close()

				var value string
				for result.Next() {
					graph.ScanNextResult(result, &value)
					fmt.Fprintf(ctx.output, "  %s\n", value)
				}

				return nil
			})
			if err != nil {
				return fmt.Errorf("could not run EXPLAIN query: %w", err)
			}

			return nil
		},
	}
}

func queryCypherCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<conn>", "<...query>"},
		help: "Executes a Cypher query and renders results in a table",
		desc: "Runs a Cypher query over an active backend connection and pretty-prints fetched rows",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage, requires: <connection name> <query>")
			}

			connName := fields[0]
			conn, ok := ctx.scope.connections[connName]
			if !ok {
				return fmt.Errorf("connection %s not found; did you `open` it?", connName)
			}

			cypherQuery := strings.Join(fields[1:], " ")

			return conn.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result := tx.Query(cypherQuery, nil)
				if err := result.Error(); err != nil {
					return fmt.Errorf("error running cypher query '%s': %w", cypherQuery, err)
				}
				defer result.Close()

				outputTable := table.NewWriter()
				style := table.StyleRounded
				style.Options.SeparateRows = true
				style.Size.WidthMax = cypherResultTableWidth()
				outputTable.SetStyle(style)

				rowCount := 0
				for result.Next() {
					values := result.Values()

					if rowCount == 0 {
						headers := buildCypherResultHeader(result.Keys(), len(values))
						outputTable.AppendHeader(headers)
						outputTable.SetColumnConfigs(buildCypherResultColumnConfigs(len(headers), style.Size.WidthMax))
					}

					outputTable.AppendRow(buildCypherResultRow(values))
					rowCount++
				}

				if err := result.Error(); err != nil {
					return fmt.Errorf("error fetching query rows: %w", err)
				}

				if rowCount == 0 {
					fmt.Fprint(ctx.output, "(0 rows)\n")
					return nil
				}

				fmt.Fprint(ctx.output, outputTable.Render())
				fmt.Fprintf(ctx.output, "\n(%d rows)\n", rowCount)
				return nil
			})
		},
	}
}

func cypherResultTableWidth() int {
	const (
		fallbackWidth = 120
		minWidth      = 40
	)

	if width, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		width -= 2
		if width >= minWidth {
			return width
		}
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

func buildCypherResultHeader(keys []string, numValues int) table.Row {
	columns := append([]string{}, keys...)

	if len(columns) < numValues {
		for idx := len(columns); idx < numValues; idx++ {
			columns = append(columns, fmt.Sprintf("column_%d", idx+1))
		}
	}

	header := make(table.Row, len(columns))
	for idx, key := range columns {
		header[idx] = key
	}

	return header
}

func buildCypherResultRow(values []any) table.Row {
	row := make(table.Row, len(values))
	for idx, value := range values {
		row[idx] = formatCypherResultValue(value)
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

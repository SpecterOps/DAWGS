package commands

import (
	"flag"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/kanmu/go-sqlfmt/sqlfmt"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"

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

			sqlQuery, err := format.SyntaxNode(result.Statement)
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

			sqlQuery, err := format.SyntaxNode(result.Statement)
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

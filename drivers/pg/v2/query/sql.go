package query

import (
	"embed"
	"fmt"
	"path"
	"strings"
)

var (
	//go:embed sql
	queryFS embed.FS
)

func stripSQLComments(multiLineContent string) string {
	builder := strings.Builder{}

	for _, line := range strings.Split(multiLineContent, "\n") {
		trimmedLine := strings.TrimSpace(line)

		// Strip empty and SQL comment lines
		if len(trimmedLine) == 0 || strings.HasPrefix(trimmedLine, "--") {
			continue
		}

		builder.WriteString(trimmedLine)
		builder.WriteString("\n")
	}

	return builder.String()
}

func readFile(name string) string {
	if content, err := queryFS.ReadFile(name); err != nil {
		panic(fmt.Sprintf("Unable to find embedded query file %s: %v", name, err))
	} else {
		return stripSQLComments(string(content))
	}
}

func loadSQL(name string) string {
	return readFile(path.Join("sql", name))
}

var (
	sqlSchemaUp           = loadSQL("schema_up.sql")
	sqlSchemaDown         = loadSQL("schema_down.sql")
	sqlSelectTableIndexes = loadSQL("select_table_indexes.sql")
	sqlSelectKindID       = loadSQL("select_table_indexes.sql")
	sqlSelectGraphs       = loadSQL("select_graphs.sql")
	sqlInsertGraph        = loadSQL("insert_graph.sql")
	sqlInsertKind         = loadSQL("insert_or_get_kind.sql")
	sqlSelectKinds        = loadSQL("select_kinds.sql")
	sqlSelectGraphByName  = loadSQL("select_graph_by_name.sql")
)

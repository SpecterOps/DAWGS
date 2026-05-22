package texttools

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/specterops/dawgs/graph"
)

func CypherOutputJSON(output io.Writer, result graph.Result) error {
	var outputColumns []string

	fmt.Fprint(output, "[\n")

	rowCount := 0
	for result.Next() {
		values := result.Values()
		insertFormat := ",\n    %s"

		if rowCount == 0 {
			outputColumns = buildCypherResultColumns(result.Keys(), len(values))
			insertFormat = "    %s"
		}

		rowOutput, err := json.MarshalIndent(buildCypherResultJSONRow(outputColumns, values), "    ", "    ")
		if err != nil {
			return fmt.Errorf("error marshalling JSON row: %v: %w", values, err)
		}

		fmt.Fprintf(output, insertFormat, rowOutput)
		rowCount++
	}

	if err := result.Error(); err != nil {
		return fmt.Errorf("error fetching query rows: %w", err)
	}

	fmt.Fprint(output, "\n]\n")

	return nil
}

func CypherOutputTable(output io.Writer, result graph.Result) error {
	var (
		outputColumns []string
		outputTable   table.Writer
	)

	outputTable = table.NewWriter()
	style := table.StyleRounded
	style.Options.SeparateRows = true
	style.Size.WidthMax = cypherResultTableWidth()
	outputTable.SetStyle(style)

	rowCount := 0
	for result.Next() {
		values := result.Values()

		if rowCount == 0 {
			outputColumns = buildCypherResultColumns(result.Keys(), len(values))

			outputTable.AppendHeader(buildCypherResultHeader(outputColumns))
			outputTable.SetColumnConfigs(buildCypherResultColumnConfigs(len(outputColumns), cypherResultTableWidth()))
		}

		outputTable.AppendRow(buildCypherResultRow(values))
		rowCount++
	}

	if err := result.Error(); err != nil {
		return fmt.Errorf("error fetching query rows: %w", err)
	}

	if rowCount == 0 {
		fmt.Fprint(output, "(0 rows)\n")
		return nil
	}

	fmt.Fprint(output, outputTable.Render())
	fmt.Fprintf(output, "\n(%d rows)\n", rowCount)

	return nil
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

func buildCypherResultColumns(keys []string, numValues int) []string {
	columns := append([]string{}, keys...)

	if len(columns) < numValues {
		for idx := len(columns); idx < numValues; idx++ {
			columns = append(columns, fmt.Sprintf("column_%d", idx+1))
		}
	}

	return columns
}

func buildCypherResultHeader(columns []string) table.Row {
	row := make(table.Row, len(columns))
	for idx, key := range columns {
		row[idx] = key
	}

	return row
}

func buildCypherResultJSONRow(columns []string, values []any) map[string]any {
	row := make(map[string]any, len(columns))
	for idx, key := range columns {
		if idx < len(values) {
			row[key] = formatCypherResultJSONValue(values[idx])
		} else {
			row[key] = nil
		}
	}

	return row
}

func formatCypherResultJSONValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
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
	case graph.ID:
		return typed.Uint64()
	case []byte:
		return string(typed)
	default:
		if marshaled, err := json.Marshal(typed); err == nil {
			var normalized any
			if err := json.Unmarshal(marshaled, &normalized); err == nil {
				return normalized
			}
		}

		return fmt.Sprintf("%v", typed)
	}
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

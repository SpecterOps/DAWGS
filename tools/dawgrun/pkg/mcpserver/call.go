package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/session"
)

func (s *Server) callTool(params []byte) callToolResult {
	var request callToolRequest
	if err := json.Unmarshal(params, &request); err != nil {
		return errorToolResult(fmt.Sprintf("invalid tools/call params: %v", err))
	}

	if request.Name == "" {
		return errorToolResult("missing tool name")
	}

	tool, ok := toolByName(request.Name)
	if !ok {
		return errorToolResult(fmt.Sprintf("unknown dawgrun tool %q", request.Name))
	}

	if tool.WriteCapable && !s.options.AllowWrites {
		return errorToolResult(fmt.Sprintf("tool %q is write-capable and is disabled; restart dawgrun-mcp with --allow-writes", request.Name))
	}

	result, err := s.executeTool(context.Background(), request.Name, request.Arguments)
	if err != nil {
		return errorToolResult(err.Error())
	}
	if formatted, ok := result.(formattedToolResult); ok {
		return successStructuredTextToolResult(formatted.Structured, formatted.Text)
	}

	return successJSONToolResult(result)
}

type formattedToolResult struct {
	Structured any
	Text       string
}

func (s *Server) executeTool(ctx context.Context, name string, args map[string]any) (any, error) {
	switch name {
	case "open_connection":
		connectionName, err := requiredString(args, "name")
		if err != nil {
			return nil, err
		}
		connectionString, err := requiredString(args, "connection_string")
		if err != nil {
			return nil, err
		}

		return s.session.OpenConnection(ctx, session.OpenConnectionRequest{
			Name:             connectionName,
			ConnectionString: connectionString,
			Driver:           optionalString(args, "driver"),
		})
	case "list_connections":
		return map[string]any{"connections": s.session.ListConnections()}, nil
	case "close_connection":
		connectionName, err := requiredString(args, "connection")
		if err != nil {
			return nil, err
		}
		if err := s.session.CloseConnection(ctx, connectionName); err != nil {
			return nil, err
		}

		return map[string]any{"closed": connectionName}, nil
	case "load_kind_map":
		connectionName, err := requiredString(args, "connection")
		if err != nil {
			return nil, err
		}

		kindMap, err := s.session.LoadKindMap(ctx, connectionName)
		if err != nil {
			return nil, err
		}

		return map[string]any{"connection": connectionName, "kind_map": kindMap}, nil
	case "lookup_kind":
		connectionName, kindName, err := requiredConnectionAndString(args, "kind_name")
		if err != nil {
			return nil, err
		}

		return s.session.LookupKind(ctx, connectionName, kindName)
	case "lookup_kind_id":
		connectionName, err := requiredString(args, "connection")
		if err != nil {
			return nil, err
		}
		kindID, err := requiredInt16(args, "kind_id")
		if err != nil {
			return nil, err
		}

		return s.session.LookupKindID(ctx, connectionName, kindID)
	case "parse_cypher":
		query, err := requiredString(args, "query")
		if err != nil {
			return nil, err
		}

		return s.session.ParseCypher(query)
	case "translate_cypher_to_pgsql":
		query, err := requiredString(args, "query")
		if err != nil {
			return nil, err
		}

		return s.session.TranslateCypherToPGSQL(ctx, query, optionalString(args, "connection"), optionalBool(args, "dump_pg_ast"))
	case "explain_cypher_pgsql":
		connectionName, query, err := requiredConnectionAndString(args, "query")
		if err != nil {
			return nil, err
		}

		return s.session.ExplainCypherPGSQL(ctx, connectionName, query)
	case "query_cypher":
		connectionName, query, err := requiredConnectionAndString(args, "query")
		if err != nil {
			return nil, err
		}
		format := optionalString(args, "format")
		switch strings.ToLower(strings.TrimSpace(format)) {
		case "", "json", "table":
		default:
			return nil, fmt.Errorf("unsupported format %q; expected json or table", format)
		}

		result, err := s.session.QueryCypher(ctx, connectionName, query)
		if err != nil {
			return nil, err
		}

		return formattedToolResult{
			Structured: result,
			Text:       formatQueryResultText(result, format),
		}, nil
	case "save_opengraph":
		connectionName, err := requiredString(args, "connection")
		if err != nil {
			return nil, err
		}

		return s.session.SaveOpenGraph(ctx, connectionName, optionalString(args, "output_path"))
	case "load_opengraph":
		connectionName, path, err := requiredConnectionAndString(args, "path")
		if err != nil {
			return nil, err
		}

		return s.session.LoadOpenGraph(ctx, connectionName, path)
	case "copy_opengraph":
		source, err := requiredString(args, "source")
		if err != nil {
			return nil, err
		}
		target, err := requiredString(args, "target")
		if err != nil {
			return nil, err
		}

		return s.session.CopyOpenGraph(ctx, source, target)
	case "start_runtime_trace":
		return s.session.StartRuntimeTrace(optionalString(args, "path"))
	case "stop_runtime_trace":
		if err := s.session.StopRuntimeTrace(); err != nil {
			return nil, err
		}

		return map[string]any{"stopped": true}, nil
	default:
		return nil, fmt.Errorf("tool %q is registered but has no implementation", name)
	}
}

func requiredConnectionAndString(args map[string]any, field string) (string, string, error) {
	connectionName, err := requiredString(args, "connection")
	if err != nil {
		return "", "", err
	}
	value, err := requiredString(args, field)
	if err != nil {
		return "", "", err
	}

	return connectionName, value, nil
}

func formatQueryResultText(result session.QueryResult, format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json":
		body, err := json.MarshalIndent(result.Rows, "", "  ")
		if err != nil {
			return fmt.Sprintf("marshal query rows: %v", err)
		}

		return string(body)
	case "table":
		return formatQueryResultTable(result)
	default:
		return fmt.Sprintf("unsupported format %q; expected json or table", format)
	}
}

func formatQueryResultTable(result session.QueryResult) string {
	if result.RowCount == 0 {
		return "(0 rows)"
	}

	var builder strings.Builder
	builder.WriteString(strings.Join(result.Columns, "\t"))
	for _, row := range result.Rows {
		builder.WriteString("\n")
		for idx, column := range result.Columns {
			if idx > 0 {
				builder.WriteString("\t")
			}
			builder.WriteString(fmt.Sprint(row[column]))
		}
	}
	fmt.Fprintf(&builder, "\n(%d rows)", result.RowCount)

	return builder.String()
}

func requiredString(args map[string]any, name string) (string, error) {
	value, ok := args[name]
	if !ok {
		return "", fmt.Errorf("missing required argument %q", name)
	}

	stringValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("argument %q must be a string", name)
	}
	if strings.TrimSpace(stringValue) == "" {
		return "", fmt.Errorf("argument %q must not be empty", name)
	}

	return stringValue, nil
}

func optionalString(args map[string]any, name string) string {
	value, ok := args[name]
	if !ok {
		return ""
	}

	stringValue, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(stringValue)
}

func optionalBool(args map[string]any, name string) bool {
	value, ok := args[name]
	if !ok {
		return false
	}

	boolValue, ok := value.(bool)
	return ok && boolValue
}

func requiredInt16(args map[string]any, name string) (int16, error) {
	value, ok := args[name]
	if !ok {
		return 0, fmt.Errorf("missing required argument %q", name)
	}

	var parsed int64
	switch typed := value.(type) {
	case float64:
		if typed != float64(int64(typed)) {
			return 0, fmt.Errorf("argument %q must be an integer", name)
		}

		parsed = int64(typed)
	case int:
		parsed = int64(typed)
	case int64:
		parsed = typed
	case string:
		var err error
		parsed, err = strconv.ParseInt(strings.TrimSpace(typed), 10, 16)
		if err != nil {
			return 0, fmt.Errorf("argument %q must be an integer: %w", name, err)
		}
	default:
		return 0, fmt.Errorf("argument %q must be an integer", name)
	}

	if parsed < -32768 || parsed > 32767 {
		return 0, fmt.Errorf("argument %q must fit in int16", name)
	}

	return int16(parsed), nil
}

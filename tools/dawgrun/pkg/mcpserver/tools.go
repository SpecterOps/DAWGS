package mcpserver

type toolDefinition struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"inputSchema"`
	WriteCapable bool           `json:"-"`
}

func tools() []toolDefinition {
	return []toolDefinition{
		{
			Name:        "open_connection",
			Description: "Open a named DAWGS-compatible backend connection. Driver is inferred from the connection string unless supplied.",
			InputSchema: objectSchema(map[string]any{
				"name":              stringSchema("Session-local connection name."),
				"connection_string": stringSchema("PostgreSQL or Neo4j connection string. Treated as sensitive."),
				"driver":            enumSchema("Optional driver override.", []string{"pg", "neo4j"}),
			}, []string{"name", "connection_string"}),
		},
		{
			Name:        "list_connections",
			Description: "List named backend connections currently open in this MCP session.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "close_connection",
			Description: "Close and remove a named backend connection from this MCP session.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("Connection name."),
			}, []string{"connection"}),
		},
		{
			Name:        "load_kind_map",
			Description: "Refresh and return the PostgreSQL kind mapping for a named connection.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("Connection name."),
			}, []string{"connection"}),
		},
		{
			Name:        "lookup_kind",
			Description: "Resolve a graph kind name to the numeric PostgreSQL kind ID for a connection.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("Connection name."),
				"kind_name":  stringSchema("Kind name, for example User or Group."),
			}, []string{"connection", "kind_name"}),
		},
		{
			Name:        "lookup_kind_id",
			Description: "Resolve a numeric PostgreSQL kind ID to its graph kind name for a connection.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("Connection name."),
				"kind_id":    integerSchema("Numeric kind ID."),
			}, []string{"connection", "kind_id"}),
		},
		{
			Name:        "parse_cypher",
			Description: "Parse a CySQL/Cypher query and return an inspectable AST dump.",
			InputSchema: objectSchema(map[string]any{
				"query": stringSchema("CySQL/Cypher query."),
			}, []string{"query"}),
		},
		{
			Name:        "translate_cypher_to_pgsql",
			Description: "Translate a CySQL/Cypher query to PostgreSQL SQL, optionally using a live connection kind map.",
			InputSchema: objectSchema(map[string]any{
				"query":       stringSchema("CySQL/Cypher query."),
				"connection":  stringSchema("Optional connection name for kind-aware translation."),
				"dump_pg_ast": booleanSchema("Whether to include the translator PostgreSQL AST dump."),
			}, []string{"query"}),
		},
		{
			Name:        "explain_cypher_pgsql",
			Description: "Translate a CySQL/Cypher query and run PostgreSQL EXPLAIN over the translated SQL.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("PostgreSQL connection name."),
				"query":      stringSchema("CySQL/Cypher query."),
			}, []string{"connection", "query"}),
		},
		{
			Name:        "query_cypher",
			Description: "Execute a CySQL/Cypher query against a named DAWGS connection and return rows.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("Connection name."),
				"query":      stringSchema("CySQL/Cypher query."),
				"format":     enumSchema("Output shape for rows.", []string{"json", "table"}),
			}, []string{"connection", "query"}),
		},
		{
			Name:        "save_opengraph",
			Description: "Export all graph data from a connection as OpenGraph JSON.",
			InputSchema: objectSchema(map[string]any{
				"connection":  stringSchema("Connection name."),
				"output_path": stringSchema("Optional output path. If omitted, return JSON content."),
			}, []string{"connection"}),
		},
		{
			Name:        "load_opengraph",
			Description: "Load OpenGraph JSON from a file into a named connection.",
			InputSchema: objectSchema(map[string]any{
				"connection": stringSchema("Target connection name."),
				"path":       stringSchema("OpenGraph JSON input path."),
			}, []string{"connection", "path"}),
			WriteCapable: true,
		},
		{
			Name:        "copy_opengraph",
			Description: "Copy all graph data from one active connection to another.",
			InputSchema: objectSchema(map[string]any{
				"source": stringSchema("Source connection name."),
				"target": stringSchema("Target connection name."),
			}, []string{"source", "target"}),
			WriteCapable: true,
		},
		{
			Name:        "start_runtime_trace",
			Description: "Start Go runtime tracing for subsequent server work.",
			InputSchema: objectSchema(map[string]any{
				"path": stringSchema("Optional trace output path. Defaults to trace.out."),
			}, nil),
		},
		{
			Name:        "stop_runtime_trace",
			Description: "Stop active Go runtime tracing.",
			InputSchema: objectSchema(nil, nil),
		},
	}
}

func toolByName(name string) (toolDefinition, bool) {
	for _, tool := range tools() {
		if tool.Name == name {
			return tool, true
		}
	}

	return toolDefinition{}, false
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	if required == nil {
		required = []string{}
	}

	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
}

func stringSchema(description string) map[string]any {
	return map[string]any{
		"type":        "string",
		"description": description,
	}
}

func integerSchema(description string) map[string]any {
	return map[string]any{
		"type":        "integer",
		"description": description,
	}
}

func booleanSchema(description string) map[string]any {
	return map[string]any{
		"type":        "boolean",
		"description": description,
	}
}

func enumSchema(description string, values []string) map[string]any {
	return map[string]any{
		"type":        "string",
		"description": description,
		"enum":        values,
	}
}

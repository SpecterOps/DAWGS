package mcpserver

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestServerAdvertisesExpectedTools(t *testing.T) {
	ctx := context.Background()
	session := connectTestServer(t, ctx, New(Options{}))

	tools := make(map[string]bool)
	for tool, err := range session.Tools(ctx, nil) {
		require.NoError(t, err)
		tools[tool.Name] = true
	}

	require.Equal(t, map[string]bool{
		"list_connections":          true,
		"open_connection":           true,
		"parse_cypher":              true,
		"translate_cypher_to_pgsql": true,
		"query_cypher":              true,
		"explain_psql":              true,
		"save_opengraph":            true,
		"load_opengraph":            true,
		"copy_opengraph":            true,
		"load_db_kinds":             true,
		"lookup_kind":               true,
		"lookup_kind_id":            true,
	}, tools)
}

func TestListConnectionsReturnsOpenNamesOnly(t *testing.T) {
	ctx := context.Background()
	session := connectTestServer(t, ctx, New(Options{}))

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "list_connections"})
	require.NoError(t, err)
	require.False(t, result.IsError)

	output := decodeToolOutput[ListConnectionsOutput](t, result)
	require.Empty(t, output.Open)
}

func TestParseCypherReturnsStructuredOutput(t *testing.T) {
	ctx := context.Background()
	session := connectTestServer(t, ctx, New(Options{}))

	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "parse_cypher",
		Arguments: map[string]any{
			"query": "MATCH (n) RETURN n",
		},
	})
	require.NoError(t, err)
	require.False(t, result.IsError)

	output := decodeToolOutput[ParseCypherOutput](t, result)
	require.Contains(t, output.AST, "RegularQuery")
}

func connectTestServer(t *testing.T, ctx context.Context, server *Server) *mcp.ClientSession {
	t.Helper()

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.MCPServer().Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { clientSession.Close() })

	return clientSession
}

func decodeToolOutput[T any](t *testing.T, result *mcp.CallToolResult) T {
	t.Helper()

	var output T
	data, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &output))

	return output
}

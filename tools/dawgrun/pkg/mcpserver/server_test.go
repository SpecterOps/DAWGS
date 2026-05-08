package mcpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/session"
)

func TestServerListsTools(t *testing.T) {
	request := framedRequest(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	var output bytes.Buffer

	server := New(strings.NewReader(request), &output, Options{})
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve() error = %v", err)
	}

	response := decodeFramedResponse(t, output.String())
	result := response["result"].(map[string]any)
	tools := result["tools"].([]any)

	if len(tools) == 0 {
		t.Fatal("expected advertised tools")
	}

	var foundClose bool
	for _, rawTool := range tools {
		tool := rawTool.(map[string]any)
		if tool["name"] == "close_connection" {
			foundClose = true
			break
		}
	}
	if !foundClose {
		t.Fatal("expected close_connection tool")
	}
}

func TestServerGuardsWriteTools(t *testing.T) {
	request := framedRequest(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"load_opengraph","arguments":{"connection":"local","path":"graph.json"}}}`)
	var output bytes.Buffer

	server := New(strings.NewReader(request), &output, Options{})
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve() error = %v", err)
	}

	response := decodeFramedResponse(t, output.String())
	result := response["result"].(map[string]any)
	if isError, _ := result["isError"].(bool); !isError {
		t.Fatal("expected write tool call to be an MCP tool error")
	}
}

func TestServerReturnsStructuredToolContent(t *testing.T) {
	request := framedRequest(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_connections","arguments":{}}}`)
	var output bytes.Buffer

	server := New(strings.NewReader(request), &output, Options{})
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve() error = %v", err)
	}

	response := decodeFramedResponse(t, output.String())
	result := response["result"].(map[string]any)
	structuredContent, ok := result["structuredContent"].(map[string]any)
	if !ok {
		t.Fatal("expected structuredContent")
	}
	if _, ok := structuredContent["connections"].([]any); !ok {
		t.Fatal("expected structured connections array")
	}
}

func TestFormatQueryResultText(t *testing.T) {
	result := session.QueryResult{
		Columns:  []string{"name"},
		Rows:     []map[string]any{{"name": "alice"}},
		RowCount: 1,
	}

	if text := formatQueryResultText(result, "json"); !strings.Contains(text, `"alice"`) {
		t.Fatalf("expected JSON text to contain row value, got %q", text)
	}
	if text := formatQueryResultText(result, "table"); !strings.Contains(text, "name\nalice\n(1 rows)") {
		t.Fatalf("expected table text, got %q", text)
	}
}

func framedRequest(body string) string {
	return fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)
}

func decodeFramedResponse(t *testing.T, response string) map[string]any {
	t.Helper()

	header, body, ok := strings.Cut(response, "\r\n\r\n")
	if !ok {
		t.Fatalf("response missing frame delimiter: %q", response)
	}

	var contentLength int
	for _, line := range strings.Split(header, "\r\n") {
		name, value, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				t.Fatalf("invalid Content-Length: %v", err)
			}

			contentLength = parsed
		}
	}
	if contentLength != len(body) {
		t.Fatalf("Content-Length = %d, body length = %d", contentLength, len(body))
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	return decoded
}

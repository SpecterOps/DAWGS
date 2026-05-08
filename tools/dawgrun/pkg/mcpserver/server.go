package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/session"
)

const defaultProtocolVersion = "2024-11-05"

type Options struct {
	AllowWrites bool
}

type Server struct {
	in      io.Reader
	out     io.Writer
	options Options
	session *session.Session
}

func New(in io.Reader, out io.Writer, options Options) *Server {
	return &Server{
		in:      in,
		out:     out,
		options: options,
		session: session.New(),
	}
}

func (s *Server) Serve() error {
	defer s.session.CloseAll(context.Background())

	reader := bufio.NewReader(s.in)

	for {
		body, err := readMessage(reader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read request: %w", err)
		}

		var request jsonRPCRequest
		if unmarshalErr := json.Unmarshal(body, &request); unmarshalErr != nil {
			if encodeErr := writeMessage(s.out, errorResponse(nil, -32700, "parse error")); encodeErr != nil {
				return fmt.Errorf("write parse error response: %w", encodeErr)
			}
		} else if request.Method == "" {
			if encodeErr := writeMessage(s.out, errorResponse(request.ID, -32600, "invalid request")); encodeErr != nil {
				return fmt.Errorf("write invalid request response: %w", encodeErr)
			}
		} else if request.ID == nil {
			// Notifications do not receive responses.
			s.handleNotification(request)
		} else if response := s.handleRequest(request); response != nil {
			if encodeErr := writeMessage(s.out, response); encodeErr != nil {
				return fmt.Errorf("write response: %w", encodeErr)
			}
		}
	}
}

func readMessage(reader *bufio.Reader) ([]byte, error) {
	contentLength := -1
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}

		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("invalid MCP header %q", line)
		}
		if strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || parsed < 0 {
				return nil, fmt.Errorf("invalid Content-Length %q", strings.TrimSpace(value))
			}

			contentLength = parsed
		}
	}

	if contentLength < 0 {
		return nil, errors.New("missing Content-Length header")
	}

	body := make([]byte, contentLength)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}

	return body, nil
}

func writeMessage(writer io.Writer, response *jsonRPCResponse) error {
	body, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	if _, err := fmt.Fprintf(writer, "Content-Length: %d\r\n\r\n", len(body)); err != nil {
		return fmt.Errorf("write response header: %w", err)
	}
	if _, err := writer.Write(body); err != nil {
		return fmt.Errorf("write response body: %w", err)
	}

	return nil
}

func (s *Server) handleNotification(request jsonRPCRequest) {
	switch request.Method {
	case "notifications/initialized", "notifications/cancelled", "notifications/progress":
		return
	}
}

func (s *Server) handleRequest(request jsonRPCRequest) *jsonRPCResponse {
	switch request.Method {
	case "initialize":
		return resultResponse(request.ID, initializeResult(request.Params))
	case "ping":
		return resultResponse(request.ID, map[string]any{})
	case "tools/list":
		return resultResponse(request.ID, map[string]any{"tools": tools()})
	case "tools/call":
		return resultResponse(request.ID, s.callTool(request.Params))
	case "resources/list":
		return resultResponse(request.ID, map[string]any{"resources": []any{}})
	case "prompts/list":
		return resultResponse(request.ID, map[string]any{"prompts": []any{}})
	default:
		return errorResponse(request.ID, -32601, fmt.Sprintf("method %q not found", request.Method))
	}
}

func initializeResult(params json.RawMessage) map[string]any {
	protocolVersion := defaultProtocolVersion
	var initializeParams struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(params, &initializeParams); err == nil && initializeParams.ProtocolVersion != "" {
		protocolVersion = initializeParams.ProtocolVersion
	}

	return map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    "dawgrun-mcp",
			"version": "0.1.0",
		},
	}
}

type jsonRPCRequest struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Result  any              `json:"result"`
	Error   *jsonRPCError    `json:"error,omitempty"`
}

func (r jsonRPCResponse) MarshalJSON() ([]byte, error) {
	response := map[string]any{
		"jsonrpc": r.JSONRPC,
	}
	if r.ID != nil {
		response["id"] = json.RawMessage(*r.ID)
	}
	if r.Error != nil {
		response["error"] = r.Error
	} else {
		response["result"] = r.Result
	}

	return json.Marshal(response)
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type callToolRequest struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

type callToolResult struct {
	Content           []toolContent `json:"content"`
	StructuredContent any           `json:"structuredContent,omitempty"`
	IsError           bool          `json:"isError,omitempty"`
}

type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func resultResponse(id *json.RawMessage, result any) *jsonRPCResponse {
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
}

func errorResponse(id *json.RawMessage, code int, message string) *jsonRPCResponse {
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &jsonRPCError{
			Code:    code,
			Message: message,
		},
	}
}

func errorToolResult(message string) callToolResult {
	return callToolResult{
		Content: []toolContent{{
			Type: "text",
			Text: message,
		}},
		IsError: true,
	}
}

func successToolResult(message string) callToolResult {
	if strings.TrimSpace(message) == "" {
		message = "ok"
	}

	return callToolResult{
		Content: []toolContent{{
			Type: "text",
			Text: message,
		}},
	}
}

func successJSONToolResult(value any) callToolResult {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return errorToolResult(fmt.Sprintf("marshal tool result: %v", err))
	}

	return successStructuredTextToolResult(value, string(body))
}

func successStructuredTextToolResult(value any, text string) callToolResult {
	return callToolResult{
		Content: []toolContent{{
			Type: "text",
			Text: text,
		}},
		StructuredContent: value,
	}
}

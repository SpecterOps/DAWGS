package mcpserver

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
)

type Options struct {
	AllowWrites bool
}

type Server struct {
	server      *mcp.Server
	scope       *commands.Scope
	commandMu   sync.Mutex
	allowWrites bool
}

type ListConnectionsInput struct{}

type ListConnectionsOutput struct {
	Open []string `json:"open"`
}

type OpenConnectionInput struct {
	Name             string `json:"name" jsonschema:"session-local connection name"`
	ConnectionString string `json:"connection_string" jsonschema:"PostgreSQL or Neo4j connection string; treated as sensitive"`
	Driver           string `json:"driver,omitempty" jsonschema:"optional driver override: pg or neo4j"`
	DefaultGraph     string `json:"default_graph,omitempty" jsonschema:"default graph name; defaults to default"`
	InitGraph        bool   `json:"init_graph,omitempty" jsonschema:"create default graph if selecting it fails"`
}

type OpenConnectionOutput struct {
	Name string `json:"name"`
}

type ParseCypherInput struct {
	Query string `json:"query" jsonschema:"CySQL/Cypher query to parse"`
}

type ParseCypherOutput struct {
	AST string `json:"ast"`
}

type TranslateCypherInput struct {
	Query      string `json:"query" jsonschema:"CySQL/Cypher query to translate"`
	Connection string `json:"connection,omitempty" jsonschema:"optional open connection name for live kind mapping"`
	DumpPGAst  bool   `json:"dump_pg_ast,omitempty" jsonschema:"include PostgreSQL AST dump"`
}

type TranslateCypherOutput struct {
	SQL   string `json:"sql"`
	PGAst string `json:"pg_ast,omitempty"`
}

func New(options Options) *Server {
	return NewWithScope(commands.NewScope(commands.RunModeCLI), options)
}

func NewWithScope(scope *commands.Scope, options Options) *Server {
	s := &Server{
		server: mcp.NewServer(&mcp.Implementation{
			Name:    "dawgrun-mcp",
			Version: "0.1.0",
		}, nil),
		scope:       scope,
		allowWrites: options.AllowWrites,
	}
	s.registerTools()

	return s
}

func (s *Server) Run(ctx context.Context) error {
	defer s.scope.CloseConnections(context.Background())
	return s.server.Run(ctx, &mcp.StdioTransport{})
}

func (s *Server) MCPServer() *mcp.Server {
	return s.server
}

func (s *Server) commandContext(ctx context.Context) *commands.CommandContext {
	cmdCtx := commands.NewCommandContext(ctx, nil, s.scope, "")
	cmdCtx.SetStyledOutputEnabled(false)
	return cmdCtx
}

func (s *Server) runCommand(ctx context.Context, name string, args []string) (string, error) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()

	cmd, ok := commands.Registry()[name]
	if !ok {
		return "", fmt.Errorf("unknown command %s", name)
	}

	cmdCtx := s.commandContext(ctx)
	if cmd.ClearFlagsFn != nil {
		defer cmd.ClearFlagsFn()
	}
	if err := cmd.Fn(cmdCtx, args); err != nil {
		return "", err
	}

	return cmdCtx.OutputString(), nil
}

func (s *Server) registerTools() {
	mcp.AddTool(s.server, readOnlyTool("list_connections", "List Connections", "List open dawgrun connection names without exposing connection strings."), s.listConnections)
	mcp.AddTool(s.server, readOnlyTool("open_connection", "Open Connection", "Open a named DAWGS backend connection. Connection strings are treated as sensitive and are not echoed."), s.openConnection)
	mcp.AddTool(s.server, readOnlyTool("parse_cypher", "Parse Cypher", "Parse a CySQL/Cypher query and return an AST dump."), s.parseCypher)
	mcp.AddTool(s.server, readOnlyTool("translate_cypher_to_pgsql", "Translate Cypher To PGSQL", "Translate a CySQL/Cypher query to PostgreSQL SQL."), s.translateCypher)
	mcp.AddTool(s.server, readOnlyTool("query_cypher", "Query Cypher", "Execute a CySQL/Cypher query against a named DAWGS connection and return bounded rows."), s.queryCypher)
	mcp.AddTool(s.server, readOnlyTool("explain_psql", "Explain PGSQL", "Translate a CySQL/Cypher query and return the PostgreSQL EXPLAIN plan."), s.explainPsql)
	mcp.AddTool(s.server, readOnlyTool("save_opengraph", "Save OpenGraph", "Export OpenGraph JSON from a named connection."), s.saveOpenGraph)
	mcp.AddTool(s.server, writeTool("load_opengraph", "Load OpenGraph", "Load OpenGraph JSON into a named connection. Requires --allow-writes."), s.loadOpenGraph)
	mcp.AddTool(s.server, writeTool("copy_opengraph", "Copy OpenGraph", "Copy OpenGraph data between named connections. Requires --allow-writes."), s.copyOpenGraph)
	mcp.AddTool(s.server, readOnlyTool("load_db_kinds", "Load DB Kinds", "Load and return the kind mapping from a named PostgreSQL connection."), s.loadDBKinds)
	mcp.AddTool(s.server, readOnlyTool("lookup_kind", "Lookup Kind", "Resolve a kind name to its PostgreSQL kind ID."), s.lookupKind)
	mcp.AddTool(s.server, readOnlyTool("lookup_kind_id", "Lookup Kind ID", "Resolve a PostgreSQL kind ID to its kind name."), s.lookupKindID)
}

func readOnlyTool(name, title, description string) *mcp.Tool {
	closedWorld := false
	return &mcp.Tool{
		Name:        name,
		Title:       title,
		Description: description,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &closedWorld},
	}
}

func writeTool(name, title, description string) *mcp.Tool {
	destructive := true
	closedWorld := false
	return &mcp.Tool{
		Name:        name,
		Title:       title,
		Description: description,
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &destructive, OpenWorldHint: &closedWorld},
	}
}

func (s *Server) listConnections(context.Context, *mcp.CallToolRequest, ListConnectionsInput) (*mcp.CallToolResult, ListConnectionsOutput, error) {
	return nil, ListConnectionsOutput{Open: s.scope.GetConnectionNames()}, nil
}

func (s *Server) openConnection(ctx context.Context, _ *mcp.CallToolRequest, input OpenConnectionInput) (*mcp.CallToolResult, OpenConnectionOutput, error) {
	name := strings.TrimSpace(input.Name)
	args := []string{}
	if driver := strings.TrimSpace(input.Driver); driver != "" {
		args = append(args, "-driver", driver)
	}
	if defaultGraph := strings.TrimSpace(input.DefaultGraph); defaultGraph != "" {
		args = append(args, "-default-graph", defaultGraph)
	}
	if input.InitGraph {
		args = append(args, "-init-graph")
	}
	args = append(args, name, input.ConnectionString)

	if _, err := s.runCommand(ctx, "open", args); err != nil {
		return nil, OpenConnectionOutput{}, err
	}

	return nil, OpenConnectionOutput{Name: name}, nil
}

func (s *Server) parseCypher(_ context.Context, _ *mcp.CallToolRequest, input ParseCypherInput) (*mcp.CallToolResult, ParseCypherOutput, error) {
	query, err := commands.ParseQueryText(input.Query)
	if err != nil {
		return nil, ParseCypherOutput{}, err
	}

	return nil, ParseCypherOutput{AST: spew.Sdump(query)}, nil
}

func (s *Server) translateCypher(ctx context.Context, _ *mcp.CallToolRequest, input TranslateCypherInput) (*mcp.CallToolResult, TranslateCypherOutput, error) {
	output, err := commands.TranslateCypherToPsql(s.commandContext(ctx), input.Query, commands.TranslateCypherOptions{
		Connection: input.Connection,
		DumpPGAst:  input.DumpPGAst,
	})
	if err != nil {
		return nil, TranslateCypherOutput{}, err
	}

	return nil, TranslateCypherOutput{SQL: output.SQL, PGAst: output.PGAst}, nil
}

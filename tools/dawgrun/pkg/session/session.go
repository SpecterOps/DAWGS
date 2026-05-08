package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/kanmu/go-sqlfmt/sqlfmt"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/frontend"
	cypherModels "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
)

type Session struct {
	mu             sync.RWMutex
	connections    map[string]graph.Database
	connectionInfo map[string]ConnectionInfo
	kindMaps       map[string]stubs.KindMap
	traceFile      *os.File
}

type ConnectionInfo struct {
	Name   string `json:"name"`
	Driver string `json:"driver"`
}

type OpenConnectionRequest struct {
	Name             string
	ConnectionString string
	Driver           string
}

type OpenConnectionResult struct {
	Connection ConnectionInfo `json:"connection"`
	Replaced   bool           `json:"replaced"`
}

type TranslateResult struct {
	SQL   string `json:"sql"`
	PGAst string `json:"pg_ast,omitempty"`
}

type ExplainResult struct {
	SQL  string   `json:"sql"`
	Plan []string `json:"plan"`
}

type QueryResult struct {
	Columns  []string         `json:"columns"`
	Rows     []map[string]any `json:"rows"`
	RowCount int              `json:"row_count"`
}

type SaveOpenGraphResult struct {
	OutputPath string `json:"output_path,omitempty"`
	JSON       string `json:"json,omitempty"`
	Nodes      int    `json:"nodes"`
	Edges      int    `json:"edges"`
}

type LoadOpenGraphResult struct {
	Connection string `json:"connection"`
	InputPath  string `json:"input_path"`
	Nodes      int    `json:"nodes"`
	Edges      int    `json:"edges"`
}

type CopyOpenGraphResult struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Nodes  int    `json:"nodes"`
	Edges  int    `json:"edges"`
}

type RuntimeTraceResult struct {
	Path string `json:"path,omitempty"`
}

func New() *Session {
	return &Session{
		connections:    make(map[string]graph.Database),
		connectionInfo: make(map[string]ConnectionInfo),
		kindMaps:       make(map[string]stubs.KindMap),
	}
}

func (w *Session) OpenConnection(ctx context.Context, request OpenConnectionRequest) (OpenConnectionResult, error) {
	name := strings.TrimSpace(request.Name)
	if name == "" {
		return OpenConnectionResult{}, errors.New("connection name must not be empty")
	}

	connectionString := strings.TrimSpace(request.ConnectionString)
	if connectionString == "" {
		return OpenConnectionResult{}, errors.New("connection string must not be empty")
	}

	driverName := strings.ToLower(strings.TrimSpace(request.Driver))
	if driverName == "" {
		detectedDriverName, err := driverFromConnectionString(connectionString)
		if err != nil {
			return OpenConnectionResult{}, err
		}

		driverName = detectedDriverName
	}

	config := dawgs.Config{ConnectionString: connectionString}
	openSuccess := false
	switch driverName {
	case pg.DriverName:
		connPool, err := pg.NewPool(drivers.DatabaseConfiguration{Connection: connectionString})
		if err != nil {
			return OpenConnectionResult{}, fmt.Errorf("open connection pool: %w", err)
		}
		defer func() {
			if !openSuccess {
				connPool.Close()
			}
		}()

		config.Pool = connPool
	case neo4j.DriverName:
	default:
		return OpenConnectionResult{}, fmt.Errorf("unsupported driver %q; expected one of: %s, %s", driverName, pg.DriverName, neo4j.DriverName)
	}

	conn, err := dawgs.Open(ctx, driverName, config)
	if err != nil {
		return OpenConnectionResult{}, fmt.Errorf("open %s database connection: %s", driverName, redactSecrets(err.Error(), connectionString))
	}

	info := ConnectionInfo{Name: name, Driver: driverName}
	w.mu.RLock()
	previousConn, replaced := w.connections[name]
	w.mu.RUnlock()

	if replaced {
		if err := previousConn.Close(ctx); err != nil {
			_ = conn.Close(ctx)
			return OpenConnectionResult{}, fmt.Errorf("close previous connection %s: %w", name, err)
		}
	}

	w.mu.Lock()
	w.connections[name] = conn
	w.connectionInfo[name] = info
	delete(w.kindMaps, name)
	w.mu.Unlock()

	openSuccess = true
	return OpenConnectionResult{Connection: info, Replaced: replaced}, nil
}

func redactSecrets(message string, secrets ...string) string {
	for _, secret := range secrets {
		if strings.TrimSpace(secret) != "" {
			message = strings.ReplaceAll(message, secret, "<redacted>")
		}
	}

	return message
}

func (w *Session) ListConnections() []ConnectionInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()

	names := slices.Sorted(maps.Keys(w.connectionInfo))
	connections := make([]ConnectionInfo, 0, len(names))
	for _, name := range names {
		connections = append(connections, w.connectionInfo[name])
	}

	return connections
}

func (w *Session) NumConnections() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return len(w.connections)
}

func (w *Session) CloseConnection(ctx context.Context, name string) error {
	w.mu.Lock()
	conn, ok := w.connections[name]
	if ok {
		delete(w.connections, name)
		delete(w.connectionInfo, name)
		delete(w.kindMaps, name)
	}
	w.mu.Unlock()

	if !ok {
		return fmt.Errorf("unknown connection %s", name)
	}

	return conn.Close(ctx)
}

func (w *Session) CloseAll(ctx context.Context) error {
	w.mu.Lock()
	connections := w.connections
	w.connections = make(map[string]graph.Database)
	w.connectionInfo = make(map[string]ConnectionInfo)
	w.kindMaps = make(map[string]stubs.KindMap)
	traceFile := w.traceFile
	w.traceFile = nil
	w.mu.Unlock()

	if traceFile != nil {
		trace.Stop()
		_ = traceFile.Close()
	}

	var closeErr error
	for name, conn := range connections {
		if err := conn.Close(ctx); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close connection %s: %w", name, err))
		}
	}

	return closeErr
}

func (w *Session) LoadKindMap(ctx context.Context, connection string) (map[string]string, error) {
	kindMap, err := w.loadKindMap(ctx, connection)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(kindMap))
	for id, kind := range kindMap {
		result[strconv.FormatInt(int64(id), 10)] = fmt.Sprint(kind)
	}

	return result, nil
}

func (w *Session) LookupKind(ctx context.Context, connection, kindName string) (map[string]any, error) {
	kindMap, err := w.kindMap(ctx, connection)
	if err != nil {
		return nil, err
	}

	mapper := stubs.MapperFromKindMap(kindMap)
	kindID, err := mapper.GetIDByKind(graph.StringKind(kindName))
	if err != nil {
		return nil, fmt.Errorf("look up kind: %w", err)
	}

	return map[string]any{"kind_name": kindName, "kind_id": kindID}, nil
}

func (w *Session) LookupKindID(ctx context.Context, connection string, kindID int16) (map[string]any, error) {
	kindMap, err := w.kindMap(ctx, connection)
	if err != nil {
		return nil, err
	}

	mapper := stubs.MapperFromKindMap(kindMap)
	kind, err := mapper.GetKindByID(kindID)
	if err != nil {
		return nil, fmt.Errorf("look up kind ID: %w", err)
	}

	return map[string]any{"kind_name": fmt.Sprint(kind), "kind_id": kindID}, nil
}

func (w *Session) ParseCypher(query string) (map[string]string, error) {
	parsedQuery, err := parseCypher(query)
	if err != nil {
		return nil, err
	}

	return map[string]string{"ast": spew.Sdump(parsedQuery)}, nil
}

func (w *Session) TranslateCypherToPGSQL(ctx context.Context, query, connection string, dumpPGAst bool) (TranslateResult, error) {
	translated, err := w.translateCypher(ctx, query, connection)
	if err != nil {
		return TranslateResult{}, err
	}

	result := TranslateResult{SQL: translated.SQL}
	if dumpPGAst {
		result.PGAst = spew.Sdump(translated.Statement)
	}

	return result, nil
}

func (w *Session) ExplainCypherPGSQL(ctx context.Context, connection, query string) (ExplainResult, error) {
	conn, err := w.connection(connection)
	if err != nil {
		return ExplainResult{}, err
	}

	translated, err := w.translateCypher(ctx, query, connection)
	if err != nil {
		return ExplainResult{}, err
	}

	explainSQL := fmt.Sprintf("EXPLAIN %s", translated.SQL)
	result := ExplainResult{SQL: explainSQL}
	err = conn.ReadTransaction(ctx, func(tx graph.Transaction) error {
		rows := tx.Raw(explainSQL, nil)
		if err := rows.Error(); err != nil {
			return fmt.Errorf("run EXPLAIN query: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var value string
			if err := graph.ScanNextResult(rows, &value); err != nil {
				return fmt.Errorf("scan EXPLAIN row: %w", err)
			}

			result.Plan = append(result.Plan, value)
		}

		return rows.Error()
	})
	if err != nil {
		return ExplainResult{}, err
	}

	return result, nil
}

func (w *Session) QueryCypher(ctx context.Context, connection, query string) (QueryResult, error) {
	conn, err := w.connection(connection)
	if err != nil {
		return QueryResult{}, err
	}

	queryResult := QueryResult{
		Columns: []string{},
		Rows:    []map[string]any{},
	}
	err = conn.ReadTransaction(ctx, func(tx graph.Transaction) error {
		rows := tx.Query(query, nil)
		if err := rows.Error(); err != nil {
			return fmt.Errorf("run cypher query: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			values := rows.Values()
			if queryResult.RowCount == 0 {
				queryResult.Columns = buildResultColumns(rows.Keys(), len(values))
			}

			queryResult.Rows = append(queryResult.Rows, buildResultRow(queryResult.Columns, values))
			queryResult.RowCount++
		}

		return rows.Error()
	})
	if err != nil {
		return QueryResult{}, err
	}

	return queryResult, nil
}

func (w *Session) SaveOpenGraph(ctx context.Context, connection, outputPath string) (SaveOpenGraphResult, error) {
	conn, err := w.connection(connection)
	if err != nil {
		return SaveOpenGraphResult{}, err
	}

	exportBuffer := new(bytes.Buffer)
	if err := opengraph.Export(ctx, conn, exportBuffer); err != nil {
		return SaveOpenGraphResult{}, fmt.Errorf("export opengraph data: %w", err)
	}

	doc, err := opengraph.ParseDocument(bytes.NewReader(exportBuffer.Bytes()))
	if err != nil {
		return SaveOpenGraphResult{}, fmt.Errorf("parse exported opengraph document: %w", err)
	}

	result := SaveOpenGraphResult{Nodes: len(doc.Graph.Nodes), Edges: len(doc.Graph.Edges)}
	if strings.TrimSpace(outputPath) == "" {
		result.JSON = exportBuffer.String()
		return result, nil
	}

	if err := os.WriteFile(outputPath, exportBuffer.Bytes(), 0o600); err != nil {
		return SaveOpenGraphResult{}, fmt.Errorf("write output file %s: %w", outputPath, err)
	}

	result.OutputPath = outputPath
	return result, nil
}

func (w *Session) LoadOpenGraph(ctx context.Context, connection, inputPath string) (LoadOpenGraphResult, error) {
	conn, err := w.connection(connection)
	if err != nil {
		return LoadOpenGraphResult{}, err
	}

	inputFile, err := os.Open(inputPath)
	if err != nil {
		return LoadOpenGraphResult{}, fmt.Errorf("open opengraph input file %s: %w", inputPath, err)
	}
	defer inputFile.Close()

	doc, err := opengraph.ParseDocument(inputFile)
	if err != nil {
		return LoadOpenGraphResult{}, fmt.Errorf("parse opengraph input file %s: %w", inputPath, err)
	}

	if _, err := opengraph.WriteGraph(ctx, conn, &doc.Graph); err != nil {
		return LoadOpenGraphResult{}, fmt.Errorf("write opengraph data into connection %s: %w", connection, err)
	}

	return LoadOpenGraphResult{Connection: connection, InputPath: inputPath, Nodes: len(doc.Graph.Nodes), Edges: len(doc.Graph.Edges)}, nil
}

func (w *Session) CopyOpenGraph(ctx context.Context, source, target string) (CopyOpenGraphResult, error) {
	if source == target {
		return CopyOpenGraphResult{}, errors.New("source and target connections must differ")
	}

	sourceConn, err := w.connection(source)
	if err != nil {
		return CopyOpenGraphResult{}, err
	}
	targetConn, err := w.connection(target)
	if err != nil {
		return CopyOpenGraphResult{}, err
	}

	pipeReader, pipeWriter := io.Pipe()
	exportErrCh := make(chan error, 1)
	go func() {
		if err := opengraph.Export(ctx, sourceConn, pipeWriter); err != nil {
			_ = pipeWriter.CloseWithError(err)
			exportErrCh <- err
			return
		}

		exportErrCh <- pipeWriter.Close()
	}()

	doc, parseErr := opengraph.ParseDocument(pipeReader)
	if parseErr != nil {
		_ = pipeReader.CloseWithError(parseErr)
	}
	exportErr := <-exportErrCh
	if exportErr != nil {
		return CopyOpenGraphResult{}, fmt.Errorf("export opengraph data from connection %s: %w", source, exportErr)
	}
	if parseErr != nil {
		return CopyOpenGraphResult{}, fmt.Errorf("parse streamed opengraph data from connection %s: %w", source, parseErr)
	}

	if _, err := opengraph.WriteGraph(ctx, targetConn, &doc.Graph); err != nil {
		return CopyOpenGraphResult{}, fmt.Errorf("copy opengraph data into connection %s: %w", target, err)
	}

	return CopyOpenGraphResult{Source: source, Target: target, Nodes: len(doc.Graph.Nodes), Edges: len(doc.Graph.Edges)}, nil
}

func (w *Session) StartRuntimeTrace(path string) (RuntimeTraceResult, error) {
	if strings.TrimSpace(path) == "" {
		path = "trace.out"
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.traceFile != nil {
		return RuntimeTraceResult{}, errors.New("runtime tracing is already enabled")
	}

	traceFile, err := os.Create(path)
	if err != nil {
		return RuntimeTraceResult{}, fmt.Errorf("create trace file: %w", err)
	}
	if err := trace.Start(traceFile); err != nil {
		_ = traceFile.Close()
		return RuntimeTraceResult{}, fmt.Errorf("start runtime trace: %w", err)
	}

	w.traceFile = traceFile
	return RuntimeTraceResult{Path: path}, nil
}

func (w *Session) StopRuntimeTrace() error {
	w.mu.Lock()
	traceFile := w.traceFile
	w.traceFile = nil
	w.mu.Unlock()

	if traceFile == nil {
		return errors.New("runtime tracing is not running")
	}

	trace.Stop()
	return traceFile.Close()
}

func (w *Session) connection(name string) (graph.Database, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	conn, ok := w.connections[name]
	if !ok {
		return nil, fmt.Errorf("unknown connection %s", name)
	}

	return conn, nil
}

func (w *Session) kindMap(ctx context.Context, connection string) (stubs.KindMap, error) {
	w.mu.RLock()
	kindMap, ok := w.kindMaps[connection]
	w.mu.RUnlock()
	if ok {
		return kindMap, nil
	}

	return w.loadKindMap(ctx, connection)
}

func (w *Session) loadKindMap(ctx context.Context, connection string) (stubs.KindMap, error) {
	conn, err := w.connection(connection)
	if err != nil {
		return nil, err
	}

	if err := conn.RefreshKinds(ctx); err != nil {
		return nil, fmt.Errorf("refresh kinds for connection %s: %w", connection, err)
	}

	kinds, err := conn.FetchKinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch kinds for connection %s: %w", connection, err)
	}

	driver, ok := conn.(*pg.Driver)
	if !ok {
		return nil, fmt.Errorf("connection %s is not a 'pg' connection", connection)
	}

	kindIDs, err := driver.MapKinds(ctx, kinds)
	if err != nil {
		return nil, fmt.Errorf("map kinds to IDs: %w", err)
	}

	kindMap := make(stubs.KindMap)
	for idx, kind := range kinds {
		kindMap[kindIDs[idx]] = kind
	}

	w.mu.Lock()
	w.kindMaps[connection] = kindMap
	w.mu.Unlock()

	return kindMap, nil
}

type translatedQuery struct {
	SQL       string
	Statement any
}

func (w *Session) translateCypher(ctx context.Context, query, connection string) (translatedQuery, error) {
	parsedQuery, err := parseCypher(query)
	if err != nil {
		return translatedQuery{}, err
	}

	kindMapper := stubs.EmptyMapper()
	if strings.TrimSpace(connection) != "" {
		kindMap, err := w.loadKindMap(ctx, connection)
		if err != nil {
			return translatedQuery{}, fmt.Errorf("load kind map: %w", err)
		}

		kindMapper = stubs.MapperFromKindMap(kindMap)
	}

	result, err := translate.Translate(ctx, parsedQuery, kindMapper, nil)
	if err != nil {
		return translatedQuery{}, fmt.Errorf("translate cypher query to pgsql: %w", err)
	}

	queryBuilder := format.NewOutputBuilder()
	if result.Parameters != nil {
		queryBuilder.WithMaterializedParameters(result.Parameters)
	}

	sqlQuery, err := format.Statement(result.Statement, queryBuilder)
	if err != nil {
		return translatedQuery{}, fmt.Errorf("format translated statement: %w", err)
	}

	formattedQuery, err := sqlfmt.Format(sqlQuery, &sqlfmt.Options{Distance: 0})
	if err != nil {
		formattedQuery = sqlQuery
	}

	return translatedQuery{SQL: formattedQuery, Statement: result.Statement}, nil
}

func parseCypher(query string) (*cypherModels.RegularQuery, error) {
	parsedQuery, err := frontend.ParseCypher(frontend.DefaultCypherContext(), query)
	if err != nil {
		return nil, fmt.Errorf("parse cypher query: %w", err)
	}

	return parsedQuery, nil
}

func driverFromConnectionString(connectionString string) (string, error) {
	parsedURL, err := url.Parse(connectionString)
	if err != nil {
		return "", fmt.Errorf("parse connection string: %w", err)
	}

	switch strings.ToLower(parsedURL.Scheme) {
	case "postgres", "postgresql":
		return pg.DriverName, nil
	case neo4j.DriverName:
		return neo4j.DriverName, nil
	default:
		return "", fmt.Errorf("unknown connection string scheme %q; expected postgres/postgresql or neo4j", parsedURL.Scheme)
	}
}

func buildResultColumns(keys []string, valueCount int) []string {
	columns := append([]string{}, keys...)
	for idx := len(columns); idx < valueCount; idx++ {
		columns = append(columns, fmt.Sprintf("column_%d", idx+1))
	}

	return columns
}

func buildResultRow(columns []string, values []any) map[string]any {
	row := make(map[string]any, len(columns))
	for idx, column := range columns {
		if idx < len(values) {
			row[column] = normalizeResultValue(values[idx])
		} else {
			row[column] = nil
		}
	}

	return row
}

func normalizeResultValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:
		return typed
	case graph.ID:
		return typed.Uint64()
	case []byte:
		return string(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

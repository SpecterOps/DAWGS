package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

const parquetCompression = "zstd"

type parquetStatement struct {
	GraphName  string
	Phase      phase
	OutputPath string
	SQL        string
}

func writeParquetCollection(ctx context.Context, outputDir string, value manifest) error {
	if !isJSONLManifestFormat(value.Format) {
		return fmt.Errorf("parquet export requires %s manifest format, got %q", manifestFormat, value.Format)
	}

	statements, err := parquetStatements(outputDir, value)
	if err != nil {
		return err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	for _, statement := range statements {
		slog.Info("retriever parquet export writing file",
			slog.String("graph", statement.GraphName),
			slog.String("phase", string(statement.Phase)),
			slog.String("path", statement.OutputPath),
		)
		if _, err := db.ExecContext(ctx, statement.SQL); err != nil {
			return fmt.Errorf("write parquet file %s: %w", statement.OutputPath, err)
		}
	}
	return nil
}

func parquetStatements(outputDir string, value manifest) ([]parquetStatement, error) {
	absoluteOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return nil, fmt.Errorf("resolve parquet output directory: %w", err)
	}

	statements := make([]parquetStatement, 0, len(value.Graphs)*2)
	for _, graphEntry := range value.Graphs {
		nodeFile, err := phaseFile(graphEntry, phaseNodes)
		if err != nil {
			return nil, err
		}
		edgeFile, err := phaseFile(graphEntry, phaseEdges)
		if err != nil {
			return nil, err
		}

		nodeOutputPath, err := parquetFilePath(graphEntry.Name, phaseNodes)
		if err != nil {
			return nil, err
		}
		edgeOutputPath, err := parquetFilePath(graphEntry.Name, phaseEdges)
		if err != nil {
			return nil, err
		}

		statements = append(statements, parquetStatement{
			GraphName:  graphEntry.Name,
			Phase:      phaseNodes,
			OutputPath: nodeOutputPath,
			SQL:        buildNodeParquetSQL(absoluteOutputDir, nodeFile, nodeOutputPath),
		})
		statements = append(statements, parquetStatement{
			GraphName:  graphEntry.Name,
			Phase:      phaseEdges,
			OutputPath: edgeOutputPath,
			SQL:        buildEdgeParquetSQL(absoluteOutputDir, nodeFile, edgeFile, edgeOutputPath),
		})
	}
	return statements, nil
}

func phaseFile(graphEntry graphManifest, nextPhase phase) (*fileManifest, error) {
	var found *fileManifest
	for idx := range graphEntry.Files {
		fileEntry := &graphEntry.Files[idx]
		if fileEntry.Phase != nextPhase {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("graph %q has multiple %s files; parquet export expects one JSONL file per phase", graphEntry.Name, nextPhase)
		}
		found = fileEntry
	}
	switch nextPhase {
	case phaseNodes:
		if graphEntry.NodeCount > 0 && found == nil {
			return nil, fmt.Errorf("graph %q has %d nodes but no node file", graphEntry.Name, graphEntry.NodeCount)
		}
	case phaseEdges:
		if graphEntry.EdgeCount > 0 && found == nil {
			return nil, fmt.Errorf("graph %q has %d edges but no edge file", graphEntry.Name, graphEntry.EdgeCount)
		}
	default:
		return nil, fmt.Errorf("unsupported parquet phase %q", nextPhase)
	}
	return found, nil
}

func parquetFilePath(graphName string, nextPhase phase) (string, error) {
	var name string
	switch nextPhase {
	case phaseNodes:
		name = "nodes.parquet"
	case phaseEdges:
		name = "edges.parquet"
	default:
		return "", fmt.Errorf("unsupported parquet phase %q", nextPhase)
	}
	return path.Join("graphs", graphDirectoryName(graphName), name), nil
}

func buildNodeParquetSQL(outputDir string, nodeFile *fileManifest, outputPath string) string {
	return fmt.Sprintf(`COPY (
  WITH nodes AS (
%s
  )
  SELECT
    id,
    json_extract_string(properties, '$.objectid') AS objectid,
    kinds,
    properties
  FROM nodes
) TO %s (FORMAT parquet, COMPRESSION %s);`,
		indentSQL(nodeSourceSQL(outputDir, nodeFile), 4),
		sqlStringLiteral(absoluteCollectionPath(outputDir, outputPath)),
		parquetCompression,
	)
}

func buildEdgeParquetSQL(outputDir string, nodeFile *fileManifest, edgeFile *fileManifest, outputPath string) string {
	return fmt.Sprintf(`COPY (
  WITH nodes AS (
%s
  ),
  edges AS (
%s
  ),
  decorated_nodes AS (
    SELECT
      id,
      json_extract_string(properties, '$.objectid') AS objectid
    FROM nodes
  )
  SELECT
    start_nodes.objectid || '_' || coalesce(e.kind, '') || '_' || end_nodes.objectid AS id,
    e.start_id,
    e.end_id,
    start_nodes.objectid AS start_objectid,
    end_nodes.objectid AS end_objectid,
    e.kind,
    e.properties
  FROM edges e
  LEFT JOIN decorated_nodes start_nodes ON e.start_id = start_nodes.id
  LEFT JOIN decorated_nodes end_nodes ON e.end_id = end_nodes.id
) TO %s (FORMAT parquet, COMPRESSION %s);`,
		indentSQL(nodeSourceSQL(outputDir, nodeFile), 4),
		indentSQL(edgeSourceSQL(outputDir, edgeFile), 4),
		sqlStringLiteral(absoluteCollectionPath(outputDir, outputPath)),
		parquetCompression,
	)
}

func nodeSourceSQL(outputDir string, nodeFile *fileManifest) string {
	if nodeFile == nil {
		return "SELECT NULL::VARCHAR AS id, CAST([] AS VARCHAR[]) AS kinds, NULL::JSON AS properties WHERE false"
	}
	return fmt.Sprintf("SELECT id::VARCHAR AS id, kinds::VARCHAR[] AS kinds, properties::JSON AS properties FROM read_ndjson(%s, columns = {id: 'VARCHAR', kinds: 'VARCHAR[]', properties: 'JSON'}, compression = 'auto_detect')", sqlStringLiteral(absoluteCollectionPath(outputDir, nodeFile.Path)))
}

func edgeSourceSQL(outputDir string, edgeFile *fileManifest) string {
	if edgeFile == nil {
		return "SELECT NULL::VARCHAR AS start_id, NULL::VARCHAR AS end_id, NULL::VARCHAR AS kind, NULL::JSON AS properties WHERE false"
	}
	return fmt.Sprintf("SELECT start_id::VARCHAR AS start_id, end_id::VARCHAR AS end_id, kind::VARCHAR AS kind, properties::JSON AS properties FROM read_ndjson(%s, columns = {start_id: 'VARCHAR', end_id: 'VARCHAR', kind: 'VARCHAR', properties: 'JSON'}, compression = 'auto_detect')", sqlStringLiteral(absoluteCollectionPath(outputDir, edgeFile.Path)))
}

func absoluteCollectionPath(outputDir, relativePath string) string {
	return filepath.ToSlash(filepath.Join(outputDir, filepath.FromSlash(relativePath)))
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func indentSQL(value string, spaces int) string {
	prefix := strings.Repeat(" ", spaces)
	lines := strings.Split(value, "\n")
	for idx, line := range lines {
		lines[idx] = prefix + line
	}
	return strings.Join(lines, "\n")
}

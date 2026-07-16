package retriever

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"path"

	parquetgo "github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
)

const parquetFragmentFormat = "Parquet"

type parquetNode struct {
	ID         string   `parquet:"id,zstd"`
	Kinds      []string `parquet:"kinds,zstd"`
	Properties string   `parquet:"properties,json,zstd"`
}

type parquetEdge struct {
	ID         string `parquet:"id,zstd"`
	StartID    string `parquet:"start_id,zstd"`
	EndID      string `parquet:"end_id,zstd"`
	Kind       string `parquet:"kind,zstd"`
	Properties string `parquet:"properties,json,zstd"`
}

type parquetFragmentMetadata struct {
	Path   string
	Rows   int
	Bytes  int64
	SHA256 string
}

func (s parquetFragmentMetadata) rowCount() int {
	return s.Rows
}

type parquetFragmentSink[T, P any] struct {
	workspace collectionWorkspace
	phase     Phase
	adapt     func(T) (P, error)
}

func newParquetNodeSinkInWorkspace(workspace collectionWorkspace) parquetFragmentSink[normalizedNode, parquetNode] {
	return parquetFragmentSink[normalizedNode, parquetNode]{
		workspace: workspace,
		phase:     PhaseNodes,
		adapt:     parquetNodeFromNormalized,
	}
}

func newParquetEdgeSinkInWorkspace(workspace collectionWorkspace) parquetFragmentSink[normalizedEdge, parquetEdge] {
	return parquetFragmentSink[normalizedEdge, parquetEdge]{
		workspace: workspace,
		phase:     PhaseEdges,
		adapt:     parquetEdgeFromNormalized,
	}
}

func (s parquetFragmentSink[T, P]) Open(ctx context.Context, id shardID) (fragmentWriter[T, parquetFragmentMetadata], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id.Phase != s.phase {
		return nil, fmt.Errorf("Parquet sink for phase %q cannot open shard for phase %q", s.phase, id.Phase)
	}

	relativePath, err := parquetFragmentPath(id.Graph, id.Phase, id.Number)
	if err != nil {
		return nil, err
	}
	artifact, err := s.workspace.Stage(ctx, relativePath)
	if err != nil {
		return nil, err
	}
	hasher := sha256.New()
	counter := &countingWriter{writer: io.MultiWriter(artifact, hasher)}
	writer := parquetgo.NewGenericWriter[P](counter, parquetgo.Compression(&parquetzstd.Codec{}))
	return &parquetFragmentWriter[T, P]{
		relativePath: relativePath,
		adapt:        s.adapt,
		artifact:     artifact,
		writer:       writer,
		counter:      counter,
		hasher:       hasher,
	}, nil
}

type parquetFragmentWriter[T, P any] struct {
	relativePath string
	adapt        func(T) (P, error)
	artifact     stagedWorkspaceFile
	writer       *parquetgo.GenericWriter[P]
	counter      *countingWriter
	hasher       hash.Hash
	rows         int
	state        parquetWriterState
}

type parquetWriterState uint8

const (
	parquetWriterOpen parquetWriterState = iota
	parquetWriterPrepared
	parquetWriterAborted
)

func (s *parquetFragmentWriter[T, P]) WriteBatch(ctx context.Context, records []T) error {
	if s.state != parquetWriterOpen {
		return fmt.Errorf("write Parquet fragment after prepare or abort")
	}

	rows := make([]P, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, err := s.adapt(record)
		if err != nil {
			return fmt.Errorf("adapt Parquet row %d: %w", s.rows+index+1, err)
		}
		rows[index] = row
	}
	written, err := s.writer.Write(rows)
	s.rows += written
	if err != nil {
		return fmt.Errorf("write Parquet rows: %w", err)
	}
	if written != len(rows) {
		return fmt.Errorf("write Parquet rows: wrote %d of %d", written, len(rows))
	}
	return nil
}

func (s *parquetFragmentWriter[T, P]) Prepare(ctx context.Context) (preparedFragment[parquetFragmentMetadata], error) {
	if s.state != parquetWriterOpen {
		return nil, fmt.Errorf("prepare Parquet fragment more than once or after abort")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.state = parquetWriterPrepared

	if err := s.writer.Close(); err != nil {
		s.state = parquetWriterAborted
		return nil, cleanupOnError(fmt.Errorf("finish Parquet fragment: %w", err), s.artifact.Abort)
	}
	if err := s.artifact.Close(); err != nil {
		s.state = parquetWriterAborted
		return nil, cleanupOnError(fmt.Errorf("close Parquet fragment: %w", err), s.artifact.Abort)
	}

	return &preparedParquetFragment{
		artifact: s.artifact,
		metadata: parquetFragmentMetadata{
			Path:   s.relativePath,
			Rows:   s.rows,
			Bytes:  s.counter.count,
			SHA256: hex.EncodeToString(s.hasher.Sum(nil)),
		},
	}, nil
}

func (s *parquetFragmentWriter[T, P]) Abort() error {
	switch s.state {
	case parquetWriterOpen:
		s.state = parquetWriterAborted
		return s.artifact.Abort()
	case parquetWriterAborted:
		return nil
	default:
		return fmt.Errorf("abort Parquet writer after prepare")
	}
}

type preparedParquetFragment struct {
	artifact stagedWorkspaceFile
	metadata parquetFragmentMetadata
	state    preparedParquetState
}

type preparedParquetState uint8

const (
	parquetFragmentPrepared preparedParquetState = iota
	parquetFragmentCommitted
	parquetFragmentAborted
)

func (s *preparedParquetFragment) Metadata() parquetFragmentMetadata {
	return s.metadata
}

func (s *preparedParquetFragment) Commit(ctx context.Context) error {
	if s.state != parquetFragmentPrepared {
		return fmt.Errorf("commit Parquet fragment that is not prepared")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.artifact.Commit(ctx); err != nil {
		s.state = parquetFragmentAborted
		return cleanupOnError(err, s.artifact.Abort)
	}
	s.state = parquetFragmentCommitted
	return nil
}

func (s *preparedParquetFragment) Abort() error {
	switch s.state {
	case parquetFragmentPrepared:
		s.state = parquetFragmentAborted
		return s.artifact.Abort()
	case parquetFragmentAborted:
		return nil
	default:
		return fmt.Errorf("abort committed Parquet fragment")
	}
}

func parquetNodeFromNormalized(node normalizedNode) (parquetNode, error) {
	properties, err := parquetProperties(node.Properties)
	if err != nil {
		return parquetNode{}, err
	}
	return parquetNode{ID: node.ID, Kinds: node.Kinds, Properties: properties}, nil
}

func parquetEdgeFromNormalized(edge normalizedEdge) (parquetEdge, error) {
	properties, err := parquetProperties(edge.Properties)
	if err != nil {
		return parquetEdge{}, err
	}
	return parquetEdge{
		ID:         edge.ID,
		StartID:    edge.StartID,
		EndID:      edge.EndID,
		Kind:       edge.Kind,
		Properties: properties,
	}, nil
}

func parquetProperties(properties map[string]any) (string, error) {
	if len(properties) == 0 {
		return "null", nil
	}
	payload, err := json.Marshal(properties)
	if err != nil {
		return "", fmt.Errorf("encode properties as JSON: %w", err)
	}
	return string(payload), nil
}

func parquetFragmentPath(graphName string, phase Phase, shardNumber int) (string, error) {
	if shardNumber <= 0 {
		return "", fmt.Errorf("shard number must be > 0")
	}

	var prefix string
	switch phase {
	case PhaseNodes:
		prefix = "nodes"
	case PhaseEdges:
		prefix = "edges"
	default:
		return "", fmt.Errorf("unsupported fragment phase %q", phase)
	}
	return path.Join("parquet", "graphs", graphDirectoryName(graphName), fmt.Sprintf("%s-%06d.parquet", prefix, shardNumber)), nil
}

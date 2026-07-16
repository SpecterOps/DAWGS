package retriever

import (
	"context"
	"fmt"
	"path"
)

type jsonlFragmentSink[T any] struct {
	workspace collectionWorkspace
	codec     CompressionCodec
	zstdLevel int
	phase     Phase
	adapt     func(T) any
}

type jsonlFragmentMetadata struct {
	Path              string
	Rows              int
	CompressedBytes   int64
	UncompressedBytes int64
	SHA256            string
}

func (s jsonlFragmentMetadata) rowCount() int {
	return s.Rows
}

func newJSONLNodeSink(options DumpOptions) jsonlFragmentSink[normalizedNode] {
	return newJSONLNodeSinkInWorkspace(options, newLocalCollectionWorkspace(options.OutputDir, options.Force))
}

func newJSONLNodeSinkInWorkspace(options DumpOptions, workspace collectionWorkspace) jsonlFragmentSink[normalizedNode] {
	return newJSONLFragmentSink(options, workspace, PhaseNodes, func(record normalizedNode) any {
		return jsonlV1NodeFromNormalized(record)
	})
}

func newJSONLEdgeSink(options DumpOptions) jsonlFragmentSink[normalizedEdge] {
	return newJSONLEdgeSinkInWorkspace(options, newLocalCollectionWorkspace(options.OutputDir, options.Force))
}

func newJSONLEdgeSinkInWorkspace(options DumpOptions, workspace collectionWorkspace) jsonlFragmentSink[normalizedEdge] {
	return newJSONLFragmentSink(options, workspace, PhaseEdges, func(record normalizedEdge) any {
		return jsonlV1EdgeFromNormalized(record)
	})
}

func newJSONLFragmentSink[T any](options DumpOptions, workspace collectionWorkspace, phase Phase, adapt func(T) any) jsonlFragmentSink[T] {
	return jsonlFragmentSink[T]{
		workspace: workspace,
		codec:     options.Compression,
		zstdLevel: options.ZstdLevel,
		phase:     phase,
		adapt:     adapt,
	}
}

func (s jsonlFragmentSink[T]) Open(ctx context.Context, id shardID) (fragmentWriter[T, jsonlFragmentMetadata], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id.Phase != s.phase {
		return nil, fmt.Errorf("JSONL sink for phase %q cannot open shard for phase %q", s.phase, id.Phase)
	}

	relativePath, err := jsonlFragmentPath(id.Graph, id.Phase, id.Number, s.codec)
	if err != nil {
		return nil, err
	}
	writer, err := newCompressedJSONLinesWriterInWorkspace(
		ctx,
		s.workspace,
		relativePath,
		s.codec,
		s.zstdLevel,
	)
	if err != nil {
		return nil, err
	}

	return &jsonlFragmentWriter[T]{
		relativePath: relativePath,
		adapt:        s.adapt,
		writer:       writer,
	}, nil
}

type jsonlFragmentWriter[T any] struct {
	relativePath string
	adapt        func(T) any
	writer       *compressedJSONLinesWriter
}

func (s *jsonlFragmentWriter[T]) WriteBatch(ctx context.Context, records []T) error {
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.writer.Write(s.adapt(record)); err != nil {
			return err
		}
	}
	return nil
}

func (s *jsonlFragmentWriter[T]) Prepare(ctx context.Context) (preparedFragment[jsonlFragmentMetadata], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	prepared, err := s.writer.Prepare()
	if err != nil {
		return nil, err
	}
	encoding := prepared.Metadata()
	metadata := jsonlFragmentMetadata{
		Path:              s.relativePath,
		Rows:              encoding.Rows,
		CompressedBytes:   encoding.CompressedBytes,
		UncompressedBytes: encoding.UncompressedBytes,
		SHA256:            encoding.SHA256,
	}
	return &preparedJSONLFragment{
		fragment: prepared,
		metadata: metadata,
	}, nil
}

func (s *jsonlFragmentWriter[T]) Abort() error {
	return s.writer.Abort()
}

type preparedJSONLFragment struct {
	fragment *preparedCompressedJSONLinesFragment
	metadata jsonlFragmentMetadata
}

func (s *preparedJSONLFragment) Metadata() jsonlFragmentMetadata {
	return s.metadata
}

func (s *preparedJSONLFragment) Commit(ctx context.Context) error {
	return s.fragment.Commit(ctx)
}

func (s *preparedJSONLFragment) Abort() error {
	return s.fragment.Abort()
}

func jsonlFragmentPath(graphName string, fragmentPhase Phase, shardNumber int, codec CompressionCodec) (string, error) {
	if shardNumber <= 0 {
		return "", fmt.Errorf("shard number must be > 0")
	}

	extension, err := compressionExtension(codec)
	if err != nil {
		return "", err
	}

	var prefix string
	switch fragmentPhase {
	case PhaseNodes:
		prefix = "nodes"
	case PhaseEdges:
		prefix = "edges"
	default:
		return "", fmt.Errorf("unsupported fragment phase %q", fragmentPhase)
	}

	return path.Join("graphs", graphDirectoryName(graphName), fmt.Sprintf("%s-%06d.jsonl%s", prefix, shardNumber, extension)), nil
}

func writeNodeFragment(outputDir, graphName string, shardNumber int, options DumpOptions, items []FragmentNode, actionCounts map[string]int) (FileManifest, error) {
	options.OutputDir = outputDir
	sink := newJSONLFragmentSink(options, newLocalCollectionWorkspace(outputDir, options.Force), PhaseNodes, func(record FragmentNode) any {
		return record
	})
	summary := shardSummary{
		ID:           shardID{Graph: graphName, Phase: PhaseNodes, Number: shardNumber},
		Rows:         len(items),
		ActionCounts: actionCounts,
	}
	metadata, err := writeFragment(context.Background(), sink, summary, items)
	if err != nil {
		return FileManifest{}, err
	}
	return newJSONLFileManifest(summary, metadata), nil
}

func writeEdgeFragment(outputDir, graphName string, shardNumber int, options DumpOptions, items []FragmentEdge, actionCounts map[string]int) (FileManifest, error) {
	options.OutputDir = outputDir
	sink := newJSONLFragmentSink(options, newLocalCollectionWorkspace(outputDir, options.Force), PhaseEdges, func(record FragmentEdge) any {
		return record
	})
	summary := shardSummary{
		ID:           shardID{Graph: graphName, Phase: PhaseEdges, Number: shardNumber},
		Rows:         len(items),
		ActionCounts: actionCounts,
	}
	metadata, err := writeFragment(context.Background(), sink, summary, items)
	if err != nil {
		return FileManifest{}, err
	}
	return newJSONLFileManifest(summary, metadata), nil
}

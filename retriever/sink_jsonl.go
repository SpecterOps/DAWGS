package retriever

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
)

type jsonlFragmentSink[T any] struct {
	outputDir string
	codec     CompressionCodec
	zstdLevel int
	phase     Phase
	adapt     func(T) any
}

func newJSONLNodeSink(options DumpOptions) jsonlFragmentSink[normalizedNode] {
	return newJSONLFragmentSink(options, PhaseNodes, func(record normalizedNode) any {
		return jsonlV1NodeFromNormalized(record)
	})
}

func newJSONLEdgeSink(options DumpOptions) jsonlFragmentSink[normalizedEdge] {
	return newJSONLFragmentSink(options, PhaseEdges, func(record normalizedEdge) any {
		return jsonlV1EdgeFromNormalized(record)
	})
}

func newJSONLFragmentSink[T any](options DumpOptions, phase Phase, adapt func(T) any) jsonlFragmentSink[T] {
	return jsonlFragmentSink[T]{
		outputDir: options.OutputDir,
		codec:     options.Compression,
		zstdLevel: options.ZstdLevel,
		phase:     phase,
		adapt:     adapt,
	}
}

func (s jsonlFragmentSink[T]) Open(ctx context.Context, spec shardSpec) (fragmentWriter[T, FileManifest], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spec.Phase != s.phase {
		return nil, fmt.Errorf("JSONL sink for phase %q cannot open shard for phase %q", s.phase, spec.Phase)
	}

	relativePath, err := jsonlFragmentPath(spec.Graph, spec.Phase, spec.Number, s.codec)
	if err != nil {
		return nil, err
	}
	writer, err := newCompressedJSONLinesWriter(
		filepath.Join(s.outputDir, filepath.FromSlash(relativePath)),
		s.codec,
		s.zstdLevel,
	)
	if err != nil {
		return nil, err
	}

	return &jsonlFragmentWriter[T]{
		spec:         spec,
		relativePath: relativePath,
		adapt:        s.adapt,
		writer:       writer,
	}, nil
}

type jsonlFragmentWriter[T any] struct {
	spec         shardSpec
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

func (s *jsonlFragmentWriter[T]) Prepare(ctx context.Context) (preparedFragment[FileManifest], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	prepared, err := s.writer.Prepare()
	if err != nil {
		return nil, err
	}
	metadata := prepared.Metadata()
	if metadata.Count != s.spec.ExpectedRows {
		_ = prepared.Abort()
		return nil, fmt.Errorf("prepared JSONL shard %d has %d rows, expected %d", s.spec.Number, metadata.Count, s.spec.ExpectedRows)
	}

	metadata.Phase = s.spec.Phase
	metadata.Path = s.relativePath
	metadata.ActionCounts = cloneActionCounts(s.spec.ActionCounts)
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
	metadata FileManifest
}

func (s *preparedJSONLFragment) Metadata() FileManifest {
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
	sink := newJSONLFragmentSink(options, PhaseNodes, func(record FragmentNode) any {
		return record
	})
	return writeFragment(context.Background(), sink, shardSpec{
		Graph:        graphName,
		Phase:        PhaseNodes,
		Number:       shardNumber,
		ExpectedRows: len(items),
		ActionCounts: actionCounts,
	}, items)
}

func writeEdgeFragment(outputDir, graphName string, shardNumber int, options DumpOptions, items []FragmentEdge, actionCounts map[string]int) (FileManifest, error) {
	options.OutputDir = outputDir
	sink := newJSONLFragmentSink(options, PhaseEdges, func(record FragmentEdge) any {
		return record
	})
	return writeFragment(context.Background(), sink, shardSpec{
		Graph:        graphName,
		Phase:        PhaseEdges,
		Number:       shardNumber,
		ExpectedRows: len(items),
		ActionCounts: actionCounts,
	}, items)
}

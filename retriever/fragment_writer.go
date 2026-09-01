package retriever

import (
	"fmt"
	"os"
)

type fragmentWriter[T any] struct {
	jsonl *compressedJSONLinesWriter

	parquet *parquetFragmentSink[T]

	jsonlPath          string
	jsonlStagingPath   string
	parquetPath        string
	parquetStagingPath string

	count  int
	closed bool
}

func newNodeFragmentWriter(jsonlPath, parquetPath string, options DumpOptions) (*fragmentWriter[FragmentNode], error) {
	return newFragmentWriter(jsonlPath, parquetPath, options, newNodeParquetSink)
}

func newEdgeFragmentWriter(jsonlPath, parquetPath string, options DumpOptions) (*fragmentWriter[FragmentEdge], error) {
	return newFragmentWriter(jsonlPath, parquetPath, options, newEdgeParquetSink)
}

func newFragmentWriter[T any](jsonlPath, parquetPath string, options DumpOptions, newParquetSink func(string) (*parquetFragmentSink[T], error)) (*fragmentWriter[T], error) {
	if !options.Parquet {
		jsonl, err := newCompressedJSONLinesWriter(jsonlPath, options.Compression, options.ZstdLevel)
		if err != nil {
			return nil, err
		}

		return &fragmentWriter[T]{
			jsonl: jsonl,
		}, nil
	}

	jsonlStagingPath := jsonlPath + ".tmp"
	jsonl, err := newCompressedJSONLinesWriterAtPaths(jsonlPath, jsonlStagingPath, options.Compression, options.ZstdLevel)
	if err != nil {
		return nil, err
	}

	parquetStagingPath := parquetPath + ".tmp"
	parquet, err := newParquetSink(parquetStagingPath)
	if err != nil {
		jsonl.Abort()
		_ = os.Remove(jsonlStagingPath)
		return nil, err
	}

	return &fragmentWriter[T]{
		jsonl:              jsonl,
		parquet:            parquet,
		jsonlPath:          jsonlPath,
		jsonlStagingPath:   jsonlStagingPath,
		parquetPath:        parquetPath,
		parquetStagingPath: parquetStagingPath,
	}, nil
}

func (s *fragmentWriter[T]) Write(fragment T) error {
	if s.closed {
		return fmt.Errorf("write closed fragment")
	}

	if err := s.jsonl.Write(fragment); err != nil {
		s.Abort()
		return err
	}
	if s.parquet != nil {
		if err := s.parquet.Write(fragment); err != nil {
			s.Abort()
			return err
		}
	}

	s.count++
	return nil
}

func (s *fragmentWriter[T]) Count() int {
	return s.count
}

func (s *fragmentWriter[T]) Close() (FileManifest, error) {
	if s.closed {
		return FileManifest{}, fmt.Errorf("close fragment more than once")
	}
	s.closed = true

	if s.parquet == nil {
		return s.jsonl.Close()
	}

	fileEntry, err := s.jsonl.finalize()
	if err != nil {
		s.cleanup(false, false)
		return FileManifest{}, err
	}
	if fileEntry.Count != s.count {
		s.cleanup(false, false)
		return FileManifest{}, fmt.Errorf("JSONL fragment count %d does not match logical count %d", fileEntry.Count, s.count)
	}
	if err := s.parquet.Close(); err != nil {
		s.cleanup(false, false)
		return FileManifest{}, fmt.Errorf("close Parquet fragment: %w", err)
	}
	if err := os.Rename(s.jsonlStagingPath, s.jsonlPath); err != nil {
		s.cleanup(false, false)
		return FileManifest{}, fmt.Errorf("publish JSONL fragment: %w", err)
	}
	if err := os.Rename(s.parquetStagingPath, s.parquetPath); err != nil {
		s.cleanup(true, false)
		return FileManifest{}, fmt.Errorf("publish Parquet fragment: %w", err)
	}

	return fileEntry, nil
}

func (s *fragmentWriter[T]) Abort() {
	if s.closed {
		return
	}
	s.closed = true
	s.cleanup(false, false)
}

func (s *fragmentWriter[T]) cleanup(jsonlPublished, parquetPublished bool) {
	s.jsonl.Abort()
	if s.parquet != nil {
		s.parquet.Abort()
	}

	_ = os.Remove(s.jsonlStagingPath)
	_ = os.Remove(s.parquetStagingPath)
	if jsonlPublished {
		_ = os.Remove(s.jsonlPath)
	}
	if parquetPublished {
		_ = os.Remove(s.parquetPath)
	}
}

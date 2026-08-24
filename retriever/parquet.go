package retriever

import (
	"errors"
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

type parquetNodeRow struct {
	ID         string   `parquet:"id"`
	Kinds      []string `parquet:"kinds,list"`
	Properties any      `parquet:"properties,variant"`
}

type parquetEdgeRow struct {
	StartID    string `parquet:"start_id"`
	EndID      string `parquet:"end_id"`
	Kind       string `parquet:"kind"`
	Properties any    `parquet:"properties,variant"`
}

type parquetFragmentSink[T any] struct {
	path   string
	write  func(T) error
	close  func() error
	abort  func()
	closed bool
}

func newNodeParquetSink(path string) (*parquetFragmentSink[FragmentNode], error) {
	return newParquetFragmentSink(path, func(fragment FragmentNode) parquetNodeRow {
		return parquetNodeRow{
			ID:         fragment.ID,
			Kinds:      fragment.Kinds,
			Properties: fragment.Properties,
		}
	})
}

func newEdgeParquetSink(path string) (*parquetFragmentSink[FragmentEdge], error) {
	return newParquetFragmentSink(path, func(fragment FragmentEdge) parquetEdgeRow {
		return parquetEdgeRow{
			StartID:    fragment.StartID,
			EndID:      fragment.EndID,
			Kind:       fragment.Kind,
			Properties: fragment.Properties,
		}
	})
}

func newParquetFragmentSink[T, R any](path string, adapt func(T) R) (*parquetFragmentSink[T], error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open parquet fragment: %w", err)
	}

	writer := parquet.NewGenericWriter[R](file)
	return &parquetFragmentSink[T]{
		path: path,
		write: func(fragment T) error {
			written, err := writer.Write([]R{adapt(fragment)})
			if err != nil {
				return fmt.Errorf("write parquet row: %w", err)
			}
			if written != 1 {
				return fmt.Errorf("write parquet row: wrote %d rows, want 1", written)
			}
			return nil
		},
		close: func() error {
			return errors.Join(writer.Close(), file.Close())
		},
		abort: func() {
			defer func() { _ = os.Remove(path) }()
			defer func() { _ = file.Close() }()
			defer func() { _ = recover() }()
			_ = writer.Close()
		},
	}, nil
}

func (s *parquetFragmentSink[T]) Write(fragment T) (err error) {
	if s.closed {
		return fmt.Errorf("write closed parquet fragment")
	}
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("write parquet fragment: %v", value)
		}
	}()
	return s.write(fragment)
}

func (s *parquetFragmentSink[T]) Close() error {
	if s.closed {
		return fmt.Errorf("close parquet fragment more than once")
	}
	s.closed = true
	return s.close()
}

func (s *parquetFragmentSink[T]) Abort() {
	if s.closed {
		return
	}
	s.closed = true
	defer func() { _ = recover() }()
	s.abort()
}

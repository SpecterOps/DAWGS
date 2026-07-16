package retriever

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type leafSinkContract[T any, M fragmentMetadata] struct {
	New    func(*testing.T) (fragmentSink[T, M], shardID, string)
	Record T
}

func TestJSONLFragmentSinkConformance(t *testing.T) {
	t.Run("nodes", func(t *testing.T) {
		runLeafSinkContract(t, leafSinkContract[normalizedNode, jsonlFragmentMetadata]{
			New: func(t *testing.T) (fragmentSink[normalizedNode, jsonlFragmentMetadata], shardID, string) {
				return newJSONLNodeSinkContract(t)
			},
			Record: normalizedNode{ID: "1", Kinds: []string{"User"}},
		})
	})

	t.Run("edges", func(t *testing.T) {
		runLeafSinkContract(t, leafSinkContract[normalizedEdge, jsonlFragmentMetadata]{
			New: func(t *testing.T) (fragmentSink[normalizedEdge, jsonlFragmentMetadata], shardID, string) {
				options := DumpOptions{
					OutputDir:   t.TempDir(),
					Compression: CompressionGzip,
					ZstdLevel:   DefaultZstdLevel,
				}
				id := shardID{Graph: "graph/name", Phase: PhaseEdges, Number: 2}
				relativePath, err := jsonlFragmentPath(id.Graph, id.Phase, id.Number, options.Compression)
				if err != nil {
					t.Fatalf("fragment path: %v", err)
				}
				return newJSONLEdgeSink(options), id, filepath.Join(options.OutputDir, filepath.FromSlash(relativePath))
			},
			Record: normalizedEdge{StartID: "1", EndID: "2", Kind: "MemberOf"},
		})
	})
}

func newJSONLNodeSinkContract(t *testing.T) (fragmentSink[normalizedNode, jsonlFragmentMetadata], shardID, string) {
	t.Helper()
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
	}
	id := shardID{Graph: "graph/name", Phase: PhaseNodes, Number: 2}
	relativePath, err := jsonlFragmentPath(id.Graph, id.Phase, id.Number, options.Compression)
	if err != nil {
		t.Fatalf("fragment path: %v", err)
	}
	return newJSONLNodeSink(options), id, filepath.Join(options.OutputDir, filepath.FromSlash(relativePath))
}

func runLeafSinkContract[T any, M fragmentMetadata](t *testing.T, contract leafSinkContract[T, M]) {
	t.Helper()

	t.Run("prepare and commit transfer ownership", func(t *testing.T) {
		sink, id, finalPath := contract.New(t)
		writer := openContractWriter(t, sink, id)
		if err := writer.WriteBatch(context.Background(), []T{contract.Record}); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		prepared, err := writer.Prepare(context.Background())
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if prepared.Metadata().rowCount() != 1 {
			t.Fatalf("prepared rows = %d", prepared.Metadata().rowCount())
		}
		assertPathAbsent(t, finalPath)
		assertPathPresent(t, finalPath+".tmp")

		assertLeafSinkError(t, writer.WriteBatch(context.Background(), []T{contract.Record}), nil)
		_, err = writer.Prepare(context.Background())
		assertLeafSinkError(t, err, nil)
		assertLeafSinkError(t, writer.Abort(), nil)

		if err := prepared.Commit(context.Background()); err != nil {
			t.Fatalf("commit: %v", err)
		}
		assertPathPresent(t, finalPath)
		assertPathAbsent(t, finalPath+".tmp")
		assertLeafSinkError(t, prepared.Commit(context.Background()), nil)
		assertLeafSinkError(t, prepared.Abort(), nil)
	})

	t.Run("writer abort is repeatable", func(t *testing.T) {
		sink, id, finalPath := contract.New(t)
		writer := openContractWriter(t, sink, id)
		if err := writer.WriteBatch(context.Background(), []T{contract.Record}); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		if err := writer.Abort(); err != nil {
			t.Fatalf("abort writer: %v", err)
		}
		if err := writer.Abort(); err != nil {
			t.Fatalf("repeat writer abort: %v", err)
		}
		assertPathAbsent(t, finalPath)
		assertPathAbsent(t, finalPath+".tmp")
		assertLeafSinkError(t, writer.WriteBatch(context.Background(), []T{contract.Record}), nil)
		_, err := writer.Prepare(context.Background())
		assertLeafSinkError(t, err, nil)
	})

	t.Run("prepared abort is repeatable", func(t *testing.T) {
		sink, id, finalPath := contract.New(t)
		writer := openContractWriter(t, sink, id)
		if err := writer.WriteBatch(context.Background(), []T{contract.Record}); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		prepared, err := writer.Prepare(context.Background())
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if err := prepared.Abort(); err != nil {
			t.Fatalf("abort prepared fragment: %v", err)
		}
		if err := prepared.Abort(); err != nil {
			t.Fatalf("repeat prepared abort: %v", err)
		}
		assertPathAbsent(t, finalPath)
		assertPathAbsent(t, finalPath+".tmp")
		assertLeafSinkError(t, prepared.Commit(context.Background()), nil)
	})

	t.Run("context cancellation", func(t *testing.T) {
		t.Run("open", func(t *testing.T) {
			sink, id, _ := contract.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := sink.Open(ctx, id)
			assertLeafSinkError(t, err, context.Canceled)
		})

		t.Run("write", func(t *testing.T) {
			sink, id, finalPath := contract.New(t)
			writer := openContractWriter(t, sink, id)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			assertLeafSinkError(t, writer.WriteBatch(ctx, []T{contract.Record}), context.Canceled)
			if err := writer.Abort(); err != nil {
				t.Fatalf("abort canceled writer: %v", err)
			}
			assertPathAbsent(t, finalPath+".tmp")
		})

		t.Run("prepare", func(t *testing.T) {
			sink, id, finalPath := contract.New(t)
			writer := openContractWriter(t, sink, id)
			if err := writer.WriteBatch(context.Background(), []T{contract.Record}); err != nil {
				t.Fatalf("write batch: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := writer.Prepare(ctx)
			assertLeafSinkError(t, err, context.Canceled)
			if err := writer.Abort(); err != nil {
				t.Fatalf("abort canceled writer: %v", err)
			}
			assertPathAbsent(t, finalPath+".tmp")
		})

		t.Run("commit", func(t *testing.T) {
			sink, id, finalPath := contract.New(t)
			writer := openContractWriter(t, sink, id)
			if err := writer.WriteBatch(context.Background(), []T{contract.Record}); err != nil {
				t.Fatalf("write batch: %v", err)
			}
			prepared, err := writer.Prepare(context.Background())
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			assertLeafSinkError(t, prepared.Commit(ctx), context.Canceled)
			if err := prepared.Abort(); err != nil {
				t.Fatalf("abort canceled commit: %v", err)
			}
			assertPathAbsent(t, finalPath)
			assertPathAbsent(t, finalPath+".tmp")
		})
	})
}

func openContractWriter[T any, M fragmentMetadata](t *testing.T, sink fragmentSink[T, M], id shardID) fragmentWriter[T, M] {
	t.Helper()
	writer, err := sink.Open(context.Background(), id)
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	return writer
}

func assertLeafSinkError(t *testing.T, err error, cause error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected leaf sink error")
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("leaf sink error = %v, want cause %v", err, cause)
	}
}

func assertPathPresent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("path %q is absent: %v", path, err)
	}
}

func assertPathAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("path %q exists or cannot be inspected: %v", path, err)
	}
}

type fragmentFailureWorkspace struct {
	collectionWorkspace
	stageErr error
	artifact stagedWorkspaceFile
}

func (s fragmentFailureWorkspace) Stage(context.Context, string) (stagedWorkspaceFile, error) {
	if s.stageErr != nil {
		return nil, s.stageErr
	}
	return s.artifact, nil
}

type fragmentFailureArtifact struct {
	bytes.Buffer
	closeErr  error
	commitErr error
	abortErr  error
	aborted   bool
}

func (s *fragmentFailureArtifact) Close() error {
	return s.closeErr
}

func (s *fragmentFailureArtifact) Commit(context.Context) error {
	return s.commitErr
}

func (s *fragmentFailureArtifact) Abort() error {
	s.aborted = true
	return s.abortErr
}

type failingJSONValue struct {
	err error
}

func (s failingJSONValue) MarshalJSON() ([]byte, error) {
	return nil, s.err
}

func TestJSONLFragmentSinkFailureConformance(t *testing.T) {
	id := shardID{Graph: "graph/name", Phase: PhaseNodes, Number: 7}
	options := DumpOptions{Compression: CompressionGzip, ZstdLevel: DefaultZstdLevel}
	newSink := func(workspace collectionWorkspace, adapt func(int) any) jsonlFragmentSink[int] {
		return newJSONLFragmentSink(options, workspace, PhaseNodes, adapt)
	}

	t.Run("open", func(t *testing.T) {
		cause := errors.New("stage failed")
		sink := newSink(fragmentFailureWorkspace{stageErr: cause}, func(value int) any { return value })
		_, err := sink.Open(context.Background(), id)
		assertLeafSinkError(t, err, cause)
	})

	t.Run("write", func(t *testing.T) {
		cause := errors.New("encode failed")
		artifact := &fragmentFailureArtifact{}
		sink := newSink(fragmentFailureWorkspace{artifact: artifact}, func(int) any { return failingJSONValue{err: cause} })
		writer := openContractWriter(t, sink, id)
		assertLeafSinkError(t, writer.WriteBatch(context.Background(), []int{1}), cause)
		if err := writer.Abort(); err != nil {
			t.Fatalf("abort failed write: %v", err)
		}
	})

	t.Run("prepare", func(t *testing.T) {
		cause := errors.New("close failed")
		cleanupCause := errors.New("prepare cleanup failed")
		artifact := &fragmentFailureArtifact{closeErr: cause, abortErr: cleanupCause}
		sink := newSink(fragmentFailureWorkspace{artifact: artifact}, func(value int) any { return value })
		writer := openContractWriter(t, sink, id)
		if err := writer.WriteBatch(context.Background(), []int{1}); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		_, err := writer.Prepare(context.Background())
		assertLeafSinkError(t, err, cause)
		if !errors.Is(err, cleanupCause) {
			t.Fatalf("prepare error does not retain cleanup failure: %v", err)
		}
		if !artifact.aborted {
			t.Fatalf("prepare failure did not abort staged artifact")
		}
		if err := writer.Abort(); err != nil {
			t.Fatalf("repeat cleanup after prepare failure: %v", err)
		}
	})

	t.Run("commit", func(t *testing.T) {
		cause := errors.New("commit failed")
		cleanupCause := errors.New("commit cleanup failed")
		artifact := &fragmentFailureArtifact{commitErr: cause, abortErr: cleanupCause}
		sink := newSink(fragmentFailureWorkspace{artifact: artifact}, func(value int) any { return value })
		writer := openContractWriter(t, sink, id)
		if err := writer.WriteBatch(context.Background(), []int{1}); err != nil {
			t.Fatalf("write batch: %v", err)
		}
		prepared, err := writer.Prepare(context.Background())
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		err = prepared.Commit(context.Background())
		assertLeafSinkError(t, err, cause)
		if !errors.Is(err, cleanupCause) {
			t.Fatalf("commit error does not retain cleanup failure: %v", err)
		}
		if !artifact.aborted {
			t.Fatalf("commit failure did not abort staged artifact")
		}
		if err := prepared.Abort(); err != nil {
			t.Fatalf("repeat cleanup after commit failure: %v", err)
		}
	})

	t.Run("abort", func(t *testing.T) {
		cause := errors.New("abort failed")
		artifact := &fragmentFailureArtifact{abortErr: cause}
		sink := newSink(fragmentFailureWorkspace{artifact: artifact}, func(value int) any { return value })
		writer := openContractWriter(t, sink, id)
		assertLeafSinkError(t, writer.Abort(), cause)
	})
}

package retriever

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/graph"
)

type fragmentFailurePoint string

const (
	fragmentFailureOpen    fragmentFailurePoint = "open"
	fragmentFailurePrepare fragmentFailurePoint = "prepare"
	fragmentFailureCommit  fragmentFailurePoint = "commit"
)

type fragmentLifecycle struct {
	opened          bool
	wrote           bool
	prepared        bool
	writerAborted   bool
	preparedAborted bool
}

type failingNodeSink struct {
	point     fragmentFailurePoint
	failure   error
	lifecycle *fragmentLifecycle
}

func (s failingNodeSink) Open(context.Context, shardID) (fragmentWriter[normalizedNode, jsonlFragmentMetadata], error) {
	if s.point == fragmentFailureOpen {
		return nil, s.failure
	}
	s.lifecycle.opened = true
	return &failingNodeWriter{
		point:     s.point,
		failure:   s.failure,
		lifecycle: s.lifecycle,
	}, nil
}

type failingNodeWriter struct {
	point     fragmentFailurePoint
	failure   error
	lifecycle *fragmentLifecycle
}

func (s *failingNodeWriter) WriteBatch(context.Context, []normalizedNode) error {
	s.lifecycle.wrote = true
	return nil
}

func (s *failingNodeWriter) Prepare(context.Context) (preparedFragment[jsonlFragmentMetadata], error) {
	if s.point == fragmentFailurePrepare {
		return nil, s.failure
	}
	s.lifecycle.prepared = true
	return &failingPreparedNodeFragment{
		failure:   s.failure,
		lifecycle: s.lifecycle,
	}, nil
}

func (s *failingNodeWriter) Abort() error {
	s.lifecycle.writerAborted = true
	return nil
}

type failingPreparedNodeFragment struct {
	failure   error
	lifecycle *fragmentLifecycle
}

func (s *failingPreparedNodeFragment) Metadata() jsonlFragmentMetadata {
	return jsonlFragmentMetadata{Path: "unused", Rows: 1, SHA256: "unused"}
}

func (s *failingPreparedNodeFragment) Commit(context.Context) error {
	return s.failure
}

func (s *failingPreparedNodeFragment) Abort() error {
	s.lifecycle.preparedAborted = true
	return nil
}

type publishFailureWorkspace struct {
	collectionWorkspace
	failure error
}

func (s publishFailureWorkspace) Publish(context.Context, string, []byte) (string, error) {
	return "", s.failure
}

func TestDumpFragmentFailuresDoNotPublishManifest(t *testing.T) {
	for _, point := range []fragmentFailurePoint{
		fragmentFailureOpen,
		fragmentFailurePrepare,
		fragmentFailureCommit,
	} {
		t.Run(string(point), func(t *testing.T) {
			outputDir := t.TempDir()
			failure := errors.New("injected " + string(point) + " failure")
			lifecycle := &fragmentLifecycle{}
			source := oneNodeGraphSource()

			_, err := runDump(
				context.Background(),
				source,
				"failure-test",
				[]GraphTarget{{Name: "source"}},
				DefaultDumpOptions(outputDir),
				dumpOverrides{
					nodeSink: failingNodeSink{
						point:     point,
						failure:   failure,
						lifecycle: lifecycle,
					},
				},
			)
			if !errors.Is(err, failure) {
				t.Fatalf("dump error = %v, want injected failure", err)
			}
			assertNoPublishedManifest(t, outputDir)

			switch point {
			case fragmentFailureOpen:
				if lifecycle.opened || lifecycle.wrote || lifecycle.prepared {
					t.Fatalf("open failure lifecycle = %+v", lifecycle)
				}
			case fragmentFailurePrepare:
				if !lifecycle.opened || !lifecycle.wrote || !lifecycle.writerAborted || lifecycle.prepared {
					t.Fatalf("prepare failure lifecycle = %+v", lifecycle)
				}
			case fragmentFailureCommit:
				if !lifecycle.opened || !lifecycle.wrote || !lifecycle.prepared || !lifecycle.preparedAborted {
					t.Fatalf("commit failure lifecycle = %+v", lifecycle)
				}
			}
		})
	}
}

func TestDumpPublishFailureLeavesFragmentsWithoutManifest(t *testing.T) {
	outputDir := t.TempDir()
	failure := errors.New("injected publish failure")
	workspace := publishFailureWorkspace{
		collectionWorkspace: newLocalCollectionWorkspace(outputDir, false),
		failure:             failure,
	}

	_, err := runDump(
		context.Background(),
		oneNodeGraphSource(),
		"failure-test",
		[]GraphTarget{{Name: "source"}},
		DefaultDumpOptions(outputDir),
		dumpOverrides{workspace: workspace},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("dump error = %v, want injected failure", err)
	}
	assertNoPublishedManifest(t, outputDir)

	fragments, err := filepath.Glob(filepath.Join(outputDir, "graphs", "source", "nodes-*.jsonl.zst"))
	if err != nil {
		t.Fatalf("find committed fragments: %v", err)
	}
	if len(fragments) != 1 {
		t.Fatalf("committed fragments = %v, want one", fragments)
	}
}

func oneNodeGraphSource() *scriptedGraphSource {
	return &scriptedGraphSource{
		snapshot: graphEntitySnapshot{NodeCount: 1},
		nodeBatches: [][]*graph.Node{{
			graph.NewNode(1, nil, graph.StringKind("User")),
		}},
	}
}

func assertNoPublishedManifest(t *testing.T, outputDir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(outputDir, manifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("manifest should not be published, stat error = %v", err)
	}
}

package retriever

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type collectionWorkspace interface {
	Root() string
	Prepare(context.Context) error
	Stage(context.Context, string) (stagedWorkspaceFile, error)
	Publish(context.Context, string, []byte) (string, error)
}

type stagedWorkspaceFile interface {
	io.Writer
	Close() error
	Commit(context.Context) error
	Abort() error
}

type localCollectionWorkspace struct {
	root  string
	force bool
}

func newLocalCollectionWorkspace(root string, force bool) *localCollectionWorkspace {
	return &localCollectionWorkspace{root: root, force: force}
}

func (s *localCollectionWorkspace) Root() string {
	return s.root
}

func (s *localCollectionWorkspace) Prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	info, err := os.Stat(s.root)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("output path %q exists and is not a directory", s.root)
		}

		entries, err := os.ReadDir(s.root)
		if err != nil {
			return fmt.Errorf("read output directory: %w", err)
		}

		if len(entries) > 0 {
			if !s.force {
				return fmt.Errorf("output directory %q is not empty; pass -force to replace it", s.root)
			}

			if err := os.RemoveAll(s.root); err != nil {
				return fmt.Errorf("replace output directory: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect output directory: %w", err)
	}

	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	return nil
}

func (s *localCollectionWorkspace) Stage(ctx context.Context, relativePath string) (stagedWorkspaceFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	finalPath, err := s.resolve(relativePath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return nil, fmt.Errorf("create artifact directory: %w", err)
	}

	tempPath := finalPath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open staged artifact: %w", err)
	}

	return &localStagedWorkspaceFile{
		file:      file,
		finalPath: finalPath,
		tempPath:  tempPath,
	}, nil
}

func (s *localCollectionWorkspace) Publish(ctx context.Context, relativePath string, payload []byte) (string, error) {
	artifact, err := s.Stage(ctx, relativePath)
	if err != nil {
		return "", err
	}

	if _, err := artifact.Write(payload); err != nil {
		return "", cleanupOnError(fmt.Errorf("write staged artifact: %w", err), artifact.Abort)
	}
	if err := artifact.Close(); err != nil {
		return "", cleanupOnError(fmt.Errorf("close staged artifact: %w", err), artifact.Abort)
	}
	if err := artifact.Commit(ctx); err != nil {
		return "", cleanupOnError(err, artifact.Abort)
	}

	return s.resolve(relativePath)
}

func (s *localCollectionWorkspace) resolve(relativePath string) (string, error) {
	if relativePath == "" || path.Clean(relativePath) != relativePath || strings.ContainsRune(relativePath, '\\') || !filepath.IsLocal(filepath.FromSlash(relativePath)) {
		return "", fmt.Errorf("unsafe workspace path %q", relativePath)
	}

	return filepath.Join(s.root, filepath.FromSlash(relativePath)), nil
}

type localStagedWorkspaceFile struct {
	file      *os.File
	finalPath string
	tempPath  string
	closed    bool
	committed bool
	aborted   bool
}

func (s *localStagedWorkspaceFile) Write(payload []byte) (int, error) {
	if s.closed || s.committed || s.aborted {
		return 0, fmt.Errorf("write staged artifact after close, commit, or abort")
	}
	return s.file.Write(payload)
}

func (s *localStagedWorkspaceFile) Close() error {
	if s.committed || s.aborted {
		return fmt.Errorf("close staged artifact after commit or abort")
	}
	if s.closed {
		return nil
	}

	s.closed = true
	return s.file.Close()
}

func (s *localStagedWorkspaceFile) Commit(ctx context.Context) error {
	if s.committed || s.aborted || !s.closed {
		return fmt.Errorf("commit artifact that is not staged and closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.Rename(s.tempPath, s.finalPath); err != nil {
		return fmt.Errorf("commit staged artifact: %w", err)
	}
	s.committed = true
	return nil
}

func (s *localStagedWorkspaceFile) Abort() error {
	if s.committed {
		return fmt.Errorf("abort committed artifact")
	}
	if s.aborted {
		return nil
	}

	s.aborted = true
	var closeErr error
	if !s.closed {
		s.closed = true
		closeErr = s.file.Close()
	}
	return collectErrors(closeErr, removeStagedArtifact(s.tempPath))
}

func removeStagedArtifact(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func prepareOutputDirectory(outputDir string, force bool) error {
	return newLocalCollectionWorkspace(outputDir, force).Prepare(context.Background())
}

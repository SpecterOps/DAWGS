package archive

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/specterops/dawgs/ret/observe"
	"github.com/stretchr/testify/require"
)

func TestCreatePublishesAuthenticatedArchiveExclusively(t *testing.T) {
	// Break caught: publishing an incomplete envelope, using a predictable
	// temporary name, or replacing a destination that appeared before Create.
	root := writeArchiveTestCollection(t, true, true)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	destinationParent := t.TempDir()
	archivePath := filepath.Join(destinationParent, "collection.ret")
	collision := filepath.Join(destinationParent, ".collection.ret.create-collision.tmp")
	require.NoError(t, os.WriteFile(collision, []byte("preserve"), 0o600))

	require.NoError(t, Create(context.Background(), CreateConfig{
		CollectionDirectory: root,
		ArchivePath:         archivePath,
		Recipient:           recipient,
	}))

	info, err := os.Stat(archivePath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Equal(t, []byte("preserve"), mustReadArchiveTestFile(t, collision))
	require.Equal(t, []string{collision}, archiveCreateTemporaryPaths(t, archivePath))

	file, err := os.Open(archivePath)
	require.NoError(t, err)
	decrypted, err := newDecryptReader(file, identity)
	require.NoError(t, err)
	contents, err := io.ReadAll(decrypted)
	require.NoError(t, err)
	require.NoError(t, decrypted.Close())
	require.NoError(t, file.Close())

	reader := tar.NewReader(bytes.NewReader(contents))
	var names []string
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		names = append(names, header.Name)
		source := filepath.Join(root, filepath.FromSlash(header.Name))
		require.Equal(t, mustReadArchiveTestFile(t, source), mustReadAllArchiveTest(t, reader))
	}
	require.Equal(t, []string{
		"graphs/example/nodes/000001.jsonl",
		"graphs/example/nodes/000001.parquet",
		"graphs/example/relationships/000001.jsonl",
		"graphs/example/relationships/000001.parquet",
		"manifest.json",
	}, names)

	before := mustReadArchiveTestFile(t, archivePath)
	err = Create(context.Background(), CreateConfig{
		CollectionDirectory: root,
		ArchivePath:         archivePath,
		Recipient:           recipient,
	})
	require.ErrorContains(t, err, "exists")
	require.Equal(t, before, mustReadArchiveTestFile(t, archivePath))
	require.Equal(t, []string{collision}, archiveCreateTemporaryPaths(t, archivePath))
}

func TestCreateRejectsArchiveLexicallyOrPhysicallyInsideCollection(t *testing.T) {
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)

	t.Run("lexical", func(t *testing.T) {
		root := writeArchiveTestCollection(t, true, false)
		archivePath := filepath.Join(root, "archive.ret")

		err := Create(context.Background(), CreateConfig{
			CollectionDirectory: root,
			ArchivePath:         archivePath,
			Recipient:           recipient,
		})

		require.ErrorContains(t, err, "inside")
		require.NoFileExists(t, archivePath)
		require.Empty(t, archiveCreateTemporaryPaths(t, archivePath))
	})

	t.Run("physical through symlinked parent", func(t *testing.T) {
		root := writeArchiveTestCollection(t, true, false)
		physicalParent := filepath.Join(root, "physical-output")
		require.NoError(t, os.Mkdir(physicalParent, 0o700))
		linkParent := filepath.Join(t.TempDir(), "linked-output")
		if err := os.Symlink(physicalParent, linkParent); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		archivePath := filepath.Join(linkParent, "archive.ret")

		err := Create(context.Background(), CreateConfig{
			CollectionDirectory: root,
			ArchivePath:         archivePath,
			Recipient:           recipient,
		})

		require.ErrorContains(t, err, "inside")
		require.NoFileExists(t, filepath.Join(physicalParent, "archive.ret"))
		require.Empty(t, archiveCreateTemporaryPaths(t, archivePath))
	})
}

func TestCreateFailureLeavesNoArchiveOrTemporaryFile(t *testing.T) {
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)

	tests := []struct {
		name    string
		prepare func(*testing.T, string) context.Context
	}{
		{
			name: "full collection verification failure",
			prepare: func(t *testing.T, root string) context.Context {
				manifest := readArchiveTestManifest(t, root)
				parquetPath := manifest.Graphs[0].NodeShards[0].Parquet.Path
				require.NoError(t, os.WriteFile(
					filepath.Join(root, filepath.FromSlash(parquetPath)),
					[]byte("corrupt"),
					0o600,
				))
				return context.Background()
			},
		},
		{
			name: "context cancelled before start",
			prepare: func(_ *testing.T, _ string) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeArchiveTestCollection(t, true, true)
			ctx := test.prepare(t, root)
			archivePath := filepath.Join(t.TempDir(), "collection.ret")

			err := Create(ctx, CreateConfig{
				CollectionDirectory: root,
				ArchivePath:         archivePath,
				Recipient:           recipient,
			})

			require.Error(t, err)
			require.NoFileExists(t, archivePath)
			require.Empty(t, archiveCreateTemporaryPaths(t, archivePath))
		})
	}
}

func TestCreateObserverCancellationSanitizesTemporaryBeforeQuarantine(t *testing.T) {
	root := writeArchiveTestCollection(t, true, true)
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)
	archiveParent := t.TempDir()
	archivePath := filepath.Join(archiveParent, "collection.ret")
	ctx, cancel := context.WithCancel(context.Background())
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if _, ok := event.(observe.ArchiveEntryProcessed); ok {
			cancel()
		}
	})

	err = Create(ctx, CreateConfig{
		CollectionDirectory: root,
		ArchivePath:         archivePath,
		Recipient:           recipient,
		Observer:            observer,
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "ownership cleanup")
	require.NoFileExists(t, archivePath)
	require.Empty(t, archiveCreateTemporaryPaths(t, archivePath))
	quarantines := archiveCleanupQuarantinePaths(t, archiveParent)
	require.Len(t, quarantines, 1)
	info, statErr := os.Stat(quarantines[0])
	require.NoError(t, statErr)
	require.True(t, info.Mode().IsRegular())
	require.Zero(t, info.Size())
}

func TestCreatePreservesTemporaryWhenSanitizationFails(t *testing.T) {
	// Break caught: quarantining a partially written archive when retained-
	// handle truncate or sync reports failure.
	tests := []struct {
		name        string
		operations  archiveOperations
		wantPayload bool
		match       string
	}{
		{
			name: "truncate",
			operations: archiveOperations{
				truncateOwnedFile: func(_ *os.File, _ int64) error {
					return errors.New("injected truncate failure")
				},
			},
			wantPayload: true,
			match:       "injected truncate failure",
		},
		{
			name: "sync",
			operations: archiveOperations{
				syncOwnedFile: func(_ *os.File) error {
					return errors.New("injected sync failure")
				},
			},
			match: "injected sync failure",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeArchiveTestCollection(t, true, true)
			recipient, _, err := GenerateKeyPair()
			require.NoError(t, err)
			archiveParent := t.TempDir()
			archivePath := filepath.Join(archiveParent, "collection.ret")
			ctx, cancel := context.WithCancel(context.Background())
			observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
				if _, ok := event.(observe.ArchiveEntryProcessed); ok {
					cancel()
				}
			})

			err = create(ctx, CreateConfig{
				CollectionDirectory: root,
				ArchivePath:         archivePath,
				Recipient:           recipient,
				Observer:            observer,
			}, runtime.GOOS, test.operations)

			require.ErrorIs(t, err, context.Canceled)
			require.ErrorContains(t, err, test.match)
			require.ErrorContains(t, err, "ownership cleanup")
			require.NoFileExists(t, archivePath)
			temporary := archiveCreateTemporaryPaths(t, archivePath)
			require.Len(t, temporary, 1)
			payload := mustReadArchiveTestFile(t, temporary[0])
			if test.wantPayload {
				require.NotEmpty(t, payload)
			} else {
				require.Empty(t, payload)
			}
			require.Empty(t, archiveCleanupQuarantinePaths(t, archiveParent))
		})
	}
}

func TestCreatePreservesDestinationCreatedWhileStreaming(t *testing.T) {
	// Break caught: replacing a destination created after the initial
	// exclusivity check but before archive publication.
	root := writeArchiveTestCollection(t, true, true)
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := filepath.Join(t.TempDir(), "collection.ret")
	created := false
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if _, ok := event.(observe.ArchiveEntryProcessed); ok && !created {
			created = true
			require.NoError(t, os.WriteFile(archivePath, []byte("concurrent owner"), 0o600))
		}
	})

	err = Create(context.Background(), CreateConfig{
		CollectionDirectory: root,
		ArchivePath:         archivePath,
		Recipient:           recipient,
		Observer:            observer,
	})

	require.Error(t, err)
	require.True(t, created)
	require.Equal(t, []byte("concurrent owner"), mustReadArchiveTestFile(t, archivePath))
	require.Empty(t, archiveCreateTemporaryPaths(t, archivePath))
}

func TestCreateRejectsTemporaryFileReplacedWhileStreaming(t *testing.T) {
	// Break caught: publishing a different file substituted at the temporary
	// pathname while Create still holds the original temporary file open.
	root := writeArchiveTestCollection(t, true, true)
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := filepath.Join(t.TempDir(), "collection.ret")
	replaced := false
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		entry, ok := event.(observe.ArchiveEntryProcessed)
		if !ok || replaced || entry.Path != "manifest.json" {
			return
		}
		temporary := archiveCreateTemporaryPaths(t, archivePath)
		require.Len(t, temporary, 1)
		require.NoError(t, os.Remove(temporary[0]))
		require.NoError(t, os.WriteFile(temporary[0], []byte("substituted"), 0o600))
		replaced = true
	})

	err = Create(context.Background(), CreateConfig{
		CollectionDirectory: root,
		ArchivePath:         archivePath,
		Recipient:           recipient,
		Observer:            observer,
	})

	require.ErrorContains(t, err, "temporary archive changed")
	require.ErrorContains(t, err, "ownership cleanup")
	require.True(t, replaced)
	require.NoFileExists(t, archivePath)
	temporary := archiveCreateTemporaryPaths(t, archivePath)
	require.Len(t, temporary, 1)
	require.Equal(t, []byte("substituted"), mustReadArchiveTestFile(t, temporary[0]))
}

func TestRequireDirectoryPathIdentityRejectsReplacedPath(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "parent")
	require.NoError(t, os.Mkdir(parent, 0o700))
	pinned, err := os.Open(parent)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pinned.Close())
	})
	moved := parent + ".moved"
	require.NoError(t, os.Rename(parent, moved))
	require.NoError(t, os.Mkdir(parent, 0o700))

	require.ErrorContains(
		t,
		requireDirectoryPathIdentity(parent, pinned),
		"changed",
	)
}

func archiveCreateTemporaryPaths(t *testing.T, archivePath string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(
		filepath.Dir(archivePath),
		"."+filepath.Base(archivePath)+".create-*.tmp",
	))
	require.NoError(t, err)
	return matches
}

func mustReadArchiveTestFile(t *testing.T, path string) []byte {
	t.Helper()
	value, err := os.ReadFile(path)
	require.NoError(t, err)
	return value
}

func mustReadAllArchiveTest(t *testing.T, reader io.Reader) []byte {
	t.Helper()
	value, err := io.ReadAll(reader)
	require.NoError(t, err)
	return value
}

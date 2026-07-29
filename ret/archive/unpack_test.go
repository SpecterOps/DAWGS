package archive

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/stretchr/testify/require"
)

type archiveTestTarEntry struct {
	header  tar.Header
	payload []byte
}

type archiveTestBoundedZeroReader struct {
	remaining  int
	maxRequest int
}

type archiveTestReadFunc func([]byte) (int, error)

func (s archiveTestReadFunc) Read(destination []byte) (int, error) {
	return s(destination)
}

func (s *archiveTestBoundedZeroReader) Read(destination []byte) (int, error) {
	if len(destination) > s.maxRequest {
		return 0, errors.New("read request exceeded fixed bound")
	}
	if s.remaining == 0 {
		return 0, io.EOF
	}
	count := min(len(destination), s.remaining)
	clear(destination[:count])
	s.remaining -= count
	return count, nil
}

func TestExtractPromotesOnlyACompleteVerifiedCollection(t *testing.T) {
	for _, outputs := range []struct {
		name              string
		jsonl, parquet    bool
		expectedFileCount int
	}{
		{name: "JSONL only", jsonl: true, expectedFileCount: 3},
		{name: "Parquet only", parquet: true, expectedFileCount: 3},
		{name: "dual", jsonl: true, parquet: true, expectedFileCount: 5},
	} {
		t.Run(outputs.name, func(t *testing.T) {
			source := writeArchiveTestCollection(t, outputs.jsonl, outputs.parquet)
			recipient, identity, err := GenerateKeyPair()
			require.NoError(t, err)
			archivePath := writeIndependentArchiveTestEnvelope(
				t,
				recipient,
				archiveTestRegularEntries(t, source),
			)
			output := filepath.Join(t.TempDir(), "collection")

			require.NoError(t, Extract(context.Background(), ExtractConfig{
				ArchivePath:     archivePath,
				OutputDirectory: output,
				Identity:        identity,
			}))

			_, err = collection.Verify(context.Background(), output, nil)
			require.NoError(t, err)
			require.Len(t, archiveTestRegularEntries(t, output), outputs.expectedFileCount)
			require.Empty(t, archiveExtractStagePaths(t, output))
			requireArchiveTestTreesEqual(t, source, output)
		})
	}
}

func TestExtractRejectsUnsafeTarEntriesWithoutFilesystemEscape(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	tests := []struct {
		name   string
		entry  archiveTestTarEntry
		match  string
		escape string
	}{
		{
			name: "absolute path",
			entry: archiveTestEntry(
				filepath.Join(string(filepath.Separator), "tmp", "ret-archive-absolute-escape"),
				[]byte("bad"),
			),
			match: "absolute",
		},
		{
			name:   "parent traversal",
			entry:  archiveTestEntry("../escape", []byte("bad")),
			match:  "traverses",
			escape: "escape",
		},
		{
			name:  "unclean path",
			entry: archiveTestEntry("graphs/../escape", []byte("bad")),
			match: "not clean",
		},
		{
			name:  "backslash path",
			entry: archiveTestEntry(`graphs\escape`, []byte("bad")),
			match: "backslash",
		},
		{
			name: "symlink",
			entry: archiveTestTarEntry{header: tar.Header{
				Name:     collection.ManifestName,
				Typeflag: tar.TypeSymlink,
				Linkname: "target",
			}},
			match: "regular",
		},
		{
			name: "hardlink",
			entry: archiveTestTarEntry{header: tar.Header{
				Name:     collection.ManifestName,
				Typeflag: tar.TypeLink,
				Linkname: "target",
			}},
			match: "regular",
		},
		{
			name: "character device",
			entry: archiveTestTarEntry{header: tar.Header{
				Name:     collection.ManifestName,
				Typeflag: tar.TypeChar,
				Devmajor: 1,
				Devminor: 3,
			}},
			match: "regular",
		},
		{
			name: "directory",
			entry: archiveTestTarEntry{header: tar.Header{
				Name:     "graphs",
				Typeflag: tar.TypeDir,
				Mode:     0o700,
			}},
			match: "regular",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := t.TempDir()
			output := filepath.Join(parent, "collection")
			archivePath := writeIndependentArchiveTestEnvelope(t, recipient, []archiveTestTarEntry{test.entry})

			err := Extract(context.Background(), ExtractConfig{
				ArchivePath:     archivePath,
				OutputDirectory: output,
				Identity:        identity,
			})

			require.ErrorContains(t, err, test.match)
			require.NoFileExists(t, output)
			require.NoDirExists(t, output)
			require.Empty(t, archiveExtractStagePaths(t, output))
			if test.escape != "" {
				require.NoFileExists(t, filepath.Join(parent, test.escape))
			}
		})
	}
}

func TestExtractRejectsSparsePAXMetadata(t *testing.T) {
	// Break caught: allowing archive/tar to expand a tiny authenticated GNU
	// sparse payload into an attacker-selected logical file size on disk.
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	entry := archiveTestEntry(collection.ManifestName, []byte("abc"))
	entry.header.Format = tar.FormatPAX
	entry.header.PAXRecords = map[string]string{
		"ABC.sparse.map":       "0,3",
		"ABC.sparse.numblocks": "1",
		"ABC.sparse.size":      "3",
	}
	plaintext := archiveTestTarPayload(t, []archiveTestTarEntry{entry})
	for _, field := range []string{"map", "numblocks", "size"} {
		before := []byte("ABC.sparse." + field)
		after := []byte("GNU.sparse." + field)
		require.True(t, bytes.Contains(plaintext, before))
		plaintext = bytes.ReplaceAll(plaintext, before, after)
	}
	archivePath := writeIndependentArchiveTestEnvelopePayload(t, recipient, plaintext)
	output := filepath.Join(t.TempDir(), "collection")

	err = Extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
	})

	require.ErrorContains(t, err, "sparse")
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
}

func TestExtractRejectsDuplicateAndNonDeclaredFileSets(t *testing.T) {
	source := writeArchiveTestCollection(t, true, true)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	valid := archiveTestRegularEntries(t, source)
	require.Greater(t, len(valid), 2)

	tests := []struct {
		name    string
		entries []archiveTestTarEntry
		match   string
	}{
		{
			name:    "duplicate",
			entries: append(append([]archiveTestTarEntry(nil), valid...), valid[0]),
			match:   "duplicate",
		},
		{
			name:    "undeclared file",
			entries: append(append([]archiveTestTarEntry(nil), valid...), archiveTestEntry("extra.txt", []byte("extra"))),
			match:   "unexpected",
		},
		{
			name: "missing manifest",
			entries: func() []archiveTestTarEntry {
				result := append([]archiveTestTarEntry(nil), valid...)
				for index := range result {
					if result[index].header.Name == collection.ManifestName {
						return append(result[:index], result[index+1:]...)
					}
				}
				t.Fatal("valid archive did not contain manifest")
				return nil
			}(),
			match: "manifest",
		},
		{
			name:    "missing artifact",
			entries: append([]archiveTestTarEntry(nil), valid[1:]...),
			match:   "missing",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "collection")
			archivePath := writeIndependentArchiveTestEnvelope(t, recipient, test.entries)

			err := Extract(context.Background(), ExtractConfig{
				ArchivePath:     archivePath,
				OutputDirectory: output,
				Identity:        identity,
			})

			require.ErrorContains(t, err, test.match)
			require.NoDirExists(t, output)
			require.NoFileExists(t, output)
			require.Empty(t, archiveExtractStagePaths(t, output))
		})
	}
}

func TestExtractRejectsAuthenticatedCorruptParquetBeforePromotion(t *testing.T) {
	source := writeArchiveTestCollection(t, true, true)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	entries := archiveTestRegularEntries(t, source)
	for index := range entries {
		if filepath.Ext(entries[index].header.Name) == ".parquet" {
			entries[index].payload = []byte("authenticated but corrupt parquet")
			entries[index].header.Size = int64(len(entries[index].payload))
			break
		}
	}
	output := filepath.Join(t.TempDir(), "collection")
	archivePath := writeIndependentArchiveTestEnvelope(t, recipient, entries)

	err = Extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
	})

	require.Error(t, err)
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
}

func TestExtractAuthenticatesFinalFrameBeforePromotion(t *testing.T) {
	source := writeArchiveTestCollection(t, true, false)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)
	wire := mustReadArchiveTestFile(t, archivePath)
	_, _, frames := parseTestEnvelope(t, wire)
	require.GreaterOrEqual(t, len(frames), 2)
	require.NoError(t, os.WriteFile(archivePath, wire[:frames[len(frames)-1].start], 0o600))
	output := filepath.Join(t.TempDir(), "collection")

	err = Extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
	})

	require.ErrorContains(t, err, "final frame")
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
}

func TestRequireZeroTarTailUsesBoundedStreamingReads(t *testing.T) {
	// Break caught: buffering an attacker-controlled authenticated zero trailer
	// after the TAR terminator instead of scanning it with fixed memory.
	reader := &archiveTestBoundedZeroReader{
		remaining:  4 * 1024 * 1024,
		maxRequest: 64 * 1024,
	}

	require.NoError(t, requireZeroTarTail(reader))
	require.Zero(t, reader.remaining)
	require.ErrorContains(t, requireZeroTarTail(bytes.NewReader([]byte{0, 0, 1, 0})), "nonzero")
}

func TestExtractPreservesAnyExistingOutput(t *testing.T) {
	source := writeArchiveTestCollection(t, true, false)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)

	t.Run("directory", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "collection")
		require.NoError(t, os.Mkdir(output, 0o700))
		marker := filepath.Join(output, "preserve")
		require.NoError(t, os.WriteFile(marker, []byte("preserve"), 0o600))

		err := Extract(context.Background(), ExtractConfig{
			ArchivePath:     archivePath,
			OutputDirectory: output,
			Identity:        identity,
		})

		require.ErrorContains(t, err, "exists")
		require.Equal(t, []byte("preserve"), mustReadArchiveTestFile(t, marker))
		require.Empty(t, archiveExtractStagePaths(t, output))
	})

	t.Run("file", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "collection")
		require.NoError(t, os.WriteFile(output, []byte("preserve"), 0o600))

		err := Extract(context.Background(), ExtractConfig{
			ArchivePath:     archivePath,
			OutputDirectory: output,
			Identity:        identity,
		})

		require.ErrorContains(t, err, "exists")
		require.Equal(t, []byte("preserve"), mustReadArchiveTestFile(t, output))
		require.Empty(t, archiveExtractStagePaths(t, output))
	})
}

func TestExtractObserverCancellationSanitizesStageBeforeQuarantine(t *testing.T) {
	source := writeArchiveTestCollection(t, true, true)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)
	outputParent := t.TempDir()
	output := filepath.Join(outputParent, "collection")
	ctx, cancel := context.WithCancel(context.Background())
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if entry, ok := event.(observe.ArchiveEntryProcessed); ok && entry.Operation == "unpack" {
			cancel()
		}
	})

	err = Extract(ctx, ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
		Observer:        observer,
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "ownership cleanup")
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
	quarantines := archiveCleanupQuarantinePaths(t, outputParent)
	require.Len(t, quarantines, 1)
	info, statErr := os.Stat(quarantines[0])
	require.NoError(t, statErr)
	require.True(t, info.IsDir())
	entries, readErr := os.ReadDir(quarantines[0])
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestExtractCleanupPreservesReplacedStageAndCleansOwnedContents(t *testing.T) {
	// Break caught: recursively deleting a replacement installed at the stage
	// pathname instead of cleaning only the pinned stage created by Extract.
	source := writeArchiveTestCollection(t, true, false)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)
	parent := t.TempDir()
	output := filepath.Join(parent, "collection")
	movedStage := filepath.Join(parent, "owned-stage-moved")
	ctx, cancel := context.WithCancel(context.Background())
	replaced := false
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		entry, ok := event.(observe.ArchiveEntryProcessed)
		if !ok || entry.Operation != "unpack" || replaced {
			return
		}
		stages := archiveExtractStagePaths(t, output)
		require.Len(t, stages, 1)
		require.NoError(t, os.Rename(stages[0], movedStage))
		require.NoError(t, os.Mkdir(stages[0], 0o700))
		require.NoError(t, os.WriteFile(
			filepath.Join(stages[0], "replacement-marker"),
			[]byte("preserve replacement"),
			0o600,
		))
		replaced = true
		cancel()
	})

	err = Extract(ctx, ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
		Observer:        observer,
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "ownership cleanup")
	require.True(t, replaced)
	require.NoDirExists(t, output)
	stages := archiveExtractStagePaths(t, output)
	require.Len(t, stages, 1)
	require.Equal(
		t,
		[]byte("preserve replacement"),
		mustReadArchiveTestFile(t, filepath.Join(stages[0], "replacement-marker")),
	)
	ownedEntries, readErr := os.ReadDir(movedStage)
	require.NoError(t, readErr)
	require.Empty(t, ownedEntries)
}

func TestCreateExtractStagePreservesNameWhenPinningFails(t *testing.T) {
	// Break caught: removing a stage using stale FileInfo when no retained root
	// was obtained, or after the retained root could not be proven identical.
	tests := []struct {
		name       string
		operations archiveOperations
		match      string
		mismatch   bool
		nilInfo    bool
		nonDir     bool
	}{
		{
			name: "open root",
			operations: archiveOperations{
				openRoot: func(_ *os.Root, _ string) (*os.Root, error) {
					return nil, errors.New("injected open root failure")
				},
			},
			match: "injected open root failure",
		},
		{
			name: "stat pinned root",
			operations: archiveOperations{
				statRoot: func(_ *os.Root) (fs.FileInfo, error) {
					return nil, errors.New("injected pinned stat failure")
				},
			},
			match: "injected pinned stat failure",
		},
		{
			name:     "mismatched pinned root",
			match:    "identity changed",
			mismatch: true,
		},
		{
			name:    "nil pinned info",
			match:   "identity changed",
			nilInfo: true,
		},
		{
			name:   "non-directory pinned info",
			match:  "identity changed",
			nonDir: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parentPath := t.TempDir()
			parent, err := os.OpenRoot(parentPath)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, parent.Close())
			})
			operations := test.operations
			if test.mismatch {
				differentPath := t.TempDir()
				different, statErr := os.Stat(differentPath)
				require.NoError(t, statErr)
				operations.statRoot = func(_ *os.Root) (fs.FileInfo, error) {
					return different, nil
				}
			}
			if test.nilInfo {
				operations.statRoot = func(_ *os.Root) (fs.FileInfo, error) {
					return nil, nil
				}
			}
			if test.nonDir {
				differentPath := filepath.Join(t.TempDir(), "regular")
				require.NoError(t, os.WriteFile(differentPath, []byte("regular"), 0o600))
				different, statErr := os.Stat(differentPath)
				require.NoError(t, statErr)
				operations.statRoot = func(_ *os.Root) (fs.FileInfo, error) {
					return different, nil
				}
			}

			name, root, _, err := createExtractStage(parent, "collection", operations)

			require.ErrorContains(t, err, test.match)
			require.ErrorContains(t, err, "ownership cleanup")
			require.Nil(t, root)
			require.NotEmpty(t, name)
			info, statErr := parent.Lstat(name)
			require.NoError(t, statErr)
			require.True(t, info.IsDir())
		})
	}
}

func TestExtractExclusiveCleanupPreservesReplacementEntry(t *testing.T) {
	// Break caught: unlinking a replacement file when extraction of the owned
	// file fails after its pathname has been substituted.
	stagePath := t.TempDir()
	stage, err := os.OpenRoot(stagePath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, stage.Close())
	})
	entryPath := filepath.Join(stagePath, "artifact.bin")
	replaced := false
	source := archiveTestReadFunc(func(_ []byte) (int, error) {
		require.NoError(t, os.Remove(entryPath))
		require.NoError(t, os.WriteFile(entryPath, []byte("preserve replacement"), 0o600))
		replaced = true
		return 0, errors.New("source failed")
	})

	err = extractExclusive(stage, "artifact.bin", 32, source, archiveOperations{})

	require.ErrorContains(t, err, "source failed")
	require.ErrorContains(t, err, "ownership cleanup")
	require.True(t, replaced)
	require.Equal(t, []byte("preserve replacement"), mustReadArchiveTestFile(t, entryPath))
}

func TestExtractExclusiveSanitizesIncompleteEntryBeforeQuarantine(t *testing.T) {
	stagePath := t.TempDir()
	stage, err := os.OpenRoot(stagePath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, stage.Close())
	})
	source := io.MultiReader(
		bytes.NewReader([]byte("partial secret")),
		archiveTestReadFunc(func(_ []byte) (int, error) {
			return 0, errors.New("source failed")
		}),
	)

	err = extractExclusive(stage, "artifact.bin", 32, source, archiveOperations{})

	require.ErrorContains(t, err, "source failed")
	require.ErrorContains(t, err, "ownership cleanup")
	require.NoFileExists(t, filepath.Join(stagePath, "artifact.bin"))
	quarantines := archiveCleanupQuarantinePaths(t, stagePath)
	require.Len(t, quarantines, 1)
	info, statErr := os.Stat(quarantines[0])
	require.NoError(t, statErr)
	require.True(t, info.Mode().IsRegular())
	require.Zero(t, info.Size())
}

func TestExtractExclusivePreservesIncompleteEntryWhenSanitizationFails(t *testing.T) {
	// Break caught: quarantining partial artifact bytes when retained-handle
	// truncate or sync reports failure.
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
			stagePath := t.TempDir()
			stage, err := os.OpenRoot(stagePath)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, stage.Close())
			})
			source := io.MultiReader(
				bytes.NewReader([]byte("partial secret")),
				archiveTestReadFunc(func(_ []byte) (int, error) {
					return 0, errors.New("source failed")
				}),
			)

			err = extractExclusive(stage, "artifact.bin", 32, source, test.operations)

			require.ErrorContains(t, err, "source failed")
			require.ErrorContains(t, err, test.match)
			require.ErrorContains(t, err, "ownership cleanup")
			payload := mustReadArchiveTestFile(t, filepath.Join(stagePath, "artifact.bin"))
			if test.wantPayload {
				require.Equal(t, []byte("partial secret"), payload)
			} else {
				require.Empty(t, payload)
			}
			require.Empty(t, archiveCleanupQuarantinePaths(t, stagePath))
		})
	}
}

func TestExtractPreservesDestinationCreatedBeforePromotion(t *testing.T) {
	// Break caught: replacing a destination created after extraction started
	// but before the verified stage was promoted.
	source := writeArchiveTestCollection(t, true, true)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)
	output := filepath.Join(t.TempDir(), "collection")
	created := false
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if _, ok := event.(observe.ArtifactVerified); ok && !created {
			created = true
			require.NoError(t, os.WriteFile(output, []byte("concurrent owner"), 0o600))
		}
	})

	err = Extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
		Observer:        observer,
	})

	require.Error(t, err)
	require.True(t, created)
	require.Equal(t, []byte("concurrent owner"), mustReadArchiveTestFile(t, output))
	require.Empty(t, archiveExtractStagePaths(t, output))
}

func TestExtractRejectsInPlaceMutationAfterPathVerification(t *testing.T) {
	// Break caught: promoting stage bytes changed in place immediately after
	// pathname-based collection verification consumed them.
	source := writeArchiveTestCollection(t, true, false)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)
	parent := t.TempDir()
	output := filepath.Join(parent, "collection")
	mutated := false
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		verified, ok := event.(observe.ArtifactVerified)
		if !ok || mutated {
			return
		}
		stages := archiveExtractStagePaths(t, output)
		require.Len(t, stages, 1)
		artifactPath := filepath.Join(stages[0], filepath.FromSlash(verified.Path))
		info, err := os.Stat(artifactPath)
		require.NoError(t, err)
		payload := mustReadArchiveTestFile(t, artifactPath)
		require.NotEmpty(t, payload)
		payload[0] ^= 0x01
		require.NoError(t, os.WriteFile(artifactPath, payload, info.Mode().Perm()))
		require.NoError(t, os.Chtimes(artifactPath, info.ModTime(), info.ModTime()))
		mutated = true
	})

	err = Extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
		Observer:        observer,
	})

	require.ErrorContains(t, err, "SHA-256")
	require.True(t, mutated)
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
}

func TestExtractRejectsMutationAfterStageSyncBeforePromotion(t *testing.T) {
	// Break caught: promoting bytes changed after their fsync because the last
	// rooted digest and exact-inventory binding ran before the sync pass.
	source := writeArchiveTestCollection(t, true, false)
	manifest := readArchiveTestManifest(t, source)
	artifact := manifest.Graphs[0].NodeShards[0].JSONL.Path
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := writeIndependentArchiveTestEnvelope(
		t,
		recipient,
		archiveTestRegularEntries(t, source),
	)
	output := filepath.Join(t.TempDir(), "collection")
	mutated := false
	operations := archiveOperations{
		afterExtractFileSync: func(_ *os.Root, relative string) error {
			if relative != artifact || mutated {
				return nil
			}
			stages := archiveExtractStagePaths(t, output)
			require.Len(t, stages, 1)
			artifactPath := filepath.Join(stages[0], filepath.FromSlash(artifact))
			info, err := os.Stat(artifactPath)
			require.NoError(t, err)
			payload := mustReadArchiveTestFile(t, artifactPath)
			require.NotEmpty(t, payload)
			payload[0] ^= 0x01
			require.NoError(t, os.WriteFile(artifactPath, payload, info.Mode().Perm()))
			require.NoError(t, os.Chtimes(artifactPath, info.ModTime(), info.ModTime()))
			mutated = true
			return nil
		},
	}

	err = extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
	}, runtime.GOOS, operations)

	require.ErrorContains(t, err, "SHA-256")
	require.True(t, mutated)
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
}

func archiveTestRegularEntries(t *testing.T, root string) []archiveTestTarEntry {
	t.Helper()
	var entries []archiveTestTarEntry
	require.NoError(t, filepath.WalkDir(root, func(candidate string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, candidate)
		require.NoError(t, err)
		entries = append(entries, archiveTestEntry(
			filepath.ToSlash(relative),
			mustReadArchiveTestFile(t, candidate),
		))
		return nil
	}))
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].header.Name < entries[right].header.Name
	})
	return entries
}

func archiveTestEntry(name string, payload []byte) archiveTestTarEntry {
	return archiveTestTarEntry{
		header: tar.Header{
			Name:     name,
			Mode:     0o600,
			Size:     int64(len(payload)),
			Typeflag: tar.TypeReg,
			Format:   tar.FormatUSTAR,
		},
		payload: append([]byte(nil), payload...),
	}
}

func writeIndependentArchiveTestEnvelope(
	t *testing.T,
	recipient PublicKey,
	entries []archiveTestTarEntry,
) string {
	t.Helper()
	return writeIndependentArchiveTestEnvelopePayload(t, recipient, archiveTestTarPayload(t, entries))
}

func archiveTestTarPayload(t *testing.T, entries []archiveTestTarEntry) []byte {
	t.Helper()
	var plaintext bytes.Buffer
	tarWriter := tar.NewWriter(&plaintext)
	for index := range entries {
		header := entries[index].header
		require.NoError(t, tarWriter.WriteHeader(&header))
		if len(entries[index].payload) != 0 {
			_, err := tarWriter.Write(entries[index].payload)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tarWriter.Close())
	return append([]byte(nil), plaintext.Bytes()...)
}

func writeIndependentArchiveTestEnvelopePayload(t *testing.T, recipient PublicKey, plaintext []byte) string {
	t.Helper()
	var encrypted bytes.Buffer
	writer, err := newEncryptWriter(&encrypted, recipient)
	require.NoError(t, err)
	_, err = writer.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	path := filepath.Join(t.TempDir(), "archive.ret")
	require.NoError(t, os.WriteFile(path, encrypted.Bytes(), 0o600))
	return path
}

func archiveExtractStagePaths(t *testing.T, output string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(
		filepath.Dir(output),
		"."+filepath.Base(output)+".extract-*.tmp",
	))
	require.NoError(t, err)
	return matches
}

func requireArchiveTestTreesEqual(t *testing.T, expected, actual string) {
	t.Helper()
	expectedEntries := archiveTestRegularEntries(t, expected)
	actualEntries := archiveTestRegularEntries(t, actual)
	require.Len(t, actualEntries, len(expectedEntries))
	for index := range expectedEntries {
		require.Equal(t, expectedEntries[index].header.Name, actualEntries[index].header.Name)
		require.Equal(t, expectedEntries[index].payload, actualEntries[index].payload)
	}
}

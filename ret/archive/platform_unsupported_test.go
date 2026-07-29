//go:build !linux && !darwin

package archive

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveWorkflowsRejectUnsupportedRuntimeBeforeFilesystemWork(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	parent := t.TempDir()
	archivePath := filepath.Join(parent, "collection.ret")
	output := filepath.Join(parent, "collection")

	err = Create(context.Background(), CreateConfig{
		CollectionDirectory: filepath.Join(parent, "missing-collection"),
		ArchivePath:         archivePath,
		Recipient:           recipient,
	})
	require.ErrorContains(t, err, "unsupported")
	require.ErrorContains(t, err, runtime.GOOS)
	require.NoFileExists(t, archivePath)

	err = Extract(context.Background(), ExtractConfig{
		ArchivePath:     filepath.Join(parent, "missing-archive.ret"),
		OutputDirectory: output,
		Identity:        identity,
	})
	require.ErrorContains(t, err, "unsupported")
	require.ErrorContains(t, err, runtime.GOOS)
	require.NoDirExists(t, output)
}

func TestOwnedCleanupOnUnsupportedRuntimePreservesPath(t *testing.T) {
	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, parent.Close())
	})
	const name = "owned.tmp"
	owned, err := parent.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, owned.Close())
	})
	_, err = owned.Write([]byte("preserve"))
	require.NoError(t, err)
	expected, err := owned.Stat()
	require.NoError(t, err)

	err = removeOwnedEntry(
		parent,
		name,
		expected,
		owned.Stat,
		"unsupported cleanup test",
		archiveOperations{},
	)

	require.ErrorContains(t, err, "unsupported")
	require.Equal(t, []byte("preserve"), mustReadArchiveTestFile(t, filepath.Join(parentPath, name)))
	require.Empty(t, archiveCleanupQuarantinePaths(t, parentPath))
}

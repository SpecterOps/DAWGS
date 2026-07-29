//go:build linux

package checkpoint

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupOrphansRejectsCheckpointStagingNonRegularFile(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	require.NoError(t, store.Save(fixtureState()))
	loaded, found, err := store.Load()
	require.NoError(t, err)
	require.True(t, found)

	staging := filepath.Join(root, FileName+".tmp-valid_nonce")
	require.NoError(t, syscall.Mkfifo(staging, 0o600))

	err = store.CleanupOrphans(loaded)
	require.ErrorContains(t, err, "not a regular file")
	requireArtifactExists(t, root, FileName+".tmp-valid_nonce")
}

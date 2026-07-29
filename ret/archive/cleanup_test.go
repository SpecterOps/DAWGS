package archive

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveOwnedEntryPreservesSubstitutionAtCheckRemovalBoundary(t *testing.T) {
	// Break caught: Lstat/SameFile succeeds, a replacement is installed, and a
	// later pathname Remove deletes the replacement rather than the owned file.
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
	expected, err := owned.Stat()
	require.NoError(t, err)
	replaced := false
	operations := archiveOperations{
		beforeOwnedEntryQuarantine: func() error {
			require.NoError(t, parent.Remove(name))
			replacement, err := parent.OpenFile(
				name,
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0o600,
			)
			require.NoError(t, err)
			_, err = replacement.Write([]byte("preserve replacement"))
			require.NoError(t, err)
			require.NoError(t, replacement.Close())
			replaced = true
			return nil
		},
	}

	err = removeOwnedEntry(
		parent,
		name,
		expected,
		owned.Stat,
		"test object",
		operations,
	)

	require.ErrorContains(t, err, "ownership cleanup")
	require.True(t, replaced)
	payload, readErr := os.ReadFile(filepath.Join(parentPath, name))
	require.NoError(t, readErr)
	require.Equal(t, []byte("preserve replacement"), payload)
	quarantines, globErr := filepath.Glob(filepath.Join(parentPath, ".ret-cleanup-*.quarantine"))
	require.NoError(t, globErr)
	require.Empty(t, quarantines)
	require.False(t, errors.Is(err, os.ErrNotExist))
}

func TestRemoveOwnedEntryPreservesQuarantinedSubstitutionWhenRestoreIsBlocked(t *testing.T) {
	// Break caught: deleting the quarantined substitution, or replacing a
	// second object that occupies the original name before restoration.
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
	expected, err := owned.Stat()
	require.NoError(t, err)
	operations := archiveOperations{
		beforeOwnedEntryQuarantine: func() error {
			require.NoError(t, parent.Remove(name))
			replacement, err := parent.OpenFile(
				name,
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0o600,
			)
			require.NoError(t, err)
			_, err = replacement.Write([]byte("first replacement"))
			require.NoError(t, err)
			return replacement.Close()
		},
		afterOwnedEntryQuarantine: func(_ string) error {
			blocker, err := parent.OpenFile(
				name,
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0o600,
			)
			require.NoError(t, err)
			_, err = blocker.Write([]byte("second replacement"))
			require.NoError(t, err)
			return blocker.Close()
		},
	}

	err = removeOwnedEntry(
		parent,
		name,
		expected,
		owned.Stat,
		"test object",
		operations,
	)

	require.ErrorContains(t, err, "ownership cleanup")
	require.ErrorContains(t, err, "quarantine")
	payload, readErr := os.ReadFile(filepath.Join(parentPath, name))
	require.NoError(t, readErr)
	require.Equal(t, []byte("second replacement"), payload)
	quarantines, globErr := filepath.Glob(filepath.Join(parentPath, ".ret-cleanup-*.quarantine"))
	require.NoError(t, globErr)
	require.Len(t, quarantines, 1)
	quarantined, readErr := os.ReadFile(quarantines[0])
	require.NoError(t, readErr)
	require.Equal(t, []byte("first replacement"), quarantined)
}

func TestRemoveOwnedDirectoryPreservesOriginalWhenRootedCleanupIsUnproven(t *testing.T) {
	// Break caught: moving a stage into quarantine after rooted enumeration,
	// entry removal, or final empty-inventory proof failed.
	tests := []struct {
		name      string
		operation func(t *testing.T) archiveOperations
		match     string
		wantEntry string
	}{
		{
			name: "initial read directory",
			operation: func(_ *testing.T) archiveOperations {
				return archiveOperations{
					readOwnedDirectory: func(_ *os.Root) ([]fs.DirEntry, error) {
						return nil, errors.New("injected read directory failure")
					},
				}
			},
			match:     "injected read directory failure",
			wantEntry: "marker",
		},
		{
			name: "remove entry",
			operation: func(_ *testing.T) archiveOperations {
				return archiveOperations{
					removeOwnedDirectoryEntry: func(_ *os.Root, _ string) error {
						return errors.New("injected remove entry failure")
					},
				}
			},
			match:     "injected remove entry failure",
			wantEntry: "marker",
		},
		{
			name: "final read directory",
			operation: func(_ *testing.T) archiveOperations {
				readCalls := 0
				return archiveOperations{
					readOwnedDirectory: func(root *os.Root) ([]fs.DirEntry, error) {
						readCalls++
						if readCalls == 2 {
							return nil, errors.New("injected final read directory failure")
						}
						return fs.ReadDir(root.FS(), ".")
					},
				}
			},
			match: "injected final read directory failure",
		},
		{
			name: "final inventory not empty",
			operation: func(t *testing.T) archiveOperations {
				readCalls := 0
				return archiveOperations{
					readOwnedDirectory: func(root *os.Root) ([]fs.DirEntry, error) {
						readCalls++
						if readCalls == 2 {
							late, err := root.OpenFile(
								"late-entry",
								os.O_WRONLY|os.O_CREATE|os.O_EXCL,
								0o600,
							)
							require.NoError(t, err)
							_, err = late.Write([]byte("late payload"))
							require.NoError(t, err)
							require.NoError(t, late.Close())
						}
						return fs.ReadDir(root.FS(), ".")
					},
				}
			},
			match:     "not empty",
			wantEntry: "late-entry",
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
			const stageName = "stage.tmp"
			require.NoError(t, parent.Mkdir(stageName, 0o700))
			stage, err := parent.OpenRoot(stageName)
			require.NoError(t, err)
			info, err := stage.Stat(".")
			require.NoError(t, err)
			marker, err := stage.OpenFile(
				"marker",
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0o600,
			)
			require.NoError(t, err)
			_, err = marker.Write([]byte("stage payload"))
			require.NoError(t, err)
			require.NoError(t, marker.Close())

			err = removeOwnedDirectory(
				parent,
				stageName,
				stage,
				info,
				"test stage",
				test.operation(t),
			)

			require.ErrorContains(t, err, test.match)
			require.ErrorContains(t, err, "ownership cleanup")
			stagePath := filepath.Join(parentPath, stageName)
			require.DirExists(t, stagePath)
			require.Empty(t, archiveCleanupQuarantinePaths(t, parentPath))
			entries, readErr := os.ReadDir(stagePath)
			require.NoError(t, readErr)
			if test.wantEntry == "" {
				require.Empty(t, entries)
			} else {
				require.Len(t, entries, 1)
				require.Equal(t, test.wantEntry, entries[0].Name())
			}
		})
	}
}

func archiveCleanupQuarantinePaths(t *testing.T, parent string) []string {
	t.Helper()
	quarantines, err := filepath.Glob(filepath.Join(parent, ".ret-cleanup-*.quarantine"))
	require.NoError(t, err)
	return quarantines
}

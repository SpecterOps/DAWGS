package archive

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivePlatformSupportIsLimitedToHandleRelativePublication(t *testing.T) {
	require.NoError(t, requirePlatformSupport("linux"))
	require.NoError(t, requirePlatformSupport("darwin"))
	for _, platform := range []string{"windows", "freebsd", "plan9"} {
		t.Run(platform, func(t *testing.T) {
			require.ErrorContains(t, requirePlatformSupport(platform), "unsupported")
			require.ErrorContains(t, requirePlatformSupport(platform), platform)
		})
	}
}

func TestCreateAndExtractGateUnsupportedPlatformsBeforeFilesystemWork(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	parent := t.TempDir()
	archivePath := filepath.Join(parent, "collection.ret")
	output := filepath.Join(parent, "collection")

	err = create(context.Background(), CreateConfig{
		CollectionDirectory: filepath.Join(parent, "missing-collection"),
		ArchivePath:         archivePath,
		Recipient:           recipient,
	}, "windows", archiveOperations{})
	require.ErrorContains(t, err, "unsupported")
	require.NoFileExists(t, archivePath)
	require.Empty(t, archiveCreateTemporaryPaths(t, archivePath))

	err = extract(context.Background(), ExtractConfig{
		ArchivePath:     filepath.Join(parent, "missing-archive.ret"),
		OutputDirectory: output,
		Identity:        identity,
	}, "windows", archiveOperations{})
	require.ErrorContains(t, err, "unsupported")
	require.NoDirExists(t, output)
	require.Empty(t, archiveExtractStagePaths(t, output))
}

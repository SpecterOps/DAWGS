package collection

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/stretchr/testify/require"
)

func TestSafeJoinKeepsCleanSlashSeparatedPathsBeneathRoot(t *testing.T) {
	root := t.TempDir()

	got, err := SafeJoin(root, "graphs/example/nodes/000001.jsonl")

	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "graphs", "example", "nodes", "000001.jsonl"), got)
}

func TestSafeJoinRejectsUnsafeOrAmbiguousPaths(t *testing.T) {
	for _, relative := range []string{
		"",
		".",
		"/absolute",
		"../escape",
		"graphs/../../escape",
		"graphs/../nodes",
		"graphs/./nodes",
		"graphs//nodes",
		`graphs\example\nodes`,
	} {
		t.Run(relative, func(t *testing.T) {
			_, err := SafeJoin(t.TempDir(), relative)
			require.Error(t, err)
		})
	}
}

func TestPathHelpersUseEscapedGraphsOneBasedPaddedIndicesAndCodecSuffixes(t *testing.T) {
	require.Equal(t, "graphs/graph%20name/nodes/000001.jsonl", NodeJSONLPath("graph name", 1, jsonl.CodecNone))
	require.Equal(t, "graphs/graph%20name/nodes/000012.jsonl.gz", NodeJSONLPath("graph name", 12, jsonl.CodecGzip))
	require.Equal(t, "graphs/graph%20name/relationships/000123.jsonl.zst", RelationshipJSONLPath("graph name", 123, jsonl.CodecZstd))
	require.Equal(t, "graphs/graph%20name/nodes/000001.parquet", NodeParquetPath("graph name", 1))
	require.Equal(t, "graphs/graph%20name/relationships/000001.parquet", RelationshipParquetPath("graph name", 1))
	require.Panics(t, func() { NodeParquetPath("graph", 0) })
	require.Panics(t, func() { NodeJSONLPath("graph", 1, jsonl.Codec("zip")) })
}

func TestWriteAndReadRoundTripValidatedManifest(t *testing.T) {
	root := t.TempDir()
	want := fixtureManifest()

	require.NoError(t, Write(root, want))
	got, err := Read(root)

	require.NoError(t, err)
	require.Equal(t, want, got)
	_, err = os.Stat(filepath.Join(root, ManifestName+".tmp"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReadRejectsInvalidAndTrailingManifestJSON(t *testing.T) {
	for name, contents := range map[string]string{
		"invalid manifest": `{"format":"wrong"}`,
		"trailing JSON":    `{"format":"wrong"} {}`,
		"unknown field":    `{"format":"wrong","unknown":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, ManifestName), []byte(contents), 0o600))

			_, err := Read(root)

			require.Error(t, err)
		})
	}
}

func TestWritePreservesExistingManifestWhenEncodingFailsBeforePublication(t *testing.T) {
	root := t.TempDir()
	original := fixtureManifest()
	require.NoError(t, Write(root, original))
	manifestPath := filepath.Join(root, ManifestName)
	before, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	injectedErr := errors.New("injected encoding failure")
	err = writeWithEncoder(root, fixtureManifest(), func(writer io.Writer, _ Manifest) error {
		_, writeErr := writer.Write([]byte(`{"partial":`))
		require.NoError(t, writeErr)
		return injectedErr
	}, os.Remove)

	require.ErrorIs(t, err, injectedErr)
	after, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(before, after))
	_, err = os.Stat(filepath.Join(root, ManifestName+".tmp"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteJoinsPrimaryAndCleanupErrorsWithoutReplacingManifest(t *testing.T) {
	root := t.TempDir()
	original := fixtureManifest()
	require.NoError(t, Write(root, original))
	manifestPath := filepath.Join(root, ManifestName)
	before, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	primaryErr := errors.New("injected encoding failure")
	err = writeWithEncoder(root, fixtureManifest(), func(writer io.Writer, _ Manifest) error {
		file, ok := writer.(*os.File)
		require.True(t, ok)
		require.NoError(t, file.Close())
		return primaryErr
	}, os.Remove)

	require.ErrorIs(t, err, primaryErr)
	require.ErrorIs(t, err, os.ErrClosed)
	require.ErrorContains(t, err, "close temporary manifest")
	after, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestWriteJoinsPrimaryAndRemovalErrorsWithoutReplacingManifest(t *testing.T) {
	root := t.TempDir()
	original := fixtureManifest()
	require.NoError(t, Write(root, original))
	manifestPath := filepath.Join(root, ManifestName)
	before, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	primaryErr := errors.New("injected encoding failure")
	cleanupErr := errors.New("injected removal failure")
	err = writeWithEncoder(root, fixtureManifest(), func(writer io.Writer, _ Manifest) error {
		_, writeErr := writer.Write([]byte(`{"partial":`))
		require.NoError(t, writeErr)
		return primaryErr
	}, func(string) error {
		return cleanupErr
	})

	require.ErrorIs(t, err, primaryErr)
	require.ErrorIs(t, err, cleanupErr)
	require.ErrorContains(t, err, "remove temporary manifest")
	after, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestWriteValidatesBeforeReplacingExistingManifest(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, Write(root, fixtureManifest()))
	manifestPath := filepath.Join(root, ManifestName)
	before, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	invalid := fixtureManifest()
	invalid.Format = "wrong"
	require.Error(t, Write(root, invalid))

	after, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

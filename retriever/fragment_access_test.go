package retriever

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFragmentAccessRejectsEscapes(t *testing.T) {
	for _, attack := range []string{"parent", "nested parent", "absolute", "file symlink", "directory symlink", "symlink chain"} {
		for _, phase := range []Phase{PhaseNodes, PhaseEdges} {
			t.Run(attack+"/"+string(phase), func(t *testing.T) {
				parent := t.TempDir()
				dir := filepath.Join(parent, "collection")
				require.NoError(t, os.Mkdir(dir, 0o700))
				value := fragmentAccessFixture(t, parent)
				// Give the unmodified phase a valid in-root fragment.
				for index := range value.Graphs[0].Files {
					entry := &value.Graphs[0].Files[index]
					contents, err := os.ReadFile(filepath.Join(parent, entry.Path))
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(filepath.Join(dir, entry.Path), contents, 0o600))
					if entry.Phase != phase {
						continue
					}
					original := entry.Path
					switch attack {
					case "parent":
						entry.Path = "../" + original
					case "nested parent":
						require.NoError(t, os.Mkdir(filepath.Join(dir, "nested"), 0o700))
						entry.Path = "nested/../../" + original
					case "absolute":
						entry.Path = filepath.ToSlash(filepath.Join(parent, original))
					case "file symlink":
						require.NoError(t, os.Symlink("../"+original, filepath.Join(dir, "link")))
						entry.Path = "link"
					case "directory symlink":
						require.NoError(t, os.Symlink("..", filepath.Join(dir, "link")))
						entry.Path = "link/" + original
					case "symlink chain":
						require.NoError(t, os.Symlink("../"+original, filepath.Join(dir, "escape")))
						require.NoError(t, os.Symlink("escape", filepath.Join(dir, "link")))
						entry.Path = "link"
					}
				}
				require.Error(t, VerifyManifestFiles(dir, value), "checksums must not read outside the root")
				require.Error(t, verifyCollectionFragments(dir, value), "preflight must not read outside the root")
				for _, entry := range value.Graphs[0].Files {
					if entry.Phase != phase {
						continue
					}
					called := false
					var count int
					var err error
					if phase == PhaseNodes {
						count, err = readNodeFragmentFile(dir, CompressionNone, entry, func(FragmentNode) error { called = true; return nil })
					} else {
						count, err = readEdgeFragmentFile(dir, CompressionNone, entry, func(FragmentEdge) error { called = true; return nil })
					}
					require.Error(t, err, "ordinary load reads must not escape the root")
					require.Zero(t, count)
					require.False(t, called, "no outside record may reach the callback")
				}
				require.NoError(t, WriteManifest(dir, value))
				_, err := Load(context.Background(), nil, "pg", LoadOptions{InputDir: dir, BatchSize: 1})
				require.ErrorContains(t, err, "fragment", "rejection must precede all database access")
			})
		}
	}
}

func TestFragmentAccessAllowsContainedFiles(t *testing.T) {
	for _, mode := range []string{"nested", "file symlink", "directory symlink", "symlink root"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			value := fragmentAccessFixture(t, dir)
			require.NoError(t, os.Mkdir(filepath.Join(dir, "nested"), 0o700))
			for index := range value.Graphs[0].Files {
				entry := &value.Graphs[0].Files[index]
				original := entry.Path
				require.NoError(t, os.Rename(filepath.Join(dir, original), filepath.Join(dir, "nested", original)))
				switch mode {
				case "file symlink":
					require.NoError(t, os.Symlink("nested/"+original, filepath.Join(dir, original)))
				case "directory symlink":
					entry.Path = "alias/" + original
				default:
					entry.Path = "nested/" + original
				}
			}
			if mode == "directory symlink" {
				require.NoError(t, os.Symlink("nested", filepath.Join(dir, "alias")))
			}
			if mode == "symlink root" {
				link := filepath.Join(t.TempDir(), "root")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			}
			require.NoError(t, VerifyManifestFiles(dir, value))
			require.NoError(t, verifyCollectionFragments(dir, value))
			count, err := readNodeFragmentFile(dir, CompressionNone, value.Graphs[0].Files[0], nil)
			require.NoError(t, err)
			require.Equal(t, 2, count)
			count, err = readEdgeFragmentFile(dir, CompressionNone, value.Graphs[0].Files[1], nil)
			require.NoError(t, err)
			require.Equal(t, 1, count)
		})
	}
}

func TestFragmentAccessRejectsInvalidPaths(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../fragment", "nested/../fragment", "/fragment", "nested//fragment", "./fragment", `nested\fragment`, "C:fragment", "C:/fragment", "fragment/"} {
		t.Run(name, func(t *testing.T) {
			_, err := openCollectionFragment("unused-missing-root", name)
			var validationError ValidationError
			require.ErrorAs(t, err, &validationError, "invalid names must fail before filesystem access")
		})
	}
}

func TestFragmentAccessReportsMissingAndNonRegularFiles(t *testing.T) {
	dir := t.TempDir()
	_, err := openCollectionFragment(filepath.Join(dir, "missing"), "fragment")
	require.ErrorContains(t, err, "open collection root")
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = openCollectionFragment(dir, "missing")
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "directory"), 0o700))
	_, err = openCollectionFragment(dir, "directory")
	require.ErrorContains(t, err, "not a regular file")
}

func TestFragmentAccessChecksEachOpenAfterSymlinkReplacement(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "collection")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "safe"), []byte("safe"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "outside"), []byte("outside"), 0o600))
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink("safe", link))
	file, err := openCollectionFragment(dir, "link")
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, os.Remove(link))
	require.NoError(t, os.Symlink("../outside", link))
	contents, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "safe", string(contents), "the opened handle stays bound to the contained file")
	_, err = openCollectionFragment(dir, "link")
	require.Error(t, err, "a prior successful read must not authorize an escaping reopen")
}

func fragmentAccessFixture(t *testing.T, dir string) Manifest {
	t.Helper()
	nodes, err := writeCompressedJSONLines(filepath.Join(dir, "nodes.jsonl"), CompressionNone, DefaultZstdLevel,
		[]FragmentNode{{ID: "a"}, {ID: "b"}})
	require.NoError(t, err)
	nodes.Phase, nodes.Path = PhaseNodes, "nodes.jsonl"
	edges, err := writeCompressedJSONLines(filepath.Join(dir, "edges.jsonl"), CompressionNone, DefaultZstdLevel,
		[]FragmentEdge{{StartID: "a", EndID: "b", Kind: "LINK"}})
	require.NoError(t, err)
	edges.Phase, edges.Path = PhaseEdges, "edges.jsonl"
	value := newValidTestManifest(1)
	value.Compression = CompressionNone
	value.Graphs = []GraphManifest{{Name: "default", NodeCount: 2, EdgeCount: 1, Files: []FileManifest{nodes, edges}}}
	return value
}

package retriever

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// openCollectionFragment applies the same containment policy to checksum,
// preflight and load reads. Root.Open resolves symlinks beneath the opened
// directory without a separate path-check/open race. Contained relative
// symlinks are allowed; absolute links and links that escape the root fail.
func openCollectionFragment(inputDir, relativePath string) (*os.File, error) {
	if !fs.ValidPath(relativePath) || relativePath == "." || strings.Contains(relativePath, "\\") || hasWindowsVolumeName(relativePath) {
		return nil, ValidationError{Message: fmt.Sprintf("fragment path %q must be a clean relative slash-separated file path", relativePath)}
	}
	root, err := os.OpenRoot(inputDir)
	if err != nil {
		return nil, fmt.Errorf("open collection root: %w", err)
	}
	defer root.Close()

	file, err := root.Open(filepath.FromSlash(relativePath))
	if err != nil {
		return nil, fmt.Errorf("open collection fragment %q: %w", relativePath, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("inspect collection fragment %q: %w", relativePath, err)
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("collection fragment %q is not a regular file", relativePath)
	}
	return file, nil
}

func verifyFragmentChecksum(inputDir string, entry FileManifest) error {
	file, err := openCollectionFragment(inputDir, entry.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	return verifyChecksumFromReader(filepath.Join(inputDir, filepath.FromSlash(entry.Path)), file, entry.SHA256, entry.CompressedBytes)
}

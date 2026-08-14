package archive

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/observe"
)

const collectionTarFileMode int64 = 0o600

type collectionTarFile struct {
	path string
	info fs.FileInfo
}

type collectionTarPlan struct {
	rootInfo        fs.FileInfo
	files           []collectionTarFile
	manifest        collection.Manifest
	artifactSHA256s map[string]string
}

func collectionTarPaths(
	ctx context.Context,
	root string,
	observer observe.Observer,
) (collectionTarPlan, error) {
	if err := ctx.Err(); err != nil {
		return collectionTarPlan{}, fmt.Errorf("prepare collection TAR: %w", err)
	}
	verification, err := collection.Verify(ctx, root, observer)
	if err != nil {
		return collectionTarPlan{}, fmt.Errorf("verify collection before TAR creation: %w", err)
	}
	declared, err := manifestTarPaths(verification.Manifest)
	if err != nil {
		return collectionTarPlan{}, err
	}
	plan, err := inventoryCollectionTarFiles(root, declared)
	if err != nil {
		return collectionTarPlan{}, err
	}
	plan.manifest = verification.Manifest
	plan.artifactSHA256s = manifestArtifactSHA256s(verification.Manifest)
	return plan, nil
}

func manifestArtifactSHA256s(manifest collection.Manifest) map[string]string {
	result := make(map[string]string)
	for _, graph := range manifest.Graphs {
		for _, shard := range graph.NodeShards {
			if shard.JSONL != nil {
				result[shard.JSONL.Path] = shard.JSONL.SHA256
			}
			if shard.Parquet != nil {
				result[shard.Parquet.Path] = shard.Parquet.SHA256
			}
		}
		for _, shard := range graph.RelationshipShards {
			if shard.JSONL != nil {
				result[shard.JSONL.Path] = shard.JSONL.SHA256
			}
			if shard.Parquet != nil {
				result[shard.Parquet.Path] = shard.Parquet.SHA256
			}
		}
	}
	return result
}

func manifestTarPaths(manifest collection.Manifest) ([]string, error) {
	declared := []string{collection.ManifestName}
	for _, graph := range manifest.Graphs {
		for _, shard := range graph.NodeShards {
			if shard.JSONL != nil {
				declared = append(declared, shard.JSONL.Path)
			}
			if shard.Parquet != nil {
				declared = append(declared, shard.Parquet.Path)
			}
		}
		for _, shard := range graph.RelationshipShards {
			if shard.JSONL != nil {
				declared = append(declared, shard.JSONL.Path)
			}
			if shard.Parquet != nil {
				declared = append(declared, shard.Parquet.Path)
			}
		}
	}

	seen := make(map[string]struct{}, len(declared))
	for _, relative := range declared {
		if _, err := collection.SafeJoin(".", relative); err != nil {
			return nil, fmt.Errorf("validate declared collection path %q: %w", relative, err)
		}
		if _, found := seen[relative]; found {
			return nil, fmt.Errorf("duplicate declared collection path %q", relative)
		}
		seen[relative] = struct{}{}
	}
	sort.Strings(declared)
	return declared, nil
}

func inventoryCollectionTarFiles(root string, declared []string) (collectionTarPlan, error) {
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return collectionTarPlan{}, fmt.Errorf("inspect collection root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return collectionTarPlan{}, fmt.Errorf("collection root is not a non-symlink directory: %q", root)
	}

	declaredFiles := make(map[string]struct{}, len(declared))
	declaredDirectories := map[string]struct{}{".": {}}
	for _, relative := range declared {
		declaredFiles[relative] = struct{}{}
		for directory := path.Dir(relative); directory != "."; directory = path.Dir(directory) {
			declaredDirectories[directory] = struct{}{}
		}
	}

	found := make(map[string]fs.FileInfo, len(declared))
	err = filepath.WalkDir(root, func(candidate string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, candidate)
		if err != nil {
			return fmt.Errorf("resolve collection entry %q: %w", candidate, err)
		}
		relative = filepath.ToSlash(relative)
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect collection entry %q: %w", relative, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("collection entry %q is a symlink", relative)
		}
		if info.IsDir() {
			if _, ok := declaredDirectories[relative]; !ok {
				return fmt.Errorf("unexpected collection directory %q", relative)
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("collection entry %q is not a regular file", relative)
		}
		if _, ok := declaredFiles[relative]; !ok {
			return fmt.Errorf("unexpected collection file %q", relative)
		}
		found[relative] = info
		return nil
	})
	if err != nil {
		return collectionTarPlan{}, fmt.Errorf("inventory collection TAR inputs: %w", err)
	}

	files := make([]collectionTarFile, 0, len(declared))
	for _, relative := range declared {
		info, ok := found[relative]
		if !ok {
			return collectionTarPlan{}, fmt.Errorf("declared collection file %q is missing", relative)
		}
		files = append(files, collectionTarFile{path: relative, info: info})
	}
	return collectionTarPlan{rootInfo: rootInfo, files: files}, nil
}

func inventoryPinnedCollectionTarFiles(root *os.Root, declared []string) (collectionTarPlan, error) {
	rootInfo, err := root.Stat(".")
	if err != nil {
		return collectionTarPlan{}, fmt.Errorf("inspect pinned collection root: %w", err)
	}
	if !rootInfo.IsDir() {
		return collectionTarPlan{}, fmt.Errorf("pinned collection root is not a directory")
	}

	declaredFiles := make(map[string]struct{}, len(declared))
	declaredDirectories := map[string]struct{}{".": {}}
	for _, relative := range declared {
		declaredFiles[relative] = struct{}{}
		for directory := path.Dir(relative); directory != "."; directory = path.Dir(directory) {
			declaredDirectories[directory] = struct{}{}
		}
	}

	found := make(map[string]fs.FileInfo, len(declared))
	err = fs.WalkDir(root.FS(), ".", func(relative string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect pinned collection entry %q: %w", relative, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("pinned collection entry %q is a symlink", relative)
		}
		if info.IsDir() {
			if _, ok := declaredDirectories[relative]; !ok {
				return fmt.Errorf("unexpected pinned collection directory %q", relative)
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("pinned collection entry %q is not a regular file", relative)
		}
		if _, ok := declaredFiles[relative]; !ok {
			return fmt.Errorf("unexpected pinned collection file %q", relative)
		}
		found[relative] = info
		return nil
	})
	if err != nil {
		return collectionTarPlan{}, fmt.Errorf("inventory pinned collection TAR inputs: %w", err)
	}

	files := make([]collectionTarFile, 0, len(declared))
	for _, relative := range declared {
		info, ok := found[relative]
		if !ok {
			return collectionTarPlan{}, fmt.Errorf("declared pinned collection file %q is missing", relative)
		}
		files = append(files, collectionTarFile{path: relative, info: info})
	}
	return collectionTarPlan{rootInfo: rootInfo, files: files}, nil
}

func readPinnedCollectionManifest(root *os.Root) (
	manifest collection.Manifest,
	digest string,
	info fs.FileInfo,
	resultErr error,
) {
	if err := requirePinnedRegularPath(root, collection.ManifestName); err != nil {
		return manifest, "", nil, err
	}
	info, err := root.Lstat(collection.ManifestName)
	if err != nil {
		return manifest, "", nil, fmt.Errorf("inspect pinned collection manifest: %w", err)
	}
	file, err := root.Open(collection.ManifestName)
	if err != nil {
		return manifest, "", nil, fmt.Errorf("open pinned collection manifest: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, wrapTarCloseError("pinned collection manifest", file.Close()))
	}()
	openedInfo, err := file.Stat()
	if err != nil {
		return manifest, "", nil, fmt.Errorf("inspect open pinned collection manifest: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return manifest, "", nil, fmt.Errorf("pinned collection manifest changed while opening")
	}

	hasher := sha256.New()
	decoder := json.NewDecoder(io.TeeReader(file, hasher))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, "", nil, fmt.Errorf("decode pinned collection manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return manifest, "", nil, fmt.Errorf("decode pinned collection manifest: trailing JSON value")
		}
		return manifest, "", nil, fmt.Errorf("decode pinned collection manifest trailing data: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return manifest, "", nil, fmt.Errorf("validate pinned collection manifest: %w", err)
	}
	return manifest, hex.EncodeToString(hasher.Sum(nil)), info, nil
}

func plannedTarFile(files []collectionTarFile, relative string) (collectionTarFile, bool) {
	for _, file := range files {
		if file.path == relative {
			return file, true
		}
	}
	return collectionTarFile{}, false
}

func tarPlanPaths(files []collectionTarFile) []string {
	paths := make([]string, len(files))
	for index, file := range files {
		paths[index] = file.path
	}
	return paths
}

func compareCollectionTarPlans(expected, actual collectionTarPlan) error {
	if !os.SameFile(expected.rootInfo, actual.rootInfo) {
		return fmt.Errorf("collection root changed during TAR creation")
	}
	if len(expected.files) != len(actual.files) {
		return fmt.Errorf("collection file set changed during TAR creation")
	}
	for index := range expected.files {
		if expected.files[index].path != actual.files[index].path ||
			!samePlannedFile(expected.files[index].info, actual.files[index].info) {
			return fmt.Errorf("collection file %q changed during TAR creation", expected.files[index].path)
		}
	}
	return nil
}

func writeCollectionTar(
	ctx context.Context,
	destination io.Writer,
	root string,
	plan collectionTarPlan,
	observer observe.Observer,
) (resultErr error) {
	if destination == nil {
		return fmt.Errorf("collection TAR destination is required")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("write collection TAR: %w", err)
	}

	pinnedRoot, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open collection root: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, wrapTarCloseError("collection root", pinnedRoot.Close()))
	}()
	pinnedRootInfo, err := pinnedRoot.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect open collection root: %w", err)
	}
	if !pinnedRootInfo.IsDir() || !os.SameFile(plan.rootInfo, pinnedRootInfo) {
		return fmt.Errorf("collection root changed before TAR creation")
	}
	pinnedManifest, manifestDigest, manifestInfo, err := readPinnedCollectionManifest(pinnedRoot)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(plan.manifest, pinnedManifest) {
		return fmt.Errorf("collection manifest changed after verification")
	}
	plannedManifest, ok := plannedTarFile(plan.files, collection.ManifestName)
	if !ok || !samePlannedFile(plannedManifest.info, manifestInfo) {
		return fmt.Errorf("collection manifest changed after inventory")
	}

	writer := tar.NewWriter(destination)
	defer func() {
		resultErr = errors.Join(resultErr, wrapTarCloseError("TAR writer", writer.Close()))
	}()

	for _, planned := range plan.files {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("write collection TAR: %w", err)
		}
		if err := requirePinnedRegularPath(pinnedRoot, planned.path); err != nil {
			return err
		}
		info, err := pinnedRoot.Lstat(filepath.FromSlash(planned.path))
		if err != nil {
			return fmt.Errorf("inspect TAR file %q: %w", planned.path, err)
		}
		if !samePlannedFile(planned.info, info) {
			return fmt.Errorf("collection file %q changed before TAR creation", planned.path)
		}

		file, err := pinnedRoot.Open(filepath.FromSlash(planned.path))
		if err != nil {
			return fmt.Errorf("open TAR file %q: %w", planned.path, err)
		}
		expectedDigest := plan.artifactSHA256s[planned.path]
		if planned.path == collection.ManifestName {
			expectedDigest = manifestDigest
		}
		if err := writeCollectionTarFile(writer, file, planned, expectedDigest); err != nil {
			return errors.Join(err, wrapTarCloseError("TAR file "+planned.path, file.Close()))
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close TAR file %q: %w", planned.path, err)
		}

		observe.Emit(ctx, observer, observe.ArchiveEntryProcessed{
			Operation: "pack",
			Path:      planned.path,
			Size:      planned.info.Size(),
		})
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("write collection TAR after %q: %w", planned.path, err)
		}
	}
	finalPlan, err := inventoryPinnedCollectionTarFiles(pinnedRoot, tarPlanPaths(plan.files))
	if err != nil {
		return fmt.Errorf("final collection TAR inventory: %w", err)
	}
	if err := compareCollectionTarPlans(plan, finalPlan); err != nil {
		return err
	}
	return nil
}

func requirePinnedRegularPath(root *os.Root, relative string) error {
	current := ""
	components := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := root.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect TAR path component %q: %w", component, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("TAR path %q contains a symlink component %q", relative, component)
		}
		if index < len(components)-1 {
			if !info.IsDir() {
				return fmt.Errorf("TAR path component %q is not a directory", component)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("TAR file %q is not a regular file", relative)
		}
	}
	return nil
}

func writeCollectionTarFile(
	writer *tar.Writer,
	file *os.File,
	planned collectionTarFile,
	expectedDigest string,
) error {
	openedInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect open TAR file %q: %w", planned.path, err)
	}
	if !openedInfo.Mode().IsRegular() || !samePlannedFile(planned.info, openedInfo) {
		return fmt.Errorf("collection file %q changed while opening", planned.path)
	}

	header := &tar.Header{
		Name:       planned.path,
		Mode:       collectionTarFileMode,
		Uid:        0,
		Gid:        0,
		Size:       planned.info.Size(),
		ModTime:    time.Unix(0, 0).UTC(),
		AccessTime: time.Time{},
		ChangeTime: time.Time{},
		Typeflag:   tar.TypeReg,
		Uname:      "",
		Gname:      "",
		Format:     tar.FormatPAX,
	}
	if err := writer.WriteHeader(header); err != nil {
		return fmt.Errorf("write TAR header %q: %w", planned.path, err)
	}
	hasher := sha256.New()
	copied, err := io.CopyN(io.MultiWriter(writer, hasher), file, planned.info.Size())
	if err != nil {
		return fmt.Errorf("write TAR contents %q after %d bytes: %w", planned.path, copied, err)
	}
	var trailing [1]byte
	n, err := file.Read(trailing[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("check TAR file size %q: %w", planned.path, err)
	}
	if n != 0 {
		return fmt.Errorf("collection file %q grew during TAR creation", planned.path)
	}
	actualDigest := hex.EncodeToString(hasher.Sum(nil))
	if expectedDigest == "" || actualDigest != expectedDigest {
		return fmt.Errorf(
			"collection file %q SHA-256 is %s, want %s",
			planned.path,
			actualDigest,
			expectedDigest,
		)
	}
	return nil
}

func samePlannedFile(expected, actual fs.FileInfo) bool {
	return os.SameFile(expected, actual) &&
		expected.Mode() == actual.Mode() &&
		expected.Size() == actual.Size() &&
		expected.ModTime().Equal(actual.ModTime())
}

func wrapTarCloseError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close %s: %w", name, err)
}

package archive

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/observe"
)

type ExtractConfig struct {
	ArchivePath     string
	OutputDirectory string
	Identity        PrivateKey
	Observer        observe.Observer
}

func Extract(ctx context.Context, config ExtractConfig) (resultErr error) {
	return extract(ctx, config, runtime.GOOS, archiveOperations{})
}

func extract(
	ctx context.Context,
	config ExtractConfig,
	platform string,
	operations archiveOperations,
) (resultErr error) {
	if err := requirePlatformSupport(platform); err != nil {
		return err
	}
	if err := validateExtractConfig(config); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("extract archive: %w", err)
	}

	archiveFile, err := openArchiveForExtraction(config.ArchivePath)
	if err != nil {
		return err
	}
	defer func() {
		if archiveFile != nil {
			resultErr = errors.Join(resultErr, wrapExtractCloseError("archive", archiveFile.Close()))
		}
	}()

	outputParentPath, outputBase, err := prepareExtractDestination(config.OutputDirectory)
	if err != nil {
		return err
	}
	outputParent, err := os.OpenRoot(outputParentPath)
	if err != nil {
		return fmt.Errorf("open extraction destination parent: %w", err)
	}
	defer func() {
		resultErr = errors.Join(
			resultErr,
			wrapExtractCloseError("extraction destination parent", outputParent.Close()),
		)
	}()
	outputParentDirectory, err := openDirectoryMatchingRoot(outputParentPath, outputParent)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(
			resultErr,
			wrapExtractCloseError("extraction destination parent directory", outputParentDirectory.Close()),
		)
	}()
	if _, err := outputParent.Lstat(outputBase); err == nil {
		return fmt.Errorf("extraction destination exists: %s", config.OutputDirectory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect extraction destination: %w", err)
	}

	stageName, stageRoot, createdStageInfo, err := createExtractStage(
		outputParent,
		outputBase,
		operations,
	)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if stageRoot == nil {
			return
		}
		if published {
			resultErr = errors.Join(
				resultErr,
				wrapExtractCloseError("extraction stage", stageRoot.Close()),
			)
		} else {
			resultErr = errors.Join(
				resultErr,
				removeOwnedDirectory(
					outputParent,
					stageName,
					stageRoot,
					createdStageInfo,
					"extraction stage",
					operations,
				),
			)
		}
		stageRoot = nil
	}()
	stagePath := filepath.Join(outputParentPath, stageName)
	if err := requireRootPathIdentity(stagePath, stageRoot, "extraction stage"); err != nil {
		return err
	}

	decrypted, err := newDecryptReader(archiveFile, config.Identity)
	if err != nil {
		return fmt.Errorf("open encrypted archive: %w", err)
	}
	defer func() {
		if decrypted != nil {
			resultErr = errors.Join(
				resultErr,
				wrapExtractCloseError("archive decryption reader", decrypted.Close()),
			)
		}
	}()

	seen, err := extractCollectionTar(
		ctx,
		decrypted,
		stageRoot,
		config.Observer,
		operations,
	)
	if err != nil {
		return err
	}
	if err := requireZeroTarTail(decrypted); err != nil {
		return err
	}
	if err := decrypted.Close(); err != nil {
		decrypted = nil
		return fmt.Errorf("authenticate archive final frame: %w", err)
	}
	decrypted = nil
	if err := archiveFile.Close(); err != nil {
		archiveFile = nil
		return fmt.Errorf("close archive: %w", err)
	}
	archiveFile = nil

	if err := requireRootPathIdentity(stagePath, stageRoot, "extraction stage before verification"); err != nil {
		return err
	}
	manifest, manifestDigest, _, err := readPinnedCollectionManifest(stageRoot)
	if err != nil {
		return fmt.Errorf("read extracted collection manifest: %w", err)
	}
	declared, err := manifestTarPaths(manifest)
	if err != nil {
		return err
	}
	if err := compareExtractedFileSet(seen, declared); err != nil {
		return err
	}
	initialPlan, err := inventoryPinnedCollectionTarFiles(stageRoot, declared)
	if err != nil {
		return fmt.Errorf("validate extracted collection file set: %w", err)
	}
	verification, err := collection.Verify(ctx, stagePath, config.Observer)
	if err != nil {
		return fmt.Errorf("verify extracted collection: %w", err)
	}
	if !reflect.DeepEqual(manifest, verification.Manifest) {
		return fmt.Errorf("verified collection manifest differs from authenticated stage manifest")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("promote extracted collection: %w", err)
	}
	if err := syncExtractedCollection(
		ctx,
		stageRoot,
		declared,
		operations,
	); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("promote synced extracted collection: %w", err)
	}
	if err := verifyPinnedExtractedCollection(
		stageRoot,
		initialPlan,
		manifest,
		manifestDigest,
		declared,
	); err != nil {
		return err
	}
	if err := requireRootPathIdentity(stagePath, stageRoot, "synced extraction stage"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("promote bound extracted collection: %w", err)
	}
	stageInfo, err := stageRoot.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect synced extraction stage: %w", err)
	}
	if _, err := outputParent.Lstat(outputBase); err == nil {
		return fmt.Errorf("extraction destination exists: %s", config.OutputDirectory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect extraction destination before promotion: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("promote extracted collection: %w", err)
	}
	if err := requireDirectoryPathIdentity(outputParentPath, outputParentDirectory); err != nil {
		return fmt.Errorf("validate extraction destination parent before promotion: %w", err)
	}
	if err := requireRootAndPhysicalEntryIdentity(
		outputParent,
		outputParentPath,
		stageName,
		"verified extraction stage",
		stageInfo,
	); err != nil {
		return err
	}
	if err := renameNoReplace(outputParentDirectory, outputParentPath, stageName, outputBase); err != nil {
		return fmt.Errorf("promote extracted collection: %w", err)
	}
	published = true
	if err := requireRootAndPhysicalEntryIdentity(
		outputParent,
		outputParentPath,
		outputBase,
		"promoted collection",
		stageInfo,
	); err != nil {
		return err
	}
	if err := outputParentDirectory.Sync(); err != nil {
		return fmt.Errorf("sync extraction destination parent: %w", err)
	}
	return nil
}

func validateExtractConfig(config ExtractConfig) error {
	if strings.TrimSpace(config.ArchivePath) == "" {
		return fmt.Errorf("archive path is required")
	}
	if strings.TrimSpace(config.OutputDirectory) == "" {
		return fmt.Errorf("output directory is required")
	}
	if !config.Identity.valid {
		return fmt.Errorf("identity private key is required")
	}
	return nil
}

func openArchiveForExtraction(archivePath string) (*os.File, error) {
	absolute, err := filepath.Abs(archivePath)
	if err != nil {
		return nil, fmt.Errorf("resolve archive path: %w", err)
	}
	physicalParent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, fmt.Errorf("resolve archive parent: %w", err)
	}
	parent, err := os.OpenRoot(physicalParent)
	if err != nil {
		return nil, fmt.Errorf("open archive parent: %w", err)
	}
	base := filepath.Base(absolute)
	info, err := parent.Lstat(base)
	if err != nil {
		_ = parent.Close()
		return nil, fmt.Errorf("inspect archive: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		_ = parent.Close()
		return nil, fmt.Errorf("archive is not a non-symlink regular file")
	}
	file, err := parent.Open(base)
	if err != nil {
		_ = parent.Close()
		return nil, fmt.Errorf("open archive: %w", err)
	}
	openedInfo, statErr := file.Stat()
	closeParentErr := parent.Close()
	if statErr != nil {
		return nil, errors.Join(
			fmt.Errorf("inspect open archive: %w", statErr),
			wrapExtractCloseError("archive", file.Close()),
			wrapExtractCloseError("archive parent", closeParentErr),
		)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return nil, errors.Join(
			fmt.Errorf("archive changed while opening"),
			wrapExtractCloseError("archive", file.Close()),
			wrapExtractCloseError("archive parent", closeParentErr),
		)
	}
	if closeParentErr != nil {
		return nil, errors.Join(
			fmt.Errorf("close archive parent: %w", closeParentErr),
			wrapExtractCloseError("archive", file.Close()),
		)
	}
	return file, nil
}

func prepareExtractDestination(output string) (string, string, error) {
	if _, err := os.Lstat(output); err == nil {
		return "", "", fmt.Errorf("extraction destination exists: %s", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("inspect extraction destination: %w", err)
	}
	absolute, err := filepath.Abs(output)
	if err != nil {
		return "", "", fmt.Errorf("resolve extraction destination: %w", err)
	}
	physicalParent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", "", fmt.Errorf("resolve extraction destination parent: %w", err)
	}
	physicalOutput := filepath.Join(physicalParent, filepath.Base(absolute))
	if _, err := os.Lstat(physicalOutput); err == nil {
		return "", "", fmt.Errorf("extraction destination exists: %s", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("inspect physical extraction destination: %w", err)
	}
	return physicalParent, filepath.Base(physicalOutput), nil
}

func requireRootPathIdentity(path string, root *os.Root, description string) error {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect %s path: %w", description, err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.IsDir() {
		return fmt.Errorf("%s path is not a non-symlink directory", description)
	}
	pinnedInfo, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect pinned %s: %w", description, err)
	}
	if !pinnedInfo.IsDir() || !os.SameFile(pathInfo, pinnedInfo) {
		return fmt.Errorf("%s changed while processing", description)
	}
	return nil
}

func createExtractStage(
	parent *os.Root,
	outputBase string,
	operations archiveOperations,
) (string, *os.Root, os.FileInfo, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, nil, fmt.Errorf("generate extraction stage name: %w", err)
		}
		name := "." + outputBase + ".extract-" + hex.EncodeToString(random[:]) + ".tmp"
		if err := parent.Mkdir(name, 0o700); err == nil {
			info, statErr := parent.Lstat(name)
			if statErr != nil {
				return "", nil, nil, errors.Join(
					fmt.Errorf("inspect extraction stage: %w", statErr),
					fmt.Errorf(
						"ownership cleanup for extraction stage: created identity is unavailable; preserving pathname",
					),
				)
			}
			root, openErr := operations.runOpenRoot(parent, name)
			if openErr != nil {
				return name, nil, info, errors.Join(
					fmt.Errorf("open extraction stage: %w", openErr),
					fmt.Errorf(
						"ownership cleanup for extraction stage: retained stage root is unavailable; preserving pathname",
					),
				)
			}
			pinnedInfo, pinnedStatErr := operations.runStatRoot(root)
			if pinnedStatErr != nil ||
				pinnedInfo == nil ||
				!pinnedInfo.IsDir() ||
				!os.SameFile(info, pinnedInfo) {
				if pinnedStatErr == nil {
					pinnedStatErr = fmt.Errorf("created stage identity changed while opening")
				}
				return name, nil, info, errors.Join(
					fmt.Errorf("inspect pinned extraction stage: %w", pinnedStatErr),
					wrapExtractCloseError("extraction stage", root.Close()),
					fmt.Errorf(
						"ownership cleanup for extraction stage: pinned stage identity is unproven; preserving pathname",
					),
				)
			}
			if err := root.Chmod(".", 0o700); err != nil {
				return "", nil, nil, errors.Join(
					fmt.Errorf("set extraction stage mode: %w", err),
					removeOwnedDirectory(
						parent,
						name,
						root,
						info,
						"extraction stage",
						operations,
					),
				)
			}
			return name, root, info, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", nil, nil, fmt.Errorf("create extraction stage: %w", err)
		}
	}
	return "", nil, nil, fmt.Errorf("create unique extraction stage: name attempts exhausted")
}

func extractCollectionTar(
	ctx context.Context,
	source io.Reader,
	stage *os.Root,
	observer observe.Observer,
	operations archiveOperations,
) (map[string]struct{}, error) {
	reader := tar.NewReader(source)
	seen := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("extract collection TAR: %w", err)
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return seen, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read collection TAR header: %w", err)
		}
		if hasSparseTarMetadata(header) {
			return nil, fmt.Errorf("TAR entry %q uses unsupported sparse metadata", header.Name)
		}
		if header.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("TAR entry %q is not a regular file", header.Name)
		}
		if _, found := seen[header.Name]; found {
			return nil, fmt.Errorf("duplicate TAR entry %q", header.Name)
		}
		if _, err := collection.SafeJoin(".", header.Name); err != nil {
			return nil, fmt.Errorf("unsafe TAR entry %q: %w", header.Name, err)
		}
		if err := extractExclusive(
			stage,
			header.Name,
			header.Size,
			reader,
			operations,
		); err != nil {
			return nil, err
		}
		seen[header.Name] = struct{}{}
		observe.Emit(ctx, observer, observe.ArchiveEntryProcessed{
			Operation: "unpack",
			Path:      header.Name,
			Size:      header.Size,
		})
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("extract collection TAR after %q: %w", header.Name, err)
		}
	}
}

func hasSparseTarMetadata(header *tar.Header) bool {
	if header.Typeflag == tar.TypeGNUSparse {
		return true
	}
	for key, value := range header.PAXRecords {
		if strings.HasPrefix(key, "GNU.sparse.") ||
			(key == "SCHILY.filetype" && strings.EqualFold(value, "sparse")) {
			return true
		}
	}
	return false
}

func requireZeroTarTail(source io.Reader) error {
	var buffer [32 * 1024]byte
	for {
		count, err := source.Read(buffer[:])
		for _, value := range buffer[:count] {
			if value != 0 {
				return fmt.Errorf("archive contains nonzero data after the TAR end marker")
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("authenticate archive remainder: %w", err)
		}
		if count == 0 {
			return fmt.Errorf("authenticate archive remainder: %w", io.ErrNoProgress)
		}
	}
}

func extractExclusive(
	root *os.Root,
	relative string,
	size int64,
	source io.Reader,
	operations archiveOperations,
) (resultErr error) {
	if size < 0 {
		return fmt.Errorf("TAR entry %q has negative size", relative)
	}
	parentPath := path.Dir(relative)
	if err := ensureExtractDirectories(root, parentPath); err != nil {
		return err
	}
	parent, err := root.OpenRoot(filepath.FromSlash(parentPath))
	if err != nil {
		return fmt.Errorf("open TAR entry parent %q: %w", parentPath, err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, wrapExtractCloseError("TAR entry parent "+parentPath, parent.Close()))
	}()

	base := path.Base(relative)
	file, err := parent.OpenFile(base, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create TAR entry %q: %w", relative, err)
	}
	createdInfo, err := file.Stat()
	if err != nil {
		return errors.Join(
			fmt.Errorf("inspect created TAR entry %q: %w", relative, err),
			wrapExtractCloseError("TAR entry "+relative, file.Close()),
			fmt.Errorf(
				"ownership cleanup for incomplete TAR entry %q: created identity is unavailable; preserving pathname",
				relative,
			),
		)
	}
	complete := false
	defer func() {
		if !complete {
			resultErr = errors.Join(
				resultErr,
				sanitizeAndRemoveOwnedEntry(
					parent,
					base,
					createdInfo,
					file,
					"incomplete TAR entry "+relative,
					operations,
				),
			)
		}
		if file != nil {
			resultErr = errors.Join(resultErr, wrapExtractCloseError("TAR entry "+relative, file.Close()))
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("set TAR entry mode %q: %w", relative, err)
	}
	copied, err := io.CopyN(file, source, size)
	if err != nil {
		return fmt.Errorf("extract TAR entry %q after %d bytes: %w", relative, copied, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync TAR entry %q: %w", relative, err)
	}
	closeErr := file.Close()
	file = nil
	if closeErr != nil {
		return fmt.Errorf("close TAR entry %q: %w", relative, closeErr)
	}
	complete = true
	return nil
}

func ensureExtractDirectories(root *os.Root, relative string) error {
	if relative == "." {
		return nil
	}
	current := ""
	for _, component := range strings.Split(filepath.FromSlash(relative), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := root.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := root.Mkdir(current, 0o700); err != nil {
				return fmt.Errorf("create TAR directory %q: %w", filepath.ToSlash(current), err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect TAR directory %q: %w", filepath.ToSlash(current), err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("TAR directory %q is not a non-symlink directory", filepath.ToSlash(current))
		}
	}
	return nil
}

func compareExtractedFileSet(seen map[string]struct{}, declared []string) error {
	declaredSet := make(map[string]struct{}, len(declared))
	for _, relative := range declared {
		declaredSet[relative] = struct{}{}
		if _, found := seen[relative]; !found {
			return fmt.Errorf("declared collection file %q is missing from archive", relative)
		}
	}
	var unexpected []string
	for relative := range seen {
		if _, declared := declaredSet[relative]; !declared {
			unexpected = append(unexpected, relative)
		}
	}
	if len(unexpected) != 0 {
		sort.Strings(unexpected)
		return fmt.Errorf("unexpected archive file %q", unexpected[0])
	}
	return nil
}

func verifyPinnedExtractedCollection(
	root *os.Root,
	initialPlan collectionTarPlan,
	manifest collection.Manifest,
	manifestDigest string,
	declared []string,
) error {
	currentManifest, currentManifestDigest, currentManifestInfo, err := readPinnedCollectionManifest(root)
	if err != nil {
		return fmt.Errorf("re-read authenticated stage manifest: %w", err)
	}
	if !reflect.DeepEqual(manifest, currentManifest) || manifestDigest != currentManifestDigest {
		return fmt.Errorf("authenticated stage manifest changed during verification")
	}
	plannedManifest, ok := plannedTarFile(initialPlan.files, collection.ManifestName)
	if !ok || !samePlannedFile(plannedManifest.info, currentManifestInfo) {
		return fmt.Errorf("authenticated stage manifest identity changed during verification")
	}

	expectedDigests := manifestArtifactSHA256s(manifest)
	for relative, expectedDigest := range expectedDigests {
		planned, ok := plannedTarFile(initialPlan.files, relative)
		if !ok {
			return fmt.Errorf("verified artifact %q was not inventoried", relative)
		}
		actualDigest, err := hashPinnedCollectionFile(root, planned)
		if err != nil {
			return err
		}
		if actualDigest != expectedDigest {
			return fmt.Errorf(
				"verified artifact %q SHA-256 is %s, want %s",
				relative,
				actualDigest,
				expectedDigest,
			)
		}
	}
	finalPlan, err := inventoryPinnedCollectionTarFiles(root, declared)
	if err != nil {
		return fmt.Errorf("final authenticated stage inventory: %w", err)
	}
	if err := compareCollectionTarPlans(initialPlan, finalPlan); err != nil {
		return fmt.Errorf("authenticated stage changed during verification: %w", err)
	}
	return nil
}

func hashPinnedCollectionFile(root *os.Root, planned collectionTarFile) (
	digest string,
	resultErr error,
) {
	if err := requirePinnedRegularPath(root, planned.path); err != nil {
		return "", err
	}
	info, err := root.Lstat(filepath.FromSlash(planned.path))
	if err != nil {
		return "", fmt.Errorf("inspect verified artifact %q: %w", planned.path, err)
	}
	if !samePlannedFile(planned.info, info) {
		return "", fmt.Errorf("verified artifact %q changed before hashing", planned.path)
	}
	file, err := root.Open(filepath.FromSlash(planned.path))
	if err != nil {
		return "", fmt.Errorf("open verified artifact %q: %w", planned.path, err)
	}
	defer func() {
		resultErr = errors.Join(
			resultErr,
			wrapExtractCloseError("verified artifact "+planned.path, file.Close()),
		)
	}()
	openedInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect open verified artifact %q: %w", planned.path, err)
	}
	if !samePlannedFile(planned.info, openedInfo) {
		return "", fmt.Errorf("verified artifact %q changed while opening", planned.path)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("hash verified artifact %q: %w", planned.path, err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func syncExtractedCollection(
	ctx context.Context,
	root *os.Root,
	declared []string,
	operations archiveOperations,
) error {
	for _, relative := range declared {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sync extracted collection: %w", err)
		}
		file, err := root.Open(filepath.FromSlash(relative))
		if err != nil {
			return fmt.Errorf("open extracted file %q for sync: %w", relative, err)
		}
		_, statErr := file.Stat()
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(
			wrapExtractStatError("extracted file "+relative, statErr),
			wrapExtractSyncError("extracted file "+relative, syncErr),
			wrapExtractCloseError("extracted file "+relative, closeErr),
		); err != nil {
			return err
		}
		if err := operations.runAfterExtractFileSync(root, relative); err != nil {
			return fmt.Errorf("after extracted file sync %q: %w", relative, err)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sync extracted collection after %q: %w", relative, err)
		}
	}

	directorySet := map[string]struct{}{".": {}}
	for _, relative := range declared {
		for directory := path.Dir(relative); directory != "."; directory = path.Dir(directory) {
			directorySet[directory] = struct{}{}
		}
	}
	directories := make([]string, 0, len(directorySet))
	for directory := range directorySet {
		directories = append(directories, directory)
	}
	sort.Slice(directories, func(left, right int) bool {
		leftDepth := strings.Count(directories[left], "/")
		rightDepth := strings.Count(directories[right], "/")
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return directories[left] > directories[right]
	})
	for _, directory := range directories {
		file, err := root.Open(filepath.FromSlash(directory))
		if err != nil {
			return fmt.Errorf("open extracted directory %q for sync: %w", directory, err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(
			wrapExtractSyncError("extracted directory "+directory, syncErr),
			wrapExtractCloseError("extracted directory "+directory, closeErr),
		); err != nil {
			return err
		}
	}
	return nil
}

func wrapExtractCloseError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close %s: %w", name, err)
}

func wrapExtractSyncError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("sync %s: %w", name, err)
}

func wrapExtractStatError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("inspect %s: %w", name, err)
}

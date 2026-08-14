package archive

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/specterops/dawgs/ret/observe"
)

type CreateConfig struct {
	CollectionDirectory string
	ArchivePath         string
	Recipient           PublicKey
	Observer            observe.Observer
}

type archiveDestination struct {
	parentPath string
	base       string
}

func Create(ctx context.Context, config CreateConfig) (resultErr error) {
	return create(ctx, config, runtime.GOOS, archiveOperations{})
}

func create(
	ctx context.Context,
	config CreateConfig,
	platform string,
	operations archiveOperations,
) (resultErr error) {
	if err := requirePlatformSupport(platform); err != nil {
		return err
	}
	if err := validateCreateConfig(config); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("create archive: %w", err)
	}

	destination, err := prepareArchiveDestination(config.CollectionDirectory, config.ArchivePath)
	if err != nil {
		return err
	}
	plan, err := collectionTarPaths(ctx, config.CollectionDirectory, config.Observer)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("create archive after collection verification: %w", err)
	}

	parent, err := os.OpenRoot(destination.parentPath)
	if err != nil {
		return fmt.Errorf("open archive destination directory: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, wrapCreateCloseError("archive destination directory", parent.Close()))
	}()
	directory, err := openDirectoryMatchingRoot(destination.parentPath, parent)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, wrapCreateCloseError("archive destination directory file", directory.Close()))
	}()
	if _, err := parent.Lstat(destination.base); err == nil {
		return fmt.Errorf("archive destination exists: %s", config.ArchivePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect archive destination: %w", err)
	}

	temporaryName, file, temporaryHandle, createdTemporaryInfo, err := createArchiveTemporary(
		parent,
		destination.base,
		operations,
	)
	if err != nil {
		return err
	}
	var encrypted io.WriteCloser
	published := false
	defer func() {
		if encrypted != nil {
			resultErr = errors.Join(resultErr, wrapCreateCloseError("archive encryption writer", encrypted.Close()))
		}
		if !published {
			resultErr = errors.Join(
				resultErr,
				sanitizeAndRemoveOwnedEntry(
					parent,
					temporaryName,
					createdTemporaryInfo,
					temporaryHandle,
					"temporary archive",
					operations,
				),
			)
		}
		if file != nil {
			resultErr = errors.Join(resultErr, wrapCreateCloseError("temporary archive", file.Close()))
		}
		if temporaryHandle != nil {
			resultErr = errors.Join(
				resultErr,
				wrapCreateCloseError("temporary archive identity handle", temporaryHandle.Close()),
			)
		}
	}()

	encrypted, err = newEncryptWriter(file, config.Recipient)
	if err != nil {
		return fmt.Errorf("start archive encryption: %w", err)
	}
	if err := writeCollectionTar(
		ctx,
		encrypted,
		config.CollectionDirectory,
		plan,
		config.Observer,
	); err != nil {
		return fmt.Errorf("create collection TAR: %w", err)
	}

	closeEncryptionErr := encrypted.Close()
	encrypted = nil
	syncErr := file.Sync()
	temporaryInfo, statErr := file.Stat()
	closeFileErr := file.Close()
	file = nil
	if err := errors.Join(
		wrapCreateCloseError("archive encryption writer", closeEncryptionErr),
		wrapCreateSyncError("temporary archive", syncErr),
		wrapCreateStatError("temporary archive", statErr),
		wrapCreateCloseError("temporary archive", closeFileErr),
	); err != nil {
		return err
	}
	if err := requireDirectoryPathIdentity(destination.parentPath, directory); err != nil {
		return fmt.Errorf("validate archive destination directory before publication: %w", err)
	}
	if err := requireRootAndPhysicalEntryIdentity(
		parent,
		destination.parentPath,
		temporaryName,
		"temporary archive",
		temporaryInfo,
	); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish archive: %w", err)
	}
	if _, err := parent.Lstat(destination.base); err == nil {
		return fmt.Errorf("archive destination exists: %s", config.ArchivePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect archive destination before publication: %w", err)
	}
	if err := renameNoReplace(directory, destination.parentPath, temporaryName, destination.base); err != nil {
		return fmt.Errorf("publish archive: %w", err)
	}
	published = true
	if err := requireRootAndPhysicalEntryIdentity(
		parent,
		destination.parentPath,
		destination.base,
		"published archive",
		temporaryInfo,
	); err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync archive destination directory: %w", err)
	}
	return nil
}

func validateCreateConfig(config CreateConfig) error {
	if strings.TrimSpace(config.CollectionDirectory) == "" {
		return fmt.Errorf("collection directory is required")
	}
	if strings.TrimSpace(config.ArchivePath) == "" {
		return fmt.Errorf("archive path is required")
	}
	if !config.Recipient.valid {
		return fmt.Errorf("recipient public key is required")
	}
	return nil
}

func prepareArchiveDestination(collectionRoot, archivePath string) (archiveDestination, error) {
	if _, err := os.Lstat(archivePath); err == nil {
		return archiveDestination{}, fmt.Errorf("archive destination exists: %s", archivePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return archiveDestination{}, fmt.Errorf("inspect archive destination: %w", err)
	}

	absoluteCollection, err := filepath.Abs(collectionRoot)
	if err != nil {
		return archiveDestination{}, fmt.Errorf("resolve collection directory: %w", err)
	}
	absoluteArchive, err := filepath.Abs(archivePath)
	if err != nil {
		return archiveDestination{}, fmt.Errorf("resolve archive path: %w", err)
	}
	if pathWithin(absoluteCollection, absoluteArchive) {
		return archiveDestination{}, fmt.Errorf("archive path must not be inside the collection")
	}

	physicalCollection, err := filepath.EvalSymlinks(absoluteCollection)
	if err != nil {
		return archiveDestination{}, fmt.Errorf("resolve physical collection directory: %w", err)
	}
	physicalParent, err := filepath.EvalSymlinks(filepath.Dir(absoluteArchive))
	if err != nil {
		return archiveDestination{}, fmt.Errorf("resolve physical archive parent: %w", err)
	}
	physicalArchive := filepath.Join(physicalParent, filepath.Base(absoluteArchive))
	if pathWithin(physicalCollection, physicalArchive) {
		return archiveDestination{}, fmt.Errorf("archive path must not be physically inside the collection")
	}
	if _, err := os.Lstat(physicalArchive); err == nil {
		return archiveDestination{}, fmt.Errorf("archive destination exists: %s", archivePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return archiveDestination{}, fmt.Errorf("inspect physical archive destination: %w", err)
	}
	parentInfo, err := os.Lstat(physicalParent)
	if err != nil {
		return archiveDestination{}, fmt.Errorf("inspect physical archive parent: %w", err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return archiveDestination{}, fmt.Errorf("archive parent is not a directory")
	}
	return archiveDestination{parentPath: physicalParent, base: filepath.Base(physicalArchive)}, nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(candidate))
	if err != nil {
		return false
	}
	return relative == "." ||
		(relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func openDirectoryMatchingRoot(path string, root *os.Root) (*os.File, error) {
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("inspect pinned directory root: %w", err)
	}
	directory, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open pinned directory file: %w", err)
	}
	directoryInfo, err := directory.Stat()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("inspect pinned directory file: %w", err),
			wrapCreateCloseError("pinned directory file", directory.Close()),
		)
	}
	if !rootInfo.IsDir() || !directoryInfo.IsDir() || !os.SameFile(rootInfo, directoryInfo) {
		return nil, errors.Join(
			fmt.Errorf("directory changed while pinning"),
			wrapCreateCloseError("pinned directory file", directory.Close()),
		)
	}
	return directory, nil
}

func requireDirectoryPathIdentity(path string, directory *os.File) error {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect directory path: %w", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.IsDir() {
		return fmt.Errorf("directory path is not a non-symlink directory")
	}
	pinnedInfo, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect pinned directory: %w", err)
	}
	if !pinnedInfo.IsDir() || !os.SameFile(pathInfo, pinnedInfo) {
		return fmt.Errorf("directory path changed while processing")
	}
	return nil
}

func requireRootAndPhysicalEntryIdentity(
	root *os.Root,
	parentPath string,
	name string,
	description string,
	expected os.FileInfo,
) error {
	rootedInfo, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect rooted %s: %w", description, err)
	}
	physicalInfo, err := os.Lstat(filepath.Join(parentPath, name))
	if err != nil {
		return fmt.Errorf("inspect physical %s: %w", description, err)
	}
	if rootedInfo.Mode()&os.ModeSymlink != 0 ||
		physicalInfo.Mode()&os.ModeSymlink != 0 ||
		rootedInfo.Mode().Type() != expected.Mode().Type() ||
		physicalInfo.Mode().Type() != expected.Mode().Type() ||
		!os.SameFile(expected, rootedInfo) ||
		!os.SameFile(expected, physicalInfo) {
		return fmt.Errorf("%s changed while processing", description)
	}
	return nil
}

func createArchiveTemporary(
	parent *os.Root,
	archiveBase string,
	operations archiveOperations,
) (string, *os.File, *os.File, os.FileInfo, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, nil, nil, fmt.Errorf("generate temporary archive name: %w", err)
		}
		name := "." + archiveBase + ".create-" + hex.EncodeToString(random[:]) + ".tmp"
		file, err := parent.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			info, statErr := file.Stat()
			if statErr != nil {
				return "", nil, nil, nil, errors.Join(
					fmt.Errorf("inspect temporary archive: %w", statErr),
					wrapCreateCloseError("temporary archive", file.Close()),
					fmt.Errorf(
						"ownership cleanup for temporary archive: created identity is unavailable; preserving pathname",
					),
				)
			}
			identityHandle, openErr := parent.OpenFile(name, os.O_RDWR, 0)
			if openErr != nil {
				return "", nil, nil, nil, errors.Join(
					fmt.Errorf("pin temporary archive identity: %w", openErr),
					sanitizeAndRemoveOwnedEntry(
						parent,
						name,
						info,
						file,
						"temporary archive",
						operations,
					),
					wrapCreateCloseError("temporary archive", file.Close()),
				)
			}
			identityInfo, identityStatErr := identityHandle.Stat()
			if identityStatErr != nil ||
				!identityInfo.Mode().IsRegular() ||
				!os.SameFile(info, identityInfo) {
				if identityStatErr == nil {
					identityStatErr = fmt.Errorf("temporary archive identity changed while pinning")
				}
				return "", nil, nil, nil, errors.Join(
					fmt.Errorf("inspect temporary archive identity handle: %w", identityStatErr),
					sanitizeAndRemoveOwnedEntry(
						parent,
						name,
						info,
						file,
						"temporary archive",
						operations,
					),
					wrapCreateCloseError("temporary archive", file.Close()),
					wrapCreateCloseError("temporary archive identity handle", identityHandle.Close()),
				)
			}
			if err := file.Chmod(0o600); err != nil {
				return "", nil, nil, nil, errors.Join(
					fmt.Errorf("set temporary archive mode: %w", err),
					sanitizeAndRemoveOwnedEntry(
						parent,
						name,
						info,
						identityHandle,
						"temporary archive",
						operations,
					),
					wrapCreateCloseError("temporary archive", file.Close()),
					wrapCreateCloseError("temporary archive identity handle", identityHandle.Close()),
				)
			}
			return name, file, identityHandle, info, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, nil, nil, fmt.Errorf("create temporary archive: %w", err)
		}
	}
	return "", nil, nil, nil, fmt.Errorf("create unique temporary archive: name attempts exhausted")
}

func wrapCreateCloseError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close %s: %w", name, err)
}

func wrapCreateSyncError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("sync %s: %w", name, err)
}

func wrapCreateStatError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("inspect %s: %w", name, err)
}

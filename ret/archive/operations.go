package archive

import (
	"io/fs"
	"os"
)

type archiveOperations struct {
	beforeOwnedEntryQuarantine func() error
	afterOwnedEntryQuarantine  func(quarantineName string) error
	truncateOwnedFile          func(file *os.File, size int64) error
	syncOwnedFile              func(file *os.File) error
	readOwnedDirectory         func(root *os.Root) ([]fs.DirEntry, error)
	removeOwnedDirectoryEntry  func(root *os.Root, name string) error
	openRoot                   func(parent *os.Root, name string) (*os.Root, error)
	statRoot                   func(root *os.Root) (fs.FileInfo, error)
	afterExtractFileSync       func(root *os.Root, relative string) error
}

func (s archiveOperations) runBeforeOwnedEntryQuarantine() error {
	if s.beforeOwnedEntryQuarantine == nil {
		return nil
	}
	return s.beforeOwnedEntryQuarantine()
}

func (s archiveOperations) runAfterOwnedEntryQuarantine(quarantineName string) error {
	if s.afterOwnedEntryQuarantine == nil {
		return nil
	}
	return s.afterOwnedEntryQuarantine(quarantineName)
}

func (s archiveOperations) runTruncateOwnedFile(file *os.File, size int64) error {
	if s.truncateOwnedFile == nil {
		return file.Truncate(size)
	}
	return s.truncateOwnedFile(file, size)
}

func (s archiveOperations) runSyncOwnedFile(file *os.File) error {
	if s.syncOwnedFile == nil {
		return file.Sync()
	}
	return s.syncOwnedFile(file)
}

func (s archiveOperations) runReadOwnedDirectory(root *os.Root) ([]fs.DirEntry, error) {
	if s.readOwnedDirectory == nil {
		return fs.ReadDir(root.FS(), ".")
	}
	return s.readOwnedDirectory(root)
}

func (s archiveOperations) runRemoveOwnedDirectoryEntry(root *os.Root, name string) error {
	if s.removeOwnedDirectoryEntry == nil {
		return root.RemoveAll(name)
	}
	return s.removeOwnedDirectoryEntry(root, name)
}

func (s archiveOperations) runOpenRoot(parent *os.Root, name string) (*os.Root, error) {
	if s.openRoot == nil {
		return parent.OpenRoot(name)
	}
	return s.openRoot(parent, name)
}

func (s archiveOperations) runStatRoot(root *os.Root) (fs.FileInfo, error) {
	if s.statRoot == nil {
		return root.Stat(".")
	}
	return s.statRoot(root)
}

func (s archiveOperations) runAfterExtractFileSync(root *os.Root, relative string) error {
	if s.afterExtractFileSync == nil {
		return nil
	}
	return s.afterExtractFileSync(root, relative)
}

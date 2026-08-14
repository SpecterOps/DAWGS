package archive

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

func removeOwnedEntry(
	parent *os.Root,
	name string,
	expected fs.FileInfo,
	retainedStat func() (fs.FileInfo, error),
	description string,
	operations archiveOperations,
) (resultErr error) {
	if err := proveOwnedEntry(parent, name, expected, retainedStat, description); err != nil {
		return err
	}
	if err := operations.runBeforeOwnedEntryQuarantine(); err != nil {
		return fmt.Errorf(
			"ownership cleanup for %s: before-quarantine operation: %w; preserving pathname",
			description,
			err,
		)
	}

	directory, err := parent.Open(".")
	if err != nil {
		return fmt.Errorf(
			"ownership cleanup for %s: open pinned parent directory: %w; preserving pathname",
			description,
			err,
		)
	}
	defer func() {
		if err := directory.Close(); err != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("ownership cleanup for %s: close pinned parent directory: %w", description, err),
			)
		}
	}()

	quarantineName, err := quarantineOwnedName(directory, name)
	if err != nil {
		return fmt.Errorf(
			"ownership cleanup for %s: atomically quarantine pathname: %w; preserving pathname",
			description,
			err,
		)
	}
	hookErr := operations.runAfterOwnedEntryQuarantine(quarantineName)
	quarantined, inspectErr := parent.Lstat(quarantineName)
	if inspectErr != nil {
		return errors.Join(
			fmt.Errorf(
				"ownership cleanup for %s: inspect quarantine %q: %w; preserving unproven quarantine",
				description,
				quarantineName,
				inspectErr,
			),
			hookErr,
		)
	}
	if quarantined.Mode().Type() == expected.Mode().Type() &&
		os.SameFile(expected, quarantined) {
		return errors.Join(
			fmt.Errorf(
				"ownership cleanup for %s: created object preserved in quarantine %q because conditional unlink by identity is unavailable",
				description,
				quarantineName,
			),
			hookErr,
		)
	}

	restoreErr := renameNoReplace(directory, "", quarantineName, name)
	if restoreErr == nil {
		return errors.Join(
			fmt.Errorf(
				"ownership cleanup for %s: pathname was substituted at the cleanup boundary; replacement restored and preserved",
				description,
			),
			hookErr,
		)
	}
	return errors.Join(
		fmt.Errorf(
			"ownership cleanup for %s: pathname was substituted at the cleanup boundary; replacement preserved in quarantine %q",
			description,
			quarantineName,
		),
		fmt.Errorf("restore quarantined replacement: %w", restoreErr),
		hookErr,
	)
}

func proveOwnedEntry(
	parent *os.Root,
	name string,
	expected fs.FileInfo,
	retainedStat func() (fs.FileInfo, error),
	description string,
) error {
	if expected == nil {
		return fmt.Errorf(
			"ownership cleanup for %s: created identity is unavailable; preserving pathname",
			description,
		)
	}
	if retainedStat == nil {
		return fmt.Errorf(
			"ownership cleanup for %s: retained identity handle is unavailable; preserving pathname",
			description,
		)
	}
	retained, err := retainedStat()
	if err != nil {
		return fmt.Errorf(
			"ownership cleanup for %s: inspect retained identity handle: %w; preserving pathname",
			description,
			err,
		)
	}
	if retained.Mode().Type() != expected.Mode().Type() || !os.SameFile(expected, retained) {
		return fmt.Errorf(
			"ownership cleanup for %s: retained identity handle changed; preserving pathname",
			description,
		)
	}
	current, err := parent.Lstat(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"ownership cleanup for %s: created pathname disappeared",
				description,
			)
		}
		return fmt.Errorf("ownership cleanup for %s: inspect pathname: %w", description, err)
	}
	if current.Mode().Type() != expected.Mode().Type() || !os.SameFile(expected, current) {
		return fmt.Errorf(
			"ownership cleanup for %s: pathname no longer identifies the created object; preserving replacement",
			description,
		)
	}
	return nil
}

func quarantineOwnedName(directory *os.File, name string) (string, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate quarantine name: %w", err)
		}
		quarantineName := ".ret-cleanup-" + hex.EncodeToString(random[:]) + ".quarantine"
		if err := renameNoReplace(directory, "", name, quarantineName); err == nil {
			return quarantineName, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	return "", fmt.Errorf("quarantine name attempts exhausted")
}

func removeOwnedDirectory(
	parent *os.Root,
	name string,
	root *os.Root,
	expected fs.FileInfo,
	description string,
	operations archiveOperations,
) error {
	if root == nil {
		return fmt.Errorf(
			"ownership cleanup for %s: pinned directory is unavailable; preserving pathname",
			description,
		)
	}
	pinned, err := root.Stat(".")
	if err != nil {
		return errors.Join(
			fmt.Errorf("ownership cleanup for %s: inspect pinned directory: %w", description, err),
			wrapExtractCloseError(description, root.Close()),
		)
	}
	if expected == nil || !pinned.IsDir() || !os.SameFile(expected, pinned) {
		return errors.Join(
			fmt.Errorf(
				"ownership cleanup for %s: pinned directory identity changed; preserving pathname",
				description,
			),
			wrapExtractCloseError(description, root.Close()),
		)
	}

	var cleanupErr error
	entries, err := operations.runReadOwnedDirectory(root)
	if err != nil {
		cleanupErr = fmt.Errorf(
			"ownership cleanup for %s: read pinned directory: %w",
			description,
			err,
		)
	} else {
		for _, entry := range entries {
			if err := operations.runRemoveOwnedDirectoryEntry(root, entry.Name()); err != nil {
				cleanupErr = errors.Join(
					cleanupErr,
					fmt.Errorf(
						"ownership cleanup for %s: remove pinned entry %q: %w",
						description,
						entry.Name(),
						err,
					),
				)
			}
		}
	}
	if cleanupErr == nil {
		remaining, err := operations.runReadOwnedDirectory(root)
		if err != nil {
			cleanupErr = fmt.Errorf(
				"ownership cleanup for %s: prove pinned directory empty: %w",
				description,
				err,
			)
		} else if len(remaining) != 0 {
			cleanupErr = fmt.Errorf(
				"ownership cleanup for %s: pinned directory is not empty after rooted cleanup",
				description,
			)
		}
	}
	retainedStat := func() (fs.FileInfo, error) {
		return root.Stat(".")
	}
	var removeErr error
	if cleanupErr == nil {
		removeErr = removeOwnedEntry(
			parent,
			name,
			expected,
			retainedStat,
			description,
			operations,
		)
	} else {
		removeErr = preserveOwnedEntry(
			parent,
			name,
			expected,
			retainedStat,
			description,
			"rooted directory emptying or proof did not complete",
		)
	}
	closeErr := wrapExtractCloseError(description, root.Close())
	return errors.Join(cleanupErr, closeErr, removeErr)
}

type ownedPath struct {
	parent      *os.Root
	handle      *os.File
	name        string
	info        fs.FileInfo
	description string
}

func preserveOwnedEntry(
	parent *os.Root,
	name string,
	expected fs.FileInfo,
	retainedStat func() (fs.FileInfo, error),
	description string,
	reason string,
) error {
	proofErr := proveOwnedEntry(parent, name, expected, retainedStat, description)
	if proofErr != nil {
		return errors.Join(
			fmt.Errorf(
				"ownership cleanup for %s: %s; preserving pathname",
				description,
				reason,
			),
			proofErr,
		)
	}
	return fmt.Errorf(
		"ownership cleanup for %s: %s; preserving proven original pathname",
		description,
		reason,
	)
}

func sanitizeOwnedFile(
	handle *os.File,
	description string,
	operations archiveOperations,
) error {
	if handle == nil {
		return fmt.Errorf("sanitize %s before quarantine: retained writable handle is unavailable", description)
	}
	if err := operations.runTruncateOwnedFile(handle, 0); err != nil {
		return fmt.Errorf("truncate %s before quarantine: %w", description, err)
	}
	if err := operations.runSyncOwnedFile(handle); err != nil {
		return fmt.Errorf("sync %s before quarantine: %w", description, err)
	}
	return nil
}

func sanitizeAndRemoveOwnedEntry(
	parent *os.Root,
	name string,
	expected fs.FileInfo,
	handle *os.File,
	description string,
	operations archiveOperations,
) error {
	if err := sanitizeOwnedFile(handle, description, operations); err != nil {
		return errors.Join(
			err,
			preserveOwnedEntry(
				parent,
				name,
				expected,
				func() (fs.FileInfo, error) {
					if handle == nil {
						return nil, fmt.Errorf("retained writable handle is unavailable")
					}
					return handle.Stat()
				},
				description,
				"sanitization did not complete",
			),
		)
	}
	return removeOwnedEntry(
		parent,
		name,
		expected,
		handle.Stat,
		description,
		operations,
	)
}

func (s *ownedPath) remove(operations archiveOperations) error {
	if s == nil || s.parent == nil {
		return fmt.Errorf("ownership cleanup: owned pathname is unavailable")
	}
	if s.handle == nil {
		return errors.Join(
			fmt.Errorf("ownership cleanup: owned object handle is unavailable"),
			s.release(),
		)
	}
	removeErr := sanitizeAndRemoveOwnedEntry(
		s.parent,
		s.name,
		s.info,
		s.handle,
		s.description,
		operations,
	)
	var handleCloseErr error
	if s.handle != nil {
		handleCloseErr = s.handle.Close()
		s.handle = nil
		if handleCloseErr != nil {
			handleCloseErr = fmt.Errorf("close owned object: %w", handleCloseErr)
		}
	}
	closeErr := s.parent.Close()
	s.parent = nil
	if closeErr != nil {
		closeErr = fmt.Errorf("close owned pathname parent: %w", closeErr)
	}
	return errors.Join(removeErr, handleCloseErr, closeErr)
}

func (s *ownedPath) release() error {
	if s == nil || s.parent == nil {
		return nil
	}
	var handleCloseErr error
	if s.handle != nil {
		handleCloseErr = s.handle.Close()
		s.handle = nil
		if handleCloseErr != nil {
			handleCloseErr = fmt.Errorf("close owned object: %w", handleCloseErr)
		}
	}
	parentCloseErr := s.parent.Close()
	s.parent = nil
	if parentCloseErr != nil {
		parentCloseErr = fmt.Errorf("close owned pathname parent: %w", parentCloseErr)
	}
	return errors.Join(handleCloseErr, parentCloseErr)
}

package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type forceReplaceOperations struct {
	afterParentPinned func(parentPath string, parent *os.Root) error
	beforeQuarantine  func(parent *os.Root, name string, target *os.Root) error
	afterQuarantine   func(parent *os.Root, originalName, quarantineName string) error
	closeRoot         func(role string, root *os.Root) error
	closeFile         func(role string, file *os.File) error
}

type forceReplacement struct {
	destination string
	tombstone   string
}

func replaceDumpDestination(target string, operations forceReplaceOperations) (result forceReplacement, resultErr error) {
	if err := validateForcePlatform(runtime.GOOS); err != nil {
		return forceReplacement{}, err
	}
	absolute, err := cleanAbsoluteForceTarget(target)
	if err != nil {
		return forceReplacement{}, err
	}
	parentPath, name := filepath.Split(absolute)
	parentPath = filepath.Clean(parentPath)
	if name == "" || name == "." || name == string(os.PathSeparator) {
		return forceReplacement{}, fmt.Errorf("unsafe dump replacement target %q", absolute)
	}

	parent, parentInfo, err := openPinnedAbsoluteDirectory(parentPath, operations)
	if err != nil {
		return forceReplacement{}, fmt.Errorf("pin dump replacement parent: %w", err)
	}
	parentClosed := false
	defer func() {
		if !parentClosed {
			resultErr = errors.Join(resultErr, wrapForceCloseError(
				"parent",
				operations.closeRootHandle("parent", parent),
			))
		}
	}()
	if operations.afterParentPinned != nil {
		if err := operations.afterParentPinned(parentPath, parent); err != nil {
			return forceReplacement{}, fmt.Errorf("force replacement after parent pin: %w", err)
		}
	}
	if err := provePinnedAbsoluteDirectory(parentPath, parentInfo, operations); err != nil {
		return forceReplacement{}, fmt.Errorf("dump replacement parent changed after validation: %w", err)
	}

	targetInfo, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return forceReplacement{destination: absolute}, nil
	}
	if err != nil {
		return forceReplacement{}, fmt.Errorf("inspect dump replacement target: %w", err)
	}
	if targetInfo.Mode()&os.ModeSymlink != 0 {
		return forceReplacement{}, fmt.Errorf("dump replacement target %q is a symbolic link", absolute)
	}
	if !targetInfo.IsDir() {
		return forceReplacement{}, fmt.Errorf("dump replacement target %q is not a directory", absolute)
	}
	if err := rejectProtectedForceIdentity(targetInfo); err != nil {
		return forceReplacement{}, err
	}

	targetRoot, err := parent.OpenRoot(name)
	if err != nil {
		return forceReplacement{}, fmt.Errorf("pin dump replacement target: %w", err)
	}
	targetClosed := false
	defer func() {
		if !targetClosed {
			resultErr = errors.Join(resultErr, wrapForceCloseError(
				"target",
				operations.closeRootHandle("target", targetRoot),
			))
		}
	}()
	pinnedTargetInfo, err := targetRoot.Stat(".")
	if err != nil {
		return forceReplacement{}, fmt.Errorf("inspect pinned dump replacement target: %w", err)
	}
	if !os.SameFile(targetInfo, pinnedTargetInfo) {
		return forceReplacement{}, fmt.Errorf("dump replacement target changed while being pinned; preserving pathname")
	}

	directory, err := parent.Open(".")
	if err != nil {
		return forceReplacement{}, fmt.Errorf("open pinned dump replacement parent: %w", err)
	}
	directoryClosed := false
	defer func() {
		if !directoryClosed {
			resultErr = errors.Join(resultErr, wrapForceCloseError(
				"parent directory handle",
				operations.closeFileHandle("parent directory handle", directory),
			))
		}
	}()

	current, err := parent.Lstat(name)
	if err != nil || !os.SameFile(targetInfo, current) {
		return forceReplacement{}, fmt.Errorf("dump replacement target changed before quarantine; preserving replacement")
	}
	if operations.beforeQuarantine != nil {
		if err := operations.beforeQuarantine(parent, name, targetRoot); err != nil {
			return forceReplacement{}, fmt.Errorf("force replacement before quarantine: %w", err)
		}
	}

	quarantineName, err := quarantineForceTarget(directory, name)
	if err != nil {
		return forceReplacement{}, fmt.Errorf("atomically quarantine dump replacement target: %w", err)
	}
	tombstone := filepath.Join(parentPath, quarantineName)
	restoreOnFailure := true
	defer func() {
		if restoreOnFailure {
			resultErr = errors.Join(
				resultErr,
				restoreForceQuarantine(directory, parentPath, name, quarantineName),
			)
		}
	}()
	if operations.afterQuarantine != nil {
		if err := operations.afterQuarantine(parent, name, quarantineName); err != nil {
			return forceReplacement{}, fmt.Errorf("force replacement after quarantine: %w", err)
		}
	}
	quarantinedInfo, err := parent.Lstat(quarantineName)
	if err != nil || !os.SameFile(targetInfo, quarantinedInfo) {
		return forceReplacement{}, fmt.Errorf("dump replacement target was substituted at quarantine boundary; replacement will be restored or preserved")
	}
	if err := provePinnedAbsoluteDirectory(parentPath, parentInfo, operations); err != nil {
		return forceReplacement{}, fmt.Errorf("dump replacement parent changed after quarantine: %w", err)
	}
	if _, err := parent.Lstat(name); err == nil {
		return forceReplacement{}, fmt.Errorf(
			"original destination %q was recreated after quarantine; refusing dump handoff",
			absolute,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return forceReplacement{}, fmt.Errorf(
			"inspect original destination %q after quarantine: %w",
			absolute,
			err,
		)
	}
	targetClosed = true
	if err := operations.closeRootHandle("target", targetRoot); err != nil {
		return forceReplacement{}, fmt.Errorf("close preserved prior dump destination: %w", err)
	}

	parentClosed = true
	if err := operations.closeRootHandle("parent", parent); err != nil {
		return forceReplacement{}, wrapForceCloseError("parent", err)
	}

	directoryClosed = true
	if err := operations.closeFileHandle("parent directory handle", directory); err != nil {
		restoreOnFailure = false
		return forceReplacement{}, fmt.Errorf(
			"%w; approved destination preserved at tombstone %q",
			wrapForceCloseError("parent directory handle", err),
			tombstone,
		)
	}

	restoreOnFailure = false
	return forceReplacement{
		destination: absolute,
		tombstone:   tombstone,
	}, nil
}

func validateForcePlatform(platform string) error {
	switch platform {
	case "linux", "darwin":
		return nil
	default:
		return fmt.Errorf(
			"dump -force is unsupported on platform %q; supported platforms are linux and darwin",
			platform,
		)
	}
}

func cleanAbsoluteForceTarget(target string) (string, error) {
	if strings.TrimSpace(target) == "" {
		return "", fmt.Errorf("unsafe dump replacement target: path is empty")
	}
	absolute, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve dump replacement target: %w", err)
	}
	absolute = filepath.Clean(absolute)
	volumeRoot := filepath.Clean(filepath.VolumeName(absolute) + string(os.PathSeparator))
	if absolute == volumeRoot {
		return "", fmt.Errorf("unsafe dump replacement target %q: filesystem root is protected", absolute)
	}
	return absolute, nil
}

func openPinnedAbsoluteDirectory(path string, operations forceReplaceOperations) (*os.Root, fs.FileInfo, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	absolute = filepath.Clean(absolute)
	volumeRoot := filepath.Clean(filepath.VolumeName(absolute) + string(os.PathSeparator))
	root, err := os.OpenRoot(volumeRoot)
	if err != nil {
		return nil, nil, err
	}
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, nil, errors.Join(
			err,
			wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
		)
	}
	if absolute == volumeRoot {
		return root, rootInfo, nil
	}
	relative, err := filepath.Rel(volumeRoot, absolute)
	if err != nil {
		return nil, nil, errors.Join(
			err,
			wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
		)
	}
	for _, component := range strings.Split(relative, string(os.PathSeparator)) {
		if component == "" || component == "." || component == ".." {
			return nil, nil, errors.Join(
				fmt.Errorf("unsafe path component %q", component),
				wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
			)
		}
		info, err := root.Lstat(component)
		if err != nil {
			return nil, nil, errors.Join(
				err,
				wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
			)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, nil, errors.Join(
				fmt.Errorf("path component %q is a symbolic link", component),
				wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
			)
		}
		if !info.IsDir() {
			return nil, nil, errors.Join(
				fmt.Errorf("path component %q is not a directory", component),
				wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
			)
		}
		child, err := root.OpenRoot(component)
		if err != nil {
			return nil, nil, errors.Join(
				err,
				wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
			)
		}
		childInfo, err := child.Stat(".")
		if err != nil || !os.SameFile(info, childInfo) {
			identityErr := err
			if identityErr == nil {
				identityErr = fmt.Errorf("physical directory identity changed")
			}
			return nil, nil, errors.Join(
				fmt.Errorf("path component %q changed while being pinned: %w", component, identityErr),
				wrapForceCloseError("absolute traversal child", operations.closeRootHandle("absolute traversal child", child)),
				wrapForceCloseError("absolute traversal root", operations.closeRootHandle("absolute traversal root", root)),
			)
		}
		if err := operations.closeRootHandle("absolute traversal root", root); err != nil {
			return nil, nil, errors.Join(
				wrapForceCloseError("absolute traversal root", err),
				wrapForceCloseError("absolute traversal child", operations.closeRootHandle("absolute traversal child", child)),
			)
		}
		root = child
		rootInfo = childInfo
	}
	return root, rootInfo, nil
}

func provePinnedAbsoluteDirectory(
	path string,
	expected fs.FileInfo,
	operations forceReplaceOperations,
) error {
	current, currentInfo, err := openPinnedAbsoluteDirectory(path, operations)
	if err != nil {
		return err
	}
	var proofErr error
	if !os.SameFile(expected, currentInfo) {
		proofErr = fmt.Errorf("physical directory identity changed")
	}
	return errors.Join(
		proofErr,
		wrapForceCloseError("absolute directory proof", operations.closeRootHandle("absolute directory proof", current)),
	)
}

func rejectProtectedForceIdentity(candidate fs.FileInfo) error {
	protectedPaths := []string{string(os.PathSeparator)}
	if home, err := os.UserHomeDir(); err == nil {
		protectedPaths = append(protectedPaths, home)
	}
	if repository, err := findRepositoryRoot(); err == nil {
		protectedPaths = append(protectedPaths, repository)
	}
	for _, protected := range protectedPaths {
		resolved := protected
		if physical, err := filepath.EvalSymlinks(protected); err == nil {
			resolved = physical
		}
		for {
			info, err := os.Stat(resolved)
			if err == nil && os.SameFile(candidate, info) {
				return fmt.Errorf("unsafe dump replacement target: physical root, home, or repository ancestor is protected")
			}
			parent := filepath.Dir(resolved)
			if parent == resolved {
				break
			}
			resolved = parent
		}
	}
	return nil
}

func quarantineForceTarget(directory *os.File, name string) (string, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate force quarantine name: %w", err)
		}
		quarantine := ".ret-force-" + hex.EncodeToString(random[:]) + ".preserved"
		if err := forceRenameNoReplace(directory, name, quarantine); err == nil {
			return quarantine, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	return "", fmt.Errorf("force quarantine name attempts exhausted")
}

func restoreForceQuarantine(
	directory *os.File,
	parentPath string,
	original string,
	quarantine string,
) error {
	if err := forceRenameNoReplace(directory, quarantine, original); err != nil {
		originalPath := filepath.Join(parentPath, original)
		quarantinePath := filepath.Join(parentPath, quarantine)
		return fmt.Errorf(
			"restore preserved prior collection %q to %q: %w; preserving both pathnames: prior collection at %q; competing destination at %q",
			quarantinePath,
			originalPath,
			err,
			quarantinePath,
			originalPath,
		)
	}
	return nil
}

func (s forceReplaceOperations) closeRootHandle(role string, root *os.Root) error {
	if s.closeRoot != nil {
		return s.closeRoot(role, root)
	}
	return root.Close()
}

func (s forceReplaceOperations) closeFileHandle(role string, file *os.File) error {
	if s.closeFile != nil {
		return s.closeFile(role, file)
	}
	return file.Close()
}

func wrapForceCloseError(description string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close force replacement %s: %w", description, err)
}

func findRepositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	current, err = filepath.Abs(current)
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	for {
		if _, err := os.Lstat(filepath.Join(current, ".git")); err == nil {
			return current, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect repository root: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("repository root not found")
		}
		current = parent
	}
}

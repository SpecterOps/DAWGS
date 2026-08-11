// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// captureBundleVersion identifies the serialized schema revision for capture bundle.
const captureBundleVersion = 1

// CaptureBundleManifest inventories the benchmark artifacts and source provenance copied into a portable bundle.
type CaptureBundleManifest struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Environment captures the environment in which the measurement ran.
	Environment RunEnvironment `json:"environment"`
	// RecordCount records case-result records included in the capture bundle.
	RecordCount int `json:"record_count"`
	// CorpusDeclaration contains the exact selected corpus declaration bundled for replay.
	CorpusDeclaration string `json:"corpus_declaration"`
	// RawArtifact identifies the uncopied artifact used as bundle input.
	RawArtifact string `json:"raw_artifact"`
	// Executable captures executable path, digest, and build metadata.
	Executable string `json:"executable"`
	// SourcePatch contains the tracked working-tree patch preserved as source provenance.
	SourcePatch string `json:"source_patch"`
	// UntrackedManifest names the bundle-relative JSON inventory of copied untracked sources.
	UntrackedManifest string `json:"untracked_manifest"`
}

// UntrackedSource describes an untracked source file copied into an artifact bundle.
type UntrackedSource struct {
	// Path records the untracked source path relative to the repository root.
	Path string `json:"path"`
	// SHA256 verifies the copied file's contents without depending on its path.
	SHA256 string `json:"sha256"`
	// Copy identifies the bundle-relative copy of an untracked source file.
	Copy string `json:"copy"`
}

// writeCaptureBundle copies run artifacts and provenance into a checksummed portable bundle.
func writeCaptureBundle(root string, corpus ScaleCorpus, records []CaseResult, environment RunEnvironment) error {
	root = filepath.Clean(root)
	if root == "." || root == string(filepath.Separator) {
		return fmt.Errorf("bundle directory must be a dedicated path")
	}
	untracked, err := listUntrackedSources(root)
	if err != nil {
		return err
	}
	for _, dir := range []string{root, filepath.Join(root, "bin"), filepath.Join(root, "source-untracked")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	patch, err := exec.Command("git", "diff", "--binary", "HEAD", "--").Output()
	if err != nil {
		return fmt.Errorf("capture tracked source patch: %w", err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.patch"), patch, 0o644); err != nil {
		return err
	}

	untrackedManifest := make([]UntrackedSource, 0, len(untracked))
	for _, source := range untracked {
		destination := filepath.Join(root, "source-untracked", source)
		if err := copyRegularFile(source, destination, 0o644); err != nil {
			return fmt.Errorf("copy untracked source %s: %w", source, err)
		}
		checksum, err := fileSHA256(source)
		if err != nil {
			return err
		}
		untrackedManifest = append(untrackedManifest, UntrackedSource{
			Path:   filepath.ToSlash(source),
			SHA256: checksum,
			Copy:   filepath.ToSlash(filepath.Join("source-untracked", source)),
		})
	}
	if err := writeIndentedJSON(filepath.Join(root, "source-untracked-manifest.json"), untrackedManifest); err != nil {
		return err
	}

	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binaryName := "graphbench-" + environment.BinarySHA256
	if err := copyRegularFile(executable, filepath.Join(root, "bin", binaryName), 0o755); err != nil {
		return fmt.Errorf("copy executable: %w", err)
	}
	if err := copyRegularFile("go.mod", filepath.Join(root, "go.mod"), 0o644); err != nil {
		return err
	}
	if err := copyRegularFile("go.sum", filepath.Join(root, "go.sum"), 0o644); err != nil {
		return err
	}
	if err := writeIndentedJSON(filepath.Join(root, "corpus-declaration.json"), corpus.DeclaredBackends()); err != nil {
		return err
	}
	if err := writeBundleJSONL(filepath.Join(root, "combined.jsonl"), records); err != nil {
		return err
	}

	manifest := CaptureBundleManifest{
		Version:           captureBundleVersion,
		Environment:       environment,
		RecordCount:       len(records),
		CorpusDeclaration: "corpus-declaration.json",
		RawArtifact:       "combined.jsonl",
		Executable:        filepath.ToSlash(filepath.Join("bin", binaryName)),
		SourcePatch:       "source.patch",
		UntrackedManifest: "source-untracked-manifest.json",
	}
	if err := writeIndentedJSON(filepath.Join(root, "manifest.json"), manifest); err != nil {
		return err
	}
	return writeBundleChecksums(root)
}

// listUntrackedSources returns untracked repository files eligible for inclusion in the bundle.
func listUntrackedSources(bundleRoot string) ([]string, error) {
	output, err := exec.Command("git", "ls-files", "--others", "--exclude-standard").Output()
	if err != nil {
		return nil, fmt.Errorf("list untracked source: %w", err)
	}
	absRoot, _ := filepath.Abs(bundleRoot)
	var paths []string
	for _, path := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if path == "" {
			continue
		}
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if absPath == absRoot || strings.HasPrefix(absPath, absRoot+string(filepath.Separator)) {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if info.Mode().IsRegular() {
			paths = append(paths, filepath.Clean(path))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// copyRegularFile copies one regular file to a newly created bundle path with the requested mode.
func copyRegularFile(source, destination string, mode os.FileMode) (err error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	_, err = io.Copy(output, input)
	return err
}

// writeIndentedJSON writes one value as indented JSON with a trailing newline.
func writeIndentedJSON(path string, value any) (err error) {
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

// writeBundleJSONL writes case records as JSON Lines inside an artifact bundle.
func writeBundleJSONL(path string, records []CaseResult) (err error) {
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	return writeJSONL(output, records)
}

// writeBundleChecksums writes sorted SHA-256 entries for every bundled file except the checksum file.
func writeBundleChecksums(root string) error {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || path == filepath.Join(root, "checksums.sha256") {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(paths)
	var lines strings.Builder
	for _, path := range paths {
		checksum, err := fileSHA256(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fmt.Fprintf(&lines, "%s  %s\n", checksum, filepath.ToSlash(relative))
	}
	return os.WriteFile(filepath.Join(root, "checksums.sha256"), []byte(lines.String()), 0o644)
}

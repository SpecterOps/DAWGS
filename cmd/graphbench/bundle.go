// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"crypto/sha256"
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
const captureBundleVersion = 3

const captureBundleChecksumFile = "checksums.sha256"

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
	// SourceClean reports whether the captured working-tree fingerprint contains no tracked or untracked changes.
	SourceClean bool `json:"source_clean"`
	// Evidence contains named, checksummed gate and plan artifacts copied into the bundle.
	Evidence []CaptureBundleEvidence `json:"evidence,omitempty"`
}

// CaptureCorpusDeclaration preserves every selected workload field needed to
// reconstruct the exact benchmark corpus rather than only its backend index.
type CaptureCorpusDeclaration struct {
	Version int         `json:"version"`
	Cases   []ScaleCase `json:"cases"`
}

// CaptureBundleEvidence identifies one auxiliary plan, A/A, correctness, resource, or decision artifact.
type CaptureBundleEvidence struct {
	// Name is a stable, user-supplied evidence identity.
	Name string `json:"name"`
	// SourceSHA256 identifies the exact input bytes before copying.
	SourceSHA256 string `json:"source_sha256"`
	// Copy names the bundle-relative payload path.
	Copy string `json:"copy"`
}

// CaptureBundleEvidenceInput supplies one auxiliary artifact to a capture bundle.
type CaptureBundleEvidenceInput struct {
	// Name is serialized as the evidence identity and file name stem.
	Name string
	// Path locates the source artifact copied into the bundle.
	Path string
}

// CaptureBundleVerification is the fail-closed result of validating a portable bundle.
type CaptureBundleVerification struct {
	// Version identifies this verification result schema.
	Version int `json:"version"`
	// ManifestVersion is the bundle schema version read from manifest.json.
	ManifestVersion int `json:"manifest_version"`
	// SourceClean reports the source state declared by the bundle manifest.
	SourceClean bool `json:"source_clean"`
	// CheckedFiles records how many checksummed payload files were verified.
	CheckedFiles int `json:"checked_files"`
	// RecordCount records how many JSONL case records were decoded and matched to the manifest.
	RecordCount int `json:"record_count"`
	// Passed reports whether every structural, checksum, and provenance invariant succeeded.
	Passed bool `json:"passed"`
	// Reasons contains stable validation failures when Passed is false.
	Reasons []string `json:"reasons,omitempty"`
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
	return writeCaptureBundleWithEvidence(root, corpus, records, environment, nil)
}

// writeCaptureBundleWithEvidence copies run artifacts, auxiliary evidence, and provenance into a checksummed portable bundle.
func writeCaptureBundleWithEvidence(root string, corpus ScaleCorpus, records []CaseResult, environment RunEnvironment, evidenceInputs []CaptureBundleEvidenceInput) error {
	root = filepath.Clean(root)
	if root == "." || root == string(filepath.Separator) {
		return fmt.Errorf("bundle directory must be a dedicated path")
	}
	if err := validateCaptureBundleDestination(root); err != nil {
		return err
	}
	currentFingerprint, err := calculateWorkingTreeSHA256(root)
	if err != nil {
		return fmt.Errorf("fingerprint current source before bundle capture: %w", err)
	}
	if !isLowerHexSHA256(environment.DirtyDiffSHA256) || currentFingerprint != environment.DirtyDiffSHA256 {
		return fmt.Errorf("current source fingerprint %s differs from run environment fingerprint %s", currentFingerprint, environment.DirtyDiffSHA256)
	}
	untracked, err := listUntrackedSources(root)
	if err != nil {
		return err
	}
	for _, dir := range []string{root, filepath.Join(root, "artifacts"), filepath.Join(root, "bin"), filepath.Join(root, "source-untracked")} {
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
	capturedFingerprint, err := capturedWorkingTreeSHA256(patch, untrackedManifest, root)
	if err != nil {
		return fmt.Errorf("fingerprint captured source: %w", err)
	}
	if !isLowerHexSHA256(environment.DirtyDiffSHA256) || capturedFingerprint != environment.DirtyDiffSHA256 {
		return fmt.Errorf("captured source fingerprint %s differs from run environment fingerprint %s", capturedFingerprint, environment.DirtyDiffSHA256)
	}
	currentFingerprint, err = calculateWorkingTreeSHA256(root)
	if err != nil {
		return fmt.Errorf("fingerprint current source after bundle capture: %w", err)
	}
	if currentFingerprint != environment.DirtyDiffSHA256 {
		return fmt.Errorf("source changed during bundle capture: current fingerprint %s differs from run environment fingerprint %s", currentFingerprint, environment.DirtyDiffSHA256)
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
	cases := append([]ScaleCase(nil), corpus.Cases...)
	sort.Slice(cases, func(i, j int) bool {
		if cases[i].Source != cases[j].Source {
			return cases[i].Source < cases[j].Source
		}
		if cases[i].Dataset != cases[j].Dataset {
			return cases[i].Dataset < cases[j].Dataset
		}
		return cases[i].Name < cases[j].Name
	})
	if err := writeIndentedJSON(filepath.Join(root, "corpus-declaration.json"), CaptureCorpusDeclaration{Version: 2, Cases: cases}); err != nil {
		return err
	}
	if err := writeBundleJSONL(filepath.Join(root, "combined.jsonl"), records); err != nil {
		return err
	}
	evidence, err := copyCaptureBundleEvidence(root, evidenceInputs)
	if err != nil {
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
		SourceClean:       environment.DirtyDiffSHA256 == cleanWorkingTreeSHA256(),
		Evidence:          evidence,
	}
	if err := writeIndentedJSON(filepath.Join(root, "manifest.json"), manifest); err != nil {
		return err
	}
	if err := writeBundleChecksums(root); err != nil {
		return err
	}
	verification, err := verifyCaptureBundle(root, false)
	if err != nil {
		return err
	}
	if !verification.Passed {
		return fmt.Errorf("verify capture bundle: %s", strings.Join(verification.Reasons, "; "))
	}
	return nil
}

// capturedWorkingTreeSHA256 reconstructs the exact byte framing used by
// workingTreeSHA256 from the patch and copied untracked payloads in a bundle.
func capturedWorkingTreeSHA256(patch []byte, untracked []UntrackedSource, root string) (string, error) {
	digest := sha256.New()
	writeWorkingTreePatchFingerprint(digest, patch)
	entries := append([]UntrackedSource(nil), untracked...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	seenPaths := map[string]struct{}{}
	seenCopies := map[string]struct{}{}
	for index, source := range entries {
		if !validUntrackedSourcePath(source.Path) {
			return "", fmt.Errorf("untracked source %d has invalid path %q", index, source.Path)
		}
		if _, duplicate := seenPaths[source.Path]; duplicate {
			return "", fmt.Errorf("untracked source path %q is duplicated", source.Path)
		}
		seenPaths[source.Path] = struct{}{}
		if !isLowerHexSHA256(source.SHA256) {
			return "", fmt.Errorf("untracked source %q has invalid SHA-256", source.Path)
		}
		expectedCopy := filepath.ToSlash(filepath.Join("source-untracked", filepath.FromSlash(source.Path)))
		if source.Copy != expectedCopy {
			return "", fmt.Errorf("untracked source %q has noncanonical copy %q; expected %q", source.Path, source.Copy, expectedCopy)
		}
		copyPath, err := resolveBundlePath(root, source.Copy)
		if err != nil {
			return "", fmt.Errorf("untracked source %q copy: %w", source.Path, err)
		}
		if _, duplicate := seenCopies[source.Copy]; duplicate {
			return "", fmt.Errorf("untracked source copy %q is duplicated", source.Copy)
		}
		seenCopies[source.Copy] = struct{}{}
		content, err := os.ReadFile(copyPath)
		if err != nil {
			return "", fmt.Errorf("read untracked source %q copy: %w", source.Path, err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(content))
		if actual != source.SHA256 {
			return "", fmt.Errorf("untracked source %q digest does not match its copy", source.Path)
		}
		writeWorkingTreeUntrackedFingerprint(digest, source.Path, content)
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), nil
}

func validUntrackedSourcePath(path string) bool {
	if path == "" || filepath.IsAbs(path) || path != filepath.ToSlash(path) {
		return false
	}
	clean := filepath.Clean(filepath.FromSlash(path))
	return clean != "." && clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator)) && filepath.ToSlash(clean) == path
}

// validateCaptureBundleDestination rejects symlinks, non-directories, and stale
// payloads so every checksum inventory is constructed in a fresh destination.
func validateCaptureBundleDestination(root string) error {
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect bundle destination: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("bundle destination must be a directory")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("inspect bundle destination: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("bundle destination must not already contain files")
	}
	return nil
}

// copyCaptureBundleEvidence validates stable names and copies every auxiliary artifact into the bundle.
func copyCaptureBundleEvidence(root string, inputs []CaptureBundleEvidenceInput) ([]CaptureBundleEvidence, error) {
	seen := map[string]struct{}{}
	evidence := make([]CaptureBundleEvidence, 0, len(inputs))
	for _, input := range inputs {
		name := strings.TrimSpace(input.Name)
		if !validBundleEvidenceName(name) {
			return nil, fmt.Errorf("invalid capture bundle evidence name %q", input.Name)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("duplicate capture bundle evidence name %q", name)
		}
		seen[name] = struct{}{}
		info, err := os.Lstat(input.Path)
		if err != nil {
			return nil, fmt.Errorf("stat capture bundle evidence %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("capture bundle evidence %q is not a regular file", name)
		}
		extension := strings.ToLower(filepath.Ext(input.Path))
		if extension == "" || len(extension) > 10 {
			extension = ".artifact"
		}
		relative := filepath.ToSlash(filepath.Join("artifacts", name+extension))
		if err := copyRegularFile(input.Path, filepath.Join(root, filepath.FromSlash(relative)), 0o644); err != nil {
			return nil, fmt.Errorf("copy capture bundle evidence %q: %w", name, err)
		}
		digest, err := fileSHA256(input.Path)
		if err != nil {
			return nil, err
		}
		evidence = append(evidence, CaptureBundleEvidence{Name: name, SourceSHA256: digest, Copy: relative})
	}
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].Name < evidence[j].Name })
	return evidence, nil
}

// validBundleEvidenceName accepts stable path-independent artifact identities.
func validBundleEvidenceName(name string) bool {
	if name == "" {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

// cleanWorkingTreeSHA256 returns the fingerprint emitted by workingTreeSHA256 for a clean source tree.
func cleanWorkingTreeSHA256() string {
	return fmt.Sprintf("%x", sha256.Sum256(nil))
}

// listUntrackedSources returns untracked repository files eligible for inclusion in the bundle.
func listUntrackedSources(bundleRoot string) ([]string, error) {
	gitPaths, err := gitUntrackedPaths()
	if err != nil {
		return nil, err
	}
	absRoot, _ := filepath.Abs(bundleRoot)
	var paths []string
	for _, path := range gitPaths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if absPath == absRoot || strings.HasPrefix(absPath, absRoot+string(filepath.Separator)) {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("untracked source %q is not a regular file", path)
		}
		paths = append(paths, filepath.Clean(path))
	}
	return paths, nil
}

// copyRegularFile copies one regular file to a newly created bundle path with the requested mode.
func copyRegularFile(source, destination string, mode os.FileMode) (err error) {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("source is not a regular file")
	}
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
		if entry.IsDir() || path == filepath.Join(root, captureBundleChecksumFile) {
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
	return os.WriteFile(filepath.Join(root, captureBundleChecksumFile), []byte(lines.String()), 0o644)
}

// verifyCaptureBundle validates bundle structure, every payload checksum, source provenance, and record count.
// When requireCleanSource is true, diagnostic bundles carrying a patch or untracked source are rejected.
func verifyCaptureBundle(root string, requireCleanSource bool) (CaptureBundleVerification, error) {
	report := CaptureBundleVerification{Version: 1, Passed: true}
	root = filepath.Clean(root)
	rootInfo, err := os.Stat(root)
	if err != nil {
		return report, fmt.Errorf("stat capture bundle: %w", err)
	}
	if !rootInfo.IsDir() {
		return report, fmt.Errorf("capture bundle path is not a directory: %s", root)
	}

	checksums, reasons, err := readBundleChecksums(root)
	if err != nil {
		return report, err
	}
	report.Reasons = append(report.Reasons, reasons...)
	for relative, expected := range checksums {
		path, pathErr := resolveBundlePath(root, relative)
		if pathErr != nil {
			report.Reasons = append(report.Reasons, pathErr.Error())
			continue
		}
		info, statErr := os.Lstat(path)
		if statErr != nil {
			report.Reasons = append(report.Reasons, fmt.Sprintf("checksummed file %q is unavailable: %v", relative, statErr))
			continue
		}
		if !info.Mode().IsRegular() {
			report.Reasons = append(report.Reasons, fmt.Sprintf("checksummed path %q is not a regular file", relative))
			continue
		}
		actual, digestErr := fileSHA256(path)
		if digestErr != nil {
			report.Reasons = append(report.Reasons, fmt.Sprintf("checksum %q: %v", relative, digestErr))
			continue
		}
		if actual != expected {
			report.Reasons = append(report.Reasons, fmt.Sprintf("checksum mismatch for %q", relative))
			continue
		}
		report.CheckedFiles++
	}

	listedReasons, err := verifyBundleFileInventory(root, checksums)
	if err != nil {
		return report, err
	}
	report.Reasons = append(report.Reasons, listedReasons...)

	manifest, reasons := verifyBundleManifest(root, checksums)
	report.ManifestVersion = manifest.Version
	report.SourceClean = manifest.SourceClean
	report.Reasons = append(report.Reasons, reasons...)
	report.Reasons = append(report.Reasons, verifyBundleCorpus(root, manifest)...)
	if requireCleanSource && !manifest.SourceClean {
		report.Reasons = append(report.Reasons, "bundle source is not clean")
	}

	recordCount, reasons := verifyBundleRecords(root, manifest)
	report.RecordCount = recordCount
	report.Reasons = append(report.Reasons, reasons...)
	report.Passed = len(report.Reasons) == 0
	return report, nil
}

func verifyBundleCorpus(root string, manifest CaptureBundleManifest) []string {
	path, err := resolveBundlePath(root, manifest.CorpusDeclaration)
	if err != nil {
		return []string{err.Error()}
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return []string{fmt.Sprintf("read corpus declaration: %v", err)}
	}
	var declaration CaptureCorpusDeclaration
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&declaration); err != nil {
		return []string{fmt.Sprintf("decode corpus declaration: %v", err)}
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return []string{"corpus declaration contains trailing JSON data"}
	}
	if declaration.Version != 2 {
		return []string{fmt.Sprintf("unsupported corpus declaration version %d", declaration.Version)}
	}
	identity := corpusIdentity(ScaleCorpus{Cases: declaration.Cases})
	if identity != manifest.Environment.CorpusSHA256 {
		return []string{fmt.Sprintf("corpus declaration identity %s differs from manifest %s", identity, manifest.Environment.CorpusSHA256)}
	}
	return nil
}

// createCaptureBundleVerification validates a portable bundle, writes its complete
// verification result, and reports whether it passed every requested invariant.
func createCaptureBundleVerification(root, outputPath string, requireCleanSource bool) (passed bool, err error) {
	if outputPath != "" {
		absoluteRoot, rootErr := filepath.Abs(filepath.Clean(root))
		absoluteOutput, outputErr := filepath.Abs(filepath.Clean(outputPath))
		if rootErr != nil {
			return false, rootErr
		}
		if outputErr != nil {
			return false, outputErr
		}
		if absoluteOutput == absoluteRoot || strings.HasPrefix(absoluteOutput, absoluteRoot+string(filepath.Separator)) {
			return false, fmt.Errorf("bundle verification output must be outside the verified bundle")
		}
	}
	report, err := verifyCaptureBundle(root, requireCleanSource)
	if err != nil {
		return false, err
	}

	var output *os.File
	if outputPath == "" {
		output = os.Stdout
	} else {
		if err := ensureOutputDir(outputPath); err != nil {
			return false, err
		}
		output, err = os.Create(outputPath)
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := output.Close(); err == nil && closeErr != nil {
				err = closeErr
				passed = false
			}
		}()
	}

	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return false, err
	}
	return report.Passed, nil
}

// readBundleChecksums parses the deterministic SHA-256 manifest without trusting its paths.
func readBundleChecksums(root string) (map[string]string, []string, error) {
	path := filepath.Join(root, captureBundleChecksumFile)
	input, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open capture bundle checksums: %w", err)
	}
	defer input.Close()

	checksums := map[string]string{}
	var reasons []string
	scanner := bufio.NewScanner(input)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		separator := strings.Index(line, "  ")
		if separator != 64 || len(line) <= separator+2 {
			reasons = append(reasons, fmt.Sprintf("malformed checksum line %d", lineNumber))
			continue
		}
		digest := line[:separator]
		relative := line[separator+2:]
		if !isLowerHexSHA256(digest) {
			reasons = append(reasons, fmt.Sprintf("invalid SHA-256 on checksum line %d", lineNumber))
			continue
		}
		if _, duplicate := checksums[relative]; duplicate {
			reasons = append(reasons, fmt.Sprintf("duplicate checksum path %q", relative))
			continue
		}
		checksums[relative] = digest
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("read capture bundle checksums: %w", err)
	}
	if len(checksums) == 0 {
		reasons = append(reasons, "capture bundle checksum manifest is empty")
	}
	return checksums, reasons, nil
}

// isLowerHexSHA256 reports whether value is one canonical lowercase SHA-256 digest.
func isLowerHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

// resolveBundlePath rejects absolute, parent, platform-ambiguous, and checksum-self references.
func resolveBundlePath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || relative != filepath.ToSlash(relative) {
		return "", fmt.Errorf("invalid bundle-relative path %q", relative)
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || relative == captureBundleChecksumFile {
		return "", fmt.Errorf("invalid bundle-relative path %q", relative)
	}
	return filepath.Join(root, clean), nil
}

// verifyBundleFileInventory rejects unchecksummed payload files and missing checksum entries.
func verifyBundleFileInventory(root string, checksums map[string]string) ([]string, error) {
	var reasons []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root {
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if info.Mode()&os.ModeSymlink != 0 {
					reasons = append(reasons, fmt.Sprintf("bundle contains symlink directory %q", path))
					return filepath.SkipDir
				}
			}
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == captureBundleChecksumFile {
			return nil
		}
		if _, listed := checksums[relative]; !listed {
			reasons = append(reasons, fmt.Sprintf("unchecksummed bundle file %q", relative))
		}
		return nil
	})
	return reasons, err
}

// verifyBundleManifest decodes the manifest and validates every referenced payload identity.
func verifyBundleManifest(root string, checksums map[string]string) (CaptureBundleManifest, []string) {
	var manifest CaptureBundleManifest
	var reasons []string
	manifestPath, present := checksums["manifest.json"]
	if !present || manifestPath == "" {
		return manifest, []string{"manifest.json is not checksummed"}
	}
	content, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return manifest, []string{fmt.Sprintf("read manifest.json: %v", err)}
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, []string{fmt.Sprintf("decode manifest.json: %v", err)}
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return manifest, []string{"manifest.json contains trailing JSON data"}
	}
	if manifest.Version != captureBundleVersion {
		reasons = append(reasons, fmt.Sprintf("unsupported capture bundle version %d", manifest.Version))
	}
	for label, relative := range map[string]string{
		"corpus declaration": manifest.CorpusDeclaration,
		"raw artifact":       manifest.RawArtifact,
		"executable":         manifest.Executable,
		"source patch":       manifest.SourcePatch,
		"untracked manifest": manifest.UntrackedManifest,
	} {
		if _, err := resolveBundlePath(root, relative); err != nil {
			reasons = append(reasons, fmt.Sprintf("%s: %v", label, err))
			continue
		}
		if _, exists := checksums[relative]; !exists {
			reasons = append(reasons, fmt.Sprintf("%s %q is not checksummed", label, relative))
		}
	}
	evidenceNames := map[string]struct{}{}
	for _, artifact := range manifest.Evidence {
		if !validBundleEvidenceName(artifact.Name) {
			reasons = append(reasons, fmt.Sprintf("invalid evidence name %q", artifact.Name))
		}
		if _, duplicate := evidenceNames[artifact.Name]; duplicate {
			reasons = append(reasons, fmt.Sprintf("duplicate evidence name %q", artifact.Name))
		}
		evidenceNames[artifact.Name] = struct{}{}
		path, pathErr := resolveBundlePath(root, artifact.Copy)
		if pathErr != nil {
			reasons = append(reasons, fmt.Sprintf("evidence %q: %v", artifact.Name, pathErr))
			continue
		}
		listedDigest, listed := checksums[artifact.Copy]
		if !listed {
			reasons = append(reasons, fmt.Sprintf("evidence %q copy %q is not checksummed", artifact.Name, artifact.Copy))
			continue
		}
		if !isLowerHexSHA256(artifact.SourceSHA256) || listedDigest != artifact.SourceSHA256 {
			reasons = append(reasons, fmt.Sprintf("evidence %q source identity does not match its bundled copy", artifact.Name))
			continue
		}
		if digest, digestErr := fileSHA256(path); digestErr != nil || digest != artifact.SourceSHA256 {
			reasons = append(reasons, fmt.Sprintf("evidence %q payload identity is invalid", artifact.Name))
		}
	}
	if manifest.Environment.BinarySHA256 == "" || manifest.Environment.BinarySHA256 == "unknown" {
		reasons = append(reasons, "manifest has no concrete executable SHA-256")
	} else if executablePath, err := resolveBundlePath(root, manifest.Executable); err == nil {
		if digest, digestErr := fileSHA256(executablePath); digestErr != nil || digest != manifest.Environment.BinarySHA256 {
			reasons = append(reasons, "manifest executable identity does not match bundled executable")
		}
	}
	if manifest.Environment.SourceCommit == "" || manifest.Environment.SourceCommit == "unknown" {
		reasons = append(reasons, "manifest has no concrete source commit")
	}
	if manifest.SourceClean && manifest.Environment.DirtyDiffSHA256 != cleanWorkingTreeSHA256() {
		reasons = append(reasons, "clean-source declaration contradicts dirty source fingerprint")
	}
	if manifest.SourceClean {
		patchPath, patchErr := resolveBundlePath(root, manifest.SourcePatch)
		if patchErr == nil {
			if patchInfo, err := os.Stat(patchPath); err != nil || patchInfo.Size() != 0 {
				reasons = append(reasons, "clean-source bundle contains a non-empty source patch")
			}
		}
		untrackedPath, untrackedErr := resolveBundlePath(root, manifest.UntrackedManifest)
		if untrackedErr == nil {
			var untracked []UntrackedSource
			content, err := os.ReadFile(untrackedPath)
			if err != nil || json.Unmarshal(content, &untracked) != nil || len(untracked) != 0 {
				reasons = append(reasons, "clean-source bundle contains untracked source entries")
			}
		}
	}
	patchPath, patchErr := resolveBundlePath(root, manifest.SourcePatch)
	untrackedPath, untrackedErr := resolveBundlePath(root, manifest.UntrackedManifest)
	if patchErr == nil && untrackedErr == nil {
		patch, readPatchErr := os.ReadFile(patchPath)
		untracked, decodeReasons := readUntrackedSourceManifest(untrackedPath)
		reasons = append(reasons, decodeReasons...)
		if readPatchErr != nil {
			reasons = append(reasons, fmt.Sprintf("read bundled source patch: %v", readPatchErr))
		} else if len(decodeReasons) == 0 {
			fingerprint, fingerprintErr := capturedWorkingTreeSHA256(patch, untracked, root)
			if fingerprintErr != nil {
				reasons = append(reasons, "reconstruct bundled source fingerprint: "+fingerprintErr.Error())
			} else {
				if !isLowerHexSHA256(manifest.Environment.DirtyDiffSHA256) || fingerprint != manifest.Environment.DirtyDiffSHA256 {
					reasons = append(reasons, "manifest dirty source fingerprint does not match bundled patch and untracked sources")
				}
				if manifest.SourceClean != (fingerprint == cleanWorkingTreeSHA256()) {
					reasons = append(reasons, "source_clean declaration does not match bundled source fingerprint")
				}
			}
		}
		manifestCopies := map[string]struct{}{}
		for _, source := range untracked {
			manifestCopies[source.Copy] = struct{}{}
			if _, listed := checksums[source.Copy]; !listed {
				reasons = append(reasons, fmt.Sprintf("untracked source %q copy %q is not checksummed", source.Path, source.Copy))
			}
		}
		for relative := range checksums {
			if strings.HasPrefix(relative, "source-untracked/") {
				if _, declared := manifestCopies[relative]; !declared {
					reasons = append(reasons, fmt.Sprintf("checksummed untracked source copy %q has no manifest entry", relative))
				}
			}
		}
	}
	return manifest, reasons
}

func readUntrackedSourceManifest(path string) ([]UntrackedSource, []string) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, []string{fmt.Sprintf("read untracked source manifest: %v", err)}
	}
	if len(strings.TrimSpace(string(content))) == 0 || strings.TrimSpace(string(content))[0] != '[' {
		return nil, []string{"untracked source manifest must be a JSON array"}
	}
	var sources []UntrackedSource
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&sources); err != nil {
		return nil, []string{fmt.Sprintf("decode untracked source manifest: %v", err)}
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, []string{"untracked source manifest contains trailing JSON data"}
	}
	return sources, nil
}

// verifyBundleRecords decodes the JSONL payload and binds every record to the manifest environment.
func verifyBundleRecords(root string, manifest CaptureBundleManifest) (int, []string) {
	artifactPath, err := resolveBundlePath(root, manifest.RawArtifact)
	if err != nil {
		return 0, []string{err.Error()}
	}
	records, err := readJSONLFile(artifactPath)
	if err != nil {
		return 0, []string{fmt.Sprintf("decode bundled records: %v", err)}
	}
	var reasons []string
	if len(records) != manifest.RecordCount {
		reasons = append(reasons, fmt.Sprintf("manifest record count %d does not match artifact count %d", manifest.RecordCount, len(records)))
	}
	for index, record := range records {
		if record.Environment == nil {
			reasons = append(reasons, fmt.Sprintf("record %d has no environment provenance", index))
			continue
		}
		if record.Environment.BinarySHA256 != manifest.Environment.BinarySHA256 ||
			record.Environment.SourceCommit != manifest.Environment.SourceCommit ||
			record.Environment.DirtyDiffSHA256 != manifest.Environment.DirtyDiffSHA256 ||
			record.Environment.CorpusSHA256 != manifest.Environment.CorpusSHA256 {
			reasons = append(reasons, fmt.Sprintf("record %d provenance does not match bundle manifest", index))
		}
	}
	return len(records), reasons
}

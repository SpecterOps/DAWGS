// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVerifyCaptureBundleValidatesChecksumsAndProvenance exercises the portable bundle verifier without depending on a built graphbench executable.
func TestVerifyCaptureBundleValidatesChecksumsAndProvenance(t *testing.T) {
	root := t.TempDir()
	environment := RunEnvironment{
		ArtifactSchemaVersion: 2,
		CorpusSHA256:          corpusIdentity(ScaleCorpus{}),
		SourceCommit:          "commit",
		DirtyDiffSHA256:       cleanWorkingTreeSHA256(),
		BinarySHA256:          "placeholder",
	}
	record := CaseResult{
		Environment:   &environment,
		Dataset:       "fixture",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bin"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "bin", "graphbench"), []byte("binary"), 0o755))
	binarySHA, err := fileSHA256(filepath.Join(root, "bin", "graphbench"))
	require.NoError(t, err)
	environment.BinarySHA256 = binarySHA
	record.Environment.BinarySHA256 = binarySHA

	require.NoError(t, os.WriteFile(filepath.Join(root, "source.patch"), nil, 0o644))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "source-untracked-manifest.json"), []UntrackedSource{}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "corpus-declaration.json"), CaptureCorpusDeclaration{Version: 2}))
	require.NoError(t, writeBundleJSONL(filepath.Join(root, "combined.jsonl"), []CaseResult{record}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "manifest.json"), CaptureBundleManifest{
		Version:           captureBundleVersion,
		Environment:       environment,
		RecordCount:       1,
		CorpusDeclaration: "corpus-declaration.json",
		RawArtifact:       "combined.jsonl",
		Executable:        "bin/graphbench",
		SourcePatch:       "source.patch",
		UntrackedManifest: "source-untracked-manifest.json",
		SourceClean:       true,
	}))
	require.NoError(t, writeBundleChecksums(root))

	report, err := verifyCaptureBundle(root, true)
	require.NoError(t, err)
	require.True(t, report.Passed, report.Reasons)
	require.Equal(t, 6, report.CheckedFiles)
	require.Equal(t, 1, report.RecordCount)

	outputPath := filepath.Join(t.TempDir(), "verification.json")
	passed, err := createCaptureBundleVerification(root, outputPath, true)
	require.NoError(t, err)
	require.True(t, passed)
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var written CaptureBundleVerification
	require.NoError(t, json.Unmarshal(content, &written))
	require.Equal(t, report, written)

	_, err = createCaptureBundleVerification(root, filepath.Join(root, "verification.json"), true)
	require.ErrorContains(t, err, "must be outside the verified bundle")

	manifestPath := filepath.Join(root, "manifest.json")
	manifestContent, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, append(manifestContent, []byte("{}\n")...), 0o644))
	require.NoError(t, writeBundleChecksums(root))
	report, err = verifyCaptureBundle(root, true)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Contains(t, report.Reasons, "manifest.json contains trailing JSON data")
}

// TestVerifyCaptureBundleFailsClosedOnTamperingDirtySourceAndUnlistedFiles covers the three qualification boundaries a checksum-only writer cannot enforce.
func TestVerifyCaptureBundleFailsClosedOnTamperingDirtySourceAndUnlistedFiles(t *testing.T) {
	root := t.TempDir()
	environment := RunEnvironment{
		ArtifactSchemaVersion: 2,
		CorpusSHA256:          corpusIdentity(ScaleCorpus{}),
		SourceCommit:          "commit",
		DirtyDiffSHA256:       "dirty",
		BinarySHA256:          "placeholder",
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "binary"), []byte("binary"), 0o755))
	binarySHA, err := fileSHA256(filepath.Join(root, "binary"))
	require.NoError(t, err)
	environment.BinarySHA256 = binarySHA
	recordEnvironment := environment
	record := CaseResult{
		Environment:   &recordEnvironment,
		Dataset:       "fixture",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "source.patch"), []byte("diff"), 0o644))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "untracked.json"), []UntrackedSource{}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "corpus.json"), CaptureCorpusDeclaration{Version: 2}))
	require.NoError(t, writeBundleJSONL(filepath.Join(root, "records.jsonl"), []CaseResult{record}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "manifest.json"), CaptureBundleManifest{
		Version:           captureBundleVersion,
		Environment:       environment,
		RecordCount:       1,
		CorpusDeclaration: "corpus.json",
		RawArtifact:       "records.jsonl",
		Executable:        "binary",
		SourcePatch:       "source.patch",
		UntrackedManifest: "untracked.json",
		SourceClean:       false,
	}))
	require.NoError(t, writeBundleChecksums(root))
	require.NoError(t, os.WriteFile(filepath.Join(root, "records.jsonl"), []byte("tampered\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "unlisted"), []byte("payload"), 0o644))

	report, err := verifyCaptureBundle(root, true)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Contains(t, report.Reasons, "checksum mismatch for \"records.jsonl\"")
	require.Contains(t, report.Reasons, "unchecksummed bundle file \"unlisted\"")
	require.Contains(t, report.Reasons, "bundle source is not clean")

	outputPath := filepath.Join(t.TempDir(), "failed-verification.json")
	passed, err := createCaptureBundleVerification(root, outputPath, true)
	require.NoError(t, err)
	require.False(t, passed)
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var written CaptureBundleVerification
	require.NoError(t, json.Unmarshal(content, &written))
	require.False(t, written.Passed)
	require.NotEmpty(t, written.Reasons)
}

// TestResolveBundlePathRejectsTraversal verifies checksum manifests cannot escape the capture root.
func TestResolveBundlePathRejectsTraversal(t *testing.T) {
	_, err := resolveBundlePath(t.TempDir(), "../escape")
	require.ErrorContains(t, err, "invalid bundle-relative path")
}

// TestCopyCaptureBundleEvidenceUsesStableNamesAndDigests verifies auxiliary plan/gate inputs are copied without retaining host paths.
func TestCopyCaptureBundleEvidenceUsesStableNamesAndDigests(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(t.TempDir(), "aa-report.json")
	require.NoError(t, os.WriteFile(input, []byte(`{"version":1}`), 0o644))

	evidence, err := copyCaptureBundleEvidence(root, []CaptureBundleEvidenceInput{{
		Name: "host-aa",
		Path: input,
	}})
	require.NoError(t, err)
	require.Len(t, evidence, 1)
	require.Equal(t, "host-aa", evidence[0].Name)
	require.Equal(t, "artifacts/host-aa.json", evidence[0].Copy)
	require.FileExists(t, filepath.Join(root, "artifacts", "host-aa.json"))
	require.NotContains(t, evidence[0].Copy, filepath.Dir(input))

	_, err = copyCaptureBundleEvidence(root, []CaptureBundleEvidenceInput{{
		Name: "../escape",
		Path: input,
	}})
	require.ErrorContains(t, err, "invalid capture bundle evidence name")

	symlink := filepath.Join(t.TempDir(), "outside.json")
	require.NoError(t, os.Symlink(input, symlink))
	_, err = copyCaptureBundleEvidence(root, []CaptureBundleEvidenceInput{{
		Name: "symlink",
		Path: symlink,
	}})
	require.ErrorContains(t, err, "is not a regular file")
}

// TestWriteCaptureBundleRejectsNonemptyDestination verifies stale payloads cannot leak into a newly checksummed bundle inventory.
func TestWriteCaptureBundleRejectsNonemptyDestination(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "stale.json"), []byte("stale"), 0o644))

	err := writeCaptureBundleWithEvidence(root, ScaleCorpus{}, nil, RunEnvironment{}, nil)
	require.ErrorContains(t, err, "must not already contain files")
	require.FileExists(t, filepath.Join(root, "stale.json"))
}

// TestWriteCaptureBundleRejectsStaleRunEnvironmentFingerprint verifies write capture bundle rejects stale run environment fingerprint behavior.
func TestWriteCaptureBundleRejectsStaleRunEnvironmentFingerprint(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bundle")
	err := writeCaptureBundleWithEvidence(root, ScaleCorpus{}, nil, RunEnvironment{
		DirtyDiffSHA256: strings.Repeat("0", 64),
	}, nil)
	require.ErrorContains(t, err, "current source fingerprint")
	require.NoDirExists(t, root)
}

// TestParseNULTerminatedPathsPreservesWhitespace verifies parse nul terminated paths preserves whitespace behavior.
func TestParseNULTerminatedPathsPreservesWhitespace(t *testing.T) {
	require.Equal(t, []string{"dir/name with spaces.go", "line\nbreak.go"}, parseNULTerminatedPaths([]byte("dir/name with spaces.go\x00line\nbreak.go\x00")))
}

// TestCopyRegularFileRejectsSymlink verifies the shared source copier cannot follow an untracked-source symlink outside the repository.
func TestCopyRegularFileRejectsSymlink(t *testing.T) {
	source := filepath.Join(t.TempDir(), "outside")
	link := filepath.Join(t.TempDir(), "untracked-link")
	require.NoError(t, os.WriteFile(source, []byte("outside"), 0o644))
	require.NoError(t, os.Symlink(source, link))

	err := copyRegularFile(link, filepath.Join(t.TempDir(), "copy"), 0o644)
	require.ErrorContains(t, err, "source is not a regular file")
}

// TestVerifyCaptureBundleBindsDirtyFingerprintToPatchAndUntrackedCopies verifies verify capture bundle binds dirty fingerprint to patch and untracked copies behavior.
func TestVerifyCaptureBundleBindsDirtyFingerprintToPatchAndUntrackedCopies(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "source-untracked", "pkg"), 0o755))
	patch := []byte("diff --git a/a.go b/a.go\n")
	content := []byte("package pkg\n")
	require.NoError(t, os.WriteFile(filepath.Join(root, "source.patch"), patch, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source-untracked", "pkg", "new.go"), content, 0o644))
	contentSHA := fmt.Sprintf("%x", sha256.Sum256(content))
	untracked := []UntrackedSource{{
		Path:   "pkg/new.go",
		SHA256: contentSHA,
		Copy:   "source-untracked/pkg/new.go",
	}}
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "untracked.json"), untracked))
	fingerprint, err := capturedWorkingTreeSHA256(patch, untracked, root)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(root, "binary"), []byte("binary"), 0o755))
	binarySHA, err := fileSHA256(filepath.Join(root, "binary"))
	require.NoError(t, err)
	environment := RunEnvironment{
		SourceCommit:    "commit",
		DirtyDiffSHA256: fingerprint,
		BinarySHA256:    binarySHA,
		CorpusSHA256:    corpusIdentity(ScaleCorpus{}),
	}
	recordEnvironment := environment
	require.NoError(t, writeBundleJSONL(filepath.Join(root, "records.jsonl"), []CaseResult{{Environment: &recordEnvironment}}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "corpus.json"), CaptureCorpusDeclaration{Version: 2}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "manifest.json"), CaptureBundleManifest{
		Version:           captureBundleVersion,
		Environment:       environment,
		RecordCount:       1,
		CorpusDeclaration: "corpus.json",
		RawArtifact:       "records.jsonl",
		Executable:        "binary",
		SourcePatch:       "source.patch",
		UntrackedManifest: "untracked.json",
		SourceClean:       false,
	}))
	require.NoError(t, writeBundleChecksums(root))

	report, err := verifyCaptureBundle(root, false)
	require.NoError(t, err)
	require.True(t, report.Passed, report.Reasons)

	manifestPath := filepath.Join(root, "manifest.json")
	var manifest CaptureBundleManifest
	raw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &manifest))
	manifest.Environment.DirtyDiffSHA256 = strings.Repeat("0", 64)
	require.NoError(t, writeIndentedJSON(manifestPath, manifest))
	require.NoError(t, writeBundleChecksums(root))
	report, err = verifyCaptureBundle(root, false)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Contains(t, report.Reasons, "manifest dirty source fingerprint does not match bundled patch and untracked sources")
}

// TestVerifyCaptureBundleRejectsMalformedOrUnchecksummedUntrackedEntries verifies verify capture bundle rejects malformed or unchecksummed untracked entries behavior.
func TestVerifyCaptureBundleRejectsMalformedOrUnchecksummedUntrackedEntries(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "source-untracked"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source.patch"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source-untracked", "new.go"), []byte("package p\n"), 0o644))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "untracked.json"), []UntrackedSource{{
		Path:   "../escape.go",
		SHA256: "bad",
		Copy:   "source-untracked/new.go",
	}}))
	require.NoError(t, os.WriteFile(filepath.Join(root, "binary"), []byte("binary"), 0o755))
	binarySHA, err := fileSHA256(filepath.Join(root, "binary"))
	require.NoError(t, err)
	environment := RunEnvironment{
		SourceCommit:    "commit",
		DirtyDiffSHA256: strings.Repeat("0", 64),
		BinarySHA256:    binarySHA,
		CorpusSHA256:    corpusIdentity(ScaleCorpus{}),
	}
	recordEnvironment := environment
	require.NoError(t, writeBundleJSONL(filepath.Join(root, "records.jsonl"), []CaseResult{{Environment: &recordEnvironment}}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "corpus.json"), CaptureCorpusDeclaration{Version: 2}))
	require.NoError(t, writeIndentedJSON(filepath.Join(root, "manifest.json"), CaptureBundleManifest{
		Version:           captureBundleVersion,
		Environment:       environment,
		RecordCount:       1,
		CorpusDeclaration: "corpus.json",
		RawArtifact:       "records.jsonl",
		Executable:        "binary",
		SourcePatch:       "source.patch",
		UntrackedManifest: "untracked.json",
		SourceClean:       false,
	}))
	require.NoError(t, writeBundleChecksums(root))
	checksums, _, err := readBundleChecksums(root)
	require.NoError(t, err)
	delete(checksums, "source-untracked/new.go")
	var lines strings.Builder
	paths := make([]string, 0, len(checksums))
	for path := range checksums {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		fmt.Fprintf(&lines, "%s  %s\n", checksums[path], path)
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, captureBundleChecksumFile), []byte(lines.String()), 0o644))

	report, err := verifyCaptureBundle(root, false)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Contains(t, strings.Join(report.Reasons, "\n"), "invalid path")
	require.Contains(t, report.Reasons, "untracked source \"../escape.go\" copy \"source-untracked/new.go\" is not checksummed")
}

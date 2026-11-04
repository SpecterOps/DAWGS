// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// RunEnvironment captures source, host, invocation, fixture, and protocol identity for a benchmark run.
type RunEnvironment struct {
	// ArtifactSchemaVersion identifies the benchmark artifact schema emitted by the run.
	ArtifactSchemaVersion int `json:"artifact_schema_version"`
	// CorpusSHA256 binds run provenance to the exact canonical workload declarations.
	CorpusSHA256 string `json:"corpus_sha256,omitempty"`
	// RunIdentitySHA256 binds resumable records to execution settings that affect comparability.
	RunIdentitySHA256 string `json:"run_identity_sha256,omitempty"`
	// SourceCommit identifies the source commit used to build the benchmark executable.
	SourceCommit string `json:"source_commit"`
	// DirtyDiffSHA256 identifies uncommitted source changes present during the run.
	DirtyDiffSHA256 string `json:"dirty_diff_sha256"`
	// BinarySHA256 identifies the benchmark executable used for the run.
	BinarySHA256 string `json:"binary_sha256"`
	// GOOS supplies the goos input to the RunEnvironment contract.
	GOOS string `json:"goos"`
	// GOARCH supplies the goarch input to the RunEnvironment contract.
	GOARCH string `json:"goarch"`
	// GoVersion identifies the schema version for go version.
	GoVersion string `json:"go_version"`
	// CPUCount records logical CPUs visible to the benchmark process.
	CPUCount int `json:"cpu_count"`
	// CPUModel supplies the cpu model input to the RunEnvironment contract.
	CPUModel string `json:"cpu_model,omitempty"`
	// Kernel supplies the kernel input to the RunEnvironment contract.
	Kernel string `json:"kernel,omitempty"`
	// CgroupCPU supplies the cgroup cpu input to the RunEnvironment contract.
	CgroupCPU string `json:"cgroup_cpu,omitempty"`
	// CgroupMemory supplies the cgroup memory input to the RunEnvironment contract.
	CgroupMemory string `json:"cgroup_memory,omitempty"`
	// CPUGovernor supplies the cpu governor input to the RunEnvironment contract.
	CPUGovernor string `json:"cpu_governor,omitempty"`
	// CPUFrequency supplies the cpu frequency input to the RunEnvironment contract.
	CPUFrequency string `json:"cpu_frequency,omitempty"`
	// HostLoad records host load averages observed during the run.
	HostLoad string `json:"host_load,omitempty"`
	// Invocation supplies the invocation input to the RunEnvironment contract.
	Invocation []string `json:"invocation"`
	// BuildCommand supplies the build command input to the RunEnvironment contract.
	BuildCommand string `json:"build_command"`
	// RunUUID groups records produced by the same resumable benchmark run series.
	RunUUID string `json:"run_uuid"`
	// Arm identifies the measurement arm that produced the sample.
	Arm string `json:"arm"`
	// ArmOrder supplies the arm order input to the RunEnvironment contract.
	ArmOrder int `json:"arm_order,omitempty"`
	// Block identifies the measurement block used to control carryover effects.
	Block int `json:"block"`
	// Round identifies the measurement round.
	Round int `json:"round"`
	// StartedAt records when the benchmark run began.
	StartedAt time.Time `json:"started_at"`
	// EndedAt records when the benchmark run finished.
	EndedAt time.Time `json:"ended_at"`
	// WarmupIterations records the number of warmup iterations.
	WarmupIterations int `json:"warmup_iterations"`
	// Selection captures the exact workload selection applied to the run.
	Selection *SelectionManifest `json:"selection,omitempty"`
	// PoolSize sets the database connection-pool size.
	PoolSize int `json:"pool_size"`
	// Concurrency supplies the concurrency input to the RunEnvironment contract.
	Concurrency []int `json:"concurrency,omitempty"`
	// SessionMemoryCeilingBytes sets the per-session memory ceiling in bytes.
	SessionMemoryCeilingBytes int64 `json:"session_memory_ceiling_bytes,omitempty"`
	// PoolMemoryCeilingBytes sets the aggregate pool memory ceiling in bytes.
	PoolMemoryCeilingBytes int64 `json:"pool_memory_ceiling_bytes,omitempty"`
	// ExistingGraph selects read-only execution against a pre-existing graph.
	ExistingGraph bool `json:"existing_graph,omitempty"`
	// Protocol identifies the measurement protocol.
	Protocol string `json:"protocol,omitempty"`
}

// PostgresEnvironment captures PostgreSQL settings, relation sizes, and schema fingerprints required for comparability.
type PostgresEnvironment struct {
	// Version identifies the serialized schema revision.
	Version string `json:"version"`
	// Database names the PostgreSQL database whose settings and schema were captured.
	Database string `json:"database"`
	// PlanCacheMode records PostgreSQL plan_cache_mode for environment comparability.
	PlanCacheMode string `json:"plan_cache_mode"`
	// TransactionIsolation records the isolation applied to measured read
	// transactions. Tool and provisional guarded orientation evidence uses
	// Repeatable Read even when the server default differs.
	TransactionIsolation string `json:"transaction_isolation"`
	// WorkMem records PostgreSQL work_mem for environment comparability.
	WorkMem string `json:"work_mem"`
	// TempFileLimit supplies the temp file limit input to the PostgresEnvironment contract.
	TempFileLimit string `json:"temp_file_limit"`
	// GraphPartitionCount records physical PostgreSQL graph partitions included in relation-size evidence.
	GraphPartitionCount int64 `json:"graph_partition_count"`
	// PostmasterStartedAt records PostgreSQL server start time for restart detection.
	PostmasterStartedAt time.Time `json:"postmaster_started_at,omitempty"`
	// DatabaseOID identifies the PostgreSQL database across environment and restart comparisons.
	DatabaseOID int64 `json:"database_oid,omitempty"`
	// Autovacuum records PostgreSQL autovacuum settings relevant to comparability.
	Autovacuum string `json:"autovacuum,omitempty"`
	// NodeRelationBytes supplies the node relation bytes input to the PostgresEnvironment contract.
	NodeRelationBytes int64 `json:"node_relation_bytes,omitempty"`
	// EdgeRelationBytes supplies the edge relation bytes input to the PostgresEnvironment contract.
	EdgeRelationBytes int64 `json:"edge_relation_bytes,omitempty"`
	// AnalyzeState records PostgreSQL analyze statistics state for the fixture.
	AnalyzeState string `json:"analyze_state,omitempty"`
	// SchemaFingerprint identifies the normalized PostgreSQL graph schema definition.
	SchemaFingerprint string `json:"schema_fingerprint,omitempty"`
	// IndexFingerprint identifies the normalized database index configuration.
	IndexFingerprint string `json:"index_fingerprint,omitempty"`
}

// resolveRunEnvironment captures reproducibility metadata, invocation, fixture selection, and run timestamps.
func resolveRunEnvironment(cfg config, args []string, selection SelectionManifest, startedAt, endedAt time.Time) RunEnvironment {
	runUUID := cfg.RunUUID
	if runUUID == "" {
		runUUID = newRunUUID()
	}
	return RunEnvironment{
		ArtifactSchemaVersion:     2,
		SourceCommit:              commandOutput("git", "rev-parse", "HEAD"),
		DirtyDiffSHA256:           workingTreeSHA256(),
		BinarySHA256:              executableSHA256(),
		GOOS:                      runtime.GOOS,
		GOARCH:                    runtime.GOARCH,
		GoVersion:                 runtime.Version(),
		CPUCount:                  runtime.NumCPU(),
		CPUModel:                  cpuModel(),
		Kernel:                    commandOutput("uname", "-srvm"),
		CgroupCPU:                 firstReadableFile("/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
		CgroupMemory:              firstReadableFile("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
		CPUGovernor:               firstReadableFile("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
		CPUFrequency:              firstReadableFile("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq"),
		HostLoad:                  firstReadableFile("/proc/loadavg"),
		Invocation:                sanitizedInvocation(args),
		BuildCommand:              cfg.BuildCommand,
		RunUUID:                   runUUID,
		Arm:                       cfg.Arm,
		ArmOrder:                  cfg.ArmOrder,
		Block:                     cfg.Block,
		Round:                     cfg.Round,
		StartedAt:                 startedAt.UTC(),
		EndedAt:                   endedAt.UTC(),
		WarmupIterations:          cfg.WarmupIterations,
		Selection:                 &selection,
		PoolSize:                  cfg.PoolSize,
		Concurrency:               append([]int(nil), cfg.Concurrency...),
		SessionMemoryCeilingBytes: cfg.SessionMemoryCeilingBytes,
		PoolMemoryCeilingBytes:    cfg.PoolMemoryCeilingBytes,
		ExistingGraph:             cfg.ExistingGraph,
		Protocol:                  benchmarkProtocol(cfg),
	}
}

// benchmarkProtocol returns the stable name of the measurement protocol selected by the command.
func benchmarkProtocol(cfg config) string {
	if cfg.Discovery {
		return "adaptive_discovery"
	}
	return "fixed_confirmation"
}

// newRunUUID generates a random RFC 4122 version 4 run identifier.
func newRunUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}

// cpuModel returns the host CPU model reported by the operating system.
func cpuModel() string {
	raw, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if name, value, found := strings.Cut(line, ":"); found && strings.TrimSpace(name) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return "unknown"
}

// firstReadableFile returns trimmed contents of the first readable path.
func firstReadableFile(paths ...string) string {
	for _, path := range paths {
		if raw, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(raw))
		}
	}
	return "unknown"
}

// sanitizedInvocation returns command arguments with connection-string credentials redacted.
func sanitizedInvocation(args []string) []string {
	const redacted = "<redacted>"
	connectionFlags := []string{"-connection", "-pg-connection", "-neo4j-connection"}
	result := append([]string(nil), args...)
	for idx := range result {
		for _, name := range connectionFlags {
			if result[idx] == name && idx+1 < len(result) {
				result[idx+1] = redacted
				break
			}
			if strings.HasPrefix(result[idx], name+"=") {
				result[idx] = name + "=" + redacted
				break
			}
		}
	}
	return result
}

// commandOutput runs a provenance command and returns its trimmed standard output.
func commandOutput(name string, args ...string) string {
	output, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

// workingTreeSHA256 hashes the tracked Git diff together with sorted untracked paths and contents.
func workingTreeSHA256() string {
	fingerprint, err := calculateWorkingTreeSHA256("")
	if err != nil {
		return "unknown"
	}
	return fingerprint
}

// requireCleanSourceCapture refuses a live capture when either tracked edits
// or untracked source files would make the binary's provenance non-reproducible.
// It is intentionally evaluated before GraphBench validates a destructive
// target or opens its run lock.
func requireCleanSourceCapture() error {
	return validateCleanSourceFingerprint(workingTreeSHA256())
}

// validateCleanSourceFingerprint isolates the fail-closed clean-source rule
// so capture wiring can be tested without consulting the caller's repository.
func validateCleanSourceFingerprint(fingerprint string) error {
	if fingerprint != cleanWorkingTreeSHA256() {
		return fmt.Errorf("clean-source capture requires a clean committed source tree")
	}
	return nil
}

// calculateWorkingTreeSHA256 computes working tree sha256.
func calculateWorkingTreeSHA256(excludedRoot string) (string, error) {
	digest := sha256.New()
	output, err := exec.Command("git", "diff", "--binary", "HEAD", "--").Output()
	if err != nil {
		return "", fmt.Errorf("capture tracked source diff: %w", err)
	}
	writeWorkingTreePatchFingerprint(digest, output)
	paths, err := gitUntrackedPaths()
	if err != nil {
		return "", err
	}
	excludedAbsolute := ""
	if excludedRoot != "" {
		excludedAbsolute, err = filepath.Abs(excludedRoot)
		if err != nil {
			return "", fmt.Errorf("resolve excluded source root: %w", err)
		}
	}
	for _, path := range paths {
		if excludedAbsolute != "" {
			absolute, err := filepath.Abs(path)
			if err != nil {
				return "", fmt.Errorf("resolve untracked source %q: %w", path, err)
			}
			if absolute == excludedAbsolute || strings.HasPrefix(absolute, excludedAbsolute+string(filepath.Separator)) {
				continue
			}
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read untracked source %q: %w", path, err)
		}
		writeWorkingTreeUntrackedFingerprint(digest, filepath.ToSlash(path), content)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// gitUntrackedPaths supports benchmark evidence processing for git untracked paths.
func gitUntrackedPaths() ([]string, error) {
	output, err := exec.Command("git", "ls-files", "-z", "--others", "--exclude-standard").Output()
	if err != nil {
		return nil, fmt.Errorf("list untracked source: %w", err)
	}
	paths := parseNULTerminatedPaths(output)
	sort.Strings(paths)
	return paths, nil
}

// parseNULTerminatedPaths parses nul terminated paths.
func parseNULTerminatedPaths(output []byte) []string {
	fields := strings.Split(string(output), "\x00")
	paths := make([]string, 0, len(fields))
	for _, path := range fields {
		if path != "" {
			paths = append(paths, path)
		}
	}
	return paths
}

// writeWorkingTreePatchFingerprint writes working tree patch fingerprint.
func writeWorkingTreePatchFingerprint(digest io.Writer, patch []byte) {
	_, _ = digest.Write(patch)
}

// writeWorkingTreeUntrackedFingerprint writes working tree untracked fingerprint.
func writeWorkingTreeUntrackedFingerprint(digest io.Writer, path string, content []byte) {
	_, _ = fmt.Fprintf(digest, "untracked:%s\x00", path)
	_, _ = digest.Write(content)
}

// executableSHA256 returns the SHA-256 digest of the running benchmark executable.
func executableSHA256() string {
	path, err := os.Executable()
	if err != nil {
		return "unknown"
	}
	checksum, err := fileSHA256(path)
	if err != nil {
		return "unknown"
	}
	return checksum
}

// sqlFingerprint returns the SHA-256 digest of the supplied SQL text exactly as provided.
func sqlFingerprint(sql string) string {
	digest := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(digest[:])
}

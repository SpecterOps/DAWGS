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
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"
)

type RunEnvironment struct {
	ArtifactSchemaVersion     int                `json:"artifact_schema_version"`
	CorpusSHA256              string             `json:"corpus_sha256,omitempty"`
	RunIdentitySHA256         string             `json:"run_identity_sha256,omitempty"`
	SourceCommit              string             `json:"source_commit"`
	DirtyDiffSHA256           string             `json:"dirty_diff_sha256"`
	BinarySHA256              string             `json:"binary_sha256"`
	GOOS                      string             `json:"goos"`
	GOARCH                    string             `json:"goarch"`
	GoVersion                 string             `json:"go_version"`
	CPUCount                  int                `json:"cpu_count"`
	CPUModel                  string             `json:"cpu_model,omitempty"`
	Kernel                    string             `json:"kernel,omitempty"`
	CgroupCPU                 string             `json:"cgroup_cpu,omitempty"`
	CgroupMemory              string             `json:"cgroup_memory,omitempty"`
	CPUGovernor               string             `json:"cpu_governor,omitempty"`
	CPUFrequency              string             `json:"cpu_frequency,omitempty"`
	HostLoad                  string             `json:"host_load,omitempty"`
	Invocation                []string           `json:"invocation"`
	BuildCommand              string             `json:"build_command"`
	RunUUID                   string             `json:"run_uuid"`
	Arm                       string             `json:"arm"`
	ArmOrder                  int                `json:"arm_order,omitempty"`
	Block                     int                `json:"block"`
	Round                     int                `json:"round"`
	StartedAt                 time.Time          `json:"started_at"`
	EndedAt                   time.Time          `json:"ended_at"`
	WarmupIterations          int                `json:"warmup_iterations"`
	Selection                 *SelectionManifest `json:"selection,omitempty"`
	PoolSize                  int                `json:"pool_size"`
	Concurrency               []int              `json:"concurrency,omitempty"`
	SessionMemoryCeilingBytes int64              `json:"session_memory_ceiling_bytes,omitempty"`
	PoolMemoryCeilingBytes    int64              `json:"pool_memory_ceiling_bytes,omitempty"`
	ExistingGraph             bool               `json:"existing_graph,omitempty"`
	Protocol                  string             `json:"protocol,omitempty"`
}

type PostgresEnvironment struct {
	Version             string    `json:"version"`
	Database            string    `json:"database"`
	PlanCacheMode       string    `json:"plan_cache_mode"`
	WorkMem             string    `json:"work_mem"`
	TempFileLimit       string    `json:"temp_file_limit"`
	GraphPartitionCount int64     `json:"graph_partition_count"`
	PostmasterStartedAt time.Time `json:"postmaster_started_at,omitempty"`
	DatabaseOID         int64     `json:"database_oid,omitempty"`
	Autovacuum          string    `json:"autovacuum,omitempty"`
	NodeRelationBytes   int64     `json:"node_relation_bytes,omitempty"`
	EdgeRelationBytes   int64     `json:"edge_relation_bytes,omitempty"`
	AnalyzeState        string    `json:"analyze_state,omitempty"`
	SchemaFingerprint   string    `json:"schema_fingerprint,omitempty"`
	IndexFingerprint    string    `json:"index_fingerprint,omitempty"`
}

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

func benchmarkProtocol(cfg config) string {
	if cfg.Discovery {
		return "adaptive_discovery"
	}
	return "fixed_confirmation"
}

func newRunUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}

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

func firstReadableFile(paths ...string) string {
	for _, path := range paths {
		if raw, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(raw))
		}
	}
	return "unknown"
}

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

func commandOutput(name string, args ...string) string {
	output, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

func workingTreeSHA256() string {
	digest := sha256.New()
	if output, err := exec.Command("git", "diff", "--binary", "HEAD", "--").Output(); err == nil {
		_, _ = digest.Write(output)
	}
	untrackedOutput, err := exec.Command("git", "ls-files", "--others", "--exclude-standard").Output()
	if err == nil {
		paths := strings.Fields(string(untrackedOutput))
		sort.Strings(paths)
		for _, path := range paths {
			_, _ = fmt.Fprintf(digest, "untracked:%s\x00", path)
			if content, err := os.ReadFile(path); err == nil {
				_, _ = digest.Write(content)
			}
		}
	}
	return hex.EncodeToString(digest.Sum(nil))
}

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

func sqlFingerprint(sql string) string {
	digest := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(digest[:])
}

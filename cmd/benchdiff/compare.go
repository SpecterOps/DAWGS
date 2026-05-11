// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func runUnitComparison(ctx context.Context, cfg resolvedConfig, baseWorktree, targetWorktree string) (comparison, error) {
	outDir := filepath.Join(cfg.OutDirAbs, "unit")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return comparison{}, err
	}

	baseFile := filepath.Join(outDir, "base.txt")
	targetFile := filepath.Join(outDir, "target.txt")

	if err := runGoBenchmarks(ctx, cfg, baseWorktree, baseFile); err != nil {
		return comparison{}, err
	}
	if err := runGoBenchmarks(ctx, cfg, targetWorktree, targetFile); err != nil {
		return comparison{}, err
	}

	benchstatOutput, err := runBenchstat(ctx, cfg, baseFile, targetFile)
	if err != nil {
		return comparison{}, err
	}
	benchstatFile := filepath.Join(outDir, "benchstat.txt")
	if err := os.WriteFile(benchstatFile, benchstatOutput, 0644); err != nil {
		return comparison{}, err
	}

	regressions, err := regressionsForFiles(baseFile, targetFile, cfg.Threshold)
	if err != nil {
		return comparison{}, err
	}

	return comparison{
		Name:          "Unit Benchmarks",
		BaseFile:      baseFile,
		TargetFile:    targetFile,
		BenchstatFile: benchstatFile,
		Benchstat:     string(benchstatOutput),
		Regressions:   regressions,
	}, nil
}

func runGoBenchmarks(ctx context.Context, cfg resolvedConfig, worktree, outputPath string) error {
	args := []string{
		"test",
		"-run", "^$",
		"-bench", cfg.Bench,
		"-benchmem",
		"-count", strconv.Itoa(cfg.BenchCount),
		"-benchtime", cfg.Benchtime,
	}
	args = append(args, strings.Fields(cfg.Packages)...)

	output, err := runCommand(ctx, worktree, nil, "go", args...)
	if writeErr := os.WriteFile(outputPath, output, 0644); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return fmt.Errorf("run Go benchmarks in %s: %w", worktree, err)
	}

	return nil
}

func runIntegrationComparison(ctx context.Context, cfg resolvedConfig, baseWorktree, targetWorktree string) (comparison, error) {
	outDir := filepath.Join(cfg.OutDirAbs, "integration")
	binDir := filepath.Join(cfg.OutDirAbs, "bin")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return comparison{}, err
	}
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return comparison{}, err
	}

	baseBinary := filepath.Join(binDir, "benchmark-base")
	targetBinary := filepath.Join(binDir, "benchmark-target")
	if err := buildBenchmarkBinary(ctx, baseWorktree, baseBinary); err != nil {
		return comparison{}, err
	}
	if err := buildBenchmarkBinary(ctx, targetWorktree, targetBinary); err != nil {
		return comparison{}, err
	}

	baseSupportsBenchfmt := benchmarkBinarySupportsFormat(ctx, baseBinary)
	targetSupportsBenchfmt := benchmarkBinarySupportsFormat(ctx, targetBinary)
	useNativeBenchfmt := baseSupportsBenchfmt && targetSupportsBenchfmt

	baseFile := filepath.Join(outDir, "base.bench")
	targetFile := filepath.Join(outDir, "target.bench")
	var notes []string
	if useNativeBenchfmt {
		if err := runBenchmarkBinary(ctx, cfg, baseWorktree, baseBinary, baseFile, true); err != nil {
			return comparison{}, err
		}
		if err := runBenchmarkBinary(ctx, cfg, targetWorktree, targetBinary, targetFile, true); err != nil {
			return comparison{}, err
		}
		notes = append(notes, "Used native benchfmt output from cmd/benchmark.")
	} else {
		baseMarkdown := filepath.Join(outDir, "base.md")
		targetMarkdown := filepath.Join(outDir, "target.md")

		if err := runBenchmarkBinary(ctx, cfg, baseWorktree, baseBinary, baseMarkdown, false); err != nil {
			return comparison{}, err
		}
		if err := runBenchmarkBinary(ctx, cfg, targetWorktree, targetBinary, targetMarkdown, false); err != nil {
			return comparison{}, err
		}

		if err := markdownFileToBenchfmt(baseMarkdown, baseFile, cfg.Driver); err != nil {
			return comparison{}, err
		}
		if err := markdownFileToBenchfmt(targetMarkdown, targetFile, cfg.Driver); err != nil {
			return comparison{}, err
		}

		notes = append(notes, "Used Markdown compatibility mode because at least one ref does not support cmd/benchmark -format benchfmt.")
	}

	benchstatOutput, err := runBenchstat(ctx, cfg, baseFile, targetFile)
	if err != nil {
		return comparison{}, err
	}
	benchstatFile := filepath.Join(outDir, "benchstat.txt")
	if err := os.WriteFile(benchstatFile, benchstatOutput, 0644); err != nil {
		return comparison{}, err
	}

	regressions, err := regressionsForFiles(baseFile, targetFile, cfg.Threshold)
	if err != nil {
		return comparison{}, err
	}

	return comparison{
		Name:          "Integration Benchmarks",
		BaseFile:      baseFile,
		TargetFile:    targetFile,
		BenchstatFile: benchstatFile,
		Benchstat:     string(benchstatOutput),
		Notes:         notes,
		Regressions:   regressions,
	}, nil
}

func buildBenchmarkBinary(ctx context.Context, worktree, outputPath string) error {
	output, err := runCommand(ctx, worktree, nil, "go", "build", "-o", outputPath, "./cmd/benchmark")
	if err != nil {
		return fmt.Errorf("build cmd/benchmark in %s: %w", worktree, err)
	}
	if len(bytes.TrimSpace(output)) > 0 {
		logPath := outputPath + ".log"
		if writeErr := os.WriteFile(logPath, output, 0644); writeErr != nil {
			return writeErr
		}
	}

	return nil
}

func benchmarkBinarySupportsFormat(ctx context.Context, binaryPath string) bool {
	output, err := runCommand(ctx, "", nil, binaryPath, "-h")
	if err != nil {
		return false
	}

	return bytes.Contains(output, []byte("-format"))
}

func runBenchmarkBinary(ctx context.Context, cfg resolvedConfig, worktree, binaryPath, outputPath string, benchfmt bool) error {
	args := []string{
		"-driver", cfg.Driver,
		"-iterations", strconv.Itoa(cfg.IntegrationIterations),
		"-dataset-dir", cfg.DatasetDirAbs,
		"-output", outputPath,
	}
	if benchfmt {
		args = append(args, "-format", "benchfmt")
	}
	if cfg.Dataset != "" {
		args = append(args, "-dataset", cfg.Dataset)
	}
	if cfg.LocalDataset != "" {
		args = append(args, "-local-dataset", cfg.LocalDataset)
	}

	output, err := runCommand(ctx, worktree, []string{"CONNECTION_STRING=" + cfg.Connection}, binaryPath, args...)
	logPath := outputPath + ".log"
	if writeErr := os.WriteFile(logPath, output, 0644); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return fmt.Errorf("run integration benchmark in %s: %w", worktree, err)
	}

	return nil
}

func markdownFileToBenchfmt(markdownPath, benchfmtPath, driver string) error {
	data, err := os.ReadFile(markdownPath)
	if err != nil {
		return err
	}

	var output bytes.Buffer
	if err := writeIntegrationBenchfmt(&output, driver, parseBenchmarkMarkdown(data)); err != nil {
		return err
	}

	return os.WriteFile(benchfmtPath, output.Bytes(), 0644)
}

func runBenchstat(ctx context.Context, cfg resolvedConfig, baseFile, targetFile string) ([]byte, error) {
	if cfg.Benchstat == "none" {
		return []byte("benchstat skipped\n"), nil
	}

	if cfg.Benchstat == "" || cfg.Benchstat == "auto" {
		if benchstatPath, err := exec.LookPath("benchstat"); err == nil {
			return runCommand(ctx, cfg.Root, nil, benchstatPath, baseFile, targetFile)
		}

		return runCommand(ctx, cfg.Root, nil, "go", "run", "golang.org/x/perf/cmd/benchstat@latest", baseFile, targetFile)
	}

	fields := strings.Fields(cfg.Benchstat)
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty benchstat command")
	}

	args := append(fields[1:], baseFile, targetFile)
	return runCommand(ctx, cfg.Root, nil, fields[0], args...)
}

func regressionsForFiles(baseFile, targetFile string, threshold float64) ([]regression, error) {
	if threshold <= 0 {
		return nil, nil
	}

	base, err := parseBenchfmtNSFile(baseFile)
	if err != nil {
		return nil, err
	}
	target, err := parseBenchfmtNSFile(targetFile)
	if err != nil {
		return nil, err
	}

	return findRegressions(base, target, threshold), nil
}

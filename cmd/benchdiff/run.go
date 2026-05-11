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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type comparison struct {
	Name          string
	BaseFile      string
	TargetFile    string
	BenchstatFile string
	Benchstat     string
	Notes         []string
	Regressions   []regression
}

type runSummary struct {
	Config      resolvedConfig
	GoVersion   string
	StartedAt   time.Time
	FinishedAt  time.Time
	Comparisons []comparison
	ReportPath  string
}

func run(ctx context.Context, cfg config) error {
	resolved, err := resolveConfig(ctx, cfg)
	if err != nil {
		return err
	}

	summary := runSummary{
		Config:    resolved,
		GoVersion: runtime.Version(),
		StartedAt: time.Now(),
	}

	if err := os.MkdirAll(resolved.OutDirAbs, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	worktreeRoot := filepath.Join(resolved.OutDirAbs, "worktrees")
	baseWorktree := filepath.Join(worktreeRoot, "base")
	targetWorktree := filepath.Join(worktreeRoot, "target")

	fmt.Fprintf(os.Stderr, "preparing benchmark worktrees for %s and %s...\n", resolved.BaseShortSHA, resolved.TargetShortSHA)

	if err := addWorktree(ctx, resolved.Root, baseWorktree, resolved.BaseSHA); err != nil {
		return err
	}
	removeBase := true
	defer func() {
		if removeBase && !resolved.KeepWorktrees {
			_ = removeWorktree(context.Background(), resolved.Root, baseWorktree)
		}
	}()

	if err := addWorktree(ctx, resolved.Root, targetWorktree, resolved.TargetSHA); err != nil {
		return err
	}
	removeTarget := true
	defer func() {
		if removeTarget && !resolved.KeepWorktrees {
			_ = removeWorktree(context.Background(), resolved.Root, targetWorktree)
		}
	}()

	if resolved.runsUnitBenchmarks() {
		fmt.Fprintln(os.Stderr, "running unit benchmarks...")
		unitComparison, err := runUnitComparison(ctx, resolved, baseWorktree, targetWorktree)
		if err != nil {
			return err
		}
		summary.Comparisons = append(summary.Comparisons, unitComparison)
	}

	if resolved.runsIntegrationBenchmarks() {
		fmt.Fprintln(os.Stderr, "running integration benchmarks...")
		integrationComparison, err := runIntegrationComparison(ctx, resolved, baseWorktree, targetWorktree)
		if err != nil {
			return err
		}
		summary.Comparisons = append(summary.Comparisons, integrationComparison)
	}

	removeBase = false
	removeTarget = false
	if !resolved.KeepWorktrees {
		if err := removeWorktree(ctx, resolved.Root, baseWorktree); err != nil {
			return err
		}
		if err := removeWorktree(ctx, resolved.Root, targetWorktree); err != nil {
			return err
		}
	}

	summary.FinishedAt = time.Now()
	summary.ReportPath = filepath.Join(resolved.OutDirAbs, "report.md")
	if err := writeRunReport(summary.ReportPath, summary); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "wrote benchmark diff report: %s\n", summary.ReportPath)

	if regressions := summary.regressions(); len(regressions) > 0 && resolved.Threshold > 0 {
		return fmt.Errorf("%d benchmark regressions exceeded %.2f%%; see %s", len(regressions), resolved.Threshold, summary.ReportPath)
	}

	return nil
}

func resolveConfig(ctx context.Context, cfg config) (resolvedConfig, error) {
	root, err := gitOutput(ctx, "", "rev-parse", "--show-toplevel")
	if err != nil {
		return resolvedConfig{}, err
	}

	baseSHA, err := resolveCommit(ctx, root, cfg.BaseRef)
	if err != nil {
		return resolvedConfig{}, err
	}
	targetSHA, err := resolveCommit(ctx, root, cfg.TargetRef)
	if err != nil {
		return resolvedConfig{}, err
	}

	baseShortSHA, err := shortCommit(ctx, root, baseSHA)
	if err != nil {
		return resolvedConfig{}, err
	}
	targetShortSHA, err := shortCommit(ctx, root, targetSHA)
	if err != nil {
		return resolvedConfig{}, err
	}

	threshold, err := parseRegressionThreshold(cfg.FailRegression)
	if err != nil {
		return resolvedConfig{}, err
	}

	if cfg.runsIntegrationBenchmarks() && cfg.Connection == "" {
		cfg.Connection = os.Getenv("CONNECTION_STRING")
	}
	if cfg.runsIntegrationBenchmarks() && cfg.Connection == "" {
		return resolvedConfig{}, fmt.Errorf("integration benchmarks require -connection or CONNECTION_STRING")
	}

	datasetDirAbs, err := resolvePath(root, cfg.DatasetDir)
	if err != nil {
		return resolvedConfig{}, err
	}

	outDir := cfg.OutDir
	if outDir == "" {
		outDir = filepath.Join(".bench", "runs", fmt.Sprintf("%s..%s-%s", baseShortSHA, targetShortSHA, time.Now().UTC().Format("20060102T150405Z")))
	}
	outDirAbs, err := resolvePath(root, outDir)
	if err != nil {
		return resolvedConfig{}, err
	}

	return resolvedConfig{
		config:         cfg,
		Root:           root,
		BaseSHA:        baseSHA,
		TargetSHA:      targetSHA,
		BaseShortSHA:   baseShortSHA,
		TargetShortSHA: targetShortSHA,
		DatasetDirAbs:  datasetDirAbs,
		OutDirAbs:      outDirAbs,
		Threshold:      threshold,
	}, nil
}

func resolvePath(root, value string) (string, error) {
	if filepath.IsAbs(value) {
		return filepath.Clean(value), nil
	}

	return filepath.Abs(filepath.Join(root, value))
}

func resolveCommit(ctx context.Context, root, ref string) (string, error) {
	sha, err := gitOutput(ctx, root, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve git ref %q: %w", ref, err)
	}

	return sha, nil
}

func shortCommit(ctx context.Context, root, sha string) (string, error) {
	shortSHA, err := gitOutput(ctx, root, "rev-parse", "--short", sha)
	if err != nil {
		return "", err
	}

	return shortSHA, nil
}

func addWorktree(ctx context.Context, root, path, sha string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("worktree path already exists: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	_, err := runCommand(ctx, root, nil, "git", "worktree", "add", "--detach", path, sha)
	if err != nil {
		return fmt.Errorf("add worktree %s: %w", path, err)
	}

	return nil
}

func removeWorktree(ctx context.Context, root, path string) error {
	_, err := runCommand(ctx, root, nil, "git", "worktree", "remove", "--force", path)
	if err != nil && !strings.Contains(err.Error(), "is not a working tree") {
		return fmt.Errorf("remove worktree %s: %w", path, err)
	}

	return nil
}

func (summary runSummary) regressions() []regression {
	var regressions []regression

	for _, comparison := range summary.Comparisons {
		regressions = append(regressions, comparison.Regressions...)
	}

	return regressions
}

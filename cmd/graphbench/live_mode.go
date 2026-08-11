// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

const existingGraphCheckpointVersion = 2

var mutationKeyword = regexp.MustCompile(`(?i)\b(create|merge|delete|detach|set|remove|drop|alter|truncate|grant|revoke|call|foreach|load\s+csv)\b`)

type ExistingGraphAnchorManifest struct {
	Version         int                            `json:"version"`
	Graph           string                         `json:"graph"`
	ContentIdentity string                         `json:"content_identity"`
	Anchors         map[string]ExistingGraphAnchor `json:"anchors"`
	Checksum        string                         `json:"-"`
}

type ExistingGraphAnchor struct {
	LogicalKey    string `json:"logical_key,omitempty"`
	PhysicalID    *int64 `json:"physical_id,omitempty"`
	ContentSHA256 string `json:"content_sha256,omitempty"`
	Kind          string `json:"kind,omitempty"`
}

type ExistingGraphAttempt struct {
	Timeout         time.Duration `json:"timeout"`
	WarmupSamples   int           `json:"warmup_samples"`
	MeasuredSamples int           `json:"measured_samples"`
	Status          string        `json:"status"`
	Error           string        `json:"error,omitempty"`
}

type ExistingGraphRun struct {
	ManifestSHA256  string                 `json:"manifest_sha256"`
	ContentIdentity string                 `json:"content_identity"`
	Protocol        string                 `json:"protocol"`
	Adaptive        bool                   `json:"adaptive"`
	Attempts        []ExistingGraphAttempt `json:"attempts,omitempty"`
	PreNodeCount    int64                  `json:"pre_node_count"`
	PreEdgeCount    int64                  `json:"pre_edge_count"`
	PostNodeCount   int64                  `json:"post_node_count"`
	PostEdgeCount   int64                  `json:"post_edge_count"`
}

type ExistingGraphProgress struct {
	At      time.Time `json:"at"`
	Stage   string    `json:"stage"`
	CaseKey string    `json:"case_key,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

type existingGraphCheckpoint struct {
	Version        int          `json:"version"`
	ManifestSHA256 string       `json:"manifest_sha256"`
	CorpusSHA256   string       `json:"corpus_sha256"`
	RunSHA256      string       `json:"run_sha256"`
	Records        []CaseResult `json:"records"`
}

func loadExistingGraphAnchorManifest(path string) (ExistingGraphAnchorManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ExistingGraphAnchorManifest{}, fmt.Errorf("read anchor manifest: %w", err)
	}
	var manifest ExistingGraphAnchorManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return ExistingGraphAnchorManifest{}, fmt.Errorf("decode anchor manifest: %w", err)
	}
	if manifest.Version != 1 {
		return ExistingGraphAnchorManifest{}, fmt.Errorf("unsupported anchor manifest version %d", manifest.Version)
	}
	if len(manifest.Anchors) == 0 {
		return ExistingGraphAnchorManifest{}, fmt.Errorf("anchor manifest must contain anchors")
	}
	if strings.TrimSpace(manifest.Graph) == "" {
		return ExistingGraphAnchorManifest{}, fmt.Errorf("anchor manifest graph must not be empty")
	}
	if matched, _ := regexp.MatchString(`^sha256:[0-9a-f]{64}$`, manifest.ContentIdentity); !matched {
		return ExistingGraphAnchorManifest{}, fmt.Errorf("anchor manifest content_identity must be a lowercase sha256 digest")
	}
	for name, anchor := range manifest.Anchors {
		if strings.TrimSpace(name) == "" {
			return ExistingGraphAnchorManifest{}, fmt.Errorf("anchor names must not be empty")
		}
		hasLogicalKey := strings.TrimSpace(anchor.LogicalKey) != ""
		hasPhysicalID := anchor.PhysicalID != nil
		if hasLogicalKey == hasPhysicalID {
			return ExistingGraphAnchorManifest{}, fmt.Errorf("anchor %s must declare exactly one of logical_key or physical_id", name)
		}
		if hasPhysicalID {
			if matched, _ := regexp.MatchString(`^sha256:[0-9a-f]{64}$`, anchor.ContentSHA256); !matched {
				return ExistingGraphAnchorManifest{}, fmt.Errorf("physical anchor %s content_sha256 must be a lowercase sha256 digest", name)
			}
		} else if anchor.ContentSHA256 != "" {
			return ExistingGraphAnchorManifest{}, fmt.Errorf("logical-key anchor %s must not declare content_sha256", name)
		}
	}
	digest := sha256.Sum256(raw)
	manifest.Checksum = hex.EncodeToString(digest[:])
	return manifest, nil
}

func validateExistingGraphCorpus(corpus ScaleCorpus, manifest ExistingGraphAnchorManifest) error {
	for _, testCase := range corpus.Cases {
		if testCase.WriteScenario != nil {
			return fmt.Errorf("existing-graph mode rejects write_scenario in case %s", testCase.Name)
		}
		if mutationKeyword.MatchString(stripCypherStringLiterals(testCase.Cypher)) {
			return fmt.Errorf("existing-graph mode rejects mutation keyword in case %s", testCase.Name)
		}
		for _, anchor := range testCase.NodeParams {
			if _, found := manifest.Anchors[anchor]; !found {
				return fmt.Errorf("case %s references anchor %q absent from the manifest", testCase.Name, anchor)
			}
		}
		for _, anchors := range testCase.NodeListParams {
			for _, anchor := range anchors {
				if _, found := manifest.Anchors[anchor]; !found {
					return fmt.Errorf("case %s references anchor %q absent from the manifest", testCase.Name, anchor)
				}
			}
		}
	}
	return nil
}

func stripCypherStringLiterals(query string) string {
	var (
		result  strings.Builder
		quote   rune
		escaped bool
	)

	for _, value := range query {
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if value == '\\' {
				escaped = true
				continue
			}
			if value == quote {
				quote = 0
			}
			result.WriteRune(' ')
			continue
		}
		if value == '\'' || value == '"' {
			quote = value
			result.WriteRune(' ')
			continue
		}
		result.WriteRune(value)
	}
	return result.String()
}

func existingGraphCaseKey(mode ExecutionMode, testCase ScaleCase) string {
	return strings.Join([]string{string(mode), testCase.Dataset, testCase.Name}, "/")
}

func corpusIdentity(corpus ScaleCorpus) string {
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
	raw, _ := json.Marshal(struct {
		Version int         `json:"version"`
		Cases   []ScaleCase `json:"cases"`
	}{Version: 2, Cases: cases})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func runConfigurationIdentity(cfg config, environment RunEnvironment) string {
	payload := struct {
		Version                   int             `json:"version"`
		SourceCommit              string          `json:"source_commit"`
		DirtyDiffSHA256           string          `json:"dirty_diff_sha256"`
		BinarySHA256              string          `json:"binary_sha256"`
		GOOS                      string          `json:"goos"`
		GOARCH                    string          `json:"goarch"`
		GoVersion                 string          `json:"go_version"`
		Modes                     []ExecutionMode `json:"modes"`
		Iterations                int             `json:"iterations"`
		WarmupIterations          int             `json:"warmup_iterations"`
		Round                     int             `json:"round"`
		Block                     int             `json:"block"`
		Arm                       string          `json:"arm"`
		ArmOrder                  int             `json:"arm_order"`
		PoolSize                  int             `json:"pool_size"`
		Concurrency               []int           `json:"concurrency"`
		SessionMemoryCeilingBytes int64           `json:"session_memory_ceiling_bytes"`
		PoolMemoryCeilingBytes    int64           `json:"pool_memory_ceiling_bytes"`
		PostgresReferences        bool            `json:"postgres_references"`
		PostgresReferenceArms     []string        `json:"postgres_reference_arms"`
		PostgresForceShortest     string          `json:"postgres_force_shortest"`
		PostgresForceExpansion    string          `json:"postgres_force_expansion"`
		Discovery                 bool            `json:"discovery"`
		TimeoutClasses            []time.Duration `json:"timeout_classes"`
		DiscoverySampleFloor      int             `json:"discovery_sample_floor"`
	}{
		Version:                   1,
		SourceCommit:              environment.SourceCommit,
		DirtyDiffSHA256:           environment.DirtyDiffSHA256,
		BinarySHA256:              environment.BinarySHA256,
		GOOS:                      environment.GOOS,
		GOARCH:                    environment.GOARCH,
		GoVersion:                 environment.GoVersion,
		Modes:                     append([]ExecutionMode(nil), cfg.Modes...),
		Iterations:                cfg.Iterations,
		WarmupIterations:          cfg.WarmupIterations,
		Round:                     cfg.Round,
		Block:                     cfg.Block,
		Arm:                       cfg.Arm,
		ArmOrder:                  cfg.ArmOrder,
		PoolSize:                  cfg.PoolSize,
		Concurrency:               append([]int(nil), cfg.Concurrency...),
		SessionMemoryCeilingBytes: cfg.SessionMemoryCeilingBytes,
		PoolMemoryCeilingBytes:    cfg.PoolMemoryCeilingBytes,
		PostgresReferences:        cfg.PostgresReferences,
		PostgresReferenceArms:     append([]string(nil), cfg.PostgresReferenceArms...),
		PostgresForceShortest:     cfg.PostgresForceShortest,
		PostgresForceExpansion:    cfg.PostgresForceExpansion,
		Discovery:                 cfg.Discovery,
		TimeoutClasses:            append([]time.Duration(nil), cfg.TimeoutClasses...),
		DiscoverySampleFloor:      cfg.DiscoverySampleFloor,
	}
	raw, _ := json.Marshal(payload)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func readExistingGraphCheckpoint(path, manifestHash, corpusHash, runHash string) ([]CaseResult, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var checkpoint existingGraphCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode existing-graph checkpoint: %w", err)
	}
	if checkpoint.Version != existingGraphCheckpointVersion || checkpoint.ManifestSHA256 != manifestHash || checkpoint.CorpusSHA256 != corpusHash || checkpoint.RunSHA256 != runHash {
		return nil, fmt.Errorf("existing-graph checkpoint identity does not match this run")
	}
	seen := map[string]struct{}{}
	runUUID := ""
	for _, record := range checkpoint.Records {
		if record.WorkloadSHA256 == "" || record.Environment == nil || record.Environment.ArtifactSchemaVersion != 2 || record.Environment.CorpusSHA256 != corpusHash || record.Environment.RunIdentitySHA256 != runHash || record.Environment.RunUUID == "" {
			return nil, fmt.Errorf("existing-graph checkpoint record identity does not match this run")
		}
		if runUUID == "" {
			runUUID = record.Environment.RunUUID
		} else if record.Environment.RunUUID != runUUID {
			return nil, fmt.Errorf("existing-graph checkpoint contains multiple run UUIDs")
		}
		key := strings.Join([]string{string(record.ExecutionMode), record.Dataset, record.Name}, "/")
		if _, found := seen[key]; found {
			return nil, fmt.Errorf("existing-graph checkpoint contains duplicate record %s", key)
		}
		seen[key] = struct{}{}
	}
	return checkpoint.Records, nil
}

func writeExistingGraphCheckpoint(path, manifestHash, corpusHash, runHash string, records []CaseResult) error {
	if path == "" {
		return nil
	}
	checkpoint := existingGraphCheckpoint{
		Version:        existingGraphCheckpointVersion,
		ManifestSHA256: manifestHash,
		CorpusSHA256:   corpusHash,
		RunSHA256:      runHash,
		Records:        records,
	}
	raw, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".graphbench-checkpoint-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(append(raw, '\n')); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

func appendExistingGraphProgress(path string, event ExistingGraphProgress) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	event.At = time.Now().UTC()
	return json.NewEncoder(file).Encode(event)
}

func redactExistingGraphRecord(record *CaseResult, manifest ExistingGraphAnchorManifest, resolved map[string]graph.ID) {
	if record == nil {
		return
	}
	record.Params = nil
	redacted := map[string]string{}
	for parameter, name := range record.NodeParams {
		anchor, found := manifest.Anchors[name]
		if !found {
			continue
		}
		seed := anchor.LogicalKey
		if seed == "" {
			seed = anchor.ContentSHA256
		}
		digest := sha256.Sum256([]byte(seed))
		redacted[parameter] = "sha256:" + hex.EncodeToString(digest[:])
	}
	record.NodeParams = redacted
	record.NodeListParams = nil
	record.Cypher = ""
	record.ObservedRows = redactObservedRows(record.ObservedRows)
	record.SQL = redactResolvedIDs(record.SQL, resolved)
	for idx := range record.PostgresPlan {
		record.PostgresPlan[idx] = redactResolvedIDs(record.PostgresPlan[idx], resolved)
	}
	if len(record.PostgresPlanJSON) > 0 {
		record.PostgresPlanJSON = redactPlanJSON(record.PostgresPlanJSON, resolved)
	}
	record.Error = redactDiagnostic(record.Error)
	for idx := range record.PostgresReferences {
		reference := &record.PostgresReferences[idx]
		reference.ObservedRows = redactObservedRows(reference.ObservedRows)
		reference.SQL = redactResolvedIDs(reference.SQL, resolved)
		for planIdx := range reference.PostgresPlan {
			reference.PostgresPlan[planIdx] = redactResolvedIDs(reference.PostgresPlan[planIdx], resolved)
		}
		if len(reference.PostgresPlanJSON) > 0 {
			reference.PostgresPlanJSON = redactPlanJSON(reference.PostgresPlanJSON, resolved)
		}
	}
	if record.ExistingGraph != nil {
		for idx := range record.ExistingGraph.Attempts {
			record.ExistingGraph.Attempts[idx].Error = redactDiagnostic(record.ExistingGraph.Attempts[idx].Error)
		}
	}
}

func redactObservedRows(rows []string) []string {
	for idx := range rows {
		digest := sha256.Sum256([]byte(rows[idx]))
		rows[idx] = "sha256:" + hex.EncodeToString(digest[:])
	}
	return rows
}

func redactDiagnostic(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func redactResolvedIDs(value string, resolved map[string]graph.ID) string {
	for _, id := range resolved {
		value = regexp.MustCompile(`\b`+regexp.QuoteMeta(fmt.Sprint(id))+`\b`).ReplaceAllString(value, "<anchor-id>")
	}
	value = regexp.MustCompile(`unmapped-(node|edge|relationship):[0-9]+`).ReplaceAllString(value, "unmapped-$1:<redacted-id>")
	return value
}

func redactPlanJSON(raw json.RawMessage, resolved map[string]graph.ID) json.RawMessage {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	var redact func(any) any
	redact = func(current any) any {
		switch typed := current.(type) {
		case string:
			return redactResolvedIDs(typed, resolved)
		case []any:
			for idx := range typed {
				typed[idx] = redact(typed[idx])
			}
		case map[string]any:
			for key := range typed {
				typed[key] = redact(typed[key])
			}
		}
		return current
	}
	encoded, err := json.Marshal(redact(value))
	if err != nil {
		return nil
	}
	return encoded
}

func validateCompletedWorkloads(completed map[string]string, corpus ScaleCorpus, fixture FixtureMetadata) error {
	expectedKeys := map[string]struct{}{}
	for _, testCase := range corpus.Cases {
		if !testCase.Supports(ModePostgresSQL) {
			continue
		}
		key := existingGraphCaseKey(ModePostgresSQL, testCase)
		expectedKeys[key] = struct{}{}
		checkpointWorkload, found := completed[key]
		if !found {
			continue
		}
		expected := newCaseResult(testCase, ModePostgresSQL, nil)
		attachFixtureMetadata(&expected, fixture)
		if checkpointWorkload == "" || checkpointWorkload != expected.WorkloadSHA256 {
			return fmt.Errorf("existing-graph checkpoint workload identity does not match %s", key)
		}
	}
	for key := range completed {
		if _, found := expectedKeys[key]; !found {
			return fmt.Errorf("existing-graph checkpoint contains unknown workload %s", key)
		}
	}
	return nil
}

func idMapForManifest(anchors map[string]graph.ID) opengraph.IDMap {
	result := make(opengraph.IDMap, len(anchors))
	for name, id := range anchors {
		result[name] = id
	}
	return result
}

// scanCheckpointJSONL is deliberately strict: a truncated last line is not a
// completed record and therefore cannot be treated as resumable evidence.
func scanCheckpointJSONL(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var value map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return err
		}
	}
	return scanner.Err()
}

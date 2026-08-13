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

// existingGraphCheckpointVersion identifies the serialized schema revision for existing graph checkpoint.
const existingGraphCheckpointVersion = 2

// mutationKeyword matches Cypher keywords that can mutate an existing graph.
var mutationKeyword = regexp.MustCompile(`(?i)\b(create|merge|delete|detach|set|remove|drop|alter|truncate|grant|revoke|call|foreach|load\s+csv)\b`)

// ExistingGraphAnchorManifest authorizes read-only live-graph workloads against validated logical or redacted physical anchors.
type ExistingGraphAnchorManifest struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Graph identifies the graph addressed by the artifact.
	Graph string `json:"graph"`
	// ContentIdentity binds resumable work to the logical contents of the live graph.
	ContentIdentity string `json:"content_identity"`
	// Anchors maps manifest anchor names to their logical or redacted physical identities.
	Anchors map[string]ExistingGraphAnchor `json:"anchors"`
	// Checksum records the digest of the validated manifest file.
	Checksum string `json:"-"`
}

// ExistingGraphAnchor maps a logical fixture key to either a logical or redacted physical identity.
type ExistingGraphAnchor struct {
	// LogicalKey identifies an anchor using a corpus-visible fixture key.
	LogicalKey string `json:"logical_key,omitempty"`
	// PhysicalID selects a backend node directly when no corpus-visible logical key is available.
	PhysicalID *int64 `json:"physical_id,omitempty"`
	// ContentSHA256 identifies scrubbed physical anchor content without exposing it.
	ContentSHA256 string `json:"content_sha256,omitempty"`
	// Kind optionally requires the resolved anchor node to carry this graph kind.
	Kind string `json:"kind,omitempty"`
}

// ExistingGraphAttempt captures the applied deadline, collected samples, and outcome of one live-graph execution.
type ExistingGraphAttempt struct {
	// Timeout records the deadline applied to this live-graph attempt; zero means no deadline.
	Timeout time.Duration `json:"timeout"`
	// WarmupSamples records untimed samples collected before live-graph measurement.
	WarmupSamples int `json:"warmup_samples"`
	// MeasuredSamples records timed samples collected for the live-graph attempt.
	MeasuredSamples int `json:"measured_samples"`
	// Status records the execution outcome.
	Status string `json:"status"`
	// Error records the failure message when the operation did not succeed.
	Error string `json:"error,omitempty"`
}

// ExistingGraphRun describes a resumable live-graph run and all attempts made in it.
type ExistingGraphRun struct {
	// ManifestSHA256 identifies the anchor manifest that authorized the run.
	ManifestSHA256 string `json:"manifest_sha256"`
	// ContentIdentity binds resumable work to the logical contents of the live graph.
	ContentIdentity string `json:"content_identity"`
	// Protocol identifies the measurement protocol.
	Protocol string `json:"protocol"`
	// Adaptive indicates that adaptive discovery, rather than a fixed protocol, produced the record.
	Adaptive bool `json:"adaptive"`
	// Attempts lists live-graph attempts in execution order.
	Attempts []ExistingGraphAttempt `json:"attempts,omitempty"`
	// PreNodeCount records graph nodes present before the live-graph run.
	PreNodeCount int64 `json:"pre_node_count"`
	// PreEdgeCount records graph relationships present before the live-graph run.
	PreEdgeCount int64 `json:"pre_edge_count"`
	// PostNodeCount records graph nodes present after the live-graph run.
	PostNodeCount int64 `json:"post_node_count"`
	// PostEdgeCount records graph relationships present after the live-graph run.
	PostEdgeCount int64 `json:"post_edge_count"`
}

// ExistingGraphProgress is one append-only progress event emitted during a live-graph run.
type ExistingGraphProgress struct {
	// At records when the progress event was emitted.
	At time.Time `json:"at"`
	// Stage identifies the stage reached by a live-graph progress event.
	Stage string `json:"stage"`
	// CaseKey identifies the dataset/case pair addressed by a progress event.
	CaseKey string `json:"case_key,omitempty"`
	// Detail contains the progress or failure detail safe to persist.
	Detail string `json:"detail,omitempty"`
}

// existingGraphCheckpoint binds completed live-graph cases to a corpus, run configuration, and fixture identity.
type existingGraphCheckpoint struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// ManifestSHA256 identifies the anchor manifest that authorized the run.
	ManifestSHA256 string `json:"manifest_sha256"`
	// CorpusSHA256 binds checkpoint records to the exact canonical workload declarations.
	CorpusSHA256 string `json:"corpus_sha256"`
	// RunSHA256 binds completed records to the exact resumable run configuration.
	RunSHA256 string `json:"run_sha256"`
	// Records contains completed CaseResults retained for resumable execution.
	Records []CaseResult `json:"records"`
}

// loadExistingGraphAnchorManifest reads and validates a live-graph anchor manifest and records its checksum.
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

// validateExistingGraphCorpus rejects mutations and anchors absent from the live-graph manifest.
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

// stripCypherStringLiterals replaces quoted Cypher contents with spaces before mutation-keyword scanning.
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

// existingGraphCaseKey joins execution mode, dataset, and case name into the checkpoint lookup key.
func existingGraphCaseKey(mode ExecutionMode, testCase ScaleCase) string {
	return strings.Join([]string{string(mode), testCase.Dataset, testCase.Name}, "/")
}

// corpusIdentity hashes the canonical corpus declaration used to bind checkpoints to workloads.
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
		// Version identifies the serialized schema revision.
		Version int `json:"version"`
		// Cases contains the canonically ordered workload declarations bound into the corpus digest.
		Cases []ScaleCase `json:"cases"`
	}{Version: 2, Cases: cases})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// runConfigurationIdentity hashes execution-affecting configuration and environment fields for checkpoint compatibility.
func runConfigurationIdentity(cfg config, environment RunEnvironment) string {
	payload := struct {
		// Version identifies the serialized schema revision.
		Version int `json:"version"`
		// SourceCommit identifies the source commit used to build the benchmark executable.
		SourceCommit string `json:"source_commit"`
		// DirtyDiffSHA256 identifies uncommitted source changes present during the run.
		DirtyDiffSHA256 string `json:"dirty_diff_sha256"`
		// BinarySHA256 identifies the benchmark executable used for the run.
		BinarySHA256 string `json:"binary_sha256"`
		// GOOS records the target operating system of the benchmark executable.
		GOOS string `json:"goos"`
		// GOARCH records the target architecture of the benchmark executable.
		GOARCH string `json:"goarch"`
		// GoVersion records the Go toolchain version used to build the executable.
		GoVersion string `json:"go_version"`
		// Modes records execution-mode order as part of resumable run identity.
		Modes []ExecutionMode `json:"modes"`
		// Iterations records the number of measured iterations.
		Iterations int `json:"iterations"`
		// WarmupIterations records the untimed iterations run before measurement.
		WarmupIterations int `json:"warmup_iterations"`
		// Round identifies the measurement round.
		Round int `json:"round"`
		// Block identifies the measurement block used to control carryover effects.
		Block int `json:"block"`
		// Arm identifies the measurement arm that produced the sample.
		Arm string `json:"arm"`
		// ArmOrder records the arm's position within its balanced measurement block.
		ArmOrder int `json:"arm_order"`
		// PoolSize sets the database connection-pool size.
		PoolSize int `json:"pool_size"`
		// Concurrency records the requested worker counts as part of resumable run identity.
		Concurrency []int `json:"concurrency"`
		// SessionMemoryCeilingBytes sets the per-session memory ceiling in bytes.
		SessionMemoryCeilingBytes int64 `json:"session_memory_ceiling_bytes"`
		// PoolMemoryCeilingBytes sets the aggregate pool memory ceiling in bytes.
		PoolMemoryCeilingBytes int64 `json:"pool_memory_ceiling_bytes"`
		// PostgresReferences records whether independent PostgreSQL references are enabled for the run identity.
		PostgresReferences bool `json:"postgres_references"`
		// PostgresReferenceArms lists independent PostgreSQL reference arms selected for measurement.
		PostgresReferenceArms []string `json:"postgres_reference_arms"`
		// PostgresForceShortest selects a forced shortest-path executor for diagnostic runs.
		PostgresForceShortest string `json:"postgres_force_shortest"`
		// PostgresForceExpansion selects a forced expansion search strategy for diagnostic runs.
		PostgresForceExpansion string `json:"postgres_force_expansion"`
		// PostgresRepeatableRead records the stable-snapshot timing contract.
		PostgresRepeatableRead bool `json:"postgres_repeatable_read"`
		// PostgresTraversalTelemetry selects the opt-in traversal evidence boundary.
		PostgresTraversalTelemetry string `json:"postgres_traversal_telemetry"`
		// PostgresExpansionOrientationShadow records the tool-only selector shadow mode.
		PostgresExpansionOrientationShadow bool `json:"postgres_expansion_orientation_shadow"`
		// PostgresExpansionOrientationTournament records the guarded selector mode.
		PostgresExpansionOrientationTournament bool `json:"postgres_expansion_orientation_tournament"`
		// PostgresExpansionOrientationPolicy records the immutable selector formula.
		PostgresExpansionOrientationPolicy string `json:"postgres_expansion_orientation_policy"`
		// Discovery enables adaptive live-graph discovery instead of the fixed confirmation protocol.
		Discovery bool `json:"discovery"`
		// TimeoutClasses lists the increasing per-attempt deadlines included in resumable run identity.
		TimeoutClasses []time.Duration `json:"timeout_classes"`
		// DiscoverySampleFloor sets the minimum live-graph samples required before adaptive discovery may stop.
		DiscoverySampleFloor int `json:"discovery_sample_floor"`
	}{
		Version:                                1,
		SourceCommit:                           environment.SourceCommit,
		DirtyDiffSHA256:                        environment.DirtyDiffSHA256,
		BinarySHA256:                           environment.BinarySHA256,
		GOOS:                                   environment.GOOS,
		GOARCH:                                 environment.GOARCH,
		GoVersion:                              environment.GoVersion,
		Modes:                                  append([]ExecutionMode(nil), cfg.Modes...),
		Iterations:                             cfg.Iterations,
		WarmupIterations:                       cfg.WarmupIterations,
		Round:                                  cfg.Round,
		Block:                                  cfg.Block,
		Arm:                                    cfg.Arm,
		ArmOrder:                               cfg.ArmOrder,
		PoolSize:                               cfg.PoolSize,
		Concurrency:                            append([]int(nil), cfg.Concurrency...),
		SessionMemoryCeilingBytes:              cfg.SessionMemoryCeilingBytes,
		PoolMemoryCeilingBytes:                 cfg.PoolMemoryCeilingBytes,
		PostgresReferences:                     cfg.PostgresReferences,
		PostgresReferenceArms:                  append([]string(nil), cfg.PostgresReferenceArms...),
		PostgresForceShortest:                  cfg.PostgresForceShortest,
		PostgresForceExpansion:                 cfg.PostgresForceExpansion,
		PostgresRepeatableRead:                 cfg.PostgresRepeatableRead,
		PostgresTraversalTelemetry:             cfg.PostgresTraversalTelemetry,
		PostgresExpansionOrientationShadow:     cfg.PostgresExpansionOrientationShadow,
		PostgresExpansionOrientationTournament: cfg.PostgresExpansionOrientationTournament,
		PostgresExpansionOrientationPolicy:     cfg.PostgresExpansionOrientationPolicy,
		Discovery:                              cfg.Discovery,
		TimeoutClasses:                         append([]time.Duration(nil), cfg.TimeoutClasses...),
		DiscoverySampleFloor:                   cfg.DiscoverySampleFloor,
	}
	raw, _ := json.Marshal(payload)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// readExistingGraphCheckpoint reads a checkpoint, returning an empty checkpoint when the file does not exist.
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

// writeExistingGraphCheckpoint atomically persists live-graph completion state with restrictive permissions.
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

// appendExistingGraphProgress appends one progress event as a durable JSON Lines record.
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

// redactExistingGraphRecord removes raw parameters and Cypher text, pseudonymizes anchor values, and scrubs resolved IDs from diagnostics and plans before a live-run record is persisted.
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

// redactObservedRows replaces each normalized observation with a SHA-256 digest for live-graph persistence.
func redactObservedRows(rows []string) []string {
	for idx := range rows {
		digest := sha256.Sum256([]byte(rows[idx]))
		rows[idx] = "sha256:" + hex.EncodeToString(digest[:])
	}
	return rows
}

// redactDiagnostic replaces a nonempty diagnostic with its SHA-256 digest.
func redactDiagnostic(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(digest[:])
}

// redactResolvedIDs replaces resolved physical node IDs and unmapped entity IDs with stable redaction markers.
func redactResolvedIDs(value string, resolved map[string]graph.ID) string {
	for _, id := range resolved {
		value = regexp.MustCompile(`\b`+regexp.QuoteMeta(fmt.Sprint(id))+`\b`).ReplaceAllString(value, "<anchor-id>")
	}
	value = regexp.MustCompile(`unmapped-(node|edge|relationship):[0-9]+`).ReplaceAllString(value, "unmapped-$1:<redacted-id>")
	return value
}

// redactPlanJSON recursively replaces resolved graph IDs in a PostgreSQL JSON plan so live-run artifacts cannot disclose dataset identifiers.
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

// validateCompletedWorkloads rejects checkpoint entries that are unknown or bound to stale workload identities.
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

// idMapForManifest builds an ID map from logical and redacted physical anchor identities.
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

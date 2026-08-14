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
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/specterops/dawgs/testutil"
)

// config contains graphbench command-line selections and safety settings.
type config struct {
	// CorpusRoot locates scale-case and template declarations.
	CorpusRoot string
	// DatasetDir locates fixture datasets loaded for managed benchmark runs.
	DatasetDir string
	// Connection contains the backend connection string.
	Connection string
	// PGConnection contains the PostgreSQL connection string.
	PGConnection string
	// Neo4jConnection contains the Neo4j connection string.
	Neo4jConnection string
	// Modes lists backend execution modes requested for each benchmark round.
	Modes []ExecutionMode
	// Iterations records the number of iterations.
	Iterations int
	// WarmupIterations records the number of warmup iterations.
	WarmupIterations int
	// Round identifies the measurement round.
	Round int
	// Block identifies the measurement block used to control carryover effects.
	Block int
	// Arm identifies the measurement arm that produced the sample.
	Arm string
	// ArmOrder supplies the arm order input to the config contract.
	ArmOrder int
	// RunUUID supplies an optional stable identity shared by every artifact in one run series.
	RunUUID string
	// Cases lists exact case names requested by the user.
	Cases []string
	// Datasets lists exact dataset selectors supplied by the user.
	Datasets []string
	// Categories lists workload categories used to filter the corpus.
	Categories []string
	// Tags lists exact tag selectors supplied by the user.
	Tags []string
	// OutputJSONL selects the benchmark-result JSON Lines destination.
	OutputJSONL string
	// AppendJSONL selects append-safe JSON Lines output instead of replacing the artifact.
	AppendJSONL bool
	// Summary selects the Markdown benchmark-summary destination.
	Summary string
	// SummaryJSON selects the JSON summary destination.
	SummaryJSON string
	// Baseline identifies the baseline version or result used for comparison.
	Baseline string
	// DAWGSVersion identifies the schema version for dawgs version.
	DAWGSVersion string
	// GateBaseline selects the baseline JSON Lines artifact for performance gating.
	GateBaseline string
	// GateCandidate selects the candidate JSON Lines artifact for performance gating.
	GateCandidate string
	// GateOutput selects the performance-gate JSON report destination.
	GateOutput string
	// GateAA selects the host A/A resolution report required by production performance gating.
	GateAA string
	// GateSeed controls deterministic performance-gate bootstrap resampling.
	GateSeed int64
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64
	// Regression sets the largest candidate-to-baseline median ratio accepted by the gate.
	Regression float64
	// GateTargets lists exact case names subject to performance gating.
	GateTargets []string
	// MaterialityRatio sets the relative change required before a difference is material.
	MaterialityRatio float64
	// MaterialityAbsolute sets the absolute duration change required before a difference is material.
	MaterialityAbsolute time.Duration
	// DestructiveLock selects the lock-file path that serializes destructive runs.
	DestructiveLock string
	// AAArtifacts select one or more benchmark record files used to estimate
	// within-arm noise. Repeating the input lets independently appended A/A arms
	// remain immutable while the reporter validates them as one logical cohort.
	AAArtifacts []string
	// AAOutput selects the A/A resolution report destination.
	AAOutput string
	// ReferenceClosureArtifact selects benchmark records used for production-to-reference closure analysis.
	ReferenceClosureArtifact string
	// ReferenceClosureOutput selects the reference-closure report destination.
	ReferenceClosureOutput string
	// ReferenceClosureArm selects the independent reference arm compared with production.
	ReferenceClosureArm string
	// ReferencePairArtifact selects benchmark records containing the two reference arms to compare.
	ReferencePairArtifact string
	// ReferencePairOutput selects the paired-reference report destination.
	ReferencePairOutput string
	// ReferencePairBaseline selects the reference arm treated as the paired baseline.
	ReferencePairBaseline string
	// ReferencePairCandidate selects the reference arm compared with the paired baseline.
	ReferencePairCandidate string
	// ReferencePairProtocol selects confirmation or discovery sample requirements for paired references.
	ReferencePairProtocol string
	// ReferenceTournamentArtifact selects records containing a predeclared three- or five-arm tournament.
	ReferenceTournamentArtifact string
	// ReferenceTournamentOutput selects the tournament report destination.
	ReferenceTournamentOutput string
	// ReferenceTournamentArms lists tournament arms with the incumbent first.
	ReferenceTournamentArms []string
	// ReferenceTournamentProtocol selects confirmation or discovery tournament requirements.
	ReferenceTournamentProtocol string
	// PoolSize sets the database connection-pool size.
	PoolSize int
	// Concurrency lists opt-in worker counts for PostgreSQL concurrency measurements.
	Concurrency []int
	// SessionMemoryCeilingBytes sets the per-session memory ceiling in bytes.
	SessionMemoryCeilingBytes int64
	// PoolMemoryCeilingBytes sets the aggregate pool memory ceiling in bytes.
	PoolMemoryCeilingBytes int64
	// PostgresReferences enables independent PostgreSQL reference-arm measurement and persistence.
	PostgresReferences bool
	// PostgresReferenceArms lists independent PostgreSQL reference arms selected for measurement.
	PostgresReferenceArms []string
	// PostgresForceShortest selects a forced shortest-path executor for diagnostic runs.
	PostgresForceShortest string
	// PostgresProductionManifest selects a provisional version-2 manifest used
	// to measure an exact guarded production statement before evidence closure.
	PostgresProductionManifest string
	// PostgresRepeatableRead measures the incumbent under the same stable
	// snapshot contract required for guarded candidate admission.
	PostgresRepeatableRead bool
	// PostgresForceExpansion selects a forced expansion search strategy for diagnostic runs.
	PostgresForceExpansion string
	// PostgresTraversalTelemetry selects off, summary, or an untimed diagnostic replay.
	PostgresTraversalTelemetry string
	// PostgresExpansionOrientationShadow executes the incumbent while recording the orientation policy's SQL-visible choice.
	PostgresExpansionOrientationShadow bool
	// PostgresExpansionOrientationTournament executes the guarded selector's
	// chosen arm in the same statement.
	PostgresExpansionOrientationTournament bool
	// PostgresExpansionOrientationPolicy selects an immutable tool-only
	// orientation formula. Empty preserves orientation-probe-v1.
	PostgresExpansionOrientationPolicy string
	// PostgresExpansionSuffixReverseGuard executes the static, full-path-only
	// suffix-reverse guard and its exact forward fallback in one statement.
	PostgresExpansionSuffixReverseGuard bool
	// PostgresSuffixGuardSuffixLimit overrides the tool-only cap+1 suffix payload limit.
	PostgresSuffixGuardSuffixLimit int64
	// PostgresSuffixGuardStateLimit overrides the tool-only cap+1 reverse-state limit.
	PostgresSuffixGuardStateLimit int64
	// ConfirmLeft selects the left artifact used for paired confirmation.
	ConfirmLeft string
	// ConfirmRight selects the right artifact used for paired confirmation.
	ConfirmRight string
	// ConfirmAA selects the A/A noise report used to classify confirmation deltas.
	ConfirmAA string
	// ConfirmOutput selects the paired confirmation report destination.
	ConfirmOutput string
	// ConfirmCases lists exact case names included in paired confirmation.
	ConfirmCases []string
	// DiagnosticGate marks output as diagnostic and therefore ineligible for a complete release-gate pass.
	DiagnosticGate bool
	// BundleDir selects the directory that receives portable artifacts and source provenance.
	BundleDir string
	// BundleEvidence lists named auxiliary artifacts copied into a newly captured bundle.
	BundleEvidence []CaptureBundleEvidenceInput
	// BundleVerify selects a portable bundle directory for standalone validation.
	BundleVerify string
	// BundleVerifyOutput selects the standalone bundle-verification JSON destination.
	BundleVerifyOutput string
	// BundleRequireClean rejects otherwise valid bundles captured from a dirty source tree.
	BundleRequireClean bool
	// PromotionManifest selects a complete evidence-closure manifest for standalone verification.
	PromotionManifest string
	// PromotionManifestOutput selects the verification report destination.
	PromotionManifestOutput string
	// PromotionBindManifest supplies the provisional manifest whose immutable
	// identity is attached to one generated evidence report.
	PromotionBindManifest string
	// PromotionBindRole names the evidence role being bound.
	PromotionBindRole string
	// PromotionBindInput and PromotionBindOutput select the unbound and bound reports.
	PromotionBindInput string
	// PromotionBindOutput supplies the promotion bind output input to the config contract.
	PromotionBindOutput string
	// OperationalGateInput selects the schema-v2 operational evidence document.
	OperationalGateInput string
	// OperationalGateOutput selects the machine-verifiable operational gate report destination.
	OperationalGateOutput string
	// BuildCommand supplies the build command input to the config contract.
	BuildCommand string
	// ExistingGraph selects read-only execution against a pre-existing graph.
	ExistingGraph bool
	// AnchorManifest selects the live-graph anchor manifest to validate and redact.
	AnchorManifest string
	// Checkpoint selects the persisted live-graph completion checkpoint.
	Checkpoint string
	// Resume allows live-graph execution to skip checkpointed workloads with matching identities.
	Resume bool
	// Progress selects the append-only live-graph progress JSON Lines destination.
	Progress string
	// Discovery enables adaptive live-graph discovery instead of the fixed confirmation protocol.
	Discovery bool
	// TimeoutClasses lists increasing per-attempt deadlines for adaptive live-graph discovery.
	TimeoutClasses []time.Duration
	// DiscoverySampleFloor sets the minimum live-graph samples required before adaptive discovery may stop.
	DiscoverySampleFloor int
	// ResourceArtifact selects benchmark records evaluated against plan-resource limits.
	ResourceArtifact string
	// ResourceOutput selects the resource-gate JSON report destination.
	ResourceOutput string
	// BackendDeltaArtifact selects records used for descriptive PostgreSQL-to-Neo4j comparison.
	BackendDeltaArtifact string
	// BackendDeltaOutput selects the cross-backend delta report destination.
	BackendDeltaOutput string
	// ExpandIntoArtifact selects records used to build the fixed-one-hop three-arm study report.
	ExpandIntoArtifact string
	// ExpandIntoOutput selects the ExpandInto study JSON destination.
	ExpandIntoOutput string
	// ExpandIntoProtocol selects discovery or confirmation evidence requirements.
	ExpandIntoProtocol string
	// OrientationShadowArtifact selects true-shadow orientation records.
	OrientationShadowArtifact string
	// OrientationIncumbentArtifact selects matched exact incumbent records.
	OrientationIncumbentArtifact string
	// OrientationReverseArtifact selects matched exact forced-reverse records.
	OrientationReverseArtifact string
	// OrientationAA selects host A/A timing resolution for selector regret.
	OrientationAA string
	// OrientationOutput selects the selector-regret and probe-overhead report destination.
	OrientationOutput string
	// OrientationProtocol selects discovery or confirmation evidence requirements.
	OrientationProtocol string
	// OrientationV2ShadowArtifact selects orientation-probe-v2 shadow records.
	OrientationV2ShadowArtifact string
	// OrientationV2IncumbentArtifact selects matched exact forward records.
	OrientationV2IncumbentArtifact string
	// OrientationV2ReverseArtifact selects matched exact reverse records.
	OrientationV2ReverseArtifact string
	// OrientationV2GuardedArtifact selects actual guarded dual-arm records.
	OrientationV2GuardedArtifact string
	// OrientationV2AA selects the host A/A timing-resolution report.
	OrientationV2AA string
	// OrientationV2Freeze binds confirmation to the preregistered discovery identity.
	OrientationV2Freeze string
	// OrientationV2DiscoveryReport supplies the checksummed training-only report bound by the freeze.
	OrientationV2DiscoveryReport string
	// OrientationV2FreezeOutput writes the preregistered identity after training-only discovery.
	OrientationV2FreezeOutput string
	// OrientationV2Output selects the four-arm qualification report destination.
	OrientationV2Output string
	// OrientationV2Protocol selects discovery or confirmation evidence requirements.
	OrientationV2Protocol string
	// SuffixGuardIncumbentArtifact selects matched exact-forward feasibility records.
	SuffixGuardIncumbentArtifact string
	// SuffixGuardReverseArtifact selects matched exact suffix-reverse feasibility records.
	SuffixGuardReverseArtifact string
	// SuffixGuardGuardedArtifact selects matched production-shaped guard records.
	SuffixGuardGuardedArtifact string
	// SuffixGuardAA selects matching host A/A timing-resolution evidence.
	SuffixGuardAA string
	// SuffixGuardOutput selects the training-only feasibility report destination.
	SuffixGuardOutput string
	// SPI1BaselineArtifact selects exact S4 records for the staged inbound-I1 study.
	SPI1BaselineArtifact string
	// SPI1CandidateArtifact selects guarded canonical-I1 records for the staged study.
	SPI1CandidateArtifact string
	// SPI1ResourceReport supplies the candidate artifact's checksummed resource gate.
	SPI1ResourceReport string
	// SPI1Freeze binds confirmation reporting or holdout capture to training-only discovery.
	SPI1Freeze string
	// SPI1DiscoveryReport supplies the checksummed training-only report bound by the freeze.
	SPI1DiscoveryReport string
	// SPI1TrainingBaseline supplies the exact S4 training evidence named by the freeze.
	SPI1TrainingBaseline string
	// SPI1TrainingCandidate supplies the exact I1 training evidence named by the freeze.
	SPI1TrainingCandidate string
	// SPI1TrainingResource supplies the exact training resource report named by the freeze.
	SPI1TrainingResource string
	// SPI1FreezeOutput writes the training-only staged-study freeze manifest.
	SPI1FreezeOutput string
	// SPI1Output selects the staged S4-to-I1 qualification report destination.
	SPI1Output string
	// SPI1Protocol selects discovery or confirmation evidence requirements.
	SPI1Protocol string
	// SPI2BaselineArtifact selects exact S4 distance records for staged SP-I2 qualification.
	SPI2BaselineArtifact string
	// SPI2CandidateArtifact selects guarded SP-I2 distance records for the staged study.
	SPI2CandidateArtifact string
	// SPI2ResourceReport supplies the candidate artifact's checksummed resource gate.
	SPI2ResourceReport string
	// SPI2Freeze binds confirmation reporting or holdout capture to training-only discovery.
	SPI2Freeze string
	// SPI2DiscoveryReport supplies the checksummed training-only report bound by the freeze.
	SPI2DiscoveryReport string
	// SPI2TrainingBaseline supplies the exact S4 distance training evidence named by the freeze.
	SPI2TrainingBaseline string
	// SPI2TrainingCandidate supplies the exact guarded-distance training evidence named by the freeze.
	SPI2TrainingCandidate string
	// SPI2TrainingResource supplies the exact training resource report named by the freeze.
	SPI2TrainingResource string
	// SPI2FreezeOutput writes the training-only staged-study freeze manifest.
	SPI2FreezeOutput string
	// SPI2Output selects the staged S4-to-I2 qualification report destination.
	SPI2Output string
	// SPI2Protocol selects discovery or confirmation evidence requirements.
	SPI2Protocol string
}

// parseConfig parses graphbench flags and rejects unsafe or incomplete workflow combinations.
func parseConfig(args []string, env func(string) string) (config, error) {
	flags := flag.NewFlagSet("graphbench", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		cfg               config
		rawModes          string
		rawGateTargets    string
		rawConcurrency    string
		rawCases          string
		rawDatasets       string
		rawCategories     string
		rawTags           string
		rawConfirmCases   string
		rawReferenceArms  string
		rawTournamentArms string
		rawTimeoutClasses string
		rawBundleEvidence []string
	)

	flags.StringVar(&cfg.CorpusRoot, "corpus-root", "benchmark/testdata/scale", "scale corpus root")
	flags.StringVar(&cfg.DatasetDir, "dataset-dir", "integration/testdata", "dataset root")
	flags.StringVar(&cfg.Connection, "connection", env("CONNECTION_STRING"), "single backend connection string")
	flags.StringVar(&cfg.PGConnection, "pg-connection", env("PG_CONNECTION_STRING"), "PostgreSQL connection string")
	flags.StringVar(&cfg.Neo4jConnection, "neo4j-connection", env("NEO4J_CONNECTION_STRING"), "Neo4j connection string")
	flags.StringVar(&rawModes, "modes", string(ModePostgresSQL), "comma-separated execution modes")
	flags.IntVar(&cfg.Iterations, "iterations", 3, "timed iterations per case")
	flags.IntVar(&cfg.WarmupIterations, "warmup-iterations", 1, "fixed untimed warmup iterations per case")
	flags.IntVar(&cfg.Round, "round", 1, "independent benchmark round identifier")
	flags.IntVar(&cfg.Block, "block", 1, "matched benchmark block identifier")
	flags.StringVar(&cfg.Arm, "arm", "unlabeled", "matched benchmark arm label")
	flags.IntVar(&cfg.ArmOrder, "arm-order", 0, "one-based execution order inside the matched block (0 when unpaired)")
	flags.StringVar(&cfg.RunUUID, "run-uuid", "", "run-series UUID (generated when empty)")
	flags.StringVar(&rawCases, "cases", "", "comma-separated exact case names")
	flags.StringVar(&rawDatasets, "datasets", "", "comma-separated exact dataset names")
	flags.StringVar(&rawCategories, "categories", "", "comma-separated exact category names")
	flags.StringVar(&rawTags, "tags", "", "comma-separated exact case tags")
	flags.StringVar(&cfg.OutputJSONL, "jsonl-output", "", "JSONL output path (default: stdout)")
	flags.BoolVar(&cfg.AppendJSONL, "append-jsonl", false, "append a validated round to an existing JSONL run-series artifact")
	flags.StringVar(&cfg.Summary, "summary", "", "markdown summary output path")
	flags.StringVar(&cfg.SummaryJSON, "summary-json", "", "JSON summary output path")
	flags.StringVar(&cfg.Baseline, "baseline", "", "previous JSONL output for baseline comparison")
	flags.StringVar(&cfg.DAWGSVersion, "dawgs-version", "", "DAWGS source version (auto-detected when empty)")
	flags.StringVar(&cfg.GateBaseline, "gate-baseline", "", "baseline JSONL artifact for comparison-only mode")
	flags.StringVar(&cfg.GateCandidate, "gate-candidate", "", "candidate JSONL artifact for comparison-only mode")
	flags.StringVar(&cfg.GateOutput, "gate-output", "", "performance-gate JSON output path (default: stdout)")
	flags.StringVar(&cfg.GateAA, "gate-aa", "", "host A/A resolution report required for production performance gating")
	flags.Int64Var(&cfg.GateSeed, "seed", 1, "deterministic bootstrap seed")
	flags.Float64Var(&cfg.Confidence, "confidence-level", defaultConfidenceLevel, "bootstrap confidence level")
	flags.Float64Var(&cfg.Regression, "regression-threshold", minimumTimingNoiseRatio, "minimum allowed comparable-case regression ratio before host A/A noise")
	flags.StringVar(&rawGateTargets, "gate-targets", "", "comma-separated PostgreSQL case names expected to improve materially")
	flags.Float64Var(&cfg.MaterialityRatio, "materiality-ratio", 0.95, "target median-ratio upper bound")
	flags.DurationVar(&cfg.MaterialityAbsolute, "materiality-absolute", 100*time.Microsecond, "target median-saving lower bound")
	flags.StringVar(&cfg.DestructiveLock, "destructive-lock", ".coverage/graphbench.lock", "local lock file guarding destructive fixture reloads")
	flags.Func("aa-artifact", "JSONL artifact used to calculate baseline A/A measurement resolution (repeat for separately captured arms)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("aa-artifact path must not be empty")
		}
		cfg.AAArtifacts = append(cfg.AAArtifacts, value)
		return nil
	})
	flags.StringVar(&cfg.AAOutput, "aa-output", "", "A/A measurement-resolution JSON output path (default: stdout)")
	flags.StringVar(&cfg.ReferenceClosureArtifact, "reference-closure-artifact", "", "JSONL artifact containing matched production raw-pgx and PostgreSQL reference samples")
	flags.StringVar(&cfg.ReferenceClosureOutput, "reference-closure-output", "", "production/reference closure JSON output path (default: stdout)")
	flags.StringVar(&cfg.ReferenceClosureArm, "reference-closure-arm", "s3_unidirectional_trail_cte", "PostgreSQL full-comparator reference arm")
	flags.StringVar(&cfg.ReferencePairArtifact, "reference-pair-artifact", "", "JSONL artifact containing two matched PostgreSQL reference arms")
	flags.StringVar(&cfg.ReferencePairOutput, "reference-pair-output", "", "matched PostgreSQL reference-pair JSON output path (default: stdout)")
	flags.StringVar(&cfg.ReferencePairBaseline, "reference-pair-baseline", "", "baseline PostgreSQL reference arm")
	flags.StringVar(&cfg.ReferencePairCandidate, "reference-pair-candidate", "", "candidate PostgreSQL reference arm")
	flags.StringVar(&cfg.ReferencePairProtocol, "reference-pair-protocol", referencePairProtocolConfirmation, "reference-pair report protocol (confirmation or discovery)")
	flags.StringVar(&cfg.ReferenceTournamentArtifact, "reference-tournament-artifact", "", "JSONL artifact containing a predeclared three- or five-arm PostgreSQL reference tournament")
	flags.StringVar(&cfg.ReferenceTournamentOutput, "reference-tournament-output", "", "reference tournament JSON output path (default: stdout)")
	flags.StringVar(&rawTournamentArms, "reference-tournament-arms", "", "comma-separated tournament arms with the incumbent first")
	flags.StringVar(&cfg.ReferenceTournamentProtocol, "reference-tournament-protocol", referencePairProtocolConfirmation, "reference tournament protocol (confirmation or discovery)")
	flags.IntVar(&cfg.PoolSize, "pool-size", 1, "PostgreSQL physical pool size")
	flags.StringVar(&rawConcurrency, "concurrency", "", "comma-separated opt-in PostgreSQL concurrency smoke levels")
	flags.Int64Var(&cfg.SessionMemoryCeilingBytes, "session-memory-ceiling-bytes", 0, "declared maximum performance workspace bytes per PostgreSQL session")
	flags.Int64Var(&cfg.PoolMemoryCeilingBytes, "pool-memory-ceiling-bytes", 0, "declared maximum performance workspace bytes for the complete PostgreSQL pool")
	flags.BoolVar(&cfg.PostgresReferences, "postgres-references", false, "capture C1 PostgreSQL component floors and full-query references")
	flags.StringVar(&rawReferenceArms, "postgres-reference-arms", "", "comma-separated PostgreSQL reference arms (default: all applicable arms)")
	flags.StringVar(&cfg.PostgresForceShortest, "postgres-force-shortest-executor", "", "tool-only forced PostgreSQL shortest executor (supported: SP-S0, SP-S0-DIRECT, SP-S3-U-D, SP-S3-U-E+MAT-M0, SP-S4-C-D, SP-S4-C-WE+MAT-M0, SP-I1-C-D, SP-I2-C-D, SP-I1-U-E+MAT-M0, SP-I1-C-WE+MAT-M0, SP-B1-C-ALT-NODE-D, SP-B1-C-ALT-NODE-WE+MAT-M0, SP-B2-C-MIN-LEVEL-D, SP-B2-C-MIN-LEVEL-WE+MAT-M0, ASP-A1-DAG, ASP-I1-U-DAG+MAT-M0, ASP-B1-DAG-ALT-NODE, ASP-B2-DAG-MIN-LEVEL)")
	flags.StringVar(&cfg.PostgresProductionManifest, "postgres-production-manifest", "", "provisional version-2 manifest for exact guarded PostgreSQL candidate measurement")
	flags.BoolVar(&cfg.PostgresRepeatableRead, "postgres-repeatable-read", false, "measure PostgreSQL under an explicit Repeatable Read transaction")
	flags.StringVar(&cfg.PostgresForceExpansion, "postgres-force-expansion-search", "", "tool-only forced PostgreSQL expansion search (supported: EXPANSION-SUFFIX-SEEDED-REVERSE, EXPANSION-ENDPOINT-SEEDED-REVERSE)")
	flags.StringVar(&cfg.PostgresTraversalTelemetry, "postgres-traversal-telemetry", postgresTraversalTelemetryOff, "PostgreSQL traversal telemetry level (off, summary, or diagnostic); replays run outside timed samples")
	flags.BoolVar(&cfg.PostgresExpansionOrientationShadow, "postgres-expansion-orientation-shadow", false, "tool-only orientation-probe shadow mode; executes only the exact incumbent traversal arm")
	flags.BoolVar(&cfg.PostgresExpansionOrientationTournament, "postgres-expansion-orientation-tournament", false, "tool-only guarded orientation-probe mode; executes the selected exact arm")
	flags.StringVar(&cfg.PostgresExpansionOrientationPolicy, "postgres-expansion-orientation-policy", "", "tool-only immutable orientation policy (orientation-probe-v1 or orientation-probe-v2; default: v1)")
	flags.BoolVar(&cfg.PostgresExpansionSuffixReverseGuard, "postgres-expansion-suffix-reverse-guard", false, "tool-only full-path suffix-reverse guard with exact forward fallback")
	flags.Int64Var(&cfg.PostgresSuffixGuardSuffixLimit, "postgres-suffix-guard-suffix-limit", 0, "tool-only suffix payload cap override (0 uses the immutable policy default)")
	flags.Int64Var(&cfg.PostgresSuffixGuardStateLimit, "postgres-suffix-guard-state-limit", 0, "tool-only reverse-state cap override (0 uses the immutable policy default)")
	flags.StringVar(&cfg.ConfirmLeft, "confirm-left", "", "left JSONL artifact for paired confirmation mode")
	flags.StringVar(&cfg.ConfirmRight, "confirm-right", "", "right JSONL artifact for paired confirmation mode")
	flags.StringVar(&cfg.ConfirmAA, "confirm-aa", "", "optional block/reload A/A resolution report")
	flags.StringVar(&cfg.ConfirmOutput, "confirm-output", "", "paired confirmation JSON output path (default: stdout)")
	flags.StringVar(&rawConfirmCases, "confirm-cases", "", "comma-separated exact primary names for paired confirmation")
	flags.BoolVar(&cfg.DiagnosticGate, "diagnostic-gate", false, "allow comparison of matching diagnostic-only subsets")
	flags.StringVar(&cfg.BundleDir, "bundle-dir", "", "write a reconstructible capture bundle to this directory")
	flags.Func("bundle-evidence", "named auxiliary bundle artifact as name=path (repeatable)", func(value string) error {
		rawBundleEvidence = append(rawBundleEvidence, value)
		return nil
	})
	flags.StringVar(&cfg.BundleVerify, "bundle-verify", "", "standalone verification of a capture bundle directory")
	flags.StringVar(&cfg.BundleVerifyOutput, "bundle-verify-output", "", "capture-bundle verification JSON output path (default: stdout)")
	flags.BoolVar(&cfg.BundleRequireClean, "bundle-require-clean", false, "require standalone bundle verification to prove a clean source capture")
	flags.StringVar(&cfg.PromotionManifest, "promotion-manifest", "", "verify a candidate promotion manifest and every bound evidence report")
	flags.StringVar(&cfg.PromotionManifestOutput, "promotion-manifest-output", "", "promotion-manifest verification JSON destination (default: stdout)")
	flags.StringVar(&cfg.PromotionBindManifest, "promotion-bind-manifest", "", "provisional promotion manifest supplying report identity")
	flags.StringVar(&cfg.PromotionBindRole, "promotion-bind-role", "", "promotion evidence role to bind")
	flags.StringVar(&cfg.PromotionBindInput, "promotion-bind-input", "", "unbound promotion evidence report")
	flags.StringVar(&cfg.PromotionBindOutput, "promotion-bind-output", "", "identity-bound promotion evidence report")
	flags.StringVar(&cfg.OperationalGateInput, "operational-gate-input", "", "schema-v2 candidate-bound operational evidence document")
	flags.StringVar(&cfg.OperationalGateOutput, "operational-gate-output", "", "operational evidence gate JSON report output path")
	flags.StringVar(&cfg.BuildCommand, "build-command", "go build -trimpath ./cmd/graphbench", "reproducible build command recorded in bundles")
	flags.BoolVar(&cfg.ExistingGraph, "existing-graph", false, "run non-mutating PostgreSQL cases against an existing graph in read-write sessions without schema, load, clear, vacuum, or persistent writes")
	flags.StringVar(&cfg.AnchorManifest, "anchor-manifest", "", "versioned logical-key anchor manifest for existing-graph mode")
	flags.StringVar(&cfg.Checkpoint, "checkpoint", "", "atomic existing-graph checkpoint path")
	flags.BoolVar(&cfg.Resume, "resume", false, "resume completed records from the matching existing-graph checkpoint")
	flags.StringVar(&cfg.Progress, "progress", "", "append-only existing-graph progress JSONL path")
	flags.BoolVar(&cfg.Discovery, "discovery", false, "label the run adaptive discovery rather than fixed confirmation")
	flags.StringVar(&rawTimeoutClasses, "timeout-classes", "", "comma-separated predeclared per-case timeout classes used by discovery")
	flags.IntVar(&cfg.DiscoverySampleFloor, "discovery-sample-floor", 1, "minimum measured samples after adaptive discovery reduction")
	flags.StringVar(&cfg.ResourceArtifact, "resource-artifact", "", "JSONL artifact used to calculate the state/resource gate")
	flags.StringVar(&cfg.ResourceOutput, "resource-output", "", "state/resource gate JSON output path (default: stdout)")
	flags.StringVar(&cfg.BackendDeltaArtifact, "backend-delta-artifact", "", "JSONL artifact used for descriptive matched PostgreSQL/Neo4j deltas")
	flags.StringVar(&cfg.BackendDeltaOutput, "backend-delta-output", "", "descriptive backend-delta JSON output path (default: stdout)")
	flags.StringVar(&cfg.ExpandIntoArtifact, "expand-into-artifact", "", "JSONL artifact used to build the fixed-one-hop three-arm study report")
	flags.StringVar(&cfg.ExpandIntoOutput, "expand-into-output", "", "ExpandInto study JSON output path (default: stdout)")
	flags.StringVar(&cfg.ExpandIntoProtocol, "expand-into-protocol", referencePairProtocolDiscovery, "ExpandInto study protocol (discovery or confirmation)")
	flags.StringVar(&cfg.OrientationShadowArtifact, "orientation-shadow-artifact", "", "true-shadow orientation JSONL artifact")
	flags.StringVar(&cfg.OrientationIncumbentArtifact, "orientation-incumbent-artifact", "", "matched exact incumbent orientation JSONL artifact")
	flags.StringVar(&cfg.OrientationReverseArtifact, "orientation-reverse-artifact", "", "matched exact forced-reverse orientation JSONL artifact")
	flags.StringVar(&cfg.OrientationAA, "orientation-aa", "", "host A/A report used by orientation selector-regret analysis")
	flags.StringVar(&cfg.OrientationOutput, "orientation-output", "", "orientation selector-regret and probe-overhead JSON output path (default: stdout)")
	flags.StringVar(&cfg.OrientationProtocol, "orientation-protocol", referencePairProtocolConfirmation, "orientation report protocol (discovery or confirmation)")
	flags.StringVar(&cfg.OrientationV2ShadowArtifact, "orientation-v2-shadow-artifact", "", "orientation-probe-v2 shadow JSONL artifact")
	flags.StringVar(&cfg.OrientationV2IncumbentArtifact, "orientation-v2-incumbent-artifact", "", "matched exact forward orientation-v2 JSONL artifact")
	flags.StringVar(&cfg.OrientationV2ReverseArtifact, "orientation-v2-reverse-artifact", "", "matched exact forced-reverse orientation-v2 JSONL artifact")
	flags.StringVar(&cfg.OrientationV2GuardedArtifact, "orientation-v2-guarded-artifact", "", "matched actual guarded orientation-v2 JSONL artifact")
	flags.StringVar(&cfg.OrientationV2AA, "orientation-v2-aa", "", "host A/A report used by orientation-v2 qualification")
	flags.StringVar(&cfg.OrientationV2Freeze, "orientation-v2-freeze", "", "discovery freeze manifest required by orientation-v2 confirmation")
	flags.StringVar(&cfg.OrientationV2DiscoveryReport, "orientation-v2-discovery-report", "", "training-only discovery report bound by the orientation-v2 freeze")
	flags.StringVar(&cfg.OrientationV2FreezeOutput, "orientation-v2-freeze-output", "", "write the training-only orientation-v2 discovery freeze manifest")
	flags.StringVar(&cfg.OrientationV2Output, "orientation-v2-output", "", "four-arm orientation-v2 qualification JSON output path (default: stdout)")
	flags.StringVar(&cfg.OrientationV2Protocol, "orientation-v2-protocol", referencePairProtocolConfirmation, "orientation-v2 report protocol (discovery or confirmation)")
	flags.StringVar(&cfg.SuffixGuardIncumbentArtifact, "suffix-guard-incumbent-artifact", "", "six-round exact-forward training artifact for the suffix-reverse stop gate")
	flags.StringVar(&cfg.SuffixGuardReverseArtifact, "suffix-guard-reverse-artifact", "", "six-round exact suffix-reverse training artifact")
	flags.StringVar(&cfg.SuffixGuardGuardedArtifact, "suffix-guard-guarded-artifact", "", "six-round production-shaped suffix-reverse guard training artifact")
	flags.StringVar(&cfg.SuffixGuardAA, "suffix-guard-aa", "", "matching order-balanced incumbent A/A resolution report")
	flags.StringVar(&cfg.SuffixGuardOutput, "suffix-guard-output", "", "training-only suffix-reverse feasibility report output path")
	flags.StringVar(&cfg.SPI1BaselineArtifact, "sp-i1-baseline-artifact", "", "matched exact S4 JSONL artifact for staged inbound-I1 qualification")
	flags.StringVar(&cfg.SPI1CandidateArtifact, "sp-i1-candidate-artifact", "", "matched guarded canonical-I1 JSONL artifact for staged inbound-I1 qualification")
	flags.StringVar(&cfg.SPI1ResourceReport, "sp-i1-resource-report", "", "resource-gate report bound to the staged canonical-I1 artifact")
	flags.StringVar(&cfg.SPI1Freeze, "sp-i1-freeze", "", "training-only freeze required by SP-I1 confirmation reporting and holdout capture")
	flags.StringVar(&cfg.SPI1DiscoveryReport, "sp-i1-discovery-report", "", "training-only discovery report bound by the SP-I1 freeze")
	flags.StringVar(&cfg.SPI1TrainingBaseline, "sp-i1-training-baseline-artifact", "", "exact S4 training artifact required to recompute a frozen SP-I1 discovery")
	flags.StringVar(&cfg.SPI1TrainingCandidate, "sp-i1-training-candidate-artifact", "", "exact canonical-I1 training artifact required to recompute a frozen SP-I1 discovery")
	flags.StringVar(&cfg.SPI1TrainingResource, "sp-i1-training-resource-report", "", "exact training resource report required to recompute a frozen SP-I1 discovery")
	flags.StringVar(&cfg.SPI1FreezeOutput, "sp-i1-freeze-output", "", "write the staged SP-I1 training-only freeze manifest")
	flags.StringVar(&cfg.SPI1Output, "sp-i1-output", "", "staged S4-to-I1 qualification JSON output path")
	flags.StringVar(&cfg.SPI1Protocol, "sp-i1-protocol", referencePairProtocolConfirmation, "staged SP-I1 report protocol (discovery or confirmation)")
	flags.StringVar(&cfg.SPI2BaselineArtifact, "sp-i2-baseline-artifact", "", "matched exact S4 distance JSONL artifact for staged SP-I2 qualification")
	flags.StringVar(&cfg.SPI2CandidateArtifact, "sp-i2-candidate-artifact", "", "matched guarded SP-I2 distance JSONL artifact")
	flags.StringVar(&cfg.SPI2ResourceReport, "sp-i2-resource-report", "", "resource-gate report bound to the staged SP-I2 artifact")
	flags.StringVar(&cfg.SPI2Freeze, "sp-i2-freeze", "", "training-only freeze required by SP-I2 confirmation reporting and holdout capture")
	flags.StringVar(&cfg.SPI2DiscoveryReport, "sp-i2-discovery-report", "", "training-only discovery report bound by the SP-I2 freeze")
	flags.StringVar(&cfg.SPI2TrainingBaseline, "sp-i2-training-baseline-artifact", "", "exact S4 distance training artifact required to recompute frozen SP-I2 discovery")
	flags.StringVar(&cfg.SPI2TrainingCandidate, "sp-i2-training-candidate-artifact", "", "exact guarded SP-I2 training artifact required to recompute frozen discovery")
	flags.StringVar(&cfg.SPI2TrainingResource, "sp-i2-training-resource-report", "", "exact training resource report required to recompute frozen SP-I2 discovery")
	flags.StringVar(&cfg.SPI2FreezeOutput, "sp-i2-freeze-output", "", "write the staged SP-I2 training-only freeze manifest")
	flags.StringVar(&cfg.SPI2Output, "sp-i2-output", "", "staged S4-distance-to-I2 qualification JSON output path")
	flags.StringVar(&cfg.SPI2Protocol, "sp-i2-protocol", referencePairProtocolConfirmation, "staged SP-I2 report protocol (discovery or confirmation)")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	var err error
	if cfg.BundleEvidence, err = parseCaptureBundleEvidenceInputs(rawBundleEvidence); err != nil {
		return config{}, err
	}
	if cfg.Iterations < 1 {
		return config{}, fmt.Errorf("iterations must be at least 1")
	}
	if cfg.WarmupIterations < 0 {
		return config{}, fmt.Errorf("warmup-iterations must not be negative")
	}
	if cfg.Round < 1 {
		return config{}, fmt.Errorf("round must be at least 1")
	}
	if cfg.Block < 1 {
		return config{}, fmt.Errorf("block must be at least 1")
	}
	if strings.TrimSpace(cfg.Arm) == "" {
		return config{}, fmt.Errorf("arm must not be empty")
	}
	if cfg.ArmOrder < 0 {
		return config{}, fmt.Errorf("arm-order must not be negative")
	}
	if cfg.PoolSize < 1 {
		return config{}, fmt.Errorf("pool-size must be at least 1")
	}
	if cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryOff &&
		cfg.PostgresTraversalTelemetry != postgresTraversalTelemetrySummary &&
		cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryDiagnostic {
		return config{}, fmt.Errorf("postgres-traversal-telemetry must be off, summary, or diagnostic")
	}
	if cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryOff && cfg.PoolSize != 1 {
		return config{}, fmt.Errorf("PostgreSQL traversal telemetry requires pool-size 1 to preserve connection identity")
	}
	if cfg.SessionMemoryCeilingBytes < 0 || cfg.PoolMemoryCeilingBytes < 0 {
		return config{}, fmt.Errorf("memory ceilings must not be negative")
	}
	if cfg.SessionMemoryCeilingBytes > 0 && cfg.PoolMemoryCeilingBytes > 0 && cfg.SessionMemoryCeilingBytes*int64(cfg.PoolSize) > cfg.PoolMemoryCeilingBytes {
		return config{}, fmt.Errorf("session memory ceiling times pool size exceeds pool memory ceiling")
	}
	for _, raw := range strings.Split(rawConcurrency, ",") {
		if raw = strings.TrimSpace(raw); raw == "" {
			continue
		}
		level, err := strconv.Atoi(raw)
		if err != nil || level < 1 {
			return config{}, fmt.Errorf("concurrency levels must be positive integers, got %q", raw)
		}
		if !slices.Contains(cfg.Concurrency, level) {
			cfg.Concurrency = append(cfg.Concurrency, level)
		}
	}
	if (cfg.GateBaseline == "") != (cfg.GateCandidate == "") {
		return config{}, fmt.Errorf("gate-baseline and gate-candidate must be supplied together")
	}
	if cfg.GateAA != "" && cfg.GateBaseline == "" {
		return config{}, fmt.Errorf("gate-aa requires gate-baseline and gate-candidate")
	}
	if (cfg.ConfirmLeft == "") != (cfg.ConfirmRight == "") {
		return config{}, fmt.Errorf("confirm-left and confirm-right must be supplied together")
	}
	if cfg.ConfirmAA != "" && cfg.ConfirmLeft == "" {
		return config{}, fmt.Errorf("confirm-aa requires confirm-left and confirm-right")
	}
	if cfg.ReferenceClosureOutput != "" && cfg.ReferenceClosureArtifact == "" {
		return config{}, fmt.Errorf("reference-closure-output requires reference-closure-artifact")
	}
	if cfg.ReferencePairOutput != "" && cfg.ReferencePairArtifact == "" {
		return config{}, fmt.Errorf("reference-pair-output requires reference-pair-artifact")
	}
	if cfg.ReferencePairArtifact != "" && (cfg.ReferencePairBaseline == "" || cfg.ReferencePairCandidate == "") {
		return config{}, fmt.Errorf("reference-pair-artifact requires baseline and candidate arms")
	}
	if cfg.ReferencePairBaseline != "" && cfg.ReferencePairBaseline == cfg.ReferencePairCandidate {
		return config{}, fmt.Errorf("reference-pair baseline and candidate must differ")
	}
	if cfg.ReferenceTournamentOutput != "" && cfg.ReferenceTournamentArtifact == "" {
		return config{}, fmt.Errorf("reference-tournament-output requires reference-tournament-artifact")
	}
	if cfg.ReferenceTournamentArtifact != "" && len(cfg.ReferenceTournamentArms) == 0 && strings.TrimSpace(rawTournamentArms) == "" {
		return config{}, fmt.Errorf("reference-tournament-artifact requires reference-tournament-arms")
	}
	if cfg.ReferenceTournamentProtocol != referencePairProtocolDiscovery && cfg.ReferenceTournamentProtocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("reference-tournament-protocol must be discovery or confirmation")
	}
	if cfg.BundleVerifyOutput != "" && cfg.BundleVerify == "" {
		return config{}, fmt.Errorf("bundle-verify-output requires bundle-verify")
	}
	if cfg.PromotionManifestOutput != "" && cfg.PromotionManifest == "" {
		return config{}, fmt.Errorf("promotion-manifest-output requires promotion-manifest")
	}
	promotionBindConfigured := cfg.PromotionBindManifest != "" || cfg.PromotionBindRole != "" || cfg.PromotionBindInput != "" || cfg.PromotionBindOutput != ""
	if promotionBindConfigured && (cfg.PromotionBindManifest == "" || cfg.PromotionBindRole == "" || cfg.PromotionBindInput == "" || cfg.PromotionBindOutput == "") {
		return config{}, fmt.Errorf("promotion report binding requires manifest, role, input, and output")
	}
	operationalGateConfigured := cfg.OperationalGateInput != "" || cfg.OperationalGateOutput != ""
	if operationalGateConfigured && (cfg.OperationalGateInput == "" || cfg.OperationalGateOutput == "") {
		return config{}, fmt.Errorf("operational gate requires operational-gate-input and operational-gate-output")
	}
	if cfg.BundleRequireClean && cfg.BundleVerify == "" {
		return config{}, fmt.Errorf("bundle-require-clean requires bundle-verify")
	}
	if len(cfg.BundleEvidence) > 0 && cfg.BundleDir == "" {
		return config{}, fmt.Errorf("bundle-evidence requires bundle-dir")
	}
	if cfg.BundleVerify != "" && cfg.BundleDir != "" {
		return config{}, fmt.Errorf("bundle-verify and bundle-dir are mutually exclusive")
	}
	if cfg.PromotionManifest != "" && (cfg.BundleVerify != "" || cfg.BundleDir != "") {
		return config{}, fmt.Errorf("promotion-manifest verification is mutually exclusive with bundle operations")
	}
	if cfg.ExpandIntoOutput != "" && cfg.ExpandIntoArtifact == "" {
		return config{}, fmt.Errorf("expand-into-output requires expand-into-artifact")
	}
	if cfg.ExpandIntoProtocol != referencePairProtocolDiscovery && cfg.ExpandIntoProtocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("expand-into-protocol must be discovery or confirmation")
	}
	orientationInputs := []string{cfg.OrientationShadowArtifact, cfg.OrientationIncumbentArtifact, cfg.OrientationReverseArtifact, cfg.OrientationAA}
	orientationConfigured := false
	for _, input := range orientationInputs {
		orientationConfigured = orientationConfigured || input != ""
	}
	if cfg.OrientationOutput != "" {
		orientationConfigured = true
	}
	if orientationConfigured {
		for _, input := range orientationInputs {
			if input == "" {
				return config{}, fmt.Errorf("orientation report requires shadow, incumbent, reverse, and A/A artifacts")
			}
		}
	}
	if cfg.OrientationProtocol != referencePairProtocolDiscovery && cfg.OrientationProtocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("orientation-protocol must be discovery or confirmation")
	}
	orientationV2Inputs := []string{
		cfg.OrientationV2ShadowArtifact, cfg.OrientationV2IncumbentArtifact, cfg.OrientationV2ReverseArtifact,
		cfg.OrientationV2GuardedArtifact, cfg.OrientationV2AA,
	}
	orientationV2Configured := cfg.OrientationV2Output != ""
	for _, input := range orientationV2Inputs {
		orientationV2Configured = orientationV2Configured || input != ""
	}
	if orientationV2Configured {
		for _, input := range orientationV2Inputs {
			if input == "" {
				return config{}, fmt.Errorf("orientation-v2 report requires shadow, incumbent, reverse, guarded, and A/A artifacts")
			}
		}
	}
	if cfg.OrientationV2Protocol != referencePairProtocolDiscovery && cfg.OrientationV2Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("orientation-v2-protocol must be discovery or confirmation")
	}
	if orientationV2Configured && cfg.OrientationV2Protocol == referencePairProtocolConfirmation && (cfg.OrientationV2Freeze == "" || cfg.OrientationV2DiscoveryReport == "") {
		return config{}, fmt.Errorf("orientation-v2 confirmation requires orientation-v2-freeze and orientation-v2-discovery-report")
	}
	if orientationV2Configured && cfg.OrientationV2Protocol == referencePairProtocolDiscovery && (cfg.OrientationV2FreezeOutput == "" || cfg.OrientationV2Output == "") {
		return config{}, fmt.Errorf("orientation-v2 discovery requires orientation-v2-output and orientation-v2-freeze-output")
	}
	if cfg.OrientationV2Freeze != "" && cfg.OrientationV2Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("orientation-v2-freeze is only valid for confirmation")
	}
	if cfg.OrientationV2DiscoveryReport != "" && cfg.OrientationV2Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("orientation-v2-discovery-report is only valid for confirmation")
	}
	if cfg.OrientationV2FreezeOutput != "" && cfg.OrientationV2Protocol != referencePairProtocolDiscovery {
		return config{}, fmt.Errorf("orientation-v2-freeze-output is only valid for discovery")
	}
	if (cfg.OrientationV2Freeze != "" || cfg.OrientationV2DiscoveryReport != "" || cfg.OrientationV2FreezeOutput != "") && !orientationV2Configured {
		return config{}, fmt.Errorf("orientation-v2-freeze requires orientation-v2 report mode")
	}
	suffixGuardInputs := []string{
		cfg.SuffixGuardIncumbentArtifact,
		cfg.SuffixGuardReverseArtifact,
		cfg.SuffixGuardGuardedArtifact,
		cfg.SuffixGuardAA,
		cfg.SuffixGuardOutput,
	}
	suffixGuardReportConfigured := false
	for _, input := range suffixGuardInputs {
		suffixGuardReportConfigured = suffixGuardReportConfigured || input != ""
	}
	if suffixGuardReportConfigured {
		for _, input := range suffixGuardInputs {
			if input == "" {
				return config{}, fmt.Errorf("suffix-guard report requires incumbent, reverse, guarded, A/A, and output artifacts")
			}
		}
		if cfg.OutputJSONL != "" || rawCases != "" || rawDatasets != "" || rawCategories != "" || rawTags != "" {
			return config{}, fmt.Errorf("suffix-guard report mode cannot also execute or select benchmark cases")
		}
		if err := validateDistinctSPI2Paths(map[string]string{
			"incumbent artifact": cfg.SuffixGuardIncumbentArtifact,
			"reverse artifact":   cfg.SuffixGuardReverseArtifact,
			"guarded artifact":   cfg.SuffixGuardGuardedArtifact,
			"A/A report":         cfg.SuffixGuardAA,
			"report output":      cfg.SuffixGuardOutput,
		}); err != nil {
			return config{}, fmt.Errorf("suffix-guard report: %w", err)
		}
	}
	spI1ReportInputs := []string{cfg.SPI1BaselineArtifact, cfg.SPI1CandidateArtifact, cfg.SPI1ResourceReport}
	spI1TrainingInputs := []string{cfg.SPI1TrainingBaseline, cfg.SPI1TrainingCandidate, cfg.SPI1TrainingResource}
	spI1ReportConfigured := cfg.SPI1Output != "" || cfg.SPI1FreezeOutput != ""
	for _, input := range spI1ReportInputs {
		spI1ReportConfigured = spI1ReportConfigured || input != ""
	}
	if cfg.SPI1Protocol != referencePairProtocolDiscovery && cfg.SPI1Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("sp-i1-protocol must be discovery or confirmation")
	}
	if spI1ReportConfigured {
		for _, input := range spI1ReportInputs {
			if input == "" {
				return config{}, fmt.Errorf("SP-I1 report requires baseline, candidate, and resource artifacts")
			}
		}
		if cfg.SPI1Output == "" {
			return config{}, fmt.Errorf("SP-I1 report requires sp-i1-output")
		}
		if cfg.SPI1Protocol == referencePairProtocolDiscovery && cfg.SPI1FreezeOutput == "" {
			return config{}, fmt.Errorf("SP-I1 discovery requires sp-i1-freeze-output")
		}
		if cfg.SPI1Protocol == referencePairProtocolConfirmation && (cfg.SPI1Freeze == "" || cfg.SPI1DiscoveryReport == "") {
			return config{}, fmt.Errorf("SP-I1 confirmation requires sp-i1-freeze and sp-i1-discovery-report")
		}
	} else if (cfg.SPI1Freeze == "") != (cfg.SPI1DiscoveryReport == "") {
		return config{}, fmt.Errorf("SP-I1 holdout capture requires both sp-i1-freeze and sp-i1-discovery-report")
	}
	trainingInputCount := 0
	for _, input := range spI1TrainingInputs {
		if input != "" {
			trainingInputCount++
		}
	}
	if cfg.SPI1Freeze != "" && trainingInputCount != len(spI1TrainingInputs) {
		return config{}, fmt.Errorf("SP-I1 frozen authorization requires all three exact training evidence artifacts")
	}
	if cfg.SPI1Freeze == "" && trainingInputCount != 0 {
		return config{}, fmt.Errorf("SP-I1 training evidence inputs require a discovery freeze")
	}
	if !spI1ReportConfigured && cfg.SPI1Freeze != "" && cfg.SPI1Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("SP-I1 holdout capture requires the confirmation protocol")
	}
	if cfg.SPI1FreezeOutput != "" && cfg.SPI1Protocol != referencePairProtocolDiscovery {
		return config{}, fmt.Errorf("sp-i1-freeze-output is only valid for discovery")
	}
	if spI1ReportConfigured && cfg.SPI1Protocol == referencePairProtocolDiscovery && (cfg.SPI1Freeze != "" || cfg.SPI1DiscoveryReport != "") {
		return config{}, fmt.Errorf("SP-I1 discovery creates a freeze and cannot consume confirmation inputs")
	}
	if spI1ReportConfigured && (cfg.OutputJSONL != "" || rawCases != "" || rawDatasets != "" || rawCategories != "" || rawTags != "") {
		return config{}, fmt.Errorf("SP-I1 report mode cannot also execute or select benchmark cases")
	}
	if spI1ReportConfigured {
		if err := validateDistinctSPI1Paths(map[string]string{
			"baseline artifact": cfg.SPI1BaselineArtifact, "candidate artifact": cfg.SPI1CandidateArtifact,
			"resource report": cfg.SPI1ResourceReport, "freeze manifest": cfg.SPI1Freeze,
			"discovery report": cfg.SPI1DiscoveryReport, "freeze output": cfg.SPI1FreezeOutput,
			"training baseline artifact":  cfg.SPI1TrainingBaseline,
			"training candidate artifact": cfg.SPI1TrainingCandidate,
			"training resource report":    cfg.SPI1TrainingResource,
			"report output":               cfg.SPI1Output,
		}); err != nil {
			return config{}, err
		}
	}
	spI2ReportInputs := []string{cfg.SPI2BaselineArtifact, cfg.SPI2CandidateArtifact, cfg.SPI2ResourceReport}
	spI2TrainingInputs := []string{cfg.SPI2TrainingBaseline, cfg.SPI2TrainingCandidate, cfg.SPI2TrainingResource}
	spI2ReportConfigured := cfg.SPI2Output != "" || cfg.SPI2FreezeOutput != ""
	for _, input := range spI2ReportInputs {
		spI2ReportConfigured = spI2ReportConfigured || input != ""
	}
	if cfg.SPI2Protocol != referencePairProtocolDiscovery && cfg.SPI2Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("sp-i2-protocol must be discovery or confirmation")
	}
	if spI2ReportConfigured {
		for _, input := range spI2ReportInputs {
			if input == "" {
				return config{}, fmt.Errorf("SP-I2 report requires baseline, candidate, and resource artifacts")
			}
		}
		if cfg.SPI2Output == "" {
			return config{}, fmt.Errorf("SP-I2 report requires sp-i2-output")
		}
		if cfg.SPI2Protocol == referencePairProtocolDiscovery && cfg.SPI2FreezeOutput == "" {
			return config{}, fmt.Errorf("SP-I2 discovery requires sp-i2-freeze-output")
		}
		if cfg.SPI2Protocol == referencePairProtocolConfirmation && (cfg.SPI2Freeze == "" || cfg.SPI2DiscoveryReport == "") {
			return config{}, fmt.Errorf("SP-I2 confirmation requires sp-i2-freeze and sp-i2-discovery-report")
		}
	} else if (cfg.SPI2Freeze == "") != (cfg.SPI2DiscoveryReport == "") {
		return config{}, fmt.Errorf("SP-I2 holdout capture requires both sp-i2-freeze and sp-i2-discovery-report")
	}
	spI2TrainingInputCount := 0
	for _, input := range spI2TrainingInputs {
		if input != "" {
			spI2TrainingInputCount++
		}
	}
	if cfg.SPI2Freeze != "" && spI2TrainingInputCount != len(spI2TrainingInputs) {
		return config{}, fmt.Errorf("SP-I2 frozen authorization requires all three exact training evidence artifacts")
	}
	if cfg.SPI2Freeze == "" && spI2TrainingInputCount != 0 {
		return config{}, fmt.Errorf("SP-I2 training evidence inputs require a discovery freeze")
	}
	if !spI2ReportConfigured && cfg.SPI2Freeze != "" && cfg.SPI2Protocol != referencePairProtocolConfirmation {
		return config{}, fmt.Errorf("SP-I2 holdout capture requires the confirmation protocol")
	}
	if cfg.SPI2FreezeOutput != "" && cfg.SPI2Protocol != referencePairProtocolDiscovery {
		return config{}, fmt.Errorf("sp-i2-freeze-output is only valid for discovery")
	}
	if spI2ReportConfigured && cfg.SPI2Protocol == referencePairProtocolDiscovery && (cfg.SPI2Freeze != "" || cfg.SPI2DiscoveryReport != "") {
		return config{}, fmt.Errorf("SP-I2 discovery creates a freeze and cannot consume confirmation inputs")
	}
	if spI2ReportConfigured && (cfg.OutputJSONL != "" || rawCases != "" || rawDatasets != "" || rawCategories != "" || rawTags != "") {
		return config{}, fmt.Errorf("SP-I2 report mode cannot also execute or select benchmark cases")
	}
	if spI2ReportConfigured {
		if err := validateDistinctSPI2Paths(map[string]string{
			"baseline artifact": cfg.SPI2BaselineArtifact, "candidate artifact": cfg.SPI2CandidateArtifact,
			"resource report": cfg.SPI2ResourceReport, "freeze manifest": cfg.SPI2Freeze,
			"discovery report": cfg.SPI2DiscoveryReport, "freeze output": cfg.SPI2FreezeOutput,
			"training baseline artifact":  cfg.SPI2TrainingBaseline,
			"training candidate artifact": cfg.SPI2TrainingCandidate,
			"training resource report":    cfg.SPI2TrainingResource,
			"report output":               cfg.SPI2Output,
		}); err != nil {
			return config{}, err
		}
	}
	modeCount := 0
	if cfg.GateBaseline != "" {
		modeCount++
	}
	if len(cfg.AAArtifacts) != 0 {
		modeCount++
	}
	if cfg.ConfirmLeft != "" {
		modeCount++
	}
	if cfg.ReferenceClosureArtifact != "" {
		modeCount++
	}
	if cfg.ReferencePairArtifact != "" {
		modeCount++
	}
	if cfg.ReferenceTournamentArtifact != "" {
		modeCount++
	}
	if cfg.ResourceArtifact != "" {
		modeCount++
	}
	if cfg.BackendDeltaArtifact != "" {
		modeCount++
	}
	if cfg.BundleVerify != "" {
		modeCount++
	}
	if cfg.PromotionManifest != "" {
		modeCount++
	}
	if promotionBindConfigured {
		modeCount++
	}
	if operationalGateConfigured {
		modeCount++
	}
	if cfg.ExpandIntoArtifact != "" {
		modeCount++
	}
	if orientationConfigured {
		modeCount++
	}
	if orientationV2Configured {
		modeCount++
	}
	if suffixGuardReportConfigured {
		modeCount++
	}
	if spI1ReportConfigured {
		modeCount++
	}
	if spI2ReportConfigured {
		modeCount++
	}
	if !spI1ReportConfigured && cfg.SPI1Freeze != "" && modeCount > 0 {
		return config{}, fmt.Errorf("SP-I1 holdout authorization cannot be combined with a standalone report mode")
	}
	if !spI2ReportConfigured && cfg.SPI2Freeze != "" && modeCount > 0 {
		return config{}, fmt.Errorf("SP-I2 holdout authorization cannot be combined with a standalone report mode")
	}
	if modeCount > 1 {
		return config{}, fmt.Errorf("performance-gate, A/A, paired-confirmation, reference-closure, reference-pair, reference-tournament, resource-gate, backend-delta, bundle-verify, promotion-manifest, promotion-bind, operational-gate, ExpandInto-report, orientation-report, orientation-v2-report, suffix-guard-report, SP-I1-report, and SP-I2-report modes are mutually exclusive")
	}
	if modeCount > 0 && cfg.BundleDir != "" {
		return config{}, fmt.Errorf("standalone report modes and bundle-dir are mutually exclusive")
	}
	if len(cfg.AAArtifacts) != 0 && cfg.GateBaseline != "" {
		return config{}, fmt.Errorf("aa-artifact and performance-gate mode are mutually exclusive")
	}
	if cfg.Confidence <= 0 || cfg.Confidence >= 1 || math.IsNaN(cfg.Confidence) || math.IsInf(cfg.Confidence, 0) {
		return config{}, fmt.Errorf("confidence-level must be between 0 and 1")
	}
	if spI1ReportConfigured && (cfg.GateSeed != 1 || cfg.Confidence != defaultConfidenceLevel) {
		return config{}, fmt.Errorf("SP-I1 reporting requires frozen seed 1 and confidence %.4f", defaultConfidenceLevel)
	}
	if spI2ReportConfigured && (cfg.GateSeed != 1 || cfg.Confidence != defaultConfidenceLevel) {
		return config{}, fmt.Errorf("SP-I2 reporting requires frozen seed 1 and confidence %.4f", defaultConfidenceLevel)
	}
	if cfg.Regression < 0 {
		return config{}, fmt.Errorf("regression-threshold must not be negative")
	}
	if cfg.MaterialityRatio <= 0 || cfg.MaterialityRatio >= 1 {
		return config{}, fmt.Errorf("materiality-ratio must be between 0 and 1")
	}
	if cfg.MaterialityAbsolute < 0 {
		return config{}, fmt.Errorf("materiality-absolute must not be negative")
	}
	if cfg.AppendJSONL && cfg.OutputJSONL == "" {
		return config{}, fmt.Errorf("append-jsonl requires jsonl-output")
	}
	if cfg.ResourceOutput != "" && cfg.ResourceArtifact == "" {
		return config{}, fmt.Errorf("resource-output requires resource-artifact")
	}
	if cfg.BackendDeltaOutput != "" && cfg.BackendDeltaArtifact == "" {
		return config{}, fmt.Errorf("backend-delta-output requires backend-delta-artifact")
	}
	if cfg.DiscoverySampleFloor < 1 {
		return config{}, fmt.Errorf("discovery-sample-floor must be at least 1")
	}
	for _, raw := range strings.Split(rawTimeoutClasses, ",") {
		if raw = strings.TrimSpace(raw); raw != "" {
			timeout, err := time.ParseDuration(raw)
			if err != nil || timeout <= 0 {
				return config{}, fmt.Errorf("timeout classes must be positive durations, got %q", raw)
			}
			if len(cfg.TimeoutClasses) > 0 && timeout <= cfg.TimeoutClasses[len(cfg.TimeoutClasses)-1] {
				return config{}, fmt.Errorf("timeout classes must be strictly increasing")
			}
			cfg.TimeoutClasses = append(cfg.TimeoutClasses, timeout)
		}
	}
	for _, target := range strings.Split(rawGateTargets, ",") {
		if target = strings.TrimSpace(target); target != "" {
			cfg.GateTargets = append(cfg.GateTargets, target)
		}
	}
	if cfg.Cases, err = parseUniqueCSV("case", rawCases); err != nil {
		return config{}, err
	}
	if cfg.Datasets, err = parseUniqueCSV("dataset", rawDatasets); err != nil {
		return config{}, err
	}
	if cfg.Categories, err = parseUniqueCSV("category", rawCategories); err != nil {
		return config{}, err
	}
	if cfg.Tags, err = parseUniqueCSV("tag", rawTags); err != nil {
		return config{}, err
	}
	if cfg.ConfirmCases, err = parseUniqueCSV("confirmation case", rawConfirmCases); err != nil {
		return config{}, err
	}
	if cfg.PostgresReferenceArms, err = parseUniqueCSV("PostgreSQL reference arm", rawReferenceArms); err != nil {
		return config{}, err
	}
	if cfg.ReferenceTournamentArms, err = parseUniqueCSV("reference tournament arm", rawTournamentArms); err != nil {
		return config{}, err
	}
	if cfg.ReferenceTournamentArtifact != "" && len(cfg.ReferenceTournamentArms) != 3 && len(cfg.ReferenceTournamentArms) != 5 {
		return config{}, fmt.Errorf("reference tournament requires exactly 3 or 5 arms")
	}
	for _, arm := range cfg.ReferenceTournamentArms {
		if !validPostgresReferenceArm(arm) {
			return config{}, fmt.Errorf("unknown PostgreSQL reference tournament arm %q", arm)
		}
	}
	for _, arm := range cfg.PostgresReferenceArms {
		if !validPostgresReferenceArm(arm) {
			return config{}, fmt.Errorf("unknown PostgreSQL reference arm %q", arm)
		}
	}
	if cfg.ReferenceClosureArtifact != "" && !validPostgresReferenceArm(cfg.ReferenceClosureArm) {
		return config{}, fmt.Errorf("unknown PostgreSQL reference closure arm %q", cfg.ReferenceClosureArm)
	}
	if len(cfg.PostgresReferenceArms) > 0 {
		cfg.PostgresReferences = true
	}
	if cfg.PostgresForceShortest != "" && !validForcedShortestPathExecutor(cfg.PostgresForceShortest) {
		return config{}, fmt.Errorf("unsupported PostgreSQL forced shortest executor %q", cfg.PostgresForceShortest)
	}
	if cfg.PostgresForceExpansion != "" && cfg.PostgresForceExpansion != "EXPANSION-SUFFIX-SEEDED-REVERSE" && cfg.PostgresForceExpansion != "EXPANSION-ENDPOINT-SEEDED-REVERSE" {
		return config{}, fmt.Errorf("unsupported PostgreSQL forced expansion search %q", cfg.PostgresForceExpansion)
	}
	if cfg.PostgresForceShortest != "" && cfg.PostgresForceExpansion != "" {
		return config{}, fmt.Errorf("PostgreSQL shortest and expansion search forces are mutually exclusive")
	}
	orientationMode := cfg.PostgresExpansionOrientationShadow || cfg.PostgresExpansionOrientationTournament
	if cfg.PostgresExpansionOrientationShadow && cfg.PostgresExpansionOrientationTournament {
		return config{}, fmt.Errorf("PostgreSQL expansion orientation shadow and tournament modes are mutually exclusive")
	}
	if orientationMode && (cfg.PostgresForceShortest != "" || cfg.PostgresForceExpansion != "") {
		return config{}, fmt.Errorf("PostgreSQL expansion orientation and forced traversal selectors are mutually exclusive")
	}
	if cfg.PostgresExpansionSuffixReverseGuard && (orientationMode || cfg.PostgresForceShortest != "" || cfg.PostgresForceExpansion != "" || cfg.PostgresProductionManifest != "") {
		return config{}, fmt.Errorf("PostgreSQL suffix-reverse guard is mutually exclusive with orientation, forced traversal, and production-manifest selectors")
	}
	if !cfg.PostgresExpansionSuffixReverseGuard && (cfg.PostgresSuffixGuardSuffixLimit != 0 || cfg.PostgresSuffixGuardStateLimit != 0) {
		return config{}, fmt.Errorf("PostgreSQL suffix-guard cap overrides require postgres-expansion-suffix-reverse-guard")
	}
	if cfg.PostgresSuffixGuardSuffixLimit < 0 || cfg.PostgresSuffixGuardStateLimit < 0 {
		return config{}, fmt.Errorf("PostgreSQL suffix-guard cap overrides must not be negative")
	}
	if cfg.PostgresExpansionSuffixReverseGuard && !cfg.PostgresRepeatableRead {
		return config{}, fmt.Errorf("PostgreSQL suffix-reverse guard measurements require postgres-repeatable-read")
	}
	if cfg.PostgresExpansionSuffixReverseGuard && cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryDiagnostic {
		return config{}, fmt.Errorf("PostgreSQL suffix-reverse guard measurements require diagnostic traversal telemetry")
	}
	if cfg.PostgresExpansionOrientationPolicy != "" && !orientationMode {
		return config{}, fmt.Errorf("PostgreSQL expansion orientation policy requires shadow or tournament mode")
	}
	if cfg.PostgresExpansionOrientationPolicy != "" &&
		cfg.PostgresExpansionOrientationPolicy != string(optimize.ExpansionSearchPolicyOrientationProbeV1) &&
		cfg.PostgresExpansionOrientationPolicy != string(optimize.ExpansionSearchPolicyOrientationProbeV2) {
		return config{}, fmt.Errorf("unsupported PostgreSQL expansion orientation policy %q", cfg.PostgresExpansionOrientationPolicy)
	}
	if (cfg.PostgresExpansionOrientationTournament || cfg.PostgresExpansionOrientationPolicy == string(optimize.ExpansionSearchPolicyOrientationProbeV2)) && !cfg.PostgresRepeatableRead {
		return config{}, fmt.Errorf("guarded and orientation-probe-v2 measurements require postgres-repeatable-read")
	}
	if cfg.PostgresExpansionOrientationPolicy == string(optimize.ExpansionSearchPolicyOrientationProbeV2) && cfg.PostgresTraversalTelemetry == postgresTraversalTelemetryOff {
		return config{}, fmt.Errorf("orientation-probe-v2 measurements require PostgreSQL traversal telemetry")
	}
	if cfg.PostgresProductionManifest != "" && (cfg.PostgresForceShortest != "" || cfg.PostgresForceExpansion != "" || orientationMode) {
		return config{}, fmt.Errorf("PostgreSQL production manifest is mutually exclusive with forced and shadow translation modes")
	}
	if cfg.PostgresProductionManifest != "" && cfg.PostgresRepeatableRead {
		return config{}, fmt.Errorf("PostgreSQL production manifest already implies Repeatable Read")
	}
	if cfg.GateBaseline != "" && !cfg.DiagnosticGate && cfg.GateAA == "" {
		return config{}, fmt.Errorf("complete performance gate requires gate-aa host calibration evidence")
	}

	modes, err := parseExecutionModes(rawModes)
	if err != nil {
		return config{}, err
	}
	cfg.Modes = modes
	if cfg.ExistingGraph {
		if cfg.AnchorManifest == "" {
			return config{}, fmt.Errorf("existing-graph mode requires anchor-manifest")
		}
		if len(cfg.Modes) != 1 || cfg.Modes[0] != ModePostgresSQL {
			return config{}, fmt.Errorf("existing-graph mode currently requires only postgres_sql mode")
		}
		if cfg.Resume && cfg.Checkpoint == "" {
			return config{}, fmt.Errorf("resume requires checkpoint")
		}
		if len(cfg.TimeoutClasses) > 0 && !cfg.Discovery {
			return config{}, fmt.Errorf("timeout-classes require discovery mode")
		}
	} else if cfg.Resume || cfg.AnchorManifest != "" || cfg.Checkpoint != "" || cfg.Progress != "" || cfg.Discovery || len(cfg.TimeoutClasses) > 0 {
		return config{}, fmt.Errorf("existing-graph workflow flags require existing-graph mode")
	}
	if !spI1ReportConfigured && cfg.SPI1Freeze != "" {
		if err := validateSPI1HoldoutCaptureConfig(cfg); err != nil {
			return config{}, err
		}
	}
	if !spI2ReportConfigured && cfg.SPI2Freeze != "" {
		if err := validateSPI2HoldoutCaptureConfig(cfg); err != nil {
			return config{}, err
		}
	}

	return cfg, nil
}

// validForcedShortestPathExecutor reports whether graphbench recognizes a
// production executor or a declared tournament identity.
func validForcedShortestPathExecutor(executor string) bool {
	switch executor {
	case "SP-S0",
		"SP-S0-DIRECT",
		"SP-S3-U-D",
		"SP-S3-U-E+MAT-M0",
		"SP-S4-C-D",
		"SP-S4-C-WE+MAT-M0",
		"SP-I1-C-D",
		"SP-I2-C-D",
		"SP-I1-U-E+MAT-M0",
		"SP-I1-C-WE+MAT-M0",
		"SP-B1-C-ALT-NODE-D",
		"SP-B1-C-ALT-NODE-WE+MAT-M0",
		"SP-B2-C-MIN-LEVEL-D",
		"SP-B2-C-MIN-LEVEL-WE+MAT-M0",
		"ASP-A1-DAG",
		"ASP-I1-U-DAG+MAT-M0",
		"ASP-B1-DAG-ALT-NODE",
		"ASP-B2-DAG-MIN-LEVEL":
		return true
	default:
		return false
	}
}

// parseCaptureBundleEvidenceInputs parses repeatable name=path bundle evidence
// declarations while keeping host paths out of serialized evidence identities.
func parseCaptureBundleEvidenceInputs(rawValues []string) ([]CaptureBundleEvidenceInput, error) {
	inputs := make([]CaptureBundleEvidenceInput, 0, len(rawValues))
	seen := map[string]struct{}{}
	for _, raw := range rawValues {
		name, path, found := strings.Cut(raw, "=")
		name = strings.TrimSpace(name)
		path = strings.TrimSpace(path)
		if !found || !validBundleEvidenceName(name) || path == "" {
			return nil, fmt.Errorf("bundle-evidence must be a valid name=path declaration, got %q", raw)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("duplicate bundle-evidence name %q", name)
		}
		seen[name] = struct{}{}
		inputs = append(inputs, CaptureBundleEvidenceInput{
			Name: name,
			Path: path,
		})
	}
	return inputs, nil
}

// parseUniqueCSV splits comma-separated selectors, rejecting duplicates and empty elements.
func parseUniqueCSV(kind, raw string) ([]string, error) {
	var values []string
	seen := map[string]struct{}{}
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("duplicate %s selector %q", kind, value)
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values, nil
}

// selectedCorpusContainsTag selects ed corpus contains tag.
func selectedCorpusContainsTag(corpus ScaleCorpus, tag string) bool {
	for _, testCase := range corpus.Cases {
		if slices.Contains(testCase.Tags, tag) {
			return true
		}
	}
	return false
}

// parseExecutionModes parses a comma-separated mode list and rejects duplicates or unsupported values.
func parseExecutionModes(raw string) ([]ExecutionMode, error) {
	var (
		modes []ExecutionMode
		seen  = map[ExecutionMode]struct{}{}
	)

	for _, part := range strings.Split(raw, ",") {
		mode, err := parseExecutionMode(part)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[mode]; duplicate {
			continue
		}

		seen[mode] = struct{}{}
		modes = append(modes, mode)
	}
	if len(modes) == 0 {
		return nil, fmt.Errorf("at least one execution mode is required")
	}

	return modes, nil
}

// fatal logs a formatted fatal error and terminates the command.
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// main runs the graphbench command.
func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		fatal("%v", err)
	}
	if cfg.BundleVerify != "" {
		passed, err := createCaptureBundleVerification(cfg.BundleVerify, cfg.BundleVerifyOutput, cfg.BundleRequireClean)
		if err != nil {
			fatal("verify capture bundle: %v", err)
		}
		if !passed {
			fatal("capture bundle verification failed")
		}
		return
	}
	if cfg.PromotionManifest != "" {
		passed, err := writePromotionManifestVerification(cfg.PromotionManifest, cfg.PromotionManifestOutput)
		if err != nil {
			fatal("verify promotion manifest: %v", err)
		}
		if !passed {
			fatal("promotion manifest verification failed")
		}
		return
	}
	if cfg.PromotionBindManifest != "" {
		if err := bindPromotionEvidenceReport(cfg.PromotionBindManifest, cfg.PromotionBindRole, cfg.PromotionBindInput, cfg.PromotionBindOutput); err != nil {
			fatal("bind promotion evidence report: %v", err)
		}
		return
	}
	if cfg.OperationalGateInput != "" {
		passed, err := createOperationalGateReport(cfg.OperationalGateInput, cfg.OperationalGateOutput)
		if err != nil {
			fatal("calculate operational gate: %v", err)
		}
		if !passed {
			fatal("operational gate failed")
		}
		return
	}
	if cfg.OrientationShadowArtifact != "" {
		passed, err := createOrientationSelectorReport(
			cfg.OrientationShadowArtifact,
			cfg.OrientationIncumbentArtifact,
			cfg.OrientationReverseArtifact,
			cfg.OrientationAA,
			cfg.OrientationOutput,
			OrientationSelectorReportOptions{
				Seed:       cfg.GateSeed,
				Confidence: cfg.Confidence,
				Protocol:   cfg.OrientationProtocol,
			},
		)
		if err != nil {
			fatal("calculate orientation selector report: %v", err)
		}
		if cfg.OrientationProtocol == referencePairProtocolConfirmation && !passed {
			fatal("orientation selector qualification failed")
		}
		return
	}
	if cfg.OrientationV2ShadowArtifact != "" {
		passed, err := createOrientationSelectorV2Report(
			cfg.OrientationV2ShadowArtifact,
			cfg.OrientationV2IncumbentArtifact,
			cfg.OrientationV2ReverseArtifact,
			cfg.OrientationV2GuardedArtifact,
			cfg.OrientationV2AA,
			cfg.OrientationV2Freeze,
			cfg.OrientationV2DiscoveryReport,
			cfg.OrientationV2FreezeOutput,
			cfg.OrientationV2Output,
			OrientationSelectorV2ReportOptions{
				Seed:       cfg.GateSeed,
				Confidence: cfg.Confidence,
				Protocol:   cfg.OrientationV2Protocol,
			},
		)
		if err != nil {
			fatal("calculate orientation-v2 selector report: %v", err)
		}
		if cfg.OrientationV2Protocol == referencePairProtocolConfirmation && !passed {
			fatal("orientation-v2 selector qualification failed")
		}
		return
	}
	if cfg.SuffixGuardIncumbentArtifact != "" {
		passed, err := createSuffixReverseGuardFeasibilityReport(
			cfg.SuffixGuardIncumbentArtifact,
			cfg.SuffixGuardReverseArtifact,
			cfg.SuffixGuardGuardedArtifact,
			cfg.SuffixGuardAA,
			cfg.SuffixGuardOutput,
			SuffixReverseGuardFeasibilityOptions{
				Seed:           cfg.GateSeed,
				Confidence:     cfg.Confidence,
				BootstrapCount: defaultBootstrapCount,
			},
		)
		if err != nil {
			fatal("calculate suffix-reverse guard feasibility: %v", err)
		}
		if !passed {
			fatal("suffix-reverse guard feasibility stop gate failed")
		}
		return
	}
	if cfg.SPI1BaselineArtifact != "" {
		passed, err := createSPI1QualificationReport(
			cfg.SPI1BaselineArtifact,
			cfg.SPI1CandidateArtifact,
			cfg.SPI1ResourceReport,
			cfg.SPI1Freeze,
			cfg.SPI1DiscoveryReport,
			cfg.SPI1FreezeOutput,
			cfg.SPI1Output,
			SPI1QualificationOptions{
				Seed:                  cfg.GateSeed,
				Confidence:            cfg.Confidence,
				Protocol:              cfg.SPI1Protocol,
				TrainingBaselinePath:  cfg.SPI1TrainingBaseline,
				TrainingCandidatePath: cfg.SPI1TrainingCandidate,
				TrainingResourcePath:  cfg.SPI1TrainingResource,
			},
		)
		if err != nil {
			fatal("calculate staged SP-I1 qualification: %v", err)
		}
		if cfg.SPI1Protocol == referencePairProtocolConfirmation && !passed {
			fatal("staged SP-I1 qualification failed")
		}
		return
	}
	if cfg.SPI2BaselineArtifact != "" {
		passed, err := createSPI2QualificationReport(
			cfg.SPI2BaselineArtifact,
			cfg.SPI2CandidateArtifact,
			cfg.SPI2ResourceReport,
			cfg.SPI2Freeze,
			cfg.SPI2DiscoveryReport,
			cfg.SPI2FreezeOutput,
			cfg.SPI2Output,
			SPI2QualificationOptions{
				Seed:                  cfg.GateSeed,
				Confidence:            cfg.Confidence,
				Protocol:              cfg.SPI2Protocol,
				TrainingBaselinePath:  cfg.SPI2TrainingBaseline,
				TrainingCandidatePath: cfg.SPI2TrainingCandidate,
				TrainingResourcePath:  cfg.SPI2TrainingResource,
			},
		)
		if err != nil {
			fatal("calculate staged SP-I2 qualification: %v", err)
		}
		if cfg.SPI2Protocol == referencePairProtocolConfirmation && !passed {
			fatal("staged SP-I2 qualification failed")
		}
		return
	}
	if cfg.ExpandIntoArtifact != "" {
		if err := createExpandIntoStudyReport(cfg.ExpandIntoArtifact, cfg.ExpandIntoOutput, ExpandIntoStudyOptions{
			Seed:                cfg.GateSeed,
			Confidence:          cfg.Confidence,
			Protocol:            cfg.ExpandIntoProtocol,
			MaterialityRatio:    cfg.MaterialityRatio,
			MaterialityAbsolute: cfg.MaterialityAbsolute,
			P95RatioLimit:       1.05,
		}); err != nil {
			fatal("calculate ExpandInto study: %v", err)
		}
		return
	}
	if cfg.GateBaseline != "" {
		corpus, err := loadScaleCorpus(cfg.CorpusRoot)
		if err != nil {
			fatal("load gate corpus declaration: %v", err)
		}
		selected, _, err := selectRunnableScaleCorpus(corpus, CorpusSelectors{
			Cases:      cfg.Cases,
			Datasets:   cfg.Datasets,
			Categories: cfg.Categories,
			Tags:       cfg.Tags,
		})
		if err != nil {
			fatal("select gate corpus: %v", err)
		}
		passed, err := comparePerformanceArtifacts(cfg.GateBaseline, cfg.GateCandidate, cfg.GateOutput, PerfGateOptions{
			Seed:                cfg.GateSeed,
			Confidence:          cfg.Confidence,
			RegressionThreshold: cfg.Regression,
			DeclaredBackends:    selected.DeclaredBackends(),
			TargetNames:         cfg.GateTargets,
			MaterialityRatio:    cfg.MaterialityRatio,
			MaterialityAbsolute: cfg.MaterialityAbsolute,
			DiagnosticMode:      cfg.DiagnosticGate,
			AAReportPath:        cfg.GateAA,
		})
		if err != nil {
			fatal("compare performance artifacts: %v", err)
		}
		if !passed {
			fatal("performance gate failed")
		}
		return
	}
	if len(cfg.AAArtifacts) != 0 {
		if err := createAAResolutionReport(cfg.AAArtifacts, cfg.AAOutput, PerfGateOptions{
			Seed:       cfg.GateSeed,
			Confidence: cfg.Confidence,
		}); err != nil {
			fatal("calculate A/A measurement resolution: %v", err)
		}
		return
	}
	if cfg.ConfirmLeft != "" {
		if err := createConfirmationReport(cfg.ConfirmLeft, cfg.ConfirmRight, cfg.ConfirmAA, cfg.ConfirmOutput, ConfirmationOptions{
			Seed:       cfg.GateSeed,
			Confidence: cfg.Confidence,
			CaseNames:  cfg.ConfirmCases,
		}); err != nil {
			fatal("calculate paired confirmation: %v", err)
		}
		return
	}
	if cfg.ReferenceClosureArtifact != "" {
		passed, err := createReferenceClosureReport(cfg.ReferenceClosureArtifact, cfg.ReferenceClosureOutput, ReferenceClosureOptions{
			Seed:               cfg.GateSeed,
			Confidence:         cfg.Confidence,
			ReferenceName:      cfg.ReferenceClosureArm,
			RatioUpperLimit:    1.10,
			AbsoluteResolution: cfg.MaterialityAbsolute,
		})
		if err != nil {
			fatal("calculate production/reference closure: %v", err)
		}
		if !passed {
			fatal("production/reference closure failed")
		}
		return
	}
	if cfg.ReferencePairArtifact != "" {
		if err := createReferencePairReport(cfg.ReferencePairArtifact, cfg.ReferencePairOutput, ReferencePairOptions{
			Seed:          cfg.GateSeed,
			Confidence:    cfg.Confidence,
			BaselineName:  cfg.ReferencePairBaseline,
			CandidateName: cfg.ReferencePairCandidate,
			Protocol:      cfg.ReferencePairProtocol,
		}); err != nil {
			fatal("calculate matched reference pair: %v", err)
		}
		return
	}
	if cfg.ReferenceTournamentArtifact != "" {
		passed, err := createReferenceTournamentReport(cfg.ReferenceTournamentArtifact, cfg.ReferenceTournamentOutput, ReferenceTournamentOptions{
			Seed:                cfg.GateSeed,
			Confidence:          cfg.Confidence,
			MaterialityRatio:    cfg.MaterialityRatio,
			MaterialityAbsolute: cfg.MaterialityAbsolute,
			P95RatioLimit:       1.05,
			Arms:                cfg.ReferenceTournamentArms,
			Protocol:            cfg.ReferenceTournamentProtocol,
		})
		if err != nil {
			fatal("calculate reference tournament: %v", err)
		}
		if cfg.ReferenceTournamentProtocol == referencePairProtocolConfirmation && !passed {
			fatal("reference tournament qualification failed")
		}
		return
	}
	if cfg.ResourceArtifact != "" {
		passed, err := createResourceGateReport(cfg.ResourceArtifact, cfg.ResourceOutput)
		if err != nil {
			fatal("calculate state/resource gate: %v", err)
		}
		if !passed {
			fatal("state/resource gate failed")
		}
		return
	}
	if cfg.BackendDeltaArtifact != "" {
		if err := createBackendDeltaReport(cfg.BackendDeltaArtifact, cfg.BackendDeltaOutput); err != nil {
			fatal("calculate descriptive backend deltas: %v", err)
		}
		return
	}
	fullCorpus, err := loadScaleCorpus(cfg.CorpusRoot)
	if err != nil {
		fatal("load corpus: %v", err)
	}
	corpus, selection, err := selectRunnableScaleCorpusWithSPI2Protection(fullCorpus, CorpusSelectors{
		Cases:      cfg.Cases,
		Datasets:   cfg.Datasets,
		Categories: cfg.Categories,
		Tags:       cfg.Tags,
	})
	if err != nil {
		fatal("select corpus: %v", err)
	}
	if selectedCorpusContainsTag(corpus, spI1HoldoutTag) || selectedCorpusContainsSPI1Holdout(corpus) || cfg.SPI1Freeze != "" {
		if cfg.SPI1Freeze == "" || cfg.SPI1DiscoveryReport == "" {
			fatal("SP-I1 holdout capture requires sp-i1-freeze and sp-i1-discovery-report before database setup")
		}
		if err := validateSPI1HoldoutCapture(
			corpus, cfg.SPI1Freeze, cfg.SPI1DiscoveryReport,
			cfg.SPI1TrainingBaseline, cfg.SPI1TrainingCandidate, cfg.SPI1TrainingResource,
		); err != nil {
			fatal("authorize SP-I1 holdout capture: %v", err)
		}
	}
	if selectedCorpusContainsTag(corpus, spI2HoldoutTag) || selectedCorpusContainsSPI2Holdout(corpus) || cfg.SPI2Freeze != "" {
		if cfg.SPI2Freeze == "" || cfg.SPI2DiscoveryReport == "" {
			fatal("SP-I2 holdout capture requires sp-i2-freeze and sp-i2-discovery-report before database setup")
		}
		if err := validateSPI2HoldoutCapture(
			corpus, cfg.SPI2Freeze, cfg.SPI2DiscoveryReport,
			cfg.SPI2TrainingBaseline, cfg.SPI2TrainingCandidate, cfg.SPI2TrainingResource,
		); err != nil {
			fatal("authorize SP-I2 holdout capture: %v", err)
		}
	}

	if !cfg.ExistingGraph {
		for _, mode := range cfg.Modes {
			var connection string
			switch mode {
			case ModePostgresSQL:
				connection = cfg.PGConnection
			case ModeNeo4j:
				connection = cfg.Neo4jConnection
			default:
				continue
			}
			if connection == "" {
				connection = cfg.Connection
			}
			if connection == "" {
				continue
			}
			if err := databaseguard.ValidateEnvironment(connection); err != nil {
				fatal("refuse destructive GraphBench target: %v", err)
			}
		}

		runLock, err := acquireDestructiveRunLock(cfg.DestructiveLock)
		if err != nil {
			fatal("acquire destructive run lock: %v", err)
		}
		defer func() {
			if err := runLock.Close(); err != nil {
				fatal("release destructive run lock: %v", err)
			}
		}()
	}

	var (
		ctx              = context.Background()
		records          []CaseResult
		existingManifest ExistingGraphAnchorManifest
		startedAt        = time.Now()
	)
	checkpointCorpusHash := corpusIdentity(corpus)
	metadata := testutil.ResolveBaselineMetadata(cfg.DAWGSVersion)
	environment := resolveRunEnvironment(cfg, os.Args, selection, startedAt, startedAt)
	checkpointRunHash := runConfigurationIdentity(cfg, environment)
	environment.CorpusSHA256 = checkpointCorpusHash
	environment.RunIdentitySHA256 = checkpointRunHash
	if cfg.ExistingGraph {
		existingManifest, err = loadExistingGraphAnchorManifest(cfg.AnchorManifest)
		if err != nil {
			fatal("load existing-graph anchor manifest: %v", err)
		}
		if err := validateExistingGraphCorpus(corpus, existingManifest); err != nil {
			fatal("validate existing-graph corpus: %v", err)
		}
		if cfg.Resume {
			records, err = readExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, checkpointRunHash)
			if err != nil {
				fatal("resume existing-graph checkpoint: %v", err)
			}
			for _, record := range records {
				if record.Environment != nil && record.Environment.RunUUID != "" {
					environment.RunUUID = record.Environment.RunUUID
					break
				}
			}
		}
	}

	for _, mode := range modesForRound(cfg.Modes, cfg.Round) {
		switch mode {
		case ModePostgresSQL:
			pgConnection := cfg.PGConnection
			if pgConnection == "" {
				pgConnection = cfg.Connection
			}
			if pgConnection == "" {
				fatal("postgres_sql mode requires -pg-connection, -connection, PG_CONNECTION_STRING, or CONNECTION_STRING")
			}

			var existingOptions *existingGraphRunnerOptions
			if cfg.ExistingGraph {
				completed := map[string]string{}
				for _, record := range records {
					completed[existingGraphCaseKey(record.ExecutionMode, ScaleCase{
						Dataset: record.Dataset,
						Name:    record.Name,
					})] = record.WorkloadSHA256
				}
				existingOptions = &existingGraphRunnerOptions{
					Manifest:       existingManifest,
					ProgressPath:   cfg.Progress,
					Discovery:      cfg.Discovery,
					TimeoutClasses: append([]time.Duration(nil), cfg.TimeoutClasses...),
					SampleFloor:    cfg.DiscoverySampleFloor,
					Completed:      completed,
					OnRecord: func(record CaseResult) error {
						setCaseRunMetadata(&record, metadata, environment)
						records = append(records, record)
						return writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, checkpointRunHash, records)
					},
					OnComplete: func(postNodes, postEdges int64) error {
						for idx := range records {
							if records[idx].ExistingGraph != nil {
								records[idx].ExistingGraph.PostNodeCount = postNodes
								records[idx].ExistingGraph.PostEdgeCount = postEdges
							}
						}
						return writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, checkpointRunHash, records)
					},
				}
			}
			runner, err := newPostgresSQLRunnerWithExistingGraph(ctx, cfg.DatasetDir, pgConnection, corpus, cfg.PoolSize, cfg.Round, cfg.Concurrency, cfg.PostgresReferences, cfg.PostgresReferenceArms, cfg.PostgresForceShortest, cfg.PostgresForceExpansion, existingOptions)
			if err != nil {
				fatal("open postgres_sql runner: %v", err)
			}
			runner.traversalTelemetry = cfg.PostgresTraversalTelemetry
			runner.repeatableRead = cfg.PostgresRepeatableRead
			runner.toolOptions.EnableExpansionOrientationShadow = cfg.PostgresExpansionOrientationShadow
			runner.toolOptions.EnableExpansionOrientationTournament = cfg.PostgresExpansionOrientationTournament
			runner.toolOptions.ExpansionOrientationPolicy = optimize.ExpansionSearchPolicy(cfg.PostgresExpansionOrientationPolicy)
			runner.toolOptions.EnableExpansionSuffixReverseGuard = cfg.PostgresExpansionSuffixReverseGuard
			runner.toolOptions.SuffixReverseGuardSuffixRowLimit = cfg.PostgresSuffixGuardSuffixLimit
			runner.toolOptions.SuffixReverseGuardStateLimit = cfg.PostgresSuffixGuardStateLimit
			if err := runner.setProductionManifest(cfg.PostgresProductionManifest); err != nil {
				_ = runner.Close(ctx)
				fatal("configure PostgreSQL production candidate: %v", err)
			}
			nextRecords, err := runner.Run(ctx, cfg.WarmupIterations, cfg.Iterations, corpus)
			closeErr := runner.Close(ctx)
			if err != nil {
				fatal("run postgres_sql: %v", err)
			}
			if closeErr != nil {
				fatal("close postgres_sql: %v", closeErr)
			}

			if !cfg.ExistingGraph {
				records = append(records, nextRecords...)
			} else {
				// OnRecord appends each completed record atomically. A resumed run
				// may have no new records, while a complete run refreshes the final
				// before/after cardinality proof below.
				if err := writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, checkpointRunHash, records); err != nil {
					fatal("finalize existing-graph checkpoint: %v", err)
				}
			}

		case ModeNeo4j:
			neo4jConnection := cfg.Neo4jConnection
			if neo4jConnection == "" {
				neo4jConnection = cfg.Connection
			}
			if neo4jConnection == "" {
				fatal("neo4j mode requires -neo4j-connection, -connection, NEO4J_CONNECTION_STRING, or CONNECTION_STRING")
			}

			runner, err := newNeo4jRunner(ctx, cfg.DatasetDir, neo4jConnection, corpus)
			if err != nil {
				fatal("open neo4j runner: %v", err)
			}

			nextRecords, err := runner.Run(ctx, cfg.WarmupIterations, cfg.Iterations, corpus)
			closeErr := runner.Close(ctx)
			if err != nil {
				fatal("run neo4j: %v", err)
			}
			if closeErr != nil {
				fatal("close neo4j: %v", closeErr)
			}

			records = append(records, nextRecords...)

		case ModeLocalTraversal:
			records = append(records, runLocalTraversalPlaceholders(corpus)...)

		default:
			fatal("execution mode %s is not implemented yet", mode)
		}
	}

	if err := validateBackendObservations(records); err != nil {
		fatal("validate backend observations: %v", err)
	}

	environment.EndedAt = time.Now().UTC()
	for idx := range records {
		if records[idx].Environment == nil {
			setCaseRunMetadata(&records[idx], metadata, environment)
		} else if records[idx].Environment.RunUUID == environment.RunUUID {
			records[idx].Environment.EndedAt = environment.EndedAt
		}
	}
	if cfg.ExistingGraph {
		if err := writeExistingGraphCheckpoint(cfg.Checkpoint, existingManifest.Checksum, checkpointCorpusHash, checkpointRunHash, records); err != nil {
			fatal("persist finalized existing-graph checkpoint: %v", err)
		}
	}

	if cfg.Baseline != "" {
		if err := applyBaseline(cfg.Baseline, records); err != nil {
			fatal("compare baseline: %v", err)
		}
	}

	var writeErr error
	if cfg.AppendJSONL {
		writeErr = appendJSONLFile(cfg.OutputJSONL, records)
	} else {
		writeErr = writeJSONLFile(cfg.OutputJSONL, records)
	}
	if writeErr != nil {
		fatal("write JSONL: %v", writeErr)
	}
	if cfg.BundleDir != "" {
		if err := writeCaptureBundleWithEvidence(cfg.BundleDir, corpus, records, environment, cfg.BundleEvidence); err != nil {
			fatal("write capture bundle: %v", err)
		}
	}

	summary := buildSummary(records)
	if cfg.Summary != "" {
		if err := writeMarkdownSummaryFile(cfg.Summary, summary); err != nil {
			fatal("write markdown summary: %v", err)
		}
	}
	if cfg.SummaryJSON != "" {
		if err := writeJSONSummaryFile(cfg.SummaryJSON, summary); err != nil {
			fatal("write JSON summary: %v", err)
		}
	}
}

// modesForRound returns execution modes in alternating round order without mutating the configured slice.
func modesForRound(modes []ExecutionMode, round int) []ExecutionMode {
	ordered := append([]ExecutionMode(nil), modes...)
	if round%2 == 0 {
		slices.Reverse(ordered)
	}
	return ordered
}

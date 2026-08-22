// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"
)

// backendDeltaKey identifies one dataset, case, and round during backend
// matching and repeated-round aggregation.
type backendDeltaKey struct {
	dataset string
	name    string
	round   int
}

// BackendDeltaReport contains descriptive PostgreSQL-to-Neo4j correctness and latency deltas for matched records.
type BackendDeltaReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Notice states that backend deltas are descriptive and not release-gate evidence.
	Notice string `json:"notice"`
	// Cases contains matched PostgreSQL-to-Neo4j comparisons in deterministic report order.
	Cases []BackendDeltaCase `json:"cases"`
	// Outliers aggregates complete repeated-round PostgreSQL losses in descending
	// PostgreSQL-to-Neo4j latency-ratio order. It is a diagnostic work ledger,
	// not a release gate.
	Outliers []BackendDeltaOutlier `json:"outliers,omitempty"`
}

// BackendDeltaOutlier aggregates one matched workload across repeated rounds
// and preserves the runtime facts needed to route optimization work.
type BackendDeltaOutlier struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the workload case.
	Name string `json:"name"`
	// Category identifies the workload family declared by the corpus.
	Category string `json:"category,omitempty"`
	// Rounds counts complete, successful, observation-matching backend pairs.
	Rounds int `json:"rounds"`
	// MedianPostgresOverNeo4j is the median of matched per-round latency ratios.
	MedianPostgresOverNeo4j float64 `json:"median_postgres_over_neo4j"`
	// P95PostgresOverNeo4j is the median of matched per-round P95 ratios.
	P95PostgresOverNeo4j float64 `json:"p95_postgres_over_neo4j,omitempty"`
	// PostgresMedian is the median repeated-round PostgreSQL p50.
	PostgresMedian time.Duration `json:"postgres_median"`
	// Neo4jMedian is the median repeated-round Neo4j p50.
	Neo4jMedian time.Duration `json:"neo4j_median"`
	// RuntimeIdentities lists every observed PostgreSQL runtime identity.
	RuntimeIdentities []string `json:"runtime_identities,omitempty"`
	// AppliedIdentities lists every observed PostgreSQL applied identity.
	AppliedIdentities []string `json:"applied_identities,omitempty"`
	// RuntimeBranches lists every observed PostgreSQL runtime branch.
	RuntimeBranches []string `json:"runtime_branches,omitempty"`
	// FallbackReasons lists every translation fallback reason.
	FallbackReasons []string `json:"fallback_reasons,omitempty"`
	// SQLFingerprints lists the SQL identities observed across repeated rounds.
	SQLFingerprints []string `json:"sql_fingerprints,omitempty"`
	// Direction records the declared traversal direction when available.
	Direction string `json:"direction,omitempty"`
	// ExpectedStateClass records the diagnostic topology classification. It is
	// never suitable as a production selector input by itself.
	ExpectedStateClass string `json:"expected_state_class,omitempty"`
	// ObservationMode records the PostgreSQL executor observation boundary.
	ObservationMode string `json:"observation_mode,omitempty"`
	// SelectorVersion records the PostgreSQL runtime selector version.
	SelectorVersions []string `json:"selector_versions,omitempty"`
}

// BackendDeltaCase compares one matched PostgreSQL and Neo4j case round without assigning release-gate status.
type BackendDeltaCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Round identifies the measurement round.
	Round int `json:"round,omitempty"`
	// Complete reports whether both backend records were present.
	Complete bool `json:"complete"`
	// IncompleteReason identifies the absent backend side.
	IncompleteReason string `json:"incomplete_reason,omitempty"`
	// PostgresStatus supplies the postgres status input to the BackendDeltaCase contract.
	PostgresStatus string `json:"postgres_status"`
	// Neo4jStatus supplies the neo4j status input to the BackendDeltaCase contract.
	Neo4jStatus string `json:"neo4j_status"`
	// PostgresMedian records PostgreSQL median latency for the matched round.
	PostgresMedian time.Duration `json:"postgres_median,omitempty"`
	// PostgresP95 records PostgreSQL P95 latency for the matched round.
	PostgresP95 time.Duration `json:"postgres_p95,omitempty"`
	// Neo4jMedian records Neo4j median latency for the matched round.
	Neo4jMedian time.Duration `json:"neo4j_median,omitempty"`
	// Neo4jP95 records Neo4j P95 latency for the matched round.
	Neo4jP95 time.Duration `json:"neo4j_p95,omitempty"`
	// MedianNeo4jOverPG reports the Neo4j-to-PostgreSQL median latency ratio.
	MedianNeo4jOverPG float64 `json:"median_neo4j_over_postgres,omitempty"`
	// P95Neo4jOverPG reports the Neo4j-to-PostgreSQL P95 latency ratio.
	P95Neo4jOverPG float64 `json:"p95_neo4j_over_postgres,omitempty"`
	// ObservationsComparable reports whether both backend records contain stable observations at the same boundary.
	ObservationsComparable bool `json:"observations_comparable"`
	// ObservationsMatch reports whether comparable backend row counts and normalized observations are equal.
	ObservationsMatch bool `json:"observations_match"`
}

// createBackendDeltaReport matches PostgreSQL and Neo4j records and writes descriptive latency and correctness deltas.
func createBackendDeltaReport(artifact, output string) error {
	records, err := readJSONLFile(artifact)
	if err != nil {
		return err
	}

	postgres, neo4j := map[backendDeltaKey]CaseResult{}, map[backendDeltaKey]CaseResult{}
	for _, record := range records {
		round := 0
		if record.Environment != nil {
			round = record.Environment.Round
		}
		nextKey := backendDeltaKey{
			dataset: record.Dataset,
			name:    record.Name,
			round:   round,
		}

		switch record.ExecutionMode {
		case ModePostgresSQL:
			if _, duplicate := postgres[nextKey]; duplicate {
				return fmt.Errorf("backend-delta artifact has duplicate PostgreSQL record for %s/%s round %d", nextKey.dataset, nextKey.name, nextKey.round)
			}
			postgres[nextKey] = record
		case ModeNeo4j:
			if _, duplicate := neo4j[nextKey]; duplicate {
				return fmt.Errorf("backend-delta artifact has duplicate Neo4j record for %s/%s round %d", nextKey.dataset, nextKey.name, nextKey.round)
			}
			neo4j[nextKey] = record
		}
	}

	report := BackendDeltaReport{
		Version: 2,
		Notice:  "Descriptive only: PostgreSQL release gates compare PostgreSQL predecessors and exact PostgreSQL references, not Neo4j latency.",
	}
	keys := make(map[backendDeltaKey]struct{}, len(postgres)+len(neo4j))
	for nextKey := range postgres {
		keys[nextKey] = struct{}{}
	}
	for nextKey := range neo4j {
		keys[nextKey] = struct{}{}
	}
	for nextKey := range keys {
		pgRecord, pgFound := postgres[nextKey]
		neoRecord, neoFound := neo4j[nextKey]
		observationsComparable := pgRecord.StableObservation && neoRecord.StableObservation
		next := BackendDeltaCase{
			Dataset:                nextKey.dataset,
			Name:                   nextKey.name,
			Round:                  nextKey.round,
			Complete:               pgFound && neoFound,
			PostgresStatus:         pgRecord.Status,
			Neo4jStatus:            neoRecord.Status,
			PostgresMedian:         pgRecord.Stats.Median,
			PostgresP95:            pgRecord.Stats.P95,
			Neo4jMedian:            neoRecord.Stats.Median,
			Neo4jP95:               neoRecord.Stats.P95,
			ObservationsComparable: observationsComparable,
			ObservationsMatch:      observationsComparable && pgRecord.RowCount == neoRecord.RowCount && slices.Equal(pgRecord.ObservedRows, neoRecord.ObservedRows),
		}
		switch {
		case !pgFound:
			next.IncompleteReason = "missing_postgres"
		case !neoFound:
			next.IncompleteReason = "missing_neo4j"
		}

		if next.Complete && next.PostgresMedian > 0 && next.Neo4jMedian > 0 {
			next.MedianNeo4jOverPG = float64(next.Neo4jMedian) / float64(next.PostgresMedian)
		}
		if next.Complete && next.PostgresP95 > 0 && next.Neo4jP95 > 0 {
			next.P95Neo4jOverPG = float64(next.Neo4jP95) / float64(next.PostgresP95)
		}
		report.Cases = append(report.Cases, next)
	}
	report.Outliers = backendDeltaOutliers(postgres, neo4j)

	if len(report.Cases) == 0 {
		return fmt.Errorf("backend-delta artifact has no PostgreSQL or Neo4j cases")
	}
	sort.Slice(report.Cases, func(i, j int) bool {
		if report.Cases[i].Dataset != report.Cases[j].Dataset {
			return report.Cases[i].Dataset < report.Cases[j].Dataset
		}
		if report.Cases[i].Name != report.Cases[j].Name {
			return report.Cases[i].Name < report.Cases[j].Name
		}
		return report.Cases[i].Round < report.Cases[j].Round
	})
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if output == "" {
		_, err = os.Stdout.Write(append(raw, '\n'))
		return err
	}
	return os.WriteFile(output, append(raw, '\n'), 0o644)
}

// backendDeltaOutliers aggregates only complete, successful, semantically
// matching backend pairs. Missing and mismatching pairs remain visible in
// BackendDeltaReport.Cases but cannot be ranked as performance work.
func backendDeltaOutliers(postgres, neo4j map[backendDeltaKey]CaseResult) []BackendDeltaOutlier {
	type aggregate struct {
		outlier       BackendDeltaOutlier
		medianRatios  []float64
		p95Ratios     []float64
		postgresTimes []float64
		neo4jTimes    []float64
		runtime       map[string]struct{}
		applied       map[string]struct{}
		branches      map[string]struct{}
		fallbacks     map[string]struct{}
		fingerprints  map[string]struct{}
		selectors     map[string]struct{}
	}

	aggregates := map[string]*aggregate{}
	for nextKey, pgRecord := range postgres {
		neoRecord, found := neo4j[nextKey]
		if !found || pgRecord.Status != StatusOK || neoRecord.Status != StatusOK ||
			!pgRecord.StableObservation || !neoRecord.StableObservation ||
			pgRecord.RowCount != neoRecord.RowCount || !slices.Equal(pgRecord.ObservedRows, neoRecord.ObservedRows) ||
			pgRecord.Stats.Median <= 0 || neoRecord.Stats.Median <= 0 {
			continue
		}

		caseKey := nextKey.dataset + "\x00" + nextKey.name
		next := aggregates[caseKey]
		if next == nil {
			next = &aggregate{
				outlier: BackendDeltaOutlier{
					Dataset:            nextKey.dataset,
					Name:               nextKey.name,
					Category:           pgRecord.Category,
					Direction:          pgRecord.Shape.Direction,
					ExpectedStateClass: pgRecord.Shape.ExpectedStateClass,
				},
				runtime:      map[string]struct{}{},
				applied:      map[string]struct{}{},
				branches:     map[string]struct{}{},
				fallbacks:    map[string]struct{}{},
				fingerprints: map[string]struct{}{},
				selectors:    map[string]struct{}{},
			}
			aggregates[caseKey] = next
		}
		next.outlier.Rounds++
		next.medianRatios = append(next.medianRatios, float64(pgRecord.Stats.Median)/float64(neoRecord.Stats.Median))
		next.postgresTimes = append(next.postgresTimes, float64(pgRecord.Stats.Median))
		next.neo4jTimes = append(next.neo4jTimes, float64(neoRecord.Stats.Median))
		if pgRecord.Stats.P95 > 0 && neoRecord.Stats.P95 > 0 {
			next.p95Ratios = append(next.p95Ratios, float64(pgRecord.Stats.P95)/float64(neoRecord.Stats.P95))
		}
		addBackendDeltaValue(next.fallbacks, pgRecord.FallbackReason)
		addBackendDeltaValue(next.fingerprints, pgRecord.SQLFingerprint)
		if pgRecord.TraversalTelemetry != nil {
			summary := pgRecord.TraversalTelemetry.Summary
			addBackendDeltaValue(next.runtime, summary.RuntimeIdentity)
			addBackendDeltaValue(next.applied, summary.AppliedIdentity)
			addBackendDeltaValue(next.branches, summary.RuntimeBranch)
			addBackendDeltaValue(next.selectors, summary.SelectorVersion)
			if next.outlier.ObservationMode == "" {
				next.outlier.ObservationMode = summary.ObservationMode
			}
		}
	}

	outliers := make([]BackendDeltaOutlier, 0, len(aggregates))
	for _, next := range aggregates {
		if next.outlier.Rounds < 2 {
			continue
		}
		next.outlier.MedianPostgresOverNeo4j = quantile(next.medianRatios, 0.5)
		if next.outlier.MedianPostgresOverNeo4j <= 1 {
			continue
		}
		next.outlier.P95PostgresOverNeo4j = quantile(next.p95Ratios, 0.5)
		next.outlier.PostgresMedian = time.Duration(quantile(next.postgresTimes, 0.5))
		next.outlier.Neo4jMedian = time.Duration(quantile(next.neo4jTimes, 0.5))
		next.outlier.RuntimeIdentities = sortedBackendDeltaValues(next.runtime)
		next.outlier.AppliedIdentities = sortedBackendDeltaValues(next.applied)
		next.outlier.RuntimeBranches = sortedBackendDeltaValues(next.branches)
		next.outlier.FallbackReasons = sortedBackendDeltaValues(next.fallbacks)
		next.outlier.SQLFingerprints = sortedBackendDeltaValues(next.fingerprints)
		next.outlier.SelectorVersions = sortedBackendDeltaValues(next.selectors)
		outliers = append(outliers, next.outlier)
	}
	sort.Slice(outliers, func(i, j int) bool {
		if outliers[i].MedianPostgresOverNeo4j != outliers[j].MedianPostgresOverNeo4j {
			return outliers[i].MedianPostgresOverNeo4j > outliers[j].MedianPostgresOverNeo4j
		}
		if outliers[i].Dataset != outliers[j].Dataset {
			return outliers[i].Dataset < outliers[j].Dataset
		}
		return outliers[i].Name < outliers[j].Name
	})
	return outliers
}

func addBackendDeltaValue(values map[string]struct{}, value string) {
	if value != "" {
		values[value] = struct{}{}
	}
}

func sortedBackendDeltaValues(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

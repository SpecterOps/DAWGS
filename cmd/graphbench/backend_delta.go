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

// BackendDeltaReport contains descriptive PostgreSQL-to-Neo4j correctness and latency deltas for matched records.
type BackendDeltaReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Notice states that backend deltas are descriptive and not release-gate evidence.
	Notice string `json:"notice"`
	// Cases contains matched PostgreSQL-to-Neo4j comparisons in deterministic report order.
	Cases []BackendDeltaCase `json:"cases"`
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

	// key identifies one dataset, case, and round during backend matching.
	type key struct {
		// dataset names the fixture shared by the matched backend records.
		dataset string
		// name identifies the workload case matched across backends.
		name string
		// round identifies the measurement round used to balance execution order.
		round int
	}

	postgres, neo4j := map[key]CaseResult{}, map[key]CaseResult{}
	for _, record := range records {
		round := 0
		if record.Environment != nil {
			round = record.Environment.Round
		}
		nextKey := key{
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
	keys := make(map[key]struct{}, len(postgres)+len(neo4j))
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

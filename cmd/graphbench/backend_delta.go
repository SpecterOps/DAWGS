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

type BackendDeltaReport struct {
	Version int                `json:"version"`
	Notice  string             `json:"notice"`
	Cases   []BackendDeltaCase `json:"cases"`
}

type BackendDeltaCase struct {
	Dataset                string        `json:"dataset"`
	Name                   string        `json:"name"`
	Round                  int           `json:"round,omitempty"`
	PostgresStatus         string        `json:"postgres_status"`
	Neo4jStatus            string        `json:"neo4j_status"`
	PostgresMedian         time.Duration `json:"postgres_median,omitempty"`
	PostgresP95            time.Duration `json:"postgres_p95,omitempty"`
	Neo4jMedian            time.Duration `json:"neo4j_median,omitempty"`
	Neo4jP95               time.Duration `json:"neo4j_p95,omitempty"`
	MedianNeo4jOverPG      float64       `json:"median_neo4j_over_postgres,omitempty"`
	P95Neo4jOverPG         float64       `json:"p95_neo4j_over_postgres,omitempty"`
	ObservationsComparable bool          `json:"observations_comparable"`
	ObservationsMatch      bool          `json:"observations_match"`
}

func createBackendDeltaReport(artifact, output string) error {
	records, err := readJSONLFile(artifact)
	if err != nil {
		return err
	}

	type key struct {
		dataset string
		name    string
		round   int
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
		Version: 1,
		Notice:  "Descriptive only: PostgreSQL release gates compare PostgreSQL predecessors and exact PostgreSQL references, not Neo4j latency.",
	}
	for nextKey, pgRecord := range postgres {
		neoRecord, found := neo4j[nextKey]
		if !found {
			continue
		}
		observationsComparable := pgRecord.StableObservation && neoRecord.StableObservation
		next := BackendDeltaCase{
			Dataset:                nextKey.dataset,
			Name:                   nextKey.name,
			Round:                  nextKey.round,
			PostgresStatus:         pgRecord.Status,
			Neo4jStatus:            neoRecord.Status,
			PostgresMedian:         pgRecord.Stats.Median,
			PostgresP95:            pgRecord.Stats.P95,
			Neo4jMedian:            neoRecord.Stats.Median,
			Neo4jP95:               neoRecord.Stats.P95,
			ObservationsComparable: observationsComparable,
			ObservationsMatch:      observationsComparable && pgRecord.RowCount == neoRecord.RowCount && slices.Equal(pgRecord.ObservedRows, neoRecord.ObservedRows),
		}
		if next.PostgresMedian > 0 {
			next.MedianNeo4jOverPG = float64(next.Neo4jMedian) / float64(next.PostgresMedian)
		}
		if next.PostgresP95 > 0 {
			next.P95Neo4jOverPG = float64(next.Neo4jP95) / float64(next.PostgresP95)
		}
		report.Cases = append(report.Cases, next)
	}
	if len(report.Cases) == 0 {
		return fmt.Errorf("backend-delta artifact has no matched PostgreSQL/Neo4j cases")
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

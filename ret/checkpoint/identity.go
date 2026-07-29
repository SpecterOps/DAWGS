package checkpoint

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

func ValidateIdentity(expected, actual Identity) error {
	var differences []string
	if !slices.Equal(expected.Graphs, actual.Graphs) {
		differences = append(differences, fmt.Sprintf(
			"ordered graph names differ: got %q want %q",
			actual.Graphs,
			expected.Graphs,
		))
	}
	if expected.EntityBatchSize != actual.EntityBatchSize {
		differences = append(differences, fmt.Sprintf(
			"entity batch size: got %d want %d",
			actual.EntityBatchSize,
			expected.EntityBatchSize,
		))
	}
	if expected.ShardSize != actual.ShardSize {
		differences = append(differences, fmt.Sprintf(
			"shard size: got %d want %d",
			actual.ShardSize,
			expected.ShardSize,
		))
	}
	if expected.JSONLEnabled != actual.JSONLEnabled {
		differences = append(differences, fmt.Sprintf(
			"JSONL enabled: got %t want %t",
			actual.JSONLEnabled,
			expected.JSONLEnabled,
		))
	}
	if expected.JSONLCodec != actual.JSONLCodec {
		differences = append(differences, fmt.Sprintf(
			"JSONL codec: got %q want %q",
			actual.JSONLCodec,
			expected.JSONLCodec,
		))
	}
	if expected.JSONLLevel != actual.JSONLLevel {
		differences = append(differences, fmt.Sprintf(
			"JSONL level: got %d want %d",
			actual.JSONLLevel,
			expected.JSONLLevel,
		))
	}
	if expected.ParquetEnabled != actual.ParquetEnabled {
		differences = append(differences, fmt.Sprintf(
			"Parquet enabled: got %t want %t",
			actual.ParquetEnabled,
			expected.ParquetEnabled,
		))
	}
	if expected.JSONLSchemaVersion != actual.JSONLSchemaVersion {
		differences = append(differences, fmt.Sprintf(
			"JSONL schema version: got %q want %q",
			actual.JSONLSchemaVersion,
			expected.JSONLSchemaVersion,
		))
	}
	if expected.ParquetSchemaVersion != actual.ParquetSchemaVersion {
		differences = append(differences, fmt.Sprintf(
			"Parquet schema version: got %q want %q",
			actual.ParquetSchemaVersion,
			expected.ParquetSchemaVersion,
		))
	}
	if expected.ScrubEnabled != actual.ScrubEnabled {
		differences = append(differences, fmt.Sprintf(
			"scrub enabled: got %t want %t",
			actual.ScrubEnabled,
			expected.ScrubEnabled,
		))
	}
	if expected.ScrubRulesFingerprint != actual.ScrubRulesFingerprint {
		differences = append(differences, fmt.Sprintf(
			"scrub rules fingerprint: got %q want %q",
			actual.ScrubRulesFingerprint,
			expected.ScrubRulesFingerprint,
		))
	}
	if expected.ScrubSaltFingerprint != actual.ScrubSaltFingerprint {
		differences = append(differences, fmt.Sprintf(
			"scrub salt fingerprint: got %q want %q",
			actual.ScrubSaltFingerprint,
			expected.ScrubSaltFingerprint,
		))
	}
	if len(differences) != 0 {
		return errors.New(strings.Join(differences, "; "))
	}
	return nil
}

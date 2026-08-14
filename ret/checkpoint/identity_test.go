package checkpoint

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateIdentityAcceptsIdenticalIdentity(t *testing.T) {
	identity := fixtureIdentity()

	require.NoError(t, ValidateIdentity(identity, identity))
}

func TestValidateIdentityReportsEveryFieldMismatchInStableOrder(t *testing.T) {
	expected := fixtureIdentity()
	actual := Identity{
		Graphs:                []string{"beta", "alpha"},
		EntityBatchSize:       101,
		ShardSize:             3,
		JSONLEnabled:          false,
		JSONLCodec:            "gzip",
		JSONLLevel:            7,
		ParquetEnabled:        false,
		JSONLSchemaVersion:    "ret-jsonl-v0",
		ParquetSchemaVersion:  "ret-parquet-v0",
		ScrubEnabled:          false,
		ScrubRulesFingerprint: strings.Repeat("c", 64),
		ScrubSaltFingerprint:  strings.Repeat("d", 64),
	}

	err := ValidateIdentity(expected, actual)
	require.EqualError(t, err,
		`ordered graph names differ: got ["beta" "alpha"] want ["alpha" "beta"]; `+
			`entity batch size: got 101 want 100; `+
			`shard size: got 3 want 2; `+
			`JSONL enabled: got false want true; `+
			`JSONL codec: got "gzip" want "zstd"; `+
			`JSONL level: got 7 want 3; `+
			`Parquet enabled: got false want true; `+
			`JSONL schema version: got "ret-jsonl-v0" want "retriever-jsonl-v1"; `+
			`Parquet schema version: got "ret-parquet-v0" want "ret-parquet-v1"; `+
			`scrub enabled: got false want true; `+
			`scrub rules fingerprint: got "`+strings.Repeat("c", 64)+`" want "`+strings.Repeat("a", 64)+`"; `+
			`scrub salt fingerprint: got "`+strings.Repeat("d", 64)+`" want "`+strings.Repeat("b", 64)+`"`,
	)
}

func TestValidateIdentityDetectsEveryFieldIndividually(t *testing.T) {
	tests := []struct {
		name   string
		label  string
		mutate func(*Identity)
	}{
		{name: "ordered graphs", label: "ordered graph names", mutate: func(value *Identity) {
			value.Graphs = []string{"beta", "alpha"}
		}},
		{name: "entity batch size", label: "entity batch size", mutate: func(value *Identity) {
			value.EntityBatchSize++
		}},
		{name: "shard size", label: "shard size", mutate: func(value *Identity) {
			value.ShardSize++
		}},
		{name: "JSONL enabled", label: "JSONL enabled", mutate: func(value *Identity) {
			value.JSONLEnabled = !value.JSONLEnabled
		}},
		{name: "JSONL codec", label: "JSONL codec", mutate: func(value *Identity) {
			value.JSONLCodec = "gzip"
		}},
		{name: "JSONL level", label: "JSONL level", mutate: func(value *Identity) {
			value.JSONLLevel++
		}},
		{name: "Parquet enabled", label: "Parquet enabled", mutate: func(value *Identity) {
			value.ParquetEnabled = !value.ParquetEnabled
		}},
		{name: "JSONL schema", label: "JSONL schema version", mutate: func(value *Identity) {
			value.JSONLSchemaVersion = "changed"
		}},
		{name: "Parquet schema", label: "Parquet schema version", mutate: func(value *Identity) {
			value.ParquetSchemaVersion = "changed"
		}},
		{name: "scrub enabled", label: "scrub enabled", mutate: func(value *Identity) {
			value.ScrubEnabled = !value.ScrubEnabled
		}},
		{name: "scrub rules", label: "scrub rules fingerprint", mutate: func(value *Identity) {
			value.ScrubRulesFingerprint = strings.Repeat("c", 64)
		}},
		{name: "scrub salt", label: "scrub salt fingerprint", mutate: func(value *Identity) {
			value.ScrubSaltFingerprint = strings.Repeat("d", 64)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := fixtureIdentity()
			actual := fixtureIdentity()
			test.mutate(&actual)

			require.ErrorContains(t, ValidateIdentity(expected, actual), test.label)
		})
	}
}

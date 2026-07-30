package ret

import (
	"testing"

	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func TestDumpConfigRequiresAnOutput(t *testing.T) {
	// Break caught: accepting a dump that has no artifact writer enabled.
	config := validDumpConfig(t)
	config.JSONL.Enabled = false
	config.Parquet.Enabled = false

	require.ErrorContains(t, config.Validate(), "output")
}

func TestParquetOnlyDumpConfigIsValid(t *testing.T) {
	// Break caught: rejecting a valid concrete Parquet-only dump.
	config := validDumpConfig(t)
	config.JSONL.Enabled = false
	config.Parquet.Enabled = true

	require.NoError(t, config.Validate())
}

func TestDumpConfigRejectsDuplicateAndUnsafeGraphNames(t *testing.T) {
	// Break caught: accepting graph names that would collide or escape collection paths.
	for _, graphs := range [][]string{{"asset", "asset"}, {"../asset"}, {"asset/node"}, {"."}} {
		config := validDumpConfig(t)
		config.Graphs = graphs

		require.ErrorIs(t, config.Validate(), ErrInvalidConfig)
	}
}

func TestDumpConfigRejectsNonPositiveBatchAndShardSizes(t *testing.T) {
	// Break caught: accepting sizes that cannot make forward progress during a dump.
	for _, mutate := range []func(*DumpConfig){
		func(config *DumpConfig) { config.EntityBatchSize = 0 },
		func(config *DumpConfig) { config.ShardSize = 0 },
	} {
		config := validDumpConfig(t)
		mutate(&config)

		require.ErrorIs(t, config.Validate(), ErrInvalidConfig)
	}
}

func TestDumpConfigReturnsFormatAndScrubValidationErrors(t *testing.T) {
	// Break caught: allowing invalid delegated output or scrub configurations into a dump.
	for _, mutate := range []func(*DumpConfig){
		func(config *DumpConfig) { config.JSONL.Codec = jsonl.Codec("zip") },
		func(config *DumpConfig) { config.JSONL.Level = 99 },
		func(config *DumpConfig) {
			config.Scrub.Rules.Classifier.ValueShapePatterns = []scrub.ValueShapeConfig{{Name: "invalid", Pattern: "("}}
		},
	} {
		config := validDumpConfig(t)
		mutate(&config)

		require.ErrorIs(t, config.Validate(), ErrInvalidConfig)
	}
}

func TestDumpConfigAllowsDisabledScrubbing(t *testing.T) {
	// Break caught: requiring a scrub policy when nil is the library-level
	// opt-out from scrubbing.
	config := validDumpConfig(t)
	config.Scrub = nil

	require.NoError(t, config.Validate())
}

func TestDumpConfigRetainsEveryDelegatedValidationCause(t *testing.T) {
	// Break caught: classifying a root validation error while silently discarding one delegated failure.
	config := validDumpConfig(t)
	config.JSONL.Codec = jsonl.Codec("zip")
	config.Scrub.Rules.Classifier.ValueShapePatterns = []scrub.ValueShapeConfig{{Name: "invalid", Pattern: "("}}

	err := config.Validate()
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.ErrorContains(t, err, `unsupported JSONL codec "zip"`)
	require.ErrorContains(t, err, `compile value shape "invalid"`)
}

func TestOtherFacadeConfigsValidateRequiredInputs(t *testing.T) {
	// Break caught: allowing a later load or verification operation to start without its required path or batch size.
	require.ErrorIs(t, (LoadConfig{Directory: t.TempDir()}).Validate(), ErrInvalidConfig)
	require.ErrorIs(t, (VerifyDatabaseConfig{Directory: t.TempDir()}).Validate(), ErrInvalidConfig)
	require.ErrorIs(t, (VerifyCollectionConfig{}).Validate(), ErrInvalidConfig)
	require.NoError(t, (LoadConfig{Directory: t.TempDir(), BatchSize: 1}).Validate())
	require.NoError(t, (VerifyCollectionConfig{Directory: t.TempDir()}).Validate())
	require.NoError(t, (VerifyDatabaseConfig{Directory: t.TempDir(), BatchSize: 1}).Validate())
}

func validDumpConfig(t *testing.T) DumpConfig {
	t.Helper()
	return DumpConfig{
		Directory:       t.TempDir(),
		Graphs:          []string{"asset"},
		EntityBatchSize: 1,
		ShardSize:       1,
		JSONL:           jsonl.Config{Enabled: true, Codec: jsonl.CodecNone},
		Parquet:         parquet.Config{Enabled: true},
		Scrub:           pointerTo(scrub.DefaultConfig()),
	}
}

func pointerTo[T any](value T) *T {
	return &value
}

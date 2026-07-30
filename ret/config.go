package ret

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

type DumpConfig struct {
	Directory       string
	Graphs          []string
	EntityBatchSize int
	ShardSize       int
	Resume          bool
	JSONL           jsonl.Config
	Parquet         parquet.Config
	Scrub           *scrub.Config // Nil disables scrubbing.
	Observer        observe.Observer
}

type LoadConfig struct {
	Directory string
	BatchSize int
	Observer  observe.Observer
}

type VerifyCollectionConfig struct {
	Directory string
	Observer  observe.Observer
}

type VerifyDatabaseConfig struct {
	Directory string
	BatchSize int
	Observer  observe.Observer
}

func (s DumpConfig) Validate() error {
	if strings.TrimSpace(s.Directory) == "" || len(s.Graphs) == 0 {
		return fmt.Errorf("%w: dump directory and at least one graph are required", ErrInvalidConfig)
	}
	if err := validateGraphNames(s.Graphs); err != nil {
		return fmt.Errorf("%w: dump graphs: %w", ErrInvalidConfig, err)
	}
	if s.EntityBatchSize <= 0 || s.ShardSize <= 0 {
		return fmt.Errorf("%w: batch and shard sizes must be positive", ErrInvalidConfig)
	}
	if !s.JSONL.Enabled && !s.Parquet.Enabled {
		return fmt.Errorf("%w: at least one output is required", ErrInvalidConfig)
	}
	var scrubErr error
	if s.Scrub != nil {
		scrubErr = s.Scrub.Validate()
	}
	if err := errors.Join(s.JSONL.Validate(), s.Parquet.Validate(), scrubErr); err != nil {
		return fmt.Errorf("%w: dump output and scrub configuration: %w", ErrInvalidConfig, err)
	}
	return nil
}

func (s LoadConfig) Validate() error {
	if strings.TrimSpace(s.Directory) == "" {
		return fmt.Errorf("%w: load directory is required", ErrInvalidConfig)
	}
	if s.BatchSize <= 0 {
		return fmt.Errorf("%w: load batch size must be positive", ErrInvalidConfig)
	}
	return nil
}

func (s VerifyCollectionConfig) Validate() error {
	if strings.TrimSpace(s.Directory) == "" {
		return fmt.Errorf("%w: collection directory is required", ErrInvalidConfig)
	}
	return nil
}

func (s VerifyDatabaseConfig) Validate() error {
	if strings.TrimSpace(s.Directory) == "" {
		return fmt.Errorf("%w: database verification directory is required", ErrInvalidConfig)
	}
	if s.BatchSize <= 0 {
		return fmt.Errorf("%w: database verification batch size must be positive", ErrInvalidConfig)
	}
	return nil
}

func validateGraphNames(graphs []string) error {
	seen := make(map[string]struct{}, len(graphs))
	for _, graph := range graphs {
		if strings.TrimSpace(graph) == "" || graph == "." || graph == ".." || path.Clean(graph) != graph || strings.ContainsAny(graph, "/\\") || strings.ContainsRune(graph, '\x00') {
			return fmt.Errorf("graph name %q is not a safe path segment", graph)
		}
		if _, found := seen[graph]; found {
			return fmt.Errorf("duplicate graph name %q", graph)
		}
		seen[graph] = struct{}{}
	}
	return nil
}

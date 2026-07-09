package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/retriever"
)

const defaultBenchSampleSize = 1_000_000

type stringList []string

func (s *stringList) Set(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("value cannot be empty")
	}

	*s = append(*s, trimmed)

	return nil
}

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

type workerList []int

func (s *workerList) Set(value string) error {
	values, err := parseWorkerList(value)
	if err != nil {
		return err
	}

	seen := make(map[int]struct{}, len(*s)+len(values))
	for _, value := range *s {
		seen[value] = struct{}{}
	}

	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		*s = append(*s, value)
	}

	return nil
}

func (s *workerList) String() string {
	values := make([]string, 0, len(*s))
	for _, value := range *s {
		values = append(values, strconv.Itoa(value))
	}

	return strings.Join(values, ",")
}

func parseWorkerList(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	workers := make([]int, 0, len(parts))
	seen := map[int]struct{}{}

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}

		count, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("parse worker count %q: %w", trimmed, err)
		}

		if count <= 0 {
			return nil, fmt.Errorf("worker counts must be > 0")
		}

		if _, ok := seen[count]; ok {
			continue
		}

		seen[count] = struct{}{}
		workers = append(workers, count)
	}

	if len(workers) == 0 {
		return nil, fmt.Errorf("at least one worker count is required")
	}

	return workers, nil
}

type benchOptions struct {
	Workers     []int
	BatchSize   int
	SampleSize  int
	Compression retriever.CompressionCodec
	ZstdLevel   int
	JSONOutput  bool
}

func (s benchOptions) validate() error {
	if len(s.Workers) == 0 {
		return fmt.Errorf("workers are required")
	}

	for _, workerCount := range s.Workers {
		if workerCount <= 0 {
			return fmt.Errorf("worker counts must be > 0")
		}
	}

	if s.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be > 0")
	}

	if s.SampleSize < 0 {
		return fmt.Errorf("sample-size must be >= 0")
	}

	if s.ZstdLevel <= 0 {
		return fmt.Errorf("zstd-level must be > 0")
	}

	if s.Compression != retriever.CompressionDisabled {
		if err := retriever.ValidateCompression(s.Compression); err != nil {
			return err
		}
	}

	return nil
}

func commonDatabaseFlags(flags *flag.FlagSet, cfg *databaseConfig) {
	flags.StringVar(&cfg.Driver, "driver", "", "Graph database driver. Inferred from -connection when omitted.")
	flags.StringVar(&cfg.Connection, "connection", "", "Graph database connection string. Falls back to CONNECTION_STRING.")
}

func fillConnectionFromEnv(cfg *databaseConfig) {
	if strings.TrimSpace(cfg.Connection) == "" {
		cfg.Connection = strings.TrimSpace(os.Getenv("CONNECTION_STRING"))
	}
}

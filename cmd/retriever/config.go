package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultShardSize       = 100_000
	defaultBatchSize       = 10_000
	defaultBenchSampleSize = 1_000_000
)

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
	*s = values
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

type dumpOptions struct {
	Database         databaseConfig
	Graphs           []string
	AllGraphs        bool
	OutputDir        string
	Force            bool
	Scrub            scrubMode
	Salt             string
	ScrubConfigPath  string
	Compression      compressionCodec
	ZstdLevel        int
	ShardSize        int
	BatchSize        int
	ProgressInterval int
}

func (s dumpOptions) validate() error {
	if strings.TrimSpace(s.OutputDir) == "" {
		return fmt.Errorf("output directory is required; pass -out")
	}
	if err := validateCompression(s.Compression); err != nil {
		return err
	}
	if s.ZstdLevel <= 0 {
		return fmt.Errorf("zstd-level must be > 0")
	}
	if s.ShardSize <= 0 {
		return fmt.Errorf("shard-size must be > 0")
	}
	if s.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be > 0")
	}
	switch s.Scrub {
	case scrubNone:
		return nil
	case scrubFull:
		if strings.TrimSpace(s.Salt) == "" {
			return fmt.Errorf("-scrub full requires -salt, RETRIEVER_SCRUB_SALT, or legacy RETRIEVR_SCRUB_SALT; refusing to write scrubbed output without deterministic pseudonymization")
		}
		return nil
	default:
		return fmt.Errorf("unsupported scrub mode %q", s.Scrub)
	}
}

type loadOptions struct {
	Database      databaseConfig
	InputDir      string
	BatchSize     int
	VerifyMetrics bool
}

func (s loadOptions) validate() error {
	if strings.TrimSpace(s.InputDir) == "" {
		return fmt.Errorf("input directory is required; pass -in")
	}
	if s.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be > 0")
	}
	return nil
}

type verifyOptions struct {
	Database  databaseConfig
	InputDir  string
	BatchSize int
}

func (s verifyOptions) validate() error {
	if strings.TrimSpace(s.InputDir) == "" {
		return fmt.Errorf("input directory is required; pass -in")
	}
	if s.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be > 0")
	}
	return nil
}

type benchOptions struct {
	Database    databaseConfig
	Graphs      []string
	AllGraphs   bool
	Workers     []int
	BatchSize   int
	SampleSize  int
	Compression compressionCodec
	ZstdLevel   int
	JSONOutput  bool
}

func (s benchOptions) validate() error {
	if len(s.Workers) == 0 {
		return fmt.Errorf("workers are required")
	}
	for _, workerCount := range s.Workers {
		if workerCount != 1 {
			return fmt.Errorf("parallel benchmark workers are disabled until safe partitioned scans are implemented; use -workers 1")
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
	if s.Compression != compressionNone {
		if err := validateCompression(s.Compression); err != nil {
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

func visitedFlags(flags *flag.FlagSet) map[string]bool {
	visited := map[string]bool{}
	flags.Visit(func(nextFlag *flag.Flag) {
		visited[nextFlag.Name] = true
	})
	return visited
}

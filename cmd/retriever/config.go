package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/ret"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

const (
	defaultGraphName       = "default"
	defaultEntityBatchSize = 10_000
	defaultShardSize       = 100_000
	defaultBenchSampleSize = 1_000_000
)

type dumpCommandConfig struct {
	database  databaseConfig
	dump      ret.DumpConfig
	graphs    []string
	allGraphs bool
	force     bool
	pprof     string
}

func parseDumpCommand(args []string, output io.Writer) (dumpCommandConfig, error) {
	config := dumpCommandConfig{
		dump: ret.DumpConfig{
			EntityBatchSize: defaultEntityBatchSize,
			ShardSize:       defaultShardSize,
			JSONL: jsonl.Config{
				Enabled: true,
				Codec:   jsonl.CodecZstd,
				Level:   0,
			},
			Parquet: parquet.Config{},
		},
	}
	var (
		graphs           stringList
		scrubMode        string
		scrubSalt        string
		scrubConfigPath  string
		jsonlCompression string
		scrubConfig      = scrub.DefaultConfig()
	)
	flags := flag.NewFlagSet("retriever dump", flag.ContinueOnError)
	flags.SetOutput(output)
	commonDatabaseFlags(flags, &config.database)
	flags.Var(&graphs, "graph", "Graph target. May be repeated.")
	flags.BoolVar(&config.allGraphs, "all-graphs", false, "Dump every graph discoverable by the selected driver.")
	flags.StringVar(&config.dump.Directory, "out", "", "Output collection directory.")
	flags.BoolVar(&config.force, "force", false, "Replace the exact output directory before a fresh dump.")
	flags.BoolVar(&config.dump.Resume, "resume", false, "Resume an interrupted dump from its validated checkpoint.")
	flags.BoolVar(&config.dump.JSONL.Enabled, "jsonl", config.dump.JSONL.Enabled, "Write JSONL artifacts.")
	flags.StringVar(&jsonlCompression, "jsonl-compression", string(config.dump.JSONL.Codec), "JSONL compression codec: zstd, gzip, or none.")
	flags.IntVar(&config.dump.JSONL.Level, "jsonl-level", config.dump.JSONL.Level, "JSONL compression level; 0 selects the package default.")
	flags.BoolVar(&config.dump.Parquet.Enabled, "parquet", config.dump.Parquet.Enabled, "Write Parquet artifacts.")
	flags.StringVar(&scrubMode, "scrub", "none", "Scrub mode: none or full.")
	flags.StringVar(&scrubSalt, "salt", "", "Scrub salt. Overrides RETRIEVER_SCRUB_SALT and is never written.")
	flags.StringVar(&scrubConfigPath, "config", "", "Optional retriever TOML scrub configuration.")
	flags.IntVar(&config.dump.ShardSize, "shard-size", config.dump.ShardSize, "Maximum entities per shard.")
	flags.IntVar(&config.dump.EntityBatchSize, "batch-size", config.dump.EntityBatchSize, "Database read batch size.")
	commonPprofFlag(flags, &config.pprof)
	if err := flags.Parse(args); err != nil {
		return dumpCommandConfig{}, err
	}

	fillConnectionFromEnv(&config.database)
	config.graphs = append([]string(nil), graphs...)
	config.dump.Directory = strings.TrimSpace(config.dump.Directory)
	config.dump.JSONL.Codec = jsonl.Codec(strings.TrimSpace(jsonlCompression))

	if path := strings.TrimSpace(scrubConfigPath); path != "" {
		loaded, err := scrub.ReadConfig(path)
		if err != nil {
			return dumpCommandConfig{}, err
		}
		scrubConfig = loaded
	}
	if strings.TrimSpace(scrubSalt) == "" {
		scrubSalt = strings.TrimSpace(os.Getenv("RETRIEVER_SCRUB_SALT"))
		if scrubSalt == "" {
			scrubSalt = strings.TrimSpace(os.Getenv("RETRIEVR_SCRUB_SALT"))
		}
	}
	scrubConfig.Salt = strings.TrimSpace(scrubSalt)
	switch strings.TrimSpace(scrubMode) {
	case "none":
		config.dump.Scrub = nil
	case "full":
		if scrubConfig.Salt == "" {
			return dumpCommandConfig{}, fmt.Errorf("-scrub full requires -salt, RETRIEVER_SCRUB_SALT, or legacy RETRIEVR_SCRUB_SALT")
		}
		config.dump.Scrub = &scrubConfig
	default:
		return dumpCommandConfig{}, fmt.Errorf("unsupported scrub mode %q", scrubMode)
	}

	if config.dump.Directory == "" {
		return dumpCommandConfig{}, fmt.Errorf("output directory is required; pass -out")
	}
	if config.force && config.dump.Resume {
		return dumpCommandConfig{}, fmt.Errorf("-force and -resume are mutually exclusive")
	}
	if config.dump.EntityBatchSize <= 0 {
		return dumpCommandConfig{}, fmt.Errorf("batch-size must be > 0")
	}
	if config.dump.ShardSize <= 0 {
		return dumpCommandConfig{}, fmt.Errorf("shard-size must be > 0")
	}
	if !config.dump.JSONL.Enabled && !config.dump.Parquet.Enabled {
		return dumpCommandConfig{}, fmt.Errorf("at least one of -jsonl or -parquet must be enabled")
	}
	if err := config.dump.JSONL.Validate(); err != nil {
		return dumpCommandConfig{}, fmt.Errorf("JSONL configuration: %w", err)
	}
	if err := config.dump.Parquet.Validate(); err != nil {
		return dumpCommandConfig{}, fmt.Errorf("Parquet configuration: %w", err)
	}
	if config.dump.Scrub != nil {
		if err := config.dump.Scrub.Validate(); err != nil {
			return dumpCommandConfig{}, fmt.Errorf("scrub configuration: %w", err)
		}
	}
	return config, nil
}

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
	Workers    []int
	BatchSize  int
	SampleSize int
	JSONL      jsonl.Config
	Parquet    parquet.Config
	JSONOutput bool
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

	if !s.JSONL.Enabled && !s.Parquet.Enabled {
		return fmt.Errorf("at least one of -jsonl or -parquet must be enabled")
	}

	if s.JSONL.Enabled {
		if err := s.JSONL.Validate(); err != nil {
			return fmt.Errorf("JSONL configuration: %w", err)
		}
	}
	if s.Parquet.Enabled {
		if err := s.Parquet.Validate(); err != nil {
			return fmt.Errorf("Parquet configuration: %w", err)
		}
	}

	return nil
}

type benchCommandConfig struct {
	database  databaseConfig
	bench     benchOptions
	graphs    []string
	allGraphs bool
	pprof     string
}

func parseBenchCommand(args []string, output io.Writer) (benchCommandConfig, error) {
	config := benchCommandConfig{
		bench: benchOptions{
			Workers:    []int{1},
			BatchSize:  defaultEntityBatchSize,
			SampleSize: defaultBenchSampleSize,
			JSONL: jsonl.Config{
				Enabled: true,
				Codec:   jsonl.CodecZstd,
			},
		},
	}
	var (
		graphs           stringList
		workers          workerList
		jsonlCompression = string(config.bench.JSONL.Codec)
	)
	flags := flag.NewFlagSet("retriever bench", flag.ContinueOnError)
	flags.SetOutput(output)
	commonDatabaseFlags(flags, &config.database)
	flags.Var(&graphs, "graph", "Graph target. May be repeated.")
	flags.BoolVar(&config.allGraphs, "all-graphs", false, "Benchmark every graph discoverable by the selected driver.")
	flags.Var(&workers, "workers", "Comma-separated worker counts.")
	flags.IntVar(&config.bench.BatchSize, "batch-size", config.bench.BatchSize, "Database read batch size.")
	flags.IntVar(&config.bench.SampleSize, "sample-size", config.bench.SampleSize, "Maximum nodes and relationships to scan per phase; 0 scans the full graph.")
	flags.BoolVar(&config.bench.JSONL.Enabled, "jsonl", config.bench.JSONL.Enabled, "Benchmark JSONL artifacts.")
	flags.StringVar(&jsonlCompression, "jsonl-compression", jsonlCompression, "JSONL compression codec: zstd, gzip, or none.")
	flags.IntVar(&config.bench.JSONL.Level, "jsonl-level", config.bench.JSONL.Level, "JSONL compression level; 0 selects the package default.")
	flags.BoolVar(&config.bench.Parquet.Enabled, "parquet", config.bench.Parquet.Enabled, "Benchmark Parquet artifacts.")
	flags.BoolVar(&config.bench.JSONOutput, "json", false, "Emit machine-readable JSON.")
	commonPprofFlag(flags, &config.pprof)
	if err := flags.Parse(args); err != nil {
		return benchCommandConfig{}, err
	}

	fillConnectionFromEnv(&config.database)
	config.graphs = append([]string(nil), graphs...)
	if len(workers) > 0 {
		config.bench.Workers = append([]int(nil), workers...)
	}
	config.bench.JSONL.Codec = jsonl.Codec(strings.TrimSpace(jsonlCompression))
	if err := config.bench.validate(); err != nil {
		return benchCommandConfig{}, err
	}
	return config, nil
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

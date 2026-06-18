package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const usage = `usage: retrievr <command> [options]

Commands:
  dump    Dump live Dawgs graph data into a manifest-based collection.
  load    Load a manifest-based collection into a Dawgs graph database.
  bench   Benchmark read throughput for dump planning.
`

type commandRuntime struct {
	stdout io.Writer
	stderr io.Writer
}

func main() {
	runtime := commandRuntime{
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
	if err := runtime.run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "retrievr: %v\n", err)
		os.Exit(1)
	}
}

func (s commandRuntime) run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(s.stderr, usage)
		return fmt.Errorf("command is required")
	}

	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(s.stdout, usage)
		return nil
	case "dump":
		return s.runDump(ctx, args[1:])
	case "load":
		return s.runLoad(ctx, args[1:])
	case "bench":
		return s.runBench(ctx, args[1:])
	default:
		fmt.Fprint(s.stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (s commandRuntime) runDump(ctx context.Context, args []string) error {
	var (
		cfg            dumpOptions
		graphs         stringList
		scrubValue     string
		compressionVal string
	)

	cfg.Scrub = scrubNone
	cfg.Compression = compressionZstd
	cfg.ZstdLevel = defaultZstdLevel
	cfg.ShardSize = defaultShardSize
	cfg.BatchSize = defaultBatchSize

	flags := flag.NewFlagSet("retrievr dump", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &cfg.Database)
	flags.Var(&graphs, "graph", "Graph target. May be repeated.")
	flags.BoolVar(&cfg.AllGraphs, "all-graphs", false, "Dump every graph discoverable by the selected driver.")
	flags.StringVar(&cfg.OutputDir, "out", "", "Output collection directory.")
	flags.BoolVar(&cfg.Force, "force", false, "Replace an existing non-empty output directory.")
	flags.StringVar(&scrubValue, "scrub", string(cfg.Scrub), "Scrub mode: none or full.")
	flags.StringVar(&cfg.Salt, "salt", "", "Scrub salt. Overrides RETRIEVR_SCRUB_SALT and is never written.")
	flags.StringVar(&cfg.ScrubConfigPath, "config", "", "Optional retrievr TOML config for scrub classifier settings.")
	flags.StringVar(&compressionVal, "compression", string(cfg.Compression), "Compression codec: zstd or gzip.")
	flags.IntVar(&cfg.ZstdLevel, "zstd-level", cfg.ZstdLevel, "zstd compression level.")
	flags.IntVar(&cfg.ShardSize, "shard-size", cfg.ShardSize, "Maximum entities per fragment.")
	flags.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Database read batch size.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	fillConnectionFromEnv(&cfg.Database)
	cfg.Graphs = []string(graphs)
	cfg.Scrub = scrubMode(strings.TrimSpace(scrubValue))
	cfg.Compression = compressionCodec(strings.TrimSpace(compressionVal))
	if strings.TrimSpace(cfg.Salt) == "" {
		cfg.Salt = strings.TrimSpace(os.Getenv("RETRIEVR_SCRUB_SALT"))
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	db, driverName, err := openDatabase(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	targets, err := resolveGraphTargets(ctx, db, driverName, cfg.Graphs, cfg.AllGraphs)
	if err != nil {
		return err
	}

	result, err := Dump(ctx, db, driverName, targets, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "dumped %d graph(s)\nmanifest: %s\nnodes: %d\nrelationships: %d\n", len(result.Manifest.Graphs), result.ManifestPath, result.NodeCount, result.EdgeCount)
	return nil
}

func (s commandRuntime) runLoad(ctx context.Context, args []string) error {
	var cfg loadOptions
	cfg.BatchSize = defaultBatchSize

	flags := flag.NewFlagSet("retrievr load", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &cfg.Database)
	flags.StringVar(&cfg.InputDir, "in", "", "Input collection directory.")
	flags.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Database write batch size.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	fillConnectionFromEnv(&cfg.Database)
	if err := cfg.validate(); err != nil {
		return err
	}

	db, driverName, err := openDatabase(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	result, err := Load(ctx, db, driverName, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "loaded %d graph(s)\nnodes: %d\nrelationships: %d\n", result.GraphCount, result.NodeCount, result.EdgeCount)
	return nil
}

func (s commandRuntime) runBench(ctx context.Context, args []string) error {
	var (
		cfg            benchOptions
		graphs         stringList
		workers        workerList
		compressionVal string
	)

	cfg.BatchSize = defaultBatchSize
	cfg.SampleSize = defaultBenchSampleSize
	cfg.ZstdLevel = defaultZstdLevel
	workers = workerList{1}

	flags := flag.NewFlagSet("retrievr bench", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &cfg.Database)
	flags.Var(&graphs, "graph", "Graph target. May be repeated.")
	flags.BoolVar(&cfg.AllGraphs, "all-graphs", false, "Benchmark every graph discoverable by the selected driver.")
	flags.Var(&workers, "workers", "Comma-separated worker counts.")
	flags.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Database read batch size.")
	flags.IntVar(&cfg.SampleSize, "sample-size", cfg.SampleSize, "Maximum nodes and relationships to scan per phase; 0 scans the full graph.")
	flags.StringVar(&compressionVal, "compression", "", "Optional compression codec to include encode/compress timing: zstd or gzip.")
	flags.IntVar(&cfg.ZstdLevel, "zstd-level", cfg.ZstdLevel, "zstd compression level.")
	flags.BoolVar(&cfg.JSONOutput, "json", false, "Emit machine-readable JSON.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	fillConnectionFromEnv(&cfg.Database)
	cfg.Graphs = []string(graphs)
	cfg.Workers = []int(workers)
	cfg.Compression = compressionCodec(strings.TrimSpace(compressionVal))
	if err := cfg.validate(); err != nil {
		return err
	}

	db, driverName, err := openDatabase(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	targets, err := resolveGraphTargets(ctx, db, driverName, cfg.Graphs, cfg.AllGraphs)
	if err != nil {
		return err
	}

	report, err := Bench(ctx, db, driverName, targets, cfg)
	if err != nil {
		return err
	}
	if cfg.JSONOutput {
		encoder := json.NewEncoder(s.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	writeBenchReport(s.stdout, report)
	return nil
}

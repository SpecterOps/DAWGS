package main

import (
	"context"
	"crypto/hpke"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const usage = `usage: retriever <command> [options]

Commands:
  keygen  Generate an HPKE recipient key pair for encrypted archives.
  dump    Dump live Dawgs graph data into a manifest-based collection.
  unpack  Decrypt and unpack an encrypted retriever archive.
  load    Load a manifest-based collection into a Dawgs graph database.
  verify  Verify loaded graph metrics against a dump manifest.
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
		fmt.Fprintf(os.Stderr, "retriever: %v\n", err)
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
	case "keygen":
		return s.runKeygen(args[1:])
	case "dump":
		return s.runDump(ctx, args[1:])
	case "unpack":
		return s.runUnpack(args[1:])
	case "load":
		return s.runLoad(ctx, args[1:])
	case "verify":
		return s.runVerify(ctx, args[1:])
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

	flags := flag.NewFlagSet("retriever dump", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &cfg.Database)
	flags.Var(&graphs, "graph", "Graph target. May be repeated.")
	flags.BoolVar(&cfg.AllGraphs, "all-graphs", false, "Dump every graph discoverable by the selected driver.")
	flags.StringVar(&cfg.OutputDir, "out", "", "Output collection directory.")
	flags.BoolVar(&cfg.Force, "force", false, "Replace an existing non-empty output directory.")
	flags.StringVar(&cfg.ArchiveOut, "archive-out", "", "Optional encrypted archive output path.")
	flags.StringVar(&cfg.RecipientPath, "recipient", "", "Recipient public key for -archive-out.")
	flags.StringVar(&scrubValue, "scrub", string(cfg.Scrub), "Scrub mode: none or full.")
	flags.StringVar(&cfg.Salt, "salt", "", "Scrub salt. Overrides RETRIEVER_SCRUB_SALT and is never written.")
	flags.StringVar(&cfg.ScrubConfigPath, "config", "", "Optional retriever TOML config for scrub classifier settings.")
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
		cfg.Salt = strings.TrimSpace(os.Getenv("RETRIEVER_SCRUB_SALT"))
		if cfg.Salt == "" {
			cfg.Salt = strings.TrimSpace(os.Getenv("RETRIEVR_SCRUB_SALT"))
		}
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	var archiveRecipient hpke.PublicKey
	if strings.TrimSpace(cfg.ArchiveOut) != "" {
		var err error
		archiveRecipient, err = loadArchivePublicKey(cfg.RecipientPath)
		if err != nil {
			return err
		}
		if err := preflightArchiveOutputPath(cfg.ArchiveOut); err != nil {
			return err
		}
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
	var archiveLine string
	if strings.TrimSpace(cfg.ArchiveOut) != "" {
		if err := writeEncryptedCollectionArchive(cfg.OutputDir, cfg.ArchiveOut, archiveRecipient); err != nil {
			return err
		}
		archiveLine = fmt.Sprintf("archive: %s\n", cfg.ArchiveOut)
	}
	fmt.Fprintf(s.stdout, "dumped %d graph(s)\nmanifest: %s\n%snodes: %d\nrelationships: %d\n", len(result.Manifest.Graphs), result.ManifestPath, archiveLine, result.NodeCount, result.EdgeCount)
	return nil
}

func (s commandRuntime) runKeygen(args []string) error {
	var cfg keygenOptions

	flags := flag.NewFlagSet("retriever keygen", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&cfg.PrivatePath, "private", "", "Private key output path.")
	flags.StringVar(&cfg.PublicPath, "public", "", "Public key output path.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := generateArchiveKeyFiles(cfg.PrivatePath, cfg.PublicPath); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "private key: %s\npublic key: %s\n", cfg.PrivatePath, cfg.PublicPath)
	return nil
}

func (s commandRuntime) runUnpack(args []string) error {
	var cfg unpackOptions

	flags := flag.NewFlagSet("retriever unpack", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&cfg.ArchivePath, "archive", "", "Encrypted archive input path.")
	flags.StringVar(&cfg.IdentityPath, "identity", "", "Recipient private key path.")
	flags.StringVar(&cfg.OutputDir, "out", "", "Output collection directory.")
	flags.BoolVar(&cfg.Force, "force", false, "Replace an existing non-empty output directory.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	identity, err := loadArchivePrivateKey(cfg.IdentityPath)
	if err != nil {
		return err
	}
	if err := unpackEncryptedCollectionArchive(cfg.ArchivePath, cfg.OutputDir, cfg.Force, identity); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "unpacked archive: %s\noutput: %s\n", cfg.ArchivePath, cfg.OutputDir)
	return nil
}

func (s commandRuntime) runLoad(ctx context.Context, args []string) error {
	var cfg loadOptions
	cfg.BatchSize = defaultBatchSize

	flags := flag.NewFlagSet("retriever load", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &cfg.Database)
	flags.StringVar(&cfg.InputDir, "in", "", "Input collection directory.")
	flags.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Database write batch size.")
	flags.BoolVar(&cfg.VerifyMetrics, "verify-metrics", false, "Verify loaded graph metrics against the dump manifest after load.")
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

func (s commandRuntime) runVerify(ctx context.Context, args []string) error {
	var cfg verifyOptions
	cfg.BatchSize = defaultBatchSize

	flags := flag.NewFlagSet("retriever verify", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &cfg.Database)
	flags.StringVar(&cfg.InputDir, "in", "", "Input collection directory.")
	flags.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Database read batch size.")
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

	result, err := Verify(ctx, db, driverName, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "verified %d graph(s)\nnodes: %d\nrelationships: %d\n", result.GraphCount, result.NodeCount, result.EdgeCount)
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

	flags := flag.NewFlagSet("retriever bench", flag.ContinueOnError)
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

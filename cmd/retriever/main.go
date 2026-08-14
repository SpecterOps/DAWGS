package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret"
	"github.com/specterops/dawgs/ret/archive"
	"github.com/specterops/dawgs/ret/observe"
)

const usage = `usage: retriever <command> [options]

Commands:
  dump               Dump live Dawgs graph data into a local collection.
  load               Load a local JSONL collection into a Dawgs database.
  verify-collection  Verify every artifact in a local collection.
  verify-database    Verify database metrics against a collection manifest.
  pack               Create an encrypted archive from a verified collection.
  unpack             Decrypt, verify, and publish a collection.
  keygen             Generate an archive recipient key pair.
  bench              Benchmark read throughput for dump planning.
`

type commandOperations struct {
	openDatabase     func(context.Context, databaseConfig) (graph.Database, string, error)
	dump             func(context.Context, graph.Database, ret.DumpConfig) (ret.DumpResult, error)
	load             func(context.Context, graph.Database, ret.LoadConfig) (ret.LoadResult, error)
	verifyCollection func(context.Context, ret.VerifyCollectionConfig) (ret.VerifyCollectionResult, error)
	verifyDatabase   func(context.Context, graph.Database, ret.VerifyDatabaseConfig) (ret.VerifyDatabaseResult, error)
	pack             func(context.Context, ret.PackConfig) error
	unpack           func(context.Context, ret.UnpackConfig) error
	keygen           func(ret.KeygenConfig) error
}

func (s commandOperations) withDefaults() commandOperations {
	if s.openDatabase == nil {
		s.openDatabase = openDatabase
	}
	if s.dump == nil {
		s.dump = ret.Dump
	}
	if s.load == nil {
		s.load = ret.Load
	}
	if s.verifyCollection == nil {
		s.verifyCollection = ret.VerifyCollection
	}
	if s.verifyDatabase == nil {
		s.verifyDatabase = ret.VerifyDatabase
	}
	if s.pack == nil {
		s.pack = ret.Pack
	}
	if s.unpack == nil {
		s.unpack = ret.Unpack
	}
	if s.keygen == nil {
		s.keygen = ret.Keygen
	}
	return s
}

type commandRuntime struct {
	stdout     io.Writer
	stderr     io.Writer
	operations commandOperations
	observer   observe.Observer
	force      forceReplaceOperations
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
	case "dump":
		return s.runDump(ctx, args[1:])
	case "load":
		return s.runLoad(ctx, args[1:])
	case "verify-collection":
		return s.runVerifyCollection(ctx, args[1:])
	case "verify-database":
		return s.runVerifyDatabase(ctx, args[1:])
	case "pack":
		return s.runPack(ctx, args[1:])
	case "unpack":
		return s.runUnpack(ctx, args[1:])
	case "keygen":
		return s.runKeygen(args[1:])
	case "bench":
		return s.runBench(ctx, args[1:])
	default:
		fmt.Fprint(s.stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (s commandRuntime) runDump(ctx context.Context, args []string) error {
	command, err := parseDumpCommand(args, s.stderr)
	if err != nil {
		return err
	}
	if err := validateGraphSelection(command.graphs, command.allGraphs); err != nil {
		return err
	}
	if address := strings.TrimSpace(command.pprof); address != "" {
		if err := validatePprofListenAddress(address); err != nil {
			return err
		}
	}
	if command.force {
		replacement, err := replaceDumpDestination(command.dump.Directory, s.force)
		if err != nil {
			return err
		}
		command.dump.Directory = replacement.destination
		if replacement.tombstone != "" {
			fmt.Fprintf(s.stderr, "force: previous destination preserved intact at %s\n", replacement.tombstone)
		}
	}

	profileServer, err := startPprofServer(command.pprof, s.stderr)
	if err != nil {
		return err
	}
	defer stopPprofServer(profileServer, s.stderr)

	operations := s.operations.withDefaults()
	var result ret.DumpResult
	if err := func() (resultErr error) {
		database, driverName, err := operations.openDatabase(ctx, command.database)
		if err != nil {
			return err
		}
		defer func() {
			resultErr = errors.Join(resultErr, closeProductDatabase(database))
		}()

		graphs, err := resolveGraphNames(ctx, database, driverName, command.graphs, command.allGraphs)
		if err != nil {
			return err
		}
		command.dump.Graphs = graphs
		command.dump.Observer = s.commandObserver()
		if err := command.dump.Validate(); err != nil {
			return err
		}
		result, err = operations.dump(ctx, database, command.dump)
		return err
	}(); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout,
		"dumped %d graph(s)\nmanifest: %s\nnodes: %d\nrelationships: %d\n",
		result.GraphCount,
		result.ManifestPath,
		result.NodeCount,
		result.RelationshipCount,
	)
	return nil
}

func (s commandRuntime) runLoad(ctx context.Context, args []string) error {
	var (
		databaseConfig databaseConfig
		config         = ret.LoadConfig{BatchSize: defaultEntityBatchSize}
		verify         bool
		pprofListen    string
	)
	flags := flag.NewFlagSet("retriever load", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &databaseConfig)
	flags.StringVar(&config.Directory, "in", "", "Input collection directory.")
	flags.IntVar(&config.BatchSize, "batch-size", config.BatchSize, "Database write batch size.")
	flags.BoolVar(&verify, "verify-database", false, "Verify database metrics after a successful load.")
	commonPprofFlag(flags, &pprofListen)
	if err := flags.Parse(args); err != nil {
		return err
	}
	fillConnectionFromEnv(&databaseConfig)
	config.Directory = strings.TrimSpace(config.Directory)
	config.Observer = s.commandObserver()
	if err := config.Validate(); err != nil {
		return err
	}

	profileServer, err := startPprofServer(pprofListen, s.stderr)
	if err != nil {
		return err
	}
	defer stopPprofServer(profileServer, s.stderr)

	operations := s.operations.withDefaults()
	var (
		result       ret.LoadResult
		verifyResult ret.VerifyDatabaseResult
	)
	if err := func() (resultErr error) {
		database, _, err := operations.openDatabase(ctx, databaseConfig)
		if err != nil {
			return err
		}
		defer func() {
			resultErr = errors.Join(resultErr, closeProductDatabase(database))
		}()

		result, err = operations.load(ctx, database, config)
		if err != nil {
			return fmt.Errorf("load failed; if a graph was partially loaded, clear it before retry: %w", err)
		}
		if verify {
			verifyResult, err = operations.verifyDatabase(ctx, database, ret.VerifyDatabaseConfig{
				Directory: config.Directory,
				BatchSize: config.BatchSize,
				Observer:  config.Observer,
			})
			if err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout,
		"loaded %d graph(s)\nnodes: %d\nrelationships: %d\n",
		result.GraphCount,
		result.NodeCount,
		result.RelationshipCount,
	)

	if verify {
		fmt.Fprintf(s.stdout,
			"verified database: %d graph(s)\nnodes: %d\nrelationships: %d\n",
			verifyResult.GraphCount,
			verifyResult.NodeCount,
			verifyResult.RelationshipCount,
		)
	}
	return nil
}

func (s commandRuntime) runVerifyCollection(ctx context.Context, args []string) error {
	var config ret.VerifyCollectionConfig
	flags := flag.NewFlagSet("retriever verify-collection", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&config.Directory, "in", "", "Input collection directory.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	config.Directory = strings.TrimSpace(config.Directory)
	config.Observer = s.commandObserver()
	if err := config.Validate(); err != nil {
		return err
	}

	result, err := s.operations.withDefaults().verifyCollection(ctx, config)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.stdout,
		"verified collection: %d graph(s)\nnodes: %d\nrelationships: %d\n",
		result.GraphCount,
		result.NodeCount,
		result.RelationshipCount,
	)
	return nil
}

func (s commandRuntime) runVerifyDatabase(ctx context.Context, args []string) error {
	var (
		databaseConfig databaseConfig
		config         = ret.VerifyDatabaseConfig{BatchSize: defaultEntityBatchSize}
		pprofListen    string
	)
	flags := flag.NewFlagSet("retriever verify-database", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	commonDatabaseFlags(flags, &databaseConfig)
	flags.StringVar(&config.Directory, "in", "", "Input collection directory.")
	flags.IntVar(&config.BatchSize, "batch-size", config.BatchSize, "Database read batch size.")
	commonPprofFlag(flags, &pprofListen)
	if err := flags.Parse(args); err != nil {
		return err
	}
	fillConnectionFromEnv(&databaseConfig)
	config.Directory = strings.TrimSpace(config.Directory)
	config.Observer = s.commandObserver()
	if err := config.Validate(); err != nil {
		return err
	}

	profileServer, err := startPprofServer(pprofListen, s.stderr)
	if err != nil {
		return err
	}
	defer stopPprofServer(profileServer, s.stderr)

	operations := s.operations.withDefaults()
	var result ret.VerifyDatabaseResult
	if err := func() (resultErr error) {
		database, _, err := operations.openDatabase(ctx, databaseConfig)
		if err != nil {
			return err
		}
		defer func() {
			resultErr = errors.Join(resultErr, closeProductDatabase(database))
		}()
		result, err = operations.verifyDatabase(ctx, database, config)
		return err
	}(); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout,
		"verified database: %d graph(s)\nnodes: %d\nrelationships: %d\n",
		result.GraphCount,
		result.NodeCount,
		result.RelationshipCount,
	)
	return nil
}

func (s commandRuntime) runPack(ctx context.Context, args []string) error {
	var (
		collectionDirectory string
		archivePath         string
		recipientPath       string
	)
	flags := flag.NewFlagSet("retriever pack", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&collectionDirectory, "in", "", "Input collection directory.")
	flags.StringVar(&archivePath, "archive", "", "Encrypted archive output path.")
	flags.StringVar(&recipientPath, "recipient", "", "Recipient public key path.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(collectionDirectory) == "" {
		return fmt.Errorf("collection directory is required; pass -in")
	}
	if strings.TrimSpace(archivePath) == "" {
		return fmt.Errorf("archive path is required; pass -archive")
	}
	if strings.TrimSpace(recipientPath) == "" {
		return fmt.Errorf("recipient key path is required; pass -recipient")
	}
	recipient, err := archive.ReadPublicKey(recipientPath)
	if err != nil {
		return err
	}
	config := ret.PackConfig{
		CollectionDirectory: strings.TrimSpace(collectionDirectory),
		ArchivePath:         strings.TrimSpace(archivePath),
		Recipient:           recipient,
		Observer:            s.commandObserver(),
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if err := s.operations.withDefaults().pack(ctx, config); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "archive: %s\n", config.ArchivePath)
	return nil
}

func (s commandRuntime) runUnpack(ctx context.Context, args []string) error {
	var (
		archivePath  string
		outputDir    string
		identityPath string
	)
	flags := flag.NewFlagSet("retriever unpack", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&archivePath, "archive", "", "Encrypted archive input path.")
	flags.StringVar(&outputDir, "out", "", "Output collection directory.")
	flags.StringVar(&identityPath, "identity", "", "Recipient private key path.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(archivePath) == "" {
		return fmt.Errorf("archive path is required; pass -archive")
	}
	if strings.TrimSpace(outputDir) == "" {
		return fmt.Errorf("output directory is required; pass -out")
	}
	if strings.TrimSpace(identityPath) == "" {
		return fmt.Errorf("identity key path is required; pass -identity")
	}
	identity, err := archive.ReadPrivateKey(identityPath)
	if err != nil {
		return err
	}
	config := ret.UnpackConfig{
		ArchivePath:     strings.TrimSpace(archivePath),
		OutputDirectory: strings.TrimSpace(outputDir),
		Identity:        identity,
		Observer:        s.commandObserver(),
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if err := s.operations.withDefaults().unpack(ctx, config); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "unpacked archive: %s\noutput: %s\n", config.ArchivePath, config.OutputDirectory)
	return nil
}

func (s commandRuntime) runKeygen(args []string) error {
	var config ret.KeygenConfig
	flags := flag.NewFlagSet("retriever keygen", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&config.PrivateKeyPath, "private-key", "", "Private key output path.")
	flags.StringVar(&config.PublicKeyPath, "public-key", "", "Public key output path.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if err := s.operations.withDefaults().keygen(config); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "private key: %s\npublic key: %s\n", config.PrivateKeyPath, config.PublicKeyPath)
	return nil
}

func (s commandRuntime) runBench(ctx context.Context, args []string) error {
	config, err := parseBenchCommand(args, s.stderr)
	if err != nil {
		return err
	}

	profileServer, err := startPprofServer(config.pprof, s.stderr)
	if err != nil {
		return err
	}
	defer stopPprofServer(profileServer, s.stderr)

	operations := s.operations.withDefaults()
	var report benchReport
	if err := func() (resultErr error) {
		database, driverName, err := operations.openDatabase(ctx, config.database)
		if err != nil {
			return err
		}
		defer func() {
			resultErr = errors.Join(resultErr, closeProductDatabase(database))
		}()

		graphNames, err := resolveGraphNames(ctx, database, driverName, config.graphs, config.allGraphs)
		if err != nil {
			return err
		}

		report, err = Bench(ctx, database, driverName, graphNames, config.bench)
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}
	if config.bench.JSONOutput {
		encoder := json.NewEncoder(s.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	writeBenchReport(s.stdout, report)
	return nil
}

func (s commandRuntime) commandObserver() observe.Observer {
	if s.observer != nil {
		return s.observer
	}
	return newCommandObserver(slog.Default())
}

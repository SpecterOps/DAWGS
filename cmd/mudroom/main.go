package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

const usage = `usage: mudroom <command> [options]

Commands:
  explore    Inspect graph properties and PII-like shapes without mutation.
  plan       Build a non-mutating OpenGraph scrub plan.
  dry-run    Write a scrub plan and manifest without graph output.
  scrub      Scrub an OpenGraph JSON file and write a manifest.
  scrub-db   Scrub a live graph database in place.
  validate   Compare original and scrubbed OpenGraph shape.
`

type commandRuntime struct {
	stdout io.Writer
	stderr io.Writer

	openDatabase func(context.Context, DatabaseConfig) (graph.Database, string, error)
}

func newCommandRuntime(stdout, stderr io.Writer) commandRuntime {
	return commandRuntime{
		stdout:       stdout,
		stderr:       stderr,
		openDatabase: openDatabase,
	}
}

func main() {
	runtime := newCommandRuntime(os.Stdout, os.Stderr)
	if err := runtime.run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "mudroom: %v\n", err)
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
	case "explore":
		return s.runExplore(ctx, args[1:])
	case "plan":
		return s.runPlan(args[1:])
	case "dry-run":
		return s.runDryRun(args[1:])
	case "scrub":
		return s.runScrub(args[1:])
	case "scrub-db":
		return s.runScrubDatabase(ctx, args[1:])
	case "validate":
		return s.runValidate(args[1:])
	default:
		fmt.Fprint(s.stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (s commandRuntime) runExplore(ctx context.Context, args []string) error {
	var (
		configPath      string
		driver          string
		connection      string
		graphName       string
		nodeSampleLimit int
		edgeSampleLimit int
		jsonOutput      bool
	)

	flags := flag.NewFlagSet("mudroom explore", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&configPath, "config", defaultConfigPath(), "Optional mudroom TOML config path.")
	flags.StringVar(&driver, "driver", "", "Graph database driver. Overrides config.")
	flags.StringVar(&connection, "connection", "", "Graph database connection string. Overrides config and CONNECTION_STRING. This value is never printed.")
	flags.StringVar(&graphName, "graph", "", "Graph target name override.")
	flags.IntVar(&nodeSampleLimit, "node-sample", 0, "Maximum hydrated nodes to sample for property analysis override.")
	flags.IntVar(&edgeSampleLimit, "edge-sample", 0, "Maximum hydrated relationships to sample for property analysis override.")
	flags.BoolVar(&jsonOutput, "json", false, "Emit JSON instead of Markdown.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := loadCommandConfig(configPath)
	if err != nil {
		return err
	}
	applyExploreOverrides(&cfg, visitedFlags(flags), exploreOverrides{
		Driver:                  driver,
		Connection:              connection,
		Graph:                   graphName,
		NodeSampleLimit:         nodeSampleLimit,
		RelationshipSampleLimit: edgeSampleLimit,
	})

	classifier, err := cfg.Classifier.Compile()
	if err != nil {
		return err
	}

	db, driverName, err := s.openDatabase(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	report, err := ExploreDatabase(ctx, db, ExploreOptions{
		Driver:                  driverName,
		Graph:                   cfg.Database.Graph,
		NodeSampleLimit:         cfg.Exploration.NodeSampleLimit,
		RelationshipSampleLimit: cfg.Exploration.RelationshipSampleLimit,
		HistogramLimit:          cfg.Exploration.HistogramLimit,
		Classifier:              classifier,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		encoder := json.NewEncoder(s.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	WriteMarkdown(s.stdout, report)
	return nil
}

func (s commandRuntime) runPlan(args []string) error {
	var (
		configPath string
		inputPath  string
		planPath   string
	)

	flags := flag.NewFlagSet("mudroom plan", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&configPath, "config", defaultConfigPath(), "Optional mudroom TOML config path.")
	flags.StringVar(&inputPath, "input", "", "OpenGraph JSON input path.")
	flags.StringVar(&planPath, "output", "", "Scrub plan JSON output path.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := loadCommandConfig(configPath)
	if err != nil {
		return err
	}
	applyPlanOverrides(&cfg, visitedFlags(flags), planOverrides{Input: inputPath, Plan: planPath})

	if strings.TrimSpace(cfg.OpenGraph.Input) == "" {
		return fmt.Errorf("OpenGraph input path is required; pass -input or configure opengraph.input")
	}
	if strings.TrimSpace(cfg.OpenGraph.Plan) == "" {
		return fmt.Errorf("scrub plan output path is required; pass -output or configure opengraph.plan")
	}

	plan, err := buildPlan(cfg)
	if err != nil {
		return err
	}
	if err := writePlan(cfg.OpenGraph.Plan, plan); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "planned %d nodes and %d relationships\nplan: %s\n", plan.NodeCount, plan.RelationshipCount, cfg.OpenGraph.Plan)
	return nil
}

func (s commandRuntime) runDryRun(args []string) error {
	var (
		configPath   string
		inputPath    string
		planPath     string
		manifestPath string
		salt         string
	)

	flags := flag.NewFlagSet("mudroom dry-run", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&configPath, "config", defaultConfigPath(), "Optional mudroom TOML config path.")
	flags.StringVar(&inputPath, "input", "", "OpenGraph JSON input path.")
	flags.StringVar(&planPath, "plan", "", "Scrub plan JSON output path. Defaults to <input>.plan.json.")
	flags.StringVar(&manifestPath, "manifest", "", "Manifest output path. Defaults to <input>.manifest.json.")
	flags.StringVar(&salt, "salt", "", "Optional scrub salt. Presence is recorded, but the value is never written.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := loadCommandConfig(configPath)
	if err != nil {
		return err
	}
	applyPlanOverrides(&cfg, visitedFlags(flags), planOverrides{
		Input:    inputPath,
		Plan:     planPath,
		Manifest: manifestPath,
		Salt:     salt,
	})

	if strings.TrimSpace(cfg.OpenGraph.Input) == "" {
		return fmt.Errorf("OpenGraph input path is required; pass -input or configure opengraph.input")
	}
	if strings.TrimSpace(cfg.OpenGraph.Plan) == "" {
		cfg.OpenGraph.Plan = cfg.OpenGraph.Input + ".plan.json"
	}
	if strings.TrimSpace(cfg.OpenGraph.Manifest) == "" {
		cfg.OpenGraph.Manifest = cfg.OpenGraph.Input + ".manifest.json"
	}

	plan, err := buildPlan(cfg)
	if err != nil {
		return err
	}
	if err := writePlan(cfg.OpenGraph.Plan, plan); err != nil {
		return err
	}
	if err := writeManifest(cfg.OpenGraph.Manifest, PlanManifest(plan, strings.TrimSpace(cfg.Scrub.Salt) != "")); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "planned %d nodes and %d relationships\nplan: %s\nmanifest: %s\n", plan.NodeCount, plan.RelationshipCount, cfg.OpenGraph.Plan, cfg.OpenGraph.Manifest)
	return nil
}

func (s commandRuntime) runScrub(args []string) error {
	var (
		configPath   string
		inputPath    string
		outputPath   string
		manifestPath string
		salt         string
	)

	flags := flag.NewFlagSet("mudroom scrub", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&configPath, "config", defaultConfigPath(), "Optional mudroom TOML config path.")
	flags.StringVar(&inputPath, "input", "", "OpenGraph JSON input path.")
	flags.StringVar(&outputPath, "output", "", "Scrubbed OpenGraph JSON output path.")
	flags.StringVar(&manifestPath, "manifest", "", "Manifest output path. Defaults to <output>.manifest.json.")
	flags.StringVar(&salt, "salt", "", "Scrub salt. Overrides config and is never written to output.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := loadCommandConfig(configPath)
	if err != nil {
		return err
	}
	applyScrubOverrides(&cfg, visitedFlags(flags), scrubOverrides{
		Input:    inputPath,
		Output:   outputPath,
		Manifest: manifestPath,
		Salt:     salt,
	})

	if strings.TrimSpace(cfg.OpenGraph.Input) == "" {
		return fmt.Errorf("OpenGraph input path is required; pass -input or configure opengraph.input")
	}
	if strings.TrimSpace(cfg.OpenGraph.Output) == "" {
		return fmt.Errorf("OpenGraph output path is required; pass -output or configure opengraph.output")
	}
	if strings.TrimSpace(cfg.OpenGraph.Manifest) == "" {
		cfg.OpenGraph.Manifest = cfg.OpenGraph.Output + ".manifest.json"
	}

	scrubber, err := NewScrubber(cfg)
	if err != nil {
		return err
	}
	inputFile, err := os.Open(cfg.OpenGraph.Input)
	if err != nil {
		return fmt.Errorf("open OpenGraph input: %w", err)
	}
	defer inputFile.Close()

	outputFile, err := os.OpenFile(cfg.OpenGraph.Output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open OpenGraph output: %w", err)
	}
	manifest, scrubErr := scrubber.ScrubOpenGraph(inputFile, outputFile)
	closeErr := outputFile.Close()
	if scrubErr != nil {
		return scrubErr
	}
	if closeErr != nil {
		return fmt.Errorf("close OpenGraph output: %w", closeErr)
	}
	if err := writeManifest(cfg.OpenGraph.Manifest, manifest); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "scrubbed %d nodes and %d relationships\noutput: %s\nmanifest: %s\n", manifest.NodeCount, manifest.RelationshipCount, cfg.OpenGraph.Output, cfg.OpenGraph.Manifest)
	return nil
}

func (s commandRuntime) runScrubDatabase(ctx context.Context, args []string) error {
	var (
		configPath        string
		driver            string
		connection        string
		graphName         string
		salt              string
		manifestPath      string
		batchSize         int
		confirmMutation   bool
		progressFrequency int
	)

	flags := flag.NewFlagSet("mudroom scrub-db", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&configPath, "config", defaultConfigPath(), "Optional mudroom TOML config path.")
	flags.StringVar(&driver, "driver", "", "Graph database driver. Overrides config.")
	flags.StringVar(&connection, "connection", "", "Graph database connection string. Overrides config and CONNECTION_STRING. This value is never printed.")
	flags.StringVar(&graphName, "graph", "", "Graph target name override.")
	flags.StringVar(&salt, "salt", "", "Scrub salt. Overrides config and is never written to output.")
	flags.StringVar(&manifestPath, "manifest", "", "Manifest output path. Defaults to mudroom-db.manifest.json.")
	flags.IntVar(&batchSize, "batch-size", defaultDatabaseScrubBatchSize, "Number of graph entities to process per batch.")
	flags.IntVar(&progressFrequency, "progress-frequency", 1, "Emit progress every N completed batches.")
	flags.BoolVar(&confirmMutation, "confirm-source-mutation", false, "Required confirmation for in-place database mutation.")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if !confirmMutation {
		return fmt.Errorf("source graph mutation requires -confirm-source-mutation")
	}
	if batchSize <= 0 {
		return fmt.Errorf("batch-size must be > 0")
	}
	if progressFrequency <= 0 {
		progressFrequency = 1
	}

	cfg, err := loadCommandConfig(configPath)
	if err != nil {
		return err
	}
	applyDatabaseOverrides(&cfg, visitedFlags(flags), databaseOverrides{
		Driver:     driver,
		Connection: connection,
		Graph:      graphName,
		Salt:       salt,
		Manifest:   manifestPath,
	})
	if strings.TrimSpace(cfg.OpenGraph.Manifest) == "" {
		cfg.OpenGraph.Manifest = "mudroom-db.manifest.json"
	}

	scrubber, err := NewScrubber(cfg)
	if err != nil {
		return err
	}
	db, _, err := s.openDatabase(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	progressCounts := map[PlanEntity]int{}
	summary, err := scrubber.ScrubDatabase(ctx, db, DatabaseScrubOptions{
		Graph:     cfg.Database.Graph,
		AllGraphs: cfg.Database.AllGraphs,
		BatchSize: batchSize,
		Progress: func(progress DatabaseScrubProgress) {
			progressCounts[progress.Entity] += 1
			if progressCounts[progress.Entity]%progressFrequency == 0 || progress.Processed == progress.Total {
				fmt.Fprintf(s.stderr, "%s: processed %d/%d updated %d\n", progress.Entity, progress.Processed, progress.Total, progress.Updated)
			}
		},
	})
	if err != nil {
		return err
	}
	if err := writeManifest(cfg.OpenGraph.Manifest, summary.Manifest); err != nil {
		return err
	}
	fmt.Fprintf(s.stdout, "scrubbed database graph %q\nnodes: %d processed, %d updated\nrelationships: %d processed, %d updated\nmanifest: %s\n", cfg.Database.Graph, summary.Manifest.NodeCount, summary.NodeUpdates, summary.Manifest.RelationshipCount, summary.RelationshipUpdates, cfg.OpenGraph.Manifest)
	return nil
}

func (s commandRuntime) runValidate(args []string) error {
	var (
		originalPath string
		scrubbedPath string
		outputPath   string
	)

	flags := flag.NewFlagSet("mudroom validate", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	flags.StringVar(&originalPath, "original", "", "Original OpenGraph JSON path.")
	flags.StringVar(&scrubbedPath, "scrubbed", "", "Scrubbed OpenGraph JSON path.")
	flags.StringVar(&outputPath, "output", "", "Optional validation JSON output path. Defaults to stdout.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(originalPath) == "" {
		return fmt.Errorf("original OpenGraph path is required; pass -original")
	}
	if strings.TrimSpace(scrubbedPath) == "" {
		return fmt.Errorf("scrubbed OpenGraph path is required; pass -scrubbed")
	}

	originalFile, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("open original OpenGraph input: %w", err)
	}
	defer originalFile.Close()
	scrubbedFile, err := os.Open(scrubbedPath)
	if err != nil {
		return fmt.Errorf("open scrubbed OpenGraph input: %w", err)
	}
	defer scrubbedFile.Close()

	result, err := ValidateOpenGraphReaders(originalFile, scrubbedFile)
	if err != nil {
		return err
	}
	if strings.TrimSpace(outputPath) != "" {
		if err := writeValidation(outputPath, result); err != nil {
			return err
		}
	} else if err := writeJSON(s.stdout, result); err != nil {
		return err
	}
	if !result.Valid {
		return fmt.Errorf("OpenGraph shape validation failed with %d findings", len(result.Findings))
	}
	return nil
}

func defaultConfigPath() string {
	for _, name := range []string{"MUDROOM_CONFIG", "GRAPH_SCRUBBER_CONFIG"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func loadCommandConfig(path string) (Config, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(cfg.Database.Connection) == "" {
		cfg.Database.Connection = strings.TrimSpace(os.Getenv("CONNECTION_STRING"))
	}
	return cfg, nil
}

func buildPlan(cfg Config) (Plan, error) {
	planner, err := NewPlanner(cfg)
	if err != nil {
		return Plan{}, err
	}
	inputFile, err := os.Open(cfg.OpenGraph.Input)
	if err != nil {
		return Plan{}, fmt.Errorf("open OpenGraph input: %w", err)
	}
	defer inputFile.Close()
	return planner.PlanOpenGraph(inputFile)
}

func writePlan(path string, plan Plan) error {
	return writeJSONFile(path, plan, "scrub plan")
}

func writeManifest(path string, manifest Manifest) error {
	return writeJSONFile(path, manifest, "scrub manifest")
}

func writeValidation(path string, result ValidationResult) error {
	return writeJSONFile(path, result, "validation result")
}

func writeJSONFile(path string, value any, description string) error {
	contents, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", description, err)
	}
	contents = append(contents, '\n')
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", description, err)
	}
	return nil
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func openDatabase(ctx context.Context, cfg DatabaseConfig) (graph.Database, string, error) {
	connection := strings.TrimSpace(cfg.Connection)
	if connection == "" {
		return nil, "", fmt.Errorf("database connection is required; pass -connection, configure database.connection, or set CONNECTION_STRING")
	}

	driverName := strings.TrimSpace(cfg.Driver)
	if driverName == "" {
		var err error
		driverName, err = driverFromConnectionString(connection)
		if err != nil {
			return nil, "", err
		}
	}

	openConfig := dawgs.Config{
		ConnectionString:      connection,
		GraphQueryMemoryLimit: size.Gibibyte,
	}

	poolOwnedByDriver := false
	switch driverName {
	case pg.DriverName:
		poolCfg, err := pgxpool.ParseConfig(connection)
		if err != nil {
			return nil, "", fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
		}
		pool, err := pg.NewPool(poolCfg)
		if err != nil {
			return nil, "", fmt.Errorf("open PostgreSQL pool: %w", err)
		}
		defer func() {
			if !poolOwnedByDriver {
				pool.Close()
			}
		}()
		openConfig.Pool = pool
	case neo4j.DriverName:
	default:
		return nil, "", fmt.Errorf("unsupported driver %q; expected %s or %s", driverName, pg.DriverName, neo4j.DriverName)
	}

	db, err := dawgs.Open(ctx, driverName, openConfig)
	if err != nil {
		return nil, "", fmt.Errorf("open %s database: %w", driverName, err)
	}
	poolOwnedByDriver = true

	openSuccess := false
	defer func() {
		if !openSuccess {
			_ = db.Close(ctx)
		}
	}()

	graphName := strings.TrimSpace(cfg.Graph)
	if graphName != "" {
		if err := db.SetDefaultGraph(ctx, graph.Graph{Name: graphName}); err != nil {
			return nil, "", fmt.Errorf("set graph target %q: %w", graphName, err)
		}
	}

	openSuccess = true
	return db, driverName, nil
}

func driverFromConnectionString(connection string) (string, error) {
	parsedURL, err := url.Parse(connection)
	if err != nil {
		return "", fmt.Errorf("parse connection string: %w", err)
	}
	switch strings.ToLower(parsedURL.Scheme) {
	case "postgres", "postgresql":
		return pg.DriverName, nil
	case neo4j.DriverName, "neo4j+s", "neo4j+ssc":
		return neo4j.DriverName, nil
	default:
		return "", fmt.Errorf("unknown connection string scheme %q; expected postgres/postgresql or neo4j", parsedURL.Scheme)
	}
}

func visitedFlags(flags *flag.FlagSet) map[string]bool {
	visited := map[string]bool{}
	flags.Visit(func(nextFlag *flag.Flag) {
		visited[nextFlag.Name] = true
	})
	return visited
}

type exploreOverrides struct {
	Driver                  string
	Connection              string
	Graph                   string
	NodeSampleLimit         int
	RelationshipSampleLimit int
}

func applyExploreOverrides(cfg *Config, flags map[string]bool, values exploreOverrides) {
	applyDatabaseOverrides(cfg, flags, databaseOverrides{
		Driver:     values.Driver,
		Connection: values.Connection,
		Graph:      values.Graph,
	})
	if flags["node-sample"] {
		cfg.Exploration.NodeSampleLimit = values.NodeSampleLimit
	}
	if flags["edge-sample"] {
		cfg.Exploration.RelationshipSampleLimit = values.RelationshipSampleLimit
	}
}

type planOverrides struct {
	Input    string
	Plan     string
	Manifest string
	Salt     string
}

func applyPlanOverrides(cfg *Config, flags map[string]bool, values planOverrides) {
	if flags["input"] {
		cfg.OpenGraph.Input = strings.TrimSpace(values.Input)
	}
	if flags["output"] || flags["plan"] {
		cfg.OpenGraph.Plan = strings.TrimSpace(values.Plan)
	}
	if flags["manifest"] {
		cfg.OpenGraph.Manifest = strings.TrimSpace(values.Manifest)
	}
	if flags["salt"] {
		cfg.Scrub.Salt = strings.TrimSpace(values.Salt)
	}
}

type scrubOverrides struct {
	Input    string
	Output   string
	Manifest string
	Salt     string
}

func applyScrubOverrides(cfg *Config, flags map[string]bool, values scrubOverrides) {
	if flags["input"] {
		cfg.OpenGraph.Input = strings.TrimSpace(values.Input)
	}
	if flags["output"] {
		cfg.OpenGraph.Output = strings.TrimSpace(values.Output)
	}
	if flags["manifest"] {
		cfg.OpenGraph.Manifest = strings.TrimSpace(values.Manifest)
	}
	if flags["salt"] {
		cfg.Scrub.Salt = strings.TrimSpace(values.Salt)
	}
}

type databaseOverrides struct {
	Driver     string
	Connection string
	Graph      string
	Salt       string
	Manifest   string
}

func applyDatabaseOverrides(cfg *Config, flags map[string]bool, values databaseOverrides) {
	if flags["driver"] {
		cfg.Database.Driver = strings.TrimSpace(values.Driver)
	}
	if flags["connection"] {
		cfg.Database.Connection = strings.TrimSpace(values.Connection)
	}
	if flags["graph"] {
		cfg.Database.Graph = strings.TrimSpace(values.Graph)
		cfg.Database.AllGraphs = false
	}
	if flags["salt"] {
		cfg.Scrub.Salt = strings.TrimSpace(values.Salt)
	}
	if flags["manifest"] {
		cfg.OpenGraph.Manifest = strings.TrimSpace(values.Manifest)
	}
}

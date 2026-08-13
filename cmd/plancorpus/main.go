package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/specterops/dawgs/testutil"
)

// commandConfig contains plancorpus command-line inputs and output selections.
type commandConfig struct {
	// DatasetDir locates fixture datasets loaded before plan capture.
	DatasetDir string
	// OutputDir selects the directory that receives captured plans and summaries.
	OutputDir string
	// SummaryMarkdown selects the Markdown plan-summary destination.
	SummaryMarkdown string
	// SummaryJSON selects the JSON summary destination.
	SummaryJSON string
	// PlanDeltaJSON selects the versioned paired plan-delta destination.
	PlanDeltaJSON string
	// Connection contains the backend connection string.
	Connection string
	// PGConnection contains the PostgreSQL connection string.
	PGConnection string
	// Neo4jConnection contains the Neo4j connection string.
	Neo4jConnection string
	// TopPlans limits expensive PostgreSQL plans included in the summary.
	TopPlans int
	// DAWGSVersion records the DAWGS source version attached to artifact provenance.
	DAWGSVersion string
}

// main runs the plancorpus command.
func main() {
	cfg := commandConfig{}
	flag.StringVar(&cfg.DatasetDir, "dataset-dir", "integration/testdata", "integration testdata directory")
	flag.StringVar(&cfg.OutputDir, "output-dir", ".coverage", "directory for JSONL plan captures")
	flag.StringVar(&cfg.SummaryMarkdown, "summary", "", "markdown summary path (default: output-dir/plan-corpus-summary.md)")
	flag.StringVar(&cfg.SummaryJSON, "summary-json", "", "JSON summary path (default: output-dir/plan-corpus-summary.json)")
	flag.StringVar(&cfg.PlanDeltaJSON, "plan-delta-json", "", "paired semantic plan-delta path (default: output-dir/plan-corpus-delta.json)")
	flag.StringVar(&cfg.Connection, "connection", os.Getenv("CONNECTION_STRING"), "single backend connection string")
	flag.StringVar(&cfg.PGConnection, "pg-connection", os.Getenv("PG_CONNECTION_STRING"), "PostgreSQL connection string")
	flag.StringVar(&cfg.Neo4jConnection, "neo4j-connection", os.Getenv("NEO4J_CONNECTION_STRING"), "Neo4j connection string")
	flag.IntVar(&cfg.TopPlans, "top", defaultTopPlans, "number of expensive PostgreSQL plans to include in summaries")
	flag.StringVar(&cfg.DAWGSVersion, "dawgs-version", "", "DAWGS source version (auto-detected when empty)")
	flag.Parse()

	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "plancorpus: %v\n", err)
		os.Exit(1)
	}
}

// run captures plans for each configured backend and writes aggregate summaries.
func run(ctx context.Context, cfg commandConfig) error {
	specs, err := captureSpecs(cfg)
	if err != nil {
		return err
	}

	suite, err := loadCorpus(cfg.DatasetDir)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	var allRecords []PlanRecord
	metadata := testutil.ResolveBaselineMetadata(cfg.DAWGSVersion)
	for _, spec := range specs {
		records, err := captureCorpus(ctx, cfg.DatasetDir, suite, spec)
		if err != nil {
			return err
		}

		for idx := range records {
			records[idx].Metadata = metadata
		}

		outputPath := filepath.Join(cfg.OutputDir, "plan-corpus-"+spec.DriverName+".jsonl")
		if err := writePlanRecords(outputPath, records); err != nil {
			return err
		}

		fmt.Fprintf(os.Stderr, "captured %d %s records in %s\n", len(records), spec.DriverName, outputPath)
		allRecords = append(allRecords, records...)
	}

	summary := buildSummary(allRecords, cfg.TopPlans)
	if cfg.SummaryMarkdown == "" {
		cfg.SummaryMarkdown = filepath.Join(cfg.OutputDir, "plan-corpus-summary.md")
	}
	if cfg.SummaryJSON == "" {
		cfg.SummaryJSON = filepath.Join(cfg.OutputDir, "plan-corpus-summary.json")
	}

	if err := writeSummaryFiles(cfg.SummaryMarkdown, cfg.SummaryJSON, summary); err != nil {
		return err
	}
	planDelta, err := buildPlanDeltaReport(allRecords)
	if err != nil {
		return err
	}
	if cfg.PlanDeltaJSON == "" {
		cfg.PlanDeltaJSON = filepath.Join(cfg.OutputDir, "plan-corpus-delta.json")
	}
	if err := writePlanDeltaReport(cfg.PlanDeltaJSON, planDelta); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote summaries to %s and %s\n", cfg.SummaryMarkdown, cfg.SummaryJSON)
	fmt.Fprintf(os.Stderr, "wrote paired plan delta to %s\n", cfg.PlanDeltaJSON)
	return nil
}

// captureSpecs validates connection inputs and returns one deterministic capture specification per driver.
func captureSpecs(cfg commandConfig) ([]captureSpec, error) {
	specsByDriver := map[string]captureSpec{}

	if cfg.Connection != "" {
		driverName, err := driverFromConnectionString(cfg.Connection)
		if err != nil {
			return nil, err
		}
		specsByDriver[driverName] = captureSpec{
			DriverName: driverName,
			Connection: cfg.Connection,
		}
	}

	if cfg.PGConnection != "" {
		specsByDriver[pgDriverName()] = captureSpec{
			DriverName: pgDriverName(),
			Connection: cfg.PGConnection,
		}
	}
	if cfg.Neo4jConnection != "" {
		specsByDriver[neo4jDriverName()] = captureSpec{
			DriverName: neo4jDriverName(),
			Connection: cfg.Neo4jConnection,
		}
	}

	if len(specsByDriver) == 0 {
		return nil, fmt.Errorf("no connection string supplied; set CONNECTION_STRING or PG_CONNECTION_STRING/NEO4J_CONNECTION_STRING")
	}

	var (
		orderedDrivers = []string{pgDriverName(), neo4jDriverName()}
		specs          = make([]captureSpec, 0, len(specsByDriver))
	)

	for _, driverName := range orderedDrivers {
		if spec, found := specsByDriver[driverName]; found {
			specs = append(specs, spec)
		}
	}
	return specs, nil
}

// pgDriverName returns the registered driver name for PostgreSQL connections.
func pgDriverName() string {
	return "pg"
}

// neo4jDriverName returns the registered driver name for Neo4j connections.
func neo4jDriverName() string {
	return "neo4j"
}

// writePlanRecords creates a JSON Lines artifact and writes every captured plan record to it.
func writePlanRecords(path string, records []PlanRecord) error {
	out, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}

	return writePlanRecordsTo(out, path, records)
}

// writePlanRecordsTo encodes plan records as JSON Lines and reports both encode and close failures.
func writePlanRecordsTo(out io.WriteCloser, path string, records []PlanRecord) error {
	encoder := json.NewEncoder(out)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			if closeErr := out.Close(); closeErr != nil {
				return fmt.Errorf("write %s: %w; close %s: %w", path, err, path, closeErr)
			}
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// writeSummaryFiles writes the requested Markdown and JSON plan summaries and closes each output.
func writeSummaryFiles(markdownPath, jsonPath string, summary PlanSummary) error {
	if markdownPath != "" {
		out, err := os.Create(markdownPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", markdownPath, err)
		}
		if err := writeMarkdownSummary(out, summary); err != nil {
			_ = out.Close()
			return fmt.Errorf("write %s: %w", markdownPath, err)
		}
		if err := out.Close(); err != nil {
			return fmt.Errorf("close %s: %w", markdownPath, err)
		}
	}

	if jsonPath != "" {
		out, err := os.Create(jsonPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", jsonPath, err)
		}
		if err := writeJSONSummary(out, summary); err != nil {
			_ = out.Close()
			return fmt.Errorf("write %s: %w", jsonPath, err)
		}
		if err := out.Close(); err != nil {
			return fmt.Errorf("close %s: %w", jsonPath, err)
		}
	}

	return nil
}

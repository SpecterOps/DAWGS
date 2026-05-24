package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

type commandConfig struct {
	DatasetDir      string
	OutputDir       string
	SummaryMarkdown string
	SummaryJSON     string
	Connection      string
	PGConnection    string
	Neo4jConnection string
	TopPlans        int
}

func main() {
	cfg := commandConfig{}
	flag.StringVar(&cfg.DatasetDir, "dataset-dir", "integration/testdata", "integration testdata directory")
	flag.StringVar(&cfg.OutputDir, "output-dir", ".coverage", "directory for JSONL plan captures")
	flag.StringVar(&cfg.SummaryMarkdown, "summary", "", "markdown summary path (default: output-dir/plan-corpus-summary.md)")
	flag.StringVar(&cfg.SummaryJSON, "summary-json", "", "JSON summary path (default: output-dir/plan-corpus-summary.json)")
	flag.StringVar(&cfg.Connection, "connection", os.Getenv("CONNECTION_STRING"), "single backend connection string")
	flag.StringVar(&cfg.PGConnection, "pg-connection", os.Getenv("PG_CONNECTION_STRING"), "PostgreSQL connection string")
	flag.StringVar(&cfg.Neo4jConnection, "neo4j-connection", os.Getenv("NEO4J_CONNECTION_STRING"), "Neo4j connection string")
	flag.IntVar(&cfg.TopPlans, "top", defaultTopPlans, "number of expensive PostgreSQL plans to include in summaries")
	flag.Parse()

	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "plancorpus: %v\n", err)
		os.Exit(1)
	}
}

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
	for _, spec := range specs {
		records, err := captureCorpus(ctx, cfg.DatasetDir, suite, spec)
		if err != nil {
			return err
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
	fmt.Fprintf(os.Stderr, "wrote summaries to %s and %s\n", cfg.SummaryMarkdown, cfg.SummaryJSON)
	return nil
}

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

func pgDriverName() string {
	return "pg"
}

func neo4jDriverName() string {
	return "neo4j"
}

func writePlanRecords(path string, records []PlanRecord) error {
	out, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer out.Close()

	encoder := json.NewEncoder(out)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return nil
}

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

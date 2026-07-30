package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

func TestParseDumpDefaultsToJSONLZstdWithoutParquet(t *testing.T) {
	config, err := parseDumpCommand([]string{"-out", t.TempDir()}, testFlagOutput(t))
	if err != nil {
		t.Fatalf("parse dump: %v", err)
	}
	if !config.dump.JSONL.Enabled {
		t.Fatal("JSONL is disabled by default")
	}
	if config.dump.JSONL.Codec != jsonl.CodecZstd {
		t.Fatalf("JSONL codec = %q, want %q", config.dump.JSONL.Codec, jsonl.CodecZstd)
	}
	if config.dump.JSONL.Level != 0 {
		t.Fatalf("JSONL level = %d, want package default 0", config.dump.JSONL.Level)
	}
	if config.dump.Parquet.Enabled {
		t.Fatal("Parquet is enabled by default")
	}
	if config.dump.Scrub != nil {
		t.Fatalf("scrubbing is enabled by default: %+v", config.dump.Scrub)
	}
}

func TestParseDumpKeepsJSONLAndParquetIndependent(t *testing.T) {
	config, err := parseDumpCommand([]string{
		"-out", t.TempDir(),
		"-jsonl=false",
		"-parquet",
	}, testFlagOutput(t))
	if err != nil {
		t.Fatalf("parse dump: %v", err)
	}
	if config.dump.JSONL.Enabled {
		t.Fatal("JSONL remained enabled")
	}
	if !config.dump.Parquet.Enabled {
		t.Fatal("Parquet was not enabled")
	}
}

func TestParseDumpRejectsInvalidIndependentJSONLConfig(t *testing.T) {
	_, err := parseDumpCommand([]string{
		"-out", t.TempDir(),
		"-jsonl-compression", "zip",
	}, testFlagOutput(t))
	if err == nil {
		t.Fatal("expected invalid JSONL codec")
	}
}

func TestParseDumpEnablesScrubbingWithRuntimeSalt(t *testing.T) {
	config, err := parseDumpCommand([]string{
		"-out", t.TempDir(),
		"-scrub", "full",
		"-salt", "runtime-salt",
	}, testFlagOutput(t))
	if err != nil {
		t.Fatalf("parse dump: %v", err)
	}
	if config.dump.Scrub == nil {
		t.Fatal("scrubbing remained disabled")
	}
	if config.dump.Scrub.Salt != "runtime-salt" {
		t.Fatalf("scrub salt = %q", config.dump.Scrub.Salt)
	}
	if !reflect.DeepEqual(config.dump.Scrub.Rules, scrub.DefaultConfig().Rules) {
		t.Fatalf("scrub rules differ from defaults: %+v", config.dump.Scrub.Rules)
	}
}

func TestParseDumpReadsDirectScrubPolicyAndOverridesFileSaltAtRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scrub.toml")
	if err := os.WriteFile(path, []byte(`
salt = "must-not-load"
fake_domain = "scrub.example"

[graph_rules]
domain_kind = "CustomDomain"

[classifier]
long_text_threshold = 8
`), 0o600); err != nil {
		t.Fatalf("write scrub config: %v", err)
	}

	config, err := parseDumpCommand([]string{
		"-out", t.TempDir(),
		"-scrub", "full",
		"-salt", "runtime-salt",
		"-config", path,
	}, testFlagOutput(t))
	if err != nil {
		t.Fatalf("parse dump: %v", err)
	}
	if config.dump.Scrub == nil {
		t.Fatal("scrubbing remained disabled")
	}
	if config.dump.Scrub.Salt != "runtime-salt" {
		t.Fatalf("scrub salt = %q", config.dump.Scrub.Salt)
	}
	if config.dump.Scrub.Rules.FakeDomain != "scrub.example" {
		t.Fatalf("fake domain = %q", config.dump.Scrub.Rules.FakeDomain)
	}
	if config.dump.Scrub.Rules.GraphRules.DomainKind != "CustomDomain" {
		t.Fatalf("domain kind = %q", config.dump.Scrub.Rules.GraphRules.DomainKind)
	}
	if config.dump.Scrub.Rules.Classifier.LongTextThreshold != 8 {
		t.Fatalf("long text threshold = %d", config.dump.Scrub.Rules.Classifier.LongTextThreshold)
	}
}

func TestParseWorkerList(t *testing.T) {
	workers, err := parseWorkerList("1,2,4,2")
	if err != nil {
		t.Fatalf("parse worker list: %v", err)
	}
	if got, want := len(workers), 3; got != want {
		t.Fatalf("worker count length = %d, want %d", got, want)
	}
	if workers[0] != 1 || workers[1] != 2 || workers[2] != 4 {
		t.Fatalf("unexpected workers: %v", workers)
	}
	if _, err := parseWorkerList("0"); err == nil {
		t.Fatal("expected invalid worker count error")
	}
}

func TestFlagListTypes(t *testing.T) {
	var graphs stringList
	if err := graphs.Set(" default "); err != nil {
		t.Fatalf("set graph: %v", err)
	}
	if err := graphs.Set(""); err == nil {
		t.Fatal("expected empty graph error")
	}
	if graphs.String() != "default" {
		t.Fatalf("graph list string = %q", graphs.String())
	}

	var workers workerList
	if err := workers.Set("2,4"); err != nil {
		t.Fatalf("set workers: %v", err)
	}
	if workers.String() != "2,4" {
		t.Fatalf("worker list string = %q", workers.String())
	}
}

func TestWorkerListAppendsRepeatedFlags(t *testing.T) {
	var workers workerList
	if err := workers.Set("1,2"); err != nil {
		t.Fatalf("set initial workers: %v", err)
	}
	if err := workers.Set("2,4"); err != nil {
		t.Fatalf("set repeated workers: %v", err)
	}
	if workers.String() != "1,2,4" {
		t.Fatalf("worker list string = %q", workers.String())
	}
	if err := workers.Set("bad"); err == nil {
		t.Fatal("expected invalid worker count")
	}
	if workers.String() != "1,2,4" {
		t.Fatalf("invalid worker update changed list to %q", workers.String())
	}
}

func TestBenchOptionsValidate(t *testing.T) {
	bench := benchOptions{
		Workers:    []int{1},
		BatchSize:  1,
		SampleSize: 1,
		JSONL: jsonl.Config{
			Enabled: true,
			Codec:   jsonl.CodecZstd,
		},
	}
	if err := bench.validate(); err != nil {
		t.Fatalf("valid bench options: %v", err)
	}
	bench.Workers = nil
	if err := bench.validate(); err == nil {
		t.Fatal("expected missing workers")
	}
	bench.Workers = []int{2}
	if err := bench.validate(); err != nil {
		t.Fatalf("valid parallel bench workers: %v", err)
	}
	bench.Workers = []int{0}
	if err := bench.validate(); err == nil {
		t.Fatal("expected invalid worker count")
	}
	bench.Workers = []int{1}
	bench.SampleSize = -1
	if err := bench.validate(); err == nil {
		t.Fatal("expected invalid sample size")
	}
}

func TestParseBenchAllowsIndependentFormatSelection(t *testing.T) {
	for name, args := range map[string][]string{
		"jsonl":   {"-jsonl=true", "-parquet=false"},
		"parquet": {"-jsonl=false", "-parquet=true"},
		"both":    {"-jsonl=true", "-parquet=true"},
	} {
		t.Run(name, func(t *testing.T) {
			config, err := parseBenchCommand(args, testFlagOutput(t))
			if err != nil {
				t.Fatalf("parse bench: %v", err)
			}
			if config.bench.JSONL.Enabled != (name != "parquet") {
				t.Fatalf("JSONL enabled = %t", config.bench.JSONL.Enabled)
			}
			if config.bench.Parquet.Enabled != (name != "jsonl") {
				t.Fatalf("Parquet enabled = %t", config.bench.Parquet.Enabled)
			}
		})
	}
}

func TestParseBenchDefaultsToConcreteJSONLZstd(t *testing.T) {
	config, err := parseBenchCommand(nil, testFlagOutput(t))
	if err != nil {
		t.Fatalf("parse bench: %v", err)
	}
	if !config.bench.JSONL.Enabled || config.bench.JSONL.Codec != jsonl.CodecZstd || config.bench.JSONL.Level != 0 {
		t.Fatalf("JSONL config = %+v", config.bench.JSONL)
	}
	if config.bench.Parquet.Enabled {
		t.Fatal("Parquet enabled by default")
	}
	if len(config.bench.Workers) != 1 || config.bench.Workers[0] != 1 {
		t.Fatalf("workers = %v", config.bench.Workers)
	}
}

func TestParseBenchRejectsNeitherFormatAndInvalidJSONLConfig(t *testing.T) {
	if _, err := parseBenchCommand([]string{"-jsonl=false", "-parquet=false"}, testFlagOutput(t)); err == nil {
		t.Fatal("expected neither-format error")
	}
	if _, err := parseBenchCommand([]string{"-jsonl-compression", "zip"}, testFlagOutput(t)); err == nil {
		t.Fatal("expected invalid JSONL codec error")
	}

	config, err := parseBenchCommand([]string{
		"-jsonl=false",
		"-parquet=true",
		"-jsonl-compression", "zip",
	}, testFlagOutput(t))
	if err != nil {
		t.Fatalf("disabled JSONL config affected Parquet-only benchmark: %v", err)
	}
	if config.bench.Parquet != (parquet.Config{Enabled: true}) {
		t.Fatalf("Parquet config = %+v", config.bench.Parquet)
	}
}

func testFlagOutput(t *testing.T) *discardWriter {
	t.Helper()
	return &discardWriter{}
}

type discardWriter struct{}

func (*discardWriter) Write(value []byte) (int, error) {
	return len(value), nil
}

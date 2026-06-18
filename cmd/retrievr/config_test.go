package main

import (
	"os"
	"path/filepath"
	"testing"
)

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
		t.Fatalf("expected invalid worker count error")
	}
}

func TestDumpOptionsScrubFullRequiresSalt(t *testing.T) {
	options := dumpOptions{
		OutputDir:   t.TempDir(),
		Scrub:       scrubFull,
		Compression: compressionZstd,
		ZstdLevel:   defaultZstdLevel,
		ShardSize:   defaultShardSize,
		BatchSize:   defaultBatchSize,
	}

	if err := options.validate(); err == nil {
		t.Fatalf("expected missing salt error")
	}
}

func TestPrepareOutputDirectoryForce(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "old"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write old file: %v", err)
	}

	if err := prepareOutputDirectory(dir, false); err == nil {
		t.Fatalf("expected non-empty directory error")
	}
	if err := prepareOutputDirectory(dir, true); err != nil {
		t.Fatalf("prepare output directory with force: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read output directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected force to clear directory, found %d entries", len(entries))
	}
}

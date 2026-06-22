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

func TestFlagListTypes(t *testing.T) {
	var graphs stringList
	if err := graphs.Set(" default "); err != nil {
		t.Fatalf("set graph: %v", err)
	}
	if err := graphs.Set(""); err == nil {
		t.Fatalf("expected empty graph error")
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

func TestOptionsValidate(t *testing.T) {
	dump := dumpOptions{
		OutputDir:   t.TempDir(),
		Scrub:       scrubNone,
		Compression: compressionGzip,
		ZstdLevel:   defaultZstdLevel,
		ShardSize:   defaultShardSize,
		BatchSize:   defaultBatchSize,
	}
	if err := dump.validate(); err != nil {
		t.Fatalf("valid dump options: %v", err)
	}
	dump.Compression = compressionCodec("zip")
	if err := dump.validate(); err == nil {
		t.Fatalf("expected invalid compression")
	}
	dump.Compression = compressionGzip
	dump.ArchiveOut = filepath.Join(t.TempDir(), "dump.tar.pq")
	if err := dump.validate(); err == nil {
		t.Fatalf("expected archive recipient validation error")
	}
	dump.RecipientPath = filepath.Join(t.TempDir(), "recipient.key")
	if err := dump.validate(); err != nil {
		t.Fatalf("valid dump archive options: %v", err)
	}

	load := loadOptions{
		InputDir:  t.TempDir(),
		BatchSize: 1,
	}
	if err := load.validate(); err != nil {
		t.Fatalf("valid load options: %v", err)
	}
	load.InputDir = ""
	if err := load.validate(); err == nil {
		t.Fatalf("expected missing input dir")
	}
	load.ArchivePath = filepath.Join(t.TempDir(), "dump.tar.pq")
	if err := load.validate(); err == nil {
		t.Fatalf("expected missing identity path")
	}
	load.IdentityPath = filepath.Join(t.TempDir(), "private.key")
	if err := load.validate(); err != nil {
		t.Fatalf("valid load archive options: %v", err)
	}
	load.InputDir = t.TempDir()
	if err := load.validate(); err == nil {
		t.Fatalf("expected mutually exclusive load input error")
	}

	unpack := unpackOptions{
		ArchivePath:  filepath.Join(t.TempDir(), "dump.tar.pq"),
		IdentityPath: filepath.Join(t.TempDir(), "private.key"),
		OutputDir:    t.TempDir(),
	}
	if err := unpack.validate(); err != nil {
		t.Fatalf("valid unpack options: %v", err)
	}
	unpack.IdentityPath = ""
	if err := unpack.validate(); err == nil {
		t.Fatalf("expected missing identity path")
	}

	keygen := keygenOptions{
		PrivatePath: filepath.Join(t.TempDir(), "private.key"),
		PublicPath:  filepath.Join(t.TempDir(), "public.key"),
	}
	if err := keygen.validate(); err != nil {
		t.Fatalf("valid keygen options: %v", err)
	}
	keygen.PublicPath = keygen.PrivatePath
	if err := keygen.validate(); err == nil {
		t.Fatalf("expected duplicate key path validation error")
	}

	verify := verifyOptions{
		InputDir:  t.TempDir(),
		BatchSize: 1,
	}
	if err := verify.validate(); err != nil {
		t.Fatalf("valid verify options: %v", err)
	}
	verify.BatchSize = 0
	if err := verify.validate(); err == nil {
		t.Fatalf("expected invalid verify batch size")
	}

	bench := benchOptions{
		Workers:    []int{1},
		BatchSize:  1,
		SampleSize: 1,
		ZstdLevel:  defaultZstdLevel,
	}
	if err := bench.validate(); err != nil {
		t.Fatalf("valid bench options: %v", err)
	}
	bench.Workers = nil
	if err := bench.validate(); err == nil {
		t.Fatalf("expected missing workers")
	}
	bench.Workers = []int{2}
	if err := bench.validate(); err == nil {
		t.Fatalf("expected unsupported parallel worker error")
	}
	bench.Workers = []int{1}
	bench.SampleSize = -1
	if err := bench.validate(); err == nil {
		t.Fatalf("expected invalid sample size")
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

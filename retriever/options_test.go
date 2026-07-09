package retriever

import (
	"bytes"
	"crypto/hpke"
	"path/filepath"
	"testing"
)

func TestDumpOptionsScrubFullRequiresSalt(t *testing.T) {
	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Scrub:       ScrubFull,
		Compression: CompressionZstd,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   DefaultShardSize,
		BatchSize:   DefaultBatchSize,
	}

	if err := options.Validate(); err == nil {
		t.Fatalf("expected missing salt error")
	}
}

func TestOptionsValidate(t *testing.T) {
	dump := DumpOptions{
		OutputDir:   t.TempDir(),
		Scrub:       ScrubNone,
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   DefaultShardSize,
		BatchSize:   DefaultBatchSize,
	}
	if err := dump.Validate(); err != nil {
		t.Fatalf("valid dump options: %v", err)
	}

	dump.Compression = CompressionCodec("zip")

	if err := dump.Validate(); err == nil {
		t.Fatalf("expected invalid compression")
	}

	load := LoadOptions{
		InputDir:  t.TempDir(),
		BatchSize: 1,
	}
	if err := load.Validate(); err != nil {
		t.Fatalf("valid load options: %v", err)
	}

	load.InputDir = ""

	if err := load.Validate(); err == nil {
		t.Fatalf("expected missing input")
	}

	load.ArchiveReader = bytes.NewReader(nil)

	if err := load.Validate(); err == nil {
		t.Fatalf("expected missing archive identity")
	}

	privateKey, _, err := generateArchiveKeyPairForOptions()
	if err != nil {
		t.Fatalf("generate archive key: %v", err)
	}

	load.ArchiveIdentity = privateKey

	if err := load.Validate(); err != nil {
		t.Fatalf("valid archive load options: %v", err)
	}

	load.InputDir = t.TempDir()

	if err := load.Validate(); err == nil {
		t.Fatalf("expected mutually exclusive load input error")
	}

	unpack := UnpackOptions{
		ArchiveReader:   bytes.NewReader(nil),
		ArchiveIdentity: privateKey,
		OutputDir:       t.TempDir(),
	}
	if err := unpack.Validate(); err != nil {
		t.Fatalf("valid unpack options: %v", err)
	}

	unpack.ArchiveIdentity = nil

	if err := unpack.Validate(); err == nil {
		t.Fatalf("expected missing identity")
	}

	keygen := KeygenOptions{
		PrivatePath: filepath.Join(t.TempDir(), "private.key"),
		PublicPath:  filepath.Join(t.TempDir(), "public.key"),
	}
	if err := keygen.Validate(); err != nil {
		t.Fatalf("valid keygen options: %v", err)
	}

	keygen.PublicPath = keygen.PrivatePath

	if err := keygen.Validate(); err == nil {
		t.Fatalf("expected duplicate key path validation error")
	}

	verify := VerifyOptions{
		InputDir:  t.TempDir(),
		BatchSize: 1,
	}
	if err := verify.Validate(); err != nil {
		t.Fatalf("valid verify options: %v", err)
	}

	verify.BatchSize = 0

	if err := verify.Validate(); err == nil {
		t.Fatalf("expected invalid verify batch size")
	}
}

func TestDefaultOptions(t *testing.T) {
	dump := DefaultDumpOptions(t.TempDir())
	if dump.Scrub != ScrubNone || dump.Compression != CompressionZstd || dump.ZstdLevel != DefaultZstdLevel {
		t.Fatalf("unexpected dump defaults: %+v", dump)
	}
	if dump.ShardSize != DefaultShardSize || dump.BatchSize != DefaultBatchSize || dump.ProgressInterval != DefaultProgressInterval {
		t.Fatalf("unexpected dump sizing defaults: %+v", dump)
	}

	if err := dump.Validate(); err != nil {
		t.Fatalf("validate default dump options: %v", err)
	}

	load := DefaultLoadOptions(t.TempDir())
	if load.BatchSize != DefaultBatchSize || load.ProgressInterval != DefaultProgressInterval {
		t.Fatalf("unexpected load defaults: %+v", load)
	}

	if err := load.Validate(); err != nil {
		t.Fatalf("validate default load options: %v", err)
	}

	verify := DefaultVerifyOptions(t.TempDir())
	if verify.BatchSize != DefaultBatchSize || verify.ProgressInterval != DefaultProgressInterval {
		t.Fatalf("unexpected verify defaults: %+v", verify)
	}

	if err := verify.Validate(); err != nil {
		t.Fatalf("validate default verify options: %v", err)
	}

	unpack := DefaultUnpackOptions(t.TempDir())
	if unpack.OutputDir == "" {
		t.Fatalf("expected unpack output directory")
	}

	keygen := DefaultKeygenOptions("private.key", "public.key")
	if keygen.PrivatePath != "private.key" || keygen.PublicPath != "public.key" {
		t.Fatalf("unexpected keygen defaults: %+v", keygen)
	}
}

func TestPrepareOutputDirectoryForce(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "old"), []byte("old"))

	if err := prepareOutputDirectory(dir, false); err == nil {
		t.Fatalf("expected non-empty directory error")
	}

	if err := prepareOutputDirectory(dir, true); err != nil {
		t.Fatalf("prepare output directory with force: %v", err)
	}
}

func generateArchiveKeyPairForOptions() (hpke.PrivateKey, hpke.PublicKey, error) {
	privateKey, err := defaultArchiveKEM().GenerateKey()
	if err != nil {
		return nil, nil, err
	}

	return privateKey, privateKey.PublicKey(), nil
}

package retriever

import (
	"crypto/hpke"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	DefaultShardSize        = 100_000
	DefaultBatchSize        = 10_000
	DefaultProgressInterval = 250_000
)

const DefaultGraphName = "default"

const (
	OperationDump    = "dump"
	OperationLoad    = "load"
	OperationVerify  = "verify"
	OperationArchive = "archive"
	OperationUnpack  = "unpack"
	OperationKeygen  = "keygen"
)

type GraphTarget struct {
	Name string
}

type ProgressEvent struct {
	Operation             string
	Message               string
	Driver                string
	Graph                 string
	Phase                 Phase
	InputDir              string
	OutputDir             string
	ArchivePath           string
	GraphIndex            int
	GraphCount            int
	FileCount             int
	BatchSize             int
	ShardSize             int
	Processed             int64
	Planned               int64
	NodeCount             int64
	EdgeCount             int64
	Compression           CompressionCodec
	Scrub                 ScrubMode
	Elapsed               time.Duration
	EntitiesPerSecond     float64
	HeapAlloc             uint64
	HeapInuse             uint64
	Sys                   uint64
	NumGC                 uint32
	RSS                   uint64
	CompressedBytesRead   int64
	DecompressedBytesRead int64
	FragmentPasses        int64
}

type ProgressFunc func(ProgressEvent)

func (s ProgressFunc) emit(event ProgressEvent) {
	if s != nil {
		event.withRuntimeTelemetry()
		s(event)
	}
}

type DumpOptions struct {
	OutputDir        string
	Force            bool
	Resume           bool
	Scrub            ScrubMode
	Salt             string
	ScrubConfig      io.Reader
	Compression      CompressionCodec
	ZstdLevel        int
	ShardSize        int
	BatchSize        int
	ProgressInterval int64
	Progress         ProgressFunc
}

func DefaultDumpOptions(outputDir string) DumpOptions {
	return DumpOptions{
		OutputDir:        outputDir,
		Scrub:            ScrubNone,
		Compression:      CompressionZstd,
		ZstdLevel:        DefaultZstdLevel,
		ShardSize:        DefaultShardSize,
		BatchSize:        DefaultBatchSize,
		ProgressInterval: DefaultProgressInterval,
	}
}

func (s DumpOptions) Validate() error {
	if strings.TrimSpace(s.OutputDir) == "" {
		return ValidationError{Message: "output directory is required; pass -out"}
	}

	if s.Force && s.Resume {
		return ValidationError{Message: "force and resume are mutually exclusive"}
	}

	if err := ValidateCompression(s.Compression); err != nil {
		return err
	}

	if s.ZstdLevel <= 0 {
		return ValidationError{Message: "zstd-level must be > 0"}
	}

	if s.ShardSize <= 0 {
		return ValidationError{Message: "shard-size must be > 0"}
	}

	if s.BatchSize <= 0 {
		return ValidationError{Message: "batch-size must be > 0"}
	}

	switch s.Scrub {
	case ScrubNone:
		return nil
	case ScrubFull:
		if strings.TrimSpace(s.Salt) == "" {
			return ValidationError{Message: "-scrub full requires -salt, RETRIEVER_SCRUB_SALT, or legacy RETRIEVR_SCRUB_SALT; refusing to write scrubbed output without deterministic pseudonymization"}
		}
		return nil
	default:
		return ValidationError{Message: fmt.Sprintf("unsupported scrub mode %q", s.Scrub)}
	}
}

func (s DumpOptions) validate() error {
	return s.Validate()
}

type LoadOptions struct {
	InputDir         string
	ArchiveReader    io.Reader
	ArchiveIdentity  hpke.PrivateKey
	BatchSize        int
	ProgressInterval int64
	VerifyMetrics    bool
	Progress         ProgressFunc
}

func DefaultLoadOptions(inputDir string) LoadOptions {
	return LoadOptions{
		InputDir:         inputDir,
		BatchSize:        DefaultBatchSize,
		ProgressInterval: DefaultProgressInterval,
	}
}

func (s LoadOptions) Validate() error {
	inputDir := strings.TrimSpace(s.InputDir)
	hasArchive := s.ArchiveReader != nil

	if inputDir != "" && hasArchive {
		return ValidationError{Message: "load accepts either an input directory or archive reader, not both"}
	}

	if inputDir == "" && !hasArchive {
		return ValidationError{Message: "input directory or archive reader is required"}
	}

	if hasArchive && s.ArchiveIdentity == nil {
		return ValidationError{Message: "archive reader requires archive identity"}
	}

	if !hasArchive && s.ArchiveIdentity != nil {
		return ValidationError{Message: "archive identity requires archive reader"}
	}

	if s.BatchSize <= 0 {
		return ValidationError{Message: "batch-size must be > 0"}
	}

	return nil
}

func (s LoadOptions) validate() error {
	return s.Validate()
}

type UnpackOptions struct {
	ArchiveReader   io.Reader
	ArchiveIdentity hpke.PrivateKey
	OutputDir       string
	Force           bool
	Progress        ProgressFunc
}

func DefaultUnpackOptions(outputDir string) UnpackOptions {
	return UnpackOptions{
		OutputDir: outputDir,
	}
}

func (s UnpackOptions) Validate() error {
	if s.ArchiveReader == nil {
		return ValidationError{Message: "archive reader is required"}
	}

	if s.ArchiveIdentity == nil {
		return ValidationError{Message: "archive identity is required"}
	}

	if strings.TrimSpace(s.OutputDir) == "" {
		return ValidationError{Message: "output directory is required; pass -out"}
	}

	return nil
}

func (s UnpackOptions) validate() error {
	return s.Validate()
}

type KeygenOptions struct {
	PrivatePath string
	PublicPath  string
}

func DefaultKeygenOptions(privatePath, publicPath string) KeygenOptions {
	return KeygenOptions{
		PrivatePath: privatePath,
		PublicPath:  publicPath,
	}
}

func (s KeygenOptions) Validate() error {
	if strings.TrimSpace(s.PrivatePath) == "" {
		return ValidationError{Message: "private key path is required; pass -private"}
	}

	if strings.TrimSpace(s.PublicPath) == "" {
		return ValidationError{Message: "public key path is required; pass -public"}
	}

	if sameCleanPath(s.PrivatePath, s.PublicPath) {
		return ValidationError{Message: "private and public key paths must be different"}
	}

	return nil
}

func (s KeygenOptions) validate() error {
	return s.Validate()
}

type VerifyOptions struct {
	InputDir         string
	BatchSize        int
	ProgressInterval int64
	Progress         ProgressFunc
}

func DefaultVerifyOptions(inputDir string) VerifyOptions {
	return VerifyOptions{
		InputDir:         inputDir,
		BatchSize:        DefaultBatchSize,
		ProgressInterval: DefaultProgressInterval,
	}
}

func (s VerifyOptions) Validate() error {
	if strings.TrimSpace(s.InputDir) == "" {
		return ValidationError{Message: "input directory is required; pass -in"}
	}

	if s.BatchSize <= 0 {
		return ValidationError{Message: "batch-size must be > 0"}
	}

	return nil
}

func (s VerifyOptions) validate() error {
	return s.Validate()
}

type ValidationError struct {
	Message string
	Err     error
}

func (s ValidationError) Error() string {
	if s.Err != nil {
		return s.Message + ": " + s.Err.Error()
	}

	return s.Message
}

func (s ValidationError) Unwrap() error {
	return s.Err
}

type ArchiveOptions struct {
	Progress ProgressFunc
}

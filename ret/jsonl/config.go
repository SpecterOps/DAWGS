// Package jsonl writes and verifies concrete JSON Lines graph artifacts.
package jsonl

import (
	"compress/gzip"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const (
	// SchemaVersion identifies the JSONL artifact metadata and record layout.
	SchemaVersion = "ret-jsonl-v1"

	defaultZstdLevel = 3
	minZstdLevel     = -5
	maxZstdLevel     = 22
)

// Codec specifies the encoding used to store an artifact.
type Codec string

const (
	CodecNone Codec = "none"
	CodecGzip Codec = "gzip"
	CodecZstd Codec = "zstd"
)

// Config controls creation of JSONL artifacts. A zero compression level uses
// the package default for gzip and zstd; it is the only valid level for none.
type Config struct {
	Enabled bool
	Codec   Codec
	Level   int
}

// Validate verifies that Codec and Level can be used to write an artifact.
func (s Config) Validate() error {
	switch s.Codec {
	case CodecNone:
		if s.Level != 0 {
			return fmt.Errorf("compression level %d is invalid for codec %q", s.Level, s.Codec)
		}
	case CodecGzip:
		if s.Level != 0 && (s.Level < gzip.HuffmanOnly || s.Level > gzip.BestCompression) {
			return fmt.Errorf("gzip compression level %d is outside %d..%d", s.Level, gzip.HuffmanOnly, gzip.BestCompression)
		}
	case CodecZstd:
		if s.Level != 0 && (s.Level < minZstdLevel || s.Level > maxZstdLevel) {
			return fmt.Errorf("zstd compression level %d is outside %d..%d", s.Level, minZstdLevel, maxZstdLevel)
		}
	default:
		return fmt.Errorf("unsupported JSONL codec %q", s.Codec)
	}

	return nil
}

func (s Config) gzipLevel() int {
	if s.Level == 0 {
		return gzip.DefaultCompression
	}
	return s.Level
}

func (s Config) zstdLevel() zstd.EncoderLevel {
	level := s.Level
	if level == 0 {
		level = defaultZstdLevel
	}
	return zstd.EncoderLevelFromZstd(level)
}

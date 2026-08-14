// Package jsonl writes and verifies concrete JSON Lines graph artifacts.
package jsonl

import (
	"compress/gzip"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const (
	// SchemaVersion identifies the JSONL artifact metadata and record layout.
	SchemaVersion = "retriever-jsonl-v1"

	minZstdLevel = -5
	maxZstdLevel = 22
)

// Codec specifies the encoding used to store an artifact.
type Codec string

const (
	CodecNone Codec = "none"
	CodecGzip Codec = "gzip"
	CodecZstd Codec = "zstd"
)

type Config struct {
	Codec Codec
	Level int
}

func (s Config) validate() error {
	return validateCodecLevel(s.Codec, s.Level)
}

func (s Config) Validate() error {
	return s.validate()
}

func validateCodecLevel(codec Codec, level int) error {
	switch codec {
	case CodecNone:
		if level != 0 {
			return fmt.Errorf("compression level %d is invalid for codec %q", level, codec)
		}
	case CodecGzip:
		if level < gzip.HuffmanOnly || level > gzip.BestCompression {
			return fmt.Errorf("gzip compression level %d is outside %d..%d", level, gzip.HuffmanOnly, gzip.BestCompression)
		}
	case CodecZstd:
		if level < minZstdLevel || level > maxZstdLevel {
			return fmt.Errorf("zstd compression level %d is outside %d..%d", level, minZstdLevel, maxZstdLevel)
		}
	default:
		return fmt.Errorf("unsupported JSONL codec %q", codec)
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
		level = 3
	}
	return zstd.EncoderLevelFromZstd(level)
}

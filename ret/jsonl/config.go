// Package jsonl writes and verifies concrete JSON Lines graph artifacts.
package jsonl

import (
	"compress/gzip"
	"fmt"
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

func validateCodecLevel(codec Codec, level int) error {
	switch codec {
	case CodecNone:
	case CodecGzip:
		if level < gzip.HuffmanOnly || level > gzip.BestCompression {
			return fmt.Errorf("invalid compression level")
		}
	case CodecZstd:
		if level < minZstdLevel || level > maxZstdLevel {
			return fmt.Errorf("invalid compression level")
		}
	default:
		return fmt.Errorf("unknown codec")
	}

	return nil
}

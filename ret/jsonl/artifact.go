package jsonl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Artifact struct {
	SchemaVersion     string
	Codec             Codec
	SHA256            string
	Level             int
	Count             int64
	UncompressedBytes int64
	StoredBytes       int64
}

func (s Artifact) validate() error {
	if s.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported JSONL artifact schema %q", s.SchemaVersion)
	} else if err := validateCodecLevel(s.Codec, s.Level); err != nil {
		return fmt.Errorf("validate JSONL artifact codec: %w", err)
	} else if s.Count < 0 || s.UncompressedBytes < 0 || s.StoredBytes < 0 {
		return fmt.Errorf("JSONL artifact sizes and count must be non-negative")
	}
	decoded, err := hex.DecodeString(s.SHA256)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("JSONL artifact SHA-256 is invalid: %q", s.SHA256)
	}

	return nil
}

package parquet

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Artifact struct {
	SchemaVersion string
	SHA256        string
	Count         int64
	StoredBytes   int64
}

func (s Artifact) validate() error {
	if s.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported Parquet artifact schema %q", s.SchemaVersion)
	}
	if s.Count < 0 || s.StoredBytes < 0 {
		return fmt.Errorf("Parquet artifact size and count must be non-negative")
	}
	decoded, err := hex.DecodeString(s.SHA256)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("Parquet artifact SHA-256 is invalid: %q", s.SHA256)
	}
	return nil
}

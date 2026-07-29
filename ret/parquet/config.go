// Package parquet writes and verifies concrete Parquet graph artifacts.
package parquet

const (
	// SchemaVersion identifies the Parquet artifact metadata and row layout.
	SchemaVersion = "ret-parquet-v1"
)

// Config controls creation of Parquet artifacts.
type Config struct {
	Enabled bool
}

// Validate verifies that the configuration can be used to write an artifact.
func (Config) Validate() error {
	return nil
}

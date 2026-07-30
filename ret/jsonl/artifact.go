package jsonl

// NodeArtifact describes one stored JSONL node artifact.
type NodeArtifact struct {
	SchemaVersion, Path, Codec, SHA256    string
	Level                                 int
	Count, UncompressedBytes, StoredBytes int64
}

// RelationshipArtifact describes one stored JSONL relationship artifact.
type RelationshipArtifact struct {
	SchemaVersion, Path, Codec, SHA256    string
	Level                                 int
	Count, UncompressedBytes, StoredBytes int64
}

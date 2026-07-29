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

type artifactMetadata struct {
	schemaVersion     string
	path              string
	codec             string
	sha256            string
	level             int
	count             int64
	uncompressedBytes int64
	storedBytes       int64
}

func (s NodeArtifact) metadata() artifactMetadata {
	return artifactMetadata{
		schemaVersion: s.SchemaVersion, path: s.Path, codec: s.Codec, sha256: s.SHA256,
		level: s.Level, count: s.Count, uncompressedBytes: s.UncompressedBytes, storedBytes: s.StoredBytes,
	}
}

func (s RelationshipArtifact) metadata() artifactMetadata {
	return artifactMetadata{
		schemaVersion: s.SchemaVersion, path: s.Path, codec: s.Codec, sha256: s.SHA256,
		level: s.Level, count: s.Count, uncompressedBytes: s.UncompressedBytes, storedBytes: s.StoredBytes,
	}
}

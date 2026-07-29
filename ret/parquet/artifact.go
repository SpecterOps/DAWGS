package parquet

type NodeArtifact struct {
	SchemaVersion, Path, SHA256 string
	Count, StoredBytes          int64
}

type RelationshipArtifact struct {
	SchemaVersion, Path, SHA256 string
	Count, StoredBytes          int64
}

type artifactMetadata struct {
	schemaVersion string
	path          string
	sha256        string
	count         int64
	storedBytes   int64
}

func (s NodeArtifact) metadata() artifactMetadata {
	return artifactMetadata{
		schemaVersion: s.SchemaVersion,
		path:          s.Path,
		sha256:        s.SHA256,
		count:         s.Count,
		storedBytes:   s.StoredBytes,
	}
}

func (s RelationshipArtifact) metadata() artifactMetadata {
	return artifactMetadata{
		schemaVersion: s.SchemaVersion,
		path:          s.Path,
		sha256:        s.SHA256,
		count:         s.Count,
		storedBytes:   s.StoredBytes,
	}
}

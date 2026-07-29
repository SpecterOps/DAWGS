package ret

type DumpResult struct {
	ManifestPath      string
	GraphCount        int
	NodeCount         int64
	RelationshipCount int64
}

type LoadResult struct {
	GraphCount        int
	NodeCount         int64
	RelationshipCount int64
}

type VerifyCollectionResult struct {
	GraphCount        int
	NodeCount         int64
	RelationshipCount int64
}

type VerifyDatabaseResult struct {
	GraphCount        int
	NodeCount         int64
	RelationshipCount int64
}

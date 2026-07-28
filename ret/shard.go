package ret

type ShardWriter struct {
	writeJsonl   bool
	writeParquet bool
}

type shardOutput struct {
	Index        int
	EntityType   string
	Count        int
	LastSourceID string
	Artifacts    []artifact
}

type artifact struct {
	Format string
	Path   string
	SHA256 string
}

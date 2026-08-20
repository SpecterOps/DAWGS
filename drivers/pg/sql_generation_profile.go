package pg

import "time"

// SQLGenerationProfile is a query-text-free timing sample for the PostgreSQL
// Cypher-to-SQL execution boundary. Durations end once pgx has accepted the
// query and returned its row stream; server planning and execution are
// captured separately through PostgreSQL EXPLAIN diagnostics.
//
// The profile intentionally contains neither Cypher nor SQL text, parameter
// values, backend identifiers, nor result data.
type SQLGenerationProfile struct {
	QueryClass string
	Parse      time.Duration
	Graph      time.Duration
	Policy     time.Duration
	Cache      time.Duration
	Translate  time.Duration
	Format     time.Duration
	Dispatch   time.Duration
}

// SQLGenerationProfileCollector receives completed query-text-free timing
// samples. Implementations must be safe for concurrent transactions.
type SQLGenerationProfileCollector interface {
	RecordSQLGenerationProfile(profile SQLGenerationProfile)
}

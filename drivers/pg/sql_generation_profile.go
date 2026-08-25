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
	// QueryClass is a low-cardinality category that omits query text and values.
	QueryClass string

	// Parse measures Cypher parsing and parse-cache lookup time.
	Parse time.Duration

	// Graph measures graph-target resolution time.
	Graph time.Duration

	// Policy measures traversal-shape classification and policy selection time.
	Policy time.Duration

	// Cache measures translation-cache lookup and binding time.
	Cache time.Duration

	// Translate measures Cypher-to-SQL translation time on cache misses.
	Translate time.Duration

	// Format measures SQL rendering time after translation.
	Format time.Duration

	// Dispatch measures client-side PostgreSQL query dispatch time.
	Dispatch time.Duration
}

// SQLGenerationProfileCollector receives completed query-text-free timing
// samples. Implementations must be safe for concurrent transactions.
type SQLGenerationProfileCollector interface {
	// RecordSQLGenerationProfile receives a completed query-text-free timing sample.
	RecordSQLGenerationProfile(profile SQLGenerationProfile)
}

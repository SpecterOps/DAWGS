package v2

// TranslationCacheStats is a query-text-free snapshot of one connection's
// translation cache activity and occupancy.
type TranslationCacheStats struct {
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Bypasses        uint64 `json:"bypasses"`
	Insertions      uint64 `json:"insertions"`
	Evictions       uint64 `json:"evictions"`
	BindingFailures uint64 `json:"binding_failures"`
	Entries         int    `json:"entries"`
	Capacity        int    `json:"capacity"`
}

func (s *TranslationCacheStats) add(other TranslationCacheStats) {
	s.Hits += other.Hits
	s.Misses += other.Misses
	s.Bypasses += other.Bypasses
	s.Insertions += other.Insertions
	s.Evictions += other.Evictions
	s.BindingFailures += other.BindingFailures
	s.Entries += other.Entries
}

// ConnectionCacheStats describes one currently live connection cache. ID is
// an opaque diagnostic identifier; it is not a backend PID or pointer value.
type ConnectionCacheStats struct {
	ID          uint64                `json:"id"`
	Translation TranslationCacheStats `json:"translation"`
}

// Stats is a query-text-free provider snapshot. Aggregate combines live and
// retired connection counters; its Capacity is the current theoretical bound
// across live connections, not a global retained-entry limit.
type Stats struct {
	SchemaGeneration      uint64                 `json:"schema_generation"`
	CapacityPerConnection int                    `json:"capacity_per_connection"`
	MinConnections        int32                  `json:"min_connections"`
	MaxConnections        int32                  `json:"max_connections"`
	LiveConnections       int                    `json:"live_connections"`
	RetiredConnections    uint64                 `json:"retired_connections"`
	Aggregate             TranslationCacheStats  `json:"aggregate"`
	Connections           []ConnectionCacheStats `json:"connections"`
}

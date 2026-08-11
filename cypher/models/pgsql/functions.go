package pgsql

const (
	// FunctionUnidirectionalASPHarness identifies the SQL harness for unidirectional all-shortest-path search.
	FunctionUnidirectionalASPHarness Identifier = "unidirectional_asp_harness"

	// FunctionUnidirectionalSPHarness identifies the SQL harness for unidirectional single-shortest-path search.
	FunctionUnidirectionalSPHarness Identifier = "unidirectional_sp_harness"

	// FunctionBidirectionalASPHarness identifies the SQL harness for bidirectional all-shortest-path search.
	FunctionBidirectionalASPHarness Identifier = "bidirectional_asp_harness"

	// FunctionBidirectionalSPHarness identifies the SQL harness for bidirectional single-shortest-path search.
	FunctionBidirectionalSPHarness Identifier = "bidirectional_sp_harness"

	// FunctionAllShortestPathsDAG identifies the SQL helper that materializes every shortest path from a predecessor DAG.
	FunctionAllShortestPathsDAG Identifier = "all_shortest_paths_dag"

	// FunctionShortestPathCompact identifies the SQL helper that materializes one compact shortest-path witness.
	FunctionShortestPathCompact Identifier = "shortest_path_compact"

	// FunctionShortestPathSelfEndpointError identifies the SQL helper that raises an invalid self-endpoint error.
	FunctionShortestPathSelfEndpointError Identifier = "shortest_path_self_endpoint_error"

	// FunctionIntArrayUnique identifies the SQL helper that removes duplicate integer-array values.
	FunctionIntArrayUnique Identifier = "uniq"

	// FunctionIntArraySort identifies the SQL helper that orders integer-array values.
	FunctionIntArraySort Identifier = "sort"

	// FunctionJSONBToTextArray identifies the SQL helper that converts a JSONB array to text[].
	FunctionJSONBToTextArray Identifier = "jsonb_to_text_array"

	// FunctionJSONBArrayElementsText identifies PostgreSQL's JSONB array-element text expansion function.
	FunctionJSONBArrayElementsText Identifier = "jsonb_array_elements_text"

	// FunctionJSONBBuildObject identifies PostgreSQL's JSONB object constructor.
	FunctionJSONBBuildObject Identifier = "jsonb_build_object"

	// FunctionJSONBArrayLength identifies PostgreSQL's JSONB array-length function.
	FunctionJSONBArrayLength Identifier = "jsonb_array_length"

	// FunctionJSONBTypeof identifies PostgreSQL's JSONB type-inspection function.
	FunctionJSONBTypeof Identifier = "jsonb_typeof"

	// FunctionToJSONB identifies PostgreSQL's conversion to JSONB.
	FunctionToJSONB Identifier = "to_jsonb"

	// FunctionCypherContains identifies the SQL helper implementing Cypher CONTAINS semantics.
	FunctionCypherContains Identifier = "cypher_contains"

	// FunctionCypherStartsWith identifies the SQL helper implementing Cypher STARTS WITH semantics.
	FunctionCypherStartsWith Identifier = "cypher_starts_with"

	// FunctionCypherEndsWith identifies the SQL helper implementing Cypher ENDS WITH semantics.
	FunctionCypherEndsWith Identifier = "cypher_ends_with"

	// FunctionCypherMin identifies the SQL aggregate implementing Cypher minimum semantics.
	FunctionCypherMin Identifier = "cypher_min"

	// FunctionCypherMax identifies the SQL aggregate implementing Cypher maximum semantics.
	FunctionCypherMax Identifier = "cypher_max"

	// FunctionArrayLength identifies PostgreSQL's dimension-aware array-length function.
	FunctionArrayLength Identifier = "array_length"

	// FunctionCardinality identifies PostgreSQL's total array-element count function.
	FunctionCardinality Identifier = "cardinality"

	// FunctionArrayAggregate identifies PostgreSQL's array aggregation function.
	FunctionArrayAggregate Identifier = "array_agg"

	// FunctionArrayRemove identifies PostgreSQL's array element-removal function.
	FunctionArrayRemove Identifier = "array_remove"

	// FunctionMin identifies PostgreSQL's minimum aggregate.
	FunctionMin Identifier = "min"

	// FunctionMax identifies PostgreSQL's maximum aggregate.
	FunctionMax Identifier = "max"

	// FunctionSum identifies PostgreSQL's sum aggregate.
	FunctionSum Identifier = "sum"

	// FunctionAvg identifies PostgreSQL's average aggregate.
	FunctionAvg Identifier = "avg"

	// FunctionLocalTimestamp identifies PostgreSQL's local timestamp constructor.
	FunctionLocalTimestamp Identifier = "localtimestamp"

	// FunctionLocalTime identifies PostgreSQL's local time constructor.
	FunctionLocalTime Identifier = "localtime"

	// FunctionCurrentTime identifies PostgreSQL's current zoned time value.
	FunctionCurrentTime Identifier = "current_time"

	// FunctionCurrentDate identifies PostgreSQL's current date value.
	FunctionCurrentDate Identifier = "current_date"

	// FunctionNow identifies PostgreSQL's current transaction timestamp function.
	FunctionNow Identifier = "now"

	// FunctionToLower identifies PostgreSQL's lowercase text function.
	FunctionToLower Identifier = "lower"

	// FunctionToUpper identifies PostgreSQL's uppercase text function.
	FunctionToUpper Identifier = "upper"

	// FunctionCoalesce identifies PostgreSQL's first-non-null expression.
	FunctionCoalesce Identifier = "coalesce"

	// FunctionNullIf identifies PostgreSQL's NULLIF function for nulling matching scalar values.
	FunctionNullIf Identifier = "nullif"

	// FunctionReplace identifies PostgreSQL's substring-replacement function.
	FunctionReplace Identifier = "replace"

	// FunctionUnnest identifies PostgreSQL's array-to-row expansion function.
	FunctionUnnest Identifier = "unnest"

	// FunctionNextValue identifies PostgreSQL's sequence increment function.
	FunctionNextValue Identifier = "nextval"

	// FunctionPGGetSerialSequence identifies PostgreSQL's serial-sequence lookup function.
	FunctionPGGetSerialSequence Identifier = "pg_get_serial_sequence"

	// FunctionJSONBSet identifies PostgreSQL's JSONB path-update function.
	FunctionJSONBSet Identifier = "jsonb_set"

	// FunctionCount identifies PostgreSQL's count aggregate.
	FunctionCount Identifier = "count"

	// FunctionStringToArray identifies PostgreSQL's delimiter-based text-to-array function.
	FunctionStringToArray Identifier = "string_to_array"

	// FunctionEdgesToPath identifies the SQL helper that builds a path from unordered edge composites.
	FunctionEdgesToPath Identifier = "edges_to_path"

	// FunctionOrderedEdgesToPath identifies the SQL helper that builds a path from ordered edge composites.
	FunctionOrderedEdgesToPath Identifier = "ordered_edges_to_path"

	// FunctionOrderedEdgeIDsToPath identifies the SQL helper that hydrates an ordered edge-ID array into a path.
	FunctionOrderedEdgeIDsToPath Identifier = "ordered_edge_ids_to_path"

	// FunctionNodesToPath identifies the SQL helper that builds a path from ordered node composites.
	FunctionNodesToPath Identifier = "nodes_to_path"

	// FunctionKindName identifies the SQL helper that resolves a kind ID to its name.
	FunctionKindName Identifier = "kind_name"

	// FunctionStartNode identifies the SQL helper that hydrates a relationship's start node.
	FunctionStartNode Identifier = "start_node"

	// FunctionEndNode identifies the SQL helper that hydrates a relationship's end node.
	FunctionEndNode Identifier = "end_node"

	// FunctionExtract identifies PostgreSQL's temporal component-extraction function.
	FunctionExtract Identifier = "extract"

	// FunctionGenerateSubscripts identifies PostgreSQL's array-index generation function.
	FunctionGenerateSubscripts Identifier = "generate_subscripts"
)

func IsAggregateFunction(function Identifier) bool {
	switch function {
	case FunctionCount, FunctionArrayAggregate, FunctionMin, FunctionMax, FunctionCypherMin, FunctionCypherMax, FunctionSum, FunctionAvg:
		return true

	default:
		return false
	}
}

package pgsql

const (
	FunctionUnidirectionalASPHarness Identifier = "unidirectional_asp_harness"
	FunctionUnidirectionalSPHarness  Identifier = "unidirectional_sp_harness"
	FunctionBidirectionalASPHarness  Identifier = "bidirectional_asp_harness"
	FunctionIntArrayUnique           Identifier = "uniq"
	FunctionIntArraySort             Identifier = "sort"
	FunctionJSONBToTextArray         Identifier = "jsonb_to_text_array"
	FunctionJSONBArrayElementsText   Identifier = "jsonb_array_elements_text"
	FunctionJSONBBuildObject         Identifier = "jsonb_build_object"
	FunctionJSONBArrayLength         Identifier = "jsonb_array_length"
	FunctionArrayLength              Identifier = "array_length"
	FunctionArrayAggregate           Identifier = "array_agg"
	FunctionArrayRemove              Identifier = "array_remove"
	FunctionMin                      Identifier = "min"
	FunctionMax                      Identifier = "max"
	FunctionLocalTimestamp           Identifier = "localtimestamp"
	FunctionLocalTime                Identifier = "localtime"
	FunctionCurrentTime              Identifier = "current_time"
	FunctionCurrentDate              Identifier = "current_date"
	FunctionNow                      Identifier = "now"
	FunctionToLower                  Identifier = "lower"
	FunctionToUpper                  Identifier = "upper"
	FunctionCoalesce                 Identifier = "coalesce"
	FunctionUnnest                   Identifier = "unnest"
	FunctionJSONBSet                 Identifier = "jsonb_set"
	FunctionCount                    Identifier = "count"
	FunctionStringToArray            Identifier = "string_to_array"
	FunctionEdgesToPath              Identifier = "edges_to_path"
	FunctionNodesToPath              Identifier = "nodes_to_path"
	FunctionExtract                  Identifier = "extract"
)

func IsAggregateFunction(function Identifier) bool {
	switch function {
	case FunctionCount, FunctionArrayAggregate:
		return true

	default:
		return false
	}
}

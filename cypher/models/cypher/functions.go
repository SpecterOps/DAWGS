package cypher

const (
	// CountFunction identifies the Cypher aggregate that counts non-null values or rows.
	CountFunction = "count"

	// DateFunction identifies the Cypher constructor for date values.
	DateFunction = "date"

	// TimeFunction identifies the Cypher constructor for zoned time values.
	TimeFunction = "time"

	// LocalTimeFunction identifies the Cypher constructor for local time values.
	LocalTimeFunction = "localtime"

	// DateTimeFunction identifies the Cypher constructor for zoned date-time values.
	DateTimeFunction = "datetime"

	// LocalDateTimeFunction identifies the Cypher constructor for local date-time values.
	LocalDateTimeFunction = "localdatetime"

	// DurationFunction identifies the Cypher constructor for duration values.
	DurationFunction = "duration"

	// IdentityFunction identifies the Cypher function that returns an entity ID.
	IdentityFunction = "id"

	// ToLowerFunction identifies the Cypher function that lowercases text.
	ToLowerFunction = "tolower"

	// ToUpperFunction identifies the Cypher function that uppercases text.
	ToUpperFunction = "toupper"

	// NodeLabelsFunction identifies the Cypher function that returns a node's labels.
	NodeLabelsFunction = "labels"

	// EdgeTypeFunction identifies the Cypher function that returns a relationship's type.
	EdgeTypeFunction = "type"

	// StartNodeFunction identifies the Cypher function that returns a relationship's start node.
	StartNodeFunction = "startnode"

	// EndNodeFunction identifies the Cypher function that returns a relationship's end node.
	EndNodeFunction = "endnode"

	// StringSplitToArrayFunction identifies the Cypher function that splits text into a list.
	StringSplitToArrayFunction = "split"

	// ToStringFunction identifies the Cypher function that converts a value to text.
	ToStringFunction = "tostring"

	// ToIntegerFunction identifies the Cypher function that converts a value to an integer.
	ToIntegerFunction = "tointeger"

	// ListSizeFunction identifies the Cypher function that returns the size of a list or string.
	ListSizeFunction = "size"

	// HeadFunction identifies the Cypher function that returns the first list element.
	HeadFunction = "head"

	// TailFunction identifies the Cypher function that returns all but the first list element.
	TailFunction = "tail"

	// NodesFunction identifies the Cypher function that returns a path's nodes in order.
	NodesFunction = "nodes"

	// RelationshipsFunction identifies the Cypher function that returns a path's relationships in order.
	RelationshipsFunction = "relationships"

	// PathLengthFunction identifies the Cypher function that returns the number of relationships in a path.
	PathLengthFunction = "length"

	// CoalesceFunction identifies the Cypher function that returns the first non-null argument.
	CoalesceFunction = "coalesce"

	// CollectFunction identifies the Cypher aggregate that collects values into a list.
	CollectFunction = "collect"

	// SumFunction identifies the Cypher aggregate that sums numeric values.
	SumFunction = "sum"

	// AvgFunction identifies the Cypher aggregate that averages numeric values.
	AvgFunction = "avg"

	// MinFunction identifies the Cypher aggregate that returns the minimum value.
	MinFunction = "min"

	// MaxFunction identifies the Cypher aggregate that returns the maximum value.
	MaxFunction = "max"

	// ITTCYear identifies the year component of a Cypher instant value.
	ITTCYear = "year"

	// ITTCMonth identifies the month component of a Cypher instant value.
	ITTCMonth = "month"

	// ITTCDay identifies the day component of a Cypher instant value.
	ITTCDay = "day"

	// ITTCHour identifies the hour component of a Cypher instant value.
	ITTCHour = "hour"

	// ITTCMinute identifies the minute component of a Cypher instant value.
	ITTCMinute = "minute"

	// ITTCSecond identifies the second component of a Cypher instant value.
	ITTCSecond = "second"

	// ITTCMillisecond identifies the millisecond component of a Cypher instant value.
	ITTCMillisecond = "millisecond"

	// ITTCMicrosecond identifies the microsecond component of a Cypher instant value.
	ITTCMicrosecond = "microsecond"

	// ITTCNanosecond identifies the nanosecond component of a Cypher instant value.
	ITTCNanosecond = "nanosecond"

	// ITTCTimeZone identifies the time-zone component of a Cypher instant value.
	ITTCTimeZone = "timezone"

	// ITTCEpochSeconds identifies the epoch-seconds component of a Cypher instant value.
	ITTCEpochSeconds = "epochseconds"

	// ITTCEpochMilliseconds identifies the epoch-milliseconds component of a Cypher instant value.
	ITTCEpochMilliseconds = "epochmillis"
)

package query

import "regexp"

var (
	pgPropertyIndexRegex = regexp.MustCompile(`(?i)^create\s+(unique)?(?:\s+)?index\s+([^ ]+)\s+on\s+\S+\s+using\s+([^ ]+)\s+\(+properties\s+->>\s+'([^:]+)::.+$`)
	pgColumnIndexRegex   = regexp.MustCompile(`(?i)^create\s+(unique)?(?:\s+)?index\s+([^ ]+)\s+on\s+\S+\s+using\s+([^ ]+)\s+\(([^)]+)\)(?:\s+include\s+\(([^)]+)\))?$`)
)

const (
	pgIndexRegexGroupUnique        = 1
	pgIndexRegexGroupName          = 2
	pgIndexRegexGroupIndexType     = 3
	pgIndexRegexGroupUsingFields   = 4
	pgIndexRegexGroupIncludeFields = 5
	pgIndexRegexNumExpectedGroups  = 6

	pgIndexTypeBTree   = "btree"
	pgIndexTypeGIN     = "gin"
	pgIndexUniqueStr   = "unique"
	pgPropertiesColumn = "properties"
)

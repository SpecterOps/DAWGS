package query

import "regexp"

var (
	pgPropertyIndexRegex = regexp.MustCompile(`(?i)^create\s+(unique)?(?:\s+)?index\s+([^ ]+)\s+on\s+\S+\s+using\s+([^ ]+)\s+\(+properties\s+->>\s+'([^:]+)::.+$`)
	pgColumnIndexRegex   = regexp.MustCompile(`(?i)^create\s+(unique)?(?:\s+)?index\s+([^ ]+)\s+on\s+\S+\s+using\s+([^ ]+)\s+\(([^)]+)\)$`)
)

const (
	pgIndexRegexGroupUnique       = 1
	pgIndexRegexGroupName         = 2
	pgIndexRegexGroupIndexType    = 3
	pgIndexRegexGroupFields       = 4
	pgIndexRegexNumExpectedGroups = 5

	pgIndexTypeBTree   = "btree"
	pgIndexTypeGIN     = "gin"
	pgIndexUniqueStr   = "unique"
	pgPropertiesColumn = "properties"
)

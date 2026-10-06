package pg

import (
	"fmt"
	"strconv"
	"strings"
)

const minimumPostgreSQLMajor = 18

func validatePostgreSQLVersion(version string) error {
	majorText := strings.SplitN(version, ".", 2)[0]
	major, err := strconv.Atoi(majorText)
	if err != nil || major < minimumPostgreSQLMajor {
		return fmt.Errorf("PostgreSQL %d or newer is required (server version %q)", minimumPostgreSQLMajor, version)
	}
	return nil
}

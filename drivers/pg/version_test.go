package pg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgreSQLMinimumVersion(t *testing.T) {
	for _, version := range []string{"18.0", "18.6 (Debian 18.6-1)", "19.1"} {
		require.NoError(t, validatePostgreSQLVersion(version))
	}
	for _, version := range []string{"", "garbage", "16.9", "17.6"} {
		require.ErrorContains(t, validatePostgreSQLVersion(version), "PostgreSQL 18 or newer is required")
	}
}

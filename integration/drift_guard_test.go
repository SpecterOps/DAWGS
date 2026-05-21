package integration

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

var forbiddenIntegrationKeys = map[string]struct{}{
	"assert_by_driver": {},
	"skip_drivers":     {},
}

func TestCoreIntegrationSuitesDoNotDriftByDriver(t *testing.T) {
	for _, root := range []string{
		filepath.Join("testdata", "cases"),
		filepath.Join("testdata", "templates"),
	} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".json" {
				return nil
			}

			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			var doc any
			if err := json.Unmarshal(raw, &doc); err != nil {
				return err
			}

			assertNoForbiddenIntegrationKeys(t, path, doc)
			return nil
		})
		if err != nil {
			t.Fatalf("failed to scan %s: %v", root, err)
		}
	}
}

func assertNoForbiddenIntegrationKeys(t *testing.T, path string, value any) {
	t.Helper()

	switch typedValue := value.(type) {
	case map[string]any:
		for key, child := range typedValue {
			if _, forbidden := forbiddenIntegrationKeys[key]; forbidden {
				t.Fatalf("%s uses %q; core integration suites must be backend-equivalent", path, key)
			}

			assertNoForbiddenIntegrationKeys(t, path, child)
		}

	case []any:
		for _, child := range typedValue {
			assertNoForbiddenIntegrationKeys(t, path, child)
		}
	}
}

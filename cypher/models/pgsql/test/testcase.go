package test

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"

	"cuelang.org/go/pkg/regexp"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/stretchr/testify/require"
)

const (
	// prefixCase introduces a named translation case in a fixture file.
	prefixCase = "case:"

	// prefixExclusiveTest marks a fixture case that must run without the other cases.
	prefixExclusiveTest = "exclusive:"

	// prefixCypherParams introduces the JSON parameter map supplied to Cypher translation.
	prefixCypherParams = "cypher_params:"

	// prefixPgSQLParams introduces the JSON parameter map expected in rendered PostgreSQL.
	prefixPgSQLParams = "pgsql_params:"
)

// testCaseFiles embeds the translation fixtures consumed by the package test runner.
//
//go:embed translation_cases/*
var testCaseFiles embed.FS

// TranslationTestCase pairs a Cypher query and dynamic parameters with the expected PostgreSQL translation.
type TranslationTestCase struct {
	// Name identifies the fixture within its source file.
	Name string
	// Cypher is the source query supplied to the frontend.
	Cypher string
	// PgSQL is the expected rendered PostgreSQL statement.
	PgSQL string
	// CypherParams contains dynamic caller parameters keyed by Cypher symbol.
	CypherParams map[string]any
	// PgSQLParams contains the expected dynamic SQL parameter bindings.
	PgSQLParams map[string]any
}

// Reset clears the fixture fields so the parser can accumulate the next case.
func (s *TranslationTestCase) Reset() {
	s.Name = ""
	s.Cypher = ""
	s.PgSQL = ""
	s.CypherParams = nil
	s.PgSQLParams = nil
}

// Copy duplicates the fixture record while retaining its parameter map references.
func (s *TranslationTestCase) Copy() *TranslationTestCase {
	return &TranslationTestCase{
		Name:         s.Name,
		Cypher:       s.Cypher,
		PgSQL:        s.PgSQL,
		CypherParams: s.CypherParams,
		PgSQLParams:  s.PgSQLParams,
	}
}

// writeStrings writes each string to writer in order and returns the first write failure.
func writeStrings(output io.Writer, strs ...string) error {
	for _, str := range strs {
		if _, err := output.Write([]byte(str)); err != nil {
			return err
		}
	}

	return nil
}

// fixtureAssets contains the exact license preamble emitted by the fixture writer.
//
//go:embed assets/license_header.sql
var fixtureAssets embed.FS

// licenseHeader preserves the fixture preamble bytes and its printf year placeholder.
var licenseHeader = loadLicenseHeader()

// loadLicenseHeader loads the embedded preamble at initialization and fails if it is missing.
func loadLicenseHeader() string {
	if header, err := fixtureAssets.ReadFile("assets/license_header.sql"); err != nil {
		panic(err)
	} else {
		return string(header)
	}
}

// WriteTo parses and translates the query, then writes its source, parameters, and SQL fixture text.
func (s *TranslationTestCase) WriteTo(output io.Writer, kindMapper pgsql.KindMapper) error {
	if regularQuery, err := frontend.ParseCypher(frontend.NewContext(), s.Cypher); err != nil {
		return err
	} else if err := writeStrings(output,
		"-- case: ",
		s.Cypher,
		"\n",
	); err != nil {
		return err
	} else {
		if len(s.CypherParams) > 0 {
			if err := walk.Cypher(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
				switch typedNode := node.(type) {
				case *cypher.Parameter:
					if value, hasValue := s.CypherParams[typedNode.Symbol]; hasValue {
						typedNode.Value = value
					}
				}
			})); err != nil {
				return err
			}

			if encodedJSON, err := json.Marshal(s.CypherParams); err != nil {
				return err
			} else if err := writeStrings(output,
				"-- cypher_params: ",
				string(encodedJSON),
				"\n"); err != nil {
				return err
			}
		}

		if translation, err := translate.Translate(context.Background(), regularQuery, kindMapper, nil, translate.DefaultGraphID); err != nil {
			return err
		} else if formattedQuery, err := translate.Translated(translation); err != nil {
			return err
		} else {
			maps.Copy(translation.Parameters, formattedQuery.Parameters)
			if len(translation.Parameters) > 0 {
				if encodedJSON, err := json.Marshal(translation.Parameters); err != nil {
					return err
				} else if err := writeStrings(output,
					"-- ", prefixPgSQLParams,
					string(encodedJSON),
					"\n",
				); err != nil {
					return err
				}
			}

			if err := writeStrings(output, formattedQuery.Statement, "\n\n"); err != nil {
				return err
			}
		}
	}

	return nil
}

// Assert translates the case and compares normalized SQL and parameters with the golden expectations.
func (s *TranslationTestCase) Assert(t *testing.T, expectedSQL string, kindMapper pgsql.KindMapper) {
	if regularQuery, err := frontend.ParseCypher(frontend.NewContext(), s.Cypher); err != nil {
		t.Fatalf("Failed to compile cypher query: %s - %v", s.Cypher, err)
	} else {
		if s.CypherParams != nil {
			if err := walk.Cypher(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
				switch typedNode := node.(type) {
				case *cypher.Parameter:
					if value, hasValue := s.CypherParams[typedNode.Symbol]; hasValue {
						typedNode.Value = value
					}
				}
			})); err != nil {
				t.Fatalf("Error attempting to set parameter values in cypher query: %v", err)
			}
		}

		if translation, err := translate.Translate(context.Background(), regularQuery, kindMapper, nil, translate.DefaultGraphID); err != nil {
			t.Fatalf("Failed to translate cypher query: %s - %v", s.Cypher, err)
		} else if formattedQuery, err := translate.Translated(translation); err != nil {
			t.Fatalf("Failed to format SQL translatedQuery: %v", err)
		} else {
			// Apply same whitespace normalization as expected SQL (from file parsing)
			normalizedActual, err := regexp.ReplaceAll("\\s+", strings.TrimSpace(formattedQuery.Statement), " ")
			if err != nil {
				t.Fatalf("error while attempting to collapse whitespace in actual query: %v", err)
			}
			require.Equalf(t, expectedSQL, normalizedActual, "Test case for cypher query: '%s' failed to match.", s.Cypher)

			if s.PgSQLParams != nil {
				maps.Copy(translation.Parameters, formattedQuery.Parameters)
				// Golden parameters are stored as JSON, whose decoder represents
				// numbers as float64. Compare the translated bag through the same
				// serialization boundary so typed integer parameters do not create a
				// false mismatch while their values and emitted casts remain exact.
				var normalizedParameters map[string]any
				encodedParameters, err := json.Marshal(translation.Parameters)
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(encodedParameters, &normalizedParameters))
				require.Equal(t, s.PgSQLParams, normalizedParameters)
			}
		}
	}
}

func (s *TranslationTestCase) AssertLive(ctx context.Context, t *testing.T, driver *pg.Driver) {
	if regularQuery, err := frontend.ParseCypher(frontend.NewContext(), s.Cypher); err != nil {
		t.Fatalf("Failed to compile cypher query: %s - %v", s.Cypher, err)
	} else {
		if s.CypherParams != nil {
			if err := walk.Cypher(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
				switch typedNode := node.(type) {
				case *cypher.Parameter:
					if value, hasValue := s.CypherParams[typedNode.Symbol]; hasValue {
						typedNode.Value = value
					}
				}
			})); err != nil {
				t.Fatalf("Error attempting to set parameter values in cypher query: %v", err)
			}
		}

		defaultGraph, hasDefaultGraph := driver.DefaultGraph()
		if !hasDefaultGraph {
			t.Fatalf("Driver has no default graph set")
		}

		if translation, err := translate.Translate(context.Background(), regularQuery, driver.KindMapper(), s.CypherParams, defaultGraph.ID); err != nil {
			t.Fatalf("Failed to translate cypher query: %s - %v", s.Cypher, err)
		} else if formattedQuery, err := translate.Translated(translation); err != nil {
			t.Fatalf("Failed to format SQL translatedQuery: %v", err)
		} else {
			maps.Copy(translation.Parameters, formattedQuery.Parameters)
			require.NoError(t, driver.Run(ctx, "explain "+formattedQuery.Statement, translation.Parameters))
		}
	}
}

type TranslationTestCaseFile struct {
	path    string
	content []byte
}

func (s *TranslationTestCaseFile) Load() ([]*TranslationTestCase, bool, error) {
	var (
		testCases         []*TranslationTestCase
		isExclusive       = false
		hasExclusiveTests = false
		nextTestCase      = &TranslationTestCase{}
		queryBuilder      = strings.Builder{}
	)

	for lineNumber, line := range strings.Split(string(s.content), "\n") {
		// Crush unnecessary whitespace
		formattedLine, err := regexp.ReplaceAll("\\s+", strings.TrimSpace(line), " ")

		if err != nil {
			return nil, false, fmt.Errorf("error while attempting to collapse whitespace in query: %w", err)
		}

		if len(formattedLine) == 0 {
			continue
		}

		if isLineComment := strings.HasPrefix(formattedLine, "--"); isLineComment {
			// Strip the comment header
			formattedLine = strings.Trim(formattedLine, "- ")

			lowerFormattedLine := strings.ToLower(formattedLine)

			if caseIndex := strings.Index(lowerFormattedLine, prefixCase); caseIndex != -1 {
				// This is a new test case - capture the comment as the cypher statement to test
				nextTestCase.Cypher = strings.TrimSpace(formattedLine[caseIndex+len(prefixCase):])
			} else if strings.Contains(lowerFormattedLine, prefixExclusiveTest) {
				if !hasExclusiveTests {
					// Clear the existing test cases
					testCases = testCases[:0]
					hasExclusiveTests = true
				}

				// The current test case as being marked as run-only
				isExclusive = true
			} else if cypherParamsIdx := strings.Index(lowerFormattedLine, prefixCypherParams); cypherParamsIdx != -1 {
				paramsContent := []byte(strings.TrimSpace(formattedLine[cypherParamsIdx+len(prefixCypherParams):]))
				nextTestCase.CypherParams = map[string]any{}

				if err := json.Unmarshal(paramsContent, &nextTestCase.CypherParams); err != nil {
					return nil, false, fmt.Errorf("failed to unmarshal cypher params on line number %d: %w", lineNumber, err)
				}
			} else if pgsqlParamsIdx := strings.Index(lowerFormattedLine, prefixPgSQLParams); pgsqlParamsIdx != -1 {
				paramsContent := []byte(strings.TrimSpace(formattedLine[pgsqlParamsIdx+len(prefixPgSQLParams):]))
				nextTestCase.PgSQLParams = map[string]any{}

				if err := json.Unmarshal(paramsContent, &nextTestCase.PgSQLParams); err != nil {
					return nil, false, fmt.Errorf("failed to unmarshal pgsql params on line number %d: %w", lineNumber, err)
				}
			}
		} else if len(nextTestCase.Cypher) > 0 {
			// Strip any comment fragments for this line. Best effort; probably better done with a regex.
			if inlineCommentIdx := strings.Index(formattedLine, "--"); inlineCommentIdx >= 0 {
				formattedLine = strings.TrimSpace(formattedLine[:inlineCommentIdx])
			}

			// Check to make sure there's translatedQuery content.
			if len(formattedLine) == 0 {
				continue
			}

			// If there's content in the translatedQuery builder, prepend a space to conjoin the lines
			if queryBuilder.Len() > 0 {
				queryBuilder.WriteRune(' ')
			}

			queryBuilder.WriteString(formattedLine)

			// SQL queries must end with a ';' character
			if strings.HasSuffix(formattedLine, ";") {
				nextTestCase.PgSQL = queryBuilder.String()

				// Format the expected SQL translation and create a sub-test
				if isExclusive || !hasExclusiveTests {
					nextTestCase.Name = filepath.Base(s.path) + " " + nextTestCase.Cypher
					testCases = append(testCases, nextTestCase.Copy())
				}

				// Reset the query builder and test case
				queryBuilder.Reset()
				nextTestCase.Reset()

				isExclusive = false
			}
		}
	}

	return testCases, hasExclusiveTests, nil
}

func ReadTranslationTestCaseFile(path string, fin fs.File) (TranslationTestCaseFile, error) {
	content, err := io.ReadAll(fin)

	return TranslationTestCaseFile{
		path:    path,
		content: content,
	}, err
}

// updatedCasesDir returns the configured fixture update directory or an isolated temporary directory.
func updatedCasesDir() (string, error) {
	if workingDir, err := os.Getwd(); err != nil {
		return "", err
	} else {
		path := filepath.Join(workingDir, "updated_cases")

		if err := os.MkdirAll(path, 0755); err != nil {
			return "", err
		}

		return path, nil
	}
}

// UpdateTranslationTestCases regenerates SQL golden files from their embedded Cypher cases.
func UpdateTranslationTestCases(mapper pgsql.KindMapper) error {
	if updatedCasesPath, err := updatedCasesDir(); err != nil {
		return err
	} else if err := fs.WalkDir(testCaseFiles, "translation_cases", func(path string, dir fs.DirEntry, err error) error {
		if !dir.IsDir() {
			var (
				caseFileName        = filepath.Base(path)
				updatedCaseFilePath = filepath.Join(updatedCasesPath, caseFileName)
			)

			if strings.HasSuffix(path, ".sql") {
				if fin, err := testCaseFiles.Open(path); err != nil {
					return err
				} else {
					defer fin.Close()

					if caseFile, err := ReadTranslationTestCaseFile(path, fin); err != nil {
						return err
					} else if nextCases, _, err := caseFile.Load(); err != nil {
						return err
					} else {
						var output strings.Builder
						formattedLicenseHeader := fmt.Sprintf(licenseHeader, time.Now().Year())

						if _, err := io.WriteString(&output, formattedLicenseHeader); err != nil {
							return err
						}

						for _, nextCase := range nextCases {
							if err := nextCase.WriteTo(&output, mapper); err != nil {
								return err
							}
						}

						trailingNewlines := "\n"
						if bytes.HasSuffix(caseFile.content, []byte("\n\n")) {
							trailingNewlines = "\n\n"
						}
						content := strings.TrimRight(output.String(), "\n") + trailingNewlines
						if err := os.WriteFile(updatedCaseFilePath, []byte(content), 0644); err != nil {
							return err
						}
					}
				}
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func ReadTranslationTestCases() ([]*TranslationTestCase, error) {
	var caseFiles []TranslationTestCaseFile

	if err := fs.WalkDir(testCaseFiles, "translation_cases", func(path string, dir fs.DirEntry, err error) error {
		if !dir.IsDir() {
			if strings.HasSuffix(path, ".sql") {
				if fin, err := testCaseFiles.Open(path); err != nil {
					return err
				} else {
					defer fin.Close()

					if caseFile, err := ReadTranslationTestCaseFile(path, fin); err != nil {
						return err
					} else {
						caseFiles = append(caseFiles, caseFile)
					}
				}
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	var (
		cases             []*TranslationTestCase
		hasExclusiveTests bool
	)

	for _, caseFile := range caseFiles {
		loadedTestCases, caseFileHasExclusiveTests, err := caseFile.Load()

		if err != nil {
			return nil, err
		}

		if !hasExclusiveTests {
			if caseFileHasExclusiveTests {
				hasExclusiveTests = true
				cases = cases[:0]
			}

			cases = append(cases, loadedTestCases...)
		} else if caseFileHasExclusiveTests {
			cases = append(cases, loadedTestCases...)
		}
	}

	return cases, nil
}

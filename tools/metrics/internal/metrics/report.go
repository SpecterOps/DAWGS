package metrics

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/fzipp/gocyclo"
	"golang.org/x/tools/cover"
)

type Options struct {
	SourceRoot     string
	CoverProfile   string
	Ignore         string
	QualityOptions QualityOptions
}

type TextOptions struct {
	Top      int
	CRAPOver float64
}

type Report struct {
	SourceRoot        string           `json:"source_root"`
	CoverProfile      string           `json:"cover_profile"`
	FunctionCount     int              `json:"function_count"`
	AverageComplexity float64          `json:"average_complexity"`
	AverageCRAP       float64          `json:"average_crap"`
	Records           []FunctionMetric `json:"records"`
	Quality           QualityReport    `json:"quality"`
}

type FunctionMetric struct {
	Package           string  `json:"package"`
	Function          string  `json:"function"`
	File              string  `json:"file"`
	Line              int     `json:"line"`
	Column            int     `json:"column"`
	Complexity        int     `json:"complexity"`
	Statements        int     `json:"statements"`
	CoveredStatements int     `json:"covered_statements"`
	Coverage          float64 `json:"coverage"`
	CRAP              float64 `json:"crap"`
}

type functionDecl struct {
	packageName string
	name        string
	file        string
	line        int
	column      int
	startLine   int
	startColumn int
	endLine     int
	endColumn   int
	complexity  int
}

func Analyze(options Options) (Report, error) {
	if options.CoverProfile == "" {
		return Report{}, fmt.Errorf("cover profile is required")
	}

	sourceRoot := options.SourceRoot
	if sourceRoot == "" {
		sourceRoot = "."
	}

	absoluteSourceRoot, err := filepath.Abs(sourceRoot)
	if err != nil {
		return Report{}, fmt.Errorf("resolve source root: %w", err)
	}

	var ignore *regexp.Regexp
	if options.Ignore != "" {
		if ignore, err = regexp.Compile(options.Ignore); err != nil {
			return Report{}, fmt.Errorf("compile ignore pattern: %w", err)
		}
	}

	functions, err := collectFunctions(absoluteSourceRoot, ignore)
	if err != nil {
		return Report{}, err
	}

	profiles, err := cover.ParseProfiles(options.CoverProfile)
	if err != nil {
		return Report{}, fmt.Errorf("parse coverage profile: %w", err)
	}

	report := Report{
		SourceRoot:   filepath.ToSlash(absoluteSourceRoot),
		CoverProfile: filepath.ToSlash(options.CoverProfile),
		Records:      make([]FunctionMetric, 0, len(functions)),
	}

	var totalComplexity int
	var totalCRAP float64
	for _, function := range functions {
		statements, coveredStatements, coverage := coverageForFunction(function, profiles)
		crap := calculateCRAP(function.complexity, coverage)

		totalComplexity += function.complexity
		totalCRAP += crap
		report.Records = append(report.Records, FunctionMetric{
			Package:           function.packageName,
			Function:          function.name,
			File:              function.file,
			Line:              function.line,
			Column:            function.column,
			Complexity:        function.complexity,
			Statements:        statements,
			CoveredStatements: coveredStatements,
			Coverage:          coverage,
			CRAP:              crap,
		})
	}

	sortMetrics(report.Records)
	report.FunctionCount = len(report.Records)
	if report.FunctionCount > 0 {
		report.AverageComplexity = float64(totalComplexity) / float64(report.FunctionCount)
		report.AverageCRAP = totalCRAP / float64(report.FunctionCount)
	}

	report.Quality = AnalyzeQuality(absoluteSourceRoot, options.QualityOptions)

	return report, nil
}

func WriteJSON(output io.Writer, report Report) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func WriteText(output io.Writer, report Report, options TextOptions) error {
	records := filterRecords(report.Records, options)

	if _, err := fmt.Fprintf(output, "CRAP report for %s\n", report.CoverProfile); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		output,
		"functions: %d, average complexity: %.2f, average CRAP: %.2f\n\n",
		report.FunctionCount,
		report.AverageComplexity,
		report.AverageCRAP,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(output, "CRAP    CYCLO  COV%    STMT       FUNCTION                                      LOCATION"); err != nil {
		return err
	}

	for _, record := range records {
		if _, err := fmt.Fprintf(
			output,
			"%7.2f %5d %6.2f %5d/%-5d %-45s %s:%d:%d\n",
			record.CRAP,
			record.Complexity,
			record.Coverage*100,
			record.CoveredStatements,
			record.Statements,
			record.Package+"."+record.Function,
			record.File,
			record.Line,
			record.Column,
		); err != nil {
			return err
		}
	}

	return nil
}

func RecordsOverCRAPThreshold(report Report, threshold float64) []FunctionMetric {
	if threshold <= 0 {
		return nil
	}

	var records []FunctionMetric
	for _, record := range report.Records {
		if record.CRAP > threshold {
			records = append(records, record)
		}
	}

	return records
}

func collectFunctions(sourceRoot string, ignore *regexp.Regexp) ([]functionDecl, error) {
	var functions []functionDecl

	if err := filepath.WalkDir(sourceRoot, func(path string, directoryEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if directoryEntry.IsDir() {
			if shouldSkipDirectory(sourceRoot, path, directoryEntry.Name()) {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(directoryEntry.Name(), ".go") || strings.HasSuffix(directoryEntry.Name(), "_test.go") {
			return nil
		}

		relativePath, err := relativeSlashPath(sourceRoot, path)
		if err != nil {
			return err
		}

		if matchesIgnoredPath(ignore, relativePath, path) {
			return nil
		}

		fileFunctions, err := collectFileFunctions(path, relativePath)
		if err != nil {
			return err
		}

		functions = append(functions, fileFunctions...)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("collect Go functions: %w", err)
	}

	return functions, nil
}

func collectFileFunctions(path, relativePath string) ([]functionDecl, error) {
	fileSet := token.NewFileSet()
	parsedFile, err := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", relativePath, err)
	}

	var functions []functionDecl
	for _, declaration := range parsedFile.Decls {
		switch typedDeclaration := declaration.(type) {
		case *ast.FuncDecl:
			if hasGocycloIgnore(typedDeclaration.Doc) {
				continue
			}

			functions = append(functions, newFunctionDecl(
				parsedFile.Name.Name,
				functionName(typedDeclaration),
				relativePath,
				fileSet,
				typedDeclaration,
			))
		case *ast.GenDecl:
			if hasGocycloIgnore(typedDeclaration.Doc) {
				continue
			}

			for _, spec := range typedDeclaration.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}

				for valueIndex, value := range valueSpec.Values {
					functionLiteral, ok := value.(*ast.FuncLit)
					if !ok || valueIndex >= len(valueSpec.Names) {
						continue
					}

					functions = append(functions, newFunctionDecl(
						parsedFile.Name.Name,
						valueSpec.Names[valueIndex].Name,
						relativePath,
						fileSet,
						functionLiteral,
					))
				}
			}
		}
	}

	return functions, nil
}

func newFunctionDecl(packageName, name, relativePath string, fileSet *token.FileSet, node ast.Node) functionDecl {
	startPosition := fileSet.Position(node.Pos())
	endPosition := fileSet.Position(node.End())

	return functionDecl{
		packageName: packageName,
		name:        name,
		file:        relativePath,
		line:        startPosition.Line,
		column:      startPosition.Column,
		startLine:   startPosition.Line,
		startColumn: startPosition.Column,
		endLine:     endPosition.Line,
		endColumn:   endPosition.Column,
		complexity:  gocyclo.Complexity(node),
	}
}

func coverageForFunction(function functionDecl, profiles []*cover.Profile) (int, int, float64) {
	var profileMatched bool
	var statements int
	var coveredStatements int

	for _, profile := range profiles {
		if !sameFile(function.file, profile.FileName) {
			continue
		}

		profileMatched = true
		for _, block := range profile.Blocks {
			if blockInFunction(function, block) {
				statements += block.NumStmt
				if block.Count > 0 {
					coveredStatements += block.NumStmt
				}
			}
		}
	}

	if !profileMatched {
		return 0, 0, 0
	}
	if statements == 0 {
		return 0, 0, 1
	}

	return statements, coveredStatements, float64(coveredStatements) / float64(statements)
}

func blockInFunction(function functionDecl, block cover.ProfileBlock) bool {
	return positionGreaterOrEqual(block.StartLine, block.StartCol, function.startLine, function.startColumn) &&
		positionLessOrEqual(block.EndLine, block.EndCol, function.endLine, function.endColumn)
}

func calculateCRAP(complexity int, coverage float64) float64 {
	uncovered := 1 - coverage
	return math.Pow(float64(complexity), 2)*math.Pow(uncovered, 3) + float64(complexity)
}

func filterRecords(records []FunctionMetric, options TextOptions) []FunctionMetric {
	filteredRecords := make([]FunctionMetric, 0, len(records))
	for _, record := range records {
		if options.CRAPOver > 0 && record.CRAP <= options.CRAPOver {
			continue
		}

		filteredRecords = append(filteredRecords, record)
		if options.Top > 0 && len(filteredRecords) == options.Top {
			break
		}
	}

	return filteredRecords
}

func sortMetrics(records []FunctionMetric) {
	sort.SliceStable(records, func(leftIndex, rightIndex int) bool {
		left := records[leftIndex]
		right := records[rightIndex]

		if left.CRAP != right.CRAP {
			return left.CRAP > right.CRAP
		}
		if left.Complexity != right.Complexity {
			return left.Complexity > right.Complexity
		}
		if left.File != right.File {
			return left.File < right.File
		}

		return left.Function < right.Function
	})
}

func sameFile(functionPath, profilePath string) bool {
	normalizedProfilePath := filepath.ToSlash(filepath.Clean(profilePath))
	if normalizedProfilePath == functionPath {
		return true
	}

	return strings.HasSuffix(normalizedProfilePath, "/"+functionPath)
}

func positionGreaterOrEqual(line, column, minimumLine, minimumColumn int) bool {
	return line > minimumLine || line == minimumLine && column >= minimumColumn
}

func positionLessOrEqual(line, column, maximumLine, maximumColumn int) bool {
	return line < maximumLine || line == maximumLine && column <= maximumColumn
}

func relativeSlashPath(root, path string) (string, error) {
	relativePath, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}

	return filepath.ToSlash(relativePath), nil
}

func matchesIgnoredPath(ignore *regexp.Regexp, relativePath, absolutePath string) bool {
	return ignore != nil && (ignore.MatchString(relativePath) || ignore.MatchString(filepath.ToSlash(absolutePath)))
}

func shouldSkipDirectory(sourceRoot, path, name string) bool {
	if path == sourceRoot {
		return false
	}

	return name == "testdata" ||
		name == "vendor" ||
		strings.HasPrefix(name, ".") ||
		strings.HasPrefix(name, "_")
}

func hasGocycloIgnore(commentGroup *ast.CommentGroup) bool {
	if commentGroup == nil {
		return false
	}

	for _, comment := range commentGroup.List {
		if strings.TrimSpace(comment.Text) == "//gocyclo:ignore" {
			return true
		}
	}

	return false
}

func functionName(function *ast.FuncDecl) string {
	if function.Recv == nil || function.Recv.NumFields() == 0 {
		return function.Name.Name
	}

	return fmt.Sprintf("(%s).%s", receiverName(function.Recv.List[0].Type), function.Name.Name)
}

func receiverName(expression ast.Expr) string {
	switch typedExpression := expression.(type) {
	case *ast.Ident:
		return typedExpression.Name
	case *ast.StarExpr:
		return "*" + receiverName(typedExpression.X)
	case *ast.IndexExpr:
		return receiverName(typedExpression.X)
	case *ast.IndexListExpr:
		return receiverName(typedExpression.X)
	default:
		return "BADRECV"
	}
}

func CreateOutput(path string) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("output path is required")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}

	output, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}

	return output, nil
}

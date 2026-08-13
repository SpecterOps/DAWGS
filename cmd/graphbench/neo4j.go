// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	neo4jcore "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/databaseguard"
	dawgsneo4j "github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

// neo4jRunner owns the Neo4j driver and database used to execute benchmark cases.
type neo4jRunner struct {
	// datasetDir locates fixture and corpus files on disk.
	datasetDir string
	// db provides graph transactions for fixture preparation and query execution.
	db graph.Database
	// planDriver supplies the Neo4j driver used only for untimed PROFILE or EXPLAIN capture.
	planDriver neo4jcore.DriverWithContext
	// databaseName selects the Neo4j database targeted by the benchmark session.
	databaseName string
}

// newNeo4jRunner opens a Neo4j driver and selects the optional database encoded in the URI.
func newNeo4jRunner(ctx context.Context, datasetDir, connection string, corpus ScaleCorpus) (*neo4jRunner, error) {
	if err := databaseguard.ValidateEnvironment(connection); err != nil {
		return nil, fmt.Errorf("refuse destructive Neo4j GraphBench target: %w", err)
	}

	db, err := dawgs.Open(ctx, dawgsneo4j.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      connection,
	})
	if err != nil {
		return nil, fmt.Errorf("open Neo4j database: %w", err)
	}

	nodeKinds, edgeKinds, err := scanDatasetKinds(datasetDir, scaleCorpusDatasets(corpus))
	if err != nil {
		_ = db.Close(ctx)
		return nil, err
	}

	if err := db.AssertSchema(ctx, benchmarkSchema(nodeKinds, edgeKinds)); err != nil {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("assert Neo4j schema: %w", err)
	}

	planDriver, databaseName, err := openNeo4jPlanDriver(connection)
	if err != nil {
		_ = db.Close(ctx)
		return nil, err
	}

	return &neo4jRunner{
		datasetDir:   datasetDir,
		db:           db,
		planDriver:   planDriver,
		databaseName: databaseName,
	}, nil
}

// Close releases both Neo4j drivers owned by the benchmark runner.
func (s *neo4jRunner) Close(ctx context.Context) error {
	var closeErr error
	if s.planDriver != nil {
		closeErr = s.planDriver.Close(ctx)
	}
	if s.db != nil {
		if err := s.db.Close(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

// Run reloads each fixture dataset and measures every corpus case supported by Neo4j.
func (s *neo4jRunner) Run(ctx context.Context, warmupIterations, iterations int, corpus ScaleCorpus) ([]CaseResult, error) {
	var (
		records        []CaseResult
		casesByDataset = scaleCasesByDataset(corpus)
	)

	for _, datasetName := range scaleCorpusDatasets(corpus) {
		fixture, err := fixtureMetadata(s.datasetDir, datasetName)
		if err != nil {
			return nil, err
		}
		if err := clearGraph(ctx, s.db); err != nil {
			return nil, fmt.Errorf("clear graph for %s: %w", datasetName, err)
		}

		idMap, err := loadDataset(ctx, s.db, s.datasetDir, datasetName)
		if err != nil {
			return nil, err
		}

		for _, testCase := range casesByDataset[datasetName] {
			if !testCase.Supports(ModeNeo4j) {
				continue
			}

			record := s.runCase(ctx, warmupIterations, iterations, testCase, idMap)
			attachFixtureMetadata(&record, fixture)
			records = append(records, record)
		}
	}

	return records, nil
}

// runCase resolves fixture parameters, measures the selected Neo4j read or write workload, and records correctness and timing status in one CaseResult.
func (s *neo4jRunner) runCase(ctx context.Context, warmupIterations, iterations int, testCase ScaleCase, idMap opengraph.IDMap) CaseResult {
	params, err := resolveCaseParams(testCase, idMap)
	record := newCaseResult(testCase, ModeNeo4j, params)
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	if testCase.WriteScenario == nil {
		rowCount, observedRows, stats, err := measureCypherWithWarmups(ctx, s.db, testCase.Cypher, params, testCase.Expected, idMap, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}

		record.RowCount = rowCount
		record.ObservedRows = observedRows
		record.Stats = stats
		labelLatencySamples(&record.Stats, ModeNeo4j, testCase)
		applyRowExpectation(&record)
	} else {
		scenario, err := resolveWriteScenario(testCase, idMap)
		if err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}

		measurement, stats, err := measureWriteCypherWithWarmups(ctx, s.db, testCase.Cypher, params, scenario, warmupIterations, iterations)
		if err != nil {
			record.Status = StatusError
			record.Error = err.Error()
			return record
		}

		record.MatchedCount = &measurement.Matched
		record.AffectedCount = &measurement.Affected
		record.PostState = measurement.PostState
		record.Stats = stats
		labelLatencySamples(&record.Stats, ModeNeo4j, testCase)
	}

	plan, operators, err := s.explain(ctx, testCase.Cypher, params, testCase.WriteScenario != nil)
	if err != nil {
		if record.Status == StatusOK {
			record.Status = StatusError
			record.Error = err.Error()
		}
		return record
	}

	record.Neo4jPlan = plan
	record.Neo4jOperators = operators
	return record
}

// explain submits native Neo4j PROFILE for reads and EXPLAIN for writes after the timed block.
func (s *neo4jRunner) explain(ctx context.Context, cypherQuery string, params map[string]any, write bool) (plan *Neo4jPlanNode, operators []string, err error) {
	accessMode := neo4jcore.AccessModeRead
	if write {
		accessMode = neo4jcore.AccessModeWrite
	}
	session := s.planDriver.NewSession(ctx, neo4jcore.SessionConfig{
		AccessMode:   accessMode,
		DatabaseName: s.databaseName,
	})
	defer func() {
		if closeErr := session.Close(ctx); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	result, err := session.Run(ctx, neo4jPlanCaptureStatement(cypherQuery, write), params)
	if err != nil {
		return nil, nil, err
	}

	summary, err := result.Consume(ctx)
	if err != nil {
		return nil, nil, err
	}
	if write {
		explainPlan := summary.Plan()
		if explainPlan == nil {
			return nil, nil, nil
		}

		metadata := neo4jProfileMetadata(explainPlan.Arguments(), neo4jServerAgent(summary), false)
		planNode := convertNeo4jPlan(explainPlan)
		planNode.ProfileMetadata = &metadata

		return &planNode, neo4jOperators(planNode), nil
	}

	profile := summary.Profile()
	if profile == nil {
		return nil, nil, nil
	}

	metadata := neo4jProfileMetadata(profile.Arguments(), neo4jServerAgent(summary), true)
	planNode := convertNeo4jProfiledPlan(profile, metadata.internalTraversalOpaque())
	planNode.ProfileMetadata = &metadata

	return &planNode, neo4jOperators(planNode), nil
}

// neo4jPlanCaptureStatement selects PROFILE only for read-only cases and retains non-executing EXPLAIN for writes.
func neo4jPlanCaptureStatement(cypherQuery string, write bool) string {
	command := "PROFILE"
	if write {
		command = "EXPLAIN"
	}

	return command + " " + cypherWithoutTerminator(cypherQuery)
}

func neo4jServerAgent(summary neo4jcore.ResultSummary) string {
	if server := summary.Server(); server != nil {
		return server.Agent()
	}

	return ""
}

// neo4jPlanDriverConfig contains a Neo4j server URI and optional target database parsed from a connection string.
type neo4jPlanDriverConfig struct {
	// Target contains the Neo4j server URI without a database path.
	Target string
	// Username contains the Neo4j username decoded from the connection URI.
	Username string
	// Password contains the Neo4j password decoded from the connection URI.
	Password string
	// DatabaseName selects the Neo4j database targeted by the session.
	DatabaseName string
}

// parseNeo4jPlanDriverConfig parses a Neo4j connection string while preserving its server URI and database path.
func parseNeo4jPlanDriverConfig(connStr string) (neo4jPlanDriverConfig, error) {
	connectionURL, err := url.Parse(connStr)
	if err != nil {
		return neo4jPlanDriverConfig{}, fmt.Errorf("parse Neo4j connection string: %w", err)
	}

	if connectionURL.Scheme != dawgsneo4j.DriverName && connectionURL.Scheme != "neo4j+s" && connectionURL.Scheme != "neo4j+ssc" {
		return neo4jPlanDriverConfig{}, fmt.Errorf("expected Neo4j connection string scheme, got %q", connectionURL.Scheme)
	}

	password, ok := connectionURL.User.Password()
	if !ok {
		return neo4jPlanDriverConfig{}, fmt.Errorf("no password provided in Neo4j connection string")
	}
	if connectionURL.Host == "" {
		return neo4jPlanDriverConfig{}, fmt.Errorf("Neo4j connection string host is required")
	}

	databaseName, err := neo4jDatabaseName(connectionURL)
	if err != nil {
		return neo4jPlanDriverConfig{}, err
	}

	return neo4jPlanDriverConfig{
		Target: (&url.URL{
			Scheme:   connectionURL.Scheme,
			Host:     connectionURL.Host,
			RawQuery: connectionURL.RawQuery,
		}).String(),
		Username:     connectionURL.User.Username(),
		Password:     password,
		DatabaseName: databaseName,
	}, nil
}

// neo4jDatabaseName returns the optional single-segment database name encoded in a Neo4j URI path.
func neo4jDatabaseName(connectionURL *url.URL) (string, error) {
	databasePath := strings.Trim(connectionURL.EscapedPath(), "/")
	if databasePath == "" {
		return "", nil
	}
	if strings.Contains(databasePath, "/") {
		return "", fmt.Errorf("Neo4j database path must contain a single database name")
	}

	databaseName, err := url.PathUnescape(databasePath)
	if err != nil {
		return "", fmt.Errorf("parse Neo4j database name: %w", err)
	}
	if strings.Contains(databaseName, "/") {
		return "", fmt.Errorf("Neo4j database path must contain a single database name")
	}

	return databaseName, nil
}

// openNeo4jPlanDriver parses the benchmark connection settings and returns a context-aware driver together with the selected database name.
func openNeo4jPlanDriver(connStr string) (neo4jcore.DriverWithContext, string, error) {
	cfg, err := parseNeo4jPlanDriverConfig(connStr)
	if err != nil {
		return nil, "", err
	}

	driver, err := neo4jcore.NewDriverWithContext(cfg.Target, neo4jcore.BasicAuth(cfg.Username, cfg.Password, ""))
	if err != nil {
		return nil, "", err
	}

	return driver, cfg.DatabaseName, nil
}

// Neo4jProfileMetadata identifies the planner, runtime, and server used for a captured plan.
type Neo4jProfileMetadata struct {
	CaptureMode           string `json:"capture_mode"`
	Profiled              bool   `json:"profiled"`
	Planner               string `json:"planner,omitempty"`
	PlannerImplementation string `json:"planner_implementation,omitempty"`
	PlannerVersion        string `json:"planner_version,omitempty"`
	Runtime               string `json:"runtime,omitempty"`
	RuntimeImplementation string `json:"runtime_implementation,omitempty"`
	RuntimeVersion        string `json:"runtime_version,omitempty"`
	CypherVersion         string `json:"cypher_version,omitempty"`
	ServerAgent           string `json:"server_agent,omitempty"`
}

// Neo4jPlanNode models the recursive operator tree returned by Neo4j PROFILE or EXPLAIN.
type Neo4jPlanNode struct {
	// Operator identifies the backend plan operator at this node.
	Operator string `json:"operator"`
	// Arguments maps backend plan argument names to stable string representations.
	Arguments map[string]string `json:"arguments,omitempty"`
	// Identifiers lists variables or identifiers referenced by the Neo4j plan node.
	Identifiers []string `json:"identifiers,omitempty"`
	// EstimatedRows records planner-estimated output rows when Neo4j supplies them.
	EstimatedRows *float64 `json:"estimated_rows,omitempty"`
	// ActualRows records rows emitted by an executed PROFILE operator.
	ActualRows *int64 `json:"actual_rows,omitempty"`
	// Loops records operator loops when Neo4j exposes them as a plan argument.
	Loops *int64 `json:"loops,omitempty"`
	// DBHits records data-store accesses reported for an executed PROFILE operator.
	DBHits *int64 `json:"db_hits,omitempty"`
	// PageCacheHits records page-cache hits reported for an executed PROFILE operator.
	PageCacheHits *int64 `json:"page_cache_hits,omitempty"`
	// PageCacheMisses records page-cache misses reported for an executed PROFILE operator.
	PageCacheMisses *int64 `json:"page_cache_misses,omitempty"`
	// PageCacheHitRatio records the server-reported page-cache hit ratio.
	PageCacheHitRatio *float64 `json:"page_cache_hit_ratio,omitempty"`
	// TimeNS records operator time in nanoseconds when exposed by the Neo4j server.
	TimeNS *int64 `json:"time_ns,omitempty"`
	// InternalTraversalWork marks Neo4j 4.4 SP/ASP relationship work as opaque.
	InternalTraversalWork string `json:"internal_traversal_work,omitempty"`
	// ProfileMetadata records root planner/runtime and capture metadata.
	ProfileMetadata *Neo4jProfileMetadata `json:"profile_metadata,omitempty"`
	// Children contains child Neo4j plan operators in backend order.
	Children []Neo4jPlanNode `json:"children,omitempty"`
}

// convertNeo4jPlan recursively converts a Neo4j plan into the stable serialized plan-node schema.
func convertNeo4jPlan(plan neo4jcore.Plan) Neo4jPlanNode {
	arguments := plan.Arguments()
	node := Neo4jPlanNode{
		Operator:      normalizeNeo4jOperator(plan.Operator()),
		Arguments:     stringifyArguments(arguments),
		Identifiers:   append([]string(nil), plan.Identifiers()...),
		EstimatedRows: neo4jFloatArgument(arguments, "EstimatedRows", "estimatedRows"),
		Loops:         neo4jIntArgument(arguments, "Loops", "loops"),
	}

	for _, child := range plan.Children() {
		node.Children = append(node.Children, convertNeo4jPlan(child))
	}

	return node
}

// convertNeo4jProfiledPlan recursively converts executed PROFILE data while preserving child order.
func convertNeo4jProfiledPlan(plan neo4jcore.ProfiledPlan, opaqueInternalTraversal bool) Neo4jPlanNode {
	arguments := plan.Arguments()
	operator := normalizeNeo4jOperator(plan.Operator())
	node := Neo4jPlanNode{
		Operator:          operator,
		Arguments:         stringifyArguments(arguments),
		Identifiers:       append([]string(nil), plan.Identifiers()...),
		EstimatedRows:     neo4jFloatArgument(arguments, "EstimatedRows", "estimatedRows"),
		ActualRows:        neo4jInt64Pointer(plan.Records()),
		Loops:             neo4jIntArgument(arguments, "Loops", "loops"),
		DBHits:            neo4jInt64Pointer(plan.DbHits()),
		PageCacheHits:     neo4jInt64Pointer(plan.PageCacheHits()),
		PageCacheMisses:   neo4jInt64Pointer(plan.PageCacheMisses()),
		PageCacheHitRatio: neo4jFloat64Pointer(plan.PageCacheHitRatio()),
		TimeNS:            neo4jInt64Pointer(plan.Time()),
	}
	if opaqueInternalTraversal && strings.Contains(strings.ToLower(neo4jOperatorBase(operator)), "shortestpath") {
		node.InternalTraversalWork = "opaque"
	}

	for _, child := range plan.Children() {
		node.Children = append(node.Children, convertNeo4jProfiledPlan(child, opaqueInternalTraversal))
	}

	return node
}

func neo4jProfileMetadata(arguments map[string]any, serverAgent string, profiled bool) Neo4jProfileMetadata {
	captureMode := "EXPLAIN"
	if profiled {
		captureMode = "PROFILE"
	}

	return Neo4jProfileMetadata{
		CaptureMode:           captureMode,
		Profiled:              profiled,
		Planner:               neo4jStringArgument(arguments, "planner"),
		PlannerImplementation: neo4jStringArgument(arguments, "planner-impl"),
		PlannerVersion:        neo4jStringArgument(arguments, "planner-version"),
		Runtime:               neo4jStringArgument(arguments, "runtime"),
		RuntimeImplementation: neo4jStringArgument(arguments, "runtime-impl"),
		RuntimeVersion:        neo4jStringArgument(arguments, "runtime-version"),
		CypherVersion:         neo4jStringArgument(arguments, "version"),
		ServerAgent:           serverAgent,
	}
}

func (s Neo4jProfileMetadata) internalTraversalOpaque() bool {
	return strings.HasPrefix(s.PlannerVersion, "4.4") ||
		strings.HasPrefix(s.RuntimeVersion, "4.4") ||
		strings.Contains(s.CypherVersion, "4.4") ||
		strings.Contains(s.ServerAgent, "/4.4")
}

func neo4jStringArgument(arguments map[string]any, name string) string {
	if value, ok := arguments[name]; ok {
		return fmt.Sprint(value)
	}

	return ""
}

func neo4jFloatArgument(arguments map[string]any, names ...string) *float64 {
	for _, name := range names {
		value, ok := arguments[name]
		if !ok {
			continue
		}

		parsed, err := strconv.ParseFloat(fmt.Sprint(value), 64)
		if err == nil {
			return neo4jFloat64Pointer(parsed)
		}
	}

	return nil
}

func neo4jIntArgument(arguments map[string]any, names ...string) *int64 {
	for _, name := range names {
		value, ok := arguments[name]
		if !ok {
			continue
		}

		parsed, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err == nil {
			return neo4jInt64Pointer(parsed)
		}
	}

	return nil
}

func neo4jInt64Pointer(value int64) *int64 {
	return &value
}

func neo4jFloat64Pointer(value float64) *float64 {
	return &value
}

// stringifyArguments converts plan arguments to stable strings in a fresh map.
func stringifyArguments(arguments map[string]any) map[string]string {
	if len(arguments) == 0 {
		return nil
	}

	values := make(map[string]string, len(arguments))
	for key, value := range arguments {
		values[key] = fmt.Sprint(value)
	}

	return values
}

// neo4jOperators flattens a Neo4j plan tree in traversal order with exactly one backend suffix.
func neo4jOperators(root Neo4jPlanNode) []string {
	var (
		operators []string
		walk      func(Neo4jPlanNode)
	)

	walk = func(node Neo4jPlanNode) {
		operators = append(operators, normalizeNeo4jOperator(node.Operator))
		for _, child := range node.Children {
			walk(child)
		}
	}
	walk(root)

	return operators
}

func normalizeNeo4jOperator(operator string) string {
	base := neo4jOperatorBase(operator)
	if base == "" {
		return ""
	}

	return base + "@neo4j"
}

func neo4jOperatorBase(operator string) string {
	operator = strings.TrimSpace(operator)
	for strings.HasSuffix(operator, "@neo4j") {
		operator = strings.TrimSpace(strings.TrimSuffix(operator, "@neo4j"))
	}

	return operator
}

// cypherWithoutTerminator trims surrounding whitespace and one trailing Cypher semicolon.
func cypherWithoutTerminator(cypherQuery string) string {
	return strings.TrimSuffix(strings.TrimSpace(cypherQuery), ";")
}

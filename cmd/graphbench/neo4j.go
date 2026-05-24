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
	"strings"

	neo4jcore "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs"
	dawgsneo4j "github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

type neo4jRunner struct {
	datasetDir   string
	db           graph.Database
	planDriver   neo4jcore.Driver
	databaseName string
}

func newNeo4jRunner(ctx context.Context, datasetDir, connection string, corpus ScaleCorpus) (*neo4jRunner, error) {
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

func (s *neo4jRunner) Close(ctx context.Context) error {
	var closeErr error
	if s.planDriver != nil {
		closeErr = s.planDriver.Close()
	}
	if s.db != nil {
		if err := s.db.Close(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

func (s *neo4jRunner) Run(ctx context.Context, iterations int, corpus ScaleCorpus) ([]CaseResult, error) {
	var records []CaseResult
	casesByDataset := scaleCasesByDataset(corpus)

	for _, datasetName := range scaleCorpusDatasets(corpus) {
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

			record := s.runCase(ctx, iterations, testCase, idMap)
			records = append(records, record)
		}
	}

	return records, nil
}

func (s *neo4jRunner) runCase(ctx context.Context, iterations int, testCase ScaleCase, idMap opengraph.IDMap) CaseResult {
	params, err := resolveCaseParams(testCase, idMap)
	record := newCaseResult(testCase, ModeNeo4j, params)
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	rowCount, stats, err := measureCypher(ctx, s.db, testCase.Cypher, params, iterations)
	if err != nil {
		record.Status = StatusError
		record.Error = err.Error()
		return record
	}

	record.RowCount = rowCount
	record.Stats = stats
	applyRowExpectation(&record)

	plan, operators, err := s.explain(testCase.Cypher, params)
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

func (s *neo4jRunner) explain(cypherQuery string, params map[string]any) (*Neo4jPlanNode, []string, error) {
	session := s.planDriver.NewSession(neo4jcore.SessionConfig{
		AccessMode:   neo4jcore.AccessModeRead,
		DatabaseName: s.databaseName,
	})
	defer session.Close()

	result, err := session.Run("EXPLAIN "+cypherWithoutTerminator(cypherQuery), params)
	if err != nil {
		return nil, nil, err
	}

	summary, err := result.Consume()
	if err != nil {
		return nil, nil, err
	}
	if summary.Plan() == nil {
		return nil, nil, nil
	}

	plan := convertNeo4jPlan(summary.Plan())
	return &plan, neo4jOperators(plan), nil
}

type neo4jPlanDriverConfig struct {
	Target       string
	Username     string
	Password     string
	DatabaseName string
}

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

	return databaseName, nil
}

func openNeo4jPlanDriver(connStr string) (neo4jcore.Driver, string, error) {
	cfg, err := parseNeo4jPlanDriverConfig(connStr)
	if err != nil {
		return nil, "", err
	}

	driver, err := neo4jcore.NewDriver(cfg.Target, neo4jcore.BasicAuth(cfg.Username, cfg.Password, ""))
	if err != nil {
		return nil, "", err
	}

	return driver, cfg.DatabaseName, nil
}

type Neo4jPlanNode struct {
	Operator    string            `json:"operator"`
	Arguments   map[string]string `json:"arguments,omitempty"`
	Identifiers []string          `json:"identifiers,omitempty"`
	Children    []Neo4jPlanNode   `json:"children,omitempty"`
}

func convertNeo4jPlan(plan neo4jcore.Plan) Neo4jPlanNode {
	node := Neo4jPlanNode{
		Operator:    plan.Operator(),
		Arguments:   stringifyArguments(plan.Arguments()),
		Identifiers: append([]string(nil), plan.Identifiers()...),
	}

	for _, child := range plan.Children() {
		node.Children = append(node.Children, convertNeo4jPlan(child))
	}

	return node
}

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

func neo4jOperators(root Neo4jPlanNode) []string {
	var operators []string
	var walk func(Neo4jPlanNode)
	walk = func(node Neo4jPlanNode) {
		operators = append(operators, node.Operator+"@neo4j")
		for _, child := range node.Children {
			walk(child)
		}
	}
	walk(root)

	return operators
}

func cypherWithoutTerminator(cypherQuery string) string {
	return strings.TrimSuffix(strings.TrimSpace(cypherQuery), ";")
}

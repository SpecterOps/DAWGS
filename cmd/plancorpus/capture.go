package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	neo4jcore "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

const defaultGraphName = "integration_test"

type captureSpec struct {
	DriverName string
	Connection string
}

type backendCapture struct {
	spec        captureSpec
	db          graph.Database
	pgDriver    *pg.Driver
	pgGraphID   int32
	neo4jDriver neo4jcore.Driver
	neo4jDBName string
}

func driverFromConnectionString(connStr string) (string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("parse connection string: %w", err)
	}

	switch u.Scheme {
	case "postgres", "postgresql":
		return pg.DriverName, nil
	case neo4j.DriverName, "neo4j+s", "neo4j+ssc":
		return neo4j.DriverName, nil
	default:
		return "", fmt.Errorf("unknown connection string scheme %q", u.Scheme)
	}
}

func captureCorpus(ctx context.Context, datasetDir string, suite corpus, spec captureSpec) ([]PlanRecord, error) {
	backend, err := openBackend(ctx, suite, spec)
	if err != nil {
		return nil, err
	}
	defer backend.close(ctx)

	var records []PlanRecord
	for _, datasetName := range suite.datasetNames {
		group := suite.caseGroups[datasetName]
		if group == nil {
			continue
		}

		datasetLoaded := false
		ensureDatasetLoaded := func() error {
			if datasetLoaded {
				return nil
			}
			if err := clearGraph(ctx, backend.db); err != nil {
				return err
			}
			if err := loadDataset(ctx, backend.db, datasetDir, datasetName); err != nil {
				return err
			}
			datasetLoaded = true
			return nil
		}

		for _, file := range group.files {
			for _, testCase := range file.Cases {
				if testCase.Fixture == nil {
					if err := ensureDatasetLoaded(); err != nil {
						return nil, err
					}
				} else {
					if err := loadCommittedFixture(ctx, backend.db, testCase.Fixture); err != nil {
						return nil, err
					}
					datasetLoaded = false
				}

				record := backend.capture(ctx, CorpusQuery{
					Source:  file.path,
					Dataset: datasetName,
					Name:    testCase.Name,
					Cypher:  testCase.Cypher,
					Params:  testCase.Params,
				})
				records = append(records, record)
			}
		}
	}

	for _, file := range suite.templateFiles {
		fileName := strings.TrimSuffix(filepath.Base(file.path), filepath.Ext(file.path))

		for _, family := range file.Families {
			if family.Fixture == nil {
				return nil, fmt.Errorf("%s/%s has no fixture", file.path, family.Name)
			}

			for _, variant := range family.Variants {
				rendered, err := renderTemplate(family.Template, variant.Vars)
				if err != nil {
					return nil, fmt.Errorf("%s/%s/%s: %w", file.path, family.Name, variant.Name, err)
				}
				if err := loadCommittedFixture(ctx, backend.db, family.Fixture); err != nil {
					return nil, err
				}

				record := backend.capture(ctx, CorpusQuery{
					Source: file.path,
					Name:   fileName + "/" + family.Name + "/" + variant.Name,
					Cypher: rendered,
					Params: mergeParams(family.Params, variant.Params),
				})
				records = append(records, record)
			}
		}

		for _, family := range file.Metamorphic {
			if family.Fixture == nil {
				return nil, fmt.Errorf("%s/%s has no fixture", file.path, family.Name)
			}
			if err := loadCommittedFixture(ctx, backend.db, family.Fixture); err != nil {
				return nil, err
			}

			for _, query := range family.Queries {
				record := backend.capture(ctx, CorpusQuery{
					Source: file.path,
					Name:   fileName + "/" + family.Name + "/" + query.Name,
					Cypher: query.Cypher,
					Params: query.Params,
				})
				records = append(records, record)
			}
		}
	}

	return records, nil
}

func openBackend(ctx context.Context, suite corpus, spec captureSpec) (*backendCapture, error) {
	cfg := dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      spec.Connection,
	}

	if spec.DriverName == pg.DriverName {
		poolCfg, err := pgxpool.ParseConfig(spec.Connection)
		if err != nil {
			return nil, fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
		}
		pool, err := pg.NewPool(poolCfg)
		if err != nil {
			return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
		}
		cfg.Pool = pool
	}

	db, err := dawgs.Open(ctx, spec.DriverName, cfg)
	if err != nil {
		return nil, fmt.Errorf("open %s database: %w", spec.DriverName, err)
	}

	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  defaultGraphName,
			Nodes: suite.nodeKinds,
			Edges: suite.edgeKinds,
		}},
		DefaultGraph: graph.Graph{Name: defaultGraphName},
	}
	if err := db.AssertSchema(ctx, schema); err != nil {
		_ = db.Close(ctx)
		return nil, fmt.Errorf("assert schema: %w", err)
	}

	backend := &backendCapture{
		spec: spec,
		db:   db,
	}

	switch spec.DriverName {
	case pg.DriverName:
		pgDriver, ok := db.(*pg.Driver)
		if !ok {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("expected *pg.Driver, got %T", db)
		}
		defaultGraph, ok := pgDriver.DefaultGraph()
		if !ok {
			_ = db.Close(ctx)
			return nil, fmt.Errorf("PostgreSQL default graph is not set")
		}
		backend.pgDriver = pgDriver
		backend.pgGraphID = defaultGraph.ID

	case neo4j.DriverName:
		neo4jDriver, databaseName, err := openNeo4jPlanDriver(spec.Connection)
		if err != nil {
			_ = db.Close(ctx)
			return nil, err
		}
		backend.neo4jDriver = neo4jDriver
		backend.neo4jDBName = databaseName
	}

	return backend, nil
}

func (s *backendCapture) close(ctx context.Context) {
	if s.neo4jDriver != nil {
		_ = s.neo4jDriver.Close()
	}
	if s.db != nil {
		_ = s.db.Close(ctx)
	}
}

func (s *backendCapture) capture(ctx context.Context, query CorpusQuery) PlanRecord {
	record := PlanRecord{
		Driver:  s.spec.DriverName,
		Source:  query.Source,
		Dataset: query.Dataset,
		Name:    query.Name,
		Cypher:  query.Cypher,
		Params:  query.Params,
	}

	switch s.spec.DriverName {
	case pg.DriverName:
		s.capturePostgres(ctx, query.Cypher, query.Params, &record)
	case neo4j.DriverName:
		s.captureNeo4j(query.Cypher, query.Params, &record)
	}

	return record
}

func (s *backendCapture) capturePostgres(ctx context.Context, cypherQuery string, params map[string]any, record *PlanRecord) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	if err != nil {
		record.Error = err.Error()
		return
	}

	translation, err := translate.Translate(ctx, regularQuery, s.pgDriver.KindMapper(), params, s.pgGraphID)
	if err != nil {
		record.Error = err.Error()
		return
	}

	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		record.Error = err.Error()
		return
	}

	var plan []string
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("EXPLAIN "+sqlQuery, translation.Parameters)
		defer result.Close()

		for result.Next() {
			values := result.Values()
			if len(values) == 0 {
				continue
			}
			plan = append(plan, fmt.Sprint(values[0]))
		}

		return result.Error()
	}); err != nil {
		record.Error = err.Error()
	}

	record.SQL = sqlQuery
	record.PGPlan = plan
	record.PGOperators = postgresOperators(plan)
	record.PlannedLowerings = loweringNames(translation.Optimization.PlannedLowerings)
	record.AppliedLowerings = loweringNames(translation.Optimization.Lowerings)
	record.SkippedLowerings = append([]translate.SkippedLowering(nil), translation.Optimization.SkippedLowerings...)
	record.Optimization = &translation.Optimization
}

func (s *backendCapture) captureNeo4j(cypherQuery string, params map[string]any, record *PlanRecord) {
	session := s.neo4jDriver.NewSession(neo4jcore.SessionConfig{
		AccessMode:   neo4jcore.AccessModeWrite,
		DatabaseName: s.neo4jDBName,
	})
	defer session.Close()

	result, err := session.Run("EXPLAIN "+cypherWithoutTerminator(cypherQuery), params)
	if err != nil {
		record.Error = err.Error()
		return
	}

	summary, err := result.Consume()
	if err != nil {
		record.Error = err.Error()
		return
	}

	if plan := summary.Plan(); plan != nil {
		planNode := convertNeo4jPlan(plan)
		record.Neo4jPlan = &planNode
		record.Neo4jOperators = neo4jOperators(planNode)
	}
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

	if connectionURL.Scheme != neo4j.DriverName && connectionURL.Scheme != "neo4j+s" && connectionURL.Scheme != "neo4j+ssc" {
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

	driver, err := neo4jcore.NewDriver(
		cfg.Target,
		neo4jcore.BasicAuth(cfg.Username, cfg.Password, ""),
	)
	if err != nil {
		return nil, "", err
	}

	return driver, cfg.DatabaseName, nil
}

func clearGraph(ctx context.Context, db graph.Database) error {
	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	})
}

func loadDataset(ctx context.Context, db graph.Database, datasetDir, name string) error {
	f, err := os.Open(filepath.Join(datasetDir, name+".json"))
	if err != nil {
		return fmt.Errorf("open dataset %s: %w", name, err)
	}
	defer f.Close()

	if _, err := opengraph.Load(ctx, db, f); err != nil {
		return fmt.Errorf("load dataset %s: %w", name, err)
	}
	return nil
}

func loadCommittedFixture(ctx context.Context, db graph.Database, fixture *opengraph.Graph) error {
	if fixture == nil {
		return fmt.Errorf("fixture is nil")
	}

	if err := clearGraph(ctx, db); err != nil {
		return err
	}

	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := opengraph.WriteGraphTx(tx, fixture)
		return err
	})
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

func postgresOperators(plan []string) []string {
	operators := make([]string, 0, len(plan))
	for _, line := range plan {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimPrefix(trimmed, "->")
		trimmed = strings.TrimSpace(trimmed)
		if trimmed == "" || strings.HasPrefix(trimmed, "Planning ") {
			continue
		}
		if idx := strings.Index(trimmed, "  ("); idx >= 0 {
			trimmed = trimmed[:idx]
		}
		operators = append(operators, trimmed)
	}
	return operators
}

func neo4jOperators(root Neo4jPlanNode) []string {
	var operators []string
	var walk func(Neo4jPlanNode)
	walk = func(node Neo4jPlanNode) {
		operators = append(operators, node.Operator)
		for _, child := range node.Children {
			walk(child)
		}
	}
	walk(root)
	return operators
}

func loweringNames(decisions []optimize.LoweringDecision) []string {
	if len(decisions) == 0 {
		return nil
	}

	names := make([]string, 0, len(decisions))
	seen := make(map[string]struct{}, len(decisions))
	for _, decision := range decisions {
		name := decision.Name
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func cypherWithoutTerminator(cypherQuery string) string {
	return strings.TrimSuffix(strings.TrimSpace(cypherQuery), ";")
}

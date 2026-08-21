package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
)

// driver is the common execution surface implemented by pooled connections and explicit pgx transactions.
type driver interface {
	// Exec executes a statement and returns its PostgreSQL command tag.
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)

	// Query executes a statement and returns its streaming row set.
	Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error)

	// QueryRow executes a statement whose first row is consumed through pgx.Row.
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

// inspectingDriver records SQL and arguments before delegating execution to a connection or transaction.
type inspectingDriver struct {
	// upstreamDriver receives each operation after its SQL and arguments have been inspected.
	upstreamDriver driver
}

// Exec coordinates PostgreSQL driver behavior for exec.
func (s inspectingDriver) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	inspector().Inspect(sql, arguments)
	return s.upstreamDriver.Exec(ctx, sql, arguments...)
}

// Query constructs the SQL model used for query.
func (s inspectingDriver) Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error) {
	inspector().Inspect(sql, arguments)
	return s.upstreamDriver.Query(ctx, sql, arguments...)
}

// QueryRow coordinates PostgreSQL driver behavior for query row.
func (s inspectingDriver) QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row {
	inspector().Inspect(sql, arguments)
	return s.upstreamDriver.QueryRow(ctx, sql, arguments...)
}

// transaction binds query execution, schema resolution, and an optional pgx transaction to one graph operation context.
type transaction struct {
	// schemaManager resolves target graphs, kind identifiers, and cached Cypher translations.
	schemaManager *SchemaManager

	// translationCache is selected for the physical connection leased by this
	// transaction. Nil deliberately bypasses translation retention.
	translationCache CypherTranslationCache

	// queryExecMode selects the pgx execution protocol supplied with each query.
	queryExecMode pgx.QueryExecMode

	// queryResultsFormat selects the pgx wire format requested for returned columns.
	queryResultsFormat pgx.QueryResultFormats

	// ctx scopes all work performed by the graph transaction.
	ctx context.Context

	// conn is the acquired pooled connection underlying this transaction wrapper.
	conn *pgxpool.Conn

	// tx is the optional explicit PostgreSQL transaction used for transactional operations.
	tx pgx.Tx

	// isolation retains the isolation while transaction is assembled or evaluated.
	isolation pgx.TxIsoLevel

	// targetSchema identifies the graph selected explicitly for subsequent operations.
	targetSchema graph.Graph

	// targetSchemaSet distinguishes an explicit target from the zero-value graph schema.
	targetSchemaSet bool

	// topologyRouteDecisions is transaction-owned shadow routing state. It is
	// allocated only after an explicit stable-snapshot transaction begins.
	topologyRouteDecisions *topologyRouteDecisionCache
}

// newTransactionWrapper configures a graph transaction and optionally begins an explicit PostgreSQL transaction.
func newTransactionWrapper(ctx context.Context, conn *pgxpool.Conn, schemaManager *SchemaManager, cfg *Config, allocateTransaction bool) (*transaction, error) {
	var physicalConnection *pgx.Conn
	if conn != nil {
		physicalConnection = conn.Conn()
	}

	wrapper := &transaction{
		schemaManager:      schemaManager,
		translationCache:   schemaManager.cypherTranslationCacheForConnection(physicalConnection),
		queryExecMode:      cfg.QueryExecMode,
		queryResultsFormat: cfg.QueryResultFormats,
		ctx:                ctx,
		conn:               conn,
		isolation:          cfg.Options.IsoLevel,
		targetSchemaSet:    false,
	}

	if allocateTransaction {
		if pgxTx, err := conn.BeginTx(ctx, cfg.Options); err != nil {
			return nil, err
		} else {
			wrapper.tx = pgxTx
		}
	}
	if wrapper.tx != nil && stableSnapshotIsolation(wrapper.isolation) {
		wrapper.topologyRouteDecisions = newTopologyRouteDecisionCache()
	}

	return wrapper, nil
}

// driver returns an inspected executor backed by the active transaction or, when absent, the pooled connection.
func (s *transaction) driver() driver {
	if s.tx != nil {
		return inspectingDriver{
			upstreamDriver: s.tx,
		}
	}

	return inspectingDriver{
		upstreamDriver: s.conn,
	}
}

// GraphQueryMemoryLimit coordinates PostgreSQL driver behavior for graph query memory limit.
func (s *transaction) GraphQueryMemoryLimit() size.Size {
	return s.schemaManager.graphQueryMemoryLimit
}

// WithGraph coordinates PostgreSQL driver behavior for with graph.
func (s *transaction) WithGraph(schema graph.Graph) graph.Transaction {
	s.targetSchema = schema
	s.targetSchemaSet = true

	return s
}

// Close coordinates PostgreSQL driver behavior for close.
func (s *transaction) Close() {
	s.invalidateTopologyRouteDecisions()
	if s.tx != nil {
		s.tx.Rollback(s.ctx)
		s.tx = nil
	}
}

// getTargetGraph resolves the explicitly selected graph or falls back to the
// driver's default graph.
func (s *transaction) getTargetGraph() (model.Graph, error) {
	if !s.targetSchemaSet {
		// Look for a default graph target
		if defaultGraph, hasDefaultGraph := s.schemaManager.DefaultGraph(); !hasDefaultGraph {
			return model.Graph{}, fmt.Errorf("driver operation requires a graph target to be set")
		} else {
			return defaultGraph, nil
		}
	}

	return s.schemaManager.AssertGraph(s, s.targetSchema)
}

// targetGraphID resolves the database ID of the transaction's explicit or default graph target.
func (s *transaction) targetGraphID() (int32, error) {
	if graphTarget, err := s.getTargetGraph(); err != nil {
		return 0, err
	} else {
		return graphTarget.ID, nil
	}
}

// CreateNode coordinates PostgreSQL driver behavior for create node.
func (s *transaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	if graphTarget, err := s.getTargetGraph(); err != nil {
		return nil, err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(s.ctx, kinds); err != nil {
		return nil, err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(properties); err != nil {
		return nil, err
	} else {
		var (
			node   graph.Node
			result = s.Raw(createNodeStatement, map[string]any{
				"graph_id":   graphTarget.ID,
				"kind_ids":   kindIDSlice,
				"properties": propertiesJSONB,
			})
		)

		defer result.Close()

		if !result.Next() {
			return nil, result.Error()
		}

		return &node, result.Scan(&node)
	}
}

// UpdateNode coordinates PostgreSQL driver behavior for update node.
func (s *transaction) UpdateNode(node *graph.Node) error {
	s.invalidateTopologyRouteDecisions()
	var (
		properties       = node.Properties
		updateStatements []graph.Criteria
	)

	if addedKinds := node.AddedKinds; len(addedKinds) > 0 {
		updateStatements = append(updateStatements, query.AddKinds(query.Node(), addedKinds))
	}

	if deletedKinds := node.DeletedKinds; len(deletedKinds) > 0 {
		updateStatements = append(updateStatements, query.DeleteKinds(query.Node(), deletedKinds))
	}

	if modifiedProperties := properties.ModifiedProperties(); len(modifiedProperties) > 0 {
		updateStatements = append(updateStatements, query.SetProperties(query.Node(), modifiedProperties))
	}

	if deletedProperties := properties.DeletedProperties(); len(deletedProperties) > 0 {
		updateStatements = append(updateStatements, query.DeleteProperties(query.Node(), deletedProperties...))
	}

	return s.Nodes().Filter(query.Equals(query.NodeID(), node.ID)).Query(func(results graph.Result) error {
		// We don't need to exhaust the result set as the defered close with discard it for us
		return results.Error()
	}, updateStatements...)
}

// Nodes coordinates PostgreSQL driver behavior for nodes.
func (s *transaction) Nodes() graph.NodeQuery {
	return &nodeQuery{
		liveQuery: newLiveQuery(s.ctx, s, s.schemaManager, s.targetGraphID),
	}
}

// CreateRelationshipByIDs coordinates PostgreSQL driver behavior for create relationship by i ds.
func (s *transaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	if graphTarget, err := s.getTargetGraph(); err != nil {
		return nil, err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(s.ctx, graph.Kinds{kind}); err != nil {
		return nil, err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(properties); err != nil {
		return nil, err
	} else {
		var (
			edge   graph.Relationship
			result = s.Raw(createEdgeStatement, map[string]any{
				"graph_id":   graphTarget.ID,
				"start_id":   startNodeID,
				"end_id":     endNodeID,
				"kind_id":    kindIDSlice[0],
				"properties": propertiesJSONB,
			})
		)

		defer result.Close()

		if !result.Next() {
			return nil, result.Error()
		}

		return &edge, result.Scan(&edge)
	}
}

// UpdateRelationship coordinates PostgreSQL driver behavior for update relationship.
func (s *transaction) UpdateRelationship(relationship *graph.Relationship) error {
	s.invalidateTopologyRouteDecisions()
	var (
		modifiedProperties    = relationship.Properties.ModifiedProperties()
		deletedProperties     = relationship.Properties.DeletedProperties()
		numModifiedProperties = len(modifiedProperties)
		numDeletedProperties  = len(deletedProperties)

		statement string
		arguments []any
	)

	if numModifiedProperties > 0 {
		if jsonbArgument, err := pgsql.ValueToJSONB(modifiedProperties); err != nil {
			return err
		} else {
			arguments = append(arguments, jsonbArgument)
		}

		if numDeletedProperties > 0 {
			if textArrayArgument, err := pgsql.StringSliceToTextArray(deletedProperties); err != nil {
				return err
			} else {
				arguments = append(arguments, textArrayArgument)
			}

			statement = edgePropertySetAndDeleteStatement
		} else {
			statement = edgePropertySetOnlyStatement
		}
	} else if numDeletedProperties > 0 {
		if textArrayArgument, err := pgsql.StringSliceToTextArray(deletedProperties); err != nil {
			return err
		} else {
			arguments = append(arguments, textArrayArgument)
		}

		statement = edgePropertyDeleteOnlyStatement
	}

	_, err := s.driver().Exec(s.ctx, statement, append(arguments, relationship.ID)...)
	return err
}

// Relationships coordinates PostgreSQL driver behavior for relationships.
func (s *transaction) Relationships() graph.RelationshipQuery {
	return &relationshipQuery{
		liveQuery: newLiveQuery(s.ctx, s, s.schemaManager, s.targetGraphID),
	}
}

// query executes SQL with the transaction's configured execution mode and
// result format, adding named parameters when present.
func (s *transaction) query(query string, parameters map[string]any) (pgx.Rows, error) {
	queryArgs := []any{s.queryExecMode, s.queryResultsFormat}

	if len(parameters) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(parameters))
	}

	return s.driver().Query(s.ctx, query, queryArgs...)
}

// Query parses and translates Cypher through the schema caches, returning translation failures as graph results.
func (s *transaction) Query(query string, parameters map[string]any) graph.Result {
	if cypherMayMutate(query) {
		s.invalidateTopologyRouteDecisions()
	}
	profile := SQLGenerationProfile{QueryClass: sqlGenerationQueryClass(query)}
	if profile.QueryClass == "shortest_path" {
		if provider, ok := s.schemaManager.translationCacheProvider.(StableSnapshotTraversalWorkspaceProvider); ok {
			if err := provider.EnsureStableSnapshotTraversalWorkspaces(s.ctx, s.conn); err != nil {
				s.recordSQLGenerationProfile(profile)
				return graph.NewErrorResult(err)
			}
		}
	}
	parseStarted := time.Now()
	parsedQuery, _, err := s.schemaManager.parseCache.Parse(query)
	profile.Parse = time.Since(parseStarted)
	if err != nil {
		s.recordSQLGenerationProfile(profile)
		return graph.NewErrorResult(err)
	}
	graphStarted := time.Now()
	graphTarget, err := s.getTargetGraph()
	profile.Graph = time.Since(graphStarted)
	if err != nil {
		s.recordSQLGenerationProfile(profile)
		return graph.NewErrorResult(err)
	}
	policyStarted := time.Now()
	shape := TraversalShape{}
	if s.schemaManager.shouldClassifyTraversal() {
		shape, _ = s.schemaManager.classifyTraversalShape(query, parsedQuery)
	}
	policy, policyIdentity := s.schemaManager.effectiveTraversalPolicyForShape(query, shape, s.isolation)
	topologyPolicy, topologyPolicyIdentity := s.schemaManager.topologyFixedSuffixPolicyForShape(shape, s.isolation)
	topologyEstimatorVersion := ""
	maximumEdgeToNodeRatioPerMille := int64(0)
	if topologyPolicy.enabled() {
		topologyEstimatorVersion = topologyPolicy.compiledManifest.TopologyEstimatorVersion
		maximumEdgeToNodeRatioPerMille = topologyPolicy.compiledManifest.TopologyThresholds["maximum_edge_to_node_ratio_per_mille"]
	}
	topologyCandidate := s.topologyRouteDecision(graphTarget.ID, shape, parameters, topologyPolicyIdentity, topologyEstimatorVersion, maximumEdgeToNodeRatioPerMille, topologyPolicy.enabled())
	profile.Policy = time.Since(policyStarted)
	if topologyCandidate {
		policy = topologyPolicy
		policyIdentity = topologyPolicyIdentity
	}
	s.schemaManager.observeTraversalStrategySelection(query, shape, policy)
	buildTranslation := func(activePolicy TraversalPolicy) func() (translate.Result, string, error) {
		return func() (translate.Result, string, error) {
			var translated translate.Result
			var translateErr error
			translateStarted := time.Now()
			if activePolicy.enabled() {
				if options, optionsErr := activePolicy.productionOptionsForShape(query, shape); optionsErr != nil {
					return translate.Result{}, "", optionsErr
				} else {
					translated, translateErr = translate.TranslateWithProductionOptions(s.ctx, parsedQuery, s.schemaManager, parameters, graphTarget.ID, options)
				}
			} else {
				translated, translateErr = translate.Translate(s.ctx, parsedQuery, s.schemaManager, parameters, graphTarget.ID)
			}
			profile.Translate += time.Since(translateStarted)
			if translateErr != nil {
				return translate.Result{}, "", translateErr
			}
			formatStarted := time.Now()
			formatted, formatErr := translate.Translated(translated)
			profile.Format += time.Since(formatStarted)
			if formatErr == nil && activePolicy.enabled() && activePolicy.compiledManifest.Version == 2 {
				if anchorErr := validateTraversalPromotionSQLAnchor(activePolicy.compiledManifest, formatted); anchorErr != nil {
					return translate.Result{}, "", anchorErr
				}
			}
			return translated, formatted, formatErr
		}
	}
	translateCached := func(activePolicy TraversalPolicy, identity string) (string, map[string]any, error) {
		builder := buildTranslation(activePolicy)
		if s.translationCache == nil {
			translated, formatted, err := builder()
			if err != nil {
				return "", nil, err
			}
			return formatted, translated.Parameters, nil
		}
		return s.translationCache.TranslateWithPolicy(query, graphTarget.ID, parameters, identity, builder)
	}
	if topologyCandidate {
		cacheStarted := time.Now()
		candidateSQL, candidateParameters, candidateErr := translateCached(policy, policyIdentity)
		if candidateErr != nil {
			profile.Cache = time.Since(cacheStarted)
			s.recordSQLGenerationProfile(profile)
			return graph.NewErrorResult(candidateErr)
		}
		fallbackSQL, fallbackParameters, fallbackErr := translateCached(TraversalPolicy{}, policyIdentity+"-fallback")
		profile.Cache = time.Since(cacheStarted)
		if fallbackErr != nil {
			s.recordSQLGenerationProfile(profile)
			return graph.NewErrorResult(fallbackErr)
		}
		dispatchStarted := time.Now()
		result := s.RawSuffixReverseRetry(candidateSQL, fallbackSQL, candidateParameters, fallbackParameters, SuffixReverseRetryLimits{
			OutputRows:  policy.compiledManifest.Caps["output_row_limit"],
			OutputBytes: policy.compiledManifest.Caps["output_bytes_limit"],
		})
		profile.Dispatch = time.Since(dispatchStarted)
		s.recordSQLGenerationProfile(profile)
		return result
	}
	buildCurrentTranslation := buildTranslation(policy)

	var sqlQuery string
	var translatedParameters map[string]any
	cacheStarted := time.Now()
	if s.translationCache == nil {
		translated, translatedSQL, translateErr := buildCurrentTranslation()
		if translateErr != nil {
			profile.Cache = time.Since(cacheStarted)
			s.recordSQLGenerationProfile(profile)
			return graph.NewErrorResult(translateErr)
		}
		sqlQuery, translatedParameters = translatedSQL, translated.Parameters
	} else {
		var translateErr error
		sqlQuery, translatedParameters, translateErr = s.translationCache.TranslateWithPolicy(query, graphTarget.ID, parameters, policyIdentity, buildCurrentTranslation)
		if translateErr != nil {
			profile.Cache = time.Since(cacheStarted)
			s.recordSQLGenerationProfile(profile)
			return graph.NewErrorResult(translateErr)
		}
	}
	profile.Cache = time.Since(cacheStarted)
	dispatchStarted := time.Now()
	result := s.raw(sqlQuery, translatedParameters)
	profile.Dispatch = time.Since(dispatchStarted)
	s.recordSQLGenerationProfile(profile)
	return result
}

func (s *transaction) recordSQLGenerationProfile(profile SQLGenerationProfile) {
	if collector, ok := s.schemaManager.translationCacheProvider.(SQLGenerationProfileCollector); ok {
		collector.RecordSQLGenerationProfile(profile)
	}
}

func sqlGenerationQueryClass(query string) string {
	if strings.Contains(strings.ToLower(query), "shortestpath") {
		return "shortest_path"
	}
	return "other"
}

func cypherMayMutate(value string) bool {
	lower := strings.ToLower(value)
	for _, keyword := range []string{" create ", " merge ", " delete ", " detach delete ", " set ", " remove "} {
		if strings.Contains(" "+lower+" ", keyword) {
			return true
		}
	}
	return false
}

// Raw coordinates PostgreSQL driver behavior for raw.
func (s *transaction) Raw(query string, parameters map[string]any) graph.Result {
	s.invalidateTopologyRouteDecisions()
	return s.raw(query, parameters)
}

func (s *transaction) raw(query string, parameters map[string]any) graph.Result {
	if rows, err := s.query(query, parameters); err != nil {
		return graph.NewErrorResult(err)
	} else {
		return &queryResult{
			ctx:        s.ctx,
			rows:       rows,
			kindMapper: s.schemaManager,
		}
	}
}

// Commit coordinates PostgreSQL driver behavior for commit.
func (s *transaction) Commit() error {
	s.invalidateTopologyRouteDecisions()
	if s.tx != nil {
		return s.tx.Commit(s.ctx)
	}

	return nil
}

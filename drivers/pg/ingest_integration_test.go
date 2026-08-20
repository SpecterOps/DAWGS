//go:build manual_integration

package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const postgresIngestTestTimeout = 15 * time.Minute

var postgresIngestGraphSequence atomic.Uint64

type postgresIngestTestDatabase struct {
	ctx    context.Context
	driver *Driver
	pool   *pgxpool.Pool
}

type postgresIngestTestGraph struct {
	Target    graph.Graph
	Model     model.Graph
	NodeTable string
	EdgeTable string
}

type postgresIngestStoredNode struct {
	ID          int64
	ObjectID    string
	IDHash      pgtype.Int4
	ContentHash []byte
	Kinds       []string
	Properties  map[string]any
}

type postgresIngestStoredEdge struct {
	ID                  int64
	StartID             int64
	EndID               int64
	StartObjectID       pgtype.Text
	EndObjectID         pgtype.Text
	Kind                string
	IDHash              pgtype.Int4
	ContentHash         []byte
	Properties          map[string]any
	ActualStartObjectID string
	ActualEndObjectID   string
}

type postgresIngestPhaseExpectation struct {
	InputRecords       int64
	CoalescedRecords   int64
	PopulatedBuckets   int64
	IdentityRowsRead   int64
	HashMatches        int64
	StagedInserts      int64
	StagedUpdates      int64
	CommittedMutations int64
}

type postgresIngestLogicalNode struct {
	ObjectID   string
	Kinds      []string
	Properties map[string]any
}

type postgresIngestLogicalEdge struct {
	StartObjectID string
	Kind          string
	EndObjectID   string
	Properties    map[string]any
}

type postgresIngestLogicalGraph struct {
	Nodes []postgresIngestLogicalNode
	Edges []postgresIngestLogicalEdge
}

func TestPostgresIngestEdgeRowShapeValidation(t *testing.T) {
	t.Parallel()

	valid := pgtype.Text{String: "resolved", Valid: true}
	for _, test := range []struct {
		name          string
		kind          pgtype.Text
		startObjectID pgtype.Text
		endObjectID   pgtype.Text
		errorContains string
	}{
		{
			name:          "complete row",
			kind:          valid,
			startObjectID: valid,
			endObjectID:   valid,
		},
		{
			name:          "unmapped kind",
			startObjectID: valid,
			endObjectID:   valid,
			errorContains: "kind",
		},
		{
			name:          "missing start endpoint",
			kind:          valid,
			endObjectID:   valid,
			errorContains: "start endpoint",
		},
		{
			name:          "empty end objectid",
			kind:          valid,
			startObjectID: valid,
			endObjectID:   pgtype.Text{Valid: true},
			errorContains: "end endpoint",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostgresIngestEdgeRowShape(
				41,
				test.kind,
				test.startObjectID,
				test.endObjectID,
			)
			if test.errorContains == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.errorContains)
			}
		})
	}
}

func TestPostgresIngestEndToEnd(t *testing.T) {
	testDB := newPostgresIngestTestDatabase(t)

	t.Run("schema and existing API compatibility", func(t *testing.T) {
		testPostgresIngestSchemaAndAPICompatibility(t, testDB)
	})
	t.Run("fresh replay and partial merge", func(t *testing.T) {
		testPostgresIngestReplayAndPartialMerge(t, testDB)
	})
	t.Run("edge identities endpoints and legacy sources", func(t *testing.T) {
		testPostgresIngestEdgeIdentities(t, testDB)
	})
	t.Run("forced identity hash collisions", func(t *testing.T) {
		testPostgresIngestForcedCollisions(t, testDB)
	})
	t.Run("partial bucket commit repair and retry", func(t *testing.T) {
		testPostgresIngestPartialCommitRetry(t, testDB)
	})
	t.Run("bucket count equivalence", func(t *testing.T) {
		testPostgresIngestBucketEquivalence(t, testDB)
	})
	t.Run("target-only clustering", func(t *testing.T) {
		testPostgresIngestTargetOnlyClustering(t, testDB)
	})
}

func newPostgresIngestTestDatabase(t *testing.T) *postgresIngestTestDatabase {
	t.Helper()

	connectionString := os.Getenv("CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	parsed, err := url.Parse(connectionString)
	require.NoError(t, err)
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	ctx, cancel := context.WithTimeout(context.Background(), postgresIngestTestTimeout)
	poolConfig, err := pgxpool.ParseConfig(connectionString)
	require.NoError(t, err)
	pool, err := NewPool(poolConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		pool.Close()
	})
	require.NoError(t, pool.Ping(ctx))

	driver := NewDriver(0, pool)
	require.NoError(t, driver.AssertSchema(ctx, graph.Schema{}))

	return &postgresIngestTestDatabase{
		ctx:    ctx,
		driver: driver,
		pool:   pool,
	}
}

func (s *postgresIngestTestDatabase) newGraph(t *testing.T) *postgresIngestTestGraph {
	t.Helper()

	name := fmt.Sprintf(
		"dawgs_ingest_e2e_%d_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
		postgresIngestGraphSequence.Add(1),
	)
	target := graph.Graph{
		Name: name,
		NodeConstraints: []graph.Constraint{{
			Field: "objectid",
			Type:  graph.BTreeIndex,
		}},
	}

	t.Cleanup(func() {
		if err := s.cleanupGraph(name); err != nil {
			t.Errorf("clean up PostgreSQL ingest test graph %q: %v", name, err)
		}
	})

	var graphModel model.Graph
	err := s.driver.SchemaManager.WriteTransaction(s.ctx, func(tx graph.Transaction) error {
		var err error
		graphModel, err = s.driver.SchemaManager.AssertGraph(tx, target)
		return err
	})
	require.NoError(t, err)
	require.Positive(t, graphModel.ID)
	require.Equal(t, model.NodePartitionTableName(graphModel.ID), graphModel.Partitions.Node.Name)
	require.Equal(t, model.EdgePartitionTableName(graphModel.ID), graphModel.Partitions.Edge.Name)
	require.Len(t, graphModel.Partitions.Node.Constraints, 1)
	require.Empty(t, graphModel.Partitions.Edge.Constraints)

	result := &postgresIngestTestGraph{
		Target:    target,
		Model:     graphModel,
		NodeTable: graphModel.Partitions.Node.Name,
		EdgeTable: graphModel.Partitions.Edge.Name,
	}
	s.requireExactObjectIDConstraint(t, result)

	return result
}

func (s *postgresIngestTestDatabase) cleanupGraph(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var graphID int32
	err := s.pool.QueryRow(ctx, "select id::integer from graph where name = $1", name).Scan(&graphID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("look up graph: %w", err)
	}

	var cleanupErr error
	for _, tableName := range []string{
		model.EdgePartitionTableName(graphID),
		model.NodePartitionTableName(graphID),
	} {
		statement := "drop table if exists " + pgx.Identifier{tableName}.Sanitize() + ";"
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("drop partition %q: %w", tableName, err))
		}
	}
	if _, err := s.pool.Exec(ctx, "delete from graph where id = $1 and name = $2", graphID, name); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete graph row: %w", err))
	}

	return cleanupErr
}

func (s *postgresIngestTestDatabase) requireExactObjectIDConstraint(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
) {
	t.Helper()

	expectedName := model.ConstraintName(testGraph.NodeTable, graph.Constraint{
		Field: "objectid",
		Type:  graph.BTreeIndex,
	})
	rows, err := s.pool.Query(s.ctx, `
		select table_namespace.nspname,
		       table_class.relname,
		       index_namespace.nspname,
		       index_class.relname,
		       access_method.amname,
		       index_definition.indisunique,
		       index_definition.indisprimary,
		       index_definition.indisvalid,
		       index_definition.indisready,
		       index_definition.indislive,
		       index_definition.indnkeyatts,
		       index_definition.indnatts,
		       index_definition.indpred is null,
		       pg_get_expr(index_definition.indexprs, index_definition.indrelid, false),
		       pg_get_indexdef(index_definition.indexrelid, 1, false),
		       pg_get_indexdef(index_definition.indexrelid)
		from pg_index as index_definition
		join pg_class as table_class on table_class.oid = index_definition.indrelid
		join pg_namespace as table_namespace on table_namespace.oid = table_class.relnamespace
		join pg_class as index_class on index_class.oid = index_definition.indexrelid
		join pg_namespace as index_namespace on index_namespace.oid = index_class.relnamespace
		join pg_am as access_method on access_method.oid = index_class.relam
		where table_namespace.nspname = 'public'
		  and table_class.relname = $1
		  and index_definition.indisunique
		  and index_definition.indexprs is not null
		order by index_class.relname
	`, testGraph.NodeTable)
	require.NoError(t, err)
	t.Cleanup(rows.Close)

	type definition struct {
		schema      string
		table       string
		indexSchema string
		name        string
		method      string
		unique      bool
		primary     bool
		valid       bool
		ready       bool
		live        bool
		keyCount    int16
		columnCount int16
		nonPartial  bool
		expression  string
		indexColumn string
		definition  string
	}
	definitions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (definition, error) {
		var value definition
		err := row.Scan(
			&value.schema,
			&value.table,
			&value.indexSchema,
			&value.name,
			&value.method,
			&value.unique,
			&value.primary,
			&value.valid,
			&value.ready,
			&value.live,
			&value.keyCount,
			&value.columnCount,
			&value.nonPartial,
			&value.expression,
			&value.indexColumn,
			&value.definition,
		)
		return value, err
	})
	require.NoError(t, err)
	require.Len(t, definitions, 1)
	actual := definitions[0]
	require.Equal(t, "public", actual.schema)
	require.Equal(t, testGraph.NodeTable, actual.table)
	require.Equal(t, "public", actual.indexSchema)
	require.Equal(t, expectedName, actual.name)
	require.Equal(t, "btree", actual.method)
	require.True(t, actual.unique)
	require.False(t, actual.primary)
	require.True(t, actual.valid)
	require.True(t, actual.ready)
	require.True(t, actual.live)
	require.EqualValues(t, 1, actual.keyCount)
	require.EqualValues(t, 1, actual.columnCount)
	require.True(t, actual.nonPartial)
	require.Equal(t,
		"properties ->> 'objectid'::text",
		normalizePostgresIngestIndexExpression(actual.expression),
	)
	require.Equal(t,
		"properties ->> 'objectid'::text",
		normalizePostgresIngestIndexExpression(actual.indexColumn),
	)
	require.NotEmpty(t, actual.definition)
}

func normalizePostgresIngestIndexExpression(expression string) string {
	normalized := strings.Join(strings.Fields(expression), " ")
	for postgresIngestExpressionHasOuterParentheses(normalized) {
		normalized = strings.TrimSpace(normalized[1 : len(normalized)-1])
	}

	return normalized
}

func postgresIngestExpressionHasOuterParentheses(expression string) bool {
	if len(expression) < 2 || expression[0] != '(' || expression[len(expression)-1] != ')' {
		return false
	}

	depth := 0
	quoted := false
	for index := 0; index < len(expression); index++ {
		switch expression[index] {
		case '\'':
			if quoted && index+1 < len(expression) && expression[index+1] == '\'' {
				index++
				continue
			}
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
				if depth == 0 && index != len(expression)-1 {
					return false
				}
			}
		}
	}

	return depth == 0 && !quoted
}

func postgresIngestTestSequence[T any](records ...T) iter.Seq2[T, error] {
	var consumed atomic.Bool

	return func(yield func(T, error) bool) {
		if !consumed.CompareAndSwap(false, true) {
			var zero T
			yield(zero, errors.New("PostgreSQL ingest test iterator was consumed more than once"))
			return
		}
		for _, record := range records {
			if !yield(record, nil) {
				return
			}
		}
	}
}

func postgresIngestTestInput(nodes []*IngestNode, edges []*IngestEdge) IngestInput {
	input := IngestInput{}
	if nodes != nil {
		input.Nodes = postgresIngestTestSequence(nodes...)
	}
	if edges != nil {
		input.Edges = postgresIngestTestSequence(edges...)
	}

	return input
}

func postgresIngestProperties(values map[string]any) *graph.Properties {
	return graph.AsProperties(values)
}

func requirePostgresIngestPhaseStats(
	t *testing.T,
	actual IngestPhaseStats,
	expected postgresIngestPhaseExpectation,
) {
	t.Helper()

	require.Equal(t, expected.InputRecords, actual.InputRecords, "input records")
	require.Equal(t, expected.CoalescedRecords, actual.CoalescedRecords, "coalesced records")
	require.Equal(t, expected.PopulatedBuckets, actual.PopulatedBuckets, "populated buckets")
	require.Equal(t, expected.IdentityRowsRead, actual.IdentityRowsRead, "identity rows read")
	require.Equal(t, expected.HashMatches, actual.HashMatches, "hash matches")
	require.Equal(t, expected.StagedInserts, actual.StagedInserts, "staged inserts")
	require.Equal(t, expected.StagedUpdates, actual.StagedUpdates, "staged updates")
	require.Equal(t, expected.CommittedMutations, actual.CommittedMutations, "committed mutations")
	if expected.InputRecords == 0 {
		require.Zero(t, actual.SpoolBytes)
	} else {
		require.Positive(t, actual.SpoolBytes)
	}
	require.GreaterOrEqual(t, actual.Duration, time.Duration(0))
}

func (s *postgresIngestTestDatabase) readNodes(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
) []postgresIngestStoredNode {
	t.Helper()

	statement := fmt.Sprintf(`
		select n.id,
		       n.properties->>'objectid',
		       n.id_hash,
		       n.content_hash,
		       coalesce((
		         select array_agg(k.name::text order by convert_to(k.name::text, 'UTF8'))
		         from unnest(n.kind_ids) as requested_kind(id)
		         join kind as k on k.id = requested_kind.id
		       ), array[]::text[]),
		       n.properties
		from %s as n
		order by convert_to(n.properties->>'objectid', 'UTF8'), n.id
	`, pgx.Identifier{testGraph.NodeTable}.Sanitize())
	rows, err := s.pool.Query(s.ctx, statement)
	require.NoError(t, err)
	defer rows.Close()

	var result []postgresIngestStoredNode
	for rows.Next() {
		var (
			row        postgresIngestStoredNode
			properties []byte
		)
		require.NoError(t, rows.Scan(
			&row.ID,
			&row.ObjectID,
			&row.IDHash,
			&row.ContentHash,
			&row.Kinds,
			&properties,
		))
		row.Properties = decodePostgresIngestProperties(t, properties)
		result = append(result, row)
	}
	require.NoError(t, rows.Err())

	return result
}

func (s *postgresIngestTestDatabase) readEdges(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
) []postgresIngestStoredEdge {
	t.Helper()

	rawRowCount := s.countPartitionRows(t, testGraph.EdgeTable)
	statement := fmt.Sprintf(`
		select e.id,
		       e.start_id,
		       e.end_id,
		       e.start_object_id,
		       e.end_object_id,
		       k.name::text,
		       e.id_hash,
		       e.content_hash,
		       e.properties,
		       start_node.properties->>'objectid',
		       end_node.properties->>'objectid'
		from %s as e
		left join kind as k on k.id = e.kind_id
		left join %s as start_node on start_node.id = e.start_id
		left join %s as end_node on end_node.id = e.end_id
		order by coalesce(e.start_object_id, start_node.properties->>'objectid'),
		         convert_to(coalesce(k.name::text, ''), 'UTF8'),
		         coalesce(e.end_object_id, end_node.properties->>'objectid'),
		         e.id
	`,
		pgx.Identifier{testGraph.EdgeTable}.Sanitize(),
		pgx.Identifier{testGraph.NodeTable}.Sanitize(),
		pgx.Identifier{testGraph.NodeTable}.Sanitize(),
	)
	rows, err := s.pool.Query(s.ctx, statement)
	require.NoError(t, err)
	defer rows.Close()

	var result []postgresIngestStoredEdge
	for rows.Next() {
		var (
			row                 postgresIngestStoredEdge
			kind                pgtype.Text
			actualStartObjectID pgtype.Text
			actualEndObjectID   pgtype.Text
			properties          []byte
		)
		require.NoError(t, rows.Scan(
			&row.ID,
			&row.StartID,
			&row.EndID,
			&row.StartObjectID,
			&row.EndObjectID,
			&kind,
			&row.IDHash,
			&row.ContentHash,
			&properties,
			&actualStartObjectID,
			&actualEndObjectID,
		))
		require.NoError(t, validatePostgresIngestEdgeRowShape(
			row.ID,
			kind,
			actualStartObjectID,
			actualEndObjectID,
		))
		row.Kind = kind.String
		row.ActualStartObjectID = actualStartObjectID.String
		row.ActualEndObjectID = actualEndObjectID.String
		row.Properties = decodePostgresIngestProperties(t, properties)
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, rawRowCount, int64(len(result)), "direct edge reader must cover every raw partition row")

	return result
}

func validatePostgresIngestEdgeRowShape(
	edgeID int64,
	kind pgtype.Text,
	actualStartObjectID pgtype.Text,
	actualEndObjectID pgtype.Text,
) error {
	var unresolved []string
	if !kind.Valid || kind.String == "" {
		unresolved = append(unresolved, "kind")
	}
	if !actualStartObjectID.Valid || actualStartObjectID.String == "" {
		unresolved = append(unresolved, "start endpoint")
	}
	if !actualEndObjectID.Valid || actualEndObjectID.String == "" {
		unresolved = append(unresolved, "end endpoint")
	}
	if len(unresolved) != 0 {
		return fmt.Errorf(
			"persisted edge row %d cannot resolve %s",
			edgeID,
			strings.Join(unresolved, ", "),
		)
	}

	return nil
}

func decodePostgresIngestProperties(t *testing.T, encoded []byte) map[string]any {
	t.Helper()

	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var properties map[string]any
	require.NoError(t, decoder.Decode(&properties))
	var trailing any
	require.ErrorIs(t, decoder.Decode(&trailing), io.EOF)

	return properties
}

func requirePostgresIngestNodeHash(t *testing.T, node postgresIngestStoredNode) {
	t.Helper()

	require.True(t, node.IDHash.Valid)
	require.Equal(t, int32(hashIngestNodeIdentity(node.ObjectID)), node.IDHash.Int32)
	require.Len(t, node.ContentHash, ingestContentHashLength)
	expected, err := hashIngestNodeContent(graph.StringsToKinds(node.Kinds), node.Properties)
	require.NoError(t, err)
	require.Equal(t, expected[:], node.ContentHash)
}

func requirePostgresIngestEdgeHash(t *testing.T, edge postgresIngestStoredEdge) {
	t.Helper()

	require.True(t, edge.StartObjectID.Valid)
	require.True(t, edge.EndObjectID.Valid)
	require.Equal(t, edge.StartObjectID.String, edge.ActualStartObjectID)
	require.Equal(t, edge.EndObjectID.String, edge.ActualEndObjectID)
	require.True(t, edge.IDHash.Valid)
	require.Equal(t, int32(hashIngestEdgeIdentity(
		edge.StartObjectID.String,
		edge.Kind,
		edge.EndObjectID.String,
	)), edge.IDHash.Int32)
	require.Len(t, edge.ContentHash, ingestContentHashLength)
	expected, err := hashIngestEdgeContent(edge.Properties)
	require.NoError(t, err)
	require.Equal(t, expected[:], edge.ContentHash)
}

func requirePostgresIngestJSON(t *testing.T, expected string, actual map[string]any) {
	t.Helper()

	encoded, err := json.Marshal(actual)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(encoded))
}

func (s *postgresIngestTestDatabase) logicalGraph(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
) postgresIngestLogicalGraph {
	t.Helper()

	nodes := s.readNodes(t, testGraph)
	edges := s.readEdges(t, testGraph)
	result := postgresIngestLogicalGraph{
		Nodes: make([]postgresIngestLogicalNode, 0, len(nodes)),
		Edges: make([]postgresIngestLogicalEdge, 0, len(edges)),
	}
	for _, node := range nodes {
		requirePostgresIngestNodeHash(t, node)
		result.Nodes = append(result.Nodes, postgresIngestLogicalNode{
			ObjectID:   node.ObjectID,
			Kinds:      append([]string(nil), node.Kinds...),
			Properties: node.Properties,
		})
	}
	for _, edge := range edges {
		requirePostgresIngestEdgeHash(t, edge)
		result.Edges = append(result.Edges, postgresIngestLogicalEdge{
			StartObjectID: edge.StartObjectID.String,
			Kind:          edge.Kind,
			EndObjectID:   edge.EndObjectID.String,
			Properties:    edge.Properties,
		})
	}

	return result
}

func postgresIngestBucketCountForNodes(records []*IngestNode, bucketCount uint64) int64 {
	buckets, err := newIngestBucketSet(bucketCount)
	if err != nil {
		panic(err)
	}
	populated := map[uint64]struct{}{}
	for _, record := range records {
		populated[buckets.Bucket(hashIngestNodeIdentity(record.ObjectID))] = struct{}{}
	}

	return int64(len(populated))
}

func postgresIngestBucketCountForEdges(records []*IngestEdge, bucketCount uint64) int64 {
	buckets, err := newIngestBucketSet(bucketCount)
	if err != nil {
		panic(err)
	}
	populated := map[uint64]struct{}{}
	for _, record := range records {
		populated[buckets.Bucket(hashIngestEdgeIdentity(
			record.StartObjectID,
			record.Kind.String(),
			record.EndObjectID,
		))] = struct{}{}
	}

	return int64(len(populated))
}

func postgresIngestSortedKinds(kinds ...string) []string {
	result := append([]string(nil), kinds...)
	sort.Strings(result)
	return result
}

func testPostgresIngestSchemaAndAPICompatibility(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	testGraph := testDB.newGraph(t)
	testDB.requireSchemaCatalog(t, testGraph)

	var (
		startNode *graph.Node
		endNode   *graph.Node
	)
	err := testDB.driver.WriteTransaction(testDB.ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(testGraph.Target)

		var err error
		startNode, err = tx.CreateNode(postgresIngestProperties(map[string]any{
			"objectid": "api-start",
			"name":     "legacy start",
		}), graph.StringKind("APIExistingNode"))
		if err != nil {
			return err
		}
		endNode, err = tx.CreateNode(postgresIngestProperties(map[string]any{
			"objectid": "api-end",
			"name":     "legacy end",
		}), graph.StringKind("APIExistingNode"))
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(
			startNode.ID,
			endNode.ID,
			graph.StringKind("APIExistingEdge"),
			postgresIngestProperties(map[string]any{"legacy": true}),
		)
		return err
	})
	require.NoError(t, err)

	nodes := testDB.readNodes(t, testGraph)
	require.Len(t, nodes, 2)
	for _, node := range nodes {
		require.False(t, node.IDHash.Valid, node.ObjectID)
		require.Nil(t, node.ContentHash, node.ObjectID)
		require.Equal(t, []string{"APIExistingNode"}, node.Kinds)
	}
	require.Equal(t, "api-end", nodes[0].ObjectID)
	requirePostgresIngestJSON(t, `{"name":"legacy end","objectid":"api-end"}`, nodes[0].Properties)
	require.Equal(t, "api-start", nodes[1].ObjectID)
	requirePostgresIngestJSON(t, `{"name":"legacy start","objectid":"api-start"}`, nodes[1].Properties)

	edges := testDB.readEdges(t, testGraph)
	require.Len(t, edges, 1)
	require.Equal(t, startNode.ID.Int64(), edges[0].StartID)
	require.Equal(t, endNode.ID.Int64(), edges[0].EndID)
	require.Equal(t, "APIExistingEdge", edges[0].Kind)
	requirePostgresIngestJSON(t, `{"legacy":true}`, edges[0].Properties)
	require.False(t, edges[0].IDHash.Valid)
	require.Nil(t, edges[0].ContentHash)
	require.False(t, edges[0].StartObjectID.Valid)
	require.False(t, edges[0].EndObjectID.Valid)
}

func (s *postgresIngestTestDatabase) requireSchemaCatalog(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
) {
	t.Helper()

	type expectedColumn struct {
		dataType string
		nullable string
	}
	expectedColumns := map[string]expectedColumn{}
	for _, tableName := range []string{"node", testGraph.NodeTable} {
		expectedColumns[tableName+".id_hash"] = expectedColumn{"int4", "YES"}
		expectedColumns[tableName+".content_hash"] = expectedColumn{"bytea", "YES"}
	}
	for _, tableName := range []string{"edge", testGraph.EdgeTable} {
		expectedColumns[tableName+".id_hash"] = expectedColumn{"int4", "YES"}
		expectedColumns[tableName+".content_hash"] = expectedColumn{"bytea", "YES"}
		expectedColumns[tableName+".start_object_id"] = expectedColumn{"text", "YES"}
		expectedColumns[tableName+".end_object_id"] = expectedColumn{"text", "YES"}
	}

	rows, err := s.pool.Query(s.ctx, `
		select table_name, column_name, udt_name, is_nullable
		from information_schema.columns
		where table_schema = 'public'
		  and table_name = any($1::text[])
		  and column_name = any($2::text[])
		order by table_name, column_name
	`,
		[]string{"node", "edge", testGraph.NodeTable, testGraph.EdgeTable},
		[]string{"id_hash", "content_hash", "start_object_id", "end_object_id"},
	)
	require.NoError(t, err)
	actualColumns := map[string]expectedColumn{}
	for rows.Next() {
		var tableName, columnName string
		var value expectedColumn
		require.NoError(t, rows.Scan(&tableName, &columnName, &value.dataType, &value.nullable))
		actualColumns[tableName+"."+columnName] = value
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, expectedColumns, actualColumns)

	rows, err = s.pool.Query(s.ctx, `
		select table_class.relname, constraint_definition.convalidated,
		       pg_get_expr(constraint_definition.conbin, constraint_definition.conrelid)
		from pg_constraint as constraint_definition
		join pg_class as table_class on table_class.oid = constraint_definition.conrelid
		join pg_namespace as table_namespace on table_namespace.oid = table_class.relnamespace
		where table_namespace.nspname = 'public'
		  and table_class.relname = any($1::text[])
		  and constraint_definition.contype = 'c'
		order by table_class.relname
	`, []string{"node", "edge"})
	require.NoError(t, err)
	checks := map[string]string{}
	for rows.Next() {
		var tableName, expression string
		var validated bool
		require.NoError(t, rows.Scan(&tableName, &validated, &expression))
		require.True(t, validated, tableName)
		checks[tableName] = strings.Join(strings.Fields(expression), " ")
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Len(t, checks, 2)
	for _, tableName := range []string{"node", "edge"} {
		require.Contains(t, checks[tableName], "content_hash IS NULL")
		require.Contains(t, checks[tableName], "octet_length(content_hash) = 16")
	}

	rows, err = s.pool.Query(s.ctx, `
		select tablename, indexname, indexdef
		from pg_indexes
		where schemaname = 'public'
		  and indexname = any($1::text[])
		order by indexname
	`, []string{"node_id_hash_index", "edge_id_hash_index"})
	require.NoError(t, err)
	parentIndexes := map[string]string{}
	for rows.Next() {
		var tableName, indexName, definition string
		require.NoError(t, rows.Scan(&tableName, &indexName, &definition))
		parentIndexes[tableName+"."+indexName] = strings.ToLower(definition)
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Len(t, parentIndexes, 2)
	require.Contains(t, parentIndexes["node.node_id_hash_index"], "using btree (id_hash)")
	require.Contains(t, parentIndexes["edge.edge_id_hash_index"], "using btree (id_hash)")

	rows, err = s.pool.Query(s.ctx, `
		select table_class.relname, count(*)
		from pg_index as index_definition
		join pg_class as table_class on table_class.oid = index_definition.indrelid
		join pg_class as index_class on index_class.oid = index_definition.indexrelid
		join pg_am as access_method on access_method.oid = index_class.relam
		join pg_attribute as attribute
		  on attribute.attrelid = index_definition.indrelid
		 and attribute.attnum = index_definition.indkey[0]
		where table_class.relname = any($1::text[])
		  and access_method.amname = 'btree'
		  and index_definition.indnkeyatts = 1
		  and index_definition.indpred is null
		  and index_definition.indexprs is null
		  and attribute.attname = 'id_hash'
		group by table_class.relname
		order by table_class.relname
	`, []string{testGraph.NodeTable, testGraph.EdgeTable})
	require.NoError(t, err)
	childIndexCounts := map[string]int64{}
	for rows.Next() {
		var tableName string
		var count int64
		require.NoError(t, rows.Scan(&tableName, &count))
		childIndexCounts[tableName] = count
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, map[string]int64{
		testGraph.NodeTable: 1,
		testGraph.EdgeTable: 1,
	}, childIndexCounts)

	type functionContract struct {
		arguments  string
		volatility string
		parallel   string
		strict     bool
		result     string
	}
	expectedFunctions := map[string]functionContract{
		"dawgs_ingest_u64be":             {"bigint", "i", "s", true, "bytea"},
		"dawgs_ingest_zigzag_varint":     {"bigint", "i", "s", true, "bytea"},
		"dawgs_ingest_canonical_number":  {"text", "i", "s", true, "bytea"},
		"dawgs_ingest_canonical_jsonb":   {"jsonb", "i", "s", true, "bytea"},
		"dawgs_ingest_node_content_hash": {"smallint[], jsonb", "s", "s", true, "bytea"},
		"dawgs_ingest_edge_content_hash": {"jsonb", "i", "s", true, "bytea"},
	}
	functionNames := make([]string, 0, len(expectedFunctions))
	for name := range expectedFunctions {
		functionNames = append(functionNames, name)
	}
	rows, err = s.pool.Query(s.ctx, `
		select procedure_definition.proname,
		       oidvectortypes(procedure_definition.proargtypes),
		       procedure_definition.provolatile::text,
		       procedure_definition.proparallel::text,
		       procedure_definition.proisstrict,
		       pg_get_function_result(procedure_definition.oid)
		from pg_proc as procedure_definition
		join pg_namespace as procedure_namespace
		  on procedure_namespace.oid = procedure_definition.pronamespace
		where procedure_namespace.nspname = 'public'
		  and procedure_definition.proname = any($1::text[])
		order by procedure_definition.proname
	`, functionNames)
	require.NoError(t, err)
	actualFunctions := map[string]functionContract{}
	for rows.Next() {
		var name string
		var contract functionContract
		require.NoError(t, rows.Scan(
			&name,
			&contract.arguments,
			&contract.volatility,
			&contract.parallel,
			&contract.strict,
			&contract.result,
		))
		actualFunctions[name] = contract
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, expectedFunctions, actualFunctions)
}

func testPostgresIngestReplayAndPartialMerge(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	testGraph := testDB.newGraph(t)
	options := IngestOptions{
		BucketCount: 1,
		TempDir:     t.TempDir(),
	}

	nodes, edges := postgresIngestCoreRecords()
	stats, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nodes, edges),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:       2,
		CoalescedRecords:   2,
		PopulatedBuckets:   1,
		IdentityRowsRead:   0,
		StagedInserts:      2,
		CommittedMutations: 2,
	})
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   0,
		StagedInserts:      1,
		CommittedMutations: 1,
	})
	require.Zero(t, stats.ClusterDuration)
	require.GreaterOrEqual(t, stats.TotalDuration, time.Duration(0))

	initialNodes, initialEdges := requirePostgresIngestCoreState(t, testDB, testGraph, false)
	initialAlphaHash := append([]byte(nil), initialNodes[0].ContentHash...)
	initialEdgeHash := append([]byte(nil), initialEdges[0].ContentHash...)

	nodes, edges = postgresIngestCoreRecords()
	stats, err = testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nodes, edges),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:     2,
		CoalescedRecords: 2,
		PopulatedBuckets: 1,
		IdentityRowsRead: 2,
		HashMatches:      2,
	})
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:     1,
		CoalescedRecords: 1,
		PopulatedBuckets: 1,
		IdentityRowsRead: 1,
		HashMatches:      1,
	})

	stats, err = testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(
			[]*IngestNode{{
				ObjectID: "alpha",
				Kinds:    graph.Kinds{graph.StringKind("Person")},
				Properties: postgresIngestProperties(map[string]any{
					"name": "Alice",
				}),
			}},
			[]*IngestEdge{{
				StartObjectID: "alpha",
				EndObjectID:   "beta",
				Kind:          graph.StringKind("Knows"),
				Properties: postgresIngestProperties(map[string]any{
					"weight": 1,
				}),
			}},
		),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   2,
		StagedUpdates:      1,
		CommittedMutations: 1,
	})
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   1,
		StagedUpdates:      1,
		CommittedMutations: 1,
	})
	noOpNodes, noOpEdges := requirePostgresIngestCoreState(t, testDB, testGraph, false)
	require.Equal(t, initialAlphaHash, noOpNodes[0].ContentHash)
	require.Equal(t, initialEdgeHash, noOpEdges[0].ContentHash)

	stats, err = testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(
			[]*IngestNode{{
				ObjectID: "alpha",
				Kinds: graph.Kinds{
					graph.StringKind("Admin"),
					graph.StringKind("Person"),
				},
				Properties: postgresIngestProperties(map[string]any{
					"active": true,
					"name":   "Alicia",
				}),
			}},
			[]*IngestEdge{{
				StartObjectID: "alpha",
				EndObjectID:   "beta",
				Kind:          graph.StringKind("Knows"),
				Properties: postgresIngestProperties(map[string]any{
					"changed": true,
					"weight":  2,
				}),
			}},
		),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   2,
		StagedUpdates:      1,
		CommittedMutations: 1,
	})
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   1,
		StagedUpdates:      1,
		CommittedMutations: 1,
	})
	requirePostgresIngestCoreState(t, testDB, testGraph, true)
}

func postgresIngestCoreRecords() ([]*IngestNode, []*IngestEdge) {
	return []*IngestNode{
			{
				ObjectID: "alpha",
				Kinds: graph.Kinds{
					graph.StringKind("Person"),
					graph.StringKind("Account"),
				},
				Properties: postgresIngestProperties(map[string]any{
					"name":   "Alice",
					"stable": "node-value",
					"nested": map[string]any{"rank": 7},
				}),
			},
			{
				ObjectID: "beta",
				Kinds:    graph.Kinds{graph.StringKind("Person")},
				Properties: postgresIngestProperties(map[string]any{
					"name": "Bob",
				}),
			},
		}, []*IngestEdge{{
			StartObjectID: "alpha",
			EndObjectID:   "beta",
			Kind:          graph.StringKind("Knows"),
			Properties: postgresIngestProperties(map[string]any{
				"note":   "full edge state",
				"weight": 1,
			}),
		}}
}

func requirePostgresIngestCoreState(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
	testGraph *postgresIngestTestGraph,
	changed bool,
) ([]postgresIngestStoredNode, []postgresIngestStoredEdge) {
	t.Helper()

	nodes := testDB.readNodes(t, testGraph)
	require.Len(t, nodes, 2)
	require.Equal(t, "alpha", nodes[0].ObjectID)
	if changed {
		require.Equal(t, postgresIngestSortedKinds("Account", "Admin", "Person"), nodes[0].Kinds)
		requirePostgresIngestJSON(t, `{
			"active":true,
			"name":"Alicia",
			"nested":{"rank":7},
			"objectid":"alpha",
			"stable":"node-value"
		}`, nodes[0].Properties)
	} else {
		require.Equal(t, postgresIngestSortedKinds("Account", "Person"), nodes[0].Kinds)
		requirePostgresIngestJSON(t, `{
			"name":"Alice",
			"nested":{"rank":7},
			"objectid":"alpha",
			"stable":"node-value"
		}`, nodes[0].Properties)
	}
	requirePostgresIngestNodeHash(t, nodes[0])
	require.Equal(t, "beta", nodes[1].ObjectID)
	require.Equal(t, []string{"Person"}, nodes[1].Kinds)
	requirePostgresIngestJSON(t, `{"name":"Bob","objectid":"beta"}`, nodes[1].Properties)
	requirePostgresIngestNodeHash(t, nodes[1])

	edges := testDB.readEdges(t, testGraph)
	require.Len(t, edges, 1)
	require.Equal(t, "alpha", edges[0].StartObjectID.String)
	require.Equal(t, "Knows", edges[0].Kind)
	require.Equal(t, "beta", edges[0].EndObjectID.String)
	if changed {
		requirePostgresIngestJSON(t, `{
			"changed":true,
			"note":"full edge state",
			"weight":2
		}`, edges[0].Properties)
	} else {
		requirePostgresIngestJSON(t, `{
			"note":"full edge state",
			"weight":1
		}`, edges[0].Properties)
	}
	requirePostgresIngestEdgeHash(t, edges[0])

	return nodes, edges
}

func testPostgresIngestEdgeIdentities(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	t.Run("pre-existing endpoints preserve exact source tuple", func(t *testing.T) {
		testGraph := testDB.newGraph(t)
		options := IngestOptions{BucketCount: 4, TempDir: t.TempDir()}
		nodes := postgresIngestEndpointNodes("existing-start", "existing-end")

		nodeStats, err := testDB.driver.Ingest(
			testDB.ctx,
			testGraph.Target,
			postgresIngestTestInput(nodes, nil),
			options,
		)
		require.NoError(t, err)
		requirePostgresIngestPhaseStats(t, nodeStats.Nodes, postgresIngestPhaseExpectation{
			InputRecords:       2,
			CoalescedRecords:   2,
			PopulatedBuckets:   postgresIngestBucketCountForNodes(nodes, 4),
			StagedInserts:      2,
			CommittedMutations: 2,
		})

		edge := &IngestEdge{
			StartObjectID: "existing-start",
			EndObjectID:   "existing-end",
			Kind:          graph.StringKind("PredatesEdgeRun"),
			Properties: postgresIngestProperties(map[string]any{
				"state": "created later",
			}),
		}
		edgeStats, err := testDB.driver.Ingest(
			testDB.ctx,
			testGraph.Target,
			postgresIngestTestInput(nil, []*IngestEdge{edge}),
			options,
		)
		require.NoError(t, err)
		requirePostgresIngestPhaseStats(t, edgeStats.Edges, postgresIngestPhaseExpectation{
			InputRecords:       1,
			CoalescedRecords:   1,
			PopulatedBuckets:   1,
			StagedInserts:      1,
			CommittedMutations: 1,
		})
		stored := testDB.readEdges(t, testGraph)
		require.Len(t, stored, 1)
		require.Equal(t, "existing-start", stored[0].StartObjectID.String)
		require.Equal(t, "PredatesEdgeRun", stored[0].Kind)
		require.Equal(t, "existing-end", stored[0].EndObjectID.String)
		requirePostgresIngestJSON(t, `{"state":"created later"}`, stored[0].Properties)
		requirePostgresIngestEdgeHash(t, stored[0])
	})

	t.Run("missing endpoint rolls back its bucket", func(t *testing.T) {
		testGraph := testDB.newGraph(t)
		options := IngestOptions{BucketCount: 4, TempDir: t.TempDir()}
		nodes := postgresIngestEndpointNodes("present-endpoint")
		_, err := testDB.driver.Ingest(
			testDB.ctx,
			testGraph.Target,
			postgresIngestTestInput(nodes, nil),
			options,
		)
		require.NoError(t, err)

		stats, err := testDB.driver.Ingest(
			testDB.ctx,
			testGraph.Target,
			postgresIngestTestInput(nil, []*IngestEdge{{
				StartObjectID: "present-endpoint",
				EndObjectID:   "missing-endpoint",
				Kind:          graph.StringKind("MissingEndpoint"),
				Properties:    postgresIngestProperties(map[string]any{"attempt": 1}),
			}}),
			options,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "endpoint resolution")
		require.Contains(t, err.Error(), "1 missing")
		require.NotContains(t, err.Error(), "present-endpoint")
		require.NotContains(t, err.Error(), "missing-endpoint")
		requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{})
		requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
			InputRecords:     1,
			PopulatedBuckets: 1,
		})
		require.Zero(t, testDB.countPartitionRows(t, testGraph.EdgeTable))
	})

	t.Run("ambiguous exact endpoint identity is rejected", func(t *testing.T) {
		testGraph := testDB.newGraph(t)
		options := IngestOptions{BucketCount: 4, TempDir: t.TempDir()}
		_, err := testDB.driver.Ingest(
			testDB.ctx,
			testGraph.Target,
			postgresIngestTestInput(
				postgresIngestEndpointNodes("ambiguous-start", "unambiguous-end"),
				nil,
			),
			options,
		)
		require.NoError(t, err)

		constraintName := model.ConstraintName(testGraph.NodeTable, graph.Constraint{
			Field: "objectid",
			Type:  graph.BTreeIndex,
		})
		_, err = testDB.pool.Exec(
			testDB.ctx,
			"drop index "+pgx.Identifier{constraintName}.Sanitize()+";",
		)
		require.NoError(t, err)
		duplicateStatement := fmt.Sprintf(`
			insert into %s (graph_id, kind_ids, properties, id_hash, content_hash)
			select graph_id, kind_ids, properties, id_hash, content_hash
			from %s
			where properties->>'objectid' = $1
		`,
			pgx.Identifier{testGraph.NodeTable}.Sanitize(),
			pgx.Identifier{testGraph.NodeTable}.Sanitize(),
		)
		commandTag, err := testDB.pool.Exec(testDB.ctx, duplicateStatement, "ambiguous-start")
		require.NoError(t, err)
		require.EqualValues(t, 1, commandTag.RowsAffected())

		stats, err := testDB.driver.Ingest(
			testDB.ctx,
			testGraph.Target,
			postgresIngestTestInput(nil, []*IngestEdge{{
				StartObjectID: "ambiguous-start",
				EndObjectID:   "unambiguous-end",
				Kind:          graph.StringKind("AmbiguousEndpoint"),
				Properties:    postgresIngestProperties(map[string]any{"attempt": true}),
			}}),
			options,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "1 ambiguous")
		require.NotContains(t, err.Error(), "ambiguous-start")
		require.NotContains(t, err.Error(), "unambiguous-end")
		requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
			InputRecords:     1,
			PopulatedBuckets: 1,
		})
		require.Zero(t, testDB.countPartitionRows(t, testGraph.EdgeTable))
	})

	for _, test := range []struct {
		name          string
		legacyState   string
		errorContains string
	}{
		{
			name:          "null legacy sources fail staged source preflight",
			legacyState:   "null",
			errorContains: "source identity preflight found 1 source identity conflicts",
		},
		{
			name:          "inconsistent legacy sources fail staged source preflight",
			legacyState:   "inconsistent",
			errorContains: "source identity preflight found 1 source identity conflicts",
		},
		{
			name:          "hashed row with null sources fails stored-row preflight",
			legacyState:   "hashed-null",
			errorContains: "null or empty source identity with a hash",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testGraph := testDB.newGraph(t)
			options := IngestOptions{BucketCount: 1, TempDir: t.TempDir()}
			testDB.createLegacyEdgeFixture(t, testGraph, options, test.legacyState)

			stats, err := testDB.driver.Ingest(
				testDB.ctx,
				testGraph.Target,
				postgresIngestTestInput(nil, []*IngestEdge{{
					StartObjectID: "legacy-start",
					EndObjectID:   "legacy-end",
					Kind:          graph.StringKind("LegacyEdge"),
					Properties:    postgresIngestProperties(map[string]any{"new": "rejected"}),
				}}),
				options,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.errorContains)
			require.NotContains(t, err.Error(), "legacy-start")
			require.NotContains(t, err.Error(), "legacy-end")
			requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
				InputRecords:     1,
				PopulatedBuckets: 1,
			})

			stored := testDB.readEdges(t, testGraph)
			require.Len(t, stored, 1)
			requirePostgresIngestJSON(t, `{"legacy":"preserved"}`, stored[0].Properties)
			switch test.legacyState {
			case "null":
				require.False(t, stored[0].IDHash.Valid)
				require.False(t, stored[0].StartObjectID.Valid)
				require.False(t, stored[0].EndObjectID.Valid)
			case "inconsistent":
				require.True(t, stored[0].IDHash.Valid)
				require.Equal(t, "wrong-start", stored[0].StartObjectID.String)
				require.Equal(t, "legacy-end", stored[0].EndObjectID.String)
			case "hashed-null":
				require.True(t, stored[0].IDHash.Valid)
				require.Len(t, stored[0].ContentHash, ingestContentHashLength)
				require.False(t, stored[0].StartObjectID.Valid)
				require.False(t, stored[0].EndObjectID.Valid)
			default:
				t.Fatalf("unknown legacy state %q", test.legacyState)
			}
		})
	}
}

func postgresIngestEndpointNodes(objectIDs ...string) []*IngestNode {
	nodes := make([]*IngestNode, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		nodes = append(nodes, &IngestNode{
			ObjectID: objectID,
			Kinds:    graph.Kinds{graph.StringKind("Endpoint")},
			Properties: postgresIngestProperties(map[string]any{
				"name": objectID,
			}),
		})
	}

	return nodes
}

func (s *postgresIngestTestDatabase) countPartitionRows(t *testing.T, tableName string) int64 {
	t.Helper()

	var count int64
	statement := "select count(*) from " + pgx.Identifier{tableName}.Sanitize()
	require.NoError(t, s.pool.QueryRow(s.ctx, statement).Scan(&count))

	return count
}

func (s *postgresIngestTestDatabase) createLegacyEdgeFixture(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
	options IngestOptions,
	legacyState string,
) {
	t.Helper()

	_, err := s.driver.Ingest(
		s.ctx,
		testGraph.Target,
		postgresIngestTestInput(postgresIngestEndpointNodes("legacy-start", "legacy-end"), nil),
		options,
	)
	require.NoError(t, err)

	nodes := s.readNodes(t, testGraph)
	require.Len(t, nodes, 2)
	idsByObjectID := map[string]graph.ID{}
	for _, node := range nodes {
		idsByObjectID[node.ObjectID] = graph.ID(node.ID)
	}
	err = s.driver.WriteTransaction(s.ctx, func(tx graph.Transaction) error {
		_, err := tx.WithGraph(testGraph.Target).CreateRelationshipByIDs(
			idsByObjectID["legacy-start"],
			idsByObjectID["legacy-end"],
			graph.StringKind("LegacyEdge"),
			postgresIngestProperties(map[string]any{"legacy": "preserved"}),
		)
		return err
	})
	require.NoError(t, err)

	edgeHash := int32(hashIngestEdgeIdentity("legacy-start", "LegacyEdge", "legacy-end"))
	updatePrefix := "update " + pgx.Identifier{testGraph.EdgeTable}.Sanitize() + " set "
	switch legacyState {
	case "null":
		return
	case "inconsistent":
		_, err = s.pool.Exec(s.ctx,
			updatePrefix+"id_hash = $1, start_object_id = $2, end_object_id = $3",
			edgeHash,
			"wrong-start",
			"legacy-end",
		)
	case "hashed-null":
		_, err = s.pool.Exec(s.ctx,
			updatePrefix+"id_hash = $1, content_hash = dawgs_ingest_edge_content_hash(properties)",
			edgeHash,
		)
	default:
		t.Fatalf("unknown legacy state %q", legacyState)
	}
	require.NoError(t, err)
}

func testPostgresIngestForcedCollisions(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	testGraph := testDB.newGraph(t)
	const bucketCount = uint64(65536)
	options := IngestOptions{BucketCount: int(bucketCount), TempDir: t.TempDir()}
	existingObjectIDs := []string{
		"collision-node",
		"collision-edge-start",
		"collision-edge-end",
		"target-edge-start",
		"target-edge-end",
	}
	targetNodeObjectID := postgresIngestFindUnusedNodeBucket(
		t,
		"target-node",
		existingObjectIDs,
		bucketCount,
	)

	_, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(postgresIngestEndpointNodes(existingObjectIDs...), nil),
		options,
	)
	require.NoError(t, err)

	forcedNodeHash := int32(hashIngestNodeIdentity(targetNodeObjectID))
	nodeUpdate := "update " + pgx.Identifier{testGraph.NodeTable}.Sanitize() +
		" set id_hash = $1 where properties->>'objectid' = $2"
	commandTag, err := testDB.pool.Exec(
		testDB.ctx,
		nodeUpdate,
		forcedNodeHash,
		"collision-node",
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, commandTag.RowsAffected())

	nodeStats, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput([]*IngestNode{{
			ObjectID: targetNodeObjectID,
			Kinds:    graph.Kinds{graph.StringKind("CollisionTarget")},
			Properties: postgresIngestProperties(map[string]any{
				"marker": "exact node target",
			}),
		}}, nil),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, nodeStats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   1,
		StagedInserts:      1,
		CommittedMutations: 1,
	})

	storedNodes := testDB.readNodes(t, testGraph)
	nodesByObjectID := make(map[string]postgresIngestStoredNode, len(storedNodes))
	for _, node := range storedNodes {
		nodesByObjectID[node.ObjectID] = node
	}
	require.Len(t, nodesByObjectID, len(existingObjectIDs)+1)
	collisionNode := nodesByObjectID["collision-node"]
	targetNode := nodesByObjectID[targetNodeObjectID]
	require.True(t, collisionNode.IDHash.Valid)
	require.True(t, targetNode.IDHash.Valid)
	require.Equal(t, forcedNodeHash, collisionNode.IDHash.Int32)
	require.Equal(t, forcedNodeHash, targetNode.IDHash.Int32)
	requirePostgresIngestJSON(t, `{
		"name":"collision-node",
		"objectid":"collision-node"
	}`, collisionNode.Properties)
	require.Equal(t, []string{"Endpoint"}, collisionNode.Kinds)
	requirePostgresIngestNodeContentHash(t, collisionNode)
	requirePostgresIngestJSON(t, fmt.Sprintf(`{
		"marker":"exact node target",
		"objectid":%q
	}`, targetNodeObjectID), targetNode.Properties)
	require.Equal(t, []string{"CollisionTarget"}, targetNode.Kinds)
	requirePostgresIngestNodeHash(t, targetNode)

	collisionEdge := &IngestEdge{
		StartObjectID: "collision-edge-start",
		EndObjectID:   "collision-edge-end",
		Kind:          graph.StringKind("CollisionEdge"),
		Properties: postgresIngestProperties(map[string]any{
			"marker": "unrelated edge",
		}),
	}
	_, err = testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nil, []*IngestEdge{collisionEdge}),
		options,
	)
	require.NoError(t, err)

	forcedEdgeHash := int32(hashIngestEdgeIdentity(
		"target-edge-start",
		"CollisionEdge",
		"target-edge-end",
	))
	edgeUpdate := "update " + pgx.Identifier{testGraph.EdgeTable}.Sanitize() +
		" set id_hash = $1 where start_object_id = $2 and end_object_id = $3"
	commandTag, err = testDB.pool.Exec(
		testDB.ctx,
		edgeUpdate,
		forcedEdgeHash,
		"collision-edge-start",
		"collision-edge-end",
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, commandTag.RowsAffected())

	targetEdge := &IngestEdge{
		StartObjectID: "target-edge-start",
		EndObjectID:   "target-edge-end",
		Kind:          graph.StringKind("CollisionEdge"),
		Properties: postgresIngestProperties(map[string]any{
			"marker": "exact edge target",
		}),
	}
	edgeStats, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nil, []*IngestEdge{targetEdge}),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, edgeStats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       1,
		CoalescedRecords:   1,
		PopulatedBuckets:   1,
		IdentityRowsRead:   1,
		StagedInserts:      1,
		CommittedMutations: 1,
	})

	storedEdges := testDB.readEdges(t, testGraph)
	require.Len(t, storedEdges, 2)
	edgesByStart := make(map[string]postgresIngestStoredEdge, len(storedEdges))
	for _, edge := range storedEdges {
		edgesByStart[edge.StartObjectID.String] = edge
	}
	storedCollisionEdge := edgesByStart["collision-edge-start"]
	storedTargetEdge := edgesByStart["target-edge-start"]
	require.Equal(t, "collision-edge-start", storedCollisionEdge.ActualStartObjectID)
	require.Equal(t, "collision-edge-end", storedCollisionEdge.EndObjectID.String)
	require.Equal(t, "collision-edge-end", storedCollisionEdge.ActualEndObjectID)
	require.Equal(t, "CollisionEdge", storedCollisionEdge.Kind)
	require.True(t, storedCollisionEdge.IDHash.Valid)
	require.Equal(t, forcedEdgeHash, storedCollisionEdge.IDHash.Int32)
	requirePostgresIngestJSON(t, `{"marker":"unrelated edge"}`, storedCollisionEdge.Properties)
	requirePostgresIngestEdgeContentHash(t, storedCollisionEdge)
	require.Equal(t, "target-edge-end", storedTargetEdge.EndObjectID.String)
	require.Equal(t, "CollisionEdge", storedTargetEdge.Kind)
	require.True(t, storedTargetEdge.IDHash.Valid)
	require.Equal(t, forcedEdgeHash, storedTargetEdge.IDHash.Int32)
	requirePostgresIngestJSON(t, `{"marker":"exact edge target"}`, storedTargetEdge.Properties)
	requirePostgresIngestEdgeHash(t, storedTargetEdge)
}

func postgresIngestFindUnusedNodeBucket(
	t *testing.T,
	prefix string,
	existingObjectIDs []string,
	bucketCount uint64,
) string {
	t.Helper()

	buckets, err := newIngestBucketSet(bucketCount)
	require.NoError(t, err)
	used := make(map[uint64]struct{}, len(existingObjectIDs))
	for _, objectID := range existingObjectIDs {
		used[buckets.Bucket(hashIngestNodeIdentity(objectID))] = struct{}{}
	}
	for candidateIndex := range 1_000_000 {
		candidate := fmt.Sprintf("%s-%d", prefix, candidateIndex)
		if _, found := used[buckets.Bucket(hashIngestNodeIdentity(candidate))]; !found {
			return candidate
		}
	}
	t.Fatalf("could not find unused node bucket for %q", prefix)

	return ""
}

func requirePostgresIngestNodeContentHash(t *testing.T, node postgresIngestStoredNode) {
	t.Helper()

	require.Len(t, node.ContentHash, ingestContentHashLength)
	expected, err := hashIngestNodeContent(graph.StringsToKinds(node.Kinds), node.Properties)
	require.NoError(t, err)
	require.Equal(t, expected[:], node.ContentHash)
}

func requirePostgresIngestEdgeContentHash(t *testing.T, edge postgresIngestStoredEdge) {
	t.Helper()

	require.Len(t, edge.ContentHash, ingestContentHashLength)
	expected, err := hashIngestEdgeContent(edge.Properties)
	require.NoError(t, err)
	require.Equal(t, expected[:], edge.ContentHash)
}

func testPostgresIngestPartialCommitRetry(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	testGraph := testDB.newGraph(t)
	const bucketCount = uint64(4)
	options := IngestOptions{BucketCount: int(bucketCount), TempDir: t.TempDir()}
	startObjectID := "retry-start"
	validEndObjectID := postgresIngestFindEdgeEndForBucket(
		t,
		startObjectID,
		"RetryEdge",
		"retry-valid-end",
		bucketCount,
		0,
		nil,
	)
	missingEndObjectID := postgresIngestFindEdgeEndForBucket(
		t,
		startObjectID,
		"RetryEdge",
		"retry-missing-end",
		bucketCount,
		3,
		map[string]struct{}{validEndObjectID: {}},
	)

	_, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(
			postgresIngestEndpointNodes(startObjectID, validEndObjectID),
			nil,
		),
		options,
	)
	require.NoError(t, err)

	validEdge := &IngestEdge{
		StartObjectID: startObjectID,
		EndObjectID:   validEndObjectID,
		Kind:          graph.StringKind("RetryEdge"),
		Properties:    postgresIngestProperties(map[string]any{"sequence": "earlier"}),
	}
	failingEdge := &IngestEdge{
		StartObjectID: startObjectID,
		EndObjectID:   missingEndObjectID,
		Kind:          graph.StringKind("RetryEdge"),
		Properties:    postgresIngestProperties(map[string]any{"sequence": "later"}),
	}
	stats, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nil, []*IngestEdge{failingEdge, validEdge}),
		options,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "1 missing")
	require.NotContains(t, err.Error(), startObjectID)
	require.NotContains(t, err.Error(), missingEndObjectID)
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       2,
		CoalescedRecords:   1,
		PopulatedBuckets:   2,
		StagedInserts:      1,
		CommittedMutations: 1,
	})

	storedEdges := testDB.readEdges(t, testGraph)
	require.Len(t, storedEdges, 1)
	require.Equal(t, validEndObjectID, storedEdges[0].EndObjectID.String)
	requirePostgresIngestJSON(t, `{"sequence":"earlier"}`, storedEdges[0].Properties)
	requirePostgresIngestEdgeHash(t, storedEdges[0])
	earlierEdgeDatabaseID := storedEdges[0].ID

	repairStats, err := testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(postgresIngestEndpointNodes(missingEndObjectID), nil),
		options,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, repairStats.Nodes.InputRecords)
	require.EqualValues(t, 1, repairStats.Nodes.CoalescedRecords)
	require.EqualValues(t, 1, repairStats.Nodes.StagedInserts)
	require.EqualValues(t, 1, repairStats.Nodes.CommittedMutations)

	stats, err = testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nil, []*IngestEdge{failingEdge, validEdge}),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       2,
		CoalescedRecords:   2,
		PopulatedBuckets:   2,
		IdentityRowsRead:   1,
		HashMatches:        1,
		StagedInserts:      1,
		CommittedMutations: 1,
	})

	storedEdges = testDB.readEdges(t, testGraph)
	require.Len(t, storedEdges, 2)
	idsByEnd := map[string]int64{}
	for _, edge := range storedEdges {
		idsByEnd[edge.EndObjectID.String] = edge.ID
		requirePostgresIngestEdgeHash(t, edge)
	}
	require.Equal(t, earlierEdgeDatabaseID, idsByEnd[validEndObjectID])
	require.NotZero(t, idsByEnd[missingEndObjectID])

	stats, err = testDB.driver.Ingest(
		testDB.ctx,
		testGraph.Target,
		postgresIngestTestInput(nil, []*IngestEdge{validEdge, failingEdge}),
		options,
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
		InputRecords:     2,
		CoalescedRecords: 2,
		PopulatedBuckets: 2,
		IdentityRowsRead: 2,
		HashMatches:      2,
	})
	finalNodes := testDB.readNodes(t, testGraph)
	require.Len(t, finalNodes, 3)
	expectedNodeIDs := map[string]struct{}{
		startObjectID:      {},
		validEndObjectID:   {},
		missingEndObjectID: {},
	}
	for _, node := range finalNodes {
		_, expected := expectedNodeIDs[node.ObjectID]
		require.True(t, expected, node.ObjectID)
		require.Equal(t, []string{"Endpoint"}, node.Kinds)
		requirePostgresIngestJSON(t, fmt.Sprintf(
			`{"name":%q,"objectid":%q}`,
			node.ObjectID,
			node.ObjectID,
		), node.Properties)
		requirePostgresIngestNodeHash(t, node)
	}
	finalEdges := testDB.readEdges(t, testGraph)
	require.Len(t, finalEdges, 2)
	for _, edge := range finalEdges {
		requirePostgresIngestEdgeHash(t, edge)
		switch edge.EndObjectID.String {
		case validEndObjectID:
			require.Equal(t, earlierEdgeDatabaseID, edge.ID)
			requirePostgresIngestJSON(t, `{"sequence":"earlier"}`, edge.Properties)
		case missingEndObjectID:
			requirePostgresIngestJSON(t, `{"sequence":"later"}`, edge.Properties)
		default:
			t.Fatalf("unexpected retry edge endpoint tuple ending at %q", edge.EndObjectID.String)
		}
	}
}

func postgresIngestFindEdgeEndForBucket(
	t *testing.T,
	startObjectID string,
	kind string,
	prefix string,
	bucketCount uint64,
	wantedBucket uint64,
	excluded map[string]struct{},
) string {
	t.Helper()

	buckets, err := newIngestBucketSet(bucketCount)
	require.NoError(t, err)
	require.Less(t, wantedBucket, bucketCount)
	for candidateIndex := range 1_000_000 {
		candidate := fmt.Sprintf("%s-%d", prefix, candidateIndex)
		if _, found := excluded[candidate]; found {
			continue
		}
		if buckets.Bucket(hashIngestEdgeIdentity(startObjectID, kind, candidate)) == wantedBucket {
			return candidate
		}
	}
	t.Fatalf("could not find edge identity for bucket %d", wantedBucket)

	return ""
}

func testPostgresIngestBucketEquivalence(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	var reference *postgresIngestLogicalGraph
	for _, bucketCount := range []int{1, 4, 256, 65536} {
		t.Run(fmt.Sprintf("%d buckets", bucketCount), func(t *testing.T) {
			testGraph := testDB.newGraph(t)
			nodes, edges := postgresIngestEquivalentRecords()
			stats, err := testDB.driver.Ingest(
				testDB.ctx,
				testGraph.Target,
				postgresIngestTestInput(nodes, edges),
				IngestOptions{BucketCount: bucketCount, TempDir: t.TempDir()},
			)
			require.NoError(t, err)
			requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{
				InputRecords:       5,
				CoalescedRecords:   4,
				PopulatedBuckets:   postgresIngestBucketCountForNodes(nodes, uint64(bucketCount)),
				StagedInserts:      4,
				CommittedMutations: 4,
			})
			requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{
				InputRecords:       4,
				CoalescedRecords:   3,
				PopulatedBuckets:   postgresIngestBucketCountForEdges(edges, uint64(bucketCount)),
				StagedInserts:      3,
				CommittedMutations: 3,
			})

			logical := testDB.logicalGraph(t, testGraph)
			requirePostgresIngestEquivalentState(t, logical)
			if reference == nil {
				reference = &logical
			} else {
				require.Equal(t, *reference, logical)
			}
		})
	}
}

func postgresIngestEquivalentRecords() ([]*IngestNode, []*IngestEdge) {
	return []*IngestNode{
			{
				ObjectID:   "equivalent-a",
				Kinds:      graph.Kinds{graph.StringKind("EquivalentPerson")},
				Properties: postgresIngestProperties(map[string]any{"left": 1}),
			},
			{
				ObjectID:   "equivalent-b",
				Kinds:      graph.Kinds{graph.StringKind("EquivalentPerson")},
				Properties: postgresIngestProperties(map[string]any{"name": "B"}),
			},
			{
				ObjectID:   "equivalent-a",
				Kinds:      graph.Kinds{graph.StringKind("EquivalentAdmin")},
				Properties: postgresIngestProperties(map[string]any{"right": 2}),
			},
			{
				ObjectID: "equivalent-c",
				Kinds:    graph.Kinds{graph.StringKind("EquivalentDevice")},
				Properties: postgresIngestProperties(map[string]any{
					"nested": map[string]any{"enabled": true},
				}),
			},
			{
				ObjectID:   "equivalent-d",
				Kinds:      graph.Kinds{graph.StringKind("EquivalentDevice")},
				Properties: postgresIngestProperties(map[string]any{"name": "D"}),
			},
		}, []*IngestEdge{
			{
				StartObjectID: "equivalent-a",
				EndObjectID:   "equivalent-b",
				Kind:          graph.StringKind("EquivalentLink"),
				Properties:    postgresIngestProperties(map[string]any{"left": 1}),
			},
			{
				StartObjectID: "equivalent-c",
				EndObjectID:   "equivalent-d",
				Kind:          graph.StringKind("EquivalentLink"),
				Properties:    postgresIngestProperties(map[string]any{"state": "second"}),
			},
			{
				StartObjectID: "equivalent-a",
				EndObjectID:   "equivalent-b",
				Kind:          graph.StringKind("EquivalentLink"),
				Properties:    postgresIngestProperties(map[string]any{"right": 2}),
			},
			{
				StartObjectID: "equivalent-b",
				EndObjectID:   "equivalent-c",
				Kind:          graph.StringKind("EquivalentDepends"),
				Properties:    postgresIngestProperties(map[string]any{"state": "middle"}),
			},
		}
}

func requirePostgresIngestEquivalentState(t *testing.T, logical postgresIngestLogicalGraph) {
	t.Helper()

	require.Len(t, logical.Nodes, 4)
	require.Equal(t, "equivalent-a", logical.Nodes[0].ObjectID)
	require.Equal(t,
		postgresIngestSortedKinds("EquivalentAdmin", "EquivalentPerson"),
		logical.Nodes[0].Kinds,
	)
	requirePostgresIngestJSON(t,
		`{"left":1,"objectid":"equivalent-a","right":2}`,
		logical.Nodes[0].Properties,
	)
	require.Equal(t, "equivalent-b", logical.Nodes[1].ObjectID)
	require.Equal(t, []string{"EquivalentPerson"}, logical.Nodes[1].Kinds)
	requirePostgresIngestJSON(t,
		`{"name":"B","objectid":"equivalent-b"}`,
		logical.Nodes[1].Properties,
	)
	require.Equal(t, "equivalent-c", logical.Nodes[2].ObjectID)
	require.Equal(t, []string{"EquivalentDevice"}, logical.Nodes[2].Kinds)
	requirePostgresIngestJSON(t,
		`{"nested":{"enabled":true},"objectid":"equivalent-c"}`,
		logical.Nodes[2].Properties,
	)
	require.Equal(t, "equivalent-d", logical.Nodes[3].ObjectID)
	require.Equal(t, []string{"EquivalentDevice"}, logical.Nodes[3].Kinds)
	requirePostgresIngestJSON(t,
		`{"name":"D","objectid":"equivalent-d"}`,
		logical.Nodes[3].Properties,
	)

	require.Len(t, logical.Edges, 3)
	require.Equal(t, postgresIngestLogicalEdge{
		StartObjectID: "equivalent-a",
		Kind:          "EquivalentLink",
		EndObjectID:   "equivalent-b",
		Properties: map[string]any{
			"left":  json.Number("1"),
			"right": json.Number("2"),
		},
	}, logical.Edges[0])
	require.Equal(t, postgresIngestLogicalEdge{
		StartObjectID: "equivalent-b",
		Kind:          "EquivalentDepends",
		EndObjectID:   "equivalent-c",
		Properties:    map[string]any{"state": "middle"},
	}, logical.Edges[1])
	require.Equal(t, postgresIngestLogicalEdge{
		StartObjectID: "equivalent-c",
		Kind:          "EquivalentLink",
		EndObjectID:   "equivalent-d",
		Properties:    map[string]any{"state": "second"},
	}, logical.Edges[2])
}

func testPostgresIngestTargetOnlyClustering(
	t *testing.T,
	testDB *postgresIngestTestDatabase,
) {
	targetGraph := testDB.newGraph(t)
	unrelatedGraph := testDB.newGraph(t)
	targetNodes, targetEdges := postgresIngestClusteringRecords("cluster-target", 512)
	unrelatedNodes, unrelatedEdges := postgresIngestClusteringRecords("cluster-unrelated", 32)

	targetStats, err := testDB.driver.Ingest(
		testDB.ctx,
		targetGraph.Target,
		postgresIngestTestInput(targetNodes, targetEdges),
		IngestOptions{BucketCount: 256, TempDir: t.TempDir()},
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, targetStats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:       512,
		CoalescedRecords:   512,
		PopulatedBuckets:   postgresIngestBucketCountForNodes(targetNodes, 256),
		StagedInserts:      512,
		CommittedMutations: 512,
	})
	requirePostgresIngestPhaseStats(t, targetStats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       511,
		CoalescedRecords:   511,
		PopulatedBuckets:   postgresIngestBucketCountForEdges(targetEdges, 256),
		StagedInserts:      511,
		CommittedMutations: 511,
	})

	unrelatedStats, err := testDB.driver.Ingest(
		testDB.ctx,
		unrelatedGraph.Target,
		postgresIngestTestInput(unrelatedNodes, unrelatedEdges),
		IngestOptions{BucketCount: 32, TempDir: t.TempDir()},
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, unrelatedStats.Nodes, postgresIngestPhaseExpectation{
		InputRecords:       32,
		CoalescedRecords:   32,
		PopulatedBuckets:   postgresIngestBucketCountForNodes(unrelatedNodes, 32),
		StagedInserts:      32,
		CommittedMutations: 32,
	})
	requirePostgresIngestPhaseStats(t, unrelatedStats.Edges, postgresIngestPhaseExpectation{
		InputRecords:       31,
		CoalescedRecords:   31,
		PopulatedBuckets:   postgresIngestBucketCountForEdges(unrelatedEdges, 32),
		StagedInserts:      31,
		CommittedMutations: 31,
	})

	require.EqualValues(t, 512, testDB.countPartitionRows(t, targetGraph.NodeTable))
	require.EqualValues(t, 511, testDB.countPartitionRows(t, targetGraph.EdgeTable))
	require.EqualValues(t, 32, testDB.countPartitionRows(t, unrelatedGraph.NodeTable))
	require.EqualValues(t, 31, testDB.countPartitionRows(t, unrelatedGraph.EdgeTable))

	for _, tableName := range []string{
		targetGraph.NodeTable,
		targetGraph.EdgeTable,
		unrelatedGraph.NodeTable,
		unrelatedGraph.EdgeTable,
	} {
		_, err := testDB.pool.Exec(
			testDB.ctx,
			"analyze "+pgx.Identifier{tableName}.Sanitize()+";",
		)
		require.NoError(t, err)
	}
	beforeCorrelation := testDB.idHashCorrelations(t, targetGraph)
	require.Len(t, beforeCorrelation, 2)
	for tableName, correlation := range beforeCorrelation {
		require.True(t, correlation.Valid, tableName)
	}
	t.Logf("target id_hash correlation before clustering: node=%f edge=%f",
		beforeCorrelation[targetGraph.NodeTable].Float64,
		beforeCorrelation[targetGraph.EdgeTable].Float64,
	)

	relations := []string{
		"node",
		"edge",
		targetGraph.NodeTable,
		targetGraph.EdgeTable,
		unrelatedGraph.NodeTable,
		unrelatedGraph.EdgeTable,
	}
	beforeIndexes := testDB.idHashClusterIndexes(t, relations)
	requirePostgresIngestIndexRelationSet(t, beforeIndexes, relations)
	for relation, index := range beforeIndexes {
		require.False(t, index.clustered, relation+"."+index.name)
	}

	stats, err := testDB.driver.Ingest(
		testDB.ctx,
		targetGraph.Target,
		IngestInput{},
		IngestOptions{
			BucketCount:        4,
			TempDir:            t.TempDir(),
			ClusterAfterIngest: true,
		},
	)
	require.NoError(t, err)
	requirePostgresIngestPhaseStats(t, stats.Nodes, postgresIngestPhaseExpectation{})
	requirePostgresIngestPhaseStats(t, stats.Edges, postgresIngestPhaseExpectation{})
	require.GreaterOrEqual(t, stats.ClusterDuration, time.Duration(0))

	afterIndexes := testDB.idHashClusterIndexes(t, relations)
	requirePostgresIngestIndexRelationSet(t, afterIndexes, relations)
	var clusteredRelations []string
	for _, relation := range relations {
		beforeIndex := beforeIndexes[relation]
		afterIndex := afterIndexes[relation]
		require.Equal(t, beforeIndex.name, afterIndex.name, relation)
		if afterIndex.clustered {
			clusteredRelations = append(clusteredRelations, relation)
		}
	}
	require.ElementsMatch(t, []string{
		targetGraph.NodeTable,
		targetGraph.EdgeTable,
	}, clusteredRelations)

	afterCorrelation := testDB.idHashCorrelations(t, targetGraph)
	require.Len(t, afterCorrelation, 2)
	for tableName, correlation := range afterCorrelation {
		require.True(t, correlation.Valid, tableName)
	}
	t.Logf("target id_hash correlation after clustering: node=%f edge=%f",
		afterCorrelation[targetGraph.NodeTable].Float64,
		afterCorrelation[targetGraph.EdgeTable].Float64,
	)
}

func postgresIngestClusteringRecords(prefix string, nodeCount int) ([]*IngestNode, []*IngestEdge) {
	nodes := make([]*IngestNode, 0, nodeCount)
	edges := make([]*IngestEdge, 0, nodeCount-1)
	for index := range nodeCount {
		objectID := fmt.Sprintf("%s-%06d", prefix, index)
		nodes = append(nodes, &IngestNode{
			ObjectID: objectID,
			Kinds:    graph.Kinds{graph.StringKind("ClusterNode")},
			Properties: postgresIngestProperties(map[string]any{
				"ordinal": index,
			}),
		})
		if index > 0 {
			edges = append(edges, &IngestEdge{
				StartObjectID: fmt.Sprintf("%s-%06d", prefix, index-1),
				EndObjectID:   objectID,
				Kind:          graph.StringKind("ClusterEdge"),
				Properties: postgresIngestProperties(map[string]any{
					"ordinal": index,
				}),
			})
		}
	}

	return nodes, edges
}

type postgresIngestClusterIndex struct {
	name          string
	method        string
	clustered     bool
	valid         bool
	ready         bool
	live          bool
	nonPartial    bool
	nonExpression bool
	keyCount      int16
	columnCount   int16
}

func requirePostgresIngestIndexRelationSet(
	t *testing.T,
	actual map[string]postgresIngestClusterIndex,
	expectedRelations []string,
) {
	t.Helper()

	require.Len(t, actual, len(expectedRelations))
	for _, relation := range expectedRelations {
		index, found := actual[relation]
		require.True(t, found, "missing id_hash index state for relation %q", relation)
		require.NotEmpty(t, index.name, relation)
		require.Equal(t, "btree", index.method, relation+"."+index.name)
		require.True(t, index.valid, relation+"."+index.name)
		require.True(t, index.ready, relation+"."+index.name)
		require.True(t, index.live, relation+"."+index.name)
		require.True(t, index.nonPartial, relation+"."+index.name)
		require.True(t, index.nonExpression, relation+"."+index.name)
		require.EqualValues(t, 1, index.keyCount, relation+"."+index.name)
		require.EqualValues(t, 1, index.columnCount, relation+"."+index.name)
	}
}

func (s *postgresIngestTestDatabase) idHashClusterIndexes(
	t *testing.T,
	relations []string,
) map[string]postgresIngestClusterIndex {
	t.Helper()

	rows, err := s.pool.Query(s.ctx, `
		select table_class.relname,
		       index_class.relname,
		       access_method.amname,
		       index_definition.indisclustered,
		       index_definition.indisvalid,
		       index_definition.indisready,
		       index_definition.indislive,
		       index_definition.indpred is null,
		       index_definition.indexprs is null,
		       index_definition.indnkeyatts,
		       index_definition.indnatts
		from pg_index as index_definition
		join pg_class as table_class on table_class.oid = index_definition.indrelid
		join pg_namespace as table_namespace on table_namespace.oid = table_class.relnamespace
		join pg_class as index_class on index_class.oid = index_definition.indexrelid
		join pg_am as access_method on access_method.oid = index_class.relam
		join pg_attribute as attribute
		  on attribute.attrelid = index_definition.indrelid
		 and attribute.attnum = index_definition.indkey[0]
		where table_namespace.nspname = 'public'
		  and table_class.relname = any($1::text[])
		  and attribute.attname = 'id_hash'
		order by table_class.relname, index_class.relname
	`, relations)
	require.NoError(t, err)
	defer rows.Close()

	result := make(map[string]postgresIngestClusterIndex, len(relations))
	for rows.Next() {
		var relation string
		var index postgresIngestClusterIndex
		require.NoError(t, rows.Scan(
			&relation,
			&index.name,
			&index.method,
			&index.clustered,
			&index.valid,
			&index.ready,
			&index.live,
			&index.nonPartial,
			&index.nonExpression,
			&index.keyCount,
			&index.columnCount,
		))
		_, duplicate := result[relation]
		require.False(t, duplicate, relation)
		result[relation] = index
	}
	require.NoError(t, rows.Err())

	return result
}

func (s *postgresIngestTestDatabase) idHashCorrelations(
	t *testing.T,
	testGraph *postgresIngestTestGraph,
) map[string]pgtype.Float8 {
	t.Helper()

	rows, err := s.pool.Query(s.ctx, `
		select tablename, correlation::double precision
		from pg_stats
		where schemaname = 'public'
		  and tablename = any($1::text[])
		  and attname = 'id_hash'
		order by tablename
	`, []string{testGraph.NodeTable, testGraph.EdgeTable})
	require.NoError(t, err)
	defer rows.Close()

	result := map[string]pgtype.Float8{}
	for rows.Next() {
		var tableName string
		var correlation pgtype.Float8
		require.NoError(t, rows.Scan(&tableName, &correlation))
		result[tableName] = correlation
	}
	require.NoError(t, rows.Err())

	return result
}

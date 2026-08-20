package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type ingestNodeSequenceItem struct {
	node *IngestNode
	err  error
}

func ingestNodeSequence(items ...ingestNodeSequenceItem) iter.Seq2[*IngestNode, error] {
	return func(yield func(*IngestNode, error) bool) {
		for _, item := range items {
			if !yield(item.node, item.err) {
				return
			}
		}
	}
}

func newTestNodeEngine(t *testing.T, bucketCount uint64) (*ingestEngine, *ingestSpool) {
	t.Helper()

	buckets, err := newIngestBucketSet(bucketCount)
	require.NoError(t, err)
	spool, err := newIngestSpool(t.TempDir(), ingestPhaseNodes, bucketCount)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })

	return &ingestEngine{
		buckets:   buckets,
		nodeSpool: spool,
	}, spool
}

func readSpooledNodes(t *testing.T, spool *ingestSpool, bucket uint64) []spooledIngestNode {
	t.Helper()

	var records []spooledIngestNode
	require.NoError(t, spool.Read(bucket, func(payload []byte) error {
		var record spooledIngestNode
		if err := json.Unmarshal(payload, &record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	}))

	return records
}

func TestSpoolIngestNodesNormalizesWithoutMutatingCallerAndSelectsDeterministicBucket(t *testing.T) {
	engine, spool := newTestNodeEngine(t, 8)
	properties := &graph.Properties{
		Map: map[string]any{
			"name":   "before",
			"nested": map[string]any{"enabled": true},
		},
		Modified: map[string]struct{}{"name": {}},
		Deleted:  map[string]struct{}{"nested": {}},
	}
	node := &IngestNode{
		ObjectID:   "node-a",
		Kinds:      graph.Kinds{graph.StringKind("User"), graph.StringKind("Admin")},
		Properties: properties,
	}

	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: node})))

	_, callerHasObjectID := properties.Map["objectid"]
	require.False(t, callerHasObjectID)
	require.Equal(t, map[string]any{"enabled": true}, properties.Map["nested"])
	require.Equal(t, int64(1), engine.stats.Nodes.InputRecords)
	require.Equal(t, int64(1), engine.stats.Nodes.PopulatedBuckets)
	require.Equal(t, spool.BytesWritten(), engine.stats.Nodes.SpoolBytes)

	expectedHash := hashIngestNodeIdentity("node-a")
	expectedBucket := engine.buckets.Bucket(expectedHash)
	require.Equal(t, []uint64{expectedBucket}, spool.PopulatedBuckets())
	records := readSpooledNodes(t, spool, expectedBucket)
	require.Equal(t, []spooledIngestNode{{
		ObjectID:   "node-a",
		IDHash:     int32(expectedHash),
		Kinds:      []string{"User", "Admin"},
		Properties: map[string]any{"name": "before", "nested": map[string]any{"enabled": true}, "objectid": "node-a"},
	}}, records)
}

func TestIngestSpoolPopulatedBucketCountTracksUniqueBuckets(t *testing.T) {
	_, spool := newTestNodeEngine(t, 4)
	require.Zero(t, spool.PopulatedBucketCount())
	require.NoError(t, spool.Append(1, map[string]any{"record": 1}))
	require.Equal(t, 1, spool.PopulatedBucketCount())
	require.NoError(t, spool.Append(1, map[string]any{"record": 2}))
	require.Equal(t, 1, spool.PopulatedBucketCount())
	require.NoError(t, spool.Append(3, map[string]any{"record": 3}))
	require.Equal(t, 2, spool.PopulatedBucketCount())
}

func TestSpoolIngestNodesValidatesRecordsWithoutExposingObjectIDs(t *testing.T) {
	t.Parallel()

	secret := "sensitive-object-id"
	iteratorErr := fmt.Errorf("upstream failure for %s", secret)
	tests := map[string]struct {
		item      ingestNodeSequenceItem
		wantCause error
	}{
		"nil record": {
			item: ingestNodeSequenceItem{},
		},
		"empty object ID": {
			item: ingestNodeSequenceItem{node: &IngestNode{Kinds: graph.Kinds{graph.StringKind("User")}}},
		},
		"invalid object ID UTF-8": {
			item: ingestNodeSequenceItem{node: &IngestNode{ObjectID: string([]byte{0xff}), Kinds: graph.Kinds{graph.StringKind("User")}}},
		},
		"empty kinds": {
			item: ingestNodeSequenceItem{node: &IngestNode{ObjectID: secret}},
		},
		"nil kind": {
			item: ingestNodeSequenceItem{node: &IngestNode{ObjectID: secret, Kinds: graph.Kinds{nil}}},
		},
		"empty kind": {
			item: ingestNodeSequenceItem{node: &IngestNode{ObjectID: secret, Kinds: graph.Kinds{graph.StringKind("")}}},
		},
		"invalid kind UTF-8": {
			item: ingestNodeSequenceItem{node: &IngestNode{ObjectID: secret, Kinds: graph.Kinds{graph.StringKind(string([]byte{0xff}))}}},
		},
		"invalid properties": {
			item: ingestNodeSequenceItem{node: &IngestNode{
				ObjectID: secret,
				Kinds:    graph.Kinds{graph.StringKind("User")},
				Properties: &graph.Properties{Map: map[string]any{
					"score": math.NaN(),
				}},
			}},
		},
		"invalid property UTF-8": {
			item: ingestNodeSequenceItem{node: &IngestNode{
				ObjectID:   secret,
				Kinds:      graph.Kinds{graph.StringKind("User")},
				Properties: &graph.Properties{Map: map[string]any{"name": string([]byte{0xff})}},
			}},
		},
		"non-string property object ID": {
			item: ingestNodeSequenceItem{node: &IngestNode{
				ObjectID:   secret,
				Kinds:      graph.Kinds{graph.StringKind("User")},
				Properties: &graph.Properties{Map: map[string]any{"objectid": 42}},
			}},
		},
		"mismatched property object ID": {
			item: ingestNodeSequenceItem{node: &IngestNode{
				ObjectID:   secret,
				Kinds:      graph.Kinds{graph.StringKind("User")},
				Properties: &graph.Properties{Map: map[string]any{"objectid": "other"}},
			}},
		},
		"iterator error": {
			item:      ingestNodeSequenceItem{err: iteratorErr},
			wantCause: iteratorErr,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, _ := newTestNodeEngine(t, 1)
			err := engine.spoolNodes(context.Background(), ingestNodeSequence(test.item))
			require.Error(t, err)
			require.Contains(t, err.Error(), "node ingest")
			require.Contains(t, err.Error(), "record 1")
			require.NotContains(t, err.Error(), secret)
			if test.wantCause != nil {
				require.ErrorIs(t, err, test.wantCause)
			}
			require.Zero(t, engine.stats.Nodes.InputRecords)
		})
	}
}

func TestSpoolIngestNodesConsumesIteratorOnceAndHonorsCancellation(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	iterations := 0
	sequence := iter.Seq2[*IngestNode, error](func(yield func(*IngestNode, error) bool) {
		iterations++
		yield(&IngestNode{ObjectID: "node-a", Kinds: graph.Kinds{graph.StringKind("User")}}, nil)
	})

	require.NoError(t, engine.spoolNodes(context.Background(), sequence))
	require.Equal(t, 1, iterations)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	engine, _ = newTestNodeEngine(t, 1)
	err := engine.spoolNodes(canceled, sequence)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, iterations)

	during, cancelDuring := context.WithCancel(context.Background())
	duringIterations := 0
	duringSequence := iter.Seq2[*IngestNode, error](func(yield func(*IngestNode, error) bool) {
		duringIterations++
		if !yield(&IngestNode{ObjectID: "node-a", Kinds: graph.Kinds{graph.StringKind("User")}}, nil) {
			return
		}
		cancelDuring()
		duringIterations++
		yield(&IngestNode{ObjectID: "node-b", Kinds: graph.Kinds{graph.StringKind("User")}}, nil)
	})
	engine, _ = newTestNodeEngine(t, 1)
	err = engine.spoolNodes(during, duringSequence)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, duringIterations)
	require.Equal(t, int64(1), engine.stats.Nodes.InputRecords)

	afterFinal, cancelAfterFinal := context.WithCancel(context.Background())
	afterFinalSequence := iter.Seq2[*IngestNode, error](func(yield func(*IngestNode, error) bool) {
		yield(&IngestNode{ObjectID: "node-a", Kinds: graph.Kinds{graph.StringKind("User")}}, nil)
		cancelAfterFinal()
	})
	engine, _ = newTestNodeEngine(t, 1)
	err = engine.spoolNodes(afterFinal, afterFinalSequence)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), engine.stats.Nodes.InputRecords)
}

func TestCoalesceIngestNodesPreservesFirstSeenOrderAndHashesMergedState(t *testing.T) {
	nodeAHash := int32(hashIngestNodeIdentity("node-a"))
	nodeBHash := int32(hashIngestNodeIdentity("node-b"))
	coalesced, err := coalesceIngestNodes([]spooledIngestNode{
		{
			ObjectID: "node-a", IDHash: nodeAHash, Kinds: []string{"User"},
			Properties: map[string]any{"objectid": "node-a", "name": "first", "keep": true},
		},
		{
			ObjectID: "node-b", IDHash: nodeBHash, Kinds: []string{"Computer"},
			Properties: map[string]any{"objectid": "node-b", "name": "middle"},
		},
		{
			ObjectID: "node-a", IDHash: nodeAHash, Kinds: []string{"Admin", "User"},
			Properties: map[string]any{"objectid": "node-a", "name": "last", "added": json.Number("7")},
		},
	})
	require.NoError(t, err)
	require.Len(t, coalesced, 2)
	require.Equal(t, "node-a", coalesced[0].ObjectID)
	require.Equal(t, "node-b", coalesced[1].ObjectID)
	require.Equal(t, []string{"User", "Admin"}, coalesced[0].Kinds)
	require.Equal(t, map[string]any{
		"objectid": "node-a",
		"name":     "last",
		"keep":     true,
		"added":    json.Number("7"),
	}, coalesced[0].Properties)

	wantHash, err := hashIngestNodeContent(
		graph.StringsToKinds([]string{"User", "Admin"}),
		map[string]any{"objectid": "node-a", "name": "last", "keep": true, "added": json.Number("7")},
	)
	require.NoError(t, err)
	require.Equal(t, wantHash[:], coalesced[0].ContentHash)
}

func TestCoalesceIngestNodesRejectsInconsistentDuplicateIdentityHashWithoutExposingIdentity(t *testing.T) {
	secret := "sensitive-object-id"
	identityHash := int32(hashIngestNodeIdentity(secret))
	_, err := coalesceIngestNodes([]spooledIngestNode{
		{ObjectID: secret, IDHash: identityHash, Kinds: []string{"User"}, Properties: map[string]any{"objectid": secret}},
		{ObjectID: secret, IDHash: identityHash + 1, Kinds: []string{"User"}, Properties: map[string]any{"objectid": secret}},
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), secret)
}

func TestNodeIngestSpoolDecodeRejectsValidJSONInvariantCorruption(t *testing.T) {
	secret := "sensitive-object-id"
	buckets, err := newIngestBucketSet(2)
	require.NoError(t, err)
	identityHash := hashIngestNodeIdentity(secret)
	correctBucket := buckets.Bucket(identityHash)
	wrongBucket := correctBucket ^ 1

	tests := map[string]struct {
		record spooledIngestNode
		bucket uint64
	}{
		"mismatched properties identity": {
			record: spooledIngestNode{
				ObjectID: secret, IDHash: int32(identityHash), Kinds: []string{"User"},
				Properties: map[string]any{"objectid": "other"},
			},
			bucket: correctBucket,
		},
		"invalid kind": {
			record: spooledIngestNode{
				ObjectID: secret, IDHash: int32(identityHash), Kinds: []string{"before\x00after"},
				Properties: map[string]any{"objectid": secret},
			},
			bucket: correctBucket,
		},
		"wrong identity hash": {
			record: spooledIngestNode{
				ObjectID: secret, IDHash: int32(identityHash) + 1, Kinds: []string{"User"},
				Properties: map[string]any{"objectid": secret},
			},
			bucket: correctBucket,
		},
		"wrong bucket": {
			record: spooledIngestNode{
				ObjectID: secret, IDHash: int32(identityHash), Kinds: []string{"User"},
				Properties: map[string]any{"objectid": secret},
			},
			bucket: wrongBucket,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			spool, err := newIngestSpool(t.TempDir(), ingestPhaseNodes, 2)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, spool.Close()) })
			require.NoError(t, spool.Append(test.bucket, test.record))

			_, err = coalesceNodeIngestBucket(spool, test.bucket)
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("bucket %d", test.bucket))
			require.Contains(t, err.Error(), "record 1")
			require.NotContains(t, err.Error(), secret)
		})
	}
}

func TestCompareNodeHashesUsesExactIdentityAndNullableHashes(t *testing.T) {
	matchingHash := []byte("0123456789abcdef")
	updateHash := []byte("fedcba9876543210")
	incoming := []coalescedIngestNode{
		{ObjectID: "match", IDHash: 5, ContentHash: matchingHash},
		{ObjectID: "null-hash", IDHash: 5, ContentHash: updateHash},
		{ObjectID: "absent-collision", IDHash: 5, ContentHash: updateHash},
	}
	stored := []storedNodeHash{
		{ObjectID: "different-collision", ContentHash: matchingHash},
		{ObjectID: "match", ContentHash: matchingHash},
		{ObjectID: "null-hash", ContentHash: nil},
	}

	mutations, stats, err := compareNodeHashes(incoming, stored)
	require.NoError(t, err)
	require.Equal(t, []nodeIngestMutation{
		{Node: incoming[1], Insert: false},
		{Node: incoming[2], Insert: true},
	}, mutations)
	require.Equal(t, int64(3), stats.IdentityRowsRead)
	require.Equal(t, int64(1), stats.HashMatches)
	require.Equal(t, int64(1), stats.StagedUpdates)
	require.Equal(t, int64(1), stats.StagedInserts)
}

func TestCompareNodeHashesRejectsMalformedStoredRowsWithoutExposingIdentity(t *testing.T) {
	t.Parallel()

	secret := "sensitive-object-id"
	tests := map[string][]storedNodeHash{
		"empty identity": {
			{ObjectID: "", ContentHash: []byte("0123456789abcdef")},
		},
		"malformed hash": {
			{ObjectID: secret, ContentHash: []byte("too-short")},
		},
		"duplicate identity": {
			{ObjectID: secret, ContentHash: []byte("0123456789abcdef")},
			{ObjectID: secret, ContentHash: []byte("0123456789abcdef")},
		},
	}

	for name, stored := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := compareNodeHashes(nil, stored)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
		})
	}
}

func TestLoadStoredNodeHashesPreservesNonNullEmptyHash(t *testing.T) {
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	graphTarget := testNodeIngestGraph()
	bucketRange := ingestBucketRange{Lower: ingestHashSignedMin}
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}).
			AddRow("node-a", []byte{}))

	stored, err := loadStoredNodeHashes(context.Background(), pool, graphTarget, bucketRange)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.NotNil(t, stored[0].ContentHash)
	require.Empty(t, stored[0].ContentHash)
	_, _, err = compareNodeHashes(nil, stored)
	require.Error(t, err)
	require.NoError(t, pool.ExpectationsWereMet())
}

type staticNodeKindMapper struct {
	idsByKind   map[string]int16
	calls       [][]string
	assertErr   error
	returnIDs   []int16
	hasReturnID bool
}

func (s *staticNodeKindMapper) AssertKinds(_ context.Context, kinds graph.Kinds) ([]int16, error) {
	names := kinds.Strings()
	s.calls = append(s.calls, append([]string(nil), names...))
	if s.assertErr != nil {
		return nil, s.assertErr
	}
	if s.hasReturnID {
		return append([]int16(nil), s.returnIDs...), nil
	}
	ids := make([]int16, len(names))
	for index, name := range names {
		id, ok := s.idsByKind[name]
		if !ok {
			return nil, fmt.Errorf("missing test kind")
		}
		ids[index] = id
	}
	return ids, nil
}

func TestNodeIngestRowsMapSortedUniqueKindsAndMarshalProperties(t *testing.T) {
	mutations := []nodeIngestMutation{{Node: coalescedIngestNode{
		ObjectID: "node-a",
		IDHash:   -42,
		Kinds:    []string{"User", "Admin", "User"},
		Properties: map[string]any{
			"objectid": "node-a",
			"name":     "A",
		},
	}}}

	rows, err := nodeIngestRows(
		context.Background(),
		map[string]int16{"Admin": 9, "User": 3},
		mutations,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "node-a", rows[0][0])
	require.Equal(t, int32(-42), rows[0][1])
	require.Equal(t, []int16{3, 9}, rows[0][2])
	require.JSONEq(t, "{\"name\":\"A\",\"objectid\":\"node-a\"}", string(rows[0][3].([]byte)))
}

func TestNodeIngestRowsRejectMissingKindSnapshot(t *testing.T) {
	mutations := []nodeIngestMutation{{Node: coalescedIngestNode{
		ObjectID: "node-a",
		IDHash:   -42,
		Kinds:    []string{"Admin"},
		Properties: map[string]any{
			"objectid": "node-a",
		},
	}}}

	_, err := nodeIngestRows(context.Background(), map[string]int16{"User": 3}, mutations)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmapped kind")
	require.NotContains(t, err.Error(), "node-a")
}

type checkingNodeDB struct {
	inner       ingestDB
	beforeBegin func() error
	beginCount  int
}

func (s *checkingNodeDB) Begin(ctx context.Context) (pgx.Tx, error) {
	s.beginCount++
	if s.beforeBegin != nil {
		if err := s.beforeBegin(); err != nil {
			return nil, err
		}
	}
	if s.inner == nil {
		return nil, fmt.Errorf("unexpected begin")
	}
	return s.inner.Begin(ctx)
}

func TestNodeIngestProcessBucketSkipsStagingForExactHashMatch(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: &IngestNode{
		ObjectID: "node-a",
		Kinds:    graph.Kinds{graph.StringKind("User")},
	}})))

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	mapper := &staticNodeKindMapper{idsByKind: map[string]int16{"User": 1}}
	checkingDB := &checkingNodeDB{
		inner: pool,
		beforeBegin: func() error {
			if len(mapper.calls) != 1 {
				return fmt.Errorf("kind assertion has not completed exactly once")
			}
			return nil
		},
	}
	engine.db = checkingDB
	engine.graphTarget = testNodeIngestGraph()
	engine.kindMapper = mapper

	coalesced, err := coalesceNodeIngestBucket(engine.nodeSpool, 0)
	require.NoError(t, err)
	require.Len(t, coalesced, 1)
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}).
			AddRow("node-a", coalesced[0].ContentHash))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processNodeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, int64(1), engine.stats.Nodes.CoalescedRecords)
	require.Equal(t, int64(1), engine.stats.Nodes.IdentityRowsRead)
	require.Equal(t, int64(1), engine.stats.Nodes.HashMatches)
	require.Zero(t, engine.stats.Nodes.CommittedMutations)
	require.Equal(t, [][]string{{"User"}}, mapper.calls)
	require.Equal(t, 1, checkingDB.beginCount)
}

func TestNodeIngestProcessBucketStagesMismatchAndUpdatesStatsOnlyAfterCommit(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: &IngestNode{
		ObjectID:   "node-a",
		Kinds:      graph.Kinds{graph.StringKind("User"), graph.StringKind("Admin"), graph.StringKind("User")},
		Properties: &graph.Properties{Map: map[string]any{"name": "A"}},
	}})))

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	engine.db = pool
	engine.graphTarget = testNodeIngestGraph()
	mapper := &staticNodeKindMapper{idsByKind: map[string]int16{"Admin": 9, "User": 3}}
	engine.kindMapper = mapper
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}))
	pool.ExpectExec(pgquery.FormatCreateNodeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.NodeIngestStagingTable}, pgquery.NodeIngestStagingColumns).
		WillReturnResult(1)
	pool.ExpectExec(pgquery.FormatUpsertIngestNodes(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processNodeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, int64(1), engine.stats.Nodes.StagedInserts)
	require.Zero(t, engine.stats.Nodes.StagedUpdates)
	require.Equal(t, int64(1), engine.stats.Nodes.CommittedMutations)
	require.Equal(t, [][]string{{"Admin", "User"}}, mapper.calls)
}

func TestNodeIngestProcessesPopulatedBucketsInAscendingSignedRanges(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 2)
	lowerObjectID := findNodeObjectIDForBucket(t, engine.buckets, 0)
	upperObjectID := findNodeObjectIDForBucket(t, engine.buckets, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(
		ingestNodeSequenceItem{node: &IngestNode{
			ObjectID: upperObjectID,
			Kinds:    graph.Kinds{graph.StringKind("User")},
		}},
		ingestNodeSequenceItem{node: &IngestNode{
			ObjectID: lowerObjectID,
			Kinds:    graph.Kinds{graph.StringKind("User")},
		}},
	)))
	lowerNodes, err := coalesceNodeIngestBucket(engine.nodeSpool, 0)
	require.NoError(t, err)
	upperNodes, err := coalesceNodeIngestBucket(engine.nodeSpool, 1)
	require.NoError(t, err)

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	mapper := &staticNodeKindMapper{idsByKind: map[string]int16{"User": 1}}
	checkingDB := &checkingNodeDB{
		inner: pool,
		beforeBegin: func() error {
			if len(mapper.calls) != 1 {
				return fmt.Errorf("kind assertion has not completed exactly once")
			}
			return nil
		},
	}
	engine.db = checkingDB
	engine.graphTarget = testNodeIngestGraph()
	engine.kindMapper = mapper
	lowerRange := engine.buckets.Range(0)
	upperRange := engine.buckets.Range(1)

	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, false)).
		WithArgs(lowerRange.Lower, *lowerRange.Upper).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}).
			AddRow(lowerObjectID, lowerNodes[0].ContentHash))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, true)).
		WithArgs(upperRange.Lower).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}).
			AddRow(upperObjectID, upperNodes[0].ContentHash))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processNodeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, [][]string{{"User"}}, mapper.calls)
	require.Equal(t, 2, checkingDB.beginCount)
	require.Equal(t, int64(2), engine.stats.Nodes.CoalescedRecords)
	require.Equal(t, int64(2), engine.stats.Nodes.HashMatches)
}

func TestNodeIngestKindAssertionFailuresHappenBeforeBegin(t *testing.T) {
	assertErr := errors.New("assert kinds failed")
	tests := map[string]struct {
		mapper    *staticNodeKindMapper
		wantCause error
	}{
		"assertion error": {
			mapper:    &staticNodeKindMapper{assertErr: assertErr},
			wantCause: assertErr,
		},
		"count mismatch": {
			mapper: &staticNodeKindMapper{hasReturnID: true, returnIDs: nil},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, _ := newTestNodeEngine(t, 1)
			require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: &IngestNode{
				ObjectID: "node-a",
				Kinds:    graph.Kinds{graph.StringKind("User")},
			}})))
			checkingDB := &checkingNodeDB{}
			engine.db = checkingDB
			engine.kindMapper = test.mapper

			err := engine.processNodeBuckets(context.Background())
			require.Error(t, err)
			if test.wantCause != nil {
				require.ErrorIs(t, err, test.wantCause)
			}
			require.Zero(t, checkingDB.beginCount)
			require.Len(t, test.mapper.calls, 1)
		})
	}
}

func TestNodeIngestProcessBucketRollsBackAndLeavesStatsUncommittedOnCountMismatch(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: &IngestNode{
		ObjectID: "node-a",
		Kinds:    graph.Kinds{graph.StringKind("User")},
	}})))

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	engine.db = pool
	engine.graphTarget = testNodeIngestGraph()
	engine.kindMapper = &staticNodeKindMapper{idsByKind: map[string]int16{"User": 1}}
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}))
	pool.ExpectExec(pgquery.FormatCreateNodeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.NodeIngestStagingTable}, pgquery.NodeIngestStagingColumns).
		WillReturnResult(1)
	pool.ExpectExec(pgquery.FormatUpsertIngestNodes(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	pool.ExpectRollback()

	err = engine.processNodeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "node ingest bucket 0")
	require.NoError(t, pool.ExpectationsWereMet())
	require.Zero(t, engine.stats.Nodes.CoalescedRecords)
	require.Zero(t, engine.stats.Nodes.StagedInserts)
	require.Zero(t, engine.stats.Nodes.CommittedMutations)
}

func newNodeFailureTestEngine(t *testing.T) (*ingestEngine, pgxmock.PgxPoolIface) {
	t.Helper()

	engine, _ := newTestNodeEngine(t, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: &IngestNode{
		ObjectID: "node-a",
		Kinds:    graph.Kinds{graph.StringKind("User")},
	}})))
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	engine.db = pool
	engine.graphTarget = testNodeIngestGraph()
	engine.kindMapper = &staticNodeKindMapper{idsByKind: map[string]int16{"User": 1}}

	return engine, pool
}

func expectEmptyNodeHashRange(pool pgxmock.PgxPoolIface, graphTarget model.Graph) {
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}))
}

func expectNodeMutationBeforeCopy(pool pgxmock.PgxPoolIface, graphTarget model.Graph) {
	expectEmptyNodeHashRange(pool, graphTarget)
	pool.ExpectExec(pgquery.FormatCreateNodeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
}

func TestNodeIngestRollsBackRowsScanFailure(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}).
			AddRow(42, []byte("0123456789abcdef")))
	pool.ExpectRollback()

	err := engine.processNodeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load stored hashes")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestRollsBackRowsErr(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	rowsErr := errors.New("rows failed")
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestNodeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"objectid", "content_hash"}).RowError(0, rowsErr))
	pool.ExpectRollback()

	err := engine.processNodeBuckets(context.Background())
	require.ErrorIs(t, err, rowsErr)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestRollsBackCopyFailure(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	copyErr := errors.New("copy failed")
	pool.ExpectBegin()
	expectNodeMutationBeforeCopy(pool, engine.graphTarget)
	copyExpectation := pool.ExpectCopyFrom(
		pgx.Identifier{pgquery.NodeIngestStagingTable},
		pgquery.NodeIngestStagingColumns,
	)
	copyExpectation.WillReturnError(copyErr)
	pool.ExpectRollback()

	err := engine.processNodeBuckets(context.Background())
	require.ErrorIs(t, err, copyErr)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestRollsBackCopyCountMismatch(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	pool.ExpectBegin()
	expectNodeMutationBeforeCopy(pool, engine.graphTarget)
	pool.ExpectCopyFrom(
		pgx.Identifier{pgquery.NodeIngestStagingTable},
		pgquery.NodeIngestStagingColumns,
	).WillReturnResult(0)
	pool.ExpectRollback()

	err := engine.processNodeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "copied 0 of 1")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestRollsBackCommitFailure(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	commitErr := errors.New("commit failed")
	pool.ExpectBegin()
	expectNodeMutationBeforeCopy(pool, engine.graphTarget)
	pool.ExpectCopyFrom(
		pgx.Identifier{pgquery.NodeIngestStagingTable},
		pgquery.NodeIngestStagingColumns,
	).WillReturnResult(1)
	pool.ExpectExec(pgquery.FormatUpsertIngestNodes(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit().WillReturnError(commitErr)
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	err := engine.processNodeBuckets(context.Background())
	require.ErrorIs(t, err, commitErr)
	require.NotErrorIs(t, err, pgx.ErrTxClosed)
	require.NotContains(t, err.Error(), pgx.ErrTxClosed.Error())
	require.NoError(t, pool.ExpectationsWereMet())
	require.Zero(t, engine.stats.Nodes.CommittedMutations)
}

type cancelAfterBeginNodeDB struct {
	inner  ingestDB
	cancel context.CancelFunc
}

func (s cancelAfterBeginNodeDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.inner.Begin(ctx)
	if err == nil {
		s.cancel()
	}
	return tx, err
}

type ingestRollbackContextObservation struct {
	called      bool
	contextErr  error
	deadline    time.Time
	hasDeadline bool
}

type observingIngestRollbackDB struct {
	inner       ingestDB
	afterBegin  func()
	observation *ingestRollbackContextObservation
}

func (s observingIngestRollbackDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.inner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if s.afterBegin != nil {
		s.afterBegin()
	}

	return observingIngestRollbackTx{Tx: tx, observation: s.observation}, nil
}

type observingIngestRollbackTx struct {
	pgx.Tx
	observation *ingestRollbackContextObservation
}

func (s observingIngestRollbackTx) Rollback(ctx context.Context) error {
	if s.observation != nil {
		s.observation.called = true
		s.observation.contextErr = ctx.Err()
		s.observation.deadline, s.observation.hasDeadline = ctx.Deadline()
	}

	return s.Tx.Rollback(ctx)
}

func requireBoundedIngestRollbackContext(
	t *testing.T,
	observation ingestRollbackContextObservation,
) {
	t.Helper()

	require.True(t, observation.called)
	require.NoError(t, observation.contextErr)
	require.True(t, observation.hasDeadline)
	remaining := time.Until(observation.deadline)
	require.Positive(t, remaining)
	require.LessOrEqual(t, remaining, 30*time.Second)
}

func TestNodeIngestCancellationAfterBeginRollsBackBeforeQuery(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	engine.db = cancelAfterBeginNodeDB{inner: pool, cancel: cancel}
	pool.ExpectBegin()
	pool.ExpectRollback()

	err := engine.processNodeBuckets(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestCanceledPathUsesIndependentBoundedRollbackContext(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	observation := ingestRollbackContextObservation{}
	engine.db = observingIngestRollbackDB{
		inner:       pool,
		afterBegin:  cancel,
		observation: &observation,
	}
	pool.ExpectBegin()
	pool.ExpectRollback()

	started := time.Now()
	err := engine.processNodeBuckets(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(started), time.Second)
	requireBoundedIngestRollbackContext(t, observation)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestJoinsPrimaryAndRollbackFailures(t *testing.T) {
	engine, pool := newNodeFailureTestEngine(t)
	copyErr := errors.New("copy failed")
	rollbackErr := errors.New("rollback failed")
	pool.ExpectBegin()
	expectNodeMutationBeforeCopy(pool, engine.graphTarget)
	copyExpectation := pool.ExpectCopyFrom(
		pgx.Identifier{pgquery.NodeIngestStagingTable},
		pgquery.NodeIngestStagingColumns,
	)
	copyExpectation.WillReturnError(copyErr)
	pool.ExpectRollback().WillReturnError(rollbackErr)

	err := engine.processNodeBuckets(context.Background())
	require.ErrorIs(t, err, copyErr)
	require.ErrorIs(t, err, rollbackErr)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestNodeIngestProcessBucketsChecksContextBeforeDatabaseWork(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{node: &IngestNode{
		ObjectID: "node-a",
		Kinds:    graph.Kinds{graph.StringKind("User")},
	}})))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	err := engine.processNodeBuckets(canceled)
	require.ErrorIs(t, err, context.Canceled)
}

func testNodeIngestGraph() model.Graph {
	return model.Graph{
		ID: 42,
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition("node_42"),
			Edge: model.NewGraphPartition("edge_42"),
		},
	}
}

func findNodeObjectIDForBucket(t *testing.T, buckets ingestBucketSet, want uint64) string {
	t.Helper()

	for index := 0; index < 10_000; index++ {
		objectID := fmt.Sprintf("node-%d", index)
		if buckets.Bucket(hashIngestNodeIdentity(objectID)) == want {
			return objectID
		}
	}
	t.Fatalf("failed to find node object ID for bucket %d", want)
	return ""
}

func TestSpoolIngestNodesReturnsNilForNilSequence(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	require.NoError(t, engine.spoolNodes(context.Background(), nil))
	require.Empty(t, engine.nodeSpool.PopulatedBuckets())
}

func TestSpoolIngestNodesPreservesIteratorCauseWithoutPrintingIt(t *testing.T) {
	engine, _ := newTestNodeEngine(t, 1)
	cause := errors.New("sensitive-object-id")
	err := engine.spoolNodes(context.Background(), ingestNodeSequence(ingestNodeSequenceItem{err: cause}))
	require.ErrorIs(t, err, cause)
	require.NotContains(t, err.Error(), cause.Error())
}

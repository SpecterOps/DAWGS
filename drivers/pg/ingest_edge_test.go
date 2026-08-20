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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type ingestEdgeSequenceItem struct {
	edge *IngestEdge
	err  error
}

func ingestEdgeSequence(items ...ingestEdgeSequenceItem) iter.Seq2[*IngestEdge, error] {
	return func(yield func(*IngestEdge, error) bool) {
		for _, item := range items {
			if !yield(item.edge, item.err) {
				return
			}
		}
	}
}

func newTestEdgeEngine(t *testing.T, bucketCount uint64) (*ingestEngine, *ingestSpool) {
	t.Helper()

	buckets, err := newIngestBucketSet(bucketCount)
	require.NoError(t, err)
	spool, err := newIngestSpool(t.TempDir(), ingestPhaseEdges, bucketCount)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })

	return &ingestEngine{
		buckets:   buckets,
		edgeSpool: spool,
	}, spool
}

func readSpooledEdges(t *testing.T, spool *ingestSpool, bucket uint64) []spooledIngestEdge {
	t.Helper()

	var records []spooledIngestEdge
	require.NoError(t, spool.Read(bucket, func(payload []byte) error {
		var record spooledIngestEdge
		if err := json.Unmarshal(payload, &record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	}))

	return records
}

func TestSpoolIngestEdgesNormalizesWithoutMutatingCallerAndSelectsDeterministicBucket(t *testing.T) {
	engine, spool := newTestEdgeEngine(t, 8)
	properties := &graph.Properties{
		Map: map[string]any{
			"weight": 1,
			"nested": map[string]any{"enabled": true},
		},
		Modified: map[string]struct{}{"weight": {}},
		Deleted:  map[string]struct{}{"nested": {}},
	}
	edge := &IngestEdge{
		StartObjectID: "start-a",
		EndObjectID:   "end-a",
		Kind:          graph.StringKind("MemberOf"),
		Properties:    properties,
	}

	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(ingestEdgeSequenceItem{edge: edge})))

	require.Equal(t, map[string]any{"enabled": true}, properties.Map["nested"])
	require.Equal(t, int64(1), engine.stats.Edges.InputRecords)
	require.Equal(t, int64(1), engine.stats.Edges.PopulatedBuckets)
	require.Equal(t, spool.BytesWritten(), engine.stats.Edges.SpoolBytes)
	require.Equal(t, map[string]struct{}{"MemberOf": {}}, engine.edgeKinds)

	expectedHash := hashIngestEdgeIdentity("start-a", "MemberOf", "end-a")
	expectedBucket := engine.buckets.Bucket(expectedHash)
	require.Equal(t, []uint64{expectedBucket}, spool.PopulatedBuckets())
	require.Equal(t, []spooledIngestEdge{{
		StartObjectID: "start-a",
		EndObjectID:   "end-a",
		Kind:          "MemberOf",
		IDHash:        int32(expectedHash),
		Properties: map[string]any{
			"weight": float64(1),
			"nested": map[string]any{"enabled": true},
		},
	}}, readSpooledEdges(t, spool, expectedBucket))
}

func TestSpoolIngestEdgesValidatesRecordsWithoutExposingEndpointIDs(t *testing.T) {
	t.Parallel()

	secretStart := "sensitive-start-id"
	secretEnd := "sensitive-end-id"
	iteratorErr := fmt.Errorf("upstream failure for %s and %s", secretStart, secretEnd)
	valid := func() *IngestEdge {
		return &IngestEdge{
			StartObjectID: secretStart,
			EndObjectID:   secretEnd,
			Kind:          graph.StringKind("MemberOf"),
		}
	}
	tests := map[string]struct {
		item      ingestEdgeSequenceItem
		wantCause error
	}{
		"nil record":  {},
		"empty start": {item: ingestEdgeSequenceItem{edge: &IngestEdge{EndObjectID: secretEnd, Kind: graph.StringKind("MemberOf")}}},
		"empty end":   {item: ingestEdgeSequenceItem{edge: &IngestEdge{StartObjectID: secretStart, Kind: graph.StringKind("MemberOf")}}},
		"nil kind":    {item: ingestEdgeSequenceItem{edge: &IngestEdge{StartObjectID: secretStart, EndObjectID: secretEnd}}},
		"empty kind":  {item: ingestEdgeSequenceItem{edge: &IngestEdge{StartObjectID: secretStart, EndObjectID: secretEnd, Kind: graph.StringKind("")}}},
		"invalid start UTF-8": {item: ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: string([]byte{0xff}), EndObjectID: secretEnd, Kind: graph.StringKind("MemberOf"),
		}}},
		"invalid end NUL": {item: ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: secretStart, EndObjectID: "bad\x00end", Kind: graph.StringKind("MemberOf"),
		}}},
		"invalid kind UTF-8": {item: ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: secretStart, EndObjectID: secretEnd, Kind: graph.StringKind(string([]byte{0xff})),
		}}},
		"invalid properties": {item: ingestEdgeSequenceItem{edge: func() *IngestEdge {
			edge := valid()
			edge.Properties = &graph.Properties{Map: map[string]any{"score": math.NaN()}}
			return edge
		}()}},
		"iterator error": {item: ingestEdgeSequenceItem{err: iteratorErr}, wantCause: iteratorErr},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, _ := newTestEdgeEngine(t, 1)
			err := engine.spoolEdges(context.Background(), ingestEdgeSequence(test.item))
			require.Error(t, err)
			require.Contains(t, err.Error(), "edge ingest")
			require.Contains(t, err.Error(), "record 1")
			require.NotContains(t, err.Error(), secretStart)
			require.NotContains(t, err.Error(), secretEnd)
			if test.wantCause != nil {
				require.ErrorIs(t, err, test.wantCause)
			}
			require.Zero(t, engine.stats.Edges.InputRecords)
		})
	}
}

func TestSpoolIngestEdgesConsumesIteratorOnceAndHonorsCancellation(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	iterations := 0
	sequence := iter.Seq2[*IngestEdge, error](func(yield func(*IngestEdge, error) bool) {
		iterations++
		yield(&IngestEdge{StartObjectID: "start", EndObjectID: "end", Kind: graph.StringKind("K")}, nil)
	})
	require.NoError(t, engine.spoolEdges(context.Background(), sequence))
	require.Equal(t, 1, iterations)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	engine, _ = newTestEdgeEngine(t, 1)
	err := engine.spoolEdges(canceled, sequence)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, iterations)

	afterFinal, cancelAfterFinal := context.WithCancel(context.Background())
	engine, _ = newTestEdgeEngine(t, 1)
	err = engine.spoolEdges(afterFinal, func(yield func(*IngestEdge, error) bool) {
		yield(&IngestEdge{StartObjectID: "start", EndObjectID: "end", Kind: graph.StringKind("K")}, nil)
		cancelAfterFinal()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), engine.stats.Edges.InputRecords)
}

func TestSpoolIngestEdgesReturnsNilForNilSequence(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), nil))
	require.Empty(t, engine.edgeSpool.PopulatedBuckets())
}

func TestCoalesceIngestEdgesPreservesDirectionKindAndFirstSeenOrder(t *testing.T) {
	record := func(start, kind, end string, properties map[string]any) spooledIngestEdge {
		return spooledIngestEdge{
			StartObjectID: start,
			EndObjectID:   end,
			Kind:          kind,
			IDHash:        int32(hashIngestEdgeIdentity(start, kind, end)),
			Properties:    properties,
		}
	}

	coalesced, err := coalesceIngestEdges([]spooledIngestEdge{
		record("a", "K", "b", map[string]any{"first": true, "replace": "old", "nested": map[string]any{"old": true}}),
		record("b", "K", "a", map[string]any{"direction": "reverse"}),
		record("a", "Other", "b", map[string]any{"kind": "other"}),
		record("a", "K", "b", map[string]any{"replace": "new", "nested": map[string]any{"new": true}}),
	})
	require.NoError(t, err)
	require.Len(t, coalesced, 3)
	require.Equal(t, []string{"a", "b", "a"}, []string{
		coalesced[0].StartObjectID, coalesced[1].StartObjectID, coalesced[2].StartObjectID,
	})
	require.Equal(t, map[string]any{
		"first": true, "replace": "new", "nested": map[string]any{"new": true},
	}, coalesced[0].Properties)
	expectedHash, err := hashIngestEdgeContent(coalesced[0].Properties)
	require.NoError(t, err)
	require.Equal(t, expectedHash[:], coalesced[0].ContentHash)
}

func TestCoalesceIngestEdgeBucketFailsClosedOnValidJSONCorruption(t *testing.T) {
	secretStart, secretEnd := "secret-start", "secret-end"
	valid := func() spooledIngestEdge {
		return spooledIngestEdge{
			StartObjectID: secretStart,
			EndObjectID:   secretEnd,
			Kind:          "K",
			IDHash:        int32(hashIngestEdgeIdentity(secretStart, "K", secretEnd)),
			Properties:    map[string]any{},
		}
	}
	tests := map[string]struct {
		bucketCount uint64
		record      func() spooledIngestEdge
		bucket      func(ingestBucketSet, spooledIngestEdge) uint64
	}{
		"wrong bucket": {
			bucketCount: 2,
			record:      valid,
			bucket: func(buckets ingestBucketSet, record spooledIngestEdge) uint64 {
				return (buckets.Bucket(uint32(record.IDHash)) + 1) % 2
			},
		},
		"wrong identity hash": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.IDHash++
				return record
			},
		},
		"empty start": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.StartObjectID = ""
				return record
			},
		},
		"NUL start": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.StartObjectID = "invalid\x00start"
				return record
			},
		},
		"empty end": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.EndObjectID = ""
				return record
			},
		},
		"NUL end": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.EndObjectID = "invalid\x00end"
				return record
			},
		},
		"empty kind": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.Kind = ""
				return record
			},
		},
		"NUL kind": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.Kind = "invalid\x00kind"
				return record
			},
		},
		"nil properties": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.Properties = nil
				return record
			},
		},
		"rejected decoded property": {
			bucketCount: 1,
			record: func() spooledIngestEdge {
				record := valid()
				record.Properties = map[string]any{"value": "invalid\x00property"}
				return record
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, spool := newTestEdgeEngine(t, test.bucketCount)
			record := test.record()
			bucket := uint64(0)
			if test.bucket != nil {
				bucket = test.bucket(engine.buckets, record)
			}
			require.NoError(t, spool.Append(bucket, record))

			_, err := coalesceEdgeIngestBucket(spool, bucket)
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("edge ingest bucket %d spool record 1 is invalid", bucket))
			require.NotContains(t, err.Error(), secretStart)
			require.NotContains(t, err.Error(), secretEnd)
			if record.StartObjectID != "" {
				require.NotContains(t, err.Error(), record.StartObjectID)
			}
			if record.EndObjectID != "" {
				require.NotContains(t, err.Error(), record.EndObjectID)
			}
		})
	}
}

func edgeText(value string) pgtype.Text {
	return pgtype.Text{String: value, Valid: true}
}

func coalescedEdge(start, kind, end string, contentHash []byte) coalescedIngestEdge {
	return coalescedIngestEdge{
		StartObjectID: start,
		EndObjectID:   end,
		Kind:          kind,
		IDHash:        int32(hashIngestEdgeIdentity(start, kind, end)),
		Properties:    map[string]any{},
		ContentHash:   contentHash,
	}
}

func TestCompareEdgeHashesUsesExactDirectedTupleAndKindID(t *testing.T) {
	hashA := []byte("0123456789abcdef")
	hashB := []byte("fedcba9876543210")
	incoming := []coalescedIngestEdge{
		coalescedEdge("a", "K", "b", hashA),
		coalescedEdge("b", "K", "a", hashA),
		coalescedEdge("a", "Other", "b", hashA),
		coalescedEdge("collision-start", "K", "collision-end", hashB),
	}
	// Force a bucket-hash collision without changing any exact source tuple.
	incoming[3].IDHash = incoming[0].IDHash
	stored := []storedEdgeHash{
		{StartObjectID: edgeText("a"), KindID: 7, EndObjectID: edgeText("b"), ContentHash: append([]byte(nil), hashA...)},
		{StartObjectID: edgeText("b"), KindID: 7, EndObjectID: edgeText("a"), ContentHash: nil},
		{StartObjectID: edgeText("a"), KindID: 8, EndObjectID: edgeText("b"), ContentHash: append([]byte(nil), hashB...)},
	}

	mutations, stats, err := compareEdgeHashes(incoming, map[string]int16{"K": 7, "Other": 8}, stored)
	require.NoError(t, err)
	require.Len(t, mutations, 3)
	require.Equal(t, "b", mutations[0].Edge.StartObjectID)
	require.False(t, mutations[0].Insert)
	require.Equal(t, int16(7), mutations[0].KindID)
	require.Equal(t, "Other", mutations[1].Edge.Kind)
	require.False(t, mutations[1].Insert)
	require.Equal(t, "collision-start", mutations[2].Edge.StartObjectID)
	require.True(t, mutations[2].Insert)
	require.Equal(t, IngestPhaseStats{
		IdentityRowsRead: 3,
		HashMatches:      1,
		StagedInserts:    1,
		StagedUpdates:    2,
	}, stats)
}

func TestCompareEdgeHashesRejectsMalformedAndDuplicateStoredRowsWithoutIDs(t *testing.T) {
	secretStart, secretEnd := "secret-start", "secret-end"
	validHash := []byte("0123456789abcdef")
	tests := map[string][]storedEdgeHash{
		"null start on hashed row":  {{KindID: 1, EndObjectID: edgeText(secretEnd), ContentHash: validHash}},
		"empty start on hashed row": {{StartObjectID: edgeText(""), KindID: 1, EndObjectID: edgeText(secretEnd), ContentHash: validHash}},
		"null end on hashed row":    {{StartObjectID: edgeText(secretStart), KindID: 1, ContentHash: validHash}},
		"empty end on hashed row":   {{StartObjectID: edgeText(secretStart), KindID: 1, EndObjectID: edgeText(""), ContentHash: validHash}},
		"non-null empty hash":       {{StartObjectID: edgeText(secretStart), KindID: 1, EndObjectID: edgeText(secretEnd), ContentHash: []byte{}}},
		"short hash":                {{StartObjectID: edgeText(secretStart), KindID: 1, EndObjectID: edgeText(secretEnd), ContentHash: []byte("short")}},
		"duplicate exact tuple": {
			{StartObjectID: edgeText(secretStart), KindID: 1, EndObjectID: edgeText(secretEnd), ContentHash: validHash},
			{StartObjectID: edgeText(secretStart), KindID: 1, EndObjectID: edgeText(secretEnd), ContentHash: validHash},
		},
	}

	for name, stored := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := compareEdgeHashes(nil, map[string]int16{}, stored)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secretStart)
			require.NotContains(t, err.Error(), secretEnd)
		})
	}
}

func TestLoadStoredEdgeHashesPreservesNullAndNonNullEmptyBytea(t *testing.T) {
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	graphTarget := testEdgeIngestGraph()
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}).
			AddRow("a", int16(1), "b", nil).
			AddRow("c", int16(2), "d", []byte{}))

	stored, err := loadStoredEdgeHashes(context.Background(), pool, graphTarget, ingestBucketRange{Lower: ingestHashSignedMin})
	require.NoError(t, err)
	require.Len(t, stored, 2)
	require.Nil(t, stored[0].ContentHash)
	require.NotNil(t, stored[1].ContentHash)
	require.Empty(t, stored[1].ContentHash)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestResolveIngestEndpointsDeduplicatesAndUsesParallelSignedHashes(t *testing.T) {
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	graphTarget := testEdgeIngestGraph()
	mutations := []edgeIngestMutation{
		{Edge: coalescedEdge("start-a", "K", "end-a", []byte("0123456789abcdef")), KindID: 3},
		{Edge: coalescedEdge("start-a", "K", "end-b", []byte("0123456789abcdef")), KindID: 3},
	}
	objectIDs := []string{"start-a", "end-a", "end-b"}
	hashes := []int32{
		int32(hashIngestNodeIdentity("start-a")),
		int32(hashIngestNodeIdentity("end-a")),
		int32(hashIngestNodeIdentity("end-b")),
	}
	pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(graphTarget)).
		WithArgs(hashes, objectIDs).
		WillReturnRows(pgxmock.NewRows([]string{"object_id", "id"}).
			AddRow("end-b", int64(33)).
			AddRow("start-a", int64(11)).
			AddRow("end-a", int64(22)))

	resolved, err := resolveIngestEndpoints(context.Background(), pool, graphTarget, 5, mutations)
	require.NoError(t, err)
	require.Equal(t, map[string]graph.ID{
		"start-a": 11,
		"end-a":   22,
		"end-b":   33,
	}, resolved)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestResolveIngestEndpointsReportsMissingAndAmbiguousCountsWithoutIDs(t *testing.T) {
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	graphTarget := testEdgeIngestGraph()
	secretStart, secretEnd := "secret-start", "secret-end"
	mutations := []edgeIngestMutation{{
		Edge:   coalescedEdge(secretStart, "K", secretEnd, []byte("0123456789abcdef")),
		KindID: 3,
	}}
	pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(graphTarget)).
		WithArgs(
			[]int32{int32(hashIngestNodeIdentity(secretStart)), int32(hashIngestNodeIdentity(secretEnd))},
			[]string{secretStart, secretEnd},
		).
		WillReturnRows(pgxmock.NewRows([]string{"object_id", "id"}).
			AddRow(secretStart, int64(11)).
			AddRow(secretStart, int64(12)))

	_, err = resolveIngestEndpoints(context.Background(), pool, graphTarget, 9, mutations)
	require.Error(t, err)
	require.Contains(t, err.Error(), "edge ingest bucket 9")
	require.Contains(t, err.Error(), "1 missing")
	require.Contains(t, err.Error(), "1 ambiguous")
	require.NotContains(t, err.Error(), secretStart)
	require.NotContains(t, err.Error(), secretEnd)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestResolveIngestEndpointsRejectsDuplicateSameIDInvalidIDsAndUnexpectedSources(t *testing.T) {
	secretStart, secretEnd := "secret-start", "secret-end"
	unexpected := "unexpected-secret-source"
	tests := map[string]struct {
		rows           *pgxmock.Rows
		wantMissing    int
		wantAmbiguous  int
		wantInvalid    int
		wantUnexpected int
	}{
		"duplicate same database ID": {
			rows: pgxmock.NewRows([]string{"object_id", "id"}).
				AddRow(secretStart, int64(11)).
				AddRow(secretStart, int64(11)).
				AddRow(secretEnd, int64(22)),
			wantAmbiguous: 1,
		},
		"NULL database ID": {
			rows: pgxmock.NewRows([]string{"object_id", "id"}).
				AddRow(secretStart, nil).
				AddRow(secretEnd, int64(22)),
			wantInvalid: 1,
		},
		"zero database ID": {
			rows: pgxmock.NewRows([]string{"object_id", "id"}).
				AddRow(secretStart, int64(0)).
				AddRow(secretEnd, int64(22)),
			wantInvalid: 1,
		},
		"negative database ID": {
			rows: pgxmock.NewRows([]string{"object_id", "id"}).
				AddRow(secretStart, int64(-1)).
				AddRow(secretEnd, int64(22)),
			wantInvalid: 1,
		},
		"unexpected source identity": {
			rows: pgxmock.NewRows([]string{"object_id", "id"}).
				AddRow(secretStart, int64(11)).
				AddRow(secretEnd, int64(22)).
				AddRow(unexpected, int64(33)),
			wantUnexpected: 1,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			graphTarget := testEdgeIngestGraph()
			mutation := edgeIngestMutation{
				Edge:   coalescedEdge(secretStart, "K", secretEnd, []byte("0123456789abcdef")),
				KindID: 3,
			}
			pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(graphTarget)).
				WithArgs(
					[]int32{int32(hashIngestNodeIdentity(secretStart)), int32(hashIngestNodeIdentity(secretEnd))},
					[]string{secretStart, secretEnd},
				).
				WillReturnRows(test.rows)

			resolved, err := resolveIngestEndpoints(context.Background(), pool, graphTarget, 6, []edgeIngestMutation{mutation})
			require.Error(t, err)
			require.Nil(t, resolved)
			require.Contains(t, err.Error(), "edge ingest bucket 6")
			require.Contains(t, err.Error(), fmt.Sprintf("%d missing", test.wantMissing))
			require.Contains(t, err.Error(), fmt.Sprintf("%d ambiguous", test.wantAmbiguous))
			require.Contains(t, err.Error(), fmt.Sprintf("%d invalid-ID", test.wantInvalid))
			require.Contains(t, err.Error(), fmt.Sprintf("%d unexpected", test.wantUnexpected))
			require.NotContains(t, err.Error(), secretStart)
			require.NotContains(t, err.Error(), secretEnd)
			require.NotContains(t, err.Error(), unexpected)
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

func TestResolveIngestEndpointsWrapsQueryScanAndRowsFailuresWithoutIDs(t *testing.T) {
	secretStart, secretEnd := "secret-start", "secret-end"
	queryErr := errors.New("query failed with " + secretStart)
	scanErrRows := pgxmock.NewRows([]string{"object_id", "id"}).AddRow(42, int64(11))
	rowsErr := errors.New("rows failed with " + secretEnd)
	tests := map[string]struct {
		queryErr error
		rows     *pgxmock.Rows
		cause    error
	}{
		"query": {queryErr: queryErr, cause: queryErr},
		"scan":  {rows: scanErrRows},
		"rows":  {rows: pgxmock.NewRows([]string{"object_id", "id"}).RowError(0, rowsErr), cause: rowsErr},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			graphTarget := testEdgeIngestGraph()
			mutation := edgeIngestMutation{
				Edge:   coalescedEdge(secretStart, "K", secretEnd, []byte("0123456789abcdef")),
				KindID: 3,
			}
			expectation := pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(graphTarget)).
				WithArgs(
					[]int32{int32(hashIngestNodeIdentity(secretStart)), int32(hashIngestNodeIdentity(secretEnd))},
					[]string{secretStart, secretEnd},
				)
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(test.rows)
			}

			_, err = resolveIngestEndpoints(context.Background(), pool, graphTarget, 4, []edgeIngestMutation{mutation})
			require.Error(t, err)
			require.Contains(t, err.Error(), "edge ingest bucket 4")
			require.NotContains(t, err.Error(), secretStart)
			require.NotContains(t, err.Error(), secretEnd)
			if test.cause != nil {
				require.ErrorIs(t, err, test.cause)
			}
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

func TestEdgeIngestRowsUseResolvedIDsSourceIdentityAndKindSnapshot(t *testing.T) {
	mutation := edgeIngestMutation{
		Edge: coalescedIngestEdge{
			StartObjectID: "start",
			EndObjectID:   "end",
			Kind:          "K",
			IDHash:        -42,
			Properties:    map[string]any{"weight": json.Number("1")},
		},
		KindID: 7,
	}

	rows, err := edgeIngestRows(context.Background(), []edgeIngestMutation{mutation}, map[string]graph.ID{
		"start": 11,
		"end":   22,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, graph.ID(11), rows[0][0])
	require.Equal(t, graph.ID(22), rows[0][1])
	require.Equal(t, "start", rows[0][2])
	require.Equal(t, "end", rows[0][3])
	require.Equal(t, int16(7), rows[0][4])
	require.Equal(t, int32(-42), rows[0][5])
	require.JSONEq(t, `{"weight":1}`, string(rows[0][6].([]byte)))
}

func TestEdgeIngestRowsRejectMissingResolvedEndpointWithoutPrintingIDs(t *testing.T) {
	secretStart, secretEnd := "secret-start", "secret-end"
	_, err := edgeIngestRows(context.Background(), []edgeIngestMutation{{
		Edge:   coalescedEdge(secretStart, "K", secretEnd, []byte("0123456789abcdef")),
		KindID: 7,
	}}, map[string]graph.ID{secretStart: 11})
	require.Error(t, err)
	require.NotContains(t, err.Error(), secretStart)
	require.NotContains(t, err.Error(), secretEnd)
}

func TestEdgeIngestProcessBucketSkipsEndpointResolutionAndStagingForExactHashMatch(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(ingestEdgeSequenceItem{edge: &IngestEdge{
		StartObjectID: "start",
		EndObjectID:   "end",
		Kind:          graph.StringKind("K"),
	}})))
	coalesced, err := coalesceEdgeIngestBucket(engine.edgeSpool, 0)
	require.NoError(t, err)

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	mapper := &staticNodeKindMapper{idsByKind: map[string]int16{"K": 7}}
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
	engine.kindMapper = mapper
	engine.graphTarget = testEdgeIngestGraph()
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}).
			AddRow("start", int16(7), "end", coalesced[0].ContentHash))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processEdgeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, [][]string{{"K"}}, mapper.calls)
	require.Equal(t, int64(1), engine.stats.Edges.CoalescedRecords)
	require.Equal(t, int64(1), engine.stats.Edges.IdentityRowsRead)
	require.Equal(t, int64(1), engine.stats.Edges.HashMatches)
	require.Zero(t, engine.stats.Edges.CommittedMutations)
}

func TestEdgeIngestProcessBucketResolvesStagesPreflightsUpsertsAndCommits(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(ingestEdgeSequenceItem{edge: &IngestEdge{
		StartObjectID: "start",
		EndObjectID:   "end",
		Kind:          graph.StringKind("K"),
		Properties:    &graph.Properties{Map: map[string]any{"weight": 1}},
	}})))

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	engine.db = pool
	engine.kindMapper = &staticNodeKindMapper{idsByKind: map[string]int16{"K": 7}}
	engine.graphTarget = testEdgeIngestGraph()
	pool.ExpectBegin()
	expectEmptyEdgeHashRange(pool, engine.graphTarget)
	pool.ExpectExec(pgquery.FormatCreateEdgeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	expectResolvedEdgeEndpoints(pool, engine.graphTarget, "start", "end")
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
		WillReturnResult(1)
	pool.ExpectQuery(pgquery.FormatValidateIngestEdgeSources(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))
	pool.ExpectExec(pgquery.FormatUpsertIngestEdges(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processEdgeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, int64(1), engine.stats.Edges.StagedInserts)
	require.Zero(t, engine.stats.Edges.StagedUpdates)
	require.Equal(t, int64(1), engine.stats.Edges.CommittedMutations)
}

type capturingEdgeCopyDB struct {
	inner ingestDB
	rows  *[][]any
}

func (s capturingEdgeCopyDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.inner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return capturingEdgeCopyTx{Tx: tx, rows: s.rows}, nil
}

type capturingEdgeCopyTx struct {
	pgx.Tx
	rows *[][]any
}

func (s capturingEdgeCopyTx) CopyFrom(
	ctx context.Context,
	tableName pgx.Identifier,
	columnNames []string,
	rowSource pgx.CopyFromSource,
) (int64, error) {
	var captured [][]any
	for rowSource.Next() {
		values, err := rowSource.Values()
		if err != nil {
			return 0, err
		}
		captured = append(captured, append([]any(nil), values...))
	}
	if err := rowSource.Err(); err != nil {
		return 0, err
	}
	*s.rows = append(*s.rows, captured...)
	return s.Tx.CopyFrom(ctx, tableName, columnNames, pgx.CopyFromRows(captured))
}

func TestEdgeIngestMapsSortedAssertedKindNamesToTheirPositionalIDsWhenStaging(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(
		ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: "zebra-start", EndObjectID: "zebra-end", Kind: graph.StringKind("Zebra"),
		}},
		ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: "alpha-start", EndObjectID: "alpha-end", Kind: graph.StringKind("Alpha"),
		}},
	)))

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	mapper := &staticNodeKindMapper{hasReturnID: true, returnIDs: []int16{91, 3}}
	var copiedRows [][]any
	engine.db = capturingEdgeCopyDB{inner: pool, rows: &copiedRows}
	engine.kindMapper = mapper
	engine.graphTarget = testEdgeIngestGraph()

	pool.ExpectBegin()
	expectEmptyEdgeHashRange(pool, engine.graphTarget)
	pool.ExpectExec(pgquery.FormatCreateEdgeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	objectIDs := []string{"zebra-start", "zebra-end", "alpha-start", "alpha-end"}
	pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(engine.graphTarget)).
		WithArgs(
			[]int32{
				int32(hashIngestNodeIdentity(objectIDs[0])),
				int32(hashIngestNodeIdentity(objectIDs[1])),
				int32(hashIngestNodeIdentity(objectIDs[2])),
				int32(hashIngestNodeIdentity(objectIDs[3])),
			},
			objectIDs,
		).
		WillReturnRows(pgxmock.NewRows([]string{"object_id", "id"}).
			AddRow(objectIDs[0], int64(11)).
			AddRow(objectIDs[1], int64(12)).
			AddRow(objectIDs[2], int64(21)).
			AddRow(objectIDs[3], int64(22)))
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
		WillReturnResult(2)
	pool.ExpectQuery(pgquery.FormatValidateIngestEdgeSources(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))
	pool.ExpectExec(pgquery.FormatUpsertIngestEdges(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 2))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processEdgeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, [][]string{{"Alpha", "Zebra"}}, mapper.calls)
	require.Len(t, copiedRows, 2)
	require.Equal(t, graph.ID(11), copiedRows[0][0])
	require.Equal(t, graph.ID(12), copiedRows[0][1])
	require.Equal(t, "zebra-start", copiedRows[0][2])
	require.Equal(t, int16(3), copiedRows[0][4])
	require.Equal(t, graph.ID(21), copiedRows[1][0])
	require.Equal(t, graph.ID(22), copiedRows[1][1])
	require.Equal(t, "alpha-start", copiedRows[1][2])
	require.Equal(t, int16(91), copiedRows[1][4])
}

func TestEdgeIngestCoalescingFailureReportsPhaseInputCountWithoutCallingItBucketCount(t *testing.T) {
	engine, spool := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(
		ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: "first-start", EndObjectID: "first-end", Kind: graph.StringKind("K"),
		}},
		ingestEdgeSequenceItem{edge: &IngestEdge{
			StartObjectID: "second-start", EndObjectID: "second-end", Kind: graph.StringKind("K"),
		}},
	)))
	corruptStart, corruptEnd := "corrupt-secret-start", "corrupt-secret-end"
	require.NoError(t, spool.Append(0, spooledIngestEdge{
		StartObjectID: corruptStart,
		EndObjectID:   corruptEnd,
		Kind:          "K",
		IDHash:        int32(hashIngestEdgeIdentity(corruptStart, "K", corruptEnd)) + 1,
		Properties:    map[string]any{},
	}))
	engine.kindMapper = &staticNodeKindMapper{idsByKind: map[string]int16{"K": 7}}

	err := engine.processEdgeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "edge ingest bucket 0 failed while coalescing after 2 phase input records")
	require.NotContains(t, err.Error(), "coalescing 2 input records")
	require.NotContains(t, err.Error(), corruptStart)
	require.NotContains(t, err.Error(), corruptEnd)
}

func TestEdgeIngestProcessesPopulatedBucketsInAscendingSignedRanges(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 2)
	lowerStart, lowerEnd := findEdgeTupleForBucket(t, engine.buckets, 0)
	upperStart, upperEnd := findEdgeTupleForBucket(t, engine.buckets, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(
		ingestEdgeSequenceItem{edge: &IngestEdge{StartObjectID: upperStart, EndObjectID: upperEnd, Kind: graph.StringKind("K")}},
		ingestEdgeSequenceItem{edge: &IngestEdge{StartObjectID: lowerStart, EndObjectID: lowerEnd, Kind: graph.StringKind("K")}},
	)))
	lowerEdges, err := coalesceEdgeIngestBucket(engine.edgeSpool, 0)
	require.NoError(t, err)
	upperEdges, err := coalesceEdgeIngestBucket(engine.edgeSpool, 1)
	require.NoError(t, err)

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	mapper := &staticNodeKindMapper{idsByKind: map[string]int16{"K": 7}}
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
	engine.kindMapper = mapper
	engine.graphTarget = testEdgeIngestGraph()
	lowerRange := engine.buckets.Range(0)
	upperRange := engine.buckets.Range(1)

	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(engine.graphTarget, false)).
		WithArgs(lowerRange.Lower, *lowerRange.Upper).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}).
			AddRow(lowerStart, int16(7), lowerEnd, lowerEdges[0].ContentHash))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(engine.graphTarget, true)).
		WithArgs(upperRange.Lower).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}).
			AddRow(upperStart, int16(7), upperEnd, upperEdges[0].ContentHash))
	pool.ExpectCommit()
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	require.NoError(t, engine.processEdgeBuckets(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
	require.Equal(t, [][]string{{"K"}}, mapper.calls)
	require.Equal(t, 2, checkingDB.beginCount)
	require.Equal(t, int64(2), engine.stats.Edges.CoalescedRecords)
	require.Equal(t, int64(2), engine.stats.Edges.HashMatches)
}

func newEdgeFailureTestEngine(t *testing.T) (*ingestEngine, pgxmock.PgxPoolIface) {
	t.Helper()

	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(ingestEdgeSequenceItem{edge: &IngestEdge{
		StartObjectID: "start",
		EndObjectID:   "end",
		Kind:          graph.StringKind("K"),
	}})))
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	engine.db = pool
	engine.kindMapper = &staticNodeKindMapper{idsByKind: map[string]int16{"K": 7}}
	engine.graphTarget = testEdgeIngestGraph()

	return engine, pool
}

func expectEmptyEdgeHashRange(pool pgxmock.PgxPoolIface, graphTarget model.Graph) {
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}))
}

func expectResolvedEdgeEndpoints(pool pgxmock.PgxPoolIface, graphTarget model.Graph, start, end string) {
	pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(graphTarget)).
		WithArgs(
			[]int32{int32(hashIngestNodeIdentity(start)), int32(hashIngestNodeIdentity(end))},
			[]string{start, end},
		).
		WillReturnRows(pgxmock.NewRows([]string{"object_id", "id"}).
			AddRow(start, int64(11)).
			AddRow(end, int64(22)))
}

func expectEdgeMutationBeforeCopy(pool pgxmock.PgxPoolIface, graphTarget model.Graph) {
	expectEmptyEdgeHashRange(pool, graphTarget)
	pool.ExpectExec(pgquery.FormatCreateEdgeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	expectResolvedEdgeEndpoints(pool, graphTarget, "start", "end")
}

func expectEdgeMutationAfterCopy(pool pgxmock.PgxPoolIface, graphTarget model.Graph) {
	pool.ExpectQuery(pgquery.FormatValidateIngestEdgeSources(graphTarget)).
		WithArgs(graphTarget.ID).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))
}

func TestEdgeIngestRollsBackStoredRowsScanFailure(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}).
			AddRow(42, int16(7), "end", []byte("0123456789abcdef")))
	pool.ExpectRollback()

	err := engine.processEdgeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load stored hashes")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestRollsBackStoredRowsErr(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	rowsErr := errors.New("stored rows failed")
	pool.ExpectBegin()
	pool.ExpectQuery(pgquery.FormatSelectIngestEdgeHashes(engine.graphTarget, true)).
		WithArgs(int32(ingestHashSignedMin)).
		WillReturnRows(pgxmock.NewRows([]string{"start_object_id", "kind_id", "end_object_id", "content_hash"}).
			RowError(0, rowsErr))
	pool.ExpectRollback()

	err := engine.processEdgeBuckets(context.Background())
	require.ErrorIs(t, err, rowsErr)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestRollsBackStagingCreateFailure(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	createErr := errors.New("create staging failed")
	pool.ExpectBegin()
	expectEmptyEdgeHashRange(pool, engine.graphTarget)
	pool.ExpectExec(pgquery.FormatCreateEdgeIngestStagingTable()).WillReturnError(createErr)
	pool.ExpectRollback()

	err := engine.processEdgeBuckets(context.Background())
	require.ErrorIs(t, err, createErr)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestRollsBackMissingEndpointResolution(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	pool.ExpectBegin()
	expectEmptyEdgeHashRange(pool, engine.graphTarget)
	pool.ExpectExec(pgquery.FormatCreateEdgeIngestStagingTable()).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	pool.ExpectQuery(pgquery.FormatResolveIngestEndpoints(engine.graphTarget)).
		WithArgs(
			[]int32{int32(hashIngestNodeIdentity("start")), int32(hashIngestNodeIdentity("end"))},
			[]string{"start", "end"},
		).
		WillReturnRows(pgxmock.NewRows([]string{"object_id", "id"}).AddRow("start", int64(11)))
	pool.ExpectRollback()

	err := engine.processEdgeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "1 missing")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestRollsBackCopyFailureAndCountMismatch(t *testing.T) {
	tests := map[string]struct {
		copyErr   error
		copyCount int64
	}{
		"failure":        {copyErr: errors.New("copy failed")},
		"count mismatch": {copyCount: 0},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, pool := newEdgeFailureTestEngine(t)
			pool.ExpectBegin()
			expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
			copyExpectation := pool.ExpectCopyFrom(
				pgx.Identifier{pgquery.EdgeIngestStagingTable},
				pgquery.EdgeIngestStagingColumns,
			)
			if test.copyErr != nil {
				copyExpectation.WillReturnError(test.copyErr)
			} else {
				copyExpectation.WillReturnResult(test.copyCount)
			}
			pool.ExpectRollback()

			err := engine.processEdgeBuckets(context.Background())
			if test.copyErr != nil {
				require.ErrorIs(t, err, test.copyErr)
			} else {
				require.Contains(t, err.Error(), "copied 0 of 1")
			}
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

func TestEdgeIngestRollsBackPreflightFailureAndConflict(t *testing.T) {
	tests := map[string]struct {
		queryErr error
		count    int64
	}{
		"query failure": {queryErr: errors.New("preflight failed")},
		"conflict":      {count: 2},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, pool := newEdgeFailureTestEngine(t)
			pool.ExpectBegin()
			expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
			pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
				WillReturnResult(1)
			preflight := pool.ExpectQuery(pgquery.FormatValidateIngestEdgeSources(engine.graphTarget)).
				WithArgs(engine.graphTarget.ID)
			if test.queryErr != nil {
				preflight.WillReturnError(test.queryErr)
			} else {
				preflight.WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(test.count))
			}
			pool.ExpectRollback()

			err := engine.processEdgeBuckets(context.Background())
			if test.queryErr != nil {
				require.ErrorIs(t, err, test.queryErr)
			} else {
				require.Contains(t, err.Error(), "2 source identity conflicts")
			}
			require.NoError(t, pool.ExpectationsWereMet())
			require.Zero(t, engine.stats.Edges.CommittedMutations)
		})
	}
}

func TestEdgeIngestRollsBackPreflightScanFailure(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	pool.ExpectBegin()
	expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
		WillReturnResult(1)
	pool.ExpectQuery(pgquery.FormatValidateIngestEdgeSources(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow("not-a-count"))
	pool.ExpectRollback()

	err := engine.processEdgeBuckets(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source identity preflight")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestRollsBackUpsertFailureAndCountMismatch(t *testing.T) {
	tests := map[string]struct {
		upsertErr error
		affected  int64
	}{
		"failure":        {upsertErr: errors.New("upsert failed")},
		"count mismatch": {affected: 0},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine, pool := newEdgeFailureTestEngine(t)
			pool.ExpectBegin()
			expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
			pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
				WillReturnResult(1)
			expectEdgeMutationAfterCopy(pool, engine.graphTarget)
			upsert := pool.ExpectExec(pgquery.FormatUpsertIngestEdges(engine.graphTarget)).
				WithArgs(engine.graphTarget.ID)
			if test.upsertErr != nil {
				upsert.WillReturnError(test.upsertErr)
			} else {
				upsert.WillReturnResult(pgxmock.NewResult("INSERT", test.affected))
			}
			pool.ExpectRollback()

			err := engine.processEdgeBuckets(context.Background())
			if test.upsertErr != nil {
				require.ErrorIs(t, err, test.upsertErr)
			} else {
				require.Contains(t, err.Error(), "affected 0 of 1")
			}
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

func TestEdgeIngestCommitFailureIgnoresExpectedClosedRollback(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	commitErr := errors.New("commit failed")
	pool.ExpectBegin()
	expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
		WillReturnResult(1)
	expectEdgeMutationAfterCopy(pool, engine.graphTarget)
	pool.ExpectExec(pgquery.FormatUpsertIngestEdges(engine.graphTarget)).
		WithArgs(engine.graphTarget.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit().WillReturnError(commitErr)
	pool.ExpectRollback().WillReturnError(pgx.ErrTxClosed)

	err := engine.processEdgeBuckets(context.Background())
	require.ErrorIs(t, err, commitErr)
	require.NotErrorIs(t, err, pgx.ErrTxClosed)
	require.NotContains(t, err.Error(), pgx.ErrTxClosed.Error())
	require.NoError(t, pool.ExpectationsWereMet())
	require.Zero(t, engine.stats.Edges.CommittedMutations)
}

func TestEdgeIngestJoinsPrimaryAndRollbackFailures(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	copyErr := errors.New("copy failed")
	rollbackErr := errors.New("rollback failed")
	pool.ExpectBegin()
	expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
		WillReturnError(copyErr)
	pool.ExpectRollback().WillReturnError(rollbackErr)

	err := engine.processEdgeBuckets(context.Background())
	require.ErrorIs(t, err, copyErr)
	require.ErrorIs(t, err, rollbackErr)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestErrorPathUsesIndependentBoundedRollbackContext(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	copyErr := errors.New("copy failed")
	observation := ingestRollbackContextObservation{}
	engine.db = observingIngestRollbackDB{inner: pool, observation: &observation}
	pool.ExpectBegin()
	expectEdgeMutationBeforeCopy(pool, engine.graphTarget)
	pool.ExpectCopyFrom(pgx.Identifier{pgquery.EdgeIngestStagingTable}, pgquery.EdgeIngestStagingColumns).
		WillReturnError(copyErr)
	pool.ExpectRollback()

	started := time.Now()
	err := engine.processEdgeBuckets(context.Background())

	require.ErrorIs(t, err, copyErr)
	require.Less(t, time.Since(started), time.Second)
	requireBoundedIngestRollbackContext(t, observation)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestCancellationAfterBeginRollsBackBeforeQuery(t *testing.T) {
	engine, pool := newEdgeFailureTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	engine.db = cancelAfterBeginNodeDB{inner: pool, cancel: cancel}
	pool.ExpectBegin()
	pool.ExpectRollback()

	err := engine.processEdgeBuckets(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestEdgeIngestKindAssertionFailureHappensBeforeBegin(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(ingestEdgeSequenceItem{edge: &IngestEdge{
		StartObjectID: "start", EndObjectID: "end", Kind: graph.StringKind("K"),
	}})))
	assertErr := errors.New("assert failed")
	engine.kindMapper = &staticNodeKindMapper{assertErr: assertErr}
	engine.db = &checkingNodeDB{}

	err := engine.processEdgeBuckets(context.Background())
	require.ErrorIs(t, err, assertErr)
	require.Zero(t, engine.db.(*checkingNodeDB).beginCount)
}

func TestEdgeIngestProcessBucketsChecksContextBeforeDatabaseWork(t *testing.T) {
	engine, _ := newTestEdgeEngine(t, 1)
	require.NoError(t, engine.spoolEdges(context.Background(), ingestEdgeSequence(ingestEdgeSequenceItem{edge: &IngestEdge{
		StartObjectID: "start", EndObjectID: "end", Kind: graph.StringKind("K"),
	}})))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	err := engine.processEdgeBuckets(canceled)
	require.ErrorIs(t, err, context.Canceled)
}

func testEdgeIngestGraph() model.Graph {
	return model.Graph{
		ID: 42,
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition("node_42"),
			Edge: model.NewGraphPartition("edge_42"),
		},
	}
}

func findEdgeTupleForBucket(t *testing.T, buckets ingestBucketSet, want uint64) (string, string) {
	t.Helper()

	for index := 0; index < 10_000; index++ {
		start := fmt.Sprintf("start-%d", index)
		end := fmt.Sprintf("end-%d", index)
		if buckets.Bucket(hashIngestEdgeIdentity(start, "K", end)) == want {
			return start, end
		}
	}
	t.Fatalf("failed to find edge tuple for bucket %d", want)
	return "", ""
}

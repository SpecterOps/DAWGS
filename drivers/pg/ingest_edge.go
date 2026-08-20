package pg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/drivers/pg/model"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
)

type spooledIngestEdge struct {
	StartObjectID string
	EndObjectID   string
	Kind          string
	IDHash        int32
	Properties    map[string]any
}

type coalescedIngestEdge struct {
	StartObjectID string
	EndObjectID   string
	Kind          string
	IDHash        int32
	Properties    map[string]any
	ContentHash   []byte
}

type storedEdgeHash struct {
	StartObjectID pgtype.Text
	KindID        int16
	EndObjectID   pgtype.Text
	ContentHash   []byte
}

type edgeIngestMutation struct {
	Edge   coalescedIngestEdge
	KindID int16
	Insert bool
}

type edgeIngestSourceKey struct {
	StartObjectID string
	Kind          string
	EndObjectID   string
}

type edgeIngestStoredKey struct {
	StartObjectID string
	KindID        int16
	EndObjectID   string
}

type edgeIngestError struct {
	message string
	cause   error
}

func (s edgeIngestError) Error() string {
	return s.message
}

func (s edgeIngestError) Unwrap() error {
	return s.cause
}

func newEdgeIngestError(cause error, format string, arguments ...any) error {
	return edgeIngestError{
		message: fmt.Sprintf(format, arguments...),
		cause:   cause,
	}
}

func (s *ingestEngine) spoolEdges(ctx context.Context, edges iter.Seq2[*IngestEdge, error]) error {
	if err := ctx.Err(); err != nil {
		return newEdgeIngestError(err, "edge ingest canceled before input record 1")
	}
	if edges == nil {
		return nil
	}
	if s.edgeSpool == nil {
		return newEdgeIngestError(nil, "edge ingest cannot spool input record 1: edge spool is not configured")
	}

	var recordCount int64
	for edge, iteratorErr := range edges {
		recordCount++
		if err := ctx.Err(); err != nil {
			return newEdgeIngestError(err, "edge ingest canceled at input record %d", recordCount)
		}
		if iteratorErr != nil {
			return newEdgeIngestError(iteratorErr, "edge ingest iterator failed at input record %d", recordCount)
		}

		spooled, bucket, err := s.normalizeEdgeForSpool(edge)
		if err != nil {
			return newEdgeIngestError(err, "edge ingest validation failed at input record %d", recordCount)
		}
		if err := s.edgeSpool.Append(bucket, spooled); err != nil {
			return newEdgeIngestError(
				err,
				"edge ingest spool failed at input record %d in bucket %d",
				recordCount,
				bucket,
			)
		}

		s.stats.Edges.InputRecords++
		s.stats.Edges.PopulatedBuckets = int64(s.edgeSpool.PopulatedBucketCount())
		s.stats.Edges.SpoolBytes = s.edgeSpool.BytesWritten()
		if s.edgeKinds == nil {
			s.edgeKinds = make(map[string]struct{})
		}
		s.edgeKinds[spooled.Kind] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		return newEdgeIngestError(err, "edge ingest canceled after input record %d", recordCount)
	}

	return nil
}

func (s *ingestEngine) normalizeEdgeForSpool(edge *IngestEdge) (spooledIngestEdge, uint64, error) {
	if edge == nil {
		return spooledIngestEdge{}, 0, fmt.Errorf("record is nil")
	}
	if edge.StartObjectID == "" {
		return spooledIngestEdge{}, 0, fmt.Errorf("start object ID is empty")
	}
	if err := validateIngestString(edge.StartObjectID); err != nil {
		return spooledIngestEdge{}, 0, fmt.Errorf("start object ID is invalid: %w", err)
	}
	if edge.EndObjectID == "" {
		return spooledIngestEdge{}, 0, fmt.Errorf("end object ID is empty")
	}
	if err := validateIngestString(edge.EndObjectID); err != nil {
		return spooledIngestEdge{}, 0, fmt.Errorf("end object ID is invalid: %w", err)
	}
	if edge.Kind == nil {
		return spooledIngestEdge{}, 0, fmt.Errorf("kind is nil")
	}
	kindName := edge.Kind.String()
	if kindName == "" {
		return spooledIngestEdge{}, 0, fmt.Errorf("kind is empty")
	}
	if err := validateIngestString(kindName); err != nil {
		return spooledIngestEdge{}, 0, fmt.Errorf("kind is invalid: %w", err)
	}

	properties, err := normalizeIngestProperties(edge.Properties)
	if err != nil {
		return spooledIngestEdge{}, 0, err
	}
	identityHash := hashIngestEdgeIdentity(edge.StartObjectID, kindName, edge.EndObjectID)
	return spooledIngestEdge{
		StartObjectID: edge.StartObjectID,
		EndObjectID:   edge.EndObjectID,
		Kind:          kindName,
		IDHash:        int32(identityHash),
		Properties:    properties,
	}, s.buckets.Bucket(identityHash), nil
}

func coalesceEdgeIngestBucket(spool *ingestSpool, bucket uint64) ([]coalescedIngestEdge, error) {
	if spool == nil {
		return nil, fmt.Errorf("edge spool is not configured")
	}

	buckets, err := newIngestBucketSet(spool.bucketCount)
	if err != nil {
		return nil, err
	}
	coalescer := newIngestEdgeCoalescer()
	recordIndex := 0
	if err := spool.Read(bucket, func(payload []byte) error {
		recordIndex++
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()

		var record spooledIngestEdge
		if err := decoder.Decode(&record); err != nil {
			return newEdgeIngestError(
				err,
				"edge ingest bucket %d spool record %d cannot be decoded",
				bucket,
				recordIndex,
			)
		}
		normalized, err := validateSpooledIngestEdge(record, buckets, &bucket)
		if err != nil {
			return newEdgeIngestError(
				err,
				"edge ingest bucket %d spool record %d is invalid",
				bucket,
				recordIndex,
			)
		}
		coalescer.Add(normalized)
		return nil
	}); err != nil {
		return nil, err
	}

	return coalescer.Finish()
}

func coalesceIngestEdges(records []spooledIngestEdge) ([]coalescedIngestEdge, error) {
	coalescer := newIngestEdgeCoalescer()
	for recordIndex, record := range records {
		normalized, err := validateSpooledIngestEdge(record, ingestBucketSet{}, nil)
		if err != nil {
			return nil, newEdgeIngestError(
				err,
				"edge ingest spool record %d is invalid",
				recordIndex+1,
			)
		}
		coalescer.Add(normalized)
	}

	return coalescer.Finish()
}

func validateSpooledIngestEdge(
	record spooledIngestEdge,
	buckets ingestBucketSet,
	expectedBucket *uint64,
) (spooledIngestEdge, error) {
	if record.StartObjectID == "" {
		return spooledIngestEdge{}, fmt.Errorf("start identity is empty")
	}
	if err := validateIngestString(record.StartObjectID); err != nil {
		return spooledIngestEdge{}, fmt.Errorf("start identity is invalid: %w", err)
	}
	if record.EndObjectID == "" {
		return spooledIngestEdge{}, fmt.Errorf("end identity is empty")
	}
	if err := validateIngestString(record.EndObjectID); err != nil {
		return spooledIngestEdge{}, fmt.Errorf("end identity is invalid: %w", err)
	}
	if record.Kind == "" {
		return spooledIngestEdge{}, fmt.Errorf("kind is empty")
	}
	if err := validateIngestString(record.Kind); err != nil {
		return spooledIngestEdge{}, fmt.Errorf("kind is invalid: %w", err)
	}
	if record.Properties == nil {
		return spooledIngestEdge{}, fmt.Errorf("properties are not an object")
	}
	properties, err := normalizeIngestProperties(&graph.Properties{Map: record.Properties})
	if err != nil {
		return spooledIngestEdge{}, err
	}

	identityHash := hashIngestEdgeIdentity(record.StartObjectID, record.Kind, record.EndObjectID)
	if record.IDHash != int32(identityHash) {
		return spooledIngestEdge{}, fmt.Errorf("identity hash is inconsistent")
	}
	if expectedBucket != nil && buckets.Bucket(identityHash) != *expectedBucket {
		return spooledIngestEdge{}, fmt.Errorf("identity hash belongs to a different bucket")
	}

	return spooledIngestEdge{
		StartObjectID: record.StartObjectID,
		EndObjectID:   record.EndObjectID,
		Kind:          record.Kind,
		IDHash:        record.IDHash,
		Properties:    properties,
	}, nil
}

type ingestEdgeCoalescer struct {
	edges      []coalescedIngestEdge
	indexByKey map[edgeIngestSourceKey]int
}

func newIngestEdgeCoalescer() *ingestEdgeCoalescer {
	return &ingestEdgeCoalescer{indexByKey: make(map[edgeIngestSourceKey]int)}
}

func (s *ingestEdgeCoalescer) Add(record spooledIngestEdge) {
	key := edgeIngestSourceKey{
		StartObjectID: record.StartObjectID,
		Kind:          record.Kind,
		EndObjectID:   record.EndObjectID,
	}
	if coalescedIndex, found := s.indexByKey[key]; found {
		for propertyName, value := range record.Properties {
			s.edges[coalescedIndex].Properties[propertyName] = value
		}
		return
	}

	s.indexByKey[key] = len(s.edges)
	s.edges = append(s.edges, coalescedIngestEdge{
		StartObjectID: record.StartObjectID,
		EndObjectID:   record.EndObjectID,
		Kind:          record.Kind,
		IDHash:        record.IDHash,
		Properties:    record.Properties,
	})
}

func (s *ingestEdgeCoalescer) Finish() ([]coalescedIngestEdge, error) {
	for index := range s.edges {
		contentHash, err := hashIngestEdgeContent(s.edges[index].Properties)
		if err != nil {
			return nil, fmt.Errorf("hash coalesced edge ingest record %d: %w", index+1, err)
		}
		s.edges[index].ContentHash = append([]byte(nil), contentHash[:]...)
	}

	return s.edges, nil
}

func compareEdgeHashes(
	incoming []coalescedIngestEdge,
	kindIDsByName map[string]int16,
	stored []storedEdgeHash,
) ([]edgeIngestMutation, IngestPhaseStats, error) {
	stats := IngestPhaseStats{IdentityRowsRead: int64(len(stored))}
	storedByKey := make(map[edgeIngestStoredKey][]byte, len(stored))

	for rowIndex, row := range stored {
		hashed := row.ContentHash != nil
		validSource := row.StartObjectID.Valid && row.StartObjectID.String != "" &&
			row.EndObjectID.Valid && row.EndObjectID.String != ""
		if hashed && !validSource {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest stored identity row %d has a null or empty source identity with a hash",
				rowIndex+1,
			)
		}
		if hashed && len(row.ContentHash) != ingestContentHashLength {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest stored identity row %d has malformed non-null content hash",
				rowIndex+1,
			)
		}
		if !validSource {
			continue
		}

		key := edgeIngestStoredKey{
			StartObjectID: row.StartObjectID.String,
			KindID:        row.KindID,
			EndObjectID:   row.EndObjectID.String,
		}
		if _, duplicate := storedByKey[key]; duplicate {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest stored identity row %d duplicates an exact stored tuple",
				rowIndex+1,
			)
		}
		storedByKey[key] = row.ContentHash
	}

	if len(incoming) > 0 && kindIDsByName == nil {
		return nil, IngestPhaseStats{}, fmt.Errorf("edge ingest kind ID snapshot is not configured")
	}
	mutations := make([]edgeIngestMutation, 0, len(incoming))
	incomingKeys := make(map[edgeIngestStoredKey]struct{}, len(incoming))
	for incomingIndex, edge := range incoming {
		if edge.StartObjectID == "" || edge.EndObjectID == "" || edge.Kind == "" {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest coalesced record %d has an empty source identity",
				incomingIndex+1,
			)
		}
		if len(edge.ContentHash) != ingestContentHashLength {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest coalesced record %d has malformed content hash",
				incomingIndex+1,
			)
		}
		kindID, found := kindIDsByName[edge.Kind]
		if !found {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest coalesced record %d has an unmapped kind",
				incomingIndex+1,
			)
		}
		key := edgeIngestStoredKey{
			StartObjectID: edge.StartObjectID,
			KindID:        kindID,
			EndObjectID:   edge.EndObjectID,
		}
		if _, duplicate := incomingKeys[key]; duplicate {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"edge ingest coalesced record %d duplicates an asserted exact tuple",
				incomingIndex+1,
			)
		}
		incomingKeys[key] = struct{}{}

		storedHash, found := storedByKey[key]
		mutation := edgeIngestMutation{Edge: edge, KindID: kindID}
		if !found {
			mutation.Insert = true
			mutations = append(mutations, mutation)
			stats.StagedInserts++
		} else if storedHash == nil || !bytes.Equal(storedHash, edge.ContentHash) {
			mutations = append(mutations, mutation)
			stats.StagedUpdates++
		} else {
			stats.HashMatches++
		}
	}

	return mutations, stats, nil
}

type ingestRowsQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func resolveIngestEndpoints(
	ctx context.Context,
	querier ingestRowsQuerier,
	graphTarget model.Graph,
	bucket uint64,
	mutations []edgeIngestMutation,
) (map[string]graph.ID, error) {
	if err := ctx.Err(); err != nil {
		return nil, newEdgeIngestError(err, "edge ingest bucket %d endpoint resolution canceled", bucket)
	}
	if len(mutations) == 0 {
		return map[string]graph.ID{}, nil
	}

	objectIDs := make([]string, 0, len(mutations)*2)
	identityHashes := make([]int32, 0, len(mutations)*2)
	requested := make(map[string]struct{}, len(mutations)*2)
	for mutationIndex, mutation := range mutations {
		for _, objectID := range []string{mutation.Edge.StartObjectID, mutation.Edge.EndObjectID} {
			if objectID == "" {
				return nil, newEdgeIngestError(
					nil,
					"edge ingest bucket %d endpoint resolution found an empty source identity in mutation %d",
					bucket,
					mutationIndex+1,
				)
			}
			if _, seen := requested[objectID]; seen {
				continue
			}
			requested[objectID] = struct{}{}
			objectIDs = append(objectIDs, objectID)
			identityHashes = append(identityHashes, int32(hashIngestNodeIdentity(objectID)))
		}
	}

	rows, err := querier.Query(
		ctx,
		pgquery.FormatResolveIngestEndpoints(graphTarget),
		identityHashes,
		objectIDs,
	)
	if err != nil {
		return nil, newEdgeIngestError(
			err,
			"edge ingest bucket %d failed to query %d unique endpoint identities",
			bucket,
			len(objectIDs),
		)
	}
	defer rows.Close()

	counts := make(map[string]int, len(objectIDs))
	resolved := make(map[string]graph.ID, len(objectIDs))
	invalidRows := 0
	unexpectedRows := 0
	for rows.Next() {
		var (
			objectID   pgtype.Text
			databaseID pgtype.Int8
		)
		if err := rows.Scan(&objectID, &databaseID); err != nil {
			return nil, newEdgeIngestError(
				err,
				"edge ingest bucket %d failed to scan endpoint resolution row",
				bucket,
			)
		}
		if !objectID.Valid {
			unexpectedRows++
			continue
		}
		if _, expected := requested[objectID.String]; !expected {
			unexpectedRows++
			continue
		}
		counts[objectID.String]++
		if !databaseID.Valid || databaseID.Int64 < 1 {
			invalidRows++
			continue
		}
		if counts[objectID.String] == 1 {
			resolved[objectID.String] = graph.ID(databaseID.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, newEdgeIngestError(
			err,
			"edge ingest bucket %d endpoint resolution rows failed",
			bucket,
		)
	}

	missing := 0
	ambiguous := 0
	for _, objectID := range objectIDs {
		switch count := counts[objectID]; {
		case count == 0:
			missing++
		case count > 1:
			ambiguous++
		}
	}
	if missing != 0 || ambiguous != 0 || invalidRows != 0 || unexpectedRows != 0 {
		return nil, newEdgeIngestError(
			nil,
			"edge ingest bucket %d endpoint resolution for %d unique identities found %d missing, %d ambiguous, %d invalid-ID, and %d unexpected rows",
			bucket,
			len(objectIDs),
			missing,
			ambiguous,
			invalidRows,
			unexpectedRows,
		)
	}

	return resolved, nil
}

func edgeIngestRows(
	ctx context.Context,
	mutations []edgeIngestMutation,
	resolved map[string]graph.ID,
) ([][]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := make([][]any, 0, len(mutations))
	for rowIndex, mutation := range mutations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		startID, startFound := resolved[mutation.Edge.StartObjectID]
		endID, endFound := resolved[mutation.Edge.EndObjectID]
		if !startFound || !endFound {
			return nil, fmt.Errorf("edge ingest row %d is missing %d resolved endpoints", rowIndex+1, 2-countBools(startFound, endFound))
		}
		propertiesJSON, err := json.Marshal(mutation.Edge.Properties)
		if err != nil {
			return nil, fmt.Errorf("marshal edge ingest row %d properties: %w", rowIndex+1, err)
		}
		rows = append(rows, []any{
			startID,
			endID,
			mutation.Edge.StartObjectID,
			mutation.Edge.EndObjectID,
			mutation.KindID,
			mutation.Edge.IDHash,
			propertiesJSON,
		})
	}

	return rows, nil
}

func countBools(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func (s *ingestEngine) processEdgeBuckets(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return newEdgeIngestError(err, "edge ingest canceled before bucket processing")
	}
	if s.edgeSpool == nil {
		return newEdgeIngestError(nil, "edge ingest cannot process buckets: edge spool is not configured")
	}
	if s.edgeSpool.PopulatedBucketCount() == 0 {
		return nil
	}
	if err := s.assertEdgeKinds(ctx); err != nil {
		return newEdgeIngestError(
			err,
			"edge ingest failed to assert %d unique kinds before bucket processing",
			len(s.edgeKinds),
		)
	}

	for _, bucket := range s.edgeSpool.PopulatedBuckets() {
		if err := ctx.Err(); err != nil {
			return newEdgeIngestError(err, "edge ingest canceled before bucket %d", bucket)
		}
		edges, err := coalesceEdgeIngestBucket(s.edgeSpool, bucket)
		if err != nil {
			return newEdgeIngestError(
				err,
				"edge ingest bucket %d failed while coalescing after %d phase input records",
				bucket,
				s.stats.Edges.InputRecords,
			)
		}
		bucketStats, err := s.processEdgeBucket(ctx, bucket, edges)
		if err != nil {
			return err
		}
		addIngestPhaseStats(&s.stats.Edges, bucketStats)
	}

	return nil
}

func (s *ingestEngine) assertEdgeKinds(ctx context.Context) error {
	if s.edgeKindIDs != nil {
		return nil
	}
	if s.kindMapper == nil {
		return fmt.Errorf("edge ingest kind mapper is not configured")
	}
	if len(s.edgeKinds) == 0 {
		return fmt.Errorf("edge ingest has populated buckets but no encountered kinds")
	}

	kindNames := make([]string, 0, len(s.edgeKinds))
	for kindName := range s.edgeKinds {
		kindNames = append(kindNames, kindName)
	}
	sort.Strings(kindNames)
	kindIDs, err := s.kindMapper.AssertKinds(ctx, graph.StringsToKinds(kindNames))
	if err != nil {
		return fmt.Errorf("assert edge ingest kinds: %w", err)
	}
	if len(kindIDs) != len(kindNames) {
		return fmt.Errorf(
			"assert edge ingest kinds returned %d IDs for %d unique kinds",
			len(kindIDs),
			len(kindNames),
		)
	}

	s.edgeKindIDs = make(map[string]int16, len(kindNames))
	for index, kindName := range kindNames {
		s.edgeKindIDs[kindName] = kindIDs[index]
	}
	return nil
}

func (s *ingestEngine) processEdgeBucket(
	ctx context.Context,
	bucket uint64,
	edges []coalescedIngestEdge,
) (bucketStats IngestPhaseStats, resultErr error) {
	if s.db == nil {
		return IngestPhaseStats{}, newEdgeIngestError(
			nil,
			"edge ingest bucket %d cannot begin transaction for %d coalesced records: database is not configured",
			bucket,
			len(edges),
		)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return IngestPhaseStats{}, newEdgeIngestError(
			err,
			"edge ingest bucket %d failed to begin transaction for %d coalesced records",
			bucket,
			len(edges),
		)
	}
	commitAttempted := false
	defer func() {
		rollbackCtx, cancelRollback := newIngestRollbackCleanupContext()
		defer cancelRollback()

		rollbackErr := tx.Rollback(rollbackCtx)
		if rollbackErr == nil || commitAttempted && errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return
		}
		resultErr = errors.Join(
			resultErr,
			newEdgeIngestError(
				rollbackErr,
				"edge ingest bucket %d failed to roll back transaction for %d coalesced records",
				bucket,
				len(edges),
			),
		)
	}()
	if err := ctx.Err(); err != nil {
		return IngestPhaseStats{}, newEdgeIngestError(
			err,
			"edge ingest bucket %d canceled after beginning transaction for %d coalesced records",
			bucket,
			len(edges),
		)
	}

	stored, err := loadStoredEdgeHashes(ctx, tx, s.graphTarget, s.buckets.Range(bucket))
	if err != nil {
		return IngestPhaseStats{}, newEdgeIngestError(
			err,
			"edge ingest bucket %d failed to load stored hashes for %d coalesced records",
			bucket,
			len(edges),
		)
	}
	mutations, bucketStats, err := compareEdgeHashes(edges, s.edgeKindIDs, stored)
	if err != nil {
		return IngestPhaseStats{}, newEdgeIngestError(
			err,
			"edge ingest bucket %d failed to compare %d coalesced records against %d stored identity rows",
			bucket,
			len(edges),
			len(stored),
		)
	}
	bucketStats.CoalescedRecords = int64(len(edges))

	if len(mutations) > 0 {
		if _, err := tx.Exec(ctx, pgquery.FormatCreateEdgeIngestStagingTable()); err != nil {
			return IngestPhaseStats{}, newEdgeIngestError(
				err,
				"edge ingest bucket %d failed to create staging for %d mutations",
				bucket,
				len(mutations),
			)
		}
		resolved, err := resolveIngestEndpoints(ctx, tx, s.graphTarget, bucket, mutations)
		if err != nil {
			return IngestPhaseStats{}, err
		}
		rows, err := edgeIngestRows(ctx, mutations, resolved)
		if err != nil {
			return IngestPhaseStats{}, newEdgeIngestError(
				err,
				"edge ingest bucket %d failed to prepare %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		copied, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{pgquery.EdgeIngestStagingTable},
			pgquery.EdgeIngestStagingColumns,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return IngestPhaseStats{}, newEdgeIngestError(
				err,
				"edge ingest bucket %d failed to copy %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		if copied != int64(len(mutations)) {
			return IngestPhaseStats{}, newEdgeIngestError(
				nil,
				"edge ingest bucket %d copied %d of %d staged mutations",
				bucket,
				copied,
				len(mutations),
			)
		}

		var sourceConflicts int64
		if err := tx.QueryRow(
			ctx,
			pgquery.FormatValidateIngestEdgeSources(s.graphTarget),
			s.graphTarget.ID,
		).Scan(&sourceConflicts); err != nil {
			return IngestPhaseStats{}, newEdgeIngestError(
				err,
				"edge ingest bucket %d failed source identity preflight for %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		if sourceConflicts != 0 {
			return IngestPhaseStats{}, newEdgeIngestError(
				nil,
				"edge ingest bucket %d source identity preflight found %d source identity conflicts for %d staged mutations",
				bucket,
				sourceConflicts,
				len(mutations),
			)
		}

		commandTag, err := tx.Exec(ctx, pgquery.FormatUpsertIngestEdges(s.graphTarget), s.graphTarget.ID)
		if err != nil {
			return IngestPhaseStats{}, newEdgeIngestError(
				err,
				"edge ingest bucket %d failed to upsert %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		affected := commandTag.RowsAffected()
		if affected != int64(len(mutations)) {
			return IngestPhaseStats{}, newEdgeIngestError(
				nil,
				"edge ingest bucket %d upsert affected %d of %d staged mutations",
				bucket,
				affected,
				len(mutations),
			)
		}
	}

	commitAttempted = true
	if err := tx.Commit(ctx); err != nil {
		return IngestPhaseStats{}, newEdgeIngestError(
			err,
			"edge ingest bucket %d failed to commit %d staged mutations",
			bucket,
			len(mutations),
		)
	}
	bucketStats.CommittedMutations = int64(len(mutations))

	return bucketStats, nil
}

func loadStoredEdgeHashes(
	ctx context.Context,
	querier ingestRowsQuerier,
	graphTarget model.Graph,
	bucketRange ingestBucketRange,
) ([]storedEdgeHash, error) {
	finalRange := bucketRange.Upper == nil
	statement := pgquery.FormatSelectIngestEdgeHashes(graphTarget, finalRange)
	arguments := []any{bucketRange.Lower}
	if bucketRange.Upper != nil {
		arguments = append(arguments, *bucketRange.Upper)
	}

	rows, err := querier.Query(ctx, statement, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stored []storedEdgeHash
	for rows.Next() {
		var (
			startObjectID pgtype.Text
			kindID        int16
			endObjectID   pgtype.Text
			contentHash   []byte
		)
		if err := rows.Scan(&startObjectID, &kindID, &endObjectID, &contentHash); err != nil {
			return nil, err
		}
		row := storedEdgeHash{
			StartObjectID: startObjectID,
			KindID:        kindID,
			EndObjectID:   endObjectID,
		}
		if contentHash != nil {
			row.ContentHash = make([]byte, len(contentHash))
			copy(row.ContentHash, contentHash)
		}
		stored = append(stored, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return stored, nil
}

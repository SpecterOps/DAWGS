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

const ingestContentHashLength = 16

type spooledIngestNode struct {
	ObjectID   string
	IDHash     int32
	Kinds      []string
	Properties map[string]any
}

type coalescedIngestNode struct {
	ObjectID    string
	IDHash      int32
	Kinds       []string
	Properties  map[string]any
	ContentHash []byte
}

type storedNodeHash struct {
	ObjectID    string
	ContentHash []byte
}

type nodeIngestMutation struct {
	Node   coalescedIngestNode
	Insert bool
}

type nodeIngestError struct {
	message string
	cause   error
}

func (s nodeIngestError) Error() string {
	return s.message
}

func (s nodeIngestError) Unwrap() error {
	return s.cause
}

func newNodeIngestError(cause error, format string, arguments ...any) error {
	return nodeIngestError{
		message: fmt.Sprintf(format, arguments...),
		cause:   cause,
	}
}

func (s *ingestEngine) spoolNodes(ctx context.Context, nodes iter.Seq2[*IngestNode, error]) error {
	if err := ctx.Err(); err != nil {
		return newNodeIngestError(err, "node ingest canceled before input record 1")
	}
	if nodes == nil {
		return nil
	}
	if s.nodeSpool == nil {
		return newNodeIngestError(nil, "node ingest cannot spool input record 1: node spool is not configured")
	}

	var recordCount int64
	for node, iteratorErr := range nodes {
		recordCount++
		if err := ctx.Err(); err != nil {
			return newNodeIngestError(err, "node ingest canceled at input record %d", recordCount)
		}
		if iteratorErr != nil {
			return newNodeIngestError(iteratorErr, "node ingest iterator failed at input record %d", recordCount)
		}

		spooled, bucket, err := s.normalizeNodeForSpool(node)
		if err != nil {
			return newNodeIngestError(err, "node ingest validation failed at input record %d", recordCount)
		}
		if err := s.nodeSpool.Append(bucket, spooled); err != nil {
			return newNodeIngestError(
				err,
				"node ingest spool failed at input record %d in bucket %d",
				recordCount,
				bucket,
			)
		}

		s.stats.Nodes.InputRecords++
		s.stats.Nodes.PopulatedBuckets = int64(s.nodeSpool.PopulatedBucketCount())
		s.stats.Nodes.SpoolBytes = s.nodeSpool.BytesWritten()
		if s.nodeKinds == nil {
			s.nodeKinds = make(map[string]struct{})
		}
		for _, kind := range spooled.Kinds {
			s.nodeKinds[kind] = struct{}{}
		}
	}
	if err := ctx.Err(); err != nil {
		return newNodeIngestError(err, "node ingest canceled after input record %d", recordCount)
	}

	return nil
}

func (s *ingestEngine) normalizeNodeForSpool(node *IngestNode) (spooledIngestNode, uint64, error) {
	if node == nil {
		return spooledIngestNode{}, 0, fmt.Errorf("record is nil")
	}
	if node.ObjectID == "" {
		return spooledIngestNode{}, 0, fmt.Errorf("object ID is empty")
	}
	if err := validateIngestString(node.ObjectID); err != nil {
		return spooledIngestNode{}, 0, fmt.Errorf("object ID is invalid: %w", err)
	}
	if len(node.Kinds) == 0 {
		return spooledIngestNode{}, 0, fmt.Errorf("kinds are empty")
	}

	kindNames := make([]string, len(node.Kinds))
	for index, kind := range node.Kinds {
		if kind == nil {
			return spooledIngestNode{}, 0, fmt.Errorf("kind %d is nil", index)
		}
		kindName := kind.String()
		if kindName == "" {
			return spooledIngestNode{}, 0, fmt.Errorf("kind %d is empty", index)
		}
		if err := validateIngestString(kindName); err != nil {
			return spooledIngestNode{}, 0, fmt.Errorf("kind %d is invalid: %w", index, err)
		}
		kindNames[index] = kindName
	}

	properties, err := normalizeIngestProperties(node.Properties)
	if err != nil {
		return spooledIngestNode{}, 0, err
	}
	if propertyObjectID, hasObjectID := properties["objectid"]; hasObjectID {
		typedObjectID, typeOK := propertyObjectID.(string)
		if !typeOK {
			return spooledIngestNode{}, 0, fmt.Errorf("objectid property is not a string")
		}
		if typedObjectID != node.ObjectID {
			return spooledIngestNode{}, 0, fmt.Errorf("objectid property does not match the explicit object ID")
		}
	} else {
		properties["objectid"] = node.ObjectID
	}

	identityHash := hashIngestNodeIdentity(node.ObjectID)
	return spooledIngestNode{
		ObjectID:   node.ObjectID,
		IDHash:     int32(identityHash),
		Kinds:      kindNames,
		Properties: properties,
	}, s.buckets.Bucket(identityHash), nil
}

func coalesceNodeIngestBucket(spool *ingestSpool, bucket uint64) ([]coalescedIngestNode, error) {
	if spool == nil {
		return nil, fmt.Errorf("node spool is not configured")
	}

	buckets, err := newIngestBucketSet(spool.bucketCount)
	if err != nil {
		return nil, err
	}
	coalescer := newIngestNodeCoalescer()
	recordIndex := 0
	if err := spool.Read(bucket, func(payload []byte) error {
		recordIndex++
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()

		var record spooledIngestNode
		if err := decoder.Decode(&record); err != nil {
			return newNodeIngestError(
				err,
				"node ingest bucket %d spool record %d cannot be decoded",
				bucket,
				recordIndex,
			)
		}
		normalized, err := validateSpooledIngestNode(record, buckets, &bucket)
		if err != nil {
			return newNodeIngestError(
				err,
				"node ingest bucket %d spool record %d is invalid",
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

func coalesceIngestNodes(records []spooledIngestNode) ([]coalescedIngestNode, error) {
	coalescer := newIngestNodeCoalescer()
	for recordIndex, record := range records {
		normalized, err := validateSpooledIngestNode(record, ingestBucketSet{}, nil)
		if err != nil {
			return nil, newNodeIngestError(
				err,
				"node ingest spool record %d is invalid",
				recordIndex+1,
			)
		}
		coalescer.Add(normalized)
	}

	return coalescer.Finish()
}

func validateSpooledIngestNode(
	record spooledIngestNode,
	buckets ingestBucketSet,
	expectedBucket *uint64,
) (spooledIngestNode, error) {
	if record.ObjectID == "" {
		return spooledIngestNode{}, fmt.Errorf("identity is empty")
	}
	if err := validateIngestString(record.ObjectID); err != nil {
		return spooledIngestNode{}, fmt.Errorf("identity is invalid: %w", err)
	}
	if len(record.Kinds) == 0 {
		return spooledIngestNode{}, fmt.Errorf("kinds are empty")
	}

	kinds := make([]string, len(record.Kinds))
	for index, kind := range record.Kinds {
		if kind == "" {
			return spooledIngestNode{}, fmt.Errorf("kind %d is empty", index)
		}
		if err := validateIngestString(kind); err != nil {
			return spooledIngestNode{}, fmt.Errorf("kind %d is invalid: %w", index, err)
		}
		kinds[index] = kind
	}

	if record.Properties == nil {
		return spooledIngestNode{}, fmt.Errorf("properties are not an object")
	}
	properties, err := normalizeIngestProperties(&graph.Properties{Map: record.Properties})
	if err != nil {
		return spooledIngestNode{}, err
	}
	propertyObjectID, hasObjectID := properties["objectid"]
	typedObjectID, typeOK := propertyObjectID.(string)
	if !hasObjectID || !typeOK || typedObjectID != record.ObjectID {
		return spooledIngestNode{}, fmt.Errorf("objectid property is missing, non-string, or inconsistent")
	}

	identityHash := hashIngestNodeIdentity(record.ObjectID)
	if record.IDHash != int32(identityHash) {
		return spooledIngestNode{}, fmt.Errorf("identity hash is inconsistent")
	}
	if expectedBucket != nil && buckets.Bucket(identityHash) != *expectedBucket {
		return spooledIngestNode{}, fmt.Errorf("identity hash belongs to a different bucket")
	}

	return spooledIngestNode{
		ObjectID:   record.ObjectID,
		IDHash:     record.IDHash,
		Kinds:      kinds,
		Properties: properties,
	}, nil
}

type ingestNodeCoalescer struct {
	nodes           []coalescedIngestNode
	indexByObjectID map[string]int
	kindSets        []map[string]struct{}
}

func newIngestNodeCoalescer() *ingestNodeCoalescer {
	return &ingestNodeCoalescer{
		indexByObjectID: make(map[string]int),
	}
}

func (s *ingestNodeCoalescer) Add(record spooledIngestNode) {
	if coalescedIndex, found := s.indexByObjectID[record.ObjectID]; found {
		existing := &s.nodes[coalescedIndex]
		for _, kind := range record.Kinds {
			if _, seen := s.kindSets[coalescedIndex][kind]; !seen {
				s.kindSets[coalescedIndex][kind] = struct{}{}
				existing.Kinds = append(existing.Kinds, kind)
			}
		}
		for key, value := range record.Properties {
			existing.Properties[key] = value
		}
		return
	}

	kinds := make([]string, 0, len(record.Kinds))
	kindSet := make(map[string]struct{}, len(record.Kinds))
	for _, kind := range record.Kinds {
		if _, seen := kindSet[kind]; !seen {
			kindSet[kind] = struct{}{}
			kinds = append(kinds, kind)
		}
	}
	s.indexByObjectID[record.ObjectID] = len(s.nodes)
	s.nodes = append(s.nodes, coalescedIngestNode{
		ObjectID:   record.ObjectID,
		IDHash:     record.IDHash,
		Kinds:      kinds,
		Properties: record.Properties,
	})
	s.kindSets = append(s.kindSets, kindSet)
}

func (s *ingestNodeCoalescer) Finish() ([]coalescedIngestNode, error) {
	for index := range s.nodes {
		contentHash, err := hashIngestNodeContent(
			graph.StringsToKinds(s.nodes[index].Kinds),
			s.nodes[index].Properties,
		)
		if err != nil {
			return nil, fmt.Errorf("hash coalesced node ingest record %d: %w", index+1, err)
		}
		s.nodes[index].ContentHash = append([]byte(nil), contentHash[:]...)
	}

	return s.nodes, nil
}

func compareNodeHashes(
	incoming []coalescedIngestNode,
	stored []storedNodeHash,
) ([]nodeIngestMutation, IngestPhaseStats, error) {
	stats := IngestPhaseStats{IdentityRowsRead: int64(len(stored))}
	storedByObjectID := make(map[string][]byte, len(stored))

	for rowIndex, row := range stored {
		if row.ObjectID == "" {
			return nil, IngestPhaseStats{}, fmt.Errorf("node ingest stored identity row %d is empty", rowIndex+1)
		}
		if row.ContentHash != nil && len(row.ContentHash) != ingestContentHashLength {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"node ingest stored identity row %d has malformed non-null content hash",
				rowIndex+1,
			)
		}
		if _, duplicate := storedByObjectID[row.ObjectID]; duplicate {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"node ingest stored identity row %d duplicates an exact stored identity",
				rowIndex+1,
			)
		}
		storedByObjectID[row.ObjectID] = row.ContentHash
	}

	mutations := make([]nodeIngestMutation, 0, len(incoming))
	for incomingIndex, node := range incoming {
		if node.ObjectID == "" {
			return nil, IngestPhaseStats{}, fmt.Errorf("node ingest coalesced record %d has an empty identity", incomingIndex+1)
		}
		if len(node.ContentHash) != ingestContentHashLength {
			return nil, IngestPhaseStats{}, fmt.Errorf(
				"node ingest coalesced record %d has malformed content hash",
				incomingIndex+1,
			)
		}

		storedHash, found := storedByObjectID[node.ObjectID]
		if !found {
			mutations = append(mutations, nodeIngestMutation{Node: node, Insert: true})
			stats.StagedInserts++
		} else if storedHash == nil || !bytes.Equal(storedHash, node.ContentHash) {
			mutations = append(mutations, nodeIngestMutation{Node: node})
			stats.StagedUpdates++
		} else {
			stats.HashMatches++
		}
	}

	return mutations, stats, nil
}

func nodeIngestRows(
	ctx context.Context,
	kindIDsByName map[string]int16,
	mutations []nodeIngestMutation,
) ([][]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(mutations) == 0 {
		return nil, nil
	}
	if kindIDsByName == nil {
		return nil, fmt.Errorf("node ingest kind ID snapshot is not configured")
	}

	rows := make([][]any, 0, len(mutations))
	for rowIndex, mutation := range mutations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		uniqueKindIDs := make(map[int16]struct{}, len(mutation.Node.Kinds))
		for _, kindName := range mutation.Node.Kinds {
			kindID, found := kindIDsByName[kindName]
			if !found {
				return nil, fmt.Errorf("node ingest row %d has an unmapped kind", rowIndex+1)
			}
			uniqueKindIDs[kindID] = struct{}{}
		}
		sortedKindIDs := make([]int16, 0, len(uniqueKindIDs))
		for kindID := range uniqueKindIDs {
			sortedKindIDs = append(sortedKindIDs, kindID)
		}
		sort.Slice(sortedKindIDs, func(left, right int) bool {
			return sortedKindIDs[left] < sortedKindIDs[right]
		})

		propertiesJSON, err := json.Marshal(mutation.Node.Properties)
		if err != nil {
			return nil, fmt.Errorf("marshal node ingest row %d properties: %w", rowIndex+1, err)
		}
		rows = append(rows, []any{
			mutation.Node.ObjectID,
			mutation.Node.IDHash,
			sortedKindIDs,
			propertiesJSON,
		})
	}

	return rows, nil
}

func (s *ingestEngine) processNodeBuckets(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return newNodeIngestError(err, "node ingest canceled before bucket processing")
	}
	if s.nodeSpool == nil {
		return newNodeIngestError(nil, "node ingest cannot process buckets: node spool is not configured")
	}
	if s.nodeSpool.PopulatedBucketCount() == 0 {
		return nil
	}
	if err := s.assertNodeKinds(ctx); err != nil {
		return newNodeIngestError(
			err,
			"node ingest failed to assert %d unique kinds before bucket processing",
			len(s.nodeKinds),
		)
	}

	for _, bucket := range s.nodeSpool.PopulatedBuckets() {
		if err := ctx.Err(); err != nil {
			return newNodeIngestError(err, "node ingest canceled before bucket %d", bucket)
		}

		nodes, err := coalesceNodeIngestBucket(s.nodeSpool, bucket)
		if err != nil {
			return newNodeIngestError(
				err,
				"node ingest bucket %d failed while coalescing %d input records",
				bucket,
				s.stats.Nodes.InputRecords,
			)
		}
		bucketStats, err := s.processNodeBucket(ctx, bucket, nodes)
		if err != nil {
			return err
		}
		addIngestPhaseStats(&s.stats.Nodes, bucketStats)
	}

	return nil
}

func (s *ingestEngine) assertNodeKinds(ctx context.Context) error {
	if s.nodeKindIDs != nil {
		return nil
	}
	if s.kindMapper == nil {
		return fmt.Errorf("node ingest kind mapper is not configured")
	}
	if len(s.nodeKinds) == 0 {
		return fmt.Errorf("node ingest has populated buckets but no encountered kinds")
	}

	kindNames := make([]string, 0, len(s.nodeKinds))
	for kindName := range s.nodeKinds {
		kindNames = append(kindNames, kindName)
	}
	sort.Strings(kindNames)
	kindIDs, err := s.kindMapper.AssertKinds(ctx, graph.StringsToKinds(kindNames))
	if err != nil {
		return fmt.Errorf("assert node ingest kinds: %w", err)
	}
	if len(kindIDs) != len(kindNames) {
		return fmt.Errorf(
			"assert node ingest kinds returned %d IDs for %d unique kinds",
			len(kindIDs),
			len(kindNames),
		)
	}

	s.nodeKindIDs = make(map[string]int16, len(kindNames))
	for index, kindName := range kindNames {
		s.nodeKindIDs[kindName] = kindIDs[index]
	}
	return nil
}

func (s *ingestEngine) processNodeBucket(
	ctx context.Context,
	bucket uint64,
	nodes []coalescedIngestNode,
) (bucketStats IngestPhaseStats, resultErr error) {
	if s.db == nil {
		return IngestPhaseStats{}, newNodeIngestError(
			nil,
			"node ingest bucket %d cannot begin transaction for %d coalesced records: database is not configured",
			bucket,
			len(nodes),
		)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return IngestPhaseStats{}, newNodeIngestError(
			err,
			"node ingest bucket %d failed to begin transaction for %d coalesced records",
			bucket,
			len(nodes),
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
			newNodeIngestError(
				rollbackErr,
				"node ingest bucket %d failed to roll back transaction for %d coalesced records",
				bucket,
				len(nodes),
			),
		)
	}()
	if err := ctx.Err(); err != nil {
		return IngestPhaseStats{}, newNodeIngestError(
			err,
			"node ingest bucket %d canceled after beginning transaction for %d coalesced records",
			bucket,
			len(nodes),
		)
	}

	bucketRange := s.buckets.Range(bucket)
	stored, err := loadStoredNodeHashes(ctx, tx, s.graphTarget, bucketRange)
	if err != nil {
		return IngestPhaseStats{}, newNodeIngestError(
			err,
			"node ingest bucket %d failed to load stored hashes for %d coalesced records",
			bucket,
			len(nodes),
		)
	}

	mutations, bucketStats, err := compareNodeHashes(nodes, stored)
	if err != nil {
		return IngestPhaseStats{}, newNodeIngestError(
			err,
			"node ingest bucket %d failed to compare %d coalesced records against %d stored identity rows",
			bucket,
			len(nodes),
			len(stored),
		)
	}
	bucketStats.CoalescedRecords = int64(len(nodes))

	if len(mutations) > 0 {
		rows, err := nodeIngestRows(ctx, s.nodeKindIDs, mutations)
		if err != nil {
			return IngestPhaseStats{}, newNodeIngestError(
				err,
				"node ingest bucket %d failed to prepare %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		if _, err := tx.Exec(ctx, pgquery.FormatCreateNodeIngestStagingTable()); err != nil {
			return IngestPhaseStats{}, newNodeIngestError(
				err,
				"node ingest bucket %d failed to create staging for %d mutations",
				bucket,
				len(mutations),
			)
		}
		copied, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{pgquery.NodeIngestStagingTable},
			pgquery.NodeIngestStagingColumns,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return IngestPhaseStats{}, newNodeIngestError(
				err,
				"node ingest bucket %d failed to copy %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		if copied != int64(len(mutations)) {
			return IngestPhaseStats{}, newNodeIngestError(
				nil,
				"node ingest bucket %d copied %d of %d staged mutations",
				bucket,
				copied,
				len(mutations),
			)
		}

		commandTag, err := tx.Exec(ctx, pgquery.FormatUpsertIngestNodes(s.graphTarget), s.graphTarget.ID)
		if err != nil {
			return IngestPhaseStats{}, newNodeIngestError(
				err,
				"node ingest bucket %d failed to upsert %d staged mutations",
				bucket,
				len(mutations),
			)
		}
		affected := commandTag.RowsAffected()
		if affected != int64(len(mutations)) {
			return IngestPhaseStats{}, newNodeIngestError(
				nil,
				"node ingest bucket %d upsert affected %d of %d staged mutations",
				bucket,
				affected,
				len(mutations),
			)
		}
	}

	commitAttempted = true
	if err := tx.Commit(ctx); err != nil {
		return IngestPhaseStats{}, newNodeIngestError(
			err,
			"node ingest bucket %d failed to commit %d staged mutations",
			bucket,
			len(mutations),
		)
	}
	bucketStats.CommittedMutations = int64(len(mutations))

	return bucketStats, nil
}

func loadStoredNodeHashes(
	ctx context.Context,
	tx pgx.Tx,
	graphTarget model.Graph,
	bucketRange ingestBucketRange,
) ([]storedNodeHash, error) {
	finalRange := bucketRange.Upper == nil
	statement := pgquery.FormatSelectIngestNodeHashes(graphTarget, finalRange)
	arguments := []any{bucketRange.Lower}
	if bucketRange.Upper != nil {
		arguments = append(arguments, *bucketRange.Upper)
	}

	rows, err := tx.Query(ctx, statement, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stored []storedNodeHash
	for rows.Next() {
		var (
			objectID    pgtype.Text
			contentHash []byte
		)
		if err := rows.Scan(&objectID, &contentHash); err != nil {
			return nil, err
		}

		row := storedNodeHash{}
		if objectID.Valid {
			row.ObjectID = objectID.String
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

func addIngestPhaseStats(target *IngestPhaseStats, delta IngestPhaseStats) {
	target.CoalescedRecords += delta.CoalescedRecords
	target.IdentityRowsRead += delta.IdentityRowsRead
	target.HashMatches += delta.HashMatches
	target.StagedInserts += delta.StagedInserts
	target.StagedUpdates += delta.StagedUpdates
	target.CommittedMutations += delta.CommittedMutations
}

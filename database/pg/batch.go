package pg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/database/pg/model"
	"github.com/specterops/dawgs/database/pg/query"
	"github.com/specterops/dawgs/graph"
)

type Int2ArrayEncoder struct {
	buffer *bytes.Buffer
}

func NewInt2ArrayEncoder() Int2ArrayEncoder {
	return Int2ArrayEncoder{
		buffer: &bytes.Buffer{},
	}
}

func (s *Int2ArrayEncoder) Encode(values []int16) string {
	s.buffer.Reset()
	s.buffer.WriteRune('{')

	for idx, value := range values {
		if idx > 0 {
			s.buffer.WriteRune(',')
		}

		s.buffer.WriteString(strconv.Itoa(int(value)))
	}

	s.buffer.WriteRune('}')
	return s.buffer.String()
}

type NodeUpsertParameters struct {
	IDFutures    []*query.Future[graph.ID]
	KindIDSlices []string
	Properties   []json.RawMessage
}

func NewNodeUpsertParameters(size int) *NodeUpsertParameters {
	return &NodeUpsertParameters{
		IDFutures:    make([]*query.Future[graph.ID], 0, size),
		KindIDSlices: make([]string, 0, size),
		Properties:   make([]json.RawMessage, 0, size),
	}
}

func (s *NodeUpsertParameters) Format(graphTarget model.Graph) []any {
	return []any{
		graphTarget.ID,
		s.KindIDSlices,
		s.Properties,
	}
}

func (s *NodeUpsertParameters) Append(ctx context.Context, update *query.NodeUpdate, schemaManager *SchemaManager, kindIDEncoder Int2ArrayEncoder) error {
	s.IDFutures = append(s.IDFutures, update.IDFuture)

	if mappedKindIDs, err := schemaManager.AssertKinds(ctx, update.Node.Kinds); err != nil {
		return fmt.Errorf("unable to map kinds %w", err)
	} else {
		s.KindIDSlices = append(s.KindIDSlices, kindIDEncoder.Encode(mappedKindIDs))
	}

	if propertiesJSONB, err := pgsql.PropertiesToJSONB(update.Node.Properties); err != nil {
		return err
	} else {
		s.Properties = append(s.Properties, propertiesJSONB)
	}

	return nil
}

func (s *NodeUpsertParameters) AppendAll(ctx context.Context, updates *query.NodeUpdateBatch, schemaManager *SchemaManager, kindIDEncoder Int2ArrayEncoder) error {
	for _, nextUpdate := range updates.Updates {
		if err := s.Append(ctx, nextUpdate, schemaManager, kindIDEncoder); err != nil {
			return err
		}
	}

	return nil
}

type RelationshipUpdateByParameters struct {
	StartIDs   []graph.ID
	EndIDs     []graph.ID
	KindIDs    []int16
	Properties []json.RawMessage
}

func NewRelationshipUpdateByParameters(size int) *RelationshipUpdateByParameters {
	return &RelationshipUpdateByParameters{
		StartIDs:   make([]graph.ID, 0, size),
		EndIDs:     make([]graph.ID, 0, size),
		KindIDs:    make([]int16, 0, size),
		Properties: make([]json.RawMessage, 0, size),
	}
}

func (s *RelationshipUpdateByParameters) Format(graphTarget model.Graph) []any {
	return []any{
		graphTarget.ID,
		s.StartIDs,
		s.EndIDs,
		s.KindIDs,
		s.Properties,
	}
}

func (s *RelationshipUpdateByParameters) Append(ctx context.Context, update *query.RelationshipUpdate, schemaManager *SchemaManager) error {
	s.StartIDs = append(s.StartIDs, update.StartID.Value)
	s.EndIDs = append(s.EndIDs, update.EndID.Value)

	if mappedKindIDs, err := schemaManager.AssertKinds(ctx, []graph.Kind{update.Relationship.Kind}); err != nil {
		return err
	} else {
		s.KindIDs = append(s.KindIDs, mappedKindIDs...)
	}

	if propertiesJSONB, err := pgsql.PropertiesToJSONB(update.Relationship.Properties); err != nil {
		return err
	} else {
		s.Properties = append(s.Properties, propertiesJSONB)
	}
	return nil
}

func (s *RelationshipUpdateByParameters) AppendAll(ctx context.Context, updates *query.RelationshipUpdateBatch, schemaManager *SchemaManager) error {
	for _, nextUpdate := range updates.Updates {
		if err := s.Append(ctx, nextUpdate, schemaManager); err != nil {
			return err
		}
	}

	return nil
}

type relationshipCreateBatch struct {
	startIDs         []uint64
	endIDs           []uint64
	edgeKindIDs      []int16
	edgePropertyBags []json.RawMessage
}

func newRelationshipCreateBatch(size int) *relationshipCreateBatch {
	return &relationshipCreateBatch{
		startIDs:         make([]uint64, 0, size),
		endIDs:           make([]uint64, 0, size),
		edgeKindIDs:      make([]int16, 0, size),
		edgePropertyBags: make([]json.RawMessage, 0, size),
	}
}

func (s *relationshipCreateBatch) Add(startID, endID uint64, edgeKindID int16) {
	s.startIDs = append(s.startIDs, startID)
	s.edgeKindIDs = append(s.edgeKindIDs, edgeKindID)
	s.endIDs = append(s.endIDs, endID)
}

func (s *relationshipCreateBatch) EncodeProperties(edgePropertiesBatch []*graph.Properties) error {
	for _, edgeProperties := range edgePropertiesBatch {
		if propertiesJSONB, err := pgsql.PropertiesToJSONB(edgeProperties); err != nil {
			return err
		} else {
			s.edgePropertyBags = append(s.edgePropertyBags, propertiesJSONB)
		}
	}

	return nil
}

type relationshipCreateBatchBuilder struct {
	keyToEdgeID             map[string]uint64
	relationshipUpdateBatch *relationshipCreateBatch
	edgePropertiesIndex     map[uint64]int
	edgePropertiesBatch     []*graph.Properties
}

func newRelationshipCreateBatchBuilder(size int) *relationshipCreateBatchBuilder {
	return &relationshipCreateBatchBuilder{
		keyToEdgeID:             map[string]uint64{},
		relationshipUpdateBatch: newRelationshipCreateBatch(size),
		edgePropertiesIndex:     map[uint64]int{},
	}
}

func (s *relationshipCreateBatchBuilder) Build() (*relationshipCreateBatch, error) {
	return s.relationshipUpdateBatch, s.relationshipUpdateBatch.EncodeProperties(s.edgePropertiesBatch)
}

func (s *relationshipCreateBatchBuilder) Add(ctx context.Context, kindMapper KindMapper, edge *graph.Relationship) error {
	keyBuilder := strings.Builder{}

	keyBuilder.WriteString(edge.StartID.String())
	keyBuilder.WriteString(edge.EndID.String())
	keyBuilder.WriteString(edge.Kind.String())

	key := keyBuilder.String()

	if existingPropertiesIdx, hasExisting := s.keyToEdgeID[key]; hasExisting {
		s.edgePropertiesBatch[existingPropertiesIdx].Merge(edge.Properties)
	} else {
		var (
			startID        = edge.StartID.Uint64()
			edgeID         = edge.ID.Uint64()
			endID          = edge.EndID.Uint64()
			edgeProperties = edge.Properties.Clone()
		)

		if edgeKindID, err := kindMapper.MapKind(ctx, edge.Kind); err != nil {
			return err
		} else {
			s.relationshipUpdateBatch.Add(startID, endID, edgeKindID)
		}

		s.keyToEdgeID[key] = edgeID

		s.edgePropertiesBatch = append(s.edgePropertiesBatch, edgeProperties)
		s.edgePropertiesIndex[edgeID] = len(s.edgePropertiesBatch) - 1
	}

	return nil
}

func (s *dawgsDriver) updateNodes(ctx context.Context, validatedBatch *query.NodeUpdateBatch) error {
	var (
		parameters    = NewNodeUpsertParameters(len(validatedBatch.Updates))
		kindIDEncoder = NewInt2ArrayEncoder()
	)

	if err := parameters.AppendAll(ctx, validatedBatch, s.schemaManager, kindIDEncoder); err != nil {
		return err
	}

	if graphTarget, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else {
		nodeUpsertQuery := query.FormatNodeUpsert(graphTarget, validatedBatch.IdentityProperties)

		if result, err := s.internalConn.Query(ctx, nodeUpsertQuery, parameters.Format(graphTarget)...); err != nil {
			return err
		} else {
			idFutureIndex := 0

			for result.Next() {
				if err := result.Scan(&parameters.IDFutures[idFutureIndex].Value); err != nil {
					return err
				}

				idFutureIndex++
			}

			result.Close()
			return result.Err()
		}
	}
}

func (s *dawgsDriver) UpdateNodes(ctx context.Context, batch []graph.NodeUpdate) error {
	if validatedBatch, err := query.ValidateNodeUpdateByBatch(batch); err != nil {
		return err
	} else {
		return s.updateNodes(ctx, validatedBatch)
	}
}

func (s *dawgsDriver) UpdateRelationships(ctx context.Context, batch []graph.RelationshipUpdate) error {
	if validatedBatch, err := query.ValidateRelationshipUpdateByBatch(batch); err != nil {
		return err
	} else if err := s.updateNodes(ctx, validatedBatch.NodeUpdates); err != nil {
		return err
	} else {
		parameters := NewRelationshipUpdateByParameters(len(validatedBatch.Updates))

		if err := parameters.AppendAll(ctx, validatedBatch, s.schemaManager); err != nil {
			return err
		}

		if graphTarget, err := s.getTargetGraph(ctx); err != nil {
			return err
		} else {
			relationshipUpsertQuery := query.FormatRelationshipPartitionUpsert(graphTarget, validatedBatch.IdentityProperties)

			if result, err := s.internalConn.Query(ctx, relationshipUpsertQuery, parameters.Format(graphTarget)...); err != nil {
				return err
			} else {
				result.Close()
			}
		}
	}

	return nil
}

func (s *dawgsDriver) CreateNodes(ctx context.Context, batch []*graph.Node) error {
	var (
		numCreates    = len(batch)
		kindIDSlices  = make([]string, numCreates)
		kindIDEncoder = Int2ArrayEncoder{
			buffer: &bytes.Buffer{},
		}
		properties = make([]json.RawMessage, numCreates)
	)

	for idx, nextNode := range batch {
		if mappedKindIDs, err := s.schemaManager.AssertKinds(ctx, nextNode.Kinds); err != nil {
			return fmt.Errorf("unable to map kinds %w", err)
		} else {
			kindIDSlices[idx] = kindIDEncoder.Encode(mappedKindIDs)
		}

		if propertiesJSONB, err := pgsql.PropertiesToJSONB(nextNode.Properties); err != nil {
			return err
		} else {
			properties[idx] = propertiesJSONB
		}
	}

	if graphTarget, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if result, err := s.internalConn.Query(ctx, createNodeWithoutIDBatchStatement, graphTarget.ID, kindIDSlices, properties); err != nil {
		return err
	} else {
		result.Close()
	}

	return nil
}

func (s *dawgsDriver) CreateRelationships(ctx context.Context, batch []*graph.Relationship) error {
	batchBuilder := newRelationshipCreateBatchBuilder(len(batch))

	for _, nextRel := range batch {
		if err := batchBuilder.Add(ctx, s.schemaManager, nextRel); err != nil {
			return err
		}
	}

	if createBatch, err := batchBuilder.Build(); err != nil {
		return err
	} else if graphTarget, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if result, err := s.internalConn.Query(ctx, createEdgeBatchStatement, graphTarget.ID, createBatch.startIDs, createBatch.endIDs, createBatch.edgeKindIDs, createBatch.edgePropertyBags); err != nil {
		return err
	} else {
		result.Close()
	}

	return nil
}

func (s *dawgsDriver) DeleteNodes(ctx context.Context, batch []graph.ID) error {
	if result, err := s.internalConn.Query(ctx, deleteNodeWithIDStatement, batch); err != nil {
		return err
	} else {
		result.Close()
	}

	return nil
}

func (s *dawgsDriver) DeleteRelationships(ctx context.Context, batch []graph.ID) error {
	if result, err := s.internalConn.Query(ctx, deleteEdgeWithIDStatement, batch); err != nil {
		return err
	} else {
		result.Close()
	}

	return nil
}

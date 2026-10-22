package neo4j

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
)

func newUpdateKey(identityKind graph.Kind, identityProperties []string, updateKinds graph.Kinds, removedKinds ...graph.Kind) string {
	kind := ""
	if identityKind != nil && !identityKind.Is(graph.EmptyKind) {
		kind = identityKind.String()
	}
	return encodeUpdateShape([]string{kind}, identityProperties, updateKinds.Strings(), graph.Kinds(removedKinds).Strings())
}

// encodeUpdateShape preserves the role and boundaries of each set. All inputs
// contain only strings, so JSON encoding cannot fail. Caller slices remain owned
// by the caller, including their order.
func encodeUpdateShape(fields ...[]string) string {
	sets := make([][]string, len(fields))
	for idx, field := range fields {
		sets[idx] = append([]string{}, field...)
		slices.Sort(sets[idx])
		sets[idx] = slices.Compact(sets[idx])
	}
	encoded, _ := json.Marshal(sets)
	return string(encoded)
}

func relUpdateKey(update graph.RelationshipUpdate) string {
	keys := []string{
		newUpdateKey(update.StartIdentityKind, update.StartIdentityProperties, update.Start.Kinds),
		newUpdateKey(update.Relationship.Kind, update.IdentityProperties, nil),
		newUpdateKey(update.EndIdentityKind, update.EndIdentityProperties, update.End.Kinds),
	}

	encoded, _ := json.Marshal(keys)
	return string(encoded)
}

type relUpdates struct {
	identityKind            graph.Kind
	identityProperties      []string
	startIdentityKind       graph.Kind
	startIdentityProperties []string
	startNodeKindsToAdd     graph.Kinds
	endIdentityKind         graph.Kind
	endIdentityProperties   []string
	endNodeKindsToAdd       graph.Kinds
	properties              []map[string]any
}

type relUpdateByMap map[string]*relUpdates

func (s relUpdateByMap) add(update graph.RelationshipUpdate) {
	var (
		updateKey        = relUpdateKey(update)
		updateProperties = map[string]any{
			"r": update.Relationship.Properties.Map,
			"s": update.Start.Properties.Map,
			"e": update.End.Properties.Map,
		}
	)

	if updates, hasUpdates := s[updateKey]; hasUpdates {
		updates.properties = append(updates.properties, updateProperties)
	} else {
		s[updateKey] = &relUpdates{
			identityKind:            update.Relationship.Kind,
			identityProperties:      update.IdentityProperties,
			startIdentityKind:       update.StartIdentityKind,
			startIdentityProperties: update.StartIdentityProperties,
			startNodeKindsToAdd:     update.Start.Kinds,
			endIdentityKind:         update.EndIdentityKind,
			endIdentityProperties:   update.EndIdentityProperties,
			endNodeKindsToAdd:       update.End.Kinds,
			properties: []map[string]any{
				updateProperties,
			},
		}
	}
}

func cypherBuildRelationshipUpdateQueryBatch(updates []graph.RelationshipUpdate) ([]string, [][]map[string]any) {
	var (
		queries         []string
		queryParameters [][]map[string]any

		output         = strings.Builder{}
		batchedUpdates = relUpdateByMap{}
	)

	for _, update := range updates {
		batchedUpdates.add(update)
	}

	for _, batch := range batchedUpdates {
		output.WriteString("unwind $p as p merge (s")

		if batch.startIdentityKind != nil && !batch.startIdentityKind.Is(graph.EmptyKind) {
			output.WriteString(fmt.Sprintf(":%s", batch.startIdentityKind.String()))
		}

		if len(batch.startIdentityProperties) > 0 {
			output.WriteString(" {")

			firstIdentityProperty := true
			for _, identityProperty := range batch.startIdentityProperties {
				if firstIdentityProperty {
					firstIdentityProperty = false
				} else {
					output.WriteString(",")
				}

				output.WriteString(identityProperty)
				output.WriteString(":p.s.")
				output.WriteString(identityProperty)
			}

			output.WriteString("}")
		}

		output.WriteString(") merge (e")
		if batch.endIdentityKind != nil && !batch.endIdentityKind.Is(graph.EmptyKind) {
			output.WriteString(fmt.Sprintf(":%s", batch.endIdentityKind.String()))
		}

		if len(batch.endIdentityProperties) > 0 {
			output.WriteString(" {")

			firstIdentityProperty := true
			for _, identityProperty := range batch.endIdentityProperties {
				if firstIdentityProperty {
					firstIdentityProperty = false
				} else {
					output.WriteString(",")
				}

				output.WriteString(identityProperty)
				output.WriteString(":p.e.")
				output.WriteString(identityProperty)
			}

			output.WriteString("}")
		}

		output.WriteString(") merge (s)-[r:")
		output.WriteString(batch.identityKind.String())

		if len(batch.identityProperties) > 0 {
			output.WriteString(" {")

			firstIdentityProperty := true
			for _, identityProperty := range batch.identityProperties {
				if firstIdentityProperty {
					firstIdentityProperty = false
				} else {
					output.WriteString(",")
				}

				output.WriteString(identityProperty)
				output.WriteString(":p.r.")
				output.WriteString(identityProperty)
			}

			output.WriteString("}")
		}

		output.WriteString("]->(e) set s += p.s, e += p.e, r += p.r")

		if len(batch.startNodeKindsToAdd) > 0 {
			for _, kindToAdd := range batch.startNodeKindsToAdd {
				if kindToAdd == graph.EmptyKind {
					continue // skip empty kinds
				}
				output.WriteString(", s:")
				output.WriteString(kindToAdd.String())
			}
		}

		if len(batch.endNodeKindsToAdd) > 0 {
			for _, kindToAdd := range batch.endNodeKindsToAdd {
				if kindToAdd == graph.EmptyKind {
					continue // skip empty kinds
				}
				output.WriteString(", e:")
				output.WriteString(kindToAdd.String())
			}
		}

		output.WriteString(", s.lastseen = datetime({timezone: 'UTC'}), e.lastseen = datetime({timezone: 'UTC'});")

		// Write out the query to be run
		queries = append(queries, output.String())
		queryParameters = append(queryParameters, batch.properties)

		output.Reset()
	}

	return queries, queryParameters
}

type nodeUpdates struct {
	identityKind       graph.Kind
	identityProperties []string
	nodeKindsToAdd     graph.Kinds
	nodeKindsToRemove  graph.Kinds
	properties         []map[string]any
}

type nodeUpdateByMap map[string]*nodeUpdates

func (s nodeUpdateByMap) add(update graph.NodeUpdate) {
	updateKey := newUpdateKey(update.IdentityKind, update.IdentityProperties, update.Node.Kinds, update.Node.DeletedKinds...)

	if updates, hasUpdates := s[updateKey]; hasUpdates {
		updates.properties = append(updates.properties, update.Node.Properties.Map)
	} else {
		s[updateKey] = &nodeUpdates{
			identityKind:       update.IdentityKind,
			identityProperties: update.IdentityProperties,
			nodeKindsToAdd:     update.Node.Kinds,
			nodeKindsToRemove:  update.Node.DeletedKinds,
			properties: []map[string]any{
				update.Node.Properties.Map,
			},
		}
	}
}

func nodeToNodeUpdateKey(node *graph.Node) string {
	return encodeUpdateShape(node.AddedKinds.Strings(), node.DeletedKinds.Strings())
}

type nodeUpdateBatch struct {
	nodeKindsToAdd    graph.Kinds
	nodeKindsToRemove graph.Kinds
	Parameters        []map[string]any
}

func nodeUpdateParameters(node *graph.Node) map[string]any {
	properties := node.Properties.Map
	if len(node.Properties.Deleted) > 0 {
		// Neo4j's map merge removes properties whose supplied value is null.
		// Keep deletion markers in this row's parameters without changing the
		// caller's property map or adding property names to generated Cypher.
		properties = make(map[string]any, len(node.Properties.Map)+len(node.Properties.Deleted))
		maps.Copy(properties, node.Properties.Map)
		for key := range node.Properties.Deleted {
			properties[key] = nil
		}
	}
	return map[string]any{"id": node.ID, "properties": properties}
}

func cypherBuildNodeUpdateQueryBatch(updates []*graph.Node) ([]string, []map[string]any) {
	var (
		queries         []string
		queryParameters []map[string]any

		output         = strings.Builder{}
		batchedUpdates = map[string]*nodeUpdateBatch{}
	)

	for _, nodeToUpdate := range updates {
		updateKey := nodeToNodeUpdateKey(nodeToUpdate)

		if existingBatch, hasBatch := batchedUpdates[updateKey]; hasBatch {
			existingBatch.Parameters = append(existingBatch.Parameters, nodeUpdateParameters(nodeToUpdate))
		} else {
			batchedUpdates[updateKey] = &nodeUpdateBatch{
				nodeKindsToAdd:    nodeToUpdate.AddedKinds,
				nodeKindsToRemove: nodeToUpdate.DeletedKinds,
				Parameters:        []map[string]any{nodeUpdateParameters(nodeToUpdate)},
			}
		}
	}

	for _, batch := range batchedUpdates {
		output.WriteString("unwind $p as p match (n) where id(n) = p.id set n += p.properties")

		if len(batch.nodeKindsToAdd) > 0 {
			for _, kindToAdd := range batch.nodeKindsToAdd {
				output.WriteString(", n:")
				output.WriteString(kindToAdd.String())
			}
		}

		if len(batch.nodeKindsToRemove) > 0 {
			output.WriteString(" remove ")

			for idx, kindToRemove := range batch.nodeKindsToRemove {
				if idx > 0 {
					output.WriteString(",")
				}

				output.WriteString("n:")
				output.WriteString(kindToRemove.String())
			}
		}

		output.WriteString(";")

		// Write out the query to be run
		queries = append(queries, output.String())
		queryParameters = append(queryParameters, map[string]any{
			"p": batch.Parameters,
		})

		output.Reset()
	}

	return queries, queryParameters
}

func cypherBuildNodeUpdateQueryByBatch(updates []graph.NodeUpdate) ([]string, []map[string]any) {
	var (
		queries         []string
		queryParameters []map[string]any

		output         = strings.Builder{}
		batchedUpdates = nodeUpdateByMap{}
	)

	for _, update := range updates {
		batchedUpdates.add(update)
	}

	for _, batch := range batchedUpdates {
		output.WriteString("unwind $p as p merge (n")

		if batch.identityKind != nil && !batch.identityKind.Is(graph.EmptyKind) {
			output.WriteString(fmt.Sprintf(":%s", batch.identityKind.String()))
		}

		if len(batch.identityProperties) > 0 {
			output.WriteString(" {")

			firstIdentityProperty := true
			for _, identityProperty := range batch.identityProperties {
				if firstIdentityProperty {
					firstIdentityProperty = false
				} else {
					output.WriteString(",")
				}

				output.WriteString(identityProperty)
				output.WriteString(":p.")
				output.WriteString(identityProperty)
			}

			output.WriteString("}")
		}

		output.WriteString(") set n += p")

		if len(batch.nodeKindsToAdd) > 0 {
			for _, kindToAdd := range batch.nodeKindsToAdd {
				output.WriteString(", n:")
				output.WriteString(kindToAdd.String())
			}
		}

		if len(batch.nodeKindsToRemove) > 0 {
			output.WriteString(" remove ")

			for idx, kindToRemove := range batch.nodeKindsToRemove {
				if idx > 0 {
					output.WriteString(",")
				}

				output.WriteString("n:")
				output.WriteString(kindToRemove.String())
			}
		}

		output.WriteString(";")

		// Write out the query to be run
		queries = append(queries, output.String())
		queryParameters = append(queryParameters, map[string]any{
			"p": batch.properties,
		})

		output.Reset()
	}

	return queries, queryParameters
}

func stripCypherQuery(rawQuery string) string {
	var (
		strippedEmitter = format.NewCypherEmitter(true)
		buffer          = &bytes.Buffer{}
	)

	if queryModel, err := frontend.ParseCypher(frontend.DefaultCypherContext(), rawQuery); err != nil {
		slog.Error(fmt.Sprintf("Error occurred parsing cypher query during sanitization: %v", err))
	} else if err = strippedEmitter.Write(queryModel, buffer); err != nil {
		slog.Error(fmt.Sprintf("Error occurred sanitizing cypher query: %v", err))
	}

	return buffer.String()
}

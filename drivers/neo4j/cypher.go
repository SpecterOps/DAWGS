package neo4j

import (
	"bytes"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
)

func newUpdateKey(identityKind graph.Kind, identityProperties []string, updateKinds graph.Kinds) string {
	var keys []string

	// Defensive check: identityKind may be nil or zero value
	if identityKind != nil && !identityKind.Is(graph.EmptyKind) {
		keys = append(keys, identityKind.String())
	}

	keys = append(keys, identityProperties...)
	keys = append(keys, updateKinds.Strings()...)

	sort.Strings(keys)

	return strings.Join(keys, "")
}

func relUpdateKey(update graph.RelationshipUpdate) string {
	keys := []string{
		newUpdateKey(update.StartIdentityKind, update.StartIdentityProperties, update.Start.Kinds),
		newUpdateKey(update.Relationship.Kind, update.IdentityProperties, nil),
		newUpdateKey(update.EndIdentityKind, update.EndIdentityProperties, update.End.Kinds),
	}

	return strings.Join(keys, "")
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
	updateKey := newUpdateKey(update.IdentityKind, update.IdentityProperties, update.Node.Kinds)

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

func cypherBuildNodeUpdateQueryBatch(updates []graph.NodeUpdate) ([]string, []map[string]any) {
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

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"
)

type ExploreOptions struct {
	Driver                  string
	Graph                   string
	NodeSampleLimit         int
	RelationshipSampleLimit int
	HistogramLimit          int
	Classifier              Classifier
}

func ExploreDatabase(ctx context.Context, db graph.Database, options ExploreOptions) (Report, error) {
	targetGraph := graph.Graph{Name: strings.TrimSpace(options.Graph)}
	if targetGraph.Name != "" {
		if err := db.SetDefaultGraph(ctx, targetGraph); err != nil {
			return Report{}, fmt.Errorf("set graph target %q: %w", targetGraph.Name, err)
		}
	}

	nextReport := Report{
		GeneratedAt:                     time.Now().UTC(),
		Driver:                          options.Driver,
		Graph:                           targetGraph.Name,
		NodePropertySampleLimit:         options.NodeSampleLimit,
		RelationshipPropertySampleLimit: options.RelationshipSampleLimit,
		HistogramLimit:                  options.HistogramLimit,
		NodeKindHistogram:               map[string]int{},
		RelationshipKindHistogram:       map[string]int{},
		NodeProperties:                  map[string]PropertyStats{},
		RelationshipProperties:          map[string]PropertyStats{},
	}

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if targetGraph.Name != "" {
			tx = tx.WithGraph(targetGraph)
		}

		nodeCount, err := tx.Nodes().Count()
		if err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}
		nextReport.NodeCount = nodeCount

		relationshipCount, err := tx.Relationships().Count()
		if err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}
		nextReport.RelationshipCount = relationshipCount

		if err := collectNodeKinds(tx, nextReport.NodeKindHistogram); err != nil {
			return err
		}

		inDegrees, outDegrees, err := collectRelationshipKindsAndDegrees(tx, nextReport.RelationshipKindHistogram)
		if err != nil {
			return err
		}

		nextReport.InDegree = SummarizeDegrees(inDegrees)
		nextReport.OutDegree = SummarizeDegrees(outDegrees)

		if err := collectNodeProperties(tx, options.NodeSampleLimit, nextReport.NodeProperties, options.Classifier); err != nil {
			return err
		}

		if err := collectRelationshipProperties(tx, options.RelationshipSampleLimit, nextReport.RelationshipProperties, options.Classifier); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return Report{}, err
	}

	FinalizePropertyStats(nextReport.NodeProperties, options.Classifier)
	FinalizePropertyStats(nextReport.RelationshipProperties, options.Classifier)
	nextReport.Notes = BuildNotes(nextReport)
	return nextReport, nil
}

func collectNodeKinds(tx graph.Transaction, histogram map[string]int) error {
	return tx.Nodes().FetchKinds(func(cursor graph.Cursor[graph.KindsResult]) error {
		for next := range cursor.Chan() {
			if len(next.Kinds) == 0 {
				histogram["<none>"] += 1
			}

			for _, kind := range next.Kinds {
				histogram[kind.String()] += 1
			}
		}

		return cursor.Error()
	})
}

func collectRelationshipKindsAndDegrees(tx graph.Transaction, histogram map[string]int) (map[uint64]uint64, map[uint64]uint64, error) {
	inDegrees := map[uint64]uint64{}
	outDegrees := map[uint64]uint64{}

	err := tx.Relationships().FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error {
		for next := range cursor.Chan() {
			if next.Kind == nil {
				histogram["<none>"] += 1
			} else {
				histogram[next.Kind.String()] += 1
			}

			outDegrees[next.StartID.Uint64()] += 1
			inDegrees[next.EndID.Uint64()] += 1
		}

		return cursor.Error()
	})

	return inDegrees, outDegrees, err
}

func collectNodeProperties(tx graph.Transaction, sampleLimit int, stats map[string]PropertyStats, classifier Classifier) error {
	query := tx.Nodes()
	if sampleLimit > 0 {
		query = query.Limit(sampleLimit)
	}

	return query.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
		sampled := 0
		for next := range cursor.Chan() {
			if sampleLimit > 0 && sampled >= sampleLimit {
				return nil
			}

			sampled++
			for key, value := range next.Properties.MapOrEmpty() {
				RecordProperty(stats, key, value, classifier)
			}
		}

		return cursor.Error()
	})
}

func collectRelationshipProperties(tx graph.Transaction, sampleLimit int, stats map[string]PropertyStats, classifier Classifier) error {
	query := tx.Relationships()
	if sampleLimit > 0 {
		query = query.Limit(sampleLimit)
	}

	return query.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
		sampled := 0
		for next := range cursor.Chan() {
			if sampleLimit > 0 && sampled >= sampleLimit {
				return nil
			}

			sampled++
			for key, value := range next.Properties.MapOrEmpty() {
				RecordProperty(stats, key, value, classifier)
			}
		}

		return cursor.Error()
	})
}

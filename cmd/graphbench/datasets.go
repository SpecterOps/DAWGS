// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

const defaultGraphName = "integration_test"

func scanDatasetKinds(datasetDir string, datasetNames []string) (graph.Kinds, graph.Kinds, error) {
	var nodeKinds, edgeKinds graph.Kinds

	for _, datasetName := range datasetNames {
		doc, err := parseDataset(datasetDir, datasetName)
		if err != nil {
			return nil, nil, err
		}

		nextNodeKinds, nextEdgeKinds := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nextNodeKinds...)
		edgeKinds = edgeKinds.Add(nextEdgeKinds...)
	}

	return nodeKinds, edgeKinds, nil
}

func parseDataset(datasetDir, name string) (opengraph.Document, error) {
	path := filepath.Join(datasetDir, name+".json")
	f, err := os.Open(path)
	if err != nil {
		return opengraph.Document{}, fmt.Errorf("open dataset %s: %w", name, err)
	}
	defer f.Close()

	doc, err := opengraph.ParseDocument(f)
	if err != nil {
		return opengraph.Document{}, fmt.Errorf("parse dataset %s: %w", name, err)
	}

	return doc, nil
}

func loadDataset(ctx context.Context, db graph.Database, datasetDir, name string) (opengraph.IDMap, error) {
	path := filepath.Join(datasetDir, name+".json")
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dataset %s: %w", name, err)
	}
	defer f.Close()

	idMap, err := opengraph.Load(ctx, db, f)
	if err != nil {
		return nil, fmt.Errorf("load dataset %s: %w", name, err)
	}

	return idMap, nil
}

func clearGraph(ctx context.Context, db graph.Database) error {
	return db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	})
}

func benchmarkSchema(nodeKinds, edgeKinds graph.Kinds) graph.Schema {
	return graph.Schema{
		Graphs: []graph.Graph{{
			Name:  defaultGraphName,
			Nodes: nodeKinds,
			Edges: edgeKinds,
		}},
		DefaultGraph: graph.Graph{Name: defaultGraphName},
	}
}

func resolveCaseParams(testCase ScaleCase, idMap opengraph.IDMap) (map[string]any, error) {
	params := make(map[string]any, len(testCase.Params)+len(testCase.NodeParams))
	for key, value := range testCase.Params {
		params[key] = value
	}

	for paramName, nodeName := range testCase.NodeParams {
		id, found := idMap[nodeName]
		if !found {
			return nil, fmt.Errorf("case %s references unknown dataset node %q", testCase.Name, nodeName)
		}

		params[paramName] = id.Int64()
	}

	if len(params) == 0 {
		return nil, nil
	}

	return params, nil
}

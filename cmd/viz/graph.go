package main

import (
	"io"
	"os"
	"strconv"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

var graphNodes = []opts.GraphNode{
	{Name: "Node1"},
	{Name: "Node2"},
	{Name: "Node3"},
	{Name: "Node4"},
	{Name: "Node5"},
	{Name: "Node6"},
	{Name: "Node7"},
	{Name: "Node8"},
}

func genLinks() []opts.GraphLink {
	links := make([]opts.GraphLink, 0)
	for i := 0; i < len(graphNodes); i++ {
		for j := 0; j < len(graphNodes); j++ {
			links = append(links, opts.GraphLink{Source: graphNodes[i].Name, Target: graphNodes[j].Name})
		}
	}
	return links
}

func graphBase() *charts.Graph {
	graph := charts.NewGraph()
	graph.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "basic graph example"}),
	)
	graph.AddSeries("graph", graphNodes, genLinks(),
		charts.WithGraphChartOpts(
			opts.GraphChart{Force: &opts.GraphForce{Repulsion: 8000}},
		),
	)
	return graph
}

func graphCircle() *charts.Graph {
	graph := charts.NewGraph()
	graph.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "Circular layout"}),
	)

	graph.AddSeries("graph", graphNodes, genLinks()).
		SetSeriesOptions(
			charts.WithGraphChartOpts(
				opts.GraphChart{
					Force:  &opts.GraphForce{Repulsion: 8000},
					Layout: "circular",
				}),
			charts.WithLabelOpts(opts.Label{Show: opts.Bool(true), Position: "right"}),
		)
	return graph
}

func graphDigraph(digraph container.DirectedGraph, direction graph.Direction) *charts.Graph {
	graph := charts.NewGraph()
	graph.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title: "demo",
		}))

	var (
		nodes []opts.GraphNode
		links []opts.GraphLink
	)

	digraph.EachNode(func(node uint64) bool {
		sourceNode := strconv.FormatUint(node, 10)

		nodes = append(nodes, opts.GraphNode{
			Name: sourceNode,
		})

		digraph.EachAdjacentNode(node, direction, func(adjacent uint64) bool {
			links = append(links, opts.GraphLink{
				Source: sourceNode,
				Target: strconv.FormatUint(adjacent, 10),
			})

			return true
		})

		return true
	})

	graph.AddSeries("graph", nodes, links).
		SetSeriesOptions(
			charts.WithGraphChartOpts(opts.GraphChart{
				Force: &opts.GraphForce{Repulsion: 8000},
			}),
		)
	return graph
}

func doTheGraph(digraph container.DirectedGraph, direction graph.Direction) {
	page := components.NewPage()
	page.AddCharts(
		graphDigraph(digraph, direction),
	)

	f, err := os.Create("graph.html")
	if err != nil {
		panic(err)

	}
	page.Render(io.MultiWriter(f))
	f.Close()
}

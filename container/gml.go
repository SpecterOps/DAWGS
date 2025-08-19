package container

import (
	"os"
	"strconv"

	"github.com/specterops/dawgs/graph"
)

func WriteDigraphToGML(digraph DirectedGraph, path string) error {
	if out, err := os.Create(path); err != nil {
		return err
	} else {
		defer out.Close()

		out.WriteString("graph [\n")
		out.WriteString("directed 1\n")

		digraph.EachNode(func(node uint64) bool {
			out.WriteString("node [\n")
			out.WriteString("id ")
			out.WriteString(strconv.FormatUint(node, 10))
			out.WriteString("\n]\n")

			return true
		})

		digraph.EachNode(func(node uint64) bool {
			digraph.EachAdjacentNode(node, graph.DirectionBoth, func(adjacent uint64) bool {
				out.WriteString("edge [\n")
				out.WriteString("source ")
				out.WriteString(strconv.FormatUint(node, 10))
				out.WriteString("\ntarget ")
				out.WriteString(strconv.FormatUint(adjacent, 10))
				out.WriteString("\n]\n")

				return true
			})

			return true
		})

		out.WriteString("]\n")
		return nil
	}
}

package container

import (
	"encoding/binary"
	"io"
	"strconv"
	"strings"
)

func MarshalSegment(segment *Segment, writer io.Writer) error {
	nextBytes := make([]byte, 8)

	for cursor := segment; cursor != nil; cursor = cursor.Previous {
		binary.LittleEndian.PutUint64(nextBytes, cursor.Node)

		if _, err := writer.Write(nextBytes); err != nil {
			return err
		}

		if cursor.Previous != nil {
			binary.LittleEndian.PutUint64(nextBytes, cursor.Edge)

			if _, err := writer.Write(nextBytes); err != nil {
				return err
			}
		}
	}

	return nil
}

func UnmarshalSegment(segmentBytes []byte) *Segment {
	var (
		nextSegment     *Segment
		terminalSegment = &Segment{}
	)

	for byteNum := 0; byteNum*8 < len(segmentBytes); byteNum += 1 {
		var (
			startIdx = byteNum * 8
			nextID   = binary.LittleEndian.Uint64(segmentBytes[startIdx : startIdx+8])
		)

		if nextSegment == nil {
			nextSegment = terminalSegment
			nextSegment.Node = nextID
		} else if nextSegment.Previous == nil {
			nextSegment.Edge = nextID
			nextSegment.Previous = &Segment{}
		} else {
			nextSegment = nextSegment.Previous
			nextSegment.Node = nextID
		}
	}

	return terminalSegment
}

type Segment struct {
	Node     uint64
	Edge     uint64
	Previous *Segment
}

func (s *Segment) Path() Path {
	path := Path{
		Edges: make([]Edge, 0, s.Depth()),
	}

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		if cursor.Previous != nil {
			path.Edges = append(path.Edges, Edge{
				ID:    cursor.Edge,
				Start: cursor.Previous.Node,
				End:   cursor.Node,
			})
		}
	}

	return path
}

func (s *Segment) Descend(node, edge uint64) *Segment {
	return &Segment{
		Node:     node,
		Edge:     edge,
		Previous: s,
	}
}

func (s *Segment) Each(visitor func(cursor *Segment) bool) {
	for cursor := s; cursor != nil; cursor = cursor.Previous {
		if !visitor(cursor) {
			break
		}
	}
}

func (s *Segment) Root() uint64 {
	var root uint64

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		root = cursor.Node
	}

	return root
}

// Trunk returns the edge closest to the root node of the segment
func (s *Segment) Trunk() uint64 {
	var trunk uint64

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		if cursor.Previous != nil {
			trunk = cursor.Edge
		}
	}

	return trunk
}

func (s *Segment) Edges() []uint64 {
	var edges []uint64

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		if cursor.Previous != nil {
			edges = append(edges, cursor.Edge)
		}
	}

	return edges
}

func (s *Segment) Nodes() []uint64 {
	var nodes []uint64

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		nodes = append(nodes, cursor.Node)
	}

	return nodes
}

func (s *Segment) Depth() int {
	depth := 0

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		depth += 1
	}

	return depth
}

type SerializedSegment struct {
	Nodes []uint64
	Edges []uint64
}

func (s SerializedSegment) ToSegment() *Segment {
	cursor := &Segment{}

	for nodeIndex := 0; nodeIndex < len(s.Nodes); nodeIndex += 1 {
		cursor.Node = s.Nodes[nodeIndex]

		if nodeIndex < len(s.Edges) {
			nextSegment := &Segment{
				Edge:     s.Edges[nodeIndex-1],
				Previous: cursor,
			}

			cursor = nextSegment
		}
	}

	return cursor
}

func (s *Segment) Format() string {
	var builder strings.Builder

	for cursor := s; cursor != nil; cursor = cursor.Previous {
		builder.WriteString("(")
		builder.WriteString(strconv.FormatUint(cursor.Node, 10))
		builder.WriteString(")")

		if cursor.Previous != nil {
			builder.WriteString("-[")
			builder.WriteString(strconv.FormatUint(cursor.Edge, 10))
			builder.WriteString("]->")
		}
	}

	return builder.String()
}

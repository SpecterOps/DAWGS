package container_test

import (
	"bytes"
	"testing"

	"github.com/specterops/dawgs/container"
)

func Test_MarshalSegment(t *testing.T) {
	var (
		buffer  = &bytes.Buffer{}
		segment = &container.Segment{
			Node: 5,
			Edge: 14,
			Previous: &container.Segment{
				Node: 4,
				Edge: 13,
				Previous: &container.Segment{
					Node: 3,
					Edge: 12,
					Previous: &container.Segment{
						Node: 2,
						Edge: 11,
						Previous: &container.Segment{
							Node: 1,
						},
					},
				},
			},
		}
	)

	container.MarshalSegment(segment, buffer)

	readSegment := container.UnmarshalSegment(buffer.Bytes())

	segment.Each(func(cursor *container.Segment) bool {
		if readSegment.Node != cursor.Node {
			t.Fatalf("Segments do not match - marshaled node %d - origin node %d", readSegment.Node, cursor.Node)
		}

		if readSegment.Previous == nil {
			if cursor.Previous != nil {
				t.Fatal("Segments do not match")
			}
		} else if readSegment.Edge != cursor.Edge {
			t.Fatal("Segments do not match")
		} else {
			readSegment = readSegment.Previous
		}

		return true
	})
}

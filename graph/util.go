package graph

import (
	"github.com/specterops/dawgs/cardinality"
	"sort"
)

func NodeSetToBitmap(nodes NodeSet) cardinality.Duplex[uint64] {
	bitmap := cardinality.NewBitmap64()

	for _, node := range nodes {
		bitmap.Add(node.ID.Uint64())
	}

	return bitmap
}

func SortAndSliceNodeSet(set NodeSet, skip, limit int) NodeSet {
	nodes := SortNodeSetById(set)
	if skip == 0 && limit == 0 {
		return NewNodeSet(nodes...)
	} else if limit == 0 {
		return NewNodeSet(nodes[skip:]...)
	} else if skip == 0 {
		return NewNodeSet(nodes[:limit]...)
	} else {
		return NewNodeSet(nodes[skip : skip+limit]...)
	}
}

func SortNodeSetById(set NodeSet) []*Node {
	ids := set.IDs()
	SortIDSlice(ids)
	results := make([]*Node, set.Len())
	for idx, id := range ids {
		results[idx] = set.Get(id)
	}

	return results
}

func SortIDSlice(ids []ID) {
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Int64() < ids[j].Int64()
	})
}

func CopyIDSlice(ids []ID) []ID {
	sliceCopy := make([]ID, len(ids))
	copy(sliceCopy, ids)

	return sliceCopy
}

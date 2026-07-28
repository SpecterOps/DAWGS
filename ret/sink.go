package ret

type sink struct {
	shardSize int

	nodeCache []normalizedNode
	relCache  []normalizedRelationship
}

func (s *sink) PushNodes(nodes []normalizedNode) (shard, bool, error) {
	s.nodeCache = append(s.nodeCache, nodes...)

	if len(s.nodeCache) < s.shardSize {
		return shard{}, false, nil
	}

	output, err := s.FlushNodes()
	if err != nil {
		return output, false, err
	}

	return output, true, nil
}

func (s *sink) FlushNodes() (shard, error) {
	return shard{}, nil
}

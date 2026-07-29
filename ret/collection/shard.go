package collection

import (
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

type NodeShard struct {
	Index        int                   `json:"index"`
	Count        int64                 `json:"count"`
	LastSourceID uint64                `json:"last_source_id"`
	ScrubCounts  scrub.ActionCounts    `json:"scrub_counts"`
	JSONL        *jsonl.NodeArtifact   `json:"jsonl,omitempty"`
	Parquet      *parquet.NodeArtifact `json:"parquet,omitempty"`
}

type RelationshipShard struct {
	Index        int                           `json:"index"`
	Count        int64                         `json:"count"`
	LastSourceID uint64                        `json:"last_source_id"`
	ScrubCounts  scrub.ActionCounts            `json:"scrub_counts"`
	JSONL        *jsonl.RelationshipArtifact   `json:"jsonl,omitempty"`
	Parquet      *parquet.RelationshipArtifact `json:"parquet,omitempty"`
}

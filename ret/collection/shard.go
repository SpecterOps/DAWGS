package collection

import (
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

type JSONLArtifact struct {
	Path string `json:"path"`
	jsonl.Artifact
}

type ParquetArtifact struct {
	Path string `json:"path"`
	parquet.Artifact
}

type NodeShard struct {
	Index        int                `json:"index"`
	Count        int64              `json:"count"`
	LastSourceID uint64             `json:"last_source_id"`
	ScrubCounts  scrub.ActionCounts `json:"scrub_counts"`
	JSONL        *JSONLArtifact     `json:"jsonl,omitempty"`
	Parquet      *ParquetArtifact   `json:"parquet,omitempty"`
}

type RelationshipShard struct {
	Index        int                `json:"index"`
	Count        int64              `json:"count"`
	LastSourceID uint64             `json:"last_source_id"`
	ScrubCounts  scrub.ActionCounts `json:"scrub_counts"`
	JSONL        *JSONLArtifact     `json:"jsonl,omitempty"`
	Parquet      *ParquetArtifact   `json:"parquet,omitempty"`
}

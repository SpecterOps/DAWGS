// Package checkpoint persists validated resumable dump progress.
package checkpoint

import (
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
)

const (
	Format   = "ret-checkpoint-v1"
	FileName = ".ret-checkpoint.json"
)

type Identity struct {
	Graphs                []string `json:"graphs"`
	EntityBatchSize       int      `json:"entity_batch_size"`
	ShardSize             int      `json:"shard_size"`
	JSONLEnabled          bool     `json:"jsonl_enabled"`
	JSONLCodec            string   `json:"jsonl_codec"`
	JSONLLevel            int      `json:"jsonl_level"`
	ParquetEnabled        bool     `json:"parquet_enabled"`
	JSONLSchemaVersion    string   `json:"jsonl_schema_version"`
	ParquetSchemaVersion  string   `json:"parquet_schema_version"`
	ScrubEnabled          bool     `json:"scrub_enabled"`
	ScrubRulesFingerprint string   `json:"scrub_rules_fingerprint"`
	ScrubSaltFingerprint  string   `json:"scrub_salt_fingerprint"`
}

type Phase string

const (
	PhaseNodes         Phase = "nodes"
	PhaseRelationships Phase = "relationships"
	PhaseComplete      Phase = "complete"
)

type GraphState struct {
	Name               string                         `json:"name"`
	Snapshot           dawgs.Snapshot                 `json:"snapshot"`
	Phase              Phase                          `json:"phase"`
	NodeCursor         uint64                         `json:"node_cursor"`
	RelationshipCursor uint64                         `json:"relationship_cursor"`
	NodeShards         []collection.NodeShard         `json:"node_shards"`
	RelationshipShards []collection.RelationshipShard `json:"relationship_shards"`
}

type State struct {
	Format   string       `json:"format"`
	Identity Identity     `json:"identity"`
	Graphs   []GraphState `json:"graphs"`
}

// Store persists one dump's checkpoint beneath Root.
//
// A dump directory must have only one active writer. Store does not provide a
// locking protocol; CleanupOrphans may remove staging files left by a crashed
// writer after the published checkpoint has been loaded and validated.
type Store struct {
	Root string
}

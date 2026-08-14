package checkpoint

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func fixtureIdentity() Identity {
	return Identity{
		Graphs:                []string{"alpha", "beta"},
		EntityBatchSize:       100,
		ShardSize:             2,
		JSONLEnabled:          true,
		JSONLCodec:            string(jsonl.CodecZstd),
		JSONLLevel:            3,
		ParquetEnabled:        true,
		JSONLSchemaVersion:    jsonl.SchemaVersion,
		ParquetSchemaVersion:  parquet.SchemaVersion,
		ScrubEnabled:          true,
		ScrubRulesFingerprint: strings.Repeat("a", 64),
		ScrubSaltFingerprint:  strings.Repeat("b", 64),
	}
}

func fixtureState() State {
	identity := fixtureIdentity()
	return State{
		Format:   Format,
		Identity: identity,
		Graphs: []GraphState{{
			Name:       "alpha",
			Snapshot:   dawgs.Snapshot{NodeCount: 5, RelationshipCount: 3},
			Phase:      PhaseNodes,
			NodeCursor: 20,
			NodeShards: []collection.NodeShard{
				fixtureNodeShard(identity, "alpha", 1, 2, 20),
			},
		}},
	}
}

func fixtureNodeShard(identity Identity, graph string, index int, count int64, cursor uint64) collection.NodeShard {
	shard := collection.NodeShard{
		Index:        index,
		Count:        count,
		LastSourceID: cursor,
		ScrubCounts:  scrub.ActionCounts{Redact: 1},
	}
	if identity.JSONLEnabled {
		shard.JSONL = &collection.JSONLArtifact{
			Path: collection.NodeJSONLPath(graph, index, jsonl.Codec(identity.JSONLCodec)),
			Artifact: jsonl.Artifact{
				SchemaVersion:     identity.JSONLSchemaVersion,
				Codec:             jsonl.Codec(identity.JSONLCodec),
				SHA256:            strings.Repeat("c", 64),
				Level:             identity.JSONLLevel,
				Count:             count,
				UncompressedBytes: 100,
				StoredBytes:       50,
			},
		}
	}
	if identity.ParquetEnabled {
		shard.Parquet = &collection.ParquetArtifact{
			Path: collection.NodeParquetPath(graph, index),
			Artifact: parquet.Artifact{
				SchemaVersion: identity.ParquetSchemaVersion,
				SHA256:        strings.Repeat("d", 64),
				Count:         count,
				StoredBytes:   75,
			},
		}
	}
	return shard
}

func fixtureRelationshipShard(identity Identity, graph string, index int, count int64, cursor uint64) collection.RelationshipShard {
	shard := collection.RelationshipShard{
		Index:        index,
		Count:        count,
		LastSourceID: cursor,
		ScrubCounts:  scrub.ActionCounts{Redact: 1},
	}
	if identity.JSONLEnabled {
		shard.JSONL = &collection.JSONLArtifact{
			Path: collection.RelationshipJSONLPath(graph, index, jsonl.Codec(identity.JSONLCodec)),
			Artifact: jsonl.Artifact{
				SchemaVersion:     identity.JSONLSchemaVersion,
				Codec:             jsonl.Codec(identity.JSONLCodec),
				SHA256:            strings.Repeat("e", 64),
				Level:             identity.JSONLLevel,
				Count:             count,
				UncompressedBytes: 120,
				StoredBytes:       60,
			},
		}
	}
	if identity.ParquetEnabled {
		shard.Parquet = &collection.ParquetArtifact{
			Path: collection.RelationshipParquetPath(graph, index),
			Artifact: parquet.Artifact{
				SchemaVersion: identity.ParquetSchemaVersion,
				SHA256:        strings.Repeat("f", 64),
				Count:         count,
				StoredBytes:   80,
			},
		}
	}
	return shard
}

func TestStoreRoundTripDoesNotPersistSalt(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	privateSalt := "private-salt"
	state.Identity.ScrubSaltFingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte(privateSalt)))
	store := Store{Root: root}

	require.NoError(t, store.Save(state))
	payload, err := os.ReadFile(filepath.Join(root, FileName))
	require.NoError(t, err)
	require.NotContains(t, string(payload), privateSalt)
	require.Contains(t, string(payload), state.Identity.ScrubSaltFingerprint)

	loaded, found, err := store.Load()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, state, loaded)
}

func TestStoreAcceptsEveryLegalPhaseBoundary(t *testing.T) {
	tests := []struct {
		name  string
		state func() State
	}{
		{name: "nodes before first shard", state: func() State {
			state := fixtureState()
			state.Graphs[0].NodeCursor = 0
			state.Graphs[0].NodeShards = nil
			return state
		}},
		{name: "nodes fully committed before transition", state: func() State {
			state := fixtureState()
			state.Graphs[0].Snapshot.NodeCount = 2
			return state
		}},
		{name: "relationships before first shard", state: func() State {
			state := fixtureState()
			state.Graphs[0].Snapshot.NodeCount = 2
			state.Graphs[0].Phase = PhaseRelationships
			return state
		}},
		{name: "relationships partially committed", state: func() State {
			state := fixtureState()
			state.Graphs[0].Snapshot.NodeCount = 2
			state.Graphs[0].Phase = PhaseRelationships
			state.Graphs[0].RelationshipCursor = 30
			state.Graphs[0].RelationshipShards = []collection.RelationshipShard{
				fixtureRelationshipShard(state.Identity, "alpha", 1, 2, 30),
			}
			return state
		}},
		{name: "relationships fully committed before transition", state: func() State {
			state := fixtureState()
			state.Graphs[0].Snapshot = dawgs.Snapshot{NodeCount: 2, RelationshipCount: 1}
			state.Graphs[0].Phase = PhaseRelationships
			state.Graphs[0].RelationshipCursor = 30
			state.Graphs[0].RelationshipShards = []collection.RelationshipShard{
				fixtureRelationshipShard(state.Identity, "alpha", 1, 1, 30),
			}
			return state
		}},
		{name: "complete", state: func() State {
			state := fixtureState()
			state.Graphs[0].Snapshot = dawgs.Snapshot{NodeCount: 2, RelationshipCount: 1}
			state.Graphs[0].Phase = PhaseComplete
			state.Graphs[0].RelationshipCursor = 30
			state.Graphs[0].RelationshipShards = []collection.RelationshipShard{
				fixtureRelationshipShard(state.Identity, "alpha", 1, 1, 30),
			}
			return state
		}},
		{name: "empty graph complete", state: func() State {
			state := fixtureState()
			state.Graphs[0] = GraphState{
				Name:     "alpha",
				Snapshot: dawgs.Snapshot{},
				Phase:    PhaseComplete,
			}
			return state
		}},
		{name: "completed prefix and active next graph", state: func() State {
			state := fixtureState()
			state.Graphs[0] = GraphState{
				Name:     "alpha",
				Snapshot: dawgs.Snapshot{},
				Phase:    PhaseComplete,
			}
			state.Graphs = append(state.Graphs, GraphState{
				Name:     "beta",
				Snapshot: dawgs.Snapshot{},
				Phase:    PhaseNodes,
			})
			return state
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := Store{Root: t.TempDir()}
			require.NoError(t, store.Save(test.state()))
		})
	}
}

func TestStoreLoadStrictlyRejectsMalformedCheckpoint(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		label   string
	}{
		{name: "invalid JSON", payload: "{", label: "decode checkpoint"},
		{name: "unknown field", payload: `{"format":"ret-checkpoint-v1","unknown":true}`, label: "unknown field"},
		{name: "trailing JSON", payload: `{}` + "\n" + `{}`, label: "trailing JSON value"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, FileName), []byte(test.payload), 0o600))

			_, found, err := (Store{Root: root}).Load()
			require.True(t, found)
			require.ErrorContains(t, err, test.label)
		})
	}
}

func TestStoreLoadMissingIsTheOnlyNotFoundResult(t *testing.T) {
	store := Store{Root: t.TempDir()}

	_, found, err := store.Load()
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, os.Mkdir(filepath.Join(store.Root, FileName), 0o700))
	_, found, err = store.Load()
	require.True(t, found)
	require.Error(t, err)
}

func TestStoreSaveValidatesBeforeTouchingCurrentCheckpoint(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	require.NoError(t, store.Save(fixtureState()))
	before, err := os.ReadFile(filepath.Join(root, FileName))
	require.NoError(t, err)

	invalid := fixtureState()
	invalid.Format = "wrong"
	err = store.Save(invalid)
	require.ErrorContains(t, err, "format")

	after, readErr := os.ReadFile(filepath.Join(root, FileName))
	require.NoError(t, readErr)
	require.Equal(t, before, after)
	require.Empty(t, checkpointStagingNames(t, root))
}

func TestStoreSavePreservesCurrentCheckpointAndCleansTemporaryOnPublishFailure(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	require.NoError(t, store.Save(fixtureState()))
	before, err := os.ReadFile(filepath.Join(root, FileName))
	require.NoError(t, err)

	originalRename := checkpointRename
	checkpointRename = func(_, _ string) error { return errors.New("injected rename failure") }
	t.Cleanup(func() { checkpointRename = originalRename })

	updated := fixtureState()
	updated.Graphs[0].Snapshot.NodeCount = 6
	err = store.Save(updated)
	require.ErrorContains(t, err, "injected rename failure")

	after, readErr := os.ReadFile(filepath.Join(root, FileName))
	require.NoError(t, readErr)
	require.Equal(t, before, after)
	require.Empty(t, checkpointStagingNames(t, root))
}

func TestStoreSaveJoinsPublishAndTemporaryCleanupFailures(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	originalRename := checkpointRename
	originalRemove := checkpointRemove
	checkpointRename = func(_, _ string) error { return errors.New("primary publish failure") }
	checkpointRemove = func(string) error { return errors.New("cleanup failure") }
	t.Cleanup(func() {
		checkpointRename = originalRename
		checkpointRemove = originalRemove
	})

	err := store.Save(fixtureState())
	require.ErrorContains(t, err, "primary publish failure")
	require.ErrorContains(t, err, "cleanup failure")
}

func TestStoreUniqueStagingDoesNotBlockSaveAndCleanupRemovesCrashLeftover(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	initial := fixtureState()
	require.NoError(t, store.Save(initial))

	updated := fixtureState()
	updated.Graphs[0].Snapshot.NodeCount = 6
	originalRename := checkpointRename
	originalRemove := checkpointRemove
	var crashedStage string
	checkpointRename = func(oldPath, _ string) error {
		crashedStage = oldPath
		return errors.New("simulated crash before checkpoint publish")
	}
	checkpointRemove = func(name string) error {
		if name == crashedStage {
			return errors.New("simulated process loss before stage cleanup")
		}
		return originalRemove(name)
	}
	t.Cleanup(func() {
		checkpointRename = originalRename
		checkpointRemove = originalRemove
	})

	err := store.Save(updated)
	require.ErrorContains(t, err, "simulated crash before checkpoint publish")
	require.ErrorContains(t, err, "simulated process loss before stage cleanup")
	checkpointRename = originalRename
	checkpointRemove = originalRemove

	staging := checkpointStagingNames(t, root)
	require.Len(t, staging, 1)
	require.NotEqual(t, FileName+".tmp", staging[0])
	nonce := strings.TrimPrefix(staging[0], FileName+".tmp-")
	require.NotEmpty(t, nonce)
	require.Regexp(t, `^[A-Za-z0-9_-]+$`, nonce)

	require.NoError(t, store.Save(updated), "a unique stage must not be blocked by the crash leftover")
	loaded, found, err := store.Load()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, updated, loaded)
	require.NoError(t, store.CleanupOrphans(loaded))
	require.Empty(t, checkpointStagingNames(t, root))
}

func TestStoreRemoveIsIdempotent(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	require.NoError(t, store.Remove())
	require.NoError(t, store.Save(fixtureState()))
	require.NoError(t, store.Remove())
	require.NoError(t, store.Remove())
	_, err := os.Stat(filepath.Join(root, FileName))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStoreAcceptsEveryConcreteOutputCombination(t *testing.T) {
	tests := []struct {
		name    string
		jsonl   bool
		parquet bool
	}{
		{name: "JSONL only", jsonl: true},
		{name: "Parquet only", parquet: true},
		{name: "dual", jsonl: true, parquet: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := fixtureStateForOutputs(test.jsonl, test.parquet)
			store := Store{Root: t.TempDir()}
			require.NoError(t, store.Save(state))
			_, found, err := store.Load()
			require.NoError(t, err)
			require.True(t, found)
		})
	}
}

func TestStoreAcceptsDisabledScrubWithoutFingerprintsOrCounts(t *testing.T) {
	state := fixtureState()
	state.Identity.ScrubEnabled = false
	state.Identity.ScrubRulesFingerprint = ""
	state.Identity.ScrubSaltFingerprint = ""
	state.Graphs[0].NodeShards[0].ScrubCounts = scrub.ActionCounts{}

	require.NoError(t, (Store{Root: t.TempDir()}).Save(state))
}

func TestStoreAcceptsParquetOnlyWithZeroDisabledJSONLConfig(t *testing.T) {
	state := fixtureStateForOutputs(false, true)
	state.Identity.JSONLCodec = ""
	state.Identity.JSONLLevel = 0

	require.NoError(t, (Store{Root: t.TempDir()}).Save(state))
}

func fixtureStateForOutputs(jsonlEnabled, parquetEnabled bool) State {
	state := fixtureState()
	state.Identity.JSONLEnabled = jsonlEnabled
	state.Identity.ParquetEnabled = parquetEnabled
	if !jsonlEnabled {
		state.Graphs[0].NodeShards[0].JSONL = nil
	}
	if !parquetEnabled {
		state.Graphs[0].NodeShards[0].Parquet = nil
	}
	return state
}

func TestStoreRejectsMalformedState(t *testing.T) {
	tests := []struct {
		name   string
		label  string
		mutate func(*State)
	}{
		{name: "format", label: "format", mutate: func(value *State) { value.Format = "wrong" }},
		{name: "empty graph identity", label: "graph", mutate: func(value *State) { value.Identity.Graphs = nil }},
		{name: "duplicate identity graph", label: "duplicate", mutate: func(value *State) {
			value.Identity.Graphs = []string{"alpha", "alpha"}
		}},
		{name: "unsafe identity graph", label: "safe", mutate: func(value *State) {
			value.Identity.Graphs[0] = "../alpha"
			value.Graphs[0].Name = "../alpha"
		}},
		{name: "nonpositive batch", label: "batch", mutate: func(value *State) { value.Identity.EntityBatchSize = 0 }},
		{name: "nonpositive shard size", label: "shard size", mutate: func(value *State) { value.Identity.ShardSize = 0 }},
		{name: "no outputs", label: "output", mutate: func(value *State) {
			value.Identity.JSONLEnabled = false
			value.Identity.ParquetEnabled = false
		}},
		{name: "invalid JSONL codec", label: "codec", mutate: func(value *State) { value.Identity.JSONLCodec = "zip" }},
		{name: "wrong JSONL schema", label: "JSONL schema", mutate: func(value *State) {
			value.Identity.JSONLSchemaVersion = "wrong"
		}},
		{name: "wrong Parquet schema", label: "Parquet schema", mutate: func(value *State) {
			value.Identity.ParquetSchemaVersion = "wrong"
		}},
		{name: "invalid scrub fingerprint", label: "fingerprint", mutate: func(value *State) {
			value.Identity.ScrubSaltFingerprint = "private-salt"
		}},
		{name: "disabled scrub fingerprints", label: "disabled scrub", mutate: func(value *State) {
			value.Identity.ScrubEnabled = false
		}},
		{name: "disabled scrub shard counts", label: "scrub counts", mutate: func(value *State) {
			value.Identity.ScrubEnabled = false
			value.Identity.ScrubRulesFingerprint = ""
			value.Identity.ScrubSaltFingerprint = ""
		}},
		{name: "negative preserve scrub count", label: "scrub count", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].ScrubCounts.Preserve = -1
		}},
		{name: "negative pseudonymize scrub count", label: "scrub count", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].ScrubCounts.Pseudonymize = -1
		}},
		{name: "negative redact scrub count", label: "scrub count", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].ScrubCounts.Redact = -1
		}},
		{name: "negative shift timestamp scrub count", label: "scrub count", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].ScrubCounts.ShiftTimestamp = -1
		}},
		{name: "graph order", label: "order", mutate: func(value *State) { value.Graphs[0].Name = "beta" }},
		{name: "duplicate graph state", label: "duplicate", mutate: func(value *State) {
			value.Graphs = append(value.Graphs, value.Graphs[0])
		}},
		{name: "negative snapshot", label: "snapshot", mutate: func(value *State) {
			value.Graphs[0].Snapshot.NodeCount = -1
		}},
		{name: "unknown phase", label: "phase", mutate: func(value *State) { value.Graphs[0].Phase = "unknown" }},
		{name: "cursor without matching shard", label: "cursor", mutate: func(value *State) { value.Graphs[0].NodeCursor++ }},
		{name: "relationship cursor without shard", label: "relationship cursor", mutate: func(value *State) {
			value.Graphs[0].RelationshipCursor = 1
		}},
		{name: "relationship shard during nodes", label: "nodes phase", mutate: func(value *State) {
			value.Graphs[0].RelationshipShards = []collection.RelationshipShard{
				fixtureRelationshipShard(value.Identity, "alpha", 1, 2, 30),
			}
			value.Graphs[0].RelationshipCursor = 30
		}},
		{name: "node shard gap", label: "node shard index", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].Index = 2
		}},
		{name: "nonpositive shard count", label: "count", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].Count = 0
		}},
		{name: "shard exceeds configured size", label: "shard size", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].Count = 3
			value.Graphs[0].NodeShards[0].JSONL.Count = 3
			value.Graphs[0].NodeShards[0].Parquet.Count = 3
		}},
		{name: "partial shard before snapshot boundary", label: "partial", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].Count = 1
			value.Graphs[0].NodeShards[0].JSONL.Count = 1
			value.Graphs[0].NodeShards[0].Parquet.Count = 1
		}},
		{name: "shards exceed snapshot", label: "snapshot", mutate: func(value *State) {
			value.Graphs[0].Snapshot.NodeCount = 1
		}},
		{name: "nonincreasing shard cursor", label: "source ID", mutate: func(value *State) {
			value.Graphs[0].NodeShards = append(value.Graphs[0].NodeShards,
				fixtureNodeShard(value.Identity, "alpha", 2, 1, 19))
		}},
		{name: "output mismatch", label: "output mismatch", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].JSONL = nil
		}},
		{name: "traversing JSONL path", label: "traverses", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].JSONL.Path = "../outside"
		}},
		{name: "wrong JSONL codec metadata", label: "codec", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].JSONL.Codec = "gzip"
		}},
		{name: "wrong JSONL count metadata", label: "count", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].JSONL.Count++
		}},
		{name: "bad JSONL digest", label: "SHA-256", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].JSONL.SHA256 = "bad"
		}},
		{name: "bad Parquet bytes", label: "stored bytes", mutate: func(value *State) {
			value.Graphs[0].NodeShards[0].Parquet.StoredBytes = 0
		}},
		{name: "relationships phase before nodes complete", label: "relationships phase", mutate: func(value *State) {
			value.Graphs[0].Phase = PhaseRelationships
		}},
		{name: "complete before relationships complete", label: "complete phase", mutate: func(value *State) {
			value.Graphs[0].Phase = PhaseComplete
		}},
		{name: "incomplete graph before later state", label: "phase progression", mutate: func(value *State) {
			second := GraphState{Name: "beta", Snapshot: dawgs.Snapshot{}, Phase: PhaseComplete}
			value.Graphs = append(value.Graphs, second)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			state := fixtureState()
			test.mutate(&state)

			err := (Store{Root: root}).Save(state)
			require.ErrorContains(t, err, test.label)
			_, statErr := os.Stat(filepath.Join(root, FileName))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestStoreLoadRejectsMalformedStateWrittenAsJSON(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	state.Graphs[0].NodeCursor++
	payload, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, FileName), payload, 0o600))

	_, found, err := (Store{Root: root}).Load()
	require.True(t, found)
	require.ErrorContains(t, err, "cursor")
}

func TestCleanupOrphansDeletesOnlyRecognizedNextShardArtifacts(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	committed := state.Graphs[0].NodeShards[0]
	installArtifact(t, root, committed.JSONL.Path)
	installArtifact(t, root, committed.Parquet.Path)

	nextJSONL := collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd)
	nextParquet := collection.NodeParquetPath("alpha", 2)
	installArtifact(t, root, nextJSONL)
	installArtifact(t, root, nextParquet)
	installArtifact(t, root, nextJSONL+".tmp-A_z09")
	installArtifact(t, root, nextParquet+".tmp-nonce")

	require.NoError(t, (Store{Root: root}).CleanupOrphans(state))

	requireArtifactExists(t, root, committed.JSONL.Path)
	requireArtifactExists(t, root, committed.Parquet.Path)
	requireArtifactMissing(t, root, nextJSONL)
	requireArtifactMissing(t, root, nextParquet)
	requireArtifactMissing(t, root, nextJSONL+".tmp-A_z09")
	requireArtifactMissing(t, root, nextParquet+".tmp-nonce")
}

func TestCleanupOrphansRejectsUnknownEntryWithoutDeletingAnything(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	next := collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd)
	installArtifact(t, root, next)
	installArtifact(t, root, "notes.txt")

	err := (Store{Root: root}).CleanupOrphans(state)
	require.ErrorContains(t, err, "unknown resume")
	requireArtifactExists(t, root, next)
	requireArtifactExists(t, root, "notes.txt")
}

func TestCleanupOrphansRejectsInvalidCheckpointStagingNameWithoutDeletingValidStage(t *testing.T) {
	tests := []string{
		FileName + ".tmp",
		FileName + ".tmp-",
		FileName + ".tmp-a.b",
		FileName + ".tmp-a=b",
		FileName + ".tmp-z.bad",
		FileName + ".tmpx-nonce",
		"x" + FileName + ".tmp-nonce",
	}

	for _, invalid := range tests {
		t.Run(invalid, func(t *testing.T) {
			root := t.TempDir()
			store := Store{Root: root}
			require.NoError(t, store.Save(fixtureState()))
			loaded, found, err := store.Load()
			require.NoError(t, err)
			require.True(t, found)

			valid := FileName + ".tmp-valid_nonce-09"
			installArtifact(t, root, valid)
			installArtifact(t, root, invalid)

			err = store.CleanupOrphans(loaded)
			require.ErrorContains(t, err, "unknown resume")
			requireArtifactExists(t, root, valid)
			requireArtifactExists(t, root, invalid)
		})
	}
}

func TestCleanupOrphansRejectsCheckpointStagingSymlink(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	require.NoError(t, store.Save(fixtureState()))
	loaded, found, err := store.Load()
	require.NoError(t, err)
	require.True(t, found)

	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("keep"), 0o600))
	staging := filepath.Join(root, FileName+".tmp-valid_nonce")
	require.NoError(t, os.Symlink(outside, staging))

	err = store.CleanupOrphans(loaded)
	require.ErrorContains(t, err, "symbolic link")
	requireArtifactExists(t, root, FileName+".tmp-valid_nonce")
	payload, readErr := os.ReadFile(outside)
	require.NoError(t, readErr)
	require.Equal(t, "keep", string(payload))
}

func TestCleanupOrphansRejectsCheckpointStagingDirectory(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: root}
	require.NoError(t, store.Save(fixtureState()))
	loaded, found, err := store.Load()
	require.NoError(t, err)
	require.True(t, found)

	staging := filepath.Join(root, FileName+".tmp-valid_nonce")
	require.NoError(t, os.Mkdir(staging, 0o700))

	err = store.CleanupOrphans(loaded)
	require.ErrorContains(t, err, "unknown resume directory")
	requireArtifactExists(t, root, FileName+".tmp-valid_nonce")
}

func TestCleanupOrphansRejectsUnsafeAndOverbroadLookalikesWithoutDeletion(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "later final index", path: collection.NodeJSONLPath("alpha", 3, jsonl.CodecZstd)},
		{name: "wrong extension", path: "graphs/alpha/nodes/000002.json"},
		{name: "empty nonce", path: collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd) + ".tmp-"},
		{name: "non URL safe nonce", path: collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd) + ".tmp-a.b"},
		{name: "unrelated graph", path: collection.NodeJSONLPath("other", 2, jsonl.CodecZstd)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			state := fixtureState()
			recognized := collection.NodeParquetPath("alpha", 2)
			installArtifact(t, root, recognized)
			installArtifact(t, root, test.path)

			err := (Store{Root: root}).CleanupOrphans(state)
			require.Error(t, err)
			requireArtifactExists(t, root, recognized)
			requireArtifactExists(t, root, test.path)
		})
	}
}

func TestCleanupOrphansRejectsSymlinkWithoutDeletingRecognizedArtifact(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("keep"), 0o600))
	state := fixtureState()
	recognized := collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd)
	installArtifact(t, root, recognized)
	link := filepath.Join(root, filepath.FromSlash("graphs/alpha/nodes/link"))
	require.NoError(t, os.Symlink(outside, link))

	err := (Store{Root: root}).CleanupOrphans(state)
	require.ErrorContains(t, err, "symbolic link")
	requireArtifactExists(t, root, recognized)
	payload, readErr := os.ReadFile(outside)
	require.NoError(t, readErr)
	require.Equal(t, "keep", string(payload))
}

func TestCleanupOrphansRejectsRootReplacedByOutsideSymlinkBeforePinning(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "collection")
	require.NoError(t, os.Mkdir(root, 0o700))
	state := fixtureState()
	recognized := collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd)
	installArtifact(t, root, recognized)

	outside := filepath.Join(parent, "outside")
	require.NoError(t, os.Mkdir(outside, 0o700))
	installArtifact(t, outside, recognized)
	movedRoot := filepath.Join(parent, "collection-before-swap")

	originalOpenRoot := checkpointOpenRoot
	checkpointOpenRoot = func(name string) (*os.Root, error) {
		require.NoError(t, os.Rename(root, movedRoot))
		require.NoError(t, os.Symlink(outside, root))
		return originalOpenRoot(name)
	}
	t.Cleanup(func() { checkpointOpenRoot = originalOpenRoot })

	err := (Store{Root: root}).CleanupOrphans(state)
	require.ErrorContains(t, err, "changed while opening")
	requireArtifactExists(t, movedRoot, recognized)
	requireArtifactExists(t, outside, recognized)
}

func TestCleanupOrphansCannotEscapeRootWhenDirectoryBecomesSymlinkBeforeRemoval(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	recognized := collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd)
	installArtifact(t, root, recognized)

	outside := t.TempDir()
	outsideArtifact := filepath.Join(outside, filepath.Base(recognized))
	require.NoError(t, os.WriteFile(outsideArtifact, []byte("outside"), 0o600))

	originalRemove := checkpointPinnedRemove
	checkpointPinnedRemove = func(parentRoot *os.Root, basename string) error {
		nodes := filepath.Join(root, filepath.FromSlash("graphs/alpha/nodes"))
		moved := filepath.Join(root, filepath.FromSlash("graphs/alpha/nodes-inventoried"))
		require.NoError(t, os.Rename(nodes, moved))
		require.NoError(t, os.Symlink(outside, nodes))
		return originalRemove(parentRoot, basename)
	}
	t.Cleanup(func() { checkpointPinnedRemove = originalRemove })

	require.NoError(t, (Store{Root: root}).CleanupOrphans(state))
	payload, readErr := os.ReadFile(outsideArtifact)
	require.NoError(t, readErr)
	require.Equal(t, "outside", string(payload))
	requireArtifactMissing(t, root, "graphs/alpha/nodes-inventoried/"+filepath.Base(recognized))
}

func TestCleanupOrphansPinsCandidateParentAgainstInRootCommittedRedirect(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	state.Graphs[0].Snapshot = dawgs.Snapshot{NodeCount: 2}
	state.Graphs[0].Phase = PhaseComplete
	state.Graphs = append(state.Graphs, GraphState{
		Name:     "beta",
		Snapshot: dawgs.Snapshot{NodeCount: 2},
		Phase:    PhaseNodes,
	})

	committed := state.Graphs[0].NodeShards[0].JSONL.Path
	candidate := collection.NodeJSONLPath("beta", 1, jsonl.CodecZstd)
	installArtifact(t, root, committed)
	installArtifact(t, root, candidate)

	originalRemove := checkpointPinnedRemove
	checkpointPinnedRemove = func(parentRoot *os.Root, basename string) error {
		betaNodes := filepath.Join(root, filepath.FromSlash("graphs/beta/nodes"))
		moved := filepath.Join(root, filepath.FromSlash("graphs/beta/nodes-inventoried"))
		require.NoError(t, os.Rename(betaNodes, moved))
		require.NoError(t, os.Symlink("../alpha/nodes", betaNodes))
		return originalRemove(parentRoot, basename)
	}
	t.Cleanup(func() { checkpointPinnedRemove = originalRemove })

	require.NoError(t, (Store{Root: root}).CleanupOrphans(state))
	requireArtifactExists(t, root, committed)
	requireArtifactMissing(t, root, "graphs/beta/nodes-inventoried/"+filepath.Base(candidate))
}

func TestCleanupOrphansRejectsInvalidTraversalStateBeforeDeleting(t *testing.T) {
	root := t.TempDir()
	state := fixtureState()
	recognized := collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd)
	installArtifact(t, root, recognized)
	state.Graphs[0].NodeShards[0].JSONL.Path = "../outside"

	err := (Store{Root: root}).CleanupOrphans(state)
	require.ErrorContains(t, err, "path")
	requireArtifactExists(t, root, recognized)
}

func TestCleanupOrphansHandlesEveryOutputCombination(t *testing.T) {
	tests := []struct {
		name    string
		jsonl   bool
		parquet bool
	}{
		{name: "JSONL only", jsonl: true},
		{name: "Parquet only", parquet: true},
		{name: "dual", jsonl: true, parquet: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			state := fixtureStateForOutputs(test.jsonl, test.parquet)
			var paths []string
			if test.jsonl {
				paths = append(paths, collection.NodeJSONLPath("alpha", 2, jsonl.CodecZstd))
			}
			if test.parquet {
				paths = append(paths, collection.NodeParquetPath("alpha", 2))
			}
			for _, path := range paths {
				installArtifact(t, root, path)
			}

			require.NoError(t, (Store{Root: root}).CleanupOrphans(state))
			for _, path := range paths {
				requireArtifactMissing(t, root, path)
			}
		})
	}
}

func installArtifact(t *testing.T, root, relative string) {
	t.Helper()
	absolute := filepath.Join(root, filepath.FromSlash(relative))
	require.NoError(t, os.MkdirAll(filepath.Dir(absolute), 0o700))
	require.NoError(t, os.WriteFile(absolute, []byte("artifact"), 0o600))
}

func requireArtifactExists(t *testing.T, root, relative string) {
	t.Helper()
	_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(relative)))
	require.NoError(t, err)
}

func requireArtifactMissing(t *testing.T, root, relative string) {
	t.Helper()
	_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(relative)))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func checkpointStagingNames(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), FileName+".tmp") {
			names = append(names, entry.Name())
		}
	}
	return names
}

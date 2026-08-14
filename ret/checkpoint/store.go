package checkpoint

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
)

const checkpointStagingPrefix = FileName + ".tmp-"

var (
	checkpointRename       = os.Rename
	checkpointRemove       = os.Remove
	checkpointOpenRoot     = os.OpenRoot
	checkpointPinnedRemove = func(root *os.Root, name string) error {
		return root.Remove(name)
	}
)

type orphanRemoval struct {
	relative string
	basename string
	parent   *os.Root
}

func (s Store) Load() (State, bool, error) {
	if err := s.validateRoot(); err != nil {
		return State{}, true, err
	}
	file, err := os.Open(filepath.Join(s.Root, FileName))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, true, fmt.Errorf("open checkpoint: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var state State
	if err := decoder.Decode(&state); err != nil {
		return State{}, true, fmt.Errorf("decode checkpoint: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return State{}, true, fmt.Errorf("decode checkpoint: trailing JSON value")
		}
		return State{}, true, fmt.Errorf("decode checkpoint trailing data: %w", err)
	}
	if err := validateState(state); err != nil {
		return State{}, true, fmt.Errorf("validate checkpoint: %w", err)
	}
	return state, true, nil
}

func (s Store) Save(state State) (resultErr error) {
	if err := validateState(state); err != nil {
		return fmt.Errorf("validate checkpoint: %w", err)
	}
	if err := s.validateRoot(); err != nil {
		return err
	}

	final := filepath.Join(s.Root, FileName)
	file, err := os.CreateTemp(s.Root, checkpointStagingPrefix+"*")
	if err != nil {
		return fmt.Errorf("create temporary checkpoint: %w", err)
	}
	temporary := file.Name()
	published := false
	defer func() {
		joined := []error{resultErr}
		if file != nil {
			if closeErr := file.Close(); closeErr != nil {
				joined = append(joined, fmt.Errorf("cleanup close temporary checkpoint: %w", closeErr))
			}
		}
		if !published {
			if removeErr := checkpointRemove(temporary); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				joined = append(joined, fmt.Errorf("cleanup remove temporary checkpoint: %w", removeErr))
			}
		}
		resultErr = errors.Join(joined...)
	}()

	if err := json.NewEncoder(file).Encode(state); err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary checkpoint: %w", err)
	}
	if err := file.Close(); err != nil {
		file = nil
		return fmt.Errorf("close temporary checkpoint: %w", err)
	}
	file = nil
	if err := checkpointRename(temporary, final); err != nil {
		return fmt.Errorf("publish checkpoint: %w", err)
	}
	published = true
	return nil
}

func (s Store) Remove() error {
	if err := s.validateRoot(); err != nil {
		return err
	}
	if err := checkpointRemove(filepath.Join(s.Root, FileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove checkpoint: %w", err)
	}
	return nil
}

func (s Store) CleanupOrphans(state State) error {
	if err := validateState(state); err != nil {
		return fmt.Errorf("validate checkpoint before orphan cleanup: %w", err)
	}
	if err := s.validateRoot(); err != nil {
		return err
	}

	rootInfo, err := os.Lstat(s.Root)
	if err != nil {
		return fmt.Errorf("inspect checkpoint collection root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("checkpoint collection root is a symbolic link")
	}
	openRoot, err := checkpointOpenRoot(s.Root)
	if err != nil {
		return fmt.Errorf("open checkpoint collection root: %w", err)
	}
	pinnedRootInfo, err := openRoot.Stat(".")
	if err != nil {
		return errors.Join(
			fmt.Errorf("inspect pinned checkpoint collection root: %w", err),
			wrapRootCloseError("checkpoint collection root", openRoot.Close()),
		)
	}
	if !rootInfo.IsDir() || !pinnedRootInfo.IsDir() || !os.SameFile(rootInfo, pinnedRootInfo) {
		return errors.Join(
			fmt.Errorf("checkpoint collection root changed while opening"),
			wrapRootCloseError("checkpoint collection root", openRoot.Close()),
		)
	}

	committed, candidates := cleanupPaths(state)
	allowedDirectories := cleanupDirectories(state.Identity)
	directoryIdentities := make(map[string]fs.FileInfo, len(allowedDirectories))
	var removals []orphanRemoval
	walkErr := fs.WalkDir(openRoot.FS(), ".", func(relative string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("resume entry %q is a symbolic link", relative)
		}
		if entry.IsDir() {
			if _, ok := allowedDirectories[relative]; !ok {
				return fmt.Errorf("unknown resume directory %q", relative)
			}
			info, err := entry.Info()
			if err != nil {
				return fmt.Errorf("inspect resume directory %q: %w", relative, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("resume directory %q changed during inventory", relative)
			}
			directoryIdentities[relative] = info
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect resume entry %q: %w", relative, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("resume entry %q is not a regular file", relative)
		}
		if relative == FileName {
			return nil
		}
		if _, ok := committed[relative]; ok {
			return nil
		}
		if isCheckpointStaging(relative) || isCleanupCandidate(relative, candidates) {
			parentPath := path.Dir(relative)
			parentIdentity, ok := directoryIdentities[parentPath]
			if !ok {
				return fmt.Errorf("resume candidate parent %q was not inventoried", parentPath)
			}
			parentRoot, err := openRoot.OpenRoot(parentPath)
			if err != nil {
				return fmt.Errorf("pin resume candidate parent %q: %w", parentPath, err)
			}
			pinnedParentInfo, err := parentRoot.Stat(".")
			if err != nil {
				return errors.Join(
					fmt.Errorf("inspect pinned resume candidate parent %q: %w", parentPath, err),
					wrapRootCloseError("resume candidate parent "+parentPath, parentRoot.Close()),
				)
			}
			if !pinnedParentInfo.IsDir() || !os.SameFile(parentIdentity, pinnedParentInfo) {
				return errors.Join(
					fmt.Errorf("resume candidate parent %q changed while pinning", parentPath),
					wrapRootCloseError("resume candidate parent "+parentPath, parentRoot.Close()),
				)
			}
			removals = append(removals, orphanRemoval{
				relative: relative,
				basename: path.Base(relative),
				parent:   parentRoot,
			})
			return nil
		}
		return fmt.Errorf("unknown resume file %q", relative)
	})
	if walkErr != nil {
		return errors.Join(
			fmt.Errorf("inventory checkpoint collection: %w", walkErr),
			closeOrphanRemovalRoots(removals),
			wrapRootCloseError("checkpoint collection root", openRoot.Close()),
		)
	}

	var removeErrs []error
	for _, removal := range removals {
		if err := checkpointPinnedRemove(removal.parent, removal.basename); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErrs = append(removeErrs, fmt.Errorf("remove uncommitted artifact %q: %w", removal.relative, err))
		}
	}
	removeErrs = append(removeErrs, closeOrphanRemovalRoots(removals))
	removeErrs = append(removeErrs, wrapRootCloseError("checkpoint collection root", openRoot.Close()))
	return errors.Join(removeErrs...)
}

func closeOrphanRemovalRoots(removals []orphanRemoval) error {
	var closeErrs []error
	for _, removal := range removals {
		closeErrs = append(
			closeErrs,
			wrapRootCloseError("resume candidate parent "+path.Dir(removal.relative), removal.parent.Close()),
		)
	}
	return errors.Join(closeErrs...)
}

func wrapRootCloseError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close %s: %w", name, err)
}

func (s Store) validateRoot() error {
	if s.Root == "" {
		return fmt.Errorf("checkpoint root is empty")
	}
	return nil
}

func validateState(state State) error {
	if state.Format != Format {
		return fmt.Errorf("checkpoint format %q does not match %q", state.Format, Format)
	}
	if err := validateIdentity(state.Identity); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if len(state.Graphs) == 0 {
		return fmt.Errorf("checkpoint must contain at least one graph state")
	}
	if len(state.Graphs) > len(state.Identity.Graphs) {
		return fmt.Errorf("checkpoint graph state count %d exceeds identity graph count %d", len(state.Graphs), len(state.Identity.Graphs))
	}

	seen := make(map[string]struct{}, len(state.Graphs))
	for index, graph := range state.Graphs {
		if _, ok := seen[graph.Name]; ok {
			return fmt.Errorf("duplicate graph state %q", graph.Name)
		}
		seen[graph.Name] = struct{}{}
		if graph.Name != state.Identity.Graphs[index] {
			return fmt.Errorf(
				"graph state order at position %d: got %q want %q",
				index+1,
				graph.Name,
				state.Identity.Graphs[index],
			)
		}
	}

	artifactPaths := make(map[string]struct{})
	for index, graph := range state.Graphs {
		if index < len(state.Graphs)-1 && graph.Phase != PhaseComplete {
			return fmt.Errorf("graph %q phase progression: only the last graph state may be incomplete", graph.Name)
		}
		if err := validateGraphState(graph, state.Identity, artifactPaths); err != nil {
			return fmt.Errorf("graph %q: %w", graph.Name, err)
		}
	}
	return nil
}

func validateIdentity(identity Identity) error {
	if len(identity.Graphs) == 0 {
		return fmt.Errorf("at least one graph is required")
	}
	seen := make(map[string]struct{}, len(identity.Graphs))
	for index, graph := range identity.Graphs {
		if err := validateGraphName(graph); err != nil {
			return fmt.Errorf("graph %d must be a safe name: %w", index+1, err)
		}
		if _, ok := seen[graph]; ok {
			return fmt.Errorf("duplicate graph name %q", graph)
		}
		seen[graph] = struct{}{}
	}
	if identity.EntityBatchSize <= 0 {
		return fmt.Errorf("entity batch size must be positive")
	}
	if identity.ShardSize <= 0 {
		return fmt.Errorf("shard size must be positive")
	}
	if !identity.JSONLEnabled && !identity.ParquetEnabled {
		return fmt.Errorf("at least one output must be enabled")
	}
	if identity.JSONLSchemaVersion != jsonl.SchemaVersion {
		return fmt.Errorf("JSONL schema %q does not match %q", identity.JSONLSchemaVersion, jsonl.SchemaVersion)
	}
	if identity.ParquetSchemaVersion != parquet.SchemaVersion {
		return fmt.Errorf("Parquet schema %q does not match %q", identity.ParquetSchemaVersion, parquet.SchemaVersion)
	}
	if identity.JSONLEnabled {
		if err := (jsonl.Config{
			Codec: jsonl.Codec(identity.JSONLCodec),
			Level: identity.JSONLLevel,
		}).Validate(); err != nil {
			return fmt.Errorf("JSONL output: %w", err)
		}
	}
	if identity.ScrubEnabled {
		if !isLowerHexDigest(identity.ScrubRulesFingerprint) || !isLowerHexDigest(identity.ScrubSaltFingerprint) {
			return fmt.Errorf("enabled scrub fingerprints must be 64 lowercase hexadecimal characters")
		}
	} else if identity.ScrubRulesFingerprint != "" || identity.ScrubSaltFingerprint != "" {
		return fmt.Errorf("disabled scrub must not contain fingerprints")
	}
	return nil
}

func validateGraphName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("name is empty")
	}
	if name == "." || name == ".." || path.Clean(name) != name {
		return fmt.Errorf("%q is not a clean path segment", name)
	}
	if strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, '\x00') {
		return fmt.Errorf("%q is not a single path segment", name)
	}
	return nil
}

func validateGraphState(graph GraphState, identity Identity, artifactPaths map[string]struct{}) error {
	if graph.Snapshot.NodeCount < 0 || graph.Snapshot.RelationshipCount < 0 {
		return fmt.Errorf(
			"snapshot counts must be nonnegative: nodes=%d relationships=%d",
			graph.Snapshot.NodeCount,
			graph.Snapshot.RelationshipCount,
		)
	}
	nodeTotal, nodeCursor, err := validateNodeShards(graph.Name, graph.NodeShards, identity, artifactPaths)
	if err != nil {
		return err
	}
	relationshipTotal, relationshipCursor, err := validateRelationshipShards(
		graph.Name,
		graph.RelationshipShards,
		identity,
		artifactPaths,
	)
	if err != nil {
		return err
	}
	if graph.NodeCursor != nodeCursor {
		return fmt.Errorf("node cursor %d does not match last committed node shard cursor %d", graph.NodeCursor, nodeCursor)
	}
	if graph.RelationshipCursor != relationshipCursor {
		return fmt.Errorf(
			"relationship cursor %d does not match last committed relationship shard cursor %d",
			graph.RelationshipCursor,
			relationshipCursor,
		)
	}
	if nodeTotal > graph.Snapshot.NodeCount || relationshipTotal > graph.Snapshot.RelationshipCount {
		return fmt.Errorf(
			"committed shard totals exceed snapshot: nodes=%d/%d relationships=%d/%d",
			nodeTotal,
			graph.Snapshot.NodeCount,
			relationshipTotal,
			graph.Snapshot.RelationshipCount,
		)
	}
	if len(graph.NodeShards) != 0 &&
		nodeTotal < graph.Snapshot.NodeCount &&
		graph.NodeShards[len(graph.NodeShards)-1].Count != int64(identity.ShardSize) {
		return fmt.Errorf(
			"partial node shard is only legal at the snapshot boundary: got %d want %d",
			graph.NodeShards[len(graph.NodeShards)-1].Count,
			identity.ShardSize,
		)
	}
	if len(graph.RelationshipShards) != 0 &&
		relationshipTotal < graph.Snapshot.RelationshipCount &&
		graph.RelationshipShards[len(graph.RelationshipShards)-1].Count != int64(identity.ShardSize) {
		return fmt.Errorf(
			"partial relationship shard is only legal at the snapshot boundary: got %d want %d",
			graph.RelationshipShards[len(graph.RelationshipShards)-1].Count,
			identity.ShardSize,
		)
	}
	switch graph.Phase {
	case PhaseNodes:
		if relationshipTotal != 0 || graph.RelationshipCursor != 0 {
			return fmt.Errorf("nodes phase must not contain committed relationship progress")
		}
	case PhaseRelationships:
		if nodeTotal != graph.Snapshot.NodeCount {
			return fmt.Errorf(
				"relationships phase requires all snapshot nodes committed: got %d want %d",
				nodeTotal,
				graph.Snapshot.NodeCount,
			)
		}
	case PhaseComplete:
		if nodeTotal != graph.Snapshot.NodeCount || relationshipTotal != graph.Snapshot.RelationshipCount {
			return fmt.Errorf(
				"complete phase requires snapshot totals: nodes=%d/%d relationships=%d/%d",
				nodeTotal,
				graph.Snapshot.NodeCount,
				relationshipTotal,
				graph.Snapshot.RelationshipCount,
			)
		}
	default:
		return fmt.Errorf("unsupported phase %q", graph.Phase)
	}
	return nil
}

func validateNodeShards(
	graph string,
	shards []collection.NodeShard,
	identity Identity,
	artifactPaths map[string]struct{},
) (int64, uint64, error) {
	var total int64
	var cursor uint64
	for offset, shard := range shards {
		if err := validateLogicalShard(
			"node",
			offset,
			shard.Index,
			shard.Count,
			shard.LastSourceID,
			cursor,
			shard.ScrubCounts,
			shard.JSONL != nil,
			shard.Parquet != nil,
			identity,
		); err != nil {
			return 0, 0, err
		}
		if offset < len(shards)-1 && shard.Count != int64(identity.ShardSize) {
			return 0, 0, fmt.Errorf(
				"node shard %d count %d does not match configured shard size %d",
				shard.Index,
				shard.Count,
				identity.ShardSize,
			)
		}
		if shard.JSONL != nil {
			expected := collection.NodeJSONLPath(graph, shard.Index, jsonl.Codec(identity.JSONLCodec))
			if err := validateJSONLArtifact(
				"node",
				shard.Index,
				shard.Count,
				shard.JSONL.SchemaVersion,
				shard.JSONL.Path,
				string(shard.JSONL.Codec),
				shard.JSONL.SHA256,
				shard.JSONL.Level,
				shard.JSONL.Count,
				shard.JSONL.UncompressedBytes,
				shard.JSONL.StoredBytes,
				expected,
				identity,
				artifactPaths,
			); err != nil {
				return 0, 0, err
			}
		}
		if shard.Parquet != nil {
			expected := collection.NodeParquetPath(graph, shard.Index)
			if err := validateParquetArtifact(
				"node",
				shard.Index,
				shard.Count,
				shard.Parquet.SchemaVersion,
				shard.Parquet.Path,
				shard.Parquet.SHA256,
				shard.Parquet.Count,
				shard.Parquet.StoredBytes,
				expected,
				identity,
				artifactPaths,
			); err != nil {
				return 0, 0, err
			}
		}
		if total > math.MaxInt64-shard.Count {
			return 0, 0, fmt.Errorf("node shard total overflows int64")
		}
		total += shard.Count
		cursor = shard.LastSourceID
	}
	return total, cursor, nil
}

func validateRelationshipShards(
	graph string,
	shards []collection.RelationshipShard,
	identity Identity,
	artifactPaths map[string]struct{},
) (int64, uint64, error) {
	var total int64
	var cursor uint64
	for offset, shard := range shards {
		if err := validateLogicalShard(
			"relationship",
			offset,
			shard.Index,
			shard.Count,
			shard.LastSourceID,
			cursor,
			shard.ScrubCounts,
			shard.JSONL != nil,
			shard.Parquet != nil,
			identity,
		); err != nil {
			return 0, 0, err
		}
		if offset < len(shards)-1 && shard.Count != int64(identity.ShardSize) {
			return 0, 0, fmt.Errorf(
				"relationship shard %d count %d does not match configured shard size %d",
				shard.Index,
				shard.Count,
				identity.ShardSize,
			)
		}
		if shard.JSONL != nil {
			expected := collection.RelationshipJSONLPath(graph, shard.Index, jsonl.Codec(identity.JSONLCodec))
			if err := validateJSONLArtifact(
				"relationship",
				shard.Index,
				shard.Count,
				shard.JSONL.SchemaVersion,
				shard.JSONL.Path,
				string(shard.JSONL.Codec),
				shard.JSONL.SHA256,
				shard.JSONL.Level,
				shard.JSONL.Count,
				shard.JSONL.UncompressedBytes,
				shard.JSONL.StoredBytes,
				expected,
				identity,
				artifactPaths,
			); err != nil {
				return 0, 0, err
			}
		}
		if shard.Parquet != nil {
			expected := collection.RelationshipParquetPath(graph, shard.Index)
			if err := validateParquetArtifact(
				"relationship",
				shard.Index,
				shard.Count,
				shard.Parquet.SchemaVersion,
				shard.Parquet.Path,
				shard.Parquet.SHA256,
				shard.Parquet.Count,
				shard.Parquet.StoredBytes,
				expected,
				identity,
				artifactPaths,
			); err != nil {
				return 0, 0, err
			}
		}
		if total > math.MaxInt64-shard.Count {
			return 0, 0, fmt.Errorf("relationship shard total overflows int64")
		}
		total += shard.Count
		cursor = shard.LastSourceID
	}
	return total, cursor, nil
}

func validateLogicalShard(
	entityType string,
	offset, index int,
	count int64,
	cursor, previousCursor uint64,
	counts scrub.ActionCounts,
	hasJSONL, hasParquet bool,
	identity Identity,
) error {
	if index != offset+1 {
		return fmt.Errorf("%s shard index: got %d want %d", entityType, index, offset+1)
	}
	if count <= 0 {
		return fmt.Errorf("%s shard %d count must be positive", entityType, index)
	}
	if count > int64(identity.ShardSize) {
		return fmt.Errorf(
			"%s shard %d count %d exceeds configured shard size %d",
			entityType,
			index,
			count,
			identity.ShardSize,
		)
	}
	if cursor == 0 {
		return fmt.Errorf("%s shard %d last source ID must be nonzero", entityType, index)
	}
	if previousCursor != 0 && cursor <= previousCursor {
		return fmt.Errorf(
			"%s shard %d last source ID %d does not increase after %d",
			entityType,
			index,
			cursor,
			previousCursor,
		)
	}
	if hasJSONL != identity.JSONLEnabled || hasParquet != identity.ParquetEnabled {
		return fmt.Errorf("%s shard %d output mismatch with checkpoint identity", entityType, index)
	}
	if !identity.ScrubEnabled && !counts.IsZero() {
		return fmt.Errorf("%s shard %d has scrub counts while scrubbing is disabled", entityType, index)
	}
	if counts.Preserve < 0 {
		return fmt.Errorf("%s shard %d has invalid scrub count %q=%d", entityType, index, "preserve", counts.Preserve)
	}
	if counts.Pseudonymize < 0 {
		return fmt.Errorf("%s shard %d has invalid scrub count %q=%d", entityType, index, "pseudonymize", counts.Pseudonymize)
	}
	if counts.Redact < 0 {
		return fmt.Errorf("%s shard %d has invalid scrub count %q=%d", entityType, index, "redact", counts.Redact)
	}
	if counts.ShiftTimestamp < 0 {
		return fmt.Errorf("%s shard %d has invalid scrub count %q=%d", entityType, index, "shift_timestamp", counts.ShiftTimestamp)
	}
	return nil
}

func validateJSONLArtifact(
	entityType string,
	index int,
	shardCount int64,
	schema, artifactPath, codec, digest string,
	level int,
	count, uncompressedBytes, storedBytes int64,
	expectedPath string,
	identity Identity,
	artifactPaths map[string]struct{},
) error {
	prefix := fmt.Sprintf("%s shard %d JSONL", entityType, index)
	if schema != identity.JSONLSchemaVersion {
		return fmt.Errorf("%s schema %q does not match %q", prefix, schema, identity.JSONLSchemaVersion)
	}
	if codec != identity.JSONLCodec {
		return fmt.Errorf("%s codec %q does not match %q", prefix, codec, identity.JSONLCodec)
	}
	if level != identity.JSONLLevel {
		return fmt.Errorf("%s level %d does not match %d", prefix, level, identity.JSONLLevel)
	}
	if count != shardCount {
		return fmt.Errorf("%s count %d does not match shard count %d", prefix, count, shardCount)
	}
	if !isLowerHexDigest(digest) {
		return fmt.Errorf("%s SHA-256 must be 64 lowercase hexadecimal characters", prefix)
	}
	if uncompressedBytes <= 0 {
		return fmt.Errorf("%s uncompressed bytes must be positive", prefix)
	}
	if storedBytes <= 0 {
		return fmt.Errorf("%s stored bytes must be positive", prefix)
	}
	return validateArtifactPath(prefix, artifactPath, expectedPath, artifactPaths)
}

func validateParquetArtifact(
	entityType string,
	index int,
	shardCount int64,
	schema, artifactPath, digest string,
	count, storedBytes int64,
	expectedPath string,
	identity Identity,
	artifactPaths map[string]struct{},
) error {
	prefix := fmt.Sprintf("%s shard %d Parquet", entityType, index)
	if schema != identity.ParquetSchemaVersion {
		return fmt.Errorf("%s schema %q does not match %q", prefix, schema, identity.ParquetSchemaVersion)
	}
	if count != shardCount {
		return fmt.Errorf("%s count %d does not match shard count %d", prefix, count, shardCount)
	}
	if !isLowerHexDigest(digest) {
		return fmt.Errorf("%s SHA-256 must be 64 lowercase hexadecimal characters", prefix)
	}
	if storedBytes <= 0 {
		return fmt.Errorf("%s stored bytes must be positive", prefix)
	}
	return validateArtifactPath(prefix, artifactPath, expectedPath, artifactPaths)
}

func validateArtifactPath(prefix, artifactPath, expectedPath string, paths map[string]struct{}) error {
	if _, err := collection.SafeJoin(".", artifactPath); err != nil {
		return fmt.Errorf("%s path: %w", prefix, err)
	}
	if artifactPath != expectedPath {
		return fmt.Errorf("%s path %q does not match deterministic path %q", prefix, artifactPath, expectedPath)
	}
	if _, ok := paths[artifactPath]; ok {
		return fmt.Errorf("%s path %q is duplicated", prefix, artifactPath)
	}
	paths[artifactPath] = struct{}{}
	return nil
}

func isLowerHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	if _, err := hex.DecodeString(value); err != nil {
		return false
	}
	return strings.ToLower(value) == value
}

func cleanupPaths(state State) (map[string]struct{}, map[string]struct{}) {
	committed := make(map[string]struct{})
	candidates := make(map[string]struct{})
	for _, graph := range state.Graphs {
		for _, shard := range graph.NodeShards {
			if shard.JSONL != nil {
				committed[shard.JSONL.Path] = struct{}{}
			}
			if shard.Parquet != nil {
				committed[shard.Parquet.Path] = struct{}{}
			}
		}
		for _, shard := range graph.RelationshipShards {
			if shard.JSONL != nil {
				committed[shard.JSONL.Path] = struct{}{}
			}
			if shard.Parquet != nil {
				committed[shard.Parquet.Path] = struct{}{}
			}
		}

		switch graph.Phase {
		case PhaseNodes:
			if shardCountNodes(graph.NodeShards) < graph.Snapshot.NodeCount {
				index := len(graph.NodeShards) + 1
				addNodeCandidates(candidates, state.Identity, graph.Name, index)
			}
		case PhaseRelationships:
			if shardCountRelationships(graph.RelationshipShards) < graph.Snapshot.RelationshipCount {
				index := len(graph.RelationshipShards) + 1
				addRelationshipCandidates(candidates, state.Identity, graph.Name, index)
			}
		}
	}
	return committed, candidates
}

func addNodeCandidates(paths map[string]struct{}, identity Identity, graph string, index int) {
	if identity.JSONLEnabled {
		paths[collection.NodeJSONLPath(graph, index, jsonl.Codec(identity.JSONLCodec))] = struct{}{}
	}
	if identity.ParquetEnabled {
		paths[collection.NodeParquetPath(graph, index)] = struct{}{}
	}
}

func addRelationshipCandidates(paths map[string]struct{}, identity Identity, graph string, index int) {
	if identity.JSONLEnabled {
		paths[collection.RelationshipJSONLPath(graph, index, jsonl.Codec(identity.JSONLCodec))] = struct{}{}
	}
	if identity.ParquetEnabled {
		paths[collection.RelationshipParquetPath(graph, index)] = struct{}{}
	}
}

func shardCountNodes(shards []collection.NodeShard) int64 {
	var count int64
	for _, shard := range shards {
		count += shard.Count
	}
	return count
}

func shardCountRelationships(shards []collection.RelationshipShard) int64 {
	var count int64
	for _, shard := range shards {
		count += shard.Count
	}
	return count
}

func cleanupDirectories(identity Identity) map[string]struct{} {
	directories := map[string]struct{}{
		".":      {},
		"graphs": {},
	}
	for _, graph := range identity.Graphs {
		graphDirectory := "graphs/" + url.PathEscape(graph)
		directories[graphDirectory] = struct{}{}
		directories[graphDirectory+"/nodes"] = struct{}{}
		directories[graphDirectory+"/relationships"] = struct{}{}
	}
	return directories
}

func isCleanupCandidate(relative string, candidates map[string]struct{}) bool {
	if _, ok := candidates[relative]; ok {
		return true
	}
	for candidate := range candidates {
		prefix := candidate + ".tmp-"
		if strings.HasPrefix(relative, prefix) && isURLSafeNonce(strings.TrimPrefix(relative, prefix)) {
			return true
		}
	}
	return false
}

func isCheckpointStaging(relative string) bool {
	if !strings.HasPrefix(relative, checkpointStagingPrefix) {
		return false
	}
	return isURLSafeNonce(strings.TrimPrefix(relative, checkpointStagingPrefix))
}

func isURLSafeNonce(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

package retriever

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

const (
	dumpCheckpointFileName = ".retriever-checkpoint.json"
	dumpCheckpointVersion  = "retriever-dump-checkpoint-v1"
)

type dumpCheckpointIdentity struct {
	Driver            string           `json:"driver"`
	Graphs            []string         `json:"graphs"`
	Compression       CompressionCodec `json:"compression"`
	CompressionLevel  int              `json:"compression_level"`
	Scrub             ScrubMode        `json:"scrub"`
	ScrubRulesVersion string           `json:"scrub_rules_version,omitempty"`
	ScrubConfigSHA256 string           `json:"scrub_config_sha256,omitempty"`
	ScrubSaltSHA256   string           `json:"scrub_salt_sha256,omitempty"`
	ShardSize         int              `json:"shard_size"`
	BatchSize         int              `json:"batch_size"`
}

type dumpGraphCheckpoint struct {
	Index              int                 `json:"index"`
	Name               string              `json:"name"`
	Snapshot           graphEntitySnapshot `json:"snapshot"`
	HasSnapshot        bool                `json:"has_snapshot"`
	Phase              Phase               `json:"phase"`
	LastCommittedID    uint64              `json:"last_committed_id,omitempty"`
	HasLastCommittedID bool                `json:"has_last_committed_id"`
	Files              []FileManifest      `json:"files,omitempty"`
}

type dumpCheckpoint struct {
	Version  string                 `json:"version"`
	Identity dumpCheckpointIdentity `json:"identity"`
	Manifest Manifest               `json:"manifest"`
	Current  *dumpGraphCheckpoint   `json:"current_graph,omitempty"`
}

func newDumpCheckpointIdentity(driverName string, targets []GraphTarget, options DumpOptions, activeScrubber *scrubber) (dumpCheckpointIdentity, error) {
	identity := dumpCheckpointIdentity{
		Driver:           driverName,
		Graphs:           make([]string, len(targets)),
		Compression:      options.Compression,
		CompressionLevel: options.ZstdLevel,
		Scrub:            options.Scrub,
		ShardSize:        options.ShardSize,
		BatchSize:        options.BatchSize,
	}
	for index, target := range targets {
		identity.Graphs[index] = target.Name
	}

	if activeScrubber != nil {
		identity.ScrubRulesVersion = scrubRulesVersion
		config := activeScrubber.config
		salt := config.Salt
		config.Salt = ""
		payload, err := json.Marshal(config)
		if err != nil {
			return dumpCheckpointIdentity{}, fmt.Errorf("encode scrub checkpoint identity: %w", err)
		}
		identity.ScrubConfigSHA256 = sha256Hex(payload)
		identity.ScrubSaltSHA256 = sha256Hex([]byte(salt))
	}

	return identity, nil
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func writeDumpCheckpoint(outputDir string, value dumpCheckpoint) error {
	value.Version = dumpCheckpointVersion
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode dump checkpoint: %w", err)
	}
	payload = append(payload, '\n')

	tempPath := filepath.Join(outputDir, dumpCheckpointFileName+".tmp")
	finalPath := filepath.Join(outputDir, dumpCheckpointFileName)
	if err := os.WriteFile(tempPath, payload, 0o600); err != nil {
		return fmt.Errorf("write dump checkpoint temp file: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish dump checkpoint: %w", err)
	}

	return nil
}

func readDumpCheckpoint(outputDir string) (dumpCheckpoint, error) {
	var value dumpCheckpoint
	checkpointPath := filepath.Join(outputDir, dumpCheckpointFileName)
	payload, err := os.ReadFile(checkpointPath)
	if err != nil {
		return value, fmt.Errorf("read dump checkpoint: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return dumpCheckpoint{}, fmt.Errorf("decode dump checkpoint: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return dumpCheckpoint{}, fmt.Errorf("decode dump checkpoint: multiple JSON values")
		}
		return dumpCheckpoint{}, fmt.Errorf("decode dump checkpoint: %w", err)
	}
	if value.Version != dumpCheckpointVersion {
		return dumpCheckpoint{}, fmt.Errorf("unsupported dump checkpoint version %q", value.Version)
	}

	return value, nil
}

func loadCompatibleDumpCheckpoint(outputDir string, expected dumpCheckpointIdentity, targetCount int) (dumpCheckpoint, error) {
	if _, err := os.Stat(filepath.Join(outputDir, manifestFileName)); err == nil {
		return dumpCheckpoint{}, fmt.Errorf("dump output %q already contains a complete manifest", outputDir)
	} else if !os.IsNotExist(err) {
		return dumpCheckpoint{}, fmt.Errorf("inspect dump manifest: %w", err)
	}

	value, err := readDumpCheckpoint(outputDir)
	if err != nil {
		return dumpCheckpoint{}, err
	}
	if !reflect.DeepEqual(value.Identity, expected) {
		return dumpCheckpoint{}, fmt.Errorf("dump checkpoint is incompatible with the requested driver, graphs, scrub, compression, shard, or batch options")
	}
	if err := validateDumpCheckpoint(value, targetCount); err != nil {
		return dumpCheckpoint{}, err
	}
	if err := removeKnownDumpCheckpointTemps(outputDir, value); err != nil {
		return dumpCheckpoint{}, err
	}
	if err := validateDumpCheckpointFiles(outputDir, value); err != nil {
		return dumpCheckpoint{}, err
	}

	return value, nil
}

func validateDumpCheckpoint(value dumpCheckpoint, targetCount int) error {
	if value.Manifest.Format != manifestFormat {
		return fmt.Errorf("dump checkpoint contains unsupported manifest format %q", value.Manifest.Format)
	}
	if value.Manifest.Source.GraphCount != targetCount {
		return fmt.Errorf("dump checkpoint graph count %d does not match requested count %d", value.Manifest.Source.GraphCount, targetCount)
	}
	if len(value.Manifest.Graphs) > targetCount {
		return fmt.Errorf("dump checkpoint contains too many completed graphs")
	}
	if value.Manifest.Metrics == nil || len(value.Manifest.Metrics.Graphs) != len(value.Manifest.Graphs) || len(value.Manifest.Schema.Graphs) != len(value.Manifest.Graphs) {
		return fmt.Errorf("dump checkpoint completed graph, schema, and metrics entries are inconsistent")
	}
	completedManifest := value.Manifest
	completedManifest.Source.GraphCount = len(completedManifest.Graphs)
	if err := completedManifest.validate(); err != nil {
		return fmt.Errorf("dump checkpoint completed manifest is invalid: %w", err)
	}
	for index, graphEntry := range value.Manifest.Graphs {
		if index >= len(value.Identity.Graphs) || graphEntry.Name != value.Identity.Graphs[index] {
			return fmt.Errorf("dump checkpoint completed graph %d does not match requested target", index+1)
		}
		if err := validateDumpCheckpointFragmentPaths(value.Identity.Compression, graphEntry.Name, graphEntry.Files); err != nil {
			return err
		}
	}

	if value.Current == nil {
		return nil
	}
	current := value.Current
	if current.Index != len(value.Manifest.Graphs) || current.Index >= targetCount {
		return fmt.Errorf("dump checkpoint current graph index %d is inconsistent", current.Index)
	}
	if current.Name != value.Identity.Graphs[current.Index] {
		return fmt.Errorf("dump checkpoint current graph %q does not match requested target", current.Name)
	}
	if !current.HasSnapshot || current.Snapshot.NodeCount < 0 || current.Snapshot.EdgeCount < 0 {
		return fmt.Errorf("dump checkpoint current graph is missing a valid source snapshot")
	}
	if current.Phase != PhaseNodes && current.Phase != PhaseEdges {
		return fmt.Errorf("dump checkpoint current graph has unsupported phase %q", current.Phase)
	}

	seenEdge := false
	var nodeCount, edgeCount int64
	for _, fileEntry := range current.Files {
		if fileEntry.Count <= 0 {
			return fmt.Errorf("dump checkpoint fragment %q has invalid count %d", fileEntry.Path, fileEntry.Count)
		}
		switch fileEntry.Phase {
		case PhaseNodes:
			if seenEdge {
				return fmt.Errorf("dump checkpoint lists node fragment after edge fragment")
			}
			nodeCount += int64(fileEntry.Count)
		case PhaseEdges:
			seenEdge = true
			edgeCount += int64(fileEntry.Count)
		default:
			return fmt.Errorf("dump checkpoint fragment %q has unsupported phase %q", fileEntry.Path, fileEntry.Phase)
		}
	}
	if nodeCount > current.Snapshot.NodeCount || edgeCount > current.Snapshot.EdgeCount {
		return fmt.Errorf("dump checkpoint fragment counts exceed the source snapshot")
	}
	if current.Phase == PhaseEdges && nodeCount != current.Snapshot.NodeCount {
		return fmt.Errorf("dump checkpoint entered edge phase before all nodes were committed")
	}
	phaseCount := nodeCount
	if current.Phase == PhaseEdges {
		phaseCount = edgeCount
	}
	if (phaseCount > 0) != current.HasLastCommittedID {
		return fmt.Errorf("dump checkpoint committed cursor does not match its fragment count")
	}
	if err := validateDumpCheckpointFragmentPaths(value.Identity.Compression, current.Name, current.Files); err != nil {
		return err
	}

	return nil
}

func validateDumpCheckpointFragmentPaths(codec CompressionCodec, graphName string, files []FileManifest) error {
	phaseShards := map[Phase]int{}
	seen := map[string]struct{}{}
	for _, fileEntry := range files {
		phaseShards[fileEntry.Phase]++
		expectedPath, err := fragmentPath(graphName, fileEntry.Phase, phaseShards[fileEntry.Phase], codec)
		if err != nil {
			return err
		}
		if fileEntry.Path != expectedPath {
			return fmt.Errorf("dump checkpoint fragment path %q does not match committed shard path %q", fileEntry.Path, expectedPath)
		}
		if _, found := seen[fileEntry.Path]; found {
			return fmt.Errorf("dump checkpoint contains duplicate fragment path %q", fileEntry.Path)
		}
		seen[fileEntry.Path] = struct{}{}
	}
	return nil
}

func removeKnownDumpCheckpointTemps(outputDir string, value dumpCheckpoint) error {
	paths := []string{
		filepath.Join(outputDir, dumpCheckpointFileName+".tmp"),
		filepath.Join(outputDir, manifestFileName+".tmp"),
	}
	if value.Current != nil {
		phaseFiles := filterPhaseFiles(value.Current.Files, value.Current.Phase)
		nextPath, err := fragmentPath(value.Current.Name, value.Current.Phase, len(phaseFiles)+1, value.Identity.Compression)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.Join(outputDir, filepath.FromSlash(nextPath))+".tmp")
	}
	for _, candidate := range paths {
		if err := os.Remove(candidate); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale dump temporary file %q: %w", candidate, err)
		}
	}
	return nil
}

func validateDumpCheckpointFiles(outputDir string, value dumpCheckpoint) error {
	expected := map[string]struct{}{dumpCheckpointFileName: {}}
	for _, graphEntry := range value.Manifest.Graphs {
		for _, fileEntry := range graphEntry.Files {
			if _, found := expected[fileEntry.Path]; found {
				return fmt.Errorf("dump checkpoint contains duplicate fragment path %q", fileEntry.Path)
			}
			expected[fileEntry.Path] = struct{}{}
			if err := verifyDumpCheckpointFile(outputDir, fileEntry); err != nil {
				return err
			}
		}
	}
	if value.Current != nil {
		for _, fileEntry := range value.Current.Files {
			if _, found := expected[fileEntry.Path]; found {
				return fmt.Errorf("dump checkpoint contains duplicate fragment path %q", fileEntry.Path)
			}
			expected[fileEntry.Path] = struct{}{}
			if err := verifyDumpCheckpointFile(outputDir, fileEntry); err != nil {
				return err
			}
		}
	}

	return filepath.WalkDir(outputDir, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relativePath, err := filepath.Rel(outputDir, filePath)
		if err != nil {
			return err
		}
		relativePath = filepath.ToSlash(relativePath)
		if _, found := expected[relativePath]; !found {
			return fmt.Errorf("dump checkpoint output contains unexpected file %q; refusing to guess whether it is committed", relativePath)
		}
		return nil
	})
}

func verifyDumpCheckpointFile(outputDir string, fileEntry FileManifest) error {
	absolutePath := filepath.Join(outputDir, filepath.FromSlash(fileEntry.Path))
	info, err := os.Lstat(absolutePath)
	if err != nil {
		return fmt.Errorf("inspect dump checkpoint fragment %q: %w", fileEntry.Path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("dump checkpoint fragment %q is not a regular file", fileEntry.Path)
	}
	return verifyChecksum(absolutePath, fileEntry.SHA256, fileEntry.CompressedBytes)
}

func filterPhaseFiles(files []FileManifest, phase Phase) []FileManifest {
	result := make([]FileManifest, 0, len(files))
	for _, fileEntry := range files {
		if fileEntry.Phase == phase {
			result = append(result, fileEntry)
		}
	}
	return result
}

func removeDumpCheckpoint(outputDir string) error {
	if err := os.Remove(filepath.Join(outputDir, dumpCheckpointFileName)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove dump checkpoint: %w", err)
	}
	return nil
}

func checkpointActionCounts(files []FileManifest, phase Phase) scrubActionCounts {
	var result scrubActionCounts
	for _, fileEntry := range files {
		if fileEntry.Phase != phase {
			continue
		}
		for action, count := range fileEntry.ActionCounts {
			switch PropertyAction(action) {
			case actionPreserve:
				result.preserve += count
			case actionPseudonymize:
				result.pseudonymize += count
			case actionRedact:
				result.redact += count
			case actionShiftTimestamp:
				result.shift += count
			}
		}
	}
	return result
}

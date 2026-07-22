package retriever

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const manifestFileName = "manifest.json"

const ManifestFileName = manifestFileName

func ReadManifest(inputDir string) (Manifest, error) {
	return readManifest(inputDir)
}

func readManifest(inputDir string) (Manifest, error) {
	var value Manifest

	manifestPath := filepath.Join(inputDir, manifestFileName)
	if contents, err := os.ReadFile(manifestPath); err != nil {
		return value, fmt.Errorf("read manifest: %w", err)
	} else if err := json.Unmarshal(contents, &value); err != nil {
		return value, fmt.Errorf("decode manifest: %w", err)
	} else if err := value.validate(); err != nil {
		return value, err
	}

	return value, nil
}

func WriteManifest(outputDir string, value Manifest) error {
	return writeManifest(outputDir, value)
}

func writeManifest(outputDir string, value Manifest) error {
	if err := value.validate(); err != nil {
		return err
	}

	tempPath := filepath.Join(outputDir, manifestFileName+".tmp")
	finalPath := filepath.Join(outputDir, manifestFileName)
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	payload = append(payload, '\n')

	if err := os.WriteFile(tempPath, payload, 0o600); err != nil {
		return fmt.Errorf("write manifest temp file: %w", err)
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename manifest: %w", err)
	}

	return nil
}

func VerifyManifestFiles(inputDir string, value Manifest) error {
	return verifyManifestFiles(inputDir, value)
}

func verifyManifestFiles(inputDir string, value Manifest) error {
	for _, graphEntry := range value.Graphs {
		for _, fileEntry := range graphEntry.Files {
			if err := verifyChecksum(filepath.Join(inputDir, filepath.FromSlash(fileEntry.Path)), fileEntry.SHA256, fileEntry.CompressedBytes); err != nil {
				return err
			}
		}
	}

	return nil
}

func manifestFragmentBytes(value Manifest) (int64, int64) {
	var compressedBytes, uncompressedBytes int64
	for _, graphEntry := range value.Graphs {
		for _, fileEntry := range graphEntry.Files {
			compressedBytes += fileEntry.CompressedBytes
			uncompressedBytes += fileEntry.UncompressedBytes
		}
	}

	return compressedBytes, uncompressedBytes
}

func graphPhaseFragmentBytes(value GraphManifest, phase Phase) (int64, int64) {
	var compressedBytes, uncompressedBytes int64
	for _, fileEntry := range value.Files {
		if fileEntry.Phase == phase {
			compressedBytes += fileEntry.CompressedBytes
			uncompressedBytes += fileEntry.UncompressedBytes
		}
	}
	return compressedBytes, uncompressedBytes
}

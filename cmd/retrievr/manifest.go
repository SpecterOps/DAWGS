package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const manifestFileName = "manifest.json"

func readManifest(inputDir string) (manifest, error) {
	var value manifest

	manifestPath := filepath.Join(inputDir, manifestFileName)
	contents, err := os.ReadFile(manifestPath)
	if err != nil {
		return value, fmt.Errorf("read manifest: %w", err)
	}
	if err := json.Unmarshal(contents, &value); err != nil {
		return value, fmt.Errorf("decode manifest: %w", err)
	}
	if err := value.validate(); err != nil {
		return value, err
	}
	return value, nil
}

func writeManifest(outputDir string, value manifest) error {
	if err := value.validate(); err != nil {
		return err
	}

	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	payload = append(payload, '\n')

	tempPath := filepath.Join(outputDir, manifestFileName+".tmp")
	finalPath := filepath.Join(outputDir, manifestFileName)
	if err := os.WriteFile(tempPath, payload, 0o600); err != nil {
		return fmt.Errorf("write manifest temp file: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}

func verifyManifestFiles(inputDir string, value manifest) error {
	for _, graphEntry := range value.Graphs {
		for _, fileEntry := range graphEntry.Files {
			if err := verifyChecksum(filepath.Join(inputDir, filepath.FromSlash(fileEntry.Path)), fileEntry.SHA256, fileEntry.CompressedBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

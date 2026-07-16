package retriever

import (
	"context"
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
	payload, err := encodeManifest(value)
	if err != nil {
		return err
	}

	_, err = newLocalCollectionWorkspace(outputDir, false).Publish(context.Background(), manifestFileName, payload)
	return err
}

func encodeManifest(value Manifest) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}

	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}

	payload = append(payload, '\n')
	return payload, nil
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

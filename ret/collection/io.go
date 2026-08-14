package collection

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func Read(root string) (Manifest, error) {
	manifestPath := filepath.Join(root, ManifestName)
	file, err := os.Open(manifestPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("open manifest: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()

	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, fmt.Errorf("decode manifest: trailing JSON value")
		}
		return Manifest{}, fmt.Errorf("decode manifest trailing data: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("validate manifest: %w", err)
	}

	return manifest, nil
}

func Write(root string, manifest Manifest) error {
	return writeWithEncoder(root, manifest, func(writer io.Writer, value Manifest) error {
		if err := json.NewEncoder(writer).Encode(value); err != nil {
			return fmt.Errorf("encode manifest: %w", err)
		}
		return nil
	}, os.Remove)
}

func writeWithEncoder(
	root string,
	manifest Manifest,
	encode func(io.Writer, Manifest) error,
	remove func(string) error,
) (resultErr error) {
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("validate manifest: %w", err)
	}

	temporary := filepath.Join(root, ManifestName+".tmp")
	final := filepath.Join(root, ManifestName)
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary manifest: %w", err)
	}
	published := false
	defer func() {
		errorsToJoin := []error{resultErr}
		if file != nil {
			if closeErr := file.Close(); closeErr != nil {
				errorsToJoin = append(errorsToJoin, fmt.Errorf("cleanup close temporary manifest: %w", closeErr))
			}
		}
		if !published {
			if removeErr := remove(temporary); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				errorsToJoin = append(errorsToJoin, fmt.Errorf("cleanup remove temporary manifest: %w", removeErr))
			}
		}
		resultErr = errors.Join(errorsToJoin...)
	}()

	if err := encode(file, manifest); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		file = nil
		return fmt.Errorf("close temporary manifest: %w", err)
	}
	file = nil

	if err := os.Rename(temporary, final); err != nil {
		return fmt.Errorf("publish manifest: %w", err)
	}
	published = true

	return nil
}

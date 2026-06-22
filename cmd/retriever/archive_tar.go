package main

import (
	"archive/tar"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const archiveFileMode int64 = 0o600

var archiveZeroTime = time.Unix(0, 0).UTC()

func writeCollectionTar(writer io.Writer, dumpDir string, excludePaths ...string) error {
	slog.Info("retriever archive walking started",
		slog.String("input_dir", dumpDir),
	)
	nextManifest, err := readManifest(dumpDir)
	if err != nil {
		return err
	}
	archivePaths, err := archivePathsFromManifest(nextManifest)
	if err != nil {
		return err
	}
	if err := validateArchiveExcludesDoNotReplaceCollectionFiles(dumpDir, archivePaths, excludePaths); err != nil {
		return err
	}
	slog.Info("retriever archive walking completed",
		slog.String("input_dir", dumpDir),
		slog.Int("file_count", len(archivePaths)),
	)

	tarWriter := tar.NewWriter(writer)
	slog.Info("retriever archive tar streaming started",
		slog.String("input_dir", dumpDir),
		slog.Int("file_count", len(archivePaths)),
	)
	for _, relativePath := range archivePaths {
		if err := writeCollectionTarFile(tarWriter, dumpDir, relativePath); err != nil {
			_ = tarWriter.Close()
			return err
		}
	}
	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("finish tar stream: %w", err)
	}
	slog.Info("retriever archive tar streaming completed",
		slog.String("input_dir", dumpDir),
		slog.Int("file_count", len(archivePaths)),
	)
	return nil
}

func archivePathsFromManifest(value manifest) ([]string, error) {
	paths := []string{manifestFileName}
	for _, graphEntry := range value.Graphs {
		for _, fileEntry := range graphEntry.Files {
			paths = append(paths, fileEntry.Path)
		}
	}

	seen := make(map[string]struct{}, len(paths))
	normalized := make([]string, 0, len(paths))
	for _, candidate := range paths {
		relativePath, err := sanitizeArchivePath(candidate)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[relativePath]; ok {
			return nil, fmt.Errorf("archive contains duplicate path %q", relativePath)
		}
		seen[relativePath] = struct{}{}
		normalized = append(normalized, relativePath)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func validateArchiveExcludesDoNotReplaceCollectionFiles(dumpDir string, archivePaths []string, excludePaths []string) error {
	if len(excludePaths) == 0 {
		return nil
	}

	collectionPaths := make(map[string]string, len(archivePaths))
	for _, relativePath := range archivePaths {
		absolutePath, err := filepath.Abs(filepath.Join(dumpDir, filepath.FromSlash(relativePath)))
		if err != nil {
			return fmt.Errorf("resolve archive path %q: %w", relativePath, err)
		}
		collectionPaths[filepath.Clean(absolutePath)] = relativePath
	}
	for _, excludePath := range excludePaths {
		if strings.TrimSpace(excludePath) == "" {
			continue
		}
		absolutePath, err := filepath.Abs(excludePath)
		if err != nil {
			return fmt.Errorf("resolve archive output path %q: %w", excludePath, err)
		}
		if relativePath, ok := collectionPaths[filepath.Clean(absolutePath)]; ok {
			return fmt.Errorf("archive output path %q would replace collection file %q", excludePath, relativePath)
		}
	}
	return nil
}

func writeCollectionTarFile(tarWriter *tar.Writer, dumpDir, relativePath string) error {
	absolutePath := filepath.Join(dumpDir, filepath.FromSlash(relativePath))
	info, err := os.Lstat(absolutePath)
	if err != nil {
		return fmt.Errorf("inspect archive file %q: %w", relativePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("archive file %q is a symlink; refusing to package it", relativePath)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("archive file %q is not a regular file", relativePath)
	}

	header := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     relativePath,
		Mode:     archiveFileMode,
		Uid:      0,
		Gid:      0,
		Uname:    "",
		Gname:    "",
		Size:     info.Size(),
		ModTime:  archiveZeroTime,
		Format:   tar.FormatUSTAR,
	}
	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("write tar header for %q: %w", relativePath, err)
	}

	file, err := os.Open(absolutePath)
	if err != nil {
		return fmt.Errorf("open archive file %q: %w", relativePath, err)
	}
	defer file.Close()

	copied, err := io.Copy(tarWriter, file)
	if err != nil {
		return fmt.Errorf("write tar file %q: %w", relativePath, err)
	}
	if copied != info.Size() {
		return fmt.Errorf("archive file %q changed while packaging: expected %d bytes, copied %d", relativePath, info.Size(), copied)
	}
	return nil
}

func unpackTar(reader io.Reader, outputDir string, force bool) error {
	slog.Info("retriever archive unpacking started",
		slog.String("output_dir", outputDir),
		slog.Bool("force", force),
	)
	if err := prepareOutputDirectory(outputDir, force); err != nil {
		return err
	}

	tarReader := tar.NewReader(reader)
	seen := map[string]struct{}{}
	var fileCount int
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}
		relativePath, err := sanitizeArchivePath(header.Name)
		if err != nil {
			return err
		}
		if _, ok := seen[relativePath]; ok {
			return fmt.Errorf("tar archive contains duplicate path %q", relativePath)
		}
		seen[relativePath] = struct{}{}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return fmt.Errorf("tar archive path %q is not a regular file", relativePath)
		}
		if header.Size < 0 {
			return fmt.Errorf("tar archive path %q has negative size", relativePath)
		}
		if err := unpackTarFile(tarReader, outputDir, relativePath, header.Size); err != nil {
			return err
		}
		fileCount++
	}
	slog.Info("retriever archive unpacking completed",
		slog.String("output_dir", outputDir),
		slog.Int("file_count", fileCount),
	)
	return nil
}

func unpackTarFile(reader io.Reader, outputDir, relativePath string, expectedSize int64) error {
	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
		return fmt.Errorf("create parent directory for %q: %w", relativePath, err)
	}

	file, err := os.OpenFile(absolutePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create unpacked file %q: %w", relativePath, err)
	}
	copied, copyErr := io.Copy(file, reader)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(absolutePath)
		return fmt.Errorf("write unpacked file %q: %w", relativePath, copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(absolutePath)
		return fmt.Errorf("close unpacked file %q: %w", relativePath, closeErr)
	}
	if copied != expectedSize {
		_ = os.Remove(absolutePath)
		return fmt.Errorf("unpacked file %q size mismatch: expected %d, wrote %d", relativePath, expectedSize, copied)
	}
	return nil
}

func sanitizeArchivePath(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("archive path is empty")
	}
	if strings.Contains(trimmed, "\\") {
		return "", fmt.Errorf("archive path %q must use slash separators", value)
	}
	if path.IsAbs(trimmed) || filepath.IsAbs(trimmed) || hasWindowsVolumeName(trimmed) {
		return "", fmt.Errorf("archive path %q must be relative", value)
	}
	for _, part := range strings.Split(trimmed, "/") {
		if part == ".." {
			return "", fmt.Errorf("archive path %q contains path traversal", value)
		}
	}

	cleaned := path.Clean(trimmed)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("archive path %q is invalid", value)
	}
	return cleaned, nil
}

func hasWindowsVolumeName(value string) bool {
	if len(value) < 2 {
		return false
	}
	first := value[0]
	return value[1] == ':' && ((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z'))
}

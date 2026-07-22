package retriever

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
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

func WriteCollectionTar(writer io.Writer, dumpDir string, excludePaths ...string) error {
	return WriteCollectionTarWithOptions(writer, dumpDir, ArchiveOptions{}, excludePaths...)
}

func WriteCollectionTarWithOptions(writer io.Writer, dumpDir string, options ArchiveOptions, excludePaths ...string) error {
	slog.Info("retriever archive walking started",
		slog.String("input_dir", dumpDir),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationArchive,
		Message:   "retriever archive walking started",
		InputDir:  dumpDir,
	})

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
	options.Progress.emit(ProgressEvent{
		Operation: OperationArchive,
		Message:   "retriever archive walking completed",
		InputDir:  dumpDir,
		FileCount: len(archivePaths),
	})

	tarWriter := tar.NewWriter(writer)

	slog.Info("retriever archive tar streaming started",
		slog.String("input_dir", dumpDir),
		slog.Int("file_count", len(archivePaths)),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationArchive,
		Message:   "retriever archive tar streaming started",
		InputDir:  dumpDir,
		FileCount: len(archivePaths),
		Planned:   int64(len(archivePaths)),
	})

	startedAt := time.Now()
	for index, relativePath := range archivePaths {
		if err := writeCollectionTarFile(tarWriter, dumpDir, relativePath); err != nil {
			_ = tarWriter.Close()
			return err
		}

		processed := int64(index + 1)
		options.Progress.emit(ProgressEvent{
			Operation:         OperationArchive,
			Message:           "retriever archive tar file streamed",
			InputDir:          dumpDir,
			FileCount:         len(archivePaths),
			Processed:         processed,
			Planned:           int64(len(archivePaths)),
			Elapsed:           time.Since(startedAt),
			EntitiesPerSecond: perSecond(processed, time.Since(startedAt)),
		})
	}

	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("finish tar stream: %w", err)
	}

	slog.Info("retriever archive tar streaming completed",
		slog.String("input_dir", dumpDir),
		slog.Int("file_count", len(archivePaths)),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationArchive,
		Message:   "retriever archive tar streaming completed",
		InputDir:  dumpDir,
		FileCount: len(archivePaths),
		Processed: int64(len(archivePaths)),
		Planned:   int64(len(archivePaths)),
		Elapsed:   time.Since(startedAt),
	})
	return nil
}

func writeCollectionTar(writer io.Writer, dumpDir string, excludePaths ...string) error {
	return WriteCollectionTar(writer, dumpDir, excludePaths...)
}

func ArchivePathsFromManifest(value Manifest) ([]string, error) {
	return archivePathsFromManifest(value)
}

func archivePathsFromManifest(value Manifest) ([]string, error) {
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

	openInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat archive file %q: %w", relativePath, err)
	}

	if !openInfo.Mode().IsRegular() {
		return fmt.Errorf("archive file %q is not a regular file", relativePath)
	}

	if !os.SameFile(info, openInfo) {
		return fmt.Errorf("archive file %q changed while packaging", relativePath)
	}

	copied, err := io.Copy(tarWriter, file)
	if err != nil {
		return fmt.Errorf("write tar file %q: %w", relativePath, err)
	}

	if copied != info.Size() {
		return fmt.Errorf("archive file %q changed while packaging: expected %d bytes, copied %d", relativePath, info.Size(), copied)
	}

	return nil
}

func UnpackTar(reader io.Reader, outputDir string, force bool) error {
	return UnpackTarWithOptions(reader, outputDir, force, ArchiveOptions{})
}

func UnpackTarWithOptions(reader io.Reader, outputDir string, force bool, options ArchiveOptions) error {
	_, err := unpackTarWithOptions(reader, outputDir, force, options, false)
	return err
}

type unpackedFileIntegrity struct {
	compressedBytes int64
	sha256          string
}

func unpackCollectionTarWithOptions(reader io.Reader, outputDir string, force bool, options ArchiveOptions) error {
	files, err := unpackTarWithOptions(reader, outputDir, force, options, true)
	if err != nil {
		return err
	}

	manifest, err := validateExtractedCollection(outputDir, files)
	if err != nil {
		return err
	}

	compressedBytes, _ := manifestFragmentBytes(manifest)
	options.Progress.emit(ProgressEvent{
		Operation:           OperationUnpack,
		Message:             "retriever archive fragment integrity validated during extraction",
		OutputDir:           outputDir,
		FileCount:           manifestFileCount(manifest),
		CompressedBytesRead: compressedBytes,
		FragmentPasses:      1,
	})

	return nil
}

func unpackTarWithOptions(reader io.Reader, outputDir string, force bool, options ArchiveOptions, trackIntegrity bool) (map[string]unpackedFileIntegrity, error) {
	slog.Info("retriever archive unpacking started",
		slog.String("output_dir", outputDir),
		slog.Bool("force", force),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationUnpack,
		Message:   "retriever archive unpacking started",
		OutputDir: outputDir,
	})

	if err := prepareOutputDirectory(outputDir, force); err != nil {
		return nil, err
	}

	tarReader := tar.NewReader(reader)
	seen := map[string]struct{}{}
	var integrity map[string]unpackedFileIntegrity
	if trackIntegrity {
		integrity = map[string]unpackedFileIntegrity{}
	}
	var fileCount int
	startedAt := time.Now()

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("read tar entry: %w", err)
		}

		relativePath, err := sanitizeArchivePath(header.Name)
		if err != nil {
			return nil, err
		}

		if _, ok := seen[relativePath]; ok {
			return nil, fmt.Errorf("tar archive contains duplicate path %q", relativePath)
		}

		seen[relativePath] = struct{}{}

		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return nil, fmt.Errorf("tar archive path %q is not a regular file", relativePath)
		}

		if header.Size < 0 {
			return nil, fmt.Errorf("tar archive path %q has negative size", relativePath)
		}

		fileIntegrity, err := unpackTarFileTracked(tarReader, outputDir, relativePath, header.Size, trackIntegrity)
		if err != nil {
			return nil, err
		}
		if trackIntegrity {
			integrity[relativePath] = fileIntegrity
		}

		fileCount++

		options.Progress.emit(ProgressEvent{
			Operation:         OperationUnpack,
			Message:           "retriever archive file unpacked",
			OutputDir:         outputDir,
			FileCount:         fileCount,
			Processed:         int64(fileCount),
			Elapsed:           time.Since(startedAt),
			EntitiesPerSecond: perSecond(int64(fileCount), time.Since(startedAt)),
		})
	}

	slog.Info("retriever archive unpacking completed",
		slog.String("output_dir", outputDir),
		slog.Int("file_count", fileCount),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationUnpack,
		Message:   "retriever archive unpacking completed",
		OutputDir: outputDir,
		FileCount: fileCount,
		Processed: int64(fileCount),
		Elapsed:   time.Since(startedAt),
	})

	return integrity, nil
}

func unpackTar(reader io.Reader, outputDir string, force bool) error {
	return UnpackTar(reader, outputDir, force)
}

func unpackTarFile(reader io.Reader, outputDir, relativePath string, expectedSize int64) error {
	_, err := unpackTarFileTracked(reader, outputDir, relativePath, expectedSize, false)
	return err
}

func unpackTarFileTracked(reader io.Reader, outputDir, relativePath string, expectedSize int64, trackIntegrity bool) (unpackedFileIntegrity, error) {
	absolutePath := filepath.Join(outputDir, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
		return unpackedFileIntegrity{}, fmt.Errorf("create parent directory for %q: %w", relativePath, err)
	}

	file, err := os.OpenFile(absolutePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return unpackedFileIntegrity{}, fmt.Errorf("create unpacked file %q: %w", relativePath, err)
	}

	var (
		hasher           = sha256.New()
		target io.Writer = file
	)
	if trackIntegrity {
		target = io.MultiWriter(file, hasher)
	}
	copied, copyErr := io.Copy(target, reader)
	closeErr := file.Close()

	if copyErr != nil {
		_ = os.Remove(absolutePath)
		return unpackedFileIntegrity{}, fmt.Errorf("write unpacked file %q: %w", relativePath, copyErr)
	}

	if closeErr != nil {
		_ = os.Remove(absolutePath)
		return unpackedFileIntegrity{}, fmt.Errorf("close unpacked file %q: %w", relativePath, closeErr)
	}

	if copied != expectedSize {
		_ = os.Remove(absolutePath)
		return unpackedFileIntegrity{}, fmt.Errorf("unpacked file %q size mismatch: expected %d, wrote %d", relativePath, expectedSize, copied)
	}

	result := unpackedFileIntegrity{compressedBytes: copied}
	if trackIntegrity {
		result.sha256 = hex.EncodeToString(hasher.Sum(nil))
	}
	return result, nil
}

func validateExtractedCollection(outputDir string, files map[string]unpackedFileIntegrity) (Manifest, error) {
	nextManifest, err := readManifest(outputDir)
	if err != nil {
		return Manifest{}, err
	}

	expectedPaths, err := archivePathsFromManifest(nextManifest)
	if err != nil {
		return Manifest{}, err
	}
	expected := make(map[string]struct{}, len(expectedPaths))
	for _, relativePath := range expectedPaths {
		expected[relativePath] = struct{}{}
	}
	for relativePath := range files {
		if _, found := expected[relativePath]; !found {
			return Manifest{}, fmt.Errorf("unpacked archive contains unexpected file %q", relativePath)
		}
	}
	for _, relativePath := range expectedPaths {
		if _, found := files[relativePath]; !found {
			return Manifest{}, fmt.Errorf("unpacked archive is missing file %q", relativePath)
		}
	}

	for _, graphEntry := range nextManifest.Graphs {
		for _, fileEntry := range graphEntry.Files {
			actual := files[fileEntry.Path]
			absolutePath := filepath.Join(outputDir, filepath.FromSlash(fileEntry.Path))
			if err := verifyChecksumValues(absolutePath, fileEntry.SHA256, fileEntry.CompressedBytes, actual.sha256, actual.compressedBytes); err != nil {
				return Manifest{}, err
			}
		}
	}

	return nextManifest, nil
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

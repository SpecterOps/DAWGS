package retriever

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

const (
	DefaultZstdLevel  = 11
	maxJSONLLineBytes = 10 * 1024 * 1024
)

type countingWriter struct {
	writer io.Writer
	count  int64
}

type compressedJSONLinesWriter struct {
	path                string
	tempPath            string
	file                *os.File
	compressor          io.WriteCloser
	encoder             *json.Encoder
	compressedCounter   *countingWriter
	uncompressedCounter *countingWriter
	hasher              hash.Hash
	count               int
	state               compressedJSONLinesWriterState
}

type compressedJSONLinesWriterState uint8

const (
	compressedWriterOpen compressedJSONLinesWriterState = iota
	compressedWriterPrepared
	compressedWriterAborted
)

type preparedCompressedJSONLinesFragment struct {
	path     string
	tempPath string
	metadata FileManifest
	state    preparedCompressedJSONLinesState
}

type preparedCompressedJSONLinesState uint8

const (
	compressedFragmentPrepared preparedCompressedJSONLinesState = iota
	compressedFragmentCommitted
	compressedFragmentAborted
)

func (s *countingWriter) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.count += int64(n)
	return n, err
}

func ValidateCompression(codec CompressionCodec) error {
	switch codec {
	case CompressionGzip, CompressionZstd:
		return nil
	default:
		return ValidationError{Message: fmt.Sprintf("unsupported compression codec %q", codec)}
	}
}

func validateCompression(codec CompressionCodec) error {
	return ValidateCompression(codec)
}

func compressionExtension(codec CompressionCodec) (string, error) {
	switch codec {
	case CompressionGzip:
		return ".gz", nil
	case CompressionZstd:
		return ".zst", nil
	default:
		return "", fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func newCompressionWriter(writer io.Writer, codec CompressionCodec, zstdLevel int) (io.WriteCloser, error) {
	switch codec {
	case CompressionGzip:
		return gzip.NewWriterLevel(writer, gzip.BestCompression)
	case CompressionZstd:
		return zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
	default:
		return nil, fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func newDecompressionReader(reader io.Reader, codec CompressionCodec) (io.ReadCloser, error) {
	switch codec {
	case CompressionGzip:
		return gzip.NewReader(reader)
	case CompressionZstd:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}

		return decoder.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func newCompressedJSONLinesWriter(path string, codec CompressionCodec, zstdLevel int) (*compressedJSONLinesWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create fragment directory: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open fragment temp file: %w", err)
	}

	hasher := sha256.New()
	compressedCounter := &countingWriter{
		writer: io.MultiWriter(file, hasher),
	}
	compressor, err := newCompressionWriter(compressedCounter, codec, zstdLevel)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)

		return nil, err
	}

	uncompressedCounter := &countingWriter{
		writer: compressor,
	}
	encoder := json.NewEncoder(uncompressedCounter)
	encoder.SetEscapeHTML(false)

	return &compressedJSONLinesWriter{
		path:                path,
		tempPath:            tempPath,
		file:                file,
		compressor:          compressor,
		encoder:             encoder,
		compressedCounter:   compressedCounter,
		uncompressedCounter: uncompressedCounter,
		hasher:              hasher,
	}, nil
}

func (s *compressedJSONLinesWriter) Write(value any) error {
	if s.state != compressedWriterOpen {
		return fmt.Errorf("write JSONL fragment after prepare or abort")
	}

	if err := s.encoder.Encode(value); err != nil {
		return fmt.Errorf("encode JSONL record %d: %w", s.count+1, err)
	}

	s.count++

	return nil
}

func (s *compressedJSONLinesWriter) Count() int {
	return s.count
}

func (s *compressedJSONLinesWriter) Prepare() (*preparedCompressedJSONLinesFragment, error) {
	if s.state != compressedWriterOpen {
		return nil, fmt.Errorf("prepare JSONL fragment more than once or after abort")
	}
	s.state = compressedWriterPrepared

	if err := s.compressor.Close(); err != nil {
		_ = s.file.Close()
		_ = os.Remove(s.tempPath)
		s.state = compressedWriterAborted

		return nil, fmt.Errorf("finish compressed fragment: %w", err)
	}

	if err := s.file.Close(); err != nil {
		_ = os.Remove(s.tempPath)
		s.state = compressedWriterAborted

		return nil, fmt.Errorf("close fragment file: %w", err)
	}

	return &preparedCompressedJSONLinesFragment{
		path:     s.path,
		tempPath: s.tempPath,
		metadata: FileManifest{
			Count:             s.count,
			CompressedBytes:   s.compressedCounter.count,
			UncompressedBytes: s.uncompressedCounter.count,
			SHA256:            hex.EncodeToString(s.hasher.Sum(nil)),
		},
		state: compressedFragmentPrepared,
	}, nil
}

func (s *compressedJSONLinesWriter) Abort() error {
	switch s.state {
	case compressedWriterOpen:
		s.state = compressedWriterAborted
		return errors.Join(
			s.compressor.Close(),
			s.file.Close(),
			removeStagedFragment(s.tempPath),
		)
	case compressedWriterAborted:
		return nil
	default:
		return fmt.Errorf("abort JSONL writer after prepare")
	}
}

func (s *preparedCompressedJSONLinesFragment) Metadata() FileManifest {
	return s.metadata
}

func (s *preparedCompressedJSONLinesFragment) Commit(ctx context.Context) error {
	if s.state != compressedFragmentPrepared {
		return fmt.Errorf("commit JSONL fragment that is not prepared")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.Rename(s.tempPath, s.path); err != nil {
		_ = removeStagedFragment(s.tempPath)
		s.state = compressedFragmentAborted
		return fmt.Errorf("rename fragment: %w", err)
	}

	s.state = compressedFragmentCommitted
	return nil
}

func (s *preparedCompressedJSONLinesFragment) Abort() error {
	switch s.state {
	case compressedFragmentPrepared:
		s.state = compressedFragmentAborted
		return removeStagedFragment(s.tempPath)
	case compressedFragmentAborted:
		return nil
	default:
		return fmt.Errorf("abort committed JSONL fragment")
	}
}

func removeStagedFragment(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func writeCompressedJSONLines[T any](path string, codec CompressionCodec, zstdLevel int, records []T) (FileManifest, error) {
	writer, err := newCompressedJSONLinesWriter(path, codec, zstdLevel)
	if err != nil {
		return FileManifest{}, err
	}

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			_ = writer.Abort()

			return FileManifest{}, err
		}
	}

	prepared, err := writer.Prepare()
	if err != nil {
		return FileManifest{}, err
	}
	if err := prepared.Commit(context.Background()); err != nil {
		_ = prepared.Abort()
		return FileManifest{}, err
	}
	return prepared.Metadata(), nil
}

func readCompressedJSONLines[T any](path string, codec CompressionCodec, handle func(T) error) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open fragment: %w", err)
	}
	defer file.Close()

	return readCompressedJSONLinesFromReader(file, codec, handle)
}

func readVerifiedCompressedJSONLines[T any](path string, codec CompressionCodec, expectedSHA256 string, expectedCompressedBytes int64, handle func(T) error) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open fragment: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	compressedCounter := &countingWriter{
		writer: hasher,
	}
	trackedReader := io.TeeReader(file, compressedCounter)

	count, decodeErr := readCompressedJSONLinesFromReader(trackedReader, codec, handle)
	if _, err := io.Copy(io.Discard, trackedReader); err != nil {
		return count, fmt.Errorf("hash checksum target: %w", err)
	}

	if err := verifyChecksumValues(path, expectedSHA256, expectedCompressedBytes, hex.EncodeToString(hasher.Sum(nil)), compressedCounter.count); err != nil {
		return count, err
	}

	return count, decodeErr
}

func readCompressedJSONLinesFromReader[T any](reader io.Reader, codec CompressionCodec, handle func(T) error) (int, error) {
	decompressor, err := newDecompressionReader(reader, codec)
	if err != nil {
		return 0, fmt.Errorf("open compressed fragment: %w", err)
	}

	scanner := bufio.NewScanner(decompressor)
	scanner.Buffer(make([]byte, maxJSONLLineBytes), maxJSONLLineBytes)
	count := 0
	var decodeErr error
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			decodeErr = fmt.Errorf("decode JSONL record %d: blank line", count+1)
			break
		}

		var record T
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			decodeErr = fmt.Errorf("decode JSONL record %d: %w", count+1, err)
			break
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			if err == nil {
				decodeErr = fmt.Errorf("decode JSONL record %d: multiple JSON values", count+1)
				break
			}

			decodeErr = fmt.Errorf("decode JSONL record %d: %w", count+1, err)
			break
		}

		count++
		if handle != nil {
			if err := handle(record); err != nil {
				decodeErr = err
				break
			}
		}
	}
	if err := scanner.Err(); decodeErr == nil && err != nil {
		decodeErr = fmt.Errorf("read JSONL record %d: %w", count+1, err)
	}

	if err := decompressor.Close(); decodeErr == nil && err != nil {
		return count, fmt.Errorf("close compressed fragment: %w", err)
	}

	return count, decodeErr
}

func verifyChecksum(path string, expectedSHA256 string, expectedCompressedBytes int64) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open checksum target: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	copied, err := copyHash(hasher, file)
	if err != nil {
		return fmt.Errorf("hash checksum target: %w", err)
	}

	return verifyChecksumValues(path, expectedSHA256, expectedCompressedBytes, hex.EncodeToString(hasher.Sum(nil)), copied)
}

func verifyChecksumValues(path string, expectedSHA256 string, expectedCompressedBytes int64, actualSHA256 string, actualCompressedBytes int64) error {
	if expectedCompressedBytes >= 0 && actualCompressedBytes != expectedCompressedBytes {
		return ByteCountMismatchError{
			Path:          path,
			ExpectedBytes: expectedCompressedBytes,
			ActualBytes:   actualCompressedBytes,
		}
	}

	if actualSHA256 != expectedSHA256 {
		return ChecksumMismatchError{
			Path:           path,
			ExpectedSHA256: expectedSHA256,
			ActualSHA256:   actualSHA256,
		}
	}

	return nil
}

func copyHash(hasher hash.Hash, reader io.Reader) (int64, error) {
	return io.Copy(hasher, reader)
}

func compressedJSONLinesSize[T any](codec CompressionCodec, zstdLevel int, records []T) (int64, int64, error) {
	var buffer bytes.Buffer
	compressor, err := newCompressionWriter(&buffer, codec, zstdLevel)
	if err != nil {
		return 0, 0, err
	}

	uncompressedCounter := &countingWriter{
		writer: compressor,
	}
	encoder := json.NewEncoder(uncompressedCounter)
	encoder.SetEscapeHTML(false)

	for index, record := range records {
		if err := encoder.Encode(record); err != nil {
			_ = compressor.Close()

			return 0, 0, fmt.Errorf("encode JSONL record %d: %w", index+1, err)
		}
	}

	if err := compressor.Close(); err != nil {
		return 0, 0, err
	}

	return uncompressedCounter.count, int64(buffer.Len()), nil
}

func CompressedJSONLinesSize[T any](codec CompressionCodec, zstdLevel int, records []T) (int64, int64, error) {
	return compressedJSONLinesSize(codec, zstdLevel, records)
}

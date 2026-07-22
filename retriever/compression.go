package retriever

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	DefaultZstdLevel       = 3
	initialJSONLBufferSize = 64 * 1024
	maxJSONLLineBytes      = 10 * 1024 * 1024
)

var zstdWriterPools sync.Map

type nopWriteCloser struct {
	io.Writer
}

func (s nopWriteCloser) Close() error { return nil }

type pooledZstdWriter struct {
	encoder  *zstd.Encoder
	pool     *sync.Pool
	closed   bool
	writeErr error
}

func (s *pooledZstdWriter) Write(value []byte) (int, error) {
	written, err := s.encoder.Write(value)
	if err != nil {
		s.writeErr = errors.Join(s.writeErr, err)
	}
	return written, err
}

func (s *pooledZstdWriter) Close() error {
	if s.closed {
		return fmt.Errorf("close zstd writer more than once")
	}
	s.closed = true

	closeErr := s.encoder.Close()
	if s.writeErr != nil || closeErr != nil {
		return errors.Join(s.writeErr, closeErr)
	}

	s.pool.Put(s.encoder)
	return nil
}

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
	closed              bool
}

func (s *countingWriter) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.count += int64(n)
	return n, err
}

func ValidateCompression(codec CompressionCodec) error {
	switch codec {
	case CompressionNone, CompressionGzip, CompressionZstd:
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
	case CompressionNone:
		return "", nil
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
	case CompressionNone:
		return nopWriteCloser{Writer: writer}, nil
	case CompressionGzip:
		return gzip.NewWriterLevel(writer, gzip.BestCompression)
	case CompressionZstd:
		poolValue, _ := zstdWriterPools.LoadOrStore(zstdLevel, &sync.Pool{})
		pool := poolValue.(*sync.Pool)
		if pooled := pool.Get(); pooled != nil {
			encoder := pooled.(*zstd.Encoder)
			encoder.Reset(writer)
			return &pooledZstdWriter{encoder: encoder, pool: pool}, nil
		}

		encoder, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			return nil, err
		}
		return &pooledZstdWriter{encoder: encoder, pool: pool}, nil
	default:
		return nil, fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func newDecompressionReader(reader io.Reader, codec CompressionCodec) (io.ReadCloser, error) {
	switch codec {
	case CompressionNone:
		return io.NopCloser(reader), nil
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
	if s.closed {
		return fmt.Errorf("write closed JSONL fragment")
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

func (s *compressedJSONLinesWriter) Close() (FileManifest, error) {
	if s.closed {
		return FileManifest{}, fmt.Errorf("close JSONL fragment more than once")
	}
	s.closed = true

	if err := s.compressor.Close(); err != nil {
		_ = s.file.Close()
		_ = os.Remove(s.tempPath)

		return FileManifest{}, fmt.Errorf("finish compressed fragment: %w", err)
	}

	if err := s.file.Close(); err != nil {
		_ = os.Remove(s.tempPath)

		return FileManifest{}, fmt.Errorf("close fragment file: %w", err)
	}

	if err := os.Rename(s.tempPath, s.path); err != nil {
		_ = os.Remove(s.tempPath)

		return FileManifest{}, fmt.Errorf("rename fragment: %w", err)
	}

	return FileManifest{
		Count:             s.count,
		CompressedBytes:   s.compressedCounter.count,
		UncompressedBytes: s.uncompressedCounter.count,
		SHA256:            hex.EncodeToString(s.hasher.Sum(nil)),
	}, nil
}

func (s *compressedJSONLinesWriter) Abort() {
	if s.closed {
		return
	}
	s.closed = true

	_ = s.compressor.Close()
	_ = s.file.Close()
	_ = os.Remove(s.tempPath)
}

func writeCompressedJSONLines[T any](path string, codec CompressionCodec, zstdLevel int, records []T) (FileManifest, error) {
	writer, err := newCompressedJSONLinesWriter(path, codec, zstdLevel)
	if err != nil {
		return FileManifest{}, err
	}

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			writer.Abort()

			return FileManifest{}, err
		}
	}

	return writer.Close()
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
	scanner.Buffer(make([]byte, initialJSONLBufferSize), maxJSONLLineBytes+1)
	count := 0
	var decodeErr error
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > maxJSONLLineBytes {
			decodeErr = fmt.Errorf("read JSONL record %d: line exceeds %d bytes", count+1, maxJSONLLineBytes)
			break
		}
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

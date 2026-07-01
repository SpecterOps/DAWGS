package main

import (
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

	"github.com/klauspost/compress/zstd"
)

const defaultZstdLevel = 11

type countingWriter struct {
	writer io.Writer
	count  int64
}

func (s *countingWriter) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.count += int64(n)
	return n, err
}

func validateCompression(codec compressionCodec) error {
	switch codec {
	case compressionGzip, compressionZstd:
		return nil
	default:
		return fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func compressionExtension(codec compressionCodec) (string, error) {
	switch codec {
	case compressionGzip:
		return ".gz", nil
	case compressionZstd:
		return ".zst", nil
	default:
		return "", fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func newCompressionWriter(writer io.Writer, codec compressionCodec, zstdLevel int) (io.WriteCloser, error) {
	switch codec {
	case compressionGzip:
		return gzip.NewWriterLevel(writer, gzip.BestCompression)
	case compressionZstd:
		return zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
	default:
		return nil, fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func newDecompressionReader(reader io.Reader, codec compressionCodec) (io.ReadCloser, error) {
	switch codec {
	case compressionGzip:
		return gzip.NewReader(reader)
	case compressionZstd:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return decoder.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported compression codec %q", codec)
	}
}

func encodeCompactJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

func writeCompressedJSON(path string, codec compressionCodec, zstdLevel int, value any) (fileManifest, error) {
	payload, err := encodeCompactJSON(value)
	if err != nil {
		return fileManifest{}, fmt.Errorf("encode fragment: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fileManifest{}, fmt.Errorf("create fragment directory: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fileManifest{}, fmt.Errorf("open fragment temp file: %w", err)
	}

	hasher := sha256.New()
	counter := &countingWriter{
		writer: io.MultiWriter(file, hasher),
	}
	compressor, err := newCompressionWriter(counter, codec, zstdLevel)
	if err != nil {
		file.Close()
		os.Remove(tempPath)
		return fileManifest{}, err
	}

	_, writeErr := compressor.Write(payload)
	closeCompressionErr := compressor.Close()
	closeFileErr := file.Close()
	if writeErr != nil {
		os.Remove(tempPath)
		return fileManifest{}, fmt.Errorf("write compressed fragment: %w", writeErr)
	}
	if closeCompressionErr != nil {
		os.Remove(tempPath)
		return fileManifest{}, fmt.Errorf("finish compressed fragment: %w", closeCompressionErr)
	}
	if closeFileErr != nil {
		os.Remove(tempPath)
		return fileManifest{}, fmt.Errorf("close fragment file: %w", closeFileErr)
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fileManifest{}, fmt.Errorf("rename fragment: %w", err)
	}

	return fileManifest{
		CompressedBytes:   counter.count,
		UncompressedBytes: int64(len(payload)),
		SHA256:            hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func readCompressedJSON(path string, codec compressionCodec, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open fragment: %w", err)
	}
	defer file.Close()

	reader, err := newDecompressionReader(file, codec)
	if err != nil {
		return fmt.Errorf("open compressed fragment: %w", err)
	}
	defer reader.Close()

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode fragment: %w", err)
	}
	return nil
}

type compressedJSONLinesWriter struct {
	finalPath             string
	tempPath              string
	file                  *os.File
	compressor            io.WriteCloser
	encoder               *json.Encoder
	compressedByteCounter *countingWriter
	uncompressedCounter   *countingWriter
	hasher                hash.Hash
	closed                bool
}

func newCompressedJSONLinesWriter(path string, codec compressionCodec, zstdLevel int) (*compressedJSONLinesWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create JSONL directory: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open JSONL temp file: %w", err)
	}

	hasher := sha256.New()
	compressedByteCounter := &countingWriter{
		writer: io.MultiWriter(file, hasher),
	}
	compressor, err := newCompressionWriter(compressedByteCounter, codec, zstdLevel)
	if err != nil {
		file.Close()
		os.Remove(tempPath)
		return nil, err
	}
	uncompressedCounter := &countingWriter{
		writer: compressor,
	}
	encoder := json.NewEncoder(uncompressedCounter)
	encoder.SetEscapeHTML(false)

	return &compressedJSONLinesWriter{
		finalPath:             path,
		tempPath:              tempPath,
		file:                  file,
		compressor:            compressor,
		encoder:               encoder,
		compressedByteCounter: compressedByteCounter,
		uncompressedCounter:   uncompressedCounter,
		hasher:                hasher,
	}, nil
}

func (s *compressedJSONLinesWriter) Encode(value any) error {
	if s.closed {
		return fmt.Errorf("write JSONL record after close")
	}
	if err := s.encoder.Encode(value); err != nil {
		return fmt.Errorf("encode JSONL record: %w", err)
	}
	return nil
}

func (s *compressedJSONLinesWriter) Close() (fileManifest, error) {
	if s.closed {
		return fileManifest{}, fmt.Errorf("close JSONL writer twice")
	}
	s.closed = true

	closeCompressionErr := s.compressor.Close()
	closeFileErr := s.file.Close()
	if closeCompressionErr != nil {
		os.Remove(s.tempPath)
		return fileManifest{}, fmt.Errorf("finish compressed JSONL file: %w", closeCompressionErr)
	}
	if closeFileErr != nil {
		os.Remove(s.tempPath)
		return fileManifest{}, fmt.Errorf("close JSONL file: %w", closeFileErr)
	}
	if err := os.Rename(s.tempPath, s.finalPath); err != nil {
		os.Remove(s.tempPath)
		return fileManifest{}, fmt.Errorf("rename JSONL file: %w", err)
	}

	return fileManifest{
		CompressedBytes:   s.compressedByteCounter.count,
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

func readCompressedJSONLines[T any](path string, codec compressionCodec, handle func(T) error) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open JSONL file: %w", err)
	}
	defer file.Close()

	reader, err := newDecompressionReader(file, codec)
	if err != nil {
		return 0, fmt.Errorf("open compressed JSONL file: %w", err)
	}
	defer reader.Close()

	var count int
	decoder := json.NewDecoder(reader)
	for {
		var value T
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return count, fmt.Errorf("decode JSONL record %d: %w", count+1, err)
		}
		if err := handle(value); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
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
	if expectedCompressedBytes >= 0 && copied != expectedCompressedBytes {
		return fmt.Errorf("compressed byte mismatch for %s: manifest has %d, file has %d", path, expectedCompressedBytes, copied)
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != expectedSHA256 {
		return fmt.Errorf("sha256 mismatch for %s: manifest has %s, file has %s", path, expectedSHA256, actual)
	}
	return nil
}

func copyHash(hasher hash.Hash, reader io.Reader) (int64, error) {
	return io.Copy(hasher, reader)
}

func compressedJSONSize(codec compressionCodec, zstdLevel int, value any) (int64, int64, error) {
	payload, err := encodeCompactJSON(value)
	if err != nil {
		return 0, 0, err
	}

	var buffer bytes.Buffer
	compressor, err := newCompressionWriter(&buffer, codec, zstdLevel)
	if err != nil {
		return 0, 0, err
	}
	if _, err := compressor.Write(payload); err != nil {
		compressor.Close()
		return 0, 0, err
	}
	if err := compressor.Close(); err != nil {
		return 0, 0, err
	}
	return int64(len(payload)), int64(buffer.Len()), nil
}

func compressedJSONLinesSize[T any](codec compressionCodec, zstdLevel int, values []T) (int64, int64, error) {
	var buffer bytes.Buffer
	compressedCounter := &countingWriter{
		writer: &buffer,
	}
	compressor, err := newCompressionWriter(compressedCounter, codec, zstdLevel)
	if err != nil {
		return 0, 0, err
	}
	uncompressedCounter := &countingWriter{
		writer: compressor,
	}
	encoder := json.NewEncoder(uncompressedCounter)
	encoder.SetEscapeHTML(false)

	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			compressor.Close()
			return 0, 0, err
		}
	}
	if err := compressor.Close(); err != nil {
		return 0, 0, err
	}
	return uncompressedCounter.count, compressedCounter.count, nil
}

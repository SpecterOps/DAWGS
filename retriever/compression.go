package retriever

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

const DefaultZstdLevel = 11

type countingWriter struct {
	writer io.Writer
	count  int64
}

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

func encodeCompactJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

func writeCompressedJSON(path string, codec CompressionCodec, zstdLevel int, value any) (FileManifest, error) {
	payload, err := encodeCompactJSON(value)
	if err != nil {
		return FileManifest{}, fmt.Errorf("encode fragment: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return FileManifest{}, fmt.Errorf("create fragment directory: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return FileManifest{}, fmt.Errorf("open fragment temp file: %w", err)
	}

	hasher := sha256.New()
	counter := &countingWriter{
		writer: io.MultiWriter(file, hasher),
	}
	compressor, err := newCompressionWriter(counter, codec, zstdLevel)
	if err != nil {
		file.Close()
		os.Remove(tempPath)

		return FileManifest{}, err
	}

	_, writeErr := compressor.Write(payload)
	closeCompressionErr := compressor.Close()
	closeFileErr := file.Close()

	if writeErr != nil {
		os.Remove(tempPath)
		return FileManifest{}, fmt.Errorf("write compressed fragment: %w", writeErr)
	}

	if closeCompressionErr != nil {
		os.Remove(tempPath)
		return FileManifest{}, fmt.Errorf("finish compressed fragment: %w", closeCompressionErr)
	}

	if closeFileErr != nil {
		os.Remove(tempPath)
		return FileManifest{}, fmt.Errorf("close fragment file: %w", closeFileErr)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return FileManifest{}, fmt.Errorf("rename fragment: %w", err)
	}

	return FileManifest{
		CompressedBytes:   counter.count,
		UncompressedBytes: int64(len(payload)),
		SHA256:            hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func readCompressedJSON(path string, codec CompressionCodec, target any) error {
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
		return ByteCountMismatchError{
			Path:          path,
			ExpectedBytes: expectedCompressedBytes,
			ActualBytes:   copied,
		}
	}

	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != expectedSHA256 {
		return ChecksumMismatchError{
			Path:           path,
			ExpectedSHA256: expectedSHA256,
			ActualSHA256:   actual,
		}
	}

	return nil
}

func copyHash(hasher hash.Hash, reader io.Reader) (int64, error) {
	return io.Copy(hasher, reader)
}

func compressedJSONSize(codec CompressionCodec, zstdLevel int, value any) (int64, int64, error) {
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

func CompressedJSONSize(codec CompressionCodec, zstdLevel int, value any) (int64, int64, error) {
	return compressedJSONSize(codec, zstdLevel, value)
}

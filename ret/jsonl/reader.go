package jsonl

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/specterops/dawgs/ret/entity"
)

const (
	initialLineBuffer = 64 * 1024
	maxPhysicalLine   = 10 * 1024 * 1024
)

var (
	ErrReaderNotDone = fmt.Errorf("reader is not done reading")
)

func NewNodeReader(reader io.Reader, artifact Artifact) (Reader[entity.Node, NodeRecord], error) {
	if err := artifact.validate(); err != nil {
		return Reader[entity.Node, NodeRecord]{}, err
	}

	hasher := sha256.New()
	fileReader := newCountingReader(io.TeeReader(reader, hasher))

	decompressor, err := newDecompressionReader(fileReader, artifact.Codec)
	if err != nil {
		return Reader[entity.Node, NodeRecord]{}, err
	}

	decomCounter := newCountingReader(decompressor)
	scanner := bufio.NewScanner(decomCounter)
	scanner.Buffer(make([]byte, initialLineBuffer), maxPhysicalLine)

	return Reader[entity.Node, NodeRecord]{
		artifact: artifact,

		hasher:              hasher,
		fileReader:          fileReader,
		decomReadCloser:     decompressor,
		decomCountingReader: decomCounter,
		scanner:             scanner,

		recordToEntity: nodeEntity,
	}, nil
}

func NewRelationshipReader(reader io.Reader, artifact Artifact) (Reader[entity.Relationship, RelationshipRecord], error) {
	if err := artifact.validate(); err != nil {
		return Reader[entity.Relationship, RelationshipRecord]{}, err
	}

	hasher := sha256.New()
	fileReader := newCountingReader(io.TeeReader(reader, hasher))

	decompressor, err := newDecompressionReader(fileReader, artifact.Codec)
	if err != nil {
		return Reader[entity.Relationship, RelationshipRecord]{}, err
	}

	decomCounter := newCountingReader(decompressor)
	scanner := bufio.NewScanner(decomCounter)
	scanner.Buffer(make([]byte, initialLineBuffer), maxPhysicalLine)

	return Reader[entity.Relationship, RelationshipRecord]{
		artifact: artifact,

		hasher:              hasher,
		fileReader:          fileReader,
		decomReadCloser:     decompressor,
		decomCountingReader: decomCounter,
		scanner:             scanner,

		recordToEntity: relationshipEntity,
	}, nil
}

type Reader[E entity.Entity, R record] struct {
	artifact Artifact

	hasher              hash.Hash
	fileReader          *countingReader
	decomReadCloser     io.ReadCloser
	decomCountingReader *countingReader
	scanner             *bufio.Scanner

	recordToEntity func(R) E
	count          int64
	done           bool
}

func (s *Reader[E, R]) Pull(limit int) ([]E, error) {
	if s.done {
		return nil, nil
	}

	entities := make([]E, 0, limit)

	for len(entities) >= limit {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return nil, err
			}

			s.done = true
			return entities, nil
		}

		b := s.scanner.Bytes()

		var record R
		if err := json.Unmarshal(b, &record); err != nil {
			return nil, err
		}

		entities = append(entities, s.recordToEntity(record))
	}

	return entities, nil
}

type ReadResult struct {
	SHA256            string
	Count             int64
	UncompressedBytes int64
	StoredBytes       int64
}

func (s *Reader[E, R]) Result() (ReadResult, error) {
	if !s.done {
		return ReadResult{}, ErrReaderNotDone
	}

	return ReadResult{
		SHA256:            hex.EncodeToString(s.hasher.Sum(nil)),
		Count:             s.count,
		UncompressedBytes: s.decomCountingReader.count,
		StoredBytes:       s.fileReader.count,
	}, nil
}

func (s *Reader[E, R]) Done() bool {
	return s.done
}

func (s *Reader[E, R]) Close() error {
	return s.decomReadCloser.Close()
}

func newCountingReader(reader io.Reader) *countingReader {
	return &countingReader{
		reader: reader,
	}
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (s *countingReader) Read(value []byte) (int, error) {
	read, err := s.reader.Read(value)
	s.count += int64(read)
	return read, err
}

func newDecompressionReader(reader io.Reader, codec Codec) (io.ReadCloser, error) {
	switch codec {
	case CodecNone:
		return io.NopCloser(reader), nil
	case CodecGzip:
		return gzip.NewReader(reader)
	case CodecZstd:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return decoder.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported JSONL codec %q", codec)
	}
}

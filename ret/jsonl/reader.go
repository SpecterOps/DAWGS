package jsonl

import (
	"bufio"
	"bytes"
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
	ErrReaderNotOpen = fmt.Errorf("reader is not open")
	ErrReaderNotDone = fmt.Errorf("reader is not done reading")
	ErrReaderFailed  = fmt.Errorf("reader failed")
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
	scanner.Buffer(make([]byte, initialLineBuffer), maxPhysicalLine+1)

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
	scanner.Buffer(make([]byte, initialLineBuffer), maxPhysicalLine+1)

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
	recordCount    int64

	state State
}

func (s *Reader[E, R]) Pull(limit int) ([]E, error) {
	switch s.state {
	case Closed:
		return nil, ErrReaderNotOpen
	case Failed:
		return nil, ErrReaderFailed
	}

	entities := make([]E, 0, limit)

	for len(entities) < limit {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				s.state = Failed
				return nil, err
			}

			s.state = Closed
			return entities, nil
		}

		var (
			line    = s.scanner.Bytes()
			record  R
			decoder = json.NewDecoder(bytes.NewReader(line))
		)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&record); err != nil {
			s.state = Failed
			return nil, err
		}

		entity := s.recordToEntity(record)
		if err := entity.Validate(); err != nil {
			s.state = Failed
			return nil, err
		}

		entities = append(entities, entity)
		s.recordCount++
	}

	return entities, nil
}

func (s *Reader[E, R]) Result() error {
	if s.state == Open {
		return ErrReaderNotDone
	} else if s.state == Failed {
		return ErrReaderFailed
	} else if s.artifact.SHA256 != hex.EncodeToString(s.hasher.Sum(nil)) {
		return fmt.Errorf("SHA256 encoding does not match")
	} else if s.artifact.Count != s.recordCount {
		return fmt.Errorf("Count does not match")
	} else if s.artifact.UncompressedBytes != s.decomCountingReader.count {
		return fmt.Errorf("UncompressedBytes does not match")
	} else if s.artifact.StoredBytes != s.fileReader.count {
		return fmt.Errorf("StoredBytes does not match")
	}

	return nil
}

func (s *Reader[E, R]) Done() bool {
	return s.state != Open
}

func (s *Reader[E, R]) Close() error {
	if err := s.decomReadCloser.Close(); err != nil {
		s.state = Failed
		return err
	} else if s.state != Failed {
		s.state = Closed
	}

	return nil
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

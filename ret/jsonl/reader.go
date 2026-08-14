package jsonl

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
	"math"
	"strconv"
	"strings"

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
	ErrInvalidLimit  = fmt.Errorf("pull limit must be positive")
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

	state          State
	resourceClosed bool
	failure        error
}

func (s *Reader[E, R]) Pull(limit int) ([]E, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if s.resourceClosed {
		return nil, ErrReaderNotOpen
	}
	switch s.state {
	case Closed:
		return nil, ErrReaderNotOpen
	case Failed:
		return nil, errors.Join(ErrReaderFailed, s.failure)
	}

	entities := make([]E, 0, limit)

	for len(entities) < limit {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return nil, s.fail(fmt.Errorf("read JSONL record %d: %w", s.recordCount+1, err))
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
		decoder.UseNumber()

		if err := decoder.Decode(&record); err != nil {
			return nil, s.fail(fmt.Errorf("decode JSONL record %d: %w", s.recordCount+1, err))
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			if err == nil {
				return nil, s.fail(fmt.Errorf("decode JSONL record %d: multiple JSON values", s.recordCount+1))
			}
			return nil, s.fail(fmt.Errorf("decode JSONL record %d: %w", s.recordCount+1, err))
		}

		entity := s.recordToEntity(record)
		if err := normalizeProperties(entityProperties(entity)); err != nil {
			return nil, s.fail(fmt.Errorf("normalize JSONL record %d properties: %w", s.recordCount+1, err))
		}
		if err := entity.Validate(); err != nil {
			return nil, s.fail(fmt.Errorf("validate JSONL record %d: %w", s.recordCount+1, err))
		}

		entities = append(entities, entity)
		s.recordCount++
	}

	return entities, nil
}

func entityProperties[E entity.Entity](value E) map[string]any {
	switch typed := any(value).(type) {
	case entity.Node:
		return typed.Properties
	case entity.Relationship:
		return typed.Properties
	default:
		return nil
	}
}

func normalizeProperties(properties map[string]any) error {
	for key, value := range properties {
		normalized, err := normalizeJSONValue(value, "properties."+key)
		if err != nil {
			return err
		}
		properties[key] = normalized
	}
	return nil
}

func normalizeJSONValue(value any, path string) (any, error) {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed, nil
	case json.Number:
		literal := typed.String()
		if !strings.ContainsAny(literal, ".eE") {
			integer, err := strconv.ParseInt(literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("JSON integer at %s is outside the int64 domain: %q", path, literal)
			}
			return integer, nil
		}
		fractional, err := strconv.ParseFloat(literal, 64)
		if err != nil || math.IsNaN(fractional) || math.IsInf(fractional, 0) {
			return nil, fmt.Errorf("JSON number at %s is not a finite float64: %q", path, literal)
		}
		return fractional, nil
	case []any:
		for index, element := range typed {
			normalized, err := normalizeJSONValue(element, fmt.Sprintf("%s[%d]", path, index))
			if err != nil {
				return nil, err
			}
			typed[index] = normalized
		}
		return typed, nil
	case map[string]any:
		for key, element := range typed {
			normalized, err := normalizeJSONValue(element, path+"."+key)
			if err != nil {
				return nil, err
			}
			typed[key] = normalized
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("JSON value at %s has unsupported decoded type %T", path, value)
	}
}

func (s *Reader[E, R]) Result() error {
	if s.state == Open {
		return ErrReaderNotDone
	} else if s.state == Failed {
		return errors.Join(ErrReaderFailed, s.failure)
	} else if actual := hex.EncodeToString(s.hasher.Sum(nil)); s.artifact.SHA256 != actual {
		return s.fail(fmt.Errorf("JSONL stored SHA-256 mismatch: got %s, want %s", actual, s.artifact.SHA256))
	} else if s.artifact.Count != s.recordCount {
		return s.fail(fmt.Errorf("JSONL record count mismatch: got %d, want %d", s.recordCount, s.artifact.Count))
	} else if s.artifact.UncompressedBytes != s.decomCountingReader.count {
		return s.fail(fmt.Errorf("JSONL uncompressed size mismatch: got %d, want %d", s.decomCountingReader.count, s.artifact.UncompressedBytes))
	} else if s.artifact.StoredBytes != s.fileReader.count {
		return s.fail(fmt.Errorf("JSONL stored size mismatch: got %d, want %d", s.fileReader.count, s.artifact.StoredBytes))
	}

	return nil
}

func (s *Reader[E, R]) Done() bool {
	return s.state != Open
}

func (s *Reader[E, R]) Close() error {
	if s.resourceClosed {
		if s.state == Failed {
			return errors.Join(ErrReaderFailed, s.failure)
		}
		return nil
	}
	s.resourceClosed = true
	if err := s.decomReadCloser.Close(); err != nil {
		s.fail(fmt.Errorf("close JSONL reader: %w", err))
	}
	if s.state == Failed {
		return errors.Join(ErrReaderFailed, s.failure)
	}

	return nil
}

func (s *Reader[E, R]) fail(err error) error {
	if s.failure == nil {
		s.failure = err
	} else {
		s.failure = errors.Join(s.failure, err)
	}
	s.state = Failed
	return err
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

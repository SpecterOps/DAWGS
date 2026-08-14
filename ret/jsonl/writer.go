package jsonl

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/specterops/dawgs/ret/entity"
)

var (
	ErrWriterNotOpen   = fmt.Errorf("writer is not open")
	ErrWriterNotClosed = fmt.Errorf("writer is not closed")
	ErrWriterFailed    = fmt.Errorf("writer failed")
)

func NewNodeWriter(writer io.Writer, config Config) (EntityWriter[entity.Node, NodeRecord], error) {
	if err := config.validate(); err != nil {
		return EntityWriter[entity.Node, NodeRecord]{}, err
	}

	hasher := sha256.New()
	output := newCountingWriter(io.MultiWriter(writer, hasher))

	compressionWriter, err := newCompressionWriter(output, config)
	if err != nil {
		return EntityWriter[entity.Node, NodeRecord]{}, err
	}

	inputWriter := newCountingWriter(compressionWriter)

	return EntityWriter[entity.Node, NodeRecord]{
		outputWriter: output,
		hasher:       hasher,

		compressionWriter: compressionWriter,
		inputWriter:       inputWriter,

		entityToRecord: nodeRecord,
		config:         config,

		state: Open,
	}, nil
}

func NewRelationshipWriter(writer io.Writer, config Config) (EntityWriter[entity.Relationship, RelationshipRecord], error) {
	if err := config.validate(); err != nil {
		return EntityWriter[entity.Relationship, RelationshipRecord]{}, err
	}

	hasher := sha256.New()
	output := newCountingWriter(io.MultiWriter(writer, hasher))

	compressionWriter, err := newCompressionWriter(output, config)
	if err != nil {
		return EntityWriter[entity.Relationship, RelationshipRecord]{}, err
	}

	inputWriter := newCountingWriter(compressionWriter)

	return EntityWriter[entity.Relationship, RelationshipRecord]{
		outputWriter: output,
		hasher:       hasher,

		compressionWriter: compressionWriter,
		inputWriter:       inputWriter,

		entityToRecord: relationshipRecord,
		config:         config,

		state: Open,
	}, nil
}

type EntityWriter[E entity.Entity, R record] struct {
	hasher hash.Hash

	outputWriter      *countingWriter
	compressionWriter io.WriteCloser
	inputWriter       *countingWriter

	recordCount int64

	entityToRecord func(E) R
	config         Config

	state          State
	resourceClosed bool
	failure        error
}

func (s *EntityWriter[E, R]) Push(entities []E) error {
	switch s.state {
	case Closed:
		return ErrWriterNotOpen
	case Failed:
		return errors.Join(ErrWriterFailed, s.failure)
	}

	for _, entity := range entities {
		if err := entity.Validate(); err != nil {
			return s.fail(fmt.Errorf("validate JSONL record %d: %w", s.recordCount+1, err))
		}

		record := s.entityToRecord(entity)

		if encoded, err := json.Marshal(record); err != nil {
			return s.fail(fmt.Errorf("encode JSONL record %d: %w", s.recordCount+1, err))
		} else if _, err := s.inputWriter.Write(append(encoded, '\n')); err != nil {
			return s.fail(fmt.Errorf("write JSONL record %d: %w", s.recordCount+1, err))
		}

		s.recordCount++
	}

	return nil
}

func (s *EntityWriter[E, R]) Close() error {
	if s.resourceClosed {
		if s.state == Failed {
			return errors.Join(ErrWriterFailed, s.failure)
		}
		return nil
	}
	s.resourceClosed = true
	if err := s.compressionWriter.Close(); err != nil {
		s.fail(fmt.Errorf("finish JSONL stream: %w", err))
	}
	if s.state == Failed {
		return errors.Join(ErrWriterFailed, s.failure)
	}
	s.state = Closed

	return nil
}

func (s *EntityWriter[E, R]) Result() (Artifact, error) {
	switch s.state {
	case Open:
		return Artifact{}, ErrWriterNotClosed
	case Failed:
		return Artifact{}, errors.Join(ErrWriterFailed, s.failure)
	}

	return Artifact{
		SchemaVersion:     SchemaVersion,
		Codec:             s.config.Codec,
		SHA256:            hex.EncodeToString(s.hasher.Sum(nil)),
		Level:             s.config.Level,
		Count:             s.recordCount,
		UncompressedBytes: s.inputWriter.count,
		StoredBytes:       s.outputWriter.count,
	}, nil
}

func (s *EntityWriter[E, R]) fail(err error) error {
	if s.failure == nil {
		s.failure = err
	} else {
		s.failure = errors.Join(s.failure, err)
	}
	s.state = Failed
	return err
}

func newCountingWriter(writer io.Writer) *countingWriter {
	return &countingWriter{writer: writer}
}

type countingWriter struct {
	writer io.Writer
	count  int64
}

func (s *countingWriter) Write(value []byte) (int, error) {
	written, err := s.writer.Write(value)
	s.count += int64(written)
	return written, err
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}

func newCompressionWriter(writer io.Writer, config Config) (io.WriteCloser, error) {
	switch config.Codec {
	case CodecNone:
		return nopWriteCloser{Writer: writer}, nil
	case CodecGzip:
		return gzip.NewWriterLevel(writer, config.gzipLevel())
	case CodecZstd:
		return zstd.NewWriter(writer, zstd.WithEncoderLevel(config.zstdLevel()))
	default:
		return nil, fmt.Errorf("unsupported JSONL codec %q", config.Codec)
	}
}

package jsonl

import (
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

func NewNodeWriter(writer io.Writer, config Config) (Writer[entity.Node], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	hasher := sha256.New()
	output := newCountingWriter(io.MultiWriter(writer, hasher))

	compressionWriter, err := newCompressionWriter(output, config)
	if err != nil {
		return nil, err
	}

	inputWriter := newCountingWriter(compressionWriter)

	return &EntityWriter[entity.Node, NodeRecord]{
		outputWriter: output,
		hasher:       hasher,

		compressionWriter: compressionWriter,
		inputWriter:       inputWriter,

		entityToRecord: nodeRecord,
		config:         config,
	}, nil
}

func NewRelationshipWriter(writer io.Writer, config Config) (Writer[entity.Relationship], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	hasher := sha256.New()
	output := newCountingWriter(io.MultiWriter(writer, hasher))

	compressionWriter, err := newCompressionWriter(output, config)
	if err != nil {
		return nil, err
	}

	inputWriter := newCountingWriter(compressionWriter)

	return &EntityWriter[entity.Relationship, RelationshipRecord]{
		outputWriter: output,
		hasher:       hasher,

		compressionWriter: compressionWriter,
		inputWriter:       inputWriter,

		entityToRecord: relationshipRecord,
		config:         config,
	}, nil
}

// Probably want ot move this to an external package and return New functions to return struct
type Writer[E entity.Entity] interface {
	Push([]E) error
	Close() (Artifact, error)
}

type EntityWriter[E entity.Entity, R record] struct {
	hasher hash.Hash

	outputWriter      *countingWriter
	compressionWriter io.WriteCloser
	inputWriter       *countingWriter

	recordCount int64

	entityToRecord func(E) R
	config         Config
}

func (s *EntityWriter[E, R]) Push(entities []E) error {
	for _, entity := range entities {
		s.recordCount++

		record := s.entityToRecord(entity)

		if encoded, err := json.Marshal(record); err != nil {
			return err
		} else if _, err := s.inputWriter.Write(append(encoded, '\n')); err != nil {
			return err
		}
	}

	return nil
}

func (s *EntityWriter[E, R]) Close() (Artifact, error) {
	if err := s.compressionWriter.Close(); err != nil {
		return Artifact{}, nil
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
		return gzip.NewWriterLevel(writer, config.Level)
	case CodecZstd:
		return zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.EncoderLevel(config.Level)))
	default:
		return nil, fmt.Errorf("unsupported JSONL codec %q", config.Codec)
	}
}

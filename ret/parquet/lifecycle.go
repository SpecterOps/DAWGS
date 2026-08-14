package parquet

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/specterops/dawgs/ret/entity"
)

var (
	ErrWriterNotOpen   = errors.New("writer is not open")
	ErrWriterNotClosed = errors.New("writer is not closed")
	ErrWriterFailed    = errors.New("writer failed")
	ErrReaderNotOpen   = errors.New("reader is not open")
	ErrReaderNotDone   = errors.New("reader is not done reading")
	ErrReaderFailed    = errors.New("reader failed")
	ErrInvalidLimit    = errors.New("pull limit must be positive")
)

type lifecycleState uint8

const (
	stateOpen lifecycleState = iota
	stateFailed
	stateClosed
)

type row interface {
	NodeRow | RelationshipRow
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

func NewNodeWriter(output io.Writer, config Config) (EntityWriter[entity.Node, NodeRow], error) {
	return newEntityWriter(output, config, func(value entity.Node) (NodeRow, error) {
		if err := value.Validate(); err != nil {
			return NodeRow{}, err
		}
		if err := validateProperties(value.Properties); err != nil {
			return NodeRow{}, fmt.Errorf("validate properties: %w", err)
		}
		return nodeRow(value), nil
	})
}

func NewRelationshipWriter(output io.Writer, config Config) (EntityWriter[entity.Relationship, RelationshipRow], error) {
	return newEntityWriter(output, config, func(value entity.Relationship) (RelationshipRow, error) {
		if value.SourceID == "" {
			return RelationshipRow{}, fmt.Errorf("relationship source ID is required")
		}
		if err := value.Validate(); err != nil {
			return RelationshipRow{}, err
		}
		if err := validateProperties(value.Properties); err != nil {
			return RelationshipRow{}, fmt.Errorf("validate properties: %w", err)
		}
		return relationshipRow(value), nil
	})
}

type EntityWriter[E entity.Entity, R row] struct {
	hasher       hash.Hash
	stored       *countingWriter
	writer       *parquetgo.GenericWriter[R]
	entityToRow  func(E) (R, error)
	recordCount  int64
	state        lifecycleState
	resourceDone bool
	failure      error
}

func newEntityWriter[E entity.Entity, R row](output io.Writer, config Config, convert func(E) (R, error)) (EntityWriter[E, R], error) {
	if err := config.Validate(); err != nil {
		return EntityWriter[E, R]{}, err
	}
	hasher := sha256.New()
	stored := &countingWriter{writer: io.MultiWriter(output, hasher)}
	return EntityWriter[E, R]{
		hasher:      hasher,
		stored:      stored,
		writer:      parquetgo.NewGenericWriter[R](stored, parquetgo.Compression(&zstd.Codec{})),
		entityToRow: convert,
		state:       stateOpen,
	}, nil
}

func (s *EntityWriter[E, R]) Push(entities []E) error {
	switch s.state {
	case stateClosed:
		return ErrWriterNotOpen
	case stateFailed:
		return errors.Join(ErrWriterFailed, s.failure)
	}
	rows := make([]R, len(entities))
	for index, value := range entities {
		row, err := s.entityToRow(value)
		if err != nil {
			return s.fail(fmt.Errorf("validate Parquet record %d: %w", s.recordCount+int64(index)+1, err))
		}
		rows[index] = row
	}
	written, err := s.writer.Write(rows)
	if err != nil {
		return s.fail(fmt.Errorf("write Parquet rows: %w", err))
	}
	if written != len(rows) {
		return s.fail(fmt.Errorf("write Parquet rows: wrote %d, want %d", written, len(rows)))
	}
	s.recordCount += int64(written)
	return nil
}

func (s *EntityWriter[E, R]) Close() error {
	if s.resourceDone {
		if s.state == stateFailed {
			return errors.Join(ErrWriterFailed, s.failure)
		}
		return nil
	}
	s.resourceDone = true
	if err := s.writer.Close(); err != nil {
		s.fail(fmt.Errorf("finish Parquet file: %w", err))
	}
	if s.state == stateFailed {
		return errors.Join(ErrWriterFailed, s.failure)
	}
	s.state = stateClosed
	return nil
}

func (s *EntityWriter[E, R]) Result() (Artifact, error) {
	switch s.state {
	case stateOpen:
		return Artifact{}, ErrWriterNotClosed
	case stateFailed:
		return Artifact{}, errors.Join(ErrWriterFailed, s.failure)
	}
	return Artifact{
		SchemaVersion: SchemaVersion,
		SHA256:        hex.EncodeToString(s.hasher.Sum(nil)),
		Count:         s.recordCount,
		StoredBytes:   s.stored.count,
	}, nil
}

func (s *EntityWriter[E, R]) fail(err error) error {
	if s.failure == nil {
		s.failure = err
	} else {
		s.failure = errors.Join(s.failure, err)
	}
	s.state = stateFailed
	return err
}

func NewNodeReader(input io.ReaderAt, size int64, artifact Artifact) (Reader[entity.Node, NodeRow], error) {
	return newEntityReader(input, size, artifact, func(value NodeRow) (entity.Node, error) {
		return value.entity()
	})
}

func NewRelationshipReader(input io.ReaderAt, size int64, artifact Artifact) (Reader[entity.Relationship, RelationshipRow], error) {
	return newEntityReader(input, size, artifact, func(value RelationshipRow) (entity.Relationship, error) {
		return value.entity()
	})
}

type Reader[E entity.Entity, R row] struct {
	artifact     Artifact
	reader       *parquetgo.GenericReader[R]
	rowToEntity  func(R) (E, error)
	recordCount  int64
	state        lifecycleState
	resourceDone bool
	failure      error
}

func newEntityReader[E entity.Entity, R row](input io.ReaderAt, size int64, artifact Artifact, convert func(R) (E, error)) (Reader[E, R], error) {
	if err := artifact.validate(); err != nil {
		return Reader[E, R]{}, err
	}
	if size != artifact.StoredBytes {
		return Reader[E, R]{}, fmt.Errorf("Parquet stored size mismatch: got %d, want %d", size, artifact.StoredBytes)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, io.NewSectionReader(input, 0, size)); err != nil {
		return Reader[E, R]{}, fmt.Errorf("hash Parquet artifact: %w", err)
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != artifact.SHA256 {
		return Reader[E, R]{}, fmt.Errorf("Parquet stored SHA-256 mismatch: got %s, want %s", actual, artifact.SHA256)
	}
	file, err := parquetgo.OpenFile(input, size)
	if err != nil {
		return Reader[E, R]{}, fmt.Errorf("open Parquet artifact: %w", err)
	}
	wantSchema := parquetgo.SchemaOf(new(R))
	if !parquetgo.EqualNodes(file.Schema(), wantSchema) {
		return Reader[E, R]{}, fmt.Errorf("Parquet row schema does not match %s", wantSchema.Name())
	}
	if file.NumRows() != artifact.Count {
		return Reader[E, R]{}, fmt.Errorf("Parquet row count mismatch: got %d, want %d", file.NumRows(), artifact.Count)
	}
	return Reader[E, R]{
		artifact:    artifact,
		reader:      parquetgo.NewGenericReader[R](file),
		rowToEntity: convert,
		state:       stateOpen,
	}, nil
}

func (s *Reader[E, R]) Pull(limit int) ([]E, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if s.resourceDone {
		return nil, ErrReaderNotOpen
	}
	switch s.state {
	case stateClosed:
		return nil, ErrReaderNotOpen
	case stateFailed:
		return nil, errors.Join(ErrReaderFailed, s.failure)
	}
	rows := make([]R, limit)
	read, readErr := s.reader.Read(rows)
	values := make([]E, 0, read)
	for index := range read {
		value, err := s.rowToEntity(rows[index])
		if err != nil {
			return nil, s.fail(fmt.Errorf("validate Parquet row %d: %w", s.recordCount+int64(index)+1, err))
		}
		values = append(values, value)
	}
	s.recordCount += int64(read)
	if errors.Is(readErr, io.EOF) {
		s.state = stateClosed
		return values, nil
	}
	if readErr != nil {
		return nil, s.fail(fmt.Errorf("read Parquet row %d: %w", s.recordCount+1, readErr))
	}
	return values, nil
}

func (s *Reader[E, R]) Done() bool {
	return s.state != stateOpen
}

func (s *Reader[E, R]) Close() error {
	if s.resourceDone {
		if s.state == stateFailed {
			return errors.Join(ErrReaderFailed, s.failure)
		}
		return nil
	}
	s.resourceDone = true
	if err := s.reader.Close(); err != nil {
		s.fail(fmt.Errorf("close Parquet reader: %w", err))
	}
	if s.state == stateFailed {
		return errors.Join(ErrReaderFailed, s.failure)
	}
	return nil
}

func (s *Reader[E, R]) Result() error {
	switch s.state {
	case stateOpen:
		return ErrReaderNotDone
	case stateFailed:
		return errors.Join(ErrReaderFailed, s.failure)
	}
	if s.recordCount != s.artifact.Count {
		return s.fail(fmt.Errorf("Parquet row count mismatch: got %d, want %d", s.recordCount, s.artifact.Count))
	}
	return nil
}

func (s *Reader[E, R]) fail(err error) error {
	if s.failure == nil {
		s.failure = err
	} else {
		s.failure = errors.Join(s.failure, err)
	}
	s.state = stateFailed
	return err
}

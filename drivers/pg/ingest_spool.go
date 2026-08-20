package pg

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	ingestSpoolWriterLimit = 64
	ingestSpoolMaxRecord   = 64 * 1024 * 1024
	ingestSpoolVersion     = 2
)

var ingestSpoolMagic = [4]byte{'D', 'W', 'G', 'I'}

type ingestPhase uint8

const (
	ingestPhaseNodes ingestPhase = iota + 1
	ingestPhaseEdges
)

type ingestSpoolWriter struct {
	bucket uint64
	file   *os.File
}

type ingestSpool struct {
	runDir      string
	phase       ingestPhase
	bucketCount uint64

	writers        *list.List
	writerElements map[uint64]*list.Element
	populated      map[uint64]struct{}
	bucketSizes    map[uint64]int64
	unrecoverable  map[uint64]error
	bytesWritten   int64
}

func newIngestRunDir(parent string) (string, error) {
	runDir, err := os.MkdirTemp(parent, "dawgs-pg-ingest-")
	if err != nil {
		return "", fmt.Errorf("create ingest run directory: %w", err)
	}

	if err := os.Chmod(runDir, 0o700); err != nil {
		removeErr := os.Remove(runDir)

		return "", errors.Join(fmt.Errorf("set ingest run directory permissions: %w", err), removeErr)
	}

	return runDir, nil
}

func newIngestSpool(runDir string, phase ingestPhase, bucketCount uint64) (*ingestSpool, error) {
	if _, err := newIngestBucketSet(bucketCount); err != nil {
		return nil, err
	}
	if !phase.valid() {
		return nil, fmt.Errorf("invalid ingest spool phase %d", phase)
	}

	return &ingestSpool{
		runDir:         runDir,
		phase:          phase,
		bucketCount:    bucketCount,
		writers:        list.New(),
		writerElements: make(map[uint64]*list.Element),
		populated:      make(map[uint64]struct{}),
		bucketSizes:    make(map[uint64]int64),
		unrecoverable:  make(map[uint64]error),
	}, nil
}

func (s *ingestSpool) Append(bucket uint64, record any) error {
	if bucket >= s.bucketCount {
		return fmt.Errorf("ingest spool bucket %d is outside configured range", bucket)
	}
	if recoveryErr, ok := s.unrecoverable[bucket]; ok {
		return fmt.Errorf("ingest spool bucket %d is unavailable after failed recovery: %w", bucket, recoveryErr)
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal ingest spool record: %w", err)
	}
	if len(payload) == 0 || len(payload) > ingestSpoolMaxRecord {
		return fmt.Errorf("ingest spool record size %d is invalid", len(payload))
	}

	writer, created, err := s.openWriter(bucket)
	if err != nil {
		return err
	}

	var frameLength [4]byte
	binary.BigEndian.PutUint32(frameLength[:], uint32(len(payload)))
	if err := writeAll(writer.file, frameLength[:]); err != nil {
		return s.recoverFailedAppend(bucket, created, fmt.Errorf("write ingest spool frame length: %w", err))
	}
	if err := writeAll(writer.file, payload); err != nil {
		return s.recoverFailedAppend(bucket, created, fmt.Errorf("write ingest spool frame payload: %w", err))
	}

	if created {
		s.populated[bucket] = struct{}{}
		s.bucketSizes[bucket] = int64(ingestSpoolHeaderSize())
	}
	frameSize := int64(len(frameLength) + len(payload))
	s.bucketSizes[bucket] += frameSize
	s.bytesWritten += frameSize
	if created {
		s.bytesWritten += int64(ingestSpoolHeaderSize())
	}

	return nil
}

func (s *ingestSpool) Read(bucket uint64, handle func([]byte) error) error {
	if handle == nil {
		return errors.New("ingest spool read handler is nil")
	}
	if _, ok := s.populated[bucket]; !ok {
		return fmt.Errorf("ingest spool bucket %d is not populated", bucket)
	}
	if recoveryErr, ok := s.unrecoverable[bucket]; ok {
		return fmt.Errorf("ingest spool bucket %d is unavailable after failed recovery: %w", bucket, recoveryErr)
	}
	if err := s.closeWriter(bucket); err != nil {
		return fmt.Errorf("close ingest spool bucket %d before reading: %w", bucket, err)
	}

	file, err := os.Open(s.pathForBucket(bucket))
	if err != nil {
		return fmt.Errorf("open ingest spool bucket %d: %w", bucket, err)
	}
	defer file.Close()

	if err := s.readHeader(file); err != nil {
		return err
	}

	for {
		var frameLength [4]byte
		_, err := io.ReadFull(file, frameLength[:])
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read ingest spool frame length: %w", err)
		}

		length := binary.BigEndian.Uint32(frameLength[:])
		if length == 0 || length > ingestSpoolMaxRecord {
			return fmt.Errorf("invalid ingest spool record size %d", length)
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(file, payload); err != nil {
			return fmt.Errorf("read ingest spool frame payload: %w", err)
		}
		decoded, err := decodeIngestSpoolPayload(payload)
		if err != nil {
			return err
		}
		if err := handle(decoded); err != nil {
			return err
		}
	}
}

func (s *ingestSpool) PopulatedBuckets() []uint64 {
	buckets := make([]uint64, 0, len(s.populated))
	for bucket := range s.populated {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(left, right int) bool { return buckets[left] < buckets[right] })

	return buckets
}

func (s *ingestSpool) PopulatedBucketCount() int {
	return len(s.populated)
}

func (s *ingestSpool) Close() error {
	var result error
	for s.writers.Len() > 0 {
		element := s.writers.Front()
		writer := element.Value.(*ingestSpoolWriter)
		s.writers.Remove(element)
		delete(s.writerElements, writer.bucket)
		result = errors.Join(result, writer.file.Close())
	}

	return result
}

func (s *ingestSpool) RemoveFiles() error {
	result := s.Close()
	for bucket := range s.populated {
		if err := os.Remove(s.pathForBucket(bucket)); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove ingest spool bucket %d: %w", bucket, err))
			continue
		}
		delete(s.populated, bucket)
		delete(s.bucketSizes, bucket)
		delete(s.unrecoverable, bucket)
	}

	return result
}

func (s *ingestSpool) BytesWritten() int64 {
	return s.bytesWritten
}

func (s *ingestSpool) pathForBucket(bucket uint64) string {
	return filepath.Join(s.runDir, fmt.Sprintf("%s-%020d.dwgi", s.phase.filename(), bucket))
}

func (s *ingestSpool) openWriter(bucket uint64) (*ingestSpoolWriter, bool, error) {
	if element, ok := s.writerElements[bucket]; ok {
		s.writers.MoveToFront(element)

		return element.Value.(*ingestSpoolWriter), false, nil
	}

	if s.writers.Len() >= ingestSpoolWriterLimit {
		if err := s.evictWriter(); err != nil {
			return nil, false, err
		}
	}

	if _, ok := s.populated[bucket]; ok {
		file, err := os.OpenFile(s.pathForBucket(bucket), os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return nil, false, fmt.Errorf("reopen ingest spool bucket %d: %w", bucket, err)
		}

		return s.cacheWriter(bucket, file), false, nil
	}

	file, err := os.OpenFile(s.pathForBucket(bucket), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("create ingest spool bucket %d: %w", bucket, err)
	}
	if err := writeAll(file, s.header()); err != nil {
		closeErr := file.Close()
		removeErr := os.Remove(s.pathForBucket(bucket))

		return nil, false, errors.Join(fmt.Errorf("write ingest spool header: %w", err), closeErr, removeErr)
	}

	return s.cacheWriter(bucket, file), true, nil
}

func (s *ingestSpool) cacheWriter(bucket uint64, file *os.File) *ingestSpoolWriter {
	writer := &ingestSpoolWriter{bucket: bucket, file: file}
	s.writerElements[bucket] = s.writers.PushFront(writer)

	return writer
}

func (s *ingestSpool) evictWriter() error {
	element := s.writers.Back()
	if element == nil {
		return nil
	}
	writer := element.Value.(*ingestSpoolWriter)
	s.writers.Remove(element)
	delete(s.writerElements, writer.bucket)
	if err := writer.file.Close(); err != nil {
		return fmt.Errorf("close evicted ingest spool bucket %d: %w", writer.bucket, err)
	}

	return nil
}

func (s *ingestSpool) closeWriter(bucket uint64) error {
	element, ok := s.writerElements[bucket]
	if !ok {
		return nil
	}
	writer := element.Value.(*ingestSpoolWriter)
	s.writers.Remove(element)
	delete(s.writerElements, bucket)

	return writer.file.Close()
}

func (s *ingestSpool) recoverFailedAppend(bucket uint64, created bool, cause error) error {
	closeErr := s.closeWriter(bucket)
	if created {
		return errors.Join(cause, closeErr, os.Remove(s.pathForBucket(bucket)))
	}

	size, ok := s.bucketSizes[bucket]
	if !ok {
		missingSizeErr := fmt.Errorf("missing committed size for ingest spool bucket %d", bucket)
		s.unrecoverable[bucket] = missingSizeErr

		return errors.Join(cause, closeErr, missingSizeErr)
	}
	truncateErr := os.Truncate(s.pathForBucket(bucket), size)
	if truncateErr != nil {
		s.unrecoverable[bucket] = truncateErr
	}

	return errors.Join(cause, closeErr, truncateErr)
}

func (s *ingestSpool) header() []byte {
	return []byte{ingestSpoolMagic[0], ingestSpoolMagic[1], ingestSpoolMagic[2], ingestSpoolMagic[3], ingestSpoolVersion, byte(s.phase)}
}

func (s *ingestSpool) readHeader(reader io.Reader) error {
	header := make([]byte, ingestSpoolHeaderSize())
	if _, err := io.ReadFull(reader, header); err != nil {
		return fmt.Errorf("read ingest spool header: %w", err)
	}
	if !bytes.Equal(header[:4], ingestSpoolMagic[:]) || header[4] != ingestSpoolVersion || header[5] != byte(s.phase) {
		return errors.New("invalid ingest spool header")
	}

	return nil
}

func (s *ingestSpool) phaseName() string {
	return s.phase.filename()
}

func (s ingestPhase) valid() bool {
	return s == ingestPhaseNodes || s == ingestPhaseEdges
}

func (s ingestPhase) filename() string {
	switch s {
	case ingestPhaseNodes:
		return "nodes"
	case ingestPhaseEdges:
		return "edges"
	default:
		return "invalid"
	}
}

func ingestSpoolHeaderSize() int {
	return len(ingestSpoolMagic) + 2
}

func decodeIngestSpoolPayload(payload []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode ingest spool record: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decode ingest spool record: multiple JSON values")
		}

		return nil, fmt.Errorf("decode ingest spool record trailing data: %w", err)
	}
	decoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode decoded ingest spool record: %w", err)
	}

	return decoded, nil
}

func writeAll(file *os.File, payload []byte) error {
	for len(payload) > 0 {
		written, err := file.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}

	return nil
}

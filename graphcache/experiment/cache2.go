package experiment
//
//import (
//	"bytes"
//	"compress/gzip"
//	"encoding/binary"
//	"errors"
//	"fmt"
//	"io"
//	"math"
//	"runtime"
//	"sort"
//	"sync"
//	"time"
//
//	"github.com/OneOfOne/xxhash"
//	"github.com/specterops/dawgs/graph"
//)
//
//type entry struct {
//	IDHash     uint64
//	EntityHash []byte
//}
//
//type bucket struct {
//	compressed     bool
//	pendingChanges []entry
//	buffer         *bytes.Buffer
//}
//
//func newBucket() *bucket {
//	return &bucket{
//		compressed: false,
//		buffer:     &bytes.Buffer{},
//	}
//}
//
//func (s *bucket) readCompact(frameBuffer []byte, reader io.Reader) ([]entry, error) {
//	var (
//		liveEntries      []entry
//		nextEntityIDHash uint64
//	)
//
//	// Sort pending changes in reverse order to avoid shuffling memory during pops
//	sort.Slice(s.pendingChanges, func(i, j int) bool {
//		return s.pendingChanges[i].IDHash > s.pendingChanges[j].IDHash
//	})
//
//	if reader != nil {
//		if frameSize, err := reader.Read(frameBuffer); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
//			return nil, err
//		} else {
//			for entryIdx := 0; entryIdx < frameSize; entryIdx += EntrySize {
//				var (
//					entityHashBuffer          = make([]byte, EntrySize/2)
//					lastInsertedPendingChange *entry
//				)
//
//				nextEntityIDHash = binary.BigEndian.Uint64(frameBuffer[entryIdx : entryIdx+EntrySize/2])
//				copy(entityHashBuffer, frameBuffer[entryIdx+EntrySize/2:entryIdx+EntrySize])
//
//				// Insert pending changes in sorted order
//				for len(s.pendingChanges) > 0 && nextEntityIDHash > s.pendingChanges[len(s.pendingChanges)-1].IDHash {
//					lastInsertedPendingChange = &s.pendingChanges[len(s.pendingChanges)-1]
//					liveEntries = append(liveEntries, *lastInsertedPendingChange)
//
//					// Pop the pending change
//					s.pendingChanges = s.pendingChanges[:len(s.pendingChanges)-1]
//				}
//
//				// If there was an inserted change, check if it overwrote this entry. If so, do not insert the entry
//				// read from the archive
//				if lastInsertedPendingChange == nil || lastInsertedPendingChange.IDHash != nextEntityIDHash {
//					liveEntries = append(liveEntries, entry{
//						IDHash:     nextEntityIDHash,
//						EntityHash: entityHashBuffer,
//					})
//				}
//			}
//		}
//
//	}
//
//	// Append any remaining changes to the end, sorted
//	if len(s.pendingChanges) > 0 {
//		for idx := len(s.pendingChanges) - 1; idx >= 0; idx-- {
//			liveEntries = append(liveEntries, s.pendingChanges[idx])
//		}
//	}
//
//	s.pendingChanges = nil
//	return liveEntries, nil
//}
//
//type emptyReader struct{}
//
//func (e emptyReader) Read(p []byte) (n int, err error) {
//	return 0, io.EOF
//}
//
//func (e emptyReader) Close() error {
//	return nil
//}
//
//func (e emptyReader) Reset(reader io.Reader) error {
//	return nil
//}
//
//var emptyReaderInst = emptyReader{}
//
//type nopWriteCloser struct {
//	io.Writer
//}
//
//func (s nopWriteCloser) Close() error {
//	return nil
//}
//
//func newNopWriteCloser(w io.Writer) io.WriteCloser {
//	return nopWriteCloser{
//		Writer: w,
//	}
//}
//
//func (s *bucket) Compact(frameBuffer []byte, compressionWriter *gzip.Writer) error {
//	// Early exit if this bucket was not mutated
//	if len(s.pendingChanges) == 0 {
//		return nil
//	}
//
//	var (
//		reader io.Reader = emptyReaderInst
//		writer           = newNopWriteCloser(s.buffer)
//	)
//
//	if s.compressed && s.buffer.Len() > 0 {
//		if frameReader, err := gzip.NewReader(s.buffer); err != nil {
//			return err
//		} else {
//			reader = frameReader
//		}
//	} else {
//		reader = s.buffer
//	}
//
//	if liveEntries, err := s.readCompact(frameBuffer, reader); err != nil {
//		return err
//	} else {
//		var (
//			idHashBuffer = make([]byte, 16)
//			nextEntry    entry
//		)
//
//		// Always reset the buffer
//		s.buffer.Reset()
//
//		// Test block size to conditionally switch compression on
//		s.compressed = len(liveEntries)*EntrySize >= CompressedBlockSizeTrigger
//
//		if s.compressed {
//			compressionWriter.Reset(s.buffer)
//			writer = compressionWriter
//		}
//
//		for idx := 0; idx < len(liveEntries); idx++ {
//			nextEntry = liveEntries[idx]
//
//			binary.BigEndian.PutUint64(idHashBuffer, nextEntry.IDHash)
//			copy(idHashBuffer[8:], nextEntry.EntityHash)
//
//			if _, err := writer.Write(idHashBuffer); err != nil {
//				return err
//			}
//		}
//
//		if s.compressed {
//			if err := writer.Close(); err != nil {
//				return err
//			}
//
//			//fmt.Printf("Wrote %d entries totaling %d bytes and compressed down to %d bytes\n", len(liveEntries), len(liveEntries)*EntrySize, s.buffer.Len())
//		}
//
//		// Reset the buffer to trim the allocation
//		s.buffer = bytes.NewBuffer(s.buffer.Bytes()[0:s.buffer.Len():s.buffer.Len()])
//		return nil
//	}
//}
//
//func (s *bucket) PutEntity(entityIDHash uint64, entityHash []byte) {
//	s.pendingChanges = append(s.pendingChanges, entry{
//		IDHash:     entityIDHash,
//		EntityHash: entityHash,
//	})
//}
//
//func (s *bucket) decompress(frameBuffer *bytes.Buffer) error {
//	if reader, err := gzip.NewReader(bytes.NewBuffer(s.buffer.Bytes())); err != nil {
//		return err
//	} else {
//		var (
//			readBuffer     = make([]byte, 4096)
//			totalFrameSize = 0
//		)
//
//		for {
//			readBytes, err := reader.Read(readBuffer)
//
//			frameBuffer.Write(readBuffer[:readBytes])
//			totalFrameSize += readBytes
//
//			if err != nil {
//				if isEOFLikeError(err) {
//					break
//				}
//
//				return err
//			}
//		}
//	}
//
//	return nil
//}
//
//func (s *bucket) Open(logic func(bucketBytes []byte)) error {
//	var frameBuffer *bytes.Buffer
//
//	if s.compressed {
//		frameBuffer = bytes.NewBuffer(make([]byte, 0, BlockSize))
//
//		if err := s.decompress(frameBuffer); err != nil {
//			return err
//		}
//	} else {
//		frameBuffer = bytes.NewBuffer(s.buffer.Bytes())
//	}
//
//	if frameBuffer.Len() > 0 {
//		logic(frameBuffer.Bytes())
//	}
//
//	return nil
//}
//
//func isEOFLikeError(err error) bool {
//	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
//}
//
//type PackMap struct {
//	pendingChanges   int
//	buckets          []*bucket
//	allocatedBuckets []*bucket
//	digester         *xxhash.XXHash64
//}
//
//func NewPackMap() *PackMap {
//	return &PackMap{
//		pendingChanges: 0,
//		buckets:        make([]*bucket, NumMapBuckets),
//		digester:       xxhash.New64(),
//	}
//}
//
//const bucketStride = math.MaxUint64 / NumMapBuckets
//
//func (s *PackMap) Stat() int {
//	return len(s.allocatedBuckets)
//}
//
//func (s *PackMap) getBucket(entityIDHash uint64) *bucket {
//	var (
//		bucketIdx      = entityIDHash / bucketStride
//		existingBucket = s.buckets[bucketIdx]
//	)
//
//	if existingBucket == nil {
//		existingBucket = newBucket()
//
//		s.buckets[bucketIdx] = existingBucket
//		s.allocatedBuckets = append(s.allocatedBuckets, existingBucket)
//	}
//
//	return existingBucket
//}
//
//func (s *PackMap) Compact() error {
//	then := time.Now()
//
//	defer func() {
//		if elapsed := time.Since(then); elapsed > 4*time.Second {
//			fmt.Printf("Compact took %d ms\n", time.Since(then).Milliseconds())
//		}
//	}()
//
//	var (
//		frameBuffer = make([]byte, BlockSize)
//		writer      = gzip.NewWriter(nil)
//	)
//
//	for _, nextBucket := range s.allocatedBuckets {
//		if err := nextBucket.Compact(frameBuffer, writer); err != nil {
//			return err
//		}
//
//		outputFrame(nextBucket.buffer.Bytes())
//	}
//
//	return nil
//}
//
//func (s *PackMap) CompactParallel() error {
//	then := time.Now()
//
//	defer func() {
//		if elapsed := time.Since(then); elapsed > 4*time.Second {
//			fmt.Printf("Compact took %d ms\n", time.Since(then).Milliseconds())
//		}
//	}()
//
//	var (
//		numWorkers = runtime.NumCPU() / 4
//		errorC     = make(chan error, numWorkers)
//		workC      = make(chan *bucket)
//		wg         = &sync.WaitGroup{}
//	)
//
//	for workerID := 0; workerID < numWorkers; workerID++ {
//		wg.Add(1)
//
//		go func(workerID int, frameBuffer []byte, writer *gzip.Writer) {
//			defer wg.Done()
//
//			for nextBucket := range workC {
//				if err := nextBucket.Compact(frameBuffer, writer); err != nil {
//					errorC <- err
//					break
//				}
//			}
//		}(workerID, make([]byte, BlockSize), gzip.NewWriter(nil))
//	}
//
//	for idx := 0; idx < len(s.allocatedBuckets); idx++ {
//		workC <- s.allocatedBuckets[idx]
//	}
//
//	close(workC)
//	wg.Wait()
//	close(errorC)
//
//	var workerErrors []error
//
//	for nextError := range errorC {
//		workerErrors = append(workerErrors, nextError)
//	}
//
//	return errors.Join(workerErrors...)
//}
//
//func (s *PackMap) GetEntity(entityID string) ([]byte, error) {
//	var entityHash []byte
//
//	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
//		return nil, err
//	} else if err := s.getBucket(entityIDHash).Open(func(bucketBytes []byte) {
//		entityHash, _ = binarySearchFrame(bucketBytes, make([]byte, 8), entityIDHash)
//	}); err != nil {
//		return nil, err
//	}
//
//	return entityHash, nil
//}
//
//func (s *PackMap) PutNode(entityID string, node *graph.Node) ([]byte, error) {
//	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
//		return nil, err
//	} else if entityHash, err := digestNodeHash(s.digester, node); err != nil {
//		return nil, err
//	} else {
//		s.getBucket(entityIDHash).PutEntity(entityIDHash, entityHash)
//
//		// Track this change and compact regularly
//		const flushSize = NumMaxEntries / 8000
//
//		if s.pendingChanges += 1; s.pendingChanges >= flushSize {
//			s.pendingChanges = 0
//			return entityHash, s.CompactParallel()
//		}
//
//		return entityHash, nil
//	}
//}

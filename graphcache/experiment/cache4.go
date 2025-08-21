package experiment
//
//import (
//	"bytes"
//	"encoding/binary"
//	"errors"
//	"fmt"
//	"io"
//	"sort"
//	"time"
//
//	"github.com/OneOfOne/xxhash"
//	"github.com/klauspost/compress/zstd"
//	"github.com/specterops/dawgs/graph"
//)
//
//type entry struct {
//	IDHash     uint64
//	EntityHash []byte
//}
//
//type bucket struct {
//	pendingChanges []entry
//	pendingDeletes []uint64
//	buffer         *bytes.Buffer
//}
//
//func newBucket() *bucket {
//	return &bucket{
//		buffer: &bytes.Buffer{},
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
//		return s.pendingChanges[i].IDHash < s.pendingChanges[j].IDHash
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
//				// If the pending change overwrote this entry, do not insert the entry read from the archive
//				if lastInsertedPendingChange != nil && nextEntityIDHash != lastInsertedPendingChange.IDHash {
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
//	s.pendingDeletes = nil
//
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
//func (s *bucket) Compact(frameBuffer []byte) error {
//	var reader io.Reader
//
//	if s.buffer.Len() > 0 {
//		//if frameReader, err := zstd.NewReader(s.buffer); err != nil {
//		//	return err
//		//} else {
//		//	reader = frameReader
//		//}
//
//		reader = s.buffer
//	}
//
//	if liveEntries, err := s.readCompact(frameBuffer, reader); err != nil {
//		return err
//	} else {
//		var (
//			entryBuffer = make([]byte, 16)
//			nextEntry   entry
//		)
//
//		for idx := 0; idx < len(liveEntries); idx++ {
//			nextEntry = liveEntries[idx]
//
//			binary.BigEndian.PutUint64(entryBuffer, nextEntry.IDHash)
//			copy(entryBuffer[8:], nextEntry.EntityHash)
//
//			if _, err := s.buffer.Write(entryBuffer); err != nil {
//				return err
//			}
//		}
//
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
//func (s *bucket) Open(reader *zstd.Decoder, logic func(uncompressed []byte)) error {
//	if err := reader.Reset(bytes.NewBuffer(s.buffer.Bytes())); err != nil {
//		return err
//	} else {
//		readBuffer := make([]byte, BlockSize)
//
//		if frameSize, err := reader.Read(readBuffer); err != nil && !errors.Is(err, io.EOF) {
//			return err
//		} else {
//			logic(readBuffer[:frameSize])
//		}
//	}
//
//	return nil
//}
//
//type PackMap struct {
//	pendingChanges int
//	buckets        []*bucket
//	digester       *xxhash.XXHash64
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
//func (s *PackMap) getBucket(entityIDHash uint64) *bucket {
//	var (
//		bucketIdx      = entityIDHash % NumMapBuckets
//		existingBucket = s.buckets[bucketIdx]
//	)
//
//	if existingBucket == nil {
//		existingBucket = newBucket()
//		s.buckets[bucketIdx] = existingBucket
//	}
//
//	return existingBucket
//}
//
//func (s *PackMap) Compact() error {
//	fmt.Printf("Compacting\n")
//
//	then := time.Now()
//
//	defer func() {
//		fmt.Printf("Compact took %d ms\n", time.Since(then).Milliseconds())
//	}()
//
//	frameBuffer := make([]byte, BlockSize)
//
//	for _, nextBucket := range s.buckets {
//		if nextBucket != nil {
//			if err := nextBucket.Compact(frameBuffer); err != nil {
//				return err
//			}
//		}
//	}
//
//	return nil
//}
//
//func (s *PackMap) PutNode(entityID string, node *graph.Node) error {
//	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
//		return err
//	} else if entityHash, err := digestNodeHash(s.digester, node); err != nil {
//		return err
//	} else {
//		s.getBucket(entityIDHash).PutEntity(entityIDHash, entityHash)
//
//		// Track this change and compact regularly
//		if s.pendingChanges += 1; s.pendingChanges > NumMaxEntries/1000 {
//			//s.pendingChanges = 0
//			//return s.Compact()
//		}
//	}
//
//	return nil
//}

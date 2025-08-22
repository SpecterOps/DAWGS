package experiment

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/OneOfOne/xxhash"
	"github.com/klauspost/compress/zstd"
	"github.com/specterops/dawgs/graph"
)

const (
	NumMaxEntries              = 8_000_000_000
	EntrySize                  = 16
	BlockSize                  = 1024 * 256
	CompressedBlockSizeTrigger = 16384 * 2
	EntriesPerBlock            = BlockSize / EntrySize
	NumMapBuckets              = NumMaxEntries / BlockSize
)

func digestEntityIDHash(digester *xxhash.XXHash64, value string) (uint64, error) {
	digester.Reset()

	if _, err := digester.WriteString(value); err != nil {
		return 0, err
	}

	return digester.Sum64(), nil
}

func digestNodeHash(digester *xxhash.XXHash64, node *graph.Node) ([]byte, error) {
	digester.Reset()

	kindStrings := node.Kinds.Strings()
	slices.Sort(kindStrings)

	for _, kindString := range kindStrings {
		if _, err := digester.WriteString(kindString); err != nil {
			return nil, err
		}
	}

	for key, value := range node.Properties.Map {
		if _, err := digester.Write([]byte(key)); err != nil {
			return nil, err
		}

		if valueContent, err := json.Marshal(value); err != nil {
			return nil, err
		} else if _, err := digester.Write(valueContent); err != nil {
			return nil, err
		}
	}

	return digester.Sum(make([]byte, 0, 8)), nil
}

type Frame struct {
	buffer *bytes.Buffer
}

func NewFrame() *Frame {
	return &Frame{
		buffer: &bytes.Buffer{},
	}
}

type ProjectedEntry struct {
	IDHash     uint64
	EntityHash []byte
}

type ProjectedFrame struct {
	Entries []ProjectedEntry
}

func NewProjectedFrame() *ProjectedFrame {
	return &ProjectedFrame{}
}

func (s *ProjectedFrame) Size() int {
	return len(s.Entries) * EntrySize
}

func (s *ProjectedFrame) sort() {
	sort.Slice(s.Entries, func(i, j int) bool {
		return s.Entries[i].IDHash < s.Entries[j].IDHash
	})
}

func (s *ProjectedFrame) Put(entry ProjectedEntry) bool {
	s.Entries = append(s.Entries, entry)
	return len(s.Entries) >= EntriesPerBlock
}

func (s *ProjectedFrame) Write(writer io.Writer) error {
	s.sort()

	var (
		idHashBuffer = make([]byte, 16)
		nextEntry    ProjectedEntry
	)

	for idx := 0; idx < len(s.Entries); idx++ {
		nextEntry = s.Entries[idx]

		binary.BigEndian.PutUint64(idHashBuffer, nextEntry.IDHash)
		copy(idHashBuffer[8:], nextEntry.EntityHash[:])

		if _, err := writer.Write(idHashBuffer); err != nil {
			return err
		}
	}

	// Upon success, clear out the pending entries
	s.Entries = nil
	return nil
}

type Archive struct {
	frames         []*Frame
	projectedFrame *ProjectedFrame
}

func NewArchive() *Archive {
	return &Archive{
		projectedFrame: NewProjectedFrame(),
	}
}

func (s *Archive) Flush(writer *zstd.Encoder) error {
	newFrame := NewFrame()
	s.frames = append(s.frames, newFrame)

	// Reset the zstd writer for this block
	writer.Reset(newFrame.buffer)

	if err := s.projectedFrame.Write(writer); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Archive) PutEntity(entityIDHash uint64, entityHash []byte) error {
	//fmt.Printf("Submitting ID %d\n", entityIDHash)

	if s.projectedFrame.Put(ProjectedEntry{
		IDHash:     entityIDHash,
		EntityHash: entityHash,
	}) {
		if writer, err := zstd.NewWriter(nil); err != nil {
			return err
		} else {
			return s.Flush(writer)
		}
	}

	return nil
}

func outputFrame(frame []byte) {
	return

	for idx := 0; idx < len(frame); idx += EntrySize {
		fmt.Printf("idx: %d - id hash: %v+ - value hash: %v+\n",
			idx,
			//binary.BigEndian.Uint64(frame[idx:idx+EntrySize/2]),
			frame[idx:idx+EntrySize/2],
			frame[idx+EntrySize/2:idx+EntrySize])
	}

	fmt.Println()
}

func binarySearchFrame(frameBytes []byte, entityHashBuffer []byte, entityIDHash uint64) ([]byte, bool) {
	var (
		nextEntityIDHash uint64
		lowEntry         = 0
		highEntry        = (len(frameBytes) - 1) / EntrySize
	)

	for lowEntry <= highEntry {
		var (
			midEntry = lowEntry + (highEntry-lowEntry)/2
			entryIdx = midEntry * EntrySize
		)

		nextEntityIDHash = binary.BigEndian.Uint64(frameBytes[entryIdx : entryIdx+EntrySize/2])
		copy(entityHashBuffer, frameBytes[entryIdx+EntrySize/2:entryIdx+EntrySize])

		if nextEntityIDHash == entityIDHash {
			return entityHashBuffer, true
		}

		if nextEntityIDHash < entityIDHash {
			lowEntry = midEntry + 1
		} else {
			highEntry = midEntry - 1
		}
	}

	return nil, false
}

func (s *Archive) Get(entityIDHash uint64) ([]byte, error) {
	if zstdReader, err := zstd.NewReader(nil); err != nil {
		return nil, err
	} else {
		var (
			nextBlockBuffer  = make([]byte, BlockSize)
			entityHashBuffer = make([]byte, EntrySize/2)
		)

		for _, block := range s.frames {
			if err := zstdReader.Reset(bytes.NewBuffer(block.buffer.Bytes())); err != nil {
				return nil, err
			}

			if bytesRead, readErr := zstdReader.Read(nextBlockBuffer); readErr != nil && !errors.Is(readErr, io.EOF) {
				return nil, readErr
			} else if hash, found := binarySearchFrame(nextBlockBuffer[:bytesRead], entityHashBuffer, entityIDHash); found {
				return hash, nil
			}
		}
	}

	return nil, nil
}

type PackedMap struct {
	buckets  []*Archive
	digester *xxhash.XXHash64
}

func NewBlockMap() *PackedMap {
	return &PackedMap{
		buckets:  make([]*Archive, NumMapBuckets),
		digester: xxhash.New64(),
	}
}

func (s *PackedMap) getBucket(entityIDHash uint64) *Archive {
	var (
		bucketIdx = entityIDHash % NumMapBuckets
		bucket    = s.buckets[bucketIdx]
	)

	if bucket == nil {
		bucket = NewArchive()
		s.buckets[bucketIdx] = bucket
	}

	return bucket
}

func (s *PackedMap) Flush() error {
	if writer, err := zstd.NewWriter(nil); err != nil {
		return err
	} else {
		for _, bucket := range s.buckets {
			if bucket != nil {
				if err := bucket.Flush(writer); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *PackedMap) PutNode(entityID string, node *graph.Node) error {
	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
		return err
	} else if entityHash, err := digestNodeHash(s.digester, node); err != nil {
		return err
	} else {
		return s.getBucket(entityIDHash).PutEntity(entityIDHash, entityHash)
	}
}

func (s *PackedMap) Get(entityID string) ([]byte, error) {
	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
		return nil, err
	} else {
		return s.getBucket(entityIDHash).Get(entityIDHash)
	}
}

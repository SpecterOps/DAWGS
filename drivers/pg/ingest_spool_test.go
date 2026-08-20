package pg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngestOptionsRejectInvalidBucketCounts(t *testing.T) {
	for _, bucketCount := range []int{-1, 0, 3, 6} {
		t.Run("invalid", func(t *testing.T) {
			require.Error(t, validateIngestOptions(IngestOptions{BucketCount: bucketCount}))
		})
	}
}

func TestIngestOptionsAcceptPowerOfTwoBucketCounts(t *testing.T) {
	for _, bucketCount := range []int{1, 2, 4, 256} {
		t.Run("valid", func(t *testing.T) {
			require.NoError(t, validateIngestOptions(IngestOptions{BucketCount: bucketCount}))
		})
	}

	maximumBucketCount := uint64(math.MaxUint32) + 1
	if uint64(int(maximumBucketCount)) == maximumBucketCount {
		require.NoError(t, validateIngestOptions(IngestOptions{BucketCount: int(maximumBucketCount)}))
	}
	if tooManyBuckets := maximumBucketCount + 1; uint64(int(tooManyBuckets)) == tooManyBuckets {
		require.Error(t, validateIngestOptions(IngestOptions{BucketCount: int(tooManyBuckets)}))
	}
}

func TestIngestInputAllowsNilSequences(t *testing.T) {
	input := IngestInput{}
	require.Nil(t, input.Nodes)
	require.Nil(t, input.Edges)
}

func TestIngestSpoolCreatesPrivateRunDirectoryAndLazyBucketFiles(t *testing.T) {
	parent := t.TempDir()
	runDir, err := newIngestRunDir(parent)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

	info, err := os.Stat(runDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())

	spool, err := newIngestSpool(runDir, ingestPhaseNodes, 4)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })

	entries, err := os.ReadDir(runDir)
	require.NoError(t, err)
	require.Empty(t, entries)

	require.NoError(t, spool.Append(2, map[string]any{"record": "first"}))
	path := spool.pathForBucket(2)
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte{'D', 'W', 'G', 'I', 2, byte(ingestPhaseNodes)}, contents[:6])
	require.Equal(t, uint32(len(contents)-10), binary.BigEndian.Uint32(contents[6:10]))
	require.Equal(t, int64(len(contents)), spool.BytesWritten())

	bytesBeforeFailure := spool.BytesWritten()
	require.Error(t, spool.Append(3, math.Inf(1)))
	require.Equal(t, bytesBeforeFailure, spool.BytesWritten())
}

func TestIngestSpoolPreservesBucketOrderAcrossWriterEviction(t *testing.T) {
	runDir, err := newIngestRunDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

	spool, err := newIngestSpool(runDir, ingestPhaseNodes, 128)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })

	require.NoError(t, spool.Append(0, map[string]any{"sequence": 0}))
	for bucket := uint64(1); bucket <= 64; bucket++ {
		require.NoError(t, spool.Append(bucket, map[string]any{"sequence": bucket}))
	}
	require.LessOrEqual(t, spool.writers.Len(), 64)
	require.NoError(t, spool.Append(0, map[string]any{"sequence": 65}))

	var sequences []json.Number
	require.NoError(t, spool.Read(0, func(payload []byte) error {
		var record map[string]json.Number
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		sequences = append(sequences, record["sequence"])

		return nil
	}))
	require.Equal(t, []json.Number{"0", "65"}, sequences)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64}, spool.PopulatedBuckets())
}

func TestIngestSpoolEvictsTheLeastRecentlyUsedWriter(t *testing.T) {
	runDir, err := newIngestRunDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

	spool, err := newIngestSpool(runDir, ingestPhaseNodes, 128)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })

	for bucket := uint64(0); bucket < 64; bucket++ {
		require.NoError(t, spool.Append(bucket, map[string]any{"sequence": bucket}))
	}
	require.NoError(t, spool.Append(0, map[string]any{"sequence": 64}))
	require.NoError(t, spool.Append(64, map[string]any{"sequence": 65}))

	require.Contains(t, spool.writerElements, uint64(0))
	require.NotContains(t, spool.writerElements, uint64(1))

	require.NoError(t, spool.Append(1, map[string]any{"sequence": 66}))
	require.Contains(t, spool.writerElements, uint64(1))
	var sequences []json.Number
	require.NoError(t, spool.Read(1, func(payload []byte) error {
		var record map[string]json.Number
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		sequences = append(sequences, record["sequence"])

		return nil
	}))
	require.Equal(t, []json.Number{"1", "66"}, sequences)
}

func TestIngestSpoolRejectsCorruptFrames(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		frame []byte
	}{
		{name: "zero length", frame: []byte{0, 0, 0, 0}},
		{name: "truncated payload", frame: []byte{0, 0, 0, 2, '{'}},
		{name: "oversized payload", frame: oversizedIngestSpoolFrame()},
		{name: "invalid JSON", frame: []byte{0, 0, 0, 1, '{'}},
		{name: "trailing JSON", frame: []byte{0, 0, 0, 3, '{', '}', 'x'}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runDir, err := newIngestRunDir(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

			spool, err := newIngestSpool(runDir, ingestPhaseEdges, 1)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, spool.Close()) })
			require.NoError(t, spool.Append(0, map[string]any{"valid": true}))
			require.NoError(t, spool.Close())

			file, err := os.OpenFile(spool.pathForBucket(0), os.O_APPEND|os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = file.Write(testCase.frame)
			require.NoError(t, err)
			require.NoError(t, file.Close())

			require.Error(t, spool.Read(0, func([]byte) error { return nil }))
		})
	}
}

func TestIngestSpoolRejectsMalformedHeadersAndPartialFrameLengths(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		contents func(*ingestSpool) []byte
	}{
		{name: "truncated header", contents: func(*ingestSpool) []byte { return []byte{'D', 'W', 'G'} }},
		{name: "wrong magic", contents: func(*ingestSpool) []byte { return []byte{'X', 'W', 'G', 'I', 2, byte(ingestPhaseEdges)} }},
		{name: "wrong phase", contents: func(spool *ingestSpool) []byte { return []byte{'D', 'W', 'G', 'I', 2, byte(ingestPhaseNodes)} }},
		{name: "partial frame length", contents: func(spool *ingestSpool) []byte { return append(spool.header(), 0) }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runDir, err := newIngestRunDir(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

			spool, err := newIngestSpool(runDir, ingestPhaseEdges, 1)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, spool.Close()) })
			require.NoError(t, spool.Append(0, map[string]any{"valid": true}))
			require.NoError(t, spool.Close())
			require.NoError(t, os.WriteFile(spool.pathForBucket(0), testCase.contents(spool), 0o600))

			require.Error(t, spool.Read(0, func([]byte) error { return nil }))
		})
	}
}

func TestNewIngestSpoolRejectsInvalidPhaseAndBucketCount(t *testing.T) {
	_, err := newIngestSpool(t.TempDir(), ingestPhase(0), 1)
	require.Error(t, err)

	_, err = newIngestSpool(t.TempDir(), ingestPhaseNodes, 0)
	require.Error(t, err)

	_, err = newIngestSpool(t.TempDir(), ingestPhaseNodes, 3)
	require.Error(t, err)
}

func TestIngestSpoolKeepsCommittedRecordsAfterAppendWriteFailure(t *testing.T) {
	runDir, err := newIngestRunDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

	spool, err := newIngestSpool(runDir, ingestPhaseNodes, 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })
	require.NoError(t, spool.Append(0, map[string]any{"sequence": 1}))

	writer := spool.writerElements[0].Value.(*ingestSpoolWriter)
	require.NoError(t, writer.file.Close())
	require.Error(t, spool.Append(0, map[string]any{"sequence": 2}))

	var sequences []json.Number
	require.NoError(t, spool.Read(0, func(payload []byte) error {
		var record map[string]json.Number
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		sequences = append(sequences, record["sequence"])

		return nil
	}))
	require.Equal(t, []json.Number{"1"}, sequences)

	require.NoError(t, spool.Append(0, map[string]any{"sequence": 3}))
	sequences = nil
	require.NoError(t, spool.Read(0, func(payload []byte) error {
		var record map[string]json.Number
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		sequences = append(sequences, record["sequence"])

		return nil
	}))
	require.Equal(t, []json.Number{"1", "3"}, sequences)
}

func TestIngestSpoolRejectsFurtherUseWhenAppendRecoveryCannotTruncate(t *testing.T) {
	runDir, err := newIngestRunDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runDir)) })

	spool, err := newIngestSpool(runDir, ingestPhaseNodes, 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })
	require.NoError(t, spool.Append(0, map[string]any{"sequence": 1}))

	writer := spool.writerElements[0].Value.(*ingestSpoolWriter)
	require.NoError(t, writer.file.Close())
	path := spool.pathForBucket(0)
	require.NoError(t, os.Chmod(path, 0o400))
	t.Cleanup(func() { require.NoError(t, os.Chmod(path, 0o600)) })
	require.Error(t, spool.Append(0, map[string]any{"sequence": 2}))
	require.NoError(t, os.Chmod(path, 0o600))

	require.Error(t, spool.Read(0, func([]byte) error { return nil }))
	require.Error(t, spool.Append(0, map[string]any{"sequence": 3}))
}

func TestIngestSpoolCleanupRemovesOnlyOwnedFiles(t *testing.T) {
	parent := t.TempDir()
	runDir, err := newIngestRunDir(parent)
	require.NoError(t, err)

	spool, err := newIngestSpool(runDir, ingestPhaseEdges, 2)
	require.NoError(t, err)
	require.NoError(t, spool.Append(1, map[string]any{"record": "only"}))
	require.NoError(t, spool.RemoveFiles())
	require.NoError(t, spool.Close())

	_, err = os.Stat(parent)
	require.NoError(t, err)
	entries, err := os.ReadDir(runDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func oversizedIngestSpoolFrame() []byte {
	frame := make([]byte, 4)
	binary.BigEndian.PutUint32(frame, 64*1024*1024+1)

	return frame
}

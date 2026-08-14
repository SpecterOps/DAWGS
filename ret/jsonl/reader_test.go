package jsonl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/stretchr/testify/require"
)

func TestReaderPullsBatchesAndVerifiesArtifact(t *testing.T) {
	stored := []byte("{\"source_id\":\"1\",\"kinds\":null,\"properties\":null}\n{\"source_id\":\"2\",\"kinds\":null,\"properties\":null}\n")
	reader, err := NewNodeReader(bytes.NewReader(stored), testArtifact(stored, 2))
	require.NoError(t, err)

	first, err := reader.Pull(1)
	require.NoError(t, err)
	require.Equal(t, []entity.Node{{SourceID: "1"}}, first)
	require.False(t, reader.Done())

	second, err := reader.Pull(1)
	require.NoError(t, err)
	require.Equal(t, []entity.Node{{SourceID: "2"}}, second)

	last, err := reader.Pull(1)
	require.NoError(t, err)
	require.Empty(t, last)
	require.True(t, reader.Done())
	require.NoError(t, reader.Result())
	require.NoError(t, reader.Close())
}

func TestReaderRejectsNonPositiveLimit(t *testing.T) {
	stored := []byte("{\"source_id\":\"1\"}\n")
	reader, err := NewNodeReader(bytes.NewReader(stored), testArtifact(stored, 1))
	require.NoError(t, err)

	_, err = reader.Pull(0)
	require.Error(t, err)
	require.False(t, reader.Done())

	_, err = reader.Pull(-1)
	require.Error(t, err)
	require.False(t, reader.Done())
}

func TestReaderRejectsMultipleJSONValuesAndLatchesFailure(t *testing.T) {
	stored := []byte("{\"source_id\":\"1\"} {}\n")
	reader, err := NewNodeReader(bytes.NewReader(stored), testArtifact(stored, 1))
	require.NoError(t, err)

	_, err = reader.Pull(1)
	require.ErrorContains(t, err, "multiple JSON values")
	require.True(t, reader.Done())
	require.ErrorIs(t, reader.Result(), ErrReaderFailed)

	_, err = reader.Pull(1)
	require.ErrorIs(t, err, ErrReaderFailed)
	require.ErrorIs(t, reader.Close(), ErrReaderFailed)
	require.ErrorIs(t, reader.Close(), ErrReaderFailed)
}

func TestReaderEnforcesOneKnownJSONRecordPerPhysicalLine(t *testing.T) {
	tests := []struct {
		name    string
		stored  []byte
		message string
	}{
		{
			name:    "unknown field",
			stored:  []byte("{\"source_id\":\"1\",\"unexpected\":true}\n"),
			message: "unknown field",
		},
		{
			name:    "blank line",
			stored:  []byte("\n"),
			message: "decode JSONL record 1",
		},
		{
			name:    "oversized line",
			stored:  append(bytes.Repeat([]byte{' '}, maxPhysicalLine+1), '\n'),
			message: "read JSONL record 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, err := NewNodeReader(bytes.NewReader(test.stored), testArtifact(test.stored, 1))
			require.NoError(t, err)

			_, err = reader.Pull(1)
			require.ErrorContains(t, err, test.message)
			require.True(t, reader.Done())
			require.ErrorIs(t, reader.Result(), ErrReaderFailed)
		})
	}
}

func TestReaderRejectsInvalidEntityAndLatchesFailure(t *testing.T) {
	stored := []byte("{\"source_id\":\"\"}\n")
	reader, err := NewNodeReader(bytes.NewReader(stored), testArtifact(stored, 1))
	require.NoError(t, err)

	_, err = reader.Pull(1)
	require.ErrorContains(t, err, "source ID")
	require.True(t, reader.Done())
	require.ErrorIs(t, reader.Result(), ErrReaderFailed)
}

func TestReaderValidatesArtifactHashBeforeReading(t *testing.T) {
	artifact := testArtifact(nil, 0)
	artifact.SHA256 = "not-a-sha256"

	_, err := NewNodeReader(bytes.NewReader(nil), artifact)
	require.ErrorContains(t, err, "SHA-256")
}

func TestReaderResultReportsIntegrityMismatch(t *testing.T) {
	stored := []byte("{\"source_id\":\"1\"}\n")
	tests := []struct {
		name    string
		mutate  func(*Artifact)
		message string
	}{
		{name: "hash", mutate: func(value *Artifact) { value.SHA256 = string(bytes.Repeat([]byte{'0'}, 64)) }, message: "SHA-256 mismatch"},
		{name: "count", mutate: func(value *Artifact) { value.Count++ }, message: "record count mismatch"},
		{name: "uncompressed bytes", mutate: func(value *Artifact) { value.UncompressedBytes++ }, message: "uncompressed size mismatch"},
		{name: "stored bytes", mutate: func(value *Artifact) { value.StoredBytes++ }, message: "stored size mismatch"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := testArtifact(stored, 1)
			test.mutate(&artifact)
			reader, err := NewNodeReader(bytes.NewReader(stored), artifact)
			require.NoError(t, err)

			_, err = reader.Pull(2)
			require.NoError(t, err)
			require.ErrorContains(t, reader.Result(), test.message)
		})
	}
}

func TestReaderCloseIsIdempotent(t *testing.T) {
	stored := []byte("{\"source_id\":\"1\"}\n")
	reader, err := NewNodeReader(bytes.NewReader(stored), testArtifact(stored, 1))
	require.NoError(t, err)

	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	_, err = reader.Pull(1)
	require.True(t, errors.Is(err, ErrReaderNotOpen) || errors.Is(err, ErrReaderFailed))
}

func testArtifact(stored []byte, count int64) Artifact {
	hash := sha256.Sum256(stored)
	return Artifact{
		SchemaVersion:     SchemaVersion,
		Codec:             CodecNone,
		SHA256:            hex.EncodeToString(hash[:]),
		Count:             count,
		UncompressedBytes: int64(len(stored)),
		StoredBytes:       int64(len(stored)),
	}
}

package parquet

import (
	"bytes"
	"errors"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/stretchr/testify/require"
)

func TestStatefulNodeCodecRoundTrip(t *testing.T) {
	var stored bytes.Buffer
	writer, err := NewNodeWriter(&stored, Config{})
	require.NoError(t, err)
	require.NoError(t, writer.Push([]entity.Node{
		{SourceID: "1", Kinds: []string{"User"}, Properties: map[string]any{"enabled": true}},
		{SourceID: "2", Kinds: []string{"Group"}},
	}))
	_, err = writer.Result()
	require.ErrorIs(t, err, ErrWriterNotClosed)
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)
	require.EqualValues(t, 2, artifact.Count)
	require.EqualValues(t, stored.Len(), artifact.StoredBytes)

	reader, err := NewNodeReader(bytes.NewReader(stored.Bytes()), int64(stored.Len()), artifact)
	require.NoError(t, err)
	first, err := reader.Pull(1)
	require.NoError(t, err)
	require.Equal(t, "1", first[0].SourceID)
	require.False(t, reader.Done())
	second, err := reader.Pull(4)
	require.NoError(t, err)
	require.Equal(t, []entity.Node{{SourceID: "2", Kinds: []string{"Group"}}}, second)
	require.True(t, reader.Done())
	require.NoError(t, reader.Result())
	require.NoError(t, reader.Close())
}

func TestStatefulParquetReaderRejectsNonPositiveLimit(t *testing.T) {
	stored, artifact := writeNodeArtifact(t, []entity.Node{{SourceID: "1"}})
	reader, err := NewNodeReader(bytes.NewReader(stored), int64(len(stored)), artifact)
	require.NoError(t, err)

	_, err = reader.Pull(0)
	require.ErrorIs(t, err, ErrInvalidLimit)
	_, err = reader.Pull(-1)
	require.ErrorIs(t, err, ErrInvalidLimit)
}

func TestStatefulParquetReaderVerifiesStoredMetadataBeforeRows(t *testing.T) {
	stored, artifact := writeNodeArtifact(t, []entity.Node{{SourceID: "1"}})

	wrongSize := artifact
	wrongSize.StoredBytes++
	_, err := NewNodeReader(bytes.NewReader(stored), int64(len(stored)), wrongSize)
	require.ErrorContains(t, err, "stored size mismatch")

	wrongHash := artifact
	wrongHash.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = NewNodeReader(bytes.NewReader(stored), int64(len(stored)), wrongHash)
	require.ErrorContains(t, err, "SHA-256 mismatch")
}

func TestStatefulParquetWriterRejectsInvalidEntity(t *testing.T) {
	var stored bytes.Buffer
	writer, err := NewNodeWriter(&stored, Config{})
	require.NoError(t, err)
	require.ErrorContains(t, writer.Push([]entity.Node{{}}), "source ID")
	repeatedCloseErr := writer.Close()
	require.ErrorIs(t, repeatedCloseErr, ErrWriterFailed)
	require.ErrorContains(t, repeatedCloseErr, "source ID")
	require.ErrorIs(t, writer.Close(), ErrWriterFailed)
	_, err = writer.Result()
	require.ErrorIs(t, err, ErrWriterFailed)
}

func TestStatefulRelationshipCodecRoundTrip(t *testing.T) {
	want := entity.Relationship{
		SourceID:   "relationship-1",
		StartID:    "node-1",
		EndID:      "node-2",
		Kind:       "MemberOf",
		Properties: map[string]any{"weight": int64(3)},
	}
	var stored bytes.Buffer
	writer, err := NewRelationshipWriter(&stored, Config{})
	require.NoError(t, err)
	require.NoError(t, writer.Push([]entity.Relationship{want}))
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)

	reader, err := NewRelationshipReader(bytes.NewReader(stored.Bytes()), int64(stored.Len()), artifact)
	require.NoError(t, err)
	values, err := reader.Pull(2)
	require.NoError(t, err)
	require.Equal(t, []entity.Relationship{want}, values)
	require.True(t, reader.Done())
	require.NoError(t, reader.Result())
	require.NoError(t, reader.Close())
}

func TestStatefulParquetReaderCloseIsIdempotent(t *testing.T) {
	stored, artifact := writeNodeArtifact(t, []entity.Node{{SourceID: "1"}})
	reader, err := NewNodeReader(bytes.NewReader(stored), int64(len(stored)), artifact)
	require.NoError(t, err)
	require.ErrorIs(t, reader.Result(), ErrReaderNotDone)

	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	_, err = reader.Pull(1)
	require.ErrorIs(t, err, ErrReaderNotOpen)
	require.ErrorIs(t, reader.Result(), ErrReaderNotDone)
}

func TestStatefulParquetWriterLatchesUnderlyingFailure(t *testing.T) {
	writer, err := NewNodeWriter(parquetFailingWriter{}, Config{})
	require.NoError(t, err)
	pushErr := writer.Push([]entity.Node{{SourceID: "1"}})
	closeErr := writer.Close()
	require.True(t, errors.Is(pushErr, errParquetWrite) || errors.Is(closeErr, errParquetWrite))
	repeatedCloseErr := writer.Close()
	require.ErrorIs(t, repeatedCloseErr, ErrWriterFailed)
	require.ErrorIs(t, repeatedCloseErr, errParquetWrite)
	_, err = writer.Result()
	require.ErrorIs(t, err, ErrWriterFailed)
}

var errParquetWrite = errors.New("injected Parquet write failure")

type parquetFailingWriter struct{}

func (parquetFailingWriter) Write([]byte) (int, error) {
	return 0, errParquetWrite
}

func writeNodeArtifact(t *testing.T, nodes []entity.Node) ([]byte, Artifact) {
	t.Helper()
	var stored bytes.Buffer
	writer, err := NewNodeWriter(&stored, Config{})
	require.NoError(t, err)
	require.NoError(t, writer.Push(nodes))
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)
	return stored.Bytes(), artifact
}

package jsonl

import (
	"bytes"
	"errors"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/stretchr/testify/require"
)

func TestWriterProducesVerifiableArtifactsAcrossCodecs(t *testing.T) {
	tests := []Config{
		{Codec: CodecNone},
		{Codec: CodecGzip},
		{Codec: CodecZstd, Level: 3},
	}
	for _, config := range tests {
		t.Run(string(config.Codec), func(t *testing.T) {
			var stored bytes.Buffer
			writer, err := NewNodeWriter(&stored, config)
			require.NoError(t, err)
			require.NoError(t, writer.Push([]entity.Node{{SourceID: "1"}, {SourceID: "2"}}))
			_, err = writer.Result()
			require.ErrorIs(t, err, ErrWriterNotClosed)
			require.NoError(t, writer.Close())

			artifact, err := writer.Result()
			require.NoError(t, err)
			require.EqualValues(t, 2, artifact.Count)
			require.EqualValues(t, stored.Len(), artifact.StoredBytes)

			reader, err := NewNodeReader(bytes.NewReader(stored.Bytes()), artifact)
			require.NoError(t, err)
			values, err := reader.Pull(3)
			require.NoError(t, err)
			require.Equal(t, []entity.Node{{SourceID: "1"}, {SourceID: "2"}}, values)
			require.NoError(t, reader.Result())
			require.NoError(t, reader.Close())
		})
	}
}

func TestWriterRejectsInvalidEntityAndNeverReturnsArtifact(t *testing.T) {
	var stored bytes.Buffer
	writer, err := NewNodeWriter(&stored, Config{Codec: CodecNone})
	require.NoError(t, err)

	err = writer.Push([]entity.Node{{}})
	require.ErrorContains(t, err, "source ID")
	require.ErrorIs(t, writer.Push([]entity.Node{{SourceID: "1"}}), ErrWriterFailed)
	closeErr := writer.Close()
	require.ErrorIs(t, closeErr, ErrWriterFailed)
	require.ErrorContains(t, closeErr, "source ID")
	closeErr = writer.Close()
	require.ErrorIs(t, closeErr, ErrWriterFailed)
	require.ErrorContains(t, closeErr, "source ID")
	_, err = writer.Result()
	require.ErrorIs(t, err, ErrWriterFailed)
}

func TestRelationshipWriterRoundTripPreservesStoredFields(t *testing.T) {
	want := entity.Relationship{
		SourceID: "relationship-1",
		StartID:  "node-1",
		EndID:    "node-2",
		Kind:     "MemberOf",
		Properties: map[string]any{
			"weight": int64(3),
		},
	}
	var stored bytes.Buffer
	writer, err := NewRelationshipWriter(&stored, Config{Codec: CodecNone})
	require.NoError(t, err)
	require.NoError(t, writer.Push([]entity.Relationship{want}))
	require.NoError(t, writer.Close())
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)

	reader, err := NewRelationshipReader(bytes.NewReader(stored.Bytes()), artifact)
	require.NoError(t, err)
	values, err := reader.Pull(2)
	require.NoError(t, err)
	// Relationship source IDs are pagination metadata and intentionally are not
	// part of the current JSONL record shape.
	want.SourceID = ""
	require.Equal(t, []entity.Relationship{want}, values)
	require.NoError(t, reader.Result())
	require.NoError(t, reader.Close())
}

func TestWriterLatchesUnderlyingWriteFailure(t *testing.T) {
	writer, err := NewNodeWriter(failingWriter{}, Config{Codec: CodecNone})
	require.NoError(t, err)

	require.ErrorIs(t, writer.Push([]entity.Node{{SourceID: "1"}}), errInjectedWrite)
	closeErr := writer.Close()
	require.ErrorIs(t, closeErr, ErrWriterFailed)
	require.ErrorIs(t, closeErr, errInjectedWrite)
	closeErr = writer.Close()
	require.ErrorIs(t, closeErr, ErrWriterFailed)
	require.ErrorIs(t, closeErr, errInjectedWrite)
	_, err = writer.Result()
	require.ErrorIs(t, err, ErrWriterFailed)
}

func TestWriterCloseIsIdempotent(t *testing.T) {
	var stored bytes.Buffer
	writer, err := NewNodeWriter(&stored, Config{Codec: CodecGzip})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, writer.Close())
	_, err = writer.Result()
	require.NoError(t, err)
}

var errInjectedWrite = errors.New("injected write failure")

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errInjectedWrite
}

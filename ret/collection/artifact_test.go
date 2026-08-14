package collection

import (
	"encoding/json"
	"testing"

	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/stretchr/testify/require"
)

func TestCodecArtifactWrappersMarshalFlat(t *testing.T) {
	jsonlValue, err := json.Marshal(JSONLArtifact{
		Path: "nodes.jsonl",
		Artifact: jsonl.Artifact{
			SchemaVersion: jsonl.SchemaVersion,
			Codec:         jsonl.CodecNone,
			SHA256:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Count:         1,
		},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"path":"nodes.jsonl","SchemaVersion":"retriever-jsonl-v1","Codec":"none","SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","Level":0,"Count":1,"UncompressedBytes":0,"StoredBytes":0}`, string(jsonlValue))

	parquetValue, err := json.Marshal(ParquetArtifact{
		Path: "nodes.parquet",
		Artifact: parquet.Artifact{
			SchemaVersion: parquet.SchemaVersion,
			SHA256:        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Count:         1,
		},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"path":"nodes.parquet","SchemaVersion":"ret-parquet-v1","SHA256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","Count":1,"StoredBytes":0}`, string(parquetValue))
}

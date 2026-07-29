package ret

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	retarchive "github.com/specterops/dawgs/ret/archive"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/stretchr/testify/require"
)

func TestArchiveFacadeConfigsValidateEveryRequiredInput(t *testing.T) {
	public, private, err := retarchive.GenerateKeyPair()
	require.NoError(t, err)

	validPack := PackConfig{
		CollectionDirectory: "collection",
		ArchivePath:         "archive.ret",
		Recipient:           public,
	}
	validUnpack := UnpackConfig{
		ArchivePath:     "archive.ret",
		OutputDirectory: "collection",
		Identity:        private,
	}
	validKeygen := KeygenConfig{PrivateKeyPath: "private.json", PublicKeyPath: "public.json"}

	for _, test := range []struct {
		name     string
		validate func() error
	}{
		{name: "pack collection", validate: func() error {
			config := validPack
			config.CollectionDirectory = ""
			return config.Validate()
		}},
		{name: "pack archive", validate: func() error {
			config := validPack
			config.ArchivePath = ""
			return config.Validate()
		}},
		{name: "pack recipient", validate: func() error {
			config := validPack
			config.Recipient = retarchive.PublicKey{}
			return config.Validate()
		}},
		{name: "unpack archive", validate: func() error {
			config := validUnpack
			config.ArchivePath = ""
			return config.Validate()
		}},
		{name: "unpack output", validate: func() error {
			config := validUnpack
			config.OutputDirectory = ""
			return config.Validate()
		}},
		{name: "unpack identity", validate: func() error {
			config := validUnpack
			config.Identity = retarchive.PrivateKey{}
			return config.Validate()
		}},
		{name: "keygen private", validate: func() error {
			config := validKeygen
			config.PrivateKeyPath = ""
			return config.Validate()
		}},
		{name: "keygen public", validate: func() error {
			config := validKeygen
			config.PublicKeyPath = ""
			return config.Validate()
		}},
		{name: "keygen same destination", validate: func() error {
			config := validKeygen
			config.PublicKeyPath = config.PrivateKeyPath
			return config.Validate()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.validate(), ErrInvalidConfig)
		})
	}

	require.NoError(t, validPack.Validate())
	require.NoError(t, validUnpack.Validate())
	require.NoError(t, validKeygen.Validate())
}

func TestPackAndUnpackDelegateAllFormatsAndObserver(t *testing.T) {
	for _, outputs := range []struct {
		name           string
		jsonl, parquet bool
	}{
		{name: "JSONL only", jsonl: true},
		{name: "Parquet only", parquet: true},
		{name: "dual", jsonl: true, parquet: true},
	} {
		t.Run(outputs.name, func(t *testing.T) {
			source := writeRootArchiveTestCollection(t, outputs.jsonl, outputs.parquet)
			public, private, err := retarchive.GenerateKeyPair()
			require.NoError(t, err)
			archivePath := filepath.Join(t.TempDir(), "collection.ret")
			output := filepath.Join(t.TempDir(), "collection")
			var events []observe.Event
			observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
				events = append(events, event)
			})

			require.NoError(t, Pack(context.Background(), PackConfig{
				CollectionDirectory: source,
				ArchivePath:         archivePath,
				Recipient:           public,
				Observer:            observer,
			}))
			require.NoError(t, Unpack(context.Background(), UnpackConfig{
				ArchivePath:     archivePath,
				OutputDirectory: output,
				Identity:        private,
				Observer:        observer,
			}))

			_, err = collection.Verify(context.Background(), output, nil)
			require.NoError(t, err)
			requireRootArchiveTestTreesEqual(t, source, output)
			require.IsType(t, observe.OperationStarted{}, events[0])
			require.Equal(t, "pack", events[0].(observe.OperationStarted).Operation)
			require.IsType(t, observe.OperationCompleted{}, events[len(events)-1])
			require.Equal(t, "unpack", events[len(events)-1].(observe.OperationCompleted).Operation)
			require.NoError(t, events[len(events)-1].(observe.OperationCompleted).Err)
		})
	}
}

func TestPackFullyVerifiesParquetBeforePublishing(t *testing.T) {
	source := writeRootArchiveTestCollection(t, true, true)
	manifest, err := collection.Read(source)
	require.NoError(t, err)
	parquetPath := manifest.Graphs[0].NodeShards[0].Parquet.Path
	require.NoError(t, os.WriteFile(
		filepath.Join(source, filepath.FromSlash(parquetPath)),
		[]byte("corrupt"),
		0o600,
	))
	public, _, err := retarchive.GenerateKeyPair()
	require.NoError(t, err)
	archivePath := filepath.Join(t.TempDir(), "collection.ret")

	err = Pack(context.Background(), PackConfig{
		CollectionDirectory: source,
		ArchivePath:         archivePath,
		Recipient:           public,
	})

	require.Error(t, err)
	require.NoFileExists(t, archivePath)
}

func TestKeygenPublishesPrivateThenPublicExclusively(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		root := t.TempDir()
		privatePath := filepath.Join(root, "private.json")
		publicPath := filepath.Join(root, "public.json")

		require.NoError(t, Keygen(KeygenConfig{
			PrivateKeyPath: privatePath,
			PublicKeyPath:  publicPath,
		}))

		public, err := retarchive.ReadPublicKey(publicPath)
		require.NoError(t, err)
		private, err := retarchive.ReadPrivateKey(privatePath)
		require.NoError(t, err)

		roundTripArchive := filepath.Join(root, "probe.ret")
		collectionRoot := writeRootArchiveTestCollection(t, true, false)
		require.NoError(t, retarchive.Create(context.Background(), retarchive.CreateConfig{
			CollectionDirectory: collectionRoot,
			ArchivePath:         roundTripArchive,
			Recipient:           public,
		}))
		require.NoError(t, retarchive.Extract(context.Background(), retarchive.ExtractConfig{
			ArchivePath:     roundTripArchive,
			OutputDirectory: filepath.Join(root, "round-trip"),
			Identity:        private,
		}))
	})

	t.Run("private exists", func(t *testing.T) {
		root := t.TempDir()
		privatePath := filepath.Join(root, "private.json")
		publicPath := filepath.Join(root, "public.json")
		require.NoError(t, os.WriteFile(privatePath, []byte("preserve-private"), 0o600))

		err := Keygen(KeygenConfig{PrivateKeyPath: privatePath, PublicKeyPath: publicPath})

		require.Error(t, err)
		require.Equal(t, []byte("preserve-private"), mustReadRootArchiveTestFile(t, privatePath))
		require.NoFileExists(t, publicPath)
	})

	t.Run("public exists rolls back private", func(t *testing.T) {
		root := t.TempDir()
		privatePath := filepath.Join(root, "private.json")
		publicPath := filepath.Join(root, "public.json")
		require.NoError(t, os.WriteFile(publicPath, []byte("preserve-public"), 0o600))

		err := Keygen(KeygenConfig{PrivateKeyPath: privatePath, PublicKeyPath: publicPath})

		require.Error(t, err)
		require.NoFileExists(t, privatePath)
		require.Equal(t, []byte("preserve-public"), mustReadRootArchiveTestFile(t, publicPath))
	})
}

func TestKeygenIsIndependentOfArchivePublicationPlatform(t *testing.T) {
	root := t.TempDir()
	privatePath := filepath.Join(root, "private.json")
	publicPath := filepath.Join(root, "public.json")

	err := Keygen(KeygenConfig{
		PrivateKeyPath: privatePath,
		PublicKeyPath:  publicPath,
	})

	require.NoError(t, err)
	_, err = retarchive.ReadPrivateKey(privatePath)
	require.NoError(t, err)
	_, err = retarchive.ReadPublicKey(publicPath)
	require.NoError(t, err)
}

func writeRootArchiveTestCollection(t *testing.T, withJSONL, withParquet bool) string {
	t.Helper()
	root := t.TempDir()
	nodes := []entity.Node{{
		SourceID:   "node-1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "Alice"},
	}}
	builder := metrics.NewBuilder()
	require.NoError(t, builder.ObserveNode(nodes[0]))
	graph := collection.Graph{
		Name:        "example",
		NodeCount:   1,
		KindCatalog: []string{"User"},
		NodeShards:  []collection.NodeShard{{Index: 1, Count: 1, LastSourceID: 1}},
		Metrics:     builder.Finalize(),
	}
	outputs := collection.OutputConfig{}

	if withJSONL {
		config := jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}
		outputs.JSONL = &collection.JSONLOutput{
			SchemaVersion: jsonl.SchemaVersion,
			Codec:         string(config.Codec),
			Level:         config.Level,
		}
		relative := collection.NodeJSONLPath(graph.Name, 1, config.Codec)
		temporary := filepath.Join(root, "nodes.jsonl.tmp")
		artifact, err := jsonl.WriteNodes(temporary, relative, config, nodes)
		require.NoError(t, err)
		installRootArchiveTestArtifact(t, root, temporary, relative)
		graph.NodeShards[0].JSONL = &artifact
	}
	if withParquet {
		config := parquet.Config{Enabled: true}
		outputs.Parquet = &collection.ParquetOutput{SchemaVersion: parquet.SchemaVersion}
		relative := collection.NodeParquetPath(graph.Name, 1)
		temporary := filepath.Join(root, "nodes.parquet.tmp")
		artifact, err := parquet.WriteNodes(temporary, relative, config, nodes)
		require.NoError(t, err)
		installRootArchiveTestArtifact(t, root, temporary, relative)
		graph.NodeShards[0].Parquet = &artifact
	}
	require.NoError(t, collection.Write(root, collection.Manifest{
		Format:    collection.Format,
		CreatedAt: time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC),
		Outputs:   outputs,
		Graphs:    []collection.Graph{graph},
	}))
	return root
}

func installRootArchiveTestArtifact(t *testing.T, root, temporary, relative string) {
	t.Helper()
	final := filepath.Join(root, filepath.FromSlash(relative))
	require.NoError(t, os.MkdirAll(filepath.Dir(final), 0o700))
	require.NoError(t, os.Rename(temporary, final))
}

func requireRootArchiveTestTreesEqual(t *testing.T, expected, actual string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(expected, func(candidate string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(expected, candidate)
		require.NoError(t, err)
		require.Equal(
			t,
			mustReadRootArchiveTestFile(t, candidate),
			mustReadRootArchiveTestFile(t, filepath.Join(actual, relative)),
		)
		return nil
	}))
}

func mustReadRootArchiveTestFile(t *testing.T, path string) []byte {
	t.Helper()
	value, err := os.ReadFile(path)
	require.NoError(t, err)
	return value
}

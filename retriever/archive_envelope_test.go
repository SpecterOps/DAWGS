package retriever

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
)

func TestEncryptedArchiveRoundTrip(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	payload := []byte("plain tar bytes")

	ciphertext := encryptArchiveBytes(t, payload, publicKey)
	plaintext, err := decryptArchiveBytes(ciphertext, privateKey)
	if err != nil {
		t.Fatalf("decrypt archive: %v", err)
	}

	if !bytes.Equal(plaintext, payload) {
		t.Fatalf("plaintext = %q, want %q", plaintext, payload)
	}
}

func TestEncryptedArchiveHeaderDoesNotContainDumpContents(t *testing.T) {
	_, publicKey := generateTestArchiveKeyPair(t)
	dumpDir := writeArchiveFixture(t)

	var tarPayload bytes.Buffer
	if err := writeCollectionTar(&tarPayload, dumpDir); err != nil {
		t.Fatalf("write tar: %v", err)
	}

	ciphertext := encryptArchiveBytes(t, tarPayload.Bytes(), publicKey)
	headerBytes := encryptedArchiveHeaderBytes(t, ciphertext)

	for _, secret := range []string{"secret-graph", "alice", "MemberOf"} {
		if bytes.Contains(headerBytes, []byte(secret)) {
			t.Fatalf("archive header contains dump content %q: %s", secret, string(headerBytes))
		}
	}
}

func TestEncryptedArchiveWrongKeyAndTamperingFailures(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	wrongPrivateKey, _ := generateTestArchiveKeyPair(t)
	ciphertext := encryptArchiveBytes(t, []byte("payload"), publicKey)

	if _, err := decryptArchiveBytes(ciphertext, wrongPrivateKey); err == nil {
		t.Fatalf("expected wrong private key to fail")
	}

	tamperedHeader := append([]byte(nil), ciphertext...)
	oldChunk := []byte(`"chunk_size":1048576`)
	newChunk := []byte(`"chunk_size":1048577`)
	headerStart := len(encryptedArchiveMagic) + 4
	headerEnd := encryptedArchiveFrameStart(t, ciphertext)
	replaced := bytes.Replace(tamperedHeader[headerStart:headerEnd], oldChunk, newChunk, 1)
	if bytes.Equal(replaced, tamperedHeader[headerStart:headerEnd]) {
		t.Fatalf("test did not tamper header chunk size")
	}

	copy(tamperedHeader[headerStart:headerEnd], replaced)

	if _, err := decryptArchiveBytes(tamperedHeader, privateKey); err == nil {
		t.Fatalf("expected tampered header to fail")
	}

	tamperedFrame := append([]byte(nil), ciphertext...)
	frameStart := encryptedArchiveFrameStart(t, tamperedFrame)
	if len(tamperedFrame) <= frameStart+5 {
		t.Fatalf("archive frame is too short")
	}

	tamperedFrame[frameStart+5] ^= 0x01

	if _, err := decryptArchiveBytes(tamperedFrame, privateKey); err == nil {
		t.Fatalf("expected tampered frame to fail")
	}

	oversizedFrame := append([]byte(nil), ciphertext...)
	binary.BigEndian.PutUint32(oversizedFrame[frameStart+1:], maxEncryptedArchiveFrameSize+1)
	if _, err := decryptArchiveBytes(oversizedFrame, privateKey); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected oversized frame to fail before allocation, got %v", err)
	}

	if _, err := decryptArchiveBytes(ciphertext[:len(ciphertext)-1], privateKey); err == nil {
		t.Fatalf("expected truncated archive to fail")
	}

	withoutFinal := stripFinalArchiveFrame(t, ciphertext)
	if _, err := decryptArchiveBytes(withoutFinal, privateKey); err == nil || !strings.Contains(err.Error(), "final frame") {
		t.Fatalf("expected missing final frame error, got %v", err)
	}
}

func TestEncryptedCollectionArchiveSyntheticRoundTrip(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.key")
	publicPath := filepath.Join(dir, "public.key")
	if err := generateArchiveKeyFiles(privatePath, publicPath); err != nil {
		t.Fatalf("generate keys: %v", err)
	}

	publicKey, err := loadArchivePublicKey(publicPath)
	if err != nil {
		t.Fatalf("load public key: %v", err)
	}

	privateKey, err := loadArchivePrivateKey(privatePath)
	if err != nil {
		t.Fatalf("load private key: %v", err)
	}

	dumpDir := writeArchiveFixture(t)
	archivePath := filepath.Join(dir, "dump.tar.pq")
	if err := writeEncryptedCollectionArchive(dumpDir, archivePath, publicKey); err != nil {
		t.Fatalf("write encrypted collection archive: %v", err)
	}

	outputDir := filepath.Join(dir, "unpacked")
	if err := unpackEncryptedCollectionArchive(archivePath, outputDir, false, privateKey); err != nil {
		t.Fatalf("unpack encrypted collection archive: %v", err)
	}

	originalTree := readFileTree(t, dumpDir)
	unpackedTree := readFileTree(t, outputDir)
	if !equalFileTrees(originalTree, unpackedTree) {
		t.Fatalf("unpacked file tree does not match original")
	}

	if _, err := readManifest(outputDir); err != nil {
		t.Fatalf("read unpacked Manifest: %v", err)
	}

	if _, err := os.Stat(archivePath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected archive temp path cleanup, got %v", err)
	}
}

func TestEncryptedCollectionArchiveProgressCallbacks(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	dumpDir := writeArchiveFixture(t)
	var archive bytes.Buffer
	var events []ProgressEvent
	options := ArchiveOptions{
		Progress: func(event ProgressEvent) {
			events = append(events, event)
		},
	}

	if err := WriteEncryptedCollectionArchiveWithOptions(&archive, dumpDir, publicKey, options); err != nil {
		t.Fatalf("write encrypted collection archive: %v", err)
	}

	outputDir := filepath.Join(t.TempDir(), "out")
	if err := UnpackEncryptedCollectionArchiveWithOptions(bytes.NewReader(archive.Bytes()), outputDir, privateKey, options); err != nil {
		t.Fatalf("unpack encrypted collection archive: %v", err)
	}

	if !progressEventsContain(events, OperationArchive, "retriever archive tar streaming completed") {
		t.Fatalf("missing archive completion progress event: %+v", events)
	}

	if !progressEventsContain(events, OperationUnpack, "retriever archive unpacking completed") {
		t.Fatalf("missing unpack completion progress event: %+v", events)
	}
}

func TestUnpackOptionsStreamRoundTripAndForceSafety(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	dumpDir := writeArchiveFixture(t)
	var archive bytes.Buffer
	if err := WriteEncryptedCollectionArchive(&archive, dumpDir, publicKey); err != nil {
		t.Fatalf("write encrypted collection archive: %v", err)
	}

	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")

	if err := Unpack(UnpackOptions{
		ArchiveReader:   bytes.NewReader(archive.Bytes()),
		ArchiveIdentity: privateKey,
		OutputDir:       outputDir,
	}); err != nil {
		t.Fatalf("unpack with options: %v", err)
	}

	if originalTree, unpackedTree := readFileTree(t, dumpDir), readFileTree(t, outputDir); !equalFileTrees(originalTree, unpackedTree) {
		t.Fatalf("unpacked file tree does not match original")
	}

	oldPath := filepath.Join(outputDir, "old")
	if err := os.WriteFile(oldPath, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("write old output: %v", err)
	}

	badArchive := stripFinalArchiveFrame(t, archive.Bytes())
	err := Unpack(UnpackOptions{
		ArchiveReader:   bytes.NewReader(badArchive),
		ArchiveIdentity: privateKey,
		OutputDir:       outputDir,
		Force:           true,
	})
	if err == nil || !strings.Contains(err.Error(), "final frame") {
		t.Fatalf("expected missing final frame error, got %v", err)
	}

	if contents, err := os.ReadFile(oldPath); err != nil {
		t.Fatalf("read old output after failed option unpack: %v", err)
	} else if string(contents) != "keep me" {
		t.Fatalf("old output was replaced after failed option unpack: %q", contents)
	}
}

func TestEncryptedCollectionArchiveFailureDoesNotReplaceOutput(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	dir := t.TempDir()
	dumpDir := writeArchiveFixture(t)
	goodArchivePath := filepath.Join(dir, "good.tar.pq")
	if err := writeEncryptedCollectionArchive(dumpDir, goodArchivePath, publicKey); err != nil {
		t.Fatalf("write encrypted collection archive: %v", err)
	}

	goodPayload, err := os.ReadFile(goodArchivePath)
	if err != nil {
		t.Fatalf("read good archive: %v", err)
	}

	badArchivePath := filepath.Join(dir, "bad.tar.pq")
	if err := os.WriteFile(badArchivePath, stripFinalArchiveFrame(t, goodPayload), 0o600); err != nil {
		t.Fatalf("write bad archive: %v", err)
	}

	outputDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	oldPath := filepath.Join(outputDir, "old")
	if err := os.WriteFile(oldPath, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("write old output: %v", err)
	}

	err = unpackEncryptedCollectionArchive(badArchivePath, outputDir, true, privateKey)
	if err == nil || !strings.Contains(err.Error(), "final frame") {
		t.Fatalf("expected missing final frame error, got %v", err)
	}

	contents, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatalf("read old output after failed unpack: %v", err)
	}

	if string(contents) != "keep me" {
		t.Fatalf("old output was replaced after failed unpack: %q", contents)
	}
}

func progressEventsContain(events []ProgressEvent, operation string, message string) bool {
	for _, event := range events {
		if event.Operation == operation && event.Message == message {
			return true
		}
	}

	return false
}

func TestPromoteUnpackStagingDirectoryForceReplacesOutput(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	stagingDir := filepath.Join(dir, "staging")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(outputDir, "old.txt"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write old output: %v", err)
	}

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("create staging dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(stagingDir, "new.txt"), []byte("new"), 0o600); err != nil {
		t.Fatalf("write staging output: %v", err)
	}

	if err := promoteUnpackStagingDirectory(stagingDir, outputDir, true); err != nil {
		t.Fatalf("promote staging dir: %v", err)
	}

	if contents, err := os.ReadFile(filepath.Join(outputDir, "new.txt")); err != nil {
		t.Fatalf("read promoted output: %v", err)
	} else if string(contents) != "new" {
		t.Fatalf("promoted output = %q", contents)
	}

	if _, err := os.Stat(filepath.Join(outputDir, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected old output to be replaced, got %v", err)
	}

	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Fatalf("expected staging dir to be moved, got %v", err)
	}
}

func TestPromoteUnpackStagingDirectoryIgnoresBackupCleanupFailure(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	stagingDir := filepath.Join(dir, "staging")

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("create staging dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(stagingDir, "new.txt"), []byte("new"), 0o600); err != nil {
		t.Fatalf("write staging output: %v", err)
	}

	cleanupErr := errors.New("cleanup failed")
	originalRemoveUnpackBackupDirectory := removeUnpackBackupDirectory
	removeUnpackBackupDirectory = func(string) error {
		return cleanupErr
	}
	t.Cleanup(func() {
		removeUnpackBackupDirectory = originalRemoveUnpackBackupDirectory
	})

	if err := promoteUnpackStagingDirectory(stagingDir, outputDir, true); err != nil {
		t.Fatalf("promote staging dir: %v", err)
	}

	if contents, err := os.ReadFile(filepath.Join(outputDir, "new.txt")); err != nil {
		t.Fatalf("read promoted output: %v", err)
	} else if string(contents) != "new" {
		t.Fatalf("promoted output = %q", contents)
	}
}

func TestPromoteUnpackStagingDirectoryRestoresOutputOnPromotionFailure(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "out")
	missingStagingDir := filepath.Join(dir, "missing-staging")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	oldPath := filepath.Join(outputDir, "old.txt")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("write old output: %v", err)
	}

	err := promoteUnpackStagingDirectory(missingStagingDir, outputDir, true)
	if err == nil || !strings.Contains(err.Error(), "promote unpacked archive") {
		t.Fatalf("expected promotion failure, got %v", err)
	}

	if contents, err := os.ReadFile(oldPath); err != nil {
		t.Fatalf("read restored output: %v", err)
	} else if string(contents) != "old" {
		t.Fatalf("restored output = %q", contents)
	}
}

func TestEncryptedCollectionArchiveRejectsInvalidCollection(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	dir := t.TempDir()
	archivePayload := encryptArchiveBytes(t, buildTarPayload(t, &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "extra.txt",
		Mode:     0o600,
		Size:     int64(len("not a collection")),
	}, []byte("not a collection")), publicKey)
	archivePath := filepath.Join(dir, "invalid.tar.pq")
	if err := os.WriteFile(archivePath, archivePayload, 0o600); err != nil {
		t.Fatalf("write invalid archive: %v", err)
	}

	outputDir := filepath.Join(dir, "out")
	err := unpackEncryptedCollectionArchive(archivePath, outputDir, false, privateKey)
	if err == nil || !strings.Contains(err.Error(), "read manifest") {
		t.Fatalf("expected manifest validation error, got %v", err)
	}

	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		t.Fatalf("invalid collection was promoted to output dir: %v", err)
	}
}

func TestEncryptedCollectionArchiveAllowsSelfConsistentMalformedJSONLines(t *testing.T) {
	privateKey, publicKey := generateTestArchiveKeyPair(t)
	dumpDir := writeArchiveFixture(t)
	value, err := readManifest(dumpDir)
	if err != nil {
		t.Fatalf("read fixture manifest: %v", err)
	}

	nodeEntry := &value.Graphs[0].Files[0]
	payload := "{\"id\":\"1\",\"kinds\":[\"User\"],\"unexpected\":true}\n"
	fragmentPath := filepath.Join(dumpDir, filepath.FromSlash(nodeEntry.Path))
	writeCompressedPayload(t, fragmentPath, value.Compression, payload)

	contents, err := os.ReadFile(fragmentPath)
	if err != nil {
		t.Fatalf("read malformed fragment: %v", err)
	}
	checksum := sha256.Sum256(contents)
	nodeEntry.CompressedBytes = int64(len(contents))
	nodeEntry.UncompressedBytes = int64(len(payload))
	nodeEntry.SHA256 = hex.EncodeToString(checksum[:])
	if err := writeManifest(dumpDir, value); err != nil {
		t.Fatalf("write self-consistent malformed collection manifest: %v", err)
	}

	var archive bytes.Buffer
	if err := WriteEncryptedCollectionArchive(&archive, dumpDir, publicKey); err != nil {
		t.Fatalf("write malformed collection archive: %v", err)
	}

	outputDir := filepath.Join(t.TempDir(), "out")
	err = Unpack(UnpackOptions{
		ArchiveReader:   bytes.NewReader(archive.Bytes()),
		ArchiveIdentity: privateKey,
		OutputDir:       outputDir,
	})
	if err != nil {
		t.Fatalf("unpack integrity-valid malformed JSONL collection: %v", err)
	}

	if originalTree, unpackedTree := readFileTree(t, dumpDir), readFileTree(t, outputDir); !equalFileTrees(originalTree, unpackedTree) {
		t.Fatalf("unpacked malformed collection does not match original")
	}

	loadOptions := DefaultLoadOptions("")
	loadOptions.ArchiveReader = bytes.NewReader(archive.Bytes())
	loadOptions.ArchiveIdentity = privateKey
	if _, err := Load(context.Background(), nil, pg.DriverName, loadOptions); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected direct archive load semantic preflight error, got %v", err)
	}
}

func TestEncryptedCollectionArchiveOutputCollisionFails(t *testing.T) {
	_, publicKey := generateTestArchiveKeyPair(t)
	dumpDir := writeArchiveFixture(t)

	if err := writeEncryptedCollectionArchive(dumpDir, filepath.Join(dumpDir, manifestFileName), publicKey); err == nil {
		t.Fatalf("expected archive path collision to fail")
	}
}

func generateTestArchiveKeyPair(t *testing.T) (hpke.PrivateKey, hpke.PublicKey) {
	t.Helper()
	privateKey, err := defaultArchiveKEM().GenerateKey()
	if err != nil {
		t.Fatalf("generate archive key: %v", err)
	}

	return privateKey, privateKey.PublicKey()
}

func encryptArchiveBytes(t *testing.T, payload []byte, publicKey hpke.PublicKey) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer, err := newEncryptedArchiveWriter(&buffer, publicKey)
	if err != nil {
		t.Fatalf("create encrypted archive writer: %v", err)
	}

	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("write encrypted archive payload: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close encrypted archive writer: %v", err)
	}

	return buffer.Bytes()
}

func decryptArchiveBytes(payload []byte, privateKey hpke.PrivateKey) ([]byte, error) {
	reader, err := newEncryptedArchiveReader(bytes.NewReader(payload), privateKey)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(reader)
}

func encryptedArchiveHeaderBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	frameStart := encryptedArchiveFrameStart(t, payload)
	headerStart := len(encryptedArchiveMagic) + 4
	return append([]byte(nil), payload[headerStart:frameStart]...)
}

func encryptedArchiveFrameStart(t *testing.T, payload []byte) int {
	t.Helper()
	if len(payload) < len(encryptedArchiveMagic)+4 {
		t.Fatalf("archive too short")
	}

	if string(payload[:len(encryptedArchiveMagic)]) != encryptedArchiveMagic {
		t.Fatalf("bad magic")
	}

	headerLen := binary.BigEndian.Uint32(payload[len(encryptedArchiveMagic):])
	frameStart := len(encryptedArchiveMagic) + 4 + int(headerLen)

	if frameStart > len(payload) {
		t.Fatalf("header length exceeds archive size")
	}

	return frameStart
}

func stripFinalArchiveFrame(t *testing.T, payload []byte) []byte {
	t.Helper()
	offset := encryptedArchiveFrameStart(t, payload)
	for offset < len(payload) {
		frameStart := offset
		if offset+5 > len(payload) {
			t.Fatalf("truncated frame header")
		}

		frameType := payload[offset]
		frameLen := int(binary.BigEndian.Uint32(payload[offset+1:]))
		offset += 5

		if offset+frameLen > len(payload) {
			t.Fatalf("truncated frame payload")
		}

		offset += frameLen

		if frameType == encryptedArchiveFrameFinal {
			return append([]byte(nil), payload[:frameStart]...)
		}
	}

	t.Fatalf("missing final frame in test archive")

	return nil
}

func readFileTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	tree := map[string][]byte{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		tree[filepath.ToSlash(relativePath)] = contents

		return nil
	}); err != nil {
		t.Fatalf("walk file tree: %v", err)
	}

	return tree
}

func equalFileTrees(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for path, leftContents := range left {
		rightContents, ok := right[path]
		if !ok || !bytes.Equal(leftContents, rightContents) {
			return false
		}
	}

	return true
}

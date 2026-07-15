package retriever

import (
	"bytes"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var removeUnpackBackupDirectory = os.RemoveAll

const (
	encryptedArchiveMagic        = "RTRV-PQ-ARCHIVE-v1"
	encryptedArchiveFormat       = "retriever-encrypted-tar-v1"
	encryptedArchiveHPKEInfo     = "retriever/archive/hpke/v1"
	encryptedArchiveChunkSize    = 1024 * 1024
	maxEncryptedArchiveFrameSize = encryptedArchiveChunkSize + 4096
	maxEncryptedArchiveHeaderLen = 64 * 1024

	encryptedArchiveFrameData  byte = 0
	encryptedArchiveFrameFinal byte = 1
)

type encryptedArchiveHeader struct {
	Format    string                       `json:"format"`
	Archive   encryptedArchiveFormatHeader `json:"archive"`
	Crypto    encryptedArchiveCryptoHeader `json:"crypto"`
	ChunkSize int                          `json:"chunk_size"`
}

type encryptedArchiveFormatHeader struct {
	Format      string `json:"format"`
	Compression string `json:"compression"`
}

type encryptedArchiveCryptoHeader struct {
	Scheme          string `json:"scheme"`
	KEM             string `json:"kem"`
	KDF             string `json:"kdf"`
	AEAD            string `json:"aead"`
	EncapsulatedKey string `json:"encapsulated_key"`
}

type encryptedArchiveWriter struct {
	writer     io.Writer
	sender     *hpke.Sender
	headerHash [sha256.Size]byte
	frameIndex uint64
	closed     bool
}

type encryptedArchiveReader struct {
	reader     io.Reader
	recipient  *hpke.Recipient
	headerHash [sha256.Size]byte
	frameIndex uint64
	plaintext  []byte
	final      bool
}

func WriteEncryptedCollectionArchive(writer io.Writer, dumpDir string, recipient hpke.PublicKey, excludePaths ...string) error {
	return WriteEncryptedCollectionArchiveWithOptions(writer, dumpDir, recipient, ArchiveOptions{}, excludePaths...)
}

func WriteEncryptedCollectionArchiveWithOptions(writer io.Writer, dumpDir string, recipient hpke.PublicKey, options ArchiveOptions, excludePaths ...string) error {
	startedAt := time.Now()
	options.Progress.emit(ProgressEvent{
		Operation: OperationArchive,
		Message:   "retriever encrypted archive streaming started",
		InputDir:  dumpDir,
	})

	archiveWriter, err := newEncryptedArchiveWriter(writer, recipient)
	if err != nil {
		return err
	}

	if err := WriteCollectionTarWithOptions(archiveWriter, dumpDir, options, excludePaths...); err != nil {
		_ = archiveWriter.Close()
		return err
	}

	if err := archiveWriter.Close(); err != nil {
		return err
	}

	options.Progress.emit(ProgressEvent{
		Operation: OperationArchive,
		Message:   "retriever encrypted archive streaming completed",
		InputDir:  dumpDir,
		Elapsed:   time.Since(startedAt),
	})
	return nil
}

func WriteEncryptedCollectionArchiveFile(dumpDir, archivePath string, recipient hpke.PublicKey) error {
	return WriteEncryptedCollectionArchiveFileWithOptions(dumpDir, archivePath, recipient, ArchiveOptions{})
}

func writeEncryptedCollectionArchive(dumpDir, archivePath string, recipient hpke.PublicKey) error {
	return WriteEncryptedCollectionArchiveFile(dumpDir, archivePath, recipient)
}

func WriteEncryptedCollectionArchiveFileWithOptions(dumpDir, archivePath string, recipient hpke.PublicKey, options ArchiveOptions) error {
	archivePath = strings.TrimSpace(archivePath)
	if err := preflightArchiveOutputPath(archivePath); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(archivePath), filepath.Base(archivePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create archive temp file: %w", err)
	}
	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()

	slog.Info("retriever archive encryption started",
		slog.String("input_dir", dumpDir),
		slog.String("archive", archivePath),
	)
	options.Progress.emit(ProgressEvent{
		Operation:   OperationArchive,
		Message:     "retriever archive encryption started",
		InputDir:    dumpDir,
		ArchivePath: archivePath,
	})

	if err := WriteEncryptedCollectionArchiveWithOptions(tempFile, dumpDir, recipient, options, archivePath, tempPath); err != nil {
		_ = tempFile.Close()
		return err
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close archive temp file: %w", err)
	}

	if err := requirePathDoesNotExist(archivePath); err != nil {
		return err
	}

	if err := os.Rename(tempPath, archivePath); err != nil {
		return fmt.Errorf("rename archive: %w", err)
	}

	cleanupTemp = false

	slog.Info("retriever archive encryption completed",
		slog.String("archive", archivePath),
	)
	options.Progress.emit(ProgressEvent{
		Operation:   OperationArchive,
		Message:     "retriever archive encryption completed",
		InputDir:    dumpDir,
		ArchivePath: archivePath,
	})
	return nil
}

func preflightArchiveOutputPath(archivePath string) error {
	archivePath = strings.TrimSpace(archivePath)
	if archivePath == "" {
		return fmt.Errorf("archive output path is required")
	}

	if err := requirePathDoesNotExist(archivePath); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(archivePath), 0o755); err != nil {
		return fmt.Errorf("create archive parent directory: %w", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(archivePath), filepath.Base(archivePath)+".preflight-*.tmp")
	if err != nil {
		return fmt.Errorf("create archive preflight temp file: %w", err)
	}
	tempPath := tempFile.Name()
	closeErr := tempFile.Close()
	removeErr := os.Remove(tempPath)

	if closeErr != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close archive preflight temp file: %w", closeErr)
	}

	if removeErr != nil {
		return fmt.Errorf("remove archive preflight temp file: %w", removeErr)
	}

	return requirePathDoesNotExist(archivePath)
}

func PreflightArchiveOutputPath(archivePath string) error {
	return preflightArchiveOutputPath(archivePath)
}

func UnpackEncryptedCollectionArchiveFile(archivePath, outputDir string, force bool, identity hpke.PrivateKey) error {
	return UnpackEncryptedCollectionArchiveFileWithOptions(archivePath, outputDir, force, identity, ArchiveOptions{})
}

func unpackEncryptedCollectionArchive(archivePath, outputDir string, force bool, identity hpke.PrivateKey) error {
	return UnpackEncryptedCollectionArchiveFile(archivePath, outputDir, force, identity)
}

func UnpackEncryptedCollectionArchiveFileWithOptions(archivePath, outputDir string, force bool, identity hpke.PrivateKey, options ArchiveOptions) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer file.Close()

	slog.Info("retriever archive decryption started",
		slog.String("archive", archivePath),
		slog.String("output_dir", outputDir),
	)
	options.Progress.emit(ProgressEvent{
		Operation:   OperationUnpack,
		Message:     "retriever archive decryption started",
		ArchivePath: archivePath,
		OutputDir:   outputDir,
	})

	if err := Unpack(UnpackOptions{
		ArchiveReader:   file,
		ArchiveIdentity: identity,
		OutputDir:       outputDir,
		Force:           force,
		Progress:        options.Progress,
	}); err != nil {
		return err
	}

	slog.Info("retriever archive decryption completed",
		slog.String("archive", archivePath),
		slog.String("output_dir", outputDir),
	)
	options.Progress.emit(ProgressEvent{
		Operation:   OperationUnpack,
		Message:     "retriever archive decryption completed",
		ArchivePath: archivePath,
		OutputDir:   outputDir,
	})
	return nil
}

func Unpack(options UnpackOptions) error {
	if err := options.validate(); err != nil {
		return err
	}

	outputDir := strings.TrimSpace(options.OutputDir)
	stagingDir, err := createUnpackStagingDirectory(outputDir, options.Force)
	if err != nil {
		return err
	}
	cleanupStaging := true
	defer func() {
		if cleanupStaging {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	slog.Info("retriever archive stream unpack started",
		slog.String("output_dir", outputDir),
		slog.Bool("force", options.Force),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationUnpack,
		Message:   "retriever archive stream unpack started",
		OutputDir: outputDir,
	})

	if err := UnpackEncryptedCollectionArchiveWithOptions(options.ArchiveReader, stagingDir, options.ArchiveIdentity, ArchiveOptions{Progress: options.Progress}); err != nil {
		return err
	}

	if err := promoteUnpackStagingDirectory(stagingDir, outputDir, options.Force); err != nil {
		return err
	}

	cleanupStaging = false

	slog.Info("retriever archive stream unpack completed",
		slog.String("output_dir", outputDir),
	)
	options.Progress.emit(ProgressEvent{
		Operation: OperationUnpack,
		Message:   "retriever archive stream unpack completed",
		OutputDir: outputDir,
	})
	return nil
}

func UnpackEncryptedCollectionArchive(reader io.Reader, outputDir string, identity hpke.PrivateKey) error {
	return UnpackEncryptedCollectionArchiveWithOptions(reader, outputDir, identity, ArchiveOptions{})
}

func UnpackEncryptedCollectionArchiveWithOptions(reader io.Reader, outputDir string, identity hpke.PrivateKey, options ArchiveOptions) error {
	startedAt := time.Now()
	options.Progress.emit(ProgressEvent{
		Operation: OperationUnpack,
		Message:   "retriever encrypted archive unpack started",
		OutputDir: outputDir,
	})

	archiveReader, err := newEncryptedArchiveReader(reader, identity)
	if err != nil {
		return err
	}

	if err := UnpackTarWithOptions(archiveReader, outputDir, false, options); err != nil {
		return err
	}

	if _, err := io.Copy(io.Discard, archiveReader); err != nil {
		return fmt.Errorf("finish encrypted archive stream: %w", err)
	}

	if err := validateUnpackedCollection(outputDir); err != nil {
		return err
	}

	options.Progress.emit(ProgressEvent{
		Operation: OperationUnpack,
		Message:   "retriever encrypted archive unpack completed",
		OutputDir: outputDir,
		Elapsed:   time.Since(startedAt),
	})
	return nil
}

func unpackEncryptedCollectionArchiveToDirectory(reader io.Reader, outputDir string, identity hpke.PrivateKey) error {
	return UnpackEncryptedCollectionArchive(reader, outputDir, identity)
}

func createUnpackStagingDirectory(outputDir string, force bool) (string, error) {
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" {
		return "", fmt.Errorf("output directory is required; pass -out")
	}

	if err := preflightUnpackOutputDirectory(outputDir, force); err != nil {
		return "", err
	}

	parentDir := filepath.Dir(outputDir)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return "", fmt.Errorf("create unpack parent directory: %w", err)
	}

	stagingDir, err := os.MkdirTemp(parentDir, "."+filepath.Base(outputDir)+".unpack-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create unpack staging directory: %w", err)
	}

	return stagingDir, nil
}

func preflightUnpackOutputDirectory(outputDir string, force bool) error {
	info, err := os.Stat(outputDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("output path %q exists and is not a directory", outputDir)
		}

		entries, err := os.ReadDir(outputDir)
		if err != nil {
			return fmt.Errorf("read output directory: %w", err)
		}

		if len(entries) > 0 && !force {
			return fmt.Errorf("output directory %q is not empty; pass -force to replace it", outputDir)
		}

		return nil
	}

	if !os.IsNotExist(err) {
		return fmt.Errorf("inspect output directory: %w", err)
	}

	return nil
}

func validateUnpackedCollection(outputDir string) error {
	nextManifest, err := readManifest(outputDir)
	if err != nil {
		return err
	}
	if err := verifyCollectionFragments(outputDir, nextManifest); err != nil {
		return err
	}

	expectedPaths, err := archivePathsFromManifest(nextManifest)
	if err != nil {
		return err
	}
	expected := make(map[string]struct{}, len(expectedPaths))
	for _, relativePath := range expectedPaths {
		expected[relativePath] = struct{}{}
	}

	if err := filepath.WalkDir(outputDir, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		relativePath, err := filepath.Rel(outputDir, filePath)
		if err != nil {
			return err
		}

		archivePath := filepath.ToSlash(relativePath)
		if _, ok := expected[archivePath]; !ok {
			return fmt.Errorf("unpacked archive contains unexpected file %q", archivePath)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func promoteUnpackStagingDirectory(stagingDir, outputDir string, force bool) error {
	if err := preflightUnpackOutputDirectory(outputDir, force); err != nil {
		return err
	}

	backupDir := ""
	if _, err := os.Stat(outputDir); err == nil {
		parentDir := filepath.Dir(outputDir)
		reservedBackupDir, err := os.MkdirTemp(parentDir, "."+filepath.Base(outputDir)+".backup-*.tmp")
		if err != nil {
			return fmt.Errorf("create output directory backup path: %w", err)
		}

		if err := os.Remove(reservedBackupDir); err != nil {
			return fmt.Errorf("reserve output directory backup path: %w", err)
		}

		if err := os.Rename(outputDir, reservedBackupDir); err != nil {
			return fmt.Errorf("backup output directory: %w", err)
		}

		backupDir = reservedBackupDir
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect output directory: %w", err)
	}

	if err := os.Rename(stagingDir, outputDir); err != nil {
		if backupDir != "" {
			if restoreErr := os.Rename(backupDir, outputDir); restoreErr != nil {
				return fmt.Errorf("promote unpacked archive: %w; restore output directory: %v", err, restoreErr)
			}
		}

		return fmt.Errorf("promote unpacked archive: %w", err)
	}

	if backupDir != "" {
		if err := removeUnpackBackupDirectory(backupDir); err != nil {
			slog.Warn("retriever unpack output directory backup cleanup failed",
				slog.String("backup_dir", backupDir),
				slog.Any("error", err),
			)
		}
	}

	return nil
}

func NewEncryptedArchiveWriter(writer io.Writer, recipient hpke.PublicKey) (io.WriteCloser, error) {
	return newEncryptedArchiveWriter(writer, recipient)
}

func newEncryptedArchiveWriter(writer io.Writer, recipient hpke.PublicKey) (*encryptedArchiveWriter, error) {
	if recipient == nil {
		return nil, fmt.Errorf("recipient public key is required")
	}

	if recipient.KEM().ID() != defaultArchiveKEM().ID() {
		return nil, fmt.Errorf("recipient public key uses unsupported KEM")
	}

	encapsulatedKey, sender, err := hpke.NewSender(recipient, defaultArchiveKDF(), defaultArchiveAEAD(), []byte(encryptedArchiveHPKEInfo))
	if err != nil {
		return nil, fmt.Errorf("create archive sender: %w", err)
	}

	header := encryptedArchiveHeader{
		Format: encryptedArchiveFormat,
		Archive: encryptedArchiveFormatHeader{
			Format:      "tar",
			Compression: "none",
		},
		Crypto: encryptedArchiveCryptoHeader{
			Scheme:          archiveCryptoScheme,
			KEM:             archiveKEMName,
			KDF:             archiveKDFName,
			AEAD:            archiveAEADName,
			EncapsulatedKey: base64.StdEncoding.EncodeToString(encapsulatedKey),
		},
		ChunkSize: encryptedArchiveChunkSize,
	}

	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode archive header: %w", err)
	}

	if len(headerBytes) > maxEncryptedArchiveHeaderLen {
		return nil, fmt.Errorf("archive header is too large")
	}

	if _, err := writer.Write([]byte(encryptedArchiveMagic)); err != nil {
		return nil, fmt.Errorf("write archive magic: %w", err)
	}

	var headerLen [4]byte
	binary.BigEndian.PutUint32(headerLen[:], uint32(len(headerBytes)))

	if _, err := writer.Write(headerLen[:]); err != nil {
		return nil, fmt.Errorf("write archive header length: %w", err)
	}

	if _, err := writer.Write(headerBytes); err != nil {
		return nil, fmt.Errorf("write archive header: %w", err)
	}

	return &encryptedArchiveWriter{
		writer:     writer,
		sender:     sender,
		headerHash: sha256.Sum256(headerBytes),
	}, nil
}

func (s *encryptedArchiveWriter) Write(p []byte) (int, error) {
	if s.closed {
		return 0, fmt.Errorf("encrypted archive writer is closed")
	}

	written := 0
	for len(p) > 0 {
		chunkSize := len(p)
		if chunkSize > encryptedArchiveChunkSize {
			chunkSize = encryptedArchiveChunkSize
		}

		if err := s.writeFrame(encryptedArchiveFrameData, p[:chunkSize]); err != nil {
			return written, err
		}

		p = p[chunkSize:]
		written += chunkSize
	}

	return written, nil
}

func (s *encryptedArchiveWriter) Close() error {
	if s.closed {
		return fmt.Errorf("encrypted archive writer is already closed")
	}
	if err := s.writeFrame(encryptedArchiveFrameFinal, nil); err != nil {
		return err
	}

	s.closed = true

	slog.Info("retriever archive encryption finalized",
		slog.Uint64("frame_count", s.frameIndex),
	)

	return nil
}

func (s *encryptedArchiveWriter) writeFrame(frameType byte, plaintext []byte) error {
	ciphertext, err := s.sender.Seal(archiveFrameAAD(s.headerHash, s.frameIndex, frameType), plaintext)
	if err != nil {
		return fmt.Errorf("encrypt archive frame %d: %w", s.frameIndex, err)
	}

	if err := writeEncryptedArchiveFrame(s.writer, frameType, ciphertext); err != nil {
		return err
	}

	s.frameIndex++
	return nil
}

func writeEncryptedArchiveFrame(writer io.Writer, frameType byte, ciphertext []byte) error {
	if uint64(len(ciphertext)) > math.MaxUint32 {
		return fmt.Errorf("encrypted archive frame is too large")
	}

	var frameHeader [5]byte
	frameHeader[0] = frameType
	binary.BigEndian.PutUint32(frameHeader[1:], uint32(len(ciphertext)))

	if _, err := writer.Write(frameHeader[:]); err != nil {
		return fmt.Errorf("write archive frame header: %w", err)
	}

	if _, err := writer.Write(ciphertext); err != nil {
		return fmt.Errorf("write archive frame: %w", err)
	}

	return nil
}

func NewEncryptedArchiveReader(reader io.Reader, identity hpke.PrivateKey) (io.Reader, error) {
	return newEncryptedArchiveReader(reader, identity)
}

func newEncryptedArchiveReader(reader io.Reader, identity hpke.PrivateKey) (*encryptedArchiveReader, error) {
	if identity == nil {
		return nil, fmt.Errorf("identity private key is required")
	}

	if identity.KEM().ID() != defaultArchiveKEM().ID() {
		return nil, fmt.Errorf("identity private key uses unsupported KEM")
	}

	headerBytes, header, err := readEncryptedArchiveHeader(reader)
	if err != nil {
		return nil, err
	}
	encapsulatedKey, err := base64.StdEncoding.DecodeString(header.Crypto.EncapsulatedKey)
	if err != nil {
		return nil, fmt.Errorf("decode encapsulated key: %w", err)
	}

	recipient, err := hpke.NewRecipient(encapsulatedKey, identity, defaultArchiveKDF(), defaultArchiveAEAD(), []byte(encryptedArchiveHPKEInfo))
	if err != nil {
		return nil, fmt.Errorf("create archive recipient: %w", err)
	}

	return &encryptedArchiveReader{
		reader:     reader,
		recipient:  recipient,
		headerHash: sha256.Sum256(headerBytes),
	}, nil
}

func readEncryptedArchiveHeader(reader io.Reader) ([]byte, encryptedArchiveHeader, error) {
	var header encryptedArchiveHeader
	magic := make([]byte, len(encryptedArchiveMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return nil, header, fmt.Errorf("read archive magic: %w", err)
	}

	if string(magic) != encryptedArchiveMagic {
		return nil, header, fmt.Errorf("unsupported archive magic %q", string(magic))
	}

	var headerLenBytes [4]byte
	if _, err := io.ReadFull(reader, headerLenBytes[:]); err != nil {
		return nil, header, fmt.Errorf("read archive header length: %w", err)
	}

	headerLen := binary.BigEndian.Uint32(headerLenBytes[:])
	if headerLen == 0 || headerLen > maxEncryptedArchiveHeaderLen {
		return nil, header, fmt.Errorf("archive header length %d is invalid", headerLen)
	}

	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return nil, header, fmt.Errorf("read archive header: %w", err)
	}

	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, header, fmt.Errorf("decode archive header: %w", err)
	}

	if err := validateEncryptedArchiveHeader(header); err != nil {
		return nil, header, err
	}

	return headerBytes, header, nil
}

func validateEncryptedArchiveHeader(header encryptedArchiveHeader) error {
	if header.Format != encryptedArchiveFormat {
		return fmt.Errorf("unsupported encrypted archive format %q", header.Format)
	}

	if header.Archive.Format != "tar" {
		return fmt.Errorf("unsupported archive payload format %q", header.Archive.Format)
	}

	if header.Archive.Compression != "none" {
		return fmt.Errorf("unsupported archive payload compression %q", header.Archive.Compression)
	}

	if err := validateArchiveCryptoMetadata(ArchiveCryptoMetadata{
		Scheme: header.Crypto.Scheme,
		KEM:    header.Crypto.KEM,
		KDF:    header.Crypto.KDF,
		AEAD:   header.Crypto.AEAD,
	}); err != nil {
		return err
	}

	if strings.TrimSpace(header.Crypto.EncapsulatedKey) == "" {
		return fmt.Errorf("archive header is missing encapsulated key")
	}

	if header.ChunkSize != encryptedArchiveChunkSize {
		return fmt.Errorf("unsupported archive chunk size %d", header.ChunkSize)
	}

	return nil
}

func (s *encryptedArchiveReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	for len(s.plaintext) == 0 {
		if s.final {
			return 0, io.EOF
		}

		if err := s.readNextFrame(); err != nil {
			return 0, err
		}
	}

	n := copy(p, s.plaintext)
	s.plaintext = s.plaintext[n:]

	return n, nil
}

func (s *encryptedArchiveReader) readNextFrame() error {
	var frameHeader [5]byte
	if _, err := io.ReadFull(s.reader, frameHeader[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("encrypted archive missing final frame")
		}

		return fmt.Errorf("read archive frame header: %w", err)
	}

	frameType := frameHeader[0]
	if frameType != encryptedArchiveFrameData && frameType != encryptedArchiveFrameFinal {
		return fmt.Errorf("archive frame %d has unsupported type %d", s.frameIndex, frameType)
	}

	ciphertextLen := binary.BigEndian.Uint32(frameHeader[1:])
	if ciphertextLen > maxEncryptedArchiveFrameSize {
		return fmt.Errorf("archive frame %d is too large", s.frameIndex)
	}

	ciphertext := make([]byte, ciphertextLen)
	if _, err := io.ReadFull(s.reader, ciphertext); err != nil {
		return fmt.Errorf("read archive frame %d: %w", s.frameIndex, err)
	}

	plaintext, err := s.recipient.Open(archiveFrameAAD(s.headerHash, s.frameIndex, frameType), ciphertext)
	if err != nil {
		return fmt.Errorf("decrypt archive frame %d: %w", s.frameIndex, err)
	}

	s.frameIndex++

	if frameType == encryptedArchiveFrameFinal {
		if len(plaintext) != 0 {
			return fmt.Errorf("encrypted archive final frame contained plaintext")
		}

		if err := requireEncryptedArchiveEOF(s.reader); err != nil {
			return err
		}

		s.final = true

		slog.Info("retriever archive decryption finalized",
			slog.Uint64("frame_count", s.frameIndex),
		)

		return nil
	}

	s.plaintext = plaintext
	return nil
}

func requireEncryptedArchiveEOF(reader io.Reader) error {
	var extra [1]byte
	n, err := reader.Read(extra[:])
	if n > 0 {
		return fmt.Errorf("encrypted archive has trailing data after final frame")
	}

	if err == nil {
		return fmt.Errorf("encrypted archive stream did not end after final frame")
	}

	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read archive trailer: %w", err)
	}

	return nil
}

func archiveFrameAAD(headerHash [sha256.Size]byte, frameIndex uint64, frameType byte) []byte {
	var aad bytes.Buffer
	aad.WriteString(encryptedArchiveMagic)
	aad.WriteByte(0)
	aad.Write(headerHash[:])

	var indexBytes [8]byte
	binary.BigEndian.PutUint64(indexBytes[:], frameIndex)

	aad.Write(indexBytes[:])
	aad.WriteByte(frameType)

	return aad.Bytes()
}

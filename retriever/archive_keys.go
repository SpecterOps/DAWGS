package retriever

import (
	"crypto/hpke"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	archiveKeyEnvelopeFormat = "retriever-hpke-key-v1"
	archiveKeyTypePrivate    = "private"
	archiveKeyTypePublic     = "public"

	archiveCryptoScheme = "hpke"
	archiveKEMName      = "ML-KEM-1024"
	archiveKDFName      = "HKDF-SHA512"
	archiveAEADName     = "AES-256-GCM"
)

type ArchiveCryptoMetadata struct {
	Scheme string `json:"scheme"`
	KEM    string `json:"kem"`
	KDF    string `json:"kdf"`
	AEAD   string `json:"aead"`
}

type ArchiveKeyEnvelope struct {
	Format string                `json:"format"`
	Type   string                `json:"type"`
	Crypto ArchiveCryptoMetadata `json:"crypto"`
	Key    string                `json:"key"`
}

func DefaultArchiveKEM() hpke.KEM {
	return defaultArchiveKEM()
}

func defaultArchiveKEM() hpke.KEM {
	return hpke.MLKEM1024()
}

func DefaultArchiveKDF() hpke.KDF {
	return defaultArchiveKDF()
}

func defaultArchiveKDF() hpke.KDF {
	return hpke.HKDFSHA512()
}

func DefaultArchiveAEAD() hpke.AEAD {
	return defaultArchiveAEAD()
}

func defaultArchiveAEAD() hpke.AEAD {
	return hpke.AES256GCM()
}

func DefaultArchiveCryptoMetadata() ArchiveCryptoMetadata {
	return defaultArchiveCryptoMetadata()
}

func defaultArchiveCryptoMetadata() ArchiveCryptoMetadata {
	return ArchiveCryptoMetadata{
		Scheme: archiveCryptoScheme,
		KEM:    archiveKEMName,
		KDF:    archiveKDFName,
		AEAD:   archiveAEADName,
	}
}

func GenerateArchiveKeyPair() (hpke.PrivateKey, hpke.PublicKey, error) {
	privateKey, err := defaultArchiveKEM().GenerateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("generate archive key: %w", err)
	}

	return privateKey, privateKey.PublicKey(), nil
}

func GenerateArchiveKeyFiles(privatePath, publicPath string) error {
	return generateArchiveKeyFiles(privatePath, publicPath)
}

func Keygen(options KeygenOptions) error {
	if err := options.validate(); err != nil {
		return err
	}

	return generateArchiveKeyFiles(options.PrivatePath, options.PublicPath)
}

func generateArchiveKeyFiles(privatePath, publicPath string) error {
	privatePath = strings.TrimSpace(privatePath)
	publicPath = strings.TrimSpace(publicPath)
	if privatePath == "" {
		return fmt.Errorf("private key path is required; pass -private")
	}

	if publicPath == "" {
		return fmt.Errorf("public key path is required; pass -public")
	}

	if sameCleanPath(privatePath, publicPath) {
		return fmt.Errorf("private and public key paths must be different")
	}

	if err := requirePathDoesNotExist(privatePath); err != nil {
		return err
	}

	if err := requirePathDoesNotExist(publicPath); err != nil {
		return err
	}

	privateKey, publicKey, err := GenerateArchiveKeyPair()
	if err != nil {
		return err
	}

	if err := writeExclusiveArchivePrivateKeyFile(privatePath, privateKey); err != nil {
		return err
	}

	if err := writeExclusiveArchivePublicKeyFile(publicPath, publicKey); err != nil {
		_ = os.Remove(privatePath)
		return err
	}

	return nil
}

func NewArchivePrivateKeyEnvelope(privateKey hpke.PrivateKey) (ArchiveKeyEnvelope, error) {
	if privateKey == nil {
		return ArchiveKeyEnvelope{}, fmt.Errorf("private key is required")
	}

	privateKeyBytes, err := privateKey.Bytes()
	if err != nil {
		return ArchiveKeyEnvelope{}, fmt.Errorf("serialize private key: %w", err)
	}

	return ArchiveKeyEnvelope{
		Format: archiveKeyEnvelopeFormat,
		Type:   archiveKeyTypePrivate,
		Crypto: defaultArchiveCryptoMetadata(),
		Key:    base64.StdEncoding.EncodeToString(privateKeyBytes),
	}, nil
}

func NewArchivePublicKeyEnvelope(publicKey hpke.PublicKey) (ArchiveKeyEnvelope, error) {
	if publicKey == nil {
		return ArchiveKeyEnvelope{}, fmt.Errorf("public key is required")
	}

	return ArchiveKeyEnvelope{
		Format: archiveKeyEnvelopeFormat,
		Type:   archiveKeyTypePublic,
		Crypto: defaultArchiveCryptoMetadata(),
		Key:    base64.StdEncoding.EncodeToString(publicKey.Bytes()),
	}, nil
}

func WriteArchivePrivateKey(writer io.Writer, privateKey hpke.PrivateKey) error {
	envelope, err := NewArchivePrivateKeyEnvelope(privateKey)
	if err != nil {
		return err
	}

	if err := writeArchiveKeyEnvelope(writer, envelope); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}

	return nil
}

func WriteArchivePublicKey(writer io.Writer, publicKey hpke.PublicKey) error {
	envelope, err := NewArchivePublicKeyEnvelope(publicKey)
	if err != nil {
		return err
	}

	if err := writeArchiveKeyEnvelope(writer, envelope); err != nil {
		return fmt.Errorf("write public key: %w", err)
	}

	return nil
}

func LoadArchivePublicKey(path string) (hpke.PublicKey, error) {
	return loadArchivePublicKey(path)
}

func loadArchivePublicKey(path string) (hpke.PublicKey, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("public key path is required")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read public key: %w", err)
	}
	defer file.Close()

	return ReadArchivePublicKey(file)
}

func ReadArchivePublicKey(reader io.Reader) (hpke.PublicKey, error) {
	keyBytes, err := readArchiveKeyBytes(reader, archiveKeyTypePublic)
	if err != nil {
		return nil, err
	}

	publicKey, err := defaultArchiveKEM().NewPublicKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}

	return publicKey, nil
}

func LoadArchivePrivateKey(path string) (hpke.PrivateKey, error) {
	return loadArchivePrivateKey(path)
}

func loadArchivePrivateKey(path string) (hpke.PrivateKey, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("private key path is required")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}
	defer file.Close()

	return ReadArchivePrivateKey(file)
}

func ReadArchivePrivateKey(reader io.Reader) (hpke.PrivateKey, error) {
	keyBytes, err := readArchiveKeyBytes(reader, archiveKeyTypePrivate)
	if err != nil {
		return nil, err
	}

	privateKey, err := defaultArchiveKEM().NewPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	return privateKey, nil
}

func readArchiveKeyBytes(reader io.Reader, expectedType string) ([]byte, error) {
	if reader == nil {
		return nil, fmt.Errorf("%s key reader is required", expectedType)
	}

	var envelope ArchiveKeyEnvelope
	if err := json.NewDecoder(reader).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode %s key envelope: %w", expectedType, err)
	}

	if err := validateArchiveKeyEnvelope(envelope, expectedType); err != nil {
		return nil, err
	}

	keyBytes, err := base64.StdEncoding.DecodeString(envelope.Key)
	if err != nil {
		return nil, fmt.Errorf("decode %s key material: %w", expectedType, err)
	}

	if len(keyBytes) == 0 {
		return nil, fmt.Errorf("%s key material is empty", expectedType)
	}

	return keyBytes, nil
}

func writeArchiveKeyEnvelope(writer io.Writer, envelope ArchiveKeyEnvelope) error {
	if writer == nil {
		return fmt.Errorf("key writer is required")
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")

	return encoder.Encode(envelope)
}

func validateArchiveKeyEnvelope(envelope ArchiveKeyEnvelope, expectedType string) error {
	if envelope.Format != archiveKeyEnvelopeFormat {
		return fmt.Errorf("unsupported archive key format %q", envelope.Format)
	}

	if envelope.Type != expectedType {
		return fmt.Errorf("expected %s key envelope, got %q", expectedType, envelope.Type)
	}

	if err := validateArchiveCryptoMetadata(envelope.Crypto); err != nil {
		return err
	}

	if strings.TrimSpace(envelope.Key) == "" {
		return fmt.Errorf("%s key envelope is missing key material", expectedType)
	}

	return nil
}

func validateArchiveCryptoMetadata(metadata ArchiveCryptoMetadata) error {
	if metadata.Scheme != archiveCryptoScheme {
		return fmt.Errorf("unsupported archive crypto scheme %q", metadata.Scheme)
	}

	if metadata.KEM != archiveKEMName {
		return fmt.Errorf("unsupported archive KEM %q", metadata.KEM)
	}

	if metadata.KDF != archiveKDFName {
		return fmt.Errorf("unsupported archive KDF %q", metadata.KDF)
	}

	if metadata.AEAD != archiveAEADName {
		return fmt.Errorf("unsupported archive AEAD %q", metadata.AEAD)
	}

	return nil
}

func writeExclusiveArchivePrivateKeyFile(path string, privateKey hpke.PrivateKey) error {
	return writeExclusiveArchiveKeyFile(path, 0o600, func(writer io.Writer) error {
		return WriteArchivePrivateKey(writer, privateKey)
	})
}

func writeExclusiveArchivePublicKeyFile(path string, publicKey hpke.PublicKey) error {
	return writeExclusiveArchiveKeyFile(path, 0o644, func(writer io.Writer) error {
		return WriteArchivePublicKey(writer, publicKey)
	})
}

func writeExclusiveArchiveKeyFile(path string, mode os.FileMode, write func(io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent directory for %s: %w", path, err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}

	writeErr := write(file)
	closeErr := file.Close()

	if writeErr != nil {
		_ = os.Remove(path)
		return writeErr
	}

	if closeErr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close %s: %w", path, closeErr)
	}

	return nil
}

func requirePathDoesNotExist(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s already exists", path)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect %s: %w", path, err)
	}

	return nil
}

func sameCleanPath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr == nil && rightErr == nil {
		return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
	}

	return filepath.Clean(left) == filepath.Clean(right)
}

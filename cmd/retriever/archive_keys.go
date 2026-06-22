package main

import (
	"crypto/hpke"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

type archiveCryptoMetadata struct {
	Scheme string `json:"scheme"`
	KEM    string `json:"kem"`
	KDF    string `json:"kdf"`
	AEAD   string `json:"aead"`
}

type archiveKeyEnvelope struct {
	Format string                `json:"format"`
	Type   string                `json:"type"`
	Crypto archiveCryptoMetadata `json:"crypto"`
	Key    string                `json:"key"`
}

func defaultArchiveKEM() hpke.KEM {
	return hpke.MLKEM1024()
}

func defaultArchiveKDF() hpke.KDF {
	return hpke.HKDFSHA512()
}

func defaultArchiveAEAD() hpke.AEAD {
	return hpke.AES256GCM()
}

func defaultArchiveCryptoMetadata() archiveCryptoMetadata {
	return archiveCryptoMetadata{
		Scheme: archiveCryptoScheme,
		KEM:    archiveKEMName,
		KDF:    archiveKDFName,
		AEAD:   archiveAEADName,
	}
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

	privateKey, err := defaultArchiveKEM().GenerateKey()
	if err != nil {
		return fmt.Errorf("generate archive key: %w", err)
	}
	privateKeyBytes, err := privateKey.Bytes()
	if err != nil {
		return fmt.Errorf("serialize private key: %w", err)
	}
	publicKeyBytes := privateKey.PublicKey().Bytes()

	privateEnvelope := archiveKeyEnvelope{
		Format: archiveKeyEnvelopeFormat,
		Type:   archiveKeyTypePrivate,
		Crypto: defaultArchiveCryptoMetadata(),
		Key:    base64.StdEncoding.EncodeToString(privateKeyBytes),
	}
	publicEnvelope := archiveKeyEnvelope{
		Format: archiveKeyEnvelopeFormat,
		Type:   archiveKeyTypePublic,
		Crypto: defaultArchiveCryptoMetadata(),
		Key:    base64.StdEncoding.EncodeToString(publicKeyBytes),
	}

	if err := writeExclusiveJSONFile(privatePath, privateEnvelope, 0o600); err != nil {
		return err
	}
	if err := writeExclusiveJSONFile(publicPath, publicEnvelope, 0o644); err != nil {
		_ = os.Remove(privatePath)
		return err
	}
	return nil
}

func loadArchivePublicKey(path string) (hpke.PublicKey, error) {
	keyBytes, err := loadArchiveKeyBytes(path, archiveKeyTypePublic)
	if err != nil {
		return nil, err
	}
	publicKey, err := defaultArchiveKEM().NewPublicKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}
	return publicKey, nil
}

func loadArchivePrivateKey(path string) (hpke.PrivateKey, error) {
	keyBytes, err := loadArchiveKeyBytes(path, archiveKeyTypePrivate)
	if err != nil {
		return nil, err
	}
	privateKey, err := defaultArchiveKEM().NewPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return privateKey, nil
}

func loadArchiveKeyBytes(path, expectedType string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%s key path is required", expectedType)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s key: %w", expectedType, err)
	}
	var envelope archiveKeyEnvelope
	if err := json.Unmarshal(contents, &envelope); err != nil {
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

func validateArchiveKeyEnvelope(envelope archiveKeyEnvelope, expectedType string) error {
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

func validateArchiveCryptoMetadata(metadata archiveCryptoMetadata) error {
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

func writeExclusiveJSONFile(path string, value any, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent directory for %s: %w", path, err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	writeErr := encoder.Encode(value)
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write %s: %w", path, writeErr)
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

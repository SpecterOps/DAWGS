package retriever

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchiveKeygenAndLoad(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "retriever-private.key")
	publicPath := filepath.Join(dir, "retriever-public.key")

	if err := generateArchiveKeyFiles(privatePath, publicPath); err != nil {
		t.Fatalf("generate archive keys: %v", err)
	}

	privateInfo, err := os.Stat(privatePath)
	if err != nil {
		t.Fatalf("stat private key: %v", err)
	}

	if got := privateInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("private key mode = %v, want 0600", got)
	}

	if _, err := os.Stat(publicPath); err != nil {
		t.Fatalf("stat public key: %v", err)
	}

	publicKey, err := loadArchivePublicKey(publicPath)
	if err != nil {
		t.Fatalf("load public key: %v", err)
	}

	privateKey, err := loadArchivePrivateKey(privatePath)
	if err != nil {
		t.Fatalf("load private key: %v", err)
	}

	payload := []byte("archive payload")
	ciphertext := encryptArchiveBytes(t, payload, publicKey)
	plaintext, err := decryptArchiveBytes(ciphertext, privateKey)
	if err != nil {
		t.Fatalf("decrypt with generated keys: %v", err)
	}

	if !bytes.Equal(plaintext, payload) {
		t.Fatalf("decrypted payload = %q, want %q", plaintext, payload)
	}

	if err := generateArchiveKeyFiles(privatePath, publicPath); err == nil {
		t.Fatalf("expected keygen to refuse existing key files")
	}

	if _, err := loadArchivePrivateKey(publicPath); err == nil {
		t.Fatalf("expected public key envelope to be rejected as private key")
	}
}

func TestArchiveKeyStreamRoundTrip(t *testing.T) {
	privateKey, publicKey, err := GenerateArchiveKeyPair()
	if err != nil {
		t.Fatalf("generate archive key pair: %v", err)
	}

	var privateBuffer bytes.Buffer
	if err := WriteArchivePrivateKey(&privateBuffer, privateKey); err != nil {
		t.Fatalf("write private key: %v", err)
	}

	readPrivateKey, err := ReadArchivePrivateKey(bytes.NewReader(privateBuffer.Bytes()))
	if err != nil {
		t.Fatalf("read private key: %v", err)
	}

	var publicBuffer bytes.Buffer

	if err := WriteArchivePublicKey(&publicBuffer, publicKey); err != nil {
		t.Fatalf("write public key: %v", err)
	}

	readPublicKey, err := ReadArchivePublicKey(bytes.NewReader(publicBuffer.Bytes()))
	if err != nil {
		t.Fatalf("read public key: %v", err)
	}

	payload := []byte("stream archive payload")
	ciphertext := encryptArchiveBytes(t, payload, readPublicKey)
	plaintext, err := decryptArchiveBytes(ciphertext, readPrivateKey)
	if err != nil {
		t.Fatalf("decrypt with stream keys: %v", err)
	}

	if !bytes.Equal(plaintext, payload) {
		t.Fatalf("decrypted payload = %q, want %q", plaintext, payload)
	}

	if _, err := ReadArchivePrivateKey(bytes.NewReader(publicBuffer.Bytes())); err == nil {
		t.Fatalf("expected public key envelope to be rejected as private key")
	}

	if err := WriteArchivePrivateKey(&privateBuffer, nil); err == nil {
		t.Fatalf("expected nil private key error")
	}
}

func TestKeygenOptions(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.key")
	publicPath := filepath.Join(dir, "public.key")

	if err := Keygen(DefaultKeygenOptions(privatePath, publicPath)); err != nil {
		t.Fatalf("keygen with options: %v", err)
	}

	if _, err := LoadArchivePrivateKey(privatePath); err != nil {
		t.Fatalf("load generated private key: %v", err)
	}

	if _, err := LoadArchivePublicKey(publicPath); err != nil {
		t.Fatalf("load generated public key: %v", err)
	}

	if err := Keygen(DefaultKeygenOptions(privatePath, publicPath)); err == nil {
		t.Fatalf("expected keygen options to refuse existing files")
	}

	if err := Keygen(DefaultKeygenOptions("", publicPath)); err == nil {
		t.Fatalf("expected keygen options to validate paths")
	}
}

func TestArchiveKeygenValidation(t *testing.T) {
	dir := t.TempDir()
	samePath := filepath.Join(dir, "same.key")

	if err := generateArchiveKeyFiles("", filepath.Join(dir, "public.key")); err == nil || !strings.Contains(err.Error(), "private key path") {
		t.Fatalf("expected missing private path error, got %v", err)
	}

	if err := generateArchiveKeyFiles(filepath.Join(dir, "private.key"), ""); err == nil || !strings.Contains(err.Error(), "public key path") {
		t.Fatalf("expected missing public path error, got %v", err)
	}

	if err := generateArchiveKeyFiles(samePath, samePath); err == nil || !strings.Contains(err.Error(), "must be different") {
		t.Fatalf("expected same path error, got %v", err)
	}
}

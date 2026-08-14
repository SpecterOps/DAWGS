package archive

import (
	"bytes"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	KeyFormat      = "ret-hpke-key-v1"
	EnvelopeFormat = "ret-encrypted-tar-v1"

	kemName  = "ML-KEM-1024"
	kdfName  = "HKDF-SHA512"
	aeadName = "AES-256-GCM"

	publicRole  = "public"
	privateRole = "private"

	publicKeyMaterialSize  = 1568
	privateKeyMaterialSize = 64
	maxKeyDocumentSize     = 8 * 1024
)

type hpkeSuite struct {
	kem  hpke.KEM
	kdf  hpke.KDF
	aead hpke.AEAD
}

var archiveHPKESuite = hpkeSuite{
	kem:  hpke.MLKEM1024(),
	kdf:  hpke.HKDFSHA512(),
	aead: hpke.AES256GCM(),
}

// PublicKey is an opaque archive recipient key.
type PublicKey struct {
	material [publicKeyMaterialSize]byte
	valid    bool
}

// PrivateKey is an opaque archive identity key.
type PrivateKey struct {
	material [privateKeyMaterialSize]byte
	valid    bool
}

type keyDocument struct {
	Format      string `json:"format"`
	Role        string `json:"role"`
	KEM         string `json:"kem"`
	KDF         string `json:"kdf"`
	AEAD        string `json:"aead"`
	Material    string `json:"material"`
	Fingerprint string `json:"public_fingerprint"`
}

// GenerateKeyPair creates an ML-KEM-1024 archive recipient and identity.
func GenerateKeyPair() (PublicKey, PrivateKey, error) {
	privateHPKEKey, err := archiveHPKESuite.kem.GenerateKey()
	if err != nil {
		return PublicKey{}, PrivateKey{}, fmt.Errorf("generate archive key pair: %w", err)
	}

	privateMaterial, err := privateHPKEKey.Bytes()
	if err != nil {
		return PublicKey{}, PrivateKey{}, fmt.Errorf("serialize archive private key: %w", err)
	}
	if len(privateMaterial) != privateKeyMaterialSize {
		return PublicKey{}, PrivateKey{}, fmt.Errorf(
			"generated private key material has length %d, want %d",
			len(privateMaterial),
			privateKeyMaterialSize,
		)
	}

	publicMaterial := privateHPKEKey.PublicKey().Bytes()
	if len(publicMaterial) != publicKeyMaterialSize {
		return PublicKey{}, PrivateKey{}, fmt.Errorf(
			"generated public key material has length %d, want %d",
			len(publicMaterial),
			publicKeyMaterialSize,
		)
	}

	var publicKey PublicKey
	copy(publicKey.material[:], publicMaterial)
	publicKey.valid = true
	var privateKey PrivateKey
	copy(privateKey.material[:], privateMaterial)
	privateKey.valid = true
	return publicKey, privateKey, nil
}

// ReadPublicKey reads and validates a public archive key document.
func ReadPublicKey(path string) (PublicKey, error) {
	document, err := readKeyDocument(path)
	if err != nil {
		return PublicKey{}, fmt.Errorf("read public key: %w", err)
	}
	material, err := validateKeyDocument(document, publicRole, publicKeyMaterialSize)
	if err != nil {
		return PublicKey{}, err
	}

	publicHPKEKey, err := archiveHPKESuite.kem.NewPublicKey(material)
	if err != nil {
		return PublicKey{}, fmt.Errorf("parse public key material: %w", err)
	}
	if err := validatePublicFingerprint(document.Fingerprint, publicHPKEKey.Bytes()); err != nil {
		return PublicKey{}, err
	}

	var key PublicKey
	copy(key.material[:], material)
	key.valid = true
	return key, nil
}

// ReadPrivateKey reads and validates a private archive key document.
func ReadPrivateKey(path string) (PrivateKey, error) {
	document, err := readKeyDocument(path)
	if err != nil {
		return PrivateKey{}, fmt.Errorf("read private key: %w", err)
	}
	material, err := validateKeyDocument(document, privateRole, privateKeyMaterialSize)
	if err != nil {
		return PrivateKey{}, err
	}

	privateHPKEKey, err := archiveHPKESuite.kem.NewPrivateKey(material)
	if err != nil {
		return PrivateKey{}, fmt.Errorf("parse private key material: %w", err)
	}
	if err := validatePublicFingerprint(document.Fingerprint, privateHPKEKey.PublicKey().Bytes()); err != nil {
		return PrivateKey{}, err
	}

	var key PrivateKey
	copy(key.material[:], material)
	key.valid = true
	return key, nil
}

// WritePublicKey writes a public archive key document without replacing a path.
func WritePublicKey(path string, key PublicKey) error {
	if !key.valid {
		return fmt.Errorf("public key is required")
	}
	document := keyDocument{
		Format:      KeyFormat,
		Role:        publicRole,
		KEM:         kemName,
		KDF:         kdfName,
		AEAD:        aeadName,
		Material:    base64.StdEncoding.EncodeToString(key.material[:]),
		Fingerprint: fingerprint(key.material[:]),
	}
	return writeKeyDocument(path, 0o644, document)
}

// WritePrivateKey writes a private archive key document without replacing a path.
func WritePrivateKey(path string, key PrivateKey) error {
	owned, err := writePrivateKeyOwned(path, key)
	if err != nil {
		return err
	}
	return owned.release()
}

func writePrivateKeyOwned(path string, key PrivateKey) (*ownedPath, error) {
	if !key.valid {
		return nil, fmt.Errorf("private key is required")
	}
	privateHPKEKey, err := archiveHPKESuite.kem.NewPrivateKey(key.material[:])
	if err != nil {
		return nil, fmt.Errorf("parse private key material: %w", err)
	}
	document := keyDocument{
		Format:      KeyFormat,
		Role:        privateRole,
		KEM:         kemName,
		KDF:         kdfName,
		AEAD:        aeadName,
		Material:    base64.StdEncoding.EncodeToString(key.material[:]),
		Fingerprint: fingerprint(privateHPKEKey.PublicKey().Bytes()),
	}
	return writeKeyDocumentOwned(path, 0o600, document)
}

// WriteKeyPair writes a private key followed by its public key without replacing
// either destination. A public-key failure removes only the private file created
// by this call when its recorded identity still owns the private pathname.
func WriteKeyPair(
	privatePath string,
	privateKey PrivateKey,
	publicPath string,
	publicKey PublicKey,
) error {
	return writeKeyPair(
		privatePath,
		privateKey,
		publicPath,
		publicKey,
		archiveOperations{},
	)
}

func writeKeyPair(
	privatePath string,
	privateKey PrivateKey,
	publicPath string,
	publicKey PublicKey,
	operations archiveOperations,
) error {
	privateOwned, err := writePrivateKeyOwned(privatePath, privateKey)
	if err != nil {
		return err
	}
	if err := WritePublicKey(publicPath, publicKey); err != nil {
		return errors.Join(err, privateOwned.remove(operations))
	}
	return privateOwned.release()
}

// PublicFingerprint returns the lowercase SHA-256 fingerprint of the public key.
func PublicFingerprint(key PublicKey) string {
	if !key.valid {
		return ""
	}
	return fingerprint(key.material[:])
}

func readKeyDocument(path string) (document keyDocument, returnErr error) {
	if strings.TrimSpace(path) == "" {
		return document, fmt.Errorf("key path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return document, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("close key document: %w", err))
		}
	}()

	payload, err := io.ReadAll(io.LimitReader(file, maxKeyDocumentSize+1))
	if err != nil {
		return document, fmt.Errorf("read key JSON: %w", err)
	}
	if len(payload) > maxKeyDocumentSize {
		return document, fmt.Errorf("key JSON exceeds %d bytes", maxKeyDocumentSize)
	}
	if err := validateExactJSONObject(
		payload,
		"format",
		"role",
		"kem",
		"kdf",
		"aead",
		"material",
		"public_fingerprint",
	); err != nil {
		return document, fmt.Errorf("validate key JSON fields: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return document, fmt.Errorf("decode key JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return document, fmt.Errorf("key JSON contains a trailing value")
		}
		return document, fmt.Errorf("decode trailing key JSON: %w", err)
	}
	return document, nil
}

func validateExactJSONObject(payload []byte, expectedFields ...string) error {
	expected := make(map[string]struct{}, len(expectedFields))
	for _, field := range expectedFields {
		expected[field] = struct{}{}
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON object start: %w", err)
	}
	opening, ok := token.(json.Delim)
	if !ok || opening != '{' {
		return fmt.Errorf("JSON value must be an object")
	}

	seen := make(map[string]struct{}, len(expectedFields))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON field name: %w", err)
		}
		field, ok := token.(string)
		if !ok {
			return fmt.Errorf("JSON object field name must be a string")
		}
		if _, ok := expected[field]; !ok {
			return fmt.Errorf(
				"unknown field %q; top-level field names must use the exact lowercase schema",
				field,
			)
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("duplicate top-level field %q", field)
		}
		seen[field] = struct{}{}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("decode JSON field %q: %w", field, err)
		}
	}

	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON object end: %w", err)
	}
	closing, ok := token.(json.Delim)
	if !ok || closing != '}' {
		return fmt.Errorf("JSON object is not terminated")
	}
	for _, field := range expectedFields {
		if _, ok := seen[field]; !ok {
			return fmt.Errorf("missing required top-level field %q", field)
		}
	}
	return nil
}

func validateKeyDocument(document keyDocument, expectedRole string, expectedMaterialSize int) ([]byte, error) {
	if document.Format != KeyFormat {
		return nil, fmt.Errorf("key format must be %q, got %q", KeyFormat, document.Format)
	}
	if document.Role != expectedRole {
		return nil, fmt.Errorf("key role must be %q, got %q", expectedRole, document.Role)
	}
	if document.KEM != kemName {
		return nil, fmt.Errorf("key KEM must be %q, got %q", kemName, document.KEM)
	}
	if document.KDF != kdfName {
		return nil, fmt.Errorf("key KDF must be %q, got %q", kdfName, document.KDF)
	}
	if document.AEAD != aeadName {
		return nil, fmt.Errorf("key AEAD must be %q, got %q", aeadName, document.AEAD)
	}
	if document.Material == "" {
		return nil, fmt.Errorf("%s key material is required", expectedRole)
	}

	material, err := base64.StdEncoding.Strict().DecodeString(document.Material)
	if err != nil || base64.StdEncoding.EncodeToString(material) != document.Material {
		if err == nil {
			err = fmt.Errorf("encoding is not canonical")
		}
		return nil, fmt.Errorf("decode %s key material as canonical base64: %w", expectedRole, err)
	}
	if len(material) != expectedMaterialSize {
		return nil, fmt.Errorf(
			"%s key material has length %d, want %d",
			expectedRole,
			len(material),
			expectedMaterialSize,
		)
	}
	return material, nil
}

func validatePublicFingerprint(got string, publicMaterial []byte) error {
	want := fingerprint(publicMaterial)
	if len(got) != sha256.Size*2 {
		return fmt.Errorf("public fingerprint must be %d lowercase hexadecimal characters", sha256.Size*2)
	}
	if _, err := hex.DecodeString(got); err != nil || strings.ToLower(got) != got {
		return fmt.Errorf("public fingerprint must be %d lowercase hexadecimal characters", sha256.Size*2)
	}
	if got != want {
		return fmt.Errorf("public fingerprint does not match public key material")
	}
	return nil
}

func fingerprint(publicMaterial []byte) string {
	digest := sha256.Sum256(publicMaterial)
	return hex.EncodeToString(digest[:])
}

func writeKeyDocument(path string, mode fs.FileMode, document keyDocument) error {
	owned, err := writeKeyDocumentOwned(path, mode, document)
	if err != nil {
		return err
	}
	return owned.release()
}

func writeKeyDocumentOwned(
	path string,
	mode fs.FileMode,
	document keyDocument,
) (*ownedPath, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("key path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve key path: %w", err)
	}
	parentPath, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, fmt.Errorf("resolve key parent: %w", err)
	}
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return nil, fmt.Errorf("open key parent: %w", err)
	}
	name := filepath.Base(absolute)
	file, err := parent.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("create key document: %w", err),
			wrapKeyCloseError("key parent", parent.Close()),
		)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("inspect created key document: %w", err),
			fmt.Errorf("close key document: %w", file.Close()),
			fmt.Errorf(
				"ownership cleanup for key document: created identity is unavailable; preserving pathname",
			),
			wrapKeyCloseError("key parent", parent.Close()),
		)
	}
	owned := &ownedPath{
		parent:      parent,
		handle:      file,
		name:        name,
		info:        info,
		description: "key document",
	}

	var primaryErr error
	if err := file.Chmod(mode); err != nil {
		primaryErr = fmt.Errorf("set key document mode: %w", err)
	} else {
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(document); err != nil {
			primaryErr = fmt.Errorf("encode key document: %w", err)
		}
	}
	if primaryErr == nil {
		return owned, nil
	}
	return nil, errors.Join(primaryErr, owned.remove(archiveOperations{}))
}

func wrapKeyCloseError(description string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close %s: %w", description, err)
}

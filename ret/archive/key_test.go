package archive

import (
	"bytes"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testKEMName             = "ML-KEM-1024"
	testKDFName             = "HKDF-SHA512"
	testAEADName            = "AES-256-GCM"
	testPublicMaterialSize  = 1568
	testPrivateMaterialSize = 64
)

type testKeyDocument struct {
	Format      string `json:"format"`
	Role        string `json:"role"`
	KEM         string `json:"kem"`
	KDF         string `json:"kdf"`
	AEAD        string `json:"aead"`
	Material    string `json:"material"`
	Fingerprint string `json:"public_fingerprint"`
}

func TestGenerateKeyPairProducesOpaqueUsableKeys(t *testing.T) {
	publicKey, privateKey, err := GenerateKeyPair()
	require.NoError(t, err)

	dir := t.TempDir()
	publicPath := filepath.Join(dir, "public.json")
	privatePath := filepath.Join(dir, "private.json")
	require.NoError(t, WritePublicKey(publicPath, publicKey))
	require.NoError(t, WritePrivateKey(privatePath, privateKey))

	publicDocument := readTestKeyDocument(t, publicPath)
	privateDocument := readTestKeyDocument(t, privatePath)
	require.Equal(t, KeyFormat, publicDocument.Format)
	require.Equal(t, KeyFormat, privateDocument.Format)
	require.Equal(t, "public", publicDocument.Role)
	require.Equal(t, "private", privateDocument.Role)
	require.Equal(t, testKEMName, publicDocument.KEM)
	require.Equal(t, testKEMName, privateDocument.KEM)
	require.Equal(t, testKDFName, publicDocument.KDF)
	require.Equal(t, testKDFName, privateDocument.KDF)
	require.Equal(t, testAEADName, publicDocument.AEAD)
	require.Equal(t, testAEADName, privateDocument.AEAD)
	require.Equal(t, privateDocument.Fingerprint, publicDocument.Fingerprint)
	require.Regexp(t, `^[0-9a-f]{64}$`, publicDocument.Fingerprint)
	require.Len(t, decodeTestMaterial(t, publicDocument.Material), testPublicMaterialSize)
	require.Len(t, decodeTestMaterial(t, privateDocument.Material), testPrivateMaterialSize)
	require.Equal(t, publicDocument.Fingerprint, PublicFingerprint(publicKey))

	readPublic, err := ReadPublicKey(publicPath)
	require.NoError(t, err)
	require.Equal(t, publicDocument.Fingerprint, PublicFingerprint(readPublic))
	_, err = ReadPrivateKey(privatePath)
	require.NoError(t, err)
}

func TestKeyWrappersExposeNoRawHPKEKeys(t *testing.T) {
	for _, wrapper := range []reflect.Type{
		reflect.TypeFor[PublicKey](),
		reflect.TypeFor[PrivateKey](),
	} {
		for fieldIndex := range wrapper.NumField() {
			field := wrapper.Field(fieldIndex)
			require.Falsef(t, field.IsExported(), "%s field %s is exported", wrapper, field.Name)
			require.NotContains(t, field.Type.String(), "hpke")
			require.NotEqual(t, "crypto/hpke", field.Type.PkgPath())
		}
		for methodIndex := range wrapper.NumMethod() {
			method := wrapper.Method(methodIndex)
			require.NotContains(t, method.Type.String(), "hpke")
		}
	}
}

func TestPublicFingerprintIsLowercaseSHA256OfPublicMaterial(t *testing.T) {
	document, _, publicBytes := newTestKeyDocuments(t)
	path := writeTestKeyDocument(t, document)

	key, err := ReadPublicKey(path)
	require.NoError(t, err)

	digest := sha256.Sum256(publicBytes)
	require.Equal(t, hex.EncodeToString(digest[:]), PublicFingerprint(key))
	require.Equal(t, PublicFingerprint(key), PublicFingerprint(key))
	require.Empty(t, PublicFingerprint(PublicKey{}))
}

func TestReadKeysRejectMalformedDocuments(t *testing.T) {
	publicDocument, privateDocument, _ := newTestKeyDocuments(t)
	tests := []struct {
		name    string
		private bool
		mutate  func(*testKeyDocument)
		match   string
	}{
		{name: "retriever format", mutate: func(document *testKeyDocument) {
			document.Format = "retriever-hpke-key-v1"
		}, match: KeyFormat},
		{name: "empty format", mutate: func(document *testKeyDocument) {
			document.Format = ""
		}, match: KeyFormat},
		{name: "wrong public role", mutate: func(document *testKeyDocument) {
			document.Role = "private"
		}, match: "public"},
		{name: "wrong private role", private: true, mutate: func(document *testKeyDocument) {
			document.Role = "public"
		}, match: "private"},
		{name: "wrong KEM", mutate: func(document *testKeyDocument) {
			document.KEM = "ML-KEM-768"
		}, match: testKEMName},
		{name: "wrong KDF", mutate: func(document *testKeyDocument) {
			document.KDF = "HKDF-SHA256"
		}, match: testKDFName},
		{name: "wrong AEAD", mutate: func(document *testKeyDocument) {
			document.AEAD = "AES-128-GCM"
		}, match: testAEADName},
		{name: "empty material", mutate: func(document *testKeyDocument) {
			document.Material = ""
		}, match: "material"},
		{name: "invalid base64", mutate: func(document *testKeyDocument) {
			document.Material = "not base64!"
		}, match: "base64"},
		{name: "noncanonical base64", mutate: func(document *testKeyDocument) {
			document.Material = document.Material[:8] + "\n" + document.Material[8:]
		}, match: "base64"},
		{name: "short public material", mutate: func(document *testKeyDocument) {
			document.Material = base64.StdEncoding.EncodeToString(make([]byte, testPublicMaterialSize-1))
		}, match: "1568"},
		{name: "long public material", mutate: func(document *testKeyDocument) {
			document.Material = base64.StdEncoding.EncodeToString(make([]byte, testPublicMaterialSize+1))
		}, match: "1568"},
		{name: "short private material", private: true, mutate: func(document *testKeyDocument) {
			document.Material = base64.StdEncoding.EncodeToString(make([]byte, testPrivateMaterialSize-1))
		}, match: "64"},
		{name: "long private material", private: true, mutate: func(document *testKeyDocument) {
			document.Material = base64.StdEncoding.EncodeToString(make([]byte, testPrivateMaterialSize+1))
		}, match: "64"},
		{name: "empty fingerprint", mutate: func(document *testKeyDocument) {
			document.Fingerprint = ""
		}, match: "fingerprint"},
		{name: "uppercase fingerprint", mutate: func(document *testKeyDocument) {
			document.Fingerprint = strings.ToUpper(document.Fingerprint)
		}, match: "fingerprint"},
		{name: "wrong fingerprint", mutate: func(document *testKeyDocument) {
			document.Fingerprint = strings.Repeat("0", 64)
		}, match: "fingerprint"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := publicDocument
			if test.private {
				document = privateDocument
			}
			test.mutate(&document)
			path := writeTestKeyDocument(t, document)

			var err error
			if test.private {
				_, err = ReadPrivateKey(path)
			} else {
				_, err = ReadPublicKey(path)
			}
			require.ErrorContains(t, err, test.match)
		})
	}
}

func TestReadKeysRejectInvalidJSONUnknownFieldsAndTrailingValues(t *testing.T) {
	publicDocument, _, _ := newTestKeyDocuments(t)
	valid, err := json.Marshal(publicDocument)
	require.NoError(t, err)

	tests := []struct {
		name    string
		payload string
		match   string
	}{
		{name: "invalid JSON", payload: `{`, match: "JSON"},
		{name: "unknown field", payload: strings.TrimSuffix(string(valid), "}") + `,"extra":true}`, match: "unknown field"},
		{name: "trailing value", payload: string(valid) + "\n{}", match: "trailing"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key.json")
			require.NoError(t, os.WriteFile(path, []byte(test.payload), 0o600))
			_, err := ReadPublicKey(path)
			require.ErrorContains(t, err, test.match)
		})
	}
}

func TestReadPublicKeyRequiresEachExactLowercaseFieldOnce(t *testing.T) {
	publicDocument, _, _ := newTestKeyDocuments(t)
	valid, err := json.Marshal(publicDocument)
	require.NoError(t, err)
	fields := []string{
		"format",
		"role",
		"kem",
		"kdf",
		"aead",
		"material",
		"public_fingerprint",
	}

	for _, field := range fields {
		t.Run(field+"/case_variant", func(t *testing.T) {
			payload := keyJSONWithCaseVariantField(t, valid, field)
			_, err := ReadPublicKey(writeTestKeyPayload(t, payload))
			require.ErrorContains(t, err, "exact lowercase")
		})
		t.Run(field+"/attacker_duplicate_first", func(t *testing.T) {
			payload := keyJSONWithDuplicateField(t, valid, field, true)
			_, err := ReadPublicKey(writeTestKeyPayload(t, payload))
			require.ErrorContains(t, err, "duplicate")
		})
		t.Run(field+"/attacker_duplicate_last", func(t *testing.T) {
			payload := keyJSONWithDuplicateField(t, valid, field, false)
			_, err := ReadPublicKey(writeTestKeyPayload(t, payload))
			require.ErrorContains(t, err, "duplicate")
		})
	}
}

func TestReadPublicKeyRejectsStructurallyInvalidPublicMaterial(t *testing.T) {
	document, _, _ := newTestKeyDocuments(t)
	material := make([]byte, testPublicMaterialSize)
	for index := range material {
		material[index] = 0xff
	}
	document.Material = base64.StdEncoding.EncodeToString(material)
	digest := sha256.Sum256(material)
	document.Fingerprint = hex.EncodeToString(digest[:])

	_, err := ReadPublicKey(writeTestKeyDocument(t, document))
	require.ErrorContains(t, err, "public key material")
}

func TestKeyWritersUseRequiredModesAndRefuseOverwrite(t *testing.T) {
	publicDocument, privateDocument, _ := newTestKeyDocuments(t)
	inputPublic, err := ReadPublicKey(writeTestKeyDocument(t, publicDocument))
	require.NoError(t, err)
	inputPrivate, err := ReadPrivateKey(writeTestKeyDocument(t, privateDocument))
	require.NoError(t, err)

	dir := t.TempDir()
	publicPath := filepath.Join(dir, "nested", "public.json")
	privatePath := filepath.Join(dir, "nested", "private.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(publicPath), 0o755))
	require.NoError(t, WritePublicKey(publicPath, inputPublic))
	require.NoError(t, WritePrivateKey(privatePath, inputPrivate))

	publicInfo, err := os.Stat(publicPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), publicInfo.Mode().Perm())
	privateInfo, err := os.Stat(privatePath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), privateInfo.Mode().Perm())

	publicBefore, err := os.ReadFile(publicPath)
	require.NoError(t, err)
	privateBefore, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	require.Error(t, WritePublicKey(publicPath, inputPublic))
	require.Error(t, WritePrivateKey(privatePath, inputPrivate))
	publicAfter, err := os.ReadFile(publicPath)
	require.NoError(t, err)
	privateAfter, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	require.Equal(t, publicBefore, publicAfter)
	require.Equal(t, privateBefore, privateAfter)
}

func TestKeyReadersAndWritersRejectEmptyInputs(t *testing.T) {
	_, err := ReadPublicKey("")
	require.ErrorContains(t, err, "path")
	_, err = ReadPrivateKey("")
	require.ErrorContains(t, err, "path")

	publicPath := filepath.Join(t.TempDir(), "public.json")
	require.ErrorContains(t, WritePublicKey(publicPath, PublicKey{}), "public key")
	_, err = os.Stat(publicPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	privatePath := filepath.Join(t.TempDir(), "private.json")
	require.ErrorContains(t, WritePrivateKey(privatePath, PrivateKey{}), "private key")
	_, err = os.Stat(privatePath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPrivateKeyRollbackPreservesReplacement(t *testing.T) {
	// Break caught: deleting a replacement installed at the private-key path
	// when public-key publication later requires rollback.
	_, private, err := GenerateKeyPair()
	require.NoError(t, err)
	privatePath := filepath.Join(t.TempDir(), "private.json")
	owned, err := writePrivateKeyOwned(privatePath, private)
	require.NoError(t, err)
	require.NoError(t, os.Remove(privatePath))
	require.NoError(t, os.WriteFile(privatePath, []byte("preserve replacement"), 0o600))

	err = owned.remove(archiveOperations{})

	require.ErrorContains(t, err, "ownership cleanup")
	require.Equal(t, []byte("preserve replacement"), mustReadArchiveTestFile(t, privatePath))
}

func TestPrivateKeyRollbackSanitizesQuarantinedShell(t *testing.T) {
	public, private, err := GenerateKeyPair()
	require.NoError(t, err)
	parent := t.TempDir()
	privatePath := filepath.Join(parent, "private.json")
	publicPath := filepath.Join(parent, "public.json")
	require.NoError(t, os.WriteFile(publicPath, []byte("preserve public"), 0o600))

	err = WriteKeyPair(privatePath, private, publicPath, public)

	require.ErrorContains(t, err, "create key document")
	require.ErrorContains(t, err, "ownership cleanup")
	require.NoFileExists(t, privatePath)
	require.Equal(t, []byte("preserve public"), mustReadArchiveTestFile(t, publicPath))
	quarantines := archiveCleanupQuarantinePaths(t, parent)
	require.Len(t, quarantines, 1)
	info, statErr := os.Stat(quarantines[0])
	require.NoError(t, statErr)
	require.True(t, info.Mode().IsRegular())
	require.Zero(t, info.Size())
}

func TestPrivateKeyRollbackPreservesOriginalWhenSanitizationFails(t *testing.T) {
	// Break caught: moving a private-key file to quarantine after its retained
	// handle could not be truncated or synced successfully.
	tests := []struct {
		name        string
		operations  archiveOperations
		wantPayload bool
		match       string
	}{
		{
			name: "truncate",
			operations: archiveOperations{
				truncateOwnedFile: func(_ *os.File, _ int64) error {
					return fmt.Errorf("injected truncate failure")
				},
			},
			wantPayload: true,
			match:       "injected truncate failure",
		},
		{
			name: "sync",
			operations: archiveOperations{
				syncOwnedFile: func(_ *os.File) error {
					return fmt.Errorf("injected sync failure")
				},
			},
			match: "injected sync failure",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			public, private, err := GenerateKeyPair()
			require.NoError(t, err)
			parent := t.TempDir()
			privatePath := filepath.Join(parent, "private.json")
			publicPath := filepath.Join(parent, "public.json")
			require.NoError(t, os.WriteFile(publicPath, []byte("preserve public"), 0o600))

			err = writeKeyPair(
				privatePath,
				private,
				publicPath,
				public,
				test.operations,
			)

			require.ErrorContains(t, err, test.match)
			require.ErrorContains(t, err, "ownership cleanup")
			privatePayload := mustReadArchiveTestFile(t, privatePath)
			if test.wantPayload {
				require.NotEmpty(t, privatePayload)
			} else {
				require.Empty(t, privatePayload)
			}
			require.Equal(t, []byte("preserve public"), mustReadArchiveTestFile(t, publicPath))
			require.Empty(t, archiveCleanupQuarantinePaths(t, parent))
		})
	}
}

func newTestKeyDocuments(t *testing.T) (testKeyDocument, testKeyDocument, []byte) {
	t.Helper()
	privateKey, err := hpke.MLKEM1024().GenerateKey()
	require.NoError(t, err)
	privateBytes, err := privateKey.Bytes()
	require.NoError(t, err)
	publicBytes := privateKey.PublicKey().Bytes()
	require.Len(t, publicBytes, testPublicMaterialSize)
	require.Len(t, privateBytes, testPrivateMaterialSize)

	digest := sha256.Sum256(publicBytes)
	fingerprint := hex.EncodeToString(digest[:])
	base := testKeyDocument{
		Format:      KeyFormat,
		KEM:         testKEMName,
		KDF:         testKDFName,
		AEAD:        testAEADName,
		Fingerprint: fingerprint,
	}
	publicDocument := base
	publicDocument.Role = "public"
	publicDocument.Material = base64.StdEncoding.EncodeToString(publicBytes)
	privateDocument := base
	privateDocument.Role = "private"
	privateDocument.Material = base64.StdEncoding.EncodeToString(privateBytes)
	return publicDocument, privateDocument, publicBytes
}

func writeTestKeyDocument(t *testing.T, document testKeyDocument) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key.json")
	payload, err := json.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	return path
}

func readTestKeyDocument(t *testing.T, path string) testKeyDocument {
	t.Helper()
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var document testKeyDocument
	require.NoError(t, json.Unmarshal(payload, &document))
	return document
}

func decodeTestMaterial(t *testing.T, value string) []byte {
	t.Helper()
	material, err := base64.StdEncoding.DecodeString(value)
	require.NoError(t, err)
	return material
}

func writeTestKeyPayload(t *testing.T, payload []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key.json")
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	return path
}

func keyJSONWithCaseVariantField(t *testing.T, valid []byte, field string) []byte {
	t.Helper()
	needle := []byte(fmt.Sprintf("%q:", field))
	replacement := []byte(fmt.Sprintf("%q:", strings.ToUpper(field)))
	payload := bytes.Replace(valid, needle, replacement, 1)
	require.NotEqual(t, valid, payload)
	return payload
}

func keyJSONWithDuplicateField(t *testing.T, valid []byte, field string, attackerFirst bool) []byte {
	t.Helper()
	require.True(t, bytes.HasPrefix(valid, []byte("{")))
	require.True(t, bytes.HasSuffix(valid, []byte("}")))
	attacker := []byte(fmt.Sprintf("%q:%q", field, "attacker-controlled"))
	if attackerFirst {
		payload := make([]byte, 0, len(valid)+len(attacker)+1)
		payload = append(payload, '{')
		payload = append(payload, attacker...)
		payload = append(payload, ',')
		return append(payload, valid[1:]...)
	}

	payload := append([]byte(nil), valid[:len(valid)-1]...)
	payload = append(payload, ',')
	payload = append(payload, attacker...)
	return append(payload, '}')
}

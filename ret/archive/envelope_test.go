package archive

import (
	"bytes"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testEnvelopeMagic           = "RET-PQ-ARCHIVE-v1"
	testEnvelopeHPKEInfo        = "ret/archive/hpke/v1"
	testEnvelopeFrameSize       = 1024 * 1024
	testEnvelopeFrameHeaderSize = 13
	testEnvelopeAEADOverhead    = 16

	testEnvelopeDataFrame  byte = 0
	testEnvelopeFinalFrame byte = 1
)

type testEnvelopeHeader struct {
	Format          string `json:"format"`
	KEM             string `json:"kem"`
	KDF             string `json:"kdf"`
	AEAD            string `json:"aead"`
	EncapsulatedKey string `json:"encapsulated_key"`
	FrameSize       int    `json:"frame_size"`
}

type testEnvelopeFrame struct {
	start           int
	end             int
	ciphertextStart int
	frameType       byte
	sequence        uint64
	ciphertextSize  uint32
}

func TestEnvelopeRoundTripAcrossFrames(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	for _, size := range []int{
		0,
		1,
		testEnvelopeFrameSize - 1,
		testEnvelopeFrameSize,
		testEnvelopeFrameSize + 1,
		2*testEnvelopeFrameSize + 31,
	} {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			plaintext := testPlaintext(size)
			ciphertext := encryptTestEnvelope(t, recipient, plaintext)

			decrypted, err := decryptTestEnvelope(identity, ciphertext)
			require.NoError(t, err)
			require.Equal(t, plaintext, decrypted)
		})
	}
}

func TestEnvelopeHeaderUsesExactFormatSuiteAndBound(t *testing.T) {
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)
	secret := []byte("header-must-not-contain-this-plaintext")
	ciphertext := encryptTestEnvelope(t, recipient, secret)

	headerBytes, header, frames := parseTestEnvelope(t, ciphertext)
	require.Equal(t, EnvelopeFormat, header.Format)
	require.Equal(t, testKEMName, header.KEM)
	require.Equal(t, testKDFName, header.KDF)
	require.Equal(t, testAEADName, header.AEAD)
	require.Equal(t, testEnvelopeFrameSize, header.FrameSize)
	require.NotContains(t, string(headerBytes), string(secret))
	require.Len(t, decodeTestMaterial(t, header.EncapsulatedKey), testPublicMaterialSize)
	require.Len(t, frames, 2)
	require.Equal(t, testEnvelopeDataFrame, frames[0].frameType)
	require.Equal(t, uint64(0), frames[0].sequence)
	require.LessOrEqual(t, frames[0].ciphertextSize, uint32(testEnvelopeFrameSize+testEnvelopeAEADOverhead))
	require.Equal(t, testEnvelopeFinalFrame, frames[1].frameType)
	require.Equal(t, uint64(1), frames[1].sequence)
	require.Equal(t, uint32(testEnvelopeAEADOverhead), frames[1].ciphertextSize)
}

func TestEnvelopeRejectsLegacyMagicAndFormat(t *testing.T) {
	_, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	_, err = newDecryptReader(bytes.NewReader([]byte("RTRV-PQ-ARCHIVE-v1")), identity)
	require.ErrorContains(t, err, "magic")

	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, []byte("payload"))
	legacyFormat := rewriteTestEnvelopeHeader(t, ciphertext, func(header *testEnvelopeHeader) {
		header.Format = "retriever-encrypted-tar-v1"
	})
	_, err = decryptTestEnvelope(identity, legacyFormat)
	require.ErrorContains(t, err, EnvelopeFormat)
}

func TestEnvelopeRejectsHeaderValidationFailures(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, []byte("payload"))

	tests := []struct {
		name   string
		mutate func(*testEnvelopeHeader)
		match  string
	}{
		{name: "wrong KEM", mutate: func(header *testEnvelopeHeader) {
			header.KEM = "ML-KEM-768"
		}, match: testKEMName},
		{name: "wrong KDF", mutate: func(header *testEnvelopeHeader) {
			header.KDF = "HKDF-SHA256"
		}, match: testKDFName},
		{name: "wrong AEAD", mutate: func(header *testEnvelopeHeader) {
			header.AEAD = "AES-128-GCM"
		}, match: testAEADName},
		{name: "wrong frame size", mutate: func(header *testEnvelopeHeader) {
			header.FrameSize++
		}, match: "frame size"},
		{name: "empty encapsulation", mutate: func(header *testEnvelopeHeader) {
			header.EncapsulatedKey = ""
		}, match: "encapsulated key"},
		{name: "invalid encapsulation base64", mutate: func(header *testEnvelopeHeader) {
			header.EncapsulatedKey = "not base64!"
		}, match: "base64"},
		{name: "noncanonical encapsulation base64", mutate: func(header *testEnvelopeHeader) {
			header.EncapsulatedKey = header.EncapsulatedKey[:8] + "\n" + header.EncapsulatedKey[8:]
		}, match: "base64"},
		{name: "short encapsulation", mutate: func(header *testEnvelopeHeader) {
			header.EncapsulatedKey = base64.StdEncoding.EncodeToString(make([]byte, testPublicMaterialSize-1))
		}, match: "1568"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tampered := rewriteTestEnvelopeHeader(t, ciphertext, test.mutate)
			_, err := decryptTestEnvelope(identity, tampered)
			require.ErrorContains(t, err, test.match)
		})
	}
}

func TestEnvelopeRejectsUnknownAndTrailingHeaderJSON(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, []byte("payload"))
	headerBytes, _, _ := parseTestEnvelope(t, ciphertext)

	unknown := append(append([]byte(nil), bytes.TrimSuffix(headerBytes, []byte("}"))...), []byte(`,"extra":true}`)...)
	_, err = decryptTestEnvelope(identity, replaceTestEnvelopeHeader(t, ciphertext, unknown))
	require.ErrorContains(t, err, "unknown field")

	trailing := append(append([]byte(nil), headerBytes...), []byte(`{}`)...)
	_, err = decryptTestEnvelope(identity, replaceTestEnvelopeHeader(t, ciphertext, trailing))
	require.ErrorContains(t, err, "trailing")
}

func TestEnvelopeHeaderRequiresEachExactLowercaseFieldOnce(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, []byte("payload"))
	validHeader, _, _ := parseTestEnvelope(t, ciphertext)
	fields := []string{
		"format",
		"kem",
		"kdf",
		"aead",
		"encapsulated_key",
		"frame_size",
	}

	for _, field := range fields {
		t.Run(field+"/case_variant", func(t *testing.T) {
			header := envelopeJSONWithCaseVariantField(t, validHeader, field)
			_, err := newDecryptReader(
				bytes.NewReader(replaceTestEnvelopeHeader(t, ciphertext, header)),
				identity,
			)
			require.ErrorContains(t, err, "exact lowercase")
		})
		t.Run(field+"/attacker_duplicate_first", func(t *testing.T) {
			header := envelopeJSONWithDuplicateField(t, validHeader, field, true)
			_, err := newDecryptReader(
				bytes.NewReader(replaceTestEnvelopeHeader(t, ciphertext, header)),
				identity,
			)
			require.ErrorContains(t, err, "duplicate")
		})
		t.Run(field+"/attacker_duplicate_last", func(t *testing.T) {
			header := envelopeJSONWithDuplicateField(t, validHeader, field, false)
			_, err := newDecryptReader(
				bytes.NewReader(replaceTestEnvelopeHeader(t, ciphertext, header)),
				identity,
			)
			require.ErrorContains(t, err, "duplicate")
		})
	}
}

func TestEnvelopeAuthenticatesExactHeaderBytesAndEncapsulation(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, []byte("payload"))
	headerBytes, _, _ := parseTestEnvelope(t, ciphertext)

	whitespaceTamper := append(append([]byte(nil), headerBytes...), ' ')
	plaintext, err := decryptTestEnvelope(identity, replaceTestEnvelopeHeader(t, ciphertext, whitespaceTamper))
	require.Error(t, err)
	require.Empty(t, plaintext)

	encapsulationTamper := rewriteTestEnvelopeHeader(t, ciphertext, func(header *testEnvelopeHeader) {
		replacement := byte('A')
		if header.EncapsulatedKey[0] == replacement {
			replacement = 'B'
		}
		header.EncapsulatedKey = string(replacement) + header.EncapsulatedKey[1:]
	})
	plaintext, err = decryptTestEnvelope(identity, encapsulationTamper)
	require.Error(t, err)
	require.Empty(t, plaintext)
}

func TestEnvelopeRejectsWrongIdentity(t *testing.T) {
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)
	_, wrongIdentity, err := GenerateKeyPair()
	require.NoError(t, err)

	plaintext, err := decryptTestEnvelope(wrongIdentity, encryptTestEnvelope(t, recipient, []byte("payload")))
	require.Error(t, err)
	require.Empty(t, plaintext)
}

func TestEnvelopeRejectsCiphertextTamperWithoutReportingCurrentFrame(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	plaintext := testPlaintext(2*testEnvelopeFrameSize + 17)
	ciphertext := encryptTestEnvelope(t, recipient, plaintext)
	_, _, frames := parseTestEnvelope(t, ciphertext)
	require.Len(t, frames, 4)

	firstTampered := append([]byte(nil), ciphertext...)
	firstTampered[frames[0].ciphertextStart] ^= 0x01
	decrypted, err := decryptTestEnvelope(identity, firstTampered)
	require.Error(t, err)
	require.Empty(t, decrypted)

	secondTampered := append([]byte(nil), ciphertext...)
	secondTampered[frames[1].ciphertextStart+11] ^= 0x01
	decrypted, err = decryptTestEnvelope(identity, secondTampered)
	require.Error(t, err)
	require.Equal(t, plaintext[:testEnvelopeFrameSize], decrypted)
}

func TestEnvelopeRejectsReorderedAndRepeatedFrameNumbers(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, testPlaintext(2*testEnvelopeFrameSize+1))
	_, _, frames := parseTestEnvelope(t, ciphertext)
	require.Len(t, frames, 4)

	reordered := append([]byte(nil), ciphertext[:frames[0].start]...)
	reordered = append(reordered, ciphertext[frames[1].start:frames[1].end]...)
	reordered = append(reordered, ciphertext[frames[0].start:frames[0].end]...)
	reordered = append(reordered, ciphertext[frames[2].start:]...)
	decrypted, err := decryptTestEnvelope(identity, reordered)
	require.ErrorContains(t, err, "sequence")
	require.Empty(t, decrypted)

	repeated := append([]byte(nil), ciphertext...)
	binary.BigEndian.PutUint64(repeated[frames[1].start+1:], frames[0].sequence)
	decrypted, err = decryptTestEnvelope(identity, repeated)
	require.ErrorContains(t, err, "sequence")
	require.Equal(t, testPlaintext(testEnvelopeFrameSize), decrypted)
}

func TestEncryptWriterReservesLastSequenceForFinalFrame(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	var destination bytes.Buffer
	writeCloser, err := newEncryptWriter(&destination, recipient)
	require.NoError(t, err)
	writer := writeCloser.(*encryptWriter)
	writer.sequence = math.MaxUint64 - 2

	plaintext := []byte("last-data-frame")
	n, err := writer.Write(plaintext)
	require.NoError(t, err)
	require.Equal(t, len(plaintext), n)
	require.Equal(t, uint64(math.MaxUint64-1), writer.sequence)
	require.NoError(t, writer.Close())
	require.Equal(t, uint64(math.MaxUint64), writer.sequence)

	_, _, frames := parseTestEnvelope(t, destination.Bytes())
	require.Len(t, frames, 2)
	require.Equal(t, uint64(math.MaxUint64-2), frames[0].sequence)
	require.Equal(t, testEnvelopeDataFrame, frames[0].frameType)
	require.Equal(t, uint64(math.MaxUint64-1), frames[1].sequence)
	require.Equal(t, testEnvelopeFinalFrame, frames[1].frameType)

	readCloser, err := newDecryptReader(bytes.NewReader(destination.Bytes()), identity)
	require.NoError(t, err)
	reader := readCloser.(*decryptReader)
	reader.sequence = math.MaxUint64 - 2
	decrypted, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
	require.NoError(t, reader.Close())
	require.Equal(t, uint64(math.MaxUint64), reader.sequence)
}

func TestEncryptWriterRejectsDataWhenOnlyFinalSequenceRemainsBeforeSeal(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	var destination bytes.Buffer
	writeCloser, err := newEncryptWriter(&destination, recipient)
	require.NoError(t, err)
	writer := writeCloser.(*encryptWriter)
	writer.sequence = math.MaxUint64 - 1
	before := append([]byte(nil), destination.Bytes()...)

	n, err := writer.Write([]byte("must-not-be-consumed"))
	require.ErrorContains(t, err, "sequence exhausted")
	require.Zero(t, n)
	require.Equal(t, before, destination.Bytes())
	require.Equal(t, uint64(math.MaxUint64-1), writer.sequence)

	// A retry after clearing only the sticky API error proves the rejected call
	// did not consume an HPKE nonce before the guard fired.
	writer.writeErr = nil
	writer.sequence = math.MaxUint64 - 2
	plaintext := []byte("nonce-zero-remains-unused")
	n, err = writer.Write(plaintext)
	require.NoError(t, err)
	require.Equal(t, len(plaintext), n)
	require.NoError(t, writer.Close())

	readCloser, err := newDecryptReader(bytes.NewReader(destination.Bytes()), identity)
	require.NoError(t, err)
	reader := readCloser.(*decryptReader)
	reader.sequence = math.MaxUint64 - 2
	decrypted, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
	require.NoError(t, reader.Close())
}

func TestEncryptWriterRejectsFinalAtExhaustedSequenceBeforeSeal(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	var destination bytes.Buffer
	writeCloser, err := newEncryptWriter(&destination, recipient)
	require.NoError(t, err)
	writer := writeCloser.(*encryptWriter)
	writer.sequence = math.MaxUint64
	before := append([]byte(nil), destination.Bytes()...)

	require.ErrorContains(t, writer.Close(), "sequence exhausted")
	require.Equal(t, before, destination.Bytes())
	require.Equal(t, uint64(math.MaxUint64), writer.sequence)

	// Re-running only the close state at the last safe final sequence proves
	// the rejected close did not consume the HPKE nonce.
	writer.closed = false
	writer.closeErr = nil
	writer.sequence = math.MaxUint64 - 1
	require.NoError(t, writer.Close())

	readCloser, err := newDecryptReader(bytes.NewReader(destination.Bytes()), identity)
	require.NoError(t, err)
	reader := readCloser.(*decryptReader)
	reader.sequence = math.MaxUint64 - 1
	decrypted, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Empty(t, decrypted)
	require.NoError(t, reader.Close())
}

func TestDecryptReaderRejectsDataWhenOnlyFinalSequenceRemainsBeforeOpen(t *testing.T) {
	wire, identity := buildTestEnvelopeWithSingleFrame(
		t,
		math.MaxUint64-1,
		testEnvelopeDataFrame,
		[]byte("must-not-be-reported"),
	)
	source := &countingReader{reader: bytes.NewReader(wire)}
	readCloser, err := newDecryptReader(source, identity)
	require.NoError(t, err)
	reader := readCloser.(*decryptReader)
	reader.sequence = math.MaxUint64 - 1
	beforeFrame := source.bytesRead
	output := []byte{0xa5}

	n, err := reader.Read(output)
	require.ErrorContains(t, err, "sequence exhausted")
	require.Zero(t, n)
	require.Equal(t, []byte{0xa5}, output)
	require.Equal(t, testEnvelopeFrameHeaderSize, source.bytesRead-beforeFrame)
	require.Equal(t, uint64(math.MaxUint64-1), reader.sequence)
	require.Nil(t, reader.plaintext)
	afterRead := source.bytesRead
	require.ErrorContains(t, reader.Close(), "sequence exhausted")
	require.Equal(t, afterRead, source.bytesRead)
}

func TestDecryptReaderRejectsFinalAtExhaustedSequenceBeforeOpen(t *testing.T) {
	wire, identity := buildTestEnvelopeWithSingleFrame(
		t,
		math.MaxUint64,
		testEnvelopeFinalFrame,
		nil,
	)
	source := &countingReader{reader: bytes.NewReader(wire)}
	readCloser, err := newDecryptReader(source, identity)
	require.NoError(t, err)
	reader := readCloser.(*decryptReader)
	reader.sequence = math.MaxUint64
	beforeFrame := source.bytesRead
	output := []byte{0xa5}

	n, err := reader.Read(output)
	require.ErrorContains(t, err, "sequence exhausted")
	require.Zero(t, n)
	require.Equal(t, []byte{0xa5}, output)
	require.Equal(t, testEnvelopeFrameHeaderSize, source.bytesRead-beforeFrame)
	require.Equal(t, uint64(math.MaxUint64), reader.sequence)
	require.Nil(t, reader.plaintext)
}

func TestEnvelopeRequiresOneTerminalAuthenticatedFinalFrame(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	plaintext := []byte("payload")
	ciphertext := encryptTestEnvelope(t, recipient, plaintext)
	_, _, frames := parseTestEnvelope(t, ciphertext)
	require.Len(t, frames, 2)

	missingFinal := append([]byte(nil), ciphertext[:frames[1].start]...)
	decrypted, err := decryptTestEnvelope(identity, missingFinal)
	require.ErrorContains(t, err, "final frame")
	require.Equal(t, plaintext, decrypted)

	duplicateFinal := append(append([]byte(nil), ciphertext...), ciphertext[frames[1].start:frames[1].end]...)
	decrypted, err = decryptTestEnvelope(identity, duplicateFinal)
	require.ErrorContains(t, err, "after final frame")
	require.Equal(t, plaintext, decrypted)

	dataAfterFinal := append(append([]byte(nil), ciphertext...), ciphertext[frames[0].start:frames[0].end]...)
	decrypted, err = decryptTestEnvelope(identity, dataAfterFinal)
	require.ErrorContains(t, err, "after final frame")
	require.Equal(t, plaintext, decrypted)
}

func TestEnvelopeRejectsAuthenticatedFinalFrameContainingPlaintext(t *testing.T) {
	wire, identity := buildTestEnvelopeWithFinalPlaintext(t, []byte("not empty"))

	plaintext, err := decryptTestEnvelope(identity, wire)
	require.ErrorContains(t, err, "final frame contained plaintext")
	require.Empty(t, plaintext)
}

func TestEnvelopeRejectsUnsupportedTypeAndOversizedFrameBeforeAllocation(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, []byte("payload"))
	_, _, frames := parseTestEnvelope(t, ciphertext)

	unsupported := append([]byte(nil), ciphertext...)
	unsupported[frames[0].start] = 2
	decrypted, err := decryptTestEnvelope(identity, unsupported)
	require.ErrorContains(t, err, "type")
	require.Empty(t, decrypted)

	oversized := append([]byte(nil), ciphertext...)
	binary.BigEndian.PutUint32(
		oversized[frames[0].start+9:],
		uint32(testEnvelopeFrameSize+testEnvelopeAEADOverhead+1),
	)
	decrypted, err = decryptTestEnvelope(identity, oversized)
	require.ErrorContains(t, err, "too large")
	require.Empty(t, decrypted)
}

func TestEnvelopeRejectsEveryTruncationOffset(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, nil)

	for cut := range len(ciphertext) {
		plaintext, err := decryptTestEnvelope(identity, ciphertext[:cut])
		if err == nil {
			t.Fatalf("truncation at byte %d of %d was accepted with plaintext %x", cut, len(ciphertext), plaintext)
		}
		require.Emptyf(t, plaintext, "truncation at byte %d reported plaintext", cut)
	}
}

func TestDecryptReaderCloseAuthenticatesUnreadRemainder(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	ciphertext := encryptTestEnvelope(t, recipient, testPlaintext(testEnvelopeFrameSize+31))

	reader, err := newDecryptReader(bytes.NewReader(ciphertext), identity)
	require.NoError(t, err)
	var one [1]byte
	n, err := reader.Read(one[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, reader.Close())
	require.ErrorIs(t, readOnce(reader), io.ErrClosedPipe)
	require.NoError(t, reader.Close())

	_, _, frames := parseTestEnvelope(t, ciphertext)
	truncated := ciphertext[:frames[len(frames)-1].start]
	reader, err = newDecryptReader(bytes.NewReader(truncated), identity)
	require.NoError(t, err)
	n, err = reader.Read(one[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.ErrorContains(t, reader.Close(), "final frame")
	require.ErrorContains(t, reader.Close(), "final frame")
}

func TestEncryptWriterCloseFinalizesOnceAndPreservesWriteFailure(t *testing.T) {
	recipient, _, err := GenerateKeyPair()
	require.NoError(t, err)

	var destination bytes.Buffer
	writer, err := newEncryptWriter(&destination, recipient)
	require.NoError(t, err)
	_, err = writer.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	sizeAfterClose := destination.Len()
	require.ErrorContains(t, writer.Close(), "closed")
	require.Equal(t, sizeAfterClose, destination.Len())
	_, err = writer.Write([]byte("more"))
	require.ErrorContains(t, err, "closed")

	emptyEnvelope := encryptTestEnvelope(t, recipient, nil)
	finalFailure := errors.New("final write failed")
	finalDestination := &quotaWriter{
		remaining: len(emptyEnvelope) - 1,
		failure:   finalFailure,
	}
	writer, err = newEncryptWriter(finalDestination, recipient)
	require.NoError(t, err)
	require.ErrorIs(t, writer.Close(), finalFailure)
	require.ErrorIs(t, writer.Close(), finalFailure)

	frameStart := testEnvelopeFrameStart(t, emptyEnvelope)
	dataFailure := errors.New("data write failed")
	dataDestination := &quotaWriter{
		remaining: frameStart + testEnvelopeFrameHeaderSize + 1,
		failure:   dataFailure,
	}
	writer, err = newEncryptWriter(dataDestination, recipient)
	require.NoError(t, err)
	n, err := writer.Write([]byte("payload"))
	require.ErrorIs(t, err, dataFailure)
	require.Zero(t, n)
	require.ErrorIs(t, writer.Close(), dataFailure)
}

func TestEnvelopeConstructorsRejectMissingInputs(t *testing.T) {
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)

	_, err = newEncryptWriter(nil, recipient)
	require.ErrorContains(t, err, "destination")
	_, err = newEncryptWriter(io.Discard, PublicKey{})
	require.ErrorContains(t, err, "recipient")
	_, err = newDecryptReader(nil, identity)
	require.ErrorContains(t, err, "source")
	_, err = newDecryptReader(bytes.NewReader(nil), PrivateKey{})
	require.ErrorContains(t, err, "identity")
}

func encryptTestEnvelope(t *testing.T, recipient PublicKey, plaintext []byte) []byte {
	t.Helper()
	var destination bytes.Buffer
	writer, err := newEncryptWriter(&destination, recipient)
	require.NoError(t, err)
	n, err := writer.Write(plaintext)
	require.NoError(t, err)
	require.Equal(t, len(plaintext), n)
	require.NoError(t, writer.Close())
	return append([]byte(nil), destination.Bytes()...)
}

func decryptTestEnvelope(identity PrivateKey, ciphertext []byte) ([]byte, error) {
	reader, err := newDecryptReader(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, err
	}
	plaintext, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	return plaintext, errors.Join(readErr, closeErr)
}

func parseTestEnvelope(t *testing.T, ciphertext []byte) ([]byte, testEnvelopeHeader, []testEnvelopeFrame) {
	t.Helper()
	require.GreaterOrEqual(t, len(ciphertext), len(testEnvelopeMagic)+4)
	require.Equal(t, testEnvelopeMagic, string(ciphertext[:len(testEnvelopeMagic)]))
	headerSize := int(binary.BigEndian.Uint32(ciphertext[len(testEnvelopeMagic):]))
	headerStart := len(testEnvelopeMagic) + 4
	frameStart := headerStart + headerSize
	require.LessOrEqual(t, frameStart, len(ciphertext))
	headerBytes := append([]byte(nil), ciphertext[headerStart:frameStart]...)
	var header testEnvelopeHeader
	require.NoError(t, json.Unmarshal(headerBytes, &header))

	var frames []testEnvelopeFrame
	for offset := frameStart; offset < len(ciphertext); {
		require.GreaterOrEqual(t, len(ciphertext)-offset, testEnvelopeFrameHeaderSize)
		frame := testEnvelopeFrame{
			start:          offset,
			frameType:      ciphertext[offset],
			sequence:       binary.BigEndian.Uint64(ciphertext[offset+1:]),
			ciphertextSize: binary.BigEndian.Uint32(ciphertext[offset+9:]),
		}
		frame.ciphertextStart = offset + testEnvelopeFrameHeaderSize
		frame.end = frame.ciphertextStart + int(frame.ciphertextSize)
		require.LessOrEqual(t, frame.end, len(ciphertext))
		frames = append(frames, frame)
		offset = frame.end
	}
	return headerBytes, header, frames
}

func testEnvelopeFrameStart(t *testing.T, ciphertext []byte) int {
	t.Helper()
	require.GreaterOrEqual(t, len(ciphertext), len(testEnvelopeMagic)+4)
	headerSize := int(binary.BigEndian.Uint32(ciphertext[len(testEnvelopeMagic):]))
	return len(testEnvelopeMagic) + 4 + headerSize
}

func envelopeJSONWithCaseVariantField(t *testing.T, valid []byte, field string) []byte {
	t.Helper()
	needle := []byte(fmt.Sprintf("%q:", field))
	replacement := []byte(fmt.Sprintf("%q:", strings.ToUpper(field)))
	payload := bytes.Replace(valid, needle, replacement, 1)
	require.NotEqual(t, valid, payload)
	return payload
}

func envelopeJSONWithDuplicateField(t *testing.T, valid []byte, field string, attackerFirst bool) []byte {
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

func rewriteTestEnvelopeHeader(
	t *testing.T,
	ciphertext []byte,
	mutate func(*testEnvelopeHeader),
) []byte {
	t.Helper()
	headerBytes, header, _ := parseTestEnvelope(t, ciphertext)
	mutate(&header)
	replacement, err := json.Marshal(header)
	require.NoError(t, err)
	require.NotEqual(t, headerBytes, replacement)
	return replaceTestEnvelopeHeader(t, ciphertext, replacement)
}

func replaceTestEnvelopeHeader(t *testing.T, ciphertext, replacement []byte) []byte {
	t.Helper()
	frameStart := testEnvelopeFrameStart(t, ciphertext)
	require.LessOrEqual(t, len(replacement), int(^uint32(0)))
	result := make([]byte, 0, len(testEnvelopeMagic)+4+len(replacement)+len(ciphertext)-frameStart)
	result = append(result, testEnvelopeMagic...)
	result = binary.BigEndian.AppendUint32(result, uint32(len(replacement)))
	result = append(result, replacement...)
	result = append(result, ciphertext[frameStart:]...)
	return result
}

func buildTestEnvelopeWithFinalPlaintext(t *testing.T, finalPlaintext []byte) ([]byte, PrivateKey) {
	return buildTestEnvelopeWithSingleFrame(t, 0, testEnvelopeFinalFrame, finalPlaintext)
}

func buildTestEnvelopeWithSingleFrame(
	t *testing.T,
	sequence uint64,
	frameType byte,
	plaintext []byte,
) ([]byte, PrivateKey) {
	t.Helper()
	rawPrivate, err := hpke.MLKEM1024().GenerateKey()
	require.NoError(t, err)
	rawPrivateBytes, err := rawPrivate.Bytes()
	require.NoError(t, err)
	rawPublic := rawPrivate.PublicKey()
	digest := sha256.Sum256(rawPublic.Bytes())
	privateDocument := testKeyDocument{
		Format:      KeyFormat,
		Role:        "private",
		KEM:         testKEMName,
		KDF:         testKDFName,
		AEAD:        testAEADName,
		Material:    base64.StdEncoding.EncodeToString(rawPrivateBytes),
		Fingerprint: hex.EncodeToString(digest[:]),
	}
	identity, err := ReadPrivateKey(writeTestKeyDocument(t, privateDocument))
	require.NoError(t, err)

	encapsulation, sender, err := hpke.NewSender(
		rawPublic,
		hpke.HKDFSHA512(),
		hpke.AES256GCM(),
		[]byte(testEnvelopeHPKEInfo),
	)
	require.NoError(t, err)
	header := testEnvelopeHeader{
		Format:          EnvelopeFormat,
		KEM:             testKEMName,
		KDF:             testKDFName,
		AEAD:            testAEADName,
		EncapsulatedKey: base64.StdEncoding.EncodeToString(encapsulation),
		FrameSize:       testEnvelopeFrameSize,
	}
	headerBytes, err := json.Marshal(header)
	require.NoError(t, err)
	headerDigest := sha256.Sum256(headerBytes)
	final := frameType == testEnvelopeFinalFrame
	ciphertext, err := sender.Seal(testFrameAAD(headerDigest[:], sequence, final), plaintext)
	require.NoError(t, err)

	wire := make([]byte, 0, len(testEnvelopeMagic)+4+len(headerBytes)+testEnvelopeFrameHeaderSize+len(ciphertext))
	wire = append(wire, testEnvelopeMagic...)
	wire = binary.BigEndian.AppendUint32(wire, uint32(len(headerBytes)))
	wire = append(wire, headerBytes...)
	wire = append(wire, frameType)
	wire = binary.BigEndian.AppendUint64(wire, sequence)
	wire = binary.BigEndian.AppendUint32(wire, uint32(len(ciphertext)))
	wire = append(wire, ciphertext...)
	return wire, identity
}

func testFrameAAD(headerDigest []byte, sequence uint64, final bool) []byte {
	aad := append([]byte(nil), headerDigest...)
	aad = binary.BigEndian.AppendUint64(aad, sequence)
	if final {
		return append(aad, 1)
	}
	return append(aad, 0)
}

func testPlaintext(size int) []byte {
	pattern := []byte("frame-data-0123456789abcdef")
	plaintext := bytes.Repeat(pattern, (size+len(pattern)-1)/len(pattern))
	return plaintext[:size]
}

func readOnce(reader io.Reader) error {
	var value [1]byte
	_, err := reader.Read(value[:])
	return err
}

type quotaWriter struct {
	buffer    bytes.Buffer
	remaining int
	failure   error
}

type countingReader struct {
	reader    io.Reader
	bytesRead int
}

func (s *countingReader) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	s.bytesRead += n
	return n, err
}

func (s *quotaWriter) Write(p []byte) (int, error) {
	if s.remaining == 0 {
		return 0, s.failure
	}
	if len(p) <= s.remaining {
		s.remaining -= len(p)
		return s.buffer.Write(p)
	}

	n, _ := s.buffer.Write(p[:s.remaining])
	s.remaining = 0
	return n, s.failure
}

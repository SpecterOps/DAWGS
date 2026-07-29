package archive

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
	"math"
)

const (
	envelopeMagic         = "RET-PQ-ARCHIVE-v1"
	envelopeHPKEInfo      = "ret/archive/hpke/v1"
	envelopeFrameSize     = 1024 * 1024
	maxEnvelopeHeaderSize = 8 * 1024

	frameHeaderSize = 13
	aeadOverhead    = 16

	dataFrame  byte = 0
	finalFrame byte = 1

	maxEnvelopeFrameSequence = math.MaxUint64 - 1
)

type envelopeHeader struct {
	Format          string `json:"format"`
	KEM             string `json:"kem"`
	KDF             string `json:"kdf"`
	AEAD            string `json:"aead"`
	EncapsulatedKey string `json:"encapsulated_key"`
	FrameSize       int    `json:"frame_size"`
}

type encryptWriter struct {
	destination  io.Writer
	sender       *hpke.Sender
	headerDigest [sha256.Size]byte
	sequence     uint64
	writeErr     error
	closeErr     error
	closed       bool
}

type decryptReader struct {
	source       io.Reader
	recipient    *hpke.Recipient
	headerDigest [sha256.Size]byte
	sequence     uint64
	plaintext    []byte
	terminalErr  error
	closeErr     error
	final        bool
	closed       bool
}

func newEncryptWriter(destination io.Writer, recipient PublicKey) (io.WriteCloser, error) {
	if destination == nil {
		return nil, fmt.Errorf("encryption destination is required")
	}
	if !recipient.valid {
		return nil, fmt.Errorf("recipient public key is required")
	}

	publicHPKEKey, err := archiveHPKESuite.kem.NewPublicKey(recipient.material[:])
	if err != nil {
		return nil, fmt.Errorf("parse recipient public key: %w", err)
	}
	encapsulatedKey, sender, err := hpke.NewSender(
		publicHPKEKey,
		archiveHPKESuite.kdf,
		archiveHPKESuite.aead,
		[]byte(envelopeHPKEInfo),
	)
	if err != nil {
		return nil, fmt.Errorf("create archive sender: %w", err)
	}
	if len(encapsulatedKey) != publicKeyMaterialSize {
		return nil, fmt.Errorf(
			"generated encapsulated key has length %d, want %d",
			len(encapsulatedKey),
			publicKeyMaterialSize,
		)
	}

	header := envelopeHeader{
		Format:          EnvelopeFormat,
		KEM:             kemName,
		KDF:             kdfName,
		AEAD:            aeadName,
		EncapsulatedKey: base64.StdEncoding.EncodeToString(encapsulatedKey),
		FrameSize:       envelopeFrameSize,
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode archive header: %w", err)
	}
	if len(headerBytes) == 0 || len(headerBytes) > maxEnvelopeHeaderSize {
		return nil, fmt.Errorf("archive header size %d is invalid", len(headerBytes))
	}

	if err := writeAll(destination, []byte(envelopeMagic)); err != nil {
		return nil, fmt.Errorf("write archive magic: %w", err)
	}
	var headerSize [4]byte
	binary.BigEndian.PutUint32(headerSize[:], uint32(len(headerBytes)))
	if err := writeAll(destination, headerSize[:]); err != nil {
		return nil, fmt.Errorf("write archive header size: %w", err)
	}
	if err := writeAll(destination, headerBytes); err != nil {
		return nil, fmt.Errorf("write archive header: %w", err)
	}

	return &encryptWriter{
		destination:  destination,
		sender:       sender,
		headerDigest: sha256.Sum256(headerBytes),
	}, nil
}

func (s *encryptWriter) Write(p []byte) (int, error) {
	if s.closed {
		return 0, fmt.Errorf("encrypted archive writer is closed")
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}

	written := 0
	for len(p) > 0 {
		chunkSize := min(len(p), envelopeFrameSize)
		if err := s.writeFrame(dataFrame, p[:chunkSize]); err != nil {
			s.writeErr = err
			return written, err
		}
		written += chunkSize
		p = p[chunkSize:]
	}
	return written, nil
}

func (s *encryptWriter) Close() error {
	if s.closed {
		if s.closeErr != nil {
			return s.closeErr
		}
		return fmt.Errorf("encrypted archive writer is already closed")
	}
	s.closed = true

	if s.writeErr != nil {
		s.closeErr = s.writeErr
		return s.closeErr
	}
	if err := s.writeFrame(finalFrame, nil); err != nil {
		s.closeErr = err
		return err
	}
	return nil
}

func (s *encryptWriter) writeFrame(frameType byte, plaintext []byte) error {
	final := frameType == finalFrame
	if err := validateFrameSequence(s.sequence, final); err != nil {
		return err
	}
	ciphertext, err := s.sender.Seal(frameAAD(s.headerDigest[:], s.sequence, final), plaintext)
	if err != nil {
		return fmt.Errorf("encrypt archive frame %d: %w", s.sequence, err)
	}
	if len(ciphertext) > envelopeFrameSize+aeadOverhead {
		return fmt.Errorf("encrypted archive frame %d is too large", s.sequence)
	}

	var header [frameHeaderSize]byte
	header[0] = frameType
	binary.BigEndian.PutUint64(header[1:9], s.sequence)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(ciphertext)))
	if err := writeAll(s.destination, header[:]); err != nil {
		return fmt.Errorf("write archive frame %d header: %w", s.sequence, err)
	}
	if err := writeAll(s.destination, ciphertext); err != nil {
		return fmt.Errorf("write archive frame %d ciphertext: %w", s.sequence, err)
	}

	s.sequence++
	return nil
}

func newDecryptReader(source io.Reader, identity PrivateKey) (io.ReadCloser, error) {
	if source == nil {
		return nil, fmt.Errorf("encrypted archive source is required")
	}
	if !identity.valid {
		return nil, fmt.Errorf("identity private key is required")
	}
	privateHPKEKey, err := archiveHPKESuite.kem.NewPrivateKey(identity.material[:])
	if err != nil {
		return nil, fmt.Errorf("parse identity private key: %w", err)
	}

	headerBytes, _, encapsulatedKey, err := readEnvelopeHeader(source)
	if err != nil {
		return nil, err
	}
	recipient, err := hpke.NewRecipient(
		encapsulatedKey,
		privateHPKEKey,
		archiveHPKESuite.kdf,
		archiveHPKESuite.aead,
		[]byte(envelopeHPKEInfo),
	)
	if err != nil {
		return nil, fmt.Errorf("create archive recipient: %w", err)
	}

	return &decryptReader{
		source:       source,
		recipient:    recipient,
		headerDigest: sha256.Sum256(headerBytes),
	}, nil
}

func (s *decryptReader) Read(p []byte) (int, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.terminalErr != nil {
		return 0, s.terminalErr
	}

	for len(s.plaintext) == 0 {
		if s.final {
			return 0, io.EOF
		}
		if err := s.readNextFrame(); err != nil {
			s.terminalErr = err
			return 0, err
		}
	}

	n := copy(p, s.plaintext)
	s.plaintext = s.plaintext[n:]
	return n, nil
}

func (s *decryptReader) Close() error {
	if s.closed {
		return s.closeErr
	}

	if s.terminalErr == nil {
		s.plaintext = nil
		for !s.final {
			if err := s.readNextFrame(); err != nil {
				s.terminalErr = err
				break
			}
			s.plaintext = nil
		}
	}
	s.closed = true
	s.closeErr = s.terminalErr
	return s.closeErr
}

func (s *decryptReader) readNextFrame() error {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(s.source, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("encrypted archive is missing its final frame")
		}
		return fmt.Errorf("read archive frame %d header: %w", s.sequence, err)
	}

	frameType := header[0]
	if frameType != dataFrame && frameType != finalFrame {
		return fmt.Errorf("archive frame %d has unsupported type %d", s.sequence, frameType)
	}
	sequence := binary.BigEndian.Uint64(header[1:9])
	if sequence != s.sequence {
		return fmt.Errorf("archive frame sequence is %d, want %d", sequence, s.sequence)
	}
	final := frameType == finalFrame
	if err := validateFrameSequence(s.sequence, final); err != nil {
		return err
	}
	ciphertextSize := binary.BigEndian.Uint32(header[9:13])
	if ciphertextSize < aeadOverhead {
		return fmt.Errorf("archive frame %d ciphertext is too short", s.sequence)
	}
	if ciphertextSize > envelopeFrameSize+aeadOverhead {
		return fmt.Errorf("archive frame %d is too large", s.sequence)
	}

	ciphertext := make([]byte, int(ciphertextSize))
	if _, err := io.ReadFull(s.source, ciphertext); err != nil {
		return fmt.Errorf("read archive frame %d ciphertext: %w", s.sequence, err)
	}
	plaintext, err := s.recipient.Open(frameAAD(s.headerDigest[:], s.sequence, final), ciphertext)
	if err != nil {
		return fmt.Errorf("authenticate archive frame %d: %w", s.sequence, err)
	}
	if len(plaintext) > envelopeFrameSize {
		return fmt.Errorf("archive frame %d plaintext is too large", s.sequence)
	}
	s.sequence++

	if final {
		if len(plaintext) != 0 {
			return fmt.Errorf("authenticated final frame contained plaintext")
		}
		if err := requireEnvelopeEOF(s.source); err != nil {
			return err
		}
		s.final = true
		return nil
	}
	if len(plaintext) == 0 {
		return fmt.Errorf("authenticated data frame %d contained no plaintext", sequence)
	}

	s.plaintext = plaintext
	return nil
}

func validateFrameSequence(sequence uint64, final bool) error {
	if sequence > maxEnvelopeFrameSequence {
		return fmt.Errorf("archive frame sequence exhausted at %d", sequence)
	}
	if !final && sequence == maxEnvelopeFrameSequence {
		return fmt.Errorf(
			"archive frame sequence exhausted at %d: sequence is reserved for the final frame",
			sequence,
		)
	}
	return nil
}

func readEnvelopeHeader(source io.Reader) ([]byte, envelopeHeader, []byte, error) {
	var header envelopeHeader
	magic := make([]byte, len(envelopeMagic))
	if _, err := io.ReadFull(source, magic); err != nil {
		return nil, header, nil, fmt.Errorf("read archive magic: %w", err)
	}
	if string(magic) != envelopeMagic {
		return nil, header, nil, fmt.Errorf("archive magic must be %q, got %q", envelopeMagic, string(magic))
	}

	var sizeBytes [4]byte
	if _, err := io.ReadFull(source, sizeBytes[:]); err != nil {
		return nil, header, nil, fmt.Errorf("read archive header size: %w", err)
	}
	headerSize := binary.BigEndian.Uint32(sizeBytes[:])
	if headerSize == 0 || headerSize > maxEnvelopeHeaderSize {
		return nil, header, nil, fmt.Errorf("archive header size %d is invalid", headerSize)
	}

	headerBytes := make([]byte, int(headerSize))
	if _, err := io.ReadFull(source, headerBytes); err != nil {
		return nil, header, nil, fmt.Errorf("read archive header: %w", err)
	}
	if err := validateExactJSONObject(
		headerBytes,
		"format",
		"kem",
		"kdf",
		"aead",
		"encapsulated_key",
		"frame_size",
	); err != nil {
		return nil, header, nil, fmt.Errorf("validate archive header JSON fields: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(headerBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&header); err != nil {
		return nil, header, nil, fmt.Errorf("decode archive header JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, header, nil, fmt.Errorf("archive header JSON contains a trailing value")
		}
		return nil, header, nil, fmt.Errorf("decode trailing archive header JSON: %w", err)
	}

	encapsulatedKey, err := validateEnvelopeHeader(header)
	if err != nil {
		return nil, header, nil, err
	}
	return headerBytes, header, encapsulatedKey, nil
}

func validateEnvelopeHeader(header envelopeHeader) ([]byte, error) {
	if header.Format != EnvelopeFormat {
		return nil, fmt.Errorf("envelope format must be %q, got %q", EnvelopeFormat, header.Format)
	}
	if header.KEM != kemName {
		return nil, fmt.Errorf("envelope KEM must be %q, got %q", kemName, header.KEM)
	}
	if header.KDF != kdfName {
		return nil, fmt.Errorf("envelope KDF must be %q, got %q", kdfName, header.KDF)
	}
	if header.AEAD != aeadName {
		return nil, fmt.Errorf("envelope AEAD must be %q, got %q", aeadName, header.AEAD)
	}
	if header.FrameSize != envelopeFrameSize {
		return nil, fmt.Errorf("envelope frame size must be %d, got %d", envelopeFrameSize, header.FrameSize)
	}
	if header.EncapsulatedKey == "" {
		return nil, fmt.Errorf("envelope encapsulated key is required")
	}

	encapsulatedKey, err := base64.StdEncoding.Strict().DecodeString(header.EncapsulatedKey)
	if err != nil || base64.StdEncoding.EncodeToString(encapsulatedKey) != header.EncapsulatedKey {
		if err == nil {
			err = fmt.Errorf("encoding is not canonical")
		}
		return nil, fmt.Errorf("decode encapsulated key as canonical base64: %w", err)
	}
	if len(encapsulatedKey) != publicKeyMaterialSize {
		return nil, fmt.Errorf(
			"encapsulated key has length %d, want %d",
			len(encapsulatedKey),
			publicKeyMaterialSize,
		)
	}
	return encapsulatedKey, nil
}

func requireEnvelopeEOF(source io.Reader) error {
	var extra [1]byte
	n, err := source.Read(extra[:])
	if n > 0 {
		return fmt.Errorf("encrypted archive has data after final frame")
	}
	if err == nil {
		return fmt.Errorf("encrypted archive source did not end after final frame")
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read encrypted archive trailer: %w", err)
	}
	return nil
}

func frameAAD(headerDigest []byte, sequence uint64, final bool) []byte {
	aad := make([]byte, 0, len(headerDigest)+9)
	aad = append(aad, headerDigest...)
	aad = binary.BigEndian.AppendUint64(aad, sequence)
	if final {
		return append(aad, 1)
	}
	return append(aad, 0)
}

func writeAll(destination io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := destination.Write(payload)
		if n < 0 || n > len(payload) {
			return fmt.Errorf("writer returned invalid byte count %d", n)
		}
		payload = payload[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

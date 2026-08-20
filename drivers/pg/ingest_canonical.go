package pg

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/specterops/dawgs/graph"
)

const (
	ingestNodeContentDomain = "dawgs:pg-ingest:node-content:v1"
	ingestEdgeContentDomain = "dawgs:pg-ingest:edge-content:v1"

	maxIngestNumericIntegerDigits    = 131072
	maxIngestNumericFractionalDigits = 16383
	maxIngestNumericLexicalExponent  = 1073741823
)

type ingestContentHash [16]byte

func normalizeIngestProperties(properties *graph.Properties) (map[string]any, error) {
	propertyMap := properties.MapOrEmpty()
	if err := validateIngestUTF8(reflect.ValueOf(propertyMap), map[ingestReference]struct{}{}); err != nil {
		return nil, err
	}

	encoded, err := json.Marshal(propertyMap)
	if err != nil {
		return nil, fmt.Errorf("marshaling ingest properties: %w", err)
	}
	if !utf8.Valid(encoded) {
		return nil, fmt.Errorf("marshaling ingest properties: invalid UTF-8")
	}
	if err := validateEncodedIngestJSONSurrogates(encoded); err != nil {
		return nil, fmt.Errorf("validating encoded ingest properties: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()

	var normalized map[string]any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, fmt.Errorf("decoding normalized ingest properties: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("decoding normalized ingest properties: trailing JSON value")
	}
	if normalized == nil {
		return nil, fmt.Errorf("decoding normalized ingest properties: expected JSON object")
	}
	if err := writeCanonicalIngestValue(io.Discard, normalized); err != nil {
		return nil, fmt.Errorf("validating normalized ingest properties: %w", err)
	}

	return normalized, nil
}

func hashIngestNodeContent(kinds graph.Kinds, properties map[string]any) (ingestContentHash, error) {
	hash := sha256.New()
	if err := writeIngestBytes(hash, []byte(ingestNodeContentDomain)); err != nil {
		return ingestContentHash{}, err
	}

	kindNames := make([]string, len(kinds))
	for index, kind := range kinds {
		if kind == nil {
			return ingestContentHash{}, fmt.Errorf("canonical ingest node content: nil kind")
		}

		kindNames[index] = kind.String()
		if err := validateIngestString(kindNames[index]); err != nil {
			return ingestContentHash{}, fmt.Errorf("canonical ingest node content kind: %w", err)
		}
	}
	sort.Strings(kindNames)

	if err := writeIngestUint64(hash, uint64(len(kindNames))); err != nil {
		return ingestContentHash{}, err
	}
	for _, kindName := range kindNames {
		if err := writeLengthFramedIngestBytes(hash, []byte(kindName)); err != nil {
			return ingestContentHash{}, err
		}
	}

	contentProperties := make(map[string]any, len(properties))
	for key, value := range properties {
		if key != "objectid" {
			contentProperties[key] = value
		}
	}
	if err := writeCanonicalIngestValue(hash, contentProperties); err != nil {
		return ingestContentHash{}, fmt.Errorf("canonical ingest node content: %w", err)
	}

	return truncateIngestContentHash(hash.Sum(nil)), nil
}

func hashIngestEdgeContent(properties map[string]any) (ingestContentHash, error) {
	hash := sha256.New()
	if err := writeIngestBytes(hash, []byte(ingestEdgeContentDomain)); err != nil {
		return ingestContentHash{}, err
	}
	if err := writeCanonicalIngestValue(hash, properties); err != nil {
		return ingestContentHash{}, fmt.Errorf("canonical ingest edge content: %w", err)
	}

	return truncateIngestContentHash(hash.Sum(nil)), nil
}

func truncateIngestContentHash(digest []byte) ingestContentHash {
	var truncated ingestContentHash
	copy(truncated[:], digest[:len(truncated)])

	return truncated
}

func writeCanonicalIngestValue(writer io.Writer, value any) error {
	switch typedValue := value.(type) {
	case nil:
		return writeIngestByte(writer, 0x00)

	case bool:
		if typedValue {
			return writeIngestByte(writer, 0x02)
		}
		return writeIngestByte(writer, 0x01)

	case string:
		if err := validateIngestString(typedValue); err != nil {
			return fmt.Errorf("canonical ingest string: %w", err)
		}
		if err := writeIngestByte(writer, 0x03); err != nil {
			return err
		}
		return writeLengthFramedIngestBytes(writer, []byte(typedValue))

	case json.Number:
		return writeCanonicalIngestNumber(writer, typedValue.String())

	case []any:
		if err := writeIngestByte(writer, 0x05); err != nil {
			return err
		}
		if err := writeIngestUint64(writer, uint64(len(typedValue))); err != nil {
			return err
		}
		for index, element := range typedValue {
			if err := writeCanonicalIngestValue(writer, element); err != nil {
				return fmt.Errorf("canonical ingest array element %d: %w", index, err)
			}
		}
		return nil

	case map[string]any:
		keys := make([]string, 0, len(typedValue))
		for key := range typedValue {
			if err := validateIngestString(key); err != nil {
				return fmt.Errorf("canonical ingest object key: %w", err)
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)

		if err := writeIngestByte(writer, 0x06); err != nil {
			return err
		}
		if err := writeIngestUint64(writer, uint64(len(keys))); err != nil {
			return err
		}
		for _, key := range keys {
			if err := writeLengthFramedIngestBytes(writer, []byte(key)); err != nil {
				return err
			}
			if err := writeCanonicalIngestValue(writer, typedValue[key]); err != nil {
				return fmt.Errorf("canonical ingest object key %q: %w", key, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("canonical ingest value has unsupported type %T", value)
	}
}

func writeCanonicalIngestNumber(writer io.Writer, number string) error {
	negative, digits, exponent, err := normalizeCanonicalIngestNumber(number)
	if err != nil {
		return err
	}

	if err := writeIngestByte(writer, 0x04); err != nil {
		return err
	}
	if negative {
		if err := writeIngestByte(writer, 0x01); err != nil {
			return err
		}
	} else if err := writeIngestByte(writer, 0x00); err != nil {
		return err
	}
	if err := writeLengthFramedIngestBytes(writer, []byte(digits)); err != nil {
		return err
	}

	var encodedExponent [binary.MaxVarintLen64]byte
	encodedLength := binary.PutUvarint(encodedExponent[:], zigZagIngestExponent(exponent))
	return writeIngestBytes(writer, encodedExponent[:encodedLength])
}

func normalizeCanonicalIngestNumber(number string) (bool, string, int64, error) {
	negative, integer, fraction, exponentDigits, exponentNegative, err := splitCanonicalIngestNumber(number)
	if err != nil {
		return false, "", 0, err
	}

	exponent := new(big.Int)
	if exponentDigits != "" {
		if _, ok := exponent.SetString(exponentDigits, 10); !ok {
			return false, "", 0, fmt.Errorf("canonical ingest number %q has invalid exponent", number)
		}
		if exponentNegative {
			exponent.Neg(exponent)
		}
	}
	lexicalExponentMagnitude := new(big.Int).Abs(new(big.Int).Set(exponent))
	if lexicalExponentMagnitude.Cmp(big.NewInt(maxIngestNumericLexicalExponent)) > 0 {
		return false, "", 0, fmt.Errorf(
			"canonical ingest number %q exceeds PostgreSQL numeric's lexical exponent range",
			number,
		)
	}
	lexicalScale := new(big.Int).Sub(big.NewInt(int64(len(fraction))), exponent)
	if lexicalScale.Sign() > 0 && lexicalScale.Cmp(big.NewInt(maxIngestNumericFractionalDigits)) > 0 {
		return false, "", 0, fmt.Errorf(
			"canonical ingest number %q exceeds PostgreSQL numeric's %d digits after the decimal point",
			number,
			maxIngestNumericFractionalDigits,
		)
	}

	digits := integer + fraction
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return false, "0", 0, nil
	}

	trailingZeros := len(digits) - len(strings.TrimRight(digits, "0"))
	digits = strings.TrimRight(digits, "0")

	exponent.Sub(exponent, big.NewInt(int64(len(fraction))))
	exponent.Add(exponent, big.NewInt(int64(trailingZeros)))

	if err := validateIngestNumericLimits(digits, exponent); err != nil {
		return false, "", 0, fmt.Errorf("canonical ingest number %q: %w", number, err)
	}
	if !exponent.IsInt64() {
		return false, "", 0, fmt.Errorf("canonical ingest number %q has exponent outside int64", number)
	}

	return negative, digits, exponent.Int64(), nil
}

func splitCanonicalIngestNumber(number string) (bool, string, string, string, bool, error) {
	if number == "" {
		return false, "", "", "", false, fmt.Errorf("canonical ingest number is empty")
	}

	index := 0
	negative := false
	if number[index] == '-' {
		negative = true
		index++
		if index == len(number) {
			return false, "", "", "", false, fmt.Errorf("canonical ingest number %q is invalid", number)
		}
	}

	integerStart := index
	if number[index] == '0' {
		index++
		if index < len(number) && isIngestDigit(number[index]) {
			return false, "", "", "", false, fmt.Errorf("canonical ingest number %q has a leading zero", number)
		}
	} else if number[index] >= '1' && number[index] <= '9' {
		for index < len(number) && isIngestDigit(number[index]) {
			index++
		}
	} else {
		return false, "", "", "", false, fmt.Errorf("canonical ingest number %q is invalid", number)
	}
	integer := number[integerStart:index]

	var fraction string
	if index < len(number) && number[index] == '.' {
		index++
		fractionStart := index
		for index < len(number) && isIngestDigit(number[index]) {
			index++
		}
		if fractionStart == index {
			return false, "", "", "", false, fmt.Errorf("canonical ingest number %q has an empty fraction", number)
		}
		fraction = number[fractionStart:index]
	}

	var (
		exponentDigits   string
		exponentNegative bool
	)
	if index < len(number) && (number[index] == 'e' || number[index] == 'E') {
		index++
		if index < len(number) && (number[index] == '+' || number[index] == '-') {
			exponentNegative = number[index] == '-'
			index++
		}
		exponentStart := index
		for index < len(number) && isIngestDigit(number[index]) {
			index++
		}
		if exponentStart == index {
			return false, "", "", "", false, fmt.Errorf("canonical ingest number %q has an empty exponent", number)
		}
		exponentDigits = number[exponentStart:index]
	}
	if index != len(number) {
		return false, "", "", "", false, fmt.Errorf("canonical ingest number %q is invalid", number)
	}

	return negative, integer, fraction, exponentDigits, exponentNegative, nil
}

func validateIngestNumericLimits(digits string, exponent *big.Int) error {
	digitsBeforeDecimal := new(big.Int).Add(big.NewInt(int64(len(digits))), exponent)
	if digitsBeforeDecimal.Sign() > 0 && digitsBeforeDecimal.Cmp(big.NewInt(maxIngestNumericIntegerDigits)) > 0 {
		return fmt.Errorf("exceeds PostgreSQL numeric's %d digits before the decimal point", maxIngestNumericIntegerDigits)
	}

	if exponent.Sign() < 0 {
		digitsAfterDecimal := new(big.Int).Neg(new(big.Int).Set(exponent))
		if digitsAfterDecimal.Cmp(big.NewInt(maxIngestNumericFractionalDigits)) > 0 {
			return fmt.Errorf("exceeds PostgreSQL numeric's %d digits after the decimal point", maxIngestNumericFractionalDigits)
		}
	}

	return nil
}

func zigZagIngestExponent(exponent int64) uint64 {
	return uint64(exponent)<<1 ^ uint64(exponent>>63)
}

func isIngestDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

func writeLengthFramedIngestBytes(writer io.Writer, value []byte) error {
	if err := writeIngestUint64(writer, uint64(len(value))); err != nil {
		return err
	}
	return writeIngestBytes(writer, value)
}

func writeIngestUint64(writer io.Writer, value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)

	return writeIngestBytes(writer, encoded[:])
}

func writeIngestByte(writer io.Writer, value byte) error {
	var encoded [1]byte
	encoded[0] = value

	return writeIngestBytes(writer, encoded[:])
}

func writeIngestBytes(writer io.Writer, value []byte) error {
	written, err := writer.Write(value)
	if err != nil {
		return fmt.Errorf("writing canonical ingest value: %w", err)
	}
	if written != len(value) {
		return fmt.Errorf("writing canonical ingest value: %w", io.ErrShortWrite)
	}

	return nil
}

type ingestReference struct {
	typeOf  reflect.Type
	pointer uintptr
	length  int
}

func validateIngestUTF8(value reflect.Value, seen map[ingestReference]struct{}) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateIngestUTF8(value.Elem(), seen)
	}

	switch value.Kind() {
	case reflect.String:
		if err := validateIngestString(value.String()); err != nil {
			return fmt.Errorf("ingest properties: %w", err)
		}

	case reflect.Pointer:
		if value.IsNil() || ingestReferenceSeen(value, seen) {
			return nil
		}
		return validateIngestUTF8(value.Elem(), seen)

	case reflect.Map:
		if value.IsNil() || ingestReferenceSeen(value, seen) {
			return nil
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateIngestUTF8(iterator.Key(), seen); err != nil {
				return err
			}
			if err := validateIngestUTF8(iterator.Value(), seen); err != nil {
				return err
			}
		}

	case reflect.Slice:
		if value.IsNil() || ingestReferenceSeen(value, seen) {
			return nil
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateIngestUTF8(value.Index(index), seen); err != nil {
				return err
			}
		}

	case reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if err := validateIngestUTF8(value.Index(index), seen); err != nil {
				return err
			}
		}

	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath == "" {
				if err := validateIngestUTF8(value.Field(index), seen); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func validateIngestString(value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("contains invalid UTF-8")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("contains U+0000")
	}

	return nil
}

func validateEncodedIngestJSONSurrogates(encoded []byte) error {
	inString := false
	for index := 0; index < len(encoded); index++ {
		switch encoded[index] {
		case '"':
			inString = !inString

		case '\\':
			if !inString || index+1 >= len(encoded) {
				continue
			}
			if encoded[index+1] != 'u' {
				index++
				continue
			}

			codePoint, ok := decodeEncodedIngestJSONCodeUnit(encoded, index)
			if !ok {
				return fmt.Errorf("malformed Unicode escape in JSON string at byte %d", index)
			}
			switch {
			case codePoint >= 0xd800 && codePoint <= 0xdbff:
				lowIndex := index + 6
				lowCodePoint, lowOK := decodeEncodedIngestJSONCodeUnit(encoded, lowIndex)
				if !lowOK || lowCodePoint < 0xdc00 || lowCodePoint > 0xdfff {
					return fmt.Errorf(
						"high surrogate escape in JSON string at byte %d is not immediately followed by a low surrogate escape",
						index,
					)
				}
				index = lowIndex + 5

			case codePoint >= 0xdc00 && codePoint <= 0xdfff:
				return fmt.Errorf(
					"low surrogate escape in JSON string at byte %d has no preceding high surrogate escape",
					index,
				)

			default:
				index += 5
			}
		}
	}

	return nil
}

func decodeEncodedIngestJSONCodeUnit(encoded []byte, escapeIndex int) (uint16, bool) {
	if escapeIndex < 0 || escapeIndex+6 > len(encoded) ||
		encoded[escapeIndex] != '\\' || encoded[escapeIndex+1] != 'u' {
		return 0, false
	}

	var codeUnit uint16
	for _, digit := range encoded[escapeIndex+2 : escapeIndex+6] {
		codeUnit <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			codeUnit |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			codeUnit |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			codeUnit |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}

	return codeUnit, true
}

func ingestReferenceSeen(value reflect.Value, seen map[ingestReference]struct{}) bool {
	reference := ingestReference{typeOf: value.Type(), pointer: value.Pointer()}
	if value.Kind() == reflect.Slice {
		reference.length = value.Len()
	}
	if _, found := seen[reference]; found {
		return true
	}
	seen[reference] = struct{}{}

	return false
}

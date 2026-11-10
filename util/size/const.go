package size

const (
	// iecUnitFactor is the binary multiplier between adjacent IEC byte units.
	iecUnitFactor = 1024

	// Bytes is the base unit of one byte.
	Bytes Size = 1

	// Kibibyte represents 1,024 bytes.
	Kibibyte = Bytes * iecUnitFactor

	// Mebibyte represents 1,024 kibibytes.
	Mebibyte = Kibibyte * iecUnitFactor

	// Gibibyte represents 1,024 mebibytes.
	Gibibyte = Mebibyte * iecUnitFactor

	// Tebibyte represents 1,024 gibibytes.
	Tebibyte = Gibibyte * iecUnitFactor

	// Pebibyte represents 1,024 tebibytes.
	Pebibyte = Tebibyte * iecUnitFactor
)

// Size is a byte count with the range of uintptr on the current architecture.
type Size uintptr

// Bytes returns the byte count as an unsigned machine-sized integer.
func (s Size) Bytes() uintptr {
	return uintptr(s)
}

// Kibibytes converts the byte count to fractional IEC kibibytes.
func (s Size) Kibibytes() float64 {
	return float64(s) / float64(Kibibyte)
}

// Mebibytes converts the byte count to fractional IEC mebibytes.
func (s Size) Mebibytes() float64 {
	return float64(s) / float64(Mebibyte)
}

// Gibibyte converts the byte count to fractional IEC gibibytes.
func (s Size) Gibibyte() float64 {
	return float64(s) / float64(Gibibyte)
}

// Tebibyte converts the byte count to fractional IEC tebibytes.
func (s Size) Tebibyte() float64 {
	return float64(s) / float64(Tebibyte)
}

// Pebibyte converts the byte count to fractional IEC pebibytes.
func (s Size) Pebibyte() float64 {
	return float64(s) / float64(Pebibyte)
}

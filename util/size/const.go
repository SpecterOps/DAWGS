package size

const (
	iecUnitFactor = 1024

	Bytes    Size = 1
	Kibibyte      = Bytes * iecUnitFactor
	Mebibyte      = Kibibyte * iecUnitFactor
	Gibibyte      = Mebibyte * iecUnitFactor
	Tebibyte      = Gibibyte * iecUnitFactor
	Pebibyte      = Tebibyte * iecUnitFactor
)

type Size uintptr

func (s Size) Bytes() uintptr {
	return uintptr(s)
}

func (s Size) Kibibytes() float64 {
	return float64(s) / float64(Kibibyte)
}

func (s Size) Mebibytes() float64 {
	return float64(s) / float64(Mebibyte)
}

func (s Size) Gibibyte() float64 {
	return float64(s) / float64(Gibibyte)
}

func (s Size) Tebibyte() float64 {
	return float64(s) / float64(Tebibyte)
}

func (s Size) Pebibyte() float64 {
	return float64(s) / float64(Pebibyte)
}

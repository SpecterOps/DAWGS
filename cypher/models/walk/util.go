package walk

import "fmt"

func ConvertSliceType[T, F any, TS []T, FS []F](fs FS) (TS, error) {
	var (
		mapped = make(TS, len(fs))
		emptyT T
	)

	for idx, rawValue := range fs {
		if typedValue, typeOK := any(rawValue).(T); !typeOK {
			return nil, fmt.Errorf("cypherSyntaxNodeSliceTypeConvert[F]: invalid type %T does not convert to %T", rawValue, emptyT)
		} else {
			mapped[idx] = typedValue
		}
	}

	return mapped, nil
}

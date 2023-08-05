package slice

import "strconv"

func StrTo[T ~int64 | ~float64 | ~bool](slice []string) ([]T, error) {
	var (
		val  T
		zero []T
		f    func(str string) (any, error)
	)

	switch any(val).(type) {
	case int64:
		f = func(str string) (any, error) {
			return strconv.ParseInt(str, 10, 64)
		}
	case float64:
		f = func(str string) (any, error) {
			return strconv.ParseFloat(str, 64)
		}
	case bool:
		f = func(str string) (any, error) {
			return strconv.ParseBool(str)
		}
	}

	newSlice := make([]T, len(slice))
	for i, pv := range slice {
		val, err := f(pv)
		if err != nil {
			return zero, err
		}
		newSlice[i] = val.(T)
	}
	return newSlice, nil
}

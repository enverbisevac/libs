package slice

import (
	"strconv"
	"time"

	"github.com/enverbisevac/libs/timeutil"
)

func StrTo[T ~int | ~int64 | ~float64 | ~bool | time.Time](slice []string) ([]T, error) {
	var (
		val T
		f   func(str string) (any, error)
	)

	switch any(val).(type) {
	case int:
		f = func(str string) (any, error) {
			v, err := strconv.ParseInt(str, 10, 32)
			return int(v), err
		}
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
	case time.Time:
		f = func(str string) (any, error) {
			return timeutil.DefaultParserFunc(str)
		}
	}

	newSlice := make([]T, len(slice))
	for i, pv := range slice {
		val, err := f(pv)
		if err != nil {
			return []T{}, err
		}
		newSlice[i] = val.(T)
	}
	return newSlice, nil
}

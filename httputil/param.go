package httputil

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/enverbisevac/libs/slice"
	"github.com/enverbisevac/libs/validator"
)

type ParamTypes interface {
	~string | ~int64 | ~bool | ~float64 |
		~[]string | ~[]int64 | ~[]bool | ~[]float64
}

type FromConstraint interface {
	*http.Request | *url.URL | url.Values
}

func QueryParamOrDefault[T ParamTypes, K FromConstraint](from K, param string, defValue T, validators ...validator.Validator[T]) (T, error) {
	var (
		zero   T
		result any
		err    error
		values url.Values
	)

	switch t := any(from).(type) {
	case *http.Request:
		values = t.URL.Query()
	case *url.URL:
		values = t.Query()
	case url.Values:
		values = t
	}

	paramValues, ok := values[param]
	if !ok || len(paramValues) == 0 {
		return defValue, nil
	}

	paramValue := paramValues[0]
	if paramValue == "" {
		return defValue, nil
	}

	switch any(defValue).(type) {
	case string:
		result = paramValue
	case int64:
		result, err = strconv.ParseInt(paramValue, 10, 64)
	case bool:
		result, err = strconv.ParseBool(paramValue)
	case float64:
		result, err = strconv.ParseFloat(paramValue, 64)
	case []string:
		result = paramValues
	case []int64:
		result, err = slice.StrTo[int64](paramValues)
	case []float64:
		result, err = slice.StrTo[float64](paramValues)
	case []bool:
		result, err = slice.StrTo[bool](paramValues)
	default:
		result = defValue
	}

	if err != nil {
		return zero, err
	}

	// check if value is validated or return default value
	if err = validator.Validate(result.(T), validators...); err != nil {
		return defValue, nil
	}

	return result.(T), nil
}

func QueryParam[T ParamTypes, K FromConstraint](from K, param string, validators ...validator.Validator[T]) (zero T, _ error) {
	return QueryParamOrDefault(from, param, zero, validators...)
}
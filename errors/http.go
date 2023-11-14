package errors

import "net/http"

var (
	HttpMap = map[Status]int{
		StatusConflict:           http.StatusConflict,
		StatusInternal:           http.StatusInternalServerError,
		StatusInvalidArgument:    http.StatusBadRequest,
		StatusNotFound:           http.StatusNotFound,
		StatusNotImplemented:     http.StatusNotImplemented,
		StatusUnauthenticated:    http.StatusUnauthorized,
		StatusUnauthorized:       http.StatusForbidden,
		StatusPreconditionFailed: http.StatusPreconditionFailed,
		StatusAborted:            http.StatusInternalServerError,
	}
)

package errors

import "net/http"

var (
	HttpMap = map[Code]int{
		CodeConflict:           http.StatusConflict,
		CodeInternal:           http.StatusInternalServerError,
		CodeInvalidArgument:    http.StatusBadRequest,
		CodeNotFound:           http.StatusNotFound,
		CodeNotImplemented:     http.StatusNotImplemented,
		CodeUnauthenticated:    http.StatusUnauthorized,
		CodeUnauthorized:       http.StatusForbidden,
		CodePreconditionFailed: http.StatusPreconditionFailed,
		CodeAborted:            http.StatusInternalServerError,
	}
)
